use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc, mpsc::Receiver, mpsc::Sender};
use tokio::time::{Duration, timeout};
use tokio_util::codec::Framed;

use futures::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};

use crate::protocol::Handshake;
use crate::{
    protocol::{
        ClientMessage, Codec, Message, NodeId, OutgoingFromServer, OutgoingReciever, ServerMessage,
    },
    server::ServerId,
};

pub struct NetworkManager {
    id: NodeId,
    codec: Codec,
    listener_addr: String,
    server_addr: String,
    server_id: ServerId,
    writers_map: Arc<WritersRegistry>,
    dispatcher_source: NetworkReceiver,
    incoming_router: IncomingRouter,
}

#[derive(Default)]
struct WritersRegistry {
    writers_map: Arc<Mutex<HashMap<NodeId, Sender<Arc<Message>>>>>,
}

impl WritersRegistry {
    async fn register(&self, node_id: NodeId) -> Receiver<Arc<Message>> {
        let (tx, rx) = mpsc::channel::<Arc<Message>>(32);
        self.writers_map.lock().await.insert(node_id, tx);
        rx
    }

    async fn send(&self, node_id: NodeId, msg: Arc<Message>) -> anyhow::Result<()> {
        let tx = self
            .writers_map
            .lock()
            .await
            .get(&node_id)
            .ok_or(anyhow::anyhow!("client not registered"))?
            .clone();
        tx.send(msg)
            .await
            .map_err(|_| anyhow::anyhow!("client input channel closed"))
    }

    async fn get_all_clients(&self) -> Vec<NodeId> {
        self.writers_map.lock().await.keys().cloned().collect()
    }
}

pub enum NetworkReceiver {
    Client(Receiver<Message>),
    Server(Receiver<OutgoingFromServer>),
}

#[derive(Clone)]
pub enum IncomingRouter {
    Client(Sender<ServerMessage>),
    Server(Sender<ClientMessage>),
}

impl NetworkManager {
    pub fn new(
        id: NodeId,
        codec: Codec,
        dispatcher_source: NetworkReceiver,
        incoming_router: IncomingRouter,
        listener_addr: String,
        server_addr: String,
        server_id: ServerId,
    ) -> NetworkManager {
        let writers_map = WritersRegistry::default();
        Self {
            id,
            codec,
            listener_addr,
            server_addr,
            dispatcher_source,
            incoming_router,
            writers_map: Arc::new(writers_map),
            server_id,
        }
    }

    //listen for incoming connections
    async fn listen(&self, listener: TcpListener) -> anyhow::Result<()> {
        loop {
            let (conn, _) = match listener.accept().await {
                Ok(val) => val,
                Err(e) => {
                    eprintln!("failed to accept incoming TCP connection {e}");
                    continue;
                }
            };

            if let Err(e) = self.handle_conn(conn).await {
                eprintln!("failure while handling connection {e}");
            }
        }
    }

    async fn handle_conn(&self, conn: TcpStream) -> anyhow::Result<()> {
        let mut framed = Framed::new(conn, self.codec.clone());
        let node_id = self
            .perform_handshake(&mut framed)
            .await
            .context("failed to perform handshake")?;

        let Some(node_id) = node_id else {
            println!("closing connection, peer returned no handshake response");
            return Ok(());
        };

        let (sink, stream) = framed.split();
        let from_dispatcher = self.writers_map.register(node_id).await;
        self.spawn_reader_writer_tasks(stream, sink, from_dispatcher)
            .await;
        Ok(())
    }

    async fn perform_handshake(
        &self,
        conn: &mut Framed<TcpStream, Codec>,
    ) -> anyhow::Result<Option<NodeId>> {
        conn.send(Arc::new(Message::HandshakeType(Handshake {
            node_id: self.id.clone(),
        })))
        .await
        .context("failed to send handshake message")?;

        let response = self
            .handshake_response(conn)
            .await
            .context("failed to read handshake response")?;
        match response {
            Some(Message::HandshakeType(msg)) => return Ok(Some(msg.node_id)),
            Some(_other) => anyhow::bail!("invalid handshake response type"),
            None => return Ok(None),
        };
    }

    async fn handshake_response(
        &self,
        conn: &mut Framed<TcpStream, Codec>,
    ) -> anyhow::Result<Option<Message>> {
        let handshake_timeout = Duration::from_secs(5);
        let msg_future = timeout(handshake_timeout, conn.next());

        match msg_future
            .await
            .context("handshake timed out after 5 seconds")?
        {
            Some(Ok(m)) => Ok(Some(m)),
            Some(Err(e)) => {
                eprintln!("codec decoding error during handshake response {e}");
                anyhow::bail!("failed to receive handshake message: {e}")
            }
            None => {
                eprintln!("peer closed connection prematurely during handshake");
                return Ok(None);
            }
        }
    }

    async fn spawn_reader_writer_tasks(
        &self,
        stream: SplitStream<Framed<TcpStream, Codec>>,
        sink: SplitSink<Framed<TcpStream, Codec>, Arc<Message>>,
        from_dispatcher: Receiver<Arc<Message>>,
    ) {
        let incoming_router = self.incoming_router.clone();
        tokio::spawn(async move {
            framed_reader(stream, incoming_router).await;
        });
        tokio::spawn(async move {
            framed_writer(sink, from_dispatcher).await;
        });
    }

    /// Used by client nodes to connect to server on startup
    async fn dial_server(&self, server_addr: &str) -> anyhow::Result<()> {
        let Some(conn) = self.connect_with_retry(server_addr).await else {
            anyhow::bail!("failed to dial server");
        };
        self.handle_conn(conn)
            .await
            .context("failed to handle connection")?;
        Ok(())
    }

    async fn connect_with_retry(&self, addr: &str) -> Option<TcpStream> {
        for _ in 1..=6 {
            match TcpStream::connect(addr).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("failed to establish TCP connection with server, retrying {e}");
                    continue;
                }
            };
        }
        None
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(self.listener_addr.as_str())
            .await
            .context("failed to bind TCP listener on given address")?;

        match self.id.clone() {
            NodeId::Client(_) => self.run_as_client(listener).await,
            NodeId::Server(_) => self.run_as_server(listener).await,
        }
    }

    pub async fn run_as_client(self, listener: TcpListener) -> anyhow::Result<()> {
        self.listen(listener).await?;
        self.dial_server(self.server_addr.as_str())
            .await
            .context("failed to dial server")?;

        dispatch_outgoing(self.dispatcher_source, self.writers_map, self.server_id).await;
        Ok(())
    }

    pub async fn run_as_server(self, listener: TcpListener) -> anyhow::Result<()> {
        self.listen(listener).await?;
        dispatch_outgoing(self.dispatcher_source, self.writers_map, self.server_id).await;
        Ok(())
    }
}

async fn framed_reader(
    mut framed_stream: SplitStream<Framed<TcpStream, Codec>>,
    incoming_router: IncomingRouter,
) {
    while let Some(incoming_msg) = framed_stream.next().await {
        let msg = match incoming_msg {
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("error decoding frame from strea, skipping frame {e}");
                continue;
            }
        };
        if let Err(e) = route_incoming(msg, &incoming_router).await {
            eprintln!("error processing incoming message: {e}");
            eprintln!("terminating reader loop");
            break;
        }
    }
}

async fn route_incoming(msg: Message, incoming_router: &IncomingRouter) -> anyhow::Result<()> {
    match msg {
        Message::Client(client_msg) => {
            let IncomingRouter::Server(to_server) = incoming_router else {
                eprintln!("expected server input channel");
                eprintln!("client received message from client, dropping message");
                return Ok(());
            };
            to_server
                .send(client_msg)
                .await
                .context("server input channel closed")?
        }
        Message::Server(server_msg) => {
            let IncomingRouter::Client(to_client) = incoming_router else {
                eprintln!("expected client input channel");
                return Ok(());
            };
            to_client
                .send(server_msg)
                .await
                .context("client output channel closed")?
        }
        Message::HandshakeType(_) => {}
    }
    Ok(())
}

//dispatches outgoing messages from current node to network
async fn dispatch_outgoing(
    source: NetworkReceiver,
    writers_map: Arc<WritersRegistry>,
    server_id: ServerId,
) {
    match source {
        NetworkReceiver::Client(from_client) => {
            dispatch_outgoing_from_client(from_client, writers_map, server_id).await
        }
        NetworkReceiver::Server(from_server) => {
            dispatch_outgoing_from_server(from_server, writers_map).await
        }
    }
}

//used if current node is client
async fn dispatch_outgoing_from_client(
    mut from_client: Receiver<Message>,
    writers_map: Arc<WritersRegistry>,
    server_id: ServerId,
) {
    while let Some(outgoing) = from_client.recv().await {
        if let Err(e) = writers_map
            .send(NodeId::Server(server_id.clone()), Arc::new(outgoing))
            .await
        {
            eprintln!("Dispatcher failed to forawrd server message to writer: {e}");
        }
    }
}

//used if current node is server
async fn dispatch_outgoing_from_server(
    mut from_server: Receiver<OutgoingFromServer>,
    writers_map: Arc<WritersRegistry>,
) {
    while let Some(outgoing) = from_server.recv().await {
        let clients = match outgoing.to {
            OutgoingReciever::Client(client_id) => {
                vec![NodeId::Client(client_id)]
            }
            OutgoingReciever::Broadcast => writers_map.get_all_clients().await,
        };

        let outgoing_msg = Arc::new(outgoing.msg);
        for client_id in clients {
            if let Err(e) = writers_map.send(client_id, Arc::clone(&outgoing_msg)).await {
                eprintln!("failed to dispatch server message {e} ");
            }
        }
    }
}

async fn framed_writer(
    mut framed_sink: SplitSink<Framed<TcpStream, Codec>, Arc<Message>>,
    mut from_dispatcher: Receiver<Arc<Message>>,
) {
    while let Some(msg) = from_dispatcher.recv().await {
        if let Err(e) = framed_sink.send(msg).await {
            eprintln!("failed to send message frame over TCP sink, writer exiting : {e}");
            break;
        }
    }
}
