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
        for attempt in 1..=6 {
            match TcpStream::connect(addr).await {
                Ok(c) => return Some(c),
                Err(e) => {
                    eprintln!(
                        "failed to establish TCP connection with server \
                         (attempt {attempt}/6), retrying: {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            };
        }
        None
    }

    /// Run this node: accept inbound connections in the background, dial the
    /// server first if we are a client (so the server writer is registered
    /// before anything is dispatched — otherwise early messages are dropped
    /// as "not registered"), then dispatch outgoing messages forever.
    pub async fn run(self, source: NetworkReceiver) -> anyhow::Result<()> {
        let listener = TcpListener::bind(self.listener_addr.as_str())
            .await
            .context("failed to bind TCP listener on given address")?;

        let this = Arc::new(self);
        let acceptor = Arc::clone(&this);
        tokio::spawn(async move {
            if let Err(e) = acceptor.listen(listener).await {
                eprintln!("accept loop exited: {e}");
            }
        });

        if let NodeId::Client(_) = this.id.clone() {
            let addr = this.server_addr.clone();
            this.dial_server(addr.as_str())
                .await
                .context("failed to dial server")?;
        }

        dispatch_outgoing(
            source,
            Arc::clone(&this.writers_map),
            this.server_id.clone(),
        )
        .await;
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

/// Tests for the networking layer: writer registry, incoming routing,
/// outgoing dispatch, `connect_with_retry`, and one TCP round-trip proving
/// handshake + framed delivery work end to end.
///
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        client::ClientId,
        protocol::{Request, Response, ServerMessagePayload},
        store::Key,
    };

    const SID: u64 = 1;

    fn client_msg(client: u64, key: &str) -> Message {
        Message::Client(ClientMessage {
            client_id: ClientId::new(client),
            request: Request::Read {
                key: Key::from_string(key.to_string()),
            },
        })
    }

    fn write_ok_msg() -> Message {
        Message::Server(ServerMessage {
            server_id: ServerId::new(SID),
            payload: ServerMessagePayload::Reply(Response::WriteOk),
        })
    }

    fn read_key_of(msg: &Message) -> Key {
        match msg {
            Message::Client(cm) => match &cm.request {
                Request::Read { key } => key.clone(),
                _ => panic!("expected Read request"),
            },
            _ => panic!("expected Client message"),
        }
    }

    #[tokio::test]
    async fn test_writers_registry_send_delivers_to_registered_node() {
        let reg = WritersRegistry::default();
        let mut rx = reg.register(NodeId::Client(ClientId::new(7))).await;

        let msg = Arc::new(client_msg(7, "k"));
        reg.send(NodeId::Client(ClientId::new(7)), Arc::clone(&msg))
            .await
            .unwrap();

        let got = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("no message delivered")
            .expect("channel closed");
        assert_eq!(read_key_of(&got), Key::from_string("k".to_string()));
    }

    #[tokio::test]
    async fn test_writers_registry_send_to_unknown_node_errors() {
        let reg = WritersRegistry::default();
        let err = reg
            .send(NodeId::Client(ClientId::new(9)), Arc::new(write_ok_msg()))
            .await
            .expect_err("expected error for unregistered node");
        assert!(err.to_string().contains("not registered"));
    }

    #[tokio::test]
    async fn test_writers_registry_get_all_clients_lists_registered_nodes() {
        let reg = WritersRegistry::default();
        reg.register(NodeId::Client(ClientId::new(1))).await;
        reg.register(NodeId::Client(ClientId::new(2))).await;

        let mut got = reg.get_all_clients().await;
        got.sort_by_key(|n| match n {
            NodeId::Client(id) => id.clone(),
            NodeId::Server(_) => ClientId::new(u64::MAX),
        });
        assert_eq!(
            got,
            vec![
                NodeId::Client(ClientId::new(1)),
                NodeId::Client(ClientId::new(2))
            ]
        );
    }

    #[tokio::test]
    async fn test_route_incoming_forwards_client_message_to_server_router() {
        let (tx, mut rx) = mpsc::channel::<ClientMessage>(8);
        let router = IncomingRouter::Server(tx);

        route_incoming(client_msg(7, "k"), &router).await.unwrap();

        let got = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("message not forwarded")
            .expect("channel closed");
        assert_eq!(got.client_id, ClientId::new(7));
    }

    #[tokio::test]
    async fn test_route_incoming_drops_client_message_on_client_router() {
        let (tx, mut rx) = mpsc::channel::<ServerMessage>(8);
        let router = IncomingRouter::Client(tx);

        // Wrong-direction message: logged and dropped, but Ok.
        route_incoming(client_msg(7, "k"), &router).await.unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_route_incoming_forwards_server_message_to_client_router() {
        let (tx, mut rx) = mpsc::channel::<ServerMessage>(8);
        let router = IncomingRouter::Client(tx);

        route_incoming(write_ok_msg(), &router).await.unwrap();

        let got = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("message not forwarded")
            .expect("channel closed");
        assert!(matches!(
            got.payload,
            ServerMessagePayload::Reply(Response::WriteOk)
        ));
    }

    #[tokio::test]
    async fn test_route_incoming_drops_server_message_on_server_router() {
        let (tx, mut rx) = mpsc::channel::<ClientMessage>(8);
        let router = IncomingRouter::Server(tx);

        route_incoming(write_ok_msg(), &router).await.unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_route_incoming_ignores_handshake() {
        let (tx, mut rx) = mpsc::channel::<ServerMessage>(8);
        let router = IncomingRouter::Client(tx);
        let hs = Message::HandshakeType(Handshake {
            node_id: NodeId::Server(ServerId::new(SID)),
        });

        route_incoming(hs, &router).await.unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_dispatch_outgoing_from_server_unicast_and_broadcast() {
        let reg = Arc::new(WritersRegistry::default());
        let mut rx1 = reg.register(NodeId::Client(ClientId::new(1))).await;
        let mut rx2 = reg.register(NodeId::Client(ClientId::new(2))).await;
        let (tx, rx) = mpsc::channel::<OutgoingFromServer>(8);
        let task = tokio::spawn(dispatch_outgoing_from_server(rx, Arc::clone(&reg)));

        // Unicast reaches only its target.
        tx.send(OutgoingFromServer {
            to: OutgoingReciever::Client(ClientId::new(1)),
            msg: write_ok_msg(),
        })
        .await
        .unwrap();
        let got = timeout(Duration::from_secs(2), rx1.recv())
            .await
            .expect("unicast not delivered")
            .expect("channel closed");
        assert!(matches!(&*got, Message::Server(ServerMessage { .. })));
        assert!(rx2.try_recv().is_err());

        // Broadcast reaches everyone registered.
        tx.send(OutgoingFromServer {
            to: OutgoingReciever::Broadcast,
            msg: write_ok_msg(),
        })
        .await
        .unwrap();
        for rx in [&mut rx1, &mut rx2] {
            timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("broadcast not delivered")
                .expect("channel closed");
        }

        task.abort();
    }

    #[tokio::test]
    async fn test_dispatch_outgoing_from_client_forwards_to_server_writer() {
        let reg = Arc::new(WritersRegistry::default());
        let mut writer_rx = reg.register(NodeId::Server(ServerId::new(SID))).await;
        let (tx, rx) = mpsc::channel::<Message>(8);
        let task = tokio::spawn(dispatch_outgoing_from_client(
            rx,
            Arc::clone(&reg),
            ServerId::new(SID),
        ));

        tx.send(client_msg(7, "hello")).await.unwrap();
        let got = timeout(Duration::from_secs(2), writer_rx.recv())
            .await
            .expect("message not forwarded to server writer")
            .expect("channel closed");
        assert_eq!(read_key_of(&got), Key::from_string("hello".to_string()));

        task.abort();
    }

    /// Helper: a `NetworkManager` for tests that drive `handle_conn` /
    /// dispatch directly (no listener needed).
    fn test_manager(id: NodeId, router: IncomingRouter) -> NetworkManager {
        NetworkManager::new(
            id,
            Codec::new(),
            router,
            String::new(),
            String::new(),
            ServerId::new(SID),
        )
    }

    #[tokio::test]
    async fn test_connect_with_retry_returns_stream_on_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let mgr = test_manager(
            NodeId::Client(ClientId::new(7)),
            IncomingRouter::Client(mpsc::channel(8).0),
        );

        let conn = timeout(Duration::from_secs(5), mgr.connect_with_retry(&addr))
            .await
            .expect("connect hung");
        assert!(conn.is_some(), "expected Some(TcpStream)");
    }

    #[tokio::test]
    async fn test_connect_with_retry_returns_none_when_unreachable() {
        // Bind then drop: guaranteed closed port, connection refused fast.
        let port = {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap().port()
        };
        let mgr = test_manager(
            NodeId::Client(ClientId::new(7)),
            IncomingRouter::Client(mpsc::channel(8).0),
        );

        let conn = timeout(
            Duration::from_secs(10),
            mgr.connect_with_retry(&format!("127.0.0.1:{port}")),
        )
        .await
        .expect("connect hung");
        assert!(conn.is_none(), "expected None for refused connection");
    }

    #[tokio::test]
    async fn test_handle_conn_handshake_and_message_flow_over_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Server side: receives ClientMessages, driven dispatcher for sends.
        let (srv_in_tx, mut srv_in_rx) = mpsc::channel::<ClientMessage>(8);
        let server_mgr = test_manager(
            NodeId::Server(ServerId::new(SID)),
            IncomingRouter::Server(srv_in_tx),
        );
        let (srv_out_tx, srv_out_rx) = mpsc::channel::<OutgoingFromServer>(8);

        // Client side: receives ServerMessages, driven dispatcher for sends.
        let (cli_in_tx, mut cli_in_rx) = mpsc::channel::<ServerMessage>(8);
        let client_mgr = test_manager(
            NodeId::Client(ClientId::new(7)),
            IncomingRouter::Client(cli_in_tx),
        );
        let (cli_out_tx, cli_out_rx) = mpsc::channel::<Message>(8);

        let accept = tokio::spawn(async move { listener.accept().await.unwrap().0 });
        let cli_sock = TcpStream::connect(addr).await.unwrap();
        let srv_sock = accept.await.unwrap();

        // Both sides handshake concurrently; each learns the peer's NodeId
        // and registers its writer tasks.
        let (sr, cr) = tokio::join!(
            server_mgr.handle_conn(srv_sock),
            client_mgr.handle_conn(cli_sock)
        );
        sr.expect("server handshake failed");
        cr.expect("client handshake failed");

        let srv_dispatch = tokio::spawn(dispatch_outgoing_from_server(
            srv_out_rx,
            Arc::clone(&server_mgr.writers_map),
        ));
        let cli_dispatch = tokio::spawn(dispatch_outgoing_from_client(
            cli_out_rx,
            Arc::clone(&client_mgr.writers_map),
            ServerId::new(SID),
        ));

        // Server -> client: invalidate-style push over the wire.
        srv_out_tx
            .send(OutgoingFromServer {
                to: OutgoingReciever::Client(ClientId::new(7)),
                msg: write_ok_msg(),
            })
            .await
            .unwrap();
        let got = timeout(Duration::from_secs(3), cli_in_rx.recv())
            .await
            .expect("server->client message never arrived")
            .expect("channel closed");
        assert!(matches!(
            got.payload,
            ServerMessagePayload::Reply(Response::WriteOk)
        ));

        // Client -> server: read request over the wire.
        cli_out_tx.send(client_msg(7, "e2e")).await.unwrap();
        let got = timeout(Duration::from_secs(3), srv_in_rx.recv())
            .await
            .expect("client->server message never arrived")
            .expect("channel closed");
        assert_eq!(got.client_id, ClientId::new(7));

        srv_dispatch.abort();
        cli_dispatch.abort();
    }
}
