use serde::{Deserialize, Serialize};

use crate::client::ClientId;
use crate::clock::Clock;
use crate::lease::{Lease, LeaseTable};
use crate::protocol::{
    AppError, ClientMessage, Message, OutgoingFromServer, OutgoingReciever, Request, Response,
    ServerMessage, ServerMessagePayload, ServerPush,
};
use crate::store::{Key, Store, Value};

use std::cmp;
use std::sync::Arc;
use std::time::Duration;

// abstracts "tell client X to invalidate key Y" so tests can fake it
pub trait ClientNotifier: Send + Sync {
    fn send_invalidate(&self, client_id: ClientId, key: &Key);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerId(u64);

pub struct Server {
    store: Arc<dyn Store>,
    leases: LeaseTable,
    notifier: Arc<dyn ClientNotifier>,
    clock: Arc<dyn Clock>,
    client_listener: tokio::sync::mpsc::Receiver<ClientMessage>,
    outgoing: tokio::sync::mpsc::Sender<OutgoingFromServer>,
    id: ServerId,
}

impl Server {
    pub fn new(
        store: Arc<dyn Store>,
        notifier: Arc<dyn ClientNotifier>,
        clock: Arc<dyn Clock>,
        leases: LeaseTable,
        client_listener: tokio::sync::mpsc::Receiver<ClientMessage>,
        outgoing: tokio::sync::mpsc::Sender<OutgoingFromServer>,
        id: ServerId,
    ) -> Self {
        Self {
            store,
            notifier,
            clock,
            leases,
            client_listener,
            outgoing,
            id,
        }
    }

    pub fn run(&mut self) {
        self.dispatch_client_msgs();
    }

    async fn dispatch_client_msgs(&mut self) {
        while let Some(msg) = self.client_listener.recv().await {
            match msg.request {
                Request::Read { key } => {
                    self.read(&key, msg.client_id);
                }

                Request::Write { key, value } => {
                    let key_clone = key.clone();
                    let id = self.id.clone();
                    let store_clone = self.store.clone();
                    let lease_table = Arc::new(self.leases.clone());
                    let outgoing = Arc::new(self.outgoing.clone());
                    tokio::spawn(async {
                        write(key_clone, value, lease_table, id, outgoing, store_clone).await;
                    });
                    println!("spawned writer task for key: {key:?}")
                }

                Request::InvalidateAck { key } => {
                    self.on_invalidate_ack(&key, msg.client_id);
                }

                Request::ReleaseLease { key } => {
                    self.release_lease(&key, msg.client_id);
                }
            };
        }
    }

    async fn read(&mut self, key: &Key, client_id: ClientId) {
        let payload = match self.leases.grant(key.clone(), client_id.clone()) {
            Ok(lease) => match self.store.get(key) {
                Ok(value_opt) => match value_opt {
                    Some(value) => ServerMessagePayload::Reply(Response::ReadOk {
                        value: value,
                        lease,
                    }),
                    None => ServerMessagePayload::Reply(Response::Error(AppError::ReadErr {
                        for_key: key.clone(),
                        error: String::from("no value found for key {key}"),
                    })),
                },

                Err(e) => ServerMessagePayload::Reply(Response::Error(AppError::ReadErr {
                    for_key: key.clone(),
                    error: String::from("Internal Server Error"),
                })),
            },

            Err(e) => ServerMessagePayload::Reply(Response::Error(AppError::ReadErr {
                for_key: key.clone(),
                error: String::from(e.to_string()),
            })),
        };

        let server_msg = ServerMessage {
            server_id: self.id.clone(),
            payload: payload,
        };

        let outgoing = OutgoingFromServer {
            to: OutgoingReciever::Client(client_id),
            msg: Message::Server(server_msg),
        };

        self.outgoing.send(outgoing).await.unwrap();
    }

    fn on_invalidate_ack(&self, key: &Key, client_id: ClientId) {
        self.release_lease(key, client_id);
    }

    fn release_lease(&self, key: &Key, client_id: ClientId) {
        match self.leases.release(key, client_id.clone()) {
            Ok(_) => {
                println!("released lease for key {key:?} for client {client_id:?}");
            }
            Err(e) => {
                eprintln!("failed to release lease {e}");
                return;
            }
        }
    }
}

async fn write(
    key: Key,
    value: Value,
    lease_table: Arc<LeaseTable>,
    id: ServerId,
    outgoing_sender: Arc<tokio::sync::mpsc::Sender<OutgoingFromServer>>,
    store: Arc<dyn Store>,
) {
    let active_leases = match lease_table.active_leases(&key) {
        Ok(leases) => leases,
        Err(e) => {
            eprintln!("{e}");
            return;
        }
    };

    for lease in &active_leases {
        let server_msg = ServerMessage {
            server_id: id.clone(),
            payload: ServerMessagePayload::Push(ServerPush::Invalidate {
                key: lease.key.clone(),
            }),
        };

        let outgoing = OutgoingFromServer {
            to: OutgoingReciever::Client(lease.client_id.clone()),
            msg: Message::Server(server_msg),
        };

        outgoing_sender.send(outgoing).await.unwrap();
    }

    wait_for_acks_or_expiry(&key, &active_leases, lease_table).await;

    let Ok(_) = store.set(key.clone(), value) else {
        eprintln!("failed to set {key:?} lock poisoned");
        return;
    };
}

async fn wait_for_acks_or_expiry(
    key: &Key,
    active_leases: &Vec<Lease>,
    lease_table: Arc<LeaseTable>,
) {
    let mut max_expiry_duration = 0;

    for lease in active_leases {
        max_expiry_duration = cmp::max(lease.expires_at, max_expiry_duration);
    }

    loop {
        tokio::select! {
            //check periodically each 150 milli seconds if all leased clients sent ack
            _ = set_timer(150) => {
                let active = lease_table.active_leases(key).unwrap();
                if active.len() == 0 {
                    break;
                }
            },

            //or wait for all leases to be expired
            _ = set_timer(max_expiry_duration) => {
                break
            },
        }
    }
}

async fn set_timer(millis: u64) {
    tokio::time::sleep(Duration::from_millis(millis)).await;
}
