use std::{collections::HashMap, sync::Arc, vec};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::{
    clock::Clock,
    lease::Lease,
    protocol::{
        AppError, ClientMessage, Message, Request, Response, ServerMessage, ServerMessagePayload,
        ServerPush,
    },
    store::{Key, Value},
};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct ClientId(u64);

pub struct ClientCache {
    entries: HashMap<Key, (Value, Lease)>,
    clock: Arc<dyn Clock>,
    client_id: ClientId,
    skew_bound_ms: u64,
    msg_sender: tokio::sync::mpsc::Sender<Message>,
    msg_reciever: tokio::sync::mpsc::Receiver<ServerMessage>,

    //list of all waiting threads for a key
    waiters_map: HashMap<Key, vec::Vec<tokio::sync::mpsc::Sender<()>>>,
}

impl ClientCache {
    pub fn new(
        clock: Arc<dyn Clock>,
        client_id: ClientId,
        skew_bound_ms: u64,
        msg_sender: tokio::sync::mpsc::Sender<Message>,
        msg_reciever: tokio::sync::mpsc::Receiver<ServerMessage>,
        waiters_map: HashMap<Key, vec::Vec<tokio::sync::mpsc::Sender<()>>>,
    ) -> Self {
        Self {
            entries: HashMap::new(),
            clock,
            client_id,
            skew_bound_ms,
            msg_sender,
            msg_reciever,
            waiters_map,
        }
    }

    async fn dispatch_server_msgs(&mut self) {
        while let Some(msg) = self.msg_reciever.recv().await {
            match msg.payload {
                ServerMessagePayload::Reply(server_response) => match server_response {
                    Response::ReadOk { value, lease } => {
                        let key = lease.key.clone();
                        self.cache_entry(value, lease);
                        self.wake_up_waiters(&key).await;
                    }

                    Response::WriteOk => {}

                    Response::Error(app_error) => match app_error {
                        AppError::ReadErr { error, for_key } => {
                            self.wake_up_waiters(&for_key).await;
                        }
                        AppError::Other(e) => {
                            eprintln!("Error from server: {e}");
                        }
                    },
                },

                ServerMessagePayload::Push(server_push) => match server_push {
                    ServerPush::Invalidate { key } => {
                        self.on_invalidate(&key).await;
                    }
                },
            }
        }
    }

    pub async fn get(&mut self, key: &Key) -> anyhow::Result<Option<Value>> {
        match self.get_cached_entry(key) {
            //if value present in cache return from it
            Some(val) => return Ok(Some(val)),

            None => {
                //if value not present in cache , request it from server
                self.request_value_from_server(key)
                    .await
                    .context("failed to request value from server")?;

                //sleep while server sends value
                self.wait_while_server_sends_value(key).await;

                //after wait if server sent a valid value we return it
                //else if server sent an error cause valid does not exists for this key,
                //then we return None
                return Ok(self.get_cached_entry(key));
            }
        };
    }

    fn get_cached_entry(&self, key: &Key) -> Option<Value> {
        match self.entries.get(key) {
            Some((val, lease)) => {
                if self.clock.now() > lease.expires_at - self.skew_bound_ms {
                    return Some(val.clone());
                }
                None
            }
            None => None,
        }
    }

    async fn wait_while_server_sends_value(&mut self, key: &Key) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
        self.waiters_map
            .entry(key.clone())
            .or_insert(vec![])
            .push(tx);
        rx.recv().await;
    }

    async fn request_value_from_server(&self, key: &Key) -> anyhow::Result<()> {
        let request = Request::Read { key: key.clone() };
        let msg = Message::Client(ClientMessage {
            client_id: self.client_id.clone(),
            request: request,
        });

        self.msg_sender.send(msg).await?;
        Ok(())
    }

    async fn on_invalidate(&mut self, key: &Key) {
        self.entries.remove(key);
        let ack = Message::Client(ClientMessage {
            client_id: self.client_id.clone(),
            request: Request::InvalidateAck { key: key.clone() },
        });
        if let Err(e) = self.msg_sender.send(ack).await {
            eprintln!("failed to send ack to server: {e} ");
        }
    }

    fn cache_entry(&mut self, value: Value, lease: Lease) {
        self.entries.insert(lease.key.clone(), (value, lease));
    }

    async fn wake_up_waiters(&mut self, key: &Key) {
        if let Some(waiters_list) = self.waiters_map.get(key) {
            for tx in waiters_list {
                if let Err(e) = tx.send(()).await {
                    eprintln!("failed to wake up waiter for key {key:?}: {e}");
                    continue;
                };
            }
        }
    }

    pub async fn write(&self, key: Key, value: Value) -> anyhow::Result<()> {
        let msg = Message::Client(ClientMessage {
            client_id: self.client_id.clone(),
            request: Request::Write { key, value },
        });
        self.msg_sender
            .send(msg)
            .await
            .context("failed to send write request to server")?;
        Ok(())
    }
}

pub fn start_client(mut client: ClientCache) {
    tokio::spawn(async move {
        client.dispatch_server_msgs().await;
    });
}
