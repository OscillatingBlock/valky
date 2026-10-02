use serde::{Deserialize, Serialize};

use crate::client::ClientId;
use crate::lease::{Lease, LeaseTable};
use crate::protocol::{
    AppError, ClientMessage, Message, OutgoingFromServer, OutgoingReciever, Request, Response,
    ServerMessage, ServerMessagePayload, ServerPush,
};
use crate::store::{Key, Store, Value};

use std::cmp;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct ServerId(u64);

impl ServerId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

pub struct Server {
    store: Arc<dyn Store>,
    leases: LeaseTable,
    from_client: tokio::sync::mpsc::Receiver<ClientMessage>,
    to_client: tokio::sync::mpsc::Sender<OutgoingFromServer>,
    id: ServerId,
}

impl Server {
    pub fn new(
        store: Arc<dyn Store>,
        leases: LeaseTable,
        from_client: tokio::sync::mpsc::Receiver<ClientMessage>,
        to_client: tokio::sync::mpsc::Sender<OutgoingFromServer>,
        id: ServerId,
    ) -> Self {
        Self {
            store,
            leases,
            from_client,
            to_client,
            id,
        }
    }

    pub async fn run(&mut self) {
        self.handle_client_msgs().await;
    }

    async fn handle_client_msgs(&mut self) {
        while let Some(msg) = self.from_client.recv().await {
            match msg.request {
                Request::Read { key } => {
                    self.read(&key, msg.client_id).await;
                }

                Request::Write { key, value } => {
                    let key_clone = key.clone();
                    let id = self.id.clone();
                    let store_clone = self.store.clone();
                    let lease_table = Arc::new(self.leases.clone());
                    let to_client = Arc::new(self.to_client.clone());
                    tokio::spawn(async {
                        write(key_clone, value, lease_table, id, to_client, store_clone).await;
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
        let value = match self.store.get(key) {
            Err(e) => {
                eprintln!("{e}");
                let payload = ServerMessagePayload::Reply(Response::Error(AppError::ReadErr {
                    for_key: key.clone(),
                    error: String::from("Internal Server Error"),
                }));
                return self.send_payload_to_client(payload, client_id).await;
            }
            Ok(value_opt) => match value_opt {
                Some(value) => value,
                None => {
                    let payload = ServerMessagePayload::Reply(Response::Error(AppError::ReadErr {
                        for_key: key.clone(),
                        error: String::from("no value found for key {key}"),
                    }));
                    return self.send_payload_to_client(payload, client_id).await;
                }
            },
        };

        let payload = match self.leases.grant(key.clone(), client_id.clone()) {
            Ok(lease) => ServerMessagePayload::Reply(Response::ReadOk {
                value: value,
                lease,
            }),
            Err(e) => {
                eprintln!("{e}");
                ServerMessagePayload::Reply(Response::Error(AppError::ReadErr {
                    for_key: key.clone(),
                    error: String::from("Internal Server Error"),
                }))
            }
        };

        return self.send_payload_to_client(payload, client_id).await;
    }

    async fn send_payload_to_client(&self, payload: ServerMessagePayload, client_id: ClientId) {
        let server_msg = ServerMessage {
            server_id: self.id.clone(),
            payload: payload,
        };

        let outgoing = OutgoingFromServer {
            to: OutgoingReciever::Client(client_id),
            msg: Message::Server(server_msg),
        };

        self.to_client.send(outgoing).await.unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::FakeClock;
    use crate::store::ServerStore;
    use bytes::Bytes;
    use std::collections::HashSet;

    const SKEW_MS: u64 = 100;
    const LEASE_MS: u64 = 10_000;

    struct Harness {
        from_client: tokio::sync::mpsc::Sender<ClientMessage>,
        to_client: tokio::sync::mpsc::Receiver<OutgoingFromServer>,
        store: Arc<ServerStore>,
        leases: LeaseTable,
        clock: Arc<FakeClock>,
        _server_task: tokio::task::JoinHandle<()>,
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            self._server_task.abort();
        }
    }

    impl Harness {
        fn new() -> Self {
            Self::with_params(1_000_000, SKEW_MS, LEASE_MS)
        }

        fn with_params(start_ms: u64, skew_bound_ms: u64, lease_duration: u64) -> Self {
            let (from_tx, from_rx) = tokio::sync::mpsc::channel(32);
            let (to_tx, to_rx) = tokio::sync::mpsc::channel(32);
            let clock = Arc::new(FakeClock::new(start_ms));
            let leases = LeaseTable::new(clock.clone(), skew_bound_ms, lease_duration);
            let store = Arc::new(ServerStore::default());
            let server = Server::new(store.clone(), leases.clone(), from_rx, to_tx, ServerId(1));
            let task = tokio::spawn(async move {
                let mut server = server;
                server.run().await;
            });
            Self {
                from_client: from_tx,
                to_client: to_rx,
                store,
                leases,
                clock,
                _server_task: task,
            }
        }

        async fn send(&self, client: u64, request: Request) {
            self.from_client
                .send(ClientMessage {
                    client_id: ClientId::new(client),
                    request,
                })
                .await
                .unwrap();
        }

        async fn recv(&mut self) -> OutgoingFromServer {
            tokio::time::timeout(Duration::from_secs(3), self.to_client.recv())
                .await
                .expect("timed out waiting for server message")
                .expect("server task ended")
        }
    }

    fn key(s: &str) -> Key {
        Key::from_string(s.to_string())
    }

    fn val(s: &str) -> Value {
        Value::from_bytes(Bytes::copy_from_slice(s.as_bytes()))
    }

    fn target_of(out: &OutgoingFromServer) -> ClientId {
        match &out.to {
            OutgoingReciever::Client(id) => id.clone(),
            OutgoingReciever::Broadcast => panic!("expected unicast, got broadcast"),
        }
    }

    fn reply_of(out: OutgoingFromServer) -> (ClientId, Response) {
        let to = target_of(&out);
        match out.msg {
            Message::Server(sm) => match sm.payload {
                ServerMessagePayload::Reply(r) => (to, r),
                ServerMessagePayload::Push(_) => panic!("expected Reply, got Push"),
            },
            _ => panic!("expected Server message"),
        }
    }

    fn push_of(out: OutgoingFromServer) -> (ClientId, Key) {
        let to = target_of(&out);
        match out.msg {
            Message::Server(sm) => match sm.payload {
                ServerMessagePayload::Push(ServerPush::Invalidate { key }) => (to, key),
                ServerMessagePayload::Reply(_) => panic!("expected Push, got Reply"),
            },
            _ => panic!("expected Server message"),
        }
    }

    async fn wait_for_store(h: &Harness, k: &Key, want: &[u8]) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let cur = h.store.get(k).unwrap().map(|v| v.as_bytes().clone());
                if cur.as_deref() == Some(want) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("timed out waiting for store value");
    }

    async fn wait_until_no_leases(h: &Harness, k: &Key) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if h.leases.active_leases(k).unwrap().is_empty() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("leases never drained");
    }

    #[tokio::test]
    async fn test_read_existing_key_returns_value_and_grants_lease() {
        let mut h = Harness::new();
        let k = key("a");
        h.store.set(k.clone(), val("hello")).unwrap();

        h.send(7, Request::Read { key: k.clone() }).await;
        let (to, resp) = reply_of(h.recv().await);
        assert_eq!(to, ClientId::new(7));
        match resp {
            Response::ReadOk { value, lease } => {
                assert_eq!(&value.as_bytes()[..], b"hello");
                assert_eq!(lease.key, k);
                assert_eq!(lease.client_id, ClientId::new(7));
            }
            Response::Error(_) => panic!("expected ReadOk, got Error"),
            Response::WriteOk => panic!("expected ReadOk, got WriteOk"),
        }

        let active = h.leases.active_leases(&k).unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].client_id, ClientId::new(7));
    }

    #[tokio::test]
    async fn test_read_missing_key_returns_error_and_grants_no_lease() {
        let mut h = Harness::new();
        let k = key("ghost");

        h.send(7, Request::Read { key: k.clone() }).await;
        let (to, resp) = reply_of(h.recv().await);
        assert_eq!(to, ClientId::new(7));
        match resp {
            Response::Error(AppError::ReadErr { for_key, .. }) => assert_eq!(for_key, k),
            Response::Error(AppError::Other(_)) => panic!("expected ReadErr, got Other"),
            Response::ReadOk { .. } => panic!("expected Error, got ReadOk"),
            Response::WriteOk => panic!("expected Error, got WriteOk"),
        }

        assert!(h.leases.active_leases(&k).unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_read_grants_independent_lease_per_calling_client() {
        let mut h = Harness::new();
        let k = key("shared");
        h.store.set(k.clone(), val("v")).unwrap();

        h.send(1, Request::Read { key: k.clone() }).await;
        let _ = reply_of(h.recv().await);
        h.send(2, Request::Read { key: k.clone() }).await;
        let _ = reply_of(h.recv().await);

        let active = h.leases.active_leases(&k).unwrap();
        assert_eq!(active.len(), 2);
        let ids: HashSet<_> = active.into_iter().map(|l| l.client_id).collect();
        assert!(ids.contains(&ClientId::new(1)));
        assert!(ids.contains(&ClientId::new(2)));
    }

    #[tokio::test]
    async fn test_write_with_no_active_leases_updates_store_quickly() {
        let mut h = Harness::new();
        let k = key("w");
        h.store.set(k.clone(), val("old")).unwrap();

        h.send(
            99,
            Request::Write {
                key: k.clone(),
                value: val("new"),
            },
        )
        .await;
        wait_for_store(&h, &k, b"new").await;

        // No lease holders, so no invalidates should have been emitted.
        assert!(h.to_client.try_recv().is_err());
        assert!(h.leases.active_leases(&k).unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_write_with_one_active_lease_sends_invalidate_to_that_client() {
        let mut h = Harness::new();
        let k = key("k1");
        h.store.set(k.clone(), val("old")).unwrap();
        h.leases.grant(k.clone(), ClientId::new(11)).unwrap();

        h.send(
            99,
            Request::Write {
                key: k.clone(),
                value: val("new"),
            },
        )
        .await;

        let (to, got_key) = push_of(h.recv().await);
        assert_eq!(to, ClientId::new(11));
        assert_eq!(got_key, k);

        // Ack lets the write finish and clears the table.
        h.send(11, Request::InvalidateAck { key: k.clone() }).await;
        wait_for_store(&h, &k, b"new").await;
        assert!(h.leases.active_leases(&k).unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_write_with_multiple_leases_sends_invalidate_to_all_holders() {
        let mut h = Harness::new();
        let k = key("multi");
        h.store.set(k.clone(), val("old")).unwrap();
        for c in [21, 22, 23] {
            h.leases.grant(k.clone(), ClientId::new(c)).unwrap();
        }

        h.send(
            99,
            Request::Write {
                key: k.clone(),
                value: val("new"),
            },
        )
        .await;

        let mut got = HashSet::new();
        for _ in 0..3 {
            let (to, got_key) = push_of(h.recv().await);
            assert_eq!(got_key, k);
            got.insert(to);
        }
        assert_eq!(
            got,
            HashSet::from([ClientId::new(21), ClientId::new(22), ClientId::new(23)])
        );

        for c in [21, 22, 23] {
            h.send(c, Request::InvalidateAck { key: k.clone() }).await;
        }
        wait_for_store(&h, &k, b"new").await;
        assert!(h.leases.active_leases(&k).unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_write_waits_for_acks_before_completing() {
        let mut h = Harness::new();
        let k = key("blocked");
        h.store.set(k.clone(), val("old")).unwrap();
        h.leases.grant(k.clone(), ClientId::new(31)).unwrap();

        h.send(
            99,
            Request::Write {
                key: k.clone(),
                value: val("new"),
            },
        )
        .await;
        // First invalidate proves the write task snapshotted the lease set.
        let (to, _) = push_of(h.recv().await);
        assert_eq!(to, ClientId::new(31));

        // Longer than the server's 150ms ack-poll interval: still blocked.
        tokio::time::sleep(Duration::from_millis(350)).await;
        let cur = h.store.get(&k).unwrap().unwrap();
        assert_eq!(&cur.as_bytes()[..], b"old");

        h.send(31, Request::InvalidateAck { key: k.clone() }).await;
        wait_for_store(&h, &k, b"new").await;
    }

    #[tokio::test]
    async fn test_write_completes_once_last_ack_received() {
        let mut h = Harness::new();
        let k = key("two-holders");
        h.store.set(k.clone(), val("old")).unwrap();
        h.leases.grant(k.clone(), ClientId::new(41)).unwrap();
        h.leases.grant(k.clone(), ClientId::new(42)).unwrap();

        h.send(
            99,
            Request::Write {
                key: k.clone(),
                value: val("new"),
            },
        )
        .await;
        let _ = push_of(h.recv().await);
        let _ = push_of(h.recv().await);

        // Partial ack must not unblock the write.
        h.send(41, Request::InvalidateAck { key: k.clone() }).await;
        tokio::time::sleep(Duration::from_millis(350)).await;
        let cur = h.store.get(&k).unwrap().unwrap();
        assert_eq!(&cur.as_bytes()[..], b"old");
        let remaining = h.leases.active_leases(&k).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].client_id, ClientId::new(42));

        h.send(42, Request::InvalidateAck { key: k.clone() }).await;
        wait_for_store(&h, &k, b"new").await;
    }

    #[tokio::test]
    async fn test_write_falls_back_to_expiry_if_ack_never_arrives() {
        // Short lease so the test is fast; clock starts far from zero to
        // prove completion tracks fake-clock expiry, not wall-clock time.
        let mut h = Harness::with_params(1_000_000, 100, 300);
        let k = key("expiring");
        h.store.set(k.clone(), val("old")).unwrap();
        h.leases.grant(k.clone(), ClientId::new(51)).unwrap();

        h.send(
            99,
            Request::Write {
                key: k.clone(),
                value: val("new"),
            },
        )
        .await;
        let _ = push_of(h.recv().await);

        // No ack: jump past expires_at + skew_bound_ms.
        h.clock.advance(500);
        wait_for_store(&h, &k, b"new").await;
    }

    #[tokio::test]
    async fn test_release_before_write_means_write_does_not_wait_on_that_client() {
        let mut h = Harness::new();
        let k = key("released");
        h.store.set(k.clone(), val("old")).unwrap();
        h.leases.grant(k.clone(), ClientId::new(61)).unwrap();

        h.send(61, Request::ReleaseLease { key: k.clone() }).await;
        wait_until_no_leases(&h, &k).await;

        h.send(
            99,
            Request::Write {
                key: k.clone(),
                value: val("new"),
            },
        )
        .await;
        wait_for_store(&h, &k, b"new").await;
        // Released holder must not have been invalidated.
        assert!(h.to_client.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_stray_and_duplicate_acks_are_ignored() {
        let mut h = Harness::new();
        let k1 = key("k1");
        let k2 = key("k2");
        h.store.set(k1.clone(), val("v1")).unwrap();
        h.store.set(k2.clone(), val("v2")).unwrap();
        h.leases.grant(k1.clone(), ClientId::new(71)).unwrap();

        // Real ack, then a duplicate of it, an ack from a client that holds
        // nothing, and an ack for a key with no pending write.
        h.send(71, Request::InvalidateAck { key: k1.clone() }).await;
        h.send(71, Request::InvalidateAck { key: k1.clone() }).await;
        h.send(999, Request::InvalidateAck { key: k1.clone() })
            .await;
        h.send(71, Request::InvalidateAck { key: k2.clone() }).await;

        // Server must still be responsive: reads re-grant, writes complete.
        h.send(71, Request::Read { key: k1.clone() }).await;
        let (to, resp) = reply_of(h.recv().await);
        assert_eq!(to, ClientId::new(71));
        assert!(matches!(resp, Response::ReadOk { .. }));

        h.send(
            99,
            Request::Write {
                key: k2.clone(),
                value: val("v2-new"),
            },
        )
        .await;
        wait_for_store(&h, &k2, b"v2-new").await;
    }
}
