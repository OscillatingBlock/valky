use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc};

use crate::{
    clock::Clock,
    lease::Lease,
    protocol::{
        AppError, ClientMessage, Message, Request, Response, ServerMessage, ServerMessagePayload,
        ServerPush,
    },
    store::{Key, Value},
};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Eq, Hash, Ord, PartialOrd)]
pub struct ClientId(u64);

impl ClientId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

/// State shared between the background dispatcher (`run`) and concurrent
/// `get` / `write` calls. Everything is behind short-lived locks, so the
/// public API takes `&self` and the cache can live behind an `Arc`.
struct ClientShared {
    entries: Mutex<HashMap<Key, (Value, Lease)>>,
    //list of all waiting threads for a key
    waiters: Mutex<HashMap<Key, Vec<mpsc::Sender<()>>>>,
    clock: Arc<dyn Clock>,
    client_id: ClientId,
    skew_bound_ms: u64,
    //client cache sends msgs to (or recieves from) these channels
    //thinking its sending them to server,
    //behind the scenes networking module handles
    //sending and recieving msgs from / to server
    to_server: mpsc::Sender<Message>,
}

pub struct ClientCache {
    shared: Arc<ClientShared>,
    from_server: Mutex<mpsc::Receiver<ServerMessage>>,
}

impl ClientCache {
    pub fn new(
        clock: Arc<dyn Clock>,
        client_id: ClientId,
        skew_bound_ms: u64,
        to_server: tokio::sync::mpsc::Sender<Message>,
        from_server: tokio::sync::mpsc::Receiver<ServerMessage>,
    ) -> Self {
        Self {
            shared: Arc::new(ClientShared {
                entries: tokio::sync::Mutex::new(HashMap::new()),
                waiters: tokio::sync::Mutex::new(HashMap::new()),
                clock,
                client_id,
                skew_bound_ms,
                to_server,
            }),
            from_server: tokio::sync::Mutex::new(from_server),
        }
    }

    /// Background task: route every server message into the cache and wake
    /// parked `get` calls. Run it as
    /// `tokio::spawn({ let c = cache.clone(); async move { c.run().await } })`
    /// on an `Arc<ClientCache>` while `get` / `write` use the same handle.
    pub async fn run(&self) {
        let mut rx = self.from_server.lock().await;
        while let Some(msg) = rx.recv().await {
            match msg.payload {
                ServerMessagePayload::Reply(server_response) => match server_response {
                    Response::ReadOk { value, lease } => {
                        let key = lease.key.clone();
                        self.cache_entry(value, lease).await;
                        self.wake_up_waiters(&key).await;
                    }

                    Response::WriteOk => {}

                    Response::Error(app_error) => match app_error {
                        AppError::ReadErr { error: _, for_key } => {
                            //do not cache anything, just wake up waiters
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

    pub async fn get(&self, key: &Key) -> anyhow::Result<Option<Value>> {
        match self.get_cached_entry(key).await {
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
                return Ok(self.get_cached_entry(key).await);
            }
        };
    }

    async fn get_cached_entry(&self, key: &Key) -> Option<Value> {
        let entries = self.shared.entries.lock().await;
        match entries.get(key) {
            Some((val, lease)) => {
                // Trust the cache only while we are safely inside the lease:
                // stop `skew_bound_ms` early, since the server may consider
                // the lease expired before we do if our clocks disagree.
                // (Written as `now + skew < expires_at` to avoid underflow
                // when `expires_at < skew_bound_ms`.)
                if self.shared.clock.now() + self.shared.skew_bound_ms < lease.expires_at {
                    return Some(val.clone());
                }
                None
            }
            None => None,
        }
    }

    async fn wait_while_server_sends_value(&self, key: &Key) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
        self.shared
            .waiters
            .lock()
            .await
            .entry(key.clone())
            .or_default()
            .push(tx);
        rx.recv().await;
    }

    async fn request_value_from_server(&self, key: &Key) -> anyhow::Result<()> {
        let request = Request::Read { key: key.clone() };
        let msg = Message::Client(ClientMessage {
            client_id: self.shared.client_id.clone(),
            request: request,
        });

        self.shared.to_server.send(msg).await?;
        Ok(())
    }

    async fn on_invalidate(&self, key: &Key) {
        self.shared.entries.lock().await.remove(key);
        let ack = Message::Client(ClientMessage {
            client_id: self.shared.client_id.clone(),
            request: Request::InvalidateAck { key: key.clone() },
        });
        if let Err(e) = self.shared.to_server.send(ack).await {
            eprintln!("failed to send ack to server: {e} ");
        }
    }

    async fn cache_entry(&self, value: Value, lease: Lease) {
        self.shared
            .entries
            .lock()
            .await
            .insert(lease.key.clone(), (value, lease));
    }

    async fn wake_up_waiters(&self, key: &Key) {
        // Drain: every parked waiter is notified exactly once, and the map
        // does not grow with dead senders over time.
        let waiters = self.shared.waiters.lock().await.remove(key);
        if let Some(waiters_list) = waiters {
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
            client_id: self.shared.client_id.clone(),
            request: Request::Write { key, value },
        });
        self.shared
            .to_server
            .send(msg)
            .await
            .context("failed to send write request to server")?;
        Ok(())
    }
}

/// Spawn the background dispatcher for an `Arc`-shared cache. The same
/// `Arc<ClientCache>` keeps serving `get` / `write` while this runs.
pub fn start_client(client: Arc<ClientCache>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        client.run().await;
    })
}

/// Tests for `ClientCache` with a fake server (mpsc channels) and a
/// `FakeClock`.
///
/// Test mechanics: the cache is shared via `Arc` between the test and a
/// `pump` that runs the background dispatcher with a timeout. Queued fake
/// server replies are processed by `pump`; a `get` that must fetch is
/// asserted via a short timeout (still pending) plus the `Read` arriving on
/// the fake server side. Parked `get`s are also tested end to end by
/// spawning them and joining after `pump` delivers the reply.
///
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{clock::FakeClock, server::ServerId};
    use bytes::Bytes;
    use std::time::Duration;

    const SKEW_MS: u64 = 100;
    const START_MS: u64 = 1_000_000;

    struct Harness {
        client: Arc<ClientCache>,
        clock: Arc<FakeClock>,
        /// Test reads what the client sends to the (fake) server here.
        to_server: tokio::sync::mpsc::Receiver<Message>,
        /// Test injects fake server replies here.
        from_server: tokio::sync::mpsc::Sender<ServerMessage>,
    }

    impl Harness {
        fn new() -> Self {
            let (to_tx, to_rx) = tokio::sync::mpsc::channel(32);
            let (from_tx, from_rx) = tokio::sync::mpsc::channel(32);
            let clock = Arc::new(FakeClock::new(START_MS));
            let client = Arc::new(ClientCache::new(
                clock.clone(),
                ClientId::new(7),
                SKEW_MS,
                to_tx,
                from_rx,
            ));
            Self {
                client,
                clock,
                to_server: to_rx,
                from_server: from_tx,
            }
        }

        fn key(s: &str) -> Key {
            Key::from_string(s.to_string())
        }

        fn val(s: &str) -> Value {
            Value::from_bytes(Bytes::copy_from_slice(s.as_bytes()))
        }

        /// Lease valid for `duration_ms` from the current fake time.
        fn lease(&self, key: &Key, duration_ms: u64) -> Lease {
            Lease {
                key: key.clone(),
                client_id: ClientId::new(7),
                expires_at: self.clock.now() + duration_ms,
            }
        }

        async fn server_reply(&self, payload: ServerMessagePayload) {
            self.from_server
                .send(ServerMessage {
                    server_id: ServerId::new(1),
                    payload,
                })
                .await
                .unwrap();
        }

        async fn read_ok(&self, value: Value, lease: Lease) {
            self.server_reply(ServerMessagePayload::Reply(Response::ReadOk {
                value,
                lease,
            }))
            .await;
        }

        /// Let the dispatcher process all queued server messages. It never
        /// returns on its own, so bound it with a timeout. Safe to call
        /// repeatedly: each call re-locks the inbound channel.
        async fn pump(&self) {
            let _ = tokio::time::timeout(Duration::from_millis(200), self.client.run()).await;
        }

        /// Wait until a spawned `get` has parked itself on `key` (so a reply
        /// delivered afterwards is guaranteed to wake it, not race it).
        async fn wait_until_parked(&self, key: &Key) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if self.client.shared.waiters.lock().await.contains_key(key) {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("get never parked on key");
        }

        /// Assert the client sent a `Read` for `key` (i.e. it fetched).
        async fn expect_read_for(&mut self, key: &Key) {
            let msg = tokio::time::timeout(Duration::from_secs(2), self.to_server.recv())
                .await
                .expect("client sent nothing")
                .expect("channel closed");
            match msg {
                Message::Client(cm) => match cm.request {
                    Request::Read { key: got } => assert_eq!(&got, key),
                    _ => panic!("expected Read request"),
                },
                _ => panic!("expected Client message"),
            }
        }

        /// Assert the client sent nothing (i.e. it served from cache).
        fn expect_no_fetch(&mut self) {
            assert!(
                self.to_server.try_recv().is_err(),
                "expected cache hit, but client sent a request"
            );
        }

        /// `get` resolving immediately (cache hit). Panics on miss/timeout.
        async fn expect_hit(&mut self, key: &Key) -> Value {
            tokio::time::timeout(Duration::from_secs(2), self.client.get(key))
                .await
                .expect("cached get should resolve immediately")
                .expect("get failed")
                .expect("expected Some value")
        }

        /// `get` staying parked (cache miss). The future is dropped, leaving
        /// its waiter registration behind — harmless within a test.
        async fn expect_miss(&mut self, key: &Key) {
            let res = tokio::time::timeout(Duration::from_millis(100), self.client.get(key)).await;
            assert!(res.is_err(), "expected get to park waiting for server");
        }
    }

    #[tokio::test]
    async fn test_get_on_empty_cache_triggers_fetch() {
        let mut h = Harness::new();
        let k = Harness::key("a");

        h.expect_miss(&k).await;
        h.expect_read_for(&k).await;
    }

    #[tokio::test]
    async fn test_get_on_cached_unexpired_key_does_not_trigger_fetch() {
        let mut h = Harness::new();
        let k = Harness::key("a");
        h.read_ok(Harness::val("v1"), h.lease(&k, 10_000)).await;
        h.pump().await;

        let got = h.expect_hit(&k).await;
        assert_eq!(&got.as_bytes()[..], b"v1");
        h.expect_no_fetch();
    }

    #[tokio::test]
    async fn test_get_on_cached_expired_key_triggers_refetch() {
        let mut h = Harness::new();
        let k = Harness::key("a");
        h.read_ok(Harness::val("v1"), h.lease(&k, 500)).await;
        h.pump().await;

        h.clock.advance(10_000);
        h.expect_miss(&k).await;
        h.expect_read_for(&k).await;
    }

    #[tokio::test]
    async fn test_get_respects_skew_bound_stops_trusting_cache_early() {
        let mut h = Harness::new();
        let k = Harness::key("a");
        // Lease expires at START + 1000; skew cuts trust off at START + 900.
        h.read_ok(Harness::val("v1"), h.lease(&k, 1_000)).await;
        h.pump().await;

        // At START + 899 (now + skew = START + 999 < expiry): still a hit,
        // even though only 101ms of lease remain.
        h.clock.advance(899);
        let got = h.expect_hit(&k).await;
        assert_eq!(&got.as_bytes()[..], b"v1");
        h.expect_no_fetch();

        // At START + 901 the raw lease is still alive, but now + skew
        // (START + 1001) is past expiry: must refetch.
        h.clock.advance(2);
        h.expect_miss(&k).await;
        h.expect_read_for(&k).await;
    }

    #[tokio::test]
    async fn test_cache_stores_independent_entries_per_key() {
        let mut h = Harness::new();
        let k1 = Harness::key("k1");
        let k2 = Harness::key("k2");
        h.read_ok(Harness::val("v1"), h.lease(&k1, 10_000)).await;
        h.read_ok(Harness::val("v2"), h.lease(&k2, 10_000)).await;
        h.pump().await;

        let got1 = h.expect_hit(&k1).await;
        let got2 = h.expect_hit(&k2).await;
        assert_eq!(&got1.as_bytes()[..], b"v1");
        assert_eq!(&got2.as_bytes()[..], b"v2");
        h.expect_no_fetch();
    }

    #[tokio::test]
    async fn test_get_after_successful_refetch_updates_cached_value_and_lease() {
        let mut h = Harness::new();
        let k = Harness::key("a");
        h.read_ok(Harness::val("v1"), h.lease(&k, 500)).await;
        h.pump().await;
        let got = h.expect_hit(&k).await;
        assert_eq!(&got.as_bytes()[..], b"v1");

        h.clock.advance(600);
        h.read_ok(Harness::val("v2"), h.lease(&k, 5_000)).await;
        h.pump().await;

        let got = h.expect_hit(&k).await;
        assert_eq!(&got.as_bytes()[..], b"v2");
        h.expect_no_fetch();
        // Lease was refreshed too, not just the value.
        let expires_at = h
            .client
            .shared
            .entries
            .lock()
            .await
            .get(&k)
            .expect("entry missing")
            .1
            .expires_at;
        assert_eq!(expires_at, h.clock.now() + 5_000);
    }

    #[tokio::test]
    async fn test_on_invalidate_removes_entry_sends_ack_and_forces_refetch() {
        let mut h = Harness::new();
        let k = Harness::key("a");
        // Long lease: still "technically unexpired" when invalidated.
        h.read_ok(Harness::val("v1"), h.lease(&k, 10_000)).await;
        h.pump().await;
        let got = h.expect_hit(&k).await;
        assert_eq!(&got.as_bytes()[..], b"v1");

        h.server_reply(ServerMessagePayload::Push(ServerPush::Invalidate {
            key: k.clone(),
        }))
        .await;
        h.pump().await;

        // Client must have acked the invalidate...
        let msg = tokio::time::timeout(Duration::from_secs(2), h.to_server.recv())
            .await
            .expect("expected InvalidateAck")
            .expect("channel closed");
        match msg {
            Message::Client(cm) => {
                assert_eq!(cm.client_id, ClientId::new(7));
                match cm.request {
                    Request::InvalidateAck { key: got } => assert_eq!(got, k),
                    _ => panic!("expected InvalidateAck request"),
                }
            }
            _ => panic!("expected Client message"),
        }

        // ...and the next get must refetch despite the unexpired lease.
        h.expect_miss(&k).await;
        h.expect_read_for(&k).await;
    }

    #[tokio::test]
    async fn test_on_invalidate_for_unknown_key_is_noop_no_panic() {
        let mut h = Harness::new();
        let k1 = Harness::key("k1");
        h.read_ok(Harness::val("v1"), h.lease(&k1, 10_000)).await;
        h.pump().await;

        h.client.on_invalidate(&Harness::key("nope")).await;

        // Other entries untouched.
        let got = h.expect_hit(&k1).await;
        assert_eq!(&got.as_bytes()[..], b"v1");
    }

    #[tokio::test]
    async fn test_write_sends_write_request_to_server() {
        let mut h = Harness::new();
        let k = Harness::key("w");

        h.client
            .write(k.clone(), Harness::val("n"))
            .await
            .expect("write failed");

        let msg = tokio::time::timeout(Duration::from_secs(2), h.to_server.recv())
            .await
            .expect("expected Write request")
            .expect("channel closed");
        match msg {
            Message::Client(cm) => {
                assert_eq!(cm.client_id, ClientId::new(7));
                match cm.request {
                    Request::Write { key, value } => {
                        assert_eq!(key, k);
                        assert_eq!(&value.as_bytes()[..], b"n");
                    }
                    _ => panic!("expected Write request"),
                }
            }
            _ => panic!("expected Client message"),
        }
    }

    #[tokio::test]
    async fn test_write_ok_response_does_not_corrupt_cache() {
        let mut h = Harness::new();
        let k = Harness::key("a");
        h.read_ok(Harness::val("v1"), h.lease(&k, 10_000)).await;
        h.pump().await;

        // `write` is fire-and-forget; the WriteOk reply wakes nothing and
        // must leave the cached entry alone.
        h.client
            .write(k.clone(), Harness::val("v2"))
            .await
            .expect("write failed");
        h.server_reply(ServerMessagePayload::Reply(Response::WriteOk))
            .await;
        h.pump().await;

        let got = h.expect_hit(&k).await;
        assert_eq!(&got.as_bytes()[..], b"v1");
    }

    #[tokio::test]
    async fn test_parked_get_resolves_when_read_ok_arrives() {
        let mut h = Harness::new();
        let k = Harness::key("a");

        // A real parked `get`: it fetches, then waits for the reply while
        // the dispatcher runs concurrently on the same `Arc` cache.
        let getter = {
            let c = Arc::clone(&h.client);
            let key = k.clone();
            tokio::spawn(async move { c.get(&key).await })
        };
        h.wait_until_parked(&k).await;
        h.expect_read_for(&k).await;

        h.read_ok(Harness::val("v1"), h.lease(&k, 10_000)).await;
        h.pump().await;

        let got = tokio::time::timeout(Duration::from_secs(2), getter)
            .await
            .expect("parked get never resolved")
            .expect("task panicked")
            .expect("get failed")
            .expect("expected Some value");
        assert_eq!(&got.as_bytes()[..], b"v1");
    }

    #[tokio::test]
    async fn test_parked_get_resolves_none_when_read_errors() {
        let mut h = Harness::new();
        let k = Harness::key("ghost");

        // Contract note: unlike tests.md's "propagates as Err", the
        // implementation resolves a failed fetch as `Ok(None)` — the
        // dispatcher wakes waiters but caches nothing, so `get`'s second
        // lookup misses and returns `None`. This test pins that behavior.
        let getter = {
            let c = Arc::clone(&h.client);
            let key = k.clone();
            tokio::spawn(async move { c.get(&key).await })
        };
        h.wait_until_parked(&k).await;
        h.expect_read_for(&k).await;

        h.server_reply(ServerMessagePayload::Reply(Response::Error(
            AppError::ReadErr {
                for_key: k.clone(),
                error: "no value".to_string(),
            },
        )))
        .await;
        h.pump().await;

        let got = tokio::time::timeout(Duration::from_secs(2), getter)
            .await
            .expect("parked get never resolved")
            .expect("task panicked")
            .expect("get failed");
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn test_response_with_no_waiters_is_cached_without_panic() {
        let mut h = Harness::new();
        let k = Harness::key("a");

        // Reply arrives with nothing parked on the key: wake is a no-op,
        // value is still cached.
        h.read_ok(Harness::val("v1"), h.lease(&k, 10_000)).await;
        h.pump().await;

        let got = h.expect_hit(&k).await;
        assert_eq!(&got.as_bytes()[..], b"v1");
    }
}
