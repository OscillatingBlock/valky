//! Lease-based read-cache coherence, end to end in one process.
//!
//! Run with:  cargo run --example lease_demo
//!
//! This wires a `Server` to two `ClientCache`s with small in-process glue
//! tasks that play the role of `net.rs` (forwarding messages between the two
//! sides). It then walks through the whole protocol:
//!
//! 1. Client A reads a missing-from-cache key: fetch from the server, which
//!    grants A a lease. A second read is a pure cache hit (the glue counts
//!    `Read`s reaching the server, so the hit is observable, not assumed).
//! 2. Client B writes the same key: the server invalidates A's copy, A acks
//!    automatically, the write applies, and A's next read refetches.
//! 3. After the lease expires, even an un-invalidated copy is refetched.
//!
//! The `assert!`s are the point: each one pins a protocol guarantee.

use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use tokio::sync::mpsc;

use valky::client::{ClientCache, ClientId, start_client};
use valky::clock::SystemClock;
use valky::lease::LeaseTable;
use valky::protocol::{
    ClientMessage, Message, OutgoingFromServer, OutgoingReciever, Request, ServerMessage,
};
use valky::server::{Server, ServerId};
use valky::store::{Key, ServerStore, Store, Value};

fn key(s: &str) -> Key {
    Key::from_string(s.to_string())
}

fn val(s: &str) -> Value {
    Value::from_bytes(Bytes::copy_from_slice(s.as_bytes()))
}

fn text(v: &Value) -> String {
    String::from_utf8_lossy(v.as_bytes()).into_owned()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = Arc::new(ServerStore::default());
    store.set(key("greeting"), val("hello"))?;

    // Short lease so step 3 doesn't keep you waiting.
    let leases = LeaseTable::new(Arc::new(SystemClock), 100, 800);

    // --- Server side -------------------------------------------------------
    let (to_srv_tx, to_srv_rx) = mpsc::channel::<ClientMessage>(32);
    let (from_srv_tx, mut from_srv_rx) = mpsc::channel::<OutgoingFromServer>(32);
    let mut server = Server::new(store, leases, to_srv_rx, from_srv_tx, ServerId::new(1));
    tokio::spawn(async move { server.run().await });

    // How many `Read`s actually reached the server. The only honest way to
    // tell a cache hit from a fetch in an example.
    let reads_seen = Arc::new(AtomicUsize::new(0));

    // --- Two clients -------------------------------------------------------
    let mut caches: HashMap<ClientId, Arc<ClientCache>> = HashMap::new();
    let mut inboxes: HashMap<ClientId, mpsc::Sender<ServerMessage>> = HashMap::new();
    for id in [7u64, 8] {
        let (c2s_tx, mut c2s_rx) = mpsc::channel::<Message>(32);
        let (s2c_tx, s2c_rx) = mpsc::channel::<ServerMessage>(32);
        let cache = Arc::new(ClientCache::new(
            Arc::new(SystemClock),
            ClientId::new(id),
            100,
            c2s_tx,
            s2c_rx,
        ));
        let _ = start_client(Arc::clone(&cache));

        // Client -> server glue: unwrap the transport envelope.
        let to_srv = to_srv_tx.clone();
        let counter = Arc::clone(&reads_seen);
        tokio::spawn(async move {
            while let Some(msg) = c2s_rx.recv().await {
                if let Message::Client(cm) = msg {
                    if matches!(cm.request, Request::Read { .. }) {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                    if to_srv.send(cm).await.is_err() {
                        break;
                    }
                }
            }
        });

        caches.insert(ClientId::new(id), cache);
        inboxes.insert(ClientId::new(id), s2c_tx);
    }

    // Server -> client glue: route to the lease holder the server named.
    // (Nothing in the server sends `Broadcast` today, so only unicast is
    // handled — `ServerMessage` isn't `Clone`.)
    tokio::spawn(async move {
        while let Some(out) = from_srv_rx.recv().await {
            let Message::Server(sm) = out.msg else {
                continue;
            };
            if let OutgoingReciever::Client(id) = &out.to {
                if let Some(tx) = inboxes.get(id) {
                    let _ = tx.send(sm).await;
                }
            }
        }
    });

    let a = &caches[&ClientId::new(7)];
    let b = &caches[&ClientId::new(8)];
    let k = key("greeting");

    println!("1. A reads `greeting`: miss -> fetch + lease");
    let v = a.get(&k).await?.context("expected the seeded value")?;
    assert_eq!(text(&v), "hello");
    assert_eq!(reads_seen.load(Ordering::SeqCst), 1);
    println!("   A got {v:?} (server saw 1 read)");

    println!("2. A reads again: cache hit, server sees nothing");
    let v = a.get(&k).await?.context("expected a cached value")?;
    assert_eq!(text(&v), "hello");
    assert_eq!(reads_seen.load(Ordering::SeqCst), 1);
    println!("   A got {v:?} (server still saw 1 read)");

    println!("3. B writes `greeting = world`: A is invalidated, then refetches");
    b.write(k.clone(), val("world")).await?;
    // `write` is fire-and-forget: the server applies it once A's ack lands
    // (usually a few hundred ms: one 150ms ack-poll after the ack).
    //
    // Deliberately a sleep, not a poll loop: every cache MISS sends a `Read`
    // and every `Read` grants a NEW lease, while the writer waits for
    // quiescence (zero active leases). A tight read-retry loop would
    // re-grant forever and livelock the writer — a known limitation of this
    // server (no per-key write serialization yet). Real readers back off;
    // so do we.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let v = a.get(&k).await?.context("expected the new value")?;
    assert_eq!(text(&v), "world");
    assert_eq!(reads_seen.load(Ordering::SeqCst), 2);
    println!("   A got \"world\" after 1 refetch (server saw 2 reads)");

    println!("4. Lease expires (800ms): A refetches even without invalidate");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let v = a.get(&k).await?.context("expected a value")?;
    assert_eq!(text(&v), "world");
    assert_eq!(reads_seen.load(Ordering::SeqCst), 3);
    println!("   A got {v:?} (server saw 3 reads)");

    println!("5. Missing keys resolve to None (server Error -> Ok(None))");
    assert!(a.get(&key("nope")).await?.is_none());
    println!("   A got None for `nope`");

    println!("\ndone: fetch -> hit -> invalidate -> refetch -> expiry -> miss");
    Ok(())
}
