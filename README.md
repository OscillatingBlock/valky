# valky

A personal learning project: a tiny distributed kv store with leases protocol for cache consistency. Clients cache reads with time-bound leases; a write invalidates every lease holder before it applies.

## Run it

```bash
cargo run -- server   # reads ./config.toml (mode = "server")
cargo run -- client   # override with a positional arg
```

All settings live in `config.toml` (`--config=PATH` overrides the file,
`VALKY_*` env vars override file keys). The client REPL speaks
`get <k> | put <k> <v> | quit`.

Or watch the whole protocol in one process (asserted at every step):

```bash
cargo run --example lease_demo
# fetch -> hit -> invalidate -> refetch -> expiry -> miss
```

## How it works

- `store.rs` — in-memory `HashMap` KV store.
- `lease.rs` — `LeaseTable` grants per-client leases; the server treats a
  lease as dead only past `expires_at + skew_bound_ms`.
- `server.rs` — reads grant leases; writes invalidate all holders, wait for
  acks (or expiry), then apply.
- `client.rs` — `ClientCache` serves reads from cache while
  `now + skew < expires_at`, refetches on miss, and auto-acks invalidates.
- `net.rs` / `protocol.rs` — TCP transport, 4-byte length-prefix + JSON
  framing, handshake, and message routing.
- `src/tests.md` — the test plan; `config.toml` — node configuration.

## Tests

`cargo test` (54 tests: `lease`, `server`, `client`, `net`, `protocol`).

The `client`, `server`, and `net` test suites were written by
Muse Spark, which also fixed the inverted cache-expiry check it found in `client.rs`
(`get_cached_entry` trusted the cache exactly when the lease was expiring).

## Known limitations

Single in-memory server, no persistence/replication/auth; writes are
fire-and-forget; no per-key write serialization — a tight read-retry loop on
a hot key can livelock a pending write (readers must back off).
