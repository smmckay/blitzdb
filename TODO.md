## TODO for 1.0

### Bug Fixes
- [ ] Extract actual errno from `fi_cq_err_entry.err` instead of hardcoding -1 (`blitzdb-common/src/driver.rs:87,128`)

### Error Handling & Robustness
- [ ] Server startup: return errors instead of panicking on missing `.mph` / `.index` / `.heap` files
- [ ] Client: handle MPH miss in `try_hash` gracefully instead of unwrapping (`blitzdb-client/src/lib.rs:72`)
- [ ] Add configurable timeout on RDMA reads (client can hang indefinitely today)
- [ ] Validate file sizes and integrity at load time

### Testing
- [ ] Negative test cases: missing keys, server crash, read timeout
- [ ] Unit tests for `IndexEntry`, `Op`, `FabricError`
- [ ] Performance benchmarks (latency, throughput)

### Observability
- [ ] Expose metrics: request latency, error rates, CQ completions (Prometheus endpoint or similar)
- [ ] Structured logging (replace ad-hoc `log!` calls with consistent fields)

### Configuration
- [ ] Config file (TOML or YAML) for: gossip port/seeds, dataset paths, RDMA provider, timeouts
- [ ] Remove hardcoded ports and paths from binaries

### Cluster & Sharding
- [ ] Client failover across multiple servers discovered via chitchat
- [ ] `(dataset, shard, version)` keyspace — multi-dataset and multi-shard support
  - [ ] index sharding
  - [ ] data sharding
- [ ] Shard assignment and discovery via chitchat gossip

### Data & Ingestion
- [ ] Atomic dataset switchover / versioning (hot-reload without downtime)
- [ ] Streaming or incremental ingestion (beyond full-batch Parquet rebuild)

### Client
- [ ] Batch `get_many()` operation to pipeline multiple RDMA reads
- [ ] Optional client-side index entry cache to avoid redundant RDMA reads per lookup

### Security
- [ ] Authentication (API key or mutual TLS)

### Documentation
- [ ] `///` rustdoc on all public types and functions
- [ ] Deployment guide (EFA / InfiniBand setup, dataset prep workflow, cluster bring-up)

## Provider Differences to Abstract Over

libfabric smooths most differences, but a few things vary between EFA and verbs that you need to handle:

| Concern             | AWS EFA                         | InfiniBand/RoCEv2                     |
|---------------------|---------------------------------|---------------------------------------|
| Max message size    | 8KB inline, larger via RDMA     | Varies by HCA                         |
| Memory registration | Explicit, expensive — cache MRs | Same                                  |
| Connection model    | Connectionless (RDM endpoint)   | Connected (RC) or connectionless (UD) |
| Ordering guarantees | Relaxed                         | Stricter on RC                        |
| `fi_read` latency   | ~5–20µs in placement group      | ~1–5µs                                |

The connection model difference is the most significant. EFA favors **RDM (Reliable Datagram)** endpoints while InfiniBand RC (Reliable Connected) is more common for one-sided ops. libfabric abstracts this but you need to request capabilities correctly at `fi_getinfo` time.

## Realistic Architecture for Your KV Store
Server side:
- Register value store memory with fi_mr_reg
- Broadcast MR keys + base addresses to clients
- CPU handles writes only; reads are serverless

Client side:
- fi_read(remote_addr + offset, size, mr_key)
- Poll completion queue
- Done — no server CPU involved

## Gossip with chitchat

chitchat would let you gossip the MR metadata as key-value state automatically disseminated to all nodes — less code to write

### Convergence Time

#### Expected Convergence Time

No published benchmarks exist for chitchat, but here are theory-backed estimates derived from the source
code parameters (fanout=3, default interval=1s):

| Cluster      | Production (1s interval) | Test settings (50ms interval) |
|--------------|--------------------------|-------------------------------|
| 100 nodes    | ~5–7 seconds             | ~250–350 ms                   |
|  1,000 nodes |  ~8–12 seconds           | ~400–600 ms                   |

The math: gossip with fanout 3 propagates a single update in O(log₃ N) rounds — about 4–5 rounds for 100
nodes, 6–7 for 1,000. Multiply by the gossip interval.

Caveats that push toward the upper bound:
- For a joining node, full state sync is slower than single-key propagation — with 1,000 nodes the full
  cluster state digest likely exceeds the 65 KB UDP MTU, so a new node needs multiple rounds just to
  receive all state.
- Peer selection is probabilistic, not guaranteed optimal, so some nodes get reached later than the
  log₃(N) lower bound.

For BlitzDB's MR broadcast use case: the production 5–12 second convergence window is probably fine for
initial cluster bring-up, but if you need faster re-convergence after a server restart (new MR keys),
you'd want to either reduce the gossip interval or treat chitchat as the eventual-consistency fallback
and keep a direct TCP notification path for urgent MR key changes.

#### Reducing Convergence Time

Convergence time scales linearly with the gossip interval, so halving it halves convergence. The costs:

Network traffic

Each node sends 3 messages per interval. Total cluster-wide message rate = N × 3 / interval.

| Interval | 100 nodes   | 1,000 nodes  |
|----------|-------------|--------------|
| 1,000 ms | 300 msg/s   | 3,000 msg/s  |
| 100 ms   | 3,000 msg/s | 30,000 msg/s |
| 50 ms    | 6,000 msg/s | 60,000 msg/s |

Message size is bounded by the delta since last sync. For BlitzDB's use case — a handful of u64 MR keys
and addresses per node — messages will be tiny (tens of bytes), so even 60,000 msg/s is negligible
bandwidth. CPU overhead for deserializing and processing gossip is likely the real ceiling before
network is.

Failure detection speed

The phi-accrual failure detector in chitchat uses heartbeat intervals to detect dead nodes. A shorter
gossip interval means faster failure detection too — useful if you want to quickly stop routing reads to
a node whose MR registrations are stale.

Practical recommendation for BlitzDB

Since your gossiped state is tiny, you can safely drop to 100–200 ms without meaningful cost. That
gives:

- 100 nodes: ~0.5–1 second convergence
- 1,000 nodes: ~1–2 seconds convergence

Going below 100 ms starts hitting CPU overhead for gossip processing more than network limits, and at
that point you're in the territory where a direct TCP push notification (on MR re-registration) is
simpler and more reliable than tuning gossip further.