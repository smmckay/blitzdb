## TODO for 0.1

### Bug Fixes
- [ ] Extract actual errno from `fi_cq_err_entry.err` instead of hardcoding -1 (`blitzdb-common/src/driver.rs:87,128`)

### libfabric
- [ ] support verbs and efa providers
- [ ] Add configurable timeout on RDMA reads (client can hang indefinitely today)
- [ ] receive buffer pooling
- [ ] deal with virtual address mode

### Testing
- [ ] Negative test cases: missing keys, server crash, read timeout

### Performance
- [ ] Performance benchmarks (latency, throughput) using EFA-supporting EC2 instances
- [ ] Demonstrate repeatable low latency and high throughput

## TODO for 1.0

### Server
- [ ] Server startup: return errors instead of panicking on missing `.mph` / `.index` / `.heap` files
- [ ] Validate file sizes and integrity at load time

### Observability
- [ ] Expose metrics: request latency, error rates, CQ completions (Prometheus endpoint or similar)
- [ ] Structured logging (replace ad-hoc `log!` calls with consistent fields)

### Configuration
- [ ] Config file (TOML or YAML) for: gossip port/seeds, dataset paths, timeouts
- [ ] Remove hardcoded ports and paths from binaries

### Cluster & Sharding
- [ ] Client load balancing and failover across multiple servers discovered via chitchat
- [ ] `(dataset, shard, version)` keyspace — multi-dataset and multi-shard support

## Future

### Data & Ingestion
- [ ] Atomic dataset switchover / versioning (hot-reload without downtime)
- [ ] Streaming or incremental ingestion (beyond full-batch Parquet rebuild)

### Documentation
- [ ] Deployment guide (EFA / InfiniBand setup, dataset prep workflow, cluster bring-up)
