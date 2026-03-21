# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Prerequisites

```bash
brew install libfabric llvm
```

## Build & Run

```bash
cargo build
cargo run -p blitzdb-server
```

## Tests

```bash
cargo test                          # all tests
cargo test -p blitzdb-server        # single crate
cargo test test_get_info            # single test by name
```

## Architecture

BlitzDB is a high-performance key-value store designed for RDMA networks (AWS EFA and InfiniBand). The goal is **serverless reads**: clients use one-sided RDMA operations to read values directly from server memory without involving server CPU.

**Design:**
- Server registers its value-store memory with `fi_mr_reg`, then broadcasts memory region (MR) keys and base addresses to clients
- Clients issue `fi_read(remote_addr + offset, size, mr_key)` and poll a completion queue — no server CPU involved in reads
- Writes go through the server CPU
- libfabric (`ofi-libfabric-sys`) abstracts over EFA (`"efa"` provider) and InfiniBand (`"verbs"` provider) — switch providers via the provider name string at `fi_getinfo` time

**Key libfabric API surface:**
- Init: `fi_getinfo` → `fi_fabric` → `fi_domain` → `fi_endpoint`
- Memory: `fi_mr_reg`, `fi_mr_key`
- One-sided ops: `fi_read`, `fi_write`, `fi_cq_read`

**Provider considerations:**
- EFA uses RDM (Reliable Datagram) endpoints; InfiniBand typically uses RC (Reliable Connected) for one-sided ops
- EFA latency ~5–20µs; InfiniBand ~1–5µs
- Memory registration is expensive on both — cache MRs

**Crate layout:**
- `blitzdb-server/` — server binary; currently bootstraps libfabric via `ofi-libfabric-sys` raw FFI bindings (`ofi_libfabric_sys::bindgen`)
- The `ofi-libfabric-sys` dependency is pinned to a specific git rev of the upstream libfabric repo
