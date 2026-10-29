# zsync-bench

`zsync-bench` is a reproducible experiment for comparing Go and Zig
implementations of Zcash compact-block wallet synchronization.

Both indexers consume the same recorded chain events, build the same logical
compact-block representation, and expose the same wallet synchronization API.
The project measures correctness first, then latency, throughput, memory use,
and behavior under load.

## Operating modes

```text
live:   Zebra Testnet -> recorder -> durable log -> replay -> indexer
fixed:  finalized dataset -----------------------> replay -> indexer
```

The indexers never communicate directly with Zebra. In live mode, the replay
harness follows records only after the recorder has committed them. In fixed
mode, it reads a finalized immutable dataset.

## Current status

Work Item 1, the system and benchmark contract, is accepted as version 1.0:

- [Project plan](PROJECT_PLAN.md)
- [System contract](docs/system-contract.md)
- [Event interface](docs/event-interface.md)
- [Benchmark specification](docs/benchmark-spec.md)

Work Item 2, the versioned dataset and immutable event-log format, is now in
progress. See [Dataset Format v1](docs/dataset-format.md).

## Initial scope

The first version targets Zcash Testnet and the compact-block synchronization
path represented by:

- `GetLatestBlock`
- `GetBlock`
- `GetBlockRange`

Transaction submission, mempool streaming, transparent-address queries,
Mainnet operation, and full `lightwalletd` compatibility are intentionally out
of scope for the initial experiment.

## Governing principle

Performance results count only after the Go and Zig implementations pass the
same conformance and differential tests. The goal is to compare competent
implementations of one architecture, not an optimized implementation against a
deliberately simple baseline.
