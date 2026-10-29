# zsync-bench Project Plan

## Objective

Build equivalent Go and Zig indexers for Zcash compact-block wallet
synchronization and measure each implementation under reproducible workloads.

The project reports metrics for each individual run. It does not declare a
language winner or apply continuation thresholds.

## Operating modes

```text
live:   Zebra Testnet -> recorder -> durable event log -> replay -> indexer
fixed:  finalized dataset ----------------------------> replay -> indexer
```

The recorder is the only component that communicates with Zebra. Both indexers
consume the same replay interface in both modes.

### Fixed mode

- Uses a finalized immutable dataset.
- Supports recorded, accelerated, and maximum-throughput timing.
- Provides deterministic, reproducible performance runs.

### Live mode

- The recorder follows Zebra Testnet and commits events to the durable log.
- Replay follows only complete committed records.
- Indexers never receive Zebra responses directly.
- Provides integration, freshness, recovery, and stability measurements.

## Accepted architectural decisions

1. Replay events carry block metadata and a content key. Indexers load immutable
   raw block bytes from the shared dataset.
2. The initial replay transport is a framed stream over stdin/stdout. Replay
   launches one indexer, sends events, and reads durable acknowledgements.
3. If a reorganization intersects an active `GetBlockRange`, the indexer aborts
   the stream and returns `Unavailable`. A response must never mix branches.
4. Go and Zig use the same logical and physical storage design for the primary
   comparison.
5. Reported performance runs execute one indexer at a time. Simultaneous runs
   are limited to correctness diagnostics.
6. The project pins the latest stable tagged revision of
   `zcash/lightwallet-protocol` when implementation begins and records its
   commit hash.
7. Reports present metrics for each run without scoring the implementations or
   producing a winner.
8. The recorder and replay harness are implemented in Go. The benchmarked
   indexers are implemented independently in Go and Zig.

## Initial scope

Included:

- Zcash Testnet
- Zebra JSON-RPC recorder
- Immutable raw-block storage
- Append-only chain event log
- Reorganization representation
- Fixed and live replay
- Compact-block construction
- `GetLatestBlock`
- `GetBlock`
- `GetBlockRange`
- Go and Zig indexers
- Differential correctness tests
- Performance and resource measurements

Deferred:

- Transaction submission
- Mempool synchronization
- Transparent-address queries
- Mainnet operation
- Viewing-key or address-specific indexing
- Public TLS termination and production authentication
- Full `lightwalletd` compatibility

## Work item 1: System contract and benchmark specification

Define:

- Component responsibilities and boundaries
- Event-interface semantics
- Live and fixed mode behavior
- Wallet synchronization RPC scope
- Reorganization behavior
- Error, cancellation, restart, and shutdown semantics
- Benchmark scenarios, metrics, and fairness controls

Deliverables:

- `docs/system-contract.md`
- `docs/event-interface.md`
- `docs/benchmark-spec.md`

Verification:

- A developer can implement a compatible producer or consumer from the written
  contracts and fixtures.
- Open architectural decisions have explicit resolutions or belong to a named
  later work item.

Status: specification drafted; accepted decisions must be incorporated before
the work item is marked complete.

## Work item 2: Dataset and immutable event-log format

Define and implement a language-neutral dataset containing immutable block
objects, an append-only event log, and a versioned manifest.

The format must provide:

- Monotonic event sequence numbers
- Content-addressed raw block objects
- Block height, hash, parent hash, and block time
- Recorder observation timestamps
- Payload and log checksums
- Atomic publication of completed records
- Reorganization events
- Recovery after interrupted writes
- Finalization for fixed datasets
- Deterministic Go and Zig readers

Verification:

- An independent validator reconstructs the canonical tip after every event.
- Every payload checksum and parent relationship validates.
- Go and Zig readers produce identical normalized events.

## Work item 3: Zebra Testnet recorder

Implement the Go recorder as the only Zebra client.

It must:

- Start from a configured height or durable resume position
- Fetch and verify raw canonical blocks
- Publish immutable block objects and chain events
- Detect and record reorganizations
- Resume without losing or duplicating committed events
- Recover from temporary Zebra unavailability

The recorder does not construct compact blocks.

Verification:

- Reach and follow the Testnet tip.
- Restart from a committed boundary.
- Recover after simulated RPC failure.
- Represent an injected reorganization correctly.
- Produce a dataset accepted by the validator.

## Work item 4: Replay harness and event transport

Implement the Go replay harness using a framed stdin/stdout protocol.

It must support:

- Recorded timing
- Configurable acceleration
- Maximum-throughput replay
- Start from an event sequence or height
- Deterministic pause and resume
- Follow mode for a growing log
- Durable consumer acknowledgements
- Per-run manifests

Verification:

- Repeated fixed runs emit identical event sequences.
- Acceleration preserves event order.
- Maximum mode introduces no intentional delay.
- Live replay never exposes partial records.
- Resume from an acknowledgement produces no gaps.

## Work item 5: Compact-block specification and fixtures

Pin the canonical light-wallet protocol revision and specify raw-block to
compact-block transformation, including:

- Block headers and metadata
- Transaction identities and endianness
- Sapling spends and outputs
- Orchard actions
- Testnet transaction versions and upgrade boundaries
- Commitment-tree metadata
- Field omission and default-value behavior
- Reorganization effects on indexed state

Create golden fixtures covering empty, transparent, Sapling, Orchard, mixed,
large, upgrade-boundary, and reorganized blocks.

Verification:

- Both indexers produce identical normalized compact-block output for every
  fixture before performance measurements begin.

## Work item 6: Go synchronization indexer

Implement the Go indexer against the shared contracts and storage design.

It must:

- Consume and acknowledge replay events
- Construct and persist compact blocks
- Maintain canonical state
- Apply and roll back reorganizations
- Expose the selected wallet RPCs
- Bound queues, ranges, caches, and active streams
- Propagate cancellation
- Recover durable state after restart
- Export metrics and profiling information

Verification:

- Pass all shared conformance fixtures.
- Build the expected chain and handle reorganizations.
- Return correct block ranges.
- Abort an intersected stream with `Unavailable` during a reorganization.
- Cancel promptly and stay within configured resource limits.

## Work item 7: Zig synchronization indexer

Implement the same behavior in Zig using the same event, storage, RPC, and
configuration contracts.

The design should use:

- Request- or stream-scoped allocators
- Bounded queues
- Reusable parsing and serialization buffers
- Explicit byte-buffer ownership
- Controlled cache capacity
- End-to-end cancellation
- Leak and allocation-failure tests

Verification uses the same requirements as the Go indexer. Behavioral parity
comes before language-specific optimization.

## Work item 8: Wallet client and differential tests

Create a black-box wallet synchronization client and comparison runner.

It must exercise:

- Range synchronization
- Incremental synchronization
- Slow reads
- Cancellation and resume
- Concurrent wallet sessions
- Reorganization during synchronization
- Restart and recovery

Verification:

- Both indexers finish with the same canonical tip.
- Normalized compact blocks and RPC errors match.
- Cancellation, restart, and reorganization outcomes match.

## Work item 9: Benchmark and measurement framework

Automate isolated runs of each indexer and report metrics without producing a
winner or recommendation.

Measurements include:

- Time to first compact block
- Range completion duration
- Blocks and bytes per second
- p50, p95, p99, and p99.9 latency
- CPU time
- Peak and steady resident memory
- Memory per active stream
- Storage bytes read and written
- Cancellation completion time
- Replay indexing delay
- Testnet tip distance in live mode

Required scenarios:

- Single historical synchronization
- Shared-range concurrency
- Distributed-range concurrency
- Incremental synchronization
- Slow clients
- Cancellation and resume
- Concurrent ingestion and serving
- Reorganization during synchronization
- Fixed-resource saturation
- Extended live stability

Every run records source revisions, compiler versions, build flags, hardware,
operating system, dataset checksum, replay configuration, cache state, process
limits, random seed, and raw samples.

Verification:

- Another developer can select a dataset, run a documented command, and
  reproduce a report containing correctness status and individual run metrics.

## Milestones

### Milestone 1: Shared foundation

- System contract
- Event interface
- Benchmark specification
- Dataset format
- Compact-block rules and initial fixtures

### Milestone 2: Data pipeline

- Zebra recorder
- Dataset validator
- Fixed replay
- Live follow-mode replay

### Milestone 3: Equivalent indexers

- Go indexer
- Zig indexer
- Wallet client
- Differential tests

### Milestone 4: Measurement

- Automated benchmark framework
- Fixed-mode runs
- Live-mode integration and stability runs
- Per-run metric reports

## Governing principle

Correctness and behavioral equivalence are required before performance metrics
count. The project measures competent implementations of the same architecture
under the same conditions. It reports what happened in each run without turning
the results into a language verdict.
