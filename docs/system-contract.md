# System Contract

Status: Accepted 1.0

This document defines the boundaries and observable behavior of the first
`zsync-bench` experiment. The words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY
are normative.

## 1. Objective

The experiment will determine whether Zig provides a material advantage over
Go for the Zcash compact-block wallet synchronization data path.

The comparison covers:

1. Converting ordered raw Zcash blocks into a compact-block index.
2. Maintaining canonical indexed state across chain reorganizations.
3. Serving block ranges to wallet clients through equivalent RPC behavior.
4. Operating ingestion and wallet serving concurrently.

The experiment does not attempt to prove that either language is universally
better or that either prototype is a complete `lightwalletd` replacement.

## 2. System boundary

```text
                               +------------------+
Zebra Testnet -> Recorder ---->| durable event log|---> Replay Harness
                               +------------------+          |
                                                            v
                                                    Go or Zig Indexer
                                                            |
                                                            v
                                                     Wallet Clients
```

### 2.1 Recorder

The recorder is the only experiment component permitted to call Zebra.

It MUST:

- Observe a configured Zcash network and identify it in every dataset.
- Fetch raw block data and verify its identity before publication.
- Record enough parent information to reconstruct canonical ordering.
- Publish immutable block payloads and append-only chain events.
- Detect and represent canonical-chain reorganizations.
- Resume without silently losing or renumbering committed events.

The recorder MUST NOT construct the compact-block representation used by the
indexers. That transformation is part of the language comparison.

### 2.2 Dataset

The dataset is the durable boundary between source acquisition and the
experiment. It consists of immutable block objects, an ordered event log, and a
manifest.

The dataset MUST be language-neutral, checksummed, versioned, and readable
without Zebra. The final format is specified by the dataset work item; its
logical event requirements are defined in `event-interface.md`.

### 2.3 Replay harness

The replay harness is the sole event source for an indexer.

It MUST:

- Preserve event order and identity.
- Deliver identical logical events to both indexer implementations.
- Support original recorded timing, accelerated timing, and maximum throughput.
- Support a finalized dataset and a growing committed log.
- Record a run manifest that identifies the exact inputs and configuration.

It MUST NOT transform raw Zcash blocks into compact blocks.

The initial implementation MUST use a framed stdin/stdout transport. The replay
harness launches one indexer process, sends event frames, and reads durable
acknowledgements. Events carry block metadata and a content key; indexers load
the referenced immutable raw bytes from the shared dataset.

### 2.4 Indexers

The Go and Zig indexers have the same responsibilities:

- Consume the common event interface.
- Validate event continuity and block identity.
- Convert raw blocks to the agreed compact representation.
- Maintain canonical state as connect and disconnect events arrive.
- Persist enough state to resume safely after restart.
- Expose the same wallet synchronization RPCs and error semantics.
- Enforce the same configured range, queue, and concurrency limits.
- Export comparable correctness and resource metrics.

An indexer MUST NOT call Zebra or infer missing source events from another
network service.

### 2.5 Wallet clients

Wallet clients are black-box consumers of the indexer RPC API. Test clients MAY
simulate slow reads, cancellation, reconnects, concurrent restores, and
incremental tip synchronization.

## 3. Operating modes

### 3.1 Fixed mode

```text
finalized dataset -> replay harness -> one indexer -> clients
```

A fixed-mode run MUST use a finalized manifest containing the dataset identity,
schema version, final event sequence, and content checksum.

The input MUST remain unchanged for the duration of the run. Repeating a run
with the same dataset and replay parameters MUST produce the same logical event
sequence.

Fixed mode is the primary mode for comparative performance measurements.

### 3.2 Live mode

```text
Zebra -> recorder -> growing durable log -> replay follow mode -> indexer
```

"Live" means following the recorder's durable log. It never means forwarding a
Zebra response directly to an indexer.

The replay harness MUST expose only complete committed records. Each consumer
MUST maintain an independent acknowledged event position. Restarting a consumer
from its last acknowledged position MUST produce neither an event gap nor an
unreported skip.

Live mode is the primary mode for integration, freshness, restart, and extended
stability testing.

## 4. Initial wallet RPC contract

The experiment initially implements the canonical Zcash light-wallet message
types needed for:

- `GetLatestBlock`
- `GetBlock`
- `GetBlockRange`

The protocol definition MUST be pinned to a documented upstream revision.

Both implementations MUST agree on:

- Valid and invalid height ranges.
- Inclusive range endpoints.
- Empty-range behavior.
- Maximum configured range size.
- Block ordering.
- Missing-block behavior.
- Status codes for invalid input and unavailable state.
- Deadline and cancellation propagation.
- Behavior when a reorganization affects a stream in progress.

If a canonical reorganization intersects the range of an active
`GetBlockRange` stream, the server MUST abort that stream with gRPC status
`Unavailable`. It MUST NOT continue the response using blocks from a different
branch.

The exact protobuf revision and normalized comparison rules will be finalized
with the compact-block conformance fixtures.

Implementation MUST pin the latest stable tagged revision of the canonical
`zcash/lightwallet-protocol` available when that work begins and record its tag
and commit hash in the repository.

## 5. Compact-block equivalence

For a given canonical raw block and prior canonical state, both indexers MUST
produce the same normalized compact-block value.

Normalization MUST account for protobuf representations where serialized byte
order is not semantically significant. Comparison MUST cover all included
fields, byte endianness, transaction identifiers, pool actions, tree metadata,
block height, block hash, parent hash, and block time.

No performance result is valid for a fixture that produces different normalized
outputs.

## 6. Reorganization semantics

A reorganization is represented as disconnect events from the old tip toward
the common ancestor, followed by connect events from the common ancestor toward
the new tip.

Example:

```text
old canonical chain: A - B - C - D
new canonical chain: A - B - X - Y

BlockDisconnected(D)
BlockDisconnected(C)
BlockConnected(X)
BlockConnected(Y)
TipChanged(Y)
```

After each event, an indexer MUST either expose a canonical state consistent
with the processed event prefix or declare itself temporarily unavailable. It
MUST NOT expose a silently mixed old/new chain.

## 7. Resource and cancellation semantics

Both indexers MUST accept equivalent configuration for:

- Maximum block range.
- Maximum active wallet streams.
- Per-stream queue capacity.
- Compact-block cache capacity.
- Event ingestion queue capacity.
- Storage location.
- Shutdown deadline.

The primary comparison MUST use the same logical and physical storage design in
both indexers. Any later language-specific storage experiment must be reported
separately and must not replace the primary results.

All queues MUST be bounded. A disconnected or cancelled wallet MUST stop causing
new storage and serialization work within a measurable cancellation interval.
No shared lock may be held across an upstream event read, storage operation, or
wallet network write.

## 8. Failure behavior

Components MUST distinguish fatal data-integrity errors from recoverable
availability errors.

Examples of fatal conditions:

- Hash mismatch for immutable block content.
- Event sequence regression.
- A connect event with an impossible parent for the declared canonical state.
- Unsupported dataset schema.

Examples of recoverable conditions:

- Zebra temporarily unavailable to the recorder.
- Replay following a live log with no new committed event.
- Wallet cancellation or deadline expiration.
- Temporary indexer unavailability during recovery.

Every fatal condition MUST produce a non-zero process result and a structured
diagnostic. Components MUST NOT silently repair or skip integrity failures.

## 9. Restart behavior

The recorder, replay harness, and indexers MUST persist explicit progress
positions. A process restart MUST resume from a verifiable committed boundary.

Indexers MUST detect disagreement between persisted indexed state and the replay
position. Recovery MAY roll back uncommitted local work, but MUST NOT claim an
event has been applied unless its canonical state is durable.

## 10. Initial exclusions

The following are outside the first experiment:

- Transaction submission.
- Mempool synchronization.
- Transparent-address balance and UTXO queries.
- Viewing-key or address-specific server indexing.
- Mainnet operation.
- Production public TLS termination.
- Production authentication and rate limiting.
- Complete `lightwalletd` compatibility.

These exclusions do not permit unrealistic RPC behavior within the selected
synchronization scope.

## 11. Specification acceptance checklist

Work Item 1 is accepted when reviewers can answer yes to all of the following:

- Every component has a single clear source of truth and responsibility.
- Both operating modes use the recorder/replay boundary.
- The event ordering and reorganization rules are unambiguous.
- Both indexers have identical inputs and observable responsibilities.
- The initial wallet RPC surface and exclusions are explicit.
- Correctness gates performance measurement.
- Benchmark scenarios, measurements, and fairness controls are specified.
- Example event sequences cover ordinary growth, restart, and reorganization.
- Open decisions are listed rather than silently assumed.

This checklist was accepted with the decisions recorded below.

## 12. Accepted decisions

1. Events carry block metadata and a content key; raw block bytes are loaded
   from the shared immutable dataset.
2. Initial replay transport is framed stdin/stdout.
3. A reorganization intersecting an active range aborts it with `Unavailable`.
4. Both indexers use the same storage design for the primary comparison.
5. Reported performance runs execute indexers separately. Simultaneous runs are
   correctness diagnostics only.
6. The canonical light-wallet protocol is pinned to the latest stable tag when
   implementation begins, including its commit hash.
7. Reports provide metrics for each run and do not score implementations or
   declare a language winner.
8. The recorder and replay harness are implemented in Go.

## 13. Decisions assigned to later work items

The following format and implementation choices remain intentionally open until
their named work items:

1. On-disk framing and serialization of the dataset.
2. Exact shared storage engine and physical layout.
3. Exact protocol tag selected when compact-block implementation begins.
4. Normalization rules for semantically equivalent protobuf encodings.
