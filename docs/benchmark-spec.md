# Benchmark Specification

Status: Accepted 1.0

This document defines what `zsync-bench` will measure and the controls required
for a fair Go/Zig comparison.

## 1. Decision question

The benchmark answers:

> How does each Go or Zig indexer run behave for the Zcash compact-block
> indexing and wallet synchronization path under controlled conditions?

Results do not establish complete `lightwalletd` suitability. Reports present
metrics for individual runs without scoring implementations, declaring a
winner, or issuing a language recommendation.

## 2. Correctness gate

A benchmark result is reportable only when both implementations pass the same:

- Event-interface conformance suite.
- Raw-block-to-compact-block fixtures.
- Canonical-chain and reorganization tests.
- Wallet RPC differential tests.
- Restart and resume scenarios.

If normalized outputs differ, the affected performance result is invalid rather
than a win for either implementation.

## 3. Measurements

### 3.1 Wallet-facing

- Time to first compact block.
- Complete range duration.
- Blocks and payload bytes per second.
- p50, p95, p99, and p99.9 response or completion latency.
- Error and timeout rate.
- Resume duration after reconnect.

### 3.2 Resource use

- Process CPU time.
- Peak and steady resident memory.
- Memory per active wallet stream.
- Allocated bytes and allocation count where comparable.
- Storage bytes read and written.
- Network bytes written.
- Context switches and system calls where available.

### 3.3 Ingestion and freshness

- Raw blocks and transactions processed per second.
- Time from replay delivery to durable indexed compact block.
- Event sequences or blocks behind replay.
- In live mode, blocks and elapsed time behind the recorded Testnet tip.
- Reorganization rollback and replacement duration.

### 3.4 Resilience

- Time from wallet cancellation to work cessation.
- Maximum observed queue depth.
- Memory growth with slow clients.
- Recovery duration after restart.
- Behavior at configured memory and concurrency limits.

## 4. Required scenarios

### 4.1 Historical single-wallet synchronization

One wallet requests a large finalized historical range. Run with cold and warm
index/cache state where applicable.

### 4.2 Shared-range concurrency

Many wallets request the same range. Test whether immutable data and cached
serialized representations are safely shared.

### 4.3 Distributed-range concurrency

Wallets request unrelated ranges to reduce cache sharing and exercise storage.

### 4.4 Incremental synchronization

Many clients request only the most recent blocks and periodically return for
updates.

### 4.5 Slow clients

Clients read below server production rate. Memory and queue depth must remain
bounded.

### 4.6 Cancellation and resume

Clients cancel at deterministic and pseudo-random points, then resume from their
last accepted height.

### 4.7 Concurrent ingest and serve

The indexer consumes replay events while historical and incremental wallet
requests run concurrently.

### 4.8 Reorganization during synchronization

Replay applies a recorded or synthetic reorganization while clients request a
range intersecting the affected heights.

### 4.9 Resource saturation

Under a fixed process memory and CPU budget, increase wallet concurrency until
the service reaches its configured limit or fails the scenario.

### 4.10 Extended live stability

Follow the growing recorder log for a defined long-duration window while
performing incremental wallet synchronization and controlled restarts.

## 5. Replay timing profiles

Each applicable fixed dataset is tested with:

- `recorded`: preserve recorded inter-event delay.
- `accelerated(N)`: divide recorded delays by the declared factor.
- `maximum`: emit the next event as soon as consumer flow control permits.

The run manifest records the selected profile and factor.

## 6. Fairness controls

Reported comparative runs MUST use the same:

- Host class and power configuration.
- Operating-system version and kernel settings.
- Dataset identity and checksum.
- Replay event start and end sequences.
- Replay timing profile.
- Index state and cache state classification.
- Storage device and filesystem class.
- CPU, memory, and file-descriptor limits.
- Wallet request schedule and random seed.
- RPC compression and TLS settings.
- Logging verbosity and metrics sampling interval.
- Queue, cache, stream, and range limits.

The implementations should run separately for primary results. Running them
simultaneously on one host is diagnostic because shared CPU, cache, storage, and
scheduler contention can bias the comparison.

The primary comparison uses the same logical and physical index storage design
for both implementations. Language-specific alternatives may be measured only
as separately labeled experiments.

## 7. Build and run disclosure

Every result bundle records:

- Source revision and dirty-state marker.
- Go and Zig versions.
- Dependency lockfile checksums.
- Build mode and compiler flags.
- Executable checksum.
- Hardware and operating-system inventory.
- Dataset and replay manifest.
- Full runtime configuration excluding secrets.
- Warm-up count, measured-run count, and run order.
- Raw samples as well as summarized statistics.

## 8. Run methodology

Each scenario has warm-up and measured iterations. Go-first and Zig-first order
must alternate or be randomized with a recorded seed. A failed iteration remains
in the raw result set and is classified rather than silently discarded.

Reports include distributions and confidence intervals where the sample count
supports them. Best-of-N results are not acceptable as the primary comparison.

## 9. Reporting policy

Reports contain the measured values, run configuration, raw samples, and
correctness status for each individual run. The reporting layer MUST NOT rank
languages, calculate a composite score, apply pass/fail performance thresholds,
or identify a winner.

Cross-run tables and plots MAY place metrics side by side when their environment
and workload controls match. Any interpretation remains outside the benchmark
artifact.

## 10. Benchmark acceptance criteria

The benchmark framework is accepted when another developer can:

1. Obtain a named dataset.
2. Build both indexers from recorded revisions.
3. Run a documented command for a scenario.
4. Produce machine-readable raw results and a human-readable comparison.
5. Confirm that correctness gates passed for that build and dataset.
6. Reproduce comparable results within the documented environmental variance.

## 11. Open benchmark decisions

- Exact dataset windows and sizes.
- Wallet concurrency levels for the standard suite.
- Duration of the live stability test.
- Statistical summaries and confidence-interval method.
- Cross-host versus same-host replay deployment.
- Tooling used for process and kernel-level measurements.
