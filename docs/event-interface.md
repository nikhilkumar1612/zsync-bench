# Event Interface

Status: Logical contract, Draft 0.1

This document specifies the language-neutral events consumed by both indexers.
It deliberately does not choose the on-disk or transport encoding.

## 1. Envelope

Every event has an envelope containing:

| Field | Meaning |
| --- | --- |
| `schema_version` | Event schema version |
| `dataset_id` | Stable identifier for the source dataset |
| `network` | Zcash network identity |
| `sequence` | Strictly increasing event sequence number |
| `recorded_at` | Recorder observation timestamp |
| `kind` | Event variant |
| `payload_checksum` | Checksum covering the normalized event payload |

Sequences begin at the manifest-declared first sequence. Within one dataset,
the sequence identifies one immutable event and MUST never be reused with
different content.

Timestamps are metadata, not ordering authority. Event order is determined only
by `sequence`.

## 2. Block reference

Chain events reference immutable block content with:

| Field | Meaning |
| --- | --- |
| `height` | Declared block height |
| `hash` | Canonical block hash |
| `parent_hash` | Hash of the preceding block |
| `block_time` | Time encoded by the block |
| `payload_key` | Dataset location or content key for raw bytes |
| `payload_checksum` | Checksum of the raw block bytes |
| `payload_length` | Raw byte length |

An indexer MUST verify the payload checksum before parsing the raw block. It
MUST verify that the parsed block identity agrees with the reference.

## 3. Event variants

### 3.1 `SourceStarted`

Marks a recorder session. It carries a recorder build identity and the Zebra RPC
identity visible to the recorder. It does not modify canonical chain state.

### 3.2 `SourceStopped`

Marks an orderly recorder stop. Absence of this event does not invalidate prior
committed events.

### 3.3 `SourceUnavailable`

Records the beginning of an interval in which the recorder cannot reliably
observe Zebra. It contains a categorized reason but MUST NOT contain secrets or
raw credentials.

### 3.4 `SourceRecovered`

Ends a source-unavailable interval. Before publishing new chain events, the
recorder MUST re-evaluate the canonical tip and detect any intervening
reorganization.

### 3.5 `BlockObserved`

Publishes an immutable raw block object into the dataset without asserting that
it is canonical. This permits a later connect event to reference already durable
content.

### 3.6 `BlockConnected`

Adds the referenced block to the canonical chain. Its parent MUST equal the
current canonical tip, except for the dataset's declared initial anchor.

### 3.7 `BlockDisconnected`

Removes the current canonical tip. The referenced block MUST equal that tip. A
disconnect never deletes the immutable raw block object.

### 3.8 `TipChanged`

Declares the canonical tip after one or more connect/disconnect events. It acts
as a consistency checkpoint and MUST agree with the canonical state derived from
the preceding event prefix.

### 3.9 `DatasetFinalized`

Closes a fixed dataset. It contains the final sequence, canonical tip, event-log
checksum, and block-object-set checksum. No event may follow it in that dataset.

## 4. Ordinary growth example

```text
100 SourceStarted(session=s1)
101 BlockObserved(height=500, hash=A, parent=P)
102 BlockConnected(height=500, hash=A, parent=P)
103 TipChanged(height=500, hash=A)
104 BlockObserved(height=501, hash=B, parent=A)
105 BlockConnected(height=501, hash=B, parent=A)
106 TipChanged(height=501, hash=B)
```

After event 106, both indexers must derive tip `(501, B)`.

## 5. Reorganization example

Starting canonical chain:

```text
500:A - 501:B - 502:C - 503:D
```

Replacement branch:

```text
500:A - 501:B - 502:X - 503:Y
```

Required logical ordering:

```text
200 BlockObserved(height=502, hash=X, parent=B)
201 BlockObserved(height=503, hash=Y, parent=X)
202 BlockDisconnected(height=503, hash=D, parent=C)
203 BlockDisconnected(height=502, hash=C, parent=B)
204 BlockConnected(height=502, hash=X, parent=B)
205 BlockConnected(height=503, hash=Y, parent=X)
206 TipChanged(height=503, hash=Y)
```

An indexer MUST reject event 203 if event 202 has not made `C` the current tip.

## 6. Source interruption example

```text
300 SourceUnavailable(category=rpc_timeout)
301 SourceRecovered()
302 BlockObserved(...)
303 BlockConnected(...)
304 TipChanged(...)
```

The absence of chain events between 300 and 301 does not prove the chain stayed
unchanged. The recorder is responsible for comparing the recovered chain with
the last committed tip and emitting any required disconnect/connect sequence.

## 7. Replay handshake

Before events, replay sends a logical handshake containing:

- Event schema version.
- Dataset identifier and network.
- Dataset state: `finalized` or `growing`.
- First available sequence.
- Requested start sequence.
- Timing mode and acceleration factor.
- Raw block delivery capability.

The consumer must reject unsupported schema or network values before accepting
the first event.

## 8. Acknowledgement

An indexer acknowledges the highest contiguous event sequence for which its
resulting canonical state is durable.

Acknowledgement does not mean merely received or parsed. Replay may redeliver
events after the last acknowledged sequence. Event application must therefore
be idempotent for an identical already-applied sequence and fatal for the same
sequence with different content.

## 9. Replay completion and waiting

In fixed mode, replay emits a clean end-of-stream only after
`DatasetFinalized`.

In live mode, reaching the latest committed event is not end-of-stream. Replay
waits for a new committed event, emits a transport-level heartbeat if required,
or terminates only by cancellation, fatal error, or operator shutdown.

## 10. Event-interface conformance

A conforming consumer must pass fixtures for:

- Ordinary linear growth.
- Start from a non-genesis anchor.
- Duplicate delivery of an acknowledged event.
- Sequence gap.
- Same sequence with altered content.
- Payload checksum failure.
- Parent mismatch.
- Multi-block reorganization.
- Source unavailable and recovery interval.
- Fixed dataset completion.
- Live wait, cancellation, and resume.
