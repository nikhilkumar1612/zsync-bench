# Dataset Format v1

Status: Draft 0.1 for Work Item 2

This document defines the portable dataset consumed by the replay harness and
both indexers. It turns the logical contract in `event-interface.md` into a
concrete on-disk format.

## 1. Design goals

The format must be:

- Immutable after finalization.
- Safely readable while a live recorder appends events.
- Recoverable after a process or machine interruption.
- Deterministic across Go and Zig.
- Independently verifiable without Zebra.
- Efficient for large raw block payloads.
- Explicitly versioned.

The format favors simple validation and reproducibility over storage density.

## 2. Package layout

```text
<dataset>/
├── manifest.json
├── events.bin
├── blocks/
│   └── <first-two-hash-chars>/
│       └── <remaining-hash-chars>.raw
└── state/
    └── recorder-checkpoint.json
```

`state/` is recorder working state and is excluded from the finalized dataset
identity. A finalized package may omit it.

No symlink is permitted within a dataset package. Readers must reject paths
that escape the dataset root.

## 3. Common encodings

- Integers in binary frame headers are unsigned little-endian.
- Protobuf fields use the encoding defined by Protocol Buffers version 3.
- JSON is UTF-8 without a byte-order mark.
- Human-readable hashes use lowercase hexadecimal.
- Zcash block hashes and parent hashes use canonical display order in JSON,
  paths, and protobuf byte fields.
- Timestamps are signed Unix nanoseconds in event messages and RFC 3339 strings
  in the human-readable manifest.
- SHA-256 is the dataset integrity hash.

Canonical display order means the same byte order shown by Zebra JSON-RPC. A
parser must convert internal hash representations at its boundary rather than
leaking language-specific endianness into the dataset.

## 4. Raw block objects

Raw blocks are stored exactly as returned by Zebra's raw `getblock` result after
hex decoding. They are not wrapped, compressed, or transformed in v1.

The content key for block hash `abcdef...` is:

```text
blocks/ab/cdef....raw
```

The complete lowercase 64-character hash is split after its first two
characters. The content key is derived from the verified block hash and is not
accepted from an untrusted path field.

### 4.1 Publication

The recorder publishes a block object by:

1. Writing the bytes to a temporary file in the destination directory.
2. Flushing the file contents.
3. Atomically renaming it to the final content path.
4. Flushing the containing directory when the platform supports it.
5. Appending a `BlockObserved` event only after the final object is visible.

If the final path already exists, its length and SHA-256 digest must match the
new object. A mismatch is a fatal integrity error.

## 5. Event-log file header

`events.bin` starts with this 16-byte header:

| Offset | Size | Field | Value |
| ---: | ---: | --- | --- |
| 0 | 8 | Magic | ASCII `ZSYNCLOG` |
| 8 | 2 | Major version | `1` |
| 10 | 2 | Minor version | `0` |
| 12 | 4 | Flags | `0` in v1 |

Readers must reject an unknown major version. They may accept a higher minor
version only when all encountered required features are understood.

## 6. Event frame

Each event follows the file header as one frame:

| Size | Field |
| ---: | --- |
| 4 | Frame payload length |
| 8 | Event sequence |
| N | Serialized `EventEnvelope` protobuf |
| 32 | SHA-256 frame digest |

The frame digest is SHA-256 over:

```text
payload_length_le || sequence_le || protobuf_payload
```

`payload_length` is the protobuf payload length and must not exceed the
manifest's `max_event_payload_bytes`. The sequence in the frame header must
equal the sequence inside `EventEnvelope`.

### 6.1 Committed frame

A frame is committed only when all header bytes, payload bytes, and digest bytes
are present and the digest validates.

In a growing dataset, a reader reaching an incomplete final frame waits for
more bytes. It must not expose that frame. An invalid digest on a complete frame
is a fatal error.

In a finalized dataset, an incomplete final frame is a fatal truncation error.

### 6.2 Sequence rules

Sequences are contiguous unsigned 64-bit integers. The first sequence is
declared by the manifest. A gap, regression, duplicate with different content,
or overflow is fatal.

## 7. Event schema

The concrete event messages are defined in:

```text
proto/zsync/dataset/v1/events.proto
```

The envelope includes schema version, dataset ID, network, sequence, recorder
timestamp, event variant, and a payload checksum.

`BlockObserved` establishes the immutable raw block object. Connect and
disconnect events repeat the block reference so a reader can validate chain
transitions without searching earlier arbitrary event payloads.

Unknown event variants are fatal in v1 because silently skipping a chain-state
transition would invalidate all following state.

## 8. Manifest

`manifest.json` conforms to:

```text
schema/manifest-v1.schema.json
```

During live recording, the manifest describes the dataset identity, schema,
network, initial anchor, safety limits, and `finalized: false`. Mutable progress
such as the latest sequence belongs in recorder checkpoint state, not in the
live manifest.

Finalization atomically replaces the live manifest with one containing:

- `finalized: true`
- Final event sequence and count
- Final canonical height and hash
- `events.bin` byte length and SHA-256
- Raw block object count
- Deterministic block-set SHA-256
- Finalization timestamp

## 9. Dataset identity

`dataset_id` is a lowercase UUID string generated when recording starts. It is
immutable and appears in the manifest and every event envelope.

Dataset identity is not an integrity claim. Integrity comes from the finalized
checksums and validation of every event and block object.

## 10. Block-set checksum

The finalized block-set digest is deterministic. For every referenced block
object, create this ASCII line:

```text
<block-hash> <payload-length-decimal> <payload-sha256>\n
```

Sort lines lexicographically by block hash, concatenate them, and compute
SHA-256 over the resulting bytes.

Unreferenced temporary files are not part of the block set and cause validator
warnings. An unreferenced final `.raw` object causes finalization or validation
to fail unless an explicit future schema feature permits retained orphan data.

## 11. Initial anchor

A dataset may begin after genesis. Its manifest then contains an anchor with:

- Height immediately preceding the first connectable block.
- Anchor block hash.
- Optional compact-state fixture reference required by later indexer work.

The first `BlockConnected` parent must equal the anchor hash. The anchor is
trusted dataset metadata and must be verified against the recorder's source
when the dataset is created.

## 12. Finalization

Finalization is one-way. The recorder:

1. Stops publishing new events.
2. Flushes and closes `events.bin`.
3. Validates all frames and referenced block objects.
4. Derives final canonical state.
5. Computes log and block-set checksums.
6. Writes the final manifest to a temporary sibling file.
7. Flushes and atomically renames it over `manifest.json`.

No writer may append to a finalized dataset. A derived dataset requires a new
dataset ID and records its parent dataset ID in the manifest.

## 13. Crash recovery

For a growing dataset, recorder recovery:

1. Validates the file header.
2. Scans frames through the last complete valid frame.
3. Truncates only an incomplete trailing frame.
4. Refuses to truncate a complete frame with an invalid digest.
5. Reconstructs canonical state from the initial anchor and valid event prefix.
6. Reconciles recorder checkpoint state with the reconstructed sequence.
7. Rechecks Zebra before publishing subsequent chain events.

The event log is authoritative. Recorder checkpoint state may accelerate
recovery but cannot override the valid log prefix.

## 14. Safety limits

The manifest declares:

- Maximum event payload bytes.
- Maximum raw block bytes.
- Maximum allowed reorganization depth for the recording policy.

Readers enforce payload and block limits before allocation. A reorganization
deeper than the configured recorder policy stops recording with a diagnostic;
it is never silently truncated.

## 15. Validation requirements

The Work Item 2 validator must verify:

- Manifest schema and supported versions.
- Safe paths and absence of symlinks.
- File magic and version.
- Frame length, sequence, and digest.
- Envelope/frame sequence equality.
- Dataset ID and network consistency.
- Block object existence, length, and SHA-256.
- Parsed raw block hash and parent identity.
- Legal connect/disconnect transitions.
- Every `TipChanged` checkpoint.
- Final log checksum and length.
- Final block-set checksum and object count.
- Final canonical height and hash.

It emits a machine-readable validation report and exits non-zero for any
integrity failure.

## 16. Remaining Work Item 2 decisions

- Exact maximum defaults for event and raw block sizes.
- Whether v1 needs an optional block compression feature.
- Exact recorder-checkpoint JSON schema.
- Whether compact-state anchor fixtures belong inside this package or in the
  later compact-block conformance corpus.
