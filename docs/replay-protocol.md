# Replay Transport Protocol v1

Status: Work Item 4 draft; fixed-dataset transport implemented.

The replay harness launches one consumer process. Replay writes protocol frames
to the child's standard input, the consumer writes protocol frames to standard
output, and both processes reserve standard error for human-readable logs.

## Framing

Every transport message is encoded as:

| Size | Field |
| ---: | --- |
| 4 bytes | Unsigned little-endian JSON length |
| N bytes | UTF-8 JSON message |

The default maximum frame length is 2 MiB. Unknown fields and unknown message
types are rejected. Transport framing is deliberately separate from Dataset
Format v1 framing: dataset digests protect durable source data, while transport
frames carry already-validated logical messages between local processes.

## Session

Replay sends exactly one `handshake` before any event. It identifies protocol
and event-schema versions, dataset ID and network, finalized or growing state,
absolute shared dataset root, first and requested sequences, timing settings,
and `shared_dataset_content_key` raw-block delivery.

The consumer must reply with `ready` and a compatible protocol version. It may
instead return a fatal `error`. Replay sends no event before `ready`.

Replay then sends `event` messages containing the Dataset Format v1
`EventEnvelope` unchanged. A consumer replies with cumulative `ack` messages.
An acknowledgement means every event through that sequence has been applied
to durable consumer state; merely receiving or parsing an event is insufficient.

Replay bounds sent-but-unacknowledged events with `--max-in-flight`. It rejects
acknowledgement regression and acknowledgement beyond the last sent sequence.
After a fixed dataset's final event, replay sends `end` and waits for the final
durable acknowledgement before closing the input stream.

## Timing

- `recorded`: preserve positive differences between event observation times.
- `accelerated`: divide those differences by `--acceleration`.
- `maximum`: add no intentional delay.

All modes preserve event sequence and content. Consumer backpressure may make a
run slower than its requested schedule but can never reorder events.

## Progress and resume

`progress.json` records the dataset ID and highest durable cumulative
acknowledgement. `--resume` requests the following sequence and rejects progress
belonging to another dataset. Identical events may be redelivered when a process
fails after making consumer state durable but before replay persists its ack;
consumers must therefore apply identical duplicate sequences idempotently.

`run-manifest.json` records the dataset identity and event-log digest, consumer
command, timing configuration, starting sequence, final acknowledgement,
timestamps, and completion status.

## Implemented verification

Automated process-level tests cover handshake negotiation, ordered fixed replay,
frame-size rejection, cumulative durable progress, injected consumer failure,
and restart from the last persisted acknowledgement.

Remaining Work Item 4 scope includes growing-log follow mode, deterministic
pause controls, start-by-height selection, and heartbeats.

## Diagnostic fixed replay

The diagnostic sink validates sequence identity and payload checksums and makes
its acknowledgement durable. It is transport test infrastructure, not either
benchmark indexer. Its acknowledgement interval must not exceed replay's
in-flight window.

```sh
go build -o bin/zsync-replay ./cmd/zsync-replay
go build -o bin/zsync-replay-sink ./cmd/zsync-replay-sink

bin/zsync-replay run \
  --dataset ./captures/testnet-50k \
  --run-dir ./runs/testnet-50k-sink \
  --timing maximum \
  --max-in-flight 256 \
  --consumer ./bin/zsync-replay-sink \
  --consumer-arg=--state \
  --consumer-arg=./runs/testnet-50k-sink/sink-state.json \
  --consumer-arg=--ack-every \
  --consumer-arg=100
```

This path has been verified against the finalized `testnet-50k` capture: all
100,004 events were delivered in order and final sequence 100,003 was durably
acknowledged by the sink.
