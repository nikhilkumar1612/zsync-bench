# Zebra Testnet Recorder

Status: Work Item 3 implementation; local conformance complete, live Testnet smoke test pending.

`zsync-recorder` is the only component that communicates with Zebra. It writes
raw blocks and canonical-chain events into Dataset Format v1; it does not build
compact blocks.

## Run modes

Start a bounded, finalized capture:

```sh
go run ./cmd/zsync-recorder record \
  --zebra-url http://127.0.0.1:18232 \
  --output ./captures/sample \
  --start-height 2500000 \
  --max-blocks 1000 \
  --shutdown-mode finalize
```

A bounded fixed capture may use a partially synchronized public-Testnet node
when its entire requested range is already committed and retained. Follow mode
still requires the source to be at or near the network tip. On pruned nodes,
the requested start height must not be below the RPC-reported `pruneheight`.

Follow Testnet and leave a growing dataset on shutdown:

```sh
go run ./cmd/zsync-recorder record \
  --zebra-url http://127.0.0.1:18232 \
  --output ./captures/live \
  --start-height 2500000 \
  --follow
```

Resume that dataset from its last committed event boundary:

```sh
go run ./cmd/zsync-recorder record \
  --zebra-url http://127.0.0.1:18232 \
  --output ./captures/live \
  --resume \
  --follow
```

If authentication is enabled, pass `--rpc-user` and set the password in
`ZSYNC_ZEBRA_RPC_PASSWORD`. Credentials are never written to events or logs.

## Correctness and recovery

- The recorder accepts only the public Zcash Testnet. It verifies the canonical
  Testnet genesis hash, so a configured development chain that also reports
  `chain: test` is rejected.
- Older nodes may report `initial_block_download_complete`. Zakura and current
  Zebra responses are accepted without that optional field when header lag is
  bounded and verification progress indicates that initial sync is complete.
- It resolves a height to a hash, fetches the raw block by that hash, and checks
  the parsed hash and parent before publishing it.
- On a reorganization, replacement block objects become durable first. The log
  then disconnects the old branch from its tip, connects the replacement
  branch, and publishes one consistent `tip_changed` event.
- Reorganizations crossing the dataset anchor or exceeding the configured
  maximum depth stop the recorder without inventing history.
- Every event frame is synchronized before its sequence is committed. Resume
  validates the complete log, truncates only an incomplete tail frame, and
  derives canonical state from the log. The JSON checkpoint is an operational
  aid; it is not the source of truth.
- Temporary RPC failures produce one `source_unavailable` event per outage and
  one `source_recovered` event. Exponential retry is bounded by `--retry-min`
  and `--retry-max`. After recovery during branch fetching, reconciliation
  restarts from a fresh chain view before publishing canonical-chain events.

## Resource limits

The HTTP client applies a request deadline and a response-size cap. Dataset v1
also limits raw block size, event payload size, and reorganization depth. The
defaults are chosen to admit protocol-valid Zcash blocks while bounding memory
and disk inputs.

## Verification

The automated suite covers linear capture, pause, finalized capture, durable
resume, an injected three-block reorganization, temporary RPC failure, client
timeouts, response-size limits, RPC errors, dataset validation, and recovery
from an incomplete event-log tail.

Work Item 3 is complete only after a capture has reached and followed a real
Zebra Testnet tip and the resulting dataset passes `zsync-dataset validate`.
