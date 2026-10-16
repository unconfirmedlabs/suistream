# suistream

Reliable, ordered SSE stream of Sui checkpoints. Multiple upstream gRPC
`SubscribeCheckpoints` sources fan into core NATS; gateways dedup, gap-fill and
serve `text/event-stream` (proto3 JSON, zstd/gzip). Resumes up to an hour back
are served by the gateway from the public checkpoint bucket, so clients need
no backfill logic of their own.

Public endpoint (Mainnet): `https://stream.mainnet.sui.mirai.cloud/v1/checkpoints`

```
source A ─▶ ingester ─┐
source B ─▶ ingester ─┴─▶ NATS ─▶ gateway ─▶ SSE
                    checkpoint bucket ─┘ (history)
```

## API

`GET /v1/checkpoints`

- `read_mask=<paths>`: comma-separated field paths of `sui.rpc.v2.Checkpoint`
  (default: the full canonical checkpoint). Parent paths mean "everything the
  gateway serves below this path".
- `from=<seq>` or `Last-Event-ID: <seq>` (resume *after* `seq`). Without
  either, the stream starts at the live tip.
- Events: `id: <seq>` / `event: checkpoint` / `data: <proto3 JSON>`;
  `event: gap` / `data: {"from":a,"to":b}` for checkpoints that cannot be
  delivered (no `id`); `: keepalive` comments every 15 s.
- `Accept-Encoding`: zstd, gzip or identity.

### History

A resume older than the gateway's in-memory ring (a few minutes) is served
from the public checkpoint bucket (`checkpoints.mainnet.sui.io`, ~30 days
retention), then from the ring, then live: strictly in order, without
duplicates or holes. The ring is only a cache; the gateway keeps no state
across restarts.

- **Span.** At most `MAX_BACKFILL_CHECKPOINTS` (default 16,000, ≈ 1 h)
  behind the head. Anything older is reported as one `gap` event first, and
  the stream continues from the oldest served checkpoint.
- **Missing objects.** A checkpoint the bucket does not have (404 well behind
  the head) becomes a `gap` event for that range. Near the head a 404 means
  "not uploaded yet" and is retried for up to 30 s.
- **Bucket failures.** If the bucket keeps failing (5xx, timeouts), the stream
  is closed *without* a gap; reconnect with `Last-Event-ID` to retry.
- **Admission.** At most `MAX_BACKFILL_STREAMS` streams per machine backfill
  at once; further resumes that need history get `429` with `Retry-After`.
  Bucket GETs run `BACKFILL_FETCH_CONCURRENCY` (16) ahead per stream;
  rendering runs on a bounded per-machine pool (`BACKFILL_RENDER_CONCURRENCY`,
  default 1, i.e. one vCPU of a shared-cpu-2x), so backfills never hold up
  live streams. A 1 h resume takes 2–3 min to catch up (~5–13 ms CPU per
  checkpoint).
- **Format.** Backfilled checkpoints are rendered like a fullnode renders
  `SubscribeCheckpoints` and go through the same mask/JSON/compression code
  as live ones. Two fields can differ from what a live source sent for the
  same checkpoint: the aggregated checkpoint `signature` (each node keeps its
  own certificate) and the order of `transactions[].signatures` for sponsored
  transactions. Clients must deduplicate by sequence number, never by bytes.
- **Live holes.** If a live checkpoint never arrives and the ingesters cannot
  supply it, the gateway fills it from the bucket before giving up with a
  `gap` event (after which the stream is closed; resuming fills it again from
  the bucket where possible).
- **Cold start.** A gateway serves as soon as NATS is connected. A stream that
  starts before the first live checkpoint arrives waits (with heartbeats);
  resumes are filled from the bucket up to that first checkpoint.
- **Idle exit.** With `IDLE_EXIT_AFTER` set (e.g. `5m`), a gateway without
  streams for that long exits with code 0; the platform starts it again on
  the next request (on Fly: `auto_stop_machines = "off"`,
  `auto_start_machines = true`, restart policy `on-failure`). A cold gateway
  serves its first live checkpoint within about a second.

The canonical mask excludes event/object JSON and object display (sources
render them differently), and the paths fullnodes never fill for checkpoints
(`transactions.objects`, `transactions.transaction_index`,
`transactions.events.events.{checkpoint,transaction_digest,transaction_index,event_index}`);
asking for them explicitly still works and returns them empty, as before.

## Deploying

One image (`Dockerfile`, `--build-arg BIN=ingester|gateway`). You need a core
NATS cluster (`max_payload` ≥ 8 MB, no JetStream), one or more ingesters per
upstream source and any number of gateways. Ingesters read their sources from
`SOURCES_FILE` (default `/etc/checkpoint-stream/sources.toml`; see
`sources.example.toml`), which is not baked into the image. Both read
`NATS_URL` (and optionally `NATS_NKEY_SEED`); see each binary's module docs
for the remaining environment variables.

## Code

Cargo workspace (`rust-toolchain.toml` pins 1.98.1). The server crates use
`sui-types` from the Sui monorepo at tag `mainnet-v1.81.1` and the
`sui-rpc` git revision that tag pins (`788907487b40`), with `tokio =1.52.1`
as the monorepo requires; bump them together. The client SDK uses crates.io
`sui-rpc = "0.4.0"`.

- `crates/common`: subjects, headers, canonical mask, `sources.toml` parsing with
  `${ENV}` expansion, chunking, NATS connection setup, metrics endpoint.
- `crates/ingester`: upstream subscription with make-before-break rotation, ordered
  publishing with `GetCheckpoint` backfill, fetch responder, health beacons.
- `crates/gateway`: receive/dedup, sequencer with gap-fill, ring buffer, SSE with
  shared per-mask/per-encoding rendering, admission limits, history from the
  checkpoint bucket (`history.rs`), idle exit.
- `crates/probe` (not deployed): Phase 0 measurements (`stream`, `sizes`, `epochs`)
  and the end-to-end comparison (`compare`).
- `crates/suistream`, `packages/suistream`: client SDKs (below).

```sh
cargo test && cargo clippy --all-targets
# local run: nats-server -c <(printf 'max_payload: 8MB\n'), then
SOURCES_FILE=sources.example.toml FLY_REGION=local cargo run -p checkpoint-ingester -- mysten
FLY_REGION=local LISTEN_ADDR=127.0.0.1:8080 cargo run -p checkpoint-gateway
# resume from ~1 h back (history from the bucket)
curl -sN "127.0.0.1:8080/v1/checkpoints?read_mask=sequence_number&from=$(( $(curl -s 127.0.0.1:8080/readyz | sed -E 's/.*Some\(([0-9]+)\).*/\1/') - 14000 ))"
# bucket render vs fullnode ListCheckpoints (opt-in; PARITY_N, PARITY_BACK),
# and the near-tip 404 retry against the real bucket
cargo test --release -p checkpoint-gateway parity -- --ignored --nocapture
cargo test -p checkpoint-gateway near_tip_404_against -- --ignored --nocapture
# compare the gateway against a direct upstream subscription for 15 minutes
SOURCES_FILE=sources.example.toml cargo run --release -p checkpoint-probe -- \
  compare http://127.0.0.1:8080/v1/checkpoints mysten 900 zstd
```

`sources.toml` holds hostnames and `${ENV}` secret references only; keep
tokens in the environment.

## SDKs

Thin client libraries (unpublished): `crates/suistream` (Rust) and
`packages/suistream` (`@unconfirmed/suistream`, TypeScript, Bun-first, also
Node). The gateway serves history, so the clients have no backfill logic of
their own: they read the SSE stream over HTTP (not `EventSource`) and keep
it going.

- **Order.** Checkpoints are yielded strictly in sequence order, without
  duplicates, deduplicated by sequence number (the SSE `id`). Anything that
  would skip a checkpoint without a `gap` event is treated as a broken
  connection and reconnected.
- **Gaps.** A `gap` event (`from..=to` cannot be delivered: older than the
  server's history, or lost live) is yielded to the caller as a typed item,
  once. The stream then continues at `to + 1`; after a live gap the server
  closes the connection and the client reconnects after `to`.
- **Resume.** Each (re)connection sends the position as `Last-Event-ID`
  (default) or `?from=` (`resume_with` / `resumeWith`). Start at the live
  tip (default), at a checkpoint (`start_at` / `startAt`), or after one
  (`resume_after` / `resumeAfter`).
- **Position.** `last_delivered` / `lastDelivered`: the last checkpoint
  yielded. `cursor`: everything up to it has been yielded, as a checkpoint or
  inside a gap; persist it and pass it to `resume_after` / `resumeAfter`.
- **Reconnects.** Backoff 1 s doubling to 30 s (reset on progress), at least
  `Retry-After` (e.g. a 429 from backfill admission). No bytes for 45 s
  (heartbeats come every 15 s) means a dead connection
  (`stall_timeout` / `stallTimeoutMs`). The timer only runs while waiting for
  the network: a slow consumer pauses reading (bounded buffers) and is never
  mistaken for a stall.
- **Errors.** Only HTTP 400/401/403/404 (and 410 in TS) end a subscription,
  as its last item (Rust) or thrown/failed (TS). Network errors, 5xx, 429,
  stalls and malformed events are retried internally and reported through
  `tracing` (Rust) or `onDiagnostic` (TS).
- **Compression.** zstd or gzip. Rust (reqwest) offers both. TS offers
  `zstd, gzip` where the runtime's `fetch` decodes zstd (Bun; Node with
  undici ≥ 7.11 and zlib zstd; verified on Node 26), else `gzip`; a custom `fetch`
  gets `gzip` unless `acceptEncoding` is set.

### Rust

Not on crates.io; depend on the repository:

```toml
suistream = { git = "https://github.com/unconfirmedlabs/suistream" }
```

```rust
use futures::StreamExt;
use suistream::{Client, Event};

let mut sub = Client::builder()                       // default: Mainnet stream
    .read_mask(["sequence_number", "digest"])         // default: full canonical checkpoint
    .resume_after(saved_cursor)                       // or .start_at(seq); default: live tip
    .build()?                                         // validates URL and headers
    .subscribe();                                     // impl Stream<Item = Result<Event, Error>>
while let Some(event) = sub.next().await {
    match event? {                                    // an Err is terminal (HTTP 4xx)
        Event::Checkpoint { seq, checkpoint } => {    // sui_rpc::proto::sui::rpc::v2::Checkpoint
            println!("{seq} {:?}", checkpoint.digest);
        }
        Event::Gap { from, to } => eprintln!("missed {from}..={to}"),
    }
    save(sub.cursor());                               // and/or sub.last_delivered()
}
```

Other builder options: `url`, `header` (e.g. `authorization`),
`resume_with(ResumeWith::Query)`, `stall_timeout`. `subscribe()` needs a
Tokio runtime; dropping the `Subscription` closes the connection. Checkpoint
types come from crates.io `sui-rpc` (the crate depends on crates.io only and
stays publishable).

```sh
cargo run --release -p suistream --example tail -- --mask sequence_number,digest --back 500 --secs 120
cargo test -p suistream -- --ignored        # live tests against Mainnet
```

### TypeScript (`packages/suistream`)

Not on npm; install from the repository (the root `package.json` is the
package: Bun runs the TypeScript source, Node gets JS built by `prepare`):

```sh
bun add github:unconfirmedlabs/suistream
npm install github:unconfirmedlabs/suistream
```

Checkpoints are `GrpcTypes.Checkpoint` from `@mysten/sui/grpc` (the type
`SuiGrpcClient` returns: `bigint` numbers, decoded from the proto3 JSON;
~120 KB minified, ~4 ms to decode a full checkpoint).

```ts
import { subscribe } from "@unconfirmed/suistream";

const sub = subscribe({ readMask: ["sequence_number", "digest"], resumeAfter: savedCursor });
for await (const item of sub) {            // throws LiveError only on HTTP 4xx
  if (item.type === "gap") console.warn(`missed ${item.from}..=${item.to}`);
  else console.log(item.seq, item.checkpoint.digest);
  save(sub.cursor);                        // and/or sub.lastDelivered
}
// sub.close() (or break) stops it and closes the connection
```

```ts
// Effect: a Stream<StreamItem, LiveError>; the connection closes with the stream's scope
import { Effect, Stream } from "effect";
import { stream, SuiStream } from "@unconfirmed/suistream";

await Effect.runPromise(
  stream({ readMask: ["sequence_number", "summary.timestamp"] }).pipe(
    Stream.runForEach((item) =>
      item.type === "gap" ? Effect.logWarning(`missed ${item.from}..=${item.to}`) : Effect.log(item.seq),
    ),
  ),
);
// or as a service: SuiStream.stream() with Effect.provide(SuiStream.layer({ ... }))
```

Options: `url`, `readMask`, `startAt` / `resumeAfter`, `headers`,
`stallTimeoutMs`, `resumeWith`, `onDiagnostic` (connect/disconnect events),
`fetch`, `acceptEncoding`.

```sh
bun install
bun run test && bun run typecheck
bun run test:live                      # opt-in live tests against Mainnet
bun examples/tail.ts --mask sequence_number,digest --ago 500 --seconds 120   # also: node examples/tail.ts
```
