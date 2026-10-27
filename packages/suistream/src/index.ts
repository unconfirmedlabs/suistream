/**
 * `@unconfirmed/suistream`: Sui checkpoints from the suistream SSE gateway,
 * in strict sequence order, without duplicates or silent skips. The gateway
 * serves history (resume) itself; this client parses, orders, deduplicates
 * and reconnects.
 *
 * - Plain: `subscribe(options)` → async iterable of `StreamItem`s, with
 *   `lastDelivered` and `cursor`.
 * - Effect: `stream(options)` → `Stream<StreamItem, LiveError>`, or the
 *   `SuiStream` service via `SuiStream.layer(options)`.
 * - Raw: `subscribeRaw` / `streamRaw`, the same with each checkpoint left as
 *   the SSE `data` string (proto3 JSON, not parsed): `RawStreamItem`s.
 */

import { Context, Effect, Layer, Stream } from "effect";
import { type Diagnostic, drive, type RawStreamItem, type StreamItem } from "./driver.ts";
import { LiveError } from "./errors.ts";
import { resolveMask } from "./mask.ts";
import {
  type CheckpointLike,
  type Decoder,
  decodeEvent,
  decodeRawEvent,
  defaultAcceptEncoding,
  type Fetch,
  type GapEvent,
  httpLiveSource,
} from "./sse.ts";

export type { Diagnostic, RawStreamItem, StreamItem } from "./driver.ts";
export { LiveError } from "./errors.ts";
export type { Checkpoint, Fetch } from "./sse.ts";

export const MAINNET_STREAM_URL = "https://stream.mainnet.sui.mirai.cloud/v1/checkpoints";

export interface SuiStreamOptions {
  /** SSE endpoint. Default: Mainnet. */
  readonly url?: string;
  /** `sui.rpc.v2.Checkpoint` field paths. Default: the full canonical checkpoint. */
  readonly readMask?: readonly string[];
  /** First checkpoint to deliver (inclusive). Default: the live tip. */
  readonly startAt?: bigint | number;
  /** Start after this checkpoint, e.g. a persisted `cursor`. */
  readonly resumeAfter?: bigint | number;
  /** Extra headers for the SSE request (e.g. `authorization`). */
  readonly headers?: Readonly<Record<string, string>>;
  /** Reconnect when no bytes arrive for this long (heartbeats every 15 s). Default 45 000. */
  readonly stallTimeoutMs?: number;
  /** Send the position as the `Last-Event-ID` header (default) or as `?from=`. */
  readonly resumeWith?: "last-event-id" | "query";
  /** Connect/disconnect events, for logging and metrics. */
  readonly onDiagnostic?: (d: Diagnostic) => void;
  /**
   * Custom `fetch`. Its zstd support is unknown, so it gets
   * `Accept-Encoding: gzip` unless `acceptEncoding` says otherwise.
   */
  readonly fetch?: Fetch;
  /** `Accept-Encoding`. Default: `zstd, gzip` where the runtime's `fetch` decodes zstd, else `gzip`. */
  readonly acceptEncoding?: string;
}

const BACKOFF_BASE_MS = 1_000;
const BACKOFF_MAX_MS = 30_000;

const startAt = (o: SuiStreamOptions): bigint | undefined => {
  if (o.startAt !== undefined && o.resumeAfter !== undefined) {
    throw new TypeError("set at most one of `startAt` and `resumeAfter`");
  }
  if (o.startAt !== undefined) return BigInt(o.startAt);
  if (o.resumeAfter !== undefined) return BigInt(o.resumeAfter) + 1n;
  return undefined;
};

/** A running subscription for code that does not use Effect. */
export interface Subscription<T = StreamItem> extends AsyncIterable<T> {
  /** Sequence number of the last checkpoint yielded. */
  readonly lastDelivered: bigint | undefined;
  /**
   * Everything up to and including this checkpoint has been yielded, as a
   * checkpoint or inside a gap. Persist it and pass it as `resumeAfter` to
   * continue later.
   */
  readonly cursor: bigint | undefined;
  /** Stop (also happens on `break` out of `for await`). Iterable once. */
  close(): Promise<void>;
}

/** Shared by `subscribe` and `subscribeRaw`, which differ only in how checkpoints are decoded. */
const open = <C extends CheckpointLike>(options: SuiStreamOptions, decode: Decoder<C>): Subscription<C | GapEvent> => {
  const start = startAt(options);
  const stallTimeoutMs = options.stallTimeoutMs ?? 45_000;
  if (!(stallTimeoutMs > 0)) throw new TypeError("`stallTimeoutMs` must be positive");
  const ctrl = new AbortController();
  const items = drive(
    {
      live: httpLiveSource(
        {
          url: new URL(options.url ?? MAINNET_STREAM_URL).toString(),
          readMask: resolveMask(options.readMask),
          headers: options.headers,
          acceptEncoding: options.acceptEncoding ?? (options.fetch ? "gzip" : defaultAcceptEncoding()),
          stallTimeoutMs,
          fetch: options.fetch,
          resumeWith: options.resumeWith,
        },
        decode,
      ),
      startAt: start,
      onDiagnostic: options.onDiagnostic,
      backoffBaseMs: BACKOFF_BASE_MS,
      backoffMaxMs: BACKOFF_MAX_MS,
    },
    ctrl.signal,
  );
  let lastDelivered: bigint | undefined;
  let cursor = start !== undefined && start > 0n ? start - 1n : undefined;
  // Abort first: wakes a pending read, so `return` cannot wait on the network.
  const close = async () => {
    ctrl.abort();
    await items.return(undefined);
  };
  const iterator: AsyncIterator<C | GapEvent> = {
    async next() {
      const r = await items.next();
      if (!r.done) {
        if (r.value.type === "checkpoint") lastDelivered = cursor = r.value.seq;
        else cursor = r.value.to;
      }
      return r;
    },
    async return() {
      await close();
      return { done: true, value: undefined };
    },
  };
  return {
    get lastDelivered() {
      return lastDelivered;
    },
    get cursor() {
      return cursor;
    },
    close,
    [Symbol.asyncIterator]: () => iterator,
  };
};

/**
 * Checkpoints and gaps in order, as an async iterable. A fatal error (bad
 * request, auth, unknown endpoint) is thrown as a `LiveError`; everything
 * else is retried internally. Invalid options throw a `TypeError`.
 */
export const subscribe = (options: SuiStreamOptions = {}): Subscription => open(options, decodeEvent);

/**
 * `subscribe`, with each checkpoint as the SSE `data` string, not parsed
 * (for forwarding or storing the JSON as is). Same order, dedup, gaps,
 * resume and errors.
 */
export const subscribeRaw = (options: SuiStreamOptions = {}): Subscription<RawStreamItem> => open(options, decodeRawEvent);

const toStream = <T>(subscription: () => Subscription<T>): Stream.Stream<T, LiveError> =>
  Stream.suspend(() =>
    Stream.fromAsyncIterable(subscription(), (e) =>
      e instanceof LiveError ? e : new LiveError({ message: String(e), fatal: true, cause: e }),
    ),
  );

/**
 * Checkpoints and gaps in order, as an Effect `Stream` (one subscription per
 * run, closed with the stream's scope). Only fatal errors fail it; invalid
 * options are a defect.
 */
export const stream = (options: SuiStreamOptions = {}): Stream.Stream<StreamItem, LiveError> =>
  toStream(() => subscribe(options));

/** `stream`, with each checkpoint as the unparsed SSE `data` string (see `subscribeRaw`). */
export const streamRaw = (options: SuiStreamOptions = {}): Stream.Stream<RawStreamItem, LiveError> =>
  toStream(() => subscribeRaw(options));

/** Effect service for apps that configure the stream once via a `Layer`. */
export class SuiStream extends Context.Service<
  SuiStream,
  { readonly stream: (overrides?: SuiStreamOptions) => Stream.Stream<StreamItem, LiveError> }
>()("@unconfirmed/suistream/SuiStream") {
  static readonly layer = (options: SuiStreamOptions = {}): Layer.Layer<SuiStream> =>
    Layer.succeed(SuiStream, { stream: (overrides) => stream({ ...options, ...overrides }) });

  /** `Stream` of items from the `SuiStream` in context. */
  static readonly stream = (overrides?: SuiStreamOptions): Stream.Stream<StreamItem, LiveError, SuiStream> =>
    Stream.unwrap(
      Effect.gen(function* () {
        const service = yield* SuiStream;
        return service.stream(overrides);
      }),
    );
}
