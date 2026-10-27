/**
 * `GET /v1/checkpoints` read with `fetch` streaming (not `EventSource`, which
 * hides `gap` semantics and resume control).
 */

import { GrpcTypes } from "@mysten/sui/grpc";
import { LiveError } from "./errors.ts";

export type Checkpoint = GrpcTypes.Checkpoint;

export type GapEvent = { readonly type: "gap"; readonly from: bigint; readonly to: bigint };
/** A checkpoint event as the driver needs it: ordered and deduplicated by `seq`. */
export type CheckpointLike = { readonly type: "checkpoint"; readonly seq: bigint };
export type CheckpointEvent = { readonly type: "checkpoint"; readonly seq: bigint; readonly checkpoint: Checkpoint };
/** A checkpoint left as the SSE `data` payload (proto3 JSON, unparsed). */
export type RawCheckpointEvent = { readonly type: "checkpoint"; readonly seq: bigint; readonly data: string };

/** One event of the stream. */
export type LiveEvent = CheckpointEvent | GapEvent;
/** One event of the stream, checkpoints unparsed. */
export type RawLiveEvent = RawCheckpointEvent | GapEvent;

/**
 * Where a connection starts: after `seq` (`Last-Event-ID`), at genesis
 * (`-1n`, sent as `from=0`), or at the tip (`undefined`).
 */
export type ResumeAfter = bigint | undefined;

/** Opens connections. Replaceable for tests or custom transports. */
export interface LiveSource<C extends CheckpointLike = CheckpointEvent> {
  connect(after: ResumeAfter, signal: AbortSignal): AsyncIterable<C | GapEvent>;
}

export interface RawSseEvent {
  readonly id: string | undefined;
  readonly event: string;
  readonly data: string;
}

/**
 * Incremental `text/event-stream` parser (WHATWG rules for the fields used
 * here: `id`, `event`, `data`, `retry`, comments; CR/LF/CRLF line endings).
 *
 * Full checkpoints are ~1 MB data lines spread over many chunks, so each
 * chunk is scanned once and partial lines are kept as pieces, joined only
 * when the line ends (no rescanning or flattening of a growing buffer).
 */
export class SseParser {
  private pieces: string[] = [];
  private pendingCr = false;
  private id: string | undefined;
  private event = "";
  private data: string[] = [];
  /** Last `retry:` value in milliseconds (informational). */
  retry: number | undefined;

  /** Feed decoded text; returns the events completed by it. */
  push(text: string): RawSseEvent[] {
    const out: RawSseEvent[] = [];
    if (text === "") return out;
    let pos = 0;
    if (this.pendingCr && text.startsWith("\n")) pos = 1;
    this.pendingCr = false;
    let lf = -1;
    let cr = -1;
    while (pos < text.length) {
      if (lf < pos) lf = text.indexOf("\n", pos);
      if (cr < pos) cr = text.indexOf("\r", pos);
      if (lf < 0) lf = text.length;
      if (cr < 0) cr = text.length;
      const end = Math.min(lf, cr);
      if (end === text.length) break;
      const tail = text.slice(pos, end);
      const line = this.pieces.length === 0 ? tail : this.pieces.join("") + tail;
      this.pieces = [];
      this.line(line, out);
      pos = end + 1;
      if (end === cr) {
        if (pos === text.length) this.pendingCr = true;
        else if (text.charCodeAt(pos) === 10) pos++;
      }
    }
    if (pos < text.length) this.pieces.push(text.slice(pos));
    return out;
  }

  private line(line: string, out: RawSseEvent[]): void {
    if (line === "") {
      if (this.data.length > 0) {
        out.push({ id: this.id, event: this.event || "message", data: this.data.join("\n") });
      }
      this.event = "";
      this.data = [];
      return;
    }
    if (line.startsWith(":")) return;
    const colon = line.indexOf(":");
    const field = colon < 0 ? line : line.slice(0, colon);
    let value = colon < 0 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    switch (field) {
      case "id":
        if (!value.includes("\0")) this.id = value;
        break;
      case "event":
        this.event = value;
        break;
      case "data":
        this.data.push(value);
        break;
      case "retry":
        if (/^\d+$/.test(value)) this.retry = Number(value);
        break;
    }
  }
}

/** A malformed event is a broken connection (retried), like in the Rust SDK. */
const bad = (message: string, cause?: unknown) => new LiveError({ message, fatal: false, cause });

const parseSeq = (s: string | undefined, what: string): bigint => {
  if (s === undefined || !/^\d+$/.test(s)) throw bad(`bad ${what}: ${s}`);
  return BigInt(s);
};

/** Turns one raw event into an event, or `undefined` for event types this SDK ignores. */
export type Decoder<C extends CheckpointLike> = (raw: RawSseEvent) => C | GapEvent | undefined;

/** Shared by both modes: the checkpoint `id` is validated, gaps are decoded the same way. */
const decoder =
  <C extends CheckpointLike>(checkpoint: (seq: bigint, data: string) => C): Decoder<C> =>
  (raw) => {
    switch (raw.event) {
      case "checkpoint":
        return checkpoint(parseSeq(raw.id, "event id"), raw.data);
      case "gap": {
        let parsed: { from?: unknown; to?: unknown };
        try {
          parsed = JSON.parse(raw.data);
        } catch (cause) {
          throw bad(`undecodable gap event: ${raw.data}`, cause);
        }
        return { type: "gap", from: parseSeq(String(parsed.from), "gap.from"), to: parseSeq(String(parsed.to), "gap.to") };
      }
      default:
        return undefined;
    }
  };

/** Decode one raw event, checkpoints as `GrpcTypes.Checkpoint`. */
export const decodeEvent: Decoder<CheckpointEvent> = decoder((seq, data) => {
  let checkpoint: Checkpoint;
  try {
    checkpoint = GrpcTypes.Checkpoint.fromJsonString(data, { ignoreUnknownFields: true });
  } catch (cause) {
    throw bad(`checkpoint ${seq}: undecodable JSON`, cause);
  }
  if (checkpoint.sequenceNumber !== undefined && checkpoint.sequenceNumber !== seq) {
    throw bad(`event id ${seq} != sequenceNumber ${checkpoint.sequenceNumber}`);
  }
  return { type: "checkpoint", seq, checkpoint };
});

/** Decode one raw event, checkpoints left as the `data` string (never parsed). */
export const decodeRawEvent: Decoder<RawCheckpointEvent> = decoder((seq, data) => ({ type: "checkpoint", seq, data }));

const atLeast = (version: string, major: number, minor: number) => {
  const [a = 0, b = 0] = version.split(".").map(Number);
  return a > major || (a === major && b >= minor);
};

/**
 * `Accept-Encoding` for the global `fetch` of this runtime: `zstd, gzip`
 * where `fetch` decodes zstd (Bun; Node with undici ≥ 7.11 and zlib zstd),
 * else `gzip`. Undici passes an encoding it cannot decode through as raw
 * bytes, so this must not over-claim.
 */
export const defaultAcceptEncoding = (): string => {
  const g = globalThis as {
    Bun?: { zstdDecompressSync?: unknown };
    process?: { versions?: Record<string, string | undefined>; getBuiltinModule?: (id: string) => unknown };
  };
  if (g.Bun) return typeof g.Bun.zstdDecompressSync === "function" ? "zstd, gzip" : "gzip";
  const undici = g.process?.versions?.undici;
  const zlib = g.process?.getBuiltinModule?.("node:zlib") as { createZstdDecompress?: unknown } | undefined;
  if (undici && atLeast(undici, 7, 11) && typeof zlib?.createZstdDecompress === "function") return "zstd, gzip";
  return "gzip";
};

/** The subset of `fetch` used here. */
export type Fetch = (input: URL, init: RequestInit) => Promise<Response>;

export interface HttpLiveOptions {
  readonly url: string;
  /** `read_mask` paths, or undefined for the full canonical checkpoint. */
  readonly readMask: readonly string[] | undefined;
  readonly headers?: Readonly<Record<string, string>> | undefined;
  /** Sent as `Accept-Encoding`. */
  readonly acceptEncoding: string;
  /** No bytes (heartbeats included) for this long → reconnect. */
  readonly stallTimeoutMs: number;
  readonly fetch?: Fetch | undefined;
  /**
   * How a connection says where to resume: the `Last-Event-ID` header
   * (default), or the equivalent `from=<seq + 1>` query parameter.
   */
  readonly resumeWith?: "last-event-id" | "query" | undefined;
}

const FATAL_STATUSES = new Set([400, 401, 403, 404, 410]);

/** The SSE source over HTTP; `decode` picks typed (default) or raw checkpoints. */
export const httpLiveSource = <C extends CheckpointLike = CheckpointEvent>(
  options: HttpLiveOptions,
  // The default is only used with the default `C`.
  decode: Decoder<C> = decodeEvent as unknown as Decoder<C>,
): LiveSource<C> => ({
  async *connect(after, signal) {
    const url = new URL(options.url);
    if (options.readMask) url.searchParams.set("read_mask", options.readMask.join(","));
    const headers: Record<string, string> = {
      accept: "text/event-stream",
      "accept-encoding": options.acceptEncoding,
      ...options.headers,
    };
    if (after === -1n) url.searchParams.set("from", "0");
    else if (after !== undefined) {
      if (options.resumeWith === "query") url.searchParams.set("from", (after + 1n).toString());
      else headers["last-event-id"] = after.toString();
    }

    // One controller per connection: aborted by the caller or by a stall.
    const ctrl = new AbortController();
    const onAbort = () => ctrl.abort(signal.reason);
    if (signal.aborted) onAbort();
    else signal.addEventListener("abort", onAbort, { once: true });
    // The stall timer runs only while waiting for the network (headers or
    // the next chunk), never while the consumer holds an event.
    let stall: ReturnType<typeof setTimeout> | undefined;
    const armStall = () => {
      clearTimeout(stall);
      stall = setTimeout(
        () => ctrl.abort(new LiveError({ message: `no data for ${options.stallTimeoutMs} ms`, fatal: false })),
        options.stallTimeoutMs,
      );
    };
    const failure = (what: string, cause: unknown) =>
      ctrl.signal.reason instanceof LiveError
        ? ctrl.signal.reason
        : new LiveError({ message: `${what}: ${String(cause)}`, fatal: false, cause });
    try {
      armStall();
      let response: Response;
      try {
        response = await (options.fetch ?? fetch)(url, { headers, signal: ctrl.signal });
      } catch (cause) {
        throw failure("connect failed", cause);
      }
      if (!response.ok) {
        const body = (await response.text().catch(() => "")).slice(0, 500);
        const retryAfter = Number(response.headers.get("retry-after"));
        throw new LiveError({
          message: `HTTP ${response.status}: ${body}`,
          fatal: FATAL_STATUSES.has(response.status),
          status: response.status,
          retryAfterMs: Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1000 : undefined,
        });
      }
      if (!response.body) throw new LiveError({ message: "response has no body", fatal: false });
      const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
      // Do not rely on the body erroring when the request is aborted:
      // cancelling the reader settles a pending read. (Never `Promise.race`
      // against a long-lived promise: every race retains its chunk.)
      const cancel = () => void reader.cancel(ctrl.signal.reason).catch(() => {});
      if (ctrl.signal.aborted) cancel();
      else ctrl.signal.addEventListener("abort", cancel, { once: true });
      const parser = new SseParser();
      try {
        for (;;) {
          armStall();
          let chunk: Awaited<ReturnType<typeof reader.read>>;
          try {
            chunk = await reader.read();
          } catch (cause) {
            throw failure("read failed", cause);
          }
          clearTimeout(stall);
          if (ctrl.signal.aborted) throw failure("aborted", ctrl.signal.reason);
          if (chunk.done) return;
          for (const raw of parser.push(chunk.value)) {
            const event = decode(raw);
            if (event) yield event;
          }
        }
      } finally {
        cancel();
      }
    } finally {
      clearTimeout(stall);
      signal.removeEventListener("abort", onAbort);
      ctrl.abort();
    }
  },
});
