/**
 * The client loop: one SSE connection at a time, resumed after the cursor,
 * with capped exponential backoff. The gateway serves history itself, so the
 * client only orders, deduplicates and reconnects.
 *
 * Cursor: the last sequence number delivered, as a checkpoint or as the end
 * of a gap. Connections resume after it, so replays are dropped and a gap is
 * reported once. Anything that would skip past the cursor without a `gap`
 * (a protocol violation) is treated as a broken connection: reconnect.
 */

import { LiveError } from "./errors.ts";
import type { Checkpoint, LiveSource, ResumeAfter } from "./sse.ts";

/** What a subscription yields, in strict sequence order. */
export type StreamItem =
  | { readonly type: "checkpoint"; readonly seq: bigint; readonly checkpoint: Checkpoint }
  /** Checkpoints `from..=to` cannot be delivered (older than the server's history, or lost). */
  | { readonly type: "gap"; readonly from: bigint; readonly to: bigint };

/** Connection lifecycle, for logging and metrics. */
export type Diagnostic =
  | { readonly type: "connect"; readonly after: ResumeAfter }
  | { readonly type: "disconnect"; readonly reason: string; readonly retryInMs: number };

export interface DriverConfig {
  readonly live: LiveSource;
  /** First checkpoint to deliver; undefined → start at the live tip. */
  readonly startAt: bigint | undefined;
  readonly onDiagnostic?: ((d: Diagnostic) => void) | undefined;
  /** Reconnect backoff: starts here, doubles up to `backoffMaxMs`, resets on progress. */
  readonly backoffBaseMs: number;
  readonly backoffMaxMs: number;
}

const sleep = (ms: number, signal: AbortSignal) =>
  new Promise<void>((resolve) => {
    if (signal.aborted) return resolve();
    const t = setTimeout(done, ms);
    function done() {
      clearTimeout(t);
      signal.removeEventListener("abort", done);
      resolve();
    }
    signal.addEventListener("abort", done, { once: true });
  });

/** Ordered, deduplicated items until `signal` aborts or a fatal error is thrown. */
export async function* drive(cfg: DriverConfig, signal: AbortSignal): AsyncGenerator<StreamItem> {
  // -1n = "from genesis" (the HTTP source sends `from=0`).
  let cursor: ResumeAfter = cfg.startAt === undefined ? undefined : cfg.startAt - 1n;
  let failures = 0;
  while (!signal.aborted) {
    cfg.onDiagnostic?.({ type: "connect", after: cursor });
    let reason = "stream closed";
    let retryAfterMs = 0;
    try {
      for await (const ev of cfg.live.connect(cursor, signal)) {
        const expected = cursor === undefined ? undefined : cursor + 1n;
        if (ev.type === "checkpoint") {
          if (expected !== undefined && ev.seq < expected) continue; // replayed
          if (expected !== undefined && ev.seq > expected) {
            reason = `checkpoint ${ev.seq} skips ${expected}`;
            break;
          }
          failures = 0;
          cursor = ev.seq;
          yield ev;
        } else {
          if (expected !== undefined && ev.to < expected) continue; // already reported
          const from = expected !== undefined && ev.from < expected ? expected : ev.from;
          if (expected !== undefined && from > expected) {
            reason = `gap ${from}..=${ev.to} skips ${expected}`;
            break;
          }
          failures = 0;
          cursor = ev.to;
          yield { type: "gap", from, to: ev.to };
        }
      }
    } catch (error) {
      if (signal.aborted) return;
      if (error instanceof LiveError && error.fatal) throw error;
      reason = error instanceof Error ? error.message : String(error);
      retryAfterMs = (error instanceof LiveError && error.retryAfterMs) || 0;
    }
    if (signal.aborted) return;
    const wait = Math.max(Math.min(cfg.backoffBaseMs * 2 ** failures, cfg.backoffMaxMs), retryAfterMs);
    failures++;
    cfg.onDiagnostic?.({ type: "disconnect", reason, retryInMs: wait });
    await sleep(wait, signal);
  }
}
