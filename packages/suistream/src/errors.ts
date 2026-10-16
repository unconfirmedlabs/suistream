import { Data } from "effect";

/**
 * A connection failed. `fatal` errors (HTTP 400/401/403/404/410: bad
 * request, auth, unknown endpoint) end the subscription; all others are
 * retried internally and only reported through `onDiagnostic`.
 */
export class LiveError extends Data.TaggedError("LiveError")<{
  readonly message: string;
  readonly fatal: boolean;
  readonly status?: number | undefined;
  readonly retryAfterMs?: number | undefined;
  readonly cause?: unknown;
}> {}
