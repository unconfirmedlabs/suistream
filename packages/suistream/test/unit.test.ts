import { afterAll, describe, expect, test } from "bun:test";
import { Effect, Stream } from "effect";
import { type Diagnostic, drive, type StreamItem } from "../src/driver.ts";
import { LiveError, stream, SuiStream, subscribe } from "../src/index.ts";
import { resolveMask } from "../src/mask.ts";
import { decodeEvent, defaultAcceptEncoding, type Fetch, httpLiveSource, type LiveEvent, type LiveSource, SseParser } from "../src/sse.ts";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const label = (i: StreamItem) => (i.type === "gap" ? `gap ${i.from}-${i.to}` : Number(i.seq));

describe("mask", () => {
  test("resolve", () => {
    expect(resolveMask()).toBeUndefined();
    expect(resolveMask([" ", ""])).toBeUndefined();
    expect(resolveMask(["digest", "*"])).toBeUndefined();
    expect(resolveMask([" digest ", "digest", "summary"])).toEqual(["digest", "summary"]);
  });
});

describe("sse parser", () => {
  const stream =
    'retry: 1000\n\nid: 5\nevent: checkpoint\ndata: {"a":1}\n\n: keepalive\n\nevent: gap\r\ndata: {"from":1,"to":2}\r\n\r\nid: 6\revent: checkpoint\rdata: x\rdata:y\r\rdata\n\n';
  const expected = [
    { id: "5", event: "checkpoint", data: '{"a":1}' },
    { id: "5", event: "gap", data: '{"from":1,"to":2}' },
    { id: "6", event: "checkpoint", data: "x\ny" },
    { id: "6", event: "message", data: "" },
  ];
  const parse = (chunks: string[]) => {
    const p = new SseParser();
    const out = chunks.flatMap((c) => p.push(c));
    return { out, retry: p.retry };
  };

  test("every split point, byte by byte; CR, LF, CRLF; comments; multi-line data", () => {
    const whole = parse([stream]);
    expect(whole.out).toEqual(expected);
    expect(whole.retry).toBe(1000);
    for (let i = 0; i <= stream.length; i++) {
      expect(parse([stream.slice(0, i), "", stream.slice(i)]).out).toEqual(expected);
    }
    expect(parse([...stream]).out).toEqual(expected);
  });

  test("CRLF split across chunks is one line ending", () => {
    const p = new SseParser();
    expect(p.push("data: a\r")).toEqual([]);
    expect(p.push("\n\r")).toEqual([{ id: undefined, event: "message", data: "a" }]);
    expect(p.push("\ndata: b\n\n")).toEqual([{ id: undefined, event: "message", data: "b" }]);
  });

  test("an event without data is not dispatched; a ~1 MB line in small chunks", () => {
    expect(parse(["id: 1\nevent: checkpoint\n\n"]).out).toEqual([]);
    const big = "x".repeat(1 << 20);
    const text = `id: 9\nevent: checkpoint\ndata: ${big}\n\n`;
    const chunks: string[] = [];
    for (let i = 0; i < text.length; i += 1000) chunks.push(text.slice(i, i + 1000));
    const { out } = parse(chunks);
    expect(out.length).toBe(1);
    expect(out[0]!.data.length).toBe(big.length);
  });

  test("decode checkpoint and gap; malformed events are retryable errors", () => {
    const ev = decodeEvent({
      id: "334000000",
      event: "checkpoint",
      data: '{"sequenceNumber":"334000000","digest":"abc","summary":{"epoch":"1200","timestamp":"2026-10-14T00:00:00Z"},"futureField":1}',
    });
    if (ev?.type !== "checkpoint") throw new Error("not a checkpoint");
    expect(ev.seq).toBe(334000000n);
    expect(ev.checkpoint.sequenceNumber).toBe(334000000n);
    expect(ev.checkpoint.summary?.epoch).toBe(1200n);
    expect(decodeEvent({ id: undefined, event: "gap", data: '{"from":10,"to":20}' })).toEqual({ type: "gap", from: 10n, to: 20n });
    expect(decodeEvent({ id: undefined, event: "other", data: "x" })).toBeUndefined();
    for (const raw of [
      { id: "1", event: "checkpoint", data: '{"sequenceNumber":"2"}' },
      { id: undefined, event: "checkpoint", data: "{}" },
      { id: "1", event: "checkpoint", data: "{" },
      { id: undefined, event: "gap", data: '{"from":1}' },
    ]) {
      let error: unknown;
      try {
        decodeEvent(raw);
      } catch (e) {
        error = e;
      }
      expect(error).toBeInstanceOf(LiveError);
      expect((error as LiveError).fatal).toBe(false);
    }
  });

  test("zstd is requested where fetch decodes it", () => {
    expect(defaultAcceptEncoding()).toBe("zstd, gzip");
  });
});

// ---- driver over a scripted source ----

/** Each connection replays one script entry: events, then end / throw; then hang. */
type Step = { events: LiveEvent[]; error?: LiveError };
const cpEv = (seq: number): LiveEvent => ({
  type: "checkpoint",
  seq: BigInt(seq),
  checkpoint: { sequenceNumber: BigInt(seq), digest: `d${seq}`, transactions: [] },
});
const gapEv = (from: number, to: number): LiveEvent => ({ type: "gap", from: BigInt(from), to: BigInt(to) });
const scripted = (steps: Step[], connects: (string | undefined)[]): LiveSource => ({
  async *connect(after, signal) {
    connects.push(after?.toString());
    const step = steps.shift();
    if (!step) {
      await new Promise((r) => signal.addEventListener("abort", r, { once: true }));
      return;
    }
    for (const e of step.events) yield e;
    if (step.error) throw step.error;
  },
});
const run = async (live: LiveSource, n: number, startAt?: number, onDiagnostic?: (d: Diagnostic) => void) => {
  const ctrl = new AbortController();
  const out: (string | number)[] = [];
  const cfg = { live, startAt: startAt === undefined ? undefined : BigInt(startAt), onDiagnostic, backoffBaseMs: 5, backoffMaxMs: 30 };
  for await (const item of drive(cfg, ctrl.signal)) {
    out.push(label(item));
    if (out.length >= n) break;
  }
  return out;
};

describe("driver", () => {
  test("tip start, in order; replays after reconnect are dropped", async () => {
    const connects: (string | undefined)[] = [];
    const src = scripted(
      [
        { events: [cpEv(10), cpEv(11)] },
        { events: [cpEv(11), cpEv(12)], error: new LiveError({ message: "reset", fatal: false }) },
        { events: [cpEv(12), cpEv(13)] },
      ],
      connects,
    );
    expect(await run(src, 4)).toEqual([10, 11, 12, 13]);
    expect(connects).toEqual([undefined, "11", "12"]);
  });

  test("start positions: startAt n → after n-1; 0 → genesis (-1)", async () => {
    const connects: (string | undefined)[] = [];
    await run(scripted([{ events: [cpEv(5)] }], connects), 1, 5);
    await run(scripted([{ events: [cpEv(0)] }], connects), 1, 0);
    expect(connects).toEqual(["4", "-1"]);
  });

  test("gaps are surfaced once and move the cursor (live gap closes the stream)", async () => {
    const connects: (string | undefined)[] = [];
    const src = scripted(
      [
        { events: [gapEv(100, 149), cpEv(150)] },
        // Replayed gap (partly covered) and checkpoint after a reconnect;
        // then a live gap, after which the server closes.
        { events: [gapEv(120, 149), cpEv(150), cpEv(151), gapEv(152, 153)] },
        { events: [gapEv(152, 153), cpEv(154)] },
      ],
      connects,
    );
    expect(await run(src, 5, 100)).toEqual(["gap 100-149", 150, 151, "gap 152-153", 154]);
    expect(connects).toEqual(["99", "150", "153"]);
  });

  test("a gap overlapping the cursor is trimmed", async () => {
    expect(await run(scripted([{ events: [cpEv(10), gapEv(5, 12), cpEv(13)] }], []), 3)).toEqual([10, "gap 11-12", 13]);
  });

  test("a silent skip is never delivered: reconnect from the cursor", async () => {
    const connects: (string | undefined)[] = [];
    const diags: Diagnostic[] = [];
    const src = scripted([{ events: [cpEv(10), cpEv(12)] }, { events: [cpEv(11), cpEv(12)] }], connects);
    expect(await run(src, 3, undefined, (d) => diags.push(d))).toEqual([10, 11, 12]);
    expect(connects).toEqual([undefined, "10"]);
    expect(diags.some((d) => d.type === "disconnect" && d.reason.includes("skips 11"))).toBe(true);
  });

  test("backoff doubles while failing, is capped, resets on progress, honours Retry-After", async () => {
    const diags: Diagnostic[] = [];
    const fail = () => ({ events: [], error: new LiveError({ message: "down", fatal: false }) });
    const busy = { events: [], error: new LiveError({ message: "429", fatal: false, retryAfterMs: 50 }) };
    const src = scripted([fail(), fail(), fail(), fail(), { events: [cpEv(1)] }, busy, { events: [cpEv(2)] }], []);
    await run(src, 2, undefined, (d) => diags.push(d));
    const waits = diags.flatMap((d) => (d.type === "disconnect" ? [d.retryInMs] : []));
    // 4 failures (5, 10, 20, 30 capped), clean close after progress (5), 429 (max(10, 50)).
    expect(waits).toEqual([5, 10, 20, 30, 5, 50]);
  });

  test("fatal errors end the subscription", async () => {
    const src = scripted([{ events: [cpEv(1)], error: new LiveError({ message: "HTTP 400", fatal: true, status: 400 }) }], []);
    const out: (string | number)[] = [];
    let error: unknown;
    try {
      for await (const item of drive({ live: src, startAt: undefined, backoffBaseMs: 1, backoffMaxMs: 1 }, new AbortController().signal)) {
        out.push(label(item));
      }
    } catch (e) {
      error = e;
    }
    expect(out).toEqual([1]);
    expect((error as LiveError).status).toBe(400);
  });
});

// ---- end to end against a local SSE server ----

describe("mock server (Bun.serve)", () => {
  type Handler = (req: Request, n: number) => Response | Promise<Response>;
  let handler: Handler = () => new Response("unset", { status: 500 });
  /** Copies: Bun recycles `Request` objects after the handler returns. */
  let requests: { url: string; headers: Headers; aborted: boolean }[] = [];
  const aborted = () => requests.filter((r) => r.aborted).length;
  const server = Bun.serve({
    port: 0,
    idleTimeout: 0,
    fetch(req) {
      const r = { url: req.url, headers: new Headers(req.headers), aborted: false };
      // Each test has its own path, so a late request from an earlier test
      // (which would be a leak) is caught instead of confusing this one.
      if (new URL(req.url).pathname !== new URL(url).pathname) {
        strays.push(req.url);
        console.error("STRAY", req.url, req.headers.get("last-event-id"), "during", url);
        return new Response("stray", { status: 500 });
      }
      requests.push(r);
      req.signal.addEventListener("abort", () => (r.aborted = true));
      return handler(req, requests.length);
    },
  });
  afterAll(() => {
    server.stop(true);
    expect(strays).toEqual([]);
  });
  const strays: string[] = [];
  let testNo = 0;
  let url = "";
  const enc = new TextEncoder();
  const ev = (seq: number) => `id: ${seq}\nevent: checkpoint\ndata: {"sequenceNumber":"${seq}","digest":"d${seq}"}\n\n`;
  const gap = (from: number, to: number) => `event: gap\ndata: {"from":${from},"to":${to}}\n\n`;
  /** An SSE body: the chunks, then either close or stay open without heartbeats. */
  const sse = (chunks: string[], hold = false) =>
    new Response(
      new ReadableStream<Uint8Array>({
        start(c) {
          c.enqueue(enc.encode("retry: 1000\n\n"));
          for (const ch of chunks) c.enqueue(enc.encode(ch));
          if (!hold) c.close();
        },
      }),
      { headers: { "content-type": "text/event-stream" } },
    );
  const reset = (h: Handler) => {
    handler = h;
    requests = [];
    url = `http://127.0.0.1:${server.port}/t${++testNo}/v1/checkpoints`;
  };
  const collect = async (n: number, opts: Parameters<typeof subscribe>[0] = {}) => {
    const sub = subscribe({ url, ...opts });
    const out: (string | number)[] = [];
    for await (const item of sub) {
      out.push(label(item));
      if (out.length >= n) break;
    }
    return { out, last: sub.lastDelivered, cursor: sub.cursor };
  };

  test("resume with Last-Event-ID across closes; mask and encoding sent; gaps surfaced; cursor", async () => {
    reset((_, n) => {
      if (n === 1) return sse([gap(90, 99), ev(100), ": keepalive\n\n", ev(101)]);
      return sse([ev(101), ev(102), gap(103, 104)]);
    });
    const { out, last, cursor } = await collect(5, { startAt: 90, readMask: ["digest"] });
    expect(out).toEqual(["gap 90-99", 100, 101, 102, "gap 103-104"]);
    expect(last).toBe(102n);
    expect(cursor).toBe(104n);
    expect(requests[0]!.headers.get("last-event-id")).toBe("89");
    expect(requests[1]!.headers.get("last-event-id")).toBe("101");
    expect(new URL(requests[0]!.url).searchParams.get("read_mask")).toBe("digest");
    expect(requests[0]!.headers.get("accept-encoding")).toBe("zstd, gzip");
  });

  test("resumeWith query; from=0; tip start; custom fetch gets gzip", async () => {
    reset(() => sse([ev(7)]));
    await collect(1, { resumeAfter: 6, resumeWith: "query" });
    expect(new URL(requests[0]!.url).searchParams.get("from")).toBe("7");
    expect(requests[0]!.headers.get("last-event-id")).toBeNull();
    reset(() => sse([ev(0)]));
    await collect(1, { startAt: 0, fetch: (i, init) => fetch(i, init) });
    expect(new URL(requests[0]!.url).searchParams.get("from")).toBe("0");
    expect(requests[0]!.headers.get("accept-encoding")).toBe("gzip");
    reset(() => sse([ev(7)]));
    await collect(1);
    expect(new URL(requests[0]!.url).search).toBe("");
    expect(requests[0]!.headers.get("last-event-id")).toBeNull();
    expect(() => subscribe({ startAt: 1, resumeAfter: 2 })).toThrow(TypeError);
    expect(() => subscribe({ stallTimeoutMs: 0 })).toThrow(TypeError);
    expect(() => subscribe({ url: "not a url" })).toThrow(TypeError);
  });

  test("429 with Retry-After is honoured, then succeeds", async () => {
    reset((_, n) =>
      n === 1 ? new Response("backfill limit reached", { status: 429, headers: { "retry-after": "2" } }) : sse([ev(5)]),
    );
    const t0 = Date.now();
    expect((await collect(1, { startAt: 5 })).out).toEqual([5]);
    expect(Date.now() - t0).toBeGreaterThanOrEqual(1950);
    expect(requests.length).toBe(2);
  });

  test("400 is terminal", async () => {
    reset(() => new Response("invalid read_mask path", { status: 400 }));
    let error: unknown;
    try {
      await collect(1, { readMask: ["nope"] });
    } catch (e) {
      error = e;
    }
    expect(error).toBeInstanceOf(LiveError);
    expect((error as LiveError).status).toBe(400);
    expect((error as LiveError).message).toContain("invalid read_mask");
    expect(requests.length).toBe(1);
  });

  test("stall: no bytes → reconnect from the cursor", async () => {
    reset((_, n) => (n === 1 ? sse([ev(1)], true) : sse([ev(2)], true)));
    const t0 = Date.now();
    const { out } = await collect(2, { stallTimeoutMs: 300 });
    expect(out).toEqual([1, 2]);
    expect(Date.now() - t0).toBeGreaterThanOrEqual(1280); // stall + 1 s backoff
    expect(requests[1]!.headers.get("last-event-id")).toBe("1");
  });

  test("a slow consumer is not a stall", async () => {
    reset(() => sse([ev(1), ev(2), ev(3)], true));
    const sub = subscribe({ url, stallTimeoutMs: 100 });
    const out = [];
    for await (const item of sub) {
      out.push(label(item));
      if (out.length === 3) break;
      await sleep(300);
    }
    expect(out).toEqual([1, 2, 3]);
    expect(requests.length).toBe(1);
  });

  test("close() during a pending read returns promptly and closes the connection", async () => {
    reset(() => sse([ev(1)], true));
    const sub = subscribe({ url });
    const it = sub[Symbol.asyncIterator]();
    expect(label((await it.next()).value)).toBe(1);
    const pending = it.next();
    await sleep(50);
    const t0 = Date.now();
    await sub.close();
    expect((await pending).done).toBe(true);
    expect(Date.now() - t0).toBeLessThan(500);
    await sleep(50);
    expect(aborted()).toBe(1);
    expect(requests.length).toBe(1);
  });

  test("zstd-compressed body (concatenated frames, streamed) decodes", async () => {
    reset(
      () =>
        new Response(
          new ReadableStream<Uint8Array>({
            async start(c) {
              for (const e of [ev(1), ev(2), ev(3)]) {
                c.enqueue(new Uint8Array(Bun.zstdCompressSync(enc.encode(e))));
                await sleep(20);
              }
            },
          }),
          { headers: { "content-type": "text/event-stream", "content-encoding": "zstd" } },
        ),
    );
    expect((await collect(3)).out).toEqual([1, 2, 3]);
  });

  test("Effect: stream and SuiStream service; fatal error fails the stream; interruption closes", async () => {
    reset(() => sse([gap(1, 2), ev(3), ev(4)], true));
    const items = await Effect.runPromise(stream({ url, startAt: 1 }).pipe(Stream.take(3), Stream.runCollect));
    expect([...items].map(label)).toEqual(["gap 1-2", 3, 4]);
    await sleep(50);
    expect(aborted()).toBe(1);

    reset(() => sse([ev(9)], true));
    const viaService = await Effect.runPromise(
      SuiStream.stream().pipe(Stream.take(1), Stream.runCollect, Effect.provide(SuiStream.layer({ url }))),
    );
    expect([...viaService].map(label)).toEqual([9]);

    reset(() => new Response("no", { status: 401 }));
    const failed = await Effect.runPromise(stream({ url }).pipe(Stream.runCollect, Effect.flip));
    expect(failed._tag).toBe("LiveError");
    expect(failed.status).toBe(401);
  });
});

// ---- HTTP source details ----

describe("http live source", () => {
  test("user headers are sent and can override Accept-Encoding", async () => {
    const seen: Record<string, string>[] = [];
    const fetch = (async (_: URL, init: RequestInit) => {
      seen.push(init.headers as Record<string, string>);
      return new Response('id: 7\nevent: checkpoint\ndata: {"sequenceNumber":"7"}\n\n');
    }) as unknown as Fetch;
    const src = httpLiveSource({
      url: "https://x/",
      readMask: undefined,
      headers: { authorization: "Bearer t", "accept-encoding": "identity" },
      acceptEncoding: "zstd, gzip",
      stallTimeoutMs: 1000,
      fetch,
    });
    const events = [];
    for await (const e of src.connect(6n, new AbortController().signal)) events.push(e);
    expect(events.length).toBe(1);
    expect(seen[0]!.authorization).toBe("Bearer t");
    expect(seen[0]!["accept-encoding"]).toBe("identity");
    expect(seen[0]!["last-event-id"]).toBe("6");
  });

  test("stall while waiting for headers → non-fatal error", async () => {
    const fetch = ((_: URL, init: RequestInit) =>
      new Promise((_, reject) => init.signal?.addEventListener("abort", () => reject(new Error("aborted"))))) as unknown as Fetch;
    const src = httpLiveSource({ url: "https://x/", readMask: undefined, acceptEncoding: "gzip", stallTimeoutMs: 50, fetch });
    let error: unknown;
    try {
      for await (const _ of src.connect(undefined, new AbortController().signal));
    } catch (e) {
      error = e;
    }
    expect((error as LiveError).fatal).toBe(false);
    expect((error as LiveError).message).toContain("no data");
  });
});
