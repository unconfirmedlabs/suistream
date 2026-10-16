/**
 * Tail the stream and check continuity (Bun or Node ≥ 23.6).
 *
 *   bun examples/tail.ts [--url U] [--mask a,b] [--from N | --ago N] [--seconds S]
 *
 * `--ago N` starts N checkpoints below the current tip (read from the stream
 * itself). Prints a JSON summary: counts, order violations, duplicates, gaps,
 * reconnects, memory.
 */

import { type Diagnostic, MAINNET_STREAM_URL, subscribe } from "../src/index.ts";
import { defaultAcceptEncoding } from "../src/sse.ts";

const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i]!.replace(/^--/, ""), process.argv[i + 1] ?? "");

const url = args.get("url") ?? MAINNET_STREAM_URL;
const mask = args.get("mask")?.split(",");
const seconds = Number(args.get("seconds") ?? 60);
let startAt: bigint | undefined = args.has("from") ? BigInt(args.get("from")!) : undefined;
if (args.has("ago")) {
  const probe = subscribe({ url, readMask: ["sequence_number"] });
  for await (const item of probe) {
    if (item.type === "checkpoint") {
      startAt = item.seq - BigInt(args.get("ago")!);
      break;
    }
  }
}

const started = Date.now();
const t = () => ((Date.now() - started) / 1000).toFixed(1);
const stats = {
  runtime: typeof Bun !== "undefined" ? `bun ${Bun.version}` : `node ${process.version}`,
  acceptEncoding: defaultAcceptEncoding(),
  url,
  mask: mask ?? "full",
  startAt: startAt?.toString(),
  delivered: 0,
  first: undefined as string | undefined,
  last: undefined as string | undefined,
  orderViolations: 0,
  duplicates: 0,
  gaps: [] as string[],
  connects: 0,
  disconnects: [] as string[],
  caughtUpAfterS: undefined as number | undefined,
  lagS: undefined as number | undefined,
  memMiB: [] as string[],
};
const onDiagnostic = (d: Diagnostic) => {
  if (d.type === "connect") stats.connects++;
  else stats.disconnects.push(`${t()}s ${d.reason.slice(0, 120)} (retry in ${d.retryInMs} ms)`);
};

const sub = subscribe({
  url,
  readMask: mask,
  startAt,
  onDiagnostic,
});
let prev: bigint | undefined;
const timer = setTimeout(() => void sub.close(), seconds * 1000);
const progress = setInterval(() => {
  const m = process.memoryUsage();
  const rss = Math.round(m.rss / 2 ** 20);
  const heap = Math.round(m.heapUsed / 2 ** 20);
  stats.memMiB.push(`${t()}s rss ${rss} heap ${heap}`);
  console.error(`[${t()}s] delivered ${stats.delivered} last ${prev} rss ${rss} MiB heap ${heap} MiB`);
}, 30_000);
try {
  for await (const item of sub) {
    if (item.type === "gap") {
      console.error(`[${t()}s] gap ${item.from}..=${item.to}`);
      stats.gaps.push(`${item.from}..=${item.to}`);
      if (prev !== undefined && item.from !== prev + 1n) stats.orderViolations++;
      prev = item.to;
      continue;
    }
    const seq = item.seq;
    if (item.checkpoint.sequenceNumber !== undefined && item.checkpoint.sequenceNumber !== seq) stats.orderViolations++;
    if (prev !== undefined && seq <= prev) stats.duplicates++;
    else if (prev !== undefined && seq !== prev + 1n) {
      stats.orderViolations++;
      console.error(`ORDER: ${seq} after ${prev}`);
    }
    prev = seq;
    stats.delivered++;
    stats.first ??= seq.toString();
    stats.last = seq.toString();
    const ts = item.checkpoint.summary?.timestamp;
    if (ts) {
      const lag = Date.now() / 1000 - Number(ts.seconds);
      stats.lagS = Math.round(lag * 10) / 10;
      if (lag < 10 && stats.caughtUpAfterS === undefined) stats.caughtUpAfterS = Number(t());
    }
  }
} catch (error) {
  console.error("terminated:", error);
  process.exitCode = 1;
} finally {
  clearTimeout(timer);
  clearInterval(progress);
}
const covered =
  stats.first && stats.last ? BigInt(stats.last) - BigInt(stats.first) + 1n : 0n;
console.log(
  JSON.stringify(
    {
      ...stats,
      elapsedS: (Date.now() - started) / 1000,
      expected: covered.toString(),
      complete: stats.gaps.length === 0 && stats.orderViolations === 0 && covered === BigInt(stats.delivered),
      lastDelivered: sub.lastDelivered?.toString(),
      cursor: sub.cursor?.toString(),
    },
    null,
    2,
  ),
);
