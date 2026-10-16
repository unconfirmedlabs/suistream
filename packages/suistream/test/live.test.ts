/**
 * Against production (Mainnet). Opt-in: `SUISTREAM_LIVE=1 bun test test/live.test.ts`
 * (or `bun run test:live`). `SUISTREAM_URL` overrides the endpoint (e.g. a
 * local gateway with history).
 */
import { describe, expect, test } from "bun:test";
import { MAINNET_STREAM_URL, type StreamItem, subscribe } from "../src/index.ts";

const live = process.env.SUISTREAM_LIVE === "1";
const url = process.env.SUISTREAM_URL ?? MAINNET_STREAM_URL;
const mask = ["sequence_number", "summary.timestamp"];

/** Read until an item less than `fresh` seconds old (after `min` items), or `limit` items. */
const run = async (opts: { from?: bigint; limit: number; fresh?: number; min?: number }) => {
  const sub = subscribe({ url, readMask: mask, startAt: opts.from });
  const items: StreamItem[] = [];
  for await (const item of sub) {
    items.push(item);
    if (items.length >= opts.limit) break;
    if (item.type === "checkpoint" && opts.fresh !== undefined && items.length >= (opts.min ?? 2)) {
      const age = Date.now() / 1000 - Number(item.checkpoint.summary!.timestamp!.seconds);
      if (age < opts.fresh) break;
    }
  }
  return { items, last: sub.lastDelivered, cursor: sub.cursor };
};

const contiguous = (items: StreamItem[]) => {
  let prev: bigint | undefined;
  for (const i of items) {
    const [a, b] = i.type === "gap" ? [i.from, i.to] : [i.seq, i.seq];
    if (prev !== undefined && a !== prev + 1n) return false;
    prev = b;
  }
  return true;
};

const tip = async () => {
  const { items } = await run({ limit: 1 });
  return (items[0] as { seq: bigint }).seq;
};

describe.skipIf(!live)("live", () => {
  test("tip: in order", async () => {
    const { items, last } = await run({ limit: 40 });
    expect(items.length).toBe(40);
    expect(items.every((i) => i.type === "checkpoint")).toBe(true);
    expect(contiguous(items)).toBe(true);
    expect(last).toBe((items.at(-1) as { seq: bigint }).seq);
  }, 60_000);

  test("resume: served by the gateway, contiguous into live", async () => {
    const from = (await tip()) - BigInt(process.env.SUISTREAM_BACK ?? 600);
    const { items } = await run({ from, limit: 100_000, fresh: 5, min: 10 });
    const first = items[0]!;
    expect(first.type === "gap" ? first.from : first.seq).toBe(from);
    expect(contiguous(items)).toBe(true);
  }, 600_000);
});
