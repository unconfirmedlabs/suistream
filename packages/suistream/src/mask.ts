/**
 * Normalize a read mask (`sui.rpc.v2.Checkpoint` field paths, sent as
 * `read_mask`; the gateway validates them): trimmed, deduplicated,
 * `undefined` for the full canonical checkpoint (empty, or containing `*`).
 * The sequence number comes from the SSE `id`, so the mask need not include
 * `sequence_number`.
 */
export const resolveMask = (paths?: readonly string[]): readonly string[] | undefined => {
  const out = [...new Set((paths ?? []).map((p) => p.trim()).filter((p) => p.length > 0))];
  return out.length === 0 || out.includes("*") ? undefined : out;
};
