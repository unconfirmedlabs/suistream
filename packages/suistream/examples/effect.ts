/** Effect usage: the `SuiStream` service provided by a `Layer`. `bun examples/effect.ts` */
import { Effect, Stream } from "effect";
import { SuiStream } from "../src/index.ts";

const program = SuiStream.stream().pipe(
  Stream.take(5),
  Stream.runForEach((item) =>
    item.type === "gap"
      ? Effect.logWarning(`gap ${item.from}..=${item.to}`)
      : Effect.log(`checkpoint ${item.seq} ${item.checkpoint.digest}`),
  ),
);

await Effect.runPromise(program.pipe(Effect.provide(SuiStream.layer({ readMask: ["digest"] }))));
