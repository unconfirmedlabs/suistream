//! Per-event cost of the SDK's hot path on recorded Mainnet events:
//! SSE framing (legacy vs current parser) and checkpoint decoding (raw mode,
//! typed with serde_json, typed with sonic-rs for comparison).
//!
//! ```sh
//! cargo bench -p suistream --bench decode            # [-- ROUNDS]
//! ```
//! Fixtures: `tests/fixtures/{lean,full}.sse.zst` (see `tests/fixtures.rs`).
//! Times are per event, p50 and mean over all events and rounds.

#[path = "../tests/support/legacy_sse.rs"]
mod legacy;

use std::hint::black_box;
use std::time::Instant;

use suistream::__private::{Frame, Parser, decode_checkpoint};
use suistream::Checkpoint;

fn fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    zstd::decode_all(std::fs::File::open(&path).expect(&path)).unwrap()
}

/// p50 and mean of `samples` (ns).
fn stats(samples: &mut [f64]) -> (f64, f64) {
    samples.sort_by(f64::total_cmp);
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    (samples[samples.len() / 2], mean)
}

fn report(name: &str, samples: &mut [f64]) {
    let (p50, mean) = stats(samples);
    println!(
        "  {name:<42} p50 {:>8.2} µs   mean {:>8.2} µs",
        p50 / 1e3,
        mean / 1e3
    );
}

/// Framing: the whole body in `chunk`-byte pieces, `rounds` times; returns
/// ns per event and MB/s (p50 over rounds).
fn framing(
    body: &[u8],
    chunk: usize,
    rounds: usize,
    events: usize,
    mut pass: impl FnMut(&[u8], usize) -> usize,
) {
    let mut per_event = vec![];
    for _ in 0..rounds {
        let t = Instant::now();
        assert_eq!(black_box(pass(body, chunk)), events);
        per_event.push(t.elapsed().as_nanos() as f64 / events as f64);
    }
    let (p50, mean) = stats(&mut per_event);
    let mbps = (body.len() as f64 / events as f64) / p50 * 1e3;
    println!(
        "    {chunk:>6} B chunks: p50 {:>7.2} µs/event   mean {:>7.2} µs/event   {mbps:>6.0} MB/s",
        p50 / 1e3,
        mean / 1e3
    );
}

fn frame_new(body: &[u8], chunk: usize) -> usize {
    let mut p = Parser::default();
    let mut out: Vec<Frame> = Vec::new();
    let mut n = 0;
    for c in body.chunks(chunk) {
        p.push(c, &mut out).unwrap();
        n += out.iter().filter(|f| f.event() == b"checkpoint").count();
        out.clear();
    }
    n
}

fn frame_legacy(body: &[u8], chunk: usize) -> usize {
    let mut p = legacy::Parser::default();
    let mut out = Vec::new();
    let mut n = 0;
    for c in body.chunks(chunk) {
        p.push(c, &mut out).unwrap();
        n += out.iter().filter(|e| e.event == "checkpoint").count();
        out.clear();
    }
    n
}

/// Each checkpoint decoded `reps` times per round; one sample per decode.
fn decode(
    events: &[(u64, Vec<u8>)],
    rounds: usize,
    reps: usize,
    f: impl Fn(u64, &[u8]),
) -> Vec<f64> {
    let mut samples = Vec::with_capacity(events.len() * rounds * reps);
    for _ in 0..rounds {
        for (seq, data) in events {
            for _ in 0..reps {
                let t = Instant::now();
                f(*seq, black_box(data));
                samples.push(t.elapsed().as_nanos() as f64);
            }
        }
    }
    samples
}

fn main() {
    let rounds: usize = std::env::args()
        .skip(1)
        .find_map(|a| a.parse().ok())
        .unwrap_or(5);
    for (name, reps) in [("lean.sse.zst", 400), ("full.sse.zst", 20)] {
        let body = fixture(name);
        let mut frames = Vec::new();
        Parser::default().push(&body, &mut frames).unwrap();
        let events: Vec<(u64, Vec<u8>)> = frames
            .iter()
            .filter(|f| f.event() == b"checkpoint")
            .map(|f| {
                let id = std::str::from_utf8(f.id().unwrap()).unwrap();
                (id.parse().unwrap(), f.data.to_vec())
            })
            .collect();
        let n = events.len();
        let mean_kb = events.iter().map(|(_, d)| d.len()).sum::<usize>() as f64 / n as f64 / 1e3;
        println!(
            "{name}: {n} checkpoints, {:.1} KB body, mean data {mean_kb:.1} KB",
            body.len() as f64 / 1e3
        );

        // Same frames, so the comparison is fair.
        assert_eq!(frame_new(&body, 16 << 10), frame_legacy(&body, 16 << 10));
        let frame_rounds = rounds * if n > 10 { 40 } else { 10 };
        for (label, f) in [
            (
                "framing, legacy (byte scan, String per line)",
                frame_legacy as fn(&[u8], usize) -> usize,
            ),
            ("framing, memchr + zero-copy Bytes", frame_new),
        ] {
            println!("  {label}");
            for chunk in [16 << 10, body.len()] {
                framing(&body, chunk, frame_rounds, n, f);
            }
        }

        report(
            "raw: no decode (Bytes handed over)",
            &mut decode(&events, rounds, reps, |_, d| {
                black_box(d);
            }),
        );
        report(
            "typed: serde_json::from_slice (0.1.0)",
            &mut decode(&events, rounds, reps, |_, d| {
                black_box(serde_json::from_slice::<Box<Checkpoint>>(d).unwrap());
            }),
        );
        report(
            "typed: decode_checkpoint (current)",
            &mut decode(&events, rounds, reps, |s, d| {
                black_box(decode_checkpoint(s, d).unwrap());
            }),
        );
        report(
            "typed: sonic_rs::from_str (not adopted)",
            &mut decode(&events, rounds, reps, |_, d| {
                let s = std::str::from_utf8(d).unwrap();
                black_box(sonic_rs::from_str::<Box<Checkpoint>>(s).unwrap());
            }),
        );
        report(
            "typed: sonic_rs::from_slice (not adopted)",
            &mut decode(&events, rounds, reps, |_, d| {
                black_box(sonic_rs::from_slice::<Box<Checkpoint>>(d).unwrap());
            }),
        );
    }
}
