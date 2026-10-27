//! The SSE parser against recorded Mainnet streams: same frames as the
//! legacy parser, however the bytes are chunked, and every checkpoint
//! decodes, identically with `serde_json::from_slice` and sonic-rs.
//!
//! - `lean.sse.zst`: 24 checkpoints, read mask
//!   `summary.bcs,transactions.digest,transactions.events.bcs`.
//! - `full.sse.zst`: 3 full checkpoints.

#[path = "support/legacy_sse.rs"]
mod legacy;

use suistream::__private::{Frame, Parser, decode_checkpoint};
use suistream::Checkpoint;

fn fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    zstd::decode_all(std::fs::File::open(&path).expect(&path)).unwrap()
}

/// (id, event, data) of each frame.
type Triple = (Option<Vec<u8>>, Vec<u8>, Vec<u8>);

fn parse(chunks: &[&[u8]]) -> Vec<Triple> {
    let mut p = Parser::default();
    let mut out: Vec<Frame> = vec![];
    for c in chunks {
        p.push(c, &mut out).unwrap();
    }
    out.into_iter()
        .map(|f| {
            (
                f.id().map(<[u8]>::to_vec),
                f.event().to_vec(),
                f.data.to_vec(),
            )
        })
        .collect()
}

fn parse_legacy(chunks: &[&[u8]]) -> Vec<Triple> {
    let mut p = legacy::Parser::default();
    let mut out = vec![];
    for c in chunks {
        p.push(c, &mut out).unwrap();
    }
    out.into_iter()
        .map(|e| {
            (
                e.id.map(String::into_bytes),
                e.event.into_bytes(),
                e.data.into_bytes(),
            )
        })
        .collect()
}

/// Splits at pseudo-random points, 1 byte to 64 KiB apart.
fn random_chunks(body: &[u8], mut seed: u64) -> Vec<&[u8]> {
    let mut out = vec![];
    let mut rest = body;
    while !rest.is_empty() {
        // xorshift64
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let n = (1 + (seed % 65_536) as usize).min(rest.len());
        let (head, tail) = rest.split_at(n);
        out.push(head);
        rest = tail;
    }
    out
}

fn check(name: &str, checkpoints: usize, seq_in_mask: bool) {
    let body = fixture(name);
    let want = parse_legacy(&[&body]);
    assert_eq!(want.len(), checkpoints, "{name}");
    assert_eq!(parse(&[&body]), want, "{name} whole");
    let chunks: Vec<&[u8]> = body.chunks(4096).collect();
    assert_eq!(parse(&chunks), want, "{name} 4 KiB");
    assert_eq!(parse_legacy(&chunks), want, "{name} 4 KiB legacy");
    for seed in 1..=8 {
        let chunks = random_chunks(&body, seed);
        assert_eq!(parse(&chunks), want, "{name} random {seed}");
    }
    for (id, event, data) in &want {
        assert_eq!(event, b"checkpoint");
        let seq: u64 = std::str::from_utf8(id.as_deref().unwrap())
            .unwrap()
            .parse()
            .unwrap();
        let cp = decode_checkpoint(seq, data).unwrap();
        // The SDK parses `&str`; 0.1.0 parsed bytes. sonic-rs was evaluated
        // as a faster backend (benches/decode.rs) and agrees too.
        let from_slice: Box<Checkpoint> = serde_json::from_slice(data).unwrap();
        assert_eq!(cp, from_slice, "{name} {seq}: from_str vs from_slice");
        let sonic: Box<Checkpoint> = sonic_rs::from_slice(data).unwrap();
        assert_eq!(cp, sonic, "{name} {seq}: serde_json vs sonic-rs");
        if seq_in_mask {
            assert_eq!(cp.sequence_number, Some(seq));
        }
    }
}

#[test]
fn lean_fixture() {
    check("lean.sse.zst", 24, false);
}

#[test]
fn full_fixture() {
    check("full.sse.zst", 3, true);
}
