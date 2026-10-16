//! Phase 0 measurements.
//!
//! ```sh
//! # Subscribe to every source for 300s: arrival latency, resets, sizes.
//! probe stream 300
//! # Sizes of specific checkpoints (GetCheckpoint, canonical mask).
//! probe sizes provider 333753000 333753001
//! # Sizes of the last checkpoint of each of the last N epochs.
//! probe epochs provider 30
//! ```
//!
//! Sources come from `SOURCES_FILE` (default `sources.toml`); header env
//! references (e.g. `PROVIDER_TOKEN`) must be set.

mod compare;

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use ingester::upstream::Upstream;
use prost::Message;
use sui_rpc::proto::sui::rpc::v2::{Checkpoint, SubscribeCheckpointsResponse};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    common::init_tracing();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let path = std::env::var("SOURCES_FILE").unwrap_or_else(|_| "sources.toml".into());
    let file = common::config::SourcesFile::parse(&std::fs::read_to_string(&path)?)?;
    let upstream = |name: &str| -> Result<Upstream> {
        Upstream::new(file.resolve(name, |k| std::env::var(k).ok())?)
    };

    match args.first().map(String::as_str) {
        Some("stream") => {
            let secs: u64 = args.get(1).map(|s| s.parse()).transpose()?.unwrap_or(60);
            let names: Vec<String> = match args.get(2) {
                Some(list) => list.split(',').map(str::to_owned).collect(),
                None => file.sources.keys().cloned().collect(),
            };
            let ups = names
                .iter()
                .map(|n| upstream(n))
                .collect::<Result<Vec<_>>>()?;
            stream(ups, Duration::from_secs(secs)).await
        }
        Some("sizes") => {
            let up = upstream(args.get(1).context("source")?)?;
            print_size_header();
            for seq in &args[2..] {
                let cp = up
                    .get_checkpoint(seq.parse()?)
                    .await?
                    .context("not found")?;
                print_sizes(&Sizes::measure(&cp, true));
            }
            Ok(())
        }
        Some("epochs") => {
            let up = upstream(args.get(1).context("source")?)?;
            let n: u64 = args.get(2).map(|s| s.parse()).transpose()?.unwrap_or(10);
            epochs(&up, n).await
        }
        Some("compare") => {
            let url = args.get(1).context("gateway url")?.clone();
            let up = upstream(args.get(2).context("source")?)?;
            let secs: u64 = args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(900);
            let enc = args.get(4).cloned().unwrap_or_else(|| "zstd".into());
            compare::run(url, enc, up, Duration::from_secs(secs)).await
        }
        _ => bail!(
            "usage: probe compare <sse-url> <source> <secs> [zstd|gzip|identity|auto] | probe stream <secs> [sources] | sizes <source> <seq>... | epochs <source> <n>"
        ),
    }
}

#[derive(Default, Clone)]
struct Sizes {
    seq: u64,
    txs: usize,
    proto: usize,
    zstd1: usize,
    zstd3: usize,
    zstd9: usize,
    zstd19: usize,
    json: usize,
    json_zstd3: usize,
    json_gzip6: usize,
    us_zstd1: u128,
    us_zstd3: u128,
    us_zstd9: u128,
    us_zstd19: u128,
    us_json: u128,
    us_json_zstd3: u128,
    us_json_gzip6: u128,
}

fn timed<T>(f: impl FnOnce() -> T) -> (T, u128) {
    let t = Instant::now();
    let v = f();
    (v, t.elapsed().as_micros())
}

impl Sizes {
    fn measure(cp: &Checkpoint, expensive: bool) -> Self {
        let mut resp = SubscribeCheckpointsResponse::default();
        resp.cursor = cp.sequence_number;
        resp.checkpoint = Some(cp.clone());
        let raw = resp.encode_to_vec();
        let z = |level| timed(|| zstd::bulk::compress(&raw, level).unwrap().len());
        let (zstd1, us_zstd1) = z(1);
        let (zstd3, us_zstd3) = z(3);
        let (zstd9, us_zstd9) = z(9);
        let (zstd19, us_zstd19) = if expensive { z(19) } else { (0, 0) };
        let (json, us_json) = timed(|| serde_json::to_vec(&resp).unwrap());
        let (json_zstd3, us_json_zstd3) = timed(|| zstd::bulk::compress(&json, 3).unwrap().len());
        let (json_gzip6, us_json_gzip6) = timed(|| {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(6));
            enc.write_all(&json).unwrap();
            enc.finish().unwrap().len()
        });
        Self {
            seq: cp.sequence_number.unwrap_or_default(),
            txs: cp.transactions.len(),
            proto: raw.len(),
            zstd1,
            zstd3,
            zstd9,
            zstd19,
            json: json.len(),
            json_zstd3,
            json_gzip6,
            us_zstd1,
            us_zstd3,
            us_zstd9,
            us_zstd19,
            us_json,
            us_json_zstd3,
            us_json_gzip6,
        }
    }
}

fn print_size_header() {
    println!(
        "seq,txs,proto,zstd1,zstd3,zstd9,zstd19,json,json_zstd3,json_gzip6,us_zstd1,us_zstd3,us_zstd9,us_zstd19,us_json,us_json_zstd3,us_json_gzip6"
    );
}

fn print_sizes(s: &Sizes) {
    println!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        s.seq,
        s.txs,
        s.proto,
        s.zstd1,
        s.zstd3,
        s.zstd9,
        s.zstd19,
        s.json,
        s.json_zstd3,
        s.json_gzip6,
        s.us_zstd1,
        s.us_zstd3,
        s.us_zstd9,
        s.us_zstd19,
        s.us_json,
        s.us_json_zstd3,
        s.us_json_gzip6
    );
}

async fn epochs(up: &Upstream, n: u64) -> Result<()> {
    use sui_rpc::field::FieldMaskUtil;
    use sui_rpc::proto::sui::rpc::v2::GetEpochRequest;
    let mut client = sui_rpc::Client::new(up.source().url.as_str())?;
    let mut headers = sui_rpc::client::HeadersInterceptor::new();
    for (k, v) in &up.source().headers {
        headers.headers_mut().insert(
            tonic::metadata::MetadataKey::from_bytes(k.as_bytes())?,
            v.parse()?,
        );
    }
    client = client.with_headers(headers);
    let mut req = GetEpochRequest::default();
    req.read_mask = Some(prost_types_mask("epoch"));
    let current = client
        .ledger_client()
        .get_epoch(req)
        .await?
        .into_inner()
        .epoch
        .and_then(|e| e.epoch)
        .context("current epoch")?;
    print_size_header();
    for epoch in current.saturating_sub(n)..current {
        let mut req = GetEpochRequest::default();
        req.epoch = Some(epoch);
        req.read_mask = Some(sui_rpc::field::FieldMask::from_paths(["last_checkpoint"]));
        let last = client
            .ledger_client()
            .get_epoch(req)
            .await?
            .into_inner()
            .epoch
            .and_then(|e| e.last_checkpoint)
            .context("last_checkpoint")?;
        let cp = up.get_checkpoint(last).await?.context("not found")?;
        print_sizes(&Sizes::measure(&cp, true));
    }
    Ok(())
}

fn prost_types_mask(path: &str) -> sui_rpc::field::FieldMask {
    use sui_rpc::field::FieldMaskUtil;
    sui_rpc::field::FieldMask::from_paths([path])
}

enum Event {
    Frame {
        source: usize,
        seq: u64,
        at: Instant,
        checkpoint: Option<Box<Checkpoint>>,
    },
    Ended {
        source: usize,
        after: Duration,
        status: String,
    },
}

/// Subscribe to every source; report arrival order/latency, stream resets and
/// size distribution.
async fn stream(ups: Vec<Upstream>, run_for: Duration) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    for (i, up) in ups.iter().cloned().enumerate() {
        let tx = tx.clone();
        tokio::spawn(async move {
            loop {
                let opened = Instant::now();
                let status = match up.subscribe().await {
                    Ok(mut stream) => loop {
                        match stream.next().await {
                            Some(Ok(frame)) => {
                                let seq = frame.cursor.unwrap_or_default();
                                let _ = tx.send(Event::Frame {
                                    source: i,
                                    seq,
                                    at: Instant::now(),
                                    checkpoint: frame.checkpoint.map(Box::new),
                                });
                            }
                            Some(Err(status)) => {
                                break format!("{:?}: {}", status.code(), status.message());
                            }
                            None => break "end of stream".to_owned(),
                        }
                    },
                    Err(status) => format!("open {:?}: {}", status.code(), status.message()),
                };
                let _ = tx.send(Event::Ended {
                    source: i,
                    after: opened.elapsed(),
                    status,
                });
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    }
    drop(tx);

    let names: Vec<String> = ups.iter().map(|u| u.source().name.clone()).collect();
    let deadline = tokio::time::Instant::now() + run_for;
    // seq -> arrival per source
    let mut arrivals: BTreeMap<u64, HashMap<usize, Instant>> = BTreeMap::new();
    let mut last_seq: HashMap<usize, u64> = HashMap::new();
    let mut gaps: HashMap<usize, Vec<(u64, u64)>> = HashMap::new();
    let mut reorders: HashMap<usize, u64> = HashMap::new();
    let mut resets: Vec<(usize, Duration, String)> = Vec::new();
    let mut sizes: Vec<Sizes> = Vec::new();
    let mut digests: HashMap<u64, (usize, String)> = HashMap::new();
    let mut digest_mismatch = 0u64;
    let mut decoded_mismatch = 0u64;
    let mut decoded: HashMap<u64, Checkpoint> = HashMap::new();

    let started = Instant::now();
    let mut csv = std::fs::File::create("probe-sizes.csv")?;
    writeln!(
        csv,
        "seq,txs,proto,zstd1,zstd3,zstd9,zstd19,json,json_zstd3,json_gzip6,us_zstd1,us_zstd3,us_zstd9,us_zstd19,us_json,us_json_zstd3,us_json_gzip6"
    )?;
    loop {
        let event = tokio::select! {
            e = rx.recv() => match e { Some(e) => e, None => break },
            _ = tokio::time::sleep_until(deadline) => break,
        };
        match event {
            Event::Frame {
                source,
                seq,
                at,
                checkpoint,
            } => {
                if let Some(prev) = last_seq.get(&source).copied() {
                    if seq > prev + 1 {
                        gaps.entry(source).or_default().push((prev + 1, seq - 1));
                    } else if seq <= prev {
                        *reorders.entry(source).or_default() += 1;
                    }
                }
                last_seq.insert(source, seq.max(last_seq.get(&source).copied().unwrap_or(0)));
                let first = !arrivals.contains_key(&seq);
                arrivals.entry(seq).or_default().entry(source).or_insert(at);
                let Some(cp) = checkpoint else { continue };
                let digest = cp.digest.clone().unwrap_or_default();
                match digests.get(&seq) {
                    Some((_, d)) if *d != digest => digest_mismatch += 1,
                    Some(_) => {}
                    None => {
                        digests.insert(seq, (source, digest));
                    }
                }
                // Semantic comparison (signature legitimately differs).
                let mut norm = (*cp).clone();
                norm.signature = None;
                match decoded.get(&seq) {
                    Some(other) if *other != norm => decoded_mismatch += 1,
                    Some(_) => {}
                    None => {
                        decoded.insert(seq, norm);
                    }
                }
                if first {
                    let s = Sizes::measure(&cp, sizes.len().is_multiple_of(10));
                    let line = format!(
                        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                        s.seq,
                        s.txs,
                        s.proto,
                        s.zstd1,
                        s.zstd3,
                        s.zstd9,
                        s.zstd19,
                        s.json,
                        s.json_zstd3,
                        s.json_gzip6,
                        s.us_zstd1,
                        s.us_zstd3,
                        s.us_zstd9,
                        s.us_zstd19,
                        s.us_json,
                        s.us_json_zstd3,
                        s.us_json_gzip6
                    );
                    writeln!(csv, "{line}")?;
                    sizes.push(s);
                }
                // Keep memory bounded.
                if decoded.len() > 200 {
                    let min = *decoded.keys().min().unwrap();
                    decoded.remove(&min);
                }
            }
            Event::Ended {
                source,
                after,
                status,
            } => {
                eprintln!(
                    "[{:>6.1}s] {} stream ended after {:.1}s: {}",
                    started.elapsed().as_secs_f64(),
                    names[source],
                    after.as_secs_f64(),
                    status
                );
                resets.push((source, after, status));
            }
        }
    }

    println!("\n== {} checkpoints over {:?}", arrivals.len(), run_for);
    // Latency of each source behind the first arrival, over seqs seen by all.
    let complete: Vec<_> = arrivals
        .values()
        .filter(|m| m.len() == names.len())
        .collect();
    for (i, name) in names.iter().enumerate() {
        let mut behind: Vec<f64> = complete
            .iter()
            .map(|m| {
                let first = m.values().min().unwrap();
                m[&i].duration_since(*first).as_secs_f64() * 1000.0
            })
            .collect();
        behind.sort_by(f64::total_cmp);
        let wins = complete
            .iter()
            .filter(|m| m.values().min() == m.get(&i))
            .count();
        let seen = arrivals.values().filter(|m| m.contains_key(&i)).count();
        println!(
            "{name:>12}: seen {seen}, first {wins}/{}, behind-first ms p50 {:.0} p90 {:.0} p99 {:.0} max {:.0}; gaps {:?}; reorders {}; ended {}",
            complete.len(),
            pct(&behind, 0.5),
            pct(&behind, 0.9),
            pct(&behind, 0.99),
            behind.last().copied().unwrap_or(0.0),
            gaps.get(&i).cloned().unwrap_or_default(),
            reorders.get(&i).copied().unwrap_or(0),
            resets.iter().filter(|r| r.0 == i).count(),
        );
    }
    println!(
        "digest mismatches: {digest_mismatch}; decoded (sans signature) mismatches: {decoded_mismatch}"
    );
    summarize_sizes(&sizes);
    Ok(())
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

fn summarize_sizes(sizes: &[Sizes]) {
    let col = |f: &dyn Fn(&Sizes) -> f64, only_nonzero: bool| {
        let mut v: Vec<f64> = sizes
            .iter()
            .map(f)
            .filter(|x| !only_nonzero || *x > 0.0)
            .collect();
        v.sort_by(f64::total_cmp);
        let mean = v.iter().sum::<f64>() / v.len().max(1) as f64;
        (
            pct(&v, 0.5),
            pct(&v, 0.99),
            v.last().copied().unwrap_or(0.0),
            mean,
        )
    };
    println!(
        "\n== sizes over {} checkpoints (bytes; mean, p50, p99, max)",
        sizes.len()
    );
    type Row<'a> = (&'a str, &'a dyn Fn(&Sizes) -> f64, bool);
    let rows: [Row; 15] = [
        ("proto", &|s| s.proto as f64, false),
        ("zstd1", &|s| s.zstd1 as f64, false),
        ("zstd3", &|s| s.zstd3 as f64, false),
        ("zstd9", &|s| s.zstd9 as f64, false),
        ("zstd19 (1/10)", &|s| s.zstd19 as f64, true),
        ("json", &|s| s.json as f64, false),
        ("json zstd3", &|s| s.json_zstd3 as f64, false),
        ("json gzip6", &|s| s.json_gzip6 as f64, false),
        ("us zstd1", &|s| s.us_zstd1 as f64, false),
        ("us zstd3", &|s| s.us_zstd3 as f64, false),
        ("us zstd9", &|s| s.us_zstd9 as f64, false),
        ("us zstd19", &|s| s.us_zstd19 as f64, true),
        ("us json", &|s| s.us_json as f64, false),
        ("us json zstd3", &|s| s.us_json_zstd3 as f64, false),
        ("us json gzip6", &|s| s.us_json_gzip6 as f64, false),
    ];
    for (name, f, nz) in rows {
        let (p50, p99, max, mean) = col(f, nz);
        println!(
            "{name:>14}: mean {mean:>10.0}  p50 {p50:>10.0}  p99 {p99:>10.0}  max {max:>10.0}"
        );
    }
}
