//! End-to-end check: the gateway SSE stream against a direct upstream
//! subscription.
//!
//! Reports gaps/reorders/gap events on the gateway stream, semantic equality
//! of every checkpoint seen on both (decoded under the canonical mask,
//! ignoring the aggregated signature, which legitimately differs per node),
//! and gateway latency relative to the upstream.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures::StreamExt;
use ingester::upstream::Upstream;
use sui_rpc::proto::sui::rpc::v2::Checkpoint;
use tokio::sync::mpsc;

enum Ev {
    Gateway(u64, Box<Checkpoint>, Instant),
    GatewayGap(String),
    GatewayEnded(String),
    Upstream(u64, Box<Checkpoint>, Instant),
}

type Seen = BTreeMap<u64, (Checkpoint, Instant)>;

#[derive(Default)]
struct Stats {
    matched: u64,
    mismatched: u64,
    /// Gateway arrival minus upstream arrival, ms.
    deltas: Vec<f64>,
}

impl Stats {
    /// Compare `seq` once both sides have it.
    fn check(&mut self, seq: u64, gw: &mut Seen, upm: &mut Seen) {
        let (Some((a, ta)), Some((b, tb))) = (gw.get(&seq), upm.get(&seq)) else {
            return;
        };
        if a == b {
            self.matched += 1;
        } else {
            self.mismatched += 1;
            eprintln!(
                "MISMATCH at {seq}: digest gw={:?} up={:?}",
                a.digest, b.digest
            );
        }
        let d = if ta >= tb {
            ta.duration_since(*tb).as_secs_f64()
        } else {
            -tb.duration_since(*ta).as_secs_f64()
        };
        self.deltas.push(d * 1000.0);
        gw.remove(&seq);
        upm.remove(&seq);
    }
}

fn normalize(mut cp: Checkpoint) -> Checkpoint {
    cp.signature = None;
    cp
}

pub async fn run(url: String, encoding: String, up: Upstream, run_for: Duration) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    {
        let tx = tx.clone();
        tokio::spawn(async move {
            // Resume after the last checkpoint seen, like EventSource does.
            let last = std::sync::atomic::AtomicU64::new(0);
            loop {
                let reason = match sse(&url, &encoding, &tx, &last).await {
                    Ok(()) => "stream ended".to_owned(),
                    Err(e) => format!("{e:#}"),
                };
                if tx.send(Ev::GatewayEnded(reason)).is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }
    tokio::spawn(async move {
        loop {
            if let Ok(mut stream) = up.subscribe().await {
                while let Some(Ok(frame)) = stream.next().await {
                    if let Some(cp) = frame.checkpoint {
                        let seq = cp.sequence_number.unwrap_or_default();
                        if tx
                            .send(Ev::Upstream(seq, Box::new(cp), Instant::now()))
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    let deadline = tokio::time::Instant::now() + run_for;
    let started = Instant::now();
    let mut gw: Seen = BTreeMap::new();
    let mut upm: Seen = BTreeMap::new();
    let mut last_gw: Option<u64> = None;
    let (mut gw_count, mut gaps, mut reorders) = (0u64, vec![], 0u64);
    let mut stats = Stats::default();
    let mut gap_events = vec![];
    let mut reconnects = vec![];
    let mut first_gw: Option<u64> = None;

    let mut report = tokio::time::interval(Duration::from_secs(600));
    report.tick().await;
    loop {
        let ev = tokio::select! {
            e = rx.recv() => match e { Some(e) => e, None => break },
            _ = tokio::time::sleep_until(deadline) => break,
            _ = report.tick() => {
                eprintln!(
                    "[{:.0}s] progress: gateway {gw_count} (last {last_gw:?}), gaps {}, gap events {}, reorders {reorders}, matched {}, mismatched {}, reconnects {}",
                    started.elapsed().as_secs_f64(),
                    gaps.len(),
                    gap_events.len(),
                    stats.matched,
                    stats.mismatched,
                    reconnects.len()
                );
                continue;
            }
        };
        match ev {
            Ev::Gateway(seq, cp, at) => {
                gw_count += 1;
                first_gw.get_or_insert(seq);
                if let Some(l) = last_gw {
                    if seq > l + 1 {
                        gaps.push((l + 1, seq - 1));
                        eprintln!("GAP on gateway stream: {}..={}", l + 1, seq - 1);
                    } else if seq <= l {
                        reorders += 1;
                        eprintln!("REORDER on gateway stream: {seq} after {l}");
                    }
                }
                last_gw = Some(seq);
                gw.insert(seq, (normalize(*cp), at));
                stats.check(seq, &mut gw, &mut upm);
            }
            Ev::GatewayGap(data) => {
                eprintln!("gap event: {data}");
                gap_events.push(data);
            }
            Ev::GatewayEnded(reason) => {
                eprintln!(
                    "[{:.0}s] gateway stream ended: {reason}",
                    started.elapsed().as_secs_f64()
                );
                reconnects.push(reason);
            }
            Ev::Upstream(seq, cp, at) => {
                upm.insert(seq, (normalize(*cp), at));
                stats.check(seq, &mut gw, &mut upm);
            }
        }
        // Bound memory (decoded checkpoints are several MB): anything this
        // far behind is unmatched.
        while gw.len() > 120 {
            gw.pop_first();
        }
        while upm.len() > 120 {
            upm.pop_first();
        }
    }

    let (matched, mismatched, mut deltas) = (stats.matched, stats.mismatched, stats.deltas);
    deltas.sort_by(f64::total_cmp);
    let p = |q: f64| {
        if deltas.is_empty() {
            0.0
        } else {
            deltas[((deltas.len() - 1) as f64 * q).round() as usize]
        }
    };
    // Upstream checkpoints within the gateway's window that it never sent.
    let window = first_gw.zip(last_gw);
    // Allow the last few to still be in flight.
    let missing = upm
        .keys()
        .filter(|s| window.is_some_and(|(a, b)| **s >= a && **s + 10 <= b))
        .count();
    println!("== compare over {:?}", run_for);
    println!("gateway checkpoints: {gw_count} (range {:?})", window);
    println!("gateway gaps (seq jumps): {gaps:?}");
    println!("gateway reorders: {reorders}");
    println!("gateway gap events: {gap_events:?}");
    println!("gateway reconnects: {}", reconnects.len());
    println!("matched equal: {matched}; mismatched: {mismatched}");
    println!("upstream checkpoints in window never matched: {missing}");
    println!(
        "gateway minus upstream arrival (ms): p10 {:.0} p50 {:.0} p90 {:.0} p99 {:.0}",
        p(0.1),
        p(0.5),
        p(0.9),
        p(0.99)
    );
    Ok(())
}

async fn sse(
    url: &str,
    encoding: &str,
    tx: &mpsc::UnboundedSender<Ev>,
    last: &std::sync::atomic::AtomicU64,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    let mut req = client.get(url);
    if encoding != "auto" {
        req = req.header("accept-encoding", encoding);
    }
    let resume = last.load(std::sync::atomic::Ordering::Relaxed);
    if resume > 0 {
        req = req.header("last-event-id", resume.to_string());
    }
    let resp = req.send().await?.error_for_status()?;
    eprintln!(
        "connected: content-encoding {:?}",
        resp.headers().get("content-encoding")
    );
    let mut body = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    let mut scanned = 0usize;
    let mut idle = tokio::time::interval(Duration::from_secs(40));
    idle.tick().await;
    loop {
        let chunk = tokio::select! {
            c = body.next() => match c { Some(c) => c?, None => return Ok(()) },
            _ = idle.tick() => anyhow::bail!("no data for 40s"),
        };
        idle.reset();
        buf.extend_from_slice(&chunk);
        loop {
            let Some(rel) = find(&buf[scanned.saturating_sub(1)..], b"\n\n") else {
                scanned = buf.len();
                break;
            };
            let pos = scanned.saturating_sub(1) + rel;
            let event: Vec<u8> = buf.drain(..pos + 2).collect();
            scanned = 0;
            let text = std::str::from_utf8(&event).context("utf8")?;
            let mut kind = "message";
            let mut data = "";
            for line in text.lines() {
                if let Some(v) = line.strip_prefix("event: ") {
                    kind = v;
                } else if let Some(v) = line.strip_prefix("data: ") {
                    data = v;
                }
            }
            match kind {
                "checkpoint" => {
                    let cp: Checkpoint = serde_json::from_str(data).context("checkpoint json")?;
                    let seq = cp.sequence_number.unwrap_or_default();
                    last.store(seq, std::sync::atomic::Ordering::Relaxed);
                    let _ = tx.send(Ev::Gateway(seq, Box::new(cp), Instant::now()));
                }
                "gap" => {
                    let _ = tx.send(Ev::GatewayGap(data.to_owned()));
                }
                _ => {}
            }
        }
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}
