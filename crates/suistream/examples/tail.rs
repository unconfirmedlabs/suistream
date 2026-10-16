//! Tail the stream and report ordering statistics.
//!
//! ```sh
//! cargo run --release -p suistream --example tail -- \
//!     [--url URL] [--mask sequence_number,digest|full] [--start SEQ | --back N] [--secs S]
//! ```
//! `--back N` starts N checkpoints behind the stream's tip (the gateway
//! serves the history). Logs (`RUST_LOG=suistream=info`) show reconnects.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use suistream::Event;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "suistream=info".into()),
        )
        .init();
    let mut args = std::env::args().skip(1);
    let (mut url, mut mask, mut start, mut back, mut secs) = (None, None, None, None, 300u64);
    while let Some(a) = args.next() {
        let mut v = || args.next().context("missing value");
        match a.as_str() {
            "--url" => url = Some(v()?),
            "--mask" => mask = Some(v()?),
            "--start" => start = Some(v()?.parse::<u64>()?),
            "--back" => back = Some(v()?.parse::<u64>()?),
            "--secs" => secs = v()?.parse()?,
            other => bail!("unknown argument {other}"),
        }
    }
    let base = || {
        let b = suistream::Client::builder();
        match &url {
            Some(u) => b.url(u.clone()),
            None => b,
        }
    };
    if let Some(back) = back {
        // The tip: the first checkpoint of a live subscription.
        let mut probe = base().read_mask(["sequence_number"]).build()?.subscribe();
        let tip = loop {
            match probe.next().await.context("stream ended")?? {
                Event::Checkpoint { seq, .. } => break seq,
                Event::Gap { .. } => continue,
            }
        };
        start = Some(tip - back);
        eprintln!("tip {tip}, starting at {}", tip - back);
    }
    let mut builder = base();
    let timestamps = match mask.filter(|m| m != "full") {
        Some(m) => {
            let has_ts = m
                .split(',')
                .any(|p| p == "summary" || p == "summary.timestamp");
            builder = builder.read_mask(m.split(','));
            has_ts
        }
        None => true,
    };
    if let Some(s) = start {
        builder = builder.start_at(s);
    }
    let mut sub = builder.build()?.subscribe();

    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    let (mut count, mut first, mut prev) = (0u64, None::<u64>, None::<u64>);
    let (mut gaps, mut gap_events, mut dupes) = (0u64, 0u64, 0u64);
    let mut caught_up: Option<f64> = None;
    let mut window = (Instant::now(), 0u64);
    let mut lag_ms: Option<i64> = None;
    let mut report = tokio::time::interval(Duration::from_secs(10));
    report.tick().await;
    loop {
        tokio::select! {
            item = sub.next() => {
                let Some(item) = item else { bail!("stream ended") };
                let (seq, cp) = match item? {
                    Event::Checkpoint { seq, checkpoint } => (seq, checkpoint),
                    Event::Gap { from, to } => {
                        gap_events += 1;
                        eprintln!("GAP EVENT {from}..={to}");
                        prev = Some(to);
                        continue;
                    }
                };
                if let Some(p) = prev {
                    if seq <= p { dupes += 1; eprintln!("DUP/REORDER {seq} after {p}"); }
                    else if seq != p + 1 { gaps += 1; eprintln!("SILENT SKIP {}..={}", p + 1, seq - 1); }
                }
                first.get_or_insert(seq);
                prev = Some(seq);
                count += 1;
                if let Some(ts) = cp.summary.as_ref().and_then(|s| s.timestamp) {
                    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
                    let l = now - (ts.seconds * 1000 + i64::from(ts.nanos) / 1_000_000);
                    lag_ms = Some(l);
                    if caught_up.is_none() && l < 5_000 {
                        caught_up = Some(started.elapsed().as_secs_f64());
                        eprintln!("caught up after {:.1}s ({count} checkpoints)", caught_up.unwrap());
                    }
                } else if !timestamps && caught_up.is_none() && back.is_none() && start.is_none() {
                    caught_up = Some(0.0);
                }
            }
            _ = report.tick() => {
                let rate = (count - window.1) as f64 / window.0.elapsed().as_secs_f64();
                window = (Instant::now(), count);
                eprintln!(
                    "[{:.0}s] count {count} last {prev:?} rate {rate:.1}/s lag {lag_ms:?}ms gap_events {gap_events} skips {gaps} dupes {dupes}",
                    started.elapsed().as_secs_f64()
                );
            }
            _ = tokio::time::sleep_until(deadline) => break,
        }
    }
    println!(
        "RESULT secs {:.0} count {count} first {first:?} last {prev:?} gap_events {gap_events} skips {gaps} dupes {dupes} caught_up {caught_up:?} cursor {:?}",
        started.elapsed().as_secs_f64(),
        sub.cursor()
    );
    Ok(())
}
