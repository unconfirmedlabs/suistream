//! `checkpoint-ingester`
//!
//! Environment:
//! - Source name: first argument, or `SOURCE` (required).
//! - `SOURCES_FILE`: path to `sources.toml` (default `/etc/checkpoint-stream/sources.toml`).
//! - `NATS_URL`, `NATS_NKEY_SEED`: see `common::nats`.
//! - `METRICS_ADDR`: Prometheus listener (default `[::]:9091`).
//! - `RING_BYTES`: compressed bytes of recent frames kept for fetches (default 128 MiB).
//! - `FLY_REGION`: region label (default `local`).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use ingester::metrics::Metrics;
use ingester::publish::{Publisher, Ring};
use ingester::upstream::Upstream;
use tokio::sync::{mpsc, watch};

#[tokio::main]
async fn main() -> Result<()> {
    common::init_tracing();
    let source_name = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("SOURCE").ok())
        .context("source name argument or SOURCE is required")?;
    let path = std::env::var("SOURCES_FILE")
        .unwrap_or_else(|_| "/etc/checkpoint-stream/sources.toml".to_owned());
    let file = common::config::SourcesFile::parse(
        &std::fs::read_to_string(&path).with_context(|| format!("read {path}"))?,
    )?;
    let source = file.resolve(&source_name, |k| std::env::var(k).ok())?;
    let region = common::region();
    common::config::validate_token(&region).context("FLY_REGION")?;
    let metrics_addr = std::env::var("METRICS_ADDR")
        .unwrap_or_else(|_| "[::]:9091".to_owned())
        .parse()
        .context("METRICS_ADDR")?;
    let ring_bytes = std::env::var("RING_BYTES")
        .ok()
        .map(|v| v.parse())
        .transpose()
        .context("RING_BYTES")?
        .unwrap_or(128 * 1024 * 1024);

    tracing::info!(source = %source.name, url = %source.url, %region, reconnect_before = ?source.reconnect_before, "starting ingester");
    let metrics = Metrics::new(&source.name, &region);
    let upstream = Upstream::new(source.clone())?;
    let nats = common::nats::connect(&format!("ingester-{}", source.name), None).await?;

    let publisher = Publisher {
        nats: nats.clone(),
        subject: common::checkpoint_subject(&source.name, &region),
        source: source.name.clone(),
        region: region.clone(),
        ring: Arc::new(Mutex::new(Ring::new(ring_bytes))),
        metrics: metrics.clone(),
    };
    let (latest_tx, latest_rx) = watch::channel(None);
    // Bounded hand-off (decoded checkpoints are several MB each): the stream
    // reader drops, and the publisher later backfills, rather than ever
    // blocking on NATS.
    let (live_tx, live_rx) = mpsc::channel(32);

    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(common::metrics::serve(
        metrics.registry.clone(),
        metrics_addr,
    ));
    tasks.spawn({
        let (up, m) = (upstream.clone(), metrics.clone());
        async move {
            ingester::stream::run(up, live_tx, m).await;
            Ok(())
        }
    });
    tasks.spawn({
        let (p, up) = (publisher.clone(), upstream.clone());
        async move {
            ingester::publish::run(p, up, live_rx, latest_tx).await;
            Ok(())
        }
    });
    tasks.spawn(ingester::fetch::run(
        publisher.clone(),
        upstream.clone(),
        latest_rx.clone(),
    ));
    tasks.spawn(health(
        nats.clone(),
        source.name.clone(),
        region.clone(),
        latest_rx,
        metrics.clone(),
    ));
    tasks.spawn(server_version(upstream, metrics));

    let shutdown = async {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = term.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
        }
        anyhow::Ok(())
    };
    tokio::select! {
        r = tasks.join_next() => {
            let r = r.expect("tasks");
            anyhow::bail!("task exited: {:?}", r.map(|r| r.err()));
        }
        _ = shutdown => {
            tracing::info!("shutting down");
            let _ = nats.flush().await;
            Ok(())
        }
    }
}

/// Publish this ingester's beacon and track the max checkpoint any ingester
/// announces (lag metric).
async fn health(
    nats: async_nats::Client,
    source: String,
    region: String,
    latest: watch::Receiver<Option<u64>>,
    metrics: Metrics,
) -> Result<()> {
    let subject = common::health_subject(&source, &region);
    let mut peers = nats.subscribe(common::HEALTH_SUBJECT_ALL).await?;
    let mut tick = tokio::time::interval(common::HEALTH_INTERVAL);
    let mut peer_max: Option<(u64, tokio::time::Instant)> = None;
    let mut stall = common::StallDetector::new(Duration::from_secs(120), 100);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let beacon = common::Health {
                    source: source.clone(),
                    region: region.clone(),
                    checkpoint: *latest.borrow(),
                    timestamp_ms: common::unix_millis(),
                };
                if let Err(e) = nats.publish(subject.clone(), serde_json::to_vec(&beacon)?.into()).await {
                    tracing::warn!("health publish: {e}");
                }
                // Forget a stale max (e.g. all peers gone) after 30s.
                if peer_max.is_some_and(|(_, at)| at.elapsed() > Duration::from_secs(30)) {
                    peer_max = None;
                }
                let own = latest.borrow().unwrap_or(0) as i64;
                let max = peer_max.map_or(own, |(m, _)| (m as i64).max(own));
                metrics.peer_max_seq.set(max);
                metrics.lag_behind_peers.set(max - own);
                if stall.stalled(*latest.borrow(), peer_max.map(|(m, _)| m), std::time::Instant::now()) {
                    tracing::error!("no progress for 120s while peers advanced; exiting for restart");
                    std::process::exit(1);
                }
            }
            Some(msg) = peers.next() => {
                if let Ok(beacon) = serde_json::from_slice::<common::Health>(&msg.payload)
                    && let Some(seq) = beacon.checkpoint
                    && peer_max.is_none_or(|(m, _)| seq >= m)
                {
                    peer_max = Some((seq, tokio::time::Instant::now()));
                }
            }
        }
    }
}

/// Export the upstream node version as `upstream_info{server=...}`.
async fn server_version(upstream: Upstream, metrics: Metrics) -> Result<()> {
    let mut current: Option<String> = None;
    loop {
        match upstream.server_version().await {
            Ok(version) if current.as_ref() != Some(&version) => {
                if let Some(old) = current.take() {
                    let _ = metrics.upstream_info.remove_label_values(&[&old]);
                }
                tracing::info!(%version, "upstream server version");
                metrics.upstream_info.with_label_values(&[&version]).set(1);
                current = Some(version);
            }
            Ok(_) => {}
            Err(status) => tracing::debug!("GetServiceInfo: {}", status.message()),
        }
        tokio::time::sleep(Duration::from_secs(300)).await;
    }
}
