//! `checkpoint-gateway`: public SSE stream of Sui mainnet checkpoints.
//!
//! Environment (defaults in brackets):
//! - `LISTEN_ADDR` [`[::]:8080`], `METRICS_ADDR` [`[::]:9091`]
//! - `NATS_URL`, `NATS_NKEY_SEED`: see `common::nats`
//! - `PRIMARY_SUBJECTS_<REGION>` or `PRIMARY_SUBJECTS` [`sui.mainnet.ckpt.>`]:
//!   comma-separated checkpoint subjects for this region
//! - `FALLBACK_SUBJECT` [`sui.mainnet.ckpt.>`]
//! - `MAX_STREAMS` [25] compressed streams, `MAX_IDENTITY_STREAMS` [5],
//!   `MAX_MASKS` [64]
//! - `LAG_CAPACITY` [32] checkpoints a stream may fall behind
//! - `RING_BYTES` [128 MiB] of compressed checkpoints kept for resume
//! - `MAX_INPUT_BYTES` [256 MiB] buffered between receive and sequencer
//! - `READY_MAX_BEHIND` [20] checkpoints behind the ingesters before `/readyz` fails
//!   (once the first checkpoint arrived; a cold gateway is ready as soon as
//!   NATS is connected)
//! - `BUCKET_URL` [`https://checkpoints.mainnet.sui.io`]: checkpoint history;
//!   empty disables history (resumes older than the ring get a `gap`)
//! - `BACKFILL_FETCH_CONCURRENCY` [16] bucket GETs in flight per backfill
//! - `BACKFILL_RENDER_CONCURRENCY` [1] bucket renders per machine (one
//!   vCPU of a shared-cpu-2x; the other stays free for live streams)
//! - `MAX_BACKFILL_STREAMS` [8] streams backfilling at once per machine
//!   (more → 429)
//! - `MAX_BACKFILL_CHECKPOINTS` [16000] (~1 h): resumes further behind the
//!   head get a `gap` for the older part
//! - `IDLE_EXIT_AFTER` [unset = disabled]: exit (code 0) after this long
//!   without streams, e.g. `5m`, `300s`, `300`

mod cors;
mod encoding;
mod fetch;
mod history;
mod hub;
mod limits;
mod mask;
mod metrics;
mod receive;
mod sequencer;
mod sse;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use futures::StreamExt;
use tokio::sync::{mpsc, watch};

use crate::encoding::Encoding;
use crate::history::History;
use crate::hub::Hub;
use crate::limits::{Limits, LimitsConfig};
use crate::metrics::Metrics;
use crate::sequencer::Head;

pub struct App {
    hub: Arc<Hub>,
    limits: Limits,
    head: Arc<Head>,
    /// Max checkpoint announced by any ingester, and when (unix ms).
    health_max: AtomicU64,
    health_at_ms: AtomicU64,
    ready_max_behind: u64,
    nats: async_nats::Client,
    metrics: Metrics,
    shutdown: watch::Receiver<bool>,
    statics: HashMap<Encoding, sse::Statics>,
    history: Option<Arc<History>>,
    /// Open streams, and when the count last changed (unix ms).
    active_streams: AtomicUsize,
    streams_changed_ms: AtomicU64,
}

/// Counts an open stream (for idle exit) while alive.
pub struct StreamGuard(Arc<App>);

impl StreamGuard {
    pub fn new(app: Arc<App>) -> Self {
        let n = app.active_streams.fetch_add(1, Ordering::SeqCst) + 1;
        app.metrics.active_streams.set(n as i64);
        app.streams_changed_ms
            .store(common::unix_millis(), Ordering::SeqCst);
        Self(app)
    }
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        let n = self.0.active_streams.fetch_sub(1, Ordering::SeqCst) - 1;
        self.0.metrics.active_streams.set(n as i64);
        self.0
            .streams_changed_ms
            .store(common::unix_millis(), Ordering::SeqCst);
    }
}

/// `5m`, `90s`, `1h`, `300` (seconds); `0` or empty → `None`.
fn parse_duration(v: &str) -> Result<Option<Duration>> {
    let v = v.trim();
    if v.is_empty() {
        return Ok(None);
    }
    let (num, unit) = match v.find(|c: char| !c.is_ascii_digit()) {
        Some(i) => v.split_at(i),
        None => (v, "s"),
    };
    let n: u64 = num.parse().with_context(|| format!("duration {v:?}"))?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        _ => anyhow::bail!("duration {v:?}: unit must be s, m or h"),
    };
    Ok((secs > 0).then(|| Duration::from_secs(secs)))
}

fn env<T: FromStr>(name: &str, default: T) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(v) => v.parse().map_err(|e| anyhow::anyhow!("{name}={v:?}: {e}")),
        Err(_) => Ok(default),
    }
}

fn primary_subjects(region: &str) -> Vec<String> {
    let list = std::env::var(format!("PRIMARY_SUBJECTS_{}", region.to_ascii_uppercase()))
        .or_else(|_| std::env::var("PRIMARY_SUBJECTS"))
        .unwrap_or_else(|_| common::CHECKPOINT_SUBJECT_ALL.to_owned());
    list.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    common::init_tracing();
    let region = common::region();
    let listen: SocketAddr = env("LISTEN_ADDR", "[::]:8080".parse()?)?;
    let metrics_addr: SocketAddr = env("METRICS_ADDR", "[::]:9091".parse()?)?;
    let limits = Limits::new(LimitsConfig {
        max_compressed: env("MAX_STREAMS", 25)?,
        max_identity: env("MAX_IDENTITY_STREAMS", 5)?,
        max_masks: env("MAX_MASKS", 64)?,
    });
    let lag_capacity: usize = env("LAG_CAPACITY", 32)?;
    let ring_bytes: usize = env("RING_BYTES", 128 * 1024 * 1024)?;
    let max_input_bytes: usize = env("MAX_INPUT_BYTES", 256 * 1024 * 1024)?;
    let primary = primary_subjects(&region);
    let fallback = std::env::var("FALLBACK_SUBJECT")
        .unwrap_or_else(|_| common::CHECKPOINT_SUBJECT_ALL.to_owned());

    let metrics = Metrics::new(&region);
    let bucket_url =
        std::env::var("BUCKET_URL").unwrap_or_else(|_| history::Config::default().bucket_url);
    let history = if bucket_url.trim().is_empty() {
        tracing::warn!("BUCKET_URL empty: history disabled");
        None
    } else {
        let d = history::Config::default();
        Some(Arc::new(History::new(
            history::Config {
                bucket_url: bucket_url.trim_end_matches('/').to_owned(),
                fetch_concurrency: env("BACKFILL_FETCH_CONCURRENCY", d.fetch_concurrency)?,
                render_concurrency: env("BACKFILL_RENDER_CONCURRENCY", d.render_concurrency)?,
                max_streams: env("MAX_BACKFILL_STREAMS", d.max_streams)?,
                max_span: env("MAX_BACKFILL_CHECKPOINTS", d.max_span)?,
                ..d
            },
            metrics.clone(),
        )?))
    };
    let idle_exit_after = parse_duration(&std::env::var("IDLE_EXIT_AFTER").unwrap_or_default())?;
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let nats = common::nats::connect("gateway", Some(events_tx)).await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let app = Arc::new(App {
        hub: Arc::new(Hub::new(lag_capacity, ring_bytes)),
        limits,
        head: Arc::new(Head::default()),
        health_max: AtomicU64::new(0),
        health_at_ms: AtomicU64::new(0),
        ready_max_behind: env("READY_MAX_BEHIND", 20)?,
        nats: nats.clone(),
        metrics: metrics.clone(),
        shutdown: shutdown_rx,
        statics: [Encoding::Identity, Encoding::Zstd, Encoding::Gzip]
            .into_iter()
            .map(|e| (e, sse::Statics::new(e)))
            .collect(),
        history: history.clone(),
        active_streams: AtomicUsize::new(0),
        streams_changed_ms: AtomicU64::new(common::unix_millis()),
    });

    let input_bytes = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::unbounded_channel();
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(common::metrics::serve(
        metrics.registry.clone(),
        metrics_addr,
    ));
    tasks.spawn(
        receive::Receiver {
            nats: nats.clone(),
            primary,
            fallback,
            tx: tx.clone(),
            input_bytes: input_bytes.clone(),
            max_input_bytes,
            head: app.head.clone(),
            metrics: metrics.clone(),
        }
        .run(events_rx),
    );
    tasks.spawn({
        let fetcher = fetch::Fetcher {
            nats: nats.clone(),
            timeout: Duration::from_secs(2),
            input_bytes: input_bytes.clone(),
            metrics: metrics.clone(),
        };
        let (hub, head, metrics) = (app.hub.clone(), app.head.clone(), metrics.clone());
        async move {
            sequencer::run(rx, tx, input_bytes, hub, head, fetcher, history, metrics).await;
            anyhow::bail!("sequencer stopped")
        }
    });
    tasks.spawn(track_health(app.clone()));
    tasks.spawn(stall_watchdog(app.clone()));
    let idle = Arc::new(tokio::sync::Notify::new());
    if let Some(after) = idle_exit_after {
        tasks.spawn(idle_exit(app.clone(), after, idle.clone()));
    }

    let router = Router::new()
        .route(
            "/v1/checkpoints",
            get(sse::checkpoints).layer(cors::layer()),
        )
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .with_state(app.clone());
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, %region, history = app.history.is_some(), ?idle_exit_after, "serving");
    let server = axum::serve(listener, router).with_graceful_shutdown(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("signal handler");
        tokio::select! {
            _ = term.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
            _ = idle.notified() => {},
        }
        // Fail readiness and end every stream; clients reconnect elsewhere
        // with Last-Event-ID.
        tracing::info!("draining");
        let _ = shutdown_tx.send(true);
        tokio::time::sleep(Duration::from_millis(500)).await;
    });

    tokio::select! {
        r = server => r.context("server"),
        r = tasks.join_next() => {
            anyhow::bail!("task exited: {:?}", r.map(|r| r.map(|r| r.err())))
        }
    }
}

/// Track the max checkpoint any ingester has announced.
async fn track_health(app: Arc<App>) -> Result<()> {
    let mut sub = app.nats.subscribe(common::HEALTH_SUBJECT_ALL).await?;
    while let Some(msg) = sub.next().await {
        let Ok(beacon) = serde_json::from_slice::<common::Health>(&msg.payload) else {
            continue;
        };
        let Some(seq) = beacon.checkpoint else {
            continue;
        };
        let now = common::unix_millis();
        // A max older than 30s is stale (e.g. all ingesters gone).
        let stale = now.saturating_sub(app.health_at_ms.load(Ordering::Relaxed)) > 30_000;
        if stale || seq >= app.health_max.load(Ordering::Relaxed) {
            app.health_max.store(seq, Ordering::Relaxed);
            app.health_at_ms.store(now, Ordering::Relaxed);
            app.metrics.health_max.set(seq as i64);
        }
    }
    anyhow::bail!("health subscription ended")
}

/// Exit (and be restarted by Fly) if this gateway stops emitting while the
/// ingesters keep advancing.
async fn stall_watchdog(app: Arc<App>) -> Result<()> {
    let mut stall = common::StallDetector::new(Duration::from_secs(120), 100);
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    loop {
        tick.tick().await;
        let fresh = common::unix_millis().saturating_sub(app.health_at_ms.load(Ordering::Relaxed))
            <= 30_000;
        let peers = fresh.then(|| app.health_max.load(Ordering::Relaxed));
        if stall.stalled(app.head.last(), peers, std::time::Instant::now()) {
            tracing::error!("no progress for 120s while ingesters advanced; exiting for restart");
            std::process::exit(1);
        }
    }
}

/// Exit cleanly once no stream has been open for `after`; Fly's proxy
/// starts the machine again on the next request.
async fn idle_exit(app: Arc<App>, after: Duration, notify: Arc<tokio::sync::Notify>) -> Result<()> {
    let mut tick = tokio::time::interval(Duration::from_secs(1).min(after));
    loop {
        tick.tick().await;
        let idle_ms =
            common::unix_millis().saturating_sub(app.streams_changed_ms.load(Ordering::SeqCst));
        if app.active_streams.load(Ordering::SeqCst) == 0 && idle_ms >= after.as_millis() as u64 {
            tracing::info!(idle_s = idle_ms / 1000, "idle: exiting");
            notify.notify_one();
            // The server finishes draining; this task just stops ticking.
            std::future::pending::<()>().await;
        }
    }
}

/// Ready unless draining, disconnected from NATS, or behind the ingesters.
/// A gateway that has not received its first checkpoint yet is ready (cold
/// start: streams wait for it, resumes come from the bucket). Never fails
/// merely because the chain (or every upstream) stalled.
async fn readyz(State(app): State<Arc<App>>) -> (StatusCode, String) {
    if *app.shutdown.borrow() {
        return (StatusCode::SERVICE_UNAVAILABLE, "draining".into());
    }
    if app.nats.connection_state() != async_nats::connection::State::Connected {
        return (StatusCode::SERVICE_UNAVAILABLE, "nats disconnected".into());
    }
    let head = app.head.last();
    let fresh =
        common::unix_millis().saturating_sub(app.health_at_ms.load(Ordering::Relaxed)) <= 30_000;
    if fresh && let Some(head) = head {
        let max = app.health_max.load(Ordering::Relaxed);
        let behind = max.saturating_sub(head);
        if behind > app.ready_max_behind {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("behind ingesters: head {head:?}, ingesters {max}"),
            );
        }
    }
    (
        StatusCode::OK,
        format!(
            "head {head:?} streams {}\n",
            app.active_streams.load(Ordering::Relaxed)
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("").unwrap(), None);
        assert_eq!(parse_duration("0").unwrap(), None);
        assert_eq!(parse_duration("0m").unwrap(), None);
        assert_eq!(
            parse_duration("300").unwrap(),
            Some(Duration::from_secs(300))
        );
        assert_eq!(
            parse_duration("90s").unwrap(),
            Some(Duration::from_secs(90))
        );
        assert_eq!(
            parse_duration("5m").unwrap(),
            Some(Duration::from_secs(300))
        );
        assert_eq!(
            parse_duration("1h").unwrap(),
            Some(Duration::from_secs(3600))
        );
        assert!(parse_duration("5d").is_err());
        assert!(parse_duration("m").is_err());
    }

    #[tokio::test]
    async fn idle_exit_waits_for_streams_to_end() {
        let app = sse::tests::test_app(None, 10).await;
        let notify = Arc::new(tokio::sync::Notify::new());
        let guard = StreamGuard::new(app.clone());
        tokio::spawn(idle_exit(
            app.clone(),
            Duration::from_millis(300),
            notify.clone(),
        ));
        // A stream is open: never idle.
        let early = tokio::time::timeout(Duration::from_millis(1_500), notify.notified()).await;
        assert!(early.is_err());
        drop(guard);
        let t0 = std::time::Instant::now();
        tokio::time::timeout(Duration::from_secs(3), notify.notified())
            .await
            .expect("idle exit after the last stream closed");
        assert!(t0.elapsed() >= Duration::from_millis(250));
    }
}
