//! NATS client setup shared by ingesters and gateways.
//!
//! - `NATS_URL`: comma-separated servers, tried in order (in-region first),
//!   e.g. `nats://nats-a:4222,nats://nats-b:4222`.
//!   `{region}` is replaced with `FLY_REGION`.
//! - `NATS_NKEY_SEED`: nkey seed for this role (optional for local runs).

use std::time::Duration;

use anyhow::{Context, Result};

/// Connect with failover across the configured servers. Connection events
/// are logged and, if `events` is given, forwarded.
pub async fn connect(
    name: &str,
    events: Option<tokio::sync::mpsc::UnboundedSender<async_nats::Event>>,
) -> Result<async_nats::Client> {
    let urls = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_owned());
    let urls = urls.replace("{region}", &crate::region());
    let servers: Vec<async_nats::ServerAddr> = urls
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().with_context(|| format!("NATS server {s:?}")))
        .collect::<Result<_>>()?;

    let mut options = match std::env::var("NATS_NKEY_SEED") {
        Ok(seed) if !seed.is_empty() => async_nats::ConnectOptions::with_nkey(seed),
        _ => async_nats::ConnectOptions::new(),
    };
    options = options
        .name(format!("{name}-{}", crate::region()))
        .ping_interval(Duration::from_secs(20))
        .connection_timeout(Duration::from_secs(5))
        .max_reconnects(None)
        .retain_servers_order()
        .ignore_discovered_servers()
        .retry_on_initial_connect()
        .subscription_capacity(2048)
        .event_callback(move |event| {
            let events = events.clone();
            async move {
                match &event {
                    async_nats::Event::Connected => tracing::info!("nats: connected"),
                    async_nats::Event::Disconnected => tracing::warn!("nats: disconnected"),
                    other => tracing::warn!("nats: {other}"),
                }
                if let Some(tx) = events {
                    let _ = tx.send(event);
                }
            }
        });

    tracing::info!(?servers, "connecting to NATS");
    options.connect(servers).await.context("connect to NATS")
}
