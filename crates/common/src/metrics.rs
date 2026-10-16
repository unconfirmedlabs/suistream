//! Prometheus `/metrics` endpoint on a private port.

use prometheus::Registry;

/// Serve `/metrics` on `addr`.
pub async fn serve(registry: Registry, addr: std::net::SocketAddr) -> anyhow::Result<()> {
    use axum::{Router, routing::get};
    let app = Router::new().route(
        "/metrics",
        get(move || {
            let registry = registry.clone();
            async move {
                let mut out = String::new();
                prometheus::TextEncoder::new()
                    .encode_utf8(&registry.gather(), &mut out)
                    .map(|_| out)
                    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
