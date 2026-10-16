//! CORS for `/v1/checkpoints`: any origin may `GET` the stream, and
//! preflights for the headers a browser client may add (`Last-Event-ID` to
//! resume, `Authorization` for keys) are answered. Simple requests
//! (`?from=` resumes, no custom headers) need no preflight.

use std::time::Duration;

use axum::http::{HeaderName, Method, header};
use tower_http::cors::{AllowOrigin, CorsLayer};

pub const MAX_AGE: Duration = Duration::from_secs(86_400);

pub fn layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(AllowOrigin::any())
        .allow_methods([Method::GET, Method::OPTIONS])
        .allow_headers([
            HeaderName::from_static("last-event-id"),
            header::AUTHORIZATION,
            header::ACCEPT,
        ])
        .max_age(MAX_AGE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use tower::ServiceExt;

    fn app() -> Router {
        // Same construction as the gateway's router.
        Router::new().route("/v1/checkpoints", get(|| async { "data" }).layer(layer()))
    }

    #[tokio::test]
    async fn preflight() {
        let res = app()
            .oneshot(
                Request::options("/v1/checkpoints")
                    .header(header::ORIGIN, "https://example.com")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header(
                        header::ACCESS_CONTROL_REQUEST_HEADERS,
                        "last-event-id,authorization",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let h = res.headers();
        assert_eq!(h[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        assert_eq!(h[header::ACCESS_CONTROL_MAX_AGE], "86400");
        let methods = h[header::ACCESS_CONTROL_ALLOW_METHODS].to_str().unwrap();
        assert!(methods.contains("GET") && methods.contains("OPTIONS"));
        let allowed = h[header::ACCESS_CONTROL_ALLOW_HEADERS].to_str().unwrap();
        for name in ["last-event-id", "authorization", "accept"] {
            assert!(allowed.contains(name), "{allowed}");
        }
    }

    #[tokio::test]
    async fn simple_get_has_one_allow_origin() {
        let res = app()
            .oneshot(
                Request::get("/v1/checkpoints")
                    .header(header::ORIGIN, "https://example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let origins: Vec<_> = res
            .headers()
            .get_all(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .iter()
            .collect();
        assert_eq!(origins, ["*"]);
    }
}
