//! The client against a local mock gateway: resume headers, encodings,
//! reconnects, retryable vs fatal statuses.

use std::collections::VecDeque;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::StreamExt;
use suistream::{Event, ResumeWith};

/// One scripted response.
#[derive(Clone)]
enum Reply {
    /// 200 with these SSE events, then close (or hang if `hang`).
    Events {
        body: String,
        hang: bool,
    },
    Status(StatusCode, Option<&'static str>),
}

/// (Last-Event-ID, from, read_mask, Accept-Encoding) of a request.
type Seen = (Option<String>, Option<String>, Option<String>, String);

#[derive(Clone, Default)]
struct Mock {
    replies: Arc<Mutex<VecDeque<Reply>>>,
    requests: Arc<Mutex<Vec<Seen>>>,
    /// Response bodies dropped (the client went away or the body ended).
    closed: Arc<AtomicUsize>,
}

/// Counts the body as closed when dropped.
struct Guard(Arc<AtomicUsize>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(serde::Deserialize)]
struct Params {
    from: Option<String>,
    read_mask: Option<String>,
}

async fn handler(State(m): State<Mock>, headers: HeaderMap, Query(p): Query<Params>) -> Response {
    let h = |n: &str| headers.get(n).map(|v| v.to_str().unwrap().to_owned());
    let accept = h("accept-encoding").unwrap_or_default();
    m.requests
        .lock()
        .unwrap()
        .push((h("last-event-id"), p.from, p.read_mask, accept.clone()));
    let reply = m.replies.lock().unwrap().pop_front();
    match reply {
        None => {
            // Hang with heartbeats only.
            Body::from_stream(futures::stream::pending::<Result<Vec<u8>, std::io::Error>>())
                .into_response()
        }
        Some(Reply::Status(code, retry_after)) => {
            let mut r = (code, "nope").into_response();
            if let Some(v) = retry_after {
                r.headers_mut()
                    .insert(header::RETRY_AFTER, v.parse().unwrap());
            }
            r
        }
        Some(Reply::Events { body, hang }) => {
            // Compress like the gateway: one zstd frame per event.
            let (bytes, encoding) = if accept.contains("zstd") {
                let mut out = vec![];
                for ev in body.split_inclusive("\n\n") {
                    out.extend(zstd::bulk::compress(ev.as_bytes(), 3).unwrap());
                }
                (out, Some("zstd"))
            } else if accept.contains("gzip") {
                let mut e = flate2::write::GzEncoder::new(vec![], flate2::Compression::fast());
                e.write_all(body.as_bytes()).unwrap();
                (e.finish().unwrap(), Some("gzip"))
            } else {
                (body.into_bytes(), None)
            };
            let first = futures::stream::iter([Ok::<_, std::io::Error>(bytes)]);
            let guard = Guard(m.closed.clone());
            let stream = if hang {
                first.chain(futures::stream::pending()).boxed()
            } else {
                first.boxed()
            }
            .map(move |c| {
                let _ = &guard;
                c
            });
            let mut r = Response::new(Body::from_stream(stream));
            r.headers_mut()
                .insert(header::CONTENT_TYPE, "text/event-stream".parse().unwrap());
            if let Some(e) = encoding {
                r.headers_mut()
                    .insert(header::CONTENT_ENCODING, e.parse().unwrap());
            }
            r
        }
    }
}

impl Mock {
    async fn serve(&self, replies: Vec<Reply>) -> String {
        *self.replies.lock().unwrap() = replies.into();
        let app = Router::new()
            .route("/v1/checkpoints", get(handler))
            .with_state(self.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/v1/checkpoints")
    }
}

fn cp(seq: u64) -> String {
    // An unknown field must be ignored (newer server protos).
    format!(
        "id: {seq}\nevent: checkpoint\ndata: {{\"sequenceNumber\":\"{seq}\",\"digest\":\"d{seq}\",\"someFutureField\":{{\"x\":1}}}}\n\n"
    )
}

fn gap(from: u64, to: u64) -> String {
    format!("event: gap\ndata: {{\"from\":{from},\"to\":{to}}}\n\n")
}

fn events(seqs: &[String], hang: bool) -> Reply {
    Reply::Events {
        body: format!("retry: 1000\n\n: keepalive\n\n{}", seqs.concat()),
        hang,
    }
}

async fn take(sub: &mut suistream::Subscription, n: usize) -> Vec<String> {
    let mut out = vec![];
    while out.len() < n {
        let ev = tokio::time::timeout(Duration::from_secs(20), sub.next())
            .await
            .expect("progress")
            .expect("open");
        out.push(match ev {
            Ok(Event::Checkpoint { seq, checkpoint }) => {
                assert_eq!(
                    checkpoint.digest.as_deref(),
                    Some(format!("d{seq}").as_str())
                );
                seq.to_string()
            }
            Ok(Event::Gap { from, to }) => format!("gap {from}-{to}"),
            Err(e) => format!("error {e}"),
        });
    }
    out
}

#[tokio::test]
async fn resumes_with_last_event_id_across_closes_and_gaps() {
    let mock = Mock::default();
    let url = mock
        .serve(vec![
            events(&[cp(10), cp(11)], false),
            Reply::Status(StatusCode::SERVICE_UNAVAILABLE, None),
            events(&[cp(11), cp(12), gap(13, 15)], false),
            events(&[cp(16)], true),
        ])
        .await;
    let mut sub = suistream::Client::builder()
        .url(url)
        .read_mask(["sequence_number", "digest"])
        .start_at(10)
        .build()
        .unwrap()
        .subscribe();
    assert_eq!(
        take(&mut sub, 5).await,
        ["10", "11", "12", "gap 13-15", "16"]
    );
    assert_eq!(sub.last_delivered(), Some(16));
    assert_eq!(sub.cursor(), Some(16));
    let reqs = mock.requests.lock().unwrap().clone();
    let ids: Vec<_> = reqs.iter().map(|r| r.0.clone()).collect();
    assert_eq!(
        ids,
        [
            Some("9".into()),
            Some("11".into()),
            Some("11".into()),
            Some("15".into())
        ]
    );
    for r in &reqs {
        assert_eq!(r.2.as_deref(), Some("sequence_number,digest"));
        assert!(r.3.contains("zstd") && r.3.contains("gzip"), "{}", r.3);
    }
}

#[tokio::test]
async fn query_resume_and_tip_start() {
    let mock = Mock::default();
    let url = mock
        .serve(vec![events(&[cp(100)], false), events(&[cp(101)], true)])
        .await;
    let mut sub = suistream::Client::builder()
        .url(url)
        .resume_with(ResumeWith::Query)
        .build()
        .unwrap()
        .subscribe();
    assert_eq!(take(&mut sub, 2).await, ["100", "101"]);
    let reqs = mock.requests.lock().unwrap().clone();
    assert_eq!((reqs[0].0.clone(), reqs[0].1.clone()), (None, None));
    assert_eq!(
        (reqs[1].0.clone(), reqs[1].1.clone()),
        (None, Some("101".into()))
    );
    // No mask: full checkpoint.
    assert_eq!(reqs[0].2, None);
}

#[tokio::test]
async fn gzip_only_server() {
    let mock = Mock::default();
    let url = mock.serve(vec![events(&[cp(1), cp(2)], true)]).await;
    let mut sub = suistream::Client::builder()
        .url(url)
        .header("accept-encoding", "gzip")
        .start_at(1)
        .build()
        .unwrap()
        .subscribe();
    assert_eq!(take(&mut sub, 2).await, ["1", "2"]);
    assert_eq!(mock.requests.lock().unwrap()[0].3, "gzip");
}

#[tokio::test]
async fn retry_after_is_honoured_and_400_is_fatal() {
    let mock = Mock::default();
    let url = mock
        .serve(vec![
            Reply::Status(StatusCode::TOO_MANY_REQUESTS, Some("2")),
            events(&[cp(5)], false),
            Reply::Status(StatusCode::BAD_REQUEST, None),
        ])
        .await;
    let t0 = std::time::Instant::now();
    let mut sub = suistream::Client::builder()
        .url(url)
        .start_at(5)
        .build()
        .unwrap()
        .subscribe();
    let got = take(&mut sub, 2).await;
    assert_eq!(got[0], "5");
    assert!(
        t0.elapsed() >= Duration::from_secs(2),
        "waited for Retry-After"
    );
    assert!(got[1].starts_with("error stream rejected the request: HTTP 400"));
    assert!(sub.next().await.is_none(), "ends after a fatal error");
}

#[tokio::test]
async fn stall_triggers_reconnect() {
    let mock = Mock::default();
    // The first connection sends one checkpoint and then nothing.
    let url = mock
        .serve(vec![events(&[cp(7)], true), events(&[cp(8)], true)])
        .await;
    let mut sub = suistream::Client::builder()
        .url(url)
        .start_at(7)
        .stall_timeout(Duration::from_millis(500))
        .build()
        .unwrap()
        .subscribe();
    assert_eq!(take(&mut sub, 2).await, ["7", "8"]);
    assert_eq!(mock.requests.lock().unwrap()[1].0.as_deref(), Some("7"));
}

#[tokio::test]
async fn dropping_the_subscription_closes_the_connection() {
    let mock = Mock::default();
    let url = mock.serve(vec![events(&[cp(1)], true)]).await;
    let mut sub = suistream::Client::builder()
        .url(url)
        .start_at(1)
        .build()
        .unwrap()
        .subscribe();
    assert_eq!(take(&mut sub, 1).await, ["1"]);
    assert_eq!(mock.closed.load(Ordering::SeqCst), 0);
    drop(sub);
    let t0 = std::time::Instant::now();
    while mock.closed.load(Ordering::SeqCst) == 0 {
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "connection still open"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(mock.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn slow_consumer_is_not_a_stall() {
    let mock = Mock::default();
    let url = mock.serve(vec![events(&[cp(1), cp(2), cp(3)], true)]).await;
    let mut sub = suistream::Client::builder()
        .url(url)
        .stall_timeout(Duration::from_millis(100))
        .build()
        .unwrap()
        .subscribe();
    for want in ["1", "2", "3"] {
        assert_eq!(take(&mut sub, 1).await, [want]);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert_eq!(mock.requests.lock().unwrap().len(), 1);
}
