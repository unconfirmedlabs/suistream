//! Client for the Sui checkpoint SSE stream
//! (`https://stream.mainnet.sui.mirai.cloud/v1/checkpoints`).
//!
//! The gateway serves history itself (recent checkpoints from its ring,
//! older ones from the checkpoint bucket), so this client only has to keep a
//! connection alive. A [`Subscription`] yields [`Event`]s strictly in
//! sequence order, without duplicates:
//!
//! - reconnects with `Last-Event-ID` (or `?from=`) and exponential backoff
//!   (1 s doubling to 30 s, at least the server's `Retry-After`), and treats
//!   45 s without bytes (heartbeats arrive every 15 s) as a dead connection;
//! - surfaces `gap` events (checkpoints the server cannot deliver, e.g.
//!   older than its history limit) as [`Event::Gap`]; the subscription then
//!   continues after the gap.
//!
//! ```no_run
//! use futures::StreamExt;
//! # async fn run() -> Result<(), suistream::Error> {
//! let mut sub = suistream::Client::builder()
//!     .read_mask(["sequence_number", "digest"])
//!     .resume_after(334_000_000)
//!     .build()?
//!     .subscribe();
//! while let Some(event) = sub.next().await {
//!     match event? {
//!         suistream::Event::Checkpoint { seq, checkpoint } => {
//!             println!("{seq} {:?}", checkpoint.digest);
//!         }
//!         suistream::Event::Gap { from, to } => eprintln!("lost {from}..={to}"),
//!     }
//!     // persist sub.cursor() to resume later with resume_after()
//! }
//! # Ok(()) }
//! ```

mod driver;
mod sse;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

pub use sui_rpc::proto::sui::rpc::v2::Checkpoint;

/// Production Mainnet stream.
pub const DEFAULT_STREAM_URL: &str = "https://stream.mainnet.sui.mirai.cloud/v1/checkpoints";

/// Errors. Any error yielded by a [`Subscription`] is terminal: the stream
/// ends after it. Transient failures (network, 5xx, 429, stalls, malformed
/// events) are retried internally and logged with `tracing`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid configuration: {0}")]
    Config(String),
    /// The stream rejected the request (400 bad mask, 401/403, 404).
    #[error("stream rejected the request: HTTP {status}: {body}")]
    Http { status: u16, body: String },
}

/// One item of a [`Subscription`].
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    /// The next checkpoint. `seq` comes from the SSE `id`, so it is set even
    /// if the read mask omits `sequence_number`.
    Checkpoint {
        seq: u64,
        checkpoint: Box<Checkpoint>,
    },
    /// Checkpoints `from..=to` cannot be delivered; the stream continues
    /// with `to + 1`.
    Gap { from: u64, to: u64 },
}

/// How a reconnect (or initial start) asks for its position.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ResumeWith {
    /// `Last-Event-ID: <seq - 1>` (default).
    #[default]
    LastEventId,
    /// `?from=<seq>`: no custom header (e.g. behind proxies that strip it).
    Query,
}

/// A configured stream; build with [`Client::builder`]. Cheap to clone; each
/// [`subscribe`](Client::subscribe) opens an independent subscription.
#[derive(Clone)]
pub struct Client {
    source: Arc<sse::HttpSource>,
    start: Option<u64>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("url", &self.source.url())
            .field("start", &self.start)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct Builder {
    url: String,
    mask: Option<Vec<String>>,
    start: Option<u64>,
    headers: Vec<(String, String)>,
    resume_with: ResumeWith,
    stall_timeout: Duration,
}

impl Client {
    pub fn builder() -> Builder {
        Builder {
            url: DEFAULT_STREAM_URL.to_owned(),
            mask: None,
            start: None,
            headers: Vec::new(),
            resume_with: ResumeWith::default(),
            stall_timeout: Duration::from_secs(45),
        }
    }

    /// Start streaming. Must be called within a Tokio runtime; dropping the
    /// [`Subscription`] stops all background work.
    pub fn subscribe(&self) -> Subscription {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let task = tokio::spawn(driver::run(self.source.clone(), self.start, tx));
        Subscription {
            rx,
            task,
            last: None,
            cursor: self.start.and_then(|s| s.checked_sub(1)),
        }
    }
}

/// Normalize a read mask: trimmed, deduplicated, `None` for the full
/// checkpoint (empty, or containing `*`).
fn normalize_mask(paths: &[String]) -> Option<String> {
    let mut out: Vec<&str> = Vec::new();
    for p in paths.iter().map(|p| p.trim()).filter(|p| !p.is_empty()) {
        if p == "*" {
            return None;
        }
        if !out.contains(&p) {
            out.push(p);
        }
    }
    (!out.is_empty()).then(|| out.join(","))
}

impl Builder {
    /// SSE endpoint (default [`DEFAULT_STREAM_URL`]).
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    /// `sui.rpc.v2.Checkpoint` field paths (default: the full canonical
    /// checkpoint). The server validates them.
    pub fn read_mask<I, S>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.mask = Some(paths.into_iter().map(Into::into).collect());
        self
    }

    /// Start at this checkpoint (inclusive). Default: the live tip.
    pub fn start_at(mut self, seq: u64) -> Self {
        self.start = Some(seq);
        self
    }

    /// Start after this checkpoint, e.g. a persisted
    /// [`Subscription::cursor`].
    pub fn resume_after(self, seq: u64) -> Self {
        self.start_at(seq.saturating_add(1))
    }

    /// Header for the SSE request (e.g. `authorization`). Setting
    /// `accept-encoding` replaces the default `gzip, zstd` (both decoded).
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// How the position is sent (default [`ResumeWith::LastEventId`]).
    pub fn resume_with(mut self, how: ResumeWith) -> Self {
        self.resume_with = how;
        self
    }

    /// Reconnect when no bytes arrive for this long (default 45 s; the
    /// server sends heartbeats every 15 s).
    pub fn stall_timeout(mut self, timeout: Duration) -> Self {
        self.stall_timeout = timeout;
        self
    }

    /// Validates the URL and headers.
    pub fn build(self) -> Result<Client, Error> {
        let source = sse::HttpSource::new(
            self.url,
            self.mask.as_deref().and_then(normalize_mask),
            &self.headers,
            self.resume_with,
            self.stall_timeout,
        )?;
        Ok(Client {
            source: Arc::new(source),
            start: self.start,
        })
    }
}

/// Ordered events. Ends only after yielding an [`Error`]. Bounded: when the
/// consumer falls behind, reading from the network pauses.
pub struct Subscription {
    rx: tokio::sync::mpsc::Receiver<Result<Event, Error>>,
    task: tokio::task::JoinHandle<()>,
    last: Option<u64>,
    cursor: Option<u64>,
}

impl Subscription {
    /// Sequence number of the last checkpoint yielded.
    pub fn last_delivered(&self) -> Option<u64> {
        self.last
    }

    /// Everything up to and including this checkpoint has been yielded,
    /// either as a checkpoint or inside a gap. Persist it and pass it to
    /// [`Builder::resume_after`] to continue later.
    pub fn cursor(&self) -> Option<u64> {
        self.cursor
    }
}

impl futures::Stream for Subscription {
    type Item = Result<Event, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = std::task::ready!(self.rx.poll_recv(cx));
        match &item {
            Some(Ok(Event::Checkpoint { seq, .. })) => {
                self.last = Some(*seq);
                self.cursor = Some(*seq);
            }
            Some(Ok(Event::Gap { to, .. })) => self.cursor = Some(*to),
            _ => {}
        }
        Poll::Ready(item)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks() {
        let m = |v: &[&str]| normalize_mask(&v.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        assert_eq!(m(&[]), None);
        assert_eq!(m(&[" ", ""]), None);
        assert_eq!(m(&["digest", "*"]), None);
        assert_eq!(
            m(&[" digest ", "digest", "summary"]).as_deref(),
            Some("digest,summary")
        );
    }

    #[test]
    fn build_validates() {
        assert!(matches!(
            Client::builder().url("not a url").build(),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            Client::builder().header("bad header", "x").build(),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            Client::builder().header("x-token", "a\nb").build(),
            Err(Error::Config(_))
        ));
        assert!(Client::builder().header("x-token", "t").build().is_ok());
    }
}
