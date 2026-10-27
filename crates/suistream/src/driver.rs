//! Keeps one SSE connection alive and turns it into an ordered sequence:
//! reconnect from the cursor, drop duplicates, surface gaps. Checkpoints are
//! decoded only after that, so duplicates are never parsed.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc;

use crate::sse::{ConnError, Source, StreamEvent};
use crate::{Checkpoint, Error, Event, RawEvent};

const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

fn backoff(failures: u32) -> Duration {
    BACKOFF_MIN
        .saturating_mul(1 << failures.min(5))
        .min(BACKOFF_MAX)
}

/// Decode a checkpoint's proto3 JSON; `seq` is its SSE `id`.
pub fn decode_checkpoint(seq: u64, data: &[u8]) -> Result<Box<Checkpoint>, String> {
    // Validating UTF-8 once and parsing a `&str` is faster than `from_slice`
    // (same result; see benches/decode.rs).
    let json = std::str::from_utf8(data).map_err(|e| format!("checkpoint {seq}: {e}"))?;
    let cp: Box<Checkpoint> =
        serde_json::from_str(json).map_err(|e| format!("checkpoint {seq} JSON: {e}"))?;
    if cp.sequence_number.is_some_and(|s| s != seq) {
        return Err(format!(
            "event id {seq} != sequenceNumber {:?}",
            cp.sequence_number
        ));
    }
    Ok(cp)
}

/// An item a subscription yields.
pub(crate) trait Item: Send + Sized + 'static {
    /// Decode checkpoint `seq`; an error is a malformed event.
    fn checkpoint(seq: u64, data: Bytes) -> Result<Self, String>;
    fn gap(from: u64, to: u64) -> Self;
    /// (checkpoint delivered, cursor) once this item is yielded.
    fn position(&self) -> (Option<u64>, u64);
}

impl Item for Event {
    fn checkpoint(seq: u64, data: Bytes) -> Result<Self, String> {
        let checkpoint = decode_checkpoint(seq, &data)?;
        Ok(Event::Checkpoint { seq, checkpoint })
    }

    fn gap(from: u64, to: u64) -> Self {
        Event::Gap { from, to }
    }

    fn position(&self) -> (Option<u64>, u64) {
        match *self {
            Event::Checkpoint { seq, .. } => (Some(seq), seq),
            Event::Gap { to, .. } => (None, to),
        }
    }
}

impl Item for RawEvent {
    fn checkpoint(seq: u64, data: Bytes) -> Result<Self, String> {
        Ok(RawEvent::Checkpoint { seq, data })
    }

    fn gap(from: u64, to: u64) -> Self {
        RawEvent::Gap { from, to }
    }

    fn position(&self) -> (Option<u64>, u64) {
        match *self {
            RawEvent::Checkpoint { seq, .. } => (Some(seq), seq),
            RawEvent::Gap { to, .. } => (None, to),
        }
    }
}

/// `start`: first checkpoint wanted (`None` = live tip).
pub(crate) async fn run<E: Item>(
    source: Arc<dyn Source>,
    start: Option<u64>,
    out: mpsc::Sender<Result<E, Error>>,
) {
    // Next checkpoint owed to the caller.
    let mut next = start;
    // Consecutive connections that delivered nothing.
    let mut failures = 0u32;
    loop {
        let mut delay = None;
        match source.connect(next).await {
            Err(ConnError::Fatal(e)) => {
                let _ = out.send(Err(e)).await;
                return;
            }
            Err(ConnError::Retry(msg, retry_after)) => {
                tracing::warn!("stream connection failed: {msg}");
                delay = retry_after;
            }
            Ok(mut events) => {
                tracing::debug!(?next, "connected");
                loop {
                    let item = tokio::select! {
                        item = events.next() => item,
                        _ = out.closed() => return,
                    };
                    let event = match item {
                        None => {
                            tracing::debug!(?next, "stream closed by server");
                            break;
                        }
                        Some(Err(ConnError::Retry(msg, _))) => {
                            tracing::warn!(?next, "stream interrupted: {msg}");
                            break;
                        }
                        Some(Err(ConnError::Fatal(e))) => {
                            let _ = out.send(Err(e)).await;
                            return;
                        }
                        Some(Ok(event)) => event,
                    };
                    let event = match (event, next) {
                        (StreamEvent::Checkpoint(seq, _), Some(n)) if seq < n => continue,
                        (StreamEvent::Gap { to, .. }, Some(n)) if to < n => continue,
                        (StreamEvent::Checkpoint(seq, _), Some(n))
                        | (StreamEvent::Gap { from: seq, .. }, Some(n))
                            if seq > n =>
                        {
                            // The server never skips silently; reconnect
                            // rather than guess.
                            tracing::warn!(next = n, got = seq, "out of order; reconnecting");
                            break;
                        }
                        (StreamEvent::Checkpoint(seq, data), _) => match E::checkpoint(seq, data) {
                            Ok(item) => {
                                next = Some(seq + 1);
                                item
                            }
                            Err(e) => {
                                tracing::warn!(?next, "malformed event: {e}; reconnecting");
                                break;
                            }
                        },
                        (StreamEvent::Gap { from, to }, n) => {
                            next = Some(to + 1);
                            E::gap(n.map_or(from, |n| n.max(from)), to)
                        }
                    };
                    failures = 0;
                    if out.send(Ok(event)).await.is_err() {
                        return;
                    }
                }
            }
        }
        let wait = backoff(failures).max(delay.unwrap_or_default());
        failures += 1;
        tracing::info!(?wait, ?next, "reconnecting");
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = out.closed() => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sse::EventStream;
    use futures::future::BoxFuture;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    type Script = Result<Vec<Result<StreamEvent, ConnError>>, ConnError>;

    /// Plays one scripted connection per `connect`, recording the resume
    /// positions; after the script, connections hang.
    struct Scripted {
        conns: Mutex<VecDeque<Script>>,
        resumes: Mutex<Vec<Option<u64>>>,
    }

    impl Source for Scripted {
        fn connect(&self, next: Option<u64>) -> BoxFuture<'static, Result<EventStream, ConnError>> {
            self.resumes.lock().unwrap().push(next);
            let script = self.conns.lock().unwrap().pop_front();
            Box::pin(async move {
                match script {
                    None => Ok(futures::stream::pending().boxed()),
                    Some(Err(e)) => Err(e),
                    Some(Ok(events)) => Ok(futures::stream::iter(events).boxed()),
                }
            })
        }
    }

    fn json(seq: u64) -> String {
        format!(r#"{{"sequenceNumber":"{seq}"}}"#)
    }

    fn cp(seq: u64) -> Result<StreamEvent, ConnError> {
        cp_data(seq, &json(seq))
    }

    fn cp_data(seq: u64, data: &str) -> Result<StreamEvent, ConnError> {
        Ok(StreamEvent::Checkpoint(
            seq,
            Bytes::copy_from_slice(data.as_bytes()),
        ))
    }

    fn gap(from: u64, to: u64) -> Result<StreamEvent, ConnError> {
        Ok(StreamEvent::Gap { from, to })
    }

    fn retry() -> ConnError {
        ConnError::Retry("boom".into(), None)
    }

    /// Items as strings: `seq` for a checkpoint whose payload is `json(seq)`,
    /// else `seq <payload>`.
    trait Show: Item {
        fn show(self) -> String;
    }

    impl Show for Event {
        fn show(self) -> String {
            match self {
                Event::Checkpoint { seq, checkpoint } => {
                    assert_eq!(checkpoint.sequence_number, Some(seq));
                    seq.to_string()
                }
                Event::Gap { from, to } => format!("gap {from}-{to}"),
            }
        }
    }

    impl Show for RawEvent {
        fn show(self) -> String {
            match self {
                RawEvent::Checkpoint { seq, data } if data == json(seq) => seq.to_string(),
                RawEvent::Checkpoint { seq, data } => {
                    format!("{seq} {}", String::from_utf8_lossy(&data))
                }
                RawEvent::Gap { from, to } => format!("gap {from}-{to}"),
            }
        }
    }

    async fn play<E: Show>(
        start: Option<u64>,
        conns: Vec<Script>,
        n: usize,
    ) -> (Vec<String>, Vec<Option<u64>>) {
        let source = Arc::new(Scripted {
            conns: Mutex::new(conns.into()),
            resumes: Mutex::new(vec![]),
        });
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn(run::<E>(source.clone(), start, tx));
        let mut got = vec![];
        while got.len() < n {
            got.push(match rx.recv().await.unwrap() {
                Ok(item) => item.show(),
                Err(e) => format!("error {e}"),
            });
        }
        task.abort();
        let resumes = source.resumes.lock().unwrap().clone();
        (got, resumes)
    }

    /// `play` in typed and raw mode; both must agree.
    async fn play_both(
        start: Option<u64>,
        conns: impl Fn() -> Vec<Script>,
        n: usize,
    ) -> (Vec<String>, Vec<Option<u64>>) {
        let typed = play::<Event>(start, conns(), n).await;
        let raw = play::<RawEvent>(start, conns(), n).await;
        assert_eq!(typed, raw, "typed vs raw");
        typed
    }

    #[tokio::test(start_paused = true)]
    async fn dedups_reconnects_and_resumes_from_cursor() {
        let (got, resumes) = play_both(
            Some(10),
            || {
                vec![
                    Ok(vec![cp(10), cp(11), Err(retry())]),
                    Err(retry()),
                    // Overlap after reconnect is dropped.
                    Ok(vec![cp(11), cp(12), cp(13)]),
                    // Clean close by the server: reconnect from 14.
                    Ok(vec![cp(14)]),
                ]
            },
            5,
        )
        .await;
        assert_eq!(got, ["10", "11", "12", "13", "14"]);
        assert_eq!(resumes, [Some(10), Some(12), Some(12), Some(14)]);
    }

    #[tokio::test(start_paused = true)]
    async fn gaps_are_surfaced_once_and_move_the_cursor() {
        let (got, resumes) = play_both(
            Some(5),
            || {
                vec![
                    // History gap, then a live gap followed by a close.
                    Ok(vec![gap(5, 7), cp(8), gap(9, 10)]),
                    // The reconnect resumes after the gap; a repeated report
                    // of it is dropped.
                    Ok(vec![gap(9, 10), cp(11)]),
                ]
            },
            4,
        )
        .await;
        assert_eq!(got, ["gap 5-7", "8", "gap 9-10", "11"]);
        assert_eq!(resumes, [Some(5), Some(11)]);
    }

    #[tokio::test(start_paused = true)]
    async fn partial_gap_overlap_is_trimmed() {
        let (got, _) = play_both(Some(5), || vec![Ok(vec![cp(5), gap(3, 7), cp(8)])], 3).await;
        assert_eq!(got, ["5", "gap 6-7", "8"]);
    }

    #[tokio::test(start_paused = true)]
    async fn silent_skip_reconnects() {
        let (got, resumes) = play_both(
            Some(1),
            || vec![Ok(vec![cp(1), cp(3)]), Ok(vec![cp(2), cp(3)])],
            3,
        )
        .await;
        assert_eq!(got, ["1", "2", "3"]);
        assert_eq!(resumes, [Some(1), Some(2)]);
    }

    #[tokio::test(start_paused = true)]
    async fn tip_start_and_fatal_error() {
        let (got, resumes) = play_both(
            None,
            || {
                vec![
                    Ok(vec![cp(100), cp(101)]),
                    Err(ConnError::Fatal(Error::Http {
                        status: 400,
                        body: "bad mask".into(),
                    })),
                ]
            },
            3,
        )
        .await;
        assert_eq!(got[..2], ["100", "101"]);
        assert!(got[2].starts_with("error stream rejected the request: HTTP 400"));
        assert_eq!(resumes, [None, Some(102)]);
    }

    async fn timed_backoff<E: Show>() {
        let t0 = tokio::time::Instant::now();
        let (got, _) = play::<E>(
            Some(1),
            vec![
                Err(ConnError::Retry("429".into(), Some(Duration::from_secs(7)))),
                Err(retry()),
                Ok(vec![cp(1)]),
            ],
            1,
        )
        .await;
        assert_eq!(got, ["1"]);
        // max(1 s, Retry-After 7 s) + 2 s.
        assert_eq!(t0.elapsed().as_secs(), 9);
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_grows_and_honours_retry_after() {
        assert_eq!(backoff(0), Duration::from_secs(1));
        assert_eq!(backoff(3), Duration::from_secs(8));
        assert_eq!(backoff(10), Duration::from_secs(30));
        timed_backoff::<Event>().await;
        timed_backoff::<RawEvent>().await;
    }

    #[test]
    fn decodes_checkpoints() {
        let cp = decode_checkpoint(7, br#"{"sequenceNumber":"7","digest":"abc"}"#).unwrap();
        assert_eq!(cp.digest.as_deref(), Some("abc"));
        assert_eq!(cp.sequence_number, Some(7));
        // The mask may omit the sequence number.
        assert!(decode_checkpoint(8, b"{}").is_ok());
        assert!(decode_checkpoint(8, br#"{"sequenceNumber":"9"}"#).is_err());
        assert!(decode_checkpoint(8, b"{").is_err());
        assert!(decode_checkpoint(8, b"{\"digest\":\"\xff\"}").is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn typed_malformed_checkpoint_reconnects_from_it() {
        let (got, resumes) = play::<Event>(
            Some(10),
            vec![
                Ok(vec![cp(10), cp_data(11, "{"), cp(12)]),
                Ok(vec![cp(10), cp(11), cp(12)]),
            ],
            3,
        )
        .await;
        assert_eq!(got, ["10", "11", "12"]);
        assert_eq!(resumes, [Some(10), Some(11)]);
        // A mismatched sequence number is malformed too.
        let (got, resumes) = play::<Event>(
            Some(10),
            vec![Ok(vec![cp_data(10, &json(9))]), Ok(vec![cp(10)])],
            1,
        )
        .await;
        assert_eq!(got, ["10"]);
        assert_eq!(resumes, [Some(10), Some(10)]);
    }

    #[tokio::test(start_paused = true)]
    async fn malformed_duplicate_is_dropped_unparsed() {
        let (got, resumes) = play_both(
            Some(10),
            || {
                vec![
                    Ok(vec![cp(10), cp(11), cp_data(11, "{"), cp(12)]),
                    Ok(vec![cp(13)]),
                ]
            },
            4,
        )
        .await;
        assert_eq!(got, ["10", "11", "12", "13"]);
        // Only the clean close after 12 reconnects.
        assert_eq!(resumes, [Some(10), Some(13)]);
    }

    #[tokio::test(start_paused = true)]
    async fn raw_delivers_malformed_bytes_untouched() {
        let (got, resumes) = play::<RawEvent>(
            Some(10),
            vec![
                Ok(vec![cp(10), cp_data(11, "{\u{0}x"), cp(12)]),
                Ok(vec![cp(13)]),
            ],
            4,
        )
        .await;
        assert_eq!(got, ["10", "11 {\u{0}x", "12", "13"]);
        assert_eq!(resumes, [Some(10), Some(13)]);
    }
}
