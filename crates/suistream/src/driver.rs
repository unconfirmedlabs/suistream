//! Keeps one SSE connection alive and turns it into an ordered sequence:
//! reconnect from the cursor, drop duplicates, surface gaps.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::sse::{ConnError, Source, StreamEvent};
use crate::{Error, Event};

const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

fn backoff(failures: u32) -> Duration {
    BACKOFF_MIN
        .saturating_mul(1 << failures.min(5))
        .min(BACKOFF_MAX)
}

/// `start`: first checkpoint wanted (`None` = live tip).
pub(crate) async fn run(
    source: Arc<dyn Source>,
    start: Option<u64>,
    out: mpsc::Sender<Result<Event, Error>>,
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
                        (StreamEvent::Checkpoint(seq, checkpoint), _) => {
                            next = Some(seq + 1);
                            Event::Checkpoint { seq, checkpoint }
                        }
                        (StreamEvent::Gap { from, to }, n) => {
                            next = Some(to + 1);
                            Event::Gap {
                                from: n.map_or(from, |n| n.max(from)),
                                to,
                            }
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

    fn cp(seq: u64) -> Result<StreamEvent, ConnError> {
        let mut c = crate::Checkpoint::default();
        c.sequence_number = Some(seq);
        Ok(StreamEvent::Checkpoint(seq, Box::new(c)))
    }

    fn gap(from: u64, to: u64) -> Result<StreamEvent, ConnError> {
        Ok(StreamEvent::Gap { from, to })
    }

    fn retry() -> ConnError {
        ConnError::Retry("boom".into(), None)
    }

    async fn play(
        start: Option<u64>,
        conns: Vec<Script>,
        n: usize,
    ) -> (Vec<String>, Vec<Option<u64>>) {
        let source = Arc::new(Scripted {
            conns: Mutex::new(conns.into()),
            resumes: Mutex::new(vec![]),
        });
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn(run(source.clone(), start, tx));
        let mut got = vec![];
        while got.len() < n {
            got.push(match rx.recv().await.unwrap() {
                Ok(Event::Checkpoint { seq, .. }) => seq.to_string(),
                Ok(Event::Gap { from, to }) => format!("gap {from}-{to}"),
                Err(e) => format!("error {e}"),
            });
        }
        task.abort();
        let resumes = source.resumes.lock().unwrap().clone();
        (got, resumes)
    }

    #[tokio::test(start_paused = true)]
    async fn dedups_reconnects_and_resumes_from_cursor() {
        let (got, resumes) = play(
            Some(10),
            vec![
                Ok(vec![cp(10), cp(11), Err(retry())]),
                Err(retry()),
                // Overlap after reconnect is dropped.
                Ok(vec![cp(11), cp(12), cp(13)]),
                // Clean close by the server: reconnect from 14.
                Ok(vec![cp(14)]),
            ],
            5,
        )
        .await;
        assert_eq!(got, ["10", "11", "12", "13", "14"]);
        assert_eq!(resumes, [Some(10), Some(12), Some(12), Some(14)]);
    }

    #[tokio::test(start_paused = true)]
    async fn gaps_are_surfaced_once_and_move_the_cursor() {
        let (got, resumes) = play(
            Some(5),
            vec![
                // History gap, then a live gap followed by a close.
                Ok(vec![gap(5, 7), cp(8), gap(9, 10)]),
                // The reconnect resumes after the gap; a repeated report of
                // it is dropped.
                Ok(vec![gap(9, 10), cp(11)]),
            ],
            4,
        )
        .await;
        assert_eq!(got, ["gap 5-7", "8", "gap 9-10", "11"]);
        assert_eq!(resumes, [Some(5), Some(11)]);
    }

    #[tokio::test(start_paused = true)]
    async fn partial_gap_overlap_is_trimmed() {
        let (got, _) = play(Some(5), vec![Ok(vec![cp(5), gap(3, 7), cp(8)])], 3).await;
        assert_eq!(got, ["5", "gap 6-7", "8"]);
    }

    #[tokio::test(start_paused = true)]
    async fn silent_skip_reconnects() {
        let (got, resumes) = play(
            Some(1),
            vec![Ok(vec![cp(1), cp(3)]), Ok(vec![cp(2), cp(3)])],
            3,
        )
        .await;
        assert_eq!(got, ["1", "2", "3"]);
        assert_eq!(resumes, [Some(1), Some(2)]);
    }

    #[tokio::test(start_paused = true)]
    async fn tip_start_and_fatal_error() {
        let (got, resumes) = play(
            None,
            vec![
                Ok(vec![cp(100), cp(101)]),
                Err(ConnError::Fatal(Error::Http {
                    status: 400,
                    body: "bad mask".into(),
                })),
            ],
            3,
        )
        .await;
        assert_eq!(got[..2], ["100", "101"]);
        assert!(got[2].starts_with("error stream rejected the request: HTTP 400"));
        assert_eq!(resumes, [None, Some(102)]);
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_grows_and_honours_retry_after() {
        assert_eq!(backoff(0), Duration::from_secs(1));
        assert_eq!(backoff(3), Duration::from_secs(8));
        assert_eq!(backoff(10), Duration::from_secs(30));
        let t0 = tokio::time::Instant::now();
        let (got, _) = play(
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
}
