//! The SSE connection: an incremental `text/event-stream` parser and the
//! reqwest-based [`Source`].

use std::collections::VecDeque;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sui_rpc::proto::sui::rpc::v2::Checkpoint;

use crate::{Error, ResumeWith};

/// One dispatched SSE event.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RawEvent {
    pub id: Option<String>,
    pub event: String,
    pub data: String,
}

/// Incremental SSE parser (WHATWG event-stream rules, minus `retry` handling,
/// which is reported but not acted on).
#[derive(Default)]
pub(crate) struct Parser {
    buf: Vec<u8>,
    /// Bytes of `buf` already scanned for a newline.
    scanned: usize,
    current: RawEvent,
    has_data: bool,
    data_lines: Vec<String>,
}

impl Parser {
    /// Feed bytes; returns the events completed by them.
    pub fn push(&mut self, chunk: &[u8], out: &mut Vec<RawEvent>) -> Result<(), String> {
        self.buf.extend_from_slice(chunk);
        let mut start = 0;
        while let Some(rel) = self.buf[self.scanned..].iter().position(|&b| b == b'\n') {
            let end = self.scanned + rel;
            let mut line = &self.buf[start..end];
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }
            let line = std::str::from_utf8(line)
                .map_err(|e| format!("invalid UTF-8 in event stream: {e}"))?
                .to_owned();
            self.line(&line, out);
            start = end + 1;
            self.scanned = start;
        }
        self.buf.drain(..start);
        self.scanned = self.buf.len();
        Ok(())
    }

    fn line(&mut self, line: &str, out: &mut Vec<RawEvent>) {
        if line.is_empty() {
            if self.has_data {
                let mut ev = std::mem::take(&mut self.current);
                ev.data = std::mem::take(&mut self.data_lines).join("\n");
                if ev.event.is_empty() {
                    ev.event = "message".to_owned();
                }
                out.push(ev);
            } else {
                self.current = RawEvent::default();
            }
            self.has_data = false;
            return;
        }
        if line.starts_with(':') {
            return; // comment / heartbeat
        }
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        match field {
            "id" => self.current.id = Some(value.to_owned()),
            "event" => self.current.event = value.to_owned(),
            "data" => {
                self.has_data = true;
                self.data_lines.push(value.to_owned());
            }
            _ => {} // retry, unknown
        }
    }
}

/// An event from one connection.
#[derive(Debug)]
pub(crate) enum StreamEvent {
    Checkpoint(u64, Box<Checkpoint>),
    Gap { from: u64, to: u64 },
}

impl StreamEvent {
    /// Decode a raw SSE event; `Ok(None)` for event types we don't use.
    pub fn decode(raw: RawEvent) -> Result<Option<Self>, String> {
        match raw.event.as_str() {
            "checkpoint" => {
                let seq = raw
                    .id
                    .as_deref()
                    .and_then(|id| id.parse::<u64>().ok())
                    .ok_or_else(|| format!("checkpoint event id {:?}", raw.id))?;
                let cp: Checkpoint = serde_json::from_str(&raw.data)
                    .map_err(|e| format!("checkpoint {seq} JSON: {e}"))?;
                if cp.sequence_number.is_some_and(|s| s != seq) {
                    return Err(format!(
                        "event id {seq} != sequenceNumber {:?}",
                        cp.sequence_number
                    ));
                }
                Ok(Some(Self::Checkpoint(seq, Box::new(cp))))
            }
            "gap" => {
                #[derive(serde::Deserialize)]
                struct Gap {
                    from: u64,
                    to: u64,
                }
                let g: Gap = serde_json::from_str(&raw.data)
                    .map_err(|e| format!("gap JSON {:?}: {e}", raw.data))?;
                Ok(Some(Self::Gap {
                    from: g.from,
                    to: g.to,
                }))
            }
            _ => Ok(None),
        }
    }
}

/// Why a connection ended or failed.
#[derive(Debug)]
pub(crate) enum ConnError {
    /// Reconnect after backoff (at least `retry_after` if set).
    Retry(String, Option<Duration>),
    /// Give up: the request itself is wrong.
    Fatal(Error),
}

pub(crate) type EventStream = BoxStream<'static, Result<StreamEvent, ConnError>>;

/// Opens connections. Abstracted for tests.
pub(crate) trait Source: Send + Sync + 'static {
    /// Connect starting at checkpoint `next` (inclusive); `None` = tip.
    fn connect(&self, next: Option<u64>) -> BoxFuture<'static, Result<EventStream, ConnError>>;
}

/// The real SSE endpoint.
pub(crate) struct HttpSource {
    client: reqwest::Client,
    url: String,
    read_mask: Option<String>,
    headers: HeaderMap,
    resume_with: ResumeWith,
    stall_timeout: Duration,
}

impl HttpSource {
    pub fn new(
        url: String,
        read_mask: Option<String>,
        headers: &[(String, String)],
        resume_with: ResumeWith,
        stall_timeout: Duration,
    ) -> Result<Self, Error> {
        let mut map = HeaderMap::new();
        for (k, v) in headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| Error::Config(format!("header name {k:?}: {e}")))?;
            let mut value = HeaderValue::from_str(v)
                .map_err(|e| Error::Config(format!("header {k} value: {e}")))?;
            value.set_sensitive(true);
            map.insert(name, value);
        }
        // reqwest (zstd + gzip features) sends `Accept-Encoding: gzip, zstd`
        // and decodes the concatenated frames transparently.
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| Error::Config(format!("http client: {e}")))?;
        reqwest::Url::parse(&url).map_err(|e| Error::Config(format!("stream url {url:?}: {e}")))?;
        if stall_timeout.is_zero() {
            return Err(Error::Config("stall timeout must be positive".into()));
        }
        Ok(Self {
            client,
            url,
            read_mask,
            headers: map,
            resume_with,
            stall_timeout,
        })
    }
}

impl HttpSource {
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Source for HttpSource {
    fn connect(&self, next: Option<u64>) -> BoxFuture<'static, Result<EventStream, ConnError>> {
        let mut req = self
            .client
            .get(&self.url)
            .headers(self.headers.clone())
            .header(reqwest::header::ACCEPT, "text/event-stream");
        if let Some(mask) = &self.read_mask {
            req = req.query(&[("read_mask", mask)]);
        }
        match (next, self.resume_with) {
            (None, _) => {}
            (Some(0), _) | (Some(_), ResumeWith::Query) => {
                req = req.query(&[("from", next.unwrap_or_default())]);
            }
            (Some(s), ResumeWith::LastEventId) => {
                req = req.header("last-event-id", (s - 1).to_string());
            }
        }
        let stall = self.stall_timeout;
        Box::pin(async move {
            let resp = tokio::time::timeout(stall, req.send())
                .await
                .map_err(|_| ConnError::Retry("no response headers".into(), None))?
                .map_err(|e| ConnError::Retry(format!("request: {e}"), None))?;
            let status = resp.status();
            if !status.is_success() {
                let retry_after = resp
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .map(Duration::from_secs);
                let body = resp.text().await.unwrap_or_default();
                let fatal = matches!(
                    status,
                    StatusCode::BAD_REQUEST
                        | StatusCode::UNAUTHORIZED
                        | StatusCode::FORBIDDEN
                        | StatusCode::NOT_FOUND
                );
                return Err(if fatal {
                    ConnError::Fatal(Error::Http {
                        status: status.as_u16(),
                        body,
                    })
                } else {
                    ConnError::Retry(format!("HTTP {status}: {body}"), retry_after)
                });
            }
            tracing::debug!(
                encoding = ?resp.headers().get("content-encoding"),
                ?next,
                "stream connected"
            );
            Ok(events(
                resp.bytes_stream().map_err(|e| e.to_string()).boxed(),
                stall,
            ))
        })
    }
}

/// Turn a body into events, failing on `stall` without any bytes.
pub(crate) fn events(
    body: BoxStream<'static, Result<bytes::Bytes, String>>,
    stall: Duration,
) -> EventStream {
    struct State {
        body: BoxStream<'static, Result<bytes::Bytes, String>>,
        parser: Parser,
        ready: VecDeque<RawEvent>,
        done: bool,
    }
    let state = State {
        body,
        parser: Parser::default(),
        ready: VecDeque::new(),
        done: false,
    };
    futures::stream::unfold(state, move |mut st| async move {
        loop {
            if st.done {
                return None;
            }
            while let Some(raw) = st.ready.pop_front() {
                match StreamEvent::decode(raw) {
                    Ok(Some(ev)) => return Some((Ok(ev), st)),
                    Ok(None) => {}
                    Err(e) => {
                        st.done = true;
                        return Some((Err(ConnError::Retry(e, None)), st));
                    }
                }
            }
            let chunk = match tokio::time::timeout(stall, st.body.next()).await {
                Err(_) => {
                    st.done = true;
                    let msg = format!("stalled: no data for {stall:?}");
                    return Some((Err(ConnError::Retry(msg, None)), st));
                }
                Ok(None) => return None,
                Ok(Some(Err(e))) => {
                    st.done = true;
                    return Some((Err(ConnError::Retry(format!("body: {e}"), None)), st));
                }
                Ok(Some(Ok(c))) => c,
            };
            let mut out = Vec::new();
            if let Err(e) = st.parser.push(&chunk, &mut out) {
                st.done = true;
                return Some((Err(ConnError::Retry(e, None)), st));
            }
            st.ready.extend(out);
        }
    })
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_chunks(chunks: &[&[u8]]) -> Vec<RawEvent> {
        let mut p = Parser::default();
        let mut out = vec![];
        for c in chunks {
            p.push(c, &mut out).unwrap();
        }
        out
    }

    #[test]
    fn parses_events_across_arbitrary_splits() {
        let stream = b"retry: 1000\n\nid: 5\nevent: checkpoint\ndata: {\"sequenceNumber\":\"5\"}\n\n: keepalive\n\nevent: gap\ndata: {\"from\":6,\"to\":9}\n\n";
        let whole = parse_chunks(&[stream]);
        assert_eq!(
            whole,
            vec![
                RawEvent {
                    id: Some("5".into()),
                    event: "checkpoint".into(),
                    data: "{\"sequenceNumber\":\"5\"}".into()
                },
                RawEvent {
                    id: None,
                    event: "gap".into(),
                    data: "{\"from\":6,\"to\":9}".into()
                },
            ]
        );
        // Every split point, including byte-by-byte.
        for i in 0..stream.len() {
            assert_eq!(
                parse_chunks(&[&stream[..i], &stream[i..]]),
                whole,
                "split {i}"
            );
        }
        let bytes: Vec<&[u8]> = stream.chunks(1).collect();
        assert_eq!(parse_chunks(&bytes), whole);
    }

    #[test]
    fn crlf_multiline_and_defaults() {
        let out = parse_chunks(&[b"data: a\r\ndata:b\r\n\r\nid\ndata\n\n"]);
        assert_eq!(out[0].data, "a\nb");
        assert_eq!(out[0].event, "message");
        assert_eq!(out[1].id.as_deref(), Some(""));
        assert_eq!(out[1].data, "");
        // An event without data is not dispatched.
        assert!(parse_chunks(&[b"id: 1\nevent: checkpoint\n\n"]).is_empty());
    }

    #[test]
    fn decodes_checkpoint_and_gap() {
        let ev = StreamEvent::decode(RawEvent {
            id: Some("7".into()),
            event: "checkpoint".into(),
            data: r#"{"sequenceNumber":"7","digest":"abc"}"#.into(),
        })
        .unwrap()
        .unwrap();
        let StreamEvent::Checkpoint(7, cp) = ev else {
            panic!("{ev:?}")
        };
        assert_eq!(cp.digest.as_deref(), Some("abc"));
        assert_eq!(cp.sequence_number, Some(7));
        let ev = StreamEvent::decode(RawEvent {
            id: None,
            event: "gap".into(),
            data: r#"{"from":1,"to":3}"#.into(),
        })
        .unwrap();
        assert!(matches!(ev, Some(StreamEvent::Gap { from: 1, to: 3 })));
        assert!(
            StreamEvent::decode(RawEvent {
                id: None,
                event: "checkpoint".into(),
                data: "{}".into()
            })
            .is_err()
        );
        assert!(
            StreamEvent::decode(RawEvent {
                id: Some("8".into()),
                event: "checkpoint".into(),
                data: r#"{"sequenceNumber":"9"}"#.into()
            })
            .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stall_is_an_error() {
        let body = futures::stream::iter(vec![Ok(bytes::Bytes::from_static(
            b"id: 1\nevent: checkpoint\ndata: {}\n\n",
        ))])
        .chain(futures::stream::pending())
        .boxed();
        let mut s = events(body, Duration::from_secs(45));
        assert!(matches!(
            s.next().await,
            Some(Ok(StreamEvent::Checkpoint(1, _)))
        ));
        assert!(
            matches!(s.next().await, Some(Err(ConnError::Retry(m, _))) if m.contains("stalled"))
        );
        assert!(s.next().await.is_none());
    }
}
