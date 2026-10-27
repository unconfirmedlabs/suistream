//! The SSE connection: an incremental `text/event-stream` parser and the
//! reqwest-based [`Source`].

use std::ops::Range;
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use crate::{Error, ResumeWith};

/// One dispatched SSE event. All fields share the parser's buffer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Frame {
    id: Option<Bytes>,
    event: Option<Bytes>,
    /// The `data` lines joined with `\n`; not UTF-8 validated.
    pub data: Bytes,
}

impl Frame {
    /// The `id` field, if the event had one.
    pub fn id(&self) -> Option<&[u8]> {
        self.id.as_deref()
    }

    /// The `event` field (default `message`).
    pub fn event(&self) -> &[u8] {
        self.event.as_deref().unwrap_or(b"message")
    }
}

/// Incremental SSE parser (WHATWG event-stream rules, minus `retry` handling,
/// which is reported but not acted on). Lines end at LF (a CR before it is
/// stripped). Fields are kept as ranges into one buffer, which is only cut at
/// event boundaries, so a single-line `data` payload is never copied.
#[derive(Default)]
pub struct Parser {
    buf: BytesMut,
    /// Start of the next line in `buf`.
    line: usize,
    /// Bytes of `buf` already scanned for a newline.
    scanned: usize,
    id: Option<Range<usize>>,
    event: Option<Range<usize>>,
    data: Vec<Range<usize>>,
}

impl Parser {
    /// Feed bytes; appends the events completed by them to `out`.
    pub fn push(&mut self, chunk: &[u8], out: &mut Vec<Frame>) -> Result<(), String> {
        self.buf.extend_from_slice(chunk);
        while let Some(rel) = memchr::memchr(b'\n', &self.buf[self.scanned..]) {
            let lf = self.scanned + rel;
            let start = std::mem::replace(&mut self.line, lf + 1);
            self.scanned = lf + 1;
            let end = if lf > start && self.buf[lf - 1] == b'\r' {
                lf - 1
            } else {
                lf
            };
            self.field(start..end, out);
        }
        self.scanned = self.buf.len();
        // Nothing pending refers to the consumed lines (comments,
        // heartbeats): drop them.
        if self.id.is_none() && self.event.is_none() && self.data.is_empty() {
            self.cut(self.line);
        }
        Ok(())
    }

    fn field(&mut self, line: Range<usize>, out: &mut Vec<Frame>) {
        let bytes = &self.buf[line.clone()];
        if bytes.is_empty() {
            if !self.data.is_empty() {
                let frame = self.dispatch();
                out.push(frame);
            } else {
                self.id = None;
                self.event = None;
            }
            return;
        }
        if bytes[0] == b':' {
            return; // comment / heartbeat
        }
        let (name, value) = match memchr::memchr(b':', bytes) {
            Some(c) => {
                let skip = usize::from(bytes.get(c + 1) == Some(&b' '));
                (
                    line.start..line.start + c,
                    line.start + c + 1 + skip..line.end,
                )
            }
            None => (line.clone(), line.end..line.end),
        };
        match &self.buf[name] {
            b"id" => self.id = Some(value),
            b"event" => self.event = Some(value),
            b"data" => self.data.push(value),
            _ => {} // retry, unknown
        }
    }

    /// The pending event, cutting the buffer through its blank line.
    fn dispatch(&mut self) -> Frame {
        let end = self.line;
        let frozen = self.buf.split_to(end).freeze();
        self.line = 0;
        self.scanned -= end;
        let data = match self.data.as_slice() {
            [one] => frozen.slice(one.clone()),
            lines => {
                let len = lines.iter().map(|r| r.len() + 1).sum::<usize>() - 1;
                let mut joined = BytesMut::with_capacity(len);
                for (i, r) in lines.iter().enumerate() {
                    if i > 0 {
                        joined.extend_from_slice(b"\n");
                    }
                    joined.extend_from_slice(&frozen[r.clone()]);
                }
                joined.freeze()
            }
        };
        self.data.clear();
        Frame {
            id: self.id.take().map(|r| frozen.slice(r)),
            event: self.event.take().map(|r| frozen.slice(r)),
            data,
        }
    }

    /// Drop the first `n` bytes of the buffer (nothing may refer to them).
    fn cut(&mut self, n: usize) {
        self.buf.advance(n);
        self.line -= n;
        self.scanned -= n;
    }
}

/// An event from one connection; checkpoints are not decoded yet.
#[derive(Debug)]
pub(crate) enum StreamEvent {
    /// Sequence number (the SSE `id`) and the `data` payload.
    Checkpoint(u64, Bytes),
    Gap {
        from: u64,
        to: u64,
    },
}

impl StreamEvent {
    /// Classify a frame; `Ok(None)` for event types we don't use.
    pub fn from_frame(frame: Frame) -> Result<Option<Self>, String> {
        match frame.event() {
            b"checkpoint" => {
                let seq = frame.id().and_then(parse_seq).ok_or_else(|| {
                    format!(
                        "checkpoint event id {:?}",
                        frame.id().map(String::from_utf8_lossy)
                    )
                })?;
                Ok(Some(Self::Checkpoint(seq, frame.data)))
            }
            b"gap" => {
                #[derive(serde::Deserialize)]
                struct Gap {
                    from: u64,
                    to: u64,
                }
                let g: Gap = serde_json::from_slice(&frame.data).map_err(|e| {
                    format!("gap JSON {:?}: {e}", String::from_utf8_lossy(&frame.data))
                })?;
                Ok(Some(Self::Gap {
                    from: g.from,
                    to: g.to,
                }))
            }
            _ => Ok(None),
        }
    }
}

/// A sequence number: ASCII digits only.
fn parse_seq(id: &[u8]) -> Option<u64> {
    if id.is_empty() || !id.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(id).ok()?.parse().ok()
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
    body: BoxStream<'static, Result<Bytes, String>>,
    stall: Duration,
) -> EventStream {
    struct State {
        body: BoxStream<'static, Result<Bytes, String>>,
        parser: Parser,
        /// Frames parsed from the last chunk; `ready[next..]` are pending.
        ready: Vec<Frame>,
        next: usize,
        done: bool,
    }
    let state = State {
        body,
        parser: Parser::default(),
        ready: Vec::new(),
        next: 0,
        done: false,
    };
    futures::stream::unfold(state, move |mut st| async move {
        loop {
            if st.done {
                return None;
            }
            while let Some(frame) = st.ready.get_mut(st.next) {
                let frame = std::mem::take(frame);
                st.next += 1;
                match StreamEvent::from_frame(frame) {
                    Ok(Some(ev)) => return Some((Ok(ev), st)),
                    Ok(None) => {}
                    Err(e) => {
                        st.done = true;
                        return Some((Err(ConnError::Retry(e, None)), st));
                    }
                }
            }
            st.ready.clear();
            st.next = 0;
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
            if let Err(e) = st.parser.push(&chunk, &mut st.ready) {
                st.done = true;
                return Some((Err(ConnError::Retry(e, None)), st));
            }
        }
    })
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_chunks(chunks: &[&[u8]]) -> Vec<Frame> {
        let mut p = Parser::default();
        let mut out = vec![];
        for c in chunks {
            p.push(c, &mut out).unwrap();
        }
        out
    }

    fn frame(id: Option<&'static str>, event: Option<&'static str>, data: &'static str) -> Frame {
        Frame {
            id: id.map(|s| Bytes::from_static(s.as_bytes())),
            event: event.map(|s| Bytes::from_static(s.as_bytes())),
            data: Bytes::from_static(data.as_bytes()),
        }
    }

    #[test]
    fn parses_events_across_arbitrary_splits() {
        let stream = b"retry: 1000\n\nid: 5\nevent: checkpoint\ndata: {\"sequenceNumber\":\"5\"}\n\n: keepalive\n\nevent: gap\ndata: {\"from\":6,\"to\":9}\n\n";
        let whole = parse_chunks(&[stream]);
        assert_eq!(
            whole,
            vec![
                frame(Some("5"), Some("checkpoint"), "{\"sequenceNumber\":\"5\"}"),
                frame(None, Some("gap"), "{\"from\":6,\"to\":9}"),
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
        assert_eq!(out[0].event(), b"message");
        assert_eq!(out[1].id(), Some(&b""[..]));
        assert_eq!(out[1].data, "");
        // An event without data is not dispatched.
        assert!(parse_chunks(&[b"id: 1\nevent: checkpoint\n\n"]).is_empty());
        // CR at the end of one chunk, LF at the start of the next.
        let out = parse_chunks(&[b"id: 1\r", b"\ndata: x\r", b"\n\r", b"\n"]);
        assert_eq!(out, [frame(Some("1"), None, "x")]);
        // A lone CR is not a line end.
        let out = parse_chunks(&[b"data: a\rb\n\n"]);
        assert_eq!(out[0].data, "a\rb");
        // One optional space after the colon; a colon in the value stays.
        let out = parse_chunks(&[b"data:  a: b\n\n"]);
        assert_eq!(out[0].data, " a: b");
    }

    #[test]
    fn id_and_event_reset_at_each_blank_line() {
        let out = parse_chunks(&[
            b"id: 1\nevent: x\n\ndata: a\n\nid: 2\nid: 3\nevent: y\nretry: 5\nfoo: z\ndata: b\n\ndata: c\n\n",
        ]);
        assert_eq!(
            out,
            [
                // The blank line without data discarded id 1 and event x.
                frame(None, None, "a"),
                // The last value wins; unknown fields are ignored.
                frame(Some("3"), Some("y"), "b"),
                // Dispatch resets them too.
                frame(None, None, "c"),
            ]
        );
    }

    #[test]
    fn long_line_in_small_chunks() {
        let payload = "x".repeat(1 << 20);
        let stream = format!("id: 9\nevent: checkpoint\ndata: {payload}\n\n");
        let chunks: Vec<&[u8]> = stream.as_bytes().chunks(1024).collect();
        let out = parse_chunks(&chunks);
        assert_eq!(out.len(), 1);
        assert_eq!(
            (out[0].id(), out[0].event()),
            (Some(&b"9"[..]), &b"checkpoint"[..])
        );
        assert_eq!(out[0].data, payload.as_bytes());
    }

    #[test]
    fn single_data_line_is_not_copied() {
        let stream: &[u8] = b": hb\n\nid: 12\nevent: checkpoint\ndata: {\"a\":1}\n\n";
        let at = |needle: &[u8]| stream.windows(needle.len()).position(|w| w == needle);
        let (id_at, data_at) = (at(b"12").unwrap(), at(b"{\"a\"").unwrap());
        for chunks in [vec![stream], stream.chunks(5).collect()] {
            let out = parse_chunks(&chunks);
            let (id, data) = (out[0].id().unwrap(), &out[0].data);
            assert_eq!(data, "{\"a\":1}");
            // Both are views of the same buffer, as far apart as in the input.
            assert_eq!(
                data.as_ptr() as usize - id.as_ptr() as usize,
                data_at - id_at
            );
        }
    }

    #[test]
    fn classifies_checkpoint_and_gap() {
        let ev = StreamEvent::from_frame(frame(Some("7"), Some("checkpoint"), "not json"));
        assert!(matches!(ev, Ok(Some(StreamEvent::Checkpoint(7, d))) if d == "not json"));
        let ev = StreamEvent::from_frame(frame(None, Some("gap"), r#"{"from":1,"to":3}"#));
        assert!(matches!(ev, Ok(Some(StreamEvent::Gap { from: 1, to: 3 }))));
        assert!(StreamEvent::from_frame(frame(None, Some("gap"), "{}")).is_err());
        for id in [
            None,
            Some(""),
            Some("x"),
            Some("+8"),
            Some("99999999999999999999"),
        ] {
            let ev = StreamEvent::from_frame(frame(id, Some("checkpoint"), "{}"));
            assert!(ev.unwrap_err().starts_with("checkpoint event id"), "{id:?}");
        }
        assert!(matches!(
            StreamEvent::from_frame(frame(Some("x"), None, "{}")),
            Ok(None)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn stall_is_an_error() {
        let body = futures::stream::iter(vec![Ok(Bytes::from_static(
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
