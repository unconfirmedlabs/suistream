//! The pre-memchr parser, kept for equivalence tests and the framing
//! benchmark. Copied from `src/sse.rs` (suistream 0.1.0); std only.

/// One dispatched SSE event.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RawEvent {
    pub id: Option<String>,
    pub event: String,
    pub data: String,
}

/// Incremental SSE parser (WHATWG event-stream rules, minus `retry` handling,
/// which is reported but not acted on).
#[derive(Default)]
pub struct Parser {
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
