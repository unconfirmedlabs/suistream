//! Splitting payloads larger than the NATS `max_payload` into numbered parts
//! (`Sui-Chunk: <i>/<n>`, 1-based) and reassembling them.
//!
//! Core NATS preserves order per publisher, so the parts of one checkpoint
//! arrive consecutively on their subject (interleaved only with other
//! subjects). The assembler therefore keeps one partial payload per subject
//! and discards it if the sequence breaks.

use std::collections::HashMap;

use bytes::{Bytes, BytesMut};

/// Split `payload` into parts of at most `max` bytes. A payload that fits is
/// returned as a single part, which is published without a chunk header.
pub fn split(payload: &Bytes, max: usize) -> Vec<Bytes> {
    assert!(max > 0);
    if payload.len() <= max {
        return vec![payload.clone()];
    }
    (0..payload.len())
        .step_by(max)
        .map(|start| payload.slice(start..(start + max).min(payload.len())))
        .collect()
}

pub fn format_header(index: usize, total: usize) -> String {
    format!("{}/{}", index + 1, total)
}

/// Parse `<i>/<n>` into a 0-based index and total.
pub fn parse_header(value: &str) -> Option<(usize, usize)> {
    let (i, n) = value.split_once('/')?;
    let (i, n): (usize, usize) = (i.parse().ok()?, n.parse().ok()?);
    (i >= 1 && i <= n).then(|| (i - 1, n))
}

struct Partial {
    seq: u64,
    total: usize,
    next: usize,
    buf: BytesMut,
}

/// Reassembles chunked payloads, keyed by subject.
#[derive(Default)]
pub struct Assembler {
    partial: HashMap<String, Partial>,
    max_bytes: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Feed {
    /// A complete payload.
    Complete(Bytes),
    /// More parts are needed.
    Pending,
    /// The part did not continue the subject's partial payload; whatever was
    /// buffered has been dropped (the checkpoint will be gap-filled).
    Broken,
}

impl Assembler {
    /// `max_bytes` bounds a single reassembled payload.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            partial: HashMap::new(),
            max_bytes,
        }
    }

    pub fn feed(
        &mut self,
        subject: &str,
        seq: u64,
        chunk: Option<(usize, usize)>,
        payload: Bytes,
    ) -> Feed {
        let Some((index, total)) = chunk else {
            // An unchunked message is complete on its own; it also ends any
            // partial payload on this subject.
            if self.partial.remove(subject).is_some() {
                tracing::warn!(subject, "chunk sequence interrupted");
            }
            return Feed::Complete(payload);
        };

        if index == 0 {
            if self.partial.remove(subject).is_some() {
                tracing::warn!(subject, seq, "chunk sequence restarted");
            }
            if total == 1 {
                return Feed::Complete(payload);
            }
            let mut buf = BytesMut::with_capacity(payload.len() * total);
            buf.extend_from_slice(&payload);
            self.partial.insert(
                subject.to_owned(),
                Partial {
                    seq,
                    total,
                    next: 1,
                    buf,
                },
            );
            return Feed::Pending;
        }

        let Some(partial) = self.partial.get_mut(subject) else {
            return Feed::Broken;
        };
        if partial.seq != seq
            || partial.total != total
            || partial.next != index
            || partial.buf.len() + payload.len() > self.max_bytes
        {
            self.partial.remove(subject);
            return Feed::Broken;
        }
        partial.buf.extend_from_slice(&payload);
        partial.next += 1;
        if partial.next < partial.total {
            return Feed::Pending;
        }
        let partial = self.partial.remove(subject).expect("present");
        Feed::Complete(Bytes::copy_from_slice(&partial.buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(payload: &[u8], max: usize) -> Vec<(Option<(usize, usize)>, Bytes)> {
        let parts = split(&Bytes::copy_from_slice(payload), max);
        let n = parts.len();
        parts
            .into_iter()
            .enumerate()
            .map(|(i, p)| {
                (
                    (n > 1).then(|| parse_header(&format_header(i, n)).unwrap()),
                    p,
                )
            })
            .collect()
    }

    #[test]
    fn split_sizes() {
        assert_eq!(split(&Bytes::from_static(b"abc"), 3).len(), 1);
        let p = split(&Bytes::from_static(b"abcdefg"), 3);
        assert_eq!(p, vec![&b"abc"[..], &b"def"[..], &b"g"[..]]);
    }

    #[test]
    fn header_round_trip() {
        assert_eq!(parse_header(&format_header(0, 3)), Some((0, 3)));
        assert_eq!(parse_header("3/3"), Some((2, 3)));
        assert_eq!(parse_header("0/3"), None);
        assert_eq!(parse_header("4/3"), None);
        assert_eq!(parse_header("x"), None);
    }

    #[test]
    fn reassembles_interleaved_subjects() {
        let mut asm = Assembler::new(1 << 20);
        let a = parts(b"aaaaaaaaaa", 4);
        let b = parts(b"bbbbbbb", 4);
        assert_eq!(asm.feed("a", 1, a[0].0, a[0].1.clone()), Feed::Pending);
        assert_eq!(asm.feed("b", 1, b[0].0, b[0].1.clone()), Feed::Pending);
        assert_eq!(asm.feed("a", 1, a[1].0, a[1].1.clone()), Feed::Pending);
        assert_eq!(
            asm.feed("b", 1, b[1].0, b[1].1.clone()),
            Feed::Complete(Bytes::from_static(b"bbbbbbb"))
        );
        assert_eq!(
            asm.feed("a", 1, a[2].0, a[2].1.clone()),
            Feed::Complete(Bytes::from_static(b"aaaaaaaaaa"))
        );
    }

    #[test]
    fn unchunked_passes_through() {
        let mut asm = Assembler::new(16);
        assert_eq!(
            asm.feed("a", 1, None, Bytes::from_static(b"x")),
            Feed::Complete(Bytes::from_static(b"x"))
        );
    }

    #[test]
    fn missing_part_breaks() {
        let mut asm = Assembler::new(1 << 20);
        let a = parts(b"aaaaaaaaaa", 4);
        assert_eq!(asm.feed("a", 1, a[0].0, a[0].1.clone()), Feed::Pending);
        assert_eq!(asm.feed("a", 1, a[2].0, a[2].1.clone()), Feed::Broken);
        // Orphan continuation with no start.
        assert_eq!(asm.feed("a", 2, Some((1, 3)), Bytes::new()), Feed::Broken);
        // A new checkpoint restarts cleanly.
        assert_eq!(asm.feed("a", 3, a[0].0, a[0].1.clone()), Feed::Pending);
        assert_eq!(asm.feed("a", 3, a[1].0, a[1].1.clone()), Feed::Pending);
        assert!(matches!(
            asm.feed("a", 3, a[2].0, a[2].1.clone()),
            Feed::Complete(_)
        ));
    }

    #[test]
    fn enforces_size_bound() {
        let mut asm = Assembler::new(6);
        let a = parts(b"aaaaaaaaaa", 4);
        assert_eq!(asm.feed("a", 1, a[0].0, a[0].1.clone()), Feed::Pending);
        assert_eq!(asm.feed("a", 1, a[1].0, a[1].1.clone()), Feed::Broken);
    }
}
