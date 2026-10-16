//! Response encodings for the SSE stream.
//!
//! Each event is compressed once and the same bytes are written to every
//! client, so the compressed body must be a concatenation of independently
//! produced pieces:
//!
//! - zstd: one complete zstd frame per event. The zstd format defines a
//!   stream as a sequence of frames; streaming decoders continue across frame
//!   boundaries.
//! - gzip: one gzip header per response, then per event a raw-deflate
//!   segment compressed from a fresh state and ended with a full flush
//!   (byte-aligned, no final block, no back-references across the boundary).
//!   Concatenated segments form one valid deflate stream, so decoders that
//!   only support single-member gzip still work. The stream never ends
//!   normally, so no trailer is written.

use bytes::Bytes;
use flate2::{Compress, Compression, FlushCompress};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Encoding {
    Identity,
    Zstd,
    Gzip,
}

pub const ZSTD_LEVEL: i32 = 3;
pub const GZIP_LEVEL: u32 = 4;

/// Fixed gzip member header: deflate, no flags, mtime 0, OS unknown.
const GZIP_HEADER: &[u8] = &[0x1f, 0x8b, 0x08, 0x00, 0, 0, 0, 0, 0x00, 0xff];

impl Encoding {
    /// Pick from `Accept-Encoding`: zstd, then gzip, then identity.
    /// Explicit `q=0` refusals are honoured.
    pub fn negotiate(accept: Option<&str>) -> Self {
        let Some(accept) = accept else {
            return Self::Identity;
        };
        if accepts(accept, "zstd") {
            Self::Zstd
        } else if accepts(accept, "gzip") {
            Self::Gzip
        } else {
            Self::Identity
        }
    }

    pub fn content_encoding(self) -> Option<&'static str> {
        match self {
            Self::Identity => None,
            Self::Zstd => Some("zstd"),
            Self::Gzip => Some("gzip"),
        }
    }

    pub fn label(self) -> &'static str {
        self.content_encoding().unwrap_or("identity")
    }

    /// Bytes written once at the start of a response body.
    pub fn preamble(self) -> Option<Bytes> {
        (self == Self::Gzip).then(|| Bytes::from_static(GZIP_HEADER))
    }

    /// Compress one self-contained piece of the stream.
    pub fn compress(self, data: &[u8]) -> Bytes {
        match self {
            Self::Identity => Bytes::copy_from_slice(data),
            Self::Zstd => common::exact_bytes(
                zstd::bulk::compress(data, ZSTD_LEVEL).expect("zstd compression of in-memory data"),
            ),
            Self::Gzip => deflate_segment(data),
        }
    }
}

/// Whether an `Accept-Encoding` value lists `name` (case-insensitive) without
/// refusing it with `q=0`.
pub fn accepts(accept: &str, name: &str) -> bool {
    accept.split(',').any(|item| {
        let mut parts = item.split(';').map(str::trim);
        let coding = parts.next().unwrap_or("");
        let refused = parts.any(|p| {
            p.strip_prefix("q=")
                .and_then(|q| q.parse::<f32>().ok())
                .is_some_and(|q| q == 0.0)
        });
        coding.eq_ignore_ascii_case(name) && !refused
    })
}

fn deflate_segment(data: &[u8]) -> Bytes {
    let mut compress = Compress::new(Compression::new(GZIP_LEVEL), false);
    let mut out = Vec::with_capacity(data.len() / 4 + 64);
    loop {
        let consumed = compress.total_in() as usize;
        out.reserve(data.len().saturating_sub(consumed) / 2 + 1024);
        compress
            .compress_vec(&data[consumed..], &mut out, FlushCompress::Full)
            .expect("deflate of in-memory data");
        // Done once all input is consumed and the flush fit in the buffer.
        if compress.total_in() as usize == data.len() && out.len() < out.capacity() {
            break;
        }
    }
    common::exact_bytes(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn negotiation() {
        assert_eq!(Encoding::negotiate(None), Encoding::Identity);
        assert_eq!(
            Encoding::negotiate(Some("gzip, deflate, br, zstd")),
            Encoding::Zstd
        );
        assert_eq!(Encoding::negotiate(Some("gzip, deflate")), Encoding::Gzip);
        assert_eq!(Encoding::negotiate(Some("zstd;q=0, gzip")), Encoding::Gzip);
        assert_eq!(Encoding::negotiate(Some("br")), Encoding::Identity);
        assert_eq!(Encoding::negotiate(Some("ZSTD")), Encoding::Zstd);
        assert!(accepts("gzip, br", "br"));
        assert!(!accepts("gzip, br;q=0", "br"));
    }

    fn events() -> Vec<Vec<u8>> {
        (0..5)
            .map(|i| {
                format!(
                    "id: {i}\nevent: checkpoint\ndata: {}\n\n",
                    "x".repeat(i * 5000)
                )
                .into_bytes()
            })
            .collect()
    }

    #[test]
    fn concatenated_zstd_frames_decode_as_one_stream() {
        let events = events();
        let mut body = Vec::new();
        for e in &events {
            body.extend_from_slice(&Encoding::Zstd.compress(e));
        }
        let decoded = zstd::stream::decode_all(&body[..]).unwrap();
        assert_eq!(decoded, events.concat());
    }

    #[test]
    fn concatenated_deflate_segments_decode_as_one_gzip_member() {
        let events = events();
        let mut body = Encoding::Gzip.preamble().unwrap().to_vec();
        let mut lens = vec![];
        for e in &events {
            let seg = Encoding::Gzip.compress(e);
            body.extend_from_slice(&seg);
            lens.push(body.len());
        }
        // Incremental decoding: each prefix ending at a segment boundary
        // yields exactly the events so far (no trailer; read to EOF errors).
        for (i, &len) in lens.iter().enumerate() {
            let mut decoder = flate2::read::GzDecoder::new(&body[..len]);
            let mut out = Vec::new();
            let _ = decoder.read_to_end(&mut out);
            assert_eq!(out, events[..=i].concat());
        }
    }

    #[test]
    fn large_segment() {
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i * 7 % 251) as u8).collect();
        let seg = deflate_segment(&data);
        let mut body = GZIP_HEADER.to_vec();
        body.extend_from_slice(&seg);
        let mut out = Vec::new();
        let _ = flate2::read::GzDecoder::new(&body[..]).read_to_end(&mut out);
        assert_eq!(out, data);
    }
}
