//! A checkpoint ready for NATS: canonical protobuf, zstd-compressed once.

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use prost::Message;
use sui_rpc::proto::sui::rpc::v2::{Checkpoint, SubscribeCheckpointsResponse};

/// zstd level for NATS payloads. Phase 0: level 3 is ~8 ms per 1 MB
/// checkpoint; higher levels cost several times the CPU for a few percent.
pub const ZSTD_LEVEL: i32 = 3;

#[derive(Clone, Debug)]
pub struct Frame {
    pub seq: u64,
    pub digest: String,
    pub timestamp_ms: u64,
    /// Size of the uncompressed encoded `SubscribeCheckpointsResponse`.
    pub raw_len: usize,
    /// zstd-compressed encoded `SubscribeCheckpointsResponse`.
    pub payload: Bytes,
}

impl Frame {
    /// Re-encode a checkpoint (from a stream frame or `GetCheckpoint`) as a
    /// canonical `SubscribeCheckpointsResponse` and compress it.
    pub fn from_checkpoint(checkpoint: Checkpoint) -> Result<Self> {
        let seq = checkpoint
            .sequence_number
            .ok_or_else(|| anyhow!("checkpoint without sequence_number"))?;
        let digest = checkpoint.digest.clone().unwrap_or_default();
        let timestamp_ms = checkpoint
            .summary
            .as_ref()
            .and_then(|s| s.timestamp)
            .and_then(|t| sui_rpc::proto::proto_to_timestamp_ms(t).ok())
            .unwrap_or_default();
        let mut response = SubscribeCheckpointsResponse::default();
        response.cursor = Some(seq);
        response.checkpoint = Some(checkpoint);
        let raw = response.encode_to_vec();
        let payload = zstd::bulk::compress(&raw, ZSTD_LEVEL).context("zstd compress")?;
        Ok(Self {
            seq,
            digest,
            timestamp_ms,
            raw_len: raw.len(),
            payload: common::exact_bytes(payload),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let mut checkpoint = Checkpoint::default();
        checkpoint.sequence_number = Some(42);
        checkpoint.digest = Some("abc".into());
        let frame = Frame::from_checkpoint(checkpoint.clone()).unwrap();
        assert_eq!(frame.seq, 42);
        assert_eq!(frame.digest, "abc");
        let raw = zstd::decode_all(&frame.payload[..]).unwrap();
        let decoded = SubscribeCheckpointsResponse::decode(&raw[..]).unwrap();
        assert_eq!(decoded.cursor, Some(42));
        assert_eq!(decoded.checkpoint, Some(checkpoint));
    }

    #[test]
    fn requires_sequence_number() {
        assert!(Frame::from_checkpoint(Checkpoint::default()).is_err());
    }
}
