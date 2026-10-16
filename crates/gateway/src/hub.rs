//! Emitted checkpoints: broadcast to streams, kept in a ring (a cache of a
//! contiguous range; history comes from the bucket) for resume, and
//! rendered once per (checkpoint, mask, encoding) for all clients.
//!
//! Rendering is lazy: the first stream that needs a checkpoint under a mask
//! decodes it (once per checkpoint), applies the mask, serializes the SSE
//! event and compresses it (once per encoding); every other stream with the
//! same mask awaits and reuses the same bytes. Renders are dropped once a
//! checkpoint falls out of the live window to bound memory.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use prost::Message;
use sui_rpc::proto::sui::rpc::v2::{Checkpoint, SubscribeCheckpointsResponse};
use tokio::sync::{OnceCell, broadcast};

use crate::encoding::Encoding;
use crate::mask::{Mask, MaskKey};

#[derive(Clone, Debug)]
pub enum HubEvent {
    Checkpoint(Arc<Entry>),
    /// Checkpoints `from..=to` could not be delivered.
    Gap {
        from: u64,
        to: u64,
    },
}

pub struct Entry {
    pub seq: u64,
    /// zstd-compressed canonical `SubscribeCheckpointsResponse` from NATS.
    pub payload: Bytes,
    cache: Mutex<Option<Arc<EntryCache>>>,
}

#[derive(Default)]
struct EntryCache {
    decoded: OnceCell<Arc<Checkpoint>>,
    renders: Mutex<HashMap<MaskKey, Arc<Rendered>>>,
}

#[derive(Default)]
struct Rendered {
    identity: OnceCell<Bytes>,
    zstd: OnceCell<Bytes>,
    gzip: OnceCell<Bytes>,
}

pub type RenderError = String;

impl Entry {
    pub fn new(seq: u64, payload: Bytes) -> Self {
        Self {
            seq,
            payload,
            cache: Mutex::new(None),
        }
    }

    fn cache(&self) -> Arc<EntryCache> {
        self.cache
            .lock()
            .unwrap()
            .get_or_insert_with(Default::default)
            .clone()
    }

    fn drop_cache(&self) {
        self.cache.lock().unwrap().take();
    }

    async fn decoded(&self, cache: &EntryCache) -> Result<Arc<Checkpoint>, RenderError> {
        cache
            .decoded
            .get_or_try_init(|| {
                let payload = self.payload.clone();
                async move {
                    tokio::task::spawn_blocking(move || decode_payload(&payload))
                        .await
                        .map_err(|e| e.to_string())?
                }
            })
            .await
            .cloned()
    }

    /// The SSE event for this checkpoint under `mask`, in `encoding`.
    pub async fn event(&self, mask: &Mask, encoding: Encoding) -> Result<Bytes, RenderError> {
        let cache = self.cache();
        let rendered = cache
            .renders
            .lock()
            .unwrap()
            .entry(mask.key())
            .or_default()
            .clone();
        let identity = rendered
            .identity
            .get_or_try_init(|| async {
                let checkpoint = self.decoded(&cache).await?;
                let (mask, seq) = (mask.clone(), self.seq);
                tokio::task::spawn_blocking(move || render_event(seq, &mask.apply(&checkpoint)))
                    .await
                    .map_err(|e| e.to_string())?
            })
            .await?
            .clone();
        let cell = match encoding {
            Encoding::Identity => return Ok(identity),
            Encoding::Zstd => &rendered.zstd,
            Encoding::Gzip => &rendered.gzip,
        };
        cell.get_or_try_init(|| async move {
            tokio::task::spawn_blocking(move || encoding.compress(&identity))
                .await
                .map_err(|e| e.to_string())
        })
        .await
        .cloned()
    }
}

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry").field("seq", &self.seq).finish()
    }
}

pub fn decode_payload(payload: &[u8]) -> Result<Arc<Checkpoint>, RenderError> {
    let raw = zstd::stream::decode_all(payload).map_err(|e| format!("zstd: {e}"))?;
    let response =
        SubscribeCheckpointsResponse::decode(&raw[..]).map_err(|e| format!("decode: {e}"))?;
    response
        .checkpoint
        .map(Arc::new)
        .ok_or_else(|| "frame without checkpoint".to_owned())
}

/// `id: <seq>\nevent: checkpoint\ndata: <json>\n\n`. serde_json never emits
/// raw newlines, so the data is a single line.
pub fn render_event(seq: u64, checkpoint: &Checkpoint) -> Result<Bytes, RenderError> {
    let mut out = format!("id: {seq}\nevent: checkpoint\ndata: ").into_bytes();
    serde_json::to_writer(&mut out, checkpoint).map_err(|e| e.to_string())?;
    out.extend_from_slice(b"\n\n");
    Ok(common::exact_bytes(out))
}

pub fn gap_event(from: u64, to: u64) -> Bytes {
    format!("event: gap\ndata: {{\"from\":{from},\"to\":{to}}}\n\n").into()
}

/// Checkpoints kept for `Last-Event-ID` / `from` resume.
struct Ring {
    entries: VecDeque<Arc<Entry>>,
    bytes: usize,
}

pub struct Hub {
    tx: broadcast::Sender<HubEvent>,
    ring: Mutex<Ring>,
    ring_max_bytes: usize,
    /// Entries this far behind the head drop their rendered output.
    render_window: usize,
    lag_capacity: usize,
}

impl Hub {
    /// `lag_capacity`: how many checkpoints a stream may fall behind before
    /// it is disconnected as a slow consumer.
    pub fn new(lag_capacity: usize, ring_max_bytes: usize) -> Self {
        Self {
            tx: broadcast::channel(lag_capacity).0,
            ring: Mutex::new(Ring {
                entries: VecDeque::new(),
                bytes: 0,
            }),
            ring_max_bytes,
            render_window: lag_capacity + 8,
            lag_capacity,
        }
    }

    /// Live checkpoints a subscriber may fall behind before it lags out.
    pub fn lag_capacity(&self) -> usize {
        self.lag_capacity
    }

    pub fn subscribe(&self) -> broadcast::Receiver<HubEvent> {
        self.tx.subscribe()
    }

    pub fn emit(&self, entry: Arc<Entry>) {
        {
            let mut ring = self.ring.lock().unwrap();
            ring.bytes += entry.payload.len();
            ring.entries.push_back(entry.clone());
            while ring.bytes > self.ring_max_bytes && ring.entries.len() > 1 {
                let old = ring.entries.pop_front().expect("non-empty");
                ring.bytes -= old.payload.len();
            }
            let len = ring.entries.len();
            if len > self.render_window {
                ring.entries[len - 1 - self.render_window].drop_cache();
            }
        }
        let _ = self.tx.send(HubEvent::Checkpoint(entry));
    }

    /// Drop renders of an entry outside the live window (after a resume
    /// replayed it), so replays do not pin rendered output in the ring.
    pub fn release_if_stale(&self, entry: &Entry) {
        let ring = self.ring.lock().unwrap();
        if let Some(last) = ring.entries.back()
            && entry.seq + (self.render_window as u64) < last.seq
        {
            entry.drop_cache();
        }
    }

    /// Live checkpoints `from..=to` are lost. The ring is a cache of a
    /// contiguous range, so it restarts after the gap; resumes from before
    /// it are served from the bucket.
    pub fn gap(&self, from: u64, to: u64) {
        {
            let mut ring = self.ring.lock().unwrap();
            ring.entries.clear();
            ring.bytes = 0;
        }
        let _ = self.tx.send(HubEvent::Gap { from, to });
    }

    /// Lowest seq held by the ring.
    pub fn first(&self) -> Option<u64> {
        self.ring.lock().unwrap().entries.front().map(|e| e.seq)
    }

    /// Ring entries with `seq >= from`, and the lowest seq held.
    pub fn replay_from(&self, from: u64) -> (Option<u64>, Vec<Arc<Entry>>) {
        let ring = self.ring.lock().unwrap();
        let first = ring.entries.front().map(|e| e.seq);
        let start = ring.entries.partition_point(|e| e.seq < from);
        (first, ring.entries.range(start..).cloned().collect())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn payload(seq: u64) -> Bytes {
        let mut cp = Checkpoint::default();
        cp.sequence_number = Some(seq);
        cp.digest = Some(format!("digest-{seq}"));
        let mut resp = SubscribeCheckpointsResponse::default();
        resp.cursor = Some(seq);
        resp.checkpoint = Some(cp);
        zstd::bulk::compress(&resp.encode_to_vec(), 3)
            .unwrap()
            .into()
    }

    #[tokio::test]
    async fn renders_and_shares() {
        let entry = Entry::new(5, payload(5));
        let full = entry.event(&Mask::Full, Encoding::Identity).await.unwrap();
        let text = std::str::from_utf8(&full).unwrap();
        assert!(text.starts_with("id: 5\nevent: checkpoint\ndata: {"));
        assert!(text.ends_with("}\n\n"));
        assert_eq!(text.matches('\n').count(), 4);
        assert!(text.contains("\"digest\":\"digest-5\""));

        let again = entry.event(&Mask::Full, Encoding::Identity).await.unwrap();
        assert_eq!(full.as_ptr(), again.as_ptr(), "rendered once, shared");

        let masked = Mask::parse(Some("sequence_number")).unwrap();
        let m = entry.event(&masked, Encoding::Identity).await.unwrap();
        assert!(!std::str::from_utf8(&m).unwrap().contains("digest"));

        let z = entry.event(&Mask::Full, Encoding::Zstd).await.unwrap();
        assert_eq!(zstd::stream::decode_all(&z[..]).unwrap(), full);
    }

    #[test]
    fn ring_replay_and_eviction() {
        let hub = Hub::new(4, payload(1).len() * 3);
        for seq in 1..=5 {
            hub.emit(Arc::new(Entry::new(seq, payload(seq))));
        }
        let (first, entries) = hub.replay_from(4);
        assert_eq!(first, Some(3));
        assert_eq!(
            entries.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![4, 5]
        );
        let (_, all) = hub.replay_from(0);
        assert_eq!(all.len(), 3);
        assert!(hub.replay_from(6).1.is_empty());
        assert_eq!(hub.first(), Some(3));
        // A gap restarts the ring so it never holds a hole.
        hub.gap(6, 7);
        assert_eq!(hub.first(), None);
        hub.emit(Arc::new(Entry::new(8, payload(8))));
        assert_eq!(hub.replay_from(0).0, Some(8));
    }
}
