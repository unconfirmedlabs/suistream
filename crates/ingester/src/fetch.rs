//! Answers gateway gap-fill requests on `sui.mainnet.fetch`.
//!
//! Every ingester receives every request (no queue group); the gateway takes
//! the first successful reply. Frames come from this ingester's ring of
//! published frames, falling back to `GetCheckpoint` for checkpoints at or
//! below the latest one published (so the source is known to have them).

use std::ops::RangeInclusive;
use std::sync::Arc;

use async_nats::HeaderMap;
use futures::StreamExt;
use tokio::sync::{Semaphore, watch};

use crate::frame::Frame;
use crate::publish::{Publisher, fetch_with_retry};
use crate::upstream::Upstream;

/// Parse `<seq>` or `<from>-<to>` (inclusive, at most `MAX_FETCH_RANGE`).
pub fn parse_request(payload: &[u8]) -> Option<RangeInclusive<u64>> {
    let text = std::str::from_utf8(payload).ok()?.trim();
    let (from, to) = match text.split_once('-') {
        Some((a, b)) => (a.parse().ok()?, b.parse().ok()?),
        None => {
            let seq = text.parse().ok()?;
            (seq, seq)
        }
    };
    (from <= to && to - from < common::MAX_FETCH_RANGE).then_some(from..=to)
}

pub async fn run(
    publisher: Publisher,
    upstream: Upstream,
    latest: watch::Receiver<Option<u64>>,
) -> anyhow::Result<()> {
    let mut requests = publisher.nats.subscribe(common::FETCH_SUBJECT).await?;
    // Bound concurrent upstream fallbacks.
    let upstream_permits = Arc::new(Semaphore::new(4));
    while let Some(msg) = requests.next().await {
        let Some(reply) = msg.reply.clone() else {
            continue;
        };
        let Some(range) = parse_request(&msg.payload) else {
            tracing::debug!("malformed fetch request");
            continue;
        };
        let publisher = publisher.clone();
        let upstream = upstream.clone();
        let latest = *latest.borrow();
        let permits = upstream_permits.clone();
        tokio::spawn(async move {
            for seq in range {
                let result = serve_one(&publisher, &upstream, latest, &permits, seq).await;
                let label = match &result {
                    Ok(_) => "ok",
                    Err(e) => *e,
                };
                publisher
                    .metrics
                    .fetch_replies
                    .with_label_values(&[label])
                    .inc();
                let sent = match result {
                    Ok(frame) => publisher.send(reply.as_str(), &frame).await,
                    Err(reason) => {
                        let mut headers = HeaderMap::new();
                        headers.insert(common::HEADER_CHECKPOINT, seq.to_string().as_str());
                        headers.insert(common::HEADER_SOURCE, publisher.source.as_str());
                        headers.insert(common::HEADER_REGION, publisher.region.as_str());
                        headers.insert(common::HEADER_ERROR, reason);
                        publisher
                            .nats
                            .publish_with_headers(reply.clone(), headers, Default::default())
                            .await
                            .map_err(Into::into)
                    }
                };
                if let Err(e) = sent {
                    tracing::debug!(seq, "fetch reply: {e:#}");
                }
            }
        });
    }
    Ok(())
}

async fn serve_one(
    publisher: &Publisher,
    upstream: &Upstream,
    latest: Option<u64>,
    permits: &Semaphore,
    seq: u64,
) -> Result<Arc<Frame>, &'static str> {
    if let Some(frame) = publisher.ring.lock().unwrap().get(seq) {
        return Ok(frame);
    }
    if latest.is_none_or(|l| seq > l) {
        return Err(common::ERROR_NOT_FOUND);
    }
    let _permit = permits
        .acquire()
        .await
        .map_err(|_| common::ERROR_INTERNAL)?;
    match fetch_with_retry(upstream, seq).await {
        Ok(Some(cp)) => tokio::task::spawn_blocking(move || Frame::from_checkpoint(cp))
            .await
            .ok()
            .and_then(Result::ok)
            .map(Arc::new)
            .ok_or(common::ERROR_INTERNAL),
        Ok(None) => Err(common::ERROR_NOT_FOUND),
        Err(e) => {
            tracing::warn!(seq, "fetch fallback failed: {e:#}");
            Err(common::ERROR_INTERNAL)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_requests() {
        assert_eq!(parse_request(b"42"), Some(42..=42));
        assert_eq!(parse_request(b" 10-12 "), Some(10..=12));
        assert_eq!(parse_request(b"12-10"), None);
        assert_eq!(parse_request(b"0-99"), Some(0..=99));
        assert_eq!(parse_request(b"0-100"), None);
        assert_eq!(parse_request(b"x"), None);
        assert_eq!(parse_request(b""), None);
    }
}
