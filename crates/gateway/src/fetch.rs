//! Gap-fill requests to the ingesters over `sui.mainnet.fetch`.
//!
//! The request is broadcast to every ingester with a private reply inbox;
//! each answers from its ring (or `GetCheckpoint`). The first successful
//! reply per checkpoint wins; `Sui-Error` replies are ignored.

use std::collections::HashSet;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::metrics::Metrics;
use crate::receive::{Parsed, parse_message};
use crate::sequencer::{Incoming, Input};

#[derive(Clone)]
pub struct Fetcher {
    pub nats: async_nats::Client,
    pub timeout: Duration,
    pub input_bytes: Arc<AtomicUsize>,
    pub metrics: Metrics,
}

impl Fetcher {
    pub fn spawn(&self, range: RangeInclusive<u64>, tx: mpsc::UnboundedSender<Input>) {
        let this = self.clone();
        tokio::spawn(async move {
            let filled = match this.fetch(range.clone(), &tx).await {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(?range, "fetch failed: {e:#}");
                    0
                }
            };
            let wanted = range.end() - range.start() + 1;
            this.metrics
                .fetched
                .with_label_values(&[if filled == wanted {
                    "complete"
                } else {
                    "incomplete"
                }])
                .inc();
            let _ = tx.send(Input::FetchDone(range));
        });
    }

    async fn fetch(
        &self,
        range: RangeInclusive<u64>,
        tx: &mpsc::UnboundedSender<Input>,
    ) -> anyhow::Result<u64> {
        let inbox = self.nats.new_inbox();
        let mut replies = self.nats.subscribe(inbox.clone()).await?;
        let request = if range.start() == range.end() {
            range.start().to_string()
        } else {
            format!("{}-{}", range.start(), range.end())
        };
        self.nats
            .publish_with_reply(common::FETCH_SUBJECT, inbox, request.into())
            .await?;

        let mut missing: HashSet<u64> = range.clone().collect();
        let mut assembler = common::chunk::Assembler::new(common::MAX_DECODING_MESSAGE_SIZE);
        let deadline = tokio::time::sleep(self.timeout);
        tokio::pin!(deadline);
        let wanted = missing.len() as u64;
        while !missing.is_empty() {
            let msg = tokio::select! {
                msg = replies.next() => match msg { Some(m) => m, None => break },
                _ = &mut deadline => break,
            };
            let Some(parsed) = parse_message(&msg) else {
                continue;
            };
            if parsed.error.is_some() || !missing.contains(&parsed.seq) {
                continue;
            }
            // Several ingesters may interleave chunks on one inbox; key the
            // assembler by responder.
            let key = format!("{}.{}", parsed.source, parsed.region);
            let Parsed {
                seq,
                chunk,
                timestamp_ms,
                ..
            } = parsed;
            let common::chunk::Feed::Complete(payload) =
                assembler.feed(&key, seq, chunk, msg.payload.clone())
            else {
                continue;
            };
            missing.remove(&seq);
            let payload = bytes::Bytes::copy_from_slice(&payload);
            self.input_bytes.fetch_add(payload.len(), Ordering::Relaxed);
            let _ = tx.send(Input::Checkpoint(Incoming {
                seq,
                source: format!("fetch:{key}").into(),
                timestamp_ms,
                payload,
            }));
        }
        let _ = replies.unsubscribe().await;
        Ok(wanted - missing.len() as u64)
    }
}
