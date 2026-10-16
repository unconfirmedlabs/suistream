//! Against a real gateway; run with `cargo test -p suistream -- --ignored`.
//! `SUISTREAM_URL` overrides the endpoint (default: production Mainnet).

use std::time::Duration;

use futures::StreamExt;
use suistream::Event;

fn builder() -> suistream::Builder {
    let b = suistream::Client::builder();
    match std::env::var("SUISTREAM_URL") {
        Ok(url) => b.url(url),
        Err(_) => b,
    }
}

/// `n` events; checkpoints must be contiguous (gaps are returned as such).
async fn take(builder: suistream::Builder, n: usize) -> Vec<Event> {
    let mut sub = builder.build().unwrap().subscribe();
    let mut out: Vec<Event> = Vec::new();
    let mut next = None;
    while out.len() < n {
        let ev = tokio::time::timeout(Duration::from_secs(120), sub.next())
            .await
            .expect("progress")
            .expect("open")
            .expect("event");
        match &ev {
            Event::Checkpoint { seq, checkpoint } => {
                if let Some(n) = next {
                    assert_eq!(*seq, n, "contiguous");
                }
                if let Some(s) = checkpoint.sequence_number {
                    assert_eq!(s, *seq);
                }
                next = Some(seq + 1);
            }
            Event::Gap { from, to } => {
                if let Some(n) = next {
                    assert_eq!(*from, n, "gap starts at the cursor");
                }
                next = Some(to + 1);
            }
        }
        out.push(ev);
    }
    assert_eq!(sub.cursor(), next.map(|n| n - 1));
    out
}

fn first_seq(events: &[Event]) -> u64 {
    match &events[0] {
        Event::Checkpoint { seq, .. } => *seq,
        Event::Gap { from, .. } => *from,
    }
}

#[tokio::test]
#[ignore]
async fn live_full_checkpoints_decode() {
    // Full canonical checkpoints decode with the crates.io sui-rpc types.
    let events = take(builder(), 10).await;
    for ev in &events {
        let Event::Checkpoint { checkpoint, .. } = ev else {
            panic!("unexpected gap")
        };
        assert!(checkpoint.summary.is_some() && checkpoint.contents.is_some());
    }
}

#[tokio::test]
#[ignore]
async fn resume_serves_history() {
    // Tip from a first subscription, then resume 1500 back (~6 min, older
    // than the ring): the server fills it from the bucket. Against a gateway
    // without history this yields a gap first.
    let tip = first_seq(&take(builder().read_mask(["digest"]), 1).await);
    let events = take(builder().read_mask(["digest"]).start_at(tip - 1500), 1600).await;
    assert_eq!(first_seq(&events), tip - 1500);
    let gaps = events
        .iter()
        .filter(|e| matches!(e, Event::Gap { .. }))
        .count();
    eprintln!("resume from {}: {gaps} gap events", tip - 1500);
}
