//! Checkpoint ingester: subscribes to one upstream source and publishes
//! canonical, zstd-compressed checkpoints to NATS.

pub mod fetch;
pub mod frame;
pub mod metrics;
pub mod publish;
pub mod stream;
pub mod upstream;
