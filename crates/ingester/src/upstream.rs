//! gRPC access to one upstream source.

use std::time::Duration;

use anyhow::{Context, Result};
use common::config::Source;
use sui_rpc::Client;
use sui_rpc::client::HeadersInterceptor;
use sui_rpc::field::FieldMaskUtil;
use sui_rpc::proto::sui::rpc::v2::{
    Checkpoint, GetCheckpointRequest, GetServiceInfoRequest, SubscribeCheckpointsRequest,
    SubscribeCheckpointsResponse, get_checkpoint_request::CheckpointId,
};
use tonic::metadata::{MetadataKey, MetadataValue};

pub type CheckpointStream = tonic::Streaming<SubscribeCheckpointsResponse>;

#[derive(Clone)]
pub struct Upstream {
    source: Source,
    headers: HeadersInterceptor,
    /// Shared connection for unary calls (backfill, fetch fallback).
    unary: Client,
}

impl Upstream {
    pub fn new(source: Source) -> Result<Self> {
        let mut headers = HeadersInterceptor::new();
        for (name, value) in &source.headers {
            let key = MetadataKey::from_bytes(name.as_bytes())
                .with_context(|| format!("header name {name:?}"))?;
            let mut value: MetadataValue<_> = value
                .parse()
                .with_context(|| format!("header {name} value"))?;
            value.set_sensitive(true);
            headers.headers_mut().insert(key, value);
        }
        let unary = Self::build_client(&source, &headers)?;
        Ok(Self {
            source,
            headers,
            unary,
        })
    }

    fn build_client(source: &Source, headers: &HeadersInterceptor) -> Result<Client> {
        Ok(Client::new(source.url.as_str())
            .with_context(|| format!("client for {}", source.url))?
            .with_headers(headers.clone())
            .with_max_decoding_message_size(common::MAX_DECODING_MESSAGE_SIZE)
            .with_response_headers_timeout(Duration::from_secs(60)))
    }

    pub fn source(&self) -> &Source {
        &self.source
    }

    /// Open a checkpoint subscription with the canonical mask on a fresh
    /// connection, so make-before-break rotation also replaces connections
    /// the server is about to age out.
    pub async fn subscribe(&self) -> Result<CheckpointStream, tonic::Status> {
        let mut client = Self::build_client(&self.source, &self.headers)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let mut request = SubscribeCheckpointsRequest::default();
        request.read_mask = Some(canonical_mask());
        Ok(client
            .subscription_client()
            .subscribe_checkpoints(request)
            .await?
            .into_inner())
    }

    /// Fetch one checkpoint with the canonical mask. `Ok(None)` if the source
    /// does not have it.
    pub async fn get_checkpoint(&self, seq: u64) -> Result<Option<Checkpoint>, tonic::Status> {
        let mut request = GetCheckpointRequest::default();
        request.read_mask = Some(canonical_mask());
        request.checkpoint_id = Some(CheckpointId::SequenceNumber(seq));
        let mut request = tonic::Request::new(request);
        // Epoch-change checkpoints are 10-20 MB and slow to serve.
        request.set_timeout(Duration::from_secs(60));
        match self
            .unary
            .clone()
            .ledger_client()
            .get_checkpoint(request)
            .await
        {
            Ok(response) => Ok(response.into_inner().checkpoint),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => Err(status),
        }
    }

    /// `GetServiceInfo.server` (node version string).
    pub async fn server_version(&self) -> Result<String, tonic::Status> {
        let mut request = tonic::Request::new(GetServiceInfoRequest::default());
        request.set_timeout(Duration::from_secs(10));
        let info = self
            .unary
            .clone()
            .ledger_client()
            .get_service_info(request)
            .await?
            .into_inner();
        Ok(info.server.unwrap_or_default())
    }
}

pub fn canonical_mask() -> prost_types::FieldMask {
    prost_types::FieldMask::from_paths(common::CANONICAL_READ_MASK)
}
