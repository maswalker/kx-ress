use alloy_primitives::{Bytes, B256};
use reth_network::NetworkHandle;
use reth_primitives::{BlockBody, Header};
use reth_ress_protocol::{GetHeaders, RessPeerRequest};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::trace;

#[derive(Clone, Debug)]
pub struct RessNetworkHandle {
    network_handle: NetworkHandle,

    peer_requests_sender: mpsc::UnboundedSender<RessPeerRequest>,
}

impl RessNetworkHandle {
    pub fn new(
        network_handle: NetworkHandle,
        peer_requests_sender: mpsc::UnboundedSender<RessPeerRequest>,
    ) -> Self {
        Self { network_handle, peer_requests_sender }
    }

    pub fn inner(&self) -> &NetworkHandle {
        &self.network_handle
    }

    fn send_request(&self, request: RessPeerRequest) -> Result<(), PeerRequestError> {
        self.peer_requests_sender.send(request).map_err(|_| PeerRequestError::ConnectionClosed)
    }
}

impl RessNetworkHandle {
    pub async fn fetch_headers(
        &self,
        request: GetHeaders,
    ) -> Result<Vec<Header>, PeerRequestError> {
        trace!(target: "ress::net", ?request, "requesting header");
        let (tx, rx) = oneshot::channel();
        self.send_request(RessPeerRequest::GetHeaders { request, tx })?;
        let response = rx.await.map_err(|_| PeerRequestError::RequestDropped)?;
        trace!(target: "ress::net", ?request, "headers received");
        Ok(response)
    }

    pub async fn fetch_block_bodies(
        &self,
        request: Vec<B256>,
    ) -> Result<Vec<BlockBody>, PeerRequestError> {
        trace!(target: "ress::net", ?request, "requesting block bodies");
        let (tx, rx) = oneshot::channel();
        self.send_request(RessPeerRequest::GetBlockBodies { request: request.clone(), tx })?;
        let response = rx.await.map_err(|_| PeerRequestError::RequestDropped)?;
        trace!(target: "ress::net", ?request, "block bodies received");
        Ok(response)
    }

    pub async fn fetch_bytecode(&self, code_hash: B256) -> Result<Bytes, PeerRequestError> {
        trace!(target: "ress::net", %code_hash, "requesting bytecode");
        let (tx, rx) = oneshot::channel();
        self.send_request(RessPeerRequest::GetBytecode { code_hash, tx })?;
        let response = rx.await.map_err(|_| PeerRequestError::RequestDropped)?;
        trace!(target: "ress::net", %code_hash, "bytecode received");
        Ok(response)
    }

    pub async fn fetch_witness(&self, block_hash: B256) -> Result<Vec<Bytes>, PeerRequestError> {
        trace!(target: "ress::net", %block_hash, "requesting witness");
        let (tx, rx) = oneshot::channel();
        self.send_request(RessPeerRequest::GetWitness { block_hash, tx })?;
        let response = rx.await.map_err(|_| PeerRequestError::RequestDropped)?;
        trace!(target: "ress::net", %block_hash, "witness received");
        Ok(response)
    }
}

#[derive(Debug, Error)]
pub enum PeerRequestError {
    #[error("Peer request dropped")]
    RequestDropped,

    #[error("Peer connection was closed")]
    ConnectionClosed,
}
