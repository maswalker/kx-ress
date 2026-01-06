// Removed: receiving messages from consensus client, but keeping ConsensusEngine core functionality

use crate::{
    download::{DownloadData, DownloadOutcome, EngineDownloader},
    tree::{DownloadRequest, EngineTree, TreeAction, TreeEvent},
};
use alloy_primitives::{map::B256HashSet, B256};
use alloy_rpc_types_engine::{PayloadStatus, PayloadStatusEnum};
use futures::{FutureExt, StreamExt};
use metrics::Histogram;
use ress_network::RessNetworkHandle;
use ress_primitives::witness::ExecutionWitness;
use ress_provider::RessProvider;
use reth_chainspec::ChainSpec;
use reth_engine_tree::tree::error::InsertBlockFatalError;
use reth_errors::ProviderError;
use reth_metrics::Metrics;
use reth_node_api::{BeaconConsensusEngineEvent, BeaconOnNewPayloadError};
use reth_node_ethereum::{consensus::EthBeaconConsensus, EthereumEngineValidator};
use reth_primitives::EthPrimitives;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Sleep,
};
// Removed: UnboundedReceiverStream - no longer receiving messages from consensus client
use tracing::*;

/// Metrics for the consensus engine
#[derive(Metrics)]
#[metrics(scope = "engine")]
pub(crate) struct ConsensusEngineMetrics {
    /// Histogram of witness sizes in bytes.
    pub witness_size_bytes: Histogram,
    /// Histogram of witness node counts.
    pub witness_nodes_count: Histogram,
}

/// Ress consensus engine.
#[allow(missing_debug_implementations)]
pub struct ConsensusEngine {
    tree: EngineTree,
    downloader: EngineDownloader,
    // Removed: from_beacon_engine - no longer receiving messages from consensus client
    parked_payload_timeout: Duration,
    parked_payload: Option<ParkedPayload>,
    metrics: ConsensusEngineMetrics,
}

impl ConsensusEngine {
    /// Initialize consensus engine.
    pub fn new(
        provider: RessProvider,
        consensus: EthBeaconConsensus<ChainSpec>,
        engine_validator: EthereumEngineValidator,
        network: RessNetworkHandle,
        engine_events_sender: mpsc::UnboundedSender<BeaconConsensusEngineEvent<EthPrimitives>>,
    ) -> Self {
        Self {
            tree: EngineTree::new(
                provider,
                consensus.clone(),
                engine_validator,
                engine_events_sender,
            ),
            downloader: EngineDownloader::new(network, consensus),
            parked_payload_timeout: Duration::from_secs(3),
            parked_payload: None,
            metrics: Default::default(),
        }
    }

    fn on_maybe_tree_event(&mut self, maybe_event: Option<TreeEvent>) {
        if let Some(event) = maybe_event {
            self.on_tree_event(event);
        }
    }

    fn on_tree_event(&mut self, event: TreeEvent) {
        match event {
            TreeEvent::Download(DownloadRequest::Block { block_hash }) => {
                self.downloader.download_full_block(block_hash);
                if !self.tree.block_buffer.witnesses.contains_key(&block_hash) {
                    self.downloader.download_witness(block_hash);
                }
            }
            TreeEvent::Download(DownloadRequest::Witness { block_hash }) => {
                self.downloader.download_witness(block_hash);
            }
            TreeEvent::Download(DownloadRequest::Finalized { block_hash }) => {
                self.downloader.download_finalized_with_ancestors(block_hash);
            }
            TreeEvent::TreeAction(TreeAction::MakeCanonical { sync_target_head }) => {
                self.tree.make_canonical(sync_target_head);
            }
        }
    }

    fn on_download_outcome(
        &mut self,
        outcome: DownloadOutcome,
    ) -> Result<(), InsertBlockFatalError> {
        let elapsed = outcome.elapsed;
        let mut unlocked_block_hashes = B256HashSet::default();
        match outcome.data {
            DownloadData::FinalizedBlock(block, ancestors) => {
                let block_num_hash = block.num_hash();
                info!(target: "ress::engine", ?block_num_hash, ancestors_len = ancestors.len(), "Downloaded finalized block");

                let recovered = block.try_recover().map_err(|_| {
                    InsertBlockFatalError::Provider(ProviderError::SenderRecoveryError)
                })?;
                self.tree.set_canonical_head(block_num_hash);
                self.tree.provider.insert_canonical_hash(recovered.number, recovered.hash());
                self.tree.provider.insert_block(recovered, None);
                for header in ancestors {
                    self.tree.provider.insert_canonical_hash(header.number, header.hash());
                }
                unlocked_block_hashes.insert(block_num_hash.hash);
            }
            DownloadData::FullBlock(block) => {
                let block_num_hash = block.num_hash();
                trace!(target: "ress::engine", ?block_num_hash, ?elapsed, "Downloaded block");
                let recovered = match block.try_recover() {
                    Ok(block) => block,
                    Err(_error) => {
                        debug!(target: "ress::engine", ?block_num_hash, "Error recovering downloaded block");
                        return Ok(())
                    }
                };
                self.tree.block_buffer.insert_block(recovered);
                unlocked_block_hashes.insert(block_num_hash.hash);
            }
            DownloadData::Witness(block_hash, witness) => {
                let code_hashes = witness.bytecode_hashes().clone();
                let missing_code_hashes =
                    self.tree.provider.missing_code_hashes(code_hashes).map_err(|error| {
                        InsertBlockFatalError::Provider(ProviderError::Database(error))
                    })?;
                let missing_bytecodes_len = missing_code_hashes.len();
                let rlp_size = humansize::format_size(witness.rlp_size_bytes(), humansize::DECIMAL);
                let witness_nodes_count = witness.state_witness().len();

                // Record witness metrics before inserting the witness
                self.record_witness_metrics(&witness);

                self.tree.block_buffer.insert_witness(
                    block_hash,
                    witness,
                    missing_code_hashes.clone(),
                );

                if Some(block_hash) == self.parked_payload.as_ref().map(|parked| parked.block_hash)
                {
                    info!(target: "ress::engine", %block_hash, missing_bytecodes_len, %rlp_size, witness_nodes_count, ?elapsed, "Downloaded for parked payload");
                } else {
                    trace!(target: "ress::engine", %block_hash, missing_bytecodes_len, %rlp_size, witness_nodes_count, ?elapsed, "Downloaded witness");
                }
                if missing_code_hashes.is_empty() {
                    unlocked_block_hashes.insert(block_hash);
                } else {
                    for code_hash in missing_code_hashes {
                        self.downloader.download_bytecode(code_hash);
                    }
                }
            }
            DownloadData::Bytecode(code_hash, bytecode) => {
                trace!(target: "ress::engine", %code_hash, ?elapsed, "Downloaded bytecode");
                match self.tree.provider.insert_bytecode(code_hash, bytecode) {
                    Ok(()) => {
                        unlocked_block_hashes
                            .extend(self.tree.block_buffer.on_bytecode_received(code_hash));
                    }
                    Err(error) => {
                        error!(target: "ress::engine", %error, "Failed to insert the bytecode");
                    }
                };
            }
        };

        for unlocked_hash in unlocked_block_hashes {
            let Some((block, witness)) = self.tree.block_buffer.remove_block(&unlocked_hash) else {
                continue
            };
            let block_num_hash = block.num_hash();
            trace!(target: "ress::engine", block = ?block_num_hash, "Inserting block after download");
            let mut result = self
                .tree
                .on_downloaded_block(block, witness)
                .map_err(BeaconOnNewPayloadError::internal);
            match &mut result {
                Ok(outcome) => {
                    self.on_maybe_tree_event(outcome.event.take());
                }
                Err(error) => {
                    error!(target: "ress::engine", block = ?block_num_hash, %error, "Error inserting downloaded block");
                }
            };
            if self
                .parked_payload
                .as_ref()
                .is_some_and(|parked| parked.block_hash == block_num_hash.hash)
            {
                let parked = self.parked_payload.take().unwrap();
                trace!(target: "ress::engine",  block = ?block_num_hash, elapsed = ?parked.parked_at.elapsed(), "Sending response for parked payload");
                if let Err(error) = parked.tx.send(result.map(|o| o.outcome)) {
                    error!(target: "ress::engine",  block = ?block_num_hash, ?error, "Failed to send payload status");
                }
            }
        }

        Ok(())
    }

    // Removed: on_engine_message - no longer receiving messages from consensus client

    /// Record witness metrics
    fn record_witness_metrics(&self, witness: &ExecutionWitness) {
        let witness_size_bytes = witness.rlp_size_bytes();
        let witness_nodes_count = witness.state_witness().len();

        self.metrics.witness_size_bytes.record(witness_size_bytes as f64);
        self.metrics.witness_nodes_count.record(witness_nodes_count as f64);
    }
}

impl Future for ConsensusEngine {
    type Output = Result<(), InsertBlockFatalError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            if let Poll::Ready(outcome) = this.downloader.poll(cx) {
                this.on_download_outcome(outcome)?;
                continue;
            }

            if let Some(parked) = &mut this.parked_payload {
                if parked.timeout.poll_unpin(cx).is_ready() {
                    let parked = this.parked_payload.take().unwrap();
                    warn!(target: "ress::engine", block_hash = %parked.block_hash, "Could not download missing payload data in time");
                    let status = PayloadStatus::from_status(PayloadStatusEnum::Syncing);
                    if let Err(error) = parked.tx.send(Ok(status)) {
                        error!(target: "ress::engine", ?error, "Failed to send parked payload status");
                    }
                } else {
                    return Poll::Pending
                }
            }

            // Removed: polling from_beacon_engine - no longer receiving messages from consensus client

            return Poll::Pending
        }
    }
}

struct ParkedPayload {
    block_hash: B256,
    tx: oneshot::Sender<Result<PayloadStatus, BeaconOnNewPayloadError>>,
    parked_at: Instant,
    timeout: Pin<Box<Sleep>>,
}

impl ParkedPayload {
    fn new(
        block_hash: B256,
        tx: oneshot::Sender<Result<PayloadStatus, BeaconOnNewPayloadError>>,
        timeout: Duration,
    ) -> Self {
        Self {
            block_hash,
            tx,
            parked_at: Instant::now(),
            timeout: Box::pin(tokio::time::sleep(timeout)),
        }
    }
}
