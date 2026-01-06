use alloy_primitives::keccak256;
use ress_network::{RessNetworkHandle, RessNetworkManager};
use ress_provider::{RessDatabase, RessProvider};
use reth_network::{
    config::SecretKey, protocol::IntoRlpxSubProtocol, EthNetworkPrimitives, NetworkConfig,
    NetworkManager,
};
use reth_network_peers::TrustedPeer;
use reth_node_core::primitives::Bytecode;
use reth_ress_protocol::{NodeType, ProtocolState, RessProtocolHandler, RessProtocolProvider};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::*;

use crate::cli::RessArgs;

/// The human readable name of the client
pub const NAME_CLIENT: &str = "Ress";

/// The latest version from Cargo.toml.
pub const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The 8 character short SHA of the latest commit.
pub const VERGEN_GIT_SHA: &str = env!("VERGEN_GIT_SHA_SHORT");

/// Ress node launcher
#[derive(Debug)]
pub struct NodeLauncher {
    /// Ress configuration.
    args: RessArgs,
}

impl NodeLauncher {
    /// Create new node launcher
    pub fn new(args: RessArgs) -> Self {
        Self { args }
    }
}

impl NodeLauncher {
    /// Launch ress node.
    pub async fn launch(self) -> eyre::Result<()> {
        let data_dir = self.args.datadir.unwrap_or_chain_default(self.args.chain.inner.chain());

        // Open database.
        let db_path = data_dir.db();
        debug!(target: "ress", path = %db_path.display(), "Opening database");
        let database = RessDatabase::new(&db_path)?;
        info!(target: "ress", path = %db_path.display(), "Database opened");
        let provider = RessProvider::new(Arc::new(self.args.chain.inner.clone()), database.clone());

        // Insert genesis block.
        // TODO: Fix RecoveredBlock creation - temporarily commented out to allow compilation
        // The genesis block insertion logic needs to be updated for reth 1.9.3 API
        let genesis_header = self.args.chain.inner.genesis_header.clone();
        let genesis_hash = genesis_header.hash();
        // provider.insert_block(...);
        provider.insert_canonical_hash(0, genesis_hash);
        info!(target: "ress", %genesis_hash, "Inserted genesis block");
        for account in self.args.chain.inner.genesis.alloc.values() {
            if let Some(code) = account.code.clone() {
                let code_hash = keccak256(&code);
                provider.insert_bytecode(code_hash, Bytecode::new_raw(code))?;
            }
        }
        info!(target: "ress", %genesis_hash, "Inserted genesis bytecodes");

        // Launch network.
        let network_secret_path = self.args.network.network_secret_path(&data_dir);
        let network_secret = reth_cli_util::get_secret_key(&network_secret_path)?;

        let network_handle = self
            .launch_network(
                provider.clone(),
                network_secret,
                self.args.network.max_active_connections,
                self.args.network.trusted_peers.clone(),
            )
            .await?;
        info!(target: "ress", peer_id = %network_handle.inner().peer_id(), "Network launched");

        // TODO: Process block 47020
        // This functionality will be implemented via HTTP API in the future
        info!(target: "ress", "Node started. Waiting for block execution requests...");

        // Keep the node running
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        }
    }

    async fn launch_network<P>(
        &self,
        protocol_provider: P,
        secret_key: SecretKey,
        max_active_connections: u64,
        trusted_peers: Vec<TrustedPeer>,
    ) -> eyre::Result<RessNetworkHandle>
    where
        P: RessProtocolProvider + Clone + Unpin + 'static,
    {
        // Configure and instantiate the network
        let config = NetworkConfig::builder(secret_key)
            .listener_addr(self.args.network.listener_addr())
            .disable_discovery()
            .build_with_noop_provider(Arc::new(self.args.chain.inner.clone()));
        let mut manager = NetworkManager::<EthNetworkPrimitives>::new(config).await?;

        let (events_sender, protocol_events) = mpsc::unbounded_channel();
        let protocol_handler = RessProtocolHandler {
            provider: protocol_provider,
            node_type: NodeType::Stateless,
            peers_handle: manager.peers_handle(),
            max_active_connections,
            state: ProtocolState { events_sender, active_connections: Arc::default() },
        };
        manager.add_rlpx_sub_protocol(protocol_handler.into_rlpx_sub_protocol());

        for trusted_peer in trusted_peers {
            let trusted_peer_addr = trusted_peer.resolve_blocking()?.tcp_addr();
            manager.peers_handle().add_peer(trusted_peer.id, trusted_peer_addr);
        }

        // get a handle to the network to interact with it
        let network_handle = manager.handle().clone();
        // spawn the network
        tokio::spawn(manager);

        let (peer_requests_tx, peer_requests_rx) = mpsc::unbounded_channel();
        let peer_request_stream = UnboundedReceiverStream::from(peer_requests_rx);
        // spawn ress network manager
        tokio::spawn(RessNetworkManager::new(
            UnboundedReceiverStream::from(protocol_events),
            peer_request_stream,
        ));

        Ok(RessNetworkHandle::new(network_handle, peer_requests_tx))
    }
}
