
use crate::consensus::ConsensusWrapper;
use crate::download::{DownloadData, DownloadOutcome, EngineDownloader};
use crate::task::{BlockBuffer, Task, TaskState, TaskResult, TaskError, TaskId, TaskRequest, calculate_state_root};
use alloy_primitives::{keccak256, map::B256Map, B256};
use std::task::{Context, Poll};
use ress_evm::BlockExecutor;
use ress_primitives::witness::ExecutionWitness;
use ress_provider::RessProvider;
use reth_chainspec::ChainSpec;
use reth_consensus::FullConsensus;
use reth_primitives::{Bytecode, EthPrimitives};
use alloy_primitives::BlockNumber;
use reth_primitives::{Block, RecoveredBlock};
use reth_trie::{HashedPostState, KeccakKeyHasher};
use reth_trie_sparse::{provider::DefaultTrieNodeProviderFactory, SparseStateTrie};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::oneshot;
use tracing::*;

const BLOCK_BUFFER_SIZE: u32 = 256;

pub struct TaskManager {
    provider: RessProvider,
    downloader: EngineDownloader,
    consensus: ConsensusWrapper,
    block_buffer: BlockBuffer<Block>,
    tasks: HashMap<TaskId, Task>,
    next_task_id: AtomicU64,
    block_to_tasks: HashMap<B256, HashSet<TaskId>>,
    parent_hash_to_tasks: HashMap<B256, HashSet<TaskId>>,
}

impl TaskManager {
    pub fn new(
        provider: RessProvider,
        network: ress_network::RessNetworkHandle,
        consensus: ConsensusWrapper,
    ) -> Self {
        // For downloader, we need to pass the consensus to it
        // EngineDownloader might need a specific type, let's check what it needs
        // For now, we'll create a temporary EthBeaconConsensus for the downloader
        // TODO: Update EngineDownloader to accept ConsensusWrapper if needed
        use reth_chainspec::ChainSpec;
        use reth_node_ethereum::consensus::EthBeaconConsensus;
        let chain_spec = provider.chain_spec();
        let eth_consensus = EthBeaconConsensus::new(chain_spec);
        let downloader = EngineDownloader::new(network, eth_consensus);
        Self {
            provider,
            downloader,
            consensus,
            block_buffer: BlockBuffer::new(BLOCK_BUFFER_SIZE),
            tasks: HashMap::new(),
            next_task_id: AtomicU64::new(0),
            block_to_tasks: HashMap::new(),
            parent_hash_to_tasks: HashMap::new(),
        }
    }
    
    pub fn provider(&self) -> &RessProvider {
        &self.provider
    }
    
    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), TaskError>> {
        loop {
            match self.downloader.poll(cx) {
                Poll::Ready(outcome) => {
                    self.on_download_outcome(outcome);
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
    pub fn create_task(
        &mut self,
        request: TaskRequest,
    ) -> oneshot::Receiver<Result<TaskResult, TaskError>> {
        let task_id = self.next_task_id.fetch_add(1, Ordering::SeqCst);
        
        let (tx, rx) = oneshot::channel();
        
        let state = TaskState {
            block_ready: false,
            parent_block_ready: false,
            witness_ready: false,
        };
        
        let task = Task {
            id: task_id,
            block_hash: request.block_hash,
            block_height: request.block_height,
            parent_hash: request.parent_hash,
            result_tx: tx,
            state,
            created_at: Instant::now(),
        };
        
        info!(target: "ress::task_manager", task_id = %task_id, block_height = %request.block_height, %request.block_hash, "Task created");
        
        self.tasks.insert(task_id, task);
        
        self.block_to_tasks
            .entry(request.block_hash)
            .or_insert_with(HashSet::new)
            .insert(task_id);
        
        self.parent_hash_to_tasks
            .entry(request.parent_hash)
            .or_insert_with(HashSet::new)
            .insert(task_id);
        
        self.start_task(task_id);

        rx
    }

    fn start_task(&mut self, task_id: TaskId) {
        let task = self.tasks.get_mut(&task_id).expect("Task should exist");
        let block_hash = task.block_hash;
        let parent_hash = task.parent_hash;

        if let Some(block) = self.provider.recovered_block(&block_hash) {
            self.block_buffer.insert_block(block);
            task.state.block_ready = true;
        } else if !self.block_buffer.blocks.contains_key(&block_hash) {
            self.downloader.download_full_block(block_hash);
        } else {
            task.state.block_ready = true;
        }
        
        if let Some(witness) = self.block_buffer.witness(&block_hash) {
            let code_hashes = witness.bytecode_hashes().clone();
            let missing_code_hashes = self.provider.missing_code_hashes(code_hashes)
                .unwrap_or_default();
            
            if missing_code_hashes.is_empty() {
                task.state.witness_ready = true;
            } else {
                for code_hash in &missing_code_hashes {
                    self.downloader.download_bytecode(*code_hash);
                }
            }
        } else {
            self.downloader.download_witness(block_hash);
        }
        
        if parent_hash == B256::ZERO {
            task.state.parent_block_ready = true;
        } else if let Some(parent) = self.provider.recovered_block(&parent_hash) {
            self.block_buffer.insert_block(parent);
            task.state.parent_block_ready = true;
        } else if !self.block_buffer.blocks.contains_key(&parent_hash) {
            self.downloader.download_full_block(parent_hash);
        } else {
            task.state.parent_block_ready = true;
        }
        
        self.check_and_execute_tasks(vec![task_id]);
    }
    pub fn on_download_outcome(&mut self, outcome: DownloadOutcome) {
        let mut affected_tasks = Vec::new();
        
        match outcome.data {
            DownloadData::FullBlock(block) => {
                affected_tasks.extend(self.on_block_downloaded(block));
            }
            
            DownloadData::Witness(block_hash, witness) => {
                affected_tasks.extend(self.on_witness_downloaded(block_hash, witness));
            }
            
            DownloadData::Bytecode(code_hash, bytecode) => {
                match self.on_bytecode_downloaded(code_hash, bytecode) {
                    Ok(tasks) => affected_tasks.extend(tasks),
                    Err(e) => error!(target: "ress::task_manager", %code_hash, error = %e, "Failed to process bytecode download"),
                }
            }
            
            DownloadData::FinalizedBlock(_, _) => {}
        }
        
        self.check_and_execute_tasks(affected_tasks);
    }
    fn on_block_downloaded(
        &mut self,
        block: reth_primitives::SealedBlock,
    ) -> Vec<TaskId> {
        let block_num_hash = block.num_hash();
        let recovered = match block.try_recover() {
            Ok(block) => block,
            Err(_error) => {
                debug!(target: "ress::task_manager", ?block_num_hash, "Error recovering downloaded block");
                return Vec::new();
            }
        };
        let block_hash = block_num_hash.hash;
        
        self.block_buffer.insert_block(recovered.clone());
        self.provider.insert_block(recovered, None);

        let mut affected_tasks = Vec::new();

        if let Some(task_ids) = self.block_to_tasks.get(&block_hash) {
            for &task_id in task_ids {
                if let Some(task) = self.tasks.get_mut(&task_id) {
                    task.state.block_ready = true;
                    affected_tasks.push(task_id);
                }
            }
        }
        if let Some(task_ids) = self.parent_hash_to_tasks.get(&block_hash) {
            for &task_id in task_ids {
                if let Some(task) = self.tasks.get_mut(&task_id) {
                    task.state.parent_block_ready = true;
                    affected_tasks.push(task_id);
                }
            }
        }
        
        affected_tasks
    }
    
    fn on_witness_downloaded(
        &mut self,
        block_hash: B256,
        witness: ExecutionWitness,
    ) -> Vec<TaskId> {
        let code_hashes = witness.bytecode_hashes();
        let missing_code_hashes = self.provider.missing_code_hashes(code_hashes.clone())
            .unwrap_or_default();
        
        self.block_buffer.insert_witness(
            block_hash,
            witness.clone(),
            missing_code_hashes.clone(),
        );

        let witness_bytes: Vec<alloy_primitives::Bytes> = witness.state_witness().clone();
        self.provider.insert_witness(block_hash, witness_bytes);
        
        let mut affected_tasks = Vec::new();
        if missing_code_hashes.is_empty() {
            if let Some(task_ids) = self.block_to_tasks.get(&block_hash) {
                for &task_id in task_ids {
                    if let Some(task) = self.tasks.get_mut(&task_id) {
                        task.state.witness_ready = true;
                        affected_tasks.push(task_id);
                    }
                }
            }
        } else {
            for code_hash in &missing_code_hashes {
                self.downloader.download_bytecode(*code_hash);
            }
        }
        
        affected_tasks
    }
    
    fn on_bytecode_downloaded(&mut self, code_hash: B256, bytecode: Bytecode) -> Result<Vec<TaskId>, TaskError> {
        self.provider.insert_bytecode(code_hash, bytecode)
            .map_err(|e| TaskError::ProviderError(reth_errors::ProviderError::Database(e)))?;
        
        let ready_block_hashes = self.block_buffer.on_bytecode_received(code_hash);
        
        let mut affected_tasks = Vec::new();
        for block_hash in ready_block_hashes {
            if self.block_buffer.witness(&block_hash).is_some() {
                if let Some(task_ids) = self.block_to_tasks.get(&block_hash) {
                    for &task_id in task_ids {
                        if let Some(task) = self.tasks.get_mut(&task_id) {
                            task.state.witness_ready = true;
                            affected_tasks.push(task_id);
                        }
                    }
                }
            }
        }
        Ok(affected_tasks)
    }
    
    fn check_and_execute_tasks(&mut self, task_ids: Vec<TaskId>) {
        for task_id in task_ids {
            if let Some(task) = self.tasks.get(&task_id) {
                if self.is_task_ready(task) {
                    if let Err(e) = self.execute_task(task_id) {
                        error!(target: "ress::task_manager", task_id = %task_id, error = %e, "Failed to execute task");
                    }
                }
            }
        }
    }
    
    fn is_task_ready(&self, task: &Task) -> bool {
        self.is_task_ready_from_state(&task.state)
    }
    
    fn is_task_ready_from_state(&self, state: &TaskState) -> bool {
        if !state.block_ready {
            return false;
        }
        if !state.parent_block_ready {
            return false;
        }
        if !state.witness_ready {
            return false;
        }
        true
    }
    
    fn execute_task(&mut self, task_id: TaskId) -> Result<(), TaskError> {
        let (block_hash, parent_hash, block_height) = {
            let task = self.tasks.get(&task_id)
                .ok_or_else(|| TaskError::ExecutionError(format!("Task {} not found", task_id)))?;
            (task.block_hash, task.parent_hash, task.block_height)
        };
        
        if let Some(task_ids) = self.block_to_tasks.get_mut(&block_hash) {
            task_ids.remove(&task_id);
            if task_ids.is_empty() {
                self.block_to_tasks.remove(&block_hash);
            }
        }
        if let Some(task_ids) = self.parent_hash_to_tasks.get_mut(&parent_hash) {
            task_ids.remove(&task_id);
            if task_ids.is_empty() {
                self.parent_hash_to_tasks.remove(&parent_hash);
            }
        }
        
        let task = self.tasks.remove(&task_id)
            .ok_or_else(|| TaskError::ExecutionError(format!("Task {} not found", task_id)))?;
        
        let block = self.block_buffer.blocks
            .get(&block_hash)
            .ok_or_else(|| TaskError::BlockNotFound(block_hash))?;
        
        // Verify that the block's hash matches the requested hash
        let computed_block_hash = block.hash();
        if computed_block_hash != block_hash {
            return Err(TaskError::ExecutionError(format!(
                "Block hash mismatch: expected {}, got {}",
                block_hash,
                computed_block_hash
            )));
        }
        
        // Verify that the block's hash equals the sealed header hash (Ethereum rule)
        // In Ethereum, block hash is the keccak256 hash of the block header
        let sealed_header = block.clone_sealed_header();
        let header_hash = sealed_header.hash();
        if computed_block_hash != header_hash {
            return Err(TaskError::ExecutionError(format!(
                "Block hash does not match header hash (Ethereum rule violation): block hash {}, header hash {}",
                computed_block_hash,
                header_hash
            )));
        }
        
        // Verify that the block's number matches the requested block height
        if block.number != block_height {
            return Err(TaskError::ExecutionError(format!(
                "Block number mismatch: expected {}, got {}",
                block_height,
                block.number
            )));
        }
        
        let witness = self.block_buffer.witness(&block_hash)
            .ok_or_else(|| TaskError::WitnessNotFound(block_hash))?;
        
        // Verify that the block's parent_hash matches the requested parent_hash
        // (if parent_hash is not zero, which indicates genesis)
        if parent_hash != B256::ZERO && block.parent_hash != parent_hash {
            return Err(TaskError::ExecutionError(format!(
                "Parent hash mismatch: expected {}, got {}",
                parent_hash,
                block.parent_hash
            )));
        }
        
        let parent_hash = block.parent_hash;
        let parent_block = self.provider.recovered_block(&parent_hash)
            .or_else(|| self.block_buffer.blocks.get(&parent_hash).cloned())
            .ok_or_else(|| TaskError::ParentNotFound(parent_hash))?;
        
        let mut bytecodes = Vec::new();
        for code_hash in witness.bytecode_hashes() {
            match self.provider.get_bytecode(*code_hash) {
                Ok(Some(bytecode)) => {
                    bytecodes.push((*code_hash, bytecode));
                }
                Ok(None) => {
                    return Err(TaskError::BytecodeNotFound(*code_hash));
                }
                Err(e) => {
                    return Err(TaskError::ProviderError(reth_errors::ProviderError::Database(e)));
                }
            }
        }
        
        let calculated_state_root = self.verify_block(block, &parent_block, witness, parent_block.state_root)?;
        
        if calculated_state_root != block.state_root {
            return Err(TaskError::StateRootMismatch {
                block: block.hash(),
                got: calculated_state_root,
                expected: block.state_root,
            });
        }
        
        let result = TaskResult {
            block: block.clone(),
            parent_block,
            witness: witness.clone(),
            bytecodes,
        };
        
        let _ = task.result_tx.send(Ok(result));
        
        info!(target: "ress::task_manager", task_id = %task_id, %block_hash, "Task executed successfully");
        Ok(())
    }
    
    fn verify_block(
        &self,
        block: &RecoveredBlock<Block>,
        parent_block: &RecoveredBlock<Block>,
        witness: &ExecutionWitness,
        parent_state_root: B256,
    ) -> Result<B256, TaskError> {
        // Validate timestamp for Kasplex chains
        // Kasplex allows block.timestamp == parent.timestamp (unlike standard Ethereum)
        // Skip validation for genesis block (block.number == 0)
        if block.number > 0 {
            match &self.consensus {
                ConsensusWrapper::Kasplex(_) => {
                    // Kasplex allows timestamp == parent.timestamp
                    if block.timestamp < parent_block.timestamp {
                        return Err(TaskError::ExecutionError(format!(
                            "Block timestamp {} is less than parent timestamp {}",
                            block.timestamp, parent_block.timestamp
                        )));
                    }
                }
                ConsensusWrapper::Ethereum(_) => {
                    // Standard Ethereum requires timestamp > parent.timestamp
                    if block.timestamp <= parent_block.timestamp {
                        return Err(TaskError::ExecutionError(format!(
                            "Block timestamp {} must be greater than parent timestamp {}",
                            block.timestamp, parent_block.timestamp
                        )));
                    }
                }
            }
        }
        
        let mut trie = SparseStateTrie::default();
        let mut state_witness = B256Map::default();
        for encoded in witness.state_witness() {
            state_witness.insert(keccak256(encoded), encoded.clone());
        }
        trie.reveal_witness(parent_state_root, &state_witness)
            .map_err(|e| TaskError::ExecutionError(format!("Failed to reveal witness: {}", e)))?;
        
        let block_executor = BlockExecutor::new(
            self.provider.clone(),
            block.parent_num_hash(),
            &trie,
        );
        let output = block_executor.execute(block)
            .map_err(|e| TaskError::ExecutionError(format!("Block execution failed: {}", e)))?;
        
        <ConsensusWrapper as FullConsensus<EthPrimitives>>::validate_block_post_execution(
            &self.consensus,
            block,
            &output.result
        ).map_err(TaskError::ConsensusError)?;
        
        use rayon::iter::IntoParallelRefIterator;
        let hashed_state = HashedPostState::from_bundle_state::<KeccakKeyHasher>(
            output.state.state.par_iter()
        );
        let provider_factory = DefaultTrieNodeProviderFactory;
        let state_root = calculate_state_root(&mut trie, hashed_state, &provider_factory)
            .map_err(|e| TaskError::ExecutionError(format!("Failed to calculate state root: {}", e)))?;
        
        Ok(state_root)
    }
}

