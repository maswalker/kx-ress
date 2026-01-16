
use crate::consensus::ConsensusWrapper;
use crate::download::{DownloadData, DownloadOutcome, EngineDownloader};
use crate::task::{BlockBuffer, Task, TaskState, TaskResult, TaskError, TaskId, TaskRequest, BlockState, calculate_state_root};
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
        // Process any ready download outcomes
        loop {
            match self.downloader.poll(cx) {
                Poll::Ready(outcome) => {
                    self.on_download_outcome(outcome);
                }
                Poll::Pending => {
                    break;
                }
            }
        }

        // Check if any tasks are ready to execute (even if no downloads completed)
        // This handles cases where data is already available locally
        let ready_tasks: Vec<TaskId> = self.tasks
            .iter()
            .filter_map(|(id, task)| {
                if self.is_task_ready(task) {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();

        if !ready_tasks.is_empty() {
            self.check_and_execute_tasks(ready_tasks);
        }

        Poll::Pending
    }

    pub fn create_task(
        &mut self,
        request: TaskRequest,
    ) -> oneshot::Receiver<Result<TaskResult, TaskError>> {
        // Validate request
        if request.block_hashes.is_empty() {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Err(TaskError::ExecutionError(
                "TaskRequest must have at least one block".to_string()
            )));
            return rx;
        }
        if request.block_hashes.len() != request.download_witnesses.len() {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Err(TaskError::ExecutionError(
                format!("block_hashes length ({}) must match download_witnesses length ({})",
                    request.block_hashes.len(), request.download_witnesses.len())
            )));
            return rx;
        }

        let task_id = self.next_task_id.fetch_add(1, Ordering::SeqCst);

        let (tx, rx) = oneshot::channel();

        // Initialize block states
        let mut block_states = HashMap::new();
        for block_hash in &request.block_hashes {
            block_states.insert(*block_hash, BlockState {
                block_ready: false,
                witness_ready: false,
            });
        }

        let state = TaskState {
            parent_block_ready: false,
            block_states,
        };

        let task = Task {
            id: task_id,
            parent_hash: request.parent_hash,
            block_hashes: request.block_hashes.clone(),
            start_block_height: request.start_block_height,
            download_witnesses: request.download_witnesses,
            result_tx: tx,
            state,
            created_at: Instant::now(),
        };

        self.tasks.insert(task_id, task);

        // Index all block_hashes to this task_id
        for block_hash in &request.block_hashes {
            self.block_to_tasks
                .entry(*block_hash)
                .or_insert_with(HashSet::new)
                .insert(task_id);
        }

        // Index parent_hash to this task_id
        self.parent_hash_to_tasks
            .entry(request.parent_hash)
            .or_insert_with(HashSet::new)
            .insert(task_id);

        self.start_task(task_id);

        rx
    }

    fn start_task(&mut self, task_id: TaskId) {
        let (parent_hash, block_hashes, download_witnesses) = {
            let task = self.tasks.get(&task_id).expect("Task should exist");
            (task.parent_hash, task.block_hashes.clone(), task.download_witnesses.clone())
        };

        let task = self.tasks.get_mut(&task_id).expect("Task should exist");

        // Start downloading blocks
        for (i, block_hash) in block_hashes.iter().enumerate() {
            let block_state = task.state.block_states.get_mut(block_hash)
                .expect("Block state should exist");
            
            if let Some(block) = self.provider.recovered_block(block_hash) {
                self.block_buffer.insert_block(block);
                block_state.block_ready = true;
            } else if !self.block_buffer.blocks.contains_key(block_hash) {
                self.downloader.download_full_block(*block_hash);
            } else {
                block_state.block_ready = true;
            }

            // Handle witness download for this block
            let download_witness = download_witnesses[i];
            if !download_witness {
                // Skip witness/bytecode download
                block_state.witness_ready = true;
            } else {
                // Normal witness download logic
                if let Some(witness) = self.block_buffer.witness(block_hash) {
                    let code_hashes = witness.bytecode_hashes().clone();
                    let missing_code_hashes = self.provider.missing_code_hashes(code_hashes)
                        .unwrap_or_default();

                    if missing_code_hashes.is_empty() {
                        block_state.witness_ready = true;
                    } else {
                        for code_hash in &missing_code_hashes {
                            self.downloader.download_bytecode(*code_hash);
                        }
                    }
                } else if let Some(witness_bytes) = self.provider.witness(block_hash) {
                    // Witness exists in provider but not in block_buffer, load it
                    if !witness_bytes.is_empty() {
                        // Calculate RLP size as sum of all bytes (approximation)
                        let rlp_size_bytes = witness_bytes.iter().map(|b| b.len()).sum();
                        let execution_witness = ExecutionWitness::new(witness_bytes, rlp_size_bytes);

                        let code_hashes = execution_witness.bytecode_hashes().clone();
                        let missing_code_hashes = self.provider.missing_code_hashes(code_hashes)
                            .unwrap_or_default();

                        self.block_buffer.insert_witness(*block_hash, execution_witness, missing_code_hashes.clone());

                        if missing_code_hashes.is_empty() {
                            block_state.witness_ready = true;
                        } else {
                            for code_hash in &missing_code_hashes {
                                self.downloader.download_bytecode(*code_hash);
                            }
                        }
                    } else {
                        self.downloader.download_witness(*block_hash);
                    }
                } else {
                    self.downloader.download_witness(*block_hash);
                }
            }
        }

        // Handle parent block download
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
            DownloadData::WitnessError(block_hash, error) => {
                error!(target: "ress::task_manager", %block_hash, %error, "TaskManager: Witness download failed");
                affected_tasks.extend(self.on_witness_download_error(block_hash, error));
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
                    if let Some(block_state) = task.state.block_states.get_mut(&block_hash) {
                        block_state.block_ready = true;
                        affected_tasks.push(task_id);
                    }
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

    fn on_witness_download_error(
        &mut self,
        block_hash: B256,
        error: String,
    ) -> Vec<TaskId> {
        error!(target: "ress::task_manager", %block_hash, %error, "TaskManager: on_witness_download_error called");
        let mut affected_tasks = Vec::new();
        if let Some(task_ids) = self.block_to_tasks.get(&block_hash).cloned() {
            for task_id in task_ids {
                if let Some(task) = self.tasks.get_mut(&task_id) {
                    // Fail the task with WitnessNotFound error
                    let task_error = TaskError::WitnessNotFound(block_hash);
                    let result_tx = std::mem::replace(&mut task.result_tx, {
                        // Create a dummy sender that will be dropped
                        let (_tx, _rx) = tokio::sync::oneshot::channel();
                        _tx
                    });
                    if let Err(send_error) = result_tx.send(Err(task_error)) {
                        warn!(target: "ress::task_manager", task_id = %task_id, send_error = ?send_error, "Failed to send error to task result channel");
                    }
                    affected_tasks.push(task_id);
                }
            }
            // Remove tasks from block_to_tasks mapping
            self.block_to_tasks.remove(&block_hash);
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
                        if let Some(block_state) = task.state.block_states.get_mut(&block_hash) {
                            block_state.witness_ready = true;
                            affected_tasks.push(task_id);
                        }
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
                            if let Some(block_state) = task.state.block_states.get_mut(&block_hash) {
                                block_state.witness_ready = true;
                                affected_tasks.push(task_id);
                            }
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
                let ready = self.is_task_ready(task);
                let blocks_status: Vec<String> = task.block_hashes.iter()
                    .map(|hash| {
                        let state = task.state.block_states.get(hash);
                        format!("{}:b={},w={}", 
                            hash.to_string().chars().take(8).collect::<String>(),
                            state.map(|s| s.block_ready).unwrap_or(false),
                            state.map(|s| s.witness_ready).unwrap_or(false))
                    })
                    .collect();
                debug!(target: "ress::task_manager",
                    task_id = %task_id,
                    block_count = task.block_hashes.len(),
                    parent_ready = task.state.parent_block_ready,
                    blocks_status = ?blocks_status,
                    is_ready = ready,
                    "TaskManager: Checking task readiness");

                if ready {
                    if let Err(e) = self.execute_task(task_id) {
                        error!(target: "ress::task_manager", task_id = %task_id, error = %e, "Failed to execute task");
                    }
                }
            }
        }
    }

    fn is_task_ready(&self, task: &Task) -> bool {
        if !task.state.parent_block_ready {
            return false;
        }
        // Check that all blocks are ready (both block_ready and witness_ready)
        for block_hash in &task.block_hashes {
            let block_state = match task.state.block_states.get(block_hash) {
                Some(state) => state,
                None => return false,
            };
            if !block_state.block_ready || !block_state.witness_ready {
                return false;
            }
        }
        true
    }

    fn execute_task(&mut self, task_id: TaskId) -> Result<(), TaskError> {
        let (block_hashes, parent_hash, start_block_height, download_witnesses) = {
            let task = self.tasks.get(&task_id)
                .ok_or_else(|| TaskError::ExecutionError(format!("Task {} not found", task_id)))?;
            (task.block_hashes.clone(), task.parent_hash, task.start_block_height, task.download_witnesses.clone())
        };

        // Remove task from all index mappings
        for block_hash in &block_hashes {
            if let Some(task_ids) = self.block_to_tasks.get_mut(block_hash) {
                task_ids.remove(&task_id);
                if task_ids.is_empty() {
                    self.block_to_tasks.remove(block_hash);
                }
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
        
        // Get parent block
        let mut current_parent = self.provider.recovered_block(&parent_hash)
            .or_else(|| self.block_buffer.blocks.get(&parent_hash).cloned())
            .ok_or_else(|| TaskError::ParentNotFound(parent_hash))?;

        let mut executed_blocks: Vec<RecoveredBlock<Block>> = Vec::new();
        let mut witnesses = Vec::new();
        let mut all_bytecodes_map = HashMap::new(); // Use HashMap to deduplicate by code_hash

        // Execute blocks sequentially
        for (i, block_hash) in block_hashes.iter().enumerate() {
            let block_height = start_block_height + i as BlockNumber;
            let download_witness = download_witnesses[i];

            let block = self.block_buffer.blocks
                .get(block_hash)
                .ok_or_else(|| TaskError::BlockNotFound(*block_hash))?;

            // Verify that the block's hash matches the requested hash
            let computed_block_hash = block.hash();
            if computed_block_hash != *block_hash {
                return Err(TaskError::ExecutionError(format!(
                    "Block hash mismatch: expected {}, got {}",
                    block_hash,
                    computed_block_hash
                )));
            }

            // Verify that the block's hash equals the sealed header hash (Ethereum rule)
            let sealed_header = block.clone_sealed_header();
            let header_hash = sealed_header.hash();
            if computed_block_hash != header_hash {
                return Err(TaskError::ExecutionError(format!(
                    "Block hash does not match header hash (Ethereum rule violation): block hash {}, header hash {}",
                    computed_block_hash,
                    header_hash
                )));
            }

            // Verify that the block's number matches the expected block height
            if block.number != block_height {
                return Err(TaskError::ExecutionError(format!(
                    "Block number mismatch: expected {}, got {}",
                    block_height,
                    block.number
                )));
            }

            // Verify that the block's parent_hash matches the current parent
            let expected_parent_hash = if i == 0 {
                parent_hash
            } else {
                executed_blocks[i - 1].hash()
            };
            if expected_parent_hash != B256::ZERO && block.parent_hash != expected_parent_hash {
                return Err(TaskError::ExecutionError(format!(
                    "Parent hash mismatch: expected {}, got {}",
                    expected_parent_hash,
                    block.parent_hash
                )));
            }

            // Check if this is an empty block and if we should skip witness verification
            let is_empty = block.body().transactions.is_empty() && block.body().ommers.is_empty();
            let should_skip_witness = is_empty && !download_witness;

            if should_skip_witness {
                // For empty blocks, only perform basic validations:
                // 1. block.hash() correctness (already verified above)
                // 2. parent_hash correctness (already verified above)
                // 3. block_number continuity (already verified above)
                // 4. state_root == parent_state_root (empty block characteristic)
                if block.state_root != current_parent.state_root {
                    return Err(TaskError::StateRootMismatch {
                        block: block.hash(),
                        got: current_parent.state_root,
                        expected: block.state_root,
                    });
                }

                // Create a minimal witness for empty blocks
                let empty_witness = ExecutionWitness::new(vec![], 0);
                witnesses.push(empty_witness);
                // No bytecodes for empty blocks
            } else {
                // Normal execution path with witness verification
                let witness = self.block_buffer.witness(block_hash)
                    .ok_or_else(|| TaskError::WitnessNotFound(*block_hash))?;

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

                let calculated_state_root = self.verify_block(block, &current_parent, witness, current_parent.state_root)?;
                if calculated_state_root != block.state_root {
                    return Err(TaskError::StateRootMismatch {
                        block: block.hash(),
                        got: calculated_state_root,
                        expected: block.state_root,
                    });
                }
                witnesses.push(witness.clone());
                // Merge bytecodes (deduplicate by code_hash)
                for (code_hash, bytecode) in bytecodes {
                    all_bytecodes_map.insert(code_hash, bytecode);
                }
            }

            executed_blocks.push(block.clone());
            // Update current_parent for next iteration
            current_parent = block.clone();
        }

        // Convert HashMap to Vec for bytecodes
        let all_bytecodes: Vec<(B256, Bytecode)> = all_bytecodes_map.into_iter().collect();

        // All blocks executed successfully, send result
        let result = TaskResult {
            blocks: executed_blocks,
            parent_block: self.provider.recovered_block(&parent_hash)
                .or_else(|| self.block_buffer.blocks.get(&parent_hash).cloned())
                .ok_or_else(|| TaskError::ParentNotFound(parent_hash))?,
            witnesses,
            bytecodes: all_bytecodes,
        };
        let _ = task.result_tx.send(Ok(result));
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

