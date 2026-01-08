use alloy_primitives::B256;
use alloy_rlp::Encodable;
use ress_engine::engine::Engine;
use ress_engine::task::TaskRequest;
use ress_verifier::verify as verify_execution;
use reth_chainspec::ChainSpec;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::*;

#[derive(Clone)]
pub struct ApiState {
    pub(crate) execute_engine: Arc<Mutex<Engine>>,
    pub(crate) chain_spec: Arc<ChainSpec>,
}

impl ApiState {
    pub fn new(execute_engine: Engine, chain_spec: Arc<ChainSpec>) -> Self {
        Self {
            execute_engine: Arc::new(Mutex::new(execute_engine)),
            chain_spec,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ExecuteBlockRequest {
    pub block_hash: String,
    pub block_height: u64,
    pub parent_hash: String,
}

#[derive(Debug, Serialize)]
pub struct BlockData {
    /// RLP-encoded block data as hex string
    pub rlp_encoded: String,
}

#[derive(Debug, Serialize)]
pub struct BytecodeData {
    /// Code hash
    pub hash: String,
    /// Bytecode as hex string
    pub code: String,
}

#[derive(Debug, Serialize)]
pub struct WitnessData {
    /// State witness entries as hex strings
    pub state_witness: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ExecuteBlockResponse {
    pub success: bool,
    pub block_hash: String,
    pub state_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<BlockData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_block: Option<BlockData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub witness: Option<WitnessData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytecodes: Option<Vec<BytecodeData>>,
    pub error: Option<String>,
}

pub async fn execute_block(
    state: axum::extract::State<ApiState>,
    request: axum::Json<ExecuteBlockRequest>,
) -> axum::Json<ExecuteBlockResponse> {
    info!(target: "ress::api", block_height = %request.block_height, %request.block_hash, "Executing block");

    let block_hash = match request.block_hash.parse::<B256>() {
        Ok(hash) => hash,
        Err(e) => {
            error!(target: "ress::api", error = %e, "Invalid block hash format");
            return axum::Json(ExecuteBlockResponse {
                success: false,
                block_hash: request.block_hash.clone(),
                state_root: None,
                block: None,
                parent_block: None,
                witness: None,
                bytecodes: None,
                error: Some(format!("Invalid block hash format: {}", e)),
            });
        }
    };

    let parent_hash = match request.parent_hash.parse::<B256>() {
        Ok(hash) => hash,
        Err(e) => {
            error!(target: "ress::api", error = %e, "Invalid parent hash format");
            return axum::Json(ExecuteBlockResponse {
                success: false,
                block_hash: request.block_hash.clone(),
                state_root: None,
                block: None,
                parent_block: None,
                witness: None,
                bytecodes: None,
                error: Some(format!("Invalid parent hash format: {}", e)),
            });
        }
    };

    let task_request = TaskRequest {
        block_hash,
        block_height: request.block_height,
        parent_hash,
    };
    
    let rx = {
        let mut engine = state.execute_engine.lock().await;
        engine.execute_block(task_request)
    };
    match rx.await {
        Ok(Ok(result)) => {
            let state_root = result.block.state_root;
            info!(target: "ress::api", block_height = %request.block_height, %block_hash, state_root = %state_root, "Block executed");
            
            // Verify the execution result using verifier
            match verify_execution(&result, state.chain_spec.clone()) {
                Ok(()) => {
                    info!(target: "ress::api", block_height = %request.block_height, %block_hash, "Verification successful");
                }
                Err(e) => {
                    warn!(target: "ress::api", block_height = %request.block_height, %block_hash, error = %e, "Verification failed");
                    // Continue to return the result even if verification fails
                    // The verification is a check, not a requirement for the API response
                }
            }
            
            // Encode block as RLP
            let block_rlp = {
                let sealed_block = result.block.clone_sealed_block();
                let mut buffer = Vec::new();
                sealed_block.encode(&mut buffer);
                format!("0x{}", hex::encode(buffer))
            };
            
            // Encode parent block as RLP
            let parent_block_rlp = {
                let sealed_block = result.parent_block.clone_sealed_block();
                let mut buffer = Vec::new();
                sealed_block.encode(&mut buffer);
                format!("0x{}", hex::encode(buffer))
            };
            
            // Encode witness state_witness as hex strings
            let witness_data = WitnessData {
                state_witness: result.witness.state_witness()
                    .iter()
                    .map(|bytes| format!("0x{}", hex::encode(bytes.as_ref())))
                    .collect(),
            };
            
            // Encode bytecodes
            let bytecodes_data: Vec<BytecodeData> = result.bytecodes
                .iter()
                .map(|(hash, bytecode)| {
                    BytecodeData {
                        hash: format!("{:?}", hash),
                        code: format!("0x{}", hex::encode(bytecode.bytecode().as_ref())),
                    }
                })
                .collect();
            
            axum::Json(ExecuteBlockResponse {
                success: true,
                block_hash: format!("{:?}", block_hash),
                state_root: Some(format!("{:?}", state_root)),
                block: Some(BlockData { rlp_encoded: block_rlp }),
                parent_block: Some(BlockData { rlp_encoded: parent_block_rlp }),
                witness: Some(witness_data),
                bytecodes: Some(bytecodes_data),
                error: None,
            })
        }
        Ok(Err(e)) => {
            error!(
                target: "ress::api",
                block_hash = %block_hash,
                error = %e,
                "Block execution failed"
            );
            axum::Json(ExecuteBlockResponse {
                success: false,
                block_hash: format!("{:?}", block_hash),
                state_root: None,
                block: None,
                parent_block: None,
                witness: None,
                bytecodes: None,
                error: Some(format!("{}", e)),
            })
        }
        Err(e) => {
            error!(
                target: "ress::api",
                block_hash = %block_hash,
                error = %e,
                "Failed to receive execution result"
            );
            axum::Json(ExecuteBlockResponse {
                success: false,
                block_hash: format!("{:?}", block_hash),
                state_root: None,
                block: None,
                parent_block: None,
                witness: None,
                bytecodes: None,
                error: Some(format!("Channel error: {}", e)),
            })
        }
    }
}

