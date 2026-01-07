//! HTTP API for executing blocks

use alloy_primitives::B256;
use ress_engine::engine::ExecuteEngine;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::*;

/// API state containing the ExecuteEngine
#[derive(Clone)]
pub struct ApiState {
    pub(crate) execute_engine: Arc<Mutex<ExecuteEngine>>,
}

impl ApiState {
    pub fn new(execute_engine: ExecuteEngine) -> Self {
        Self {
            execute_engine: Arc::new(Mutex::new(execute_engine)),
        }
    }
}

/// Request to execute a block
#[derive(Debug, Deserialize)]
pub struct ExecuteBlockRequest {
    /// Block hash to execute
    pub block_hash: String,
}

/// Response from block execution
#[derive(Debug, Serialize)]
pub struct ExecuteBlockResponse {
    /// Whether execution was successful
    pub success: bool,
    /// Block hash that was executed
    pub block_hash: String,
    /// State root after execution
    pub state_root: Option<String>,
    /// Error message if execution failed
    pub error: Option<String>,
}

/// Execute a block by hash
pub async fn execute_block(
    state: axum::extract::State<ApiState>,
    request: axum::Json<ExecuteBlockRequest>,
) -> axum::Json<ExecuteBlockResponse> {
    info!(target: "ress::api", block_hash = %request.block_hash, "Received block execution request");

    // Parse block hash
    let block_hash = match request.block_hash.parse::<B256>() {
        Ok(hash) => hash,
        Err(e) => {
            error!(target: "ress::api", error = %e, "Invalid block hash format");
            return axum::Json(ExecuteBlockResponse {
                success: false,
                block_hash: request.block_hash.clone(),
                state_root: None,
                error: Some(format!("Invalid block hash format: {}", e)),
            });
        }
    };

    // Call execute_block on the engine
    let rx = {
        let mut engine = state.execute_engine.lock().await;
        engine.execute_block(block_hash)
    };

    // Wait for execution result
    match rx.await {
        Ok(Ok(result)) => {
            info!(
                target: "ress::api",
                block_hash = %block_hash,
                state_root = %result.state_root,
                "Block execution completed successfully"
            );
            axum::Json(ExecuteBlockResponse {
                success: true,
                block_hash: format!("{:?}", block_hash),
                state_root: Some(format!("{:?}", result.state_root)),
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
                error: Some(format!("Channel error: {}", e)),
            })
        }
    }
}

