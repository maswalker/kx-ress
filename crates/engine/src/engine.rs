use crate::consensus::ConsensusWrapper;
use crate::download::{DownloadData, DownloadOutcome, EngineDownloader};
use crate::task::{TaskManager, TaskResult, TaskError, TaskRequest};
use ress_network::RessNetworkHandle;
use ress_primitives::execution::{ExecutionResult, ExecuteEngineError};
use ress_provider::RessProvider;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::oneshot;
use tracing::*;

#[allow(missing_debug_implementations)]
pub struct Engine {
    task_manager: TaskManager,
}

impl Engine {
    pub fn new(
        provider: RessProvider,
        network: RessNetworkHandle,
        consensus: ConsensusWrapper,
    ) -> Self {
        let task_manager = TaskManager::new(
            provider,
            network,
            consensus,
        );
        
        Self {
            task_manager,
        }
    }

    pub fn execute_block(
        &mut self,
        request: TaskRequest,
    ) -> oneshot::Receiver<Result<ExecutionResult, ExecuteEngineError>> {

        let task_rx = self.task_manager.create_task(request);

        let (tx, rx) = oneshot::channel();

        tokio::spawn(async move {
            match task_rx.await {
                Ok(Ok(task_result)) => {
                    let execution_result = ExecutionResult {
                        block: task_result.block,
                        parent_block: task_result.parent_block,
                        witness: task_result.witness,
                        bytecodes: task_result.bytecodes,
                    };
                    let _ = tx.send(Ok(execution_result));
                }
                Ok(Err(e)) => {
                    let _ = tx.send(Err(ExecuteEngineError::TaskError(format!("{}", e))));
                }
                Err(_) => {
                    let _ = tx.send(Err(ExecuteEngineError::TaskError(
                        "Task channel closed".to_string()
                    )));
                }
            }
        });

        rx
    }
}

impl Future for Engine {
    type Output = Result<(), ExecuteEngineError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.task_manager.poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(ExecuteEngineError::TaskError(format!("{}", e)))),
            Poll::Pending => Poll::Pending,
        }
    }
}
