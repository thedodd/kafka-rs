//! Kafka client implementation.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::clitask::{ClientMsg, ClientTask};

/// A Kafka client.
///
/// This client is `Send + Sync + Clone`, and cloning the client to share it among application
/// tasks is encouraged.
#[derive(Clone)]
pub struct Client {
    /// The channel used for communicating with the client task.
    tx: mpsc::Sender<ClientMsg>,
    /// The shutdown signal for this client, which will be triggered once all client handles are dropped.
    _shutdown: Arc<DropGuard>,
}

impl Client {
    /// Construct a new instance.
    pub fn new(seed_list: Vec<String>) -> Self {
        let (tx, rx) = mpsc::channel(1_000);
        let shutdown = CancellationToken::new();
        let task = ClientTask::new(seed_list, rx, shutdown.clone());
        tokio::spawn(task.run());
        Self {
            tx,
            _shutdown: Arc::new(shutdown.drop_guard()),
        }
    }
}
