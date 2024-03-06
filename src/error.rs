//! Crate error types.

use kafka_protocol::messages::{BrokerId, RequestKind};

/// Client errors from interacting with a Kafka cluster.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Error while interacting with a broker.
    #[error("error while interacting with a broker: {0:?}")]
    BrokerError(BrokerRequestError),
    /// The client is disconnected.
    #[error("the client is disconnected")]
    Disconnected,
    /// Error while encoding a batch of records.
    #[error("error while encoding a batch of records: {0}")]
    EncodingError(String),
    /// The specified topic has no available partitions.
    #[error("the specified topic has no available partitions: {0}")]
    NoPartitionsAvailable(String),
    /// A broker has disappeared from the cluster during client operations.
    ///
    /// Typically, application code should just retry the request in the face of this error.
    #[error("broker has disappeared from the cluster during client operations: {0:?}")]
    UnknownBroker(BrokerId),
    /// The specified topic is unknown to the cluster.
    #[error("the specified topic is unknown to the cluster: {0}")]
    UnknownTopic(String),
}

/// Broker connection level error.
#[derive(Debug, thiserror::Error)]
#[error("broker connection error: {kind:?}")]
pub struct BrokerRequestError {
    /// The original request payload.
    pub(crate) payload: RequestKind,
    /// The kind of error which has taken place.
    pub(crate) kind: BrokerErrorKind,
}

/// Broker connection level error kind.
#[derive(Debug, thiserror::Error)]
pub enum BrokerErrorKind {
    /// The connection to the broker has terminated.
    #[error("the client is disconnected")]
    Disconnected,
    /// The broker returned a malformed response.
    #[error("the broker returned a malformed response")]
    MalformedBrokerResponse,
}
