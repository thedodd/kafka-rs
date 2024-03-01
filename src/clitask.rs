//! The client task which drives all IO and async interaction with the Kafka cluster.
//!
//! Everything owned by the task should be opaque to the frontend `Client` used by consumers
//! of this library.

use std::collections::BTreeMap;
use std::time::Duration;

use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
use kafka_protocol::messages::{BrokerId, MetadataResponse, TopicName};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::broker::{Broker, BrokerMeta};

/// A message from a client.
pub enum ClientMsg {}

/// An async task which owns all interaction with a Kafka cluster.
///
/// Client tasks have a corresponding frontend `Client` which is used to drive interaction with
/// the target Kafka cluster.
pub struct ClientTask {
    /// The starting list of brokers to connect to for metadata.
    ///
    /// The seed list is only used to establish initial connections to the Kafka cluster.
    /// After the first set of metadata responses are returned, the brokers described in
    /// the returned metadata will be used for all following client connections.
    seed_list: Vec<String>,
    /// The channel used for receiving client interaction requests.
    rx: mpsc::Receiver<ClientMsg>,
    /// Client shutdown signal.
    shutdown: CancellationToken,

    /// All topics which have been discovered from cluster metadata queries.
    topics: BTreeMap<TopicName, BTreeMap<i32, BrokerId>>,
    /// Connections to discovered brokers.
    brokers: BTreeMap<BrokerId, Broker>,
    /// All brokers which have been discovered from cluster metadata queries.
    brokers_meta: BTreeMap<BrokerId, MetadataResponseBroker>,
    /// All topic/partitions per broker.
    broker_ptns: BTreeMap<BrokerId, Vec<(TopicName, i32)>>,
}

impl ClientTask {
    /// Construct a new instance.
    pub(crate) fn new(seed_list: Vec<String>, rx: mpsc::Receiver<ClientMsg>, shutdown: CancellationToken) -> Self {
        Self {
            seed_list,
            rx,
            shutdown,
            topics: BTreeMap::default(),
            brokers: BTreeMap::default(),
            brokers_meta: BTreeMap::default(),
            broker_ptns: BTreeMap::default(),
        }
    }

    /// Construct a new instance.
    pub(crate) async fn run(mut self) {
        self.bootstrap_cluster().await;

        tracing::debug!("kafka client initialized");
        loop {
            tokio::select! {
                Some(msg) = self.rx.recv() => self.handle_client_msg(msg).await,
                _ = self.shutdown.cancelled() => break,
            }
        }

        tracing::debug!("kafka client has shutdown");
    }

    async fn handle_client_msg(&mut self, msg: ClientMsg) {
        todo!()
    }

    /// Bootstrap cluster connections, API versions, and metadata, all from the starting seed list of brokers.
    async fn bootstrap_cluster(&mut self) {
        tracing::debug!("bootstrapping kafka cluster connections");
        loop {
            let seeds = self.seed_list.clone();
            for host in seeds {
                if self.shutdown.is_cancelled() {
                    return;
                };

                // We have a connection object which will gather API versions info on its own (straightaway).
                // Now, just fetch cluster metadata info.
                let conn = Broker::new(BrokerMeta::Host(host));
                let meta = match conn.get_metadata().await {
                    Ok(meta) => meta,
                    Err(err) => {
                        tracing::warn!(error = ?err, "error fetching metadata from broker");
                        continue;
                    }
                };

                // Establish connections to any newly discovered brokers.
                self.update_cluster_metadata(meta);
                return; // We only need 1 initial payload of metadata, so return here.
            }

            // Failed to bootstrap cluster, so sleep and then try again.
            if self.shutdown.is_cancelled() {
                return;
            };
            sleep(Duration::from_secs(2)).await;
        }
    }

    /// Update the cluster metadata in response to a metadata fetch on a broker.
    fn update_cluster_metadata(&mut self, meta: MetadataResponse) {
        // Update broker metadata.
        self.brokers_meta.clear();
        for (id, broker) in meta.brokers {
            self.brokers_meta.insert(id, broker);
        }

        // Update topic partitions along with their broker leader.
        self.topics.clear();
        self.broker_ptns.clear();
        for (id, topic) in meta.topics {
            for ptn in topic.partitions {
                if ptn.error_code != 0 {
                    continue;
                };
                let topic_ptns = self.topics.entry(id.clone()).or_default();
                topic_ptns.insert(ptn.partition_index, ptn.leader_id);
                let broker_ptns = self.broker_ptns.entry(ptn.leader_id).or_default();
                broker_ptns.push((id.clone(), ptn.partition_index));
            }
        }

        // Establish connections to any new brokers.
        for (id, meta) in self.brokers_meta.iter() {
            if !self.brokers.contains_key(id) {
                let broker = Broker::new(meta.clone());
                self.brokers.insert(*id, broker);
            }
        }
        self.brokers.retain(|id, _| self.brokers_meta.contains_key(id)); // Remove old brokers.

        tracing::debug!(?self.brokers_meta, ?self.topics, ?self.broker_ptns, "cluster metadata updated");
    }
}
