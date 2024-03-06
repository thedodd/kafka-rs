//! The client task which drives all IO and async interaction with the Kafka cluster.
//!
//! Everything owned by the task should be opaque to the frontend `Client` used by consumers
//! of this library.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
use kafka_protocol::messages::{BrokerId, MetadataResponse, ResponseKind};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::{mpsc, watch};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::broker::{Broker, BrokerConnInfo, BrokerPtr, BrokerResponse};

/// Discovered metadata on a Kafka cluster along with broker connections.
///
/// NOTE WELL: this value should never be updated outside of the `ClientTask`.
pub(crate) type ClusterMeta = Arc<ArcSwap<Cluster>>;

/// Discovered metadata on a Kafka cluster along with broker connections.
#[derive(Clone, Debug)]
pub(crate) struct Cluster {
    /// A signal indicating if the cluster's metadata has been fetched at least once.
    pub(crate) bootstrap: watch::Receiver<bool>,
    /// Broker connections and metadata of all discovered cluster brokers.
    pub(crate) brokers: BTreeMap<BrokerId, BrokerMetaPtr>,
    /// All topics which have been discovered from cluster metadata queries.
    pub(crate) topics: BTreeMap<StrBytes, BTreeMap<i32, BrokerMetaPtr>>,
}

impl Cluster {
    /// Construct a new instance.
    fn new(bootstrap: watch::Receiver<bool>) -> Self {
        Self {
            bootstrap,
            brokers: Default::default(),
            topics: Default::default(),
        }
    }
}

/// Metadata and connection to a Kafka broker.
pub(crate) type BrokerMetaPtr = Arc<BrokerMeta>;

/// Metadata and connection to a Kafka broker.
pub(crate) struct BrokerMeta {
    /// The ID of a broker.
    pub(crate) id: BrokerId,
    /// The connection to the broker.
    pub(crate) conn: BrokerPtr,
    /// The cluster metadata of the broker.
    pub(crate) meta: MetadataResponseBroker,
}

impl std::fmt::Debug for BrokerMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerMeta").field("id", &self.id).field("meta", &self.meta).finish()
    }
}

/// An async task which owns all interaction with a Kafka cluster.
///
/// Client tasks have a corresponding frontend `Client` which is used to drive interaction with
/// the target Kafka cluster.
pub(crate) struct ClientTask {
    /// The starting list of brokers to connect to for metadata.
    ///
    /// The seed list is only used to establish initial connections to the Kafka cluster.
    /// After the first set of metadata responses are returned, the brokers described in
    /// the returned metadata will be used for all following client connections.
    seed_list: Vec<String>,
    /// The channel used for receiving client interaction requests.
    rx: mpsc::Receiver<Msg>,
    /// The channel used for receiving responses from brokers.
    resp_tx: mpsc::UnboundedSender<BrokerResponse>,
    /// The channel used for receiving responses from brokers.
    resp_rx: mpsc::UnboundedReceiver<BrokerResponse>,
    /// Cluster metadata bootstrap signal.
    bootstrap_tx: watch::Sender<bool>,
    /// Client shutdown signal.
    shutdown: CancellationToken,

    /// Discovered metadata on a Kafka cluster along with broker connections.
    ///
    /// NOTE WELL: this value should never be updated outside of the `ClientTask`.
    pub(crate) cluster: ClusterMeta,
}

impl ClientTask {
    /// Construct a new instance.
    pub(crate) fn new(seed_list: Vec<String>, rx: mpsc::Receiver<Msg>, shutdown: CancellationToken) -> Self {
        let (bootstrap_tx, bootstrap_rx) = watch::channel(false);
        let (resp_tx, resp_rx) = mpsc::unbounded_channel();
        Self {
            seed_list,
            rx,
            resp_tx,
            resp_rx,
            cluster: Arc::new(ArcSwap::new(Arc::new(Cluster::new(bootstrap_rx.clone())))),
            bootstrap_tx,
            shutdown,
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

    async fn handle_client_msg(&mut self, _msg: Msg) {
        // TODO: probably will only need this for fetching updated cluster metadata and such.
        // TODO: setup an interval for fetching updated metadata from cluster.
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
                let conn = Broker::new(BrokerConnInfo::Host(host));
                let uid = uuid::Uuid::new_v4();
                conn.get_metadata(uid, self.resp_tx.clone()).await;

                // Await response from broker.
                let res = loop {
                    let Some(res) = self.resp_rx.recv().await else {
                        unreachable!("both ends of channel are heald, receiving None should not be possible")
                    };
                    if res.id == uid {
                        break res;
                    }
                };

                let (_, res) = match res.result {
                    Ok(res) => res,
                    Err(err) => {
                        tracing::error!(error = ?err, "error fetching metadata for cluster");
                        continue;
                    }
                };
                let ResponseKind::MetadataResponse(meta) = res else {
                    tracing::error!("malformed response received from cluster, expected a metadata response, got {:?}", res);
                    continue;
                };

                // Establish connections to any newly discovered brokers.
                self.update_cluster_metadata(meta);
                let _ = self.bootstrap_tx.send(true);
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
    #[allow(clippy::mutable_key_type)]
    fn update_cluster_metadata(&mut self, meta: MetadataResponse) {
        let mut cluster_ptr = self.cluster.load_full();
        let cluster = Arc::make_mut(&mut cluster_ptr);

        // Establish connections to any new brokers.
        cluster.brokers.retain(|id, _| meta.brokers.contains_key(id)); // Remove brokers which no longer exist.
        for (id, meta) in meta.brokers {
            let broker_opt = cluster.brokers.get(&id);
            let needs_update = broker_opt.map(|_meta| _meta.meta != meta).unwrap_or(true);
            if needs_update {
                let conn = Broker::new(meta.clone());
                cluster.brokers.insert(id, Arc::new(BrokerMeta { id, conn, meta }));
            }
        }

        // Update topic partitions along with their broker leader. We have to clear the map and re-build
        // in order to ensure that we are not holding onto old broker ptrs (same ID, different connection/metadata).
        cluster.topics.clear();
        for (id, topic) in meta.topics {
            for ptn in topic.partitions {
                if ptn.error_code != 0 {
                    continue;
                };
                let ptns = cluster.topics.entry(id.0.clone()).or_default();
                if let Some(broker) = cluster.brokers.get(&ptn.leader_id).cloned() {
                    ptns.insert(ptn.partition_index, broker);
                }
            }
        }
        tracing::debug!(?cluster, "cluster metadata updated");
        self.cluster.store(cluster_ptr);
    }
}

/// A message from a client.
pub(crate) enum Msg {}
