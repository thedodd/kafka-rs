//! Internal cluster interfaces.

use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::fetch_response::PartitionData;
use kafka_protocol::messages::{FetchRequest, MetadataResponse, ResponseKind};
use kafka_protocol::ResponseError;
use tokio::sync::oneshot;

use crate::client::unpack_broker_response;
use crate::clitask::Msg;
use crate::error::{ClientError, ClientResult};
use crate::{Client, StrBytes};

/// A client wrapper for cluster internal interactions.
#[derive(Clone)]
pub struct InternalClient {
    /// A handle to the underlying client.
    pub client: Client,
}

impl InternalClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Update this client's metadata based on the given payload.
    ///
    /// This is typically not what a normal Kafka client will want to use. For 99% of use cases, just use the
    /// clients default `MetadataPolicy::Automatic` which will bootstrap the client's metadata, and will poll
    /// the cluster for metadata changes periodically.
    pub async fn update_metadata(&self, payload: MetadataResponse) {
        let _ = self.client.tx.send(Msg::UpdateClusterMetadata(payload)).await;
    }

    /// Fetch a payload of data as a replica from the target topic partition leader.
    pub async fn fetch_as_replica(&self, topic: StrBytes, ptn: i32, replica_id: i32, replica_log_start: i64, fetch_from: i64) -> ClientResult<PartitionData> {
        // Get the broker responsible for the target topic/partition.
        let cluster = self.client.get_cluster_metadata_cache().await?;
        let topic_ptns = cluster.topics.get(&topic).ok_or(ClientError::UnknownTopic(topic.to_string()))?;
        let ptn_meta = topic_ptns.get(&ptn).ok_or(ClientError::UnknownPartition(topic.to_string(), ptn))?;
        let broker = ptn_meta.leader.clone().ok_or(ClientError::NoPartitionLeader(topic.to_string(), ptn))?;

        let mut req = FetchRequest::default();
        // Replicate all data, even open TXs.
        req.isolation_level = 0;
        // Max of 16MiB payloads.
        req.max_bytes = 1024i32.pow(2) * 16;
        // Long-poll 60s.
        req.max_wait_ms = 60_000;
        // As long as we have some data, replicate immediately.
        req.min_bytes = 1;
        req.replica_id = replica_id.into();

        // Construct topic & partition info for request.
        let mut topic_req = FetchTopic::default();
        let mut ptn_req = FetchPartition::default();
        topic_req.topic = topic.clone().into();
        ptn_req.fetch_offset = fetch_from;
        ptn_req.partition = ptn;
        ptn_req.partition_max_bytes = 1024i32.pow(2) * 16;
        ptn_req.current_leader_epoch = ptn_meta.metadata.leader_epoch;
        ptn_req.log_start_offset = replica_log_start;
        topic_req.partitions = vec![ptn_req];
        req.topics = vec![topic_req];

        // Submit the request.
        let (uid, (tx, rx)) = (uuid::Uuid::new_v4(), oneshot::channel());
        broker.conn.fetch(uid, req, tx).await;

        // Handle response.
        unpack_broker_response(rx)
            .await
            .and_then(|(_, res)| {
                if let ResponseKind::FetchResponse(res) = res {
                    Ok(res)
                } else {
                    Err(ClientError::MalformedResponse)
                }
            })

            // Ensure we dont' have a top-level error, then unpack the target topic/partition response data.
            .and_then(|res| {
                // Handle top-level response error.
                if res.error_code != 0 {
                    return Err(ClientError::ResponseError(res.error_code, ResponseError::try_from_code(res.error_code), None));
                }

                // Get the response specifically for the topic we requested for replication.
                res.responses
                    .into_iter()
                    .find(|topic_res| topic_res.topic.0 == topic)
                    .and_then(|val| val.partitions.into_iter().find(|ptn_res| ptn_res.partition_index == ptn))
                    .ok_or(ClientError::MalformedResponse)
            })

            // Ensure the partition data response did not contain an error.
            .and_then(|ptn_data| {
                if ptn_data.error_code != 0 {
                    return Err(ClientError::ResponseError(ptn_data.error_code, ResponseError::try_from_code(ptn_data.error_code), None));
                }
                Ok(ptn_data)
            })
    }
}
