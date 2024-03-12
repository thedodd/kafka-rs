//! Kafka client implementation.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use kafka_protocol::{
    indexmap::IndexMap,
    messages::{
        fetch_request::{FetchPartition, FetchTopic},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        produce_request::PartitionProduceData,
        FetchRequest, ListOffsetsRequest, ProduceRequest, ResponseKind,
    },
    protocol::StrBytes,
    records::{Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType},
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::clitask::{ClientTask, ClusterMeta, Msg};
use crate::error::ClientError;
use crate::{broker::BrokerResponse, error::ClientResult};

/*
- TODO: build a TopicBatcher on top of a TopicProducer which works like a sink,
  batches based on configured max size, will handle retries, and the other good stuff.
*/

/// The default timeout used for requests (10s).
pub const DEFAULT_TIMEOUT: i32 = 10 * 1000;

/// Headers of a message.
pub type MessageHeaders = IndexMap<StrBytes, Option<Bytes>>;

/// A Kafka client.
///
/// This client is `Send + Sync + Clone`, and cloning this client to share it among application
/// tasks is encouraged.
///
/// This client spawns a task which manages the full lifecycle of interaction with a Kafka cluster,
/// include initial broker connections based on seed list, cluster metadata discovery, connections
/// to new brokers, establish API versions of brokers, handling reconnects, and anything else
/// related to maintain connections to a Kafka cluster.
#[derive(Clone)]
pub struct Client {
    /// The channel used for communicating with the client task.
    _tx: mpsc::Sender<Msg>,
    /// Discovered metadata on a Kafka cluster along with broker connections.
    ///
    /// NOTE WELL: this value should never be updated outside of the `ClientTask`.
    cluster: ClusterMeta,
    /// The shutdown signal for this client, which will be triggered once all client handles are dropped.
    _shutdown: Arc<DropGuard>,
}

impl Client {
    /// Construct a new instance.
    pub fn new(seed_list: Vec<String>) -> Self {
        let (tx, rx) = mpsc::channel(1_000);
        let shutdown = CancellationToken::new();
        let task = ClientTask::new(seed_list, rx, shutdown.clone());
        let topics = task.cluster.clone();
        tokio::spawn(task.run());
        Self {
            _tx: tx,
            cluster: topics,
            _shutdown: Arc::new(shutdown.drop_guard()),
        }
    }

    /// List topic partition offsets.
    pub async fn list_offsets(&self, topic: StrBytes, ptn: i32, pos: ListOffsetsPosition) -> ClientResult<i64> {
        let mut cluster = self.cluster.load();
        if !*cluster.bootstrap.borrow() {
            let mut sig = cluster.bootstrap.clone();
            let _ = sig.wait_for(|val| *val).await; // Ensure the cluster metadata is bootstrapped.
            cluster = self.cluster.load();
        }

        // Get the broker responsible for the target topic/partition.
        let topic_ptns = cluster.topics.get(&topic).ok_or(ClientError::UnknownTopic(topic.to_string()))?;
        let broker = topic_ptns.get(&ptn).ok_or(ClientError::UnknownPartition(topic.to_string(), ptn))?;

        // Build request.
        let uid = uuid::Uuid::new_v4();
        let mut req = ListOffsetsRequest::default();
        // req.isolation_level = 0; // TODO: update this.
        let mut req_topic = ListOffsetsTopic::default();
        req_topic.name = topic.clone().into();
        let mut req_ptn = ListOffsetsPartition::default();
        req_ptn.partition_index = ptn;
        req_ptn.timestamp = match pos {
            ListOffsetsPosition::Earliest => -2,
            ListOffsetsPosition::Latest => -1,
            ListOffsetsPosition::Timestamp(val) => val,
        };
        req_topic.partitions.push(req_ptn);
        req.topics.push(req_topic);

        // Send request.
        let (tx, rx) = oneshot::channel();
        broker.conn.list_offsets(uid, req, tx).await;
        let res = rx.await;

        // Unpack response & handle errors.
        // TODO: check for error codes in response.
        let offset = res
            .map_err(|_| ClientError::Other("response channel dropped by broker, which should never happen".into()))?
            .result
            .map_err(ClientError::BrokerError)
            .and_then(|(_, res)| {
                if let ResponseKind::ListOffsetsResponse(res) = res {
                    Ok(res)
                } else {
                    Err(ClientError::MalformedResponse)
                }
            })
            .and_then(|res| {
                res.topics
                    .iter()
                    .find(|topic_res| topic_res.name.0 == topic)
                    .and_then(|topic_res| topic_res.partitions.iter().find(|ptn_res| ptn_res.partition_index == ptn).map(|ptn_res| ptn_res.offset))
                    .ok_or(ClientError::MalformedResponse)
            })?;

        Ok(offset)
    }

    /// Fetch a batch of records from the target topic partition.
    pub async fn fetch(&self, topic: StrBytes, ptn: i32, start: i64) -> ClientResult<Option<Vec<Record>>> {
        let mut cluster = self.cluster.load();
        if !*cluster.bootstrap.borrow() {
            let mut sig = cluster.bootstrap.clone();
            let _ = sig.wait_for(|val| *val).await; // Ensure the cluster metadata is bootstrapped.
            cluster = self.cluster.load();
        }

        // Get the broker responsible for the target topic/partition.
        let topic_ptns = cluster.topics.get(&topic).ok_or(ClientError::UnknownTopic(topic.to_string()))?;
        let broker = topic_ptns.get(&ptn).ok_or(ClientError::UnknownPartition(topic.to_string(), ptn))?;

        // Build request.
        let uid = uuid::Uuid::new_v4();
        let mut req = FetchRequest::default();
        req.max_bytes = 1024i32.pow(2);
        req.max_wait_ms = 10_000;
        // req.isolation_level = 0; // TODO: update this.
        let mut req_topic = FetchTopic::default();
        req_topic.topic = topic.clone().into();
        let mut req_ptn = FetchPartition::default();
        req_ptn.partition = ptn;
        req_ptn.partition_max_bytes = 1024i32.pow(2);
        req_ptn.fetch_offset = start;
        req_topic.partitions.push(req_ptn);
        req.topics.push(req_topic);
        tracing::debug!("about to send request: {:?}", req);

        // Send request.
        let (tx, rx) = oneshot::channel();
        broker.conn.fetch(uid, req, tx).await;
        let res = rx.await;

        // Unpack response & handle errors.
        // TODO: check for error codes in response.
        let batch_opt = res
            .map_err(|_| ClientError::Other("response channel dropped by broker, which should never happen".into()))?
            .result
            .map_err(ClientError::BrokerError)
            .and_then(|(_, res)| {
                tracing::debug!("res: {:?}", res);
                if let ResponseKind::FetchResponse(res) = res {
                    Ok(res)
                } else {
                    Err(ClientError::MalformedResponse)
                }
            })
            .and_then(|res| {
                res.responses
                    .iter()
                    .find(|topic_res| topic_res.topic.0 == topic)
                    .and_then(|topic_res| topic_res.partitions.iter().find(|ptn_res| ptn_res.partition_index == ptn).map(|ptn_res| ptn_res.records.clone()))
                    .ok_or(ClientError::MalformedResponse)
            })?;

        // If some data was returned, then decode the batch.
        let Some(mut batch) = batch_opt else { return Ok(None) };
        let records = RecordBatchDecoder::decode(&mut batch).map_err(|_| ClientError::MalformedResponse)?;

        Ok(Some(records))
    }

    /// Build a producer for a topic.
    pub fn topic_producer(&self, topic: &str, acks: Acks, timeout_ms: Option<i32>, compression: Option<Compression>) -> TopicProducer {
        let (tx, rx) = mpsc::unbounded_channel();
        let compression = compression.unwrap_or(Compression::None);
        let encode_opts = RecordEncodeOptions { version: 2, compression };
        TopicProducer {
            _client: self.clone(),
            tx,
            rx,
            cluster: self.cluster.clone(),
            topic: StrBytes::from_string(topic.into()),
            acks,
            timeout_ms: timeout_ms.unwrap_or(DEFAULT_TIMEOUT),
            encode_opts,
            buf: BytesMut::with_capacity(1024 * 1024),
            batch_buf: Vec::with_capacity(1024),
            last_ptn: -1,
        }
    }
}

/// A message to be encoded as a Kafka record within a record batch.
pub struct Message {
    /// An optional key for the record.
    pub key: Option<Bytes>,
    /// An optional value as the body of the record.
    pub value: Option<Bytes>,
    /// Optional headers to be included in the record.
    pub headers: MessageHeaders,
}

impl Message {
    /// Construct a new record.
    pub fn new(key: Option<Bytes>, value: Option<Bytes>, headers: MessageHeaders) -> Self {
        Self { key, value, headers }
    }
}

/// Write acknowledgements required for a request.
#[derive(Clone, Copy)]
#[repr(i16)]
pub enum Acks {
    /// Leader and replicas.
    All = -1,
    /// None.
    None = 0,
    /// Leader only.
    Leader = 1,
}

/// A producer for a specific topic.
pub struct TopicProducer {
    /// The client handle from which this producer was created.
    _client: Client,
    /// The channel used by this producer.
    tx: mpsc::UnboundedSender<BrokerResponse>,
    /// The channel used by this producer.
    rx: mpsc::UnboundedReceiver<BrokerResponse>,
    /// Discovered metadata on a Kafka cluster along with broker connections.
    ///
    /// NOTE WELL: this value should never be updated outside of the `ClientTask`.
    cluster: ClusterMeta,
    /// The topic being produced to.
    pub topic: StrBytes,
    /// Acks level to use for produce requests.
    acks: Acks,
    /// Timeout for produce requests.
    timeout_ms: i32,
    /// Record batch encoding config.
    encode_opts: RecordEncodeOptions,

    /// A buffer used for encoding batches.
    buf: BytesMut,
    /// A buffer to accumulating records to be sent to a broker.
    batch_buf: Vec<Record>,
    /// The last partition used for round-robin or uniform sticky batch assignment.
    last_ptn: i32,
}

impl TopicProducer {
    /// Produce a batch of records to the specified topic.
    pub async fn produce(&mut self, messages: &[Message]) -> ClientResult<(i64, i64)> {
        // TODO: allow for producer to specify partition instead of using sticky rotating partitions.

        // Check for topic metadata.
        if messages.is_empty() {
            return Err(ClientError::ProducerMessagesEmpty);
        }
        self.batch_buf.clear(); // Ensure buf is clear.
        let mut cluster = self.cluster.load();
        if !*cluster.bootstrap.borrow() {
            let mut sig = cluster.bootstrap.clone();
            let _ = sig.wait_for(|val| *val).await; // Ensure the cluster metadata is bootstrapped.
            cluster = self.cluster.load();
        }
        let Some(topic_ptns) = cluster.topics.get(&self.topic) else {
            return Err(ClientError::UnknownTopic(self.topic.to_string()));
        };

        // Target the next partition of this topic for this batch.
        let Some((sticky_ptn, sticky_broker)) = topic_ptns
            .range((self.last_ptn + 1)..)
            .next()
            .or_else(|| topic_ptns.range(..).next())
            .map(|(key, val)| (*key, val.clone()))
        else {
            return Err(ClientError::NoPartitionsAvailable(self.topic.to_string()));
        };
        self.last_ptn = sticky_ptn;

        // Transform the given messages into their record form.
        let timestamp = chrono::Utc::now().timestamp();
        for msg in messages.iter() {
            self.batch_buf.push(Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: 0,
                producer_epoch: 0,
                timestamp,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                sequence: 0,
                key: msg.key.clone(),
                value: msg.value.clone(),
                headers: msg.headers.clone(),
            });
        }

        // Encode the records into a request.
        // TODO: until https://github.com/tychedelia/kafka-protocol-rs/issues/55 is g2g, we overestimate a bit.
        let size = self.batch_buf.iter().fold(0usize, |mut acc, record| {
            acc += 21; // Max size of the varint encoded values in a v2 record.
            if let Some(key) = record.key.as_ref() {
                acc += key.len();
            }
            if let Some(val) = record.value.as_ref() {
                acc += val.len();
            }
            for (k, v) in record.headers.iter() {
                acc += 4 + k.len() + 4 + v.as_ref().map(|v| v.len()).unwrap_or(0);
            }
            acc
        });
        self.buf.reserve(size);
        let res = RecordBatchEncoder::encode(&mut self.buf, self.batch_buf.iter(), &self.encode_opts).map_err(|err| ClientError::EncodingError(format!("{:?}", err)));
        self.batch_buf.clear();
        res?;

        // Create the request object for the broker.
        let mut req = ProduceRequest::default();
        req.acks = self.acks as i16;
        req.timeout_ms = self.timeout_ms;
        let topic = req.topic_data.entry(self.topic.clone().into()).or_default();
        let mut ptn_data = PartitionProduceData::default();
        ptn_data.index = sticky_ptn;
        ptn_data.records = Some(self.buf.split().freeze());
        topic.partition_data.push(ptn_data);

        // Send off request & await response.
        let uid = uuid::Uuid::new_v4();
        sticky_broker.conn.produce(uid, req, self.tx.clone()).await;
        let res = loop {
            let Some(res) = self.rx.recv().await else {
                unreachable!("both ends of channel are heald, receiving None should not be possible")
            };
            if res.id == uid {
                break res;
            }
        };

        // Handle response.
        // TODO: check for error codes in response.
        res.result
            .map_err(ClientError::BrokerError)
            .and_then(|res| {
                // Unpack the expected response type.
                if let ResponseKind::ProduceResponse(inner) = res.1 {
                    Ok(inner)
                } else {
                    tracing::error!("expected broker to return a ProduceResponse, got: {:?}", res.1);
                    Err(ClientError::MalformedResponse)
                }
            })
            .and_then(|res| {
                // Unpack the base offset & calculate the final offset.
                res.responses
                    .iter()
                    .find(|topic| topic.0 .0 == self.topic)
                    .and_then(|val| {
                        val.1.partition_responses.first().map(|val| {
                            debug_assert!(!messages.is_empty(), "messages len should always be validated at start of function");
                            let last_offset = val.base_offset + (messages.len() - 1) as i64;
                            (val.base_offset, last_offset)
                        })
                    })
                    .ok_or(ClientError::MalformedResponse)
            })
    }
}

/// The position in a log to fetch an offset for.
pub enum ListOffsetsPosition {
    /// Fetch the offset of the beginning of the partition's log.
    Earliest,
    /// Fetch the next offset after the last offset of the partition's log.
    Latest,
    /// Fetch the offset for the corresponding timestamp.
    Timestamp(i64),
}
