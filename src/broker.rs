//! Kafka connection interface.

use std::sync::Arc;
use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result};
use futures::prelude::*;
use kafka_protocol::{
    messages::{metadata_response::MetadataResponseBroker, ApiKey, ApiVersionsRequest, MetadataRequest, ProduceRequest, RequestHeader, RequestKind, ResponseHeader, ResponseKind},
    protocol::{Decodable, Message, Request as RequestProto, VersionRange},
};
use tokio::{net::TcpStream, sync::mpsc, time::timeout};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::codec::{self, KafkaReader, KafkaWriter, Request, Response};
use crate::error::{BrokerErrorKind, BrokerRequestError};

/* TODO:
- use timer queue for timeouts of outstanding requests.
*/

/// A result from a broker interaction.
pub(crate) type BrokerResult<T> = Result<T, BrokerRequestError>;
/// The channel type used for getting responses from a broker connection.
pub(crate) type MsgTx = mpsc::UnboundedSender<BrokerResponse>;

/// A response from a broker along with the UUID of the original request.
///
/// In the case of an error, the result error variant will include the original payload, which can
/// be used for retries and the like.
pub(crate) struct BrokerResponse {
    /// The ID of the original request.
    pub(crate) id: uuid::Uuid,
    /// The result from the broker.
    pub(crate) result: BrokerResult<(ResponseHeader, ResponseKind)>,
}

pub(crate) enum Msg {
    GetMetadata(uuid::Uuid, MsgTx),
    Produce(uuid::Uuid, ProduceRequest, MsgTx),
}

/// A handle to a broker connection.
pub(crate) type BrokerPtr = Arc<Broker>;

/// A handle to a broker connection.
pub(crate) struct Broker {
    chan: mpsc::Sender<Msg>,
    _shutdown: DropGuard,
}

impl Broker {
    /// Create a new instance.
    pub(crate) fn new<T: Into<BrokerConnInfo>>(broker: T) -> BrokerPtr {
        let (tx, rx) = mpsc::channel(1_000);
        let shutdown = CancellationToken::new();
        let task = BrokerTask::new(broker.into(), rx, shutdown.clone());
        tokio::spawn(task.run());
        Arc::new(Self {
            chan: tx,
            _shutdown: shutdown.drop_guard(),
        })
    }

    /// Get the cluster's metadata according to this broker (should be uniform across all brokers).
    pub(crate) async fn get_metadata(&self, id: uuid::Uuid, tx: MsgTx) {
        let _ = self.chan.send(Msg::GetMetadata(id, tx)).await; // Unreachable error case.
    }

    /// Produce data to the broker.
    ///
    /// If the request fails, the original request payload is returned.
    pub(crate) async fn produce(&self, id: uuid::Uuid, req: ProduceRequest, tx: MsgTx) {
        let _ = self.chan.send(Msg::Produce(id, req.clone(), tx)).await; // Unreachable error case.
    }
}

/// All possible states in which a broker connection may exist.
enum BrokerState {
    Connecting(BrokerConnecting),
    Versioning(BrokerVersioning),
    Ready(BrokerReady),
    Terminated(BrokerTask),
}

/// The core state of a broker connection.
struct BrokerTask {
    /// Metadata on the target broker.
    broker: BrokerConnInfo,
    /// The channel used for communicating with this connection.
    chan: mpsc::Receiver<Msg>,
    /// A shutdown signal for this broker connection.
    shutdown: CancellationToken,

    /// All supported API versions of this broker.
    api_versions: BTreeMap<i16, (i16, i16)>,
    /// All outstanding requests on this connection, indexed by correlation ID.
    requests: BTreeMap<i32, OutboundRequest>,
    /// The next correlation ID to use, eventually wrapping.
    next_correlation_id: i32,
}

impl BrokerTask {
    /// Create a new instance.
    fn new(broker: BrokerConnInfo, chan: mpsc::Receiver<Msg>, shutdown: CancellationToken) -> Self {
        Self {
            broker,
            chan,
            shutdown,
            api_versions: BTreeMap::default(),
            requests: BTreeMap::default(),
            next_correlation_id: 0,
        }
    }

    async fn run(self) {
        // Core task loop as a state machine.
        let mut next = BrokerState::Connecting(BrokerConnecting { inner: self });
        let state = loop {
            next = match next {
                BrokerState::Connecting(inner) => inner.run().await,
                BrokerState::Versioning(inner) => inner.run().await,
                BrokerState::Ready(inner) => inner.run().await,
                BrokerState::Terminated(inner) => break inner,
            }
        };
        tracing::trace!(?state.broker, "broker connection terminated");
    }

    /// Handle output from the connection framed decoder.
    async fn handle_incoming_frame(&mut self, opt_res: Option<Result<Response, std::io::Error>>) -> Result<()> {
        let resp = opt_res
            .context("broker connection lost, reconnect needed")?
            .context("error handling response from broker, disconnecting")?;
        self.handle_response(resp).await;
        Ok(())
    }

    async fn handle_msg(&mut self, writer: &mut KafkaWriter, msg: Msg) -> Result<()> {
        match msg {
            // Msg::GetApiVersions(tx) => self.handle_get_api_versions(Some(tx)).await,
            Msg::GetMetadata(id, tx) => self.get_metadata(writer, id, Some(tx)).await,
            Msg::Produce(id, req, tx) => self.produce(writer, id, req, Some(tx)).await,
        }
    }

    /// Write the request to the broker, and queue for response.
    async fn write(&mut self, writer: &mut KafkaWriter, cid: i32, req: OutboundRequest) -> Result<()> {
        if let Err(err) = writer.send(&req.request).await.context("error sending request to broker") {
            if let Some(chan) = req.chan.as_ref() {
                let req_err = BrokerRequestError {
                    payload: req.request.kind,
                    kind: BrokerErrorKind::Disconnected,
                };
                let _ = chan.send(BrokerResponse { id: req.id, result: Err(req_err) });
            }
            return Err(err);
        }

        self.requests.insert(cid, req);
        Ok(())
    }

    /// Fetch API versions info from this broker.
    ///
    /// This call should only err if the underlying connection has disconnected.
    async fn get_api_versions(&mut self, writer: &mut KafkaWriter, chan: Option<MsgTx>) -> Result<()> {
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1);

        let (min, max) = self.api_versions.get(&ApiVersionsRequest::KEY).copied().unwrap_or((0, 0));
        let supported_versions = ApiVersionsRequest::VERSIONS.intersect(&VersionRange { min, max });
        tracing::trace!(?supported_versions, correlation_id, "sending api versions request");

        let api_key = ApiKey::ApiVersionsKey;
        let mut header = RequestHeader::default();
        header.request_api_key = api_key as i16;
        header.request_api_version = supported_versions.max;
        header.correlation_id = correlation_id;

        let req = OutboundRequest {
            id: uuid::Uuid::new_v4(),
            request: Request {
                header,
                kind: RequestKind::ApiVersionsRequest(ApiVersionsRequest::default()),
            },
            api_version: supported_versions.max,
            api_key,
            chan,
        };
        self.write(writer, correlation_id, req).await
    }

    async fn get_metadata(&mut self, writer: &mut KafkaWriter, id: uuid::Uuid, chan: Option<MsgTx>) -> Result<()> {
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1);

        let (min, max) = self.api_versions.get(&MetadataRequest::KEY).copied().unwrap_or((0, 0));
        let supported_versions = MetadataRequest::VERSIONS.intersect(&VersionRange { min, max });
        tracing::debug!(?supported_versions, correlation_id, "sending metadata request");

        let api_key = ApiKey::MetadataKey;
        let mut header = RequestHeader::default();
        header.request_api_key = api_key as i16;
        header.request_api_version = supported_versions.max;
        header.correlation_id = correlation_id;

        let req = OutboundRequest {
            id,
            request: Request {
                header,
                kind: RequestKind::MetadataRequest(MetadataRequest::default()),
            },
            api_version: supported_versions.max,
            api_key,
            chan,
        };
        self.write(writer, correlation_id, req).await
    }

    async fn produce(&mut self, writer: &mut KafkaWriter, id: uuid::Uuid, req: ProduceRequest, chan: Option<MsgTx>) -> Result<()> {
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1);

        let (min, max) = self.api_versions.get(&ProduceRequest::KEY).copied().unwrap_or((0, 0));
        let supported_versions = ProduceRequest::VERSIONS.intersect(&VersionRange { min, max });
        tracing::debug!(?supported_versions, correlation_id, "sending produce request");

        let api_key = ApiKey::ProduceKey;
        let mut header = RequestHeader::default();
        header.request_api_key = api_key as i16;
        header.request_api_version = supported_versions.max;
        header.correlation_id = correlation_id;

        let req = OutboundRequest {
            id,
            request: Request {
                header,
                kind: RequestKind::ProduceRequest(req),
            },
            api_version: supported_versions.max,
            api_key,
            chan,
        };
        self.write(writer, correlation_id, req).await
    }

    /// Handle a response from the broker.
    async fn handle_response(&mut self, mut resp: Response) {
        // Decode header.
        tracing::trace!("handling broker response for request {}", resp.correlation_id);
        let Some(pending) = self.requests.remove(&resp.correlation_id) else { return };
        let header_version = pending.api_key.response_header_version(pending.api_version);
        let Ok(response_header) = ResponseHeader::decode(&mut resp.body, header_version) else {
            if let Some(chan) = pending.chan {
                let _ = chan.send(BrokerResponse {
                    id: pending.id,
                    result: Err(BrokerRequestError {
                        payload: pending.request.kind,
                        kind: BrokerErrorKind::MalformedBrokerResponse,
                    }),
                });
            }
            return;
        };

        // Decode body based on API key.
        use kafka_protocol::messages::*;
        let res = match pending.api_key {
            ApiKey::ProduceKey => ProduceResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::ProduceResponse),
            ApiKey::FetchKey => FetchResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::FetchResponse),
            ApiKey::ListOffsetsKey => ListOffsetsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::ListOffsetsResponse),
            ApiKey::MetadataKey => MetadataResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::MetadataResponse),
            ApiKey::LeaderAndIsrKey => LeaderAndIsrResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::LeaderAndIsrResponse),
            ApiKey::StopReplicaKey => StopReplicaResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::StopReplicaResponse),
            ApiKey::UpdateMetadataKey => UpdateMetadataResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::UpdateMetadataResponse),
            ApiKey::ControlledShutdownKey => ControlledShutdownResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::ControlledShutdownResponse),
            ApiKey::OffsetCommitKey => OffsetCommitResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::OffsetCommitResponse),
            ApiKey::OffsetFetchKey => OffsetFetchResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::OffsetFetchResponse),
            ApiKey::FindCoordinatorKey => FindCoordinatorResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::FindCoordinatorResponse),
            ApiKey::JoinGroupKey => JoinGroupResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::JoinGroupResponse),
            ApiKey::HeartbeatKey => HeartbeatResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::HeartbeatResponse),
            ApiKey::LeaveGroupKey => LeaveGroupResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::LeaveGroupResponse),
            ApiKey::SyncGroupKey => SyncGroupResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::SyncGroupResponse),
            ApiKey::DescribeGroupsKey => DescribeGroupsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DescribeGroupsResponse),
            ApiKey::ListGroupsKey => ListGroupsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::ListGroupsResponse),
            ApiKey::SaslHandshakeKey => SaslHandshakeResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::SaslHandshakeResponse),
            ApiKey::ApiVersionsKey => ApiVersionsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::ApiVersionsResponse),
            ApiKey::CreateTopicsKey => CreateTopicsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::CreateTopicsResponse),
            ApiKey::DeleteTopicsKey => DeleteTopicsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DeleteTopicsResponse),
            ApiKey::DeleteRecordsKey => DeleteRecordsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DeleteRecordsResponse),
            ApiKey::InitProducerIdKey => InitProducerIdResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::InitProducerIdResponse),
            ApiKey::OffsetForLeaderEpochKey => OffsetForLeaderEpochResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::OffsetForLeaderEpochResponse),
            ApiKey::AddPartitionsToTxnKey => AddPartitionsToTxnResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::AddPartitionsToTxnResponse),
            ApiKey::AddOffsetsToTxnKey => AddOffsetsToTxnResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::AddOffsetsToTxnResponse),
            ApiKey::EndTxnKey => EndTxnResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::EndTxnResponse),
            ApiKey::WriteTxnMarkersKey => WriteTxnMarkersResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::WriteTxnMarkersResponse),
            ApiKey::TxnOffsetCommitKey => TxnOffsetCommitResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::TxnOffsetCommitResponse),
            ApiKey::DescribeAclsKey => DescribeAclsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DescribeAclsResponse),
            ApiKey::CreateAclsKey => CreateAclsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::CreateAclsResponse),
            ApiKey::DeleteAclsKey => DeleteAclsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DeleteAclsResponse),
            ApiKey::DescribeConfigsKey => DescribeConfigsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DescribeConfigsResponse),
            ApiKey::AlterConfigsKey => AlterConfigsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::AlterConfigsResponse),
            ApiKey::AlterReplicaLogDirsKey => AlterReplicaLogDirsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::AlterReplicaLogDirsResponse),
            ApiKey::DescribeLogDirsKey => DescribeLogDirsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DescribeLogDirsResponse),
            ApiKey::SaslAuthenticateKey => SaslAuthenticateResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::SaslAuthenticateResponse),
            ApiKey::CreatePartitionsKey => CreatePartitionsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::CreatePartitionsResponse),
            ApiKey::CreateDelegationTokenKey => CreateDelegationTokenResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::CreateDelegationTokenResponse),
            ApiKey::RenewDelegationTokenKey => RenewDelegationTokenResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::RenewDelegationTokenResponse),
            ApiKey::ExpireDelegationTokenKey => ExpireDelegationTokenResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::ExpireDelegationTokenResponse),
            ApiKey::DescribeDelegationTokenKey => DescribeDelegationTokenResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DescribeDelegationTokenResponse),
            ApiKey::DeleteGroupsKey => DeleteGroupsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DeleteGroupsResponse),
            ApiKey::ElectLeadersKey => ElectLeadersResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::ElectLeadersResponse),
            ApiKey::IncrementalAlterConfigsKey => IncrementalAlterConfigsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::IncrementalAlterConfigsResponse),
            ApiKey::AlterPartitionReassignmentsKey => AlterPartitionReassignmentsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::AlterPartitionReassignmentsResponse),
            ApiKey::ListPartitionReassignmentsKey => ListPartitionReassignmentsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::ListPartitionReassignmentsResponse),
            ApiKey::OffsetDeleteKey => OffsetDeleteResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::OffsetDeleteResponse),
            ApiKey::DescribeClientQuotasKey => DescribeClientQuotasResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DescribeClientQuotasResponse),
            ApiKey::AlterClientQuotasKey => AlterClientQuotasResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::AlterClientQuotasResponse),
            ApiKey::DescribeUserScramCredentialsKey => DescribeUserScramCredentialsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DescribeUserScramCredentialsResponse),
            ApiKey::AlterUserScramCredentialsKey => AlterUserScramCredentialsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::AlterUserScramCredentialsResponse),
            ApiKey::VoteKey => VoteResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::VoteResponse),
            ApiKey::BeginQuorumEpochKey => BeginQuorumEpochResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::BeginQuorumEpochResponse),
            ApiKey::EndQuorumEpochKey => EndQuorumEpochResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::EndQuorumEpochResponse),
            ApiKey::DescribeQuorumKey => DescribeQuorumResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DescribeQuorumResponse),
            ApiKey::AlterPartitionKey => AlterPartitionResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::AlterPartitionResponse),
            ApiKey::UpdateFeaturesKey => UpdateFeaturesResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::UpdateFeaturesResponse),
            ApiKey::EnvelopeKey => EnvelopeResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::EnvelopeResponse),
            ApiKey::FetchSnapshotKey => FetchSnapshotResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::FetchSnapshotResponse),
            ApiKey::DescribeClusterKey => DescribeClusterResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DescribeClusterResponse),
            ApiKey::DescribeProducersKey => DescribeProducersResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DescribeProducersResponse),
            ApiKey::BrokerRegistrationKey => BrokerRegistrationResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::BrokerRegistrationResponse),
            ApiKey::BrokerHeartbeatKey => BrokerHeartbeatResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::BrokerHeartbeatResponse),
            ApiKey::UnregisterBrokerKey => UnregisterBrokerResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::UnregisterBrokerResponse),
            ApiKey::DescribeTransactionsKey => DescribeTransactionsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::DescribeTransactionsResponse),
            ApiKey::ListTransactionsKey => ListTransactionsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::ListTransactionsResponse),
            ApiKey::AllocateProducerIdsKey => AllocateProducerIdsResponse::decode(&mut resp.body, pending.api_version).map(ResponseKind::AllocateProducerIdsResponse),
        };
        let Ok(response_body) = res else {
            if let Some(chan) = pending.chan {
                let _ = chan.send(BrokerResponse {
                    id: pending.id,
                    result: Err(BrokerRequestError {
                        payload: pending.request.kind,
                        kind: BrokerErrorKind::MalformedBrokerResponse,
                    }),
                });
            }
            return;
        };

        // If this is an API versions response, always update our local cache of version info.
        if let ResponseKind::ApiVersionsResponse(res) = &response_body {
            if res.error_code == 0 {
                self.api_versions.clear();
                for (key, ver) in res.api_keys.iter() {
                    self.api_versions.insert(*key, (ver.min_version, ver.max_version));
                }
                tracing::trace!(?self.api_versions, "updated api versions cache info");
            }
        }

        if let Some(chan) = pending.chan {
            let _ = chan.send(BrokerResponse {
                id: pending.id,
                result: Ok((response_header, response_body)),
            });
        }
    }
}

/// The broker needs to (re-)connect.
struct BrokerConnecting {
    inner: BrokerTask,
}

impl BrokerConnecting {
    #[tracing::instrument(level = "trace", parent = None, skip_all, fields(broker=?self.inner.broker))]
    async fn run(mut self) -> BrokerState {
        tracing::trace!("connecting to broker");

        // In the case of a reconnect, drain pending requests.
        self.drain_pending_requests().await;

        let shutdown = self.inner.shutdown.clone();
        loop {
            let res = tokio::select! {
                res = self.try_connect() => res,
                _ = shutdown.cancelled() => return BrokerState::Terminated(self.inner),
            };
            match res {
                Ok((reader, writer)) => return BrokerState::Versioning(BrokerVersioning { inner: self.inner, reader, writer }),
                Err(err) => {
                    tracing::error!(error = ?err, "error connecting to broker, will retry");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }

    async fn try_connect(&mut self) -> Result<(KafkaReader, KafkaWriter)> {
        // Attempt to establish a connection to the broker.
        let host = self.inner.broker.connection_string();
        let conn = TcpStream::connect(&host).await.context("unable to connect to host")?;

        // Set TCP nodelay on sockets.
        {
            let keepalive = socket2::TcpKeepalive::new().with_time(Duration::from_secs(10)).with_interval(Duration::from_secs(20)).with_retries(5);
            let sock = socket2::SockRef::from(&conn);
            sock.set_nodelay(true)
                .and_then(|_| sock.set_nodelay(true))
                .and_then(|_| sock.set_tcp_keepalive(&keepalive))
                .context("error configuring keepalive on broker socket")?;
        }

        // Wrap TCP stream in framed codecs.
        Ok(codec::new_kafka_transport(conn, None))
    }

    /// Drain any pending requests upon reconnect.
    async fn drain_pending_requests(&mut self) {
        while let Some((_, req)) = self.inner.requests.pop_first() {
            if let Some(chan) = req.chan {
                let _ = chan.send(BrokerResponse {
                    id: req.id,
                    result: Err(BrokerRequestError {
                        payload: req.request.kind,
                        kind: BrokerErrorKind::Disconnected,
                    }),
                });
            }
        }
    }
}

/// The broker needs to gather API versions info.
struct BrokerVersioning {
    inner: BrokerTask,
    reader: KafkaReader,
    writer: KafkaWriter,
}

impl BrokerVersioning {
    #[tracing::instrument(level = "trace", parent = None, skip_all, fields(broker=?self.inner.broker))]
    async fn run(mut self) -> BrokerState {
        tracing::trace!("fetching api versions info from broker");
        let shutdown = self.inner.shutdown.clone();
        let res = tokio::select! {
            res = self.try_get_api_versions() => res,
            _ = shutdown.cancelled() => return BrokerState::Terminated(self.inner),
        };

        if let Err(err) = res {
            tracing::error!(error = ?err, "error fetching api versions info from broker");
            tokio::time::sleep(Duration::from_secs(2)).await;
            return BrokerState::Connecting(BrokerConnecting { inner: self.inner });
        }

        // API versions info has been gathered, proceed to ready state.
        return BrokerState::Ready(BrokerReady {
            inner: self.inner,
            reader: self.reader,
            writer: self.writer,
        });
    }

    async fn try_get_api_versions(&mut self) -> Result<()> {
        // Fetch initial API versions info for the broker.
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.inner.get_api_versions(&mut self.writer, Some(tx)).await.context("broker connection lost, reconnecting")?;

        // Wait for an API versions response with a timeout.
        let fut = timeout(Duration::from_secs(10), rx.recv());
        tokio::pin!(fut);
        loop {
            tokio::select! {
                res = &mut fut => {
                    res
                        .context("timeout while waiting for api versions response")?
                        .context("channel dropped while awaiting api versions response")?
                        .result?;
                        // .context("error in api versions response from broker")?;
                    return Ok(());
                },
                opt_res = self.reader.next() => {
                    self.inner.handle_incoming_frame(opt_res).await
                        .context("error handling broker response")?;
                },
            }
        }
    }
}

/// The broker is ready for normal usage.
struct BrokerReady {
    inner: BrokerTask,
    reader: KafkaReader,
    writer: KafkaWriter,
}

impl BrokerReady {
    #[tracing::instrument(level = "trace", parent = None, skip_all, fields(broker=?self.inner.broker))]
    async fn run(mut self) -> BrokerState {
        // Main operations loop.
        let shutdown = self.inner.shutdown.clone();
        loop {
            let res = tokio::select! {
                opt = self.reader.next() => self.inner.handle_incoming_frame(opt).await,
                Some(msg) = self.inner.chan.recv() => self.inner.handle_msg(&mut self.writer, msg).await,
                _ = shutdown.cancelled() => return BrokerState::Terminated(self.inner),
            };
            if let Err(err) = res {
                tracing::error!(error = ?err, "error received from broker, need to reconnect");
                return BrokerState::Connecting(BrokerConnecting { inner: self.inner });
            }
        }
    }
}

/// An outbound request to be sent to a broker.
struct OutboundRequest {
    /// The application ID of the corresponding request (not the correlation ID for the broker).
    id: uuid::Uuid,
    /// The original request.
    request: Request,
    /// The API key of the corresponding request.
    api_key: ApiKey,
    /// The API version of the corresponding request.
    api_version: i16,
    /// The channel used to send the finalized response.
    chan: Option<MsgTx>,
}

/// Metadata on a broker, used for establishing connections.
#[derive(Debug)]
pub(crate) enum BrokerConnInfo {
    Meta(MetadataResponseBroker),
    Host(String),
}

impl BrokerConnInfo {
    /// Get the connection string to be used for connecting to this broker.
    pub(crate) fn connection_string(&self) -> String {
        match self {
            Self::Meta(meta) => format!("{}:{}", meta.host.as_str(), meta.port),
            Self::Host(host) => host.clone(),
        }
    }
}

impl From<String> for BrokerConnInfo {
    fn from(value: String) -> Self {
        Self::Host(value)
    }
}

impl From<MetadataResponseBroker> for BrokerConnInfo {
    fn from(value: MetadataResponseBroker) -> Self {
        Self::Meta(value)
    }
}
