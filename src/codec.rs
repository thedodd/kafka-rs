use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestHeader, RequestKind};
use kafka_protocol::protocol::buf::ByteBuf;
use kafka_protocol::protocol::Encodable;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite, LengthDelimitedCodec};

/// The default max size for API messages sent to Kafka.
const DEFAULT_MAX_SIZE: usize = 1024usize.pow(2) * 16; // 16MiB.

pub(crate) type KafkaReader = FramedRead<OwnedReadHalf, KafkaCodecReader>;
pub(crate) type KafkaWriter = FramedWrite<OwnedWriteHalf, KafkaCodecWriter>;

/// Construct a new Kafka transport, establishing framed codecs for the reader & writer halves of the given socket.
pub(crate) fn new_kafka_transport(conn: TcpStream, max_size: Option<usize>) -> (KafkaReader, KafkaWriter) {
    let max = max_size.unwrap_or(DEFAULT_MAX_SIZE);
    let codec = tokio_util::codec::LengthDelimitedCodec::builder().big_endian().length_field_length(4).max_frame_length(max).new_codec();

    let (rx, tx) = conn.into_split();
    let framed_tx = FramedWrite::new(tx, KafkaCodecWriter);
    let framed_rx = FramedRead::new(rx, KafkaCodecReader(codec.clone()));

    (framed_rx, framed_tx)
}

/// A codec used for encoding & decoding Kafka API messages.
pub(crate) struct KafkaCodecReader(LengthDelimitedCodec);

/// A response received from a Kafka broker.
pub(crate) struct Response {
    /// The correlation ID of this response.
    pub correlation_id: i32,
    /// The full header + body response payload, not including the length delimiter.
    pub body: Bytes,
}

impl Decoder for KafkaCodecReader {
    type Item = Response;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let Some(frame) = self.0.decode(src)? else { return Ok(None) };
        let mut body = frame.freeze();

        // Decode the correlation ID, which will always be the first 4 bytes following the length delimiter.
        let correlation_id = body.peek_bytes(0..4).get_i32();
        Ok(Some(Response { correlation_id, body }))
    }
}

/// A codec used for encoding & decoding Kafka API messages.
pub(crate) struct KafkaCodecWriter;

/// A request ready to be sent to a Kafka broker.
pub(crate) struct Request {
    /// The request header.
    pub header: RequestHeader,
    /// The request body.
    pub kind: RequestKind,
}

impl Encoder<&'_ Request> for KafkaCodecWriter {
    type Error = anyhow::Error;

    fn encode(&mut self, item: &'_ Request, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Compute the encoding size of the header.
        let key = ApiKey::try_from(item.header.request_api_key).map_err(|_| anyhow::anyhow!("unknown API key"))?;
        let header_version = key.request_header_version(item.header.request_api_version);
        let header_size = item.header.compute_size(header_version).context("error computing encoding size of request header")?;

        // Compute the encoding size of the request body.
        let size_res = match &item.kind {
            RequestKind::ProduceRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::FetchRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::ListOffsetsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::MetadataRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::LeaderAndIsrRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::StopReplicaRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::UpdateMetadataRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::ControlledShutdownRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::OffsetCommitRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::OffsetFetchRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::FindCoordinatorRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::JoinGroupRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::HeartbeatRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::LeaveGroupRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::SyncGroupRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DescribeGroupsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::ListGroupsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::SaslHandshakeRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::ApiVersionsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::CreateTopicsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DeleteTopicsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DeleteRecordsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::InitProducerIdRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::OffsetForLeaderEpochRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::AddPartitionsToTxnRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::AddOffsetsToTxnRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::EndTxnRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::WriteTxnMarkersRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::TxnOffsetCommitRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DescribeAclsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::CreateAclsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DeleteAclsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DescribeConfigsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::AlterConfigsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::AlterReplicaLogDirsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DescribeLogDirsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::SaslAuthenticateRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::CreatePartitionsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::CreateDelegationTokenRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::RenewDelegationTokenRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::ExpireDelegationTokenRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DescribeDelegationTokenRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DeleteGroupsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::ElectLeadersRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::IncrementalAlterConfigsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::AlterPartitionReassignmentsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::ListPartitionReassignmentsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::OffsetDeleteRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DescribeClientQuotasRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::AlterClientQuotasRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DescribeUserScramCredentialsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::AlterUserScramCredentialsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::VoteRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::BeginQuorumEpochRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::EndQuorumEpochRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DescribeQuorumRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::AlterPartitionRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::UpdateFeaturesRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::EnvelopeRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::FetchSnapshotRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DescribeClusterRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DescribeProducersRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::BrokerRegistrationRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::BrokerHeartbeatRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::UnregisterBrokerRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::DescribeTransactionsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::ListTransactionsRequest(inner) => inner.compute_size(item.header.request_api_version),
            RequestKind::AllocateProducerIdsRequest(inner) => inner.compute_size(item.header.request_api_version),
            kind => anyhow::bail!("unknown request kind received: {:?}", kind),
        };
        let body_size = size_res.context("error computing encoding size of request body")?;
        let request_size = header_size + body_size;
        dst.reserve(request_size);

        // Encode the length delimiter and header based on the given API key and versions.
        dst.put_i32(request_size as i32);
        item.header.encode(dst, header_version).context("error encoding request header")?;

        // Encode the request body.
        let res = match &item.kind {
            RequestKind::ProduceRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::FetchRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::ListOffsetsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::MetadataRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::LeaderAndIsrRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::StopReplicaRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::UpdateMetadataRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::ControlledShutdownRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::OffsetCommitRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::OffsetFetchRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::FindCoordinatorRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::JoinGroupRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::HeartbeatRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::LeaveGroupRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::SyncGroupRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DescribeGroupsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::ListGroupsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::SaslHandshakeRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::ApiVersionsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::CreateTopicsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DeleteTopicsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DeleteRecordsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::InitProducerIdRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::OffsetForLeaderEpochRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::AddPartitionsToTxnRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::AddOffsetsToTxnRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::EndTxnRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::WriteTxnMarkersRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::TxnOffsetCommitRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DescribeAclsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::CreateAclsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DeleteAclsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DescribeConfigsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::AlterConfigsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::AlterReplicaLogDirsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DescribeLogDirsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::SaslAuthenticateRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::CreatePartitionsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::CreateDelegationTokenRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::RenewDelegationTokenRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::ExpireDelegationTokenRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DescribeDelegationTokenRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DeleteGroupsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::ElectLeadersRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::IncrementalAlterConfigsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::AlterPartitionReassignmentsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::ListPartitionReassignmentsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::OffsetDeleteRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DescribeClientQuotasRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::AlterClientQuotasRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DescribeUserScramCredentialsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::AlterUserScramCredentialsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::VoteRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::BeginQuorumEpochRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::EndQuorumEpochRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DescribeQuorumRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::AlterPartitionRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::UpdateFeaturesRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::EnvelopeRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::FetchSnapshotRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DescribeClusterRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DescribeProducersRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::BrokerRegistrationRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::BrokerHeartbeatRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::UnregisterBrokerRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::DescribeTransactionsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::ListTransactionsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            RequestKind::AllocateProducerIdsRequest(inner) => inner.encode(dst, item.header.request_api_version),
            kind => anyhow::bail!("unknown request kind received: {:?}", kind),
        };
        res.context("error encoding request body")?;
        Ok(())
    }
}
