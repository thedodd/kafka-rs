use anyhow::{Context, Result};
use bytes::Bytes;
use kafka_rs::{Acks, Client, Compression, ListOffsetsPosition, Message, StrBytes};
use tracing_subscriber::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        // Filter spans based on the RUST_LOG env var.
        .with(tracing_subscriber::EnvFilter::new("error,simple_producer=debug,kafka_rs=debug"))
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false) // Too verbose, so disable target.
                .with_level(true) // Shows tracing level: debug, info, error &c.
                .compact(),
        )
        // Install this registry as the global tracing registry.
        .try_init()
        .expect("error initializing tracing");

    let topic = StrBytes::from_string(std::env::var("TOPIC").context("TOPIC env var must be specified")?);
    let cli = Client::new(vec!["localhost:9092".into()]);

    // Fetch the starting position of the target topic.
    let start = cli.list_offsets(topic.clone(), 0, ListOffsetsPosition::Earliest).await.context("error listing offsets")?;
    tracing::info!(start, "found log start");

    // Produce some data.
    let mut producer = cli.topic_producer(topic.as_str(), Acks::All, None, Some(Compression::Snappy));
    let messages = vec![Message::new(Some("key0".into()), Some(Bytes::from_static(b"val0")), Default::default())];
    let (first_offset, last_offset) = producer.produce(&messages).await.context("error producing data to cluster")?;
    tracing::info!("first_offset={}; last_offset={}", first_offset, last_offset);

    // Fetch the same data back.
    let records = cli.fetch(topic.clone(), 0, 0).await.context("error fetching batch")?;
    tracing::info!(?records, "fetched batch");

    Ok(())
}
