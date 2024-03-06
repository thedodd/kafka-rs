use anyhow::{Context, Result};
use bytes::Bytes;
use kafka_rs::{Acks, Client, Compression, Message};
use tracing_subscriber::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        // Filter spans based on the RUST_LOG env var.
        .with(tracing_subscriber::EnvFilter::new("error,kafka_rs=debug"))
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false) // Too verbose, so disable target.
                .with_level(true) // Shows tracing level: debug, info, error &c.
                .compact(),
        )
        // Install this registry as the global tracing registry.
        .try_init()
        .expect("error initializing tracing");

    let cli = Client::new(vec!["localhost:9092".into()]);
    let mut producer = cli.topic_producer("testing", Acks::All, None, Some(Compression::Snappy));

    let messages = vec![Message::new(Some("key0".into()), Some(Bytes::from_static(b"val0")), Default::default())];
    producer.produce(&messages).await.context("error producing data to cluster")
}
