use anyhow::{Context, Result};
use kafka_protocol::messages::create_topics_request::{CreatableTopicBuilder, CreateTopicsRequestBuilder};
use kafka_protocol::messages::TopicName;
use kafka_rs::{Client, IndexMap, StrBytes};
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
    let host = std::env::var("HOST").context("HOST env var must be specified")?;
    let port = std::env::var("PORT").context("PORT env var must be specified")?;
    let cli = Client::new(vec![(host + ":" + &port).into()]);

    // Create new topic.
    let mut admin = cli.admin();

    let mut ctb = CreatableTopicBuilder::default();
    ctb.replication_factor(1);
    ctb.num_partitions(1);
    let ct = ctb.build().unwrap();

    let mut ctrb = CreateTopicsRequestBuilder::default();
    let mut imap = IndexMap::new();
    imap.insert(TopicName(topic), ct);
    ctrb.topics(imap);
    let req = ctrb.build().unwrap();
    println!("{}", req.topics.len());

    admin.create_topics(req).await.context("error creating topic")?;
    Ok(())
}
