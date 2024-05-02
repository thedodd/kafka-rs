use anyhow::{Context, Result};
use kafka_rs::Client;
use tracing_subscriber::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        // Filter spans based on the RUST_LOG env var.
        .with(tracing_subscriber::EnvFilter::new("error,get_metadata=debug"))
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false) // Too verbose, so disable target.
                .with_level(true) // Shows tracing level: debug, info, error &c.
                .compact(),
        )
        // Install this registry as the global tracing registry.
        .try_init()
        .expect("error initializing tracing");

    // Fetch cluster metadata and print it out.
    let cli = Client::new(vec!["localhost:9092".into()]);
    let metadata = cli.get_metadata(0).await.context("error calling get_metadata")?;
    tracing::info!(?metadata);

    Ok(())
}
