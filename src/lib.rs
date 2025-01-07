#![allow(clippy::single_match)] // Single-match is great for future-proofing code.

mod broker;
pub mod client;
mod clitask;
mod codec;
pub mod error;
#[cfg(feature = "internal")]
pub mod internal;

pub use kafka_protocol;
pub use kafka_protocol::indexmap;

#[cfg(all(feature = "internal", feature = "mock"))]
pub use client::MockClientApi;
pub use client::{Acks, Client, ClientApi, ListOffsetsPosition, Message, TopicProducer};
pub use kafka_protocol::indexmap::IndexMap;
pub use kafka_protocol::protocol::StrBytes;
pub use kafka_protocol::records::{Compression, Record};

#[cfg(all(feature = "internal", feature = "mock"))]
pub use internal::MockInternalClientApi;
#[cfg(feature = "internal")]
pub use internal::{InternalClient, InternalClientApi};

/* TODO:
- look into using the multishot crate throughout, instead of lots of oneshot allocations or unwieldy unbounded_mpsc.
*/
