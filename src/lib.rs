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

pub use client::{Acks, Client, ListOffsetsPosition, Message, TopicProducer};
pub use kafka_protocol::indexmap::IndexMap;
pub use kafka_protocol::protocol::StrBytes;
pub use kafka_protocol::records::{Compression, Record};

#[cfg(feature = "internal")]
pub use internal::InternalClient;

/* TODO:
- look into using the multishot crate throughout, instead of lots of oneshot allocations or unwieldy unbounded_mpsc.
*/
