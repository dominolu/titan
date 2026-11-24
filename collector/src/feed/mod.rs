//! Sources the recorder reads market updates from.
//!
//! A recorder market does not care whether its feed is the venue adapter compiled into this crate
//! or the shared connector stream; both arrive as [`collector::book::FeedMessage`].

pub mod connector;
