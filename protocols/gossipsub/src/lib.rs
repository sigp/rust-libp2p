// Copyright 2020 Sigma Prime Pty Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

//! Gossipsub is a P2P pubsub (publish/subscription) routing layer designed to extend upon
//! flooodsub and meshsub routing protocols.
//!
//! # Overview
//!
//! *Note: The gossipsub protocol specifications
//! (https://github.com/libp2p/specs/tree/master/pubsub/gossipsub) provide an outline for the
//! routing protocol. They should be consulted for further detail.*
//!
//! Gossipsub  is a blend of meshsub for data and randomsub for mesh metadata. It provides bounded
//! degree and amplification factor with the meshsub construction and augments it using gossip
//! propagation of metadata with the randomsub technique.
//!
//! The router maintains an overlay mesh network of peers on which to efficiently send messages and
//! metadata.  Peers use control messages to broadcast and request known messages and
//! subscribe/unsubscribe from topics in the mesh network.
//!
//! # Important Discrepancies
//!
//! This section outlines the current implementation's potential discrepancies from that of other
//! implementations, due to undefined elements in the current specification.
//!
//! - **Topics** -  In gossipsub, topics configurable by the `hash_topics` configuration parameter.
//! Topics are of type [`TopicHash`]. The current go implementation uses raw utf-8 strings, and this
//! is default configuration in rust-libp2p. Topics can be hashed (SHA256 hashed then base64
//! encoded) by setting the `hash_topics` configuration parameter to true.
//!
//! - **Sequence Numbers** - A message on the gossipsub network is identified by the source
//! `PeerId` and a nonce (sequence number) of the message. The sequence numbers in this
//! implementation are sent as raw bytes across the wire. They are 64-bit big-endian unsigned
//! integers. They are chosen at random in this implementation of gossipsub, but are sequential in
//! the current go implementation.
//!
//! # Using Gossipsub
//!
// mxinden: I think it will be hard to keep this part of the documentation in sync. Is the
// duplication really necessary, or would the link suffice?
//! ## GossipsubConfig
//!
//! The [`GenericGossipsubConfig`] struct specifies various network performance/tuning configuration
//! parameters. Specifically it specifies:
//!
//! [`GenericGossipsubConfig`]: struct.GenericGossipsubConfig.html
//!
//! This struct implements the `Default` trait and can be initialised via
//! `GenericGossipsubConfig::default()`.
//!
//!
//! ## Gossipsub
//!
//! The [`GenericGossipsub`] struct implements the `NetworkBehaviour` trait allowing it to act as the
//! routing behaviour in a `Swarm`. This struct requires an instance of `PeerId` and
//! [`GenericGossipsubConfig`].
//!
//! [`GenericGossipsub`]: struct.Gossipsub.html

//! ## Example
//!
//! An example of initialising a gossipsub compatible swarm:
//!
// mxinden: Why ignore the doc example? This would be a great way to keep it up-to-date.
//! ```ignore
//! #extern crate libp2p;
//! #extern crate futures;
//! #extern crate tokio;
//! #use libp2p::gossipsub::GossipsubEvent;
//! #use libp2p::{identity, gossipsub,
//! #    tokio_codec::{FramedRead, LinesCodec},
//! #};
//! let local_key = identity::Keypair::generate_ed25519();
//! let local_pub_key = local_key.public();
//!
//! // Set up an encrypted TCP Transport over the Mplex and Yamux protocols
// mxinden: I suggest using the memory transport to not depend on any assumptions of the executing
// machine.
//! let transport = libp2p::build_development_transport(local_key);
//!
//! // Create a Floodsub/Gossipsub topic
//! let topic = libp2p::floodsub::TopicBuilder::new("example").build();
//!
//! // Create a Swarm to manage peers and events
//! let mut swarm = {
//!     // set default parameters for gossipsub
//!     let gossipsub_config = gossipsub::GossipsubConfig::default();
//!     // build a gossipsub network behaviour
//!     let mut gossipsub =
//!         gossipsub::Gossipsub::new(local_pub_key.clone().into_peer_id(), gossipsub_config);
//!     gossipsub.subscribe(topic.clone());
//!     libp2p::Swarm::new(
//!         transport,
//!         gossipsub,
//!         libp2p::core::topology::MemoryTopology::empty(local_pub_key),
//!     )
//! };
//!
//! // Listen on all interfaces and whatever port the OS assigns.
//! let addr = libp2p::Swarm::listen_on(&mut swarm, "/ip4/0.0.0.0/tcp/0".parse().unwrap()).unwrap();
//! println!("Listening on {:?}", addr);
//! ```

pub mod error;
pub mod protocol;

mod backoff;
mod behaviour;
mod config;
mod gossip_promises;
mod handler;
mod mcache;
mod peer_score;
pub mod subscription_filter;
pub mod time_cache;
mod topic;
mod types;

#[cfg(test)]
#[macro_use]
extern crate derive_builder;

mod rpc_proto;

pub use self::behaviour::{
    GenericGossipsub, GenericGossipsubEvent, Gossipsub, GossipsubEvent, MessageAuthenticity,
};
pub use self::config::{
    GenericGossipsubConfig, GenericGossipsubConfigBuilder, GossipsubConfig, GossipsubConfigBuilder,
    ValidationMode,
};
pub use self::peer_score::{
    score_parameter_decay, score_parameter_decay_with_base, PeerScoreParams, PeerScoreThresholds,
    TopicScoreParams,
};
pub use self::topic::{Hasher, Topic, TopicHash};
pub use self::types::{
    FastMessageId, GenericGossipsubMessage, GossipsubMessage, GossipsubRpc, MessageAcceptance,
    MessageId, RawGossipsubMessage,
};
pub type IdentTopic = Topic<self::topic::IdentityHash>;
pub type Sha256Topic = Topic<self::topic::Sha256Hash>;
