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

//! A collection of types using the Gossipsub system.
#[cfg(feature = "partial_messages")]
use std::collections::HashMap;
use std::{
    collections::BTreeSet,
    fmt::{self, Debug},
};

use futures_timer::Delay;
use hashlink::LinkedHashMap;
use libp2p_identity::PeerId;
use libp2p_swarm::ConnectionId;
use quick_protobuf::MessageWrite;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use web_time::Instant;

use crate::{queue::Queue, rpc_proto::proto, TopicHash};

/// Messages that have expired while attempting to be sent to a peer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FailedMessages {
    /// The number of messages that were failed to be sent to the priority queue as it was
    /// full.
    pub priority: usize,
    /// The number of messages that were failed to be sent to the non priority queue as it was
    /// full.
    pub non_priority: usize,
}

#[derive(Debug)]
/// Validation kinds from the application for received messages.
pub enum MessageAcceptance {
    /// The message is considered valid, and it should be delivered and forwarded to the network.
    Accept,
    /// The message is considered invalid, and it should be rejected and trigger the P₄ penalty.
    Reject,
    /// The message is neither delivered nor forwarded to the network, but the router does not
    /// trigger the P₄ penalty.
    Ignore,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(pub Vec<u8>);

impl MessageId {
    pub fn new(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl<T: Into<Vec<u8>>> From<T> for MessageId {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex_fmt::HexFmt(&self.0))
    }
}

impl std::fmt::Debug for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MessageId({})", hex_fmt::HexFmt(&self.0))
    }
}

#[derive(Debug)]
/// Connected peer details.
pub(crate) struct PeerDetails {
    /// The kind of protocol the peer supports.
    pub(crate) kind: PeerKind,
    /// The Extensions supported by the peer if any.
    pub(crate) extensions: Option<Extensions>,
    /// If the peer is an outbound connection.
    pub(crate) outbound: bool,
    /// Its current connections.
    pub(crate) connections: Vec<ConnectionId>,
    /// Subscribed topics.
    pub(crate) topics: BTreeSet<TopicHash>,
    /// Don't send messages.
    pub(crate) dont_send: LinkedHashMap<MessageId, Instant>,

    /// Message queue consumed by the connection handler.
    pub(crate) messages: Queue,

    /// Peer Partial messages.
    #[cfg(feature = "partial_messages")]
    pub(crate) partial_messages: HashMap<TopicHash, HashMap<Vec<u8>, PartialData>>,

    /// Partial only subscribed topics.
    #[cfg(feature = "partial_messages")]
    pub(crate) partial_only_topics: BTreeSet<TopicHash>,
}

/// Stored `Metadata` for a peer.
#[cfg(feature = "partial_messages")]
#[derive(Debug)]
pub(crate) enum PeerMetadata {
    Remote(Vec<u8>),
    Local(Box<dyn crate::partial::Metadata>),
}

#[cfg(feature = "partial_messages")]
impl AsRef<[u8]> for PeerMetadata {
    fn as_ref(&self) -> &[u8] {
        match self {
            PeerMetadata::Remote(metadata) => metadata,
            PeerMetadata::Local(metadata) => metadata.as_slice(),
        }
    }
}

/// The partial message data the peer has.
#[cfg(feature = "partial_messages")]
#[derive(Debug)]
pub(crate) struct PartialData {
    /// The current peer partial metadata.
    pub(crate) metadata: Option<PeerMetadata>,
    /// The remaining heartbeats for this message to be deleted.
    pub(crate) ttl: usize,
}

#[cfg(feature = "partial_messages")]
impl Default for PartialData {
    fn default() -> Self {
        Self {
            metadata: Default::default(),
            ttl: 5,
        }
    }
}

/// Describes the types of peers that can exist in the gossipsub context.
#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq)]
#[cfg_attr(
    feature = "metrics",
    derive(prometheus_client::encoding::EncodeLabelValue)
)]
pub enum PeerKind {
    /// A gossipsub 1.3 peer.
    Gossipsubv1_3,
    /// A gossipsub 1.2 peer.
    Gossipsubv1_2,
    /// A gossipsub 1.1 peer.
    Gossipsubv1_1,
    /// A gossipsub 1.0 peer.
    Gossipsub,
    /// A floodsub peer.
    Floodsub,
    /// The peer doesn't support any of the protocols.
    NotSupported,
}

/// A message received by the gossipsub system and stored locally in caches..
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct RawMessage {
    /// Id of the peer that published this message.
    pub source: Option<PeerId>,

    /// Content of the message. Its meaning is out of scope of this library.
    pub data: Vec<u8>,

    /// A random sequence number.
    pub sequence_number: Option<u64>,

    /// The topic this message belongs to
    pub topic: TopicHash,

    /// The signature of the message if it's signed.
    pub signature: Option<Vec<u8>>,

    /// The public key of the message if it is signed and the source [`PeerId`] cannot be inlined.
    pub key: Option<Vec<u8>>,

    /// Flag indicating if this message has been validated by the application or not.
    pub validated: bool,
}

impl PeerKind {
    /// Returns true if peer speaks any gossipsub version.
    pub(crate) fn is_gossipsub(&self) -> bool {
        matches!(
            self,
            Self::Gossipsubv1_2 | Self::Gossipsubv1_1 | Self::Gossipsub
        )
    }
}

impl RawMessage {
    /// Calculates the encoded length of this message (used for calculating metrics).
    pub fn raw_protobuf_len(&self) -> usize {
        let message = proto::Message {
            from: self.source.map(|m| m.to_bytes()),
            data: Some(self.data.clone()),
            seqno: self.sequence_number.map(|s| s.to_be_bytes().to_vec()),
            topic: TopicHash::into_string(self.topic.clone()),
            signature: self.signature.clone(),
            key: self.key.clone(),
        };
        message.get_size()
    }
}

impl From<RawMessage> for proto::Message {
    fn from(raw: RawMessage) -> Self {
        proto::Message {
            from: raw.source.map(|m| m.to_bytes()),
            data: Some(raw.data),
            seqno: raw.sequence_number.map(|s| s.to_be_bytes().to_vec()),
            topic: TopicHash::into_string(raw.topic),
            signature: raw.signature,
            key: raw.key,
        }
    }
}

/// The message sent to the user after a [`RawMessage`] has been transformed by a
/// [`crate::DataTransform`].
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Message {
    /// Id of the peer that published this message.
    pub source: Option<PeerId>,

    /// Content of the message.
    pub data: Vec<u8>,

    /// A random sequence number.
    pub sequence_number: Option<u64>,

    /// The topic this message belongs to
    pub topic: TopicHash,
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Message")
            .field(
                "data",
                &format_args!("{:<20}", &hex_fmt::HexFmt(&self.data)),
            )
            .field("source", &self.source)
            .field("sequence_number", &self.sequence_number)
            .field("topic", &self.topic)
            .finish()
    }
}

/// A subscription received by the gossipsub system.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Subscription {
    /// Action to perform.
    pub action: SubscriptionAction,
    /// The topic from which to subscribe or unsubscribe.
    pub topic_hash: TopicHash,
    /// Peer only wants to receive partial messages instead of full messages.
    #[cfg(feature = "partial_messages")]
    pub partial: bool,
}

/// Action that a subscription wants to perform.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubscriptionAction {
    /// The remote wants to subscribe to the given topic.
    Subscribe,
    /// The remote wants to unsubscribe from the given topic.
    Unsubscribe,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PeerInfo {
    pub(crate) peer_id: Option<PeerId>,
    // TODO add this when RFC: Signed Address Records got added to the spec (see pull request
    // https://github.com/libp2p/specs/pull/217)
    // pub signed_peer_record: ?,
}

/// A Control message received by the gossipsub system.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ControlAction {
    /// Node broadcasts known messages per topic - IHave control message.
    IHave(IHave),
    /// The node requests specific message ids (peer_id + sequence _number) - IWant control
    /// message.
    IWant(IWant),
    /// The node has been added to the mesh - Graft control message.
    Graft(Graft),
    /// The node has been removed from the mesh - Prune control message.
    Prune(Prune),
    /// The node requests us to not forward message ids (peer_id + sequence _number) - IDontWant
    /// control message.
    IDontWant(IDontWant),
    /// The Node has sent us its supported extensions.
    Extensions(Option<Extensions>),
}

/// Node broadcasts known messages per topic - IHave control message.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IHave {
    /// The topic of the messages.
    pub(crate) topic_hash: TopicHash,
    /// A list of known message ids (peer_id + sequence _number) as a string.
    pub(crate) message_ids: Vec<MessageId>,
}

/// The node requests specific message ids (peer_id + sequence _number) - IWant control message.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IWant {
    /// A list of known message ids (peer_id + sequence _number) as a string.
    pub(crate) message_ids: Vec<MessageId>,
}

/// The node has been added to the mesh - Graft control message.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Graft {
    /// The mesh topic the peer should be added to.
    pub(crate) topic_hash: TopicHash,
}

/// The node has been removed from the mesh - Prune control message.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Prune {
    /// The mesh topic the peer should be removed from.
    pub(crate) topic_hash: TopicHash,
    /// A list of peers to be proposed to the removed peer as peer exchange
    pub(crate) peers: Vec<PeerInfo>,
    /// The backoff time in seconds before we allow to reconnect
    pub(crate) backoff: Option<u64>,
}

/// The node requests us to not forward message ids - IDontWant control message.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IDontWant {
    /// A list of known message ids.
    pub(crate) message_ids: Vec<MessageId>,
}

/// A received partial message.
#[cfg(feature = "partial_messages")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartialMessage {
    /// The topic ID this partial message belongs to.
    pub topic_id: TopicHash,
    /// The group ID that identifies the complete logical message.
    pub group_id: Vec<u8>,
    /// The partial metadata we have and we want.
    pub metadata: Option<Vec<u8>>,
    /// The partial message itself.
    pub message: Option<Vec<u8>>,
}

/// The node has sent us the supported Gossipsub Extensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Extensions {
    pub(crate) test_extension: Option<bool>,
    pub(crate) partial_messages: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TestExtension {}

/// A Gossipsub RPC message sent.
#[derive(Debug)]
pub enum RpcOut {
    /// Publish a Gossipsub message on network.`timeout` limits the duration the message
    /// can wait to be sent before it is abandoned.
    Publish {
        message_id: MessageId,
        message: RawMessage,
        timeout: Delay,
    },
    /// Forward a Gossipsub message on network. `timeout` limits the duration the message
    /// can wait to be sent before it is abandoned.
    Forward {
        message_id: MessageId,
        message: RawMessage,
        timeout: Delay,
    },
    /// Subscribe a topic.
    Subscribe {
        topic: TopicHash,
        #[cfg(feature = "partial_messages")]
        partial_only: bool,
    },
    /// Unsubscribe a topic.
    Unsubscribe(TopicHash),
    /// Send a GRAFT control message.
    Graft(Graft),
    /// Send a PRUNE control message.
    Prune(Prune),
    /// Send a IHave control message.
    IHave(IHave),
    /// Send a IWant control message.
    IWant(IWant),
    /// The node requests us to not forward message ids (peer_id + sequence _number) - IDontWant
    /// control message.
    IDontWant(IDontWant),
    /// Send a Extensions control message.
    Extensions(Extensions),
    /// Send a test extension message.
    TestExtension,
    /// Send a partial messages extension.
    PartialMessage {
        /// The group ID that identifies the complete logical message.
        group_id: Vec<u8>,
        /// The topic ID this partial message belongs to.
        topic_id: TopicHash,
        /// The partial message itself.
        message: Option<Vec<u8>>,
        /// The partial metadata we have and want.
        metadata: Vec<u8>,
    },
}

impl RpcOut {
    /// Converts the GossipsubRPC into its protobuf format.
    // A convenience function to avoid explicitly specifying types.
    pub fn into_protobuf(self) -> proto::RPC {
        self.into()
    }

    /// Returns true if the `RpcOut` is priority.
    pub(crate) fn priority(&self) -> bool {
        matches!(
            self,
            RpcOut::Subscribe { .. }
                | RpcOut::Unsubscribe(_)
                | RpcOut::Graft(_)
                | RpcOut::Prune(_)
                | RpcOut::IDontWant(_)
        )
    }
}

impl From<RpcOut> for proto::RPC {
    /// Converts the RPC into protobuf format.
    fn from(rpc: RpcOut) -> Self {
        match rpc {
            RpcOut::Publish { message, .. } => proto::RPC {
                subscriptions: Vec::new(),
                publish: vec![message.into()],
                control: None,
                testExtension: None,
                partial: None,
            },
            RpcOut::Forward { message, .. } => proto::RPC {
                publish: vec![message.into()],
                subscriptions: Vec::new(),
                control: None,
                testExtension: None,
                partial: None,
            },
            RpcOut::Subscribe {
                topic,
                #[cfg(feature = "partial_messages")]
                partial_only,
            } => proto::RPC {
                publish: Vec::new(),
                subscriptions: vec![proto::SubOpts {
                    subscribe: Some(true),
                    topic_id: Some(topic.into_string()),
                    #[cfg(not(feature = "partial_messages"))]
                    partial: None,
                    #[cfg(feature = "partial_messages")]
                    partial: Some(partial_only),
                }],
                control: None,
                testExtension: None,
                partial: None,
            },
            RpcOut::Unsubscribe(topic) => proto::RPC {
                publish: Vec::new(),
                subscriptions: vec![proto::SubOpts {
                    subscribe: Some(false),
                    topic_id: Some(topic.into_string()),
                    partial: None,
                }],
                control: None,
                testExtension: None,
                partial: None,
            },
            RpcOut::IHave(IHave {
                topic_hash,
                message_ids,
            }) => proto::RPC {
                publish: Vec::new(),
                subscriptions: Vec::new(),
                control: Some(proto::ControlMessage {
                    ihave: vec![proto::ControlIHave {
                        topic_id: Some(topic_hash.into_string()),
                        message_ids: message_ids.into_iter().map(|msg_id| msg_id.0).collect(),
                    }],
                    iwant: vec![],
                    graft: vec![],
                    prune: vec![],
                    idontwant: vec![],
                    extensions: None,
                }),
                testExtension: None,
                partial: None,
            },
            RpcOut::IWant(IWant { message_ids }) => proto::RPC {
                publish: Vec::new(),
                subscriptions: Vec::new(),
                control: Some(proto::ControlMessage {
                    ihave: vec![],
                    iwant: vec![proto::ControlIWant {
                        message_ids: message_ids.into_iter().map(|msg_id| msg_id.0).collect(),
                    }],
                    graft: vec![],
                    prune: vec![],
                    idontwant: vec![],
                    extensions: None,
                }),
                testExtension: None,
                partial: None,
            },
            RpcOut::Graft(Graft { topic_hash }) => proto::RPC {
                publish: Vec::new(),
                subscriptions: vec![],
                control: Some(proto::ControlMessage {
                    ihave: vec![],
                    iwant: vec![],
                    graft: vec![proto::ControlGraft {
                        topic_id: Some(topic_hash.into_string()),
                    }],
                    prune: vec![],
                    idontwant: vec![],
                    extensions: None,
                }),
                testExtension: None,
                partial: None,
            },
            RpcOut::Prune(Prune {
                topic_hash,
                peers,
                backoff,
            }) => {
                proto::RPC {
                    publish: Vec::new(),
                    subscriptions: vec![],
                    control: Some(proto::ControlMessage {
                        ihave: vec![],
                        iwant: vec![],
                        graft: vec![],
                        prune: vec![proto::ControlPrune {
                            topic_id: Some(topic_hash.into_string()),
                            peers: peers
                                .into_iter()
                                .map(|info| proto::PeerInfo {
                                    peer_id: info.peer_id.map(|id| id.to_bytes()),
                                    // TODO, see https://github.com/libp2p/specs/pull/217
                                    signed_peer_record: None,
                                })
                                .collect(),
                            backoff,
                        }],
                        idontwant: vec![],
                        extensions: None,
                    }),
                    testExtension: None,
                    partial: None,
                }
            }
            RpcOut::IDontWant(IDontWant { message_ids }) => proto::RPC {
                publish: Vec::new(),
                subscriptions: Vec::new(),
                control: Some(proto::ControlMessage {
                    ihave: vec![],
                    iwant: vec![],
                    graft: vec![],
                    prune: vec![],
                    idontwant: vec![proto::ControlIDontWant {
                        message_ids: message_ids.into_iter().map(|msg_id| msg_id.0).collect(),
                    }],
                    extensions: None,
                }),
                testExtension: None,
                partial: None,
            },
            RpcOut::Extensions(Extensions {
                partial_messages,
                test_extension,
            }) => proto::RPC {
                publish: Vec::new(),
                subscriptions: Vec::new(),
                control: Some(proto::ControlMessage {
                    ihave: vec![],
                    iwant: vec![],
                    graft: vec![],
                    prune: vec![],
                    idontwant: vec![],
                    extensions: Some(proto::ControlExtensions {
                        testExtension: test_extension,
                        partialMessages: partial_messages,
                    }),
                }),
                testExtension: None,
                partial: None,
            },
            RpcOut::TestExtension => proto::RPC {
                subscriptions: vec![],
                publish: vec![],
                control: None,
                testExtension: Some(proto::TestExtension {}),
                partial: None,
            },
            RpcOut::PartialMessage {
                topic_id,
                group_id,
                metadata,
                message,
            } => proto::RPC {
                subscriptions: vec![],
                publish: vec![],
                control: None,
                testExtension: None,
                partial: Some(proto::PartialMessagesExtension {
                    topicID: Some(topic_id.as_str().as_bytes().to_vec()),
                    groupID: Some(group_id),
                    partialMessage: message,
                    partsMetadata: Some(metadata),
                }),
            },
        }
    }
}

/// A Gossipsub RPC message received.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RpcIn {
    /// List of messages that were part of this RPC query.
    pub messages: Vec<RawMessage>,
    /// List of subscriptions.
    pub subscriptions: Vec<Subscription>,
    /// List of Gossipsub control messages.
    pub control_msgs: Vec<ControlAction>,
    /// Gossipsub test extension.
    pub test_extension: Option<TestExtension>,
    /// Partial messages extension.
    #[cfg(feature = "partial_messages")]
    pub partial_message: Option<PartialMessage>,
}

impl fmt::Debug for RpcIn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut b = f.debug_struct("GossipsubRpc");
        if !self.messages.is_empty() {
            b.field("messages", &self.messages);
        }
        if !self.subscriptions.is_empty() {
            b.field("subscriptions", &self.subscriptions);
        }
        if !self.control_msgs.is_empty() {
            b.field("control_msgs", &self.control_msgs);
        }
        #[cfg(feature = "partial_messages")]
        b.field("partial_messages", &self.partial_message);

        b.finish()
    }
}

impl PeerKind {
    pub fn as_static_ref(&self) -> &'static str {
        match self {
            Self::NotSupported => "Not Supported",
            Self::Floodsub => "Floodsub",
            Self::Gossipsub => "Gossipsub v1.0",
            Self::Gossipsubv1_1 => "Gossipsub v1.1",
            Self::Gossipsubv1_2 => "Gossipsub v1.2",
            Self::Gossipsubv1_3 => "Gossipsub v1.3",
        }
    }
}

impl AsRef<str> for PeerKind {
    fn as_ref(&self) -> &str {
        self.as_static_ref()
    }
}

impl fmt::Display for PeerKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}
