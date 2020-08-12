//! A collection of types using the Gossipsub system.
use crate::TopicHash;
use libp2p_core::PeerId;
use std::fmt;

/// A type for gossipsub message ids.
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

#[derive(Debug, Clone, PartialEq)]
/// Describes the types of peers that can exist in the gossipsub context.
pub enum PeerKind {
    /// A gossipsub 1.1 peer.
    Gossipsubv1_1,
    /// A gossipsub 1.0 peer.
    Gossipsub,
    /// A floodsub peer.
    Floodsub,
    /// The peer doesn't support any of the protocols.
    NotSupported,
}

/// A message received by the gossipsub system.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct GossipsubMessage {
    /// Id of the peer that published this message.
    pub source: Option<PeerId>,

    /// Content of the message. Its meaning is out of scope of this library.
    pub data: Vec<u8>,

    /// A random sequence number.
    pub sequence_number: Option<u64>,

    /// List of topics this message belongs to.
    ///
    /// Each message can belong to multiple topics at once.
    pub topics: Vec<TopicHash>,

    /// The signature of the message if it's signed.
    pub signature: Option<Vec<u8>>,

    /// The public key of the message if it is signed and the source `PeerId` cannot be inlined.
    pub key: Option<Vec<u8>>,

    /// Flag indicating if this message has been validated by the application or not.
    pub validated: bool,
}

impl fmt::Debug for GossipsubMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GossipsubMessage")
            .field(
                "data",
                &format_args!("{:<20}", &hex_fmt::HexFmt(&self.data)),
            )
            .field("source", &self.source)
            .field("sequence_number", &self.sequence_number)
            .field("topics", &self.topics)
            .finish()
    }
}

/// A subscription received by the gossipsub system.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GossipsubSubscription {
    /// Action to perform.
    pub action: GossipsubSubscriptionAction,
    /// The topic from which to subscribe or unsubscribe.
    pub topic_hash: TopicHash,
}

/// Action that a subscription wants to perform.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GossipsubSubscriptionAction {
    /// The remote wants to subscribe to the given topic.
    Subscribe,
    /// The remote wants to unsubscribe from the given topic.
    Unsubscribe,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerInfo {
    pub peer_id: Option<PeerId>,
    //TODO add this when RFC: Signed Address Records got added to the spec (see pull request
    // https://github.com/libp2p/specs/pull/217)
    //pub signed_peer_record: ?,
}

/// A Control message received by the gossipsub system.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GossipsubControlAction {
    /// Node broadcasts known messages per topic - IHave control message.
    IHave {
        /// The topic of the messages.
        topic_hash: TopicHash,
        /// A list of known message ids (peer_id + sequence _number) as a string.
        message_ids: Vec<MessageId>,
    },
    /// The node requests specific message ids (peer_id + sequence _number) - IWant control message.
    IWant {
        /// A list of known message ids (peer_id + sequence _number) as a string.
        message_ids: Vec<MessageId>,
    },
    /// The node has been added to the mesh - Graft control message.
    Graft {
        /// The mesh topic the peer should be added to.
        topic_hash: TopicHash,
    },
    /// The node has been removed from the mesh - Prune control message.
    Prune {
        /// The mesh topic the peer should be removed from.
        topic_hash: TopicHash,
        /// A list of peers to be proposed to the removed peer as peer exchange
        peers: Vec<PeerInfo>,
        /// The backoff time in seconds before we allow to reconnect
        backoff: Option<u64>,
    },
}

/// An RPC received/sent.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct GossipsubRpc {
    /// List of messages that were part of this RPC query.
    pub messages: Vec<GossipsubMessage>,
    /// List of subscriptions.
    pub subscriptions: Vec<GossipsubSubscription>,
    /// List of Gossipsub control messages.
    pub control_msgs: Vec<GossipsubControlAction>,
}

impl fmt::Debug for GossipsubRpc {
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
        b.finish()
    }
}
