//! The builder allows for the easy construction of a [`Gossipsub`] struct.
use std::collections::{HashMap, HashSet, VecDeque};

use libp2p_core::{identity::Keypair, PeerId};
use prometheus_client::registry::Registry;
use wasm_timer::Instant;

use super::Gossipsub;
use crate::backoff::BackoffStorage;
use crate::config::GossipsubConfig;
use crate::mcache::MessageCache;
use crate::metrics::{Config as MetricsConfig, Metrics};
use crate::subscription_filter::TopicSubscriptionFilter;
use crate::time_cache::{DuplicateCache, TimeCache};
use crate::transform::DataTransform;
use wasm_timer::Interval;

pub struct GossipsubBuilder<D, F>
where
    D: DataTransform,
    F: TopicSubscriptionFilter,
{
    /// The gossipsub configuration to be used.
    config: GossipsubConfig,
    /// The type of configuration used to publish messages.
    publish_config: PublishConfig,
    /// The type of validation that should be done on messages.
    validation_mode: ValidationMode,
    /// Optionally allow gossipsub metrics to be enabled and run.
    metrics: Option<Metrics>,
    /// If messages require to the data to be transformed, i.e a compression algorithm is required,
    /// this can be added here.
    data_transform: D,
    /// If a filter to the topics peers are able to subscribe to is required. It is set here.
    topic_subscription_filter: F,
}

impl<D, F> GossipsubBuilder<D, F>
where
    D: DataTransform + Default,
    F: TopicSubscriptionFilter + Default,
{
    /// Creates a new builder.
    pub fn new(message_authenticity: MessageAuthenticity) -> Self {
        Self {
            config: GossipsubConfig::default(),
            publish_config: message_authenticity.into(),
            validation_mode: ValidationMode::Strict, // Default validation mode is strict.
            metrics: None,
            data_transform: D::default(),
            topic_subscription_filter: F::default(),
        }
    }

    /// Sets the [`GossipsubConfig`] configuration.
    pub fn config(mut self, config: GossipsubConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets the [`MessageAuthenticity`] for how the Gossipsub router should publish messages.
    pub fn message_authenticity(mut self, message_authenticity: MessageAuthenticity) -> Self {
        self.publish_config = message_authenticity.into();
        self
    }

    /// Sets the [`ValidationMode`] of how the Gossipsub router should validate messages. This must
    /// be set accordingly to the [`MessageAuthenticity`].
    pub fn validation_mode(mut self, validation_mode: ValidationMode) -> Self {
        self.validation_mode = validation_mode;
        self
    }

    /// Enables metrics on the Gossipsub router by passing a [`Registry`] and configuration
    /// ([`MetricsConfig`]).
    pub fn metrics(mut self, registry: &mut Registry, config: MetricsConfig) -> Self {
        self.metrics = Some(Metrics::new(registry, config));
        self
    }

    /// Sets the [`DataTransform`] of the Gossipsub router allowing users to customise any data
    /// transforms of raw messages. This is useful to adding custom compression algorithms for
    /// example.
    pub fn data_transform(mut self, data_transform: D) -> Self {
        self.data_transform = data_transform;
        self
    }

    /// Sets the topic subscription filter. This allows the user to specify how many and which
    /// topics are allowed to be subscribed to by peers on the network. See
    /// [`TopicSubscriptionFilter`] for further details and implementations.
    pub fn topic_subscription_filter(mut self, topic_subscription_filter: F) -> Self {
        self.topic_subscription_filter = topic_subscription_filter;
        self
    }

    /// Consumes the builder and constructs a [`Gossipsub`] router from the configuration.
    pub fn build(self) -> Result<Gossipsub<D, F>, &'static str> {
        // Set up the router given the configuration settings.

        // We do not allow configurations where a published message would also be rejected if it
        // were received locally.
        validate_config(&self.publish_config, &self.validation_mode)?;

        Ok(Gossipsub {
            metrics: self.metrics,
            events: VecDeque::new(),
            control_pool: HashMap::new(),
            publish_config: self.publish_config,
            validation_mode: self.validation_mode,
            duplicate_cache: DuplicateCache::new(self.config.duplicate_cache_time()),
            fast_message_id_cache: TimeCache::new(self.config.duplicate_cache_time()),
            topic_peers: HashMap::new(),
            peer_topics: HashMap::new(),
            explicit_peers: HashSet::new(),
            blacklisted_peers: HashSet::new(),
            mesh: HashMap::new(),
            fanout: HashMap::new(),
            fanout_last_pub: HashMap::new(),
            backoffs: BackoffStorage::new(
                &self.config.prune_backoff(),
                self.config.heartbeat_interval(),
                self.config.backoff_slack(),
            ),
            mcache: MessageCache::new(self.config.history_gossip(), self.config.history_length()),
            heartbeat: Interval::new_at(
                Instant::now() + self.config.heartbeat_initial_delay(),
                self.config.heartbeat_interval(),
            ),
            heartbeat_ticks: 0,
            px_peers: HashSet::new(),
            outbound_peers: HashSet::new(),
            peer_score: None,
            count_received_ihave: HashMap::new(),
            count_sent_iwant: HashMap::new(),
            pending_iwant_msgs: HashSet::new(),
            connected_peers: HashMap::new(),
            published_message_ids: DuplicateCache::new(
                self.config.published_message_ids_cache_time(),
            ),
            config: self.config,
            subscription_filter: self.topic_subscription_filter,
            data_transform: self.data_transform,
        })
    }
}

/// Validates the combination of signing, privacy and message validation to ensure the
/// configuration will not reject published messages.
fn validate_config(
    publish_config: &PublishConfig,
    validation_mode: &ValidationMode,
) -> Result<(), &'static str> {
    match validation_mode {
        ValidationMode::Anonymous => {
            if publish_config.is_signing() {
                return Err("Cannot enable message signing with an Anonymous validation mode. Consider changing either the ValidationMode or MessageAuthenticity");
            }

            if !publish_config.is_anonymous() {
                return Err("Published messages contain an author but incoming messages with an author will be rejected. Consider adjusting the validation or privacy settings in the config");
            }
        }
        ValidationMode::Strict => {
            if !publish_config.is_signing() {
                return Err(
                    "Messages will be
                published unsigned and incoming unsigned messages will be rejected. Consider adjusting
                the validation or privacy settings in the config"
                );
            }
        }
        _ => {}
    }
    Ok(())
}

/// The types of message validation that can be employed by gossipsub.
#[derive(Debug, Clone)]
pub enum ValidationMode {
    /// This is the default setting. This requires the message author to be a valid [`PeerId`] and to
    /// be present as well as the sequence number. All messages must have valid signatures.
    ///
    /// NOTE: This setting will reject messages from nodes using
    /// [`crate::behaviour::MessageAuthenticity::Anonymous`] and all messages that do not have
    /// signatures.
    Strict,
    /// This setting permits messages that have no author, sequence number or signature. If any of
    /// these fields exist in the message these are validated.
    Permissive,
    /// This setting requires the author, sequence number and signature fields of a message to be
    /// empty. Any message that contains these fields is considered invalid.
    Anonymous,
    /// This setting does not check the author, sequence number or signature fields of incoming
    /// messages. If these fields contain data, they are simply ignored.
    ///
    /// NOTE: This setting will consider messages with invalid signatures as valid messages.
    None,
}

/// Determines if published messages should be signed or not.
///
/// Without signing, a number of privacy preserving modes can be selected.
///
/// NOTE: The default validation settings are to require signatures. The [`ValidationMode`]
/// should be updated to allow for unsigned messages.
#[derive(Clone)]
pub enum MessageAuthenticity {
    /// Message signing is enabled. The author will be the owner of the key and the sequence number
    /// will be a random number.
    Signed(Keypair),
    /// Message signing is disabled.
    ///
    /// The specified [`PeerId`] will be used as the author of all published messages. The sequence
    /// number will be randomized.
    Author(PeerId),
    /// Message signing is disabled.
    ///
    /// A random [`PeerId`] will be used when publishing each message. The sequence number will be
    /// randomized.
    RandomAuthor,
    /// Message signing is disabled.
    ///
    /// The author of the message and the sequence numbers are excluded from the message.
    ///
    /// NOTE: Excluding these fields may make these messages invalid by other nodes who
    /// enforce validation of these fields. See [`ValidationMode`] in the [`GossipsubConfig`]
    /// for how to customise this for rust-libp2p gossipsub.  A custom `message_id`
    /// function will need to be set to prevent all messages from a peer being filtered
    /// as duplicates.
    Anonymous,
}

impl MessageAuthenticity {}

/// A data structure for storing configuration for publishing messages. See [`MessageAuthenticity`]
/// for further details.
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum PublishConfig {
    /// Sign the message with a keypair and a specific author.
    Signing {
        keypair: Keypair,
        author: PeerId,
        inline_key: Option<Vec<u8>>,
    },

    /// The author will be published with all messages. Message signing is disabled.
    Author(PeerId),
    /// A random author will be used. Message signing is disabled.
    RandomAuthor,
    /// Message signing  is disabled and the author field is left blank.
    Anonymous,
}

impl PublishConfig {
    pub fn get_own_id(&self) -> Option<&PeerId> {
        match self {
            Self::Signing { author, .. } => Some(author),
            Self::Author(author) => Some(author),
            _ => None,
        }
    }

    /// Returns true if signing is enabled.
    pub fn is_signing(&self) -> bool {
        matches!(self, PublishConfig::Signing { .. })
    }

    pub fn is_anonymous(&self) -> bool {
        matches!(self, PublishConfig::Anonymous)
    }
}

impl From<MessageAuthenticity> for PublishConfig {
    fn from(authenticity: MessageAuthenticity) -> Self {
        match authenticity {
            MessageAuthenticity::Signed(keypair) => {
                let public_key = keypair.public();
                let key_enc = public_key.to_protobuf_encoding();
                let key = if key_enc.len() <= 42 {
                    // The public key can be inlined in [`rpc_proto::Message::from`], so we don't include it
                    // specifically in the [`rpc_proto::Message::key`] field.
                    None
                } else {
                    // Include the protobuf encoding of the public key in the message.
                    Some(key_enc)
                };

                PublishConfig::Signing {
                    keypair,
                    author: public_key.to_peer_id(),
                    inline_key: key,
                }
            }
            MessageAuthenticity::Author(peer_id) => PublishConfig::Author(peer_id),
            MessageAuthenticity::RandomAuthor => PublishConfig::RandomAuthor,
            MessageAuthenticity::Anonymous => PublishConfig::Anonymous,
        }
    }
}
