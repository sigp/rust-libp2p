// Copyright 2021 Sigma Prime Pty Ltd.
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

//! This provides a builder for constructing a [`Gossipsub`] behaviour.


pub struct GossipsubBuilder< {
    D: DataTransform,
    F: TopicSubscriptionFilter,
    > {
        /// The [`DataTransform`] to be used (if any). This is for custom message compression/pre-processing
        data_transform: D,
        /// An optional filter for topic subscriptions to prevent other nodes from
        /// spamming/over-subscribing to topics on the network.
        topic_subscription_filter: F,
        /// Determines if published messages should be signed or not. See [`MessageAuthenticity`]
        /// for further details. The default is Anonymous.
        message_authenticity: MessageAuthenticity,
        /// The configuration parameters for the gossipsub protocol. See `[GossipsubConfig`] for
        /// further details.
        config: GossipsubConfig,
        /// Optional metrics registry and configuration to be used when metrics are enabled.
        metrics_data: Option<(&mut Registry, MetricsConfig)>,
        /// Peer scoring parameters if enabled.
        peer_score_params: Option<(PeerScoreParams, PeerScoreThresholds, Option<fn(&PeerId, &TopicHash, f64)>)>,
    }
}

impl<D: DataTransform, F: TopicSubscriptionFilter> Default for GossipsubBuilder<D,F> {
    fn default() -> GossipsubBuilder {
        data_transform: IdentityTransform,
        topic_subscription_filter: AllowAllSubscriptionFilter,
        message_authenticity: MessageAuthenticity::Anonymous,
        config: GossipsubConfig::default(),
        metrics_data: None,
        peer_score_params: None,
    }
}

impl<D: DataTransform, F: TopicSubscriptionFilter> GossipsubBuilder<D,F> {

    /// Sets the [`DataTransform`] which can pre-process messages before sending to the
    /// application. This is useful to employ compression for example. The default setting is
    /// the [`IdentityTransform`] which does not transform the data.
    pub fn data_transform(&mut self, data_transform: D) -> &mut Self {
        self.data_transform = data_transform;
        self
    }

    /// Sets the [`TopicSubscriptionFilter`] of gossipsub which can prevent nodes spamming
    /// topic subscriptions on the network. The default is
    /// [`AllowAllSubscriptionFilter`].
    pub fn topic_subscription_filter(&mut self, topic_subscription_filter: F) -> &mut Self {
        self.topic_subscription_filter: topic_subscription_filter;
        self
    }

    /// Sets the [`MessageAuthenticity`] for published and received messages. The default value
    /// is [`MessageAuthenticity::Anonymous`].
    pub fn message_authenticity(&mut self, message_authenticity: MessageAuthenticity) -> &mut Self {
        self.message_authenticity = message_authenticity;
        self
    }

    /// Sets the gossipsub configuration parameters. The defaults are used if this is not
    /// explicitly set.
    pub fn config(&mut self, config: GossipsubConfig) -> &mut Self {
        self.config = config;
        self
    }

    /// Enables internal gossipsub metrics. This requires a mutable reference to a metrics [`Registry`] as well as the set of metric configuration paramters, [`MetricsConfig`].
    pub fn with_metrics(&mut self, registry: &mut Registry, config: MetricsConfig) -> &mut Self {
        self.metrics_data = Some((registry, config));
        self
    }

    /// Enables Gossipsub v1.1 peer scoring. This requires a set of peer scoring parameters,
    /// [`PeerScoreParams`], along with thresholds [`PeerScoreThresholds`] and optionally a
    /// callback specify the time it took to for a message to be delivered (primarily for
    /// debugging at this point). 
    pub fn with_scoring(&mut self, params: PeerScoreParams, thresholds: PeerScoreThresholds, callback: Option<fn(&PeerId, &TopicHash, f64)>) -> &mut Self {
        self.peer_score_params = Some((params, thresholds, callback));
        self
    }

    /// Attempts to build the [`Gossipsub`] behaviour from the configurations specified. 
    pub fn build(self) -> Result<Gossipsub, &'static str> { 

        // We do not allow configurations where a published message would also be rejected if it
        // were received locally.
        validate_config(&self.message_authenticity, config.validation_mode())?;

        Ok(Gossipsub {
            metrics: metrics.map(|(registry, cfg)| Metrics::new(registry, cfg)),
            events: VecDeque::new(),
            control_pool: HashMap::new(),
            publish_config: privacy.into(),
            duplicate_cache: DuplicateCache::new(config.duplicate_cache_time()),
            fast_messsage_id_cache: TimeCache::new(config.duplicate_cache_time()),
            topic_peers: HashMap::new(),
            peer_topics: HashMap::new(),
            explicit_peers: HashSet::new(),
            blacklisted_peers: HashSet::new(),
            mesh: HashMap::new(),
            fanout: HashMap::new(),
            fanout_last_pub: HashMap::new(),
            backoffs: BackoffStorage::new(
                &config.prune_backoff(),
                config.heartbeat_interval(),
                config.backoff_slack(),
            ),
            mcache: MessageCache::new(config.history_gossip(), config.history_length()),
            heartbeat: Interval::new_initial(
                config.heartbeat_initial_delay(),
                config.heartbeat_interval(),
            ),
            heartbeat_ticks: 0,
            px_peers: HashSet::new(),
            outbound_peers: HashSet::new(),
            peer_score: None,
            count_received_ihave: HashMap::new(),
            count_sent_iwant: HashMap::new(),
            connected_peers: HashMap::new(),
            published_message_ids: DuplicateCache::new(config.published_message_ids_cache_time()),
            config,
            subscription_filter,
            data_transform,
        })
    }





    }




/// Validates the combination of signing, privacy and message validation to ensure the
/// configuration will not reject published messages.
fn validate_config(
    authenticity: &MessageAuthenticity,
    validation_mode: &ValidationMode,
) -> Result<(), &'static str> {
    match validation_mode {
        ValidationMode::Anonymous => {
            if authenticity.is_signing() {
                return Err("Cannot enable message signing with an Anonymous validation mode. Consider changing either the ValidationMode or MessageAuthenticity");
            }

            if !authenticity.is_anonymous() {
                return Err("Published messages contain an author but incoming messages with an author will be rejected. Consider adjusting the validation or privacy settings in the config");
            }
        }
        ValidationMode::Strict => {
            if !authenticity.is_signing() {
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




/// Determines if published messages should be signed or not.
///
/// Without signing, a number of privacy preserving modes can be selected.
///
/// NOTE: The default validation settings are to require signatures. The [`ValidationMode`]
/// should be updated in the [`GossipsubConfig`] to allow for unsigned messages.
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

impl MessageAuthenticity {
    /// Returns true if signing is enabled.
    pub fn is_signing(&self) -> bool {
        matches!(self, MessageAuthenticity::Signed(_))
    }

    pub fn is_anonymous(&self) -> bool {
        matches!(self, MessageAuthenticity::Anonymous)
    }
}

