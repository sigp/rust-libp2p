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

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use libp2p_core::PeerId;

use crate::types::{FastMessageId, GossipsubMessage, MessageId, RawGossipsubMessage};
use crate::{ChokingStrategy, DefaultStrat};

/// Selector for custom Protocol Id
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GossipsubVersion {
    /// Gossipsub v1.0
    V1_0,
    /// Gossipsub v1.1 (peer scoring)
    V1_1,
    /// Gossipsub v1.2 (episub)
    V1_2,
}

/// Configuration parameters that define the performance of the gossipsub network.
#[derive(Clone)]
pub struct GossipsubConfig<S: ChokingStrategy = DefaultStrat> {
    protocol_id: Cow<'static, str>,
    custom_id_version: Option<GossipsubVersion>,
    history_length: usize,
    history_gossip: usize,
    mesh_n: usize,
    mesh_n_low: usize,
    mesh_n_high: usize,
    episub_heartbeat_ticks: u64,
    choking_strategy: S,
    retain_scores: usize,
    gossip_lazy: usize,
    gossip_factor: f64,
    heartbeat_initial_delay: Duration,
    heartbeat_interval: Duration,
    fanout_ttl: Duration,
    check_explicit_peers_ticks: u64,
    max_transmit_size: usize,
    idle_timeout: Duration,
    duplicate_cache_time: Duration,
    validate_messages: bool,
    message_id_fn: Arc<dyn Fn(&GossipsubMessage) -> MessageId + Send + Sync + 'static>,
    fast_message_id_fn:
        Option<Arc<dyn Fn(&RawGossipsubMessage) -> FastMessageId + Send + Sync + 'static>>,
    allow_self_origin: bool,
    do_px: bool,
    prune_peers: usize,
    prune_backoff: Duration,
    unsubscribe_backoff: Duration,
    backoff_slack: u32,
    flood_publish: bool,
    graft_flood_threshold: Duration,
    mesh_outbound_min: usize,
    opportunistic_graft_ticks: u64,
    opportunistic_graft_peers: usize,
    gossip_retransimission: u32,
    max_messages_per_rpc: Option<usize>,
    max_ihave_length: usize,
    max_ihave_messages: usize,
    iwant_followup_time: Duration,
    support_floodsub: bool,
    disable_episub: bool,
    published_message_ids_cache_time: Duration,
}

impl<S: ChokingStrategy> GossipsubConfig<S> {
    // All the getters

    /// The protocol id to negotiate this protocol. By default, the resulting protocol id has the form
    /// `/<prefix>/<supported-versions>`, but can optionally be changed to a literal form by providing some GossipsubVersion as custom_id_version.
    /// As gossipsub supports version 1.0 and 1.1, there are two suffixes supported for the resulting protocol id.
    ///
    /// Calling `GossipsubConfigBuilder::protocol_id_prefix` will set a new prefix and retain the prefix logic.
    /// Calling `GossipsubConfigBuilder::protocol_id` will set a custom `protocol_id` and disable the prefix logic.
    ///
    /// The default prefix is `meshsub`, giving the supported protocol ids: `/meshsub/1.1.0` and `/meshsub/1.0.0`, negotiated in that order.
    pub fn protocol_id(&self) -> &Cow<'static, str> {
        &self.protocol_id
    }

    pub fn custom_id_version(&self) -> &Option<GossipsubVersion> {
        &self.custom_id_version
    }

    // Overlay network parameters.
    /// Number of heartbeats to keep in the `memcache` (default is 5).
    pub fn history_length(&self) -> usize {
        self.history_length
    }

    /// Number of past heartbeats to gossip about (default is 3).
    pub fn history_gossip(&self) -> usize {
        self.history_gossip
    }

    /// Target number of peers for the mesh network (D in the spec, default is 6).
    pub fn mesh_n(&self) -> usize {
        self.mesh_n
    }

    /// Minimum number of peers in mesh network before adding more (D_lo in the spec, default is 5).
    pub fn mesh_n_low(&self) -> usize {
        self.mesh_n_low
    }

    /// Maximum number of peers in mesh network before removing some (D_high in the spec, default
    /// is 12).
    pub fn mesh_n_high(&self) -> usize {
        self.mesh_n_high
    }

    /// The number of heartbeats required to form a `episub_heartbeat_interval`. This interval
    /// determines the frequency peers are choked/unchoked. The default value is 20.
    pub fn episub_heartbeat_ticks(&self) -> u64 {
        self.episub_heartbeat_ticks
    }

    /// The [`ChokingStrategy`] used to decide how to choke/unchoke peers. The default is to use
    /// [`DefaultStrat`] with default values.
    pub fn choking_strategy(&self) -> &S {
        &self.choking_strategy
    }

    /// Affects how peers are selected when pruning a mesh due to over subscription.
    ///
    ///  At least `retain_scores` of the retained peers will be high-scoring, while the remainder are
    ///  chosen randomly (D_score in the spec, default is 4).
    pub fn retain_scores(&self) -> usize {
        self.retain_scores
    }

    /// Minimum number of peers to emit gossip to during a heartbeat (D_lazy in the spec,
    /// default is 6).
    pub fn gossip_lazy(&self) -> usize {
        self.gossip_lazy
    }

    /// Affects how many peers we will emit gossip to at each heartbeat.
    ///
    /// We will send gossip to `gossip_factor * (total number of non-mesh peers)`, or
    /// `gossip_lazy`, whichever is greater. The default is 0.25.
    pub fn gossip_factor(&self) -> f64 {
        self.gossip_factor
    }

    /// Initial delay in each heartbeat (default is 5 seconds).
    pub fn heartbeat_initial_delay(&self) -> Duration {
        self.heartbeat_initial_delay
    }

    /// Time between each heartbeat (default is 1 second).
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// Time to live for fanout peers (default is 60 seconds).
    pub fn fanout_ttl(&self) -> Duration {
        self.fanout_ttl
    }

    /// The number of heartbeat ticks until we recheck the connection to explicit peers and
    /// reconnecting if necessary (default 300).
    pub fn check_explicit_peers_ticks(&self) -> u64 {
        self.check_explicit_peers_ticks
    }

    /// The maximum byte size for each gossipsub RPC (default is 65536 bytes).
    ///
    /// This represents the maximum size of the entire protobuf payload. It must be at least
    /// large enough to support basic control messages. If Peer eXchange is enabled, this
    /// must be large enough to transmit the desired peer information on pruning. It must be at
    /// least 100 bytes. Default is 65536 bytes.
    pub fn max_transmit_size(&self) -> usize {
        self.max_transmit_size
    }

    /// The time a connection is maintained to a peer without being in the mesh and without
    /// send/receiving a message from. Connections that idle beyond this timeout are disconnected.
    /// Default is 120 seconds.
    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    /// Duplicates are prevented by storing message id's of known messages in an LRU time cache.
    /// This settings sets the time period that messages are stored in the cache. Duplicates can be
    /// received if duplicate messages are sent at a time greater than this setting apart. The
    /// default is 1 minute.
    pub fn duplicate_cache_time(&self) -> Duration {
        self.duplicate_cache_time
    }

    /// When set to `true`, prevents automatic forwarding of all received messages. This setting
    /// allows a user to validate the messages before propagating them to their peers. If set to
    /// true, the user must manually call [`crate::Gossipsub::report_message_validation_result()`]
    /// on the behaviour to forward message once validated (default is `false`).
    /// The default is `false`.
    pub fn validate_messages(&self) -> bool {
        self.validate_messages
    }

    /// A user-defined function allowing the user to specify the message id of a gossipsub message.
    /// The default value is to concatenate the source peer id with a sequence number. Setting this
    /// parameter allows the user to address packets arbitrarily. One example is content based
    /// addressing, where this function may be set to `hash(message)`. This would prevent messages
    /// of the same content from being duplicated.
    ///
    /// The function takes a [`GossipsubMessage`] as input and outputs a String to be interpreted as
    /// the message id.
    pub fn message_id(&self, message: &GossipsubMessage) -> MessageId {
        (self.message_id_fn)(message)
    }

    /// A user-defined optional function that computes fast ids from raw messages. This can be used
    /// to avoid possibly expensive transformations from [`RawGossipsubMessage`] to
    /// [`GossipsubMessage`] for duplicates. Two semantically different messages must always
    /// have different fast message ids, but it is allowed that two semantically identical messages
    /// have different fast message ids as long as the message_id_fn produces the same id for them.
    ///
    /// The function takes a [`RawGossipsubMessage`] as input and outputs a String to be
    /// interpreted as the fast message id. Default is None.
    pub fn fast_message_id(&self, message: &RawGossipsubMessage) -> Option<FastMessageId> {
        self.fast_message_id_fn
            .as_ref()
            .map(|fast_message_id_fn| fast_message_id_fn(message))
    }

    /// By default, gossipsub will reject messages that are sent to us that have the same message
    /// source as we have specified locally. Enabling this, allows these messages and prevents
    /// penalizing the peer that sent us the message. Default is false.
    pub fn allow_self_origin(&self) -> bool {
        self.allow_self_origin
    }

    /// Whether Peer eXchange is enabled; this should be enabled in bootstrappers and other well
    /// connected/trusted nodes. The default is false.
    ///
    /// Note: Peer exchange is not implemented today, see
    /// https://github.com/libp2p/rust-libp2p/issues/2398.
    pub fn do_px(&self) -> bool {
        self.do_px
    }

    /// Controls the number of peers to include in prune Peer eXchange.
    /// When we prune a peer that's eligible for PX (has a good score, etc), we will try to
    /// send them signed peer records for up to `prune_peers` other peers that we
    /// know of. It is recommended that this value is larger than `mesh_n_high` so that the pruned
    /// peer can reliably form a full mesh. The default is typically 16 however until signed
    /// records are spec'd this is disabled and set to 0.
    pub fn prune_peers(&self) -> usize {
        self.prune_peers
    }

    /// Controls the backoff time for pruned peers. This is how long
    /// a peer must wait before attempting to graft into our mesh again after being pruned.
    /// When pruning a peer, we send them our value of `prune_backoff` so they know
    /// the minimum time to wait. Peers running older versions may not send a backoff time,
    /// so if we receive a prune message without one, we will wait at least `prune_backoff`
    /// before attempting to re-graft. The default is one minute.
    pub fn prune_backoff(&self) -> Duration {
        self.prune_backoff
    }

    /// Controls the backoff time when unsubscribing from a topic.
    ///
    /// This is how long to wait before resubscribing to the topic. A short backoff period in case
    /// of an unsubscribe event allows reaching a healthy mesh in a more timely manner. The default
    /// is 10 seconds.
    pub fn unsubscribe_backoff(&self) -> Duration {
        self.unsubscribe_backoff
    }

    /// Number of heartbeat slots considered as slack for backoffs. This gurantees that we wait
    /// at least backoff_slack heartbeats after a backoff is over before we try to graft. This
    /// solves problems occuring through high latencies. In particular if
    /// `backoff_slack * heartbeat_interval` is longer than any latencies between processing
    /// prunes on our side and processing prunes on the receiving side this guarantees that we
    /// get not punished for too early grafting. The default is 1.
    pub fn backoff_slack(&self) -> u32 {
        self.backoff_slack
    }

    /// Whether to do flood publishing or not. If enabled newly created messages will always be
    /// sent to all peers that are subscribed to the topic and have a good enough score.
    /// The default is true.
    pub fn flood_publish(&self) -> bool {
        self.flood_publish
    }

    /// If a GRAFT comes before `graft_flood_threshold` has elapsed since the last PRUNE,
    /// then there is an extra score penalty applied to the peer through P7.
    pub fn graft_flood_threshold(&self) -> Duration {
        self.graft_flood_threshold
    }

    /// Minimum number of outbound peers in the mesh network before adding more (D_out in the spec).
    /// This value must be smaller or equal than `mesh_n / 2` and smaller than `mesh_n_low`.
    /// The default is 2.
    pub fn mesh_outbound_min(&self) -> usize {
        self.mesh_outbound_min
    }

    /// Number of heartbeat ticks that specifcy the interval in which opportunistic grafting is
    /// applied. Every `opportunistic_graft_ticks` we will attempt to select some high-scoring mesh
    /// peers to replace lower-scoring ones, if the median score of our mesh peers falls below a
    /// threshold (see https://godoc.org/github.com/libp2p/go-libp2p-pubsub#PeerScoreThresholds).
    /// The default is 60.
    pub fn opportunistic_graft_ticks(&self) -> u64 {
        self.opportunistic_graft_ticks
    }

    /// Controls how many times we will allow a peer to request the same message id through IWANT
    /// gossip before we start ignoring them. This is designed to prevent peers from spamming us
    /// with requests and wasting our resources. The default is 3.
    pub fn gossip_retransimission(&self) -> u32 {
        self.gossip_retransimission
    }

    /// The maximum number of new peers to graft to during opportunistic grafting. The default is 2.
    pub fn opportunistic_graft_peers(&self) -> usize {
        self.opportunistic_graft_peers
    }

    /// The maximum number of messages we will process in a given RPC. If this is unset, there is
    /// no limit. The default is None.
    pub fn max_messages_per_rpc(&self) -> Option<usize> {
        self.max_messages_per_rpc
    }

    /// The maximum number of messages to include in an IHAVE message.
    /// Also controls the maximum number of IHAVE ids we will accept and request with IWANT from a
    /// peer within a heartbeat, to protect from IHAVE floods. You should adjust this value from the
    /// default if your system is pushing more than 5000 messages in GossipSubHistoryGossip
    /// heartbeats; with the defaults this is 1666 messages/s. The default is 5000.
    pub fn max_ihave_length(&self) -> usize {
        self.max_ihave_length
    }

    /// GossipSubMaxIHaveMessages is the maximum number of IHAVE messages to accept from a peer
    /// within a heartbeat.
    pub fn max_ihave_messages(&self) -> usize {
        self.max_ihave_messages
    }

    /// Time to wait for a message requested through IWANT following an IHAVE advertisement.
    /// If the message is not received within this window, a broken promise is declared and
    /// the router may apply behavioural penalties. The default is 3 seconds.
    pub fn iwant_followup_time(&self) -> Duration {
        self.iwant_followup_time
    }

    /// Enable support for flooodsub peers. Default false.
    pub fn support_floodsub(&self) -> bool {
        self.support_floodsub
    }

    /// Disables choking other peers and mesh additions. Gossipsub will still support other episub
    /// nodes and respond to their choking requests, but will not choke its peers.
    pub fn disable_episub(&self) -> bool {
        self.disable_episub
    }

    /// Published message ids time cache duration. The default is 10 seconds.
    pub fn published_message_ids_cache_time(&self) -> Duration {
        self.published_message_ids_cache_time
    }
}

impl<S: ChokingStrategy + Default> Default for GossipsubConfig<S> {
    fn default() -> Self {
        // use ConfigBuilder to also validate defaults
        GossipsubConfigBuilder::default()
            .build()
            .expect("Default config parameters should be valid parameters")
    }
}

/// The builder struct for constructing a gossipsub configuration.
pub struct GossipsubConfigBuilder<S: ChokingStrategy + Default> {
    config: GossipsubConfig<S>,
}

impl<S: ChokingStrategy + Default> Default for GossipsubConfigBuilder<S> {
    fn default() -> Self {
        GossipsubConfigBuilder {
            config: GossipsubConfig {
                protocol_id: Cow::Borrowed("meshsub"),
                custom_id_version: None,
                history_length: 5,
                history_gossip: 3,
                mesh_n: 6,
                mesh_n_low: 5,
                mesh_n_high: 12,
                episub_heartbeat_ticks: 20,
                choking_strategy: S::default(),
                retain_scores: 4,
                gossip_lazy: 6, // default to mesh_n
                gossip_factor: 0.25,
                heartbeat_initial_delay: Duration::from_secs(5),
                heartbeat_interval: Duration::from_secs(1),
                fanout_ttl: Duration::from_secs(60),
                check_explicit_peers_ticks: 300,
                max_transmit_size: 65536,
                idle_timeout: Duration::from_secs(120),
                duplicate_cache_time: Duration::from_secs(60),
                validate_messages: false,
                message_id_fn: Arc::new(|message| {
                    // default message id is: source + sequence number
                    // NOTE: If either the peer_id or source is not provided, we set to 0;
                    let mut source_string = if let Some(peer_id) = message.source.as_ref() {
                        peer_id.to_base58()
                    } else {
                        PeerId::from_bytes(&[0, 1, 0])
                            .expect("Valid peer id")
                            .to_base58()
                    };
                    source_string
                        .push_str(&message.sequence_number.unwrap_or_default().to_string());
                    MessageId::from(source_string)
                }),
                fast_message_id_fn: None,
                allow_self_origin: false,
                do_px: false,
                prune_peers: 0, // NOTE: Increasing this currently has little effect until Signed records are implemented.
                prune_backoff: Duration::from_secs(60),
                unsubscribe_backoff: Duration::from_secs(10),
                backoff_slack: 1,
                flood_publish: true,
                graft_flood_threshold: Duration::from_secs(10),
                mesh_outbound_min: 2,
                opportunistic_graft_ticks: 60,
                opportunistic_graft_peers: 2,
                gossip_retransimission: 3,
                max_messages_per_rpc: None,
                max_ihave_length: 5000,
                max_ihave_messages: 10,
                iwant_followup_time: Duration::from_secs(3),
                support_floodsub: false,
                disable_episub: false,
                published_message_ids_cache_time: Duration::from_secs(10),
            },
        }
    }
}

impl<S: ChokingStrategy + Default> From<GossipsubConfig<S>> for GossipsubConfigBuilder<S> {
    fn from(config: GossipsubConfig<S>) -> Self {
        GossipsubConfigBuilder { config }
    }
}

impl<S: ChokingStrategy + Default> GossipsubConfigBuilder<S> {
    /// The protocol id prefix to negotiate this protocol (default is `/meshsub/1.0.0`).
    pub fn protocol_id_prefix(mut self, protocol_id_prefix: impl Into<Cow<'static, str>>) -> Self {
        self.config.custom_id_version = None;
        self.config.protocol_id = protocol_id_prefix.into();
        self
    }

    /// The full protocol id to negotiate this protocol (does not append `/1.0.0` or `/1.1.0`).
    pub fn protocol_id(
        mut self,
        protocol_id: impl Into<Cow<'static, str>>,
        custom_id_version: GossipsubVersion,
    ) -> Self {
        self.config.custom_id_version = Some(custom_id_version);
        self.config.protocol_id = protocol_id.into();
        self
    }

    /// Number of heartbeats to keep in the `memcache` (default is 5).
    pub fn history_length(mut self, history_length: usize) -> Self {
        self.config.history_length = history_length;
        self
    }

    /// Number of past heartbeats to gossip about (default is 3).
    pub fn history_gossip(mut self, history_gossip: usize) -> Self {
        self.config.history_gossip = history_gossip;
        self
    }

    /// Target number of peers for the mesh network (D in the spec, default is 6).
    pub fn mesh_n(mut self, mesh_n: usize) -> Self {
        self.config.mesh_n = mesh_n;
        self
    }

    /// Minimum number of peers in mesh network before adding more (D_lo in the spec, default is 4).
    pub fn mesh_n_low(mut self, mesh_n_low: usize) -> Self {
        self.config.mesh_n_low = mesh_n_low;
        self
    }

    /// Maximum number of peers in mesh network before removing some (D_high in the spec, default
    /// is 12).
    pub fn mesh_n_high(mut self, mesh_n_high: usize) -> Self {
        self.config.mesh_n_high = mesh_n_high;
        self
    }

    /// The number of heartbeats required to form a `episub_heartbeat_interval`. This interval
    /// determines the frequency peers are choked/unchoked. Default value is 20.
    pub fn episub_heartbeat_ticks(mut self, episub_heartbeat_ticks: u64) -> Self {
        self.config.episub_heartbeat_ticks = episub_heartbeat_ticks;
        self
    }

    /// The [`ChokingStrategy`] to use for choking/unchoking peers.
    pub fn choking_strategy(mut self, choking_strategy: S) -> Self {
        self.config.choking_strategy = choking_strategy;
        self
    }

    /// Affects how peers are selected when pruning a mesh due to over subscription.
    ///
    /// At least [`Self::retain_scores`] of the retained peers will be high-scoring, while the remainder are
    /// chosen randomly (D_score in the spec, default is 4).
    pub fn retain_scores(mut self, retain_scores: usize) -> Self {
        self.config.retain_scores = retain_scores;
        self
    }

    /// Minimum number of peers to emit gossip to during a heartbeat (D_lazy in the spec,
    /// default is 6).
    pub fn gossip_lazy(mut self, gossip_lazy: usize) -> Self {
        self.config.gossip_lazy = gossip_lazy;
        self
    }

    /// Affects how many peers we will emit gossip to at each heartbeat.
    ///
    /// We will send gossip to `gossip_factor * (total number of non-mesh peers)`, or
    /// `gossip_lazy`, whichever is greater. The default is 0.25.
    pub fn gossip_factor(mut self, gossip_factor: f64) -> Self {
        self.config.gossip_factor = gossip_factor;
        self
    }

    /// Initial delay in each heartbeat (default is 5 seconds).
    pub fn heartbeat_initial_delay(mut self, heartbeat_initial_delay: Duration) -> Self {
        self.config.heartbeat_initial_delay = heartbeat_initial_delay;
        self
    }

    /// Time between each heartbeat (default is 1 second).
    pub fn heartbeat_interval(mut self, heartbeat_interval: Duration) -> Self {
        self.config.heartbeat_interval = heartbeat_interval;
        self
    }

    /// The number of heartbeat ticks until we recheck the connection to explicit peers and
    /// reconnecting if necessary (default 300).
    pub fn check_explicit_peers_ticks(mut self, check_explicit_peers_ticks: u64) -> Self {
        self.config.check_explicit_peers_ticks = check_explicit_peers_ticks;
        self
    }

    /// Time to live for fanout peers (default is 60 seconds).
    pub fn fanout_ttl(mut self, fanout_ttl: Duration) -> Self {
        self.config.fanout_ttl = fanout_ttl;
        self
    }

    /// The maximum byte size for each gossip (default is 2048 bytes).
    pub fn max_transmit_size(mut self, max_transmit_size: usize) -> Self {
        self.config.max_transmit_size = max_transmit_size;
        self
    }

    /// The time a connection is maintained to a peer without being in the mesh and without
    /// send/receiving a message from. Connections that idle beyond this timeout are disconnected.
    /// Default is 120 seconds.
    pub fn idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.config.idle_timeout = idle_timeout;
        self
    }

    /// Duplicates are prevented by storing message id's of known messages in an LRU time cache.
    /// This settings sets the time period that messages are stored in the cache. Duplicates can be
    /// received if duplicate messages are sent at a time greater than this setting apart. The
    /// default is 1 minute.
    pub fn duplicate_cache_time(mut self, cache_size: Duration) -> Self {
        self.config.duplicate_cache_time = cache_size;
        self
    }

    /// When set, prevents automatic forwarding of all received messages. This setting
    /// allows a user to validate the messages before propagating them to their peers. If set,
    /// the user must manually call [`crate::Gossipsub::report_message_validation_result()`] on the
    /// behaviour to forward a message once validated.
    pub fn validate_messages(mut self) -> Self {
        self.config.validate_messages = true;
        self
    }

    /// A user-defined function allowing the user to specify the message id of a gossipsub message.
    /// The default value is to concatenate the source peer id with a sequence number. Setting this
    /// parameter allows the user to address packets arbitrarily. One example is content based
    /// addressing, where this function may be set to `hash(message)`. This would prevent messages
    /// of the same content from being duplicated.
    ///
    /// The function takes a [`GossipsubMessage`] as input and outputs a String to be
    /// interpreted as the message id.
    pub fn message_id_fn<F>(mut self, id_fn: F) -> Self
    where
        F: Fn(&GossipsubMessage) -> MessageId + Send + Sync + 'static,
    {
        self.config.message_id_fn = Arc::new(id_fn);
        self
    }

    /// A user-defined optional function that computes fast ids from raw messages. This can be used
    /// to avoid possibly expensive transformations from [`RawGossipsubMessage`] to
    /// [`GossipsubMessage`] for duplicates. Two semantically different messages must always
    /// have different fast message ids, but it is allowed that two semantically identical messages
    /// have different fast message ids as long as the message_id_fn produces the same id for them.
    ///
    /// The function takes a [`RawGossipsubMessage`] as input and outputs a String to be interpreted
    /// as the fast message id. Default is None.
    pub fn fast_message_id_fn<F>(mut self, fast_id_fn: F) -> Self
    where
        F: Fn(&RawGossipsubMessage) -> FastMessageId + Send + Sync + 'static,
    {
        self.config.fast_message_id_fn = Some(Arc::new(fast_id_fn));
        self
    }

    /// Enables Peer eXchange. This should be enabled in bootstrappers and other well
    /// connected/trusted nodes. The default is false.
    ///
    /// Note: Peer exchange is not implemented today, see
    /// https://github.com/libp2p/rust-libp2p/issues/2398.
    pub fn do_px(mut self) -> Self {
        self.config.do_px = true;
        self
    }

    /// Controls the number of peers to include in prune Peer eXchange.
    ///
    /// When we prune a peer that's eligible for PX (has a good score, etc), we will try to
    /// send them signed peer records for up to [`Self::prune_peers] other peers that we
    /// know of. It is recommended that this value is larger than [`Self::mesh_n_high`] so that the
    /// pruned peer can reliably form a full mesh. The default is 16.
    pub fn prune_peers(mut self, prune_peers: usize) -> Self {
        self.config.prune_peers = prune_peers;
        self
    }

    /// Controls the backoff time for pruned peers. This is how long
    /// a peer must wait before attempting to graft into our mesh again after being pruned.
    /// When pruning a peer, we send them our value of [`Self::prune_backoff`] so they know
    /// the minimum time to wait. Peers running older versions may not send a backoff time,
    /// so if we receive a prune message without one, we will wait at least [`Self::prune_backoff`]
    /// before attempting to re-graft. The default is one minute.
    pub fn prune_backoff(mut self, prune_backoff: Duration) -> Self {
        self.config.prune_backoff = prune_backoff;
        self
    }

    /// Controls the backoff time when unsubscribing from a topic.
    ///
    /// This is how long to wait before resubscribing to the topic. A short backoff period in case
    /// of an unsubscribe event allows reaching a healthy mesh in a more timely manner. The default
    /// is 10 seconds.
    pub fn unsubscribe_backoff(mut self, unsubscribe_backoff: u64) -> Self {
        self.config.unsubscribe_backoff = Duration::from_secs(unsubscribe_backoff);
        self
    }

    /// Number of heartbeat slots considered as slack for backoffs. This gurantees that we wait
    /// at least backoff_slack heartbeats after a backoff is over before we try to graft. This
    /// solves problems occuring through high latencies. In particular if
    /// `backoff_slack * heartbeat_interval` is longer than any latencies between processing
    /// prunes on our side and processing prunes on the receiving side this guarantees that we
    /// get not punished for too early grafting. The default is 1.
    pub fn backoff_slack(mut self, backoff_slack: u32) -> Self {
        self.config.backoff_slack = backoff_slack;
        self
    }

    /// Whether to do flood publishing or not. If enabled newly created messages will always be
    /// sent to all peers that are subscribed to the topic and have a good enough score.
    /// The default is true.
    pub fn flood_publish(mut self, flood_publish: bool) -> Self {
        self.config.flood_publish = flood_publish;
        self
    }

    /// If a GRAFT comes before `graft_flood_threshold` has elapsed since the last PRUNE,
    /// then there is an extra score penalty applied to the peer through P7.
    pub fn graft_flood_threshold(mut self, graft_flood_threshold: Duration) -> Self {
        self.config.graft_flood_threshold = graft_flood_threshold;
        self
    }

    /// Minimum number of outbound peers in the mesh network before adding more (D_out in the spec).
    /// This value must be smaller or equal than `mesh_n / 2` and smaller than `mesh_n_low`.
    /// The default is 2.
    pub fn mesh_outbound_min(mut self, mesh_outbound_min: usize) -> Self {
        self.config.mesh_outbound_min = mesh_outbound_min;
        self
    }

    /// Number of heartbeat ticks that specifcy the interval in which opportunistic grafting is
    /// applied. Every `opportunistic_graft_ticks` we will attempt to select some high-scoring mesh
    /// peers to replace lower-scoring ones, if the median score of our mesh peers falls below a
    /// threshold (see https://godoc.org/github.com/libp2p/go-libp2p-pubsub#PeerScoreThresholds).
    /// The default is 60.
    pub fn opportunistic_graft_ticks(mut self, opportunistic_graft_ticks: u64) -> Self {
        self.config.opportunistic_graft_ticks = opportunistic_graft_ticks;
        self
    }

    /// Controls how many times we will allow a peer to request the same message id through IWANT
    /// gossip before we start ignoring them. This is designed to prevent peers from spamming us
    /// with requests and wasting our resources.
    pub fn gossip_retransimission(mut self, gossip_retransimission: u32) -> Self {
        self.config.gossip_retransimission = gossip_retransimission;
        self
    }

    /// The maximum number of new peers to graft to during opportunistic grafting. The default is 2.
    pub fn opportunistic_graft_peers(mut self, opportunistic_graft_peers: usize) -> Self {
        self.config.opportunistic_graft_peers = opportunistic_graft_peers;
        self
    }

    /// The maximum number of messages we will process in a given RPC. If this is unset, there is
    /// no limit. The default is None.
    pub fn max_messages_per_rpc(mut self, max: Option<usize>) -> Self {
        self.config.max_messages_per_rpc = max;
        self
    }

    /// The maximum number of messages to include in an IHAVE message.
    /// Also controls the maximum number of IHAVE ids we will accept and request with IWANT from a
    /// peer within a heartbeat, to protect from IHAVE floods. You should adjust this value from the
    /// default if your system is pushing more than 5000 messages in GossipSubHistoryGossip
    /// heartbeats; with the defaults this is 1666 messages/s. The default is 5000.
    pub fn max_ihave_length(mut self, max_ihave_length: usize) -> Self {
        self.config.max_ihave_length = max_ihave_length;
        self
    }

    /// GossipSubMaxIHaveMessages is the maximum number of IHAVE messages to accept from a peer
    /// within a heartbeat.
    pub fn max_ihave_messages(mut self, max_ihave_messages: usize) -> Self {
        self.config.max_ihave_messages = max_ihave_messages;
        self
    }

    /// By default, gossipsub will reject messages that are sent to us that has the same message
    /// source as we have specified locally. Enabling this, allows these messages and prevents
    /// penalizing the peer that sent us the message. Default is false.
    pub fn allow_self_origin(mut self, allow_self_origin: bool) -> Self {
        self.config.allow_self_origin = allow_self_origin;
        self
    }

    /// Time to wait for a message requested through IWANT following an IHAVE advertisement.
    /// If the message is not received within this window, a broken promise is declared and
    /// the router may apply behavioural penalties. The default is 3 seconds.
    pub fn iwant_followup_time(mut self, iwant_followup_time: Duration) -> Self {
        self.config.iwant_followup_time = iwant_followup_time;
        self
    }

    /// Enable support for flooodsub peers.
    pub fn support_floodsub(mut self) -> Self {
        self.config.support_floodsub = true;
        self
    }

    /// Disable support for the episub upgrade.
    pub fn disable_episub(mut self) -> Self {
        self.config.disable_episub = true;
        self
    }

    /// Published message ids time cache duration. The default is 10 seconds.
    pub fn published_message_ids_cache_time(
        mut self,
        published_message_ids_cache_time: Duration,
    ) -> Self {
        self.config.published_message_ids_cache_time = published_message_ids_cache_time;
        self
    }

    /// Constructs a [`GossipsubConfig`] from the given configuration and validates the settings.
    pub fn build(self) -> Result<GossipsubConfig<S>, &'static str> {
        // check all constraints on config

        if self.config.max_transmit_size < 100 {
            return Err("The maximum transmission size must be greater than 100 to permit basic control messages");
        }

        if self.config.history_length < self.config.history_gossip {
            return Err(
                "The history_length must be greater than or equal to the history_gossip \
                length",
            );
        }

        if !(self.config.mesh_outbound_min <= self.config.mesh_n_low
            && self.config.mesh_n_low <= self.config.mesh_n
            && self.config.mesh_n <= self.config.mesh_n_high)
        {
            return Err("The following inequality doesn't hold \
                mesh_outbound_min <= mesh_n_low <= mesh_n <= mesh_n_high");
        }

        if self.config.mesh_outbound_min * 2 > self.config.mesh_n {
            return Err(
                "The following inequality doesn't hold mesh_outbound_min <= self.config.mesh_n / 2",
            );
        }

        if self.config.unsubscribe_backoff.as_millis() == 0 {
            return Err("The unsubscribe_backoff parameter should be positive.");
        }

        Ok(self.config)
    }
}

impl<S: ChokingStrategy> std::fmt::Debug for GossipsubConfig<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut builder = f.debug_struct("GossipsubConfig");
        let _ = builder.field("protocol_id", &self.protocol_id);
        let _ = builder.field("custom_id_version", &self.custom_id_version);
        let _ = builder.field("history_length", &self.history_length);
        let _ = builder.field("history_gossip", &self.history_gossip);
        let _ = builder.field("mesh_n", &self.mesh_n);
        let _ = builder.field("mesh_n_low", &self.mesh_n_low);
        let _ = builder.field("mesh_n_high", &self.mesh_n_high);
        let _ = builder.field("retain_scores", &self.retain_scores);
        let _ = builder.field("gossip_lazy", &self.gossip_lazy);
        let _ = builder.field("gossip_factor", &self.gossip_factor);
        let _ = builder.field("heartbeat_initial_delay", &self.heartbeat_initial_delay);
        let _ = builder.field("heartbeat_interval", &self.heartbeat_interval);
        let _ = builder.field("fanout_ttl", &self.fanout_ttl);
        let _ = builder.field("max_transmit_size", &self.max_transmit_size);
        let _ = builder.field("idle_timeout", &self.idle_timeout);
        let _ = builder.field("duplicate_cache_time", &self.duplicate_cache_time);
        let _ = builder.field("validate_messages", &self.validate_messages);
        let _ = builder.field("allow_self_origin", &self.allow_self_origin);
        let _ = builder.field("do_px", &self.do_px);
        let _ = builder.field("prune_peers", &self.prune_peers);
        let _ = builder.field("prune_backoff", &self.prune_backoff);
        let _ = builder.field("backoff_slack", &self.backoff_slack);
        let _ = builder.field("flood_publish", &self.flood_publish);
        let _ = builder.field("graft_flood_threshold", &self.graft_flood_threshold);
        let _ = builder.field("mesh_outbound_min", &self.mesh_outbound_min);
        let _ = builder.field("opportunistic_graft_ticks", &self.opportunistic_graft_ticks);
        let _ = builder.field("opportunistic_graft_peers", &self.opportunistic_graft_peers);
        let _ = builder.field("max_messages_per_rpc", &self.max_messages_per_rpc);
        let _ = builder.field("max_ihave_length", &self.max_ihave_length);
        let _ = builder.field("max_ihave_messages", &self.max_ihave_messages);
        let _ = builder.field("iwant_followup_time", &self.iwant_followup_time);
        let _ = builder.field("support_floodsub", &self.support_floodsub);
        let _ = builder.field(
            "published_message_ids_cache_time",
            &self.published_message_ids_cache_time,
        );
        builder.finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::topic::IdentityHash;
    use crate::types::PeerKind;
    use crate::Topic;
    use crate::{Gossipsub, GossipsubBuilder, MessageAuthenticity};
    use libp2p_core::UpgradeInfo;
    use libp2p_swarm::{ConnectionHandler, NetworkBehaviour};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    #[test]
    fn create_thing() {
        let builder: GossipsubConfig = GossipsubConfigBuilder::default()
            .protocol_id_prefix("purple")
            .build()
            .unwrap();

        dbg!(builder);
    }

    fn get_gossipsub_message() -> GossipsubMessage {
        GossipsubMessage {
            source: None,
            data: vec![12, 34, 56],
            sequence_number: None,
            topic: Topic::<IdentityHash>::new("test").hash(),
        }
    }

    fn get_expected_message_id() -> MessageId {
        MessageId::from([
            49, 55, 56, 51, 56, 52, 49, 51, 52, 51, 52, 55, 51, 51, 53, 52, 54, 54, 52, 49, 101,
        ])
    }

    fn message_id_plain_function(message: &GossipsubMessage) -> MessageId {
        let mut s = DefaultHasher::new();
        message.data.hash(&mut s);
        let mut v = s.finish().to_string();
        v.push('e');
        MessageId::from(v)
    }

    #[test]
    fn create_config_with_message_id_as_plain_function() {
        let builder: GossipsubConfig = GossipsubConfigBuilder::default()
            .protocol_id_prefix("purple")
            .message_id_fn(message_id_plain_function)
            .build()
            .unwrap();

        let result = builder.message_id(&get_gossipsub_message());
        assert_eq!(result, get_expected_message_id());
    }

    #[test]
    fn create_config_with_message_id_as_closure() {
        let closure = |message: &GossipsubMessage| {
            let mut s = DefaultHasher::new();
            message.data.hash(&mut s);
            let mut v = s.finish().to_string();
            v.push('e');
            MessageId::from(v)
        };

        let builder: GossipsubConfig = GossipsubConfigBuilder::default()
            .protocol_id_prefix("purple")
            .message_id_fn(closure)
            .build()
            .unwrap();

        let result = builder.message_id(&get_gossipsub_message());
        assert_eq!(result, get_expected_message_id());
    }

    #[test]
    fn create_config_with_message_id_as_closure_with_variable_capture() {
        let captured: char = 'e';
        let closure = move |message: &GossipsubMessage| {
            let mut s = DefaultHasher::new();
            message.data.hash(&mut s);
            let mut v = s.finish().to_string();
            v.push(captured);
            MessageId::from(v)
        };

        let builder: GossipsubConfig = GossipsubConfigBuilder::default()
            .protocol_id_prefix("purple")
            .message_id_fn(closure)
            .build()
            .unwrap();

        let result = builder.message_id(&get_gossipsub_message());
        assert_eq!(result, get_expected_message_id());
    }

    #[test]
    fn create_config_with_protocol_id_prefix() {
        let builder: GossipsubConfig = GossipsubConfigBuilder::default()
            .protocol_id_prefix("purple")
            .message_id_fn(message_id_plain_function)
            .build()
            .unwrap();

        assert_eq!(builder.protocol_id(), "purple");
        assert_eq!(builder.custom_id_version(), &None);

        let mut gossipsub: Gossipsub = GossipsubBuilder::new(MessageAuthenticity::Anonymous)
            .validation_mode(crate::ValidationMode::Permissive)
            .config(builder)
            .build()
            .expect("Correct configuration");

        let handler = gossipsub.new_handler();
        let (protocol_config, _) = handler.listen_protocol().into_upgrade();
        let protocol_ids = protocol_config.protocol_info();

        assert_eq!(protocol_ids.len(), 3);

        assert_eq!(protocol_ids[0].protocol_id, b"/purple/1.1.2".to_vec());
        assert_eq!(protocol_ids[0].kind, PeerKind::Gossipsubv1_2);

        assert_eq!(protocol_ids[1].protocol_id, b"/purple/1.1.0".to_vec());
        assert_eq!(protocol_ids[1].kind, PeerKind::Gossipsubv1_1);

        assert_eq!(protocol_ids[2].protocol_id, b"/purple/1.0.0".to_vec());
        assert_eq!(protocol_ids[2].kind, PeerKind::Gossipsub);
    }

    #[test]
    fn create_config_with_custom_protocol_id() {
        let builder: GossipsubConfig = GossipsubConfigBuilder::default()
            .protocol_id("purple", GossipsubVersion::V1_0)
            .message_id_fn(message_id_plain_function)
            .build()
            .unwrap();

        assert_eq!(builder.protocol_id(), "purple");
        assert_eq!(builder.custom_id_version(), &Some(GossipsubVersion::V1_0));

        let mut gossipsub: Gossipsub = GossipsubBuilder::new(MessageAuthenticity::Anonymous)
            .validation_mode(crate::ValidationMode::Permissive)
            .config(builder)
            .build()
            .expect("Correct configuration");

        let handler = gossipsub.new_handler();
        let (protocol_config, _) = handler.listen_protocol().into_upgrade();
        let protocol_ids = protocol_config.protocol_info();

        assert_eq!(protocol_ids.len(), 1);

        assert_eq!(protocol_ids[0].protocol_id, b"purple".to_vec());
        assert_eq!(protocol_ids[0].kind, PeerKind::Gossipsub);
    }
}
