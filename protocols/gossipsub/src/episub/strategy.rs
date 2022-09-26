// Copyright 2022 Sigma Prime Pty Ltd.
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

//! There are a number of strategies one can use to decide whether to CHOKE or UNCHOKE a peer. This
//! defines the ChokingStrategy trait as well as some pre-built strategies that can be used for
//! CHOKE'ing and UNCHOKE'ing peers.

use super::metrics::EpisubMetrics;
use crate::behaviour::ChokeState;
use crate::TopicHash;
use libp2p_core::PeerId;
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub trait ChokingStrategy {
    /// This function defines which peers should be CHOKE'd given a set of metrics. The resulting
    /// peers will then be choked by the gossipsub router.
    /// NOTE: Its up to the router to decide if these peers are in the mesh or not.
    fn choke_peers(
        &self,
        topic_peers: &mut HashMap<TopicHash, BTreeMap<PeerId, ChokeState>>,
        mesh_peers: &HashMap<TopicHash, BTreeSet<PeerId>>,
        metrics: &mut EpisubMetrics,
    );

    /// This function defines which peers should be UNCHOKE'd given a set mesh peers and episub metrics. The resulting
    /// peers will the be unchoked by the gossipsub router.
    /// This should also handle adding fanout peers into the mesh. i.e If the result returns a
    /// peer that is not in the mesh, that peer will be added into the mesh if possible.
    fn unchoke_peers(
        &self,
        topic_peers: &mut HashMap<TopicHash, BTreeMap<PeerId, ChokeState>>,
        mesh_peers: &HashMap<TopicHash, BTreeSet<PeerId>>,
        metrics: &mut EpisubMetrics,
    );
}

/// A built-in struct that implements [`ChokingStrategy`] to allow for easy access to some simple
/// choking/unchoking strategies.
#[derive(Clone)]
pub struct DefaultStrat {
    /// The maximum number of peers to return for each invocation of `choke_peers` per topic.
    /// Default value is 2.
    choke_churn: u8,
    /// Require a percentage of duplicates for each peer before making them eligible to be
    /// choked.
    choke_duplicates_threshold: Option<u8>,
    /// The specific choking strategy to use. See [`ChokeStrat`] for more details.
    choke_strategy: ChokeStrategy,
    /// The maximum number of peers to return that are in the mesh and currently choked per topic. The default value is 2.
    unchoke_churn: u8,
    /// The specific UNCHOKE'ing strategy to use. See [`UnchokeStrat`] for more details.
    unchoke_strategy: UnchokeStrategy,
    /// The maximum number of peers to consider to add into the mesh from the fanout if they
    /// pass the fanout_addition_strategy per topic.
    fanout_churn: u8,
    /// The strategy to use to consider fanout peers for addition into the mesh.
    fanout_addition_strategy: UnchokeStrategy,
}

/// The list of possible choking strategies that can be used to CHOKE peers. More can be added in
/// the future.
#[derive(Clone)]
pub enum ChokeStrategy {
    /// Set a latency (in milliseconds) such that any peer gets choked that send messages who's
    /// average exceeds this cutoff. This cannot be used in conjunction with
    /// `percentile_latency_cut_off` or `latency_order_cut_off`.
    RawLatencyCutoff(usize),
    /// Set a percentile latency cut off and message percent threshold respectively. If a
    /// peer sends messages in the last specified percentile over the message percent threshold, it
    /// gets choked. I.e percentile: 80 and message_threshold: 10 would specify that if a peer exists in the 80th percentile of
    /// latency with more than 10% of the messages it has sent, it will get choked.
    PercentileLatencyCutoff {
        /// The latency percentile at which we start counting messages.
        percentile: u8,
        /// The message percentage threshold over which a peer will get choked.
        message_threshold: u8,
    },
    /// Choke based on the average order of duplicates sent to us. If a peer's average is over this
    /// order, consider it for choking.
    LatencyOrderCutoff(u8),
}

/// The list of possible strategies one can use to unchoke a peer. More can be added in the future..
#[derive(Clone)]
pub enum UnchokeStrategy {
    /// The percentage of IHAVE messages received from a peer that arrived before we witnessed the
    /// message from our mesh peers. If a peer sends over this percentage it is eligible to be
    /// UNCHOKE'd.
    IHaveMessagePercent(u8),
}

/// Implements a default choking strategy for out-of-the-box Episub.
impl Default for DefaultStrat {
    fn default() -> Self {
        DefaultStrat {
            choke_churn: 2,
            choke_duplicates_threshold: Some(30), // If 30% of messages from a peer are duplicates,
            // make them eligible for choking
            choke_strategy: ChokeStrategy::PercentileLatencyCutoff {
                percentile: 70, // Additionally if the peer sends messages in the last 70th
                // percentile of message latency
                message_threshold: 10, // And 10% of their messages exist in this percentile, choke
                                       // them.
            },
            unchoke_churn: 2,
            unchoke_strategy: UnchokeStrategy::IHaveMessagePercent(30), // If a peer sends us IHAVE
            // messages for 30% of messages we have
            // received, we unchoke it.
            fanout_churn: 1,
            // If a fanout peer sends us IHAVE messages for 30% of messages we have received, attempt to add
            // them to the mesh.
            fanout_addition_strategy: UnchokeStrategy::IHaveMessagePercent(30),
        }
    }
}

/// A builder for creating a custom [`DefaultStrat`].
#[derive(Default, Clone)]
pub struct DefaultStratBuilder {
    /// The underlying default strategy to be built.
    default_strat: DefaultStrat,
}

impl DefaultStratBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// The maximum number of peers to consider to be choked per invocation of `choke_peers()`.
    pub fn choke_churn(mut self, churn: u8) -> Self {
        self.default_strat.choke_churn = churn;
        self
    }

    /// Require a percentage of duplicates for each peer before making them eligible to be
    /// choked. Setting this to None, removes any duplicate threshold requirement.
    pub fn choke_duplicates_threshold(mut self, threshold: Option<u8>) -> Self {
        self.default_strat.choke_duplicates_threshold = threshold;
        self
    }

    /// Sets the specific choking strategy to use. See [`ChokeStrat`] for more details.
    pub fn choke_strat(mut self, choke_strat: ChokeStrategy) -> Self {
        self.default_strat.choke_strategy = choke_strat;
        self
    }

    /// The maximum number of peers that are in the mesh and choked to consider to be unchoked.
    pub fn unchoke_churn(mut self, churn: u8) -> Self {
        self.default_strat.unchoke_churn = churn;
        self
    }

    /// Sets the specific unchoking strategy to use. See [`UnChokeStrat`] for more details.
    pub fn unchoke_strat(mut self, unchoke_strat: UnchokeStrategy) -> Self {
        self.default_strat.unchoke_strategy = unchoke_strat;
        self
    }

    /// The maximum number of peers that are in the fanout that can be considered to be added to
    /// the mesh.
    pub fn fanout_churn(mut self, churn: u8) -> Self {
        self.default_strat.unchoke_churn = churn;
        self
    }

    /// Sets the specific unchoking strategy to use when considering adding fanout peers to the
    /// mesh. See [`UnChokeStrat`] for further details.
    pub fn fanout_addition_strategy(mut self, unchoke_strat: UnchokeStrategy) -> Self {
        self.default_strat.fanout_addition_strategy = unchoke_strat;
        self
    }

    /// Consumes the builder and creates the resulting [`DefaultStat`].
    pub fn build(self) -> DefaultStrat {
        self.default_strat
    }
}

// Now the magic for the strategy implementation

impl ChokingStrategy for DefaultStrat {
    fn choke_peers(
        &self,
        topic_peers: &mut HashMap<TopicHash, BTreeMap<PeerId, ChokeState>>,
        mesh_peers: &HashMap<TopicHash, BTreeSet<PeerId>>,
        metrics: &mut EpisubMetrics,
    ) {
        // If we have a duplicates threshold, calculate the duplicates and see which peers
        // are actually eligible to be choked.
        let duplicate_metrics = {
            if let Some(threshold) = self.choke_duplicates_threshold {
                Some((threshold, metrics.duplicates_percentage()))
            } else {
                None
            }
        };

        // Chokes a peer if it passes the duplicates threshold, exists in the mesh and is currently
        // unchoked.
        // Returns true, if the peer became choked.
        let mut choke_potential_peer = |topic: &TopicHash, peer_id: &PeerId| {
            // Check if the peer exists in the mesh
            if !(mesh_peers.get(topic).map(|peers| peers.contains(&peer_id)) == Some(true)) {
                return false;
            }

            // Check the peer satisfies the duplicates threshold constraint
            if let Some((threshold, metrics)) = duplicate_metrics.as_ref() {
                if let Some(duplicates_seen) = metrics.get(topic).and_then(|map| map.get(peer_id)) {
                    if duplicates_seen >= threshold {
                        return false;
                    }
                }
            }

            // If the peer is in the unchoked state, choke it

            if let Some(peer_choke_state) = topic_peers
                .get_mut(&topic)
                .and_then(|set| set.get_mut(&peer_id))
            {
                if peer_choke_state.peer_is_choked == false {
                    // Choke the peer and increment the counter
                    peer_choke_state.peer_is_choked = true;
                    return true;
                }
            }

            false
        };

        // Perform the choking strategy logic.
        match self.choke_strategy {
            // Check if the peer is unchoked
            ChokeStrategy::RawLatencyCutoff(cutoff) => {
                // Obtain the average latency for all peers and filter those that have an average
                // latency larger than this cut-off
                let mut choked_peers = 0;
                for (topic, peer_map) in metrics.average_stat_per_topic_peer().into_iter() {
                    for (peer_id, stat) in peer_map.into_iter() {
                        if stat.latency >= cutoff {
                            // Choke the peer, if its in the mesh.
                            if choke_potential_peer(&topic, &peer_id) {
                                choked_peers += 1;
                                if choked_peers >= self.choke_churn {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
            ChokeStrategy::PercentileLatencyCutoff {
                percentile,
                message_threshold,
            } => {
                // Obtain the message counts for peers lying in this percentile and filter them
                // based on the message_threshold.
                let mut choked_peers = 0;
                for (topic, peer_map) in metrics
                    .percentile_latency_per_topic_peer(percentile)
                    .into_iter()
                {
                    for (peer_id, message_percent) in peer_map.into_iter() {
                        if message_percent >= message_threshold {
                            // Choke the peer, if its in the mesh.
                            if choke_potential_peer(&topic, &peer_id) {
                                choked_peers += 1;
                                if choked_peers >= self.choke_churn {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
            ChokeStrategy::LatencyOrderCutoff(cutoff) => {
                // Obtain the average message order for each peer and apply the cutoff.
                let mut choked_peers = 0;
                for (topic, peer_map) in metrics.average_stat_per_topic_peer().into_iter() {
                    for (peer_id, stat) in peer_map.into_iter() {
                        if stat.order as u8 >= cutoff {
                            // Choke the peer, if its in the mesh.
                            if choke_potential_peer(&topic, &peer_id) {
                                choked_peers += 1;
                                if choked_peers >= self.choke_churn {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // NOTE: This function returns a list of peers, the list can include peers that are not in
    // the mesh. These peers are fanout peers and the router may add them into the mesh.
    fn unchoke_peers(
        &self,
        topic_peers: &mut HashMap<TopicHash, BTreeMap<PeerId, ChokeState>>,
        mesh_peers: &HashMap<TopicHash, BTreeSet<PeerId>>,
        metrics: &mut EpisubMetrics,
    ) {
        // NOTE: There is currently only one strategy used here. To avoid duplicate
        // calculations, we store state here to use the same result for fanout peers. If new
        // strategies are added this logic will need to be modified.
        let ihave_message_stats = metrics.ihave_messages_stats();

        // Unchoke's a peer if it is currently choked. `in_mesh` decides whether to unchoke peers
        // that are in the mesh or outside the mesh (fanout).
        // Returns true, if the peer was choked.
        let mut unchoke_peer = |topic: &TopicHash, peer_id: &PeerId, in_mesh: bool| {
            // Is the peer in the mesh
            if !(mesh_peers.get(topic).map(|peers| peers.contains(peer_id)) == Some(in_mesh)) {
                // Doesn't fit the requirements of in/out of mesh
                return false;
            }

            if let Some(choke_state) = topic_peers
                .get_mut(topic)
                .and_then(|set| set.get_mut(peer_id))
            {
                if choke_state.peer_is_choked == true {
                    choke_state.peer_is_choked = false;
                }
            }
            false
        };

        match self.unchoke_strategy {
            UnchokeStrategy::IHaveMessagePercent(percent) => {
                // Determine the percentage of messages we have received that a peer has sent
                // IHAVE messages before receiving them on the mesh.
                let mut unchoked_count = 0;
                'main: for (topic, map) in ihave_message_stats.iter() {
                    for (peer_id, message_percent) in map.iter() {
                        if *message_percent >= percent {
                            if unchoke_peer(topic, peer_id, true) {
                                unchoked_count += 1;
                                if unchoked_count >= self.unchoke_churn {
                                    break 'main;
                                }
                            }
                        }
                    }
                }
            }
        }

        match self.fanout_addition_strategy {
            UnchokeStrategy::IHaveMessagePercent(percent) => {
                let mut inserted_count = 0;
                'main: for (topic, map) in ihave_message_stats.into_iter() {
                    for (peer_id, message_percent) in map.iter() {
                        if *message_percent >= percent {
                            if unchoke_peer(&topic, peer_id, false) {
                                {
                                    inserted_count += 1;
                                    if inserted_count >= self.fanout_churn {
                                        break 'main;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
