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
use crate::TopicHash;
use libp2p_core::PeerId;
use std::collections::{HashMap, HashSet};

pub trait ChokingStrategy {
    /// This should return a list of peers that are eligible to choke. The router will decide if
    /// the peers are ultimately choked.
    fn choke_peers(&self, metrics: &mut EpisubMetrics) -> HashMap<TopicHash, HashSet<PeerId>>;

    /// This should return a list of peers that are eligible to unchoke. The router will decide if
    /// the peers are ultimately unchoked.
    fn unchoke_peers(&self, metrics: &mut EpisubMetrics) -> HashMap<TopicHash, HashSet<PeerId>>;

    /// Proposes a set of peers for the router to consider adding to the mesh. The router will
    /// decide if the mesh can handle extra peers, then handle the necessary control messages to
    /// add proposed peers to the mesh.
    /// The strategy should makes sure the peers are not already in the mesh and are connected to
    /// the router.
    fn mesh_addition(&self, metrics: &mut EpisubMetrics) -> HashMap<TopicHash, HashSet<PeerId>>;

    /// The number of peers in the mesh that should remain unchoked
    fn mesh_non_choke(&self) -> usize;

    /// The maximum number of peers to choke per episub heartbeat.
    fn choke_churn(&self) -> usize;

    /// The maximum number of peers to unchoke per episub heartbeat.
    fn unchoke_churn(&self) -> usize;

    /// The maximum number of peers to add to the mesh per episub heartbeat.
    fn mesh_addition_churn(&self) -> usize;
}

/// A built-in struct that implements [`ChokingStrategy`] to allow for easy access to some simple
/// choking/unchoking strategies.
#[derive(Clone)]
pub struct DefaultStrat {
    /// The minimum number of peers in the mesh that cannot be choked. Default value is 2.
    mesh_non_choke: usize,
    /// The maximum number of peers to return for each invocation of `choke_peers` per topic.
    /// Default value is 2.
    choke_churn: usize,
    /// Require a percentage of duplicates for each peer before making them eligible to be
    /// choked. Default is Some(30).
    choke_duplicates_threshold: Option<u8>,
    /// The specific choking strategy to use. See [`ChokeStrat`] for more details. Default is
    /// PercentileLatency ( percentile: 70, message_threshold: 10)
    choke_strategy: ChokeStrategy,
    /// The maximum number of peers to return that are in the mesh and currently choked per topic. The default value is 2.
    unchoke_churn: usize,
    /// The specific UNCHOKE'ing strategy to use. See [`UnchokeStrat`] for more details.
    /// Default value is IHaveMessagePercent(30).
    unchoke_strategy: UnchokeStrategy,
    /// The maximum number of peers to consider to add into the mesh from the mesh_addition if they
    /// pass the mesh_addition_addition_strategy per topic. Default value is 1.
    mesh_addition_churn: usize,
    /// The strategy to use to consider mesh_addition peers for addition into the mesh. Default
    /// value is IHaveMessagePercent(30).
    mesh_addition_strategy: UnchokeStrategy,
}

/// The list of possible choking strategies that can be used to CHOKE peers. More can be added in
/// the future.
#[derive(Clone)]
pub enum ChokeStrategy {
    /// Set a latency (in milliseconds) such that any peer gets choked that send messages who's
    /// average exceeds this cutoff. This cannot be used in conjunction with
    /// `percentile_latency_cut_off` or `latency_order_cut_off`.
    RawLatency(usize),
    /// Set a percentile latency cut off and message percent threshold respectively. If a
    /// peer sends messages in the last specified percentile over the message percent threshold, it
    /// gets choked. I.e percentile: 80 and message_threshold: 10 would specify that if a peer exists in the 80th percentile of
    /// latency with more than 10% of the messages it has sent, it will get choked.
    PercentileLatency {
        /// The latency percentile at which we start counting messages.
        percentile: u8,
        /// The message percentage threshold over which a peer will get choked.
        message_threshold: u8,
    },
    /// Choke based on the average order of duplicates sent to us. If a peer's average is over this
    /// order, consider it for choking.
    LatencyOrder(u8),
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
            mesh_non_choke: 2, // Leave at least 2 peers in each mesh unchoked.
            choke_churn: 2,
            choke_duplicates_threshold: Some(30), // If 30% of messages from a peer are duplicates,
            // make them eligible for choking
            choke_strategy: ChokeStrategy::PercentileLatency {
                percentile: 70, // Additionally if the peer sends messages in the last 70th
                // percentile of message latency
                message_threshold: 10, // And 10% of their messages exist in this percentile, choke
                                       // them.
            },
            unchoke_churn: 2,
            unchoke_strategy: UnchokeStrategy::IHaveMessagePercent(30), // If a peer sends us IHAVE
            // messages for 30% of messages we have
            // received, we unchoke it.
            mesh_addition_churn: 1,
            // If a mesh_addition peer sends us IHAVE messages for 30% of messages we have received, attempt to add
            // them to the mesh.
            mesh_addition_strategy: UnchokeStrategy::IHaveMessagePercent(30),
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

    /// The minimum number of peers in the mesh that cannot be choked.     
    pub fn mesh_non_choke(mut self, non_choke: usize) -> Self {
        self.default_strat.mesh_non_choke = non_choke;
        self
    }

    /// The maximum number of peers to consider to be choked per invocation of `choke_peers()`.
    pub fn choke_churn(mut self, churn: usize) -> Self {
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
    pub fn unchoke_churn(mut self, churn: usize) -> Self {
        self.default_strat.unchoke_churn = churn;
        self
    }

    /// Sets the specific unchoking strategy to use. See [`UnChokeStrat`] for more details.
    pub fn unchoke_strat(mut self, unchoke_strat: UnchokeStrategy) -> Self {
        self.default_strat.unchoke_strategy = unchoke_strat;
        self
    }

    /// The maximum number of peers that are in the mesh_addition that can be added to
    /// the mesh.
    pub fn mesh_addition_churn(mut self, churn: usize) -> Self {
        self.default_strat.unchoke_churn = churn;
        self
    }

    /// Sets the specific unchoking strategy to use when considering adding mesh_addition peers to the
    /// mesh. See [`UnChokeStrat`] for further details.
    pub fn mesh_addition_strategy(mut self, unchoke_strat: UnchokeStrategy) -> Self {
        self.default_strat.mesh_addition_strategy = unchoke_strat;
        self
    }

    /// Consumes the builder and creates the resulting [`DefaultStat`].
    pub fn build(self) -> DefaultStrat {
        self.default_strat
    }
}

// Now the magic for the strategy implementation

impl ChokingStrategy for DefaultStrat {
    fn mesh_non_choke(&self) -> usize {
        self.mesh_non_choke
    }

    fn choke_churn(&self) -> usize {
        self.choke_churn
    }

    fn unchoke_churn(&self) -> usize {
        self.unchoke_churn
    }

    fn mesh_addition_churn(&self) -> usize {
        self.mesh_addition_churn
    }

    fn choke_peers(&self, metrics: &mut EpisubMetrics) -> HashMap<TopicHash, HashSet<PeerId>> {
        // If we have a duplicates threshold, calculate the duplicates and see which peers
        // are actually eligible to be choked.
        let duplicate_metrics = self
            .choke_duplicates_threshold
            .map(|threshold| (threshold, metrics.duplicates_percentage()));

        // Chokes a peer if it passes the duplicates threshold
        let passes_duplicate_threshold = |topic: &TopicHash, peer_id: &PeerId| {
            // Check the peer satisfies the duplicates threshold constraint
            if let Some((threshold, metrics)) = duplicate_metrics.as_ref() {
                if let Some(duplicates_seen) = metrics.get(topic).and_then(|map| map.get(peer_id)) {
                    if duplicates_seen <= threshold {
                        return false;
                    }
                }
            }
            true
        };

        // Perform the choking strategy logic.
        match self.choke_strategy {
            // Check if the peer is unchoked
            ChokeStrategy::RawLatency(cutoff) => {
                // Obtain the average latency for all peers and filter those that have an average
                // latency larger than this cut-off
                metrics
                    .average_stat_per_topic_peer()
                    .into_iter()
                    .filter_map(|(topic, peer_map)| {
                        let filtered_map: HashSet<PeerId> = peer_map
                            .into_iter()
                            .filter_map(|(peer_id, stat)| {
                                if stat.latency >= cutoff {
                                    // Mark the peer as eligible if it passes the duplicate threshold also.
                                    if passes_duplicate_threshold(&topic, &peer_id) {
                                        return Some(peer_id);
                                    }
                                }
                                None
                            })
                            .collect();
                        if !filtered_map.is_empty() {
                            Some((topic, filtered_map))
                        } else {
                            None
                        }
                    })
                    .collect()
            }
            ChokeStrategy::PercentileLatency {
                percentile,
                message_threshold,
            } => {
                // Obtain the message counts for peers lying in this percentile and filter them
                // based on the message_threshold.
                metrics
                    .percentile_latency_per_topic_peer(percentile)
                    .into_iter()
                    .filter_map(|(topic, peer_map)| {
                        let filtered_map: HashSet<PeerId> = peer_map
                            .into_iter()
                            .filter_map(|(peer_id, message_percent)| {
                                if message_percent >= message_threshold {
                                    // Mark the peer as eligible if it passes the duplicate threshold also.
                                    if passes_duplicate_threshold(&topic, &peer_id) {
                                        return Some(peer_id);
                                    }
                                }
                                None
                            })
                            .collect();
                        if !filtered_map.is_empty() {
                            Some((topic, filtered_map))
                        } else {
                            None
                        }
                    })
                    .collect()
            }
            ChokeStrategy::LatencyOrder(cutoff) => {
                // Obtain the average message order for each peer and apply the cutoff.
                metrics
                    .average_stat_per_topic_peer()
                    .into_iter()
                    .filter_map(|(topic, peer_map)| {
                        let filtered_map: HashSet<PeerId> = peer_map
                            .into_iter()
                            .filter_map(|(peer_id, stat)| {
                                if stat.order as u8 >= cutoff {
                                    // Mark the peer as eligible if it passes the duplicate threshold also.
                                    if passes_duplicate_threshold(&topic, &peer_id) {
                                        return Some(peer_id);
                                    }
                                }
                                None
                            })
                            .collect();
                        if !filtered_map.is_empty() {
                            Some((topic, filtered_map))
                        } else {
                            None
                        }
                    })
                    .collect()
            }
        }
    }

    // NOTE: This function returns a list of peers, the list can include peers that are not in
    // the mesh. These peers are mesh_addition peers and the router may add them into the mesh.
    fn unchoke_peers(&self, metrics: &mut EpisubMetrics) -> HashMap<TopicHash, HashSet<PeerId>> {
        match self.unchoke_strategy {
            UnchokeStrategy::IHaveMessagePercent(percent) => {
                // Determine the percentage of messages we have received that a peer has sent
                // IHAVE messages before receiving them on the mesh.
                metrics
                    .ihave_messages_stats()
                    .into_iter()
                    .filter_map(|(topic, peer_map)| {
                        let filtered_map: HashSet<PeerId> = peer_map
                            .into_iter()
                            .filter_map(|(peer_id, message_percent)| {
                                if message_percent >= percent {
                                    Some(peer_id)
                                } else {
                                    None
                                }
                            })
                            .collect();

                        if !filtered_map.is_empty() {
                            Some((topic, filtered_map))
                        } else {
                            None
                        }
                    })
                    .collect()
            }
        }
    }

    /// Proposes a set of peers for the router to consider adding to the mesh. The router will
    /// decide if the mesh can handle extra peers, then handle the necessary control messages to
    /// add proposed peers to the mesh.
    fn mesh_addition(&self, metrics: &mut EpisubMetrics) -> HashMap<TopicHash, HashSet<PeerId>> {
        match self.mesh_addition_strategy {
            UnchokeStrategy::IHaveMessagePercent(percent) => metrics
                .ihave_messages_stats()
                .into_iter()
                .filter_map(|(topic, peer_map)| {
                    let filtered_map: HashSet<PeerId> = peer_map
                        .into_iter()
                        .filter_map(|(peer_id, message_percent)| {
                            if message_percent >= percent {
                                Some(peer_id)
                            } else {
                                None
                            }
                        })
                        .collect();

                    if !filtered_map.is_empty() {
                        Some((topic, filtered_map))
                    } else {
                        None
                    }
                })
                .collect(),
        }
    }
}
