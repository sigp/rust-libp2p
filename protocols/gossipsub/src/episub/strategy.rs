//! There are a number of strategies one can use to decide whether to CHOKE or UNCHOKE a peer. This
//! defines the ChokingStrategy trait as well as some pre-built strategies that can be used for
//! CHOKE'ing and UNCHOKE'ing peers.

use super::metrics::EpisubMetrics;
use libp2p_core::PeerId;
use std::collections::HashSet;

pub trait ChokingStrategy {
    /// This function defines which peers should be CHOKE'd given a set of metrics. The resulting
    /// peers will then be choked by the gossipsub router.
    /// NOTE: Its up to the router to decide if these peers are in the mesh or not.
    fn choke_peers(&self, metrics: &mut EpisubMetrics) -> HashSet<PeerId>;

    /// This function defines which peers should be UNCHOKE'd given a set of metrics. The resulting
    /// peers will the be unchoked by the gossipsub router.
    fn unchoke_peers(&self, metrics: &mut EpisubMetrics) -> HashSet<PeerId>;
}

/// A built-in struct that implements [`ChokingStrategy`] to allow for easy access to some simple
/// choking/unchoking strategies.
#[derive(Clone)]
pub struct DefaultStrat {
    /// Require a percentage of duplicates for each peer before making them eligible to be
    /// choked.
    choke_duplicates_threshold: Option<u8>,
    /// The specific choking strategy to use. See [`ChokeStrat`] for more details.
    choke_strategy: ChokeStrategy,
    /// The specific UNCHOKE'ing strategy to use. See [`UnchokeStrat`] for more details.
    unchoke_strategy: UnchokeStrategy,
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
            choke_duplicates_threshold: Some(30), // If 30% of messages from a peer are duplicates,
            // make them eligible for choking
            choke_strategy: ChokeStrategy::PercentileLatencyCutoff {
                percentile: 70, // Additionally if the peer sends messages in the last 70th
                // percentile of message latency
                message_threshold: 10, // And 10% of their messages exist in this percentile, choke
                                       // them.
            },
            unchoke_strategy: UnchokeStrategy::IHaveMessagePercent(30), // If a peer sends us IHAVE
                                                                        // messages for 30% of messages we have
                                                                        // received, we unchoke it.
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

    /// Sets the specific unchoking strategy to use. See [`UnChokeStrat`] for more details.
    pub fn unchoke_strat(mut self, unchoke_strat: UnchokeStrategy) -> Self {
        self.default_strat.unchoke_strategy = unchoke_strat;
        self
    }

    /// Consumes the builder and creates the resulting [`DefaultStat`].
    pub fn build(self) -> DefaultStrat {
        self.default_strat
    }
}

// Now the magic for the strategy implementation

impl ChokingStrategy for DefaultStrat {
    fn choke_peers(&self, metrics: &mut EpisubMetrics) -> HashSet<PeerId> {
        // Perform the choking strategy logic.
        let mut potential_choked_peers: HashSet<PeerId> = match self.choke_strategy {
            ChokeStrategy::RawLatencyCutoff(cutoff) => {
                // Obtain the average latency for all peers and filter those that have an average
                // latency larger than this cut-off
                metrics
                    .average_stat_per_peer()
                    .iter()
                    .filter_map(|(peer_id, stat)| {
                        if stat.latency >= cutoff {
                            Some(*peer_id)
                        } else {
                            None
                        }
                    })
                    .collect()
            }
            ChokeStrategy::PercentileLatencyCutoff {
                percentile,
                message_threshold,
            } => {
                // Obtain the message counts for peers lying in this percentile and filter them
                // based on the message_threshold.
                metrics
                    .percentile_latency_per_peer(percentile)
                    .iter()
                    .filter_map(|(peer_id, message_percent)| {
                        if *message_percent >= message_threshold {
                            Some(*peer_id)
                        } else {
                            None
                        }
                    })
                    .collect()
            }
            ChokeStrategy::LatencyOrderCutoff(cutoff) => {
                // Obtain the average message order for each peer and apply the cutoff.
                metrics
                    .average_stat_per_peer()
                    .iter()
                    .filter_map(|(peer_id, stat)| {
                        if stat.order as u8 >= cutoff {
                            Some(*peer_id)
                        } else {
                            None
                        }
                    })
                    .collect()
            }
        };

        // If we have a duplicates threshold, calculate the duplicates and see which peers
        // are actually eligible to be choked.
        if let Some(threshold) = self.choke_duplicates_threshold {
            for (peer_id, duplicate_percent) in metrics.duplicates_percentage().iter() {
                // If the peer doesn't meet the threshold, then remove it from consideration of
                // being choked.
                if *duplicate_percent < threshold {
                    potential_choked_peers.remove(peer_id);
                }
            }
        }

        potential_choked_peers
    }

    fn unchoke_peers(&self, metrics: &mut EpisubMetrics) -> HashSet<PeerId> {
        match self.unchoke_strategy {
            UnchokeStrategy::IHaveMessagePercent(percent) => {
                // Determine the percentage of messages we have received that a peer has sent
                // IHAVE messages before receiving them on the mesh.
                metrics
                    .ihave_messages_stats()
                    .iter()
                    .filter_map(|(peer_id, message_percent)| {
                        if *message_percent >= percent {
                            Some(*peer_id)
                        } else {
                            None
                        }
                    })
                    .collect()
            }
        }
    }
}
