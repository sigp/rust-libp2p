//! The metrics obtained each episub metric window.
//! These metrics are used by various choking/unchoking algorithms in order to make their
//! decisions.

use crate::time_cache::{Entry, TimeCache};
use crate::MessageId;
use libp2p_core::PeerId;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

// NOTE: These are calculated independently of the scoring parameters (which also track first
// message deliveries, as scoring is typically optional, and Episub can work independently of
// scoring.
// NOTE: These metrics currently ignore the complexities of handling application-level validation of messages. Specifically, we
// include messages here that an application may REJECT or IGNORE.
/// The metrics passed to choking algorithms.
pub struct EpisubMetrics {
    /// Raw information within the moving window that we are capturing these metrics
    raw_deliveries: TimeCache<MessageId, DeliveryData>,
    /// A collection of IHAVE messages we have received. This is used for determining if we should
    /// unchoke a peer.
    ihave_msgs: TimeCache<MessageId, HashSet<PeerId>>,
    /// The number of duplicates per peer in the moving window.
    current_duplicates_per_peer: HashMap<PeerId, usize>,
}

/// A struct for storing message data along with their duplicates for building basic
/// statistical data.
struct DeliveryData {
    /// The first peer that sent us this message.
    first_sender: PeerId,
    /// The time the message was received.
    time_received: Instant,
    /// The peers that have sent us a duplicate of this message along with the time in ms since
    /// we received the message from the first sender.
    duplicates: HashMap<PeerId, BasicStat>,
}

/// The basic statistics we are measuring with respect to duplicates.
#[derive(Default, Copy, Clone)]
pub struct BasicStat {
    /// The order relative to other peers that a duplicate has come in.
    pub order: usize,
    /// The latency (in ms) since the first message that we received this duplicate.
    pub latency: usize,
}

impl std::ops::AddAssign for BasicStat {
    fn add_assign(&mut self, other: Self) {
        *self = Self {
            order: self.order + other.order,
            latency: self.latency + other.latency,
        }
    }
}

impl BasicStat {
    /// Divides both fields by a scalar.
    pub fn scalar_div(mut self, scalar: usize) -> Self {
        self.order /= scalar;
        self.latency /= scalar;

        self
    }
}

impl DeliveryData {
    /// Create a new instance of `DeliveryData`. This implies the message has first been received.
    pub fn new(peer_id: PeerId, time_received: Instant) -> Self {
        DeliveryData {
            first_sender: peer_id,
            time_received,
            duplicates: HashMap::with_capacity(5), // Assume most applications have at least 5 in the mesh.
        }
    }
}

impl EpisubMetrics {
    pub fn new(window_duration: Duration) -> Self {
        EpisubMetrics {
            raw_deliveries: TimeCache::new(window_duration),
            ihave_msgs: TimeCache::new(window_duration),
            current_duplicates_per_peer: HashMap::new(),
        }
    }

    /// Record that a message has been received.
    pub fn message_received(&mut self, message_id: MessageId, peer_id: PeerId, received: Instant) {
        match self.raw_deliveries.entry_without_removal(message_id) {
            // This is the first time we have seen this message in this window
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(DeliveryData::new(peer_id, received));
            }
            Entry::Occupied(occupied_entry) => {
                let entry = occupied_entry.into_mut();
                // The latency of this message
                let latency = received.duration_since(entry.time_received).as_millis() as usize;
                // The order that this peer sent us this message
                let order = entry.duplicates.len();
                let dupe = BasicStat { order, latency };
                entry.duplicates.insert(peer_id, dupe);

                *self.current_duplicates_per_peer.entry(peer_id).or_default() += 1;
            }
        }
    }

    /// Record that an IHAVE message has been received.
    pub fn ihave_received(&mut self, message_ids: Vec<MessageId>, peer_id: PeerId) {
        for message_id in message_ids {
            // Register the message as being unique if we haven't already seen this message before
            // (it should already be filtered by the duplicates filter) and record it against the
            // peers id.

            // NOTE: This check, prunes old messages.
            if self.raw_deliveries.contains_key(&message_id) {
                continue;
            }

            // Add this peer to the list
            self.ihave_msgs
                .entry(message_id)
                .or_insert_with(|| HashSet::new())
                .insert(peer_id);
        }
    }

    /// Returns the current number of duplicates a peer has sent us in this moving window.
    /// NOTE: This may contain expired elements and `prune_expired_elements()` should be called to remove these
    /// elements.
    pub fn duplicates(&self, peer_id: &PeerId) -> usize {
        *self.current_duplicates_per_peer.get(peer_id).unwrap_or(&0)
    }

    /// The unsorted average basic stat per peer over the current moving window.
    /// NOTE: The first message sender is considered to have no latency, i.e latency == 0, anyone who does not
    /// send a duplicate does not get counted.
    pub fn average_stat_per_peer(&mut self) -> Vec<(PeerId, BasicStat)> {
        // Remove any expired elements
        self.prune_expired_elements();
        let mut total_latency: HashMap<PeerId, BasicStat> = HashMap::new();
        // The number of messages participated in.
        let mut count: HashMap<PeerId, usize> = HashMap::new();

        for (_message_id, delivery_data) in self.raw_deliveries.iter() {
            // The sender receives 0 latency
            *count.entry(delivery_data.first_sender).or_default() += 1;

            // Add the duplicate latencies
            for (peer_id, stat) in delivery_data.duplicates.iter() {
                println!("Inside: {} {}", peer_id, stat.latency);
                *total_latency.entry(*peer_id).or_default() += *stat;
                *count.entry(*peer_id).or_default() += 1;
            }
        }

        for (p, l) in total_latency.iter() {
            println!("{} {}", p, l.latency);
        }

        return total_latency
            .into_iter()
            .map(|(peer_id, stat)| (peer_id, stat.scalar_div(*count.get(&peer_id).unwrap_or(&1))))
            .collect();
    }

    /// Given a percentile, provides the number of messages per peer that exist in that
    /// percentile. The percentile must be a number between 0 and 100.
    /// Elements from the cache get pruned before counting.
    pub fn percentile_latency_per_peer(&mut self, percentile: u8) -> HashMap<PeerId, usize> {
        // Remove any old messages from the moving window cache.
        self.prune_expired_elements();

        // Collect the latency for all duplicate messages
        let mut latency_percentile = self
            .raw_deliveries
            .iter()
            .map(|(_message_id, delivery_data)| {
                delivery_data
                    .duplicates
                    .iter()
                    .map(|(peer_id, basic_stat)| (*peer_id, basic_stat.latency))
            })
            .flatten()
            .collect::<Vec<_>>();

        // Sort the latency of all messages
        latency_percentile.sort_by(|(_peer_id, latency), (_peer_id2, latency_2)| {
            latency.partial_cmp(latency_2).expect("never none")
        });

        // Count the number of times a peer ends up in the `percentile`.
        let percentile_cutoff = percentile as usize * latency_percentile.len() / 100;

        if percentile_cutoff >= latency_percentile.len() {
            // NOTE: The percentile was > 100, we don't through an error, just return an empty set.
            return HashMap::new();
        }

        let mut counts_per_peer = HashMap::new();

        for (peer_id, _latency) in latency_percentile[percentile_cutoff..].iter() {
            *counts_per_peer.entry(*peer_id).or_default() += 1;
        }

        counts_per_peer
    }

    /// Returns the number of IHAVE messages that were received before an actual
    /// message. This indicates that a peer is sending us messages faster than our mesh peers and
    /// may be an indicator to unchoke the peer.
    pub fn ihave_messages_stats(&mut self) -> HashMap<PeerId, usize> {
        let mut ihave_count = HashMap::new();
        for (_message_id, peer_id_hashset) in self.ihave_msgs.iter() {
            for peer_id in peer_id_hashset.iter() {
                *ihave_count.entry(*peer_id).or_default() += 1;
            }
        }
        ihave_count
    }

    /// Prunes expired data from the moving window.
    // This is used to handle current cumulative values. We can add/subtract values as we go for
    // more complex metrics.
    pub fn prune_expired_elements(&mut self) {
        while let Some((_message_id, delivery_data)) = self.raw_deliveries.remove_expired() {
            for (peer_id, _basic_stat) in delivery_data.duplicates {
                // Subtract an expired registered duplicate
                // NOTE: We want this to panic in debug mode, as it should never happen.
                *self.current_duplicates_per_peer.entry(peer_id).or_default() -= 1;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_latency_and_order() {
        let mut metrics = EpisubMetrics::new(Duration::from_millis(100));
        // Used to keep track of expired messages.
        let start_time = Instant::now();

        // Lets have 5 Peer Ids. In the first 100ms the peers send messages as follows
        // Message 1: P1, 10ms P2, 5ms P3, 10ms P4, P5
        // Message 2: P2, 2ms P1, 3ms P3, 10ms, P4, P5
        // Message 3: P5, 3ms P3, 5ms P1, 20ms, P2, P4

        // Average latency for P1 = ( 0 + 2 + 8 ) /3 = 3
        // Average latency for P2 = ( 10 + 0 + 28) /3 = 12
        // Average latency for P3 = ( 15 + 5 + 3 ) /3 = 7
        // Average latency for P4 = ( 25 + 15 + 28 ) /3 = 22
        // Average latency for P5 = ( 25 + 15 + 0 ) /3 = 13

        let peers: Vec<PeerId> = (0..5).map(|_| PeerId::random()).collect();
        let message_ids: Vec<MessageId> = (0..3).map(|id| MessageId::new(&[id as u8])).collect();

        // First message
        metrics.message_received(message_ids[0].clone(), peers[0].clone(), start_time.clone());
        metrics.message_received(
            message_ids[0].clone(),
            peers[1].clone(),
            start_time.clone() + Duration::from_millis(10),
        );
        metrics.message_received(
            message_ids[0].clone(),
            peers[2].clone(),
            start_time.clone() + Duration::from_millis(15),
        );
        metrics.message_received(
            message_ids[0].clone(),
            peers[3].clone(),
            start_time.clone() + Duration::from_millis(25),
        );
        metrics.message_received(
            message_ids[0].clone(),
            peers[4].clone(),
            start_time.clone() + Duration::from_millis(25),
        );

        // Second message
        let start_time = Instant::now();
        metrics.message_received(message_ids[1].clone(), peers[1].clone(), start_time.clone());
        metrics.message_received(
            message_ids[1].clone(),
            peers[0].clone(),
            start_time.clone() + Duration::from_millis(2),
        );
        metrics.message_received(
            message_ids[1].clone(),
            peers[2].clone(),
            start_time.clone() + Duration::from_millis(5),
        );
        metrics.message_received(
            message_ids[1].clone(),
            peers[3].clone(),
            start_time.clone() + Duration::from_millis(15),
        );
        metrics.message_received(
            message_ids[1].clone(),
            peers[4].clone(),
            start_time.clone() + Duration::from_millis(15),
        );

        // Third message
        let start_time = Instant::now();
        metrics.message_received(message_ids[2].clone(), peers[4].clone(), start_time.clone());
        metrics.message_received(
            message_ids[2].clone(),
            peers[2].clone(),
            start_time.clone() + Duration::from_millis(3),
        );
        metrics.message_received(
            message_ids[2].clone(),
            peers[0].clone(),
            start_time.clone() + Duration::from_millis(8),
        );
        metrics.message_received(
            message_ids[2].clone(),
            peers[1].clone(),
            start_time.clone() + Duration::from_millis(28),
        );
        metrics.message_received(
            message_ids[2].clone(),
            peers[3].clone(),
            start_time.clone() + Duration::from_millis(28),
        );

        // Expected latencies
        let expected_latencies = [3, 12, 7, 22, 13];

        // Check the results
        for (peer_id, basic_stat) in metrics.average_stat_per_peer() {
            let peer_idx = peers
                .iter()
                .position(|peer| peer == &peer_id)
                .expect("Must exist");
            println!("Peer: {} Avg_Latency: {}", peer_idx + 1, basic_stat.latency);
            assert_eq!(expected_latencies[peer_idx], basic_stat.latency);
        }
    }
}
