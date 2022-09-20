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
                let order = entry.duplicates.len() + 1; // We add 1 as the first entry gets the 0
                                                        // score.
                let dupe = BasicStat { order, latency };
                entry.duplicates.insert(peer_id, dupe);

                *self.current_duplicates_per_peer.entry(peer_id).or_default() += 1;
            }
        }
    }

    /// If a message turns out to be invalid, we want to remove the data here to prevent peers
    /// sending us a bunch of invalid messages lowering their average message delivery statistics.
    pub fn remove_invalid_message(&mut self, message_id: &MessageId) {
        if let Some(delivery_data) = self.raw_deliveries.remove(message_id) {
            for (peer_id, _basic_stat) in delivery_data.duplicates {
                // Subtract an expired registered duplicate
                // NOTE: We want this to panic in debug mode, as it should never happen.
                *self.current_duplicates_per_peer.entry(peer_id).or_default() -= 1;
            }
        }
    }

    /// Record that an IHAVE message has been received.
    pub fn ihave_received(&mut self, message_ids: &[MessageId], peer_id: PeerId) {
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
                .entry(message_id.clone())
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

    /// Calculates the percentage of duplicates sent for each peer in the given moving window. This
    /// removes expired elements.
    pub fn duplicates_percentage(&mut self) -> HashMap<PeerId, u8> {
        self.prune_expired_elements();
        let messages = self.raw_deliveries.len();

        self.current_duplicates_per_peer
            .iter()
            .map(|(peer_id, duplicates)| (*peer_id, (*duplicates * 100 / messages) as u8))
            .collect()
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
                *total_latency.entry(*peer_id).or_default() += *stat;
                *count.entry(*peer_id).or_default() += 1;
            }
        }

        return total_latency
            .into_iter()
            .map(|(peer_id, stat)| (peer_id, stat.scalar_div(*count.get(&peer_id).unwrap_or(&1))))
            .collect();
    }

    /// Given a percentile, provides the percentage of messages per peer that exist in that
    /// percentile. The percentile must be a number between 0 and 100.
    /// Elements from the cache get pruned before counting.
    pub fn percentile_latency_per_peer(&mut self, percentile: u8) -> HashMap<PeerId, u8> {
        // Remove any old messages from the moving window cache.
        self.prune_expired_elements();

        // A little struct to store efficiently in a BinaryHeap and get the correct ordering.
        #[derive(PartialEq, Eq, PartialOrd, Ord)]
        struct PercentileData {
            latency: usize, // Order based on latency first.
            peer_id: PeerId,
        }

        // Collect the latency for all duplicate messages
        let mut message_count = 0;
        let mut data_points_count = 0;
        // Assume there's going to be quite a few messages.
        let mut latency_percentile = std::collections::BinaryHeap::with_capacity(1000);
        for (_message_id, delivery_data) in self.raw_deliveries.iter() {
            message_count += 1;
            latency_percentile.push(PercentileData {
                peer_id: delivery_data.first_sender,
                latency: 0,
            });
            data_points_count += 1; // The first message sender counts as a data point with 0
                                    // latency.
            for (peer_id, basic_stat) in delivery_data.duplicates.iter() {
                latency_percentile.push(PercentileData {
                    latency: basic_stat.latency,
                    peer_id: *peer_id,
                });
                data_points_count += 1;
            }
        }

        // Count the number of times a peer ends up in the `percentile`.
        let percentile_cutoff =
            ((percentile as f32 * data_points_count as f32 / 100.0).ceil()) as usize;

        if percentile_cutoff > data_points_count {
            // NOTE: The percentile was > 100, we don't return an error, just return an empty set.
            return HashMap::new();
        }

        let mut percentage_counts_per_peer: HashMap<PeerId, usize> = HashMap::new();

        // Remove the elements that should exist in the percentile
        // The -1 is to account for the rounding in calculating the cutoff to account for
        // percentiles that split data indexes. This makes the percentile inclusive.
        for _ in percentile_cutoff.saturating_sub(1)..data_points_count {
            if let Some(PercentileData {
                peer_id,
                latency: _,
            }) = latency_percentile.pop()
            {
                *percentage_counts_per_peer.entry(peer_id).or_default() += 100; // Results in a total percentage
            }
        }

        // Calculate the percentage
        percentage_counts_per_peer
            .into_iter()
            .map(|(peer_id, count)| (peer_id, (count / message_count) as u8))
            .collect()
    }

    /// Returns the percentage of IHAVE messages that were received before an actual
    /// message compared to actual messages received. This is calculated for each peer.
    /// To put another way, for all messages we received, this calculates the percentage of these
    /// messages that a specific peer sent an IHAVE prior to us receiving the message from the
    /// mesh.
    /// This indicates that a peer is sending us messages faster than our mesh peers and
    /// may be an indicator to unchoke the peer.
    // NOTE: We don't want peers to send us a bunch of random IHAVE messages in an attempt to be
    // unchoked (if scoring is enabled, they would be punished if these messages are not real).
    // Therefore we only count IHAVE messages that correspond to a message that was later received
    // by another peer.
    pub fn ihave_messages_stats(&mut self) -> HashMap<PeerId, u8> {
        let mut ihave_count: HashMap<PeerId, usize> = HashMap::new();
        for (message_id, peer_id_hashset) in self.ihave_msgs.iter() {
            // Make sure we actually received this message from another peer.
            if let Some(delivery_data) = self.raw_deliveries.get(message_id) {
                for peer_id in peer_id_hashset.iter() {
                    // If we received the message from another peer.
                    if !delivery_data.duplicates.is_empty()
                        || &delivery_data.first_sender != peer_id
                    {
                        *ihave_count.entry(*peer_id).or_default() += 100;
                    }
                }
            }
        }
        let total_messages = self.raw_deliveries.len();

        ihave_count
            .into_iter()
            .map(|(peer_id, message_count)| (peer_id, (message_count / total_messages) as u8))
            .collect()
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
    fn test_latency_order_and_percentile() {
        let mut metrics = EpisubMetrics::new(Duration::from_millis(100));
        // Used to keep track of expired messages.

        let peers: Vec<PeerId> = (0..5).map(|_| PeerId::random()).collect();
        let message_ids: Vec<MessageId> = (0..3).map(|id| MessageId::new(&[id as u8])).collect();

        // Make this a closure so we can run it multiple times to make sure pruning is working as
        // expected.
        let mut run_test = || {
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

            // Expected average latencies
            let expected_latencies = [3, 12, 7, 22, 13];

            // Average Order for P1 = ( 0 + 1 + 2 ) /3 = 1
            // Average Order for P2 = ( 1 + 0 + 3 ) /3 = 1
            // Average Order for P3 = ( 2 + 2 + 1 ) /3 = 1
            // Average Order for P4 = ( 3 + 3 + 4 ) /3 = 3
            // Average Order for P5 = ( 4 + 4 + 0 ) /3 = 2

            // Expected average orders
            let expected_orders = [1, 1, 1, 3, 2];

            // Percentile Latency Counts
            // M1P1, M2P2, M3P5, M2P1 (2ms), M3P3 (3ms), M2P3 (5ms), M3P1 (8ms) |50th Percentile|, M1P2 (10ms)   M1P3 (15ms), M1P4
            // (15ms), M1P5 (15ms),| 80th Percentile| M1P4(25ms) , M1P5 (25ms) |90th Percentile|, M3P2 (28ms) , M3P4 (28ms).

            let expected_50_percentile_counts = [0, 66, 33, 100, 66];
            let expected_80_percentile_counts = [0, 33, 0, 66, 33];
            let expected_90_percentile_counts = [0, 33, 0, 33, 0];
            let expected_percentiles = [
                expected_50_percentile_counts,
                expected_80_percentile_counts,
                expected_90_percentile_counts,
            ];

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

            // Check the results
            for (peer_id, basic_stat) in metrics.average_stat_per_peer() {
                let peer_idx = peers
                    .iter()
                    .position(|peer| peer == &peer_id)
                    .expect("Must exist");
                println!("Peer: {} Avg_Latency: {}", peer_idx + 1, basic_stat.latency);
                assert_eq!(expected_latencies[peer_idx], basic_stat.latency);
                println!("Peer: {} Avg_Order: {}", peer_idx + 1, basic_stat.order);
                assert_eq!(expected_orders[peer_idx], basic_stat.order);
            }

            // Check the percentile calculations
            let latency_checks = [50u8, 80, 90];

            let mut check_id = 0;
            for latency_check in latency_checks {
                for (peer_id, percentage_count) in
                    metrics.percentile_latency_per_peer(latency_check).iter()
                {
                    let peer_idx = peers
                        .iter()
                        .position(|peer| peer == peer_id)
                        .expect("Must exist");
                    println!(
                        "Peer: {}, {} ,  {}_percentile_latency: {}",
                        peer_idx + 1,
                        peer_id,
                        latency_check,
                        percentage_count
                    );
                    assert_eq!(expected_percentiles[check_id][peer_idx], *percentage_count);
                }
                // Keep track of expected result id.
                check_id += 1;
            }
        };

        // Perform the test.
        run_test();

        // Test to make sure we prune expired elements.
        std::thread::sleep(Duration::from_millis(100));

        run_test();
    }

    #[test]
    fn test_ihave_message_percent() {
        let mut metrics = EpisubMetrics::new(Duration::from_millis(100));

        // Lets say there are three peers. Peer 1 sends IHave messages 20% of time for all
        // messages, Peer 2 sends 50% and Peer 3 never sends any.
        let expected_percentages = [20, 50, 0];

        let total_messages = 100u8;
        let peers: Vec<PeerId> = (0..3).map(|_| PeerId::random()).collect();

        for id in 0..total_messages {
            let message_id = MessageId::new(&id.to_be_bytes());

            if id % 5 == 0 {
                // Peer 1 sends an IHAVE message 20% of the time.
                metrics.ihave_received(&vec![message_id.clone()], peers[0]);
            }

            if id % 2 == 0 {
                // Peer 2 sends an IHAVE message 40% of the time.
                metrics.ihave_received(&vec![message_id.clone()], peers[1]);
            }

            // Peer 3 is always the first, but later peer 1 and peer 2 send the message also.
            metrics.message_received(message_id.clone(), peers[2], Instant::now());

            metrics.message_received(message_id.clone(), peers[0], Instant::now());
            metrics.message_received(message_id, peers[1], Instant::now());
        }

        // Check to make sure the percentages work out.

        for (peer_id, ihave_percentage) in metrics.ihave_messages_stats() {
            let peer_idx = peers
                .iter()
                .position(|peer| *peer == peer_id)
                .expect("Must exist");
            println!("Peer: {}, {}", peer_idx + 1, ihave_percentage,);
            assert_eq!(expected_percentages[peer_idx], ihave_percentage);
        }
    }
}
