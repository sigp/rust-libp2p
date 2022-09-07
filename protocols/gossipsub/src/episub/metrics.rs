//! The metrics obtained each episub metric window.
//! These metrics are used by various choking/unchoking algorithms in order to make their
//! decisions. 


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
    current_duplicates_per_peer: HashMap<PeerId, usize>
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
    duplicates: HashMap<PeerId, BasicStat>
}

/// The basic statistics we are measuring with respect to duplicates.
pub struct BasicStat {
    /// The order relative to other peers that a duplicate has come in.
    pub order: usize,
    /// The latency (in ms) since the first message that we received this duplicate.
    pub latency: usize,
}

impl DeliveryData {
    /// Create a new instance of `DeliveryData`. This implies the message has first been received.
    pub fn new(peer_id: PeerId, time_received: Instant) -> Self {
        DeliveryData {
            first_sender: peer_id,
            time_received,
            duplicates: HashMap::with_capacity(5),// Assume most applications have at least 5 in the mesh.
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
        match raw_deliveries.entry_without_removal(message_id) {
            // This is the first time we have seen this message in this window
            Entry::Vacant(vacant_entry) => vacant_entry.insert(DeliveryData::new(peer_id, received)),
            Entry::Occupied(occupied_entry) => {
                let entry = occupied_entry.into_mut();
                // The latency of this message
                let latency = Instant::now().duration_since(entry.time_received).as_millis();
                // The order that this peer sent us this message
                let order = duplicates.len();
                let dupe = Duplicate {
                    order,
                    latency
                };
                let entry.duplicates.insert(peer_id, dupe);

                *self.current_duplicates_per_peer.entry(peer_id).or_default() +=1;
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
            if self.raw_deliveries.contains_key(message_id) {
                continue;
            }

            // Add this peer to the list
            self.ihave_msgs.entry(message_id).or_insert_with(|| HashSet::new()).insert(peer_id);
        }
    }

    /// Returns the current number of duplicates a peer has sent us in this moving window.
    /// NOTE: This may contain expired elements and `prune_expired_elements()` should be called to remove these
    /// elements.
    pub fn duplicates(&self, peer_id: &PeerId) -> usize {
        self.current_duplicates_per_peer.get(peer_id).unwrap_or(0) 
    }

    /// The unsorted average basic stat per peer over the current moving window.
    /// NOTE: The first message sender is considered to have no latency, i.e latency == 0, anyone who does not
    /// send a duplicate does not get counted.
    pub fn average_stat_per_peer(&mut self) -> Vec<(PeerId, BasicStat)> { 
        // Remove any expired elements
        self.prune_expired_elements();
        let mut total_latency: HashMap<PeerId, BasicStat> = HashMap::new();
        let mut count: HashMap<PeerId, usize> = HashMap::new();

        for (_message_id, delivery_data) in self.raw_deliveries.iter() { 
           // The sender receives 0 latency 
           *count.entry(delivery_data.first_sender).or_default() += 1;

           // Add the duplicate latencies
           for (peer_id, stat) in delivery_data.duplicates.iter() {
               *total_latency.entry(peer_id).or_default() += stat;
               *count.entry(peer_id).or_default() += 1;
            }
        }

        return total_latency.into_iter().map(|(peer_id, stat)| (peer_id, stat/count.get(peer_id).unwrap_or(1))).collect();
    }

    /// Given a percentile, provides the number of messages per peer that exist in that
    /// percentile. The percentile must be a number between 0 and 100.
    /// Elements from the cache get pruned before counting.
    pub fn percentile_latency_per_peer(&mut self, percentile: u8) -> HashMap<PeerId, usize> {
        // Remove any old messages from the moving window cache.
        self.prune_expired_elements();

        // Collect the latency for all duplicate messages
        let mut latency_percentile = self.raw_deliveries.iter().map(|(_message_id, delivery_data)| delivery_data.duplicates.iter().map(|(peer_id,basic_stat)| (peer_id, basic_stat.latency))).collect::<Vec<_>>(); 

        // Sort the latency of all messages
        latency_percentile.sort_by(|(peer_id, latency)| latency);

        // Count the number of times a peer ends up in the `percentile`.
        let percentile_cutoff = percentile*latency_percentile.len()/100;

        if percentile_cutoff >= latency_percentile.len() {
            return Vec::new(); // Percentile was > 100
        }

        let mut counts_per_peer = HashMap::new();

        for (peer_id, _latency) in latency_percentile[percentile_cutoff..] {
            counts_per_peer.entry(peer_id).or_default() +=1;
        }
    }

    /// Returns the number of IHAVE messages and average latency from each peer, that was received before an actual
    /// message.
    pub fn ihave_messages_stats(&mut self) -> HashMap<PeerId, usize> {
        let mut ihave_count = HashMap::new();
        for (_message_id, peer_id_hashset) in self.ihave_msgs.iter() {
            for peer_id in peer_id_hashset.iter() {
                *ihave_count.entry(peer_id).or_default() += 1;
            }
        }
        ihave_count
    }

    /// Prunes expired data from the moving window.
    // This is used to handle current cumulative values. We can add/subtract values as we go for
    // more complex metrics.
    pub fn prune_expired_elements(&mut self) {
        while let Some((message_id, delivery_data)) = self.raw_deliveries.remove_expired() {
            for peer_id in delivery_data.duplicates {
                // Subtract an expired registered duplicate
                // NOTE: We want this to panic in debug mode, as it should never happen.
                *self.current_duplicates_per_peer.entry(peer_id) -= 1;
            }
        }
    }
}
