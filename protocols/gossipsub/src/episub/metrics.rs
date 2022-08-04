//! The metrics obtained each episub metric window.
//! These metrics are used by various choking/unchoking algorithms in order to make their
//! decisions. 


// NOTE: These are calculated independently of the scoring parameters (which also track first
// message deliveries, as scoring is typically optional, and Episub can work independently of
// scoring.
// NOTE: These metrics currently ignore the complexities of handling application-level validation of messages. Specifically, we
// include messages here that an application may REJECT or IGNORE.
/// The metrics passed to choking algorithms. 
pub fn EpisubMetrics {
    /// Raw information within the moving window that we are capturing these metrics
    raw_deliveries: TimeCache<MessageId, DeliveryData>
    /// The number of duplicates per peer in the moving window.
    current_duplicates_per_peer: HashMap<PeerId, usize>
}

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
struct BasicStat {
    /// The order relative to other peers that a duplicate has come in.
    order: usize,
    /// The latency (in ms) since the first message that we received this duplicate.
    latency: usize,
}

impl DeliveryData {
    /// Create a new instance of `DeliveryData`. This implies the message has first been received.
    pub fn new(peer_id: PeerId, time_received: Instant) -> Self {
        DeliveryData {
            first_sender: peer_id,
            time_received,
            duplicates: HashMap::with_capacity(5); // Assume most applications have at least 5 in the mesh.
       }
    }
}


impl EpisubMetrics {

    /// Record that a message has been received.
    pub fn message_received(&mut self, message_id: MessageId, peer_id: PeerId, received: Instant) {
        match raw_deliveries.entry_without_removal(message_id) {
            // This is the first time we have seen this message in this window
            Entry::Vacant(vacant_entry) => vacant_entry.insert(DeliveryData::new(peer_id, received))
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

        for (message_id, delivery_data) in self.raw_deliveries.iter() { 
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

    pub fn percentile_latency_per_peer(&mut self, percentile: u8) -> Vec<(PeerId, BasicStat)> {

    }

    /// Prunes expired data from the moving window.
    // This is used to handle current cumulative values. We can add/subtract values as we go for
    // more complex metrics.
    pub fn prune_expired_elements(&mut self) {
        while let Some((message_id, delivery_data)) = self.raw_deliveries.remove_expired() {
            for peer_id in delivery_data.duplicates {
                // Subtract an expired registered duplicate
                // NOTE: I want this to panic in debug mode, as it should never happen.
                *self.current_duplicates_per_peer.entry(peer_id) -= 1;
            }
        }
    }
}
