use rand::Rng;
use sha3::{Digest, Keccak256};
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::timer::DelayQueue;

/// Max allowed node entries across all topics.
const MAX_ENTRIES: usize = 1000;
/// Max allowed entries within a topic queue.
const MAX_ENTRIES_PER_TOPIC: usize = 50;

pub type TopicHash = [u8; 32];

/// TODO: change to some unique identifier type
pub type TicketId = Vec<u8>;

/// Representation of a ticket issued to peer for topic registration.
#[derive(Debug, Clone)]
pub struct Ticket<TNodeId> {
    /// Unique identifier for ticket.
    pub id: TicketId,
    /// Topic hash for ticket.
    pub topic_hash: TopicHash,
    /// Peer id that sent the ticket or we sent the ticket to.
    pub node_id: TNodeId,
    /// Wait time for ticket to be allowed for topic registration.
    pub wait_time: Duration,
    /// Time instant at which ticket was registered
    pub created_time: Instant,
}

impl<TNodeId> Ticket<TNodeId> {
    pub fn new(
        topic_hash: TopicHash,
        node_id: TNodeId,
        wait_time: u64,
        id: Option<TicketId>,
    ) -> Self {
        let mut rng = rand::thread_rng();
        let ticket_id: u64 = rng.gen();
        Ticket {
            id: id.unwrap_or(ticket_id.to_le_bytes().to_vec()), // TODO
            topic_hash,
            node_id: node_id,
            wait_time: Duration::from_secs(wait_time),
            created_time: Instant::now(),
        }
    }

    /// Checks if wait time for ticket has passed.
    pub fn has_wait_elapsed(&self) -> bool {
        if self.created_time + self.wait_time < Instant::now() {
            return false;
        } else {
            return true;
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct Topic(String);

impl Topic {
    pub fn get_topic_hash(&self) -> TopicHash {
        let mut hasher = Keccak256::new();
        hasher.input(&self.0);
        let mut result: [u8; 32] = std::default::Default::default();
        result.clone_from_slice(hasher.result().as_slice());
        result
    }
}

#[derive(Debug)]
pub struct TopicQueue<TNodeId> {
    topic: TopicHash,
    queue: VecDeque<(TNodeId, Instant)>,
}

impl<TNodeId> TopicQueue<TNodeId> {
    pub fn new(topic: TopicHash) -> Self {
        TopicQueue {
            topic,
            queue: VecDeque::with_capacity(MAX_ENTRIES_PER_TOPIC),
        }
    }

    pub fn size(&self) -> usize {
        self.queue.len()
    }

    /// Add a peer to the topic queue.
    /// TODO: make queue a set
    pub fn add_to_queue(&mut self, peer: TNodeId) {
        if self.queue.len() == MAX_ENTRIES_PER_TOPIC {
            self.remove_from_queue();
        }
        self.queue.push_back((peer, Instant::now()));
    }

    /// Remove element from queue according to some policy
    /// TODO
    pub fn remove_from_queue(&mut self) {
        unimplemented!()
    }

    /// Get wait time for queue.
    /// TODO
    pub fn get_wait_time(&self) -> u64 {
        unimplemented!()
    }
}

/// Global queue containing all topic queues and issued tickets
/// TODO: Change name to something less atrocious
#[derive(Debug)]
pub struct GlobalTopicQueue<TNodeId> {
    pub topic_map: BTreeMap<TopicHash, TopicQueue<TNodeId>>,
    /// Tickets that were issued by us to other peers.
    pub sent_tickets: BTreeMap<TicketId, Ticket<TNodeId>>,
    /// Tickets that we received from other peers.
    pub received_tickets: DelayQueue<Ticket<TNodeId>>,
}

impl<TNodeId> GlobalTopicQueue<TNodeId>
where
    TNodeId: Clone + std::fmt::Debug,
{
    pub fn new() -> Self {
        GlobalTopicQueue {
            topic_map: BTreeMap::new(),
            sent_tickets: BTreeMap::new(),
            received_tickets: DelayQueue::new(),
        }
    }

    /// Get combined size of all peers across all topic queues.
    fn get_queue_size(&self) -> usize {
        self.topic_map.iter().map(|(_, v)| v.size()).sum()
    }

    /// Add a peer to the topic queue.
    /// Returns None if ticket doesn't exist or wait time hasn't elapsed.
    pub fn add_to_queue(&mut self, peer: &TNodeId, ticket: &TicketId) -> Option<()> {
        if !self.is_ticket_valid(ticket) {
            return None;
        }
        let topic = self.sent_tickets.get(ticket)?.topic_hash.clone();

        if self.get_queue_size() == MAX_ENTRIES {
            self.remove_from_queue();
        }
        if let Some(queue) = self.topic_map.get_mut(&topic) {
            queue.add_to_queue(peer.clone());
        } else {
            let mut tq = TopicQueue::new(topic.clone());
            tq.add_to_queue(peer.clone());
            self.topic_map.insert(topic, tq);
        };
        Some(())
    }

    /// Remove element from one of the topic queues according to some policy
    /// TODO
    pub fn remove_from_queue(&mut self) {
        unimplemented!()
    }

    pub fn issue_ticket(&mut self, peer: &TNodeId, topic: TopicHash) -> (TicketId, u64) {
        let wait_time = self
            .topic_map
            .get(&topic)
            .map(|v| v.get_wait_time())
            .unwrap_or(5);
        let ticket = Ticket::new(topic, peer.clone(), wait_time, None);
        self.sent_tickets.insert(ticket.id.clone(), ticket.clone());
        (ticket.id, wait_time)
    }

    /// Checks if ticket is registered in map and the wait time has elapsed.
    pub fn is_ticket_valid(&self, ticket_id: &TicketId) -> bool {
        if let Some(ticket) = self.sent_tickets.get(ticket_id) {
            ticket.has_wait_elapsed();
        }
        false
    }

    /// Add received ticket to map.
    pub fn register_received_ticket(
        &mut self,
        topic: &TopicHash,
        ticket_id: &Vec<u8>,
        wait_time: u64,
        peer: &TNodeId,
    ) {
        let ticket = Ticket::new(
            topic.clone(),
            peer.clone(),
            wait_time,
            Some(ticket_id.clone()),
        );
        // TODO: check if ticket already present and handle appropriately.
        self.received_tickets
            .insert(ticket, Duration::from_secs(wait_time));
    }
}
