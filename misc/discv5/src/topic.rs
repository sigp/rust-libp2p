use sha3::{Digest, Keccak256};
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Max allowed node entries across all topics.
const MAX_ENTRIES: usize = 1000;
/// Max allowed entries within a topic queue.
const MAX_ENTRIES_PER_TOPIC: usize = 50;

pub type TopicHash = [u8; 32];
pub type Topic = String;

fn hash256_to_fixed_array(s: &str) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.input(s);
    let mut result: [u8; 32] = std::default::Default::default();
    result.clone_from_slice(hasher.result().as_slice());
    result
}

/// Representation of a ticket issued to peer for topic registration.
#[derive(Debug, Clone)]
pub struct Ticket<TPeerId> {
    /// Unique identifier for ticket.
    /// TODO: change to some unique identifier type
    id: String,
    /// Id of peer to which ticket is issued.
    peer_id: TPeerId,
    /// Wait time for ticket to be allowed for topic registration.
    wait_time: Duration,
    /// Time instant at which ticket was registered
    created_time: Instant,
}

impl<TPeerId> Ticket<TPeerId> {
    pub fn new(peer: TPeerId, wait_time: u64) -> Self {
        Ticket {
            id: String::from("test"), // TODO
            peer_id: peer,
            wait_time: Duration::from_secs(wait_time),
            created_time: Instant::now(),
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct TopicInfo {
    /// Topic name.
    topic: Topic,
    /// Hash of topic for topic search.
    hash: TopicHash,
}
impl TopicInfo {
    pub fn new(topic: String) -> Self {
        TopicInfo {
            topic: topic.clone(),
            hash: hash256_to_fixed_array(&topic),
        }
    }
}

#[derive(Debug)]
pub struct TopicQueue<TPeerId> {
    topic: TopicInfo,
    queue: VecDeque<(TPeerId, Instant)>,
}

impl<TPeerId> TopicQueue<TPeerId> {
    pub fn new(topic: String) -> Self {
        let topic_info = TopicInfo::new(topic);
        TopicQueue {
            topic: topic_info,
            queue: VecDeque::with_capacity(MAX_ENTRIES_PER_TOPIC),
        }
    }

    pub fn size(&self) -> usize {
        self.queue.len()
    }

    /// Add a peer to the topic queue.
    pub fn add_to_queue(&mut self, peer: TPeerId) {
        if self.queue.len() == MAX_ENTRIES_PER_TOPIC {
            self.remove_from_queue();
        }
        self.queue.push_back((peer, Instant::now()));
    }

    /// Remove element from queue according to some policy
    pub fn remove_from_queue(&mut self) {
        unimplemented!()
    }
}

/// Global queue containing all topic queues and issued tickets
/// TODO: Change name to something less atrocious
#[derive(Debug)]
pub struct GlobalTopicQueue<TPeerId> {
    topic_map: BTreeMap<Topic, TopicQueue<TPeerId>>,
    tickets: Vec<Ticket<TPeerId>>,
}

impl<TPeerId> GlobalTopicQueue<TPeerId> {
    pub fn new() -> Self {
        GlobalTopicQueue {
            topic_map: BTreeMap::new(),
            tickets: Vec::new(),
        }
    }

    fn get_queue_size(&self) -> usize {
        self.topic_map.iter().map(|(_, v)| v.size()).sum()
    }

    /// Add a peer to the topic queue.
    pub fn add_to_queue(&mut self, peer: TPeerId, topic: Topic) {
        if self.get_queue_size() == MAX_ENTRIES {
            self.remove_from_queue();
        }
        if let Some(queue) = self.topic_map.get_mut(&topic) {
            queue.add_to_queue(peer);
        }
        else {
            let mut tq = TopicQueue::new(topic.clone());
            tq.add_to_queue(peer);
            self.topic_map.insert(topic, tq);
        };
    }

    /// Remove element from one of the queues according to some policy
    pub fn remove_from_queue(&mut self) {
        unimplemented!()
    }
}
