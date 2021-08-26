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

//! A set of metrics used to help track and diagnose the network behaviour of the gossipsub
//! protocol.

pub mod slot_metrics;

use crate::topic::TopicHash;
use libp2p_core::PeerId;
use log::warn;
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use self::slot_metrics::{MeshSlotData, SlotChurnMetric, SlotMetrics};

lazy_static! {
    pub static ref METRICS: InternalMetrics = InternalMetrics::default();
}

/// A collection of metrics used throughout the gossipsub behaviour.
pub struct InternalMetrics {
    /// Current metrics for all known mesh data. See [`MeshSlotData`] for further information.
    pub mesh_slot_data: RwLock<HashMap<TopicHash, MeshSlotData>>,
    /// The number of broken promises (this metric is indicative of nodes with invalid message-ids)
    pub broken_promises: AtomicUsize,
    /// The number of messages requested via IWANT (this metric indicates the mesh isn't performing
    /// as optimally as we would like, we have had to request for extra messages via gossip)
    pub iwant_requests: AtomicUsize,
    /// When the user validates a message, it tries to re propagate it to its mesh peers. If the
    /// message expires from the memcache before it can be validated, we count this a cache miss
    /// and it is an indicator that the memcache size should be increased.
    pub memcache_misses: AtomicUsize,
    /// The number of duplicate messages we are receiving and filtering. A large number could
    /// indicate a large amplification on a specific topic. Lowering the gossip_D parameter could
    /// help minimize duplicates.
    pub duplicates_filtered: RwLock<HashMap<TopicHash, usize>>,
}

impl Default for InternalMetrics {
    fn default() -> Self {
        InternalMetrics {
            mesh_slot_data: RwLock::new(HashMap::new()),
            broken_promises: AtomicUsize::new(0),
            iwant_requests: AtomicUsize::new(0),
            memcache_misses: AtomicUsize::new(0),
            duplicates_filtered: RwLock::new(HashMap::new()),
        }
    }
}

impl InternalMetrics {
    /// Returns the slot metrics for a given topic
    pub fn slot_metrics_for_topic(&self, topic: &TopicHash) -> Option<Vec<SlotMetrics>> {
        Some(self.mesh_slot_data.read().get(topic)?.slot_metrics())
    }

    /// Returns the current number of broken promises.
    pub fn broken_promises(&self) -> usize {
        self.broken_promises.load(Ordering::Relaxed)
    }

    /// Returns the current number of IWANT requests.
    pub fn iwant_requests(&self) -> usize {
        self.iwant_requests.load(Ordering::Relaxed)
    }

    /// Returns the current number of memcache misses.
    pub fn memcache_misses(&self) -> usize {
        self.memcache_misses.load(Ordering::Relaxed)
    }

    /// Returns the current number of duplicates filtered, for a given topic.
    pub fn duplicates_filtered(&self, topic: &TopicHash) -> Option<usize> {
        Some(self.duplicates_filtered.read().get(topic)?.clone())
    }

    /// Churns a slot in the mesh_slot_data. This assumes the peer is in the mesh.
    pub fn churn_slot(&self, topic: &TopicHash, peer_id: &PeerId, churn_reason: SlotChurnMetric) {
        match self.mesh_slot_data.write().get_mut(topic) {
            Some(slot_data) => slot_data.churn_slot(peer_id, churn_reason),
            None => {
                warn!(
                "metrics_event[{}]: [slot --] increment {} peer {} FAILURE [retrieving slot_data]",
                topic, <SlotChurnMetric as Into<&'static str>>::into(churn_reason), peer_id,
            )
            }
        }
    }

    /// Churn the slot for a peer, this may be a new topic so we add the topic if it does not
    /// already exist.
    pub fn new_churn_slot(
        &self,
        topic: &TopicHash,
        peer_id: &PeerId,
        churn_reason: SlotChurnMetric,
    ) {
        let mut write_lock = self.mesh_slot_data.write();
        let slot_data = write_lock
            .entry(topic.clone())
            .or_insert_with(|| MeshSlotData::new(topic.clone()));
        slot_data.churn_slot(peer_id, churn_reason);
    }

    /// Assign slots to peers.
    pub fn assign_slots_to_peers<U>(&self, topic: &TopicHash, peer_list: U)
    where
        U: Iterator<Item = PeerId>,
    {
        let mut write_lock = self.mesh_slot_data.write();
        let slot_data = write_lock
            .entry(topic.clone())
            .or_insert_with(|| MeshSlotData::new(topic.clone()));
        slot_data.assign_slots_to_peers(peer_list);
    }
}
