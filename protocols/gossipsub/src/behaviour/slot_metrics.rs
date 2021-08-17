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

use crate::TopicHash;
use log::{debug, error, warn};
use std::collections::{BTreeSet, HashMap};
use std::ops::AddAssign;

use libp2p_core::PeerId;
use strum::IntoEnumIterator;
use strum_macros::{EnumIter, IntoStaticStr};

#[derive(Default)]
pub struct SlotMetrics {
    assign_sum: u32,
    messages_all: u32,
    messages_first: u32,
    messages_ignored: u32,
    messages_rejected: u32,
    messages_validated: u32,
    churn_disconnected: u32,
    churn_excess: u32,
    churn_leave: u32,
    churn_prune: u32,
    churn_score: u32,
    churn_sum: u32,
    churn_unknown: u32,
    churn_unsubscribed: u32,
    current_peer: Option<Box<PeerId>>,
}

#[derive(IntoStaticStr, EnumIter, Clone, Copy)]
pub enum SlotMessageMetric {
    MessagesAll,
    MessagesFirst,
    MessagesIgnored,
    MessagesRejected,
    MessagesValidated,
}

#[derive(IntoStaticStr, EnumIter, Clone, Copy)]
pub enum SlotChurnMetric {
    ChurnDisconnected,
    ChurnExcess,
    ChurnLeave,
    ChurnPrune,
    ChurnScore,
    ChurnUnknown,
    ChurnUnsubscribed,
}

pub enum SlotMetricType {
    MessageMetric(SlotMessageMetric),
    ChurnMetric(SlotChurnMetric),
    ChurnSum,
    AssignSum,
}

impl SlotMetricType {
    pub fn iter() -> impl Iterator<Item = SlotMetricType> {
        SlotMessageMetric::iter()
            .map(|message_metric| SlotMetricType::MessageMetric(message_metric))
            .chain(
                SlotChurnMetric::iter()
                    .map(|churn_metric| SlotMetricType::ChurnMetric(churn_metric)),
            )
            .chain(std::iter::once(SlotMetricType::ChurnSum))
            .chain(std::iter::once(SlotMetricType::AssignSum))
    }
}

impl SlotMetrics {
    #[inline]
    fn get_message_metric(&self, message_metric: SlotMessageMetric) -> u32 {
        match message_metric {
            SlotMessageMetric::MessagesAll => self.messages_all,
            SlotMessageMetric::MessagesFirst => self.messages_first,
            SlotMessageMetric::MessagesIgnored => self.messages_ignored,
            SlotMessageMetric::MessagesRejected => self.messages_rejected,
            SlotMessageMetric::MessagesValidated => self.messages_validated,
        }
    }

    #[inline]
    fn increment_message_metric(&mut self, message_metric: SlotMessageMetric) {
        match message_metric {
            SlotMessageMetric::MessagesAll => self.messages_all.add_assign(1),
            SlotMessageMetric::MessagesFirst => self.messages_first.add_assign(1),
            SlotMessageMetric::MessagesIgnored => self.messages_ignored.add_assign(1),
            SlotMessageMetric::MessagesRejected => self.messages_rejected.add_assign(1),
            SlotMessageMetric::MessagesValidated => self.messages_validated.add_assign(1),
        };
    }

    #[inline]
    fn get_churn_metric(&self, churn_reason: SlotChurnMetric) -> u32 {
        match churn_reason {
            SlotChurnMetric::ChurnDisconnected => self.churn_disconnected,
            SlotChurnMetric::ChurnExcess => self.churn_excess,
            SlotChurnMetric::ChurnLeave => self.churn_leave,
            SlotChurnMetric::ChurnPrune => self.churn_prune,
            SlotChurnMetric::ChurnScore => self.churn_score,
            SlotChurnMetric::ChurnUnknown => self.churn_unknown,
            SlotChurnMetric::ChurnUnsubscribed => self.churn_unsubscribed,
        }
    }

    pub fn churn_slot(&mut self, churn_reason: SlotChurnMetric) -> u32 {
        self.current_peer = None;
        self.churn_sum.add_assign(1);
        match churn_reason {
            SlotChurnMetric::ChurnDisconnected => self.churn_disconnected.add_assign(1),
            SlotChurnMetric::ChurnExcess => self.churn_excess.add_assign(1),
            SlotChurnMetric::ChurnLeave => self.churn_leave.add_assign(1),
            SlotChurnMetric::ChurnPrune => self.churn_prune.add_assign(1),
            SlotChurnMetric::ChurnScore => self.churn_score.add_assign(1),
            SlotChurnMetric::ChurnUnknown => self.churn_unknown.add_assign(1),
            SlotChurnMetric::ChurnUnsubscribed => self.churn_unsubscribed.add_assign(1),
        };
        self.churn_sum
    }

    pub fn current_peer(&self) -> &Option<Box<PeerId>> {
        &self.current_peer
    }

    pub fn assign_slot(&mut self, peer: PeerId) -> u32 {
        self.current_peer = Some(Box::new(peer));
        self.assign_sum.add_assign(1);
        self.assign_sum
    }

    pub fn get_slot_metric(&self, metric_type: SlotMetricType) -> u32 {
        match metric_type {
            SlotMetricType::MessageMetric(message_metric) => {
                self.get_message_metric(message_metric)
            }
            SlotMetricType::ChurnMetric(churn_reason) => self.get_churn_metric(churn_reason),
            SlotMetricType::ChurnSum => self.churn_sum,
            SlotMetricType::AssignSum => self.assign_sum,
        }
    }

    pub fn type_iter() -> impl Iterator<Item = SlotMetricType> {
        SlotMetricType::iter()
    }

    pub fn with_names(&self) -> Vec<(&'static str, u32)> {
        SlotMetricType::iter()
            .map(|t| match t {
                SlotMetricType::MessageMetric(message_metric) => (
                    <SlotMessageMetric as Into<&'static str>>::into(message_metric),
                    self.get_message_metric(message_metric),
                ),
                SlotMetricType::ChurnMetric(churn_reason) => (
                    <SlotChurnMetric as Into<&'static str>>::into(churn_reason),
                    self.get_churn_metric(churn_reason),
                ),
                SlotMetricType::ChurnSum => ("ChurnSum", self.churn_sum),
                SlotMetricType::AssignSum => ("AssignSum", self.assign_sum),
            })
            .collect()
    }

    pub fn new() -> Self {
        SlotMetrics::default()
    }
}

pub type MeshSlot = usize;
pub struct MeshSlotData {
    /// The topic this MeshSlotData belongs to (useful for debugging)
    topic: TopicHash,
    /// Vector of SlotMetrics (vector indexed by MeshSlot)
    metrics_vec: Vec<SlotMetrics>,
    /// Map of PeerId to MeshSlot
    slot_map: HashMap<PeerId, MeshSlot>,
    /// Set of Vacant MeshSlots (due to peers leaving the mesh)
    vacant_slots: BTreeSet<MeshSlot>,
}

impl MeshSlotData {
    pub fn new(topic: TopicHash) -> Self {
        MeshSlotData {
            topic,
            // the first element in the vector is for peers that aren't in the mesh
            metrics_vec: vec![SlotMetrics::new()],
            slot_map: HashMap::new(),
            vacant_slots: BTreeSet::new(),
        }
    }

    /// Increments the message metric for the specified peer
    pub fn increment_message_metric(&mut self, peer: &PeerId, message_metric: SlotMessageMetric) {
        let slot = self
            .slot_map
            .get(peer)
            .map(|s| *s)
            // peers that aren't in the mesh get slot 0
            .unwrap_or(0);
        match self
            .metrics_vec
            .get_mut(slot)
        {
            Some(slot_metrics) => slot_metrics.increment_message_metric(message_metric),
            None => error!(
                "metrics_event[{}]: [slot {:02}] increment {} peer {} FAILURE [mesh_slots contains peer with slot not existing in mesh_slot_metrics!]",
                self.topic,
                slot,
                <SlotMessageMetric as Into<&'static str>>::into(message_metric),
                peer,
            ),
        };
    }

    /// Assigns a slot to the peer if the peer doesn't already have one.
    pub fn assign_slot_if_unassigned(&mut self, peer: PeerId) {
        if let std::collections::hash_map::Entry::Vacant(entry) = self.slot_map.entry(peer) {
            match self.vacant_slots.iter().next() {
                Some(slot_ref) => match self.metrics_vec.get_mut(*slot_ref) {
                    Some(slot_metrics) => {
                        let slot = *slot_ref;
                        let assign_sum = slot_metrics.assign_slot(peer);
                        self.vacant_slots.remove(&slot);
                        entry.insert(slot);
                        debug!(
                            "metrics_event[{}]: [slot {:02}] assigning vacant slot to peer {} SUCCESS AssignSum[{}]",
                                self.topic, slot, peer, assign_sum,
                        );
                    },
                    None => error!(
                        "metrics_event[{}]: [slot {:02}] assigning vacant slot to peer {} FAILURE [SlotMetrics doesn't exist in metrics vector!]",
                            self.topic, slot_ref, peer
                    ),
                },
                None => {
                    let slot = self.metrics_vec.len();
                    let mut slot_metrics = SlotMetrics::new();
                    let assign_sum = slot_metrics.assign_slot(peer);
                    self.metrics_vec.push(slot_metrics);
                    entry.insert(slot);
                    debug!(
                        "metrics_event[{}]: [slot {:02}] assigning new slot to peer {} SUCCESS AssignSum[{}]",
                            self.topic, slot, peer, assign_sum,
                    );
                }
            };
        }
    }

    pub fn assign_slots_to_peers<U>(&mut self, peer_iter: U)
    where
        U: Iterator<Item = PeerId>,
    {
        for peer in peer_iter {
            self.assign_slot_if_unassigned(peer);
        }
    }

    /// Churns the slot occupied by peer.
    pub fn churn_slot(&mut self, peer: &PeerId, churn_reason: SlotChurnMetric) {
        match self.slot_map.get(peer).cloned() {
            Some(slot) => match self.metrics_vec.get_mut(slot) {
                Some(slot_metrics) => {
                    debug_assert!(!self.vacant_slots.contains(&slot),
                        "metrics_event[{}] [slot {:02}] increment {} peer {} FAILURE [vacant slots already contains this slot!]",
                            self.topic, slot, <SlotChurnMetric as Into<&'static str>>::into(churn_reason), peer
                    );
                    let churn_sum = slot_metrics.churn_slot(churn_reason);
                    self.vacant_slots.insert(slot);
                    self.slot_map.remove(peer);
                    debug!(
                        "metrics_event[{}]: [slot {:02}] increment {} peer {} SUCCESS ChurnSum[{}]",
                            self.topic, slot, <SlotChurnMetric as Into<&'static str>>::into(churn_reason), peer, churn_sum,
                    );
                },
                None => warn!(
                    "metrics_event[{}]: [slot {:02}] increment {} peer {} FAILURE [retrieving slot_metrics]",
                        self.topic, slot, <SlotChurnMetric as Into<&'static str>>::into(churn_reason), peer
                ),
            },
            None => warn!(
                "metrics_event[{}]: [slot --] increment {} peer {} FAILURE [retrieving slot]",
                    self.topic, <SlotChurnMetric as Into<&'static str>>::into(churn_reason), peer
            ),
        };
    }

    /// Churns all slots in this topic that aren't already vacant (while incrementing
    /// churn_reason). Also clears the slot_map. This loop is faster than doing this individually
    /// for each peer in the topic because it minimizes redundant lookups and only traverses
    /// a vector.
    pub fn churn_all_slots(&mut self, churn_reason: SlotChurnMetric) {
        for slot in (1..self.metrics_vec.len())
            .filter(|s| !self.vacant_slots.contains(s))
            .collect::<Vec<_>>()
        {
            match self.metrics_vec.get_mut(slot) {
                Some(slot_metrics) => {
                    let previous = slot_metrics.current_peer().as_ref().map(|p| **p);
                    let churn_sum = slot_metrics.churn_slot(churn_reason);
                    self.vacant_slots.insert(slot);
                    match previous {
                        Some(peer) => debug!(
                            "metrics_event[{}]: [slot {:02}] increment {} peer {} SUCCESS ChurnSum[{}]",
                                self.topic, slot, <SlotChurnMetric as Into<&'static str>>::into(churn_reason), peer, churn_sum,
                        ),
                        None => warn!(
                            "metrics_event[{}]: [slot {:02}] increment {} WARNING [current_peer not assigned with non-vacant slot!] ChurnSum[{}]",
                                self.topic, slot, <SlotChurnMetric as Into<&'static str>>::into(churn_reason), churn_sum,
                        ),
                    };
                },
                None => error!(
                    "metrics_event[{}]: [slot {:02}] increment {} FAILURE [mesh_slots contains peer with slot not existing in mesh_slot_metrics!]",
                    self.topic, slot, <SlotChurnMetric as Into<&'static str>>::into(churn_reason),
                ),
            };
        }
        self.slot_map.clear();
    }

    /// This function verifies that the MeshSlotData is synchronized perfectly with the mesh.
    /// It's useful for debugging.
    #[allow(dead_code)]
    pub fn validate_mesh_slots(&self, mesh: &BTreeSet<PeerId>) -> Result<(), String> {
        let mut result = true;
        let mut errors = String::new();
        // No peers are in the slot_map that aren't in the mesh
        for (peer, slot) in self
            .slot_map
            .iter()
            .filter(|(peer, ..)| !mesh.contains(peer))
        {
            result = false;
            let message = format!(
                "metrics_event[{}]: [slot {:02}] peer {} exists in slot_map but not in the mesh!\n",
                self.topic, slot, peer
            );
            errors.push_str(message.as_str());
            error!("{}", message);
        }
        // No peers are in the mesh that aren't in the slot_map
        for peer in mesh.iter().filter(|peer| !self.slot_map.contains_key(peer)) {
            result = false;
            let message = format!(
                "metrics_event[{}]: [slot --] peer {} exists in mesh but not in the slot_map!\n",
                self.topic, peer
            );
            errors.push_str(message.as_str());
            error!("{}", message);
        }

        // vacant_slots.len() + slot_map.len() == metrics_vec.len() + 1
        if self.vacant_slots.len() + self.slot_map.len() + 1 != self.metrics_vec.len() {
            result = false;
            let message = format!(
                "metrics_event[{}] vacant_slots.len()[{}] + slot_map.len()[{}] + 1 != metrics_vec.len()[{}]",
                    self.topic, self.vacant_slots.len(), self.slot_map.len(), self.metrics_vec.len(),
            );
            errors.push_str(message.as_str());
            error!("{}", message);
        }

        if result {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn slot_iter(&self) -> impl Iterator<Item = &SlotMetrics> {
        self.metrics_vec.iter()
    }
}
