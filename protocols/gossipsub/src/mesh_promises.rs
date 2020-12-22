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

use crate::time_cache::ExpiringElement;
use crate::{MessageId, SemanticMessageId};
use libp2p_core::PeerId;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::time::{Duration, Instant};

/// A promise consists of a set of peers that are expected to satisfy the promise, the list of
/// message ids that are considered duplicates via the semantic message id, and the `from` peer that
/// sent us the first of the considered messages.
pub type MeshPromise = (BTreeSet<PeerId>, Vec<MessageId>, PeerId);

pub(crate) struct MeshPromises {
    /// An ordered list of promises by expires time (similar to time_cache).
    list: VecDeque<ExpiringElement<SemanticMessageId>>,
    /// The window for promises
    pub(crate) window: Duration,
    /// A mapping that maps semantic ids that we are currently tracking to a promise that contains
    /// the list of peers that need to satisfy the promise + a list of all message ids corresponding
    /// to this semantic id + the `from` peer.
    promises: HashMap<SemanticMessageId, MeshPromise>,
}

impl MeshPromises {
    pub fn new(window: Duration) -> Self {
        Self {
            promises: Default::default(),
            list: Default::default(),
            window,
        }
    }

    /// Add a new mesh promise to the data structure.
    pub fn add_promise(
        &mut self,
        semantic_message_id: SemanticMessageId,
        message_id: MessageId,
        mesh_peers: BTreeSet<PeerId>,
        from: &PeerId,
    ) {
        self.promises.insert(
            semantic_message_id.clone(),
            (mesh_peers, vec![message_id], from.clone()),
        );
        self.list.push_back(ExpiringElement {
            element: semantic_message_id,
            expires: Instant::now() + self.window,
        });
    }

    /// Registers a new `message_id` for some `semantic_message_id`. Returns `true` if the given
    /// `semantic_message_id` is currently tracked and `false` otherwise.
    pub fn try_add_duplicate_message_id(
        &mut self,
        semantic_message_id: &SemanticMessageId,
        message_id: &MessageId,
    ) -> bool {
        if let Some((_, message_ids, _)) = self.promises.get_mut(&semantic_message_id) {
            message_ids.push(message_id.clone());
            true
        } else {
            false
        }
    }

    /// Extracts the set of expired promises (does not check if any peers broke the promise or not).
    pub fn extract_promises(&mut self) -> Vec<MeshPromise> {
        let now = Instant::now();
        let mut result = Vec::new();
        while let Some(element) = self.list.pop_front() {
            if element.expires > now {
                self.list.push_front(element);
                break;
            } else {
                if let Some(promise) = self.promises.remove(&element.element) {
                    result.push(promise);
                }
            }
        }
        result
    }

    /// Checks whether the given `semantic_message_id` gets currently tracked.
    pub fn contains(&self, semantic_message_id: &SemanticMessageId) -> bool {
        self.promises.contains_key(semantic_message_id)
    }
}
