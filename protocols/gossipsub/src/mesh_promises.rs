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
use crate::MessageId;
use libp2p_core::PeerId;
use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

pub type MeshPromise = (MessageId, PeerId);

pub(crate) struct MeshPromises {
    /// Set of all stored promises.
    promises: HashSet<MeshPromise>,
    /// An ordered list of promises by expires time (similar to time_cache).
    list: VecDeque<ExpiringElement<MeshPromise>>,
    /// The window for promises
    pub(crate) window: Duration,
}

impl MeshPromises {
    pub fn new(window: Duration) -> Self {
        Self {
            promises: Default::default(),
            list: Default::default(),
            window,
        }
    }

    pub fn add_promise(&mut self, message_id: MessageId, peer_id: PeerId) {
        let promise = (message_id, peer_id);
        self.promises.insert(promise.clone());
        self.list.push_back(ExpiringElement {
            element: promise,
            expires: Instant::now() + self.window,
        });
    }

    /// Returns true iff there was an open promise for this message id and this peer to resolve
    pub fn resolve_promise(&mut self, message_id: &MessageId, peer_id: &PeerId) -> bool {
        let promise = (message_id.clone(), peer_id.clone());
        self.promises.remove(&promise)
    }

    pub fn extract_broken_promises(&mut self) -> Vec<MeshPromise> {
        let now = Instant::now();
        let mut result = Vec::new();
        while let Some(element) = self.list.pop_front() {
            if element.expires > now {
                self.list.push_front(element);
                break;
            } else {
                self.promises.remove(&element.element);
                result.push(element.element);
            }
        }
        result
    }
}
