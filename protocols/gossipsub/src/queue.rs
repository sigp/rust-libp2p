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

use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc, Mutex},
    task::{Context, Poll, Waker},
};

use crate::{types::RpcOut, MessageId};

const CONTROL_MSGS_LIMIT: usize = 20_000;

/// An async priority queue used to dispatch messages from the `NetworkBehaviour`
/// Provides a clean abstraction over high-priority (unbounded), control (bounded),
/// and low-priority (bounded) message queues.
#[derive(Debug)]
pub(crate) struct Queue {
    /// High-priority unbounded queue (Subscribe, Unsubscribe)
    pub(crate) high_priority: Shared,
    /// Control messages bounded queue (Graft, Prune, IDontWant)
    pub(crate) control: Shared,
    /// Low-priority bounded queue (Publish, Forward, IHave, IWant)
    pub(crate) low_priority: Shared,
    /// The id of the current reference of the counter.
    pub(crate) id: usize,
    /// The total number of references for the queue.
    pub(crate) count: Arc<AtomicUsize>,
}

impl Queue {
    /// Create a new `Queue` with the `capacity`.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            high_priority: Shared::new(),
            control: Shared::with_capacity(CONTROL_MSGS_LIMIT),
            low_priority: Shared::with_capacity(capacity),
            id: 1,
            count: Arc::new(AtomicUsize::new(1)),
        }
    }

    /// Try to add an item to the Queue, return Err if the queue is full.
    pub(crate) fn try_push(&mut self, message: RpcOut) -> Result<(), RpcOut> {
        match message {
            RpcOut::Subscribe(_) | RpcOut::Unsubscribe(_) => {
                self.high_priority
                    .try_push(message)
                    .expect("Shared is unbounded");
                Ok(())
            }
            RpcOut::Graft(_) | RpcOut::Prune(_) | RpcOut::IDontWant(_) => {
                self.control.try_push(message)
            }
            RpcOut::Publish { .. }
            | RpcOut::Forward { .. }
            | RpcOut::IHave(_)
            | RpcOut::IWant(_) => self.low_priority.try_push(message),
        }
    }

    /// Remove pending low piority Publish and Forward messages.
    pub(crate) fn remove_data_messages(&mut self, message_ids: &[MessageId]) {
        self.low_priority.retain(|message| match message {
            RpcOut::Publish { message_id, .. } | RpcOut::Forward { message_id, .. } => {
                !message_ids.contains(message_id)
            }
            _ => true,
        })
    }

    /// Pop an element from the queue.
    pub(crate) fn poll_pop(&mut self, cx: &mut Context) -> Poll<RpcOut> {
        // First we try the high priority messages.
        if let Poll::Ready(rpc) = Pin::new(&mut self.high_priority).poll_pop(cx) {
            return Poll::Ready(rpc);
        }

        // Then we try the control messages.
        if let Poll::Ready(rpc) = Pin::new(&mut self.control).poll_pop(cx) {
            return Poll::Ready(rpc);
        }

        // Finaly we try the low priority messages
        if let Poll::Ready(rpc) = Pin::new(&mut self.low_priority).poll_pop(cx) {
            return Poll::Ready(rpc);
        }

        Poll::Pending
    }

    /// Check if the queue is empty.
    pub(crate) fn is_empty(&self) -> bool {
        if !self.high_priority.is_empty() {
            return false;
        }

        if !self.control.is_empty() {
            return false;
        }

        if !self.low_priority.is_empty() {
            return false;
        }

        true
    }

    /// Returns the length of the queue.
    #[cfg(feature = "metrics")]
    pub(crate) fn len(&self) -> usize {
        self.high_priority.len() + self.control.len() + self.low_priority.len()
    }

    /// Attempts to pop an item from the queue.
    /// this method returns an error if the queue is empty.
    #[cfg(test)]
    pub(crate) fn try_pop(&mut self) -> Result<RpcOut, ()> {
        // Try high priority first
        if let Ok(msg) = self.high_priority.try_pop() {
            return Ok(msg);
        }

        // Then control messages
        if let Ok(msg) = self.control.try_pop() {
            return Ok(msg);
        }

        // Finally low priority
        if let Ok(msg) = self.low_priority.try_pop() {
            return Ok(msg);
        }

        Err(())
    }
}

impl Clone for Queue {
    fn clone(&self) -> Self {
        let new_id = self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self {
            high_priority: Shared {
                inner: self.high_priority.inner.clone(),
                capacity: self.high_priority.capacity,
                id: new_id,
            },
            control: Shared {
                inner: self.control.inner.clone(),
                capacity: self.control.capacity,
                id: new_id,
            },
            low_priority: Shared {
                inner: self.low_priority.inner.clone(),
                capacity: self.low_priority.capacity,
                id: new_id,
            },
            id: self.id.clone(),
            count: self.count.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Shared {
    inner: Arc<Mutex<SharedInner>>,
    capacity: Option<usize>,
    id: usize,
}

impl Shared {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SharedInner {
                queue: VecDeque::new(),
                pending_pops: Default::default(),
            })),
            capacity: Some(capacity),
            id: 1,
        }
    }

    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SharedInner {
                queue: VecDeque::new(),
                pending_pops: Default::default(),
            })),
            capacity: None,
            id: 1,
        }
    }

    /// Pop an element from the queue.
    pub(crate) fn poll_pop(self: std::pin::Pin<&mut Self>, cx: &mut Context) -> Poll<RpcOut> {
        let mut guard = self.inner.lock().expect("lock to not be poisoned");
        match guard.queue.pop_front() {
            Some(t) => Poll::Ready(t),
            None => {
                guard
                    .pending_pops
                    .entry(self.id)
                    .or_insert(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    pub(crate) fn try_push(&mut self, message: RpcOut) -> Result<(), RpcOut> {
        let mut guard = self.inner.lock().expect("lock to not be poisoned");
        if self
            .capacity
            .is_some_and(|capacity| guard.queue.len() >= capacity)
        {
            return Err(message);
        }

        guard.queue.push_back(message);
        // Wake pending registered pops.
        for (_, s) in guard.pending_pops.drain() {
            s.wake();
        }

        Ok(())
    }

    /// Retain only the elements specified by the predicate.
    /// In other words, remove all elements e for which f(&e) returns false. The elements are
    /// visited in unsorted (and unspecified) order. Returns the cleared items.
    pub(crate) fn retain<F: FnMut(&RpcOut) -> bool>(&mut self, f: F) {
        let mut shared = self.inner.lock().expect("lock to not be poisoned");
        shared.queue.retain(f);
    }

    /// Check if the queue is empty.
    pub(crate) fn is_empty(&self) -> bool {
        let guard = self.inner.lock().expect("lock to not be poisoned");
        guard.queue.len() == 0
    }

    /// Returns the length of the queue.
    #[cfg(feature = "metrics")]
    pub(crate) fn len(&self) -> usize {
        let guard = self.inner.lock().expect("lock to not be poisoned");
        guard.queue.len()
    }

    /// Attempts to pop an item from the queue.
    /// this method returns an error if the queue is empty.
    #[cfg(test)]
    pub(crate) fn try_pop(&mut self) -> Result<RpcOut, ()> {
        let mut guard = self.inner.lock().expect("lock to not be poisoned");
        guard.queue.pop_front().ok_or(())
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        let mut guard = self.inner.lock().expect("lock to not be poisoned");
        guard.pending_pops.remove(&self.id);
    }
}

/// The shared stated by the `NetworkBehaviour`s and the `ConnectionHandler`s.
#[derive(Debug)]
struct SharedInner {
    queue: VecDeque<RpcOut>,
    pending_pops: HashMap<usize, Waker>,
}
