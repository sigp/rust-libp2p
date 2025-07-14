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
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
};

use crate::types::RpcOut;


/// Control message queue capacity limit.
const CONTROL_QUEUE_CAPACITY: usize = 10_000;

/// A three-tier message queue system optimized for gossipsub RPC dispatching.
/// Provides clean abstraction over high-priority (unbounded), control (bounded), 
/// and low-priority (bounded) message queues.
#[derive(Debug)]
pub(crate) struct RpcQueue {
    shared: Arc<Mutex<RpcQueueShared>>,
    low_priority_capacity: usize,
    id: usize,
    count: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct RpcQueueShared {
    /// High-priority unbounded queue (Subscribe, Unsubscribe)
    high_priority: VecDeque<RpcOut>,
    /// Control messages bounded queue (Graft, Prune, IDontWant)
    control: VecDeque<RpcOut>,
    /// Low-priority bounded queue (Publish, Forward, IHave, IWant)
    low_priority: VecDeque<RpcOut>,
    /// Pending pop operations waiting for messages
    pending_pops: HashMap<usize, Waker>,
}

impl RpcQueue {
    /// Create a new three-tier RPC queue with specified low-priority capacity.
    pub(crate) fn new(low_priority_capacity: usize) -> Self {
        Self {
            shared: Arc::new(Mutex::new(RpcQueueShared {
                high_priority: VecDeque::new(),
                control: VecDeque::with_capacity(CONTROL_QUEUE_CAPACITY),
                low_priority: VecDeque::with_capacity(low_priority_capacity),
                pending_pops: HashMap::new(),
            })),
            low_priority_capacity,
            id: 1,
            count: Arc::new(AtomicUsize::new(1)),
        }
    }

    /// Push a message to the appropriate queue based on its priority.
    /// High-priority messages are always accepted (unbounded).
    /// Control and low-priority messages may be rejected if their respective queues are full.
    pub(crate) fn try_push(&mut self, message: RpcOut) -> Result<(), RpcOut> {
        let mut shared = self.shared.lock().expect("lock to not be poisoned");
        
        match Self::classify_message(&message) {
            MessagePriority::High => {
                // High priority queue is unbounded
                shared.high_priority.push_back(message);
            }
            MessagePriority::Control => {
                if shared.control.len() >= CONTROL_QUEUE_CAPACITY {
                    return Err(message);
                }
                shared.control.push_back(message);
            }
            MessagePriority::Low => {
                if shared.low_priority.len() >= self.low_priority_capacity {
                    return Err(message);
                }
                shared.low_priority.push_back(message);
            }
        }

        // Wake all pending pops since we added a message
        for (_, waker) in shared.pending_pops.drain() {
            waker.wake();
        }
        
        Ok(())
    }


    /// Poll for the next message, prioritizing high -> control -> low priority queues.
    pub(crate) fn poll_pop(self: std::pin::Pin<&mut Self>, cx: &mut Context) -> Poll<RpcOut> {
        let mut shared = self.shared.lock().expect("lock to not be poisoned");
        
        // Check high priority first
        if let Some(message) = shared.high_priority.pop_front() {
            return Poll::Ready(message);
        }
        
        // Then control messages
        if let Some(message) = shared.control.pop_front() {
            return Poll::Ready(message);
        }
        
        // Finally low priority
        if let Some(message) = shared.low_priority.pop_front() {
            return Poll::Ready(message);
        }
        
        // No messages available, register waker
        shared.pending_pops.entry(self.id).or_insert(cx.waker().clone());
        Poll::Pending
    }


    /// Optimized retain for low-priority messages - only searches the low-priority queue.
    pub(crate) fn retain_low_priority<F>(&mut self, mut predicate: F)
    where
        F: FnMut(&RpcOut) -> bool,
    {
        let mut shared = self.shared.lock().expect("lock to not be poisoned");
        shared.low_priority.retain(&mut predicate);
    }

    /// Check if all queues are empty.
    pub(crate) fn is_empty(&self) -> bool {
        let shared = self.shared.lock().expect("lock to not be poisoned");
        shared.high_priority.is_empty() && shared.control.is_empty() && shared.low_priority.is_empty()
    }

    /// Get total number of messages across all queues.
    #[cfg(feature = "metrics")]
    pub(crate) fn len(&self) -> usize {
        let shared = self.shared.lock().expect("lock to not be poisoned");
        shared.high_priority.len() + shared.control.len() + shared.low_priority.len()
    }


    fn classify_message(message: &RpcOut) -> MessagePriority {
        match message {
            RpcOut::Subscribe(_) | RpcOut::Unsubscribe(_) => MessagePriority::High,
            RpcOut::Graft(_) | RpcOut::Prune(_) | RpcOut::IDontWant(_) => MessagePriority::Control,
            RpcOut::Publish { .. } | RpcOut::Forward { .. } | RpcOut::IHave(_) | RpcOut::IWant(_) => MessagePriority::Low,
        }
    }
}

impl Clone for RpcQueue {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            low_priority_capacity: self.low_priority_capacity,
            count: self.count.clone(),
            id: self.count.fetch_add(1, Ordering::SeqCst),
        }
    }
}

impl Drop for RpcQueue {
    fn drop(&mut self) {
        let mut shared = self.shared.lock().expect("lock to not be poisoned");
        shared.pending_pops.remove(&self.id);
    }
}

#[derive(Debug, Clone, Copy)]
enum MessagePriority {
    High,
    Control,
    Low,
}

