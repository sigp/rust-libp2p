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
    collections::{BinaryHeap, HashMap},
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
};

/// An async priority queue used to dispatch messages from the `NetworkBehaviour`
/// to the `ConnectionHandler`. Inspired by loole and flume.
#[derive(Debug)]
pub(crate) struct Queue<T> {
    shared: Arc<Mutex<Shared<T>>>,
    capacity: usize,
    id: usize,
    count: Arc<AtomicUsize>,
}

/// The shared stated by the `NetworkBehaviour`s and the `ConnectionHandler`s.
#[derive(Debug)]
pub(crate) struct Shared<T> {
    queue: BinaryHeap<T>,
    pending_pops: HashMap<usize, Waker>,
}

impl<T: Ord> Queue<T> {
    /// Create a new `Queue` with the `capacity`.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            shared: Arc::new(Mutex::new(Shared {
                queue: BinaryHeap::with_capacity(capacity),
                pending_pops: Default::default(),
            })),
            capacity,
            count: Arc::new(AtomicUsize::new(1)),
            id: 1,
        }
    }

    /// Try to add an item to the Queue, return Err if the queue is full.
    pub(crate) fn try_push(&mut self, item: T) -> Result<(), T> {
        let mut shared = self.shared.lock().unwrap();
        if self.capacity == shared.queue.len() {
            return Err(item);
        }
        shared.queue.push(item);
        // Wake pending registered pops.
        for (_, s) in shared.pending_pops.drain() {
            s.wake();
        }
        Ok(())
    }

    /// Pop an element from the queue.
    pub(crate) fn poll_pop(self: std::pin::Pin<&mut Self>, cx: &mut Context) -> Poll<T> {
        let mut shared = self.shared.lock().unwrap();
        match shared.queue.pop() {
            Some(t) => Poll::Ready(t),
            None => {
                shared
                    .pending_pops
                    .entry(self.id)
                    .or_insert(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    /// Attempts to pop an item from the queue.
    /// this method returns an error if the queue is empty.
    #[cfg(test)]
    pub(crate) fn try_pop(&mut self) -> Result<T, ()> {
        let mut shared = self.shared.lock().unwrap();
        shared.queue.pop().ok_or(())
    }

    /// Retain only the elements specified by the predicate.
    /// In other words, remove all elements e for which f(&e) returns false. The elements are visited in unsorted (and unspecified) order.
    /// Returns the cleared items.
    pub(crate) fn retain_mut<F: FnMut(&mut T) -> bool>(&mut self, mut f: F) -> Vec<T> {
        let mut shared = self.shared.lock().unwrap();
        // `BinaryHeap` doesn't impl `retain_mut`, this seems like a practical way to achieve it.
        // `BinaryHeap::drain` is O(n) as it returns an iterator over the removed elements in its internal arbitrary order.
        // `BinaryHeap::push` is ~O(1) which makes this function O(n).
        let mut queue = mem::replace(&mut shared.queue, BinaryHeap::with_capacity(self.capacity));
        let mut cleared = vec![];
        for mut item in queue.drain() {
            if f(&mut item) {
                shared.queue.push(item);
            } else {
                cleared.push(item);
            }
        }
        cleared
    }

    /// Returns the length of the queue.
    pub(crate) fn len(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.queue.len()
    }

    /// Check if the queue is empty.
    pub(crate) fn is_empty(&self) -> bool {
        let shared = self.shared.lock().unwrap();
        shared.queue.len() == 0
    }
}

impl<T> Clone for Queue<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            capacity: self.capacity,
            count: self.count.clone(),
            id: self.count.fetch_add(1, Ordering::SeqCst),
        }
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        let mut shared = self.shared.lock().unwrap();
        shared.pending_pops.remove(&self.id);
    }
}
