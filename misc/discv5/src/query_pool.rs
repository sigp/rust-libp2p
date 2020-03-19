// Copyright 2019 Parity Technologies (UK) Ltd.
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

mod peers;

use enr::{CombinedKey, Enr, NodeId};
pub use peers::closest::{FindNodeQuery, FindNodeQueryConfig};
pub use peers::predicate::{PredicateQuery, PredicateQueryConfig};
pub use peers::{QueryState, ReturnPeer};

use crate::kbucket::Key;
use fnv::FnvHashMap;
use std::time::Instant;

/// A `QueryPool` provides an aggregate state machine for driving `Query`s to completion.
///
/// Internally, a `Query` is in turn driven by an underlying `QueryPeerIter`
/// that determines the peer selection strategy, i.e. the order in which the
/// peers involved in the query should be contacted.
pub struct QueryPool<TTarget, TNodeId> {
    next_id: usize,
    queries: FnvHashMap<QueryId, Query<TTarget, TNodeId>>,
}

/// The observable states emitted by [`QueryPool::poll`].
pub enum QueryPoolState<'a, TTarget, TNodeId> {
    /// The pool is idle, i.e. there are no queries to process.
    Idle,
    /// At least one query is waiting for results. `Some(request)` indicates
    /// that a new request is now being waited on.
    Waiting(Option<(&'a mut Query<TTarget, TNodeId>, ReturnPeer<TNodeId>)>),
    /// A query has finished.
    Finished(Query<TTarget, TNodeId>),
}

impl<TTarget, TNodeId> QueryPool<TTarget, TNodeId>
where
    TTarget: Into<Key<TTarget>> + Clone,
    TNodeId: Into<Key<TNodeId>> + Eq + Clone,
{
    /// Creates a new `QueryPool` with the given configuration.
    pub fn new() -> Self {
        QueryPool {
            next_id: 0,
            queries: Default::default(),
        }
    }

    /// Returns an iterator over the queries in the pool.
    pub fn iter(&self) -> impl Iterator<Item = &Query<TTarget, TNodeId>> {
        self.queries.values()
    }

    /// Adds a query to the pool that iterates towards the closest peers to the target.
    pub fn add_findnode_query<I>(
        &mut self,
        config: FindNodeQueryConfig,
        target: TTarget,
        peers: I,
        iterations: usize,
    ) -> QueryId
    where
        I: IntoIterator<Item = Key<TNodeId>>,
    {
        let findnode_query = FindNodeQuery::with_config(config, target.clone(), peers, iterations);
        let peer_iter = QueryPeerIter::FindNode(findnode_query);
        self.add(peer_iter, target)
    }

    /// Adds a query to the pool that returns peers that satisfy a predicate.
    pub fn add_predicate_query<I>(
        &mut self,
        config: PredicateQueryConfig,
        target: TTarget,
        peers: I,
        iterations: usize,
        predicate: impl Fn(&TNodeId, &[u8]) -> bool + 'static,
        value: Vec<u8>,
    ) -> QueryId
    where
        I: IntoIterator<Item = Key<TNodeId>>,
    {
        let predicate_query =
            PredicateQuery::with_config(config, target, peers, iterations, predicate, value);
        let peer_iter = QueryPeerIter::Predicate(predicate_query);
        self.add(peer_iter, target)
    }

    fn add(&mut self, peer_iter: QueryPeerIter<TTarget, TNodeId>, target: TTarget) -> QueryId {
        let id = QueryId(self.next_id);
        self.next_id = self.next_id.wrapping_add(1);
        let query = Query::new(id, peer_iter, target);
        self.queries.insert(id, query);
        id
    }

    /// Returns a mutablereference to a query with the given ID, if it is in the pool.
    pub fn get_mut(&mut self, id: &QueryId) -> Option<&mut Query<TTarget, TNodeId>> {
        self.queries.get_mut(id)
    }

    /// Polls the pool to advance the queries.
    pub fn poll(&mut self) -> QueryPoolState<TTarget, TNodeId> {
        let mut finished = None;
        let mut waiting = None;

        for (&query_id, query) in self.queries.iter_mut() {
            match query.next() {
                QueryState::Finished => {
                    finished = Some(query_id);
                    break;
                }
                QueryState::Waiting(Some(return_peer)) => {
                    waiting = Some((query_id, return_peer));
                    break;
                }
                QueryState::Waiting(None) | QueryState::WaitingAtCapacity => {}
            }
        }

        if let Some((query_id, return_peer)) = waiting {
            let query = self.queries.get_mut(&query_id).expect("s.a.");
            return QueryPoolState::Waiting(Some((query, return_peer)));
        }

        if let Some(query_id) = finished {
            let query = self.queries.remove(&query_id).expect("s.a.");
            return QueryPoolState::Finished(query);
        }

        if self.queries.is_empty() {
            return QueryPoolState::Idle;
        } else {
            return QueryPoolState::Waiting(None);
        }
    }
}

/// Unique identifier for an active query.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct QueryId(pub usize);

/// A query in a `QueryPool`.
pub struct Query<TTarget, TNodeId> {
    /// The unique ID of the query.
    id: QueryId,
    /// The peer iterator that drives the query state.
    peer_iter: QueryPeerIter<TTarget, TNodeId>,
    /// Target we are looking for
    target: TTarget,
}

/// The peer selection strategies that can be used by queries.
enum QueryPeerIter<TTarget, TNodeId> {
    FindNode(FindNodeQuery<TTarget, TNodeId>),
    Predicate(PredicateQuery<TTarget, TNodeId>),
}

impl<TTarget, TNodeId> Query<TTarget, TNodeId>
where
    TTarget: Into<Key<TTarget>> + Clone,
    TNodeId: Into<Key<TNodeId>> + Eq + Clone,
{
    /// Creates a new query without starting it.
    fn new(id: QueryId, peer_iter: QueryPeerIter<TTarget, TNodeId>, target: TTarget) -> Self {
        Query {
            id,
            peer_iter,
            target,
        }
    }

    /// Gets the unique ID of the query.
    pub fn id(&self) -> QueryId {
        self.id
    }

    /// Informs the query that the attempt to contact `peer` failed.
    pub fn on_failure(&mut self, peer: &TNodeId) {
        match &mut self.peer_iter {
            QueryPeerIter::FindNode(iter) => iter.on_failure(peer),
            QueryPeerIter::Predicate(iter) => iter.on_failure(peer),
        }
    }

    /// Informs the query that the attempt to contact `peer` succeeded,
    /// possibly resulting in new peers that should be incorporated into
    /// the query, if applicable.
    pub fn on_success(&mut self, peer: &TNodeId, new_peers: Vec<TNodeId>) {
        match &mut self.peer_iter {
            QueryPeerIter::FindNode(iter) => iter.on_success(peer, new_peers),
            QueryPeerIter::Predicate(iter) => iter.on_success(peer, new_peers),
        }
    }

    /// Advances the state of the underlying peer iterator.
    fn next(&mut self) -> QueryState<TNodeId> {
        let now = Instant::now();
        match &mut self.peer_iter {
            QueryPeerIter::FindNode(iter) => iter.next(now),
        }
    }

    /// Consumes the query, producing the final `QueryResult`.
    pub fn into_result(self) -> QueryResult<TTarget, impl Iterator<Item = TNodeId>> {
        let peers = match self.peer_iter {
            QueryPeerIter::FindNode(iter) => iter.into_result(),
        };
        QueryResult {
            target: self.target,
            closest_peers: peers,
        }
    }

    /// Returns a reference to the query `target`.
    pub fn target(&self) -> &TTarget {
        &self.target
    }

    /// Returns a mutable reference to the query `target`.
    pub fn target_mut(&mut self) -> &mut TTarget {
        &mut self.target
    }
}

/// The result of a `Query`.
pub struct QueryResult<TTarget, TClosest> {
    /// The target of the query.
    pub target: TTarget,
    /// The closest peers to the target found by the query.
    pub closest_peers: TClosest,
}
