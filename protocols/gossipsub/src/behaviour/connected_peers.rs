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

use crate::metrics::{Churn, Metrics};
use crate::topic::TopicHash;
use crate::types::{PeerConnections, PeerKind, RpcSender};
use libp2p_identity::PeerId;
use libp2p_swarm::ConnectionId;
use std::collections::{BTreeSet, HashMap, HashSet};

/// The current state of all connected peers. This is a self contained data structure to ensure the
/// state between all peers is in sync.
#[derive(Debug, Default)]
pub(crate) struct ConnectedPeers {
    peer_connections: HashMap<PeerId, PeerConnections>,
    /// A map of all connected peers to their subscribed topics.
    peer_topics: HashMap<PeerId, BTreeSet<TopicHash>>,
    /// A map of all connected peers - A map of topic hash to a list of gossipsub peer Ids.
    topic_peers: HashMap<TopicHash, BTreeSet<PeerId>>,
    /// Overlay network of connected peers - Maps topics to connected gossipsub peers.
    mesh: HashMap<TopicHash, BTreeSet<PeerId>>,
    /// Map of topics to list of peers that we publish to, but don't subscribe to.
    fanout: HashMap<TopicHash, BTreeSet<PeerId>>,
    /// Set of connected outbound peers (we only consider true outbound peers found through
    /// discovery and not by PX).
    outbound_peers: HashSet<PeerId>,
}

impl ConnectedPeers {
    // Non mutable functions //

    /// Returns true if we are subscribed to the topic.
    pub(crate) fn are_we_subscribed_to_topic(&self, topic: &TopicHash) -> bool {
        self.mesh.contains_key(topic)
    }

    /// Gives the current number of connected peers.
    pub(crate) fn len(&self) -> usize {
        self.peer_connections.len()
    }

    /// Returns a reference to the mesh mapping.
    pub(crate) fn mesh(&self) -> &HashMap<TopicHash, BTreeSet<PeerId>> {
        &self.mesh
    }

    /// Returns a reference to the fanout mapping.
    pub(crate) fn fanout(&self) -> &HashMap<TopicHash, BTreeSet<PeerId>> {
        &self.fanout
    }

    /// List all the connected peers.
    pub(crate) fn all_peers(&self) -> impl Iterator<Item = &PeerId> {
        self.peer_connections.keys()
    }

    /// Lists all known peers and their associated subscribed topics.
    pub(crate) fn all_peers_topics(&self) -> impl Iterator<Item = (&PeerId, Vec<&TopicHash>)> {
        self.peer_topics
            .iter()
            .map(|(peer_id, topic_set)| (peer_id, topic_set.iter().collect()))
    }

    /// Lists all known peers and their associated protocol.
    pub(crate) fn peer_protocol_list(&self) -> impl Iterator<Item = (&PeerId, &PeerKind)> {
        self.peer_connections.iter().map(|(k, v)| (k, &v.kind))
    }

    /// Get a specific peer's protocol.
    pub(crate) fn peer_protocol(&self, peer_id: &PeerId) -> Option<PeerKind> {
        self.peer_connections.get(peer_id).map(|v| v.kind.clone())
    }

    /// Returns if the peer is connected or not.
    pub(crate) fn is_peer_connected(&self, peer_id: &PeerId) -> bool {
        self.peer_connections.contains_key(peer_id)
    }

    /// Returns peers that are subscribed to a specific topic.
    pub(crate) fn get_peers_on_topic(&self, topic: &TopicHash) -> Option<&BTreeSet<PeerId>> {
        self.topic_peers.get(topic)
    }

    /// Gets the list of topics a peer is subscribed to.
    pub(crate) fn get_topics_for_peer(&self, peer_id: &PeerId) -> Option<&BTreeSet<TopicHash>> {
        self.peer_topics.get(peer_id)
    }

    /// Returns peers that are subscribed to a specific topic and are not floodsub peers.
    pub(crate) fn get_gossipsub_peers_on_topic(
        &self,
        topic: &TopicHash,
    ) -> Option<impl Iterator<Item = &PeerId>> {
        let peers = self.topic_peers.get(topic)?;
        Some(peers.iter().filter(|p| {
            match self
                .peer_connections
                .get(p)
                .expect("Peer Topics must be in sync with peer connections")
            {
                connections if connections.kind == PeerKind::Gossipsub => true,
                connections if connections.kind == PeerKind::Gossipsubv1_1 => true,
                _ => false,
            }
        }))
    }

    /// Returns the mesh peers for a given topic.
    pub(crate) fn mesh_peers(&self, topic: &TopicHash) -> Option<&BTreeSet<PeerId>> {
        self.mesh.get(topic)
    }

    /// Returns the fanout peers for a given topic.
    pub(crate) fn fanout_peers(&self, topic: &TopicHash) -> Option<&BTreeSet<PeerId>> {
        self.fanout.get(topic)
    }

    /// Returns the first connection ID for a peer if it is connected.
    pub(crate) fn connection_id(&self, peer_id: &PeerId) -> Option<ConnectionId> {
        self.peer_connections.get(peer_id).map(|v| v.connections[0])
    }

    /// Returns true if the peer is in a mesh.
    pub(crate) fn is_peer_in_mesh(&self, peer_id: &PeerId) -> bool {
        if let Some(topics) = self.peer_topics.get(peer_id) {
            for topic in topics {
                if let Some(mesh_peers) = self.mesh.get(topic) {
                    if mesh_peers.contains(peer_id) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Returns a reference to the outbound peer set.
    pub(crate) fn outbound_peers(&self) -> &HashSet<PeerId> {
        &self.outbound_peers
    }

    // Mutable functions

    /// Adds an outbound peer to the mapping.
    pub(crate) fn add_outbound_peer(&mut self, peer_id: PeerId) {
        debug_assert!(self.peer_connections.contains_key(&peer_id));
        debug_assert!(self.peer_topics.contains_key(&peer_id));

        self.outbound_peers.insert(peer_id);
    }

    /// Adds peers to the fanout for a given topic.
    pub(crate) fn add_to_fanout<'a, I: Iterator<Item = &'a PeerId>>(
        &mut self,
        topic: TopicHash,
        peers: I,
    ) {
        let fanout_peers = self.fanout.entry(topic).or_default();

        for peer_id in peers {
            debug_assert!(self.peer_connections.contains_key(peer_id));
            debug_assert!(self.peer_topics.contains_key(peer_id));

            tracing::debug!(%peer_id, "Peer added to fanout");
            fanout_peers.insert(*peer_id);
        }
    }

    /// Removes a single peer from the fanout. Returns true if the peer existed in the mesh.
    pub(crate) fn remove_peer_from_fanout(&mut self, peer_id: &PeerId, topic: &TopicHash) -> bool {
        debug_assert!(self.peer_connections.contains_key(peer_id));
        debug_assert!(self.peer_topics.contains_key(peer_id));

        if let Some(peer_set) = self.fanout.get_mut(topic) {
            peer_set.remove(peer_id)
        } else {
            false
        }
    }

    /// Adds peers to the mesh.
    pub(crate) fn add_to_mesh<'a, I: Iterator<Item = &'a PeerId>>(
        &mut self,
        topic: TopicHash,
        peers: I,
    ) {
        let mesh_peers = self.mesh.entry(topic).or_default();

        for peer_id in peers {
            debug_assert!(self.peer_connections.contains_key(peer_id));
            debug_assert!(self.peer_topics.contains_key(peer_id));

            tracing::debug!(%peer_id, "Peer added to fanout");
            mesh_peers.insert(*peer_id);
        }
    }

    /// Removes a single peer from the mesh. Returns true if the peer existed in the mesh.
    pub(crate) fn remove_peer_from_mesh(&mut self, peer_id: &PeerId, topic: &TopicHash) -> bool {
        debug_assert!(self.peer_connections.contains_key(peer_id));
        debug_assert!(self.peer_topics.contains_key(peer_id));

        if let Some(peer_set) = self.mesh.get_mut(topic) {
            peer_set.remove(peer_id)
        } else {
            false
        }
    }

    /// Records a peer being subscribed to a specific topic. This fails if the peer is not in the
    /// connected set. This returns Ok(true) if the peer was not previously in the topic.
    pub(crate) fn add_peer_to_a_topic(
        &mut self,
        peer_id: &PeerId,
        topic: TopicHash,
    ) -> Result<bool, ()> {
        if !self.is_peer_connected(peer_id) {
            return Err(());
        }

        // Add the peer to the topic mappings
        self.peer_topics
            .entry(*peer_id)
            .or_default()
            .insert(topic.clone());
        Ok(self.topic_peers.entry(topic).or_default().insert(*peer_id))
    }

    /// Records a peer being unsubscribed from a specific topic. This fails if the peer is not in the
    /// connected set. This returns Ok(true) if the peer was in the topic.
    pub(crate) fn remove_peer_from_a_topic(
        &mut self,
        peer_id: &PeerId,
        topic: &TopicHash,
    ) -> Result<bool, ()> {
        if !self.is_peer_connected(peer_id) {
            return Err(());
        }

        // Remove the peer from the mappings
        let Some(topics) = self.peer_topics.get_mut(peer_id) else {
            return Err(());
        };
        topics.remove(topic);

        // Remove the peer from the mappings
        let Some(peers) = self.topic_peers.get_mut(topic) else {
            return Ok(false); // We may not know of this topic
        };
        Ok(peers.remove(peer_id))
    }

    /// Removes and returns all fanout peers for a specific topic.
    pub(crate) fn remove_all_fanout_peers(
        &mut self,
        topic: &TopicHash,
    ) -> Option<BTreeSet<PeerId>> {
        self.fanout.remove(topic)
    }

    /// Removes and returns all mesh peers for a specific topic.
    pub(crate) fn remove_all_mesh_peers(&mut self, topic: &TopicHash) -> Option<BTreeSet<PeerId>> {
        self.mesh.remove(topic)
    }

    // Connection / Disconnection Mutable Functions

    pub(crate) fn peer_connected(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        sender: RpcSender,
    ) {
        // By default we assume a peer is only a floodsub peer.
        //
        // The protocol negotiation occurs once a message is sent/received. Once this happens we
        // update the type of peer that this is in order to determine which kind of routing should
        // occur.
        //
        let connected_peer = self
            .peer_connections
            .entry(peer_id)
            .or_insert(PeerConnections {
                kind: PeerKind::Floodsub,
                connections: vec![],
                sender,
            });
        connected_peer.connections.push(connection_id);

        // Add the peer to the required mappings
        self.peer_topics.entry(peer_id).or_default();
    }

    /// A peer has disconnected, so all mappings need to be updated to reflect this.
    pub(crate) fn peer_disconnected(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        metrics: &mut Option<Metrics>,
    ) {
        let Some(peer_connection) = self.peer_connections.get_mut(&peer_id) else {
            tracing::error!(peer_id=%peer_id, "Libp2p reported a disconnection for a non-connected peer");
            self.remove_peer_from_all_mappings(peer_id, metrics);
            return;
        };

        // Remove the connection from the list
        let index = peer_connection
            .connections
            .iter()
            .position(|v| v == &connection_id)
            .expect("Previously established connection to peer must be present");
        peer_connection.connections.remove(index);

        // If the peer is no longer connected, remove it from all the mappings
        if peer_connection.connections.is_empty() {
            self.remove_peer_from_all_mappings(peer_id, metrics);
        }
    }

    // Helper function to remove the peer from all connected mappings.
    fn remove_peer_from_all_mappings(&mut self, peer_id: PeerId, metrics: &mut Option<Metrics>) {
        tracing::debug!(peer=%peer_id, "Peer disconnected");

        // Find all the topics the peer is subsribed to
        let subscribed_topics = self
            .peer_topics
            .get(&peer_id)
            .expect("Peer must be be in this mapping on connection");

        for topic in subscribed_topics {
            // Remove from any meshes
            if let Some(mesh_peers) = self.mesh.get_mut(topic) {
                // check if the peer is in the mesh and remove it
                if mesh_peers.remove(&peer_id) {
                    if let Some(m) = metrics.as_mut() {
                        m.peers_removed(topic, Churn::Dc, 1);
                        m.set_mesh_peers(topic, mesh_peers.len());
                    }
                };
            }

            // Remove from topic_peers
            if let Some(peer_list) = self.topic_peers.get_mut(topic) {
                if !peer_list.remove(&peer_id) {
                    tracing::warn!(
                        peer=%peer_id,
                        "Disconnected node: peer not in topic_peers"
                    );
                }
                if let Some(m) = metrics.as_mut() {
                    m.set_topic_peers(topic, peer_list.len())
                }
            } else {
                tracing::warn!(
                    peer=%peer_id,
                    topic=%topic,
                    "Disconnected node: peer with topic not in topic_peers"
                );
            }

            // Remove from Fanout
            if let Some(peer_list) = self.fanout.get_mut(topic) {
                peer_list.remove(&peer_id);
            }
        }

        // Remove it from peer_topics
        self.peer_topics.remove(&peer_id);

        // Remove it from peer_connections
        self.peer_connections.remove(&peer_id);

        // Remove outbound peers
        self.outbound_peers.remove(&peer_id);
    }

    /// Updates the connection kind
    pub(crate) fn update_connection_kind(&mut self, peer_id: PeerId, new_kind: PeerKind) {
        if let Some(connection) = self.peer_connections.get_mut(&peer_id) {
            if let PeerKind::Floodsub = connection.kind {
                // Only change the value if the old value is Floodsub (the default set in
                // `NetworkBehaviour::on_event` with FromSwarm::ConnectionEstablished).
                // All other PeerKind changes are ignored.
                tracing::debug!(
                    peer_id=%peer_id,
                    peer_kind=%new_kind,
                    "New peer type found for peer"
                );
                connection.kind = new_kind;
            }
        }
    }

    // Send Queue Handler functions

    /// Get the send queue handler for all peer connections.
    pub(crate) fn all_handler_senders(
        &mut self,
    ) -> impl Iterator<Item = (&PeerId, &mut RpcSender)> {
        self.peer_connections
            .iter_mut()
            .map(|(k, v)| (k, &mut v.sender))
    }

    /// Get the send queue handler for a peer.
    pub(crate) fn get_sender(&mut self, peer_id: &PeerId) -> Option<&mut RpcSender> {
        let connection = self.peer_connections.get_mut(peer_id)?;
        Some(&mut connection.sender)
    }

    // Test helper functions

    #[cfg(test)]
    /// Returns the number of active connections for a peer.
    pub(crate) fn total_connections(&self, peer_id: &PeerId) -> usize {
        self.peer_connections
            .get(peer_id)
            .map(|connection| connection.connections.len())
            .unwrap_or(0)
    }
}
