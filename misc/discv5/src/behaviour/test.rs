#![cfg(test)]

use crate::behaviour::RpcRequest;
use crate::kbucket::*;
use crate::*;
use crate::{Discv5, Discv5Event};
use env_logger;
use libp2p_core::{
    identity, muxing::StreamMuxerBox, nodes::Substream, transport::boxed::Boxed,
    transport::MemoryTransport, PeerId, Transport,
};
use libp2p_swarm::Swarm;
use tokio::prelude::*;
use tokio::runtime::Runtime;

use crate::kbucket;
use enr::NodeId;
use enr::{Enr, EnrBuilder};
use libp2p_secio::SecioConfig;
use libp2p_yamux as yamux;
use rand_core::{RngCore, SeedableRng};
use rand_xorshift;
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

type SwarmType =
    Swarm<Boxed<(PeerId, StreamMuxerBox), io::Error>, Discv5<Substream<StreamMuxerBox>>>;

fn init() {
    let _ = env_logger::builder().is_test(true).try_init();
}

fn build_swarms(n: usize, base_port: u16) -> Vec<SwarmType> {
    let mut swarms = Vec::new();
    let ip: IpAddr = "127.0.0.1".parse().unwrap();

    for port in base_port..base_port + n as u16 {
        let keypair = identity::Keypair::generate_secp256k1();
        let enr = EnrBuilder::new("v4")
            .ip(ip.clone().into())
            .udp(port)
            .build(&keypair)
            .unwrap();
        // transport for building a swarm
        let transport = MemoryTransport::default()
            .upgrade(libp2p_core::upgrade::Version::V1)
            .authenticate(SecioConfig::new(keypair.clone()))
            .multiplex(yamux::Config::default())
            .map(|(p, m), _| (p, StreamMuxerBox::new(m)))
            .map_err(|e| panic!("Failed to create transport: {:?}", e))
            .boxed();
        let discv5 = Discv5::new(enr, keypair.clone(), ip.into(), false).unwrap();
        swarms.push(Swarm::new(
            transport,
            discv5,
            keypair.public().into_peer_id(),
        ));
    }
    swarms
}

/// Build `n` swarms using passed keypairs.
fn build_swarms_from_keypairs(keypairs: Vec<identity::Keypair>) -> Vec<SwarmType> {
    let base_port = 10000u16;
    let mut swarms = Vec::new();
    let ip: IpAddr = "127.0.0.1".parse().unwrap();

    // for port in base_port..base_port + keypairs.len() as u16 {
    for i in 0..keypairs.len() {
        let port = base_port + i as u16;
        let keypair = &keypairs[i];
        let enr = EnrBuilder::new("v4")
            .ip(ip.clone().into())
            .udp(port)
            .build(&keypair)
            .unwrap();
        // transport for building a swarm
        let transport = MemoryTransport::default()
            .upgrade(libp2p_core::upgrade::Version::V1)
            .authenticate(SecioConfig::new(keypair.clone()))
            .multiplex(yamux::Config::default())
            .map(|(p, m), _| (p, StreamMuxerBox::new(m)))
            .map_err(|e| panic!("Failed to create transport: {:?}", e))
            .boxed();
        let discv5 = Discv5::new(enr, keypair.clone(), ip.into(), false).unwrap();
        swarms.push(Swarm::new(
            transport,
            discv5,
            keypair.public().into_peer_id(),
        ));
    }
    swarms
}

/// Generate `n` deterministic keypairs from a given seed.
fn generate_deterministic_keypair(n: usize, seed: u64) -> Vec<identity::Keypair> {
    let mut keypairs = Vec::new();
    for i in 0..n {
        let sk = {
            let rng = &mut rand_xorshift::XorShiftRng::seed_from_u64(seed + i as u64);
            let mut b = [0; secp256k1::util::SECRET_KEY_SIZE];
            loop {
                // until a value is given within the curve order
                rng.fill_bytes(&mut b);
                if let Ok(k) = identity::secp256k1::SecretKey::from_bytes(&mut b) {
                    break k;
                }
            }
        };
        let kp = identity::Keypair::Secp256k1(identity::secp256k1::Keypair::from(sk));
        keypairs.push(kp);
    }
    keypairs
}

/// Test for a star topology with `num_nodes` connected to a `bootstrap_node`
/// FINDNODE request is sent from any of the `num_nodes` nodes to a `target_node`
/// which isn't part of the swarm.
/// The seed for the keypair generation is chosen such that all `num_nodes` nodes
/// and the `target_node` are in the 256th k-bucket of the bootstrap node.
/// This ensures that all nodes are found in a single FINDNODE query.
#[test]
fn test_discovery_star_topology() {
    init();
    let node_num = 12;
    // Seed is chosen such that all nodes are in the 256th bucket of bootstrap
    let seed = 1654;
    // Generate `num_nodes` + bootstrap_node and target_node keypairs from given seed
    let keypairs = generate_deterministic_keypair(node_num + 2, seed);
    let mut swarms = build_swarms_from_keypairs(keypairs);
    // Last node is bootstrap node in a star topology
    let mut bootstrap_node = swarms.pop().unwrap();
    // target_node is not polled.
    let target_node = swarms.pop().unwrap();
    println!("Bootstrap node: {}", bootstrap_node.local_enr().node_id());
    println!("Target node: {}", target_node.local_enr().node_id());
    for swarm in swarms.iter_mut() {
        let key: kbucket::Key<NodeId> = swarm.local_enr().node_id().clone().into();
        let distance = key
            .log2_distance(&bootstrap_node.local_enr().node_id().clone().into())
            .unwrap();
        println!(
            "Distance of node {} relative to node {}: {}",
            swarm.local_enr().node_id(),
            bootstrap_node.local_enr().node_id(),
            distance
        );
        swarm.add_enr(bootstrap_node.local_enr().clone());
        bootstrap_node.add_enr(swarm.local_enr().clone());
    }
    // Start a FINDNODE query of target
    let target_random_node_id = target_node.local_enr().node_id();
    swarms
        .first_mut()
        .unwrap()
        .find_node(target_random_node_id.clone());
    swarms.push(bootstrap_node);
    Runtime::new()
        .unwrap()
        .block_on(future::poll_fn(move || -> Result<_, io::Error> {
            for swarm in swarms.iter_mut() {
                loop {
                    match swarm.poll().unwrap() {
                        Async::Ready(Some(Discv5Event::FindNodeResult {
                            closer_peers, ..
                        })) => {
                            println!(
                                "Query found {} peers, Total peers {}",
                                closer_peers.len(),
                                node_num
                            );
                            assert!(closer_peers.len() == node_num);
                            return Ok(Async::Ready(()));
                        }
                        Async::Ready(_) => (),
                        Async::NotReady => break,
                    }
                }
            }
            Ok(Async::NotReady)
        }))
        .unwrap();
}

#[test]
fn test_findnode_query() {
    init();
    // build a collection of 8 nodes
    let node_num = 8;
    let mut swarms = build_swarms(node_num, 30000);
    let node_enrs: Vec<Enr> = swarms.iter().map(|n| n.local_enr().clone()).collect();

    // link the nodes together
    for (swarm, previous_node_enr) in swarms.iter_mut().skip(1).zip(node_enrs.clone()) {
        let key: kbucket::Key<NodeId> = swarm.local_enr().node_id().clone().into();
        let distance = key
            .log2_distance(&previous_node_enr.node_id().clone().into())
            .unwrap();
        println!("Distance of node relative to next: {}", distance);
        swarm.add_enr(previous_node_enr);
    }

    // pick a random node target
    let target_random_node_id = NodeId::random();

    // start a query on the last node
    swarms
        .last_mut()
        .unwrap()
        .find_node(target_random_node_id.clone());

    // build expectations
    let expected_node_ids: Vec<NodeId> = node_enrs
        .iter()
        .map(|enr| enr.node_id().clone())
        .take(node_num - 1)
        .collect();

    let test_result = Arc::new(Mutex::new(true));
    let thread_result = test_result.clone();

    tokio::run(
        future::poll_fn(move || -> Result<_, io::Error> {
            for swarm in swarms.iter_mut() {
                loop {
                    match swarm.poll().unwrap() {
                        Async::Ready(Some(Discv5Event::FindNodeResult { key, closer_peers })) => {
                            // NOTE: The number of peers found is statistical, as we only ask
                            // peers for specific buckets, there is a chance our node doesn't
                            // exist if the first few buckets asked for.
                            assert_eq!(key, target_random_node_id);
                            println!(
                                "Query found {} peers. Total peers were: {}",
                                closer_peers.len(),
                                expected_node_ids.len()
                            );
                            assert!(closer_peers.len() <= expected_node_ids.len());
                            return Ok(Async::Ready(()));
                        }
                        Async::Ready(_) => (),
                        Async::NotReady => break,
                    }
                }
            }
            Ok(Async::NotReady)
        })
        .timeout(Duration::from_millis(500))
        .map_err(move |_| *thread_result.lock().unwrap() = false)
        .map(|_| ()),
    );

    assert!(*test_result.lock().unwrap());
}

#[test]
fn test_updating_connection_on_ping() {
    let keypair = identity::Keypair::generate_secp256k1();
    let ip: IpAddr = "127.0.0.1".parse().unwrap();
    let enr = EnrBuilder::new("v4")
        .ip(ip.clone().into())
        .udp(10001)
        .build(&keypair)
        .unwrap();
    let ip2: IpAddr = "127.0.0.1".parse().unwrap();
    let keypair2 = identity::Keypair::generate_secp256k1();
    let enr2 = EnrBuilder::new("v4")
        .ip(ip2.clone().into())
        .udp(10002)
        .build(&keypair2)
        .unwrap();

    // Set up discv5 with one disconnected node
    let mut discv5: Discv5<Box<u64>> = Discv5::new(enr, keypair.clone(), ip.into(), false).unwrap();
    discv5.add_enr(enr2.clone());
    discv5.connection_updated(enr2.node_id().clone(), None, NodeStatus::Disconnected);

    let mut buckets = discv5.kbuckets.clone();
    let mut node = buckets.iter().next().unwrap();
    assert_eq!(node.status, NodeStatus::Disconnected);

    // Add a fake request
    let ping_response = rpc::Response::Ping {
        enr_seq: 2,
        ip: ip2,
        port: 10002,
    };
    let ping_request = rpc::Request::Ping { enr_seq: 2 };
    let req = RpcRequest(2, enr2.node_id().clone());
    discv5
        .active_rpc_requests
        .insert(req, (Some(1), ping_request.clone()));

    // Handle the ping and expect the disconnected Node to become connected
    discv5.handle_rpc_response(enr2.node_id().clone(), 2, ping_response);
    buckets = discv5.kbuckets.clone();

    node = buckets.iter().next().unwrap();
    assert_eq!(node.status, NodeStatus::Connected);
}

// The kbuckets table can have maximum 10 nodes in the same /24 subnet across all buckets
#[test]
fn test_table_limits() {
    let keypair = identity::Keypair::generate_secp256k1();
    let ip: IpAddr = "127.0.0.1".parse().unwrap();
    let enr = EnrBuilder::new("v4")
        .ip(ip.clone().into())
        .udp(9000)
        .build(&keypair)
        .unwrap();
    let mut discv5: Discv5<Box<u64>> = Discv5::new(enr, keypair.clone(), ip.into(), true).unwrap();
    let table_limit: usize = 10;
    // Generate `table_limit + 1` nodes in the same subnet.
    let enrs: Vec<Enr> = (1..=table_limit + 1)
        .map(|i| {
            let kp = identity::Keypair::generate_secp256k1();
            let ip: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 168, 1, i as u8));
            EnrBuilder::new("v4")
                .ip(ip.clone().into())
                .udp(9000 + i as u16)
                .build(&kp)
                .unwrap()
        })
        .collect();
    for enr in enrs {
        discv5.add_enr(enr.clone());
    }
    // Number of entries could be < `table_limit` as per bucket limit is 2.
    assert!(discv5.kbuckets_entries().collect::<Vec<_>>().len() <= table_limit);
}

// Each bucket can have maximum 2 nodes in the same /24 subnet
#[test]
fn test_bucket_limits() {
    let keypair = identity::Keypair::generate_secp256k1();
    let ip: IpAddr = "127.0.0.1".parse().unwrap();
    let enr = EnrBuilder::new("v4")
        .ip(ip.clone().into())
        .udp(9500)
        .build(&keypair)
        .unwrap();
    let bucket_limit: usize = 2;
    // Generate `bucket_limit + 1` keypairs that go in `enr` node's 256th bucket.
    let keypairs = {
        let mut keypairs = Vec::new();
        for _ in 0..bucket_limit + 1 {
            loop {
                let keypair = identity::Keypair::generate_secp256k1();
                let enr_new = EnrBuilder::new("v4").build(&keypair).unwrap();
                let key: kbucket::Key<NodeId> = enr.node_id().clone().into();
                let distance = key
                    .log2_distance(&enr_new.node_id().clone().into())
                    .unwrap();
                if distance == 256 {
                    keypairs.push(keypair);
                    break;
                }
            }
        }
        keypairs
    };
    // Generate `bucket_limit + 1` nodes in the same subnet.
    let enrs: Vec<Enr> = (1..=bucket_limit + 1)
        .map(|i| {
            let kp = &keypairs[i - 1];
            let ip: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 168, 1, i as u8));
            EnrBuilder::new("v4")
                .ip(ip.clone().into())
                .udp(9500 + i as u16)
                .build(kp)
                .unwrap()
        })
        .collect();
    let mut discv5: Discv5<Box<u64>> = Discv5::new(enr, keypair.clone(), ip.into(), true).unwrap();
    for enr in enrs {
        discv5.add_enr(enr.clone());
    }

    // Number of entries should be equal to `bucket_limit`.
    assert_eq!(
        discv5.kbuckets_entries().collect::<Vec<_>>().len(),
        bucket_limit
    );
}
