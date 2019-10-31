#![cfg(test)]

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
use rand::{
    RngCore,
    {prng::XorShiftRng, SeedableRng},
};
use std::io;
use std::net::IpAddr;
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
        let discv5 = Discv5::new(enr, keypair.clone(), ip.into()).unwrap();
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
        let discv5 = Discv5::new(enr, keypair.clone(), ip.into()).unwrap();
        swarms.push(Swarm::new(
            transport,
            discv5,
            keypair.public().into_peer_id(),
        ));
    }
    swarms
}

/// Return log2 distance between 2 node ids
fn get_distance(node1: &NodeId, node2: &NodeId) -> Option<u64> {
    let node1: kbucket::Key<NodeId> = node1.clone().into();
    node1.log2_distance(&node2.clone().into())
}

/// Generate `n` keypairs such that the node ids they generate are at kbucket `bucket_num` of
/// the generated bootstrap node. Return the `n` nodes along with the bootstrap node.
fn generate_keypairs(n: usize, bucket_num: u64) -> Vec<identity::Keypair> {
    let mut keypairs: Vec<identity::Keypair> = Vec::new();
    let bootstrap_keypair = identity::Keypair::generate_secp256k1();
    let bootstrap_node_id = EnrBuilder::new("v4")
        .build(&bootstrap_keypair)
        .unwrap()
        .node_id()
        .clone();
    for _ in 0..n {
        loop {
            let keypair = identity::Keypair::generate_secp256k1();
            let enr = EnrBuilder::new("v4").build(&keypair).unwrap();
            let key = enr.node_id();
            let distance = get_distance(&bootstrap_node_id, key).unwrap();
            // Any distance > 254 should be good enough for discovering nodes in one
            // complete query
            if distance == bucket_num {
                keypairs.push(keypair);
                break;
            }
        }
    }
    keypairs.push(bootstrap_keypair);
    keypairs
}

fn generate_deterministic_keypair(n: usize, seed: u64) -> Vec<identity::Keypair> {
    let mut keypairs = Vec::new();
    for i in 0..n {
        let sk = {
            let mut rng = &mut XorShiftRng::seed_from_u64(seed + i as u64);
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
        // println!("Keypair {:?}", kp.public());
        keypairs.push(kp);
    }
    keypairs
}

#[test]
fn test_deterministic() {
    let mut i = 0;
    loop {
        i += 1;
        let keypairs = generate_deterministic_keypair(14, i);
        let mut swarms = build_swarms_from_keypairs(keypairs);
        let mut bootstrap_node = swarms.pop().unwrap();
        let mut check = true;
        for swarm in swarms.iter_mut() {
            let key: kbucket::Key<NodeId> = swarm.local_enr().node_id().clone().into();
            let distance = key
                .log2_distance(&bootstrap_node.local_enr().node_id().clone().into())
                .unwrap();
            if distance != 256 {
                check = false;
                break;
            }
        }
        if check {
            println!("Seed {}", i);
            break;
        }
        if i % 100 == 0 {
            println!("seed value {}", i);
        }
    }
    assert!(true);
}

#[test]
fn test_discovery_star_topology() {
    init();
    let node_num = 12;
    // Generate `num_nodes` nodes with the bootstrap node and 1 target_node
    // let keypairs = generate_keypairs(node_num + 1, 256);
    let keypairs = generate_deterministic_keypair(node_num + 2, 1654);
    let mut swarms = build_swarms_from_keypairs(keypairs);
    // Last node is bootstrap node in a star topology
    let mut bootstrap_node = swarms.pop().unwrap();
    // target_node is a node "close" to the bootstrap node for the FINDNODE query
    //target_node is not polled.
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
