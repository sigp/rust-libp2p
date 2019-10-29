//! The identifier for an ENR record. This is the keccak256 hash of the public key (for secp256k1
//! this is the uncompressed encoded form of the public key).

use crate::enr_keypair::EnrPublicKey;
use libp2p_core::identity::PublicKey;
use sha3::{Digest, Keccak256};

type RawNodeId = [u8; 32];

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId {
    raw: RawNodeId,
}

impl NodeId {
    pub fn new(raw_input: &[u8; 32]) -> Self {
        NodeId { raw: raw_input.clone() }
    }

    pub fn parse(raw_input: &[u8]) -> Result<Self, &'static str> {
        if raw_input.len() > 32 {
            return Err("Input too large");
        }

        let mut raw: RawNodeId = [0u8; 32];
        raw[..std::cmp::min(32, raw_input.len())].copy_from_slice(raw_input);

        Ok(NodeId { raw })
    }

    pub fn random() -> Self {
        NodeId {
            raw: rand::random(),
        }
    }

    pub fn raw(&self) -> RawNodeId {
        self.raw
    }
}

/// Returns the node-id of the associated ENR record. This is the keccak256
/// hash of the public key. ENR records cannot be created without a valid public key.
/// Therefore this will always return a value.
impl From<PublicKey> for NodeId {
    fn from(public_key: PublicKey) -> Self {
        let pubkey_bytes = EnrPublicKey::from(public_key.clone()).encode_uncompressed();
        NodeId::parse(&Keccak256::digest(&pubkey_bytes)).expect("must be the correct length")
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let hex_encode = hex::encode(self.raw);
        write!(
            f,
            "0x{}..{}",
            &hex_encode[0..4],
            &hex_encode[hex_encode.len() - 4..]
        )
    }
}
