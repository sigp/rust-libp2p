//! The identifier for an ENR record. This is the keccak256 hash of the public key (for secp256k1
//! keys this is the uncompressed encoded form of the public key).

use super::Enr;
use crate::keys::{EnrKey, EnrPublicKey};
use sha3::{Digest, Keccak256};

type RawNodeId = [u8; 32];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
/// The `NodeId` of an ENR (a 32 byte identifier).
pub struct NodeId {
    raw: RawNodeId,
}

impl NodeId {
    /// Creates a new node record from 32 bytes.
    #[must_use]
    pub const fn new(raw_input: &[u8; 32]) -> Self {
        Self { raw: *raw_input }
    }

    /// Parses a byte slice to form a node Id. This fails if the slice isn't of length 32.
    pub fn parse(raw_input: &[u8]) -> Result<Self, &'static str> {
        if raw_input.len() > 32 {
            return Err("Input too large");
        }

        let mut raw: RawNodeId = [0_u8; 32];
        raw[..std::cmp::min(32, raw_input.len())].copy_from_slice(raw_input);

        Ok(Self { raw })
    }

    /// Generates a random `NodeId`.
    #[must_use]
    pub fn random() -> Self {
        Self {
            raw: rand::random(),
        }
    }

    /// Returns a `RawNodeId` which is a 32 byte list.
    #[must_use]
    pub const fn raw(&self) -> RawNodeId {
        self.raw
    }
}

impl<T: EnrPublicKey> From<T> for NodeId {
    fn from(public_key: T) -> Self {
        let pubkey_bytes = public_key.encode_uncompressed();
        Self::parse(&Keccak256::digest(&pubkey_bytes)).expect("must be the correct length")
    }
}

impl<T: EnrKey> From<Enr<T>> for NodeId {
    fn from(enr: Enr<T>) -> Self {
        enr.node_id()
    }
}

impl<T: EnrKey> From<&Enr<T>> for NodeId {
    fn from(enr: &Enr<T>) -> Self {
        enr.node_id()
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
