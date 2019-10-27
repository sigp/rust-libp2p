//! This module defines the raw UDP message packets for Discovery v5.
//!
//! The [discv5 wire specification](https://github.com/ethereum/devp2p/blob/master/discv5/discv5.md) provides further information on UDP message packets as implemented in this module.
//!
//! The `Packet` struct defines all raw UDP message variants and implements the encoding/decoding
//! logic.
//!
//! Note, that all message encryption/decryption is handled outside of this module.

mod auth_header;

pub use auth_header::AuthHeader;
pub use auth_header::AuthResponse;
use log::debug;
use rlp::{Decodable, DecoderError, RlpStream};
use std::default::Default;

pub const TAG_LENGTH: usize = 32;
const AUTH_TAG_LENGTH: usize = 12;
pub const MAGIC_LENGTH: usize = 32;
pub const ID_NONCE_LENGTH: usize = 32;

/// The authentication nonce (12 bytes).
pub type AuthTag = [u8; AUTH_TAG_LENGTH];
/// Packet Tag
pub type Tag = [u8; TAG_LENGTH];
/// The nonce sent in a WHOAREYOU packet.
pub type Nonce = [u8; ID_NONCE_LENGTH];
/// The magic packet.
pub type Magic = [u8; MAGIC_LENGTH];

#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    /// Packet for establishing handshake.
    RandomPacket {
        /// The XOR(SHA256(dest-node-id), src-node-id).
        tag: Tag,

        /// Random auth_tag formatted as rlp_bytes(bytes).
        auth_tag: AuthTag,

        /// At least 44 bytes of random data.
        data: Vec<u8>,
    },
    /// Handshake packet to establish identities.
    WhoAreYou {
        /// SHA256(`dest-node-id` || "WHOAREYOU").
        magic: [u8; MAGIC_LENGTH],

        /// The auth-tag of the request.
        token: AuthTag, //potentially rename to auth-tag

        /// The `id-nonce` to prevent handshake replays.
        id_nonce: Nonce,

        /// Highest known ENR sequence number of node.
        enr_seq: u64,
    },
    /// Message sent with an extended authentication header.
    AuthMessage {
        /// The XOR(SHA256(dest-node-id), src-node-id).
        tag: Tag,

        /// Authentication header.
        auth_header: AuthHeader,

        /// The encrypted message including the authentication header.
        message: Vec<u8>,
    },
    /// A standard discv5 message.
    Message {
        /// The XOR(SHA256(dest-node-id), src-node-id).
        tag: Tag,

        /// 12 byte Authentication nonce.
        auth_tag: AuthTag,

        /// The encrypted message as raw bytes.
        message: Vec<u8>,
    },
}

impl Packet {
    /// Generates a Packet::Random given a `tag`.
    pub fn random(tag: Tag) -> Packet {
        let data: Vec<u8> = (0..44).map(|_| rand::random::<u8>()).collect();

        Packet::RandomPacket {
            tag,
            auth_tag: rand::random(),
            data,
        }
    }

    /// The authentication tag for all packets except WHOAREYOU.
    pub fn auth_tag(&self) -> Option<&AuthTag> {
        match &self {
            Packet::RandomPacket { auth_tag, .. } => Some(auth_tag),
            Packet::AuthMessage { auth_header, .. } => Some(&auth_header.auth_tag),
            Packet::Message { auth_tag, .. } => Some(auth_tag),
            Packet::WhoAreYou { .. } => None,
        }
    }

    /// Returns true if the packet is a WHOAREYOU packet.
    pub fn is_whoareyou(&self) -> bool {
        match &self {
            Packet::RandomPacket { .. } => false,
            Packet::AuthMessage { .. } => false,
            Packet::Message { .. } => false,
            Packet::WhoAreYou { .. } => true,
        }
    }

    /// Encodes a packet to bytes.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Packet::RandomPacket {
                tag,
                auth_tag,
                data,
            } => {
                let mut buf = Vec::with_capacity(TAG_LENGTH + AUTH_TAG_LENGTH + 1 + 44); // at least 44 random bytes
                buf.extend_from_slice(tag);
                buf.extend_from_slice(&rlp::encode(&auth_tag.to_vec()));
                buf.extend_from_slice(&data);
                buf
            }
            Packet::WhoAreYou {
                magic,
                token,
                id_nonce,
                enr_seq,
            } => {
                let mut buf =
                    Vec::with_capacity(MAGIC_LENGTH + AUTH_TAG_LENGTH + ID_NONCE_LENGTH + 8 + 2); // + enr + rlp
                buf.extend_from_slice(magic);
                let list = {
                    let mut s = RlpStream::new();
                    s.begin_list(3);
                    s.append(&token.to_vec());
                    s.append(&id_nonce.to_vec());
                    s.append(enr_seq);
                    s.drain()
                };
                buf.extend_from_slice(&list);
                buf
            }
            Packet::AuthMessage {
                tag,
                auth_header,
                message,
            } => {
                let mut buf = Vec::with_capacity(TAG_LENGTH + 60); // TODO: Estimate correctly
                buf.extend_from_slice(tag);
                buf.extend_from_slice(&rlp::encode(auth_header));
                buf.extend_from_slice(&message.to_vec());
                buf
            }
            Packet::Message {
                tag,
                auth_tag,
                message,
            } => {
                let mut buf = Vec::with_capacity(TAG_LENGTH + AUTH_TAG_LENGTH + 1 + 24);
                buf.extend_from_slice(tag);
                buf.extend_from_slice(&rlp::encode(&auth_tag.to_vec()));
                buf.extend_from_slice(&message.to_vec());
                buf
            }
        }
    }

    /// Decodes a WHOAREYOU packet.
    fn decode_whoareyou(data: &[u8]) -> Result<Self, PacketError> {
        // 32 magic + 32 token + 12 id + 2 enr + 1 rlp
        // decode the rlp list
        let rlp_list = data[MAGIC_LENGTH..].to_vec();
        let rlp = rlp::Rlp::new(&rlp_list);
        let mut decoded_list = match rlp.as_list::<Vec<u8>>() {
            Ok(v) => v,
            Err(_) => {
                debug!("Could not decode WHOAREYOU packet: {:?}", data);
                return Err(PacketError::UnknownFormat);
            }
        };
        // build objects
        let mut magic: [u8; MAGIC_LENGTH] = Default::default();
        magic.clone_from_slice(&data[0..MAGIC_LENGTH]);

        if decoded_list.len() != 3 {
            debug!(
                "Failed to decode WHOAREYOU packet. Incorrect list size. Length: {}, expected 3",
                decoded_list.len()
            );
            return Err(PacketError::UnknownFormat);
        }

        let enr_seq_bytes = decoded_list.pop().expect("List is long enough");
        let id_nonce_bytes = decoded_list.pop().expect("List is long enough");
        let token_bytes = decoded_list.pop().expect("List is long enough");

        if id_nonce_bytes.len() != ID_NONCE_LENGTH || token_bytes.len() != AUTH_TAG_LENGTH {
            return Err(PacketError::InvalidByteSize);
        }

        let enr_seq = u64_from_be_vec(&enr_seq_bytes)?;

        let mut id_nonce: [u8; ID_NONCE_LENGTH] = Default::default();
        id_nonce.clone_from_slice(&id_nonce_bytes);

        let mut token: AuthTag = Default::default();
        token.clone_from_slice(&token_bytes);

        Ok(Packet::WhoAreYou {
            magic,
            token,
            id_nonce,
            enr_seq,
        })
    }

    /// Decodes a regular message into a `Packet`.
    fn decode_standard_message(tag: Tag, data: &[u8]) -> Result<Self, PacketError> {
        let rlp = rlp::Rlp::new(&data[TAG_LENGTH..=TAG_LENGTH + AUTH_TAG_LENGTH]);
        let auth_tag_bytes: Vec<u8> = match rlp.as_val() {
            Ok(v) => v,
            Err(_) => {
                debug!("Couldn't decode auth_tag for message: {:?}", data);
                return Err(PacketError::UnknownFormat);
            }
        };

        let mut auth_tag: AuthTag = Default::default();
        auth_tag.clone_from_slice(&auth_tag_bytes);

        Ok(Packet::Message {
            tag,
            auth_tag,
            message: data[TAG_LENGTH + AUTH_TAG_LENGTH + 1..].to_vec(),
        })
    }

    /// Decodes a message that contains an authentication header.
    fn decode_auth_header(tag: Tag, data: &[u8], rlp_length: usize) -> Result<Self, PacketError> {
        let auth_header_rlp = rlp::Rlp::new(&data[TAG_LENGTH..TAG_LENGTH + rlp_length]);
        let auth_header = AuthHeader::decode(&auth_header_rlp)?;

        let message_start = TAG_LENGTH + rlp_length;
        let message = data[message_start..].to_vec();

        Ok(Packet::AuthMessage {
            tag,
            auth_header,
            message,
        })
    }

    /// Decode raw bytes into a packet. The `magic` value (SHA2256(node-id, b"WHOAREYOU")) is passed as a parameter to check for
    /// the magic byte sequence.
    pub fn decode(data: &[u8], magic_data: &Magic) -> Result<Self, PacketError> {
        // ensure the packet is large enough to contain the correct headers
        if data.len() < TAG_LENGTH + AUTH_TAG_LENGTH + 1 {
            debug!("Packet length too small. Length: {}", data.len());
            return Err(PacketError::TooSmall);
        }

        // initially look for a WHOAREYOU packet
        if data.len() >= MAGIC_LENGTH && &data[0..MAGIC_LENGTH] == magic_data {
            return Packet::decode_whoareyou(data);
        }
        // not a WHOAREYOU packet

        // check for RLP(bytes) or RLP(list)
        else if data[TAG_LENGTH] == 140 {
            // 8c in hex - rlp encoded bytes of length 12 -i.e rlp_bytes(auth_tag)
            // we have either a random-packet or standard message
            // return the encrypted standard message.
            let mut tag: [u8; TAG_LENGTH] = Default::default();
            tag.clone_from_slice(&data[0..TAG_LENGTH]);
            return Packet::decode_standard_message(tag, data);
        }
        // not a Random Packet or standard message, may be a message with authentication header
        let mut tag: [u8; TAG_LENGTH] = Default::default();
        tag.clone_from_slice(&data[0..TAG_LENGTH]);

        let rlp = rlp::Rlp::new(&data[TAG_LENGTH..]);
        if rlp.is_list() {
            // potentially authentication header

            let rlp_length = rlp
                .payload_info()
                .map_err(|_| {
                    debug!("Could not determine Auth header rlp length");
                    PacketError::UnknownFormat
                })?
                .total();

            return Packet::decode_auth_header(tag, data, rlp_length);
        }
        // the data is unrecognizable or corrupt.
        debug!("Failed identifying message: {:?}", data);
        Err(PacketError::UnknownPacket)
    }
}

fn u64_from_be_vec(data: &[u8]) -> Result<u64, PacketError> {
    if data.len() > 8 {
        return Err(PacketError::InvalidByteSize);
    }
    let mut val = [0u8; 8];
    val[8 - data.len()..].copy_from_slice(data);
    Ok(u64::from_be_bytes(val))
}

#[derive(Debug, Clone)]
/// Types of packet errors.
pub enum PacketError {
    UnknownFormat,
    UnknownPacket,
    DecodingError(DecoderError),
    TooSmall,
    InvalidByteSize,
}

impl From<DecoderError> for PacketError {
    fn from(err: DecoderError) -> PacketError {
        PacketError::DecodingError(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p_core::identity::Keypair;
    use rand;
    use sha2::{Digest, Sha256};
    use simple_logger;

    fn hash256_to_fixed_array(s: &'static str) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.input(s);
        let mut result: [u8; 32] = std::default::Default::default();
        result.clone_from_slice(hasher.result().as_slice());
        result
    }

    /* This section provides a series of reference tests for the encoding of packets */

    #[test]
    fn ref_test_encode_random_packet() {
        // reference input
        let tag = [1u8; TAG_LENGTH]; // all 1's.
        let auth_tag = [2u8; AUTH_TAG_LENGTH]; // all 2's
        let random_data = [4u8; 44]; // 44 bytes of 4's;

        // expected hex output
        let expected_output = hex::decode("01010101010101010101010101010101010101010101010101010101010101018c0202020202020202020202020404040404040404040404040404040404040404040404040404040404040404040404040404040404040404").unwrap();

        let packet = Packet::RandomPacket {
            tag,
            auth_tag,
            data: random_data.to_vec(),
        };

        assert_eq!(packet.encode(), expected_output);
    }

    #[test]
    fn ref_test_encode_whoareyou_packet() {
        // reference input
        let magic = [1u8; MAGIC_LENGTH]; // all 1's.
        let token = [2u8; AUTH_TAG_LENGTH]; // all 2's
        let id_nonce = [3u8; ID_NONCE_LENGTH]; // all 3's
        let enr_seq = 1;

        // expected hex output
        let expected_output = hex::decode("0101010101010101010101010101010101010101010101010101010101010101ef8c020202020202020202020202a0030303030303030303030303030303030303030303030303030303030303030301").unwrap();

        let packet = Packet::WhoAreYou {
            magic,
            token,
            id_nonce,
            enr_seq,
        };

        assert_eq!(packet.encode(), expected_output);
    }

    #[test]
    fn ref_test_encode_auth_message_packet() {
        // reference input
        let tag = [1u8; TAG_LENGTH]; // all 1's.
        let auth_header = AuthHeader::new(
            [2; AUTH_TAG_LENGTH], // all 2's
            [3; ID_NONCE_LENGTH], // all 3's
            [4; 64].to_vec(),     // all 4's
            [5; 42].to_vec(),     // all 5's
        );

        let random_data = [6; 44].to_vec();

        // expected hex output
        let expected_output = hex::decode("0101010101010101010101010101010101010101010101010101010101010101f89f8c020202020202020202020202a003030303030303030303030303030303030303030303030303030303030303038367636db84004040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404aa0505050505050505050505050505050505050505050505050505050505050505050505050505050505050606060606060606060606060606060606060606060606060606060606060606060606060606060606060606").unwrap();

        let packet = Packet::AuthMessage {
            tag,
            auth_header,
            message: random_data,
        };

        assert_eq!(packet.encode(), expected_output);
    }

    #[test]
    fn ref_test_encode_message_packet() {
        // reference input
        let tag = [1u8; TAG_LENGTH]; // all 1's.
        let auth_tag = [2; AUTH_TAG_LENGTH]; // all 2's
        let random_data = [3; 44].to_vec();

        // expected hex output
        let expected_output = hex::decode("01010101010101010101010101010101010101010101010101010101010101018c0202020202020202020202020303030303030303030303030303030303030303030303030303030303030303030303030303030303030303").unwrap();

        let packet = Packet::Message {
            tag,
            auth_tag,
            message: random_data,
        };

        assert_eq!(packet.encode(), expected_output);
    }

    /* This section provides functionality testing of the packets */

    #[test]
    fn encode_decode_random_packet() {
        let _ = simple_logger::init_with_level(log::Level::Debug);
        let tag = hash256_to_fixed_array("test-tag");
        let auth_tag: [u8; AUTH_TAG_LENGTH] = rand::random();
        let random_magic: Magic = rand::random();
        let random_data: [u8; 44] = [17; 44];

        let packet = Packet::RandomPacket {
            tag: tag.clone(),
            auth_tag: auth_tag.clone(),
            data: random_data.to_vec(),
        };

        let encoded_packet = packet.encode();
        let decoded_packet = Packet::decode(&encoded_packet, &random_magic).unwrap();
        let expected_packet = Packet::Message {
            tag,
            auth_tag,
            message: random_data.to_vec(),
        };

        assert_eq!(decoded_packet, expected_packet);
    }

    #[test]
    fn encode_decode_whoareyou_packet() {
        let _ = simple_logger::init_with_level(log::Level::Debug);
        let magic = hash256_to_fixed_array("magic");
        let id_nonce: [u8; ID_NONCE_LENGTH] = rand::random();
        let token: [u8; AUTH_TAG_LENGTH] = rand::random();
        let enr_seq: u64 = rand::random();

        let packet = Packet::WhoAreYou {
            magic,
            token,
            id_nonce,
            enr_seq,
        };

        let encoded_packet = packet.clone().encode();
        let decoded_packet = Packet::decode(&encoded_packet, &magic).unwrap();

        assert_eq!(decoded_packet, packet);
    }

    #[test]
    fn encode_decode_auth_packet() {
        let _ = simple_logger::init_with_level(log::Level::Debug);
        let tag = hash256_to_fixed_array("test-tag");
        let magic = hash256_to_fixed_array("test-magic");

        // auth header data
        let auth_tag: [u8; AUTH_TAG_LENGTH] = rand::random();
        let id_nonce: [u8; ID_NONCE_LENGTH] = rand::random();
        let ephemeral_pubkey = Keypair::generate_secp256k1()
            .public()
            .into_protobuf_encoding();
        let auth_response: [u8; 32] = rand::random();
        let auth_response = auth_response.to_vec();

        let auth_header = AuthHeader {
            id_nonce,
            auth_tag,
            auth_scheme_name: "gcm",
            ephemeral_pubkey,
            auth_response,
        };

        let message: [u8; 16] = rand::random();
        let message = message.to_vec();

        let packet = Packet::AuthMessage {
            tag,
            auth_header,
            message,
        };

        let encoded_packet = packet.clone().encode();
        let decoded_packet = Packet::decode(&encoded_packet, &magic).unwrap();

        assert_eq!(decoded_packet, packet);
    }
}
