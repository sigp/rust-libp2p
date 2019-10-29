//! Implementation for generating session keys in the Discv5 protocol.
//! Currently, Diffie-Hellman key agreement is performed with known public key types. Session keys
//! are then derived using the HKDF (SHA2-256) key derivation function.
//!
//! There is no abstraction in this module as the specification explicitly defines a singular
//! encryption and key-derivation algorithms. Future versions may abstract some of these to allow
//! for different algorithms.
use crate::error::Discv5Error;
use crate::packet::{AuthHeader, AuthResponse, AuthTag, Nonce};
use enr::{Enr, NodeId};
use hkdf::Hkdf;
use libp2p_core::{identity::Keypair, PublicKey};
use openssl::symm::{decrypt_aead, encrypt_aead, Cipher};
use sha2::Sha256;
use secp256k1::Signature;
use super::ecdh_ident::EcdhIdent;

const NODE_ID_LENGTH: usize = 32;
const INFO_LENGTH: usize = 26 + 2 * NODE_ID_LENGTH;
const KEY_LENGTH: usize = 16;
const KEY_AGREEMENT_STRING: &str = "discovery v5 key agreement";
const KNOWN_SCHEME: &str = "gcm";
const NONCE_PREFIX: &str = "discovery-id-nonce";

type Key = [u8; KEY_LENGTH];

/* Session key generation */

/// Generates session and auth-response keys for a nonce and remote ENR. This currently only
/// supports Secp256k1 signed ENR's. This returns four keys; initiator key, responder key, auth
/// response key and the ephemeral public key.
pub fn generate_session_keys(
    local_id: &NodeId,
    remote_enr: &Enr,
    id_nonce: &Nonce,
) -> Result<(Key, Key, Key, Vec<u8>), Discv5Error> {
    // verify we know the public key and it's type
    let pubkey = remote_enr.public_key();

    let (secret, ephem_pk) = match &pubkey {
        PublicKey::Rsa(_) => {
            // don't support ephemeral RSA keys yet
            return Err(Discv5Error::KeyTypeNotSupported("RSA"));
        }
        PublicKey::Ed25519(_) => {
            // TODO: Convert the node's ed25519 public key (which is encoded as a
            // curve25519_dalek::curve::CompressedEdwardsY to a montgomery point such
            // that it can be read as x25519 public key to perform Diffie Hellman.

            return Err(Discv5Error::KeyTypeNotSupported("Ed25519"));
        }

        PublicKey::Secp256k1(pubkey) => {
            let remote_pk = secp256k1::PublicKey::parse(&pubkey.encode_uncompressed()).expect("is a valid key");

            let ephem_libp2p_sk = libp2p_core::identity::secp256k1::SecretKey::generate(); 
            let ephem_sk = secp256k1::SecretKey::parse(&ephem_libp2p_sk.to_bytes()).expect("valid key");
            let ephem_pk = secp256k1::PublicKey::from_secret_key(&ephem_sk);

            let secret = secp256k1::SharedSecret::<EcdhIdent>::new(&remote_pk, &ephem_sk).map_err(|_| Discv5Error::KeyDerivationFailed)?;
            // store as uncompressed, strip the first byte and send only 64 bytes.
            let ephem_pk = ephem_pk.serialize()[1..].to_vec();

            (secret, ephem_pk)
        }
    };

    let (initiator_key, responder_key, auth_resp_key) =
        derive_key(secret.as_ref(), local_id, remote_enr.node_id(), id_nonce)?;

    Ok((initiator_key, responder_key, auth_resp_key, ephem_pk))
}

fn derive_key(
    secret: &[u8],
    first_id: &NodeId,
    second_id: &NodeId,
    id_nonce: &Nonce,
) -> Result<(Key, Key, Key), Discv5Error> {

    let mut info = [0u8; INFO_LENGTH];
    info[0..26].copy_from_slice(KEY_AGREEMENT_STRING.as_bytes());
    info[26..26 + NODE_ID_LENGTH].copy_from_slice(&first_id.raw());
    info[26 + NODE_ID_LENGTH..].copy_from_slice(&second_id.raw());

    let hk = Hkdf::<Sha256>::new(Some(id_nonce), secret);

    let mut okm = [0u8; 3 * KEY_LENGTH];
    hk.expand(&info, &mut okm)
        .map_err(|_| Discv5Error::KeyDerivationFailed)?;

    let mut initiator_key: Key = Default::default();
    let mut responder_key: Key = Default::default();
    let mut auth_resp_key: Key = Default::default();
    initiator_key.copy_from_slice(&okm[0..KEY_LENGTH]);
    responder_key.copy_from_slice(&okm[KEY_LENGTH..2 * KEY_LENGTH]);
    auth_resp_key.copy_from_slice(&okm[2 * KEY_LENGTH..3 * KEY_LENGTH]);

    Ok((initiator_key, responder_key, auth_resp_key))
}

/// Derives the session keys for a public key type that matches the local keypair.
pub fn derive_keys_from_pubkey(
    local_keypair: &Keypair,
    local_id: &NodeId,
    remote_id: &NodeId,
    id_nonce: &Nonce,
    ephem_pubkey: &[u8],
) -> Result<(Key, Key, Key), Discv5Error> {
    let secret = match local_keypair {
        Keypair::Rsa(_) => {
            // don't support RSA keys yet
            return Err(Discv5Error::KeyTypeNotSupported("RSA"));
        }
        Keypair::Ed25519(_) => {
            // don't support RSA keys yet
            return Err(Discv5Error::KeyTypeNotSupported("Ed25519"));
        }
        Keypair::Secp256k1(key) => {
            // convert remote pubkey into secp256k1 public key
            // the key type should match our own node record
            let remote_pubkey = secp256k1::PublicKey::parse_slice(ephem_pubkey, None)
                .map_err(|_| Discv5Error::InvalidRemotePublicKey)?;

            let sk = secp256k1::SecretKey::parse(&key.secret().to_bytes()).expect("valid key");

            let secret = secp256k1::SharedSecret::<EcdhIdent>::new(&remote_pubkey, &sk).map_err(|_| Discv5Error::KeyDerivationFailed)?;
            secret.as_ref().to_vec()
        }
    };

    derive_key(&secret, remote_id, local_id, id_nonce)
}

/* Nonce Signing */

/// Generates a signature of a nonce given a keypair. This prefixes the `NONCE_PREFIX` to the
/// signature.
pub fn sign_nonce(keypair: &Keypair, nonce: &Nonce, ephem_pubkey: &[u8]) -> Result<Vec<u8>, Discv5Error> {
    let signing_nonce = generate_signing_nonce(nonce, ephem_pubkey);

    match keypair {
        Keypair::Rsa(_) => unimplemented!("RSA keys are not supported"),
        Keypair::Ed25519(_) => unimplemented!("Ed25519 keys are not supported"),
        // builds a compact secp256k1 serialized compact signature
        Keypair::Secp256k1(key) => {
            let der_sig = key.secret().sign(&signing_nonce).map_err(|_| Discv5Error::Custom("Nonce signing failed"))?;
            Ok(Signature::parse_der(&der_sig).map_err(|_| Discv5Error::Custom("Invalid DER signature"))?.serialize().to_vec())
        }
    }
}

/// Verifies the authentication header nonce.
pub fn verify_authentication_nonce(
    remote_pubkey: &PublicKey,
    remote_ephem_pubkey: &[u8],
    nonce: &Nonce,
    sig: &[u8],
) -> bool {
    let signing_nonce = generate_signing_nonce(nonce, remote_ephem_pubkey);

    match remote_pubkey {
        PublicKey::Rsa(_) => unimplemented!("RSA keys are not supported"),
        PublicKey::Ed25519(_) => unimplemented!("Ed25519 keys are not supported"),
        // verifies secp256k1 serialized compact signatures
        PublicKey::Secp256k1(pk) => {
            if let Ok(signature) = Signature::parse_slice(sig).map(|s| s.serialize_der()) {
                return pk.verify(&signing_nonce, signature.as_ref());
            }
            false
        }
    }
}

/// Builds the signature for a given nonce. The SHA256 hash occurs in the secp256k1 signing
/// function.
fn generate_signing_nonce(id_nonce: &Nonce, ephem_pubkey: &[u8]) -> Vec<u8> {
    let mut nonce = NONCE_PREFIX.as_bytes().to_vec();
    nonce.append(&mut id_nonce.to_vec());
    nonce.append(&mut ephem_pubkey.to_vec());
    nonce
}

/* Decryption related functions */

/// Verifies the encoding and nonce signature given in the authentication header. If
/// the header contains an updated ENR, it is returned.
pub fn decrypt_authentication_header(
    auth_resp_key: &Key,
    header: &AuthHeader,
) -> Result<AuthResponse, Discv5Error> {
    if header.auth_scheme_name != KNOWN_SCHEME {
        return Err(Discv5Error::Custom("Invalid authentication scheme"));
    }

    // decrypt the auth-response
    let rlp_auth_response = decrypt_message(auth_resp_key, [0u8; 12], &header.auth_response, &[])?;
    let auth_response = rlp::decode::<AuthResponse>(&rlp_auth_response)
        .map_err(|e| Discv5Error::RLPError(e))?;
    Ok(auth_response)
}


/// Decrypt messages that are post-fixed with an authenticated MAC.
pub fn decrypt_message(
    key: &Key,
    nonce: AuthTag,
    message: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, Discv5Error> {
    if message.len() < 16 {
        return Err(Discv5Error::DecryptionFail(
            "Message not long enough to contain a MAC".into(),
        ));
    }

    let mut mac: [u8; 16] = Default::default();
    mac.copy_from_slice(&message[message.len() - 16..]);

    decrypt_aead(
        Cipher::aes_128_gcm(),
        key,
        Some(&nonce),
        aad,
        &message[..message.len() - 16],
        &mac,
    )
    .map_err(|e| Discv5Error::DecryptionFail(format!("Could not decrypt message. Error: {:?}", e)))

}

/* Encryption related functions */

/// A wrapper around the underlying default AES_GCM implementation. This may be abstracted in the
/// future.
pub fn encrypt_message(
    key: &Key,
    nonce: AuthTag,
    message: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, Discv5Error> {

    let mut mac: [u8; 16] = Default::default();
    let mut msg_cipher = encrypt_aead(
        Cipher::aes_128_gcm(),
        key,
        Some(&nonce),
        aad,
        message,
        &mut mac,
    )
    .map_err(|e| Discv5Error::EncryptionFail(format!("{:?}", e)))?;

    // concat the ciphertext with the MAC
    msg_cipher.append(&mut mac.to_vec());
    Ok(msg_cipher)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::packet::Tag;
    use enr::EnrBuilder;
    use libp2p_core::identity::Keypair;
    use rand;

    /* This section provides a series of reference tests for the encoding of packets */

    #[test]
    fn ref_test_ecdh() {
        let remote_pubkey = hex::decode("9961e4c2356d61bedb83052c115d311acb3a96f5777296dcf297351130266231503061ac4aaee666073d7e5bc2c80c3f5c5b500c1cb5fd0a76abbb6b675ad157").unwrap();
        let local_secret_key = hex::decode("fb757dc581730490a1d7a00deea65e9b1936924caaea8f44d476014856b68736").unwrap();

        let expected_secret = hex::decode("033b11a2a1f214567e1537ce5e509ffd9b21373247f2a3ff6841f4976f53165e7e").unwrap();

        let mut remote_pk_bytes = [0;65];
        remote_pk_bytes[0] = 4; // pre-fixes a magic byte indicating this is in uncompressed form
        remote_pk_bytes[1..].copy_from_slice(&remote_pubkey);
        let mut local_sk_bytes = [0;32];
        local_sk_bytes.copy_from_slice(&local_secret_key);

        let remote_pk = secp256k1::PublicKey::parse(&remote_pk_bytes).unwrap();
        let local_sk = secp256k1::SecretKey::parse(&local_sk_bytes).unwrap();

        let secret = secp256k1::SharedSecret::<EcdhIdent>::new(&remote_pk, &local_sk).unwrap();

        assert_eq!(secret.as_ref(), expected_secret.as_slice());
    }

    #[test]
    fn ref_key_derivation() {
        let secret = hex::decode("02a77e3aa0c144ae7c0a3af73692b7d6e5b7a2fdc0eda16e8d5e6cb0d08e88dd04").unwrap();
        let first_node_id = NodeId::parse(&hex::decode("a448f24c6d18e575453db13171562b71999873db5b286df957af199ec94617f7").unwrap()).unwrap();
        let second_node_id = NodeId::parse(&hex::decode("885bba8dfeddd49855459df852ad5b63d13a3fae593f3f9fa7e317fd43651409").unwrap()).unwrap();
        let id_nonce = [1; 32];

        let expected_first_key = hex::decode("238d8b50e4363cf603a48c6cc3542967").unwrap();
        let expected_second_key = hex::decode("bebc0183484f7e7ca2ac32e3d72c8891").unwrap();
        let expected_auth_resp_key = hex::decode("e987ad9e414d5b4f9bfe4ff1e52f2fae").unwrap();

        let (first_key, second_key, auth_resp_key) = derive_key(&secret, &first_node_id, &second_node_id, &id_nonce).unwrap();

        assert_eq!(first_key.to_vec(), expected_first_key);
        assert_eq!(second_key.to_vec(), expected_second_key);
        assert_eq!(auth_resp_key.to_vec(), expected_auth_resp_key);
    }

    #[test]
    fn ref_nonce_signing() {
        let nonce = [1; 32];
        let ephemeral_pubkey = hex::decode("9961e4c2356d61bedb83052c115d311acb3a96f5777296dcf297351130266231503061ac4aaee666073d7e5bc2c80c3f5c5b500c1cb5fd0a76abbb6b675ad157").unwrap();
        let local_secret_key = hex::decode("fb757dc581730490a1d7a00deea65e9b1936924caaea8f44d476014856b68736").unwrap();

        let expected_nonce = hex::decode("3b7b8ce9df3fbd9b6367c365622ccc82a2cb9d94219401e7b08e3194f9f835764a07caad38bf0f5a7a89501a8156bb053c880774502f5cd8a6190fbe374adc89").unwrap();


        let secret_key = libp2p_core::identity::secp256k1::SecretKey::from_bytes(local_secret_key).unwrap();

        let key = Keypair::Secp256k1(libp2p_core::identity::secp256k1::Keypair::from(secret_key));
        let nonce = sign_nonce(&key, &nonce, &ephemeral_pubkey).unwrap();

        assert_eq!(nonce, expected_nonce);
    }

    #[test]
    fn ref_encryption() {
        let key_bytes = hex::decode("9f2d77db7004bf8a1a85107ac686990b").unwrap();
        let nonce_bytes = hex::decode("27b5af763c446acd2749fe8e").unwrap();
        let pt = hex::decode("01c20101").unwrap();
        let ad = hex::decode("93a7400fa0d6a694ebc24d5cf570f65d04215b6ac00757875e3f3a5f42107903").unwrap();
        let expected_ciphertext = hex::decode("a5d12a2d94b8ccb3ba55558229867dc13bfa3648").unwrap();

        let mut key = [0u8;16];
        key.copy_from_slice(&key_bytes);
        let mut nonce = [0u8;12];
        nonce.copy_from_slice(&nonce_bytes);

        let ciphertext = encrypt_message(&key, nonce,&pt, &ad).unwrap(); 

        assert_eq!(ciphertext, expected_ciphertext);
    }

    /* This section provides functionality testing */

    #[test]
    fn derive_symmetric_keys() {
        let node1_kp = Keypair::generate_secp256k1();
        let node2_kp = Keypair::generate_secp256k1();

        let node1_enr = EnrBuilder::new("v4").build(&node1_kp).unwrap();
        let node2_enr = EnrBuilder::new("v4").build(&node2_kp).unwrap();

        let nonce: Nonce = rand::random();

        let (key1, key2, key3, pk) =
            generate_session_keys(node1_enr.node_id(), &node2_enr, &nonce).unwrap();
        let (key4, key5, key6) = derive_keys_from_pubkey(
            &node2_kp,
            node2_enr.node_id(),
            node1_enr.node_id(),
            &nonce,
            &pk,
        )
        .unwrap();

        assert_eq!(key1, key4);
        assert_eq!(key2, key5);
        assert_eq!(key3, key6);
    }

    #[test]
    fn encrypt_decrypt() {
        let tag: Tag = rand::random();
        let msg: Vec<u8> = vec![1, 2, 3, 4, 5, 6];
        let key: Key = rand::random();
        let nonce: AuthTag = rand::random();

        let cipher = encrypt_message(&key, nonce, &msg, &tag).unwrap();

        let plain_text = decrypt_message(&key, nonce, &cipher, &tag).unwrap();

        assert_eq!(plain_text, msg);
    }

}
