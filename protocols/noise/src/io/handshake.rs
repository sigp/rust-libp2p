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

//! Noise protocol handshake I/O.

mod payload_proto {
    include!(concat!(env!("OUT_DIR"), "/payload.proto.rs"));
}

use super::NoiseOutput;
use crate::error::NoiseError;
use crate::io::SnowState;
use crate::protocol::{KeypairIdentity, Protocol, PublicKey};
use futures::io::AsyncReadExt;
use futures::prelude::*;
use futures::task;
use libp2p_core::identity;
use prost::Message;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// The identity of the remote established during a handshake.
pub enum RemoteIdentity<C> {
    /// The remote provided no identifying information.
    ///
    /// The identity of the remote is unknown and must be obtained through
    /// a different, out-of-band channel.
    Unknown,

    /// The remote provided a static DH public key.
    ///
    /// The static DH public key is authentic in the sense that a successful
    /// handshake implies that the remote possesses a corresponding secret key.
    ///
    /// > **Note**: To rule out active attacks like a MITM, trust in the public key must
    /// > still be established, e.g. by comparing the key against an expected or
    /// > otherwise known public key.
    StaticDhKey(PublicKey<C>),

    /// The remote provided a public identity key in addition to a static DH
    /// public key and the latter is authentic w.r.t. the former.
    ///
    /// > **Note**: To rule out active attacks like a MITM, trust in the public key must
    /// > still be established, e.g. by comparing the key against an expected or
    /// > otherwise known public key.
    IdentityKey(identity::PublicKey),
}

/// The options for identity exchange in an authenticated handshake.
///
/// > **Note**: Even if a remote's public identity key is known a priori,
/// > unless the authenticity of the key is [linked](Protocol::linked) to
/// > the authenticity of a remote's static DH public key, an authenticated
/// > handshake will still send the associated signature of the provided
/// > local [`KeypairIdentity`] in order for the remote to verify that the static
/// > DH public key is authentic w.r.t. the known public identity key.
pub enum IdentityExchange {
    /// Send the local public identity to the remote.
    ///
    /// The remote identity is unknown (i.e. expected to be received).
    Mutual,
    /// Send the local public identity to the remote.
    ///
    /// The remote identity is known.
    Send { remote: identity::PublicKey },
    /// Don't send the local public identity to the remote.
    ///
    /// The remote identity is unknown, i.e. expected to be received.
    Receive,
    /// Don't send the local public identity to the remote.
    ///
    /// The remote identity is known, thus identities must be mutually known
    /// in order for the handshake to succeed.
    None { remote: identity::PublicKey },
}

/// A future performing a Noise handshake pattern.
pub struct Handshake<T, C>(
    Pin<Box<dyn Future<Output = Result<(RemoteIdentity<C>, NoiseOutput<T>), NoiseError>> + Send>>,
);

impl<T, C> Future for Handshake<T, C> {
    type Output = Result<(RemoteIdentity<C>, NoiseOutput<T>), NoiseError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> task::Poll<Self::Output> {
        Pin::new(&mut self.0).poll(ctx)
    }
}

/// Creates an authenticated Noise handshake for the initiator of a
/// single roundtrip (2 message) handshake pattern.
///
/// Subject to the chosen [`IdentityExchange`], this message sequence
/// identifies the local node to the remote with the first message payload
/// (i.e. unencrypted) and expects the remote to identify itself in the
/// second message payload.
///
/// This message sequence is suitable for authenticated 2-message Noise handshake
/// patterns where the static keys of the initiator and responder are either
/// known (i.e. appear in the pre-message pattern) or are sent with
/// the first and second message, respectively (e.g. `IK` or `IX`).
///
/// ```raw
/// initiator -{id}-> responder
/// initiator <-{id}- responder
/// ```
pub fn rt1_initiator<T, C>(
    io: T,
    session: Result<snow::HandshakeState, NoiseError>,
    identity: KeypairIdentity,
    identity_x: IdentityExchange,
) -> Handshake<T, C>
where
    T: AsyncWrite + AsyncRead + Send + Unpin + 'static,
    C: Protocol<C> + AsRef<[u8]>,
{
    Handshake(Box::pin(async move {
        let mut state = State::new(io, session, identity, identity_x)?;
        send_identity(&mut state).await?;
        recv_identity(&mut state).await?;
        state.finish()
    }))
}

/// Creates an authenticated Noise handshake for the responder of a
/// single roundtrip (2 message) handshake pattern.
///
/// Subject to the chosen [`IdentityExchange`], this message sequence expects the
/// remote to identify itself in the first message payload (i.e. unencrypted)
/// and identifies the local node to the remote in the second message payload.
///
/// This message sequence is suitable for authenticated 2-message Noise handshake
/// patterns where the static keys of the initiator and responder are either
/// known (i.e. appear in the pre-message pattern) or are sent with the first
/// and second message, respectively (e.g. `IK` or `IX`).
///
/// ```raw
/// initiator -{id}-> responder
/// initiator <-{id}- responder
/// ```
pub fn rt1_responder<T, C>(
    io: T,
    session: Result<snow::HandshakeState, NoiseError>,
    identity: KeypairIdentity,
    identity_x: IdentityExchange,
) -> Handshake<T, C>
where
    T: AsyncWrite + AsyncRead + Send + Unpin + 'static,
    C: Protocol<C> + AsRef<[u8]>,
{
    Handshake(Box::pin(async move {
        let mut state = State::new(io, session, identity, identity_x)?;
        recv_identity(&mut state).await?;
        send_identity(&mut state).await?;
        state.finish()
    }))
}

/// Creates an authenticated Noise handshake for the initiator of a
/// 1.5-roundtrip (3 message) handshake pattern.
///
/// Subject to the chosen [`IdentityExchange`], this message sequence expects
/// the remote to identify itself in the second message payload and
/// identifies the local node to the remote in the third message payload.
/// The first (unencrypted) message payload is always empty.
///
/// This message sequence is suitable for authenticated 3-message Noise handshake
/// patterns where the static keys of the responder and initiator are either known
/// (i.e. appear in the pre-message pattern) or are sent with the second and third
/// message, respectively (e.g. `XX`).
///
/// ```raw
/// initiator --{}--> responder
/// initiator <-{id}- responder
/// initiator -{id}-> responder
/// ```
pub fn rt15_initiator<T, C>(
    io: T,
    session: Result<snow::HandshakeState, NoiseError>,
    identity: KeypairIdentity,
    identity_x: IdentityExchange,
) -> Handshake<T, C>
where
    T: AsyncWrite + AsyncRead + Unpin + Send + 'static,
    C: Protocol<C> + AsRef<[u8]>,
{
    Handshake(Box::pin(async move {
        let mut state = State::new(io, session, identity, identity_x)?;
        send_empty(&mut state).await?;
        recv_identity(&mut state).await?;
        send_identity(&mut state).await?;
        state.finish()
    }))
}

/// Creates an authenticated Noise handshake for the responder of a
/// 1.5-roundtrip (3 message) handshake pattern.
///
/// Subject to the chosen [`IdentityExchange`], this message sequence
/// identifies the local node in the second message payload and expects
/// the remote to identify itself in the third message payload. The first
/// (unencrypted) message payload is always empty.
///
/// This message sequence is suitable for authenticated 3-message Noise handshake
/// patterns where the static keys of the responder and initiator are either known
/// (i.e. appear in the pre-message pattern) or are sent with the second and third
/// message, respectively (e.g. `XX`).
///
/// ```raw
/// initiator --{}--> responder
/// initiator <-{id}- responder
/// initiator -{id}-> responder
/// ```
pub fn rt15_responder<T, C>(
    io: T,
    session: Result<snow::HandshakeState, NoiseError>,
    identity: KeypairIdentity,
    identity_x: IdentityExchange,
) -> Handshake<T, C>
where
    T: AsyncWrite + AsyncRead + Unpin + Send + 'static,
    C: Protocol<C> + AsRef<[u8]>,
{
    Handshake(Box::pin(async move {
        let mut state = State::new(io, session, identity, identity_x)?;
        recv_empty(&mut state).await?;
        send_identity(&mut state).await?;
        recv_identity(&mut state).await?;
        state.finish()
    }))
}

//////////////////////////////////////////////////////////////////////////////
// Internal

/// Handshake state.
struct State<T> {
    /// The underlying I/O resource.
    io: NoiseOutput<T>,
    /// The associated public identity of the local node's static DH keypair,
    /// which can be sent to the remote as part of an authenticated handshake.
    identity: KeypairIdentity,
    /// The received signature over the remote's static DH public key, if any.
    dh_remote_pubkey_sig: Option<Vec<u8>>,
    /// The known or received public identity key of the remote, if any.
    id_remote_pubkey: Option<identity::PublicKey>,
    /// Whether to send the public identity key of the local node to the remote.
    send_identity: bool,
}

impl<T> State<T> {
    /// Initializes the state for a new Noise handshake, using the given local
    /// identity keypair and local DH static public key. The handshake messages
    /// will be sent and received on the given I/O resource and using the
    /// provided session for cryptographic operations according to the chosen
    /// Noise handshake pattern.
    fn new(
        io: T,
        session: Result<snow::HandshakeState, NoiseError>,
        identity: KeypairIdentity,
        identity_x: IdentityExchange,
    ) -> Result<Self, NoiseError> {
        let (id_remote_pubkey, send_identity) = match identity_x {
            IdentityExchange::Mutual => (None, true),
            IdentityExchange::Send { remote } => (Some(remote), true),
            IdentityExchange::Receive => (None, false),
            IdentityExchange::None { remote } => (Some(remote), false),
        };
        session.map(|s| State {
            identity,
            io: NoiseOutput::new(io, SnowState::Handshake(s)),
            dh_remote_pubkey_sig: None,
            id_remote_pubkey,
            send_identity,
        })
    }
}

impl<T> State<T> {
    /// Finish a handshake, yielding the established remote identity and the
    /// [`NoiseOutput`] for communicating on the encrypted channel.
    fn finish<C>(self) -> Result<(RemoteIdentity<C>, NoiseOutput<T>), NoiseError>
    where
        C: Protocol<C> + AsRef<[u8]>,
    {
        let dh_remote_pubkey = match self.io.session.get_remote_static() {
            None => None,
            Some(k) => match C::public_from_bytes(k) {
                Err(e) => return Err(e),
                Ok(dh_pk) => Some(dh_pk),
            },
        };
        match self.io.session.into_transport_mode() {
            Err(e) => Err(e.into()),
            Ok(s) => {
                let remote = match (self.id_remote_pubkey, dh_remote_pubkey) {
                    (_, None) => RemoteIdentity::Unknown,
                    (None, Some(dh_pk)) => RemoteIdentity::StaticDhKey(dh_pk),
                    (Some(id_pk), Some(dh_pk)) => {
                        if C::verify(&id_pk, &dh_pk, &self.dh_remote_pubkey_sig) {
                            RemoteIdentity::IdentityKey(id_pk)
                        } else {
                            return Err(NoiseError::InvalidKey);
                        }
                    }
                };
                Ok((
                    remote,
                    NoiseOutput {
                        session: SnowState::Transport(s),
                        ..self.io
                    },
                ))
            }
        }
    }
}

//////////////////////////////////////////////////////////////////////////////
// Handshake Message Futures

/// A future for receiving a Noise handshake message with an empty payload.
async fn recv_empty<T>(state: &mut State<T>) -> Result<(), NoiseError>
where
    T: AsyncRead + Unpin,
{
    state.io.read(&mut []).await?;
    Ok(())
}

/// A future for sending a Noise handshake message with an empty payload.
async fn send_empty<T>(state: &mut State<T>) -> Result<(), NoiseError>
where
    T: AsyncWrite + Unpin,
{
    state.io.write(&[]).await?;
    state.io.flush().await?;
    Ok(())
}

/// A future for receiving a Noise handshake message with a payload
/// identifying the remote.
async fn recv_identity<T>(state: &mut State<T>) -> Result<(), NoiseError>
where
    T: AsyncRead + Unpin,
{
    log::debug!("Reading identity");
    let mut payload_buf = Vec::with_capacity(64);

    let bytes_read = ReadToEnd::new(&mut state.io, &mut payload_buf).await?;
    log::debug!("Total bytes read: {}", bytes_read);
    let pb = payload_proto::NoiseHandshakePayload::decode(&payload_buf[..])?;

    if !pb.identity_key.is_empty() {
        let pk = identity::PublicKey::from_protobuf_encoding(&pb.identity_key)
            .map_err(|_| NoiseError::InvalidKey)?;
        if let Some(ref k) = state.id_remote_pubkey {
            if k != &pk {
                return Err(NoiseError::InvalidKey);
            }
        }
        state.id_remote_pubkey = Some(pk);
    }
    if !pb.identity_sig.is_empty() {
        state.dh_remote_pubkey_sig = Some(pb.identity_sig);
    }

    Ok(())
}

/// Send a Noise handshake message with a payload identifying the local node to the remote.
async fn send_identity<T>(state: &mut State<T>) -> Result<(), NoiseError>
where
    T: AsyncWrite + Unpin,
{
    let mut pb = payload_proto::NoiseHandshakePayload::default();
    if state.send_identity {
        pb.identity_key = state.identity.public.clone().into_protobuf_encoding()
    }
    if let Some(ref sig) = state.identity.signature {
        pb.identity_sig = sig.clone()
    }
    let mut buf = Vec::with_capacity(pb.encoded_len());
    pb.encode(&mut buf)
        .expect("Vec<u8> provides capacity as needed");
    state.io.write_all(&buf).await?;
    state.io.flush().await?;
    Ok(())
}

// NOTE: THIS IS A VERY DIRTY HACK AS A TEMPORARY WORKAROUND
// Hack to read identity buffer without a length prefix
//
// Modified AsyncReadExt::ReadToEnd()
use std::io;
use std::ptr;

#[inline]
unsafe fn initialize<R: AsyncRead>(_reader: &R, buf: &mut [u8]) {
    #[cfg(feature = "read-initializer")]
    {
        if !_reader.initializer().should_initialize() {
            return;
        }
    }
    ptr::write_bytes(buf.as_mut_ptr(), 0, buf.len())
}

#[derive(Debug)]
pub struct ReadToEnd<'a, R: ?Sized> {
    reader: &'a mut R,
    buf: &'a mut Vec<u8>,
    start_len: usize,
}

impl<R: ?Sized + Unpin> Unpin for ReadToEnd<'_, R> {}

impl<'a, R: AsyncRead + ?Sized + Unpin> ReadToEnd<'a, R> {
    pub(super) fn new(reader: &'a mut R, buf: &'a mut Vec<u8>) -> Self {
        let start_len = buf.len();
        Self {
            reader,
            buf,
            start_len,
        }
    }
}

struct Guard<'a> {
    buf: &'a mut Vec<u8>,
    len: usize,
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        unsafe {
            self.buf.set_len(self.len);
        }
    }
}

// This uses an adaptive system to extend the vector when it fills. We want to
// avoid paying to allocate and zero a huge chunk of memory if the reader only
// has 4 bytes while still making large reads if the reader does have a ton
// of data to return. Simply tacking on an extra DEFAULT_BUF_SIZE space every
// time is 4,500 times (!) slower than this if the reader has a very small
// amount of data to return.
//
// Because we're extending the buffer with uninitialized data for trusted
// readers, we need to make sure to truncate that if any of this panics.
pub(super) fn read_to_end_internal<R: AsyncRead + ?Sized>(
    mut rd: Pin<&mut R>,
    cx: &mut Context<'_>,
    buf: &mut Vec<u8>,
    start_len: usize,
) -> Poll<io::Result<usize>> {
    let mut g = Guard {
        len: buf.len(),
        buf,
    };
    let mut first_poll = true;
    let ret;
    loop {
        if g.len == g.buf.len() {
            unsafe {
                g.buf.reserve(32);
                let capacity = g.buf.capacity();
                g.buf.set_len(capacity);
                initialize(&rd, &mut g.buf[g.len..]);
            }
        }

        match rd.as_mut().poll_read(cx, &mut g.buf[g.len..]) {
            Poll::Ready(Ok(0)) => {
                ret = Poll::Ready(Ok(g.len - start_len));
                break;
            }
            Poll::Ready(Ok(n)) => {
                g.len += n;
                first_poll = false;
            }
            Poll::Pending => {
                if first_poll {
                    return Poll::Pending;
                } else {
                    ret = Poll::Ready(Ok(g.len - start_len));
                    break;
                }
            }
            Poll::Ready(Err(e)) => {
                ret = Poll::Ready(Err(e));
                break;
            }
        }
    }

    ret
}

impl<A> Future for ReadToEnd<'_, A>
where
    A: AsyncRead + ?Sized + Unpin,
{
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        read_to_end_internal(Pin::new(&mut this.reader), cx, this.buf, this.start_len)
    }
}
