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

use std::fmt::Debug;

use crate::error::PartialMessageError;

/// PartialMessage is a message that can be broken up into parts.
/// This trait allows applications to define custom strategies for splitting large messages
/// into parts and reconstructing them from received partial data. It provides the core
/// operations needed for the gossipsub partial messages extension.
///
/// The partial message protocol works as follows:
/// 1. Applications implement this trait to define how messages are split and reconstructed
/// 2. Peers advertise available parts using `available_parts()` metadata in PartialIHAVE
/// 3. Peers request missing parts using `missing_parts()` metadata in PartialIWANT
/// 4. When requests are received, `partial_message_bytes_from_metadata()` generates the response
/// 5. Received partial data is integrated using `extend_from_encoded_partial_message()`
/// 6. The `group_id()` ties all parts of the same logical message together
pub trait Partial {
    /// Returns the unique identifier for this message group.
    ///
    /// All partial messages belonging to the same logical message should return
    /// the same group ID. This is used to associate partial messages together
    /// during reconstruction.
    fn group_id(&self) -> impl AsRef<[u8]>;

    /// Returns application defined metadata describing which parts of the message
    /// are available and which parts we want.
    ///
    /// The returned bytes will be sent in partsMetadata field to advertise
    /// available and wanted parts to peers.
    fn parts_metadata(&self) -> impl AsRef<[u8]>;

    /// Generates partial message bytes from the given metadata.
    ///
    /// When a peer requests specific parts (via PartialIWANT), this method
    /// generates the actual message data to send back. The `metadata` parameter
    /// describes what parts are being requested.
    ///
    /// Returns a [`PublishAction`] for the given metadata, or an error.
    fn partial_message_bytes_from_metadata(
        &self,
        metadata: Option<impl AsRef<[u8]>>,
    ) -> Result<PublishAction, PartialMessageError>;
}

pub trait Metadata: Debug + Send + Sync {
    /// Return the `Metadata` as a byte slice.
    fn as_slice(&self) -> &[u8];
    /// try to Update the `Metadata` with the remote data,
    /// return true if it was updated.
    fn update(&mut self, data: &[u8]) -> Result<bool, PartialMessageError>;
}

/// Indicates the action to take for the given metadata.
pub enum PublishAction {
    /// The provided input metadata is the same as the output,
    /// this means we have the same data as the peer.
    SameMetadata,
    /// We have nothing to send to the peer, but we need parts from the peer.
    NothingToSend,
    /// We have something of interest to this peer, but can not send everything it needs. Send a
    /// message and associate some new metadata to the peer, representing the remaining need.
    Send {
        message: Vec<u8>,
        metadata: Box<dyn Metadata>,
    },
}
