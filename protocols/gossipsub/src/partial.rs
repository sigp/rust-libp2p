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

    /// Extends this message with received partial message data.
    ///
    /// When partial message data is received from a peer, this method integrates
    /// it into the current message state. The implementation should validate and
    /// store the received data appropriately.
    ///
    /// Returns `Ok(())` if the data was successfully integrated, or `Err`,
    /// if the data was invalid or couldn't be processed.
    fn extend_from_encoded_partial_message(
        &mut self,
        data: &[u8],
    ) -> Result<(), PartialMessageError>;
}

/// Indicates the action to take for the given metadata.
pub enum PublishAction {
    /// The metadata signals that the peer already has all data. Do not keep track of the peer
    /// anymore.
    PeerHasAllData,
    /// While the peer still needs data, we do not have any data it needs, and therefore send
    /// nothing but keep the metadata.
    NothingToSend,
    /// We have something of interest to this peer, but can not send everything it needs. Send a
    /// message and associate some new metadata to the peer, representing the remaining need.
    Send { message: Vec<u8>, metadata: Vec<u8> },
    /// We can send everything this peer needs. Send message, then do not keep track of the peer
    /// anymore.
    SendRemaining { message: Vec<u8> },
}
