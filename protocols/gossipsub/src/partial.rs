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
    fn group_id(&self) -> &[u8];

    /// Returns metadata describing which parts of the message are missing.
    ///
    /// This metadata is application-defined and should encode information about
    /// what parts need to be requested from other peers. Returns `None` if the
    /// message is complete or if no specific parts can be identified as missing.
    ///
    /// The returned bytes will be sent in PartialIWANT messages to request
    /// missing parts from peers.
    fn missing_parts(&self) -> Option<&[u8]>;

    /// Returns metadata describing which parts of the message are available.
    ///
    /// This metadata is application-defined and should encode information about
    /// what parts this peer can provide to others. Returns `None` if no parts
    /// are available.
    ///
    /// The returned bytes will be sent in PartialIHAVE messages to advertise
    /// available parts to peers.
    fn available_parts(&self) -> Option<&[u8]>;

    /// Generates partial message bytes from the given metadata.
    ///
    /// When a peer requests specific parts (via PartialIWANT), this method
    /// generates the actual message data to send back. The `metadata` parameter
    /// describes what parts are being requested.
    ///
    /// Returns a tuple of:
    /// - The encoded partial message bytes to send over the network
    /// - Optional remaining metadata if more parts are still available after this one
    fn partial_message_bytes_from_metadata(&self, metadata: &[u8]) -> (Vec<u8>, Option<Vec<u8>>);

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
        data: Vec<u8>,
    ) -> Result<(), PartialMessageError>;

    /// Consumes self and returns the message data.
    ///
    /// This method should only be called when the partial message reconstruction
    /// is complete (i.e., when `missing_parts()` returns `None`).
    /// Calling this method on an incomplete partial message may return partial data,
    /// invalid data, or panic, depending on the implementation.
    ///
    /// # Returns
    ///
    /// The complete message data as a `Vec<u8>`. The format and contents of this
    /// data are application-defined and should match what would have been sent
    /// in a regular gossipsub message.
    fn into_data(self) -> Vec<u8>;

    /// Returns the complete message data without consuming self.
    ///
    /// This method provides access to the reconstructed message data for use cases
    /// like eager pushing, where the partial message implementation needs to send
    /// complete or substantial portions of the message proactively to peers.
    ///
    /// Unlike `into_data()`, this method does not consume self, allowing the
    /// partial message to continue participating in the reconstruction process.
    /// The returned data should represent the current state of reconstruction -
    /// this may be incomplete data, complete data, or application-specific
    /// encoded data depending on the implementation's needs.
    ///
    /// This method is called during `publish_partial` operations when
    /// applications want to eagerly push data to peers without waiting for
    /// explicit requests via PartialIWANT messages.
    fn as_data(&self) -> &[u8];
}

/// Default implementation that disables partial messages.
impl Partial for () {
    fn group_id(&self) -> &[u8] {
        &[]
    }

    fn missing_parts(&self) -> Option<&[u8]> {
        None
    }

    fn available_parts(&self) -> Option<&[u8]> {
        None
    }

    fn partial_message_bytes_from_metadata(&self, _metadata: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
        (vec![], None)
    }

    fn extend_from_encoded_partial_message(
        &mut self,
        _data: Vec<u8>,
    ) -> Result<(), PartialMessageError> {
        // This should never be called since we never advertise having or wanting parts,
        // but if it is called, just ignore the data silently
        Ok(())
    }

    fn into_data(self) -> Vec<u8> {
        vec![]
    }

    fn as_data(&self) -> &[u8] {
        &[]
    }
}
