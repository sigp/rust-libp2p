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

use std::any::Any;
use std::cmp::Ordering;
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
    type Metadata: Metadata;

    /// Returns the unique identifier for this message group.
    ///
    /// All partial messages belonging to the same logical message should return
    /// the same group ID. This is used to associate partial messages together
    /// during reconstruction.
    fn group_id(&self) -> Vec<u8>;

    /// Returns application defined metadata describing which parts of the message
    /// are available and which parts we want.
    ///
    /// The returned bytes will be sent in partsMetadata field to advertise
    /// available and wanted parts to peers.
    fn metadata(&self) -> &Self::Metadata;

    /// Generates partial message bytes from the given metadata.
    ///
    /// When a peer requests specific parts (via PartialIWANT), this method
    /// generates the actual message data to send back. The `metadata` parameter
    /// describes what parts are being requested.
    ///
    /// Returns a [`PublishAction`] for the given metadata, or an error.
    fn partial_message_bytes_from_metadata(
        &self,
        metadata: &Self::Metadata,
    ) -> Result<Option<Vec<u8>>, PartialMessageError>;

    fn data_for_eager_push(&self)
        -> Result<Option<(Vec<u8>, Self::Metadata)>, PartialMessageError>;
}

pub(crate) trait DynamicPartial {
    fn decode_metadata(&self, metadata: &[u8]) -> Result<Box<dyn DynamicMetadata>, PartialMessageError>;

    fn metadata(&self) -> &dyn DynamicMetadata;

    /// Generates partial message bytes from the given metadata.
    ///
    /// When a peer requests specific parts (via PartialIWANT), this method
    /// generates the actual message data to send back. The `metadata` parameter
    /// describes what parts are being requested.
    ///
    /// Returns a [`PublishAction`] for the given metadata, or an error.
    fn partial_message_bytes_from_metadata(
        &self,
        metadata: &dyn DynamicMetadata,
    ) -> Result<Option<Vec<u8>>, PartialMessageError>;
}

impl<P: Partial> DynamicPartial for P {
    fn decode_metadata(&self, metadata: &[u8]) -> Result<Box<dyn DynamicMetadata>, PartialMessageError> {
        Ok(Box::new(P::Metadata::decode(metadata)?))
    }

    fn metadata(&self) -> &dyn DynamicMetadata {
        self.metadata()
    }

    fn partial_message_bytes_from_metadata(
        &self,
        metadata: &dyn DynamicMetadata,
    ) -> Result<Option<Vec<u8>>, PartialMessageError> {
        let metadata: &dyn Any = metadata;
        let Some(metadata) = metadata.downcast_ref::<P::Metadata>() else {
            return Err(PartialMessageError::InvalidFormat)
        };
        self.partial_message_bytes_from_metadata(metadata)
    }
}

pub trait Metadata: Debug + Send + Sync + Any {
    fn decode(bytes: &[u8]) -> Result<Self, PartialMessageError>
    where
        Self: Sized;

    fn compare(&self, other: &Self) -> Option<Ordering>;
    // Return the `Metadata` as a byte slice.
    fn encode(&self) -> Vec<u8>;
    /// try to Update the `Metadata` with the remote data,
    /// return true if it was updated.
    fn update(&mut self, data: &Self) -> Result<bool, PartialMessageError>;
}

pub(crate) trait DynamicMetadata: Debug + Send + Sync + Any {
    /// try to Update the `Metadata` with the remote data,
    /// return true if it was updated.
    fn update(&mut self, data: &[u8]) -> Result<bool, PartialMessageError>;
    /// try to Update the `Metadata` with the remote data,
    /// return true if it was updated.
    fn update_dynamic(&mut self, data: &dyn DynamicMetadata) -> Result<bool, PartialMessageError>;
    fn encode(&self) -> Vec<u8>;
}

impl<M: Metadata> DynamicMetadata for M {
    fn update(&mut self, metadata: &[u8]) -> Result<bool, PartialMessageError> {
        self.update(&M::decode(metadata)?)
    }

    fn update_dynamic(&mut self, metadata: &dyn DynamicMetadata) -> Result<bool, PartialMessageError> {
        let metadata: &dyn Any = metadata;
        let Some(metadata) = metadata.downcast_ref::<M>() else {
            return Err(PartialMessageError::InvalidFormat)
        };
        self.update(metadata)
    }

    fn encode(&self) -> Vec<u8> {
        self.encode()
    }
}
