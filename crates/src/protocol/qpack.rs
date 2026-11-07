//! QPACK codec.

use std::cmp;
use std::ffi::{c_char, c_int, c_void};
use std::marker::PhantomPinned;
use std::mem::{self, MaybeUninit};
use std::pin::Pin;
use std::ptr;
use std::slice;

use bytes::Bytes;
use tracing::debug;

use crate::common::types::{Headers, StreamId};
use crate::protocol::qpack::sys::{
    lsqpack_dec, lsqpack_dec_hset_if, lsqpack_enc, lsqpack_enc_status, lsqpack_read_header_status,
    lsxpack_header,
};

mod sys {
    #![allow(warnings, clippy::all, clippy::pedantic, clippy::restriction)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub(super) struct Decoder {
    inner: Pin<Box<InnerDecoder>>,
}

unsafe impl Send for Decoder {}

impl Decoder {
    pub(super) fn new() -> Self {
        let mut decoder = Self {
            inner: Box::pin(InnerDecoder {
                _pin: PhantomPinned,
                decoder: unsafe { MaybeUninit::zeroed().assume_init() },
                header_set_interface: lsqpack_dec_hset_if {
                    dhi_unblocked: None,
                    dhi_prepare_decode: Some(dhi_prepare_decode),
                    dhi_process_header: Some(dhi_process_header),
                },
            }),
        };

        let inner = unsafe { decoder.inner.as_mut().get_unchecked_mut() };

        unsafe {
            sys::lsqpack_dec_init(
                &raw mut inner.decoder,
                ptr::null_mut(),
                0,
                0,
                &raw const inner.header_set_interface,
                0,
            );
        }

        decoder
    }

    pub(super) fn decode(
        &mut self,
        stream_id: StreamId,
        data: &[u8],
    ) -> Result<Headers, QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        let mut context = HeaderBlockContext {
            error: None,
            header: unsafe { MaybeUninit::zeroed().assume_init() },
            header_buffer: Vec::with_capacity(4096),
            headers: Headers::new(),
        };
        let mut cursor = data.as_ptr();

        // SAFETY: The decoder releases the context on return because a complete block never suspends without a dynamic table.
        let status = unsafe {
            sys::lsqpack_dec_header_in(
                &raw mut inner.decoder,
                (&raw mut context).cast::<c_void>(),
                stream_id,
                data.len(),
                &raw mut cursor,
                data.len(),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };

        if let Some(e) = context.error {
            return Err(e);
        }

        if status != lsqpack_read_header_status::LQRHS_DONE {
            return Err(QpackError::DecoderError);
        }

        Ok(context.headers)
    }

    pub(super) fn feed_encoder(&mut self, data: &[u8]) -> Result<(), QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        let result =
            unsafe { sys::lsqpack_dec_enc_in(&raw mut inner.decoder, data.as_ptr(), data.len()) };

        if result == 0 {
            Ok(())
        } else {
            Err(QpackError::DecoderError)
        }
    }
}

pub(super) struct Encoder {
    inner: Pin<Box<InnerEncoder>>,
}

unsafe impl Send for Encoder {}

impl Encoder {
    pub(super) fn new() -> Self {
        let mut encoder = Self {
            inner: Box::pin(InnerEncoder {
                _pin: PhantomPinned,
                encoder: unsafe { MaybeUninit::zeroed().assume_init() },
                header_buffer: Vec::new(),
            }),
        };

        let inner = unsafe { encoder.inner.as_mut().get_unchecked_mut() };

        unsafe {
            sys::lsqpack_enc_preinit(&raw mut inner.encoder, ptr::null_mut());
        }

        encoder
    }

    pub(super) fn encode(
        &mut self,
        stream_id: StreamId,
        headers: &Headers,
    ) -> Result<Vec<u8>, QpackError> {
        let mut qpack_headers = Vec::with_capacity(headers.len());
        for (key, value) in headers {
            qpack_headers.push(Header::new(key, value)?);
        }

        let mut lsxpack_headers = Vec::with_capacity(qpack_headers.len());
        for header in &mut qpack_headers {
            lsxpack_headers.push(header.create_lsxpack_header());
        }

        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        if unsafe { sys::lsqpack_enc_start_header(&raw mut inner.encoder, stream_id, 0) } != 0 {
            return Err(QpackError::EncoderError);
        }

        inner.header_buffer.clear();
        inner.header_buffer.reserve(4096);

        for header in &mut lsxpack_headers {
            inner.encode_single_header(header)?;
        }

        let max_prefix =
            unsafe { sys::lsqpack_enc_header_block_prefix_size(&raw const inner.encoder) };
        let total_length = max_prefix + inner.header_buffer.len();
        let mut final_header_block = Vec::with_capacity(total_length);

        let result = unsafe {
            sys::lsqpack_enc_end_header(
                &raw mut inner.encoder,
                final_header_block.as_mut_ptr(),
                max_prefix,
                ptr::null_mut(),
            )
        };

        let prefix_length = match usize::try_from(result) {
            Ok(prefix_length) if prefix_length > 0 => prefix_length,
            _ => return Err(QpackError::EncoderError),
        };

        // SAFETY: FFI execution and contiguous non-overlapping copy strictly initialize the extended length.
        unsafe {
            ptr::copy_nonoverlapping(
                inner.header_buffer.as_ptr(),
                final_header_block.as_mut_ptr().add(prefix_length),
                inner.header_buffer.len(),
            );
            final_header_block.set_len(prefix_length + inner.header_buffer.len());
        }

        Ok(final_header_block)
    }

    pub(super) fn feed_decoder(&mut self, data: &[u8]) -> Result<(), QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        let result = unsafe {
            sys::lsqpack_enc_decoder_in(&raw mut inner.encoder, data.as_ptr(), data.len())
        };

        if result == 0 {
            Ok(())
        } else {
            Err(QpackError::EncoderError)
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum QpackError {
    DecoderError,
    EncoderError,
    HeaderTooLong,
}

#[derive(Debug)]
struct Header {
    buffer: Vec<u8>,
    name_length: u16,
    value_length: u16,
}

impl Header {
    fn new<N, V>(name: N, value: V) -> Result<Self, QpackError>
    where
        N: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let name = name.as_ref();
        let value = value.as_ref();

        let name_length = name.len().try_into().map_err(|e| {
            debug!(
                "qpack_field_name convert failed actual={} expected=u16 err={e:?}",
                name.len()
            );
            QpackError::HeaderTooLong
        })?;
        let value_length = value.len().try_into().map_err(|e| {
            debug!(
                "qpack_field_value convert failed actual={} expected=u16 err={e:?}",
                value.len()
            );
            QpackError::HeaderTooLong
        })?;

        let mut buffer = Vec::with_capacity(name.len() + value.len());
        buffer.extend_from_slice(name);
        buffer.extend_from_slice(value);

        Ok(Self {
            buffer,
            name_length,
            value_length,
        })
    }

    fn create_lsxpack_header(&mut self) -> lsxpack_header {
        let mut header: lsxpack_header = unsafe { MaybeUninit::zeroed().assume_init() };

        header.buf = self.buffer.as_mut_ptr().cast::<c_char>();
        header.name_len = self.name_length;
        header.name_offset = 0;
        header.val_len = self.value_length;
        header.val_offset = i32::from(self.name_length);

        header
    }
}

struct HeaderBlockContext {
    error: Option<QpackError>,
    header: lsxpack_header,
    header_buffer: Vec<u8>,
    headers: Headers,
}

struct InnerDecoder {
    _pin: PhantomPinned,
    decoder: lsqpack_dec,
    header_set_interface: lsqpack_dec_hset_if,
}

impl Drop for InnerDecoder {
    fn drop(&mut self) {
        // SAFETY: lsqpack_dec_cleanup exclusively frees decoder-internal memory and invokes no callbacks.
        unsafe { sys::lsqpack_dec_cleanup(&raw mut self.decoder) };
    }
}

struct InnerEncoder {
    _pin: PhantomPinned,
    encoder: lsqpack_enc,
    header_buffer: Vec<u8>,
}

impl Drop for InnerEncoder {
    fn drop(&mut self) {
        // SAFETY: lsqpack_enc_cleanup exclusively frees encoder-internal memory and invokes no callbacks.
        unsafe { sys::lsqpack_enc_cleanup(&raw mut self.encoder) };
    }
}

impl InnerEncoder {
    fn encode_single_header(&mut self, header: &mut lsxpack_header) -> Result<(), QpackError> {
        let header_offset = self.header_buffer.len();
        self.header_buffer
            .reserve(usize::from(header.name_len) + usize::from(header.val_len) + 256);

        let mut encoder_written = 0;
        let mut header_written = self.header_buffer.capacity() - header_offset;
        let header_cursor = unsafe { self.header_buffer.as_mut_ptr().add(header_offset) };

        // SAFETY: The encoder never writes to the zero-length encoder stream buffer without a dynamic table.
        let status = unsafe {
            sys::lsqpack_enc_encode(
                &raw mut self.encoder,
                header_cursor,
                &raw mut encoder_written,
                header_cursor,
                &raw mut header_written,
                header,
                0,
            )
        };

        if status != lsqpack_enc_status::LQES_OK {
            return Err(QpackError::EncoderError);
        }

        // SAFETY: FFI contract guarantees header_written bytes are initialized upon LQES_OK.
        unsafe {
            self.header_buffer.set_len(header_offset + header_written);
        }

        Ok(())
    }
}

unsafe extern "C" fn dhi_prepare_decode(
    hblock_ctx: *mut c_void,
    header: *mut lsxpack_header,
    space: usize,
) -> *mut lsxpack_header {
    let hblock_ctx = unsafe { &mut *hblock_ctx.cast::<HeaderBlockContext>() };

    let used_bytes = if header.is_null() {
        hblock_ctx.header = unsafe { mem::zeroed() };
        0
    } else {
        hblock_ctx.header = unsafe { *header };
        let header = &hblock_ctx.header;

        let name_offset = usize::try_from(header.name_offset).unwrap_or(0);
        let val_offset = usize::try_from(header.val_offset).unwrap_or(0);

        let name_end = name_offset + usize::from(header.name_len);
        let value_end = val_offset + usize::from(header.val_len);
        cmp::max(name_end, value_end)
    };

    let Ok(val_len) = u16::try_from(space) else {
        hblock_ctx.error = Some(QpackError::HeaderTooLong);
        return ptr::null_mut();
    };

    // SAFETY: Synchronization of the Rust Vec's view of physical bytes written by the C library.
    unsafe {
        hblock_ctx.header_buffer.set_len(used_bytes);
    }
    hblock_ctx
        .header_buffer
        .reserve(space.saturating_sub(used_bytes));

    hblock_ctx.header.buf = hblock_ctx.header_buffer.as_mut_ptr().cast::<c_char>();
    hblock_ctx.header.val_len = val_len;

    &raw mut hblock_ctx.header
}

unsafe extern "C" fn dhi_process_header(
    hblock_ctx: *mut c_void,
    header: *mut lsxpack_header,
) -> c_int {
    let hblock_ctx = unsafe { &mut *hblock_ctx.cast::<HeaderBlockContext>() };

    if hblock_ctx.error.is_some() {
        return 1;
    }

    let header = unsafe { &*header };

    let Ok(name_offset) = usize::try_from(header.name_offset) else {
        return 1;
    };
    let Ok(val_offset) = usize::try_from(header.val_offset) else {
        return 1;
    };

    let name = unsafe {
        slice::from_raw_parts(
            header.buf.add(name_offset).cast::<u8>(),
            usize::from(header.name_len),
        )
    };
    let value = unsafe {
        slice::from_raw_parts(
            header.buf.add(val_offset).cast::<u8>(),
            usize::from(header.val_len),
        )
    };

    hblock_ctx
        .headers
        .push((Bytes::copy_from_slice(name), Bytes::copy_from_slice(value)));

    0
}

#[cfg(test)]
mod tests;
