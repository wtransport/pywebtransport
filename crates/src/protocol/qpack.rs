//! QPACK encoder and decoder bindings with safe Rust abstractions.

use std::cmp;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void};
use std::marker::PhantomPinned;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::ptr;
use std::slice;

use bytes::Bytes;

mod sys {
    #![allow(warnings, clippy::all, clippy::pedantic, clippy::restriction)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

/// Encoder dynamic table capacity physical limit.
const ENCODER_MAX_TABLE_CAPACITY_LIMIT: u32 = 4096;
/// Encoder maximum blocked streams physical limit.
const ENCODER_MAX_BLOCKED_STREAMS_LIMIT: u32 = 16;

// High-level wrapper for the QPACK Encoder.
pub(super) struct Encoder {
    inner: Pin<Box<InnerEncoder>>,
    max_blocked_streams: u32,
}

unsafe impl Send for Encoder {}

impl Encoder {
    // Encoder instance initialization.
    pub(super) fn new() -> Self {
        let mut inner = Box::pin(InnerEncoder {
            encoder: unsafe { MaybeUninit::zeroed().assume_init() },
            enc_buffer: Vec::new(),
            hdr_buffer: Vec::new(),
            _pin: PhantomPinned,
        });

        let inner_ptr = unsafe { inner.as_mut().get_unchecked_mut() };
        unsafe {
            sys::lsqpack_enc_init(
                &raw mut inner_ptr.encoder,
                ptr::null_mut(),
                ENCODER_MAX_TABLE_CAPACITY_LIMIT,
                0,
                ENCODER_MAX_BLOCKED_STREAMS_LIMIT,
                0,
                ptr::null_mut(),
                ptr::null_mut(),
            );
        }

        Self {
            inner,
            max_blocked_streams: ENCODER_MAX_BLOCKED_STREAMS_LIMIT,
        }
    }

    // Dynamic table capacity configuration.
    pub(super) fn apply_settings(
        &mut self,
        max_table_capacity: u64,
        blocked_streams: u64,
    ) -> Result<Vec<u8>, QpackError> {
        let blocked = u32::try_from(blocked_streams).unwrap_or(u32::MAX);
        if blocked > self.max_blocked_streams {
            return Err(QpackError::EncoderError);
        }

        let mut buffer = vec![0u8; 1024];
        let mut written: usize = buffer.len();

        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };
        let capacity = u32::try_from(max_table_capacity).unwrap_or(u32::MAX);

        let result = unsafe {
            sys::lsqpack_enc_set_max_capacity(
                &raw mut inner.encoder,
                capacity,
                buffer.as_mut_ptr().cast::<u8>(),
                &raw mut written,
            )
        };

        if result == 0 {
            buffer.truncate(written);
            Ok(buffer)
        } else {
            Err(QpackError::EncoderError)
        }
    }

    // Stream-specific header block encoding.
    pub(super) fn encode(
        &mut self,
        stream_id: u64,
        headers: &[(Bytes, Bytes)],
    ) -> Result<(Vec<u8>, Vec<u8>), QpackError> {
        let mut q_headers = Vec::with_capacity(headers.len());
        for (n, v) in headers {
            q_headers.push(Header::new(n, v)?);
        }

        let mut ls_headers = Vec::with_capacity(q_headers.len());
        for h in &mut q_headers {
            ls_headers.push(h.create_lsxpack_header());
        }

        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        if unsafe { sys::lsqpack_enc_start_header(&raw mut inner.encoder, stream_id, 0) } != 0 {
            return Err(QpackError::EncoderError);
        }

        inner.enc_buffer.clear();
        inner.hdr_buffer.clear();
        inner.enc_buffer.reserve(1024);
        inner.hdr_buffer.reserve(4096);

        for hdr in &mut ls_headers {
            let enc_off = inner.enc_buffer.len();
            let hdr_off = inner.hdr_buffer.len();

            if inner.enc_buffer.capacity() - enc_off < 256 {
                inner.enc_buffer.reserve(256);
            }
            if inner.hdr_buffer.capacity() - hdr_off < 1024 {
                inner.hdr_buffer.reserve(1024);
            }

            let mut enc_written = inner.enc_buffer.capacity() - enc_off;
            let mut hdr_written = inner.hdr_buffer.capacity() - hdr_off;

            let res = unsafe {
                sys::lsqpack_enc_encode(
                    &raw mut inner.encoder,
                    inner.enc_buffer.as_mut_ptr().add(enc_off).cast::<u8>(),
                    &raw mut enc_written,
                    inner.hdr_buffer.as_mut_ptr().add(hdr_off).cast::<u8>(),
                    &raw mut hdr_written,
                    hdr,
                    0,
                )
            };

            if res != sys::lsqpack_enc_status_LQES_OK {
                return Err(QpackError::EncoderError);
            }

            unsafe {
                inner.enc_buffer.set_len(enc_off + enc_written);
                inner.hdr_buffer.set_len(hdr_off + hdr_written);
            }
        }

        let max_prefix =
            unsafe { sys::lsqpack_enc_header_block_prefix_size(&raw const inner.encoder) };
        let mut final_hdr_block = vec![0u8; max_prefix + inner.hdr_buffer.len()];

        let res = unsafe {
            sys::lsqpack_enc_end_header(
                &raw mut inner.encoder,
                final_hdr_block.as_mut_ptr().cast::<u8>(),
                max_prefix,
                ptr::null_mut(),
            )
        };

        if res < 0 {
            return Err(QpackError::EncoderError);
        }

        let prefix_len = usize::try_from(res).map_err(|_e| QpackError::EncoderError)?;

        if prefix_len == 0 && max_prefix > 0 {
            return Err(QpackError::EncoderError);
        }

        unsafe {
            ptr::copy_nonoverlapping(
                inner.hdr_buffer.as_ptr(),
                final_hdr_block.as_mut_ptr().add(prefix_len),
                inner.hdr_buffer.len(),
            );
            final_hdr_block.set_len(prefix_len + inner.hdr_buffer.len());
        }

        Ok((final_hdr_block, std::mem::take(&mut inner.enc_buffer)))
    }

    // Decoder stream instruction ingestion.
    pub(super) fn feed_decoder(&mut self, data: &[u8]) {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };
        unsafe {
            sys::lsqpack_enc_decoder_in(
                &raw mut inner.encoder,
                data.as_ptr().cast::<u8>(),
                data.len(),
            );
        }
    }
}

// High-level wrapper for the QPACK Decoder.
pub(super) struct Decoder {
    inner: Pin<Box<InnerDecoder>>,
    pending_blocks: HashMap<u64, Pin<Box<PendingBlock>>>,
    unblocked_queue: Vec<u64>,
}

unsafe impl Send for Decoder {}

impl Decoder {
    // Decoder instance initialization.
    pub(super) fn new(max_table_size: u32, dyn_table_size: u32) -> Self {
        let mut inner = Box::pin(InnerDecoder {
            decoder: unsafe { MaybeUninit::zeroed().assume_init() },
            cb: sys::lsqpack_dec_hset_if {
                dhi_unblocked: Some(cb_unblocked),
                dhi_prepare_decode: Some(cb_prepare_decode),
                dhi_process_header: Some(cb_process_header),
            },
            dec_buffer: Vec::with_capacity(1024),
            _pin: PhantomPinned,
        });

        let inner_ptr = unsafe { inner.as_mut().get_unchecked_mut() };
        unsafe {
            sys::lsqpack_dec_init(
                &raw mut inner_ptr.decoder,
                ptr::null_mut(),
                max_table_size,
                dyn_table_size,
                &raw const inner_ptr.cb,
                0,
            );
        }

        Self {
            inner,
            pending_blocks: HashMap::new(),
            unblocked_queue: Vec::new(),
        }
    }

    // Header block decoding.
    pub(super) fn decode_header(
        &mut self,
        stream_id: u64,
        data: Bytes,
    ) -> Result<(Vec<u8>, DecodeStatus), QpackError> {
        let ctx = HeaderBlockCtx {
            unblocked_queue_ptr: &raw mut self.unblocked_queue,
            stream_id,
            headers: Vec::new(),
            header_buf: Vec::with_capacity(4096),
            header_struct: unsafe { MaybeUninit::zeroed().assume_init() },
            error: None,
        };

        let mut pending = Box::pin(PendingBlock {
            _stream_id: stream_id,
            data,
            ctx,
            _pin: PhantomPinned,
        });

        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        let target_size = cmp::max(1024, pending.data.len() * 2);
        if inner.dec_buffer.capacity() < target_size {
            inner
                .dec_buffer
                .reserve(target_size - inner.dec_buffer.len());
        }
        inner.dec_buffer.clear();
        inner.dec_buffer.resize(target_size, 0);
        let mut dec_len = target_size;

        let pending_ptr = unsafe { pending.as_mut().get_unchecked_mut() };
        let mut data_ptr = pending_ptr.data.as_ptr();
        let data_len = pending_ptr.data.len();

        let res = unsafe {
            sys::lsqpack_dec_header_in(
                &raw mut inner.decoder,
                (&raw mut pending_ptr.ctx).cast::<c_void>(),
                stream_id,
                data_len,
                &raw mut data_ptr,
                data_len,
                inner.dec_buffer.as_mut_ptr(),
                &raw mut dec_len,
            )
        };

        if dec_len > target_size {
            dec_len = target_size;
        }
        inner.dec_buffer.truncate(dec_len);
        let dec_instructions = inner.dec_buffer.clone();

        if res == sys::lsqpack_read_header_status_LQRHS_DONE {
            if let Some(e) = pending_ptr.ctx.error {
                Err(e)
            } else {
                Ok((
                    dec_instructions,
                    DecodeStatus::Complete(std::mem::take(&mut pending_ptr.ctx.headers)),
                ))
            }
        } else if res == sys::lsqpack_read_header_status_LQRHS_BLOCKED {
            self.pending_blocks.insert(stream_id, pending);
            Ok((dec_instructions, DecodeStatus::Blocked))
        } else {
            Err(QpackError::DecoderError)
        }
    }

    // Encoder stream instruction ingestion.
    pub(super) fn feed_encoder(&mut self, data: &[u8]) -> Result<Vec<u64>, QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        let res =
            unsafe { sys::lsqpack_dec_enc_in(&raw mut inner.decoder, data.as_ptr(), data.len()) };

        if res == 0 {
            let unblocked = std::mem::take(&mut self.unblocked_queue);
            Ok(unblocked)
        } else {
            Err(QpackError::DecoderError)
        }
    }

    // Blocked header processing resumption.
    pub(super) fn resume_header(
        &mut self,
        stream_id: u64,
    ) -> Result<Option<Vec<(Bytes, Bytes)>>, QpackError> {
        let Some(mut pending) = self.pending_blocks.remove(&stream_id) else {
            return Ok(None);
        };

        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };
        let pending_ptr = unsafe { pending.as_mut().get_unchecked_mut() };

        let mut data_ptr = pending_ptr.data.as_ptr();
        let data_len = pending_ptr.data.len();

        let target_size = cmp::max(1024, data_len * 2);
        if inner.dec_buffer.capacity() < target_size {
            inner
                .dec_buffer
                .reserve(target_size - inner.dec_buffer.len());
        }
        inner.dec_buffer.clear();
        inner.dec_buffer.resize(target_size, 0);
        let mut dec_len = target_size;

        let res = unsafe {
            sys::lsqpack_dec_header_in(
                &raw mut inner.decoder,
                (&raw mut pending_ptr.ctx).cast::<c_void>(),
                stream_id,
                data_len,
                &raw mut data_ptr,
                data_len,
                inner.dec_buffer.as_mut_ptr(),
                &raw mut dec_len,
            )
        };

        if res == sys::lsqpack_read_header_status_LQRHS_DONE {
            if let Some(e) = pending_ptr.ctx.error {
                Err(e)
            } else {
                Ok(Some(std::mem::take(&mut pending_ptr.ctx.headers)))
            }
        } else if res == sys::lsqpack_read_header_status_LQRHS_BLOCKED {
            self.pending_blocks.insert(stream_id, pending);
            Ok(None)
        } else {
            Err(QpackError::DecoderError)
        }
    }
}

// Header block decoding status.
#[derive(Debug)]
pub(super) enum DecodeStatus {
    Blocked,
    Complete(Vec<(Bytes, Bytes)>),
}

// QPACK operation error definitions.
#[derive(Clone, Copy, Debug)]
pub(super) enum QpackError {
    DecoderError,
    EncoderError,
    HeaderTooLong,
}

// HTTP header pair container.
#[derive(Debug)]
struct Header {
    buffer: Vec<u8>,
    name_len: u16,
    value_len: u16,
}

impl Header {
    // Header instance constructor.
    fn new<N, V>(name: N, value: V) -> Result<Self, QpackError>
    where
        N: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let name = name.as_ref();
        let value = value.as_ref();

        let name_len = name
            .len()
            .try_into()
            .map_err(|_e| QpackError::HeaderTooLong)?;
        let value_len = value
            .len()
            .try_into()
            .map_err(|_e| QpackError::HeaderTooLong)?;

        let mut buffer = Vec::with_capacity(name.len() + value.len());
        buffer.extend_from_slice(name);
        buffer.extend_from_slice(value);

        Ok(Self {
            buffer,
            name_len,
            value_len,
        })
    }

    // C-compatible lsxpack_header conversion.
    fn create_lsxpack_header(&mut self) -> sys::lsxpack_header {
        let mut hdr: sys::lsxpack_header = unsafe { MaybeUninit::zeroed().assume_init() };
        hdr.buf = self.buffer.as_mut_ptr().cast::<c_char>();
        hdr.name_len = self.name_len;
        hdr.name_offset = 0;
        hdr.val_len = self.value_len;
        hdr.val_offset = i32::from(self.name_len);
        hdr
    }
}

// Header block decoding context.
struct HeaderBlockCtx {
    unblocked_queue_ptr: *mut Vec<u64>,
    stream_id: u64,
    headers: Vec<(Bytes, Bytes)>,
    header_buf: Vec<u8>,
    header_struct: sys::lsxpack_header,
    error: Option<QpackError>,
}

// Internal decoder implementation details.
struct InnerDecoder {
    decoder: sys::lsqpack_dec,
    cb: sys::lsqpack_dec_hset_if,
    dec_buffer: Vec<u8>,
    _pin: PhantomPinned,
}

impl Drop for InnerDecoder {
    // Resource cleanup.
    fn drop(&mut self) {
        unsafe { sys::lsqpack_dec_cleanup(&raw mut self.decoder) };
    }
}

// Internal encoder implementation details.
struct InnerEncoder {
    encoder: sys::lsqpack_enc,
    enc_buffer: Vec<u8>,
    hdr_buffer: Vec<u8>,
    _pin: PhantomPinned,
}

impl Drop for InnerEncoder {
    // Resource cleanup.
    fn drop(&mut self) {
        unsafe { sys::lsqpack_enc_cleanup(&raw mut self.encoder) };
    }
}

// Pinned decoding block context.
struct PendingBlock {
    _stream_id: u64,
    data: Bytes,
    ctx: HeaderBlockCtx,
    _pin: PhantomPinned,
}

// Stream unblocked callback.
unsafe extern "C" fn cb_unblocked(ctx: *mut c_void) {
    unsafe {
        if ctx.is_null() {
            return;
        }
        let ctx_ref = &mut *ctx.cast::<HeaderBlockCtx>();
        let queue = &mut *ctx_ref.unblocked_queue_ptr;
        queue.push(ctx_ref.stream_id);
    }
}

// Decoding buffer preparation callback.
unsafe extern "C" fn cb_prepare_decode(
    ctx: *mut c_void,
    hdr_block: *mut sys::lsxpack_header,
    space: usize,
) -> *mut sys::lsxpack_header {
    unsafe {
        let ctx_ref = &mut *ctx.cast::<HeaderBlockCtx>();

        if hdr_block.is_null() {
            ctx_ref.header_struct = std::mem::zeroed();
        } else {
            ctx_ref.header_struct = *hdr_block;
        }

        let Ok(val_len) = u16::try_from(space) else {
            ctx_ref.error = Some(QpackError::HeaderTooLong);
            return ptr::null_mut();
        };

        if ctx_ref.header_buf.len() < space {
            ctx_ref.header_buf.resize(space, 0);
        }

        ctx_ref.header_struct.buf = ctx_ref.header_buf.as_mut_ptr().cast::<c_char>();
        ctx_ref.header_struct.val_len = val_len;

        &raw mut ctx_ref.header_struct
    }
}

// Header processing callback.
unsafe extern "C" fn cb_process_header(
    ctx: *mut c_void,
    header: *mut sys::lsxpack_header,
) -> c_int {
    unsafe {
        let ctx_ref = &mut *ctx.cast::<HeaderBlockCtx>();
        if ctx_ref.error.is_some() {
            return 1;
        }

        let h = &*header;

        let Ok(name_offset) = usize::try_from(h.name_offset) else {
            return 1;
        };
        let Ok(val_offset) = usize::try_from(h.val_offset) else {
            return 1;
        };

        let name_slice =
            slice::from_raw_parts(h.buf.add(name_offset).cast::<u8>(), h.name_len as usize);
        let val_slice =
            slice::from_raw_parts(h.buf.add(val_offset).cast::<u8>(), h.val_len as usize);

        ctx_ref.headers.push((
            Bytes::copy_from_slice(name_slice),
            Bytes::copy_from_slice(val_slice),
        ));
        0
    }
}

#[cfg(test)]
mod tests;
