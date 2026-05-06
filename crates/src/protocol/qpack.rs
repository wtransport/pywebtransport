//! QPACK encoder and decoder bindings with safe Rust abstractions.

use std::cell::UnsafeCell;
use std::cmp;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void};
use std::marker::PhantomPinned;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::ptr;
use std::slice;

use bytes::Bytes;
use tracing::debug;

use crate::common::types::Headers;

// Internal FFI bindings module.
mod sys {
    #![allow(warnings, clippy::all, clippy::pedantic, clippy::restriction)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

// Fixed byte size for the decoder instruction output buffer.
const DECODER_INSTRUCTION_BUFFER_SIZE: usize = 1024;
// Decoder pending block capacity for DoS protection.
const DECODER_PENDING_BLOCK_CAPACITY: usize = 256;
// Encoder blocked stream capacity.
const ENCODER_BLOCKED_STREAM_CAPACITY: u32 = 16;
// Maximum retry iterations for encoder memory fallback.
const ENCODER_FALLBACK_RETRY: usize = 4;
// Encoder table size.
const ENCODER_TABLE_SIZE: u32 = 65536;

// Header block decoding status.
#[derive(Debug)]
pub(super) enum DecodeStatus {
    Blocked,
    Complete(Headers),
}

// High-level wrapper for the QPACK Decoder.
pub(super) struct Decoder {
    inner: Pin<Box<InnerDecoder>>,
    pending_blocks: HashMap<u64, Pin<Box<PendingBlock>>>,
}

unsafe impl Send for Decoder {}

impl Decoder {
    // Decoder instance initialization.
    pub(super) fn new(max_table_size: u32, dyn_table_size: u32) -> Self {
        let mut inner = Box::pin(InnerDecoder {
            _pin: PhantomPinned,
            cb: sys::lsqpack_dec_hset_if {
                dhi_unblocked: Some(cb_unblocked),
                dhi_prepare_decode: Some(cb_prepare_decode),
                dhi_process_header: Some(cb_process_header),
            },
            dec_buffer: Vec::with_capacity(DECODER_INSTRUCTION_BUFFER_SIZE),
            decoder: unsafe { MaybeUninit::zeroed().assume_init() },
            unblocked_queue: UnsafeCell::new(Vec::new()),
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
        }
    }

    // Pending header block abandonment.
    pub(super) fn abandon_header_block(&mut self, stream_id: u64) -> Vec<u8> {
        let Some(pending) = self.pending_blocks.get_mut(&stream_id) else {
            return Vec::new();
        };

        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };
        let pending_ptr = unsafe { pending.as_mut().get_unchecked_mut() };
        let mut cancel_buf = [0u8; 16];

        let res = unsafe {
            sys::lsqpack_dec_cancel_stream(
                &raw mut inner.decoder,
                (&raw mut pending_ptr.ctx).cast::<c_void>(),
                cancel_buf.as_mut_ptr(),
                cancel_buf.len(),
            )
        };

        if res < 0 {
            return Vec::new();
        }

        self.pending_blocks.remove(&stream_id);

        let len = usize::try_from(res).unwrap_or(0);
        cancel_buf.get(..len).unwrap_or_default().to_vec()
    }

    // Header block decoding.
    pub(super) fn decode_header(
        &mut self,
        stream_id: u64,
        data: Bytes,
    ) -> Result<(Vec<u8>, DecodeStatus), QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        let ctx = HeaderBlockCtx {
            error: None,
            header_buf: Vec::with_capacity(4096),
            header_struct: unsafe { MaybeUninit::zeroed().assume_init() },
            headers: Headers::new(),
            stream_id,
            unblocked_queue_ptr: inner.unblocked_queue.get(),
        };
        let pending = Box::pin(PendingBlock {
            _pin: PhantomPinned,
            _stream_id: stream_id,
            ctx,
            data,
        });

        self.execute_decode(stream_id, pending)
    }

    // Encoder stream instruction ingestion.
    pub(super) fn feed_encoder(&mut self, data: &[u8]) -> Result<Vec<u64>, QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        let res =
            unsafe { sys::lsqpack_dec_enc_in(&raw mut inner.decoder, data.as_ptr(), data.len()) };

        if res == 0 {
            let unblocked = unsafe { std::mem::take(&mut *inner.unblocked_queue.get()) };
            Ok(unblocked)
        } else {
            Err(QpackError::DecoderError)
        }
    }

    // Blocked header processing resumption.
    pub(super) fn resume_header(
        &mut self,
        stream_id: u64,
    ) -> Result<(Vec<u8>, Option<Headers>), QpackError> {
        let Some(pending) = self.pending_blocks.remove(&stream_id) else {
            return Ok((Vec::new(), None));
        };

        match self.execute_read(stream_id, pending) {
            Ok((instr, DecodeStatus::Complete(h))) => Ok((instr, Some(h))),
            Ok((instr, DecodeStatus::Blocked)) => Ok((instr, None)),
            Err(e) => Err(e),
        }
    }

    // FFI cursor advancement and boundary validation.
    fn advance_pending_cursor(
        pending_ptr: &mut PendingBlock,
        new_ptr: *const u8,
    ) -> Result<(), QpackError> {
        let base = pending_ptr.data.as_ptr() as usize;
        let end = base + pending_ptr.data.len();
        let new = new_ptr as usize;

        if new < base || new > end {
            debug!(
                "qpack_field_section validate invalid actual={new} ptr={base} size={}",
                pending_ptr.data.len()
            );
            return Err(QpackError::DecoderError);
        }

        let consumed = new - base;

        if consumed > 0 {
            pending_ptr.data = pending_ptr.data.slice(consumed..);
        }

        Ok(())
    }

    // C library status evaluation and lifecycle management.
    fn evaluate_dec_status(
        &mut self,
        stream_id: u64,
        mut pending: Pin<Box<PendingBlock>>,
        res: sys::lsqpack_read_header_status,
        dec_len: usize,
    ) -> Result<(Vec<u8>, DecodeStatus), QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        if res != sys::lsqpack_read_header_status::LQRHS_DONE
            && res != sys::lsqpack_read_header_status::LQRHS_BLOCKED
        {
            return Err(QpackError::DecoderError);
        }

        if dec_len > DECODER_INSTRUCTION_BUFFER_SIZE {
            debug!(
                "qpack_decoder_instruction validate exceeded actual={dec_len} expected=decoder_instruction_buffer_size"
            );
            return Err(QpackError::DecoderError);
        }

        // SAFETY: FFI contract guarantees dec_len bytes are initialized upon LQRHS_DONE or LQRHS_BLOCKED.
        unsafe {
            inner.dec_buffer.set_len(dec_len);
        }

        let dec_instructions = inner.dec_buffer.clone();
        let pending_ptr = unsafe { pending.as_mut().get_unchecked_mut() };

        if res == sys::lsqpack_read_header_status::LQRHS_DONE {
            if let Some(e) = pending_ptr.ctx.error {
                Err(e)
            } else {
                Ok((
                    dec_instructions,
                    DecodeStatus::Complete(std::mem::take(&mut pending_ptr.ctx.headers)),
                ))
            }
        } else {
            if self.pending_blocks.len() >= DECODER_PENDING_BLOCK_CAPACITY {
                debug!(
                    "qpack_pending_block validate exceeded actual={} expected=decoder_pending_block_capacity",
                    self.pending_blocks.len()
                );
                return Err(QpackError::DecoderError);
            }

            self.pending_blocks.insert(stream_id, pending);

            Ok((dec_instructions, DecodeStatus::Blocked))
        }
    }

    // FFI decoding execution.
    fn execute_decode(
        &mut self,
        stream_id: u64,
        mut pending: Pin<Box<PendingBlock>>,
    ) -> Result<(Vec<u8>, DecodeStatus), QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        inner.dec_buffer.clear();
        inner.dec_buffer.reserve(DECODER_INSTRUCTION_BUFFER_SIZE);
        let mut dec_len = DECODER_INSTRUCTION_BUFFER_SIZE;

        let pending_ptr = unsafe { pending.as_mut().get_unchecked_mut() };
        let mut ffi_ptr = pending_ptr.data.as_ptr();
        let data_len = pending_ptr.data.len();

        let res = unsafe {
            sys::lsqpack_dec_header_in(
                &raw mut inner.decoder,
                (&raw mut pending_ptr.ctx).cast::<c_void>(),
                stream_id,
                data_len,
                &raw mut ffi_ptr,
                data_len,
                inner.dec_buffer.as_mut_ptr(),
                &raw mut dec_len,
            )
        };

        Self::advance_pending_cursor(pending_ptr, ffi_ptr)?;

        self.evaluate_dec_status(stream_id, pending, res, dec_len)
    }

    // FFI header read execution.
    fn execute_read(
        &mut self,
        stream_id: u64,
        mut pending: Pin<Box<PendingBlock>>,
    ) -> Result<(Vec<u8>, DecodeStatus), QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        inner.dec_buffer.clear();
        inner.dec_buffer.reserve(DECODER_INSTRUCTION_BUFFER_SIZE);
        let mut dec_len = DECODER_INSTRUCTION_BUFFER_SIZE;

        let pending_ptr = unsafe { pending.as_mut().get_unchecked_mut() };
        let mut ffi_ptr = pending_ptr.data.as_ptr();
        let data_len = pending_ptr.data.len();

        let res = unsafe {
            sys::lsqpack_dec_header_read(
                &raw mut inner.decoder,
                (&raw mut pending_ptr.ctx).cast::<c_void>(),
                &raw mut ffi_ptr,
                data_len,
                inner.dec_buffer.as_mut_ptr(),
                &raw mut dec_len,
            )
        };

        Self::advance_pending_cursor(pending_ptr, ffi_ptr)?;

        self.evaluate_dec_status(stream_id, pending, res, dec_len)
    }
}

// High-level wrapper for the QPACK Encoder.
pub(super) struct Encoder {
    inner: Pin<Box<InnerEncoder>>,
}

unsafe impl Send for Encoder {}

impl Encoder {
    // Encoder instance initialization.
    pub(super) fn new() -> Self {
        let mut inner = Box::pin(InnerEncoder {
            _pin: PhantomPinned,
            enc_buffer: Vec::new(),
            encoder: unsafe { MaybeUninit::zeroed().assume_init() },
            hdr_buffer: Vec::new(),
        });

        let inner_ptr = unsafe { inner.as_mut().get_unchecked_mut() };

        unsafe {
            sys::lsqpack_enc_init(
                &raw mut inner_ptr.encoder,
                ptr::null_mut(),
                ENCODER_TABLE_SIZE,
                0,
                ENCODER_BLOCKED_STREAM_CAPACITY,
                0,
                ptr::null_mut(),
                ptr::null_mut(),
            );
        }

        Self { inner }
    }

    // Dynamic table capacity configuration.
    pub(super) fn apply_settings(
        &mut self,
        max_table_capacity: u64,
        _blocked_streams: u64,
    ) -> Result<Vec<u8>, QpackError> {
        let mut buffer = [0u8; 64];
        let mut written: usize = buffer.len();
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };
        let requested_capacity = u32::try_from(max_table_capacity).unwrap_or(u32::MAX);
        let capacity = cmp::min(requested_capacity, ENCODER_TABLE_SIZE);

        let result = unsafe {
            sys::lsqpack_enc_set_max_capacity(
                &raw mut inner.encoder,
                capacity,
                buffer.as_mut_ptr(),
                &raw mut written,
            )
        };

        if result == 0 {
            Ok(buffer.get(..written).unwrap_or_default().to_vec())
        } else {
            Err(QpackError::EncoderError)
        }
    }

    // Stream-specific header block encoding.
    pub(super) fn encode(
        &mut self,
        stream_id: u64,
        headers: &Headers,
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
            inner.encode_single_header(hdr)?;
        }

        let max_prefix =
            unsafe { sys::lsqpack_enc_header_block_prefix_size(&raw const inner.encoder) };
        let total_len = max_prefix + inner.hdr_buffer.len();
        let mut final_hdr_block = Vec::with_capacity(total_len);

        let res = unsafe {
            sys::lsqpack_enc_end_header(
                &raw mut inner.encoder,
                final_hdr_block.as_mut_ptr(),
                max_prefix,
                ptr::null_mut(),
            )
        };

        if res < 0 {
            return Err(QpackError::EncoderError);
        }

        let prefix_len = usize::try_from(res).map_err(|e| {
            debug!("qpack_prefix convert failed actual={res} expected=usize err={e:?}");
            QpackError::EncoderError
        })?;

        if prefix_len == 0 && max_prefix > 0 {
            return Err(QpackError::EncoderError);
        }

        // SAFETY: FFI execution and contiguous non-overlapping copy strictly initialize the extended length.
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

        let name_len = name.len().try_into().map_err(|e| {
            debug!(
                "qpack_field_name convert failed actual={} expected=u16 err={e:?}",
                name.len()
            );
            QpackError::HeaderTooLong
        })?;
        let value_len = value.len().try_into().map_err(|e| {
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
    error: Option<QpackError>,
    header_buf: Vec<u8>,
    header_struct: sys::lsxpack_header,
    headers: Headers,
    stream_id: u64,
    unblocked_queue_ptr: *mut Vec<u64>,
}

// Internal decoder implementation details.
struct InnerDecoder {
    _pin: PhantomPinned,
    cb: sys::lsqpack_dec_hset_if,
    dec_buffer: Vec<u8>,
    decoder: sys::lsqpack_dec,
    unblocked_queue: UnsafeCell<Vec<u64>>,
}

impl Drop for InnerDecoder {
    // Resource cleanup.
    fn drop(&mut self) {
        // SAFETY: lsqpack_dec_cleanup strictly avoids callback invocations, precluding UAF of pending blocks.
        unsafe { sys::lsqpack_dec_cleanup(&raw mut self.decoder) };
    }
}

// Internal encoder implementation details.
struct InnerEncoder {
    _pin: PhantomPinned,
    enc_buffer: Vec<u8>,
    encoder: sys::lsqpack_enc,
    hdr_buffer: Vec<u8>,
}

impl Drop for InnerEncoder {
    // Resource cleanup.
    fn drop(&mut self) {
        unsafe { sys::lsqpack_enc_cleanup(&raw mut self.encoder) };
    }
}

impl InnerEncoder {
    // Single header FFI encoding with heuristic allocation and retry fallback.
    fn encode_single_header(&mut self, hdr: &mut sys::lsxpack_header) -> Result<(), QpackError> {
        let enc_off = self.enc_buffer.len();
        let hdr_off = self.hdr_buffer.len();
        let required_space = usize::from(hdr.name_len) + usize::from(hdr.val_len) + 256;

        if self.enc_buffer.capacity() - enc_off < required_space {
            self.enc_buffer.reserve(required_space);
        }
        if self.hdr_buffer.capacity() - hdr_off < required_space {
            self.hdr_buffer.reserve(required_space);
        }

        let mut retries = 0;

        loop {
            let mut enc_written = self.enc_buffer.capacity() - enc_off;
            let mut hdr_written = self.hdr_buffer.capacity() - hdr_off;

            let res = unsafe {
                sys::lsqpack_enc_encode(
                    &raw mut self.encoder,
                    self.enc_buffer.as_mut_ptr().add(enc_off).cast::<u8>(),
                    &raw mut enc_written,
                    self.hdr_buffer.as_mut_ptr().add(hdr_off).cast::<u8>(),
                    &raw mut hdr_written,
                    hdr,
                    0,
                )
            };

            if res == sys::lsqpack_enc_status::LQES_OK {
                // SAFETY: FFI contract guarantees enc_written and hdr_written bytes are initialized upon LQES_OK.
                unsafe {
                    self.enc_buffer.set_len(enc_off + enc_written);
                    self.hdr_buffer.set_len(hdr_off + hdr_written);
                }
                break;
            }

            retries += 1;

            if retries > ENCODER_FALLBACK_RETRY {
                return Err(QpackError::EncoderError);
            }

            if res == sys::lsqpack_enc_status::LQES_NOBUF_ENC {
                self.enc_buffer.reserve(1024);
            } else if res == sys::lsqpack_enc_status::LQES_NOBUF_HEAD {
                self.hdr_buffer.reserve(1024);
            } else {
                return Err(QpackError::EncoderError);
            }
        }

        Ok(())
    }
}

// Pinned decoding block context.
struct PendingBlock {
    _pin: PhantomPinned,
    _stream_id: u64,
    ctx: HeaderBlockCtx,
    data: Bytes,
}

// Decoding buffer preparation callback.
unsafe extern "C" fn cb_prepare_decode(
    ctx: *mut c_void,
    hdr_block: *mut sys::lsxpack_header,
    space: usize,
) -> *mut sys::lsxpack_header {
    let ctx_ref = unsafe { &mut *ctx.cast::<HeaderBlockCtx>() };

    let used_bytes = if hdr_block.is_null() {
        ctx_ref.header_struct = unsafe { std::mem::zeroed() };
        0
    } else {
        ctx_ref.header_struct = unsafe { *hdr_block };
        let h = &ctx_ref.header_struct;

        let name_offset = usize::try_from(h.name_offset).unwrap_or(0);
        let val_offset = usize::try_from(h.val_offset).unwrap_or(0);

        let name_end = name_offset + usize::from(h.name_len);
        let val_end = val_offset + usize::from(h.val_len);
        cmp::max(name_end, val_end)
    };

    let Ok(val_len) = u16::try_from(space) else {
        ctx_ref.error = Some(QpackError::HeaderTooLong);
        return ptr::null_mut();
    };

    // SAFETY: Synchronization of the Rust Vec's view of physical bytes written by the C library.
    unsafe {
        ctx_ref.header_buf.set_len(used_bytes);
    }
    ctx_ref.header_buf.reserve(space.saturating_sub(used_bytes));

    ctx_ref.header_struct.buf = ctx_ref.header_buf.as_mut_ptr().cast::<c_char>();
    ctx_ref.header_struct.val_len = val_len;

    &raw mut ctx_ref.header_struct
}

// Header processing callback.
unsafe extern "C" fn cb_process_header(
    ctx: *mut c_void,
    header: *mut sys::lsxpack_header,
) -> c_int {
    let ctx_ref = unsafe { &mut *ctx.cast::<HeaderBlockCtx>() };

    if ctx_ref.error.is_some() {
        return 1;
    }

    let h = unsafe { &*header };

    let Ok(name_offset) = usize::try_from(h.name_offset) else {
        return 1;
    };
    let Ok(val_offset) = usize::try_from(h.val_offset) else {
        return 1;
    };

    let name_slice = unsafe {
        slice::from_raw_parts(h.buf.add(name_offset).cast::<u8>(), usize::from(h.name_len))
    };
    let val_slice = unsafe {
        slice::from_raw_parts(h.buf.add(val_offset).cast::<u8>(), usize::from(h.val_len))
    };

    ctx_ref.headers.push((
        Bytes::copy_from_slice(name_slice),
        Bytes::copy_from_slice(val_slice),
    ));

    0
}

// Stream unblocked callback.
unsafe extern "C" fn cb_unblocked(ctx: *mut c_void) {
    if ctx.is_null() {
        return;
    }

    let ctx_ref = unsafe { &*ctx.cast::<HeaderBlockCtx>() };
    let queue = unsafe { &mut *ctx_ref.unblocked_queue_ptr };

    queue.push(ctx_ref.stream_id);
}

#[cfg(test)]
mod tests;
