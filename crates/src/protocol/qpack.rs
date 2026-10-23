//! QPACK codec.

use std::cell::UnsafeCell;
use std::cmp;
use std::ffi::{c_char, c_int, c_void};
use std::marker::PhantomPinned;
use std::mem::{self, MaybeUninit};
use std::pin::Pin;
use std::ptr;
use std::slice;

use bytes::Bytes;
use rustc_hash::FxHashMap;
use tracing::debug;

use crate::common::types::Headers;
use crate::protocol::qpack::sys::{
    lsqpack_dec, lsqpack_dec_hset_if, lsqpack_enc, lsqpack_enc_status, lsqpack_read_header_status,
    lsxpack_header,
};

mod sys {
    #![allow(warnings, clippy::all, clippy::pedantic, clippy::restriction)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

const DECODER_INSTRUCTION_BUFFER_SIZE: usize = 1024;
const DECODER_PENDING_BLOCK_CAPACITY: usize = 256;
const ENCODER_FALLBACK_RETRY: usize = 4;

#[derive(Debug)]
pub(super) enum DecodeStatus {
    Blocked,
    Done(Headers),
}

pub(super) struct Decoder {
    inner: Pin<Box<InnerDecoder>>,
    pending_blocks: FxHashMap<u64, Pin<Box<PendingBlock>>>,
}

unsafe impl Send for Decoder {}

impl Decoder {
    pub(super) fn new(max_table_capacity: u32, blocked_streams: u32) -> Self {
        let mut inner = Box::pin(InnerDecoder {
            _pin: PhantomPinned,
            decoder: unsafe { MaybeUninit::zeroed().assume_init() },
            decoder_buffer: Vec::with_capacity(DECODER_INSTRUCTION_BUFFER_SIZE),
            header_set_interface: lsqpack_dec_hset_if {
                dhi_unblocked: Some(dhi_unblocked),
                dhi_prepare_decode: Some(dhi_prepare_decode),
                dhi_process_header: Some(dhi_process_header),
            },
            unblocked_queue: UnsafeCell::new(Vec::new()),
        });

        let inner_ptr = unsafe { inner.as_mut().get_unchecked_mut() };

        unsafe {
            sys::lsqpack_dec_init(
                &raw mut inner_ptr.decoder,
                ptr::null_mut(),
                max_table_capacity,
                blocked_streams,
                &raw const inner_ptr.header_set_interface,
                0,
            );
        }

        Self {
            inner,
            pending_blocks: FxHashMap::default(),
        }
    }

    pub(super) fn abandon_header_block(&mut self, stream_id: u64) -> Vec<u8> {
        let Some(pending) = self.pending_blocks.get_mut(&stream_id) else {
            return Vec::new();
        };

        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };
        let pending_ptr = unsafe { pending.as_mut().get_unchecked_mut() };
        let mut cancel_buffer = [0u8; 16];

        let result = unsafe {
            sys::lsqpack_dec_cancel_stream(
                &raw mut inner.decoder,
                (&raw mut pending_ptr.context).cast::<c_void>(),
                cancel_buffer.as_mut_ptr(),
                cancel_buffer.len(),
            )
        };

        if result < 0 {
            return Vec::new();
        }

        self.pending_blocks.remove(&stream_id);

        let len = usize::try_from(result).unwrap_or(0);
        cancel_buffer.get(..len).unwrap_or_default().to_vec()
    }

    pub(super) fn decode_header(
        &mut self,
        stream_id: u64,
        data: Bytes,
    ) -> Result<(Vec<u8>, DecodeStatus), QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        let context = HeaderBlockContext {
            error: None,
            header_buffer: Vec::with_capacity(4096),
            header_struct: unsafe { MaybeUninit::zeroed().assume_init() },
            headers: Headers::new(),
            stream_id,
            unblocked_queue_ptr: inner.unblocked_queue.get(),
        };
        let pending = Box::pin(PendingBlock {
            _pin: PhantomPinned,
            context,
            data,
        });

        self.execute_decode(stream_id, pending)
    }

    pub(super) fn feed_encoder(&mut self, data: &[u8]) -> Result<Vec<u64>, QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        let result =
            unsafe { sys::lsqpack_dec_enc_in(&raw mut inner.decoder, data.as_ptr(), data.len()) };

        if result == 0 {
            let unblocked = unsafe { mem::take(&mut *inner.unblocked_queue.get()) };
            Ok(unblocked)
        } else {
            Err(QpackError::DecoderError)
        }
    }

    pub(super) fn resume_header(
        &mut self,
        stream_id: u64,
    ) -> Result<(Vec<u8>, Option<Headers>), QpackError> {
        let Some(pending) = self.pending_blocks.remove(&stream_id) else {
            return Ok((Vec::new(), None));
        };

        match self.execute_read(stream_id, pending) {
            Ok((instructions, DecodeStatus::Done(headers))) => Ok((instructions, Some(headers))),
            Ok((instructions, DecodeStatus::Blocked)) => Ok((instructions, None)),
            Err(e) => Err(e),
        }
    }

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

    fn evaluate_decode_status(
        &mut self,
        stream_id: u64,
        mut pending: Pin<Box<PendingBlock>>,
        status: lsqpack_read_header_status,
        decoder_len: usize,
    ) -> Result<(Vec<u8>, DecodeStatus), QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        if status != lsqpack_read_header_status::LQRHS_DONE
            && status != lsqpack_read_header_status::LQRHS_BLOCKED
        {
            return Err(QpackError::DecoderError);
        }

        if decoder_len > DECODER_INSTRUCTION_BUFFER_SIZE {
            debug!(
                "qpack_decoder_instruction validate exceeded actual={decoder_len} expected=decoder_instruction_buffer_size"
            );
            return Err(QpackError::DecoderError);
        }

        // SAFETY: FFI contract guarantees decoder_len bytes are initialized upon LQRHS_DONE or LQRHS_BLOCKED.
        unsafe {
            inner.decoder_buffer.set_len(decoder_len);
        }

        let decoder_instructions = inner.decoder_buffer.clone();
        let pending_ptr = unsafe { pending.as_mut().get_unchecked_mut() };

        if status == lsqpack_read_header_status::LQRHS_DONE {
            if let Some(e) = pending_ptr.context.error {
                Err(e)
            } else {
                Ok((
                    decoder_instructions,
                    DecodeStatus::Done(mem::take(&mut pending_ptr.context.headers)),
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

            Ok((decoder_instructions, DecodeStatus::Blocked))
        }
    }

    fn execute_decode(
        &mut self,
        stream_id: u64,
        mut pending: Pin<Box<PendingBlock>>,
    ) -> Result<(Vec<u8>, DecodeStatus), QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        inner.decoder_buffer.clear();
        inner
            .decoder_buffer
            .reserve(DECODER_INSTRUCTION_BUFFER_SIZE);
        let mut decoder_len = DECODER_INSTRUCTION_BUFFER_SIZE;

        let pending_ptr = unsafe { pending.as_mut().get_unchecked_mut() };
        let mut ffi_ptr = pending_ptr.data.as_ptr();
        let data_len = pending_ptr.data.len();

        let status = unsafe {
            sys::lsqpack_dec_header_in(
                &raw mut inner.decoder,
                (&raw mut pending_ptr.context).cast::<c_void>(),
                stream_id,
                data_len,
                &raw mut ffi_ptr,
                data_len,
                inner.decoder_buffer.as_mut_ptr(),
                &raw mut decoder_len,
            )
        };

        Self::advance_pending_cursor(pending_ptr, ffi_ptr)?;

        self.evaluate_decode_status(stream_id, pending, status, decoder_len)
    }

    fn execute_read(
        &mut self,
        stream_id: u64,
        mut pending: Pin<Box<PendingBlock>>,
    ) -> Result<(Vec<u8>, DecodeStatus), QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        inner.decoder_buffer.clear();
        inner
            .decoder_buffer
            .reserve(DECODER_INSTRUCTION_BUFFER_SIZE);
        let mut decoder_len = DECODER_INSTRUCTION_BUFFER_SIZE;

        let pending_ptr = unsafe { pending.as_mut().get_unchecked_mut() };
        let mut ffi_ptr = pending_ptr.data.as_ptr();
        let data_len = pending_ptr.data.len();

        let status = unsafe {
            sys::lsqpack_dec_header_read(
                &raw mut inner.decoder,
                (&raw mut pending_ptr.context).cast::<c_void>(),
                &raw mut ffi_ptr,
                data_len,
                inner.decoder_buffer.as_mut_ptr(),
                &raw mut decoder_len,
            )
        };

        Self::advance_pending_cursor(pending_ptr, ffi_ptr)?;

        self.evaluate_decode_status(stream_id, pending, status, decoder_len)
    }
}

pub(super) struct Encoder {
    inner: Pin<Box<InnerEncoder>>,
}

unsafe impl Send for Encoder {}

impl Encoder {
    pub(super) fn new(max_table_capacity: u32, blocked_streams: u32) -> Self {
        let mut inner = Box::pin(InnerEncoder {
            _pin: PhantomPinned,
            encoder: unsafe { MaybeUninit::zeroed().assume_init() },
            encoder_buffer: Vec::new(),
            header_buffer: Vec::new(),
            initialized: false,
            max_table_capacity,
        });

        let inner_ptr = unsafe { inner.as_mut().get_unchecked_mut() };

        let result = unsafe {
            sys::lsqpack_enc_init(
                &raw mut inner_ptr.encoder,
                ptr::null_mut(),
                max_table_capacity,
                0,
                blocked_streams,
                0,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };

        if result == 0 {
            inner_ptr.initialized = true;
        } else {
            debug!("qpack_encoder create failed actual={result}");
        }

        Self { inner }
    }

    pub(super) fn apply_settings(
        &mut self,
        max_table_capacity: u64,
        _blocked_streams: u64,
    ) -> Result<Vec<u8>, QpackError> {
        let mut buffer = [0u8; 64];
        let mut written: usize = buffer.len();
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };
        let requested_capacity = u32::try_from(max_table_capacity).unwrap_or(u32::MAX);
        let capacity = cmp::min(requested_capacity, inner.max_table_capacity);

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

    pub(super) fn encode(
        &mut self,
        stream_id: u64,
        headers: &Headers,
    ) -> Result<(Vec<u8>, Vec<u8>), QpackError> {
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

        inner.encoder_buffer.clear();
        inner.header_buffer.clear();
        inner.encoder_buffer.reserve(1024);
        inner.header_buffer.reserve(4096);

        for header in &mut lsxpack_headers {
            inner.encode_single_header(header)?;
        }

        let max_prefix =
            unsafe { sys::lsqpack_enc_header_block_prefix_size(&raw const inner.encoder) };
        let total_len = max_prefix + inner.header_buffer.len();
        let mut final_header_block = Vec::with_capacity(total_len);

        let result = unsafe {
            sys::lsqpack_enc_end_header(
                &raw mut inner.encoder,
                final_header_block.as_mut_ptr(),
                max_prefix,
                ptr::null_mut(),
            )
        };

        if result < 0 {
            return Err(QpackError::EncoderError);
        }

        let prefix_len = usize::try_from(result).map_err(|e| {
            debug!("qpack_prefix convert failed actual={result} expected=usize err={e:?}");
            QpackError::EncoderError
        })?;

        if prefix_len == 0 && max_prefix > 0 {
            return Err(QpackError::EncoderError);
        }

        // SAFETY: FFI execution and contiguous non-overlapping copy strictly initialize the extended length.
        unsafe {
            ptr::copy_nonoverlapping(
                inner.header_buffer.as_ptr(),
                final_header_block.as_mut_ptr().add(prefix_len),
                inner.header_buffer.len(),
            );
            final_header_block.set_len(prefix_len + inner.header_buffer.len());
        }

        Ok((final_header_block, mem::take(&mut inner.encoder_buffer)))
    }

    pub(super) fn feed_decoder(&mut self, data: &[u8]) -> Result<(), QpackError> {
        let inner = unsafe { self.inner.as_mut().get_unchecked_mut() };

        let result = unsafe {
            sys::lsqpack_enc_decoder_in(
                &raw mut inner.encoder,
                data.as_ptr().cast::<u8>(),
                data.len(),
            )
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
    name_len: u16,
    value_len: u16,
}

impl Header {
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

    fn create_lsxpack_header(&mut self) -> lsxpack_header {
        let mut header: lsxpack_header = unsafe { MaybeUninit::zeroed().assume_init() };

        header.buf = self.buffer.as_mut_ptr().cast::<c_char>();
        header.name_len = self.name_len;
        header.name_offset = 0;
        header.val_len = self.value_len;
        header.val_offset = i32::from(self.name_len);

        header
    }
}

struct HeaderBlockContext {
    error: Option<QpackError>,
    header_buffer: Vec<u8>,
    header_struct: lsxpack_header,
    headers: Headers,
    stream_id: u64,
    unblocked_queue_ptr: *mut Vec<u64>,
}

struct InnerDecoder {
    _pin: PhantomPinned,
    decoder: lsqpack_dec,
    decoder_buffer: Vec<u8>,
    header_set_interface: lsqpack_dec_hset_if,
    unblocked_queue: UnsafeCell<Vec<u64>>,
}

impl Drop for InnerDecoder {
    fn drop(&mut self) {
        // SAFETY: lsqpack_dec_cleanup strictly avoids callback invocations, precluding UAF of pending blocks.
        unsafe { sys::lsqpack_dec_cleanup(&raw mut self.decoder) };
    }
}

struct InnerEncoder {
    _pin: PhantomPinned,
    encoder: lsqpack_enc,
    encoder_buffer: Vec<u8>,
    header_buffer: Vec<u8>,
    initialized: bool,
    max_table_capacity: u32,
}

impl Drop for InnerEncoder {
    fn drop(&mut self) {
        if self.initialized {
            // SAFETY: lsqpack_enc_cleanup exclusively frees encoder-internal memory and invokes no callbacks.
            unsafe { sys::lsqpack_enc_cleanup(&raw mut self.encoder) };
        }
    }
}

impl InnerEncoder {
    fn encode_single_header(&mut self, header: &mut lsxpack_header) -> Result<(), QpackError> {
        let encoder_offset = self.encoder_buffer.len();
        let header_offset = self.header_buffer.len();
        let required_space = usize::from(header.name_len) + usize::from(header.val_len) + 256;

        if self.encoder_buffer.capacity() - encoder_offset < required_space {
            self.encoder_buffer.reserve(required_space);
        }
        if self.header_buffer.capacity() - header_offset < required_space {
            self.header_buffer.reserve(required_space);
        }

        let mut retries = 0;

        loop {
            let mut encoder_written = self.encoder_buffer.capacity() - encoder_offset;
            let mut header_written = self.header_buffer.capacity() - header_offset;

            let status = unsafe {
                sys::lsqpack_enc_encode(
                    &raw mut self.encoder,
                    self.encoder_buffer
                        .as_mut_ptr()
                        .add(encoder_offset)
                        .cast::<u8>(),
                    &raw mut encoder_written,
                    self.header_buffer
                        .as_mut_ptr()
                        .add(header_offset)
                        .cast::<u8>(),
                    &raw mut header_written,
                    header,
                    0,
                )
            };

            if status == lsqpack_enc_status::LQES_OK {
                // SAFETY: FFI contract guarantees encoder_written and header_written bytes are initialized upon LQES_OK.
                unsafe {
                    self.encoder_buffer
                        .set_len(encoder_offset + encoder_written);
                    self.header_buffer.set_len(header_offset + header_written);
                }
                break;
            }

            retries += 1;

            if retries > ENCODER_FALLBACK_RETRY {
                return Err(QpackError::EncoderError);
            }

            if status == lsqpack_enc_status::LQES_NOBUF_ENC {
                self.encoder_buffer.reserve(1024);
            } else if status == lsqpack_enc_status::LQES_NOBUF_HEAD {
                self.header_buffer.reserve(1024);
            } else {
                return Err(QpackError::EncoderError);
            }
        }

        Ok(())
    }
}

struct PendingBlock {
    _pin: PhantomPinned,
    context: HeaderBlockContext,
    data: Bytes,
}

unsafe extern "C" fn dhi_prepare_decode(
    hblock_ctx: *mut c_void,
    header: *mut lsxpack_header,
    space: usize,
) -> *mut lsxpack_header {
    let hblock_ctx = unsafe { &mut *hblock_ctx.cast::<HeaderBlockContext>() };

    let used_bytes = if header.is_null() {
        hblock_ctx.header_struct = unsafe { mem::zeroed() };
        0
    } else {
        hblock_ctx.header_struct = unsafe { *header };
        let header = &hblock_ctx.header_struct;

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

    hblock_ctx.header_struct.buf = hblock_ctx.header_buffer.as_mut_ptr().cast::<c_char>();
    hblock_ctx.header_struct.val_len = val_len;

    &raw mut hblock_ctx.header_struct
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

    let name_slice = unsafe {
        slice::from_raw_parts(
            header.buf.add(name_offset).cast::<u8>(),
            usize::from(header.name_len),
        )
    };
    let value_slice = unsafe {
        slice::from_raw_parts(
            header.buf.add(val_offset).cast::<u8>(),
            usize::from(header.val_len),
        )
    };

    hblock_ctx.headers.push((
        Bytes::copy_from_slice(name_slice),
        Bytes::copy_from_slice(value_slice),
    ));

    0
}

unsafe extern "C" fn dhi_unblocked(hblock_ctx: *mut c_void) {
    if hblock_ctx.is_null() {
        return;
    }

    let hblock_ctx = unsafe { &*hblock_ctx.cast::<HeaderBlockContext>() };
    let queue = unsafe { &mut *hblock_ctx.unblocked_queue_ptr };

    queue.push(hblock_ctx.stream_id);
}

#[cfg(test)]
mod tests;
