//! Internal utility functions for flow control, stream ID logic, error mapping, and encoding.

use std::io::Cursor;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::Serializer;
use serde::ser::SerializeSeq;

use crate::common::constants::{
    ERR_H3_FRAME_ERROR, ERR_LIB_INTERNAL_ERROR, ERR_WT_APPLICATION_ERROR_FIRST,
    ERR_WT_APPLICATION_ERROR_LAST, MAX_STREAM_ID,
};
use crate::common::types::{ErrorCode, Headers, StreamDirection, StreamId};

// H3 error mapping constant reserved offset.
const H3_ERROR_RESERVED_OFFSET: u64 = 0x21;
// H3 error mapping constant reserved modulo.
const H3_ERROR_RESERVED_MODULO: u64 = 0x1F;
// WebTransport error mapping constant divisor.
const WT_ERROR_MAP_DIVISOR: u64 = 0x1E;

// Stream reception capability check.
pub(super) fn can_receive_on_stream(stream_id: StreamId, is_client: bool) -> bool {
    if is_bidirectional_stream(stream_id) {
        return true;
    }

    (is_client && is_server_initiated_stream(stream_id))
        || (!is_client && is_client_initiated_stream(stream_id))
}

// Stream transmission capability check.
pub(super) fn can_send_on_stream(stream_id: StreamId, is_client: bool) -> bool {
    if is_bidirectional_stream(stream_id) {
        return true;
    }

    (is_client && is_client_initiated_stream(stream_id))
        || (!is_client && is_server_initiated_stream(stream_id))
}

// Case-insensitive header search.
pub(super) fn find_header(headers: &Headers, key: &str) -> Option<Bytes> {
    let key_bytes = key.as_bytes();

    for (k, v) in headers {
        if k.len() == key_bytes.len() && k.eq_ignore_ascii_case(key_bytes) {
            return Some(v.clone());
        }
    }

    None
}

// Header value search and UTF-8 decoding.
pub(super) fn find_header_str(headers: &Headers, key: &str) -> Option<String> {
    let val = find_header(headers, key)?;
    String::from_utf8(val.to_vec()).ok()
}

// HTTP/3 to WebTransport error code mapping.
pub(super) fn http_to_wt_error(http_error_code: u64) -> Option<ErrorCode> {
    if !(ERR_WT_APPLICATION_ERROR_FIRST..=ERR_WT_APPLICATION_ERROR_LAST).contains(&http_error_code)
    {
        return None;
    }

    if (http_error_code - H3_ERROR_RESERVED_OFFSET).is_multiple_of(H3_ERROR_RESERVED_MODULO) {
        return None;
    }

    let shifted = http_error_code - ERR_WT_APPLICATION_ERROR_FIRST;
    let result = shifted - (shifted / H3_ERROR_RESERVED_MODULO);

    Some(result)
}

// Bidirectional stream check.
pub(super) fn is_bidirectional_stream(stream_id: StreamId) -> bool {
    (stream_id & 0x2) == 0
}

// Peer-initiated stream check.
pub(super) fn is_peer_initiated_stream(stream_id: StreamId, is_client: bool) -> bool {
    if is_client {
        is_server_initiated_stream(stream_id)
    } else {
        is_client_initiated_stream(stream_id)
    }
}

// Request-response stream identification.
pub(super) fn is_request_response_stream(stream_id: StreamId) -> bool {
    is_bidirectional_stream(stream_id) && is_client_initiated_stream(stream_id)
}

// Unidirectional stream check.
pub(super) fn is_unidirectional_stream(stream_id: StreamId) -> bool {
    (stream_id & 0x2) != 0
}

// Header set merging operation.
pub(super) fn merge_headers(base: &Headers, update: &Headers) -> Headers {
    let mut out = base.clone();
    out.extend_from_slice(update);
    out
}

// Data limit auto-scaling calculation.
pub(super) fn next_data_limit(
    current_limit: u64,
    consumed: u64,
    window_size: u64,
    auto_scale: bool,
    force_update: bool,
) -> Option<u64> {
    if !auto_scale {
        return None;
    }

    let new_limit = consumed.saturating_add(window_size);
    let threshold = if force_update { 0 } else { window_size / 2 };

    if new_limit > current_limit.saturating_add(threshold) {
        Some(new_limit)
    } else {
        None
    }
}

// Stream concurrency limit auto-scaling calculation.
pub(super) fn next_stream_limit(
    current_limit: u64,
    closed_count: u64,
    initial_window: u64,
    auto_scale: bool,
    force_update: bool,
) -> Option<u64> {
    if !auto_scale {
        return None;
    }

    let new_limit = closed_count.saturating_add(initial_window);
    let threshold = if force_update { 0 } else { initial_window / 2 };

    if new_limit > current_limit && (new_limit >= current_limit.saturating_add(threshold)) {
        Some(new_limit)
    } else {
        None
    }
}

// Variable-length integer decoding.
pub(super) fn read_varint(buf: &mut Cursor<&[u8]>) -> Result<u64, ErrorCode> {
    if !buf.has_remaining() {
        return Err(ERR_H3_FRAME_ERROR);
    }

    let chunk = buf.chunk();
    let first = *chunk.first().ok_or(ERR_H3_FRAME_ERROR)?;
    let prefix = first >> 6;
    let length = 1 << prefix;

    if buf.remaining() < length {
        return Err(ERR_H3_FRAME_ERROR);
    }

    let val = match length {
        1 => u64::from(buf.get_u8() & 0x3f),
        2 => u64::from(buf.get_u16() & 0x3fff),
        4 => u64::from(buf.get_u32() & 0x3fff_ffff),
        8 => buf.get_u64() & 0x3fff_ffff_ffff_ffff,
        _ => return Err(ERR_H3_FRAME_ERROR),
    };

    Ok(val)
}

// Header serialization for diagnostics.
pub(super) fn serialize_headers<S>(headers: &Headers, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = serializer.serialize_seq(Some(headers.len()))?;

    for (k, v) in headers {
        let k_str = String::from_utf8_lossy(k);
        let v_str = String::from_utf8_lossy(v);
        seq.serialize_element(&(k_str, v_str))?;
    }

    seq.end()
}

// Stream direction resolution from ID.
pub(super) fn stream_dir_from_id(stream_id: StreamId, is_client: bool) -> StreamDirection {
    if cfg!(debug_assertions) {
        debug_assert!(
            validate_stream_id(stream_id).is_ok(),
            "Invalid stream ID encountered in debug path"
        );
    }

    match (
        is_bidirectional_stream(stream_id),
        can_send_on_stream(stream_id, is_client),
    ) {
        (true, _) => StreamDirection::Bidirectional,
        (false, true) => StreamDirection::SendOnly,
        (false, false) => StreamDirection::ReceiveOnly,
    }
}

// Control stream ID validation.
pub(super) fn validate_control_stream_id(stream_id: StreamId) -> Result<(), String> {
    if !is_request_response_stream(stream_id) {
        return Err(format!(
            "Invalid Session ID format: {stream_id} (must be client-initiated bidirectional)"
        ));
    }

    Ok(())
}

// WebTransport stream ID validation.
pub(super) fn validate_stream_id(stream_id: StreamId) -> Result<(), String> {
    if stream_id > MAX_STREAM_ID {
        return Err(format!("Stream ID {stream_id} out of valid range"));
    }

    Ok(())
}

// Unidirectional stream ID validation.
pub(super) fn validate_unidirectional_stream_id(
    stream_id: StreamId,
    context: &str,
) -> Result<(), String> {
    if !is_unidirectional_stream(stream_id) {
        return Err(format!(
            "{context} stream ID {stream_id} must be unidirectional."
        ));
    }

    Ok(())
}

// WebTransport to HTTP/3 error code mapping.
pub(super) fn wt_to_http_error(app_error_code: ErrorCode) -> Option<u64> {
    let base = ERR_WT_APPLICATION_ERROR_FIRST;
    let divisor = WT_ERROR_MAP_DIVISOR;

    let shifted = base.checked_add(app_error_code)?;
    let offset = app_error_code / divisor;

    Some(shifted + offset)
}

// Variable-length integer encoding.
pub(super) fn write_varint(buf: &mut BytesMut, value: u64) -> Result<(), ErrorCode> {
    if value <= 63 {
        buf.put_u8(u8::try_from(value).map_err(|_e| ERR_LIB_INTERNAL_ERROR)?);
    } else if value <= 16383 {
        buf.put_u16(u16::try_from(value).map_err(|_e| ERR_LIB_INTERNAL_ERROR)? | 0x4000);
    } else if value <= 1_073_741_823 {
        buf.put_u32(u32::try_from(value).map_err(|_e| ERR_LIB_INTERNAL_ERROR)? | 0x8000_0000);
    } else if value <= 4_611_686_018_427_387_903 {
        buf.put_u64(value | 0xC000_0000_0000_0000);
    } else {
        return Err(ERR_LIB_INTERNAL_ERROR);
    }

    Ok(())
}

// Client initiated stream ID check.
fn is_client_initiated_stream(stream_id: StreamId) -> bool {
    (stream_id & 0x1) == 0
}

// Server initiated stream ID check.
fn is_server_initiated_stream(stream_id: StreamId) -> bool {
    (stream_id & 0x1) == 1
}

#[cfg(test)]
mod tests;
