//! Internal utility functions for flow control, stream ID logic, error mapping, and encoding.

use std::borrow::Cow;
use std::io::Cursor;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tracing::debug;

use crate::common::constants::{
    ERR_H3_FRAME_ERROR, ERR_LIB_INTERNAL_ERROR, ERR_WT_APPLICATION_ERROR_FIRST,
    ERR_WT_APPLICATION_ERROR_LAST, QUIC_MAX_STREAM_ID, QUIC_STREAM_DIRECTION_MASK,
    QUIC_STREAM_INITIATOR_MASK,
};
use crate::common::types::{ErrorCode, Headers, StreamDirection, StreamId};

// H3 error mapping constant reserved offset.
const H3_ERROR_RESERVED_OFFSET: u64 = 0x21;
// H3 error mapping constant reserved modulo.
const H3_ERROR_RESERVED_MODULO: u64 = 0x1F;
// WebTransport error mapping constant divisor.
const WT_ERROR_MAP_DIVISOR: u64 = 0x1E;

// Header value search and UTF-8 decoding.
pub(crate) fn find_header_str(headers: &Headers, key: &str) -> Option<String> {
    let val = find_header(headers, key)?;
    String::from_utf8(val.to_vec()).ok()
}

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

// Encodes a list of protocols into a Structured Fields String List.
pub(super) fn encode_wt_protocol_list(protocols: &[String]) -> Result<Bytes, Cow<'static, str>> {
    let mut buf = BytesMut::new();
    for (i, p) in protocols.iter().enumerate() {
        if i > 0 {
            buf.put_slice(b", ");
        }
        buf.put_u8(b'"');
        for &b in p.as_bytes() {
            if b == b'\\' || b == b'"' {
                buf.put_u8(b'\\');
            }
            if (0x20..=0x7E).contains(&b) {
                buf.put_u8(b);
            } else {
                debug!("wt_protocol validate invalid actual={b} expected=printable_ascii");
                return Err("wt_protocol validate invalid".into());
            }
        }
        buf.put_u8(b'"');
    }

    Ok(buf.freeze())
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
    (stream_id & QUIC_STREAM_DIRECTION_MASK) == 0
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
    (stream_id & QUIC_STREAM_DIRECTION_MASK) != 0
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

// Parses a Structured Fields String List securely.
pub(super) fn parse_wt_protocol_list(header: &[u8]) -> Option<Vec<String>> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut escaping = false;
    let mut has_parsed_item = false;

    for &b in header {
        if escaping {
            match b {
                b'"' | b'\\' => {
                    current.push(b as char);
                    escaping = false;
                }
                _ => return None,
            }
        } else if in_string {
            match b {
                b'\\' => escaping = true,
                b'"' => {
                    in_string = false;
                    result.push(current.clone());
                    current.clear();
                    has_parsed_item = true;
                }
                0x20..=0x7E => current.push(b as char),
                _ => return None,
            }
        } else {
            match b {
                b' ' | b'\t' => {}
                b'"' if !has_parsed_item => in_string = true,
                b',' if has_parsed_item => has_parsed_item = false,
                _ => return None,
            }
        }
    }

    if in_string || escaping || (!has_parsed_item && !result.is_empty()) || result.is_empty() {
        return None;
    }

    Some(result)
}

// Parses a single Structured Fields String securely.
pub(super) fn parse_wt_protocol_string(header: &[u8]) -> Option<String> {
    let mut list = parse_wt_protocol_list(header)?;
    if list.len() == 1 { list.pop() } else { None }
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

// Stream direction resolution from ID.
pub(super) fn stream_dir_from_id(stream_id: StreamId, is_client: bool) -> StreamDirection {
    if cfg!(debug_assertions) {
        debug_assert!(
            stream_id <= QUIC_MAX_STREAM_ID,
            "quic_stream validate exceeded actual={stream_id} expected=quic_max_stream_id"
        );
    }

    match (
        can_send_on_stream(stream_id, is_client),
        can_receive_on_stream(stream_id, is_client),
    ) {
        (true, true) => StreamDirection::Bidirectional,
        (true, false) => StreamDirection::SendOnly,
        (false, true) => StreamDirection::ReceiveOnly,
        (false, false) => unreachable!("quic_stream resolve failed"),
    }
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
    (stream_id & QUIC_STREAM_INITIATOR_MASK) == 0
}

// Server initiated stream ID check.
fn is_server_initiated_stream(stream_id: StreamId) -> bool {
    (stream_id & QUIC_STREAM_INITIATOR_MASK) != 0
}

#[cfg(test)]
mod tests;
