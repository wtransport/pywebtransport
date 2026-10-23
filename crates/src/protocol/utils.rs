//! Protocol utilities.

use std::borrow::Cow;
use std::io::Cursor;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tracing::debug;

use crate::common::constants::{
    ERR_H3_FRAME_ERROR, ERR_LIB_INTERNAL_ERROR, ERR_WT_APPLICATION_ERROR_FIRST,
    ERR_WT_APPLICATION_ERROR_LAST, QUIC_STREAM_DIRECTION_MASK, QUIC_STREAM_INITIATOR_MASK,
    WT_AVAILABLE_PROTOCOLS, WT_PROTOCOL,
};
use crate::common::types::{ErrorCode, Headers, StreamDirection, StreamId};

const H3_ERROR_RESERVED_MODULO: u64 = 0x1F;
const H3_ERROR_RESERVED_OFFSET: u64 = 0x21;
const WT_ERROR_MAP_DIVISOR: u64 = 0x1E;

pub(super) fn can_receive_on_stream(stream_id: StreamId, is_client: bool) -> bool {
    if is_bidirectional_stream(stream_id) {
        return true;
    }

    (is_client && is_server_initiated_stream(stream_id))
        || (!is_client && is_client_initiated_stream(stream_id))
}

pub(super) fn can_send_on_stream(stream_id: StreamId, is_client: bool) -> bool {
    if is_bidirectional_stream(stream_id) {
        return true;
    }

    (is_client && is_client_initiated_stream(stream_id))
        || (!is_client && is_server_initiated_stream(stream_id))
}

pub(super) fn encode_wt_protocol_list(protocols: &[String]) -> Result<Bytes, Cow<'static, str>> {
    let mut buffer = BytesMut::new();
    for (index, protocol) in protocols.iter().enumerate() {
        if index > 0 {
            buffer.put_slice(b", ");
        }
        buffer.put_u8(b'"');
        for &byte in protocol.as_bytes() {
            if byte == b'\\' || byte == b'"' {
                buffer.put_u8(b'\\');
            }
            if (0x20..=0x7E).contains(&byte) {
                buffer.put_u8(byte);
            } else {
                debug!("wt_protocol validate invalid actual={byte} expected=printable_ascii");
                return Err("wt_protocol validate invalid".into());
            }
        }
        buffer.put_u8(b'"');
    }

    Ok(buffer.freeze())
}

pub(super) fn find_header(headers: &Headers, key: &str) -> Option<Bytes> {
    let key_bytes = key.as_bytes();

    for (header_key, header_value) in headers {
        if header_key.len() == key_bytes.len() && header_key.eq_ignore_ascii_case(key_bytes) {
            return Some(header_value.clone());
        }
    }

    None
}

pub(super) fn find_header_str(headers: &Headers, key: &str) -> Option<String> {
    let value = find_header(headers, key)?;
    String::from_utf8(value.to_vec()).ok()
}

pub(super) fn http_code_to_webtransport_code(http_error_code: ErrorCode) -> Option<u32> {
    if !(ERR_WT_APPLICATION_ERROR_FIRST..=ERR_WT_APPLICATION_ERROR_LAST).contains(&http_error_code)
    {
        return None;
    }

    if (http_error_code - H3_ERROR_RESERVED_OFFSET).is_multiple_of(H3_ERROR_RESERVED_MODULO) {
        return None;
    }

    let shifted = http_error_code - ERR_WT_APPLICATION_ERROR_FIRST;
    let result = shifted - (shifted / H3_ERROR_RESERVED_MODULO);

    u32::try_from(result).ok()
}

pub(super) fn is_bidirectional_stream(stream_id: StreamId) -> bool {
    (stream_id & QUIC_STREAM_DIRECTION_MASK) == 0
}

pub(super) fn is_connection_specific_header(key: &[u8], value: &[u8], is_request: bool) -> bool {
    if key == b"te" {
        return !(is_request && value.eq_ignore_ascii_case(b"trailers"));
    }

    matches!(
        key,
        b"connection" | b"keep-alive" | b"proxy-connection" | b"transfer-encoding" | b"upgrade"
    )
}

pub(super) fn is_peer_initiated_stream(stream_id: StreamId, is_client: bool) -> bool {
    if is_client {
        is_server_initiated_stream(stream_id)
    } else {
        is_client_initiated_stream(stream_id)
    }
}

pub(super) fn is_request_response_stream(stream_id: StreamId) -> bool {
    is_bidirectional_stream(stream_id) && is_client_initiated_stream(stream_id)
}

pub(super) fn is_unidirectional_stream(stream_id: StreamId) -> bool {
    (stream_id & QUIC_STREAM_DIRECTION_MASK) != 0
}

pub(super) fn is_valid_header(key: &[u8], value: &[u8]) -> bool {
    let name = key.strip_prefix(b":").unwrap_or(key);

    !name.is_empty()
        && name.iter().all(|&byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
        && !matches!(value, [b' ' | b'\t', ..] | [.., b' ' | b'\t'])
        && value
            .iter()
            .all(|&byte| matches!(byte, b'\t' | b' '..=b'~' | 0x80..=0xFF))
}

pub(super) fn is_valid_user_headers(headers: &Headers, is_request: bool) -> bool {
    headers.iter().all(|(key, value)| {
        let key = key.as_ref();
        let value = value.as_ref();

        is_valid_header(key, value)
            && !(key.starts_with(b":")
                || is_connection_specific_header(key, value, is_request)
                || (is_request && key == b"host")
                || matches!(
                    key,
                    b"content-length" | WT_AVAILABLE_PROTOCOLS | WT_PROTOCOL
                ))
    })
}

pub(super) fn next_data_limit(
    current_limit: u64,
    consumed: u64,
    window_size: u64,
    force_update: bool,
) -> Option<u64> {
    let new_limit = consumed.saturating_add(window_size);
    let threshold = if force_update { 0 } else { window_size / 2 };

    if new_limit > current_limit.saturating_add(threshold) {
        Some(new_limit)
    } else {
        None
    }
}

pub(super) fn next_stream_limit(
    current_limit: u64,
    closed_count: u64,
    initial_window: u64,
    force_update: bool,
) -> Option<u64> {
    let new_limit = closed_count.saturating_add(initial_window);
    let threshold = if force_update { 0 } else { initial_window / 2 };

    if new_limit > current_limit && (new_limit >= current_limit.saturating_add(threshold)) {
        Some(new_limit)
    } else {
        None
    }
}

pub(super) fn parse_wt_protocol_list(header: &[u8]) -> Option<Vec<String>> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut escaping = false;
    let mut has_parsed_item = false;

    for &byte in header {
        if escaping {
            match byte {
                b'"' | b'\\' => {
                    current.push(byte as char);
                    escaping = false;
                }
                _ => return None,
            }
        } else if in_string {
            match byte {
                b'\\' => escaping = true,
                b'"' => {
                    in_string = false;
                    result.push(current.clone());
                    current.clear();
                    has_parsed_item = true;
                }
                0x20..=0x7E => current.push(byte as char),
                _ => return None,
            }
        } else {
            match byte {
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

pub(super) fn parse_wt_protocol_string(header: &[u8]) -> Option<String> {
    let mut list = parse_wt_protocol_list(header)?;
    if list.len() == 1 { list.pop() } else { None }
}

pub(super) fn read_varint(buffer: &mut Cursor<&[u8]>) -> Result<u64, ErrorCode> {
    if !buffer.has_remaining() {
        return Err(ERR_H3_FRAME_ERROR);
    }

    let chunk = buffer.chunk();
    let first = *chunk.first().ok_or(ERR_H3_FRAME_ERROR)?;
    let prefix = first >> 6;
    let len = 1 << prefix;

    if buffer.remaining() < len {
        return Err(ERR_H3_FRAME_ERROR);
    }

    let value = match len {
        1 => u64::from(buffer.get_u8() & 0x3f),
        2 => u64::from(buffer.get_u16() & 0x3fff),
        4 => u64::from(buffer.get_u32() & 0x3fff_ffff),
        8 => buffer.get_u64() & 0x3fff_ffff_ffff_ffff,
        _ => return Err(ERR_H3_FRAME_ERROR),
    };

    Ok(value)
}

pub(super) fn stream_direction_from_id(stream_id: StreamId, is_client: bool) -> StreamDirection {
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

pub(super) fn varint_size(value: u64) -> usize {
    if value <= 63 {
        1
    } else if value <= 16383 {
        2
    } else if value <= 1_073_741_823 {
        4
    } else {
        8
    }
}

pub(super) fn webtransport_code_to_http_code(application_error_code: u32) -> ErrorCode {
    let application_error_code = u64::from(application_error_code);
    let base = ERR_WT_APPLICATION_ERROR_FIRST;
    let divisor = WT_ERROR_MAP_DIVISOR;

    let shifted = base + application_error_code;
    let offset = application_error_code / divisor;

    shifted + offset
}

pub(super) fn write_varint(buffer: &mut BytesMut, value: u64) -> Result<(), ErrorCode> {
    if value <= 63 {
        buffer.put_u8(u8::try_from(value).map_err(|_e| ERR_LIB_INTERNAL_ERROR)?);
    } else if value <= 16383 {
        buffer.put_u16(u16::try_from(value).map_err(|_e| ERR_LIB_INTERNAL_ERROR)? | 0x4000);
    } else if value <= 1_073_741_823 {
        buffer.put_u32(u32::try_from(value).map_err(|_e| ERR_LIB_INTERNAL_ERROR)? | 0x8000_0000);
    } else if value <= 4_611_686_018_427_387_903 {
        buffer.put_u64(value | 0xC000_0000_0000_0000);
    } else {
        return Err(ERR_LIB_INTERNAL_ERROR);
    }

    Ok(())
}

fn is_client_initiated_stream(stream_id: StreamId) -> bool {
    (stream_id & QUIC_STREAM_INITIATOR_MASK) == 0
}

fn is_server_initiated_stream(stream_id: StreamId) -> bool {
    (stream_id & QUIC_STREAM_INITIATOR_MASK) != 0
}

#[cfg(test)]
mod tests;
