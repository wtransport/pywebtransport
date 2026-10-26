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
use crate::common::types::{ErrorCode, Headers, SessionId, StreamDirection, StreamId};

const H3_ERROR_RESERVED_MODULO: u64 = 0x1F;
const H3_ERROR_RESERVED_OFFSET: u64 = 0x21;
const WT_ERROR_MAP_DIVISOR: u64 = 0x1E;

pub(super) fn combine_header<'a>(headers: &'a Headers, key: &[u8]) -> Option<Cow<'a, [u8]>> {
    let values: Vec<&[u8]> = headers
        .iter()
        .filter(|(header_key, _)| header_key == key)
        .map(|(_, header_value)| header_value.as_ref())
        .collect();

    match values.as_slice() {
        [] => None,
        [value] => Some(Cow::Borrowed(*value)),
        _ => Some(Cow::Owned(values.join(b", ".as_slice()))),
    }
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

pub(super) fn find_header<'a>(headers: &'a Headers, key: &[u8]) -> Option<&'a Bytes> {
    for (header_key, header_value) in headers {
        if header_key == key {
            return Some(header_value);
        }
    }

    None
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
    let is_client_initiated = (stream_id & QUIC_STREAM_INITIATOR_MASK) == 0;

    is_client_initiated != is_client
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

pub(super) fn is_valid_session_id(session_id: SessionId) -> bool {
    (session_id & (QUIC_STREAM_DIRECTION_MASK | QUIC_STREAM_INITIATOR_MASK)) == 0
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

pub(super) fn parse_wt_protocol_list(header: &[u8]) -> Option<Vec<String>> {
    let mut cursor = Cursor::new(header);
    let mut protocols = Vec::new();

    let spaces_len = cursor
        .chunk()
        .iter()
        .take_while(|&&byte| byte == b' ')
        .count();
    cursor.advance(spaces_len);

    while cursor.has_remaining() {
        protocols.push(parse_string(&mut cursor)?);
        discard_parameters(&mut cursor)?;

        let whitespace_len = cursor
            .chunk()
            .iter()
            .take_while(|&&byte| matches!(byte, b' ' | b'\t'))
            .count();
        cursor.advance(whitespace_len);
        if !cursor.has_remaining() {
            break;
        }
        if cursor.get_u8() != b',' {
            return None;
        }

        let whitespace_len = cursor
            .chunk()
            .iter()
            .take_while(|&&byte| matches!(byte, b' ' | b'\t'))
            .count();
        cursor.advance(whitespace_len);
        if !cursor.has_remaining() {
            return None;
        }
    }

    (!protocols.is_empty()).then_some(protocols)
}

pub(super) fn parse_wt_protocol_string(header: &[u8]) -> Option<String> {
    let mut cursor = Cursor::new(header);

    let spaces_len = cursor
        .chunk()
        .iter()
        .take_while(|&&byte| byte == b' ')
        .count();
    cursor.advance(spaces_len);
    let protocol = parse_string(&mut cursor)?;
    discard_parameters(&mut cursor)?;
    let spaces_len = cursor
        .chunk()
        .iter()
        .take_while(|&&byte| byte == b' ')
        .count();
    cursor.advance(spaces_len);

    (!cursor.has_remaining()).then_some(protocol)
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
        is_unidirectional_stream(stream_id),
        is_peer_initiated_stream(stream_id, is_client),
    ) {
        (false, _) => StreamDirection::Bidirectional,
        (true, false) => StreamDirection::SendOnly,
        (true, true) => StreamDirection::ReceiveOnly,
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

fn discard_number(cursor: &mut Cursor<&[u8]>) -> Option<bool> {
    if cursor.chunk().first() == Some(&b'-') {
        cursor.advance(1);
    }

    let integer_len = cursor
        .chunk()
        .iter()
        .take_while(|&&byte| byte.is_ascii_digit())
        .count();
    if integer_len == 0 {
        return None;
    }
    cursor.advance(integer_len);

    if cursor.chunk().first() != Some(&b'.') {
        return (integer_len <= 15).then_some(false);
    }
    cursor.advance(1);

    let fraction_len = cursor
        .chunk()
        .iter()
        .take_while(|&&byte| byte.is_ascii_digit())
        .count();
    cursor.advance(fraction_len);

    (integer_len <= 12 && (1..=3).contains(&fraction_len)).then_some(true)
}

fn discard_parameters(cursor: &mut Cursor<&[u8]>) -> Option<()> {
    while cursor.chunk().first() == Some(&b';') {
        cursor.advance(1);
        let spaces_len = cursor
            .chunk()
            .iter()
            .take_while(|&&byte| byte == b' ')
            .count();
        cursor.advance(spaces_len);

        if !cursor
            .chunk()
            .first()
            .is_some_and(|&byte| byte.is_ascii_lowercase() || byte == b'*')
        {
            return None;
        }
        let key_len = cursor
            .chunk()
            .iter()
            .take_while(
                |&&byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b'*'),
            )
            .count();
        cursor.advance(key_len);

        if cursor.chunk().first() != Some(&b'=') {
            continue;
        }
        cursor.advance(1);

        match cursor.chunk().first().copied()? {
            b'-' | b'0'..=b'9' => {
                discard_number(cursor)?;
            }
            b'"' => {
                parse_string(cursor)?;
            }
            b'*' | b'A'..=b'Z' | b'a'..=b'z' => {
                let token_len = cursor
                    .chunk()
                    .iter()
                    .take_while(|&&byte| {
                        byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~:/".contains(&byte)
                    })
                    .count();
                cursor.advance(token_len);
            }
            b':' => {
                cursor.advance(1);

                let content_len = cursor.chunk().iter().position(|&byte| byte == b':')?;
                let content = cursor.chunk().get(..content_len).unwrap_or_default();
                let data_len = content
                    .iter()
                    .rposition(|&byte| byte != b'=')
                    .map_or(0, |index| index + 1);
                let padding_len = content_len - data_len;

                let is_valid = content
                    .get(..data_len)
                    .unwrap_or_default()
                    .iter()
                    .all(|&byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/'))
                    && padding_len <= 2
                    && data_len % 4 != 1
                    && (padding_len == 0 || content_len % 4 == 0);
                if !is_valid {
                    return None;
                }
                cursor.advance(content_len + 1);
            }
            b'?' => {
                cursor.advance(1);
                if !matches!(cursor.chunk().first(), Some(b'0' | b'1')) {
                    return None;
                }
                cursor.advance(1);
            }
            b'@' => {
                cursor.advance(1);
                let is_decimal = discard_number(cursor)?;
                if is_decimal {
                    return None;
                }
            }
            b'%' => {
                cursor.advance(1);
                if cursor.chunk().first() != Some(&b'"') {
                    return None;
                }
                cursor.advance(1);

                let mut value = Vec::new();

                loop {
                    if !cursor.has_remaining() {
                        return None;
                    }
                    match cursor.get_u8() {
                        b'%' => {
                            let mut octet = 0;
                            for _ in 0..2 {
                                if !cursor.has_remaining() {
                                    return None;
                                }
                                let digit = match cursor.get_u8() {
                                    byte @ b'0'..=b'9' => byte - b'0',
                                    byte @ b'a'..=b'f' => byte - b'a' + 10,
                                    _ => return None,
                                };
                                octet = octet * 16 + digit;
                            }
                            value.push(octet);
                        }
                        b'"' => break,
                        byte @ 0x20..=0x7E => value.push(byte),
                        _ => return None,
                    }
                }

                str::from_utf8(&value).ok()?;
            }
            _ => return None,
        }
    }

    Some(())
}

fn parse_string(cursor: &mut Cursor<&[u8]>) -> Option<String> {
    if cursor.chunk().first() != Some(&b'"') {
        return None;
    }
    cursor.advance(1);

    let mut value = String::new();

    while cursor.has_remaining() {
        match cursor.get_u8() {
            b'\\' if cursor.has_remaining() => match cursor.get_u8() {
                byte @ (b'"' | b'\\') => value.push(char::from(byte)),
                _ => return None,
            },
            b'"' => return Some(value),
            byte @ 0x20..=0x7E => value.push(char::from(byte)),
            _ => return None,
        }
    }

    None
}

#[cfg(test)]
mod tests;
