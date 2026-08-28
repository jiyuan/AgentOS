use super::limits::{MAX_EVENT_BYTES, MAX_FRAME_BYTES, MAX_HEADERS, MAX_HEADER_BYTES};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct FeishuFrame {
    pub(super) seq_id: u64,
    pub(super) log_id: u64,
    pub(super) service: i32,
    pub(super) method: i32,
    pub(super) headers: Vec<FeishuHeader>,
    pub(super) payload_encoding: String,
    pub(super) payload_type: String,
    pub(super) payload: Vec<u8>,
    pub(super) log_id_new: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FeishuHeader {
    pub(super) key: String,
    pub(super) value: String,
}

impl FeishuFrame {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        if self.seq_id != 0 {
            write_varint_field(&mut out, 1, self.seq_id);
        }
        if self.log_id != 0 {
            write_varint_field(&mut out, 2, self.log_id);
        }
        if self.service != 0 {
            write_varint_field(&mut out, 3, self.service as u64);
        }
        if self.method != 0 {
            write_varint_field(&mut out, 4, self.method as u64);
        }
        for header in &self.headers {
            let mut encoded = Vec::new();
            write_bytes_field(&mut encoded, 1, header.key.as_bytes());
            write_bytes_field(&mut encoded, 2, header.value.as_bytes());
            write_bytes_field(&mut out, 5, &encoded);
        }
        if !self.payload_encoding.is_empty() {
            write_bytes_field(&mut out, 6, self.payload_encoding.as_bytes());
        }
        if !self.payload_type.is_empty() {
            write_bytes_field(&mut out, 7, self.payload_type.as_bytes());
        }
        if !self.payload.is_empty() {
            write_bytes_field(&mut out, 8, &self.payload);
        }
        if !self.log_id_new.is_empty() {
            write_bytes_field(&mut out, 9, self.log_id_new.as_bytes());
        }
        out
    }

    pub(super) fn decode(input: &[u8]) -> Result<Self, String> {
        if input.len() > MAX_FRAME_BYTES {
            return Err(format!(
                "Feishu frame exceeds the {MAX_FRAME_BYTES}-byte limit"
            ));
        }
        let mut frame = Self::default();
        let mut cursor = 0;
        while cursor < input.len() {
            let key = read_varint(input, &mut cursor)?;
            let field = key >> 3;
            let wire = key & 0x07;
            match (field, wire) {
                (1, 0) => frame.seq_id = read_varint(input, &mut cursor)?,
                (2, 0) => frame.log_id = read_varint(input, &mut cursor)?,
                (3, 0) => frame.service = read_varint(input, &mut cursor)? as i32,
                (4, 0) => frame.method = read_varint(input, &mut cursor)? as i32,
                (5, 2) => {
                    if frame.headers.len() >= MAX_HEADERS {
                        return Err(format!(
                            "Feishu frame carries more than {MAX_HEADERS} headers"
                        ));
                    }
                    let bytes = read_bytes(input, &mut cursor)?;
                    frame.headers.push(decode_header(bytes)?);
                }
                (6, 2) => {
                    frame.payload_encoding = decode_header_string(read_bytes(input, &mut cursor)?)?
                }
                (7, 2) => {
                    frame.payload_type = decode_header_string(read_bytes(input, &mut cursor)?)?
                }
                (8, 2) => {
                    let payload = read_bytes(input, &mut cursor)?;
                    if payload.len() > MAX_EVENT_BYTES {
                        return Err(format!(
                            "Feishu event payload exceeds the {MAX_EVENT_BYTES}-byte limit"
                        ));
                    }
                    frame.payload = payload.to_vec();
                }
                (9, 2) => frame.log_id_new = decode_header_string(read_bytes(input, &mut cursor)?)?,
                (_, _) => skip_proto_field(input, &mut cursor, wire)?,
            }
        }
        Ok(frame)
    }
}

pub(super) fn success_frame(frame: &FeishuFrame, biz_rt_ms: u64) -> FeishuFrame {
    let mut headers = frame.headers.clone();
    headers.push(FeishuHeader {
        key: "biz_rt".to_owned(),
        value: biz_rt_ms.to_string(),
    });
    FeishuFrame {
        seq_id: frame.seq_id,
        log_id: frame.log_id,
        service: frame.service,
        method: 1,
        headers,
        payload_encoding: frame.payload_encoding.clone(),
        payload_type: frame.payload_type.clone(),
        payload: br#"{"code":200,"headers":null,"data":null}"#.to_vec(),
        log_id_new: frame.log_id_new.clone(),
    }
}

pub(super) fn pong_frame(frame: &FeishuFrame) -> FeishuFrame {
    FeishuFrame {
        seq_id: frame.seq_id,
        log_id: frame.log_id,
        service: frame.service,
        method: 0,
        headers: vec![FeishuHeader {
            key: "type".to_owned(),
            value: "pong".to_owned(),
        }],
        payload_encoding: "json".to_owned(),
        payload_type: "application/json".to_owned(),
        payload: Vec::new(),
        log_id_new: frame.log_id_new.clone(),
    }
}

pub(super) fn header_value<'a>(headers: &'a [FeishuHeader], key: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|header| header.key == key)
        .map(|header| header.value.as_str())
}

fn decode_header(input: &[u8]) -> Result<FeishuHeader, String> {
    if input.len() > MAX_HEADER_BYTES * 2 {
        return Err(format!(
            "Feishu header exceeds the {}-byte limit",
            MAX_HEADER_BYTES * 2
        ));
    }
    let mut cursor = 0;
    let mut key = String::new();
    let mut value = String::new();
    while cursor < input.len() {
        let field_key = read_varint(input, &mut cursor)?;
        let field = field_key >> 3;
        let wire = field_key & 0x07;
        match (field, wire) {
            (1, 2) => key = decode_header_string(read_bytes(input, &mut cursor)?)?,
            (2, 2) => value = decode_header_string(read_bytes(input, &mut cursor)?)?,
            (_, _) => skip_proto_field(input, &mut cursor, wire)?,
        }
    }
    Ok(FeishuHeader { key, value })
}

fn write_varint_field(out: &mut Vec<u8>, field: u64, value: u64) {
    write_varint(out, field << 3);
    write_varint(out, value);
}

fn write_bytes_field(out: &mut Vec<u8>, field: u64, value: &[u8]) {
    write_varint(out, (field << 3) | 2);
    write_varint(out, value.len() as u64);
    out.extend_from_slice(value);
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn read_varint(input: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let Some(byte) = input.get(*cursor).copied() else {
            return Err("unexpected end of protobuf varint".to_owned());
        };
        *cursor += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("protobuf varint overflow".to_owned())
}

fn read_bytes<'a>(input: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], String> {
    let len = usize::try_from(read_varint(input, cursor)?)
        .map_err(|_| "protobuf length does not fit this platform".to_owned())?;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| "protobuf length overflow".to_owned())?;
    if end > input.len() {
        return Err("unexpected end of protobuf bytes".to_owned());
    }
    let bytes = &input[*cursor..end];
    *cursor = end;
    Ok(bytes)
}

fn decode_header_string(input: &[u8]) -> Result<String, String> {
    if input.len() > MAX_HEADER_BYTES {
        return Err(format!(
            "Feishu header field exceeds the {MAX_HEADER_BYTES}-byte limit"
        ));
    }
    decode_string(input)
}

fn decode_string(input: &[u8]) -> Result<String, String> {
    std::str::from_utf8(input)
        .map(ToOwned::to_owned)
        .map_err(|err| err.to_string())
}

fn skip_proto_field(input: &[u8], cursor: &mut usize, wire: u64) -> Result<(), String> {
    match wire {
        0 => {
            let _ = read_varint(input, cursor)?;
            Ok(())
        }
        1 => {
            *cursor = cursor
                .checked_add(8)
                .ok_or_else(|| "protobuf fixed64 overflow".to_owned())?;
            if *cursor > input.len() {
                Err("unexpected end of protobuf fixed64".to_owned())
            } else {
                Ok(())
            }
        }
        2 => {
            let _ = read_bytes(input, cursor)?;
            Ok(())
        }
        5 => {
            *cursor = cursor
                .checked_add(4)
                .ok_or_else(|| "protobuf fixed32 overflow".to_owned())?;
            if *cursor > input.len() {
                Err("unexpected end of protobuf fixed32".to_owned())
            } else {
                Ok(())
            }
        }
        other => Err(format!("unsupported protobuf wire type {other}")),
    }
}
