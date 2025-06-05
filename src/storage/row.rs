use std::io;

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Integer(i32),
    Text(String),
    Boolean(bool),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowData(pub Vec<ColumnValue>);

impl RowData {
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(&(self.0.len() as u16).to_le_bytes());
        for col in &self.0 {
            match col {
                ColumnValue::Integer(i) => {
                    buf.push(0x01);
                    buf.extend(&i.to_le_bytes());
                }
                ColumnValue::Text(s) => {
                    buf.push(0x02);
                    buf.extend(&(s.len() as u32).to_le_bytes());
                    buf.extend(s.as_bytes());
                }
                ColumnValue::Boolean(b) => {
                    buf.push(0x03);
                    buf.push(if *b { 1 } else { 0 });
                }
            }
        }
        buf
    }

    pub fn deserialize(bytes: &[u8]) -> io::Result<RowData> {
        if bytes.len() < 2 {
            return Err(io::Error::new(io::ErrorKind::Other, "Row too short"));
        }
        let mut offset = 0;
        let num_cols = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        let mut cols = Vec::with_capacity(num_cols);
        for _ in 0..num_cols {
            if offset >= bytes.len() {
                return Err(io::Error::new(io::ErrorKind::Other, "Unexpected EOF"));
            }
            let tag = bytes[offset];
            offset += 1;
            match tag {
                0x01 => {
                    if offset + 4 > bytes.len() {
                        return Err(io::Error::new(io::ErrorKind::Other, "EOF"));
                    }
                    let val = i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
                    offset += 4;
                    cols.push(ColumnValue::Integer(val));
                }
                0x02 => {
                    if offset + 4 > bytes.len() {
                        return Err(io::Error::new(io::ErrorKind::Other, "EOF"));
                    }
                    let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    if offset + len > bytes.len() {
                        return Err(io::Error::new(io::ErrorKind::Other, "EOF"));
                    }
                    let val = String::from_utf8_lossy(&bytes[offset..offset + len]).to_string();
                    offset += len;
                    cols.push(ColumnValue::Text(val));
                }
                0x03 => {
                    if offset >= bytes.len() {
                        return Err(io::Error::new(io::ErrorKind::Other, "EOF"));
                    }
                    let b = bytes[offset] != 0;
                    offset += 1;
                    cols.push(ColumnValue::Boolean(b));
                }
                _ => {
                    return Err(io::Error::new(io::ErrorKind::Other, "Unknown type tag"));
                }
            }
        }
        Ok(RowData(cols))
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    pub key: i32,
    pub data: RowData,
}
