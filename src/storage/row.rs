use std::io;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnType {
    Integer,
    Text,
    Boolean,
    Char(usize),
}

impl ColumnType {
    pub fn from_str(s: &str) -> Option<Self> {
        let upper = s.to_uppercase();
        if upper.starts_with("CHAR") {
            if let Some(start) = s.find('(') {
                if let Some(end) = s.find(')') {
                    if let Ok(sz) = s[start + 1..end].parse::<usize>() {
                        return Some(ColumnType::Char(sz));
                    }
                }
            }
            return Some(ColumnType::Char(1));
        }
        match upper.as_str() {
            "INTEGER" | "INT" => Some(ColumnType::Integer),
            "TEXT" => Some(ColumnType::Text),
            "BOOLEAN" | "BOOL" => Some(ColumnType::Boolean),
            _ => None,
        }
    }

    pub fn as_str(&self) -> String {
        match self {
            ColumnType::Integer => "INTEGER".into(),
            ColumnType::Text => "TEXT".into(),
            ColumnType::Boolean => "BOOLEAN".into(),
            ColumnType::Char(size) => format!("CHAR({})", size),
        }
    }

    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            1 => Some(ColumnType::Integer),
            2 => Some(ColumnType::Text),
            3 => Some(ColumnType::Boolean),
            4 => Some(ColumnType::Char(0)),
            _ => None,
        }
    }

    pub fn to_code(&self) -> i32 {
        match self {
            ColumnType::Integer => 1,
            ColumnType::Text => 2,
            ColumnType::Boolean => 3,
            ColumnType::Char(_) => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Integer(i32),
    Text(String),
    Boolean(bool),
    Char(String),
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
                ColumnValue::Char(s) => {
                    buf.push(0x04);
                    buf.extend(&(s.len() as u32).to_le_bytes());
                    buf.extend(s.as_bytes());
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
                0x04 => {
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
                    cols.push(ColumnValue::Char(val));
                }
                _ => {
                    return Err(io::Error::new(io::ErrorKind::Other, "Unknown type tag"));
                }
            }
        }
        Ok(RowData(cols))
    }
}

/// Build a `RowData` from raw string values according to the declared column
/// types. Returns an error if any value cannot be converted or the counts do
/// not match.
pub fn build_row_data(values: &[String], columns: &[(String, ColumnType)]) -> Result<RowData, String> {
    if values.len() != columns.len() {
        return Err(format!("Expected {} values, got {}", columns.len(), values.len()));
    }
    let mut cols = Vec::with_capacity(columns.len());
    for (v, (name, ty)) in values.iter().zip(columns.iter()) {
        match ty {
            ColumnType::Integer => match v.parse::<i32>() {
                Ok(i) => cols.push(ColumnValue::Integer(i)),
                Err(_) => {
                    return Err(format!("Value '{}' for column '{}' is not a valid INTEGER", v, name));
                }
            },
            ColumnType::Text => cols.push(ColumnValue::Text(v.clone())),
            ColumnType::Boolean => match v.to_ascii_lowercase().as_str() {
                "true" => cols.push(ColumnValue::Boolean(true)),
                "false" => cols.push(ColumnValue::Boolean(false)),
                _ => {
                    return Err(format!("Value '{}' for column '{}' is not a valid BOOLEAN", v, name));
                }
            },
            ColumnType::Char(len) => {
                let mut s = v.clone();
                if s.len() > *len {
                    s.truncate(*len);
                } else if s.len() < *len {
                    s.push_str(&" ".repeat(*len - s.len()));
                }
                cols.push(ColumnValue::Char(s));
            }
        }
    }
    Ok(RowData(cols))
}

#[derive(Debug, Clone)]
pub struct Row {
    pub key: i32,
    pub data: RowData,
}
