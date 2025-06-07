use std::io;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnType {
    Integer,
    Text,
    Boolean,
    Char(usize),
    SmallInt { width: usize, unsigned: bool },
    MediumInt { width: usize, unsigned: bool },
    Double { precision: usize, scale: usize, unsigned: bool },
    Date,
    DateTime,
    Timestamp,
    Time,
    Year,
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
        // handle SMALLINT, MEDIUMINT, DOUBLE with optional size/precision and UNSIGNED
        let mut base = upper.as_str();
        let mut unsigned = false;
        if base.ends_with(" UNSIGNED") {
            unsigned = true;
            base = &base[..base.len() - 9];
        }
        if base.starts_with("SMALLINT") {
            let mut width = 0usize;
            if let Some(start) = base.find('(') {
                if let Some(end) = base.find(')') {
                    if let Ok(w) = base[start + 1..end].parse::<usize>() {
                        if w <= 255 {
                            width = w;
                        } else {
                            return None;
                        }
                    }
                }
                base = &base[..start];
            }
            return Some(ColumnType::SmallInt { width, unsigned });
        }
        if base.starts_with("MEDIUMINT") {
            let mut width = 0usize;
            if let Some(start) = base.find('(') {
                if let Some(end) = base.find(')') {
                    if let Ok(w) = base[start + 1..end].parse::<usize>() {
                        if w <= 255 {
                            width = w;
                        } else {
                            return None;
                        }
                    }
                }
                base = &base[..start];
            }
            return Some(ColumnType::MediumInt { width, unsigned });
        }
        if base.starts_with("DOUBLE") {
            let mut precision = 10usize;
            let mut scale = 0usize;
            if let Some(start) = base.find('(') {
                if let Some(end) = base.find(')') {
                    let args = &base[start + 1..end];
                    let parts: Vec<&str> = args.split(',').collect();
                    if parts.len() == 2 {
                        if let (Ok(p), Ok(s)) = (parts[0].trim().parse::<usize>(), parts[1].trim().parse::<usize>()) {
                            if p <= 255 && s <= 255 && p >= s {
                                precision = p;
                                scale = s;
                            } else {
                                return None;
                            }
                        }
                    }
                }
                base = &base[..start];
            }
            return Some(ColumnType::Double { precision, scale, unsigned });
        }
        if base == "DATE" {
            return Some(ColumnType::Date);
        }
        if base == "DATETIME" {
            return Some(ColumnType::DateTime);
        }
        if base == "TIMESTAMP" {
            return Some(ColumnType::Timestamp);
        }
        if base == "TIME" {
            return Some(ColumnType::Time);
        }
        if base == "YEAR" {
            return Some(ColumnType::Year);
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
            ColumnType::SmallInt { width, unsigned } => {
                let mut s = String::from("SMALLINT");
                if *width > 0 { s.push_str(&format!("({})", width)); }
                if *unsigned { s.push_str(" UNSIGNED"); }
                s
            }
            ColumnType::MediumInt { width, unsigned } => {
                let mut s = String::from("MEDIUMINT");
                if *width > 0 { s.push_str(&format!("({})", width)); }
                if *unsigned { s.push_str(" UNSIGNED"); }
                s
            }
            ColumnType::Double { precision, scale, unsigned } => {
                let mut s = format!("DOUBLE({},{})", precision, scale);
                if *unsigned { s.push_str(" UNSIGNED"); }
                s
            }
            ColumnType::Date => "DATE".into(),
            ColumnType::DateTime => "DATETIME".into(),
            ColumnType::Timestamp => "TIMESTAMP".into(),
            ColumnType::Time => "TIME".into(),
            ColumnType::Year => "YEAR".into(),
        }
    }

    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            1 => Some(ColumnType::Integer),
            2 => Some(ColumnType::Text),
            3 => Some(ColumnType::Boolean),
            4 => Some(ColumnType::Char(0)),
            5 => Some(ColumnType::SmallInt { width: 0, unsigned: false }),
            6 => Some(ColumnType::MediumInt { width: 0, unsigned: false }),
            7 => Some(ColumnType::Double { precision: 10, scale: 0, unsigned: false }),
            8 => Some(ColumnType::Date),
            9 => Some(ColumnType::DateTime),
            10 => Some(ColumnType::Timestamp),
            11 => Some(ColumnType::Time),
            12 => Some(ColumnType::Year),
            _ => None,
        }
    }

    pub fn to_code(&self) -> i32 {
        match self {
            ColumnType::Integer => 1,
            ColumnType::Text => 2,
            ColumnType::Boolean => 3,
            ColumnType::Char(_) => 4,
            ColumnType::SmallInt { .. } => 5,
            ColumnType::MediumInt { .. } => 6,
            ColumnType::Double { .. } => 7,
            ColumnType::Date => 8,
            ColumnType::DateTime => 9,
            ColumnType::Timestamp => 10,
            ColumnType::Time => 11,
            ColumnType::Year => 12,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Integer(i32),
    Text(String),
    Boolean(bool),
    Char(String),
    Double(f64),
    Date(i32),
    DateTime(i64),
    Timestamp(i64),
    Time(i32),
    Year(u16),
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
                ColumnValue::Double(f) => {
                    buf.push(0x05);
                    buf.extend(&f.to_le_bytes());
                }
                ColumnValue::Date(d) => {
                    buf.push(0x06);
                    buf.extend(&d.to_le_bytes());
                }
                ColumnValue::DateTime(ts) => {
                    buf.push(0x07);
                    buf.extend(&ts.to_le_bytes());
                }
                ColumnValue::Timestamp(ts) => {
                    buf.push(0x08);
                    buf.extend(&ts.to_le_bytes());
                }
                ColumnValue::Time(t) => {
                    buf.push(0x09);
                    buf.extend(&t.to_le_bytes());
                }
                ColumnValue::Year(y) => {
                    buf.push(0x0A);
                    buf.extend(&y.to_le_bytes());
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
                0x05 => {
                    if offset + 8 > bytes.len() {
                        return Err(io::Error::new(io::ErrorKind::Other, "EOF"));
                    }
                    let val = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    cols.push(ColumnValue::Double(val));
                }
                0x06 => {
                    if offset + 4 > bytes.len() {
                        return Err(io::Error::new(io::ErrorKind::Other, "EOF"));
                    }
                    let val = i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
                    offset += 4;
                    cols.push(ColumnValue::Date(val));
                }
                0x07 => {
                    if offset + 8 > bytes.len() {
                        return Err(io::Error::new(io::ErrorKind::Other, "EOF"));
                    }
                    let val = i64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    cols.push(ColumnValue::DateTime(val));
                }
                0x08 => {
                    if offset + 8 > bytes.len() {
                        return Err(io::Error::new(io::ErrorKind::Other, "EOF"));
                    }
                    let val = i64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    cols.push(ColumnValue::Timestamp(val));
                }
                0x09 => {
                    if offset + 4 > bytes.len() {
                        return Err(io::Error::new(io::ErrorKind::Other, "EOF"));
                    }
                    let val = i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
                    offset += 4;
                    cols.push(ColumnValue::Time(val));
                }
                0x0A => {
                    if offset + 2 > bytes.len() {
                        return Err(io::Error::new(io::ErrorKind::Other, "EOF"));
                    }
                    let val = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
                    offset += 2;
                    cols.push(ColumnValue::Year(val));
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
                if v.len() > *len {
                    return Err(format!(
                        "Value '{}' for column '{}' exceeds length {}",
                        v, name, len
                    ));
                }
                let mut s = v.clone();
                if s.len() < *len {
                    s.push_str(&" ".repeat(*len - s.len()));
                }
                cols.push(ColumnValue::Char(s));
            }
            ColumnType::SmallInt { unsigned, .. } => {
                let val = v.parse::<i32>().map_err(|_| {
                    format!("Value '{}' for column '{}' is not a valid SMALLINT", v, name)
                })?;
                if *unsigned {
                    if !(0..=65535).contains(&val) {
                        return Err(format!("Value '{}' for column '{}' out of range", v, name));
                    }
                } else if !(-32768..=32767).contains(&val) {
                    return Err(format!("Value '{}' for column '{}' out of range", v, name));
                }
                cols.push(ColumnValue::Integer(val));
            }
            ColumnType::MediumInt { unsigned, .. } => {
                let val = v.parse::<i32>().map_err(|_| {
                    format!("Value '{}' for column '{}' is not a valid MEDIUMINT", v, name)
                })?;
                if *unsigned {
                    if !(0..=16_777_215).contains(&val) {
                        return Err(format!("Value '{}' for column '{}' out of range", v, name));
                    }
                } else if !(-8_388_608..=8_388_607).contains(&val) {
                    return Err(format!("Value '{}' for column '{}' out of range", v, name));
                }
                cols.push(ColumnValue::Integer(val));
            }
            ColumnType::Double { unsigned, .. } => {
                let val = v.parse::<f64>().map_err(|_| {
                    format!("Value '{}' for column '{}' is not a valid DOUBLE", v, name)
                })?;
                if *unsigned && val < 0.0 {
                    return Err(format!("Value '{}' for column '{}' out of range", v, name));
                }
                cols.push(ColumnValue::Double(val));
            }
            ColumnType::Date => {
                match parse_date(v) {
                    Some(d) => cols.push(ColumnValue::Date(d)),
                    None => {
                        return Err(format!("Value '{}' for column '{}' is not a valid DATE", v, name));
                    }
                }
            }
            ColumnType::DateTime => {
                match parse_datetime(v) {
                    Some(ts) => cols.push(ColumnValue::DateTime(ts)),
                    None => {
                        return Err(format!("Value '{}' for column '{}' is not a valid DATETIME", v, name));
                    }
                }
            }
            ColumnType::Timestamp => {
                match parse_datetime(v) {
                    Some(ts) => cols.push(ColumnValue::Timestamp(ts)),
                    None => {
                        return Err(format!("Value '{}' for column '{}' is not a valid TIMESTAMP", v, name));
                    }
                }
            }
            ColumnType::Time => {
                match parse_time(v) {
                    Some(t) => cols.push(ColumnValue::Time(t)),
                    None => {
                        return Err(format!("Value '{}' for column '{}' is not a valid TIME", v, name));
                    }
                }
            }
            ColumnType::Year => {
                match parse_year(v) {
                    Some(y) => cols.push(ColumnValue::Year(y)),
                    None => {
                        return Err(format!("Value '{}' for column '{}' is not a valid YEAR", v, name));
                    }
                }
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

impl ColumnValue {
    pub fn to_string_value(&self) -> String {
        match self {
            ColumnValue::Integer(i) => i.to_string(),
            ColumnValue::Text(s) => s.clone(),
            ColumnValue::Boolean(b) => b.to_string(),
            ColumnValue::Char(s) => s.clone(),
            ColumnValue::Double(f) => f.to_string(),
            ColumnValue::Date(d) => {
                use chrono::{NaiveDate, Duration};
                let epoch = NaiveDate::from_ymd_opt(1970,1,1).unwrap();
                let date = epoch + Duration::days(*d as i64);
                date.format("%Y-%m-%d").to_string()
            }
            ColumnValue::DateTime(ts) | ColumnValue::Timestamp(ts) => {
                use chrono::NaiveDateTime;
                NaiveDateTime::from_timestamp_opt(*ts, 0).unwrap().format("%Y-%m-%d %H:%M:%S").to_string()
            }
            ColumnValue::Time(t) => {
                let neg = *t < 0;
                let mut s = t.abs();
                let h = s / 3600;
                let m = (s % 3600) / 60;
                let sec = s % 60;
                format!("{}{:02}:{:02}:{:02}", if neg { "-" } else { "" }, h, m, sec)
            }
            ColumnValue::Year(y) => format!("{:04}", y),
        }
    }
}

pub(crate) fn parse_date(s: &str) -> Option<i32> {
    use chrono::NaiveDate;
    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    Some((date - epoch).num_days() as i32)
}

pub(crate) fn parse_datetime(s: &str) -> Option<i64> {
    use chrono::NaiveDateTime;
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|dt| dt.timestamp())
}

pub(crate) fn parse_time(s: &str) -> Option<i32> {
    let neg = s.starts_with('-');
    let t = if neg { &s[1..] } else { s };
    let parts: Vec<&str> = t.split(':').collect();
    if parts.len() != 3 { return None; }
    let h: i32 = parts[0].parse().ok()?;
    let m: i32 = parts[1].parse().ok()?;
    let sec: i32 = parts[2].parse().ok()?;
    if h > 838 || m > 59 || sec > 59 { return None; }
    let mut total = h * 3600 + m * 60 + sec;
    if neg { total = -total; }
    Some(total)
}

pub(crate) fn parse_year(s: &str) -> Option<u16> {
    if s.len() != 4 { return None; }
    let y: u16 = s.parse().ok()?;
    if y == 0 || (1901..=2155).contains(&y) { Some(y) } else { None }
}
