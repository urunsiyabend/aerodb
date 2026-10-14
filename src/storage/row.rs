use std::io;

use crate::transaction::TransactionId;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnType {
    Integer,
    Text,
    Boolean,
    Char(usize),
    Varchar(usize),
    Decimal {
        precision: u8,
        scale: u8,
    },
    SmallInt {
        width: usize,
        unsigned: bool,
    },
    MediumInt {
        width: usize,
        unsigned: bool,
    },
    Double {
        precision: usize,
        scale: usize,
        unsigned: bool,
    },
    Date,
    DateTime,
    Timestamp,
    Time,
    Year,
}

impl ColumnType {
    pub fn from_str(s: &str) -> Option<Self> {
        let upper = s.trim().to_uppercase();
        if let Some(ty) = Self::parse_varchar_or_decimal(&upper) {
            return ty;
        }
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
                        if let (Ok(p), Ok(s)) = (
                            parts[0].trim().parse::<usize>(),
                            parts[1].trim().parse::<usize>(),
                        ) {
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
            return Some(ColumnType::Double {
                precision,
                scale,
                unsigned,
            });
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

    /// Parses VARCHAR/CHARACTER VARYING/DECIMAL/NUMERIC. Returns `None` when the
    /// text is not one of those families, `Some(None)` when it is but invalid.
    fn parse_varchar_or_decimal(upper: &str) -> Option<Option<Self>> {
        let (name, args) = match upper.find('(') {
            Some(start) => {
                if !upper.ends_with(')') {
                    // Only claim the family if the name matches, so garbage is rejected.
                    let name = upper[..start].trim();
                    return matches!(
                        name,
                        "VARCHAR" | "CHARACTER VARYING" | "DECIMAL" | "NUMERIC"
                    )
                    .then_some(None);
                }
                (
                    upper[..start].trim(),
                    Some(&upper[start + 1..upper.len() - 1]),
                )
            }
            None => (upper.trim(), None),
        };
        match name {
            "VARCHAR" | "CHARACTER VARYING" => {
                let n = args.and_then(|a| a.trim().parse::<usize>().ok());
                Some(match n {
                    Some(n) if (1..=65535).contains(&n) => Some(ColumnType::Varchar(n)),
                    _ => None,
                })
            }
            "DECIMAL" | "NUMERIC" => {
                let (p, sc) = match args {
                    None => (Some(18u8), Some(0u8)),
                    Some(a) => {
                        let parts: Vec<&str> = a.split(',').map(str::trim).collect();
                        match parts.as_slice() {
                            [p] => (p.parse::<u8>().ok(), Some(0)),
                            [p, sc] => (p.parse::<u8>().ok(), sc.parse::<u8>().ok()),
                            _ => (None, None),
                        }
                    }
                };
                Some(match (p, sc) {
                    (Some(p), Some(sc)) if (1..=38).contains(&p) && sc <= p => {
                        Some(ColumnType::Decimal {
                            precision: p,
                            scale: sc,
                        })
                    }
                    _ => None,
                })
            }
            _ => None,
        }
    }

    pub fn as_str(&self) -> String {
        match self {
            ColumnType::Varchar(n) => format!("VARCHAR({})", n),
            ColumnType::Decimal { precision, scale } => {
                format!("DECIMAL({},{})", precision, scale)
            }
            ColumnType::Integer => "INTEGER".into(),
            ColumnType::Text => "TEXT".into(),
            ColumnType::Boolean => "BOOLEAN".into(),
            ColumnType::Char(size) => format!("CHAR({})", size),
            ColumnType::SmallInt { width, unsigned } => {
                let mut s = String::from("SMALLINT");
                if *width > 0 {
                    s.push_str(&format!("({})", width));
                }
                if *unsigned {
                    s.push_str(" UNSIGNED");
                }
                s
            }
            ColumnType::MediumInt { width, unsigned } => {
                let mut s = String::from("MEDIUMINT");
                if *width > 0 {
                    s.push_str(&format!("({})", width));
                }
                if *unsigned {
                    s.push_str(" UNSIGNED");
                }
                s
            }
            ColumnType::Double {
                precision,
                scale,
                unsigned,
            } => {
                let mut s = format!("DOUBLE({},{})", precision, scale);
                if *unsigned {
                    s.push_str(" UNSIGNED");
                }
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
            5 => Some(ColumnType::SmallInt {
                width: 0,
                unsigned: false,
            }),
            6 => Some(ColumnType::MediumInt {
                width: 0,
                unsigned: false,
            }),
            7 => Some(ColumnType::Double {
                precision: 10,
                scale: 0,
                unsigned: false,
            }),
            8 => Some(ColumnType::Date),
            9 => Some(ColumnType::DateTime),
            10 => Some(ColumnType::Timestamp),
            11 => Some(ColumnType::Time),
            12 => Some(ColumnType::Year),
            13 => Some(ColumnType::Varchar(0)),
            14 => Some(ColumnType::Decimal {
                precision: 18,
                scale: 0,
            }),
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
            ColumnType::Varchar(_) => 13,
            ColumnType::Decimal { .. } => 14,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Null,
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
    Decimal(crate::sql::decimal::Decimal),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowData(pub Vec<ColumnValue>);

/// Transaction id assigned to rows that were written before row-level MVCC
/// metadata existed on disk.  Real transaction ids start at 1 today, so 0 is
/// reserved as a committed bootstrap transaction for legacy payloads.
pub const COMMITTED_BOOTSTRAP_TX: TransactionId = 0;

/// Current row payload format.  The high-bit marker keeps the version byte out
/// of the range normally used by the low byte of small legacy column counts.
pub const MVCC_ROW_PAYLOAD_FORMAT_VERSION: u8 = 0x81;

impl RowData {
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(&(self.0.len() as u16).to_le_bytes());
        for col in &self.0 {
            match col {
                ColumnValue::Null => {
                    buf.push(0x00);
                }
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
                ColumnValue::Decimal(d) => {
                    buf.push(0x0B);
                    buf.extend(&d.mantissa().to_le_bytes());
                    buf.push(d.scale());
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
                0x00 => {
                    cols.push(ColumnValue::Null);
                }
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
                    let len =
                        u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
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
                    let len =
                        u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
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
                0x0B => {
                    if offset + 17 > bytes.len() {
                        return Err(io::Error::other("EOF"));
                    }
                    let mantissa =
                        i128::from_le_bytes(bytes[offset..offset + 16].try_into().unwrap());
                    let scale = bytes[offset + 16];
                    offset += 17;
                    let d = crate::sql::decimal::Decimal::new(mantissa, scale)
                        .map_err(|_| io::Error::other("Invalid decimal value"))?;
                    cols.push(ColumnValue::Decimal(d));
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
/// types (0.5.1 helper: the text `NULL`, any case, is SQL NULL). Conversion
/// is the shared assignment path, [`crate::sql::value::Value::to_column_value`]
/// (CHAR/VARCHAR lengths in characters, DECIMAL rounded half away from zero).
pub fn build_row_data(
    values: &[String],
    columns: &[(String, ColumnType)],
) -> Result<RowData, String> {
    if values.len() != columns.len() {
        return Err(format!(
            "Expected {} values, got {}",
            columns.len(),
            values.len()
        ));
    }
    let mut cols = Vec::with_capacity(columns.len());
    for (v, (name, ty)) in values.iter().zip(columns.iter()) {
        if v.eq_ignore_ascii_case("NULL") {
            cols.push(ColumnValue::Null);
            continue;
        }
        let value = crate::sql::value::Value::Text(v.clone());
        cols.push(value.to_column_value(ty, name).map_err(|e| e.to_string())?);
    }
    Ok(RowData(cols))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowVersionRef {
    pub page_num: u32,
    pub slot_index: u16,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub key: i32,
    pub data: RowData,
    pub created_tx: TransactionId,
    pub deleted_tx: Option<TransactionId>,
    pub version_ptr: Option<RowVersionRef>,
}

impl Row {
    pub fn new(key: i32, data: RowData) -> Self {
        Self {
            key,
            data,
            created_tx: COMMITTED_BOOTSTRAP_TX,
            deleted_tx: None,
            version_ptr: None,
        }
    }

    pub fn serialize_mvcc_payload(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(MVCC_ROW_PAYLOAD_FORMAT_VERSION);
        buf.extend(&self.created_tx.to_le_bytes());
        match self.deleted_tx {
            Some(deleted_tx) => {
                buf.push(1);
                buf.extend(&deleted_tx.to_le_bytes());
            }
            None => buf.push(0),
        }
        match self.version_ptr {
            Some(version_ptr) => {
                buf.push(1);
                buf.extend(&version_ptr.page_num.to_le_bytes());
                buf.extend(&version_ptr.slot_index.to_le_bytes());
            }
            None => buf.push(0),
        }
        buf.extend(self.data.serialize());
        buf
    }

    pub fn deserialize_mvcc_payload(key: i32, bytes: &[u8]) -> io::Result<Self> {
        if bytes.first().copied() != Some(MVCC_ROW_PAYLOAD_FORMAT_VERSION) {
            return Ok(Self {
                key,
                data: RowData::deserialize(bytes)?,
                created_tx: COMMITTED_BOOTSTRAP_TX,
                deleted_tx: None,
                version_ptr: None,
            });
        }

        let mut offset = 1;
        if offset + 8 > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "MVCC row missing created_tx",
            ));
        }
        let created_tx =
            TransactionId::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        if offset >= bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "MVCC row missing deleted_tx flag",
            ));
        }
        let deleted_tx = if bytes[offset] == 0 {
            offset += 1;
            None
        } else {
            offset += 1;
            if offset + 8 > bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "MVCC row missing deleted_tx",
                ));
            }
            let tx = TransactionId::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            Some(tx)
        };

        if offset >= bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "MVCC row missing version_ptr flag",
            ));
        }
        let version_ptr = if bytes[offset] == 0 {
            offset += 1;
            None
        } else {
            offset += 1;
            if offset + 6 > bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "MVCC row missing version_ptr",
                ));
            }
            let page_num = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let slot_index = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
            offset += 2;
            Some(RowVersionRef {
                page_num,
                slot_index,
            })
        };

        Ok(Self {
            key,
            data: RowData::deserialize(&bytes[offset..])?,
            created_tx,
            deleted_tx,
            version_ptr,
        })
    }
}

impl ColumnValue {
    pub fn to_string_value(&self) -> String {
        match self {
            ColumnValue::Null => "NULL".into(),
            ColumnValue::Integer(i) => i.to_string(),
            ColumnValue::Text(s) => s.clone(),
            ColumnValue::Boolean(b) => b.to_string(),
            ColumnValue::Char(s) => s.clone(),
            ColumnValue::Double(f) => f.to_string(),
            ColumnValue::Date(d) => {
                use chrono::{Duration, NaiveDate};
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let date = epoch + Duration::days(*d as i64);
                date.format("%Y-%m-%d").to_string()
            }
            ColumnValue::DateTime(ts) | ColumnValue::Timestamp(ts) => {
                use chrono::NaiveDateTime;
                NaiveDateTime::from_timestamp_opt(*ts, 0)
                    .unwrap()
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string()
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
            ColumnValue::Decimal(d) => d.to_string(),
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
        .map(|dt| dt.and_utc().timestamp())
}

pub(crate) fn parse_time(s: &str) -> Option<i32> {
    let neg = s.starts_with('-');
    let t = if neg { &s[1..] } else { s };
    let parts: Vec<&str> = t.split(':').collect();
    if parts.len() != 3 {
        return None;
    }
    let h: i32 = parts[0].parse().ok()?;
    let m: i32 = parts[1].parse().ok()?;
    let sec: i32 = parts[2].parse().ok()?;
    if h > 838 || m > 59 || sec > 59 {
        return None;
    }
    let mut total = h * 3600 + m * 60 + sec;
    if neg {
        total = -total;
    }
    Some(total)
}

pub(crate) fn parse_year(s: &str) -> Option<u16> {
    if s.len() != 4 {
        return None;
    }
    let y: u16 = s.parse().ok()?;
    if y == 0 || (1901..=2155).contains(&y) {
        Some(y)
    } else {
        None
    }
}

#[cfg(test)]
mod mvcc_tests {
    use super::*;

    #[test]
    fn mvcc_payload_round_trips_metadata_and_column_encoding() {
        let row = Row {
            key: 7,
            data: RowData(vec![
                ColumnValue::Integer(42),
                ColumnValue::Text("alice".into()),
            ]),
            created_tx: 12,
            deleted_tx: Some(15),
            version_ptr: Some(RowVersionRef {
                page_num: 3,
                slot_index: 2,
            }),
        };

        let payload = row.serialize_mvcc_payload();
        assert_eq!(payload[0], MVCC_ROW_PAYLOAD_FORMAT_VERSION);

        let decoded = Row::deserialize_mvcc_payload(row.key, &payload).unwrap();
        assert_eq!(decoded.key, row.key);
        assert_eq!(decoded.data, row.data);
        assert_eq!(decoded.created_tx, row.created_tx);
        assert_eq!(decoded.deleted_tx, row.deleted_tx);
        assert_eq!(decoded.version_ptr, row.version_ptr);
    }

    #[test]
    fn legacy_payload_defaults_to_committed_bootstrap_metadata() {
        let data = RowData(vec![ColumnValue::Boolean(true)]);
        let legacy_payload = data.serialize();

        let decoded = Row::deserialize_mvcc_payload(9, &legacy_payload).unwrap();
        assert_eq!(decoded.key, 9);
        assert_eq!(decoded.data, data);
        assert_eq!(decoded.created_tx, COMMITTED_BOOTSTRAP_TX);
        assert_eq!(decoded.deleted_tx, None);
        assert_eq!(decoded.version_ptr, None);
    }

    #[test]
    fn decimal_column_value_round_trips() {
        use crate::sql::decimal::Decimal;
        let row = RowData(vec![
            ColumnValue::Decimal(Decimal::parse("-123.4500").unwrap()),
            ColumnValue::Decimal(Decimal::parse("0.30").unwrap()),
            ColumnValue::Integer(1),
        ]);
        let bytes = row.serialize();
        assert_eq!(bytes[2], 0x0B);
        let back = RowData::deserialize(&bytes).unwrap();
        assert_eq!(back, row);
        if let ColumnValue::Decimal(d) = &back.0[0] {
            assert_eq!(d.scale(), 4);
        }
        assert_eq!(back.0[1].to_string_value(), "0.30");
    }

    #[test]
    fn truncated_decimal_is_an_error() {
        let row = RowData(vec![ColumnValue::Decimal(
            crate::sql::decimal::Decimal::parse("1.5").unwrap(),
        )]);
        let mut bytes = row.serialize();
        bytes.truncate(bytes.len() - 1);
        assert!(RowData::deserialize(&bytes).is_err());
    }

    #[test]
    fn column_type_parses_decimal_and_varchar() {
        assert_eq!(
            ColumnType::from_str("DECIMAL(10,2)"),
            Some(ColumnType::Decimal { precision: 10, scale: 2 })
        );
        assert_eq!(
            ColumnType::from_str("numeric"),
            Some(ColumnType::Decimal { precision: 18, scale: 0 })
        );
        assert_eq!(
            ColumnType::from_str("DECIMAL(5)"),
            Some(ColumnType::Decimal { precision: 5, scale: 0 })
        );
        assert_eq!(
            ColumnType::from_str("VARCHAR(10)"),
            Some(ColumnType::Varchar(10))
        );
        assert_eq!(
            ColumnType::from_str("character varying(7)"),
            Some(ColumnType::Varchar(7))
        );
        assert_eq!(ColumnType::from_str("DECIMAL(40,2)"), None);
        assert_eq!(ColumnType::from_str("DECIMAL(5,6)"), None);
        assert_eq!(ColumnType::from_str("DECIMAL(0)"), None);
        assert_eq!(ColumnType::from_str("VARCHAR"), None);
        assert_eq!(ColumnType::from_str("VARCHAR(0)"), None);
        assert_eq!(ColumnType::from_str("VARCHAR(70000)"), None);
        assert_eq!(ColumnType::from_str("CHAR(4)"), Some(ColumnType::Char(4)));
    }

    #[test]
    fn column_type_text_and_codes() {
        assert_eq!(ColumnType::Varchar(9).as_str(), "VARCHAR(9)");
        assert_eq!(
            ColumnType::Decimal { precision: 10, scale: 2 }.as_str(),
            "DECIMAL(10,2)"
        );
        assert_eq!(ColumnType::Varchar(9).to_code(), 13);
        assert_eq!(ColumnType::Decimal { precision: 1, scale: 0 }.to_code(), 14);
        assert!(matches!(ColumnType::from_code(13), Some(ColumnType::Varchar(_))));
        assert!(matches!(ColumnType::from_code(14), Some(ColumnType::Decimal { .. })));
    }

    #[test]
    fn build_row_data_handles_varchar_and_decimal() {
        let cols = vec![
            ("v".to_string(), ColumnType::Varchar(3)),
            ("d".to_string(), ColumnType::Decimal { precision: 5, scale: 2 }),
        ];
        let ok = build_row_data(&["abc".into(), "1.005".into()], &cols).unwrap();
        assert_eq!(ok.0[0], ColumnValue::Text("abc".into()));
        assert_eq!(ok.0[1].to_string_value(), "1.01");
        let e = build_row_data(&["abcd".into(), "1".into()], &cols).unwrap_err();
        assert!(e.contains("exceeds length"), "{e}");
        assert!(build_row_data(&["a".into(), "1000".into()], &cols).is_err());
        assert!(build_row_data(&["a".into(), "x".into()], &cols).is_err());
    }
}
