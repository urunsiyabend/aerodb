//! Typed runtime SQL values and operations over them.

use crate::sql::decimal::{Decimal, DecimalError};
use crate::storage::row::{
    ColumnType, ColumnValue, parse_date, parse_datetime, parse_time, parse_year,
};
use std::cmp::Ordering;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Decimal(Decimal),
    Double(f64),
    Text(String),
    Char(String),
    Date(i32),
    DateTime(i64),
    Timestamp(i64),
    Time(i32),
    Year(u16),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SqlType {
    Boolean,
    Integer,
    SmallInt,
    BigInt,
    Decimal { precision: u8, scale: u8 },
    Double,
    Text,
    Char(usize),
    Varchar(usize),
    Date,
    DateTime,
    Timestamp,
    Time,
    Year,
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueError {
    DivisionByZero,
    NumericOverflow,
    InvalidCast(String),
    StringTooLong(String),
    TypeMismatch(String),
}

impl fmt::Display for ValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueError::DivisionByZero => write!(f, "division by zero"),
            ValueError::NumericOverflow => write!(f, "numeric value out of range"),
            ValueError::InvalidCast(m) | ValueError::StringTooLong(m) | ValueError::TypeMismatch(m) => {
                write!(f, "{}", m)
            }
        }
    }
}

fn dec_err(e: DecimalError) -> ValueError {
    match e {
        DecimalError::Overflow => ValueError::NumericOverflow,
        DecimalError::DivisionByZero => ValueError::DivisionByZero,
        DecimalError::Invalid(m) => ValueError::InvalidCast(m),
    }
}

#[derive(Clone, Copy, PartialEq)]
enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

impl ArithOp {
    fn symbol(self) -> &'static str {
        match self {
            ArithOp::Add => "+",
            ArithOp::Sub => "-",
            ArithOp::Mul => "*",
            ArithOp::Div => "/",
            ArithOp::Rem => "%",
        }
    }
}

fn is_numeric(v: &Value) -> bool {
    matches!(v, Value::Int(_) | Value::Decimal(_) | Value::Double(_))
}

fn as_decimal(v: &Value) -> Option<Decimal> {
    match v {
        Value::Int(i) => Some(Decimal::from_i64(*i)),
        Value::Decimal(d) => Some(*d),
        _ => None,
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Decimal(d) => Some(d.to_f64()),
        Value::Double(f) => Some(*f),
        _ => None,
    }
}

fn is_text(v: &Value) -> bool {
    matches!(v, Value::Text(_) | Value::Char(_))
}

fn text_of(v: &Value) -> Option<&str> {
    match v {
        Value::Text(s) | Value::Char(s) => Some(s),
        _ => None,
    }
}

fn cmp_f64(a: f64, b: f64) -> Option<Ordering> {
    a.partial_cmp(&b)
}

/// Parses text for comparison against a temporal value, producing a Value of
/// the same kind as `like`.
fn parse_text_like(text: &str, like: &Value) -> Result<Value, ValueError> {
    let parsed = match like {
        Value::Date(_) => parse_date(text).map(Value::Date),
        Value::DateTime(_) => parse_datetime(text).map(Value::DateTime),
        Value::Timestamp(_) => parse_datetime(text).map(Value::Timestamp),
        Value::Time(_) => parse_time(text).map(Value::Time),
        Value::Year(_) => parse_year(text).map(Value::Year),
        _ => None,
    };
    parsed.ok_or_else(|| {
        ValueError::InvalidCast(format!(
            "'{}' is not a valid {}",
            text,
            like.type_name()
        ))
    })
}

fn is_temporal(v: &Value) -> bool {
    matches!(
        v,
        Value::Date(_) | Value::DateTime(_) | Value::Timestamp(_) | Value::Time(_) | Value::Year(_)
    )
}

fn temporal_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Date(x), Value::Date(y)) => Some(x.cmp(y)),
        (Value::DateTime(x), Value::DateTime(y)) => Some(x.cmp(y)),
        (Value::Timestamp(x), Value::Timestamp(y)) => Some(x.cmp(y)),
        (Value::Time(x), Value::Time(y)) => Some(x.cmp(y)),
        (Value::Year(x), Value::Year(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

fn int_in_range(v: i64, ty: &SqlType) -> bool {
    match ty {
        SqlType::Integer => i32::try_from(v).is_ok(),
        SqlType::SmallInt => i16::try_from(v).is_ok(),
        _ => true,
    }
}

fn round_f64_to_i64(f: f64) -> Result<i64, ValueError> {
    if f.is_nan() {
        return Err(ValueError::InvalidCast("NaN cannot be cast to an integer".into()));
    }
    let r = f.round(); // half away from zero
    if r >= -9_223_372_036_854_775_808.0 && r < 9_223_372_036_854_775_808.0 {
        Ok(r as i64)
    } else {
        Err(ValueError::NumericOverflow)
    }
}

fn parse_f64_text(s: &str) -> Result<f64, ValueError> {
    let t = s.trim();
    match t.parse::<f64>() {
        Ok(f) if f.is_finite() => Ok(f),
        _ => Err(ValueError::InvalidCast(format!(
            "'{}' is not a valid DOUBLE",
            s
        ))),
    }
}

fn parse_bool_text(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "1" => Some(true),
        "false" | "f" | "0" => Some(false),
        _ => None,
    }
}

/// Apply CHAR(n)/VARCHAR(n) length rules to text.
fn fit_string(s: &str, n: usize, pad: bool) -> Result<String, ValueError> {
    let len = s.chars().count();
    if len > n {
        return Err(ValueError::StringTooLong(format!(
            "Value '{}' exceeds length {}",
            s, n
        )));
    }
    if pad && len < n {
        let mut out = String::from(s);
        out.push_str(&" ".repeat(n - len));
        Ok(out)
    } else {
        Ok(s.to_string())
    }
}

impl SqlType {
    /// SQL spelling of the type (parses back through `CAST(.. AS <name>)`).
    pub fn name(&self) -> String {
        match self {
            SqlType::Boolean => "BOOLEAN".into(),
            SqlType::Integer => "INTEGER".into(),
            SqlType::SmallInt => "SMALLINT".into(),
            SqlType::BigInt => "BIGINT".into(),
            SqlType::Decimal { precision, scale } => format!("DECIMAL({},{})", precision, scale),
            SqlType::Double => "DOUBLE".into(),
            SqlType::Text => "TEXT".into(),
            SqlType::Char(n) => format!("CHAR({})", n),
            SqlType::Varchar(n) => format!("VARCHAR({})", n),
            SqlType::Date => "DATE".into(),
            SqlType::DateTime => "DATETIME".into(),
            SqlType::Timestamp => "TIMESTAMP".into(),
            SqlType::Time => "TIME".into(),
            SqlType::Year => "YEAR".into(),
            SqlType::Null => "NULL".into(),
        }
    }

    /// The SQL type of values stored in a column of `ct`.
    pub fn from_column_type(ct: &ColumnType) -> SqlType {
        match ct {
            ColumnType::Integer | ColumnType::MediumInt { .. } => SqlType::Integer,
            ColumnType::SmallInt { .. } => SqlType::SmallInt,
            ColumnType::Text => SqlType::Text,
            ColumnType::Boolean => SqlType::Boolean,
            ColumnType::Char(n) => SqlType::Char(*n),
            ColumnType::Varchar(n) => SqlType::Varchar(*n),
            ColumnType::Decimal { precision, scale } => SqlType::Decimal {
                precision: *precision,
                scale: *scale,
            },
            ColumnType::Double { .. } => SqlType::Double,
            ColumnType::Date => SqlType::Date,
            ColumnType::DateTime => SqlType::DateTime,
            ColumnType::Timestamp => SqlType::Timestamp,
            ColumnType::Time => SqlType::Time,
            ColumnType::Year => SqlType::Year,
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            SqlType::Integer | SqlType::SmallInt | SqlType::BigInt | SqlType::Decimal { .. } | SqlType::Double
        )
    }

    pub fn is_text(&self) -> bool {
        matches!(self, SqlType::Text | SqlType::Char(_) | SqlType::Varchar(_))
    }
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// The SQL type describing this value (`SqlType::Null` for NULL).
    pub fn sql_type(&self) -> SqlType {
        match self {
            Value::Null => SqlType::Null,
            Value::Bool(_) => SqlType::Boolean,
            Value::Int(_) => SqlType::Integer,
            Value::Decimal(d) => {
                let digits = d.mantissa().unsigned_abs().to_string().len() as u8;
                let scale = d.scale();
                SqlType::Decimal {
                    precision: digits.max(scale + 1).min(crate::sql::decimal::MAX_PRECISION),
                    scale,
                }
            }
            Value::Double(_) => SqlType::Double,
            Value::Text(_) => SqlType::Text,
            Value::Char(s) => SqlType::Char(s.chars().count()),
            Value::Date(_) => SqlType::Date,
            Value::DateTime(_) => SqlType::DateTime,
            Value::Timestamp(_) => SqlType::Timestamp,
            Value::Time(_) => SqlType::Time,
            Value::Year(_) => SqlType::Year,
        }
    }

    pub fn from_column_value(cv: &ColumnValue) -> Value {
        match cv {
            ColumnValue::Null => Value::Null,
            ColumnValue::Integer(i) => Value::Int(*i as i64),
            ColumnValue::Text(s) => Value::Text(s.clone()),
            ColumnValue::Boolean(b) => Value::Bool(*b),
            ColumnValue::Char(s) => Value::Char(s.clone()),
            ColumnValue::Double(f) => Value::Double(*f),
            ColumnValue::Date(d) => Value::Date(*d),
            ColumnValue::DateTime(t) => Value::DateTime(*t),
            ColumnValue::Timestamp(t) => Value::Timestamp(*t),
            ColumnValue::Time(t) => Value::Time(*t),
            ColumnValue::Year(y) => Value::Year(*y),
            ColumnValue::Decimal(d) => Value::Decimal(*d),
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "NULL",
            Value::Bool(_) => "BOOLEAN",
            Value::Int(_) => "INTEGER",
            Value::Decimal(_) => "DECIMAL",
            Value::Double(_) => "DOUBLE",
            Value::Text(_) => "TEXT",
            Value::Char(_) => "CHAR",
            Value::Date(_) => "DATE",
            Value::DateTime(_) => "DATETIME",
            Value::Timestamp(_) => "TIMESTAMP",
            Value::Time(_) => "TIME",
            Value::Year(_) => "YEAR",
        }
    }

    pub fn compare(&self, other: &Value) -> Result<Option<Ordering>, ValueError> {
        if self.is_null() || other.is_null() {
            return Ok(None);
        }
        if is_numeric(self) && is_numeric(other) {
            if matches!(self, Value::Double(_)) || matches!(other, Value::Double(_)) {
                return Ok(cmp_f64(as_f64(self).unwrap(), as_f64(other).unwrap()));
            }
            return Ok(Some(as_decimal(self).unwrap().cmp(&as_decimal(other).unwrap())));
        }
        if let (Some(a), Some(b)) = (text_of(self), text_of(other)) {
            if matches!(self, Value::Char(_)) || matches!(other, Value::Char(_)) {
                // PAD SPACE: trailing spaces are insignificant against CHAR.
                return Ok(Some(a.trim_end_matches(' ').cmp(b.trim_end_matches(' '))));
            }
            return Ok(Some(a.cmp(b)));
        }
        if let (Value::Bool(a), Value::Bool(b)) = (self, other) {
            return Ok(Some(a.cmp(b)));
        }
        if is_temporal(self) {
            if let Some(t) = text_of(other) {
                let parsed = parse_text_like(t, self)?;
                return Ok(temporal_cmp(self, &parsed));
            }
            if let Some(o) = temporal_cmp(self, other) {
                return Ok(Some(o));
            }
        } else if is_temporal(other) {
            if let Some(t) = text_of(self) {
                let parsed = parse_text_like(t, other)?;
                return Ok(temporal_cmp(&parsed, other));
            }
        }
        Err(ValueError::TypeMismatch(format!(
            "cannot compare {} with {}",
            self.type_name(),
            other.type_name()
        )))
    }

    fn arith(&self, o: &Value, op: ArithOp) -> Result<Value, ValueError> {
        if self.is_null() || o.is_null() {
            return Ok(Value::Null);
        }
        if !is_numeric(self) || !is_numeric(o) {
            return Err(ValueError::TypeMismatch(format!(
                "operator {} is not defined for {} and {}",
                op.symbol(),
                self.type_name(),
                o.type_name()
            )));
        }
        if matches!(self, Value::Double(_)) || matches!(o, Value::Double(_)) {
            let (a, b) = (as_f64(self).unwrap(), as_f64(o).unwrap());
            if matches!(op, ArithOp::Div | ArithOp::Rem) && b == 0.0 {
                return Err(ValueError::DivisionByZero);
            }
            let r = match op {
                ArithOp::Add => a + b,
                ArithOp::Sub => a - b,
                ArithOp::Mul => a * b,
                ArithOp::Div => a / b,
                ArithOp::Rem => a % b,
            };
            if !r.is_finite() {
                return Err(ValueError::NumericOverflow);
            }
            return Ok(Value::Double(r));
        }
        if let (Value::Int(a), Value::Int(b)) = (self, o) {
            if matches!(op, ArithOp::Div | ArithOp::Rem) && *b == 0 {
                return Err(ValueError::DivisionByZero);
            }
            let r = match op {
                ArithOp::Add => a.checked_add(*b),
                ArithOp::Sub => a.checked_sub(*b),
                ArithOp::Mul => a.checked_mul(*b),
                ArithOp::Div => a.checked_div(*b),
                ArithOp::Rem => a.checked_rem(*b),
            };
            return r.map(Value::Int).ok_or(ValueError::NumericOverflow);
        }
        let (a, b) = (as_decimal(self).unwrap(), as_decimal(o).unwrap());
        let r = match op {
            ArithOp::Add => a.add(&b),
            ArithOp::Sub => a.sub(&b),
            ArithOp::Mul => a.mul(&b),
            ArithOp::Div => a.div(&b),
            ArithOp::Rem => a.rem(&b),
        };
        r.map(Value::Decimal).map_err(dec_err)
    }

    pub fn add(&self, o: &Value) -> Result<Value, ValueError> {
        self.arith(o, ArithOp::Add)
    }

    pub fn sub(&self, o: &Value) -> Result<Value, ValueError> {
        self.arith(o, ArithOp::Sub)
    }

    pub fn mul(&self, o: &Value) -> Result<Value, ValueError> {
        self.arith(o, ArithOp::Mul)
    }

    pub fn div(&self, o: &Value) -> Result<Value, ValueError> {
        self.arith(o, ArithOp::Div)
    }

    pub fn rem(&self, o: &Value) -> Result<Value, ValueError> {
        self.arith(o, ArithOp::Rem)
    }

    pub fn neg(&self) -> Result<Value, ValueError> {
        match self {
            Value::Null => Ok(Value::Null),
            Value::Int(i) => i
                .checked_neg()
                .map(Value::Int)
                .ok_or(ValueError::NumericOverflow),
            Value::Decimal(d) => Ok(Value::Decimal(d.neg())),
            Value::Double(f) => Ok(Value::Double(-f)),
            other => Err(ValueError::TypeMismatch(format!(
                "cannot negate {}",
                other.type_name()
            ))),
        }
    }

    pub fn concat(&self, o: &Value) -> Result<Value, ValueError> {
        if self.is_null() || o.is_null() {
            return Ok(Value::Null);
        }
        Ok(Value::Text(format!("{}{}", self, o)))
    }

    pub fn cast_to(&self, ty: &SqlType) -> Result<Value, ValueError> {
        if self.is_null() || *ty == SqlType::Null {
            return Ok(Value::Null);
        }
        let cast_err = |what: &str| {
            ValueError::InvalidCast(format!(
                "cannot cast {} value '{}' to {}",
                self.type_name(),
                self,
                what
            ))
        };
        match ty {
            SqlType::Null => Ok(Value::Null),
            SqlType::Text => Ok(Value::Text(self.to_string())),
            SqlType::Char(n) => Ok(Value::Char(fit_string(&self.to_string(), *n, true)?)),
            SqlType::Varchar(n) => Ok(Value::Text(fit_string(&self.to_string(), *n, false)?)),
            SqlType::Boolean => match self {
                Value::Bool(_) => Ok(self.clone()),
                Value::Int(i) => Ok(Value::Bool(*i != 0)),
                v if is_text(v) => parse_bool_text(text_of(v).unwrap())
                    .map(Value::Bool)
                    .ok_or_else(|| cast_err("BOOLEAN")),
                _ => Err(cast_err("BOOLEAN")),
            },
            SqlType::Integer | SqlType::SmallInt | SqlType::BigInt => {
                let i = match self {
                    Value::Int(i) => *i,
                    Value::Bool(b) => *b as i64,
                    Value::Decimal(d) => {
                        let r = d.round_to_scale(0).map_err(dec_err)?;
                        r.to_i64_trunc().ok_or(ValueError::NumericOverflow)?
                    }
                    Value::Double(f) => round_f64_to_i64(*f)?,
                    v if is_text(v) => text_of(v)
                        .unwrap()
                        .trim()
                        .parse::<i64>()
                        .map_err(|_| cast_err("INTEGER"))?,
                    _ => return Err(cast_err("INTEGER")),
                };
                if int_in_range(i, ty) {
                    Ok(Value::Int(i))
                } else {
                    Err(ValueError::NumericOverflow)
                }
            }
            SqlType::Decimal { precision, scale } => {
                let d = match self {
                    Value::Int(i) => Decimal::from_i64(*i),
                    Value::Decimal(d) => *d,
                    Value::Double(f) => Decimal::from_f64(*f).map_err(dec_err)?,
                    v if is_text(v) => {
                        Decimal::parse(text_of(v).unwrap()).map_err(|e| match e {
                            DecimalError::Invalid(_) => cast_err("DECIMAL"),
                            other => dec_err(other),
                        })?
                    }
                    _ => return Err(cast_err("DECIMAL")),
                };
                d.rescale(*precision, *scale)
                    .map(Value::Decimal)
                    .map_err(dec_err)
            }
            SqlType::Double => match self {
                Value::Int(_) | Value::Decimal(_) | Value::Double(_) => {
                    Ok(Value::Double(as_f64(self).unwrap()))
                }
                v if is_text(v) => parse_f64_text(text_of(v).unwrap()).map(Value::Double),
                _ => Err(cast_err("DOUBLE")),
            },
            SqlType::Date | SqlType::DateTime | SqlType::Timestamp | SqlType::Time | SqlType::Year => {
                let (like, name) = match ty {
                    SqlType::Date => (Value::Date(0), "DATE"),
                    SqlType::DateTime => (Value::DateTime(0), "DATETIME"),
                    SqlType::Timestamp => (Value::Timestamp(0), "TIMESTAMP"),
                    SqlType::Time => (Value::Time(0), "TIME"),
                    _ => (Value::Year(0), "YEAR"),
                };
                if std::mem::discriminant(self) == std::mem::discriminant(&like) {
                    return Ok(self.clone());
                }
                if let Some(t) = text_of(self) {
                    return parse_text_like(t.trim(), &like);
                }
                match (self, ty) {
                    (Value::DateTime(t), SqlType::Timestamp) => Ok(Value::Timestamp(*t)),
                    (Value::Timestamp(t), SqlType::DateTime) => Ok(Value::DateTime(*t)),
                    (Value::Date(d), SqlType::DateTime) => Ok(Value::DateTime(*d as i64 * 86_400)),
                    (Value::Date(d), SqlType::Timestamp) => Ok(Value::Timestamp(*d as i64 * 86_400)),
                    (Value::DateTime(t) | Value::Timestamp(t), SqlType::Date) => {
                        i32::try_from(t.div_euclid(86_400))
                            .map(Value::Date)
                            .map_err(|_| ValueError::NumericOverflow)
                    }
                    _ => Err(cast_err(name)),
                }
            }
        }
    }

    /// Assignment coercion used by INSERT/UPDATE.
    pub fn to_column_value(
        &self,
        col: &ColumnType,
        column_name: &str,
    ) -> Result<ColumnValue, ValueError> {
        if self.is_null() {
            return Ok(ColumnValue::Null);
        }
        let bad = |what: &str| {
            ValueError::InvalidCast(format!(
                "Value '{}' for column '{}' is not a valid {}",
                self, column_name, what
            ))
        };
        // Integral value of a numeric or numeric-text value, rejecting fractions.
        let integral = |what: &str| -> Result<i64, ValueError> {
            match self {
                Value::Int(i) => Ok(*i),
                Value::Decimal(d) => {
                    let r = d.round_to_scale(0).map_err(dec_err)?;
                    if r.num_eq(d) {
                        r.to_i64_trunc().ok_or(ValueError::NumericOverflow)
                    } else {
                        Err(bad(what))
                    }
                }
                Value::Double(f) => {
                    if f.is_finite() && f.fract() == 0.0 {
                        round_f64_to_i64(*f)
                    } else {
                        Err(bad(what))
                    }
                }
                v if is_text(v) => text_of(v)
                    .unwrap()
                    .trim()
                    .parse::<i64>()
                    .map_err(|_| bad(what)),
                _ => Err(bad(what)),
            }
        };
        let range = |v: i64, lo: i64, hi: i64| -> Result<ColumnValue, ValueError> {
            if (lo..=hi).contains(&v) {
                Ok(ColumnValue::Integer(v as i32))
            } else {
                Err(ValueError::NumericOverflow)
            }
        };
        match col {
            ColumnType::Integer => range(integral("INTEGER")?, i32::MIN as i64, i32::MAX as i64),
            ColumnType::SmallInt { unsigned, .. } => {
                let (lo, hi) = if *unsigned { (0, 65_535) } else { (-32_768, 32_767) };
                range(integral("SMALLINT")?, lo, hi)
            }
            ColumnType::MediumInt { unsigned, .. } => {
                let (lo, hi) = if *unsigned {
                    (0, 16_777_215)
                } else {
                    (-8_388_608, 8_388_607)
                };
                range(integral("MEDIUMINT")?, lo, hi)
            }
            ColumnType::Decimal { precision, scale } => {
                let d = match self {
                    Value::Int(i) => Decimal::from_i64(*i),
                    Value::Decimal(d) => *d,
                    Value::Double(f) => Decimal::from_f64(*f).map_err(dec_err)?,
                    v if is_text(v) => Decimal::parse(text_of(v).unwrap()).map_err(|e| match e {
                        DecimalError::Invalid(_) => bad("DECIMAL"),
                        other => dec_err(other),
                    })?,
                    _ => return Err(bad("DECIMAL")),
                };
                d.rescale(*precision, *scale)
                    .map(ColumnValue::Decimal)
                    .map_err(dec_err)
            }
            ColumnType::Double { unsigned, .. } => {
                let f = match self {
                    Value::Int(_) | Value::Decimal(_) | Value::Double(_) => as_f64(self).unwrap(),
                    v if is_text(v) => {
                        parse_f64_text(text_of(v).unwrap()).map_err(|_| bad("DOUBLE"))?
                    }
                    _ => return Err(bad("DOUBLE")),
                };
                if *unsigned && f < 0.0 {
                    return Err(ValueError::NumericOverflow);
                }
                Ok(ColumnValue::Double(f))
            }
            ColumnType::Text => Ok(ColumnValue::Text(self.to_string())),
            ColumnType::Char(n) => Ok(ColumnValue::Char(fit_string(&self.to_string(), *n, true)?)),
            ColumnType::Varchar(n) => {
                Ok(ColumnValue::Text(fit_string(&self.to_string(), *n, false)?))
            }
            ColumnType::Boolean => match self {
                Value::Bool(b) => Ok(ColumnValue::Boolean(*b)),
                v if is_text(v) => match text_of(v).unwrap().to_ascii_lowercase().as_str() {
                    "true" => Ok(ColumnValue::Boolean(true)),
                    "false" => Ok(ColumnValue::Boolean(false)),
                    _ => Err(bad("BOOLEAN")),
                },
                _ => Err(bad("BOOLEAN")),
            },
            ColumnType::Date => match self {
                Value::Date(d) => Ok(ColumnValue::Date(*d)),
                v if is_text(v) => parse_date(text_of(v).unwrap())
                    .map(ColumnValue::Date)
                    .ok_or_else(|| bad("DATE")),
                _ => Err(bad("DATE")),
            },
            ColumnType::DateTime => match self {
                Value::DateTime(t) | Value::Timestamp(t) => Ok(ColumnValue::DateTime(*t)),
                v if is_text(v) => parse_datetime(text_of(v).unwrap())
                    .map(ColumnValue::DateTime)
                    .ok_or_else(|| bad("DATETIME")),
                _ => Err(bad("DATETIME")),
            },
            ColumnType::Timestamp => match self {
                Value::DateTime(t) | Value::Timestamp(t) => Ok(ColumnValue::Timestamp(*t)),
                v if is_text(v) => parse_datetime(text_of(v).unwrap())
                    .map(ColumnValue::Timestamp)
                    .ok_or_else(|| bad("TIMESTAMP")),
                _ => Err(bad("TIMESTAMP")),
            },
            ColumnType::Time => match self {
                Value::Time(t) => Ok(ColumnValue::Time(*t)),
                v if is_text(v) => parse_time(text_of(v).unwrap())
                    .map(ColumnValue::Time)
                    .ok_or_else(|| bad("TIME")),
                _ => Err(bad("TIME")),
            },
            ColumnType::Year => match self {
                Value::Year(y) => Ok(ColumnValue::Year(*y)),
                Value::Int(i) => parse_year(&i.to_string())
                    .map(ColumnValue::Year)
                    .ok_or_else(|| bad("YEAR")),
                v if is_text(v) => parse_year(text_of(v).unwrap())
                    .map(ColumnValue::Year)
                    .ok_or_else(|| bad("YEAR")),
                _ => Err(bad("YEAR")),
            },
        }
    }

    pub fn truthiness(&self) -> Result<Option<bool>, ValueError> {
        match self {
            Value::Bool(b) => Ok(Some(*b)),
            Value::Null => Ok(None),
            other => Err(ValueError::TypeMismatch(format!(
                "expected BOOLEAN, found {}",
                other.type_name()
            ))),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(i) => write!(f, "{}", i),
            Value::Decimal(d) => write!(f, "{}", d),
            Value::Double(x) => write!(f, "{}", x),
            Value::Text(s) | Value::Char(s) => write!(f, "{}", s),
            Value::Date(d) => write!(f, "{}", ColumnValue::Date(*d).to_string_value()),
            Value::DateTime(t) => write!(f, "{}", ColumnValue::DateTime(*t).to_string_value()),
            Value::Timestamp(t) => write!(f, "{}", ColumnValue::Timestamp(*t).to_string_value()),
            Value::Time(t) => write!(f, "{}", ColumnValue::Time(*t).to_string_value()),
            Value::Year(y) => write!(f, "{}", ColumnValue::Year(*y).to_string_value()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Value {
        Value::Decimal(Decimal::parse(s).unwrap())
    }
    fn text(s: &str) -> Value {
        Value::Text(s.to_string())
    }

    #[test]
    fn null_propagates_through_arithmetic() {
        assert_eq!(Value::Null.add(&Value::Int(1)), Ok(Value::Null));
        assert_eq!(Value::Int(1).mul(&Value::Null), Ok(Value::Null));
        assert_eq!(Value::Null.neg(), Ok(Value::Null));
        assert_eq!(Value::Null.concat(&text("a")), Ok(Value::Null));
    }

    #[test]
    fn int_division_truncates_toward_zero() {
        assert_eq!(Value::Int(7).div(&Value::Int(2)), Ok(Value::Int(3)));
        assert_eq!(Value::Int(-7).div(&Value::Int(2)), Ok(Value::Int(-3)));
        assert_eq!(Value::Int(-7).rem(&Value::Int(2)), Ok(Value::Int(-1)));
    }

    #[test]
    fn division_by_zero_in_every_kind() {
        assert_eq!(Value::Int(1).div(&Value::Int(0)), Err(ValueError::DivisionByZero));
        assert_eq!(Value::Int(1).rem(&Value::Int(0)), Err(ValueError::DivisionByZero));
        assert_eq!(Value::Double(1.0).div(&Value::Double(0.0)), Err(ValueError::DivisionByZero));
        assert_eq!(dec("1").div(&dec("0.0")), Err(ValueError::DivisionByZero));
        assert_eq!(Value::Int(1).div(&dec("0")), Err(ValueError::DivisionByZero));
    }

    #[test]
    fn int_overflow_is_detected() {
        assert_eq!(
            Value::Int(i64::MAX).add(&Value::Int(1)),
            Err(ValueError::NumericOverflow)
        );
        assert_eq!(Value::Int(i64::MIN).neg(), Err(ValueError::NumericOverflow));
        assert_eq!(
            Value::Int(i64::MIN).div(&Value::Int(-1)),
            Err(ValueError::NumericOverflow)
        );
    }

    #[test]
    fn mixed_numeric_promotion() {
        assert_eq!(Value::Int(2).add(&dec("0.5")), Ok(dec("2.5")));
        let r = Value::Double(20.0).mul(&dec("1.05")).unwrap();
        assert!(matches!(r, Value::Double(_)));
        assert_eq!(r.to_string(), "21");
    }

    #[test]
    fn double_overflow_is_an_error() {
        assert_eq!(
            Value::Double(f64::MAX).mul(&Value::Double(2.0)),
            Err(ValueError::NumericOverflow)
        );
        assert_eq!(
            Value::Double(f64::MAX).add(&Value::Double(f64::MAX)),
            Err(ValueError::NumericOverflow)
        );
        assert_eq!(
            Value::Double(1e300).div(&Value::Double(1e-300)),
            Err(ValueError::NumericOverflow)
        );
    }

    #[test]
    fn text_is_not_implicitly_numeric() {
        assert!(matches!(
            Value::Int(1).add(&text("1")),
            Err(ValueError::TypeMismatch(_))
        ));
    }

    #[test]
    fn compare_rules() {
        assert_eq!(Value::Null.compare(&Value::Int(1)), Ok(None));
        assert_eq!(
            Value::Char("a  ".into()).compare(&text("a")),
            Ok(Some(Ordering::Equal))
        );
        assert_eq!(text("a ").compare(&text("a")), Ok(Some(Ordering::Greater)));
        assert!(matches!(
            Value::Int(1).compare(&text("1")),
            Err(ValueError::TypeMismatch(_))
        ));
        assert_eq!(
            Value::Int(1).compare(&dec("1.00")),
            Ok(Some(Ordering::Equal))
        );
        assert_eq!(
            Value::Double(0.5).compare(&dec("0.25")),
            Ok(Some(Ordering::Greater))
        );
        assert_eq!(
            Value::Bool(false).compare(&Value::Bool(true)),
            Ok(Some(Ordering::Less))
        );
    }

    #[test]
    fn compare_text_literal_with_temporal() {
        let d = Value::from_column_value(&ColumnValue::Date(
            crate::storage::row::parse_date("2024-01-02").unwrap(),
        ));
        assert_eq!(d.compare(&text("2024-01-02")), Ok(Some(Ordering::Equal)));
        assert_eq!(text("2024-01-03").compare(&d), Ok(Some(Ordering::Greater)));
        assert!(d.compare(&text("garbage")).is_err());
    }

    #[test]
    fn cast_text_to_integer() {
        assert_eq!(text("12").cast_to(&SqlType::Integer), Ok(Value::Int(12)));
        assert_eq!(text(" 12 ").cast_to(&SqlType::Integer), Ok(Value::Int(12)));
        assert!(matches!(
            text("abc").cast_to(&SqlType::Integer),
            Err(ValueError::InvalidCast(_))
        ));
        assert!(matches!(
            text("1.5").cast_to(&SqlType::Integer),
            Err(ValueError::InvalidCast(_))
        ));
    }

    #[test]
    fn cast_numeric_to_integer_rounds_half_away() {
        assert_eq!(dec("2.5").cast_to(&SqlType::Integer), Ok(Value::Int(3)));
        assert_eq!(dec("-2.5").cast_to(&SqlType::Integer), Ok(Value::Int(-3)));
        assert_eq!(Value::Double(2.4).cast_to(&SqlType::BigInt), Ok(Value::Int(2)));
        assert_eq!(
            Value::Int(40_000).cast_to(&SqlType::SmallInt),
            Err(ValueError::NumericOverflow)
        );
        assert_eq!(
            Value::Int(3_000_000_000).cast_to(&SqlType::Integer),
            Err(ValueError::NumericOverflow)
        );
    }

    #[test]
    fn cast_to_decimal_and_text() {
        let ty = SqlType::Decimal { precision: 5, scale: 2 };
        assert_eq!(text("1.005").cast_to(&ty), Ok(dec("1.01")));
        assert_eq!(Value::Int(1000).cast_to(&ty), Err(ValueError::NumericOverflow));
        assert_eq!(dec("0.30").cast_to(&SqlType::Text), Ok(text("0.30")));
        assert_eq!(text("tRuE").cast_to(&SqlType::Boolean), Ok(Value::Bool(true)));
        assert_eq!(text("0").cast_to(&SqlType::Boolean), Ok(Value::Bool(false)));
    }

    #[test]
    fn cast_to_char_and_varchar() {
        assert_eq!(
            text("ab").cast_to(&SqlType::Char(3)),
            Ok(Value::Char("ab ".into()))
        );
        assert!(matches!(
            text("abcd").cast_to(&SqlType::Varchar(3)),
            Err(ValueError::StringTooLong(_))
        ));
        assert_eq!(text("abc").cast_to(&SqlType::Varchar(3)), Ok(text("abc")));
        // length counts characters, not bytes
        assert_eq!(text("ééé").cast_to(&SqlType::Varchar(3)), Ok(text("ééé")));
    }

    #[test]
    fn cast_to_temporal() {
        assert!(matches!(
            text("2024-02-30").cast_to(&SqlType::Date),
            Err(ValueError::InvalidCast(_))
        ));
        let v = text("2024-01-02").cast_to(&SqlType::Date).unwrap();
        assert_eq!(v.to_string(), "2024-01-02");
        assert_eq!(v.cast_to(&SqlType::Text), Ok(text("2024-01-02")));
    }

    #[test]
    fn to_column_value_string_types() {
        assert!(matches!(
            text("abcd").to_column_value(&ColumnType::Varchar(3), "c"),
            Err(ValueError::StringTooLong(_))
        ));
        assert_eq!(
            text("ab").to_column_value(&ColumnType::Char(3), "c"),
            Ok(ColumnValue::Char("ab ".into()))
        );
        assert_eq!(
            Value::Int(5).to_column_value(&ColumnType::Text, "c"),
            Ok(ColumnValue::Text("5".into()))
        );
    }

    #[test]
    fn to_column_value_numeric_types() {
        let ty = ColumnType::Decimal { precision: 10, scale: 2 };
        let cv = dec("1.005").to_column_value(&ty, "c").unwrap();
        assert_eq!(cv.to_string_value(), "1.01");
        assert_eq!(
            Value::Int(3_000_000_000).to_column_value(&ColumnType::Integer, "c"),
            Err(ValueError::NumericOverflow)
        );
        assert_eq!(
            text("42").to_column_value(&ColumnType::Integer, "c"),
            Ok(ColumnValue::Integer(42))
        );
        assert_eq!(
            dec("3.00").to_column_value(&ColumnType::Integer, "c"),
            Ok(ColumnValue::Integer(3))
        );
        assert!(matches!(
            dec("3.5").to_column_value(&ColumnType::Integer, "c"),
            Err(ValueError::InvalidCast(_))
        ));
        assert_eq!(
            Value::Int(-1).to_column_value(
                &ColumnType::SmallInt { width: 0, unsigned: true },
                "c"
            ),
            Err(ValueError::NumericOverflow)
        );
        assert_eq!(
            Value::Int(2).to_column_value(
                &ColumnType::Double { precision: 10, scale: 0, unsigned: false },
                "c"
            ),
            Ok(ColumnValue::Double(2.0))
        );
        assert_eq!(Value::Null.to_column_value(&ColumnType::Integer, "c"), Ok(ColumnValue::Null));
    }

    #[test]
    fn to_column_value_temporal_and_bool_errors() {
        let err = text("nope").to_column_value(&ColumnType::Date, "d").unwrap_err();
        match err {
            ValueError::InvalidCast(m) => assert!(m.contains("not a valid DATE"), "{m}"),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(
            text("FALSE").to_column_value(&ColumnType::Boolean, "b"),
            Ok(ColumnValue::Boolean(false))
        );
    }

    #[test]
    fn truthiness_and_concat_and_display() {
        assert_eq!(Value::Bool(true).truthiness(), Ok(Some(true)));
        assert_eq!(Value::Null.truthiness(), Ok(None));
        assert!(Value::Int(1).truthiness().is_err());
        assert_eq!(text("a").concat(&Value::Int(1)), Ok(text("a1")));
        assert_eq!(Value::Null.to_string(), "NULL");
        assert_eq!(Value::Double(20.0).to_string(), "20");
        assert_eq!((Value::Double(100.0).div(&Value::Double(6.0))).unwrap().to_string(), "16.666666666666668");
        assert_eq!(Value::Bool(true).to_string(), "true");
    }
}
