//! Exact fixed-point decimal arithmetic backed by an i128 mantissa.

use std::cmp::Ordering;
use std::fmt;

/// value = mantissa / 10^scale, with |mantissa| < 10^38 and scale <= 38.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal {
    mantissa: i128,
    scale: u8,
}

pub const MAX_PRECISION: u8 = 38;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecimalError {
    Overflow,
    DivisionByZero,
    Invalid(String),
}

const POW10: [i128; 39] = {
    let mut t = [1i128; 39];
    let mut i = 1;
    while i < 39 {
        t[i] = t[i - 1] * 10;
        i += 1;
    }
    t
};

fn pow10(n: u32) -> i128 {
    POW10[n as usize]
}

/// Divide by 10^n rounding half away from zero.
fn div_pow10_round(m: i128, n: u32) -> i128 {
    if n == 0 {
        return m;
    }
    let p = pow10(n);
    let q = m / p;
    let r = m % p;
    if r.unsigned_abs() * 2 >= p as u128 {
        if m < 0 { q - 1 } else { q + 1 }
    } else {
        q
    }
}

fn in_range(m: i128) -> bool {
    m.unsigned_abs() < POW10[38] as u128
}

impl Decimal {
    pub fn new(mantissa: i128, scale: u8) -> Result<Self, DecimalError> {
        if scale > MAX_PRECISION {
            return Err(DecimalError::Invalid(format!("scale {} exceeds 38", scale)));
        }
        if !in_range(mantissa) {
            return Err(DecimalError::Overflow);
        }
        Ok(Decimal { mantissa, scale })
    }

    pub fn from_i64(v: i64) -> Self {
        Decimal {
            mantissa: v as i128,
            scale: 0,
        }
    }

    /// Parses `[+-]digits[.digits]`; exponents are not accepted.
    pub fn parse(s: &str) -> Result<Self, DecimalError> {
        Self::parse_inner(s, false)
    }

    fn parse_inner(s: &str, round_excess: bool) -> Result<Self, DecimalError> {
        let invalid = || DecimalError::Invalid(format!("'{}' is not a valid decimal", s));
        let t = s.trim();
        let (neg, body) = match t.as_bytes().first() {
            Some(b'-') => (true, &t[1..]),
            Some(b'+') => (false, &t[1..]),
            _ => (false, t),
        };
        let (int_part, frac_part) = match body.split_once('.') {
            Some((i, f)) => (i, f),
            None => (body, ""),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return Err(invalid());
        }
        if !int_part.bytes().all(|b| b.is_ascii_digit())
            || !frac_part.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(invalid());
        }
        let mut frac = frac_part;
        let mut round_up = false;
        if frac.len() > MAX_PRECISION as usize {
            if !round_excess {
                return Err(DecimalError::Overflow);
            }
            round_up = frac.as_bytes()[MAX_PRECISION as usize] >= b'5';
            frac = &frac[..MAX_PRECISION as usize];
        }
        let mut m: i128 = 0;
        for b in int_part.bytes().chain(frac.bytes()) {
            m = m
                .checked_mul(10)
                .and_then(|m| m.checked_add((b - b'0') as i128))
                .ok_or(DecimalError::Overflow)?;
        }
        if round_up {
            m += 1;
        }
        if !in_range(m) {
            return Err(DecimalError::Overflow);
        }
        Ok(Decimal {
            mantissa: if neg { -m } else { m },
            scale: frac.len() as u8,
        })
    }

    pub fn mantissa(&self) -> i128 {
        self.mantissa
    }

    pub fn scale(&self) -> u8 {
        self.scale
    }

    /// Mantissas of both operands expressed at the larger scale.
    fn align(&self, o: &Self) -> Result<(i128, i128, u8), DecimalError> {
        let scale = self.scale.max(o.scale);
        let a = self
            .mantissa
            .checked_mul(pow10((scale - self.scale) as u32))
            .ok_or(DecimalError::Overflow)?;
        let b = o
            .mantissa
            .checked_mul(pow10((scale - o.scale) as u32))
            .ok_or(DecimalError::Overflow)?;
        Ok((a, b, scale))
    }

    pub fn add(&self, o: &Self) -> Result<Self, DecimalError> {
        let (a, b, scale) = self.align(o)?;
        Self::new(a.checked_add(b).ok_or(DecimalError::Overflow)?, scale)
    }

    pub fn sub(&self, o: &Self) -> Result<Self, DecimalError> {
        let (a, b, scale) = self.align(o)?;
        Self::new(a.checked_sub(b).ok_or(DecimalError::Overflow)?, scale)
    }

    pub fn mul(&self, o: &Self) -> Result<Self, DecimalError> {
        let m = self
            .mantissa
            .checked_mul(o.mantissa)
            .ok_or(DecimalError::Overflow)?;
        let scale = self.scale as u32 + o.scale as u32;
        if scale > MAX_PRECISION as u32 {
            Self::new(
                div_pow10_round(m, scale - MAX_PRECISION as u32),
                MAX_PRECISION,
            )
        } else {
            Self::new(m, scale as u8)
        }
    }

    pub fn div(&self, o: &Self) -> Result<Self, DecimalError> {
        if o.mantissa == 0 {
            return Err(DecimalError::DivisionByZero);
        }
        let rs = (self.scale as u32 + 6).max(o.scale as u32).max(6).min(38);
        // quotient mantissa at scale rs = m1 * 10^(rs + s2 - s1) / m2
        let exp = rs + o.scale as u32 - self.scale as u32;
        let a = self.mantissa.unsigned_abs();
        let d = o.mantissa.unsigned_abs();
        // Long division one decimal digit at a time; remainders stay below
        // d < 2^127 so every intermediate fits in u128.
        let mut q = a / d;
        let mut r = a % d;
        let limit = POW10[38] as u128;
        for _ in 0..exp {
            let mut t = 0u128;
            let mut digit = 0u128;
            for _ in 0..10 {
                t += r;
                if t >= d {
                    t -= d;
                    digit += 1;
                }
            }
            r = t;
            q = q
                .checked_mul(10)
                .and_then(|q| q.checked_add(digit))
                .ok_or(DecimalError::Overflow)?;
            if q >= limit {
                return Err(DecimalError::Overflow);
            }
        }
        if r * 2 >= d {
            q += 1;
        }
        if q >= limit {
            return Err(DecimalError::Overflow);
        }
        let neg = (self.mantissa < 0) != (o.mantissa < 0);
        let m = if neg { -(q as i128) } else { q as i128 };
        Self::new(m, rs as u8)
    }

    /// Remainder with the sign of the dividend, at the larger operand scale.
    pub fn rem(&self, o: &Self) -> Result<Self, DecimalError> {
        if o.mantissa == 0 {
            return Err(DecimalError::DivisionByZero);
        }
        let (a, b, scale) = self.align(o)?;
        Self::new(a % b, scale)
    }

    pub fn neg(&self) -> Self {
        Decimal {
            mantissa: -self.mantissa,
            scale: self.scale,
        }
    }

    /// Round half away from zero to `scale`, then require the result to fit
    /// in `precision` total digits.
    pub fn rescale(&self, precision: u8, scale: u8) -> Result<Self, DecimalError> {
        if precision == 0 || precision > MAX_PRECISION || scale > precision {
            return Err(DecimalError::Invalid(format!(
                "invalid DECIMAL({},{})",
                precision, scale
            )));
        }
        let r = self.round_to_scale(scale)?;
        if r.mantissa.unsigned_abs() >= pow10(precision as u32) as u128 {
            return Err(DecimalError::Overflow);
        }
        Ok(r)
    }

    pub fn round_to_scale(&self, scale: u8) -> Result<Self, DecimalError> {
        if scale > MAX_PRECISION {
            return Err(DecimalError::Invalid(format!("scale {} exceeds 38", scale)));
        }
        if scale >= self.scale {
            let m = self
                .mantissa
                .checked_mul(pow10((scale - self.scale) as u32))
                .ok_or(DecimalError::Overflow)?;
            Self::new(m, scale)
        } else {
            Self::new(
                div_pow10_round(self.mantissa, (self.scale - scale) as u32),
                scale,
            )
        }
    }

    /// Strip trailing fractional zeros (1.500 -> 1.5).
    pub fn normalize(&self) -> Self {
        let mut m = self.mantissa;
        let mut s = self.scale;
        while s > 0 && m % 10 == 0 {
            m /= 10;
            s -= 1;
        }
        Decimal {
            mantissa: m,
            scale: s,
        }
    }

    pub fn to_f64(&self) -> f64 {
        self.to_string().parse::<f64>().unwrap_or(f64::NAN)
    }

    pub fn to_i64_trunc(&self) -> Option<i64> {
        i64::try_from(self.mantissa / pow10(self.scale as u32)).ok()
    }

    pub fn is_zero(&self) -> bool {
        self.mantissa == 0
    }

    /// Converts via the shortest round-trip decimal representation.
    pub fn from_f64(v: f64) -> Result<Self, DecimalError> {
        if !v.is_finite() {
            return Err(DecimalError::Invalid(format!("{} is not a finite number", v)));
        }
        Self::parse_inner(&format!("{}", v), true)
    }

    /// Numeric equality (1.0 equals 1.00), unlike the structural `==`.
    pub fn num_eq(&self, o: &Self) -> bool {
        self.cmp(o) == Ordering::Equal
    }
}

impl Ord for Decimal {
    fn cmp(&self, o: &Self) -> Ordering {
        let (sa, sb) = (self.mantissa.signum(), o.mantissa.signum());
        if sa != sb {
            return sa.cmp(&sb);
        }
        if sa == 0 {
            return Ordering::Equal;
        }
        match self.align(o) {
            Ok((a, b, _)) => a.cmp(&b),
            Err(_) => {
                // The operand with the smaller scale grew past i128, so it has
                // the larger magnitude.
                let self_bigger = self.scale < o.scale;
                if self_bigger == (sa > 0) {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
        }
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let digits = self.mantissa.unsigned_abs().to_string();
        let sign = if self.mantissa < 0 { "-" } else { "" };
        let scale = self.scale as usize;
        if scale == 0 {
            return write!(f, "{}{}", sign, digits);
        }
        let padded = if digits.len() <= scale {
            format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
        } else {
            digits
        };
        let (int_part, frac_part) = padded.split_at(padded.len() - scale);
        write!(f, "{}{}.{}", sign, int_part, frac_part)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        Decimal::parse(s).unwrap()
    }

    #[test]
    fn add_keeps_max_scale() {
        assert_eq!(d("0.10").add(&d("0.20")).unwrap().to_string(), "0.30");
        assert_eq!(d("1").add(&d("0.005")).unwrap().to_string(), "1.005");
    }

    #[test]
    fn sub_and_neg() {
        assert_eq!(d("1.5").sub(&d("2.25")).unwrap().to_string(), "-0.75");
        assert_eq!(d("0.5").neg().to_string(), "-0.5");
    }

    #[test]
    fn mul_adds_scales_and_rounds_past_38() {
        assert_eq!(d("1.5").mul(&d("0.25")).unwrap().to_string(), "0.375");
        let tiny = Decimal::new(5, 38).unwrap();
        let r = tiny.mul(&d("0.5")).unwrap();
        assert_eq!(r.scale(), 38);
        assert_eq!(r.mantissa(), 3); // 2.5 rounds away from zero
    }

    #[test]
    fn mul_of_two_38_digit_values_overflows() {
        let big = d("99999999999999999999999999999999999999");
        assert_eq!(big.mul(&big), Err(DecimalError::Overflow));
    }

    #[test]
    fn add_overflow_past_38_digits() {
        let big = d("99999999999999999999999999999999999999");
        assert_eq!(big.add(&d("1")), Err(DecimalError::Overflow));
    }

    #[test]
    fn rescale_rounds_half_away_from_zero() {
        assert_eq!(d("1.005").rescale(10, 2).unwrap().to_string(), "1.01");
        assert_eq!(d("-1.005").rescale(10, 2).unwrap().to_string(), "-1.01");
        assert_eq!(d("1.004").rescale(10, 2).unwrap().to_string(), "1.00");
    }

    #[test]
    fn rescale_checks_precision() {
        assert_eq!(d("999.995").rescale(5, 2), Err(DecimalError::Overflow));
        assert_eq!(d("123.45").rescale(5, 2).unwrap().to_string(), "123.45");
        assert_eq!(d("5").rescale(5, 2).unwrap().to_string(), "5.00");
    }

    #[test]
    fn div_default_scale_and_rounding() {
        assert_eq!(d("1").div(&d("3")).unwrap().to_string(), "0.333333");
        assert_eq!(d("2").div(&d("3")).unwrap().to_string(), "0.666667");
        assert_eq!(d("-2").div(&d("3")).unwrap().to_string(), "-0.666667");
        assert_eq!(d("10.00").div(&d("4")).unwrap().to_string(), "2.50000000");
    }

    #[test]
    fn div_by_zero() {
        assert_eq!(d("1").div(&d("0.00")), Err(DecimalError::DivisionByZero));
    }

    #[test]
    fn rem_follows_dividend_sign() {
        assert_eq!(d("7.5").rem(&d("2")).unwrap().to_string(), "1.5");
        assert_eq!(d("-7.5").rem(&d("2")).unwrap().to_string(), "-1.5");
        assert_eq!(d("1").rem(&d("0")), Err(DecimalError::DivisionByZero));
    }

    #[test]
    fn ordering_ignores_scale() {
        assert_eq!(d("1.0").cmp(&d("1.00")), Ordering::Equal);
        assert!(d("1.0").num_eq(&d("1.00")));
        assert_ne!(d("1.0"), d("1.00"));
        assert_eq!(d("-2").cmp(&d("1")), Ordering::Less);
        assert_eq!(d("0.5").cmp(&d("0.49")), Ordering::Greater);
        let big = d("99999999999999999999999999999999999999");
        assert_eq!(big.cmp(&d("0.00000000000000000001")), Ordering::Greater);
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!(matches!(Decimal::parse("abc"), Err(DecimalError::Invalid(_))));
        assert!(matches!(Decimal::parse("1e3"), Err(DecimalError::Invalid(_))));
        assert!(matches!(Decimal::parse(""), Err(DecimalError::Invalid(_))));
        assert!(matches!(Decimal::parse("-"), Err(DecimalError::Invalid(_))));
        assert!(matches!(Decimal::parse("1.2.3"), Err(DecimalError::Invalid(_))));
    }

    #[test]
    fn parse_accepts_signs_and_fractions() {
        assert_eq!(d("+1.50").to_string(), "1.50");
        assert_eq!(d("-0.05").to_string(), "-0.05");
        assert_eq!(d("007").to_string(), "7");
    }

    #[test]
    fn display_formats() {
        assert_eq!(Decimal::new(-5, 1).unwrap().to_string(), "-0.5");
        assert_eq!(Decimal::new(30, 2).unwrap().to_string(), "0.30");
        assert_eq!(Decimal::new(7, 0).unwrap().to_string(), "7");
        assert_eq!(Decimal::new(5, 3).unwrap().to_string(), "0.005");
    }

    #[test]
    fn conversions() {
        assert_eq!(d("2.5").to_f64(), 2.5);
        assert_eq!(d("-2.9").to_i64_trunc(), Some(-2));
        assert_eq!(Decimal::from_i64(i64::MIN).to_string(), "-9223372036854775808");
        assert_eq!(Decimal::from_f64(0.1).unwrap().to_string(), "0.1");
        assert_eq!(Decimal::from_f64(21.0).unwrap().to_string(), "21");
        assert!(matches!(Decimal::from_f64(f64::NAN), Err(DecimalError::Invalid(_))));
        assert!(Decimal::from_f64(f64::INFINITY).is_err());
        assert!(d("0.00").is_zero());
    }

    #[test]
    fn normalize_strips_trailing_zeros() {
        assert_eq!(d("1.500").normalize().to_string(), "1.5");
        assert_eq!(d("100").normalize(), d("100"));
    }
}
