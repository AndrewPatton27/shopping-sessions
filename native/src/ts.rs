//! Timestamp parsing (rules.md "Timestamps"): F1 `YYYY-MM-DDTHH:MM:SSZ`,
//! F2 with a `+HH:MM` / `-HH:MM` offset, F3 ASCII digits (epoch ms).

pub const DAY_MS: i64 = 86_400_000;

#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

const MDAYS: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/// Epoch milliseconds, or None for a bad timestamp form. An all-digit value too
/// long for i64 saturates to i64::MAX (never inside any window).
#[inline]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if (n == 20 && s[19] == b'Z') || (n == 25 && (s[19] == b'+' || s[19] == b'-')) {
        return parse_iso(s);
    }
    parse_digits(s)
}

#[inline]
fn parse_digits(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    let mut over = false;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        // saturating accumulate
        match v.checked_mul(10).and_then(|x| x.checked_add(d as u64)) {
            Some(x) => v = x,
            None => over = true,
        }
    }
    if over || v > i64::MAX as u64 {
        Some(i64::MAX)
    } else {
        Some(v as i64)
    }
}

#[inline]
fn parse_iso(s: &[u8]) -> Option<i64> {
    let d = |i: usize| s[i].wrapping_sub(b'0') as u32;
    let digs = [0usize, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18];
    let mut bad = false;
    for &i in &digs {
        bad |= d(i) > 9;
    }
    bad |= s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':';
    if bad {
        return None;
    }
    let y = d(0) * 1000 + d(1) * 100 + d(2) * 10 + d(3);
    let mo = d(5) * 10 + d(6);
    let da = d(8) * 10 + d(9);
    let h = d(11) * 10 + d(12);
    let mi = d(14) * 10 + d(15);
    let se = d(17) * 10 + d(18);
    if mo < 1 || mo > 12 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let dim = if mo == 2 && leap { 29 } else { MDAYS[(mo - 1) as usize] };
    if da < 1 || da > dim {
        return None;
    }
    let days = days_from_civil(y as i64, mo as i64, da as i64);
    let mut ms = ((days * 24 + h as i64) * 60 + mi as i64) * 60_000 + se as i64 * 1000;
    if s.len() == 25 {
        if d(20) > 9 || d(21) > 9 || s[22] != b':' || d(23) > 9 || d(24) > 9 {
            return None;
        }
        let oh = d(20) * 10 + d(21);
        let om = d(23) * 10 + d(24);
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh as i64 * 60 + om as i64) * 60_000;
        if s[19] == b'+' {
            ms -= off;
        } else {
            ms += off;
        }
    }
    Some(ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn forms() {
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), Some(1791071890000));
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(1791071890000));
        assert_eq!(parse_ts(b"1791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b"0001791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b"86400000"), Some(86400000));
        assert_eq!(parse_ts(b"123456789012345678901234"), Some(i64::MAX));
        assert_eq!(parse_ts(b"12345678901234567890"), Some(i64::MAX));
        assert_eq!(parse_ts(b"1234567890123456789012345"), Some(i64::MAX));
        assert_eq!(parse_ts(b"2026-10-03T10:00:00-00:00"), parse_ts(b"2026-10-03T10:00:00Z"));
        assert_eq!(parse_ts(b"2028-02-29T00:00:00Z"), Some(1835395200000));
        assert_eq!(parse_ts(b"1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_ts(b"2026-10-04T01:30:00+23:59").is_some(), true);
        for bad in [
            &b""[..], b"2026-10-03 10:00:00Z", b"2026-10-03T10:00:00z", b"2026-10-03t10:00:00Z",
            b"2026-10-03T10:00:00.5Z", b"2026-13-01T00:00:00Z", b"2026-02-29T00:00:00Z",
            b"2026-02-30T00:00:00Z", b"2026-10-03T24:00:00Z", b"2026-10-03T10:00:60Z",
            b"2026-10-03T10:00:00+24:00", b"2026-10-03T10:00:00+05:60", b"2026-10-03T10:00:00+0200",
            b"1.79e12", b"-5", b"1791071890000.0", b"2026-10-03T10:00:00", b"2026-00-03T10:00:00Z",
            b"2026-10-00T10:00:00Z", b"2026-10-03T10:60:00Z", b"2026-10-03T10:00:00Z\r", b" 123", b"2100-02-29T00:00:00Z",
            b"2026-10-03T10:00:00+0a:00", b"2026-10-03T10:00:00 02:00",
        ] {
            assert_eq!(parse_ts(bad), None, "{:?}", std::str::from_utf8(bad));
        }
    }
}
