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

// ------------------------------------------------------------------ fast path
//
// Same results as parse_ts, for fields inside a padded chunk buffer: it reads
// 16 bytes from the start of the field whatever its length. F1/F2 are checked
// and split with SWAR on three words; the month's first day comes from a table
// covering the dataset's years (anything else takes days_from_civil). F3 of up
// to 16 digits is converted 8 digits per multiply chain.

const LO_NIB: u64 = 0xF0F0_F0F0_F0F0_F0F0;
const ZEROS: u64 = 0x3030_3030_3030_3030;

/// Every byte under `m` is an ASCII digit.
#[inline(always)]
fn all_digits(v: u64, m: u64) -> bool {
    (v & LO_NIB & m) == (ZEROS & m) && (v.wrapping_add(0x0606_0606_0606_0606) & LO_NIB & m) == (ZEROS & m)
}

/// 8 ASCII digits, first digit in the lowest byte.
#[inline(always)]
fn parse8(v: u64) -> u64 {
    let x = v.wrapping_sub(ZEROS);
    let x = (x.wrapping_mul(10).wrapping_add(x >> 8)) & 0x00FF_00FF_00FF_00FF;
    let x = (x.wrapping_mul(100).wrapping_add(x >> 16)) & 0x0000_FFFF_0000_FFFF;
    (x.wrapping_mul(10000).wrapping_add(x >> 32)) & 0xFFFF_FFFF
}

pub struct Cal {
    y0: i64,
    /// Epoch day of the 1st of each month from January of y0.
    first: Vec<i64>,
    dim: Vec<u8>,
}

impl Cal {
    /// Table for the years around [lo_ms, hi_ms).
    pub fn new(lo_ms: i64, hi_ms: i64) -> Cal {
        let year = |ms: i64| -> i64 {
            // civil year of an epoch day (inverse of days_from_civil)
            let z = ms.div_euclid(DAY_MS) + 719468;
            let era = z.div_euclid(146097);
            let doe = z - era * 146097;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let m = if mp < 10 { mp + 3 } else { mp - 9 };
            yoe + era * 400 + (m <= 2) as i64
        };
        let y0 = (year(lo_ms) - 1).clamp(0, 9999);
        let y1 = (year(hi_ms) + 1).clamp(0, 9999);
        let (mut first, mut dim) = (Vec::new(), Vec::new());
        for y in y0..=y1 {
            let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
            for m in 1..=12i64 {
                first.push(days_from_civil(y, m, 1));
                dim.push(if m == 2 && leap { 29 } else { MDAYS[(m - 1) as usize] as u8 });
            }
        }
        Cal { y0, first, dim }
    }

    /// parse_ts of the n bytes at p. Requires 16 readable bytes at p.
    #[inline(always)]
    pub unsafe fn parse(&self, p: *const u8, n: usize) -> Option<i64> {
        if n == 20 || n == 25 {
            let c3 = *p.add(19);
            if (n == 20 && c3 == b'Z') || (n == 25 && (c3 == b'+' || c3 == b'-')) {
                return self.iso(p, n);
            }
        }
        if n == 0 || n > 16 {
            return parse_digits(std::slice::from_raw_parts(p, n));
        }
        let a = std::ptr::read_unaligned(p as *const u64);
        let b = std::ptr::read_unaligned(p.add(8) as *const u64);
        // left-pad with '0' to 16 digits: shift the n bytes to the top
        let x = (a as u128) | ((b as u128) << 64);
        let sh = 8 * (16 - n) as u32;
        let x = if sh == 0 { x } else { (x << sh) | (u128::MAX >> (128 - sh) & 0x3030_3030_3030_3030_3030_3030_3030_3030) };
        let (lo, hi) = (x as u64, (x >> 64) as u64);
        if !all_digits(lo, !0) || !all_digits(hi, !0) {
            return None;
        }
        Some((parse8(lo) * 100_000_000 + parse8(hi)) as i64)
    }

    #[inline(always)]
    unsafe fn iso(&self, p: *const u8, n: usize) -> Option<i64> {
        // a: YYYY-MM-   b: DDTHH:MM   c: :SS? (+ offset bytes 20..25)
        let a = std::ptr::read_unaligned(p as *const u64);
        let b = std::ptr::read_unaligned(p.add(8) as *const u64);
        let c = std::ptr::read_unaligned(p.add(16) as *const u32) as u64;
        const SA: u64 = 0xFF00_00FF_0000_0000;
        const SB: u64 = 0x0000_FF00_00FF_0000;
        const SC: u64 = 0x0000_00FF;
        const EA: u64 = (b'-' as u64) << 32 | (b'-' as u64) << 56;
        const EB: u64 = (b'T' as u64) << 16 | (b':' as u64) << 40;
        const EC: u64 = b':' as u64;
        if (a & SA) != EA || (b & SB) != EB || (c & SC) != EC {
            return None;
        }
        if !all_digits(a, !SA) || !all_digits(b, !SB) || !all_digits(c, 0x00FF_FF00) {
            return None;
        }
        // separators zeroed first so no byte borrows from its neighbour
        let da = (a & !SA) - (ZEROS & !SA);
        let db = (b & !SB) - (ZEROS & !SB);
        let dc = (c & 0x00FF_FF00) - 0x0030_3000;
        let byte = |v: u64, i: u32| ((v >> (8 * i)) & 0xFF) as i64;
        let y = byte(da, 0) * 1000 + byte(da, 1) * 100 + byte(da, 2) * 10 + byte(da, 3);
        let mo = byte(da, 5) * 10 + byte(da, 6);
        let d = byte(db, 0) * 10 + byte(db, 1);
        let h = byte(db, 3) * 10 + byte(db, 4);
        let mi = byte(db, 6) * 10 + byte(db, 7);
        let se = byte(dc, 1) * 10 + byte(dc, 2);
        if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 {
            return None;
        }
        let k = (y - self.y0) * 12 + mo - 1;
        let day = if (k as u64) < self.first.len() as u64 {
            if d < 1 || d > self.dim[k as usize] as i64 {
                return None;
            }
            self.first[k as usize] + d - 1
        } else {
            let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
            let dim = if mo == 2 && leap { 29 } else { MDAYS[(mo - 1) as usize] as i64 };
            if d < 1 || d > dim {
                return None;
            }
            days_from_civil(y, mo, d)
        };
        let mut ms = day * DAY_MS + ((h * 60 + mi) * 60 + se) * 1000;
        if n == 25 {
            let s = std::slice::from_raw_parts(p, 25);
            let dg = |i: usize| s[i].wrapping_sub(b'0') as i64;
            if dg(20) > 9 || dg(21) > 9 || s[22] != b':' || dg(23) > 9 || dg(24) > 9 {
                return None;
            }
            let oh = dg(20) * 10 + dg(21);
            let om = dg(23) * 10 + dg(24);
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 60 + om) * 60_000;
            if s[19] == b'+' {
                ms -= off;
            } else {
                ms += off;
            }
        }
        Some(ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_matches_slow() {
        let cal = Cal::new(1_790_000_000_000, 1_792_000_000_000);
        let mut cases: Vec<Vec<u8>> = Vec::new();
        for s in [
            &b"2026-10-03T23:58:10Z"[..], b"2026-10-03T19:58:10-04:00", b"1791071890000", b"0001791071890000",
            b"86400000", b"123456789012345678901234", b"12345678901234567890", b"9999999999999999",
            b"2026-10-03T10:00:00-00:00", b"2028-02-29T00:00:00Z", b"1970-01-01T00:00:00Z", b"0", b"7",
            b"2026-10-04T01:30:00+23:59", b"", b"2026-10-03 10:00:00Z", b"2026-10-03T10:00:00z",
            b"2026-10-03t10:00:00Z", b"2026-10-03T10:00:00.5Z", b"2026-13-01T00:00:00Z", b"2026-02-29T00:00:00Z",
            b"2026-02-30T00:00:00Z", b"2026-10-03T24:00:00Z", b"2026-10-03T10:00:60Z", b"2026-10-03T10:00:00+24:00",
            b"2026-10-03T10:00:00+05:60", b"2026-10-03T10:00:00+0200", b"1.79e12", b"-5", b"1791071890000.0",
            b"2026-10-03T10:00:00", b"2026-00-03T10:00:00Z", b"2026-10-00T10:00:00Z", b"2026-10-03T10:60:00Z",
            b"2026-10-03T10:00:00Z\r", b" 123", b"2100-02-29T00:00:00Z", b"2026-10-03T10:00:00+0a:00",
            b"2026-10-03T10:00:00 02:00", b"2026-1/-03T10:00:00Z", b"2026-10-03T10:0::00Z", b"20:6-10-03T10:00:00Z",
            b"1791071890000\r", b"17910718900:0", b"2024-02-29T23:59:59Z", b"2023-02-29T00:00:00Z",
            b"9999-12-31T23:59:59Z", b"0000-01-01T00:00:00Z", b"2026-12-31T23:59:59+12:00",
        ] {
            cases.push(s.to_vec());
        }
        let mut x: u64 = 1;
        for _ in 0..200_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let base = cases[(x % 52) as usize % cases.len()].clone();
            let mut s = base;
            if !s.is_empty() {
                let i = (x >> 8) as usize % s.len();
                s[i] = b"0123456789:-TZ+/ \x00\xff"[(x >> 20) as usize % 19];
            }
            cases.push(s);
            if cases.len() > 5000 {
                cases.truncate(60);
            }
            let s = cases.last().unwrap();
            let mut buf = s.clone();
            buf.extend_from_slice(&[b'9'; 16]);
            let got = unsafe { cal.parse(buf.as_ptr(), s.len()) };
            assert_eq!(got, parse_ts(s), "{:?}", String::from_utf8_lossy(s));
        }
    }

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
