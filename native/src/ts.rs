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

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[repr(C)]
union U {
    b: [u8; 16],
    w: [i16; 8],
    v: std::arch::x86_64::__m128i,
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
struct M(std::arch::x86_64::__m128i);
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
unsafe impl Sync for M {}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
const fn m8(b: [u8; 16]) -> M {
    M(unsafe { U { b }.v })
}
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
const fn m16(w: [i16; 8]) -> M {
    M(unsafe { U { w }.v })
}

/// Digit pairs of "-MM-DDTHH:MM:SS?" -> MM DD HH MM SS.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
static PAIRS: M = m8([1, 2, 4, 5, 7, 8, 10, 11, 13, 14, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]);
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
static TENS: M = m8([10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0, 0, 0]);
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
static SEPS: M = m8([b'-', 0, 0, b'-', 0, 0, b'T', 0, 0, b':', 0, 0, b':', 0, 0, 0]);
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
static MAXS: M = m16([12, 31, 23, 59, 59, 0, 0, 0]);
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
static MINS: M = m16([1, 1, 0, 0, 0, 0, 0, 0]);

pub struct Cal {
    y0: i64,
    /// Epoch day of the 1st of each month from January of y0.
    first: Vec<i64>,
    dim: Vec<u8>,
    /// first << 5 | dim, for parse8's gather.
    fd: Vec<i32>,
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
        let fd = first.iter().zip(&dim).map(|(&f, &d)| ((f as i32) << 5) | d as i32).collect();
        Cal { y0, first, dim, fd }
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

    /// parse (or, `trusted`, f1_trusted) of 8 fields at once: field i is the
    /// n[i] bytes at base + a[i]. A lane's result is its epoch ms, i64::MIN
    /// for None. Lanes the vector code does not settle (long digit runs,
    /// dates outside the table, trusted values of another shape) are left to
    /// the caller: their bits are set in the returned mask. Requires 32
    /// readable bytes after each field start and 3 before it.
    ///
    /// Lane-parallel over the rows: F1/F2 bytes come from gathers at fixed
    /// offsets, F3 digits from gathers ending at the field end; both forms are
    /// computed for every lane and selected with masks, so no branch depends on
    /// a row's form. 256-bit vectors only (AVX-512VL): no 512-bit frequency
    /// license.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f,avx512vl,avx512bw,avx512vbmi2,avx2")]
    #[inline]
    pub unsafe fn parse8(&self, base: *const u8, a: &[u32; 8], n: &[u32; 8], trusted: bool, out: &mut [i64; 8]) -> u8 {
        use std::arch::x86_64::*;
        let av = _mm256_loadu_si256(a.as_ptr() as *const _);
        let nv = _mm256_loadu_si256(n.as_ptr() as *const _);
        let s1 = |v: i32| _mm256_set1_epi32(v);
        let zero = _mm256_setzero_si256();
        let z = _mm256_set1_epi8(b'0' as i8);
        let ten = _mm256_set1_epi16(0x010A);
        let hund = s1(100 | (1 << 16));
        let lo16 = s1(0xFFFF);
        let gp = base as *const i32;
        macro_rules! g {
            ($m:expr, $off:expr) => {
                _mm256_mmask_i32gather_epi32::<1>(z, $m, _mm256_add_epi32(av, s1($off)), gp)
            };
        }
        // lanes with a byte under m (high bit per byte) that is not a digit
        macro_rules! nd {
            ($t:expr, $m:expr) => {
                _mm256_test_epi32_mask(_mm256_adds_epu8($t, _mm256_set1_epi8(0x76)), s1($m as i32))
            };
        }
        macro_rules! eq {
            ($x:expr, $m:expr, $v:expr) => {
                _mm256_cmpeq_epi32_mask(_mm256_and_si256($x, s1($m as i32)), s1($v as i32))
            };
        }
        // a 2-digit pair's value from the low two bytes of t
        macro_rules! p2 {
            ($t:expr) => {
                _mm256_and_si256(_mm256_maddubs_epi16($t, ten), lo16)
            };
        }
        macro_rules! ltu {
            ($x:expr, $b:expr) => {
                _mm256_cmplt_epu32_mask($x, s1($b))
            };
        }
        let all: __mmask8 = 0xFF;
        // ---- F1 / F2: "YYYY-MM-DDTHH:MM:SS" + "Z" or "+HH:MM"
        let xa = g!(all, 0);
        let xb = g!(all, 4);
        let xc = g!(all, 8);
        let xd = g!(all, 12);
        let xe = g!(all, 16);
        let (ta, tb, tc, td, te) =
            (_mm256_sub_epi8(xa, z), _mm256_sub_epi8(xb, z), _mm256_sub_epi8(xc, z), _mm256_sub_epi8(xd, z), _mm256_sub_epi8(xe, z));
        let bad = nd!(ta, 0x8080_8080u32) | nd!(tb, 0x0080_8000u32) | nd!(tc, 0x8000_8080u32) | nd!(td, 0x8080_0080u32) | nd!(te, 0x0080_8000u32);
        let seps = eq!(xb, 0xFF00_00FFu32, (b'-' as u32) | (b'-' as u32) << 24)
            & eq!(xc, 0x00FF_0000u32, (b'T' as u32) << 16)
            & eq!(xd, 0x0000_FF00u32, (b':' as u32) << 8)
            & eq!(xe, 0x0000_00FFu32, b':' as u32);
        let shape = seps & !bad;
        let c19 = _mm256_srli_epi32::<24>(xe);
        let n20 = _mm256_cmpeq_epi32_mask(nv, s1(20));
        let n25 = _mm256_cmpeq_epi32_mask(nv, s1(25));
        let is_z = n20 & _mm256_cmpeq_epi32_mask(c19, s1(b'Z' as i32));
        let plus = _mm256_cmpeq_epi32_mask(c19, s1(b'+' as i32));
        let is_pm = n25 & (plus | _mm256_cmpeq_epi32_mask(c19, s1(b'-' as i32)));
        let y = _mm256_madd_epi16(_mm256_maddubs_epi16(ta, ten), hund);
        let mo = p2!(_mm256_srli_epi32::<8>(tb));
        let d = p2!(tc);
        let h = p2!(_mm256_shrdi_epi32::<24>(tc, td));
        let mi = p2!(_mm256_srli_epi32::<16>(td));
        let se = p2!(_mm256_srli_epi32::<8>(te));
        let k = _mm256_add_epi32(_mm256_mullo_epi32(_mm256_sub_epi32(y, s1(self.y0 as i32)), s1(12)), _mm256_sub_epi32(mo, s1(1)));
        let kin = ltu!(k, self.fd.len() as i32);
        let tab = _mm256_mmask_i32gather_epi32::<4>(zero, kin, k, self.fd.as_ptr());
        let first = _mm256_srai_epi32::<5>(tab);
        let dim = _mm256_and_si256(tab, s1(31));
        let mut secs = _mm256_add_epi32(_mm256_mullo_epi32(_mm256_add_epi32(_mm256_mullo_epi32(h, s1(60)), mi), s1(60)), se);
        let day = _mm256_add_epi32(first, _mm256_sub_epi32(d, s1(1)));
        let iso_ok;
        let slow;
        if trusted {
            iso_ok = n20 & shape & kin;
            slow = !iso_ok;
        } else {
            let rng = ltu!(_mm256_sub_epi32(mo, s1(1)), 12) & ltu!(h, 24) & ltu!(mi, 60) & ltu!(se, 60) & ltu!(_mm256_sub_epi32(d, s1(1)), 31);
            let mut ok = (is_z | is_pm) & shape & rng;
            // F2 offset "+HH:MM": bytes 20..24 and 21..25
            let xf = g!(is_pm, 20);
            let xg = g!(is_pm, 21);
            let (tf, tg) = (_mm256_sub_epi8(xf, z), _mm256_sub_epi8(xg, z));
            let oh = p2!(tf);
            let om = p2!(_mm256_srli_epi32::<16>(tg));
            let off_ok = !(nd!(tf, 0x8000_8080u32) | nd!(tg, 0x8000_0000u32)) & eq!(xf, 0x00FF_0000u32, (b':' as u32) << 16) & ltu!(oh, 24) & ltu!(om, 60);
            ok &= is_z | off_ok;
            let off = _mm256_mullo_epi32(_mm256_add_epi32(_mm256_mullo_epi32(oh, s1(60)), om), s1(60));
            // '+' subtracts the offset, '-' adds it
            secs = _mm256_mask_sub_epi32(secs, is_pm & plus, secs, off);
            secs = _mm256_mask_add_epi32(secs, is_pm & !plus, secs, off);
            let dok = _mm256_cmple_epu32_mask(d, dim);
            slow = (ok & !kin) | (_mm256_cmpgt_epu32_mask(nv, s1(16)) & !(is_z | is_pm));
            iso_ok = ok & kin & dok;
        }
        // ---- F3: up to 16 digits, read as four 4-byte groups ending at the
        // field end; bytes before the field start count as '0'
        let mut q = [zero; 4];
        let mut dok: __mmask8 = 0;
        if !trusted {
            let dig = ltu!(_mm256_sub_epi32(nv, s1(1)), 16);
            let mut dbad: __mmask8 = 0;
            for (j, qj) in q.iter_mut().enumerate() {
                let j4 = 4 * j as i32;
                let m = dig & _mm256_cmpgt_epu32_mask(nv, s1(j4));
                let x = _mm256_mmask_i32gather_epi32::<1>(z, m, _mm256_add_epi32(av, _mm256_sub_epi32(nv, s1(j4 + 4))), gp);
                let sh = _mm256_slli_epi32::<3>(_mm256_max_epi32(_mm256_sub_epi32(s1(j4 + 4), nv), zero));
                let keep = _mm256_sllv_epi32(s1(-1), sh);
                // keep ? x : '0'
                let x = _mm256_ternarylogic_epi32::<0xCA>(keep, x, z);
                let t = _mm256_sub_epi8(x, z);
                dbad |= nd!(t, 0x8080_8080u32);
                *qj = _mm256_madd_epi16(_mm256_maddubs_epi16(t, ten), hund);
            }
            dok = dig & !dbad;
        }
        let hi8 = _mm256_add_epi32(_mm256_mullo_epi32(q[3], s1(10_000)), q[2]);
        let lo8 = _mm256_add_epi32(_mm256_mullo_epi32(q[1], s1(10_000)), q[0]);
        // ---- to i64, 4 lanes per half
        let none = _mm256_set1_epi64x(i64::MIN);
        for half in 0..2 {
            let pick = |v: __m256i| if half == 0 { _mm256_castsi256_si128(v) } else { _mm256_extracti128_si256::<1>(v) };
            let ms = _mm256_add_epi64(
                _mm256_mul_epi32(_mm256_cvtepi32_epi64(pick(day)), _mm256_set1_epi64x(DAY_MS)),
                _mm256_mul_epi32(_mm256_cvtepi32_epi64(pick(secs)), _mm256_set1_epi64x(1000)),
            );
            let num = _mm256_add_epi64(_mm256_mul_epu32(_mm256_cvtepu32_epi64(pick(hi8)), _mm256_set1_epi64x(100_000_000)), _mm256_cvtepu32_epi64(pick(lo8)));
            let sh4 = 4 * half as u32;
            let r = _mm256_mask_mov_epi64(_mm256_mask_mov_epi64(none, (dok >> sh4) & 0xF, num), (iso_ok >> sh4) & 0xF, ms);
            _mm256_storeu_si256(out.as_mut_ptr().add(4 * half) as *mut _, r);
        }
        slow
    }

    /// Epoch day of year y, month mo (1..=12), day d; None for an impossible date.
    #[inline(always)]
    fn day_of(&self, y: i64, mo: i64, d: i64) -> Option<i64> {
        let k = (y - self.y0) * 12 + mo - 1;
        if (k as u64) < self.first.len() as u64 {
            if d < 1 || d > self.dim[k as usize] as i64 {
                return None;
            }
            Some(self.first[k as usize] + d - 1)
        } else {
            let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
            let dim = if mo == 2 && leap { 29 } else { MDAYS[(mo - 1) as usize] as i64 };
            if d < 1 || d > dim {
                return None;
            }
            Some(days_from_civil(y, mo, d))
        }
    }

    /// An F1 value the rules guarantee valid (ingest_ts): no checks. Any other
    /// length takes the full parser.
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[inline(always)]
    pub unsafe fn f1_trusted(&self, p: *const u8, n: usize) -> Option<i64> {
        use std::arch::x86_64::*;
        if n != 20 {
            return self.parse(p, n);
        }
        let v = _mm_loadu_si128(p.add(4) as *const __m128i);
        let t = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let vals = _mm_maddubs_epi16(_mm_shuffle_epi8(t, PAIRS.0), TENS.0);
        let lo = _mm_cvtsi128_si64(vals) as u64;
        let se = _mm_extract_epi16::<4>(vals) as i64;
        let y4 = std::ptr::read_unaligned(p as *const u32).wrapping_sub(0x3030_3030);
        let y = ((y4 & 0xFF) * 1000 + ((y4 >> 8) & 0xFF) * 100 + ((y4 >> 16) & 0xFF) * 10 + (y4 >> 24)) as i64;
        let (mo, d, h, mi) = ((lo & 0xFFFF) as i64, ((lo >> 16) & 0xFFFF) as i64, ((lo >> 32) & 0xFFFF) as i64, (lo >> 48) as i64);
        let k = (y - self.y0) * 12 + mo - 1;
        if (k as u64) >= self.first.len() as u64 {
            return self.parse(p, n);
        }
        Some((self.first[k as usize] + d - 1) * DAY_MS + ((h * 60 + mi) * 60 + se) * 1000)
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    #[inline(always)]
    pub unsafe fn f1_trusted(&self, p: *const u8, n: usize) -> Option<i64> {
        self.parse(p, n)
    }

    /// F1/F2 (n == 20 or 25, byte 19 already checked): SSE checks of the
    /// separators and digits of bytes 4..19, digit pairs combined by maddubs.
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[inline(always)]
    unsafe fn iso(&self, p: *const u8, n: usize) -> Option<i64> {
        use std::arch::x86_64::*;
        // v = "-MM-DDTHH:MM:SS?" (bytes 4..20)
        let v = _mm_loadu_si128(p.add(4) as *const __m128i);
        let t = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let dig = _mm_cmpeq_epi8(_mm_min_epu8(t, _mm_set1_epi8(9)), t);
        let sep = _mm_cmpeq_epi8(v, SEPS.0);
        let m = (_mm_movemask_epi8(dig) as u32 & 0x6DB6) | (_mm_movemask_epi8(sep) as u32 & 0x1249);
        let y4 = std::ptr::read_unaligned(p as *const u32);
        let yd = y4.wrapping_sub(0x3030_3030);
        let ydig = (y4 & 0xF0F0_F0F0) == 0x3030_3030 && (y4.wrapping_add(0x0606_0606) & 0xF0F0_F0F0) == 0x3030_3030;
        if m != 0x7FFF || !ydig {
            return None;
        }
        let vals = _mm_maddubs_epi16(_mm_shuffle_epi8(t, PAIRS.0), TENS.0);
        let bad = _mm_or_si128(_mm_cmpgt_epi16(vals, MAXS.0), _mm_cmpgt_epi16(MINS.0, vals));
        if _mm_movemask_epi8(bad) != 0 {
            return None;
        }
        let lo = _mm_cvtsi128_si64(vals) as u64;
        let se = _mm_extract_epi16::<4>(vals) as i64;
        let y = ((yd & 0xFF) * 1000 + ((yd >> 8) & 0xFF) * 100 + ((yd >> 16) & 0xFF) * 10 + (yd >> 24)) as i64;
        let (mo, d, h, mi) = ((lo & 0xFFFF) as i64, ((lo >> 16) & 0xFFFF) as i64, ((lo >> 32) & 0xFFFF) as i64, (lo >> 48) as i64);
        let day = self.day_of(y, mo, d)?;
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

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
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
