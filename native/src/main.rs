//! Shopping Sessions: native pipeline. See README.md for the design.
//!
//! Stages (each one parallel over NCPU threads):
//!   A  parse. Every event row becomes one 32 B record in the partition of
//!      hash(event_id), every valid click one 24 B record in the partition of
//!      hash(click_id). No table lookups. Thread 0 first builds the link table.
//!   B  per id partition: dedup in a cache-sized table, then map each survivor
//!      to its person (link table) and write a 16 B / 24 B record to the
//!      partition of hash(person)
//!   C  per person partition: group by person, sort, sessionize, attribute,
//!      aggregate; write Parquet (one user_daily file per thread)
//!
//! IDs are 64-bit keys: IDs of 1..=9 bytes in 0x01..=0x7F are packed 7 bits
//! per byte, big-endian, so integer order is byte-wise string order; any other
//! ID is interned (exact). Datasets the compact records cannot hold (a window
//! over ~49 days, more than 256 categories) run the previous pipeline.

mod halloc;
mod legacy;
mod pq;
mod ts;

#[global_allocator]
static GA: halloc::Alloc = halloc::Alloc;

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AO};
use std::sync::{LazyLock, Mutex, OnceLock};
use std::time::Instant;

use ts::{parse_ts, Cal, DAY_MS};

const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const CHUNK: u64 = 1 << 20;
const DEAD: u32 = u32::MAX;
/// user_daily rows per row group: the rows buffered per thread before they
/// are written (a small reused buffer, not the whole output).
const RG_ROWS: usize = 1 << 16;
const NONE: u32 = u32::MAX;
const SLOW: u64 = 1 << 63;

// ------------------------------------------------------------------ keys

struct Interner {
    map: HashMap<Box<[u8]>, u32>,
    strs: Vec<Box<[u8]>>,
}

static INTERN: LazyLock<Mutex<Interner>> =
    LazyLock::new(|| Mutex::new(Interner { map: HashMap::new(), strs: Vec::new() }));

thread_local! {
    static LCACHE: RefCell<HashMap<Box<[u8]>, u64>> = RefCell::new(HashMap::new());
}

#[cold]
#[inline(never)]
fn intern(s: &[u8]) -> u64 {
    LCACHE.with(|c| {
        let mut c = c.borrow_mut();
        if let Some(&k) = c.get(s) {
            return k;
        }
        let i = {
            let mut g = INTERN.lock().unwrap();
            match g.map.get(s) {
                Some(&i) => i,
                None => {
                    let i = g.strs.len() as u32;
                    g.strs.push(s.into());
                    g.map.insert(s.into(), i);
                    i
                }
            }
        };
        let k = SLOW | i as u64;
        c.insert(s.into(), k);
        k
    })
}

#[cold]
#[inline(never)]
fn find_interned(s: &[u8]) -> Option<u64> {
    INTERN.lock().unwrap().map.get(s).map(|&i| SLOW | i as u64)
}

/// Packed key of an ID of 1..=9 bytes, each in 0x01..=0x7F. Reads 8 bytes at p.
#[inline(always)]
unsafe fn pack7(p: *const u8, len: usize) -> Option<u64> {
    if len.wrapping_sub(1) >= 9 {
        return None;
    }
    let v = u64::from_be_bytes(std::ptr::read_unaligned(p as *const [u8; 8]));
    let m = !0u64 << (64 - 8 * len.min(8));
    let v = v & m;
    let b8 = if len == 9 { *p.add(8) as u64 } else { 0 };
    let t = v | (!m & 0x0101_0101_0101_0101);
    let zero = t.wrapping_sub(0x0101_0101_0101_0101) & !t & 0x8080_8080_8080_8080;
    let bad = ((v | b8) & 0x8080_8080_8080_8080) | zero | ((len == 9) & (b8 == 0)) as u64;
    if bad != 0 {
        return None;
    }
    let x = ((v & 0x7F00_7F00_7F00_7F00) >> 1) | (v & 0x007F_007F_007F_007F);
    let x = ((x & 0x3FFF_0000_3FFF_0000) >> 2) | (x & 0x0000_3FFF_0000_3FFF);
    let x = ((x & 0x0FFF_FFFF_0000_0000) >> 4) | (x & 0x0000_0000_0FFF_FFFF);
    Some((x << 7) | b8)
}

/// pack7 of 16 IDs at once: ID i is the n[i] bytes at base + a[i]. Lanes
/// pack7 rejects are set in the returned mask (their out value is garbage).
/// Requires 16 readable bytes at each ID start.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,avx512dq,avx512vbmi2,avx2")]
#[inline]
unsafe fn pack16(base: *const u8, a: &[u32; 16], n: &[u32; 16], out: &mut [u64; 16]) -> u16 {
    use std::arch::x86_64::*;
    let av = _mm512_loadu_si512(a.as_ptr() as *const _);
    let nv = _mm512_loadu_si512(n.as_ptr() as *const _);
    // 1..=9 bytes
    let mut slow = !_mm512_cmplt_epu32_mask(_mm512_sub_epi32(nv, _mm512_set1_epi32(1)), _mm512_set1_epi32(9));
    // byte 8 of each ID, 0 unless it has 9 bytes
    let b8w = _mm512_i32gather_epi32::<1>(_mm512_add_epi32(av, _mm512_set1_epi32(8)), base as *const i32);
    let n9 = _mm512_cmpeq_epi32_mask(nv, _mm512_set1_epi32(9));
    let b8w = _mm512_maskz_and_epi32(n9, b8w, _mm512_set1_epi32(0xFF));
    slow |= n9 & _mm512_cmpeq_epi32_mask(b8w, _mm512_setzero_si512());
    let bswap = _mm512_set_epi64(
        0x08090A0B0C0D0E0F, 0x0001020304050607, 0x08090A0B0C0D0E0F, 0x0001020304050607,
        0x08090A0B0C0D0E0F, 0x0001020304050607, 0x08090A0B0C0D0E0F, 0x0001020304050607,
    );
    let s64 = |v: u64| _mm512_set1_epi64(v as i64);
    let mut bad16: u16 = 0;
    for half in 0..2 {
        let (ai, ni, bi) = if half == 0 {
            (_mm512_castsi512_si256(av), _mm512_castsi512_si256(nv), _mm512_castsi512_si256(b8w))
        } else {
            (_mm512_extracti64x4_epi64::<1>(av), _mm512_extracti64x4_epi64::<1>(nv), _mm512_extracti64x4_epi64::<1>(b8w))
        };
        let raw = _mm512_i32gather_epi64::<1>(ai, base as *const i64);
        let v = _mm512_shuffle_epi8(raw, bswap);
        let len = _mm512_min_epu64(_mm512_cvtepu32_epi64(ni), s64(8));
        let b8 = _mm512_cvtepu32_epi64(bi);
        // m = !0 << (64 - 8 * len) (0 for len 0)
        let m = _mm512_sllv_epi64(s64(!0), _mm512_sub_epi64(s64(64), _mm512_slli_epi64::<3>(len)));
        let v = _mm512_and_si512(v, m);
        let t = _mm512_or_si512(v, _mm512_andnot_si512(m, s64(0x0101_0101_0101_0101)));
        let zero = _mm512_andnot_si512(t, _mm512_sub_epi64(t, s64(0x0101_0101_0101_0101)));
        let hib = _mm512_or_si512(_mm512_or_si512(v, b8), zero);
        let bad = _mm512_test_epi64_mask(hib, s64(0x8080_8080_8080_8080));
        bad16 |= (bad as u16) << (8 * half);
        let x = _mm512_or_si512(_mm512_srli_epi64::<1>(_mm512_and_si512(v, s64(0x7F00_7F00_7F00_7F00))), _mm512_and_si512(v, s64(0x007F_007F_007F_007F)));
        let x = _mm512_or_si512(_mm512_srli_epi64::<2>(_mm512_and_si512(x, s64(0x3FFF_0000_3FFF_0000))), _mm512_and_si512(x, s64(0x0000_3FFF_0000_3FFF)));
        let x = _mm512_or_si512(_mm512_srli_epi64::<4>(_mm512_and_si512(x, s64(0x0FFF_FFFF_0000_0000))), _mm512_and_si512(x, s64(0x0000_0000_0FFF_FFFF)));
        let k = _mm512_or_si512(_mm512_slli_epi64::<7>(x), b8);
        _mm512_storeu_si512(out.as_mut_ptr().add(8 * half) as *mut _, k);
    }
    slow | bad16
}

/// Exact key of a non-empty ID (interns other IDs).
#[inline(always)]
unsafe fn key_of(p: *const u8, len: usize) -> u64 {
    match pack7(p, len) {
        Some(k) => k,
        None => intern(std::slice::from_raw_parts(p, len)),
    }
}

/// Exact key of a non-empty ID, None if it is not packable and never interned.
#[inline(always)]
unsafe fn key_find(p: *const u8, len: usize) -> Option<u64> {
    match pack7(p, len) {
        Some(k) => Some(k),
        None => find_interned(std::slice::from_raw_parts(p, len)),
    }
}

fn key_bytes(k: u64, out: &mut Vec<u8>) {
    if k & SLOW == 0 {
        for i in 0..9 {
            let c = ((k >> (56 - 7 * i)) & 0x7F) as u8;
            if c == 0 {
                break;
            }
            out.push(c);
        }
    } else {
        let g = INTERN.lock().unwrap();
        out.extend_from_slice(&g.strs[(k & !SLOW) as usize]);
    }
}

/// Byte-wise order of the IDs behind two keys.
#[inline(always)]
fn key_cmp(a: u64, b: u64) -> Ordering {
    if (a | b) & SLOW == 0 {
        a.cmp(&b)
    } else {
        key_cmp_slow(a, b)
    }
}

#[cold]
#[inline(never)]
fn key_cmp_slow(a: u64, b: u64) -> Ordering {
    let (mut x, mut y) = (Vec::new(), Vec::new());
    key_bytes(a, &mut x);
    key_bytes(b, &mut y);
    x.cmp(&y)
}

#[inline(always)]
fn mix(k: u64) -> u64 {
    let m = ((k ^ 0x9E37_79B9_7F4A_7C15) as u128) * 0xD1B5_4A32_D192_ED03u128;
    (m as u64) ^ ((m >> 64) as u64)
}

#[inline(always)]
fn part_of(h: u64, bits: u32) -> usize {
    ((h >> 1) >> (63 - bits)) as usize
}

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        _mm_prefetch(p as *const i8, _MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = p;
}

/// Small open-addressing map key -> u32 (products, campaigns). Key 0 never occurs.
struct KeyMap {
    slots: Vec<(u64, u32)>,
    mask: usize,
}

impl KeyMap {
    fn new(n: usize) -> KeyMap {
        let cap = (n * 2).max(16).next_power_of_two();
        KeyMap { slots: vec![(0, 0); cap], mask: cap - 1 }
    }
    fn insert(&mut self, k: u64, v: u32) {
        let mut j = mix(k) as usize & self.mask;
        loop {
            if self.slots[j].0 == 0 || self.slots[j].0 == k {
                self.slots[j] = (k, v);
                return;
            }
            j = (j + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u32> {
        let mut j = mix(k) as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(j) };
            if s.0 == k {
                return Some(s.1);
            }
            if s.0 == 0 {
                return None;
            }
            j = (j + 1) & self.mask;
        }
    }
}

// ------------------------------------------------------------------ CSV
//
// Only '\n' ends a line. A line is well-formed when it has exactly NF-1 commas
// and every '"' is the first or last byte of a field of length >= 2 that both
// starts and ends with one.

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let c = _mm256_set1_epi8(b',' as i8);
    let n = _mm256_set1_epi8(b'\n' as i8);
    let q = _mm256_set1_epi8(b'"' as i8);
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let m = |v: __m256i, x: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, x)) as u32 as u64;
    (m(a, c) | (m(b, c) << 32), m(a, n) | (m(b, n) << 32), m(a, q) | (m(b, q) << 32))
}

#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let c = _mm_set1_epi8(b',' as i8);
    let n = _mm_set1_epi8(b'\n' as i8);
    let q = _mm_set1_epi8(b'"' as i8);
    let (mut cm, mut nm, mut qm) = (0u64, 0u64, 0u64);
    for i in 0..4 {
        let v = _mm_loadu_si128(p.add(16 * i) as *const __m128i);
        let m = |x: __m128i| (_mm_movemask_epi8(_mm_cmpeq_epi8(v, x)) as u32 as u64) << (16 * i);
        cm |= m(c);
        nm |= m(n);
        qm |= m(q);
    }
    (cm, nm, qm)
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    let (mut cm, mut nm, mut qm) = (0u64, 0u64, 0u64);
    for i in 0..64 {
        let b = *p.add(i);
        cm |= ((b == b',') as u64) << i;
        nm |= ((b == b'\n') as u64) << i;
        qm |= ((b == b'"') as u64) << i;
    }
    (cm, nm, qm)
}

/// Calls `row` with the NF fields (start, end offsets from `base`, quotes
/// stripped) of every well-formed line starting in [lo, hi). Requires
/// base[hi] == b'\n' and 128 readable bytes after it.
///
/// Lines under 128 bytes take a branch-free mask path: with separators
/// S = commas | newline, a field starts after each separator and ends before
/// the next. Quoting is valid iff every quote sits at a field start (qs) or
/// end (qe), no quote is both (a 1-byte `"` field), and each field starting
/// with a quote ends with one: adding qs to the non-separator mask carries
/// each start quote to its field's separator, which must equal qe << 1.
#[inline(always)]
unsafe fn scan<const NF: usize>(base: *const u8, lo: usize, hi: usize, row: impl FnMut(&[(usize, usize); NF])) {
    scan_w::<NF, false>(base, lo, hi, row)
}

/// Byte offsets 0..64, for the AVX-512 field split.
static IOTA: [u8; 64] = {
    let mut a = [0u8; 64];
    let mut i = 0;
    while i < 64 {
        a[i] = i as u8;
        i += 1;
    }
    a
};

/// scan; W: split fields with AVX-512 VBMI2 compress (callers must check the
/// CPU and run inside a function that enables avx512f/bw/vbmi2).
#[inline(always)]
unsafe fn scan_w<const NF: usize, const W: bool>(base: *const u8, lo: usize, hi: usize, mut row: impl FnMut(&[(usize, usize); NF])) {
    let mut pos = lo;
    while pos < hi {
        let p = base.add(pos);
        let (c0, n0, q0) = masks64(p);
        let (c, n, q) = if n0 != 0 {
            (c0 as u128, n0 as u128, q0 as u128)
        } else {
            let (c1, n1, q1) = masks64(p.add(64));
            (c0 as u128 | ((c1 as u128) << 64), (n1 as u128) << 64, q0 as u128 | ((q1 as u128) << 64))
        };
        if n == 0 {
            pos = slow_line::<NF>(base, pos, &mut row);
            continue;
        }
        let nl = n.trailing_zeros();
        let nlb = 1u128 << nl;
        let below = nlb - 1;
        let c = c & below;
        let q = q & below;
        let next = pos + nl as usize + 1;
        if c.count_ones() != (NF - 1) as u32 {
            pos = next;
            continue;
        }
        let sp = c | nlb;
        let qs = q & ((c << 1) | 1);
        let qe = q & (sp >> 1);
        {
            let ns = below & !c;
            let mapped = (ns.wrapping_add(qs) ^ ns) & sp;
            if (q & !(qs | qe)) | (qs & qe) | (mapped ^ (qe << 1)) != 0 {
                pos = next;
                continue;
            }
        }
        // field starts (after each separator) and ends (at each separator),
        // moved inward past a field's quotes: then field i is the i-th set
        // bit of each mask, with no per-field shifts or quote tests
        let mut st = (((c << 1) | 1) & !qs) | (qs << 1);
        let mut en = (sp & !(qe << 1)) | qe;
        let mut f = [(0usize, 0usize); NF];
        #[cfg(target_arch = "x86_64")]
        if W {
            // offsets of the set bits of st / en, in order, by compressing the
            // byte offsets of each 64-bit half (the high half lands after the
            // low half's count)
            use std::arch::x86_64::*;
            let mut sb = [0u8; 128];
            let mut eb = [0u8; 128];
            let il = _mm512_loadu_si512(IOTA.as_ptr() as *const _);
            let ih = _mm512_add_epi8(il, _mm512_set1_epi8(64));
            let (sl, sh, el, eh) = (st as u64, (st >> 64) as u64, en as u64, (en >> 64) as u64);
            _mm512_storeu_si512(sb.as_mut_ptr() as *mut _, _mm512_maskz_compress_epi8(sl, il));
            _mm512_storeu_si512(eb.as_mut_ptr() as *mut _, _mm512_maskz_compress_epi8(el, il));
            if (sh | eh) != 0 {
                _mm512_storeu_si512(sb.as_mut_ptr().add((sl.count_ones() as usize).min(64)) as *mut _, _mm512_maskz_compress_epi8(sh, ih));
                _mm512_storeu_si512(eb.as_mut_ptr().add((el.count_ones() as usize).min(64)) as *mut _, _mm512_maskz_compress_epi8(eh, ih));
            }
            for (k, x) in f.iter_mut().enumerate() {
                *x = (pos + sb[k] as usize, pos + eb[k] as usize);
            }
            row(&f);
            pos = next;
            continue;
        }
        for x in f.iter_mut() {
            let a = st.trailing_zeros();
            st &= st - 1;
            let e = en.trailing_zeros();
            en &= en - 1;
            *x = (pos + a as usize, pos + e as usize);
        }
        row(&f);
        pos = next;
    }
}

/// One line of 128 bytes or more, scalar. Returns the next line's start.
#[cold]
#[inline(never)]
unsafe fn slow_line<const NF: usize>(base: *const u8, pos: usize, row: &mut impl FnMut(&[(usize, usize); NF])) -> usize {
    let mut f = [(0usize, 0usize); NF];
    let mut nf = 0usize;
    let mut a = pos;
    let mut i = pos;
    loop {
        let b = *base.add(i);
        if b == b',' || b == b'\n' {
            if nf < NF {
                f[nf] = (a, i);
            }
            nf += 1;
            a = i + 1;
            if b == b'\n' {
                break;
            }
        }
        i += 1;
    }
    let next = i + 1;
    if nf != NF {
        return next;
    }
    for x in f.iter_mut() {
        let (a, b) = *x;
        let s = std::slice::from_raw_parts(base.add(a), b - a);
        let nq = s.iter().filter(|&&c| c == b'"').count();
        if nq == 0 {
            continue;
        }
        if nq == 2 && s.len() >= 2 && s[0] == b'"' && s[s.len() - 1] == b'"' {
            *x = (a + 1, b - 1);
        } else {
            return next;
        }
    }
    row(&f);
    next
}

/// Grows buf (zero-filling only the new part) to at least n bytes. buf is a
/// reused read buffer: its length is a high-water mark, every byte below it
/// initialized, so a chunk is read over the previous one with no fill first.
fn ensure(buf: &mut Vec<u8>, n: usize) {
    if buf.len() < n {
        buf.resize(n.max(buf.len() * 5 / 4), 0);
    }
}

/// Reads [from, to) of f into buf at `at`; returns at + (to - from).
fn read_range(f: &mut File, from: u64, to: u64, buf: &mut Vec<u8>, at: usize) -> usize {
    let n = (to - from) as usize;
    ensure(buf, at + n);
    f.seek(SeekFrom::Start(from)).expect("seek");
    f.read_exact(&mut buf[at..at + n]).expect("read");
    at + n
}

#[inline]
fn find_nl(s: &[u8]) -> Option<usize> {
    s.iter().position(|&b| b == b'\n')
}

/// Loads the lines of `f` that start in [s, e) into `buf`, minus the header.
/// Returns (lo, hi) for scan; buf[hi] is '\n', followed by 192 zero bytes.
fn load_chunk(f: &mut File, s: u64, e: u64, fsize: u64, buf: &mut Vec<u8>) -> (usize, usize) {
    if fsize == 0 || s >= e {
        return (0, 0);
    }
    let rs = if s == 0 { 0 } else { s - 1 };
    let mut rend = (e + 512).min(fsize);
    let mut used = read_range(f, rs, rend, buf, 0);
    let lim = (e - 1 - rs) as usize; // index of the chunk's last byte
    let mut lo = 0usize;
    if s > 0 {
        // a line starts after each '\n'; it is ours if it starts before e
        match find_nl(&buf[..lim]) {
            Some(i) => lo = i + 1,
            None => return (0, 0),
        }
    }
    // the last line of the chunk ends at the first '\n' at or after e-1 (or EOF)
    let mut from = lim;
    let hi = loop {
        if let Some(i) = find_nl(&buf[from..used]) {
            break from + i;
        }
        if rend >= fsize {
            break used;
        }
        from = used;
        let nend = (rend + (rend - rs).max(4096)).min(fsize);
        used = read_range(f, rend, nend, buf, used);
        rend = nend;
    };
    ensure(buf, hi + 1 + 192);
    buf[hi] = b'\n';
    buf[hi + 1..hi + 1 + 192].fill(0);
    if s == 0 {
        lo = match find_nl(&buf[..hi]) {
            Some(i) => i + 1,
            None => hi,
        };
    }
    (lo, hi)
}

fn load_whole(path: &Path, buf: &mut Vec<u8>) -> (usize, usize) {
    let mut f = File::open(path).unwrap_or_else(|e| panic!("open {}: {}", path.display(), e));
    let size = f.metadata().expect("stat").len();
    load_chunk(&mut f, 0, size, size, buf)
}

/// Like sorted(glob("DIR/*.csv")): name order, no dotfiles.
fn csv_files(dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut v: Vec<(String, PathBuf, u64)> = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".csv") || name.starts_with('.') {
                continue;
            }
            let p = e.path();
            if let Ok(m) = fs::metadata(&p) {
                if m.is_file() {
                    v.push((name, p, m.len()));
                }
            }
        }
    }
    v.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    v.into_iter().map(|(_, p, n)| (p, n)).collect()
}

// ------------------------------------------------------------------ records

/// Stage A event row. ts: ms since window_start, DEAD = failed validation (or,
/// after dedup, a losing copy). pc: price << 8 | category, 0 for view/cart.
#[derive(Clone, Copy)]
#[repr(C)]
struct ERec {
    eid: u64,
    user: u64,
    ing: i64,
    ts: u32,
    pc: u32,
}

/// Stage A click row. ord: read order (task << 32 | row), the first copy read
/// wins. ts: ms since window_start - 7 days. camp: campaign index, DEAD for an
/// invalid click or, after dedup, a repeated copy.
#[derive(Clone, Copy)]
#[repr(C)]
struct CRec {
    cid: u64,
    user: u64,
    ord: u64,
    ts: u32,
    camp: u32,
}

/// Stage B event row: person, ts << 32 | pc.
#[derive(Clone, Copy)]
#[repr(C)]
struct PRec {
    person: u64,
    key: u64,
}

/// Stage B click row.
#[derive(Clone, Copy)]
#[repr(C)]
struct PCk {
    person: u64,
    cid: u64,
    ts: u32,
    camp: u32,
}

/// Stage C click.
#[derive(Clone, Copy)]
#[repr(C)]
struct CK {
    cid: u64,
    ts: u32,
    camp: u32,
}

struct Ptr<T>(*mut T);
impl<T> Clone for Ptr<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Ptr<T> {}
unsafe impl<T> Send for Ptr<T> {}
unsafe impl<T> Sync for Ptr<T> {}

/// Bytes per arena chunk: whole 192 B stage groups, and whole records of
/// every row type (16, 24, 32 B).
const CH: usize = 128 * STAGE;

/// One thread's chunk source: chunks it has freed first (rows already
/// consumed), else fresh 64 B aligned chunks carved from big slabs. Reusing
/// consumed chunks spares the kernel zeroing fresh pages, and handing out
/// chunks on demand replaces per-partition regions sized with headroom.
/// Slabs are never freed (the process exits without destructors).
struct Arena {
    free: Vec<*mut u8>,
    next: *mut u8,
    left: usize,
    slab: usize,
}

impl Arena {
    /// A leaked arena (Parts keep a pointer to it across threads; only the
    /// thread that pushes uses it).
    fn leak(slab: usize) -> *mut Arena {
        let slab = slab.div_ceil(CH).max(16) * CH;
        Box::into_raw(Box::new(Arena { free: Vec::new(), next: std::ptr::null_mut(), left: 0, slab }))
    }
    #[inline]
    fn get(&mut self) -> *mut u8 {
        if let Some(c) = self.free.pop() {
            return c;
        }
        if self.left < CH {
            let mut v: Vec<u8> = Vec::with_capacity(self.slab + 64);
            let p = v.as_mut_ptr();
            std::mem::forget(v);
            let off = p.align_offset(64);
            self.next = unsafe { p.add(off) };
            self.left = self.slab;
        }
        let c = self.next;
        self.next = unsafe { c.add(CH) };
        self.left -= CH;
        c
    }
}

/// One thread's rows, partitioned: each partition is a list of full arena
/// chunks plus the current one. Rows first collect in a cache-resident 192 B
/// stage group per partition; a full group goes to the chunk with
/// non-temporal stores (no read for ownership of the destination lines, no
/// cache pollution). `flush` drains partial groups and must run before the
/// rows are read, on the writing thread.
struct Parts<T: Copy> {
    arena: *mut Arena,
    full: Vec<Vec<*mut T>>,
    cur: Vec<*mut T>,
    fill: Vec<u32>,
    stage: Vec<Stage>,
    cnt: Vec<u8>,
}

const STAGE: usize = 192;

#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Stage([u8; STAGE]);

unsafe impl<T: Copy> Send for Parts<T> {}
unsafe impl<T: Copy> Sync for Parts<T> {}

impl<T: Copy> Parts<T> {
    const K: usize = STAGE / std::mem::size_of::<T>();
    const CAP: usize = CH / std::mem::size_of::<T>();

    fn new(np: usize, arena: *mut Arena) -> Parts<T> {
        const { assert!(STAGE % std::mem::size_of::<T>() == 0 && STAGE / std::mem::size_of::<T>() < 256) };
        Parts {
            arena,
            full: (0..np).map(|_| Vec::new()).collect(),
            cur: vec![std::ptr::null_mut(); np],
            fill: vec![Self::CAP as u32; np],
            stage: vec![Stage([0; STAGE]); np],
            cnt: vec![0; np],
        }
    }
    #[inline(always)]
    fn push(&mut self, p: usize, v: T) {
        unsafe {
            let c = self.cnt.get_unchecked_mut(p);
            (self.stage.get_unchecked_mut(p).0.as_mut_ptr() as *mut T).add(*c as usize).write(v);
            *c += 1;
            if *c as usize == Self::K {
                *c = 0;
                self.group(p);
            }
        }
    }
    /// Room for n more rows in partition p's current chunk (a new chunk if not).
    #[inline(always)]
    unsafe fn room(&mut self, p: usize, n: usize) -> *mut T {
        if *self.fill.get_unchecked(p) as usize + n > Self::CAP {
            self.new_chunk(p);
        }
        let f = self.fill.get_unchecked_mut(p);
        let d = self.cur.get_unchecked(p).add(*f as usize);
        *f += n as u32;
        d
    }
    #[inline(never)]
    unsafe fn new_chunk(&mut self, p: usize) {
        let old = self.cur[p];
        if !old.is_null() {
            self.full[p].push(old);
        }
        self.cur[p] = (*self.arena).get() as *mut T;
        self.fill[p] = 0;
    }
    /// Moves partition p's full stage group to its chunk.
    #[inline(always)]
    unsafe fn group(&mut self, p: usize) {
        let dst = self.room(p, Self::K) as *mut u8;
        let src = self.stage.get_unchecked(p).0.as_ptr();
        #[cfg(target_arch = "x86_64")]
        {
            use std::arch::x86_64::*;
            for i in 0..STAGE / 16 {
                _mm_stream_si128(dst.add(16 * i) as *mut __m128i, _mm_load_si128(src.add(16 * i) as *const __m128i));
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        std::ptr::copy_nonoverlapping(src, dst, STAGE);
    }
    /// Drains the partial stage groups; orders the non-temporal stores.
    fn flush(&mut self) {
        for p in 0..self.cnt.len() {
            let n = self.cnt[p] as usize;
            if n > 0 {
                self.cnt[p] = 0;
                unsafe {
                    let d = self.room(p, n);
                    std::ptr::copy_nonoverlapping(self.stage[p].0.as_ptr() as *const T, d, n);
                }
            }
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence()
        };
    }
    fn len(&self, p: usize) -> usize {
        let c = if self.cur[p].is_null() { 0 } else { self.fill[p] as usize };
        self.full[p].len() * Self::CAP + c + self.cnt[p] as usize
    }
    /// The written chunks of partition p; each starts at its chunk's base.
    fn segs(&self, p: usize, out: &mut Vec<(Ptr<T>, usize)>) {
        for &c in &self.full[p] {
            out.push((Ptr(c), Self::CAP));
        }
        let c = self.cur[p];
        if !c.is_null() && self.fill[p] > 0 {
            out.push((Ptr(c), self.fill[p] as usize));
        }
    }
}

struct Cfg {
    ws: i64,
    we: i64,
    cal: Cal,
    products: KeyMap,
    unknown_cat: u32,
    campaigns: KeyMap,
    pe_bits: u32,
    pc_bits: u32,
    q_bits: u32,
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Events,
    Clicks,
}

struct Task {
    kind: Kind,
    file: usize,
    s: u64,
    e: u64,
}

#[inline]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = (v * 10 + d as u64).min(1 << 40);
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

#[inline(always)]
unsafe fn rd32(p: *const u8) -> u32 {
    std::ptr::read_unaligned(p as *const u32)
}

/// The CPU has AVX-512 F/BW/DQ/VBMI2 (compress field split, 16-lane timestamps).
fn avx512() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        static ON: LazyLock<bool> = LazyLock::new(|| {
            std::env::var_os("NO_AVX512").is_none()
                && is_x86_feature_detected!("avx512f")
                && is_x86_feature_detected!("avx512bw")
                && is_x86_feature_detected!("avx512dq")
                && is_x86_feature_detected!("avx512vbmi2")
        });
        *ON
    }
    #[cfg(not(target_arch = "x86_64"))]
    false
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,avx512dq,avx512vbmi2,avx2,bmi1,bmi2,popcnt,lzcnt")]
unsafe fn parse_events_512(cfg: &Cfg, base: *const u8, lo: usize, hi: usize, out: &mut Parts<ERec>) {
    parse_events_w::<true>(cfg, base, lo, hi, out)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,avx512dq,avx512vbmi2,avx2,bmi1,bmi2,popcnt,lzcnt")]
unsafe fn parse_clicks_512(cfg: &Cfg, base: *const u8, lo: usize, hi: usize, task: u64, out: &mut Parts<CRec>) {
    parse_clicks_w::<true>(cfg, base, lo, hi, task, out)
}

unsafe fn parse_events(cfg: &Cfg, base: *const u8, lo: usize, hi: usize, out: &mut Parts<ERec>) {
    #[cfg(target_arch = "x86_64")]
    if avx512() {
        return parse_events_512(cfg, base, lo, hi, out);
    }
    parse_events_w::<false>(cfg, base, lo, hi, out)
}

unsafe fn parse_clicks(cfg: &Cfg, base: *const u8, lo: usize, hi: usize, task: u64, out: &mut Parts<CRec>) {
    #[cfg(target_arch = "x86_64")]
    if avx512() {
        return parse_clicks_512(cfg, base, lo, hi, task, out);
    }
    parse_clicks_w::<false>(cfg, base, lo, hi, task, out)
}

#[inline(always)]
unsafe fn parse_events_w<const W: bool>(cfg: &Cfg, base: *const u8, lo: usize, hi: usize, out: &mut Parts<ERec>) {
    if W {
        // AVX-512: rows collect 16 at a time; their ts and ingest_ts columns
        // are parsed 16 lanes at once, then each row is finished on its own
        let mut bt = Batch16 { f: [[(0, 0); 7]; 16], ta: [0; 16], tn: [0; 16], ia: [0; 16], inn: [0; 16], ea: [0; 16], en: [0; 16], ua: [0; 16], un: [0; 16], cnt: 0 };
        scan_w::<7, W>(base, lo, hi, |f| {
            if f[0].0 == f[0].1 {
                return;
            }
            let i = bt.cnt;
            *bt.f.get_unchecked_mut(i) = *f;
            *bt.ta.get_unchecked_mut(i) = f[2].0 as u32;
            *bt.tn.get_unchecked_mut(i) = (f[2].1 - f[2].0) as u32;
            *bt.ia.get_unchecked_mut(i) = f[3].0 as u32;
            *bt.inn.get_unchecked_mut(i) = (f[3].1 - f[3].0) as u32;
            *bt.ea.get_unchecked_mut(i) = f[0].0 as u32;
            *bt.en.get_unchecked_mut(i) = (f[0].1 - f[0].0) as u32;
            *bt.ua.get_unchecked_mut(i) = f[1].0 as u32;
            *bt.un.get_unchecked_mut(i) = (f[1].1 - f[1].0) as u32;
            bt.cnt = i + 1;
            if i == 15 {
                bt.run(cfg, base, out);
            }
        });
        if bt.cnt > 0 {
            bt.run(cfg, base, out);
        }
        return;
    }
    scan_w::<7, W>(base, lo, hi, |f| {
        if f[0].0 == f[0].1 {
            return;
        }
        let ing = cfg.cal.f1_trusted(base.add(f[3].0), f[3].1 - f[3].0).unwrap_or(i64::MAX);
        event_row(
            cfg,
            base,
            f,
            key_of(base.add(f[0].0), f[0].1 - f[0].0),
            ing,
            || cfg.cal.parse(base.add(f[2].0), f[2].1 - f[2].0),
            || key_of(base.add(f[1].0), f[1].1 - f[1].0),
            out,
        );
    });
}

/// Up to 16 event rows (non-empty event_id) waiting for the 16-lane parse:
/// their fields, and the starts and lengths of ts, ingest_ts, event_id and
/// user_id.
struct Batch16 {
    f: [[(usize, usize); 7]; 16],
    ta: [u32; 16],
    tn: [u32; 16],
    ia: [u32; 16],
    inn: [u32; 16],
    ea: [u32; 16],
    en: [u32; 16],
    ua: [u32; 16],
    un: [u32; 16],
    cnt: usize,
}

impl Batch16 {
    #[inline(always)]
    unsafe fn run(&mut self, cfg: &Cfg, base: *const u8, out: &mut Parts<ERec>) {
        let n = self.cnt;
        self.cnt = 0;
        // unused lanes repeat lane 0 (readable bytes; results ignored)
        for i in n..16 {
            self.ta[i] = self.ta[0];
            self.tn[i] = self.tn[0];
            self.ia[i] = self.ia[0];
            self.inn[i] = self.inn[0];
            self.ea[i] = self.ea[0];
            self.en[i] = self.en[0];
            self.ua[i] = self.ua[0];
            self.un[i] = self.un[0];
        }
        let mut tv = [0i64; 16];
        let mut iv = [0i64; 16];
        let mut ev = [0u64; 16];
        let mut uv = [0u64; 16];
        let (ts_slow, ing_slow, e_slow, u_slow) = {
            #[cfg(target_arch = "x86_64")]
            {
                (
                    cfg.cal.parse16(base, &self.ta, &self.tn, false, &mut tv),
                    cfg.cal.parse16(base, &self.ia, &self.inn, true, &mut iv),
                    pack16(base, &self.ea, &self.en, &mut ev),
                    pack16(base, &self.ua, &self.un, &mut uv),
                )
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
                (u16::MAX, u16::MAX, u16::MAX, u16::MAX)
            }
        };
        for i in 0..n {
            let f = &self.f[i];
            let ing = if (ing_slow >> i) & 1 != 0 {
                cfg.cal.f1_trusted(base.add(f[3].0), f[3].1 - f[3].0).unwrap_or(i64::MAX)
            } else {
                iv[i]
            };
            let slow = (ts_slow >> i) & 1 != 0;
            let t = tv[i];
            let eid = if (e_slow >> i) & 1 != 0 { key_of(base.add(f[0].0), f[0].1 - f[0].0) } else { ev[i] };
            let (us, u) = ((u_slow >> i) & 1 != 0, uv[i]);
            event_row(
                cfg,
                base,
                f,
                eid,
                ing,
                || if slow { cfg.cal.parse(base.add(f[2].0), f[2].1 - f[2].0) } else if t == i64::MIN { None } else { Some(t) },
                || if us { key_of(base.add(f[1].0), f[1].1 - f[1].0) } else { u },
                out,
            );
        }
    }
}

/// One event row with a non-empty event_id, given its key and ingest time,
/// its ts parser (called only once the cheaper checks pass) and its user key
/// (called only for a valid row).
#[inline(always)]
#[allow(clippy::too_many_arguments)]
unsafe fn event_row(
    cfg: &Cfg,
    base: *const u8,
    f: &[(usize, usize); 7],
    eid: u64,
    ing: i64,
    ts: impl FnOnce() -> Option<i64>,
    user: impl FnOnce() -> u64,
    out: &mut Parts<ERec>,
) {
    const VIEW: u32 = u32::from_le_bytes(*b"view");
    const CART: u32 = u32::from_le_bytes(*b"cart");
    const PURCHASE: u64 = u64::from_le_bytes(*b"purchase");
    let (ws, we) = (cfg.ws, cfg.we);
    let mut r = ERec { eid, user: 0, ing, ts: DEAD, pc: 0 };
    'v: {
        let (ua, ub) = f[1];
        let (pa, pb) = f[5];
        let (ta, tb) = f[4];
        if ua == ub || pa == pb {
            break 'v;
        }
        let tl = tb - ta;
        let purchase = tl == 8 && std::ptr::read_unaligned(base.add(ta) as *const u64) == PURCHASE;
        let w = rd32(base.add(ta));
        if !purchase & !((tl == 4) & ((w == VIEW) | (w == CART))) {
            break 'v;
        }
        let ts = match ts() {
            Some(t) if t >= ws && t < we => t,
            _ => break 'v,
        };
        if ing.saturating_sub(ts) > DAY_MS {
            break 'v;
        }
        if purchase {
            let price = match parse_price(std::slice::from_raw_parts(base.add(f[6].0), f[6].1 - f[6].0)) {
                Some(p) => p,
                None => break 'v,
            };
            let cat = key_find(base.add(pa), pb - pa).and_then(|k| cfg.products.get(k)).unwrap_or(cfg.unknown_cat);
            r.pc = (price << 8) | cat;
        }
        r.user = user();
        r.ts = (ts - ws) as u32;
    }
    out.push(part_of(mix(eid), cfg.pe_bits), r);
}

/// Invalid clicks are kept (camp DEAD): dedup comes before validation.
#[inline(always)]
unsafe fn parse_clicks_w<const W: bool>(cfg: &Cfg, base: *const u8, lo: usize, hi: usize, task: u64, out: &mut Parts<CRec>) {
    let mut ord = task << 32;
    if W {
        // AVX-512: as for events, 16 rows at a time
        let mut bt = CBatch16 { f: [[(0, 0); 4]; 16], ca: [0; 16], cn: [0; 16], ua: [0; 16], un: [0; 16], ta: [0; 16], tn: [0; 16], cnt: 0 };
        scan_w::<4, W>(base, lo, hi, |f| {
            if f[0].0 == f[0].1 {
                return;
            }
            let i = bt.cnt;
            *bt.f.get_unchecked_mut(i) = *f;
            *bt.ca.get_unchecked_mut(i) = f[0].0 as u32;
            *bt.cn.get_unchecked_mut(i) = (f[0].1 - f[0].0) as u32;
            *bt.ua.get_unchecked_mut(i) = f[1].0 as u32;
            *bt.un.get_unchecked_mut(i) = (f[1].1 - f[1].0) as u32;
            *bt.ta.get_unchecked_mut(i) = f[3].0 as u32;
            *bt.tn.get_unchecked_mut(i) = (f[3].1 - f[3].0) as u32;
            bt.cnt = i + 1;
            if i == 15 {
                bt.run(cfg, base, &mut ord, out);
            }
        });
        if bt.cnt > 0 {
            bt.run(cfg, base, &mut ord, out);
        }
        return;
    }
    scan_w::<4, W>(base, lo, hi, |f| {
        if f[0].0 == f[0].1 {
            return;
        }
        click_row(
            cfg,
            base,
            f,
            key_of(base.add(f[0].0), f[0].1 - f[0].0),
            ord,
            || cfg.cal.parse(base.add(f[3].0), f[3].1 - f[3].0),
            || key_of(base.add(f[1].0), f[1].1 - f[1].0),
            out,
        );
        ord += 1;
    });
}

/// Up to 16 click rows (non-empty click_id) waiting for the 16-lane parse:
/// their fields, and the starts and lengths of click_id, user_id and ts.
struct CBatch16 {
    f: [[(usize, usize); 4]; 16],
    ca: [u32; 16],
    cn: [u32; 16],
    ua: [u32; 16],
    un: [u32; 16],
    ta: [u32; 16],
    tn: [u32; 16],
    cnt: usize,
}

impl CBatch16 {
    #[inline(always)]
    unsafe fn run(&mut self, cfg: &Cfg, base: *const u8, ord: &mut u64, out: &mut Parts<CRec>) {
        let n = self.cnt;
        self.cnt = 0;
        // unused lanes repeat lane 0 (readable bytes; results ignored)
        for i in n..16 {
            self.ca[i] = self.ca[0];
            self.cn[i] = self.cn[0];
            self.ua[i] = self.ua[0];
            self.un[i] = self.un[0];
            self.ta[i] = self.ta[0];
            self.tn[i] = self.tn[0];
        }
        let mut tv = [0i64; 16];
        let mut cv = [0u64; 16];
        let mut uv = [0u64; 16];
        let (ts_slow, c_slow, u_slow) = {
            #[cfg(target_arch = "x86_64")]
            {
                (
                    cfg.cal.parse16(base, &self.ta, &self.tn, false, &mut tv),
                    pack16(base, &self.ca, &self.cn, &mut cv),
                    pack16(base, &self.ua, &self.un, &mut uv),
                )
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
                (u16::MAX, u16::MAX, u16::MAX)
            }
        };
        for i in 0..n {
            let f = &self.f[i];
            let cid = if (c_slow >> i) & 1 != 0 { key_of(base.add(f[0].0), f[0].1 - f[0].0) } else { cv[i] };
            let (ts_s, t) = ((ts_slow >> i) & 1 != 0, tv[i]);
            let (us, u) = ((u_slow >> i) & 1 != 0, uv[i]);
            click_row(
                cfg,
                base,
                f,
                cid,
                *ord,
                || if ts_s { cfg.cal.parse(base.add(f[3].0), f[3].1 - f[3].0) } else if t == i64::MIN { None } else { Some(t) },
                || if us { key_of(base.add(f[1].0), f[1].1 - f[1].0) } else { u },
                out,
            );
            *ord += 1;
        }
    }
}

/// One click row with a non-empty click_id, given its key and read order,
/// its ts parser and its user key (each called only when needed).
#[inline(always)]
#[allow(clippy::too_many_arguments)]
unsafe fn click_row(
    cfg: &Cfg,
    base: *const u8,
    f: &[(usize, usize); 4],
    cid: u64,
    ord: u64,
    ts: impl FnOnce() -> Option<i64>,
    user: impl FnOnce() -> u64,
    out: &mut Parts<CRec>,
) {
    let (cs, we) = (cfg.ws - WEEK_MS, cfg.we);
    let mut r = CRec { cid, user: 0, ord, ts: 0, camp: DEAD };
    'v: {
        let (ua, ub) = f[1];
        let (ca, cb) = f[2];
        if ua == ub || ca == cb {
            break 'v;
        }
        let camp = match key_find(base.add(ca), cb - ca).and_then(|k| cfg.campaigns.get(k)) {
            Some(c) => c,
            None => break 'v,
        };
        let ts = match ts() {
            Some(t) if t >= cs && t < we => t,
            _ => break 'v,
        };
        r.user = user();
        r.ts = (ts - cs) as u32;
        r.camp = camp;
    }
    out.push(part_of(mix(cid), cfg.pc_bits), r);
}

// ------------------------------------------------------------------ links

/// user ID -> person name (the byte-wise smallest ID of its component), for
/// the IDs that are not their own person's name; any other ID maps to itself.
/// Buckets of 4 keys and their values, one cache line each, at most half
/// full; a probe compares all four keys without branching on the outcome
/// (whether an ID is linked is unpredictable), and moves to the next bucket
/// only when one is full and holds no match.
struct Links {
    bk: Vec<Bucket>,
}

#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Bucket {
    k: [u64; 4],
    v: [u64; 4],
}

impl Links {
    #[inline(always)]
    fn home(&self, h: u64) -> usize {
        ((h as u128 * self.bk.len() as u128) >> 64) as usize
    }
    #[inline(always)]
    fn prefetch(&self, h: u64) {
        if !self.bk.is_empty() {
            prefetch(unsafe { self.bk.as_ptr().add(self.home(h)) });
        }
    }
    #[inline(always)]
    fn lookup(&self, k: u64, h: u64) -> u64 {
        if self.bk.is_empty() {
            return k;
        }
        let mut b = self.home(h);
        loop {
            let bk = unsafe { self.bk.get_unchecked(b) };
            let mut m = 0u32;
            let mut z = 0u32;
            for i in 0..4 {
                m |= ((bk.k[i] == k) as u32) << i;
                z |= ((bk.k[i] == 0) as u32) << i;
            }
            if (m | z) != 0 {
                let v = bk.v[(m.trailing_zeros() & 3) as usize];
                return if m != 0 { v } else { k };
            }
            b += 1;
            if b == self.bk.len() {
                b = 0;
            }
        }
    }
}

unsafe fn parse_links(base: *const u8, lo: usize, hi: usize, pairs: &mut Vec<(u64, u64)>) {
    scan::<2>(base, lo, hi, |f| {
        let ((a0, a1), (b0, b1)) = (f[0], f[1]);
        if a0 == a1 || b0 == b1 {
            return;
        }
        let a = key_of(base.add(a0), a1 - a0);
        let b = key_of(base.add(b0), b1 - b0);
        if a != b {
            pairs.push((a, b));
        }
    });
}

fn build_links(pairs: &[(u64, u64)]) -> Links {
    let np = pairs.len();
    if np == 0 {
        return Links { bk: Vec::new() };
    }
    let cap = (np * 2 + np / 2).max(16).next_power_of_two();
    let mask = cap - 1;
    let mut slots = vec![(0u64, 0u64); cap];
    let mut keys: Vec<u64> = Vec::with_capacity(np + np / 2);
    let mut parent: Vec<u32> = Vec::with_capacity(keys.capacity());
    assert!(cap <= u32::MAX as usize);

    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        loop {
            let p = parent[x as usize];
            if p == x {
                return x;
            }
            let g = parent[p as usize];
            parent[x as usize] = g;
            x = g;
        }
    }

    let mut hs = [(0u64, 0u64); 32];
    for chunk in pairs.chunks(32) {
        for (i, &(a, b)) in chunk.iter().enumerate() {
            let (ha, hb) = (mix(a), mix(b));
            hs[i] = (ha, hb);
            prefetch(unsafe { slots.as_ptr().add(ha as usize & mask) });
            prefetch(unsafe { slots.as_ptr().add(hb as usize & mask) });
        }
        for (i, &(a, b)) in chunk.iter().enumerate() {
            let mut idx = [0u32; 2];
            for (n, (k, h)) in [(a, hs[i].0), (b, hs[i].1)].into_iter().enumerate() {
                let mut j = h as usize & mask;
                idx[n] = loop {
                    let s = &mut slots[j];
                    if s.0 == k {
                        break s.1 as u32;
                    }
                    if s.0 == 0 {
                        let id = keys.len() as u32;
                        *s = (k, id as u64);
                        keys.push(k);
                        parent.push(id);
                        break id;
                    }
                    j = (j + 1) & mask;
                };
            }
            let mut ra = find(&mut parent, idx[0]);
            let mut rb = find(&mut parent, idx[1]);
            if ra != rb {
                // the root is always the component's smallest ID
                if key_cmp(keys[rb as usize], keys[ra as usize]) == Ordering::Less {
                    std::mem::swap(&mut ra, &mut rb);
                }
                parent[rb as usize] = ra;
            }
        }
    }
    drop(slots);
    let mut moved: Vec<(u64, u64)> = Vec::new();
    for i in 0..keys.len() as u32 {
        let r = find(&mut parent, i);
        if r != i {
            moved.push((keys[i as usize], keys[r as usize]));
        }
    }
    let nbk = (moved.len() / 2).max(4);
    let mut out = Links { bk: vec![Bucket { k: [0; 4], v: [0; 4] }; nbk] };
    for &(k, v) in &moved {
        let mut b = out.home(mix(k));
        'ins: loop {
            for i in 0..4 {
                if out.bk[b].k[i] == 0 {
                    out.bk[b].k[i] = k;
                    out.bk[b].v[i] = v;
                    break 'ins;
                }
            }
            b += 1;
            if b == nbk {
                b = 0;
            }
        }
    }
    out
}

// ------------------------------------------------------------------ stage B

/// Rows ahead whose table slot is prefetched.
const LOOK: usize = 16;

/// Sizes and clears the dedup table for n rows.
fn table(tab: &mut Vec<u64>, n: usize) -> usize {
    let cap = (n * 2).next_power_of_two();
    if tab.len() < cap {
        tab.resize(cap, 0);
    }
    tab[..cap].fill(0);
    cap - 1
}

/// Marks every copy but the one with the smallest ingest_ts DEAD.
unsafe fn dedup_ev(segs: &[(Ptr<ERec>, usize)], tab: &mut Vec<u64>) {
    let n: usize = segs.iter().map(|s| s.1).sum();
    if n < 2 {
        return;
    }
    assert!(segs.len() <= 4096);
    let mask = table(tab, n);
    let t = tab.as_mut_ptr();
    for (si, &(ptr, len)) in segs.iter().enumerate() {
        assert!(len < (1 << 28));
        for i in 0..len {
            if i + LOOK < len {
                prefetch(t.add(mix((*ptr.0.add(i + LOOK)).eid) as usize & mask));
            }
            let r = ptr.0.add(i);
            let eid = (*r).eid;
            let h = mix(eid);
            let tag = ((h >> 24) & 0xFF_FFFF) | 1;
            let new = (tag << 40) | ((si as u64) << 28) | i as u64;
            let mut j = h as usize & mask;
            loop {
                let s = *t.add(j);
                if s == 0 {
                    *t.add(j) = new;
                    break;
                }
                if s >> 40 == tag {
                    let o = segs.get_unchecked(((s >> 28) & 0xFFF) as usize).0 .0.add((s & 0x0FFF_FFFF) as usize);
                    if (*o).eid == eid {
                        if (*r).ing < (*o).ing {
                            (*o).ts = DEAD;
                            *t.add(j) = new;
                        } else {
                            (*r).ts = DEAD;
                        }
                        break;
                    }
                }
                j = (j + 1) & mask;
            }
        }
    }
}

/// Marks every copy of a click but the first read DEAD.
unsafe fn dedup_ck(segs: &[(Ptr<CRec>, usize)], tab: &mut Vec<u64>) {
    let n: usize = segs.iter().map(|s| s.1).sum();
    if n < 2 {
        return;
    }
    assert!(segs.len() <= 4096);
    let mask = table(tab, n);
    let t = tab.as_mut_ptr();
    for (si, &(ptr, len)) in segs.iter().enumerate() {
        assert!(len < (1 << 28));
        for i in 0..len {
            if i + LOOK < len {
                prefetch(t.add(mix((*ptr.0.add(i + LOOK)).cid) as usize & mask));
            }
            let r = ptr.0.add(i);
            let cid = (*r).cid;
            let h = mix(cid);
            let tag = ((h >> 24) & 0xFF_FFFF) | 1;
            let new = (tag << 40) | ((si as u64) << 28) | i as u64;
            let mut j = h as usize & mask;
            loop {
                let s = *t.add(j);
                if s == 0 {
                    *t.add(j) = new;
                    break;
                }
                if s >> 40 == tag {
                    let o = segs.get_unchecked(((s >> 28) & 0xFFF) as usize).0 .0.add((s & 0x0FFF_FFFF) as usize);
                    if (*o).cid == cid {
                        if (*r).ord < (*o).ord {
                            (*o).camp = DEAD;
                            *t.add(j) = new;
                        } else {
                            (*r).camp = DEAD;
                        }
                        break;
                    }
                }
                j = (j + 1) & mask;
            }
        }
    }
}

unsafe fn route_ev(segs: &[(Ptr<ERec>, usize)], links: &Links, qb: u32, out: &mut Parts<PRec>) {
    for &(ptr, len) in segs {
        let p = ptr.0;
        for i in 0..len {
            if i + LOOK < len {
                links.prefetch(mix((*p.add(i + LOOK)).user));
            }
            let r = *p.add(i);
            if r.ts == DEAD {
                continue;
            }
            let person = links.lookup(r.user, mix(r.user));
            out.push(part_of(mix(person), qb), PRec { person, key: ((r.ts as u64) << 32) | r.pc as u64 });
        }
    }
}

unsafe fn route_ck(segs: &[(Ptr<CRec>, usize)], links: &Links, qb: u32, out: &mut Parts<PCk>) {
    for &(ptr, len) in segs {
        let p = ptr.0;
        for i in 0..len {
            if i + LOOK < len {
                links.prefetch(mix((*p.add(i + LOOK)).user));
            }
            let r = *p.add(i);
            if r.camp == DEAD {
                continue;
            }
            let person = links.lookup(r.user, mix(r.user));
            out.push(part_of(mix(person), qb), PCk { person, cid: r.cid, ts: r.ts, camp: r.camp });
        }
    }
}

// ------------------------------------------------------------------ stage C

#[derive(Clone, Copy, Default)]
struct CampAgg {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assisted: i64,
}

/// Per-thread credited counts of one (day, campaign): 16 B, so the table a
/// thread updates once per session is half the cache lines of CampAgg.
#[derive(Clone, Copy, Default)]
struct Cred {
    sessions: u32,
    purchasing: u32,
    revenue: i64,
}

/// Dense: rows by (day, campaign), plus the assisted counts as their own
/// small array, so one session's assist increments hit one short run of it.
enum CampTab {
    Dense(Vec<Cred>, Vec<u32>, usize),
    Sparse(HashMap<(u32, u32), CampAgg>),
}

impl CampTab {
    #[inline(always)]
    fn credit(&mut self, camp: u32, day: u32, purchased: bool, rev: i64) {
        match self {
            CampTab::Dense(v, _, nc) => {
                let r = unsafe { v.get_unchecked_mut(day as usize * *nc + camp as usize) };
                r.sessions += 1;
                r.purchasing += purchased as u32;
                r.revenue += rev;
            }
            CampTab::Sparse(m) => {
                let r = m.entry((camp, day)).or_default();
                r.sessions += 1;
                r.purchasing += purchased as i64;
                r.revenue += rev;
            }
        }
    }
    #[inline(always)]
    fn assist(&mut self, active: &[u32], day: u32) {
        match self {
            CampTab::Dense(_, a, nc) => {
                let row = &mut a[day as usize * *nc..(day as usize + 1) * *nc];
                for &c in active {
                    unsafe { *row.get_unchecked_mut(c as usize) += 1 };
                }
            }
            CampTab::Sparse(m) => {
                for &c in active {
                    m.entry((c, day)).or_default().assisted += 1;
                }
            }
        }
    }
}

/// One row group of user_daily rows, buffered; `UOut::spill` writes it to
/// the thread's file and clears it, keeping the capacity.
#[derive(Default)]
struct UOut {
    rows: usize,
    total: usize,
    uid: Vec<u8>,
    day: Vec<i32>,
    sess: Vec<i32>,
    evc: Vec<i32>,
    rev: Vec<i64>,
    cat: Vec<u32>,
    path: PathBuf,
    cat_names: Vec<Vec<u8>>,
    w: Option<pq::Writer>,
    defs: Vec<u8>,
    vals: Vec<u8>,
}

impl UOut {
    fn new(path: PathBuf, cat_names: Vec<Vec<u8>>) -> UOut {
        let n = RG_ROWS;
        UOut {
            uid: Vec::with_capacity(n * 16),
            day: Vec::with_capacity(n),
            sess: Vec::with_capacity(n),
            evc: Vec::with_capacity(n),
            rev: Vec::with_capacity(n),
            cat: Vec::with_capacity(n),
            path,
            cat_names,
            ..UOut::default()
        }
    }
    /// Writes the buffered rows as one row group.
    fn spill(&mut self) {
        if self.rows == 0 {
            return;
        }
        let w = self.w.get_or_insert_with(|| user_writer(&self.path));
        self.defs.clear();
        self.defs.resize(self.rows.div_ceil(8), 0);
        self.vals.clear();
        for (i, &c) in self.cat.iter().enumerate() {
            if c != NONE {
                self.defs[i >> 3] |= 1 << (i & 7);
                let name = &self.cat_names[c as usize];
                self.vals.extend_from_slice(&(name.len() as u32).to_le_bytes());
                self.vals.extend_from_slice(name);
            }
        }
        w.row_group(
            self.rows,
            &[
                pq::ColData { values: &self.uid, defs: None },
                pq::ColData { values: pq::bytes_of(&self.day), defs: None },
                pq::ColData { values: pq::bytes_of(&self.sess), defs: None },
                pq::ColData { values: pq::bytes_of(&self.evc), defs: None },
                pq::ColData { values: pq::bytes_of(&self.rev), defs: None },
                pq::ColData { values: &self.vals, defs: Some(&self.defs) },
            ],
        )
        .expect("write user_daily");
        self.total += self.rows;
        self.rows = 0;
        self.uid.clear();
        self.day.clear();
        self.sess.clear();
        self.evc.clear();
        self.rev.clear();
        self.cat.clear();
    }
    /// Writes what is left and closes the file (if any rows were written).
    fn finish(&mut self) -> usize {
        self.spill();
        if let Some(w) = self.w.take() {
            w.finish().expect("finish user_daily");
        }
        self.total
    }
}

struct Agg {
    ws: i64,
    cs: i64,
    day_min: i64,
    organic: u32,
    camp: CampTab,
    ccnt: Vec<u32>,
    active: Vec<u32>,
    apos: Vec<u32>,
    csum: Vec<i64>,
    touched: Vec<u32>,
    cat_rank: Vec<u32>,
    out: UOut,
    name: Vec<u8>,
}

impl Agg {
    #[inline]
    fn flush_row(&mut self, day: i64, sessions: i32, events: i32, rev: i64) {
        let mut best = NONE;
        let mut best_sum = 0i64;
        for &c in &self.touched {
            let s = self.csum[c as usize];
            self.csum[c as usize] = 0;
            if s > best_sum || (s == best_sum && self.cat_rank[c as usize] < self.cat_rank[best as usize]) {
                best = c;
                best_sum = s;
            }
        }
        self.touched.clear();
        let o = &mut self.out;
        o.rows += 1;
        o.uid.extend_from_slice(&(self.name.len() as u32).to_le_bytes());
        o.uid.extend_from_slice(&self.name);
        o.day.push(day as i32);
        o.sess.push(sessions);
        o.evc.push(events);
        o.rev.push(rev);
        o.cat.push(best);
        if o.rows == RG_ROWS {
            o.spill();
        }
    }

    /// One person: events (ts << 32 | pc) sorted, clicks sorted by (ts, click_id).
    fn person(&mut self, pkey: u64, evs: &[u64], cks: &[CK]) {
        self.name.clear();
        key_bytes(pkey, &mut self.name);
        let n = evs.len();
        let nc = cks.len();
        let (mut lo, mut hi) = (0usize, 0usize);
        let mut cur_day = i64::MIN;
        let (mut r_sess, mut r_ev, mut r_rev) = (0i32, 0i32, 0i64);
        let mut i = 0usize;
        while i < n {
            let srel = (evs[i] >> 32) as i64;
            let start = self.ws + srel;
            let day = start.div_euclid(DAY_MS);
            if day != cur_day {
                if cur_day != i64::MIN {
                    self.flush_row(cur_day, r_sess, r_ev, r_rev);
                }
                cur_day = day;
                r_sess = 0;
                r_ev = 0;
                r_rev = 0;
            }
            let mut prev = srel;
            let mut j = i;
            let mut rev = 0i64;
            let mut purchased = false;
            while j < n {
                let e = evs[j];
                let t = (e >> 32) as i64;
                if t - prev > GAP_MS {
                    break;
                }
                prev = t;
                let pc = e as u32;
                if pc != 0 {
                    let (price, cat) = ((pc >> 8) as i64, pc & 0xFF);
                    purchased = true;
                    rev += price;
                    let s = &mut self.csum[cat as usize];
                    if *s == 0 {
                        self.touched.push(cat);
                    }
                    *s += price;
                }
                j += 1;
            }
            r_sess += 1;
            r_ev += (j - i) as i32;
            r_rev += rev;
            i = j;

            let d = (day - self.day_min) as u32;
            let mut credit = self.organic;
            if nc != 0 {
                let cs = self.cs;
                while hi < nc && cs + cks[hi].ts as i64 <= start {
                    let c = cks[hi].camp as usize;
                    if self.ccnt[c] == 0 {
                        self.apos[c] = self.active.len() as u32;
                        self.active.push(c as u32);
                    }
                    self.ccnt[c] += 1;
                    hi += 1;
                }
                while lo < hi && cs + (cks[lo].ts as i64) < start - WEEK_MS {
                    let c = cks[lo].camp as usize;
                    self.ccnt[c] -= 1;
                    if self.ccnt[c] == 0 {
                        let p = self.apos[c] as usize;
                        let last = self.active.pop().unwrap();
                        if last as usize != c {
                            self.active[p] = last;
                            self.apos[last as usize] = p as u32;
                        }
                    }
                    lo += 1;
                }
                if lo < hi {
                    let top = cks[hi - 1].ts;
                    let mut k = hi - 1;
                    while k > lo && cks[k - 1].ts == top {
                        k -= 1;
                    }
                    credit = cks[k].camp;
                    self.camp.assist(&self.active, d);
                }
            }
            self.camp.credit(credit, d, purchased, rev);
        }
        if cur_day != i64::MIN {
            self.flush_row(cur_day, r_sess, r_ev, r_rev);
        }
        for &c in &self.active {
            self.ccnt[c as usize] = 0;
        }
        self.active.clear();
    }
}

/// Partition-local person table: key -> dense id.
struct Local {
    slots: Vec<u32>,
    mask: usize,
    keys: Vec<u64>,
}

impl Local {
    fn reset(&mut self, hint: usize) {
        let cap = (hint / 4).max(64).next_power_of_two();
        if self.slots.len() < cap {
            self.slots.resize(cap, 0);
        }
        self.slots[..cap].fill(0);
        self.mask = cap - 1;
        self.keys.clear();
    }
    fn grow(&mut self) {
        let cap = (self.mask + 1) * 2;
        if self.slots.len() < cap {
            self.slots.resize(cap, 0);
        }
        self.slots[..cap].fill(0);
        self.mask = cap - 1;
        for (i, &k) in self.keys.iter().enumerate() {
            let mut j = mix(k) as usize & self.mask;
            while self.slots[j] != 0 {
                j = (j + 1) & self.mask;
            }
            self.slots[j] = i as u32 + 1;
        }
    }
    #[inline(always)]
    fn get_or_insert(&mut self, k: u64) -> u32 {
        let mut j = mix(k) as usize & self.mask;
        loop {
            let s = unsafe { *self.slots.get_unchecked(j) };
            if s == 0 {
                break;
            }
            if unsafe { *self.keys.get_unchecked(s as usize - 1) } == k {
                return s - 1;
            }
            j = (j + 1) & self.mask;
        }
        let id = self.keys.len() as u32;
        self.keys.push(k);
        self.slots[j] = id + 1;
        if self.keys.len() * 2 > self.mask + 1 {
            self.grow();
        }
        id
    }
    #[inline(always)]
    fn get(&self, k: u64) -> u32 {
        let mut j = mix(k) as usize & self.mask;
        loop {
            let s = unsafe { *self.slots.get_unchecked(j) };
            if s == 0 {
                return NONE;
            }
            if unsafe { *self.keys.get_unchecked(s as usize - 1) } == k {
                return s - 1;
            }
            j = (j + 1) & self.mask;
        }
    }
}

#[derive(Default)]
struct CBufs {
    ids: Vec<u32>,
    cids: Vec<u32>,
    off: Vec<u32>,
    coff: Vec<u32>,
    evs: Vec<u64>,
    cks: Vec<CK>,
    tmp: Vec<u64>,
    segs: Vec<(Ptr<PRec>, usize)>,
    csegs: Vec<(Ptr<PCk>, usize)>,
}

/// Persons with at least this many events sort them by radix.
const RADIX_MIN: usize = 256;

/// Sorts v by its high 32 bits (ts): LSD radix, 11 bits per pass over ts - min.
fn radix_ts(v: &mut [u64], tmp: &mut Vec<u64>) {
    let n = v.len();
    let (mut lo, mut hi) = (u64::MAX, 0u64);
    for &x in v.iter() {
        let t = x >> 32;
        lo = lo.min(t);
        hi = hi.max(t);
    }
    let passes = ((64 - (hi - lo).leading_zeros()) as usize).div_ceil(11);
    if passes == 0 {
        return;
    }
    let mut h = [[0u32; 2048]; 3];
    for &x in v.iter() {
        let t = (x >> 32) - lo;
        h[0][(t & 2047) as usize] += 1;
        h[1][((t >> 11) & 2047) as usize] += 1;
        h[2][((t >> 22) & 2047) as usize] += 1;
    }
    for hp in h.iter_mut().take(passes) {
        let mut acc = 0u32;
        for x in hp.iter_mut() {
            let c = *x;
            *x = acc;
            acc += c;
        }
    }
    if tmp.len() < n {
        tmp.resize(n, 0);
    }
    let tmp = &mut tmp[..n];
    for (p, hp) in h.iter_mut().enumerate().take(passes) {
        let sh = 11 * p;
        let (src, dst): (&[u64], &mut [u64]) = if p % 2 == 0 { (&*v, &mut *tmp) } else { (&*tmp, &mut *v) };
        for &x in src {
            let d = &mut hp[((((x >> 32) - lo) >> sh) & 2047) as usize];
            unsafe { *dst.get_unchecked_mut(*d as usize) = x };
            *d += 1;
        }
    }
    if passes % 2 == 1 {
        v.copy_from_slice(tmp);
    }
}

unsafe fn do_part(q: usize, pes: &[Parts<PRec>], pcs: &[Parts<PCk>], local: &mut Local, b: &mut CBufs, agg: &mut Agg) {
    b.segs.clear();
    for p in pes {
        p.segs(q, &mut b.segs);
    }
    let n: usize = b.segs.iter().map(|s| s.1).sum();
    if n == 0 {
        return;
    }
    assert!(n < u32::MAX as usize);
    local.reset(n);
    b.ids.clear();
    b.off.clear();
    for &(ptr, len) in &b.segs {
        for e in std::slice::from_raw_parts(ptr.0, len) {
            let id = local.get_or_insert(e.person);
            if id as usize == b.off.len() {
                b.off.push(0);
            }
            *b.off.get_unchecked_mut(id as usize) += 1;
            b.ids.push(id);
        }
    }
    let np = b.off.len();
    // counts -> start offsets (off[np] = n)
    let mut acc = 0u32;
    for x in b.off.iter_mut() {
        let c = *x;
        *x = acc;
        acc += c;
    }
    b.off.push(acc);
    // every slot below n is written by the scatter: grow only, never refill
    if b.evs.len() < n {
        b.evs.resize(n, 0);
    }
    let mut k = 0usize;
    for &(ptr, len) in &b.segs {
        for e in std::slice::from_raw_parts(ptr.0, len) {
            let id = *b.ids.get_unchecked(k) as usize;
            k += 1;
            let pos = b.off.get_unchecked_mut(id);
            *b.evs.get_unchecked_mut(*pos as usize) = e.key;
            *pos += 1;
        }
    }
    // off[id] is now the end of person id; the start is off[id-1] (or 0)

    b.csegs.clear();
    for p in pcs {
        p.segs(q, &mut b.csegs);
    }
    b.cids.clear();
    b.coff.clear();
    b.coff.resize(np + 1, 0);
    let mut ncl = 0u32;
    for &(ptr, len) in &b.csegs {
        for c in std::slice::from_raw_parts(ptr.0, len) {
            let id = local.get(c.person);
            if id != NONE {
                b.coff[id as usize] += 1;
                ncl += 1;
            }
            b.cids.push(id);
        }
    }
    if ncl != 0 {
        let mut acc = 0u32;
        for x in b.coff.iter_mut() {
            let c = *x;
            *x = acc;
            acc += c;
        }
        if b.cks.len() < ncl as usize {
            b.cks.resize(ncl as usize, CK { cid: 0, ts: 0, camp: 0 });
        }
        let mut k = 0usize;
        for &(ptr, len) in &b.csegs {
            for c in std::slice::from_raw_parts(ptr.0, len) {
                let id = b.cids[k];
                k += 1;
                if id != NONE {
                    let pos = b.coff[id as usize];
                    b.coff[id as usize] = pos + 1;
                    b.cks[pos as usize] = CK { cid: c.cid, ts: c.ts, camp: c.camp };
                }
            }
        }
    }

    let mut es = 0usize;
    let mut cs = 0usize;
    for p in 0..np {
        let ee = b.off[p] as usize;
        let evs = &mut b.evs[es..ee];
        es = ee;
        if evs.len() >= RADIX_MIN {
            radix_ts(evs, &mut b.tmp);
        } else if evs.len() > 1 {
            evs.sort_unstable();
        }
        let cks: &[CK] = if ncl != 0 {
            let ce = b.coff[p] as usize;
            let c = &mut b.cks[cs..ce];
            cs = ce;
            if c.len() > 1 {
                c.sort_unstable_by(|x, y| x.ts.cmp(&y.ts).then_with(|| key_cmp(x.cid, y.cid)));
            }
            c
        } else {
            &[]
        };
        agg.person(local.keys[p], evs, cks);
    }
}

fn user_writer(path: &Path) -> pq::Writer {
    let schema = [
        ("user_id", pq::Kind::Str),
        ("day", pq::Kind::Date),
        ("sessions", pq::Kind::I32),
        ("events", pq::Kind::I32),
        ("revenue_cents", pq::Kind::I64),
        ("top_category", pq::Kind::OptStr),
    ];
    pq::Writer::create(path, &schema).expect("create user_daily file")
}

// ------------------------------------------------------------------ main

fn manifest_ts(txt: &str, key: &str) -> i64 {
    let pat = format!("\"{}\"", key);
    let i = txt.find(&pat).unwrap_or_else(|| panic!("manifest: no {}", key));
    let rest = &txt[i + pat.len()..];
    let c = rest.find(':').expect("manifest: no colon");
    let rest = &rest[c + 1..];
    let a = rest.find('"').expect("manifest: no value");
    let rest = &rest[a + 1..];
    let b = rest.find('"').expect("manifest: unterminated value");
    parse_ts(rest[..b].as_bytes()).unwrap_or_else(|| panic!("manifest: bad {}", key))
}

fn clean_dir(dir: &Path) {
    fs::create_dir_all(dir).unwrap_or_else(|e| panic!("mkdir {}: {}", dir.display(), e));
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            if e.file_name().to_string_lossy().ends_with(".parquet") {
                let _ = fs::remove_file(e.path());
            }
        }
    }
}

/// "rss MB / peak rss MB / minor faults" now (Linux), for the stage line.
fn mem_now() -> String {
    let rss = fs::read_to_string("/proc/self/statm").ok().and_then(|t| t.split_whitespace().nth(1).and_then(|x| x.parse::<u64>().ok()));
    let flt = fs::read_to_string("/proc/self/stat").ok().and_then(|t| t.rsplit(')').next().and_then(|r| r.split_whitespace().nth(7).and_then(|x| x.parse::<u64>().ok())));
    let hwm = fs::read_to_string("/proc/self/status").ok().and_then(|t| {
        t.lines().find(|l| l.starts_with("VmHWM:")).and_then(|l| l.split_whitespace().nth(1).and_then(|x| x.parse::<u64>().ok()))
    });
    match (rss, flt) {
        (Some(r), Some(f)) => format!("{}M/{}M/{}", r * 4096 >> 20, hwm.unwrap_or(0) >> 10, f),
        _ => "-".into(),
    }
}

fn bits_for(rows: u64, per: u64) -> u32 {
    let per = per.max(1);
    let parts = ((rows / per).max(1)).next_power_of_two().min(2048);
    parts.trailing_zeros()
}

fn main() {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let ncpu = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));

    let man = fs::read_to_string(data.join("manifest.json")).expect("read manifest.json");
    let ws = manifest_ts(&man, "window_start");
    let we = manifest_ts(&man, "window_end");
    // relative timestamps must fit u32 below DEAD
    if we <= ws || we.saturating_sub(ws).saturating_add(WEEK_MS) >= DEAD as i64 {
        legacy::run();
    }

    // products.csv -> category index; later rows win, like a dict
    let mut buf = Vec::new();
    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    let mut cat_ix: HashMap<Vec<u8>, u32> = HashMap::new();
    let mut prod_rows: Vec<(u64, u32)> = Vec::new();
    {
        let (lo, hi) = load_whole(&data.join("products.csv"), &mut buf);
        let base = buf.as_ptr();
        unsafe {
            scan::<2>(base, lo, hi, |f| {
                let (a, b) = f[0];
                if a == b {
                    return;
                }
                let name = std::slice::from_raw_parts(base.add(f[1].0), f[1].1 - f[1].0);
                let c = *cat_ix.entry(name.to_vec()).or_insert_with(|| {
                    cat_names.push(name.to_vec());
                    cat_names.len() as u32 - 1
                });
                prod_rows.push((key_of(base.add(a), b - a), c));
            });
        }
    }
    let unknown_cat = *cat_ix.entry(b"unknown".to_vec()).or_insert_with(|| {
        cat_names.push(b"unknown".to_vec());
        cat_names.len() as u32 - 1
    });
    if cat_names.len() > 256 {
        legacy::run();
    }

    let ud_dir = out_dir.join("user_daily");
    let cd_dir = out_dir.join("campaign_daily");
    clean_dir(&ud_dir);
    clean_dir(&cd_dir);

    let mut products = KeyMap::new(prod_rows.len());
    for &(k, c) in &prod_rows {
        products.insert(k, c);
    }
    let mut order: Vec<u32> = (0..cat_names.len() as u32).collect();
    order.sort_by(|&a, &b| cat_names[a as usize].cmp(&cat_names[b as usize]));
    let mut cat_rank = vec![0u32; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c as usize] = r as u32;
    }

    // campaigns.csv -> campaign index
    let mut camp_names: Vec<Vec<u8>> = Vec::new();
    let mut camp_chan: Vec<Vec<u8>> = Vec::new();
    let mut camp_ix: HashMap<u64, u32> = HashMap::new();
    {
        let (lo, hi) = load_whole(&data.join("campaigns.csv"), &mut buf);
        let base = buf.as_ptr();
        unsafe {
            scan::<2>(base, lo, hi, |f| {
                let (a, b) = f[0];
                if a == b {
                    return;
                }
                let k = key_of(base.add(a), b - a);
                let chan = std::slice::from_raw_parts(base.add(f[1].0), f[1].1 - f[1].0).to_vec();
                match camp_ix.get(&k) {
                    Some(&i) => camp_chan[i as usize] = chan,
                    None => {
                        camp_ix.insert(k, camp_names.len() as u32);
                        camp_names.push(std::slice::from_raw_parts(base.add(a), b - a).to_vec());
                        camp_chan.push(chan);
                    }
                }
            });
        }
    }
    let mut campaigns = KeyMap::new(camp_names.len());
    for (&k, &i) in &camp_ix {
        campaigns.insert(k, i);
    }
    let organic = match camp_names.iter().position(|n| n == b"organic") {
        Some(i) => i as u32,
        None => {
            camp_names.push(b"organic".to_vec());
            camp_chan.push(b"organic".to_vec());
            camp_names.len() as u32 - 1
        }
    };
    let ncamp = camp_names.len();

    let ev_files = csv_files(&data.join("events"));
    let ck_files = csv_files(&data.join("clicks"));
    let ln_files = csv_files(&data.join("links"));
    let ev_bytes: u64 = ev_files.iter().map(|f| f.1).sum();
    let ck_bytes: u64 = ck_files.iter().map(|f| f.1).sum();
    let est_ev = ev_bytes / 70 + 64;
    let est_ck = ck_bytes / 40 + 64;

    let nt = ncpu.min(((ev_bytes + ck_bytes) >> 18) as usize + 1).clamp(1, 1024);
    let cfg = Cfg {
        ws,
        we,
        cal: Cal::new(ws - WEEK_MS, we + WEEK_MS),
        products,
        unknown_cat,
        campaigns,
        pe_bits: bits_for(est_ev, 32768),
        pc_bits: bits_for(est_ck, 32768),
        q_bits: bits_for(est_ev, 65536),
    };
    let (npe, npc, nq) = (1usize << cfg.pe_bits, 1usize << cfg.pc_bits, 1usize << cfg.q_bits);

    let chunks = |files: &[(PathBuf, u64)], kind: Kind, tasks: &mut Vec<Task>| {
        for (fi, &(_, size)) in files.iter().enumerate() {
            let n = size.div_ceil(CHUNK);
            for c in 0..n {
                tasks.push(Task { kind, file: fi, s: size * c / n, e: size * (c + 1) / n });
            }
        }
    };
    let mut tasks: Vec<Task> = Vec::new();
    chunks(&ev_files, Kind::Events, &mut tasks);
    chunks(&ck_files, Kind::Clicks, &mut tasks);
    let mut ltasks: Vec<Task> = Vec::new();
    chunks(&ln_files, Kind::Events, &mut ltasks);
    let t_setup = t0.elapsed();
    let m_setup = mem_now();

    // ---- stage A: parse into id partitions; thread 0 first builds the links
    let links: OnceLock<Links> = OnceLock::new();
    let next = AtomicUsize::new(0);
    // slabs of about half a thread's share of the stage-A rows
    let slab_a = ((est_ev * 32 + est_ck * 32) as usize / nt).max(1 << 22) / 2;
    let mut pes: Vec<Parts<ERec>> = Vec::with_capacity(nt);
    let mut pcs: Vec<Parts<CRec>> = Vec::with_capacity(nt);
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nt)
            .map(|t| {
                let (cfg, tasks, next, links) = (&cfg, &tasks, &next, &links);
                let (ev_files, ck_files, ln_files, ltasks) = (&ev_files, &ck_files, &ln_files, &ltasks);
                sc.spawn(move || {
                    let mut buf: Vec<u8> = Vec::with_capacity(CHUNK as usize + 8192);
                    if t == 0 {
                        // about 20 bytes per link line: reserve instead of doubling
                        let mut pairs: Vec<(u64, u64)> = Vec::with_capacity((ln_files.iter().map(|f| f.1).sum::<u64>() / 18) as usize);
                        for tk in ltasks {
                            let (path, size) = &ln_files[tk.file];
                            let mut f = File::open(path).unwrap_or_else(|e| panic!("open {}: {}", path.display(), e));
                            let (lo, hi) = load_chunk(&mut f, tk.s, tk.e, *size, &mut buf);
                            if lo < hi {
                                unsafe { parse_links(buf.as_ptr(), lo, hi, &mut pairs) };
                            }
                        }
                        let _ = links.set(build_links(&pairs));
                    }
                    let arena = Arena::leak(slab_a);
                    let mut pe = Parts::<ERec>::new(npe, arena);
                    let mut pc = Parts::<CRec>::new(npc, arena);
                    loop {
                        let i = next.fetch_add(1, AO::Relaxed);
                        if i >= tasks.len() {
                            break;
                        }
                        let tk = &tasks[i];
                        let (path, size) = match tk.kind {
                            Kind::Events => &ev_files[tk.file],
                            Kind::Clicks => &ck_files[tk.file],
                        };
                        let mut f = File::open(path).unwrap_or_else(|e| panic!("open {}: {}", path.display(), e));
                        let (lo, hi) = load_chunk(&mut f, tk.s, tk.e, *size, &mut buf);
                        if lo >= hi {
                            continue;
                        }
                        let base = buf.as_ptr();
                        unsafe {
                            match tk.kind {
                                Kind::Events => parse_events(cfg, base, lo, hi, &mut pe),
                                Kind::Clicks => parse_clicks(cfg, base, lo, hi, i as u64, &mut pc),
                            }
                        }
                    }
                    pe.flush();
                    pc.flush();
                    (pe, pc)
                })
            })
            .collect();
        for h in hs {
            let (pe, pc) = h.join().expect("parse thread");
            pes.push(pe);
            pcs.push(pc);
        }
    });
    let links = links.into_inner().expect("links");
    let t_a = t0.elapsed();
    let m_a = mem_now();

    // ---- stage B: dedup per id partition, route survivors to person partitions
    let mut qes: Vec<Parts<PRec>> = Vec::with_capacity(nt);
    let mut qcs: Vec<Parts<PCk>> = Vec::with_capacity(nt);
    {
        let slab_b = ((est_ev * 16 + est_ck * 24) as usize / nt).max(1 << 22) / 4;
        let mut border: Vec<(usize, usize)> = (0..npe)
            .map(|p| (pes.iter().map(|x| x.len(p)).sum::<usize>(), p))
            .chain((0..npc).map(|p| (pcs.iter().map(|x| x.len(p)).sum::<usize>() / 2, npe + p)))
            .collect();
        border.sort_unstable_by(|a, b| b.cmp(a));
        let mut esegs: Vec<Vec<(Ptr<ERec>, usize)>> = vec![Vec::new(); npe];
        let mut csegs: Vec<Vec<(Ptr<CRec>, usize)>> = vec![Vec::new(); npc];
        for x in pes.iter() {
            for (p, v) in esegs.iter_mut().enumerate() {
                x.segs(p, v);
            }
        }
        for x in pcs.iter() {
            for (p, v) in csegs.iter_mut().enumerate() {
                x.segs(p, v);
            }
        }
        let next = AtomicUsize::new(0);
        let qb = cfg.q_bits;
        std::thread::scope(|sc| {
            let hs: Vec<_> = (0..nt)
                .map(|_| {
                    let (esegs, csegs, next, links, border) = (&esegs, &csegs, &next, &links, &border);
                    sc.spawn(move || {
                        // outputs go first into chunks of partitions this
                        // thread has consumed
                        let arena = Arena::leak(slab_b);
                        let mut oe = Parts::<PRec>::new(nq, arena);
                        let mut oc = Parts::<PCk>::new(nq, arena);
                        let mut tab: Vec<u64> = Vec::new();
                        loop {
                            let i = next.fetch_add(1, AO::Relaxed);
                            if i >= border.len() {
                                break;
                            }
                            let p = border[i].1;
                            // SAFETY: partition p's rows are touched by this task only.
                            unsafe {
                                if p < npe {
                                    dedup_ev(&esegs[p], &mut tab);
                                    route_ev(&esegs[p], links, qb, &mut oe);
                                    (*arena).free.extend(esegs[p].iter().map(|s| s.0 .0 as *mut u8));
                                } else {
                                    dedup_ck(&csegs[p - npe], &mut tab);
                                    route_ck(&csegs[p - npe], links, qb, &mut oc);
                                    (*arena).free.extend(csegs[p - npe].iter().map(|s| s.0 .0 as *mut u8));
                                }
                            }
                        }
                        oe.flush();
                        oc.flush();
                        (oe, oc)
                    })
                })
                .collect();
            for h in hs {
                let (oe, oc) = h.join().expect("dedup thread");
                qes.push(oe);
                qcs.push(oc);
            }
        });
    }
    let t_b = t0.elapsed();
    let m_b = mem_now();

    // ---- stage C: sessionize per person partition, then write user_daily
    let day_min = ws.div_euclid(DAY_MS);
    let ndays = (((we - 1).div_euclid(DAY_MS) - day_min + 1).max(1)) as usize;
    let dense = ncamp.saturating_mul(ndays) <= (1 << 22);
    let mut order: Vec<(usize, usize)> = (0..nq).map(|q| (qes.iter().map(|c| c.len(q)).sum::<usize>(), q)).collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
    let next = AtomicUsize::new(0);
    let mut camp_tabs: Vec<CampTab> = Vec::new();
    let mut total_rows = 0usize;
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nt)
            .map(|t| {
                let (next, qes, qcs, order, cat_rank, cat_names, ud_dir) =
                    (&next, &qes, &qcs, &order, &cat_rank, &cat_names, &ud_dir);
                sc.spawn(move || {
                    let mut agg = Agg {
                        ws,
                        cs: ws - WEEK_MS,
                        day_min,
                        organic,
                        camp: if dense {
                            CampTab::Dense(vec![Cred::default(); ncamp * ndays], vec![0; ncamp * ndays], ncamp)
                        } else {
                            CampTab::Sparse(HashMap::new())
                        },
                        ccnt: vec![0; ncamp],
                        active: Vec::new(),
                        apos: vec![0; ncamp],
                        csum: vec![0; 256],
                        touched: Vec::new(),
                        cat_rank: cat_rank.clone(),
                        out: UOut::new(ud_dir.join(format!("part-{}.parquet", t)), cat_names.clone()),
                        name: Vec::new(),
                    };
                    let mut local = Local { slots: Vec::new(), mask: 0, keys: Vec::new() };
                    let mut bufs = CBufs::default();
                    loop {
                        let i = next.fetch_add(1, AO::Relaxed);
                        if i >= order.len() {
                            break;
                        }
                        unsafe { do_part(order[i].1, qes, qcs, &mut local, &mut bufs, &mut agg) };
                    }
                    let rows = agg.out.finish();
                    (agg.camp, rows)
                })
            })
            .collect();
        for h in hs {
            let (c, r) = h.join().expect("session thread");
            camp_tabs.push(c);
            total_rows += r;
        }
    });
    if total_rows == 0 {
        user_writer(&ud_dir.join("part-0.parquet")).finish().expect("finish user_daily");
    }
    let t_d = t0.elapsed();
    let m_d = mem_now();

    // ---- campaign_daily
    let mut merged: Vec<((u32, u32), CampAgg)> = Vec::new();
    if dense {
        let mut sum = vec![CampAgg::default(); ncamp * ndays];
        for t in &camp_tabs {
            if let CampTab::Dense(v, a, _) = t {
                for ((s, x), &y) in sum.iter_mut().zip(v.iter()).zip(a.iter()) {
                    s.sessions += x.sessions as i64;
                    s.purchasing += x.purchasing as i64;
                    s.revenue += x.revenue;
                    s.assisted += y as i64;
                }
            }
        }
        for (i, s) in sum.iter().enumerate() {
            if s.sessions != 0 || s.assisted != 0 {
                merged.push((((i % ncamp) as u32, (i / ncamp) as u32), *s));
            }
        }
    } else {
        let mut m: HashMap<(u32, u32), CampAgg> = HashMap::new();
        for t in camp_tabs {
            if let CampTab::Sparse(v) = t {
                for (k, x) in v {
                    let s = m.entry(k).or_default();
                    s.sessions += x.sessions;
                    s.purchasing += x.purchasing;
                    s.revenue += x.revenue;
                    s.assisted += x.assisted;
                }
            }
        }
        merged.extend(m.into_iter().filter(|(_, s)| s.sessions != 0 || s.assisted != 0));
    }
    {
        let schema = [
            ("campaign_id", pq::Kind::Str),
            ("channel", pq::Kind::Str),
            ("day", pq::Kind::Date),
            ("sessions", pq::Kind::I64),
            ("purchasing_sessions", pq::Kind::I64),
            ("revenue_cents", pq::Kind::I64),
            ("assisted_sessions", pq::Kind::I64),
        ];
        let (mut id, mut ch) = (Vec::new(), Vec::new());
        let mut day: Vec<i32> = Vec::new();
        let (mut se, mut pu, mut re, mut asst) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for &((c, d), s) in &merged {
            let (n, h) = (&camp_names[c as usize], &camp_chan[c as usize]);
            id.extend_from_slice(&(n.len() as u32).to_le_bytes());
            id.extend_from_slice(n);
            ch.extend_from_slice(&(h.len() as u32).to_le_bytes());
            ch.extend_from_slice(h);
            day.push((day_min + d as i64) as i32);
            se.push(s.sessions);
            pu.push(s.purchasing);
            re.push(s.revenue);
            asst.push(s.assisted);
        }
        let mut w = pq::Writer::create(&cd_dir.join("part-0.parquet"), &schema).expect("create campaign_daily file");
        w.row_group(
            merged.len(),
            &[
                pq::ColData { values: &id, defs: None },
                pq::ColData { values: &ch, defs: None },
                pq::ColData { values: pq::bytes_of(&day), defs: None },
                pq::ColData { values: pq::bytes_of(&se), defs: None },
                pq::ColData { values: pq::bytes_of(&pu), defs: None },
                pq::ColData { values: pq::bytes_of(&re), defs: None },
                pq::ColData { values: pq::bytes_of(&asst), defs: None },
            ],
        )
        .expect("write campaign_daily");
        w.finish().expect("finish campaign_daily");
    }
    let t_e = t0.elapsed();
    let ms = |d: std::time::Duration| d.as_secs_f64() * 1e3;
    eprintln!(
        "threads {} parts {}/{}/{} | setup {:.0} parse {:.0} dedup+route {:.0} sessions+write {:.0} campaigns {:.0} | total {:.0} ms | user rows {} campaign rows {} | avx512 {}",
        nt, npe, npc, nq,
        ms(t_setup), ms(t_a - t_setup), ms(t_b - t_a), ms(t_d - t_b), ms(t_e - t_d), ms(t_e),
        total_rows, merged.len(), avx512() as u8
    );
    eprintln!("mem (rss/peak/minflt) setup {} parse {} dedup+route {} sessions+write {} end {}", m_setup, m_a, m_b, m_d, mem_now());
    if let Ok(st) = fs::read_to_string("/proc/self/stat") {
        // minor faults, user and system CPU ticks: how much time the kernel took
        let f: Vec<&str> = st.rsplit(')').next().unwrap_or("").split_whitespace().collect();
        if f.len() > 13 {
            eprintln!("minflt {} utime {} stime {}", f[7], f[11], f[12]);
        }
    }
    // Skip destructors: unmapping gigabytes of buffers is pure wall time.
    std::process::exit(0);
}
