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
const RG_ROWS: usize = 1 << 20;
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
unsafe fn scan<const NF: usize>(base: *const u8, lo: usize, hi: usize, mut row: impl FnMut(&[(usize, usize); NF])) {
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
        if q != 0 {
            let sp = c | nlb;
            let qs = q & ((c << 1) | 1);
            let qe = q & (sp >> 1);
            let ns = below & !c;
            let mapped = (ns.wrapping_add(qs) ^ ns) & sp;
            if (q & !(qs | qe)) | (qs & qe) | (mapped ^ (qe << 1)) != 0 {
                pos = next;
                continue;
            }
        }
        let mut f = [(0usize, 0usize); NF];
        let mut a = 0u32;
        let mut cc = c;
        for (i, x) in f.iter_mut().enumerate() {
            let e = if i + 1 < NF {
                let t = cc.trailing_zeros();
                cc &= cc - 1;
                t
            } else {
                nl
            };
            let qb = ((q >> a) & 1) as u32;
            *x = (pos + (a + qb) as usize, pos + (e - qb) as usize);
            a = e + 1;
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

fn read_range(f: &mut File, from: u64, to: u64, buf: &mut Vec<u8>) {
    let n = (to - from) as usize;
    let old = buf.len();
    buf.reserve(n + 256);
    f.seek(SeekFrom::Start(from)).expect("seek");
    buf.resize(old + n, 0);
    f.read_exact(&mut buf[old..]).expect("read");
}

#[inline]
fn find_nl(s: &[u8]) -> Option<usize> {
    s.iter().position(|&b| b == b'\n')
}

/// Loads the lines of `f` that start in [s, e) into `buf`, minus the header.
/// Returns (lo, hi) for scan.
fn load_chunk(f: &mut File, s: u64, e: u64, fsize: u64, buf: &mut Vec<u8>) -> (usize, usize) {
    buf.clear();
    if fsize == 0 || s >= e {
        return (0, 0);
    }
    let rs = if s == 0 { 0 } else { s - 1 };
    let mut rend = (e + 512).min(fsize);
    read_range(f, rs, rend, buf);
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
        if let Some(i) = find_nl(&buf[from..]) {
            break from + i;
        }
        if rend >= fsize {
            break buf.len();
        }
        from = buf.len();
        let nend = (rend + (rend - rs).max(4096)).min(fsize);
        read_range(f, rend, nend, buf);
        rend = nend;
    };
    if hi == buf.len() {
        buf.push(b'\n');
    }
    buf.extend_from_slice(&[0u8; 192]);
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

/// Stage A click row (valid clicks only). ts: ms since window_start - 7 days.
/// camp: campaign index, DEAD after dedup for a repeated copy.
#[derive(Clone, Copy)]
#[repr(C)]
struct CRec {
    cid: u64,
    user: u64,
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

/// One thread's rows, partitioned: partition p fills buf[start[p]..end[p]),
/// then spills to over[p].
struct Parts<T: Copy> {
    buf: Vec<T>,
    ce: Vec<(u32, u32)>,
    start: Vec<u32>,
    over: Vec<Vec<T>>,
}

unsafe impl<T: Copy> Send for Parts<T> {}
unsafe impl<T: Copy> Sync for Parts<T> {}

impl<T: Copy> Parts<T> {
    fn new(np: usize, per: usize) -> Parts<T> {
        let per = per.min((u32::MAX as usize - 1) / np.max(1));
        let buf = Vec::with_capacity(np * per);
        Parts {
            buf,
            ce: (0..np).map(|p| ((p * per) as u32, ((p + 1) * per) as u32)).collect(),
            start: (0..np).map(|p| (p * per) as u32).collect(),
            over: (0..np).map(|_| Vec::new()).collect(),
        }
    }
    #[inline(always)]
    fn push(&mut self, p: usize, v: T) {
        unsafe {
            let ce = self.ce.get_unchecked_mut(p);
            if ce.0 < ce.1 {
                // buf's capacity covers every region; only written slots are read
                self.buf.as_mut_ptr().add(ce.0 as usize).write(v);
                ce.0 += 1;
            } else {
                self.over.get_unchecked_mut(p).push(v);
            }
        }
    }
    fn len(&self, p: usize) -> usize {
        (self.ce[p].0 - self.start[p]) as usize + self.over[p].len()
    }
    /// The two written slices of partition p, writable.
    fn segs_mut(&mut self, p: usize, out: &mut Vec<(Ptr<T>, usize)>) {
        let s = self.start[p] as usize;
        let n = self.ce[p].0 as usize - s;
        if n > 0 {
            out.push((Ptr(unsafe { self.buf.as_mut_ptr().add(s) }), n));
        }
        let o = &mut self.over[p];
        if !o.is_empty() {
            out.push((Ptr(o.as_mut_ptr()), o.len()));
        }
    }
    /// The two written slices of partition p, read-only.
    fn segs(&self, p: usize, out: &mut Vec<(Ptr<T>, usize)>) {
        let s = self.start[p] as usize;
        let n = self.ce[p].0 as usize - s;
        if n > 0 {
            out.push((Ptr(unsafe { self.buf.as_ptr().add(s) as *mut T }), n));
        }
        let o = &self.over[p];
        if !o.is_empty() {
            out.push((Ptr(o.as_ptr() as *mut T), o.len()));
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

static EXP: LazyLock<u32> = LazyLock::new(|| std::env::var("EXP").ok().and_then(|s| s.parse().ok()).unwrap_or(0));
unsafe fn parse_events(cfg: &Cfg, base: *const u8, lo: usize, hi: usize, out: &mut Parts<ERec>) {
    let (ws, we) = (cfg.ws, cfg.we);
    let exp = *EXP;
    if exp == 3 { return; }
    if exp == 2 { let mut x = 0usize; scan::<7>(base, lo, hi, |f| { x = x.wrapping_add(f[0].0 ^ f[6].1); }); std::hint::black_box(x); return; }
    const VIEW: u32 = u32::from_le_bytes(*b"view");
    const CART: u32 = u32::from_le_bytes(*b"cart");
    const PURCHASE: u64 = u64::from_le_bytes(*b"purchase");
    scan::<7>(base, lo, hi, |f| {
        let (a, b) = f[0];
        if a == b {
            return;
        }
        let eid = key_of(base.add(a), b - a);
        let ing = cfg.cal.parse(base.add(f[3].0), f[3].1 - f[3].0).unwrap_or(i64::MAX);
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
            if !purchase && !(tl == 4 && (rd32(base.add(ta)) == VIEW || rd32(base.add(ta)) == CART)) {
                break 'v;
            }
            let ts = match cfg.cal.parse(base.add(f[2].0), f[2].1 - f[2].0) {
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
            r.user = key_of(base.add(ua), ub - ua);
            r.ts = (ts - ws) as u32;
        }
        if exp == 1 { std::hint::black_box(r); return; }
        out.push(part_of(mix(eid), cfg.pe_bits), r);
    });
}

/// Invalid clicks are dropped here: copies of a click have identical values,
/// so every copy of an invalid click is invalid.
unsafe fn parse_clicks(cfg: &Cfg, base: *const u8, lo: usize, hi: usize, out: &mut Parts<CRec>) {
    let (cs, we) = (cfg.ws - WEEK_MS, cfg.we);
    scan::<4>(base, lo, hi, |f| {
        let (a, b) = f[0];
        let (ua, ub) = f[1];
        let (ca, cb) = f[2];
        if a == b || ua == ub || ca == cb {
            return;
        }
        let camp = match key_find(base.add(ca), cb - ca).and_then(|k| cfg.campaigns.get(k)) {
            Some(c) => c,
            None => return,
        };
        let ts = match cfg.cal.parse(base.add(f[3].0), f[3].1 - f[3].0) {
            Some(t) if t >= cs && t < we => t,
            _ => return,
        };
        let cid = key_of(base.add(a), b - a);
        let user = key_of(base.add(ua), ub - ua);
        out.push(part_of(mix(cid), cfg.pc_bits), CRec { cid, user, ts: (ts - cs) as u32, camp });
    });
}

// ------------------------------------------------------------------ links

/// user ID -> person name (the byte-wise smallest ID of its component).
struct Links {
    slots: Vec<(u64, u64)>,
    mask: usize,
}

impl Links {
    #[inline(always)]
    fn prefetch(&self, h: u64) {
        if !self.slots.is_empty() {
            prefetch(unsafe { self.slots.as_ptr().add(h as usize & self.mask) });
        }
    }
    #[inline(always)]
    fn lookup(&self, k: u64, h: u64) -> u64 {
        if self.slots.is_empty() {
            return k;
        }
        let mut j = h as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(j) };
            if s.0 == k {
                return s.1;
            }
            if s.0 == 0 {
                return k;
            }
            j = (j + 1) & self.mask;
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
        return Links { slots: Vec::new(), mask: 0 };
    }
    let cap = (np * 2 + np / 2).max(16).next_power_of_two();
    let mask = cap - 1;
    let mut slots = vec![(0u64, 0u64); cap];
    let mut keys: Vec<u64> = Vec::with_capacity(np + np / 2);
    let mut slotpos: Vec<u32> = Vec::with_capacity(keys.capacity());
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
                        slotpos.push(j as u32);
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
    for i in 0..keys.len() as u32 {
        let r = find(&mut parent, i);
        slots[slotpos[i as usize] as usize].1 = keys[r as usize];
    }
    Links { slots, mask }
}

// ------------------------------------------------------------------ stage B

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

/// Marks every copy of a click but the first DEAD.
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
                        (*r).camp = DEAD;
                        break;
                    }
                }
                j = (j + 1) & mask;
            }
        }
    }
}

const LOOK: usize = 8;

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
            let hu = mix(r.user);
            let person = links.lookup(r.user, hu);
            let hp = if person == r.user { hu } else { mix(person) };
            out.push(part_of(hp, qb), PRec { person, key: ((r.ts as u64) << 32) | r.pc as u64 });
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
            let hu = mix(r.user);
            let person = links.lookup(r.user, hu);
            let hp = if person == r.user { hu } else { mix(person) };
            out.push(part_of(hp, qb), PCk { person, cid: r.cid, ts: r.ts, camp: r.camp });
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

enum CampTab {
    Dense(Vec<CampAgg>, usize),
    Sparse(HashMap<(u32, u32), CampAgg>),
}

impl CampTab {
    #[inline(always)]
    fn at(&mut self, camp: u32, day: u32) -> &mut CampAgg {
        match self {
            CampTab::Dense(v, nd) => unsafe { v.get_unchecked_mut(camp as usize * *nd + day as usize) },
            CampTab::Sparse(m) => m.entry((camp, day)).or_default(),
        }
    }
}

#[derive(Default)]
struct UOut {
    rows: usize,
    uid: Vec<u8>,
    marks: Vec<usize>,
    day: Vec<i32>,
    sess: Vec<i32>,
    evc: Vec<i32>,
    rev: Vec<i64>,
    cat: Vec<u32>,
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
        if o.rows % RG_ROWS == 0 {
            o.marks.push(o.uid.len());
        }
        o.rows += 1;
        o.uid.extend_from_slice(&(self.name.len() as u32).to_le_bytes());
        o.uid.extend_from_slice(&self.name);
        o.day.push(day as i32);
        o.sess.push(sessions);
        o.evc.push(events);
        o.rev.push(rev);
        o.cat.push(best);
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
                    for a in 0..self.active.len() {
                        let c = self.active[a];
                        self.camp.at(c, d).assisted += 1;
                    }
                }
            }
            let r = self.camp.at(credit, d);
            r.sessions += 1;
            r.purchasing += purchased as i64;
            r.revenue += rev;
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
    segs: Vec<(Ptr<PRec>, usize)>,
    csegs: Vec<(Ptr<PCk>, usize)>,
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
    b.evs.clear();
    b.evs.resize(n, 0);
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
        b.cks.clear();
        b.cks.resize(ncl as usize, CK { cid: 0, ts: 0, camp: 0 });
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
        if evs.len() > 1 {
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

fn write_user(path: &Path, o: &UOut, cat_names: &[Vec<u8>]) {
    let schema = [
        ("user_id", pq::Kind::Str),
        ("day", pq::Kind::Date),
        ("sessions", pq::Kind::I32),
        ("events", pq::Kind::I32),
        ("revenue_cents", pq::Kind::I64),
        ("top_category", pq::Kind::OptStr),
    ];
    let mut w = pq::Writer::create(path, &schema).expect("create user_daily file");
    let mut defs: Vec<u8> = Vec::new();
    let mut vals: Vec<u8> = Vec::new();
    let mut g = 0usize;
    while g * RG_ROWS < o.rows {
        let a = g * RG_ROWS;
        let b = (a + RG_ROWS).min(o.rows);
        let ua = o.marks[g];
        let ub = if g + 1 < o.marks.len() { o.marks[g + 1] } else { o.uid.len() };
        defs.clear();
        defs.resize((b - a).div_ceil(8), 0);
        vals.clear();
        for (i, &c) in o.cat[a..b].iter().enumerate() {
            if c != NONE {
                defs[i >> 3] |= 1 << (i & 7);
                let name = &cat_names[c as usize];
                vals.extend_from_slice(&(name.len() as u32).to_le_bytes());
                vals.extend_from_slice(name);
            }
        }
        w.row_group(
            b - a,
            &[
                pq::ColData { values: &o.uid[ua..ub], defs: None },
                pq::ColData { values: pq::bytes_of(&o.day[a..b]), defs: None },
                pq::ColData { values: pq::bytes_of(&o.sess[a..b]), defs: None },
                pq::ColData { values: pq::bytes_of(&o.evc[a..b]), defs: None },
                pq::ColData { values: pq::bytes_of(&o.rev[a..b]), defs: None },
                pq::ColData { values: &vals, defs: Some(&defs) },
            ],
        )
        .expect("write user_daily");
        g += 1;
    }
    w.finish().expect("finish user_daily");
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

    // ---- stage A: parse into id partitions; thread 0 first builds the links
    let links: OnceLock<Links> = OnceLock::new();
    let next = AtomicUsize::new(0);
    // a thread's share of rows, with headroom; overflow spills
    let share = |est: u64, np: usize| (est as usize / nt) * 6 / 5 / np + 16;
    let (per_e, per_c) = (share(est_ev, npe), share(est_ck, npc));
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
                        let mut pairs: Vec<(u64, u64)> = Vec::new();
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
                    let mut pe = Parts::<ERec>::new(npe, per_e);
                    let mut pc = Parts::<CRec>::new(npc, per_c);
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
                                Kind::Clicks => parse_clicks(cfg, base, lo, hi, &mut pc),
                            }
                        }
                    }
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

    // ---- stage B: dedup per id partition, route survivors to person partitions
    let mut qes: Vec<Parts<PRec>> = Vec::with_capacity(nt);
    let mut qcs: Vec<Parts<PCk>> = Vec::with_capacity(nt);
    {
        let (per_qe, per_qc) = (share(est_ev, nq), share(est_ck, nq));
        let mut border: Vec<(usize, usize)> = (0..npe)
            .map(|p| (pes.iter().map(|x| x.len(p)).sum::<usize>(), p))
            .chain((0..npc).map(|p| (pcs.iter().map(|x| x.len(p)).sum::<usize>() / 2, npe + p)))
            .collect();
        border.sort_unstable_by(|a, b| b.cmp(a));
        let mut esegs: Vec<Vec<(Ptr<ERec>, usize)>> = vec![Vec::new(); npe];
        let mut csegs: Vec<Vec<(Ptr<CRec>, usize)>> = vec![Vec::new(); npc];
        for x in pes.iter_mut() {
            for (p, v) in esegs.iter_mut().enumerate() {
                x.segs_mut(p, v);
            }
        }
        for x in pcs.iter_mut() {
            for (p, v) in csegs.iter_mut().enumerate() {
                x.segs_mut(p, v);
            }
        }
        let next = AtomicUsize::new(0);
        let qb = cfg.q_bits;
        std::thread::scope(|sc| {
            let hs: Vec<_> = (0..nt)
                .map(|_| {
                    let (esegs, csegs, next, links, border) = (&esegs, &csegs, &next, &links, &border);
                    sc.spawn(move || {
                        let mut oe = Parts::<PRec>::new(nq, per_qe);
                        let mut oc = Parts::<PCk>::new(nq, per_qc);
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
                                } else {
                                    dedup_ck(&csegs[p - npe], &mut tab);
                                    route_ck(&csegs[p - npe], links, qb, &mut oc);
                                }
                            }
                        }
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
                            CampTab::Dense(vec![CampAgg::default(); ncamp * ndays], ndays)
                        } else {
                            CampTab::Sparse(HashMap::new())
                        },
                        ccnt: vec![0; ncamp],
                        active: Vec::new(),
                        apos: vec![0; ncamp],
                        csum: vec![0; 256],
                        touched: Vec::new(),
                        cat_rank: cat_rank.clone(),
                        out: UOut::default(),
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
                    if agg.out.rows > 0 {
                        write_user(&ud_dir.join(format!("part-{}.parquet", t)), &agg.out, cat_names);
                    }
                    (agg.camp, agg.out.rows)
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
        write_user(&ud_dir.join("part-0.parquet"), &UOut::default(), &cat_names);
    }
    let t_d = t0.elapsed();

    // ---- campaign_daily
    let mut merged: Vec<((u32, u32), CampAgg)> = Vec::new();
    if dense {
        let mut sum = vec![CampAgg::default(); ncamp * ndays];
        for t in &camp_tabs {
            if let CampTab::Dense(v, _) = t {
                for (s, x) in sum.iter_mut().zip(v.iter()) {
                    s.sessions += x.sessions;
                    s.purchasing += x.purchasing;
                    s.revenue += x.revenue;
                    s.assisted += x.assisted;
                }
            }
        }
        for (i, s) in sum.iter().enumerate() {
            if s.sessions != 0 || s.assisted != 0 {
                merged.push((((i / ndays) as u32, (i % ndays) as u32), *s));
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
        "threads {} parts {}/{}/{} | setup {:.0} parse {:.0} dedup+route {:.0} sessions+write {:.0} campaigns {:.0} | total {:.0} ms | user rows {} campaign rows {}",
        nt, npe, npc, nq,
        ms(t_setup), ms(t_a - t_setup), ms(t_b - t_a), ms(t_d - t_b), ms(t_e - t_d), ms(t_e),
        total_rows, merged.len()
    );
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
