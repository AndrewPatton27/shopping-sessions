//! Previous pipeline, kept as the exact fallback for datasets the fast
//! pipeline does not cover (windows over ~49 days, more than 256 categories).
//!
//! Stages (each one parallel over NCPU threads):
//!   A  thread 0 builds the link table (user -> person) while the others
//!      parse; each parsed row is mapped to its person at once (rows parsed
//!      before the table is ready are queued, then mapped) and its payload is
//!      written to its person's partition, its dedup record to hash(id)'s
//!   B  dedup per id partition, marking losing copies dead in the payload
//!   D  per person partition: group by person, sort, sessionize, attribute,
//!      aggregate; write Parquet (one user_daily file per thread)

#![allow(dead_code)]
use crate::pq;
use crate::ts;

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
const INVALID: u32 = u32::MAX;
const RG_ROWS: usize = 1 << 20;
const NONE: u32 = u32::MAX;

// ------------------------------------------------------------------ keys
//
// An ID is carried as a u128 key. IDs of 1..=15 bytes are packed exactly:
// bytes big-endian from the top, length in the low byte, so integer order is
// byte-wise string order. Longer user/click/product/campaign IDs are interned
// in a global table (low byte 0xFE, exact); longer event IDs, which are only
// ever compared for equality, become a 120-bit hash (low byte 0xFF).

const LONG_ID: u8 = 0xFE;

#[inline(always)]
unsafe fn pack_short(p: *const u8, len: usize) -> u128 {
    let x = u128::from_be_bytes(std::ptr::read_unaligned(p as *const [u8; 16]));
    (x & (!0u128 << (8 * (16 - len)))) | len as u128
}

struct Interner {
    map: HashMap<Box<[u8]>, u32>,
    strs: Vec<Box<[u8]>>,
}

static INTERN: LazyLock<Mutex<Interner>> =
    LazyLock::new(|| Mutex::new(Interner { map: HashMap::new(), strs: Vec::new() }));

thread_local! {
    static LCACHE: RefCell<HashMap<Box<[u8]>, u32>> = RefCell::new(HashMap::new());
}

#[inline]
fn long_key(i: u32) -> u128 {
    ((i as u128) << 8) | LONG_ID as u128
}

#[cold]
#[inline(never)]
fn intern_long(s: &[u8]) -> u128 {
    LCACHE.with(|c| {
        let mut c = c.borrow_mut();
        if let Some(&i) = c.get(s) {
            return long_key(i);
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
        c.insert(s.into(), i);
        long_key(i)
    })
}

#[cold]
#[inline(never)]
fn find_long(s: &[u8]) -> Option<u128> {
    INTERN.lock().unwrap().map.get(s).map(|&i| long_key(i))
}

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let m = (a as u128) * (b as u128);
    (m as u64) ^ ((m >> 64) as u64)
}

#[cold]
#[inline(never)]
fn hash_long(s: &[u8]) -> u128 {
    let one = |seed: u64| -> u64 {
        let mut h = seed ^ (s.len() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut it = s.chunks_exact(8);
        for c in &mut it {
            let x = u64::from_le_bytes(c.try_into().unwrap());
            h = mum(h ^ x, 0xA076_1D64_78BD_642F).wrapping_add(0xE703_7ED1_A0B4_28DB);
        }
        let r = it.remainder();
        let mut tail = [0u8; 8];
        tail[..r.len()].copy_from_slice(r);
        h = mum(h ^ u64::from_le_bytes(tail), 0x8EBC_6AF0_9C88_C6E3);
        mum(h, 0x5899_65CC_7537_4CC3 ^ seed)
    };
    let a = one(0x243F_6A88_85A3_08D3);
    let b = one(0x1319_8A2E_0370_7344);
    (((a as u128) << 64) | b as u128) | 0xFF
}

/// Exact key of a non-empty ID (interns long ones).
#[inline(always)]
unsafe fn id_key(p: *const u8, len: usize) -> u128 {
    if len <= 15 {
        pack_short(p, len)
    } else {
        intern_long(std::slice::from_raw_parts(p, len))
    }
}

/// Exact key of a non-empty ID, None if it is long and was never interned.
#[inline(always)]
unsafe fn id_key_find(p: *const u8, len: usize) -> Option<u128> {
    if len <= 15 {
        Some(pack_short(p, len))
    } else {
        find_long(std::slice::from_raw_parts(p, len))
    }
}

/// Equality-only key of a non-empty ID.
#[inline(always)]
unsafe fn eq_key(p: *const u8, len: usize) -> u128 {
    if len <= 15 {
        pack_short(p, len)
    } else {
        hash_long(std::slice::from_raw_parts(p, len))
    }
}

fn key_bytes(k: u128, out: &mut Vec<u8>) {
    let tag = k as u8;
    if tag < LONG_ID {
        out.extend_from_slice(&k.to_be_bytes()[..tag as usize]);
    } else {
        let g = INTERN.lock().unwrap();
        out.extend_from_slice(&g.strs[(k >> 8) as usize]);
    }
}

/// Byte-wise order of the IDs behind two exact keys.
#[inline(always)]
fn key_cmp(a: u128, b: u128) -> Ordering {
    if (a as u8) < LONG_ID && (b as u8) < LONG_ID {
        a.cmp(&b)
    } else {
        key_cmp_slow(a, b)
    }
}

#[cold]
#[inline(never)]
fn key_cmp_slow(a: u128, b: u128) -> Ordering {
    let (mut x, mut y) = (Vec::new(), Vec::new());
    key_bytes(a, &mut x);
    key_bytes(b, &mut y);
    x.cmp(&y)
}

#[inline(always)]
fn hk(k: u128) -> u64 {
    mum((k >> 64) as u64 ^ 0x9E37_79B9_7F4A_7C15, k as u64 ^ 0xD1B5_4A32_D192_ED03)
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

/// Small open-addressing map key -> u32 (products, campaigns).
struct KeyMap {
    slots: Vec<(u128, u32)>,
    mask: usize,
}

impl KeyMap {
    fn new(n: usize) -> KeyMap {
        let cap = (n * 2).max(16).next_power_of_two();
        KeyMap { slots: vec![(0, 0); cap], mask: cap - 1 }
    }
    fn insert(&mut self, k: u128, v: u32) {
        let mut j = hk(k) as usize & self.mask;
        loop {
            if self.slots[j].0 == 0 || self.slots[j].0 == k {
                self.slots[j] = (k, v);
                return;
            }
            j = (j + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, k: u128) -> Option<u32> {
        let mut j = hk(k) as usize & self.mask;
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
// starts and ends with one (checked by counting: quotes == 2 * wrapped fields).

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
/// base[hi] == b'\n' and 128 readable bytes after it (the timestamp parser
/// reads up to 16 bytes from the start of a field).
#[inline(always)]
unsafe fn scan_lines<const NF: usize>(
    base: *const u8,
    lo: usize,
    hi: usize,
    mut row: impl FnMut(&[(usize, usize); NF]),
) {
    let mut pos = lo;
    while pos < hi {
        let mut ends = [0usize; NF];
        let mut nf = 0usize;
        let mut qc = 0u32;
        let mut off = pos;
        let nl;
        loop {
            let (cm, nm, qm) = masks64(base.add(off));
            let below = if nm != 0 { (nm & nm.wrapping_neg()) - 1 } else { !0 };
            let mut c = cm & below;
            qc += (qm & below).count_ones();
            while c != 0 {
                if nf < NF - 1 {
                    ends[nf] = off + c.trailing_zeros() as usize;
                }
                nf += 1;
                c &= c - 1;
            }
            if nm != 0 {
                nl = off + nm.trailing_zeros() as usize;
                break;
            }
            off += 64;
        }
        let start = pos;
        pos = nl + 1;
        if nf != NF - 1 {
            continue;
        }
        ends[NF - 1] = nl;
        let mut f = [(0usize, 0usize); NF];
        let mut a = start;
        for i in 0..NF {
            f[i] = (a, ends[i]);
            a = ends[i] + 1;
        }
        if qc != 0 {
            let mut wrapped = 0u32;
            for x in f.iter_mut() {
                let (a, b) = *x;
                if b - a >= 2 && *base.add(a) == b'"' && *base.add(b - 1) == b'"' {
                    *x = (a + 1, b - 1);
                    wrapped += 1;
                }
            }
            if qc != 2 * wrapped {
                continue;
            }
        }
        row(&f);
    }
}

fn read_range(f: &mut File, from: u64, to: u64, buf: &mut Vec<u8>) {
    let n = (to - from) as usize;
    let old = buf.len();
    buf.reserve(n + 128);
    f.seek(SeekFrom::Start(from)).expect("seek");
    buf.resize(old + n, 0);
    f.read_exact(&mut buf[old..]).expect("read");
}

#[inline]
fn find_nl(s: &[u8]) -> Option<usize> {
    s.iter().position(|&b| b == b'\n')
}

/// Loads the lines of `f` that start in [s, e) into `buf`, minus the header.
/// Returns (lo, hi) for scan_lines.
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
    buf.extend_from_slice(&[0u8; 128]);
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

/// Event payload row. price: 0 = view/cart, 1..=10M = purchase, INVALID = dropped.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev {
    u: u128,
    ts: i64,
    price: u32,
    cat: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Ck {
    u: u128,
    cid: u128,
    ts: i64,
    camp: u32,
    ok: u32,
}

/// Dedup record; the copy with the smallest (ord, loc) wins. `loc` =
/// (thread << 48 | partition << 32 | index) is where the copy's payload row
/// sits, or NOLOC for a copy that failed validation (it has no payload).
/// Events: ord = ingest_ts. Copies with equal ingest_ts are identical, so the
/// loc tie-break picks any one. Clicks: ord = read order (chunk << 32 | row),
/// which keeps the first copy read.
#[derive(Clone, Copy)]
#[repr(C)]
struct DRec {
    k0: u64,
    k1: u64,
    ord: i64,
    loc: u64,
}

const NOLOC: u64 = u64::MAX;

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct E16 {
    ts: i64,
    price: u32,
    cat: u32,
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct C32 {
    cid: u128,
    ts: i64,
    camp: u32,
}

#[derive(Clone, Copy)]
struct Ptr<T>(*mut T);
unsafe impl<T> Send for Ptr<T> {}
unsafe impl<T> Sync for Ptr<T> {}

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
    slot: usize,
}

/// One parse thread's output: payload rows already partitioned by person,
/// dedup records partitioned by hash(id).
struct POut {
    pe: Vec<Vec<Ev>>,
    pc: Vec<Vec<Ck>>,
    de: Vec<Vec<DRec>>,
    dc: Vec<Vec<DRec>>,
    /// Rows parsed before the link table was ready, routed afterwards.
    qe: Vec<PendE>,
    qc: Vec<PendC>,
}

#[inline(always)]
unsafe fn sl<'a>(base: *const u8, f: (usize, usize)) -> &'a [u8] {
    std::slice::from_raw_parts(base.add(f.0), f.1 - f.0)
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

/// Parsed rows wait in a ring of this size while their user's link-table slot
/// is prefetched.
const RING: usize = 16;

#[inline(always)]
fn push_d(d: &mut [Vec<DRec>], bits: u32, key: u128, ord: i64, loc: u64) {
    let p = part_of(hk(key), bits);
    unsafe { d.get_unchecked_mut(p) }.push(DRec { k0: (key >> 64) as u64, k1: key as u64, ord, loc });
}

/// Person of user `u` (hash `h`) and the person's partition.
#[inline(always)]
fn route(links: &Links, u: u128, h: u64, bits: u32) -> (u128, usize) {
    let p = links.lookup(u, h);
    let hp = if p == u { h } else { hk(p) };
    (p, part_of(hp, bits))
}

#[derive(Clone, Copy, Default)]
struct PendE {
    u: u128,
    eid: u128,
    h: u64,
    ts: i64,
    ing: i64,
    price: u32,
    cat: u32,
}

#[inline(always)]
fn emit_e(cfg: &Cfg, links: &Links, t: u64, p: &PendE, out: &mut POut) {
    let (person, q) = route(links, p.u, p.h, cfg.q_bits);
    let v = unsafe { out.pe.get_unchecked_mut(q) };
    let loc = (t << 48) | ((q as u64) << 32) | v.len() as u64;
    v.push(Ev { u: person, ts: p.ts, price: p.price, cat: p.cat });
    push_d(&mut out.de, cfg.pe_bits, p.eid, p.ing, loc);
}

#[allow(clippy::too_many_arguments)]
unsafe fn parse_events(cfg: &Cfg, links: Option<&Links>, t: u64, base: *const u8, lo: usize, hi: usize, out: &mut POut) {
    let (ws, we) = (cfg.ws, cfg.we);
    let mut ring = [PendE::default(); RING];
    let mut k = 0usize;
    scan_lines::<7>(base, lo, hi, |f| {
        let (a, b) = f[0];
        if a == b {
            return;
        }
        let eid = eq_key(base.add(a), b - a);
        let ing = cfg.cal.parse(base.add(f[3].0), f[3].1 - f[3].0).unwrap_or(i64::MAX);
        'v: {
            let (ua, ub) = f[1];
            let (pa, pb) = f[5];
            if ua == ub || pa == pb {
                break 'v;
            }
            let purchase = match sl(base, f[4]) {
                b"view" | b"cart" => false,
                b"purchase" => true,
                _ => break 'v,
            };
            let ts = match cfg.cal.parse(base.add(f[2].0), f[2].1 - f[2].0) {
                Some(t) if t >= ws && t < we => t,
                _ => break 'v,
            };
            if ing.saturating_sub(ts) > DAY_MS {
                break 'v;
            }
            let (mut price, mut cat) = (0u32, 0u32);
            if purchase {
                price = match parse_price(sl(base, f[6])) {
                    Some(p) => p,
                    None => break 'v,
                };
                cat = id_key_find(base.add(pa), pb - pa)
                    .and_then(|k| cfg.products.get(k))
                    .unwrap_or(cfg.unknown_cat);
            }
            let u = id_key(base.add(ua), ub - ua);
            let h = hk(u);
            let pend = PendE { u, eid, h, ts, ing, price, cat };
            let Some(links) = links else {
                out.qe.push(pend);
                return;
            };
            links.prefetch(h);
            let s = k & (RING - 1);
            if k >= RING {
                emit_e(cfg, links, t, &ring[s], out);
            }
            ring[s] = pend;
            k += 1;
            return;
        }
        push_d(&mut out.de, cfg.pe_bits, eid, ing, NOLOC);
    });
    if let Some(links) = links {
        for j in k.saturating_sub(RING)..k {
            emit_e(cfg, links, t, &ring[j & (RING - 1)], out);
        }
    }
}

#[derive(Clone, Copy, Default)]
struct PendC {
    u: u128,
    cid: u128,
    h: u64,
    ts: i64,
    r: u64,
    camp: u32,
}

#[inline(always)]
fn emit_c(cfg: &Cfg, links: &Links, t: u64, p: &PendC, out: &mut POut) {
    let (person, q) = route(links, p.u, p.h, cfg.q_bits);
    let v = unsafe { out.pc.get_unchecked_mut(q) };
    let loc = (t << 48) | ((q as u64) << 32) | v.len() as u64;
    v.push(Ck { u: person, cid: p.cid, ts: p.ts, camp: p.camp, ok: 1 });
    push_d(&mut out.dc, cfg.pc_bits, p.cid, p.r as i64, loc);
}

#[allow(clippy::too_many_arguments)]
unsafe fn parse_clicks(cfg: &Cfg, links: Option<&Links>, t: u64, base: *const u8, lo: usize, hi: usize, slot: usize, out: &mut POut) {
    let (cs, we) = (cfg.ws - WEEK_MS, cfg.we);
    let mut ring = [PendC::default(); RING];
    let mut k = 0usize;
    let mut r = (slot as u64) << 32;
    scan_lines::<4>(base, lo, hi, |f| {
        let (a, b) = f[0];
        if a == b {
            return;
        }
        let cid = id_key(base.add(a), b - a);
        let rr = r;
        r += 1;
        'v: {
            let (ua, ub) = f[1];
            let (ca, cb) = f[2];
            if ua == ub || ca == cb {
                break 'v;
            }
            let camp = match id_key_find(base.add(ca), cb - ca).and_then(|k| cfg.campaigns.get(k)) {
                Some(c) => c,
                None => break 'v,
            };
            let ts = match cfg.cal.parse(base.add(f[3].0), f[3].1 - f[3].0) {
                Some(t) if t >= cs && t < we => t,
                _ => break 'v,
            };
            let u = id_key(base.add(ua), ub - ua);
            let h = hk(u);
            let pend = PendC { u, cid, h, ts, r: rr, camp };
            let Some(links) = links else {
                out.qc.push(pend);
                return;
            };
            links.prefetch(h);
            let s = k & (RING - 1);
            if k >= RING {
                emit_c(cfg, links, t, &ring[s], out);
            }
            ring[s] = pend;
            k += 1;
            return;
        }
        push_d(&mut out.dc, cfg.pc_bits, cid, rr as i64, NOLOC);
    });
    if let Some(links) = links {
        for j in k.saturating_sub(RING)..k {
            emit_c(cfg, links, t, &ring[j & (RING - 1)], out);
        }
    }
}

/// Routes the rows a thread parsed before the link table was ready.
fn drain_deferred(cfg: &Cfg, links: &Links, t: u64, out: &mut POut) {
    let qe = std::mem::take(&mut out.qe);
    for (i, p) in qe.iter().enumerate() {
        if let Some(n) = qe.get(i + RING) {
            links.prefetch(n.h);
        }
        emit_e(cfg, links, t, p, out);
    }
    let qc = std::mem::take(&mut out.qc);
    for (i, p) in qc.iter().enumerate() {
        if let Some(n) = qc.get(i + RING) {
            links.prefetch(n.h);
        }
        emit_c(cfg, links, t, p, out);
    }
}

// ------------------------------------------------------------------ links

#[derive(Clone, Copy)]
#[repr(C)]
struct LSlot {
    k: u128,
    v: u128,
}

/// user ID -> person name (the byte-wise smallest ID of its component).
struct Links {
    slots: Vec<LSlot>,
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
    fn lookup(&self, k: u128, h: u64) -> u128 {
        if self.slots.is_empty() {
            return k;
        }
        let mut j = h as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(j) };
            if s.k == k {
                return s.v;
            }
            if s.k == 0 {
                return k;
            }
            j = (j + 1) & self.mask;
        }
    }
}

unsafe fn parse_links(base: *const u8, lo: usize, hi: usize, pairs: &mut Vec<(u128, u128)>) {
    scan_lines::<2>(base, lo, hi, |f| {
        let ((a0, a1), (b0, b1)) = (f[0], f[1]);
        if a0 == a1 || b0 == b1 {
            return;
        }
        let a = id_key(base.add(a0), a1 - a0);
        let b = id_key(base.add(b0), b1 - b0);
        if a != b {
            pairs.push((a, b));
        }
    });
}

fn build_links(parts: &[Vec<(u128, u128)>]) -> Links {
    let np: usize = parts.iter().map(|p| p.len()).sum();
    if np == 0 {
        return Links { slots: Vec::new(), mask: 0 };
    }
    let cap = (np * 3).max(16).next_power_of_two();
    let mask = cap - 1;
    let mut slots = vec![LSlot { k: 0, v: 0 }; cap];
    let mut keys: Vec<u128> = Vec::with_capacity(np + np / 2);
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
    for chunk in parts.iter().flat_map(|p| p.chunks(32)) {
        for (i, &(a, b)) in chunk.iter().enumerate() {
            let (ha, hb) = (hk(a), hk(b));
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
                    if s.k == k {
                        break s.v as u32;
                    }
                    if s.k == 0 {
                        let id = keys.len() as u32;
                        *s = LSlot { k, v: id as u128 };
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
        slots[slotpos[i as usize] as usize].v = keys[r as usize];
    }
    Links { slots, mask }
}

// ------------------------------------------------------------------ stage B

/// Calls `kill(loc)` for every copy but the smallest (ord, loc) of each
/// key in partition `p` of the per-thread record lists `lists`.
fn dedup_part(lists: &[&Vec<Vec<DRec>>], p: usize, tab: &mut Vec<u64>, kill: impl Fn(u64)) {
    let n: usize = lists.iter().map(|l| l[p].len()).sum();
    if n < 2 {
        return;
    }
    let cap = (n * 2).next_power_of_two();
    let mask = cap - 1;
    if tab.len() < cap {
        tab.resize(cap, 0);
    }
    tab[..cap].fill(0);
    for (t, l) in lists.iter().enumerate() {
        let v = &l[p];
        assert!(v.len() <= u32::MAX as usize);
        for (i, r) in v.iter().enumerate() {
            let key = ((r.k0 as u128) << 64) | r.k1 as u128;
            let h = hk(key);
            let tag = ((h >> 24) & 0xFF_FFFF) | 1;
            let new = (tag << 40) | ((t as u64) << 32) | i as u64;
            let mut j = h as usize & mask;
            loop {
                let s = unsafe { *tab.get_unchecked(j) };
                if s == 0 {
                    unsafe { *tab.get_unchecked_mut(j) = new };
                    break;
                }
                if s >> 40 == tag {
                    let o = &lists[((s >> 32) & 0xFF) as usize][p][s as u32 as usize];
                    if o.k0 == r.k0 && o.k1 == r.k1 {
                        if (r.ord, r.loc) < (o.ord, o.loc) {
                            kill(o.loc);
                            unsafe { *tab.get_unchecked_mut(j) = new };
                        } else {
                            kill(r.loc);
                        }
                        break;
                    }
                }
                j = (j + 1) & mask;
            }
        }
    }
}

// ------------------------------------------------------------------ stage D

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

    /// One person: events sorted by ts, clicks sorted by (ts, click_id).
    fn person(&mut self, pkey: u128, evs: &[E16], cks: &[C32]) {
        self.name.clear();
        key_bytes_fast(pkey, &mut self.name);
        let n = evs.len();
        let nc = cks.len();
        let (mut lo, mut hi) = (0usize, 0usize);
        let mut cur_day = i64::MIN;
        let (mut r_sess, mut r_ev, mut r_rev) = (0i32, 0i32, 0i64);
        let mut i = 0usize;
        while i < n {
            let start = evs[i].ts;
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
            let mut prev = start;
            let mut j = i;
            let mut rev = 0i64;
            let mut purchased = false;
            while j < n && evs[j].ts - prev <= GAP_MS {
                let e = &evs[j];
                prev = e.ts;
                if e.price != 0 {
                    purchased = true;
                    rev += e.price as i64;
                    let s = &mut self.csum[e.cat as usize];
                    if *s == 0 {
                        self.touched.push(e.cat);
                    }
                    *s += e.price as i64;
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
                while hi < nc && cks[hi].ts <= start {
                    let c = cks[hi].camp as usize;
                    if self.ccnt[c] == 0 {
                        self.apos[c] = self.active.len() as u32;
                        self.active.push(c as u32);
                    }
                    self.ccnt[c] += 1;
                    hi += 1;
                }
                while lo < hi && cks[lo].ts < start - WEEK_MS {
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

#[inline(always)]
fn key_bytes_fast(k: u128, out: &mut Vec<u8>) {
    let tag = k as u8;
    if tag < LONG_ID {
        out.extend_from_slice(&k.to_be_bytes()[..tag as usize]);
    } else {
        key_bytes(k, out);
    }
}

/// Partition-local person table: key -> dense id.
struct Local {
    slots: Vec<u32>,
    mask: usize,
    keys: Vec<u128>,
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
            let mut j = hk(k) as usize & self.mask;
            while self.slots[j] != 0 {
                j = (j + 1) & self.mask;
            }
            self.slots[j] = i as u32 + 1;
        }
    }
    #[inline(always)]
    fn get_or_insert(&mut self, k: u128, h: u64) -> u32 {
        let mut j = h as usize & self.mask;
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
    fn get(&self, k: u128, h: u64) -> u32 {
        let mut j = h as usize & self.mask;
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
struct DBufs {
    ids: Vec<u32>,
    cids: Vec<u32>,
    off: Vec<u32>,
    coff: Vec<u32>,
    evs: Vec<E16>,
    cks: Vec<C32>,
}

fn do_part(q: usize, pes: &[Vec<Vec<Ev>>], pcs: &[Vec<Vec<Ck>>], local: &mut Local, b: &mut DBufs, agg: &mut Agg) {
    let n: usize = pes.iter().map(|c| c[q].len()).sum();
    if n == 0 {
        return;
    }
    assert!(n < u32::MAX as usize);
    local.reset(n);
    b.ids.clear();
    b.off.clear();
    for c in pes {
        for e in c[q].iter().filter(|e| e.price != INVALID) {
            let id = local.get_or_insert(e.u, hk(e.u));
            if id as usize == b.off.len() {
                b.off.push(0);
            }
            b.off[id as usize] += 1;
            b.ids.push(id);
        }
    }
    let n = b.ids.len();
    if n == 0 {
        return;
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
    b.evs.resize(n, E16::default());
    let mut k = 0usize;
    for c in pes {
        for e in c[q].iter().filter(|e| e.price != INVALID) {
            let id = b.ids[k] as usize;
            k += 1;
            let pos = b.off[id];
            b.off[id] = pos + 1;
            b.evs[pos as usize] = E16 { ts: e.ts, price: e.price, cat: e.cat };
        }
    }
    // off[id] is now the end of person id; the start is off[id-1] (or 0)

    b.cids.clear();
    b.coff.clear();
    b.coff.resize(np + 1, 0);
    let mut ncl = 0u32;
    for c in pcs {
        for ck in c[q].iter().filter(|c| c.ok != 0) {
            let id = local.get(ck.u, hk(ck.u));
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
        b.cks.resize(ncl as usize, C32::default());
        let mut k = 0usize;
        for c in pcs {
            for ck in c[q].iter().filter(|c| c.ok != 0) {
                let id = b.cids[k];
                k += 1;
                if id != NONE {
                    let pos = b.coff[id as usize];
                    b.coff[id as usize] = pos + 1;
                    b.cks[pos as usize] = C32 { cid: ck.cid, ts: ck.ts, camp: ck.camp };
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
            evs.sort_unstable_by_key(|e| e.ts);
        }
        let cks: &[C32] = if ncl != 0 {
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
        defs.resize((b - a + 7) / 8, 0);
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

#[cfg(target_os = "linux")]
fn prefetch_files(paths: Vec<PathBuf>) {
    use std::os::fd::AsRawFd;
    extern "C" {
        fn posix_fadvise(fd: i32, off: i64, len: i64, advice: i32) -> i32;
    }
    const POSIX_FADV_WILLNEED: i32 = 3;
    std::thread::spawn(move || {
        for p in paths {
            if let Ok(f) = File::open(&p) {
                unsafe { posix_fadvise(f.as_raw_fd(), 0, 0, POSIX_FADV_WILLNEED) };
            }
        }
    });
}

#[cfg(not(target_os = "linux"))]
fn prefetch_files(_paths: Vec<PathBuf>) {}

fn bits_for(rows: u64, per: u64) -> u32 {
    let per = per.max(1);
    let parts = ((rows / per).max(1)).next_power_of_two().min(4096);
    parts.trailing_zeros()
}

pub fn run() -> ! {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let ncpu = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));

    let ud_dir = out_dir.join("user_daily");
    let cd_dir = out_dir.join("campaign_daily");
    clean_dir(&ud_dir);
    clean_dir(&cd_dir);

    let man = fs::read_to_string(data.join("manifest.json")).expect("read manifest.json");
    let ws = manifest_ts(&man, "window_start");
    let we = manifest_ts(&man, "window_end");

    // products.csv -> category index; later rows win, like a dict
    let mut buf = Vec::new();
    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    let mut cat_ix: HashMap<Vec<u8>, u32> = HashMap::new();
    let mut prod_rows: Vec<(u128, u32)> = Vec::new();
    {
        let (lo, hi) = load_whole(&data.join("products.csv"), &mut buf);
        let base = buf.as_ptr();
        unsafe {
            scan_lines::<2>(base, lo, hi, |f| {
                let (a, b) = f[0];
                if a == b {
                    return;
                }
                let name = sl(base, f[1]);
                let c = *cat_ix.entry(name.to_vec()).or_insert_with(|| {
                    cat_names.push(name.to_vec());
                    cat_names.len() as u32 - 1
                });
                prod_rows.push((id_key(base.add(a), b - a), c));
            });
        }
    }
    let unknown_cat = *cat_ix.entry(b"unknown".to_vec()).or_insert_with(|| {
        cat_names.push(b"unknown".to_vec());
        cat_names.len() as u32 - 1
    });
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
    let mut camp_ix: HashMap<u128, u32> = HashMap::new();
    {
        let (lo, hi) = load_whole(&data.join("campaigns.csv"), &mut buf);
        let base = buf.as_ptr();
        unsafe {
            scan_lines::<2>(base, lo, hi, |f| {
                let (a, b) = f[0];
                if a == b {
                    return;
                }
                let k = id_key(base.add(a), b - a);
                let chan = sl(base, f[1]).to_vec();
                match camp_ix.get(&k) {
                    Some(&i) => camp_chan[i as usize] = chan,
                    None => {
                        camp_ix.insert(k, camp_names.len() as u32);
                        camp_names.push(sl(base, f[0]).to_vec());
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
    let est_ev = ev_bytes / 60;
    let est_ck = ck_bytes / 40;

    let nt = ncpu.min(((ev_bytes + ck_bytes) >> 18) as usize + 1).clamp(1, 200);
    let cfg = Cfg {
        ws,
        we,
        cal: Cal::new(ws - WEEK_MS, we + WEEK_MS),
        products,
        unknown_cat,
        campaigns,
        // >= 2 MiB per thread and partition, so the buffers get huge pages
        pe_bits: bits_for(est_ev, 262144),
        pc_bits: bits_for(est_ck, 262144),
        q_bits: bits_for(est_ev, 262144),
    };
    let (npe, npc, nq) = (1usize << cfg.pe_bits, 1usize << cfg.pc_bits, 1usize << cfg.q_bits);

    let chunks = |files: &[(PathBuf, u64)], kind: Kind, tasks: &mut Vec<Task>| {
        let mut slot = 0usize;
        for (fi, &(_, size)) in files.iter().enumerate() {
            let n = size.div_ceil(CHUNK);
            for c in 0..n {
                tasks.push(Task { kind, file: fi, s: size * c / n, e: size * (c + 1) / n, slot });
                slot += 1;
            }
        }
    };
    let mut tasks: Vec<Task> = Vec::new();
    chunks(&ev_files, Kind::Events, &mut tasks);
    chunks(&ck_files, Kind::Clicks, &mut tasks);
    let mut ltasks: Vec<Task> = Vec::new();
    chunks(&ln_files, Kind::Events, &mut ltasks);
    // Ask the kernel to start reading every input file now, in the order the
    // parse threads will want it, so a cold page cache is filled with deep
    // I/O queues while we compute. Warm caches make this a cheap no-op.
    prefetch_files(ln_files.iter().chain(&ev_files).chain(&ck_files).map(|f| f.0.clone()).collect());
    let t_setup = t0.elapsed();

    // ---- stage A: thread 0 builds the link table while the others parse;
    // rows parsed before it is ready wait in a per-thread queue, then every
    // row is routed to its person's partition.
    let links: OnceLock<Links> = OnceLock::new();
    let next = AtomicUsize::new(0);
    let res_d = (est_ev as usize / (nt * npe)) * 5 / 4 + 32;
    let res_dc = (est_ck as usize / (nt * npc)) * 5 / 4 + 32;
    let res_e = (est_ev as usize / (nt * nq)) * 5 / 4 + 32;
    let res_c = (est_ck as usize / (nt * nq)) * 5 / 4 + 32;
    let mut pes: Vec<Vec<Vec<Ev>>> = Vec::with_capacity(nt);
    let mut pcs: Vec<Vec<Vec<Ck>>> = Vec::with_capacity(nt);
    let mut des: Vec<Vec<Vec<DRec>>> = Vec::with_capacity(nt);
    let mut dcs: Vec<Vec<Vec<DRec>>> = Vec::with_capacity(nt);
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nt)
            .map(|t| {
                let (cfg, tasks, next, links) = (&cfg, &tasks, &next, &links);
                let (ev_files, ck_files, ln_files, ltasks) = (&ev_files, &ck_files, &ln_files, &ltasks);
                sc.spawn(move || {
                    let mut out = POut {
                        pe: (0..nq).map(|_| Vec::with_capacity(res_e)).collect(),
                        pc: (0..nq).map(|_| Vec::with_capacity(res_c)).collect(),
                        de: (0..npe).map(|_| Vec::with_capacity(res_d)).collect(),
                        dc: (0..npc).map(|_| Vec::with_capacity(res_dc)).collect(),
                        qe: Vec::new(),
                        qc: Vec::new(),
                    };
                    let mut buf: Vec<u8> = Vec::with_capacity(CHUNK as usize + 8192);
                    if t == 0 {
                        let mut pairs: Vec<(u128, u128)> = Vec::new();
                        for tk in ltasks {
                            let (path, size) = &ln_files[tk.file];
                            let mut f = File::open(path).unwrap_or_else(|e| panic!("open {}: {}", path.display(), e));
                            let (lo, hi) = load_chunk(&mut f, tk.s, tk.e, *size, &mut buf);
                            if lo < hi {
                                unsafe { parse_links(buf.as_ptr(), lo, hi, &mut pairs) };
                            }
                        }
                        let _ = links.set(build_links(&[pairs]));
                    }
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
                        let lk = links.get();
                        unsafe {
                            match tk.kind {
                                Kind::Events => parse_events(cfg, lk, t as u64, base, lo, hi, &mut out),
                                Kind::Clicks => parse_clicks(cfg, lk, t as u64, base, lo, hi, tk.slot, &mut out),
                            }
                        }
                    }
                    if !out.qe.is_empty() || !out.qc.is_empty() {
                        drain_deferred(cfg, links.wait(), t as u64, &mut out);
                    }
                    out
                })
            })
            .collect();
        for h in hs {
            let o = h.join().expect("parse thread");
            pes.push(o.pe);
            pcs.push(o.pc);
            des.push(o.de);
            dcs.push(o.dc);
        }
    });
    let t_a = t0.elapsed();

    // ---- stage B: dedup; losing copies are marked dead in place
    {
        let pe_ptr: Vec<Vec<Ptr<Ev>>> =
            pes.iter_mut().map(|v| v.iter_mut().map(|x| Ptr(x.as_mut_ptr())).collect()).collect();
        let pc_ptr: Vec<Vec<Ptr<Ck>>> =
            pcs.iter_mut().map(|v| v.iter_mut().map(|x| Ptr(x.as_mut_ptr())).collect()).collect();
        let dl: Vec<&Vec<Vec<DRec>>> = des.iter().collect();
        let cl: Vec<&Vec<Vec<DRec>>> = dcs.iter().collect();
        let next = AtomicUsize::new(0);
        std::thread::scope(|sc| {
            for _ in 0..nt {
                let (dl, cl, next, pe_ptr, pc_ptr) = (&dl, &cl, &next, &pe_ptr, &pc_ptr);
                sc.spawn(move || {
                    let mut tab: Vec<u64> = Vec::new();
                    loop {
                        let i = next.fetch_add(1, AO::Relaxed);
                        if i >= npe + npc {
                            break;
                        }
                        // SAFETY: a payload row is referenced by exactly one dedup record.
                        if i < npe {
                            dedup_part(dl, i, &mut tab, |loc| unsafe {
                                if loc != NOLOC {
                                    let p = pe_ptr[(loc >> 48) as usize][((loc >> 32) & 0xFFFF) as usize].0;
                                    (*p.add(loc as u32 as usize)).price = INVALID;
                                }
                            });
                        } else {
                            dedup_part(cl, i - npe, &mut tab, |loc| unsafe {
                                if loc != NOLOC {
                                    let p = pc_ptr[(loc >> 48) as usize][((loc >> 32) & 0xFFFF) as usize].0;
                                    (*p.add(loc as u32 as usize)).ok = 0;
                                }
                            });
                        }
                    }
                });
            }
        });
    }
    let t_b = t0.elapsed();

    // ---- stage D: sessionize per person partition, then write user_daily
    let day_min = ws.div_euclid(DAY_MS);
    let ndays = (((we - 1).div_euclid(DAY_MS) - day_min + 1).max(1)) as usize;
    let dense = ncamp.saturating_mul(ndays) <= (1 << 22);
    let mut order: Vec<(usize, usize)> =
        (0..nq).map(|q| (pes.iter().map(|c| c[q].len()).sum::<usize>(), q)).collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
    let next = AtomicUsize::new(0);
    let mut camp_tabs: Vec<CampTab> = Vec::new();
    let mut total_rows = 0usize;
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nt)
            .map(|t| {
                let (next, pes, pcs, order, cat_rank, cat_names, ud_dir) =
                    (&next, &pes, &pcs, &order, &cat_rank, &cat_names, &ud_dir);
                sc.spawn(move || {
                    let mut agg = Agg {
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
                        csum: vec![0; cat_names.len()],
                        touched: Vec::new(),
                        cat_rank: cat_rank.clone(),
                        out: UOut {
                            rows: 0,
                            uid: Vec::new(),
                            marks: Vec::new(),
                            day: Vec::new(),
                            sess: Vec::new(),
                            evc: Vec::new(),
                            rev: Vec::new(),
                            cat: Vec::new(),
                        },
                        name: Vec::new(),
                    };
                    let mut local = Local { slots: Vec::new(), mask: 0, keys: Vec::new() };
                    let mut bufs = DBufs::default();
                    loop {
                        let i = next.fetch_add(1, AO::Relaxed);
                        if i >= order.len() {
                            break;
                        }
                        do_part(order[i].1, pes, pcs, &mut local, &mut bufs, &mut agg);
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
        let empty = UOut {
            rows: 0,
            uid: Vec::new(),
            marks: Vec::new(),
            day: Vec::new(),
            sess: Vec::new(),
            evc: Vec::new(),
            rev: Vec::new(),
            cat: Vec::new(),
        };
        write_user(&ud_dir.join("part-0.parquet"), &empty, &cat_names);
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
        "threads {} parts {}/{}/{} | setup {:.0} parse {:.0} dedup {:.0} sessions+write {:.0} campaigns {:.0} | total {:.0} ms | user rows {} campaign rows {}",
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
