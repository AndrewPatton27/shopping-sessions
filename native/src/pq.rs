//! Minimal Parquet writer: uncompressed, PLAIN encoding, data page v1, one page
//! per column chunk. Just enough of the format (and of Thrift's compact
//! protocol) for the two output tables.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    /// required BYTE_ARRAY, logical type STRING
    Str,
    /// optional BYTE_ARRAY, logical type STRING
    OptStr,
    /// required INT32, logical type DATE
    Date,
    I32,
    I64,
}

/// One column of one row group, already PLAIN-encoded by the caller:
/// strings as (u32 LE length, bytes)*, integers as little-endian values.
/// `defs` (optional columns only) is the definition-level bitmap, LSB first,
/// ceil(rows / 8) bytes; `values` then holds only the non-null values.
pub struct ColData<'a> {
    pub values: &'a [u8],
    pub defs: Option<&'a [u8]>,
}

struct ChunkMeta {
    offset: u64,
    size: u64,
}

struct RgMeta {
    rows: u64,
    chunks: Vec<ChunkMeta>,
}

pub struct Writer {
    w: BufWriter<File>,
    pos: u64,
    schema: Vec<(&'static str, Kind)>,
    rgs: Vec<RgMeta>,
}

// ---- Thrift compact protocol ----
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BIN: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

struct Thrift {
    b: Vec<u8>,
    last: Vec<i16>,
}

impl Thrift {
    fn new() -> Self {
        Thrift { b: Vec::with_capacity(256), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let delta = id - *last;
        *last = id;
        if delta > 0 && delta <= 15 {
            self.b.push(((delta as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zigzag(id as i64);
        }
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.zigzag(v);
    }
    fn bin_raw(&mut self, s: &[u8]) {
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn bin(&mut self, id: i16, s: &[u8]) {
        self.field(id, T_BIN);
        self.bin_raw(s);
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    /// Start a struct that is a field of the current struct.
    fn struct_field(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.last.push(0);
    }
    /// Start a struct that is a list element.
    fn struct_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
}

fn phys(k: Kind) -> i32 {
    match k {
        Kind::Str | Kind::OptStr => 6, // BYTE_ARRAY
        Kind::Date | Kind::I32 => 1,   // INT32
        Kind::I64 => 2,                // INT64
    }
}

impl Writer {
    pub fn create(path: &Path, schema: &[(&'static str, Kind)]) -> io::Result<Writer> {
        let f = File::create(path)?;
        let mut w = BufWriter::with_capacity(1 << 22, f);
        w.write_all(b"PAR1")?;
        Ok(Writer { w, pos: 4, schema: schema.to_vec(), rgs: Vec::new() })
    }

    pub fn row_group(&mut self, rows: usize, cols: &[ColData]) -> io::Result<()> {
        assert_eq!(cols.len(), self.schema.len());
        if rows == 0 {
            return Ok(());
        }
        let mut chunks = Vec::with_capacity(cols.len());
        for (c, &(_, kind)) in cols.iter().zip(self.schema.iter()) {
            let mut lv: Vec<u8> = Vec::new();
            if kind == Kind::OptStr {
                // definition levels: RLE/bit-packed hybrid, one bit-packed run, bit width 1
                let defs = c.defs.expect("optional column needs defs");
                let groups = (rows + 7) / 8;
                assert_eq!(defs.len(), groups);
                let mut t = Thrift::new();
                t.varint(((groups as u64) << 1) | 1);
                let body = (t.b.len() + defs.len()) as u32;
                lv.extend_from_slice(&body.to_le_bytes());
                lv.extend_from_slice(&t.b);
                lv.extend_from_slice(defs);
            }
            let page_size = lv.len() + c.values.len();
            assert!(page_size < i32::MAX as usize, "page too large");
            let mut h = Thrift::new();
            h.i32(1, 0); // DATA_PAGE
            h.i32(2, page_size as i32);
            h.i32(3, page_size as i32);
            h.struct_field(5);
            h.i32(1, rows as i32);
            h.i32(2, 0); // PLAIN
            h.i32(3, 3); // RLE
            h.i32(4, 3); // RLE
            h.end();
            h.b.push(0);
            let offset = self.pos;
            self.w.write_all(&h.b)?;
            self.w.write_all(&lv)?;
            self.w.write_all(c.values)?;
            let size = (h.b.len() + page_size) as u64;
            self.pos += size;
            chunks.push(ChunkMeta { offset, size });
        }
        self.rgs.push(RgMeta { rows: rows as u64, chunks });
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<()> {
        let mut t = Thrift::new();
        t.i32(1, 1); // version
        t.list(2, T_STRUCT, self.schema.len() + 1);
        t.struct_elem();
        t.bin(4, b"schema");
        t.i32(5, self.schema.len() as i32);
        t.end();
        for &(name, kind) in &self.schema {
            t.struct_elem();
            t.i32(1, phys(kind));
            t.i32(3, if kind == Kind::OptStr { 1 } else { 0 });
            t.bin(4, name.as_bytes());
            match kind {
                Kind::Str | Kind::OptStr => {
                    t.i32(6, 0); // UTF8
                    t.struct_field(10);
                    t.struct_field(1); // STRING
                    t.end();
                    t.end();
                }
                Kind::Date => {
                    t.i32(6, 6); // DATE
                    t.struct_field(10);
                    t.struct_field(6); // DATE
                    t.end();
                    t.end();
                }
                _ => {}
            }
            t.end();
        }
        let total_rows: u64 = self.rgs.iter().map(|r| r.rows).sum();
        t.i64(3, total_rows as i64);
        t.list(4, T_STRUCT, self.rgs.len());
        for rg in &self.rgs {
            t.struct_elem();
            t.list(1, T_STRUCT, rg.chunks.len());
            for (ch, &(name, kind)) in rg.chunks.iter().zip(self.schema.iter()) {
                t.struct_elem();
                t.i64(2, ch.offset as i64); // file_offset
                t.struct_field(3); // ColumnMetaData
                t.i32(1, phys(kind));
                t.list(2, T_I32, 2);
                t.zigzag(0); // PLAIN
                t.zigzag(3); // RLE
                t.list(3, T_BIN, 1);
                t.bin_raw(name.as_bytes());
                t.i32(4, 0); // UNCOMPRESSED
                t.i64(5, rg.rows as i64);
                t.i64(6, ch.size as i64);
                t.i64(7, ch.size as i64);
                t.i64(9, ch.offset as i64); // data_page_offset
                t.end();
                t.end();
            }
            let total: u64 = rg.chunks.iter().map(|c| c.size).sum();
            t.i64(2, total as i64);
            t.i64(3, rg.rows as i64);
            t.end();
        }
        t.bin(6, b"shopping-sessions-native");
        t.b.push(0);
        self.w.write_all(&t.b)?;
        self.w.write_all(&(t.b.len() as u32).to_le_bytes())?;
        self.w.write_all(b"PAR1")?;
        self.w.flush()
    }
}

/// View a slice of plain integers as its little-endian bytes (x86-64 host).
pub fn bytes_of<T: Copy>(s: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(s.as_ptr() as *const u8, std::mem::size_of_val(s)) }
}
