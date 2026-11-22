//! Minimal Parquet writer: one row group, one uncompressed data page (v1) per column (PLAIN, or
//! RLE_DICTIONARY after a dictionary page for small-range INT32 columns), flat schema. Supports REQUIRED/OPTIONAL BYTE_ARRAY (UTF8), INT32 (DATE)
//! and INT64. Metadata is Thrift compact protocol, hand-encoded.

use std::io::Write;

pub enum Col<'a> {
    /// UTF8 string column; `None` entries are nulls (column must be optional).
    Str(Vec<Option<&'a [u8]>>, bool /* optional */),
    Date(Vec<i32>),
    I64(Vec<i64>),
    /// plain INT32 (the rules accept any integer width)
    I32(Vec<i32>),
    /// a column of `n` values whose data page body is written by the closure into a buffer
    /// reused across columns (see str_page / fixed_page): no per-column value vectors
    Fill(Kind, usize, &'a (dyn Fn(&mut Vec<u8>) + Sync)),
}

/// Physical/logical kind of a Fill column.
#[derive(Clone, Copy)]
pub enum Kind {
    Str(bool /* optional */),
    Date,
    I64,
    I32,
    /// INT32 (a DATE if the flag is set) whose values all lie in base..base + count: written as a
    /// dictionary page holding exactly those `count` values in order and one RLE_DICTIONARY data
    /// page of bit-packed indexes (value - base); the closure writes that page with dict_page
    Dict32(bool /* date */, i32 /* base */, u32 /* count */),
}

/// Bits per index for a dictionary of `count` values (at least 1).
pub fn dict_bits(count: u32) -> u32 {
    (32 - count.saturating_sub(1).leading_zeros()).max(1)
}

/// Body of an RLE_DICTIONARY data page: the bit width, then one bit-packed run of all n indexes
/// (get(i) < 2^width; LSB first, padded with zeros to a multiple of 8 values).
pub fn dict_page(page: &mut Vec<u8>, n: usize, width: u32, get: impl Fn(usize) -> u32) {
    let groups = n.div_ceil(8);
    page.reserve(groups * width as usize + 8);
    page.push(width as u8);
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        page.push((h as u8) | 0x80);
        h >>= 7;
    }
    page.push(h as u8);
    let (mut acc, mut nb) = (0u64, 0u32);
    for i in 0..groups * 8 {
        let v = if i < n { get(i) as u64 } else { 0 };
        debug_assert!(v < (1u64 << width));
        acc |= v << nb;
        nb += width;
        while nb >= 8 {
            page.push(acc as u8);
            acc >>= 8;
            nb -= 8;
        }
    }
    debug_assert!(nb == 0);
}

/// Body of a PLAIN BYTE_ARRAY page: definition levels (optional columns) then the values.
pub fn str_page<'b>(page: &mut Vec<u8>, n: usize, get: impl Fn(usize) -> Option<&'b [u8]>, optional: bool) {
    if optional {
        // RLE runs of the definition levels, prefixed by their byte length
        let at = page.len();
        page.extend_from_slice(&[0u8; 4]);
        let mut i = 0;
        while i < n {
            let v = get(i).is_some();
            let mut j = i + 1;
            while j < n && get(j).is_some() == v {
                j += 1;
            }
            let mut h = ((j - i) as u64) << 1;
            while h >= 0x80 {
                page.push((h as u8) | 0x80);
                h >>= 7;
            }
            page.push(h as u8);
            page.push(v as u8);
            i = j;
        }
        let len = (page.len() - at - 4) as u32;
        page[at..at + 4].copy_from_slice(&len.to_le_bytes());
    }
    for i in 0..n {
        if let Some(s) = get(i) {
            page.extend_from_slice(&(s.len() as u32).to_le_bytes());
            page.extend_from_slice(s);
        }
    }
}

/// Body of a PLAIN fixed-width page (little-endian values).
pub fn fixed_page<const W: usize>(page: &mut Vec<u8>, n: usize, get: impl Fn(usize) -> [u8; W]) {
    page.reserve(n * W);
    for i in 0..n {
        page.extend_from_slice(&get(i));
    }
}

// ---- Thrift compact protocol
struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}
impl T {
    fn new() -> Self {
        T { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn fh(&mut self, id: i16, ty: u8) {
        let d = id - *self.last.last().unwrap();
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.fh(id, 5);
        self.zz(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.fh(id, 6);
        self.zz(v);
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.fh(id, 8);
        self.varint(v.len() as u64);
        self.b.extend_from_slice(v);
    }
    fn begin(&mut self, id: i16) {
        self.fh(id, 12);
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, n: usize, elem: u8) {
        self.fh(id, 9);
        self.list_hdr(n, elem);
    }
    fn list_hdr(&mut self, n: usize, elem: u8) {
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    // a struct that is a list element: no field header
    fn elem_begin(&mut self) {
        self.last.push(0);
    }
}

/// RLE/bit-packed hybrid, bit width 1, as RLE runs only.
fn def_levels(defs: &[bool]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < defs.len() {
        let v = defs[i];
        let mut j = i;
        while j < defs.len() && defs[j] == v {
            j += 1;
        }
        let mut h = ((j - i) as u64) << 1;
        while h >= 0x80 {
            out.push((h as u8) | 0x80);
            h >>= 7;
        }
        out.push(h as u8);
        out.push(v as u8);
        i = j;
    }
    out
}

pub fn write(path: &std::path::Path, cols: &[(&str, Col)]) -> std::io::Result<()> {
    let nrows = match &cols[0].1 {
        Col::Str(v, _) => v.len(),
        Col::Date(v) => v.len(),
        Col::I64(v) => v.len(),
        Col::I32(v) => v.len(),
        Col::Fill(_, n, _) => *n,
    };
    let mut f = std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(path)?);
    f.write_all(b"PAR1")?;
    let mut off: i64 = 4;
    // (chunk offset, chunk size, type, data page offset if a dictionary page comes first)
    let mut metas: Vec<(i64, i64, i32, Option<i64>)> = vec![];
    let mut page: Vec<u8> = Vec::new();
    for (_, col) in cols {
        page.clear();
        let ty;
        match col {
            Col::Fill(k, _, f) => {
                ty = match k {
                    Kind::Str(_) => 6,
                    Kind::Date | Kind::I32 | Kind::Dict32(..) => 1,
                    Kind::I64 => 2,
                };
                f(&mut page);
            }
            Col::Str(v, optional) => {
                ty = 6; // BYTE_ARRAY
                if *optional {
                    let defs: Vec<bool> = v.iter().map(|x| x.is_some()).collect();
                    let enc = def_levels(&defs);
                    page.extend_from_slice(&(enc.len() as u32).to_le_bytes());
                    page.extend_from_slice(&enc);
                }
                for s in v.iter().flatten() {
                    page.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    page.extend_from_slice(s);
                }
            }
            Col::Date(v) => {
                ty = 1; // INT32
                for x in v {
                    page.extend_from_slice(&x.to_le_bytes());
                }
            }
            Col::I64(v) => {
                ty = 2; // INT64
                for x in v {
                    page.extend_from_slice(&x.to_le_bytes());
                }
            }
            Col::I32(v) => {
                ty = 1; // INT32
                for x in v {
                    page.extend_from_slice(&x.to_le_bytes());
                }
            }
        }
        // dictionary page first (Dict32 columns): the `count` values base.., PLAIN
        let mut dict_size = 0i64;
        let dict = if let Col::Fill(Kind::Dict32(_, base, count), ..) = col { Some((*base, *count)) } else { None };
        if let Some((base, count)) = dict {
            let mut d = T::new();
            d.i32(1, 2); // DICTIONARY_PAGE
            d.i32(2, count as i32 * 4);
            d.i32(3, count as i32 * 4);
            d.begin(7); // DictionaryPageHeader
            d.i32(1, count as i32);
            d.i32(2, 0); // PLAIN
            d.end();
            d.b.push(0);
            f.write_all(&d.b)?;
            for k in 0..count {
                f.write_all(&(base + k as i32).to_le_bytes())?;
            }
            dict_size = d.b.len() as i64 + count as i64 * 4;
        }
        let mut h = T::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, page.len() as i32);
        h.i32(3, page.len() as i32);
        h.begin(5); // DataPageHeader
        h.i32(1, nrows as i32);
        h.i32(2, if dict.is_some() { 8 } else { 0 }); // RLE_DICTIONARY or PLAIN
        h.i32(3, 3); // RLE
        h.i32(4, 3); // RLE
        h.end();
        h.b.push(0);
        f.write_all(&h.b)?;
        f.write_all(&page)?;
        let size = dict_size + (h.b.len() + page.len()) as i64;
        metas.push((off, size, ty, dict.map(|_| off + dict_size)));
        off += size;
    }
    // FileMetaData
    let mut m = T::new();
    m.i32(1, 1); // version
    m.list(2, cols.len() + 1, 12);
    m.elem_begin();
    m.bin(4, b"schema");
    m.i32(5, cols.len() as i32);
    m.end();
    for (name, col) in cols {
        m.elem_begin();
        let (ty, opt) = match col {
            Col::Str(_, o) => (6, *o),
            Col::Date(_) => (1, false),
            Col::I64(_) => (2, false),
            Col::I32(_) => (1, false),
            Col::Fill(Kind::Str(o), ..) => (6, *o),
            Col::Fill(Kind::Date | Kind::I32 | Kind::Dict32(..), ..) => (1, false),
            Col::Fill(Kind::I64, ..) => (2, false),
        };
        m.i32(1, ty);
        m.i32(3, if opt { 1 } else { 0 });
        m.bin(4, name.as_bytes());
        match col {
            Col::Str(..) | Col::Fill(Kind::Str(_), ..) => {
                m.i32(6, 0); // converted UTF8
                m.begin(10); // LogicalType
                m.begin(1); // STRING
                m.end();
                m.end();
            }
            Col::Date(_) | Col::Fill(Kind::Date | Kind::Dict32(true, ..), ..) => {
                m.i32(6, 6); // converted DATE
                m.begin(10);
                m.begin(6); // DATE
                m.end();
                m.end();
            }
            _ => {}
        }
        m.end();
    }
    m.i64(3, nrows as i64);
    m.list(4, 1, 12);
    m.elem_begin(); // RowGroup
    m.list(1, cols.len(), 12);
    let total: i64 = metas.iter().map(|x| x.1).sum();
    for ((name, _), (poff, size, ty, data_off)) in cols.iter().zip(&metas) {
        m.elem_begin(); // ColumnChunk
        m.i64(2, *poff);
        m.begin(3); // ColumnMetaData
        m.i32(1, *ty);
        if data_off.is_some() {
            m.list(2, 3, 5);
            m.zz(0); // PLAIN (dictionary page)
            m.zz(3); // RLE
            m.zz(8); // RLE_DICTIONARY
        } else {
            m.list(2, 2, 5);
            m.zz(0); // PLAIN
            m.zz(3); // RLE
        }
        m.list(3, 1, 8);
        m.varint(name.len() as u64);
        m.b.extend_from_slice(name.as_bytes());
        m.i32(4, 0); // UNCOMPRESSED
        m.i64(5, nrows as i64);
        m.i64(6, *size);
        m.i64(7, *size);
        m.i64(9, data_off.unwrap_or(*poff)); // data_page_offset
        if data_off.is_some() {
            m.i64(11, *poff); // dictionary_page_offset
        }
        m.end();
        m.end();
    }
    m.i64(2, total);
    m.i64(3, nrows as i64);
    m.end();
    m.bin(6, b"goonies-sessions");
    m.b.push(0);
    f.write_all(&m.b)?;
    f.write_all(&(m.b.len() as u32).to_le_bytes())?;
    f.write_all(b"PAR1")?;
    f.flush()
}
