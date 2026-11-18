//! Prometheus TSDB index parser (format v1 and v2).
//!
//! The block index is a table of contents (TOC) at the end of the file. We
//! parse the symbol table and the series section; postings and label indexes
//! are not needed for a full scan. Every section read is checked against its
//! CRC-32C trailer.

use crate::error::{Error, Result};
use crate::varint::{align16, u32_be, u64_be, uvarint, varint};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const MAGIC: u32 = 0xBAAA_D700;
const TOC_LEN: usize = 52;

pub type Labels = BTreeMap<String, String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Toc {
    pub symbols: usize,
    pub series: usize,
    pub label_indices: usize,
    pub label_offset_table: usize,
    pub postings: usize,
    pub postings_offset_table: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkMeta {
    pub min_time_ms: i64,
    pub max_time_ms: i64,
    pub reference: u64,
}

impl ChunkMeta {
    pub fn segment(&self) -> u32 {
        (self.reference >> 32) as u32
    }

    pub fn offset(&self) -> u32 {
        self.reference as u32
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesEntry {
    /// Series reference used by postings and tombstones: `offset / 16`.
    pub id: u64,
    pub labels: Labels,
    pub chunks: Vec<ChunkMeta>,
}

impl SeriesEntry {
    pub fn metric_name(&self) -> Option<&str> {
        self.labels.get("__name__").map(String::as_str)
    }
}

#[derive(Debug)]
pub struct Index {
    path: PathBuf,
    version: u8,
    symbols: Vec<String>,
    series: Vec<SeriesEntry>,
}

impl Index {
    pub fn open(path: &Path) -> Result<Self> {
        let data = std::fs::read(path).map_err(|e| Error::io(path, e))?;
        Self::parse(path, &data)
    }

    pub fn parse(path: &Path, data: &[u8]) -> Result<Self> {
        if data.len() < 5 + TOC_LEN {
            return Err(Error::parse(path, 0, "index file is too short"));
        }
        let magic = u32_be(data, 0, path)?;
        if magic != MAGIC {
            return Err(Error::parse(
                path,
                0,
                format!("invalid index magic {magic:#010x}"),
            ));
        }
        let version = data[4];
        if version != 1 && version != 2 {
            return Err(Error::Unsupported {
                path: path.to_path_buf(),
                feature: format!("index format version {version}"),
            });
        }

        let toc_pos = data.len() - TOC_LEN;
        let toc_bytes = data
            .get(toc_pos..toc_pos + 48)
            .ok_or_else(|| Error::parse(path, toc_pos, "truncated TOC"))?;
        let stored = u32_be(data, toc_pos + 48, path)?;
        if crc32c::crc32c(toc_bytes) != stored {
            return Err(Error::Checksum {
                path: path.to_path_buf(),
                section: "TOC",
                offset: toc_pos,
            });
        }
        let mut at = toc_pos;
        let mut next = || -> Result<usize> {
            let value = u64_be(data, at, path)?;
            at += 8;
            usize::try_from(value)
                .map_err(|_| Error::parse(path, at - 8, "TOC offset does not fit usize"))
        };
        let toc = Toc {
            symbols: next()?,
            series: next()?,
            label_indices: next()?,
            label_offset_table: next()?,
            postings: next()?,
            postings_offset_table: next()?,
        };
        for (name, offset) in [
            ("symbols", toc.symbols),
            ("series", toc.series),
            ("label_indices", toc.label_indices),
            ("label_offset_table", toc.label_offset_table),
            ("postings", toc.postings),
            ("postings_offset_table", toc.postings_offset_table),
        ] {
            if offset > data.len() {
                return Err(Error::parse(
                    path,
                    offset,
                    format!("{name} TOC entry points beyond the file"),
                ));
            }
        }

        let symbols = read_symbols(path, data, toc.symbols)?;
        let series = read_series(path, data, toc.series, toc.label_indices, &symbols, version)?;
        Ok(Self {
            path: path.to_path_buf(),
            version,
            symbols,
            series,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    pub fn series(&self) -> &[SeriesEntry] {
        &self.series
    }

    pub fn into_series(self) -> Vec<SeriesEntry> {
        self.series
    }
}

fn read_symbols(path: &Path, data: &[u8], offset: usize) -> Result<Vec<String>> {
    let len = u32_be(data, offset, path)? as usize;
    let start = offset
        .checked_add(4)
        .ok_or_else(|| Error::parse(path, offset, "symbol table offset overflow"))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| Error::parse(path, start, "symbol table length overflow"))?;
    let table = data
        .get(start..end)
        .ok_or_else(|| Error::parse(path, start, "truncated symbol table"))?;
    let stored = u32_be(data, end, path)?;
    if crc32c::crc32c(table) != stored {
        return Err(Error::Checksum {
            path: path.to_path_buf(),
            section: "symbol table",
            offset,
        });
    }

    let count = u32_be(table, 0, path)? as usize;
    let mut symbols = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        let (len, used) = uvarint(table, pos, path)?;
        pos += used;
        let len = usize::try_from(len)
            .map_err(|_| Error::parse(path, pos, "symbol length does not fit usize"))?;
        let raw = table
            .get(pos..pos + len)
            .ok_or_else(|| Error::parse(path, pos, "truncated symbol"))?;
        let symbol = std::str::from_utf8(raw)
            .map_err(|e| Error::parse(path, pos, format!("symbol is not UTF-8: {e}")))?;
        symbols.push(symbol.to_string());
        pos += len;
    }
    if pos != table.len() {
        return Err(Error::parse(
            path,
            pos,
            format!("symbol table has {} trailing bytes", table.len() - pos),
        ));
    }
    Ok(symbols)
}

fn read_series(
    path: &Path,
    data: &[u8],
    start: usize,
    end: usize,
    symbols: &[String],
    version: u8,
) -> Result<Vec<SeriesEntry>> {
    if end < start || end > data.len() {
        return Err(Error::parse(path, start, "invalid series section bounds"));
    }
    let mut out = Vec::new();
    let mut pos = if version == 1 { start } else { align16(start) };
    while pos + 1 < end {
        // The section is padded with zeroes between entries; entries are
        // aligned to 16 bytes so the offset can be used as the series id.
        if data[pos] == 0 {
            let next = align16(pos + 1);
            if next <= pos {
                break;
            }
            pos = next;
            continue;
        }
        let (len, used) = uvarint(data, pos, path)?;
        let len = usize::try_from(len)
            .map_err(|_| Error::parse(path, pos, "series length does not fit usize"))?;
        let body_start = pos + used;
        let body_end = body_start
            .checked_add(len)
            .ok_or_else(|| Error::parse(path, body_start, "series length overflow"))?;
        let body = data
            .get(body_start..body_end)
            .ok_or_else(|| Error::parse(path, body_start, "truncated series entry"))?;
        let stored = u32_be(data, body_end, path)?;
        if crc32c::crc32c(body) != stored {
            return Err(Error::Checksum {
                path: path.to_path_buf(),
                section: "series",
                offset: pos,
            });
        }
        let id = if version == 1 {
            pos as u64
        } else {
            (pos / 16) as u64
        };
        out.push(parse_series_body(
            path, id, body, data, symbols, version, body_start,
        )?);
        pos = if version == 1 {
            body_end + 4
        } else {
            align16(body_end + 4)
        };
    }
    Ok(out)
}

fn parse_series_body(
    path: &Path,
    id: u64,
    body: &[u8],
    index_data: &[u8],
    symbols: &[String],
    version: u8,
    body_offset: usize,
) -> Result<SeriesEntry> {
    let mut pos = 0usize;
    let (label_count, used) = uvarint(body, pos, path)?;
    pos += used;
    let label_count = usize::try_from(label_count)
        .map_err(|_| Error::parse(path, body_offset + pos, "label count too large"))?;
    let mut labels = Labels::new();
    for _ in 0..label_count {
        let (name_ref, used) = uvarint(body, pos, path)?;
        pos += used;
        let (value_ref, used) = uvarint(body, pos, path)?;
        pos += used;
        let name = symbol(
            path,
            index_data,
            symbols,
            version,
            name_ref,
            body_offset + pos,
        )?;
        let value = symbol(
            path,
            index_data,
            symbols,
            version,
            value_ref,
            body_offset + pos,
        )?;
        if labels.insert(name.to_string(), value.to_string()).is_some() {
            return Err(Error::parse(
                path,
                body_offset + pos,
                format!("series {id} has duplicate label {name}"),
            ));
        }
    }

    let (chunk_count, used) = uvarint(body, pos, path)?;
    pos += used;
    let chunk_count = usize::try_from(chunk_count)
        .map_err(|_| Error::parse(path, body_offset + pos, "chunk count too large"))?;
    let mut chunks = Vec::with_capacity(chunk_count);
    let mut prev_maxt = 0i64;
    let mut prev_ref = 0u64;
    for index in 0..chunk_count {
        let (min_time_ms, max_time_ms, reference);
        if index == 0 {
            let (mint, used) = varint(body, pos, path)?;
            pos += used;
            let (span, used) = uvarint(body, pos, path)?;
            pos += used;
            let (r, used) = uvarint(body, pos, path)?;
            pos += used;
            min_time_ms = mint;
            max_time_ms = mint
                .checked_add(i64::try_from(span).map_err(|_| {
                    Error::parse(path, body_offset + pos, "chunk span does not fit i64")
                })?)
                .ok_or_else(|| Error::parse(path, body_offset + pos, "chunk max time overflow"))?;
            reference = r;
        } else {
            let (gap, used) = uvarint(body, pos, path)?;
            pos += used;
            let (span, used) = uvarint(body, pos, path)?;
            pos += used;
            let (ref_delta, used) = varint(body, pos, path)?;
            pos += used;
            min_time_ms = prev_maxt
                .checked_add(i64::try_from(gap).map_err(|_| {
                    Error::parse(path, body_offset + pos, "chunk time gap does not fit i64")
                })?)
                .ok_or_else(|| Error::parse(path, body_offset + pos, "chunk min time overflow"))?;
            max_time_ms = min_time_ms
                .checked_add(i64::try_from(span).map_err(|_| {
                    Error::parse(path, body_offset + pos, "chunk span does not fit i64")
                })?)
                .ok_or_else(|| Error::parse(path, body_offset + pos, "chunk max time overflow"))?;
            reference = if ref_delta >= 0 {
                prev_ref.checked_add(ref_delta as u64)
            } else {
                prev_ref.checked_sub(ref_delta.unsigned_abs())
            }
            .ok_or_else(|| Error::parse(path, body_offset + pos, "chunk reference overflow"))?;
        }
        if max_time_ms < min_time_ms {
            return Err(Error::parse(
                path,
                body_offset + pos,
                format!("series {id} chunk {index} has maxt < mint"),
            ));
        }
        prev_maxt = max_time_ms;
        prev_ref = reference;
        chunks.push(ChunkMeta {
            min_time_ms,
            max_time_ms,
            reference,
        });
    }
    if pos != body.len() {
        return Err(Error::parse(
            path,
            body_offset + pos,
            format!("series {id} has {} trailing bytes", body.len() - pos),
        ));
    }
    Ok(SeriesEntry { id, labels, chunks })
}

fn symbol<'a>(
    path: &Path,
    index_data: &'a [u8],
    symbols: &'a [String],
    version: u8,
    reference: u64,
    offset: usize,
) -> Result<&'a str> {
    let reference = usize::try_from(reference)
        .map_err(|_| Error::parse(path, offset, "symbol reference does not fit usize"))?;
    if version == 1 {
        // V1 symbol references are absolute byte offsets into the index file.
        let start = reference;
        let (len, used) = uvarint(index_data, start, path)
            .map_err(|_| Error::parse(path, offset, "invalid V1 symbol offset"))?;
        let body = start + used;
        let len = usize::try_from(len)
            .map_err(|_| Error::parse(path, body, "V1 symbol length does not fit usize"))?;
        let raw = index_data
            .get(body..body + len)
            .ok_or_else(|| Error::parse(path, body, "truncated V1 symbol"))?;
        return std::str::from_utf8(raw)
            .map_err(|e| Error::parse(path, body, format!("V1 symbol is not UTF-8: {e}")));
    }
    symbols.get(reference).map(String::as_str).ok_or_else(|| {
        Error::parse(
            path,
            offset,
            format!("symbol reference {reference} is out of range"),
        )
    })
}
