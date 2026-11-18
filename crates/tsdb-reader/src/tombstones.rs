//! Prometheus tombstones: deleted time intervals keyed by series reference.

use crate::error::{Error, Result};
use crate::varint::{u32_be, uvarint, varint};
use std::collections::BTreeMap;
use std::path::Path;

pub const MAGIC: u32 = 0x0130_BA30;
const HEADER_LEN: usize = 5;
const CRC_LEN: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interval {
    pub mint: i64,
    pub maxt: i64,
}

impl Interval {
    pub fn contains(&self, timestamp_ms: i64) -> bool {
        timestamp_ms >= self.mint && timestamp_ms <= self.maxt
    }
}

#[derive(Debug, Clone, Default)]
pub struct Tombstones {
    intervals: BTreeMap<u64, Vec<Interval>>,
}

impl Tombstones {
    pub fn is_empty(&self) -> bool {
        self.intervals.is_empty()
    }

    pub fn intervals_for(&self, series: u64) -> &[Interval] {
        self.intervals.get(&series).map_or(&[], Vec::as_slice)
    }

    pub fn is_deleted(&self, series: u64, timestamp_ms: i64) -> bool {
        self.intervals_for(series)
            .iter()
            .any(|interval| interval.contains(timestamp_ms))
    }

    pub fn parse(path: &Path, data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_LEN + CRC_LEN {
            return Err(Error::parse(path, 0, "tombstones file is too short"));
        }
        let magic = u32_be(data, 0, path)?;
        if magic != MAGIC {
            return Err(Error::parse(
                path,
                0,
                format!("invalid tombstones magic {magic:#010x}"),
            ));
        }
        let version = data[4];
        if version != 1 {
            return Err(Error::Unsupported {
                path: path.to_path_buf(),
                feature: format!("tombstones format version {version}"),
            });
        }
        let body_end = data.len() - CRC_LEN;
        let stored = u32_be(data, body_end, path)?;
        if crc32c::crc32c(&data[HEADER_LEN..body_end]) != stored {
            return Err(Error::Checksum {
                path: path.to_path_buf(),
                section: "tombstones",
                offset: 0,
            });
        }

        let mut intervals: BTreeMap<u64, Vec<Interval>> = BTreeMap::new();
        let mut pos = HEADER_LEN;
        while pos < body_end {
            let (series, used) = uvarint(data, pos, path)?;
            pos += used;
            let (mint, used) = varint(data, pos, path)?;
            pos += used;
            let (maxt, used) = varint(data, pos, path)?;
            pos += used;
            if maxt < mint {
                return Err(Error::parse(
                    path,
                    pos,
                    "tombstone interval has maxt < mint",
                ));
            }
            intervals
                .entry(series)
                .or_default()
                .push(Interval { mint, maxt });
        }
        for values in intervals.values_mut() {
            values.sort_by_key(|interval| (interval.mint, interval.maxt));
        }
        Ok(Self { intervals })
    }

    pub fn read(block_dir: &Path) -> Result<Self> {
        let path = block_dir.join("tombstones");
        match std::fs::read(&path) {
            Ok(data) => Self::parse(&path, &data),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(Error::io(path, error)),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn encode(series: u64, mint: i64, maxt: i64) -> Vec<u8> {
        let mut body = vec![1u8];
        put_uvarint(&mut body, series);
        put_varint(&mut body, mint);
        put_varint(&mut body, maxt);
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC.to_be_bytes());
        out.extend_from_slice(&body);
        let crc = crc32c::crc32c(&body[1..]);
        out.extend_from_slice(&crc.to_be_bytes());
        out
    }

    fn put_uvarint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    fn put_varint(out: &mut Vec<u8>, value: i64) {
        put_uvarint(out, ((value << 1) ^ (value >> 63)) as u64);
    }

    #[test]
    fn parses_intervals_and_checks_crc() {
        let path = std::path::PathBuf::from("tombstones");
        let bytes = encode(42, 100, 200);
        let tombstones = Tombstones::parse(&path, &bytes).unwrap();
        assert!(tombstones.is_deleted(42, 150));
        assert!(!tombstones.is_deleted(42, 201));
        assert!(!tombstones.is_deleted(43, 150));

        let mut corrupt = bytes;
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(Tombstones::parse(&path, &corrupt).is_err());
    }
}
