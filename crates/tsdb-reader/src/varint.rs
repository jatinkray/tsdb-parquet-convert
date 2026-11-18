//! Checked big-endian and varint readers used by the TSDB formats.

use crate::error::{Error, Result};
use std::path::Path;

/// Reads an unsigned LEB128 `u64`, rejecting overflow and over-long encodings.
pub fn uvarint(data: &[u8], pos: usize, path: &Path) -> Result<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0u32;
    let mut cursor = pos;
    for _ in 0..10 {
        let byte = *data
            .get(cursor)
            .ok_or_else(|| Error::parse(path, cursor, "truncated varint"))?;
        cursor += 1;
        if shift == 63 && byte & 0xfe != 0 {
            return Err(Error::parse(path, pos, "varint overflows u64"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, cursor - pos));
        }
        shift += 7;
    }
    Err(Error::parse(path, pos, "varint is longer than 10 bytes"))
}

/// Reads a signed varint encoded as zig-zag + uvarint.
pub fn varint(data: &[u8], pos: usize, path: &Path) -> Result<(i64, usize)> {
    let (raw, used) = uvarint(data, pos, path)?;
    Ok((((raw >> 1) as i64) ^ -((raw & 1) as i64), used))
}

pub fn u32_be(data: &[u8], pos: usize, path: &Path) -> Result<u32> {
    let bytes = data
        .get(pos..pos + 4)
        .ok_or_else(|| Error::parse(path, pos, "truncated u32"))?;
    Ok(u32::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| Error::parse(path, pos, "invalid u32"))?,
    ))
}

pub fn u64_be(data: &[u8], pos: usize, path: &Path) -> Result<u64> {
    let bytes = data
        .get(pos..pos + 8)
        .ok_or_else(|| Error::parse(path, pos, "truncated u64"))?;
    Ok(u64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| Error::parse(path, pos, "invalid u64"))?,
    ))
}

pub fn align16(value: usize) -> usize {
    (value + 15) & !15
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use std::path::PathBuf;

    #[test]
    fn decodes_unsigned_and_zigzag_values() {
        let path = PathBuf::from("test");
        // 300 = 0xAC 0x02
        assert_eq!(uvarint(&[0xac, 0x02], 0, &path).unwrap(), (300, 2));
        // 300 zig-zag => 600 = 0xD8 0x04; -300 => 599 = 0xD7 0x04
        assert_eq!(varint(&[0xd8, 0x04], 0, &path).unwrap(), (300, 2));
        assert_eq!(varint(&[0xd7, 0x04], 0, &path).unwrap(), (-300, 2));
    }

    #[test]
    fn rejects_truncated_and_overlong_varints() {
        let path = PathBuf::from("test");
        assert!(uvarint(&[0x80], 0, &path).is_err());
        assert!(uvarint(&[0xff; 11], 0, &path).is_err());
        assert!(u32_be(&[0, 0], 0, &path).is_err());
    }

    #[test]
    fn aligns_to_series_entry_boundaries() {
        assert_eq!(align16(0), 0);
        assert_eq!(align16(1), 16);
        assert_eq!(align16(16), 16);
        assert_eq!(align16(17), 32);
    }
}
