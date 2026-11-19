//! Prometheus chunk-file reader with CRC checking and XOR float decoding.
//!
//! Chunk files are split into <=512 MiB segments. A chunk reference packs the
//! segment sequence in the high 32 bits and the byte offset in the low 32.

use crate::error::{Error, Result};
use crate::varint::uvarint;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

pub const MAGIC: u32 = 0x85BD_40DD;
const FILE_HEADER_LEN: u64 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Xor,
    Histogram,
    FloatHistogram,
    Xor2,
    HistogramSt,
    FloatHistogramSt,
}

impl Encoding {
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Xor),
            2 => Some(Self::Histogram),
            3 => Some(Self::FloatHistogram),
            4 => Some(Self::Xor2),
            5 => Some(Self::HistogramSt),
            6 => Some(Self::FloatHistogramSt),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Xor => "XOR",
            Self::Histogram => "histogram",
            Self::FloatHistogram => "float histogram",
            Self::Xor2 => "XOR2",
            Self::HistogramSt => "histogramST",
            Self::FloatHistogramSt => "float histogramST",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FloatSample {
    pub timestamp_ms: i64,
    pub value: f64,
}

impl FloatSample {
    pub fn is_stale_marker(&self) -> bool {
        self.value.to_bits() == 0x7ff0_0000_0000_0002
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OwnedHistogram {
    pub timestamp_ms: i64,
    pub count: f64,
    pub zero_count: f64,
    pub sum: f64,
    pub schema: i32,
    pub zero_threshold: f64,
    pub pos_spans: Vec<tsdb_reader_upstream::Span>,
    pub neg_spans: Vec<tsdb_reader_upstream::Span>,
    pub pos_buckets: Vec<f64>,
    pub neg_buckets: Vec<f64>,
    pub custom_values: Vec<f64>,
    pub reset_hint: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChunkSamples {
    Floats(Vec<FloatSample>),
    Histograms(Vec<OwnedHistogram>),
}

#[derive(Default)]
struct DecodeSink {
    floats: Vec<FloatSample>,
    histograms: Vec<OwnedHistogram>,
}

impl tsdb_reader_upstream::SampleSink for DecodeSink {
    fn push_float(&mut self, ts: i64, value: f64) {
        self.floats.push(FloatSample {
            timestamp_ms: ts,
            value,
        });
    }

    fn push_histogram(&mut self, histogram: &tsdb_reader_upstream::HistogramSample<'_>) {
        self.histograms.push(OwnedHistogram {
            timestamp_ms: histogram.ts,
            count: histogram.count,
            zero_count: histogram.zero_count,
            sum: histogram.sum,
            schema: histogram.schema,
            zero_threshold: histogram.zero_threshold,
            pos_spans: histogram.pos_spans.to_vec(),
            neg_spans: histogram.neg_spans.to_vec(),
            pos_buckets: histogram.pos_buckets.to_vec(),
            neg_buckets: histogram.neg_buckets.to_vec(),
            custom_values: histogram.custom_values.to_vec(),
            reset_hint: histogram.reset_hint,
        });
    }
}

#[derive(Debug)]
pub struct ChunkReader {
    block_dir: PathBuf,
}

impl ChunkReader {
    pub fn new(block_dir: impl Into<PathBuf>) -> Self {
        Self {
            block_dir: block_dir.into(),
        }
    }

    pub fn read_chunk(&self, reference: u64) -> Result<(Encoding, ChunkSamples)> {
        let segment = (reference >> 32) as u32;
        let offset = reference as u32;
        let path = self
            .block_dir
            .join("chunks")
            .join(format!("{:06}", segment + 1));
        let mut file = std::fs::File::open(&path).map_err(|e| Error::io(&path, e))?;
        verify_segment_header(&path, &mut file)?;
        file.seek(SeekFrom::Start(u64::from(offset)))
            .map_err(|e| Error::io(&path, e))?;

        let mut len_bytes = Vec::with_capacity(10);
        loop {
            let mut byte = [0u8; 1];
            file.read_exact(&mut byte)
                .map_err(|e| Error::io(&path, e))?;
            len_bytes.push(byte[0]);
            if byte[0] & 0x80 == 0 {
                break;
            }
            if len_bytes.len() == 10 {
                return Err(Error::parse(
                    &path,
                    FILE_HEADER_LEN as usize + offset as usize,
                    "chunk length varint is too long",
                ));
            }
        }
        let (len, _) = uvarint(&len_bytes, 0, &path)?;
        let len = usize::try_from(len)
            .map_err(|_| Error::parse(&path, 0, "chunk length does not fit usize"))?;
        let mut encoding_byte = [0u8; 1];
        file.read_exact(&mut encoding_byte)
            .map_err(|e| Error::io(&path, e))?;
        let encoding = Encoding::from_byte(encoding_byte[0]).ok_or_else(|| Error::Unsupported {
            path: path.clone(),
            feature: format!("chunk encoding {}", encoding_byte[0]),
        })?;
        let mut data = vec![0u8; len];
        file.read_exact(&mut data)
            .map_err(|e| Error::io(&path, e))?;
        let mut crc_bytes = [0u8; 4];
        file.read_exact(&mut crc_bytes)
            .map_err(|e| Error::io(&path, e))?;
        let stored = u32::from_be_bytes(crc_bytes);
        let mut checked = Vec::with_capacity(len + 1);
        checked.push(encoding_byte[0]);
        checked.extend_from_slice(&data);
        if crc32c::crc32c(&checked) != stored {
            return Err(Error::Checksum {
                path,
                section: "chunk",
                offset: (u64::from(offset)) as usize,
            });
        }

        if encoding == Encoding::Xor {
            return Ok((encoding, ChunkSamples::Floats(decode_xor(&path, &data)?)));
        }
        let mut sink = DecodeSink::default();
        tsdb_reader_upstream::chunkenc::decode(encoding_byte[0], &data, &mut sink).map_err(
            |error| Error::parse(&path, 0, format!("{} decode: {error}", encoding.name())),
        )?;
        if !sink.histograms.is_empty() {
            Ok((encoding, ChunkSamples::Histograms(sink.histograms)))
        } else {
            Ok((encoding, ChunkSamples::Floats(sink.floats)))
        }
    }
}

fn verify_segment_header(path: &Path, file: &mut std::fs::File) -> Result<()> {
    let mut header = [0u8; FILE_HEADER_LEN as usize];
    file.read_exact(&mut header)
        .map_err(|e| Error::io(path, e))?;
    let magic = u32::from_be_bytes(
        header[0..4]
            .try_into()
            .map_err(|_| Error::parse(path, 0, "invalid chunk segment header"))?,
    );
    if magic != MAGIC {
        return Err(Error::parse(
            path,
            0,
            format!("invalid chunk segment magic {magic:#010x}"),
        ));
    }
    if header[4] != 1 {
        return Err(Error::Unsupported {
            path: path.to_path_buf(),
            feature: format!("chunk segment version {}", header[4]),
        });
    }
    Ok(())
}

/// Decodes the XOR float chunk body (the bytes after the encoding byte).
pub fn decode_xor(path: &Path, data: &[u8]) -> Result<Vec<FloatSample>> {
    if data.len() < 2 {
        return Err(Error::parse(
            path,
            0,
            "XOR chunk is shorter than its sample count",
        ));
    }
    let total = u16::from_be_bytes([data[0], data[1]]) as usize;
    if total == 0 {
        return Ok(Vec::new());
    }
    let mut bits = BitReader::new(&data[2..]);
    let mut out = Vec::with_capacity(total);
    let first_ts = bits.read_varint().map_err(|e| bit_error(path, e))?;
    let first_bits = bits.read_bits(64).map_err(|e| bit_error(path, e))?;
    let mut timestamp = first_ts;
    let mut value_bits = first_bits;
    let mut leading = 0xffu8;
    let mut trailing = 0u8;
    out.push(FloatSample {
        timestamp_ms: timestamp,
        value: f64::from_bits(value_bits),
    });
    if total == 1 {
        return Ok(out);
    }

    let mut delta = bits.read_uvarint().map_err(|e| bit_error(path, e))?;
    timestamp = timestamp
        .checked_add(
            i64::try_from(delta)
                .map_err(|_| Error::parse(path, 0, "XOR timestamp delta does not fit i64"))?,
        )
        .ok_or_else(|| Error::parse(path, 0, "XOR timestamp overflow"))?;
    read_value(&mut bits, &mut value_bits, &mut leading, &mut trailing)
        .map_err(|e| bit_error(path, e))?;
    out.push(FloatSample {
        timestamp_ms: timestamp,
        value: f64::from_bits(value_bits),
    });

    for _ in 2..total {
        let dod = {
            let mut code = 0u8;
            for _ in 0..4 {
                let bit = bits.read_bit().map_err(|e| bit_error(path, e))?;
                code = (code << 1) | bit;
                if bit == 0 {
                    break;
                }
            }
            match code {
                0b0 => 0,
                0b10 => read_signed_bits(&mut bits, 14).map_err(|e| bit_error(path, e))?,
                0b110 => read_signed_bits(&mut bits, 17).map_err(|e| bit_error(path, e))?,
                0b1110 => read_signed_bits(&mut bits, 20).map_err(|e| bit_error(path, e))?,
                0b1111 => bits.read_bits(64).map_err(|e| bit_error(path, e))? as i64,
                _ => return Err(Error::parse(path, 0, "invalid XOR timestamp control code")),
            }
        };
        let delta_i = i64::try_from(delta)
            .map_err(|_| Error::parse(path, 0, "XOR timestamp delta does not fit i64"))?;
        let next_delta = delta_i
            .checked_add(dod)
            .ok_or_else(|| Error::parse(path, 0, "XOR timestamp delta overflow"))?;
        if next_delta < 0 {
            return Err(Error::parse(path, 0, "XOR timestamp delta became negative"));
        }
        delta = next_delta as u64;
        timestamp = timestamp
            .checked_add(next_delta)
            .ok_or_else(|| Error::parse(path, 0, "XOR timestamp overflow"))?;
        read_value(&mut bits, &mut value_bits, &mut leading, &mut trailing)
            .map_err(|e| bit_error(path, e))?;
        out.push(FloatSample {
            timestamp_ms: timestamp,
            value: f64::from_bits(value_bits),
        });
    }
    Ok(out)
}

fn read_signed_bits(bits: &mut BitReader<'_>, width: u8) -> std::result::Result<i64, BitError> {
    let raw = bits.read_bits(width)?;
    // This mirrors Prometheus' XOR iterator, which treats the positive half
    // range asymmetrically (bitRange allows one extra positive value).
    if raw > (1u64 << (width - 1)) {
        Ok(raw as i64 - (1i64 << width))
    } else {
        Ok(raw as i64)
    }
}

fn read_value(
    bits: &mut BitReader<'_>,
    value_bits: &mut u64,
    leading: &mut u8,
    trailing: &mut u8,
) -> std::result::Result<(), BitError> {
    if bits.read_bit()? == 0 {
        return Ok(());
    }
    if bits.read_bit()? == 0 {
        if *leading == 0xff {
            return Err(BitError::Malformed("reused window before initialization"));
        }
        let width = 64u8
            .checked_sub(*leading)
            .and_then(|v| v.checked_sub(*trailing))
            .ok_or(BitError::Malformed("invalid XOR window"))?;
        if width == 0 || width > 64 {
            return Err(BitError::Malformed("invalid XOR reused width"));
        }
        let payload = bits.read_bits(width)?;
        *value_bits ^= payload << *trailing;
        return Ok(());
    }

    let new_leading = bits.read_bits(5)? as u8;
    let sigbits = bits.read_bits(6)? as u8;
    let sigbits = if sigbits == 0 { 64 } else { sigbits };
    let new_trailing = 64u8
        .checked_sub(new_leading)
        .and_then(|v| v.checked_sub(sigbits))
        .ok_or(BitError::Malformed("invalid XOR window width"))?;
    if sigbits > 64 || new_leading + sigbits > 64 {
        return Err(BitError::Malformed("invalid XOR significant bits"));
    }
    let payload = bits.read_bits(sigbits)?;
    *value_bits ^= payload << new_trailing;
    *leading = new_leading;
    *trailing = new_trailing;
    Ok(())
}

fn bit_error(path: &Path, error: BitError) -> Error {
    Error::parse(path, 0, error.to_string())
}

#[derive(Debug, thiserror::Error)]
enum BitError {
    #[error("truncated XOR bit stream")]
    Eof,
    #[error("{0}")]
    Malformed(&'static str),
}

struct BitReader<'a> {
    data: &'a [u8],
    bit: usize,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, bit: 0 }
    }

    fn read_bit(&mut self) -> std::result::Result<u8, BitError> {
        let byte = *self.data.get(self.bit / 8).ok_or(BitError::Eof)?;
        let bit = (byte >> (7 - (self.bit % 8))) & 1;
        self.bit += 1;
        Ok(bit)
    }

    fn read_bits(&mut self, count: u8) -> std::result::Result<u64, BitError> {
        if count == 0 {
            return Ok(0);
        }
        if count > 64 {
            return Err(BitError::Malformed("bit count exceeds 64"));
        }
        let end = self.bit.checked_add(count as usize).ok_or(BitError::Eof)?;
        if end > self.data.len() * 8 {
            return Err(BitError::Eof);
        }
        let mut value = 0u64;
        for _ in 0..count {
            value = (value << 1) | u64::from(self.read_bit()?);
        }
        Ok(value)
    }

    fn read_uvarint(&mut self) -> std::result::Result<u64, BitError> {
        let mut value = 0u64;
        let mut shift = 0u32;
        for _ in 0..10 {
            let byte = self.read_bits(8)? as u8;
            if shift == 63 && byte & 0xfe != 0 {
                return Err(BitError::Malformed("varint overflows u64"));
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
        }
        Err(BitError::Malformed("varint is too long"))
    }

    fn read_varint(&mut self) -> std::result::Result<i64, BitError> {
        let raw = self.read_uvarint()?;
        Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn decodes_single_sample_chunk() {
        // count=1, zig-zag timestamp 1000 => 2000 (0xD0 0x0F), value 1.5.
        let mut data = vec![0, 1];
        data.extend_from_slice(&[0xd0, 0x0f]);
        data.extend_from_slice(&1.5f64.to_bits().to_be_bytes());
        let samples = decode_xor(Path::new("chunk"), &data).unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].timestamp_ms, 1000);
        assert_eq!(samples[0].value, 1.5);
    }

    #[test]
    fn rejects_truncated_chunk() {
        assert!(decode_xor(Path::new("chunk"), &[0, 2, 0]).is_err());
    }

    #[test]
    fn bit_reader_is_msb_first() {
        let mut reader = BitReader::new(&[0b1010_0000]);
        assert_eq!(reader.read_bits(4).unwrap(), 0b1010);
        assert_eq!(reader.read_bit().unwrap(), 0);
    }
}

#[cfg(test)]
mod segment_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn uvarint_bytes(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
        out
    }

    #[test]
    fn reads_a_chunk_from_a_second_segment() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("chunks")).unwrap();
        let mut body = vec![0, 1, 0xd0, 0x0f];
        body.extend_from_slice(&1.5f64.to_bits().to_be_bytes());
        let mut record = uvarint_bytes(body.len() as u64);
        record.push(1);
        record.extend_from_slice(&body);
        record.extend_from_slice(
            &crc32c::crc32c(&{
                let mut checked = vec![1];
                checked.extend_from_slice(&body);
                checked
            })
            .to_be_bytes(),
        );

        let mut segment = Vec::new();
        segment.extend_from_slice(&MAGIC.to_be_bytes());
        segment.extend_from_slice(&[1, 0, 0, 0]);
        segment.extend_from_slice(&record);
        std::fs::write(dir.path().join("chunks/000002"), segment).unwrap();

        let reader = ChunkReader::new(dir.path());
        let (encoding, samples) = reader.read_chunk((1u64 << 32) | 8).unwrap();
        assert_eq!(encoding, Encoding::Xor);
        match samples {
            ChunkSamples::Floats(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].timestamp_ms, 1000);
                assert_eq!(samples[0].value, 1.5);
            }
            ChunkSamples::Histograms(_) => panic!("expected float samples"),
        }
    }
}
