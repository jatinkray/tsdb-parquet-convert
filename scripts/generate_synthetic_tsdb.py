#!/usr/bin/env python3
"""Generate a Prometheus TSDB v2 block without a Prometheus install.

The generator is intentionally small and streaming: it writes one chunk segment,
then builds the index from chunk references kept in memory. It exists so the
100M-sample conversion benchmark is reproducible on a machine without a live
Prometheus or enough free space for a TSV staging copy.
"""

from __future__ import annotations

import argparse
import json
import os
import struct
import zlib
from pathlib import Path

MAGIC_INDEX = 0xBAAAD700
MAGIC_CHUNKS = 0x85BD40DD
POLLY = 0x82F63B78


def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (POLLY if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def uvarint(value: int) -> bytes:
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def varint(value: int) -> bytes:
    return uvarint((value << 1) ^ (value >> 63))


class BitWriter:
    def __init__(self) -> None:
        self.data = bytearray()
        self.current = 0
        self.used = 0

    def bit(self, value: int) -> None:
        self.current = (self.current << 1) | (value & 1)
        self.used += 1
        if self.used == 8:
            self.data.append(self.current)
            self.current = 0
            self.used = 0

    def bits(self, value: int, count: int) -> None:
        for shift in range(count - 1, -1, -1):
            self.bit((value >> shift) & 1)

    def uvarint(self, value: int) -> None:
        while value >= 0x80:
            self.bits((value & 0x7F) | 0x80, 8)
            value >>= 7
        self.bits(value, 8)

    def varint(self, value: int) -> None:
        self.uvarint((value << 1) ^ (value >> 63))

    def finish(self) -> bytes:
        if self.used:
            self.data.append(self.current << (8 - self.used))
            self.current = 0
            self.used = 0
        return bytes(self.data)


def xor_chunk(first_ts: int, count: int, delta: int, value: float) -> bytes:
    bits = BitWriter()
    bits.varint(first_ts)
    bits.bits(struct.unpack(">Q", struct.pack(">d", value))[0], 64)
    if count > 1:
        bits.uvarint(delta)
        bits.bit(0)  # second value equals the first
        for _ in range(2, count):
            bits.bit(0)  # delta-of-delta is zero
            bits.bit(0)  # value is unchanged
    body = struct.pack(">H", count) + bits.finish()
    return body


def write_chunks(path: Path, series: list[list[tuple[int, int, int]]], samples: int,
                 chunk_samples: int, delta: int) -> int:
    """Write chunks; returns samples written."""
    written = 0
    with path.open("wb") as fh:
        fh.write(struct.pack(">IBxxx", MAGIC_CHUNKS, 1))
        for chunks in series:
            for chunk_index, (first_ts, count, value_index) in enumerate(chunks):
                body = xor_chunk(first_ts, count, delta, float(value_index))
                offset = fh.tell()
                record = uvarint(len(body)) + b"\x01" + body
                record += struct.pack(">I", crc32c(b"\x01" + body))
                fh.write(record)
                chunks[chunk_index] = (first_ts, count, offset)
                written += count
    return written


def build_series_body(labels: list[tuple[int, int]], chunks: list[tuple[int, int, int]],
                      delta: int) -> bytes:
    out = bytearray()
    out += uvarint(len(labels))
    for name, value in labels:
        out += uvarint(name) + uvarint(value)
    out += uvarint(len(chunks))
    prev_maxt = 0
    prev_ref = 0
    for index, (mint, count, ref) in enumerate(chunks):
        span = (count - 1) * delta
        if index == 0:
            out += varint(mint) + uvarint(span) + uvarint(ref)
        else:
            gap = mint - prev_maxt
            out += uvarint(gap) + uvarint(span) + varint(ref - prev_ref)
        prev_maxt = mint + span
        prev_ref = ref
    return bytes(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("out", type=Path)
    ap.add_argument("--samples", type=int, default=100_000_000)
    ap.add_argument("--series", type=int, default=1_000)
    ap.add_argument("--chunk-samples", type=int, default=120)
    ap.add_argument("--interval-ms", type=int, default=15_000)
    ap.add_argument("--start-ms", type=int, default=1_700_000_000_000)
    args = ap.parse_args()

    out = args.out
    (out / "chunks").mkdir(parents=True, exist_ok=True)
    per_series = args.samples // args.series
    if per_series * args.series != args.samples:
        raise SystemExit("--samples must be divisible by --series")
    full, remainder = divmod(per_series, args.chunk_samples)
    counts = [args.chunk_samples] * full
    if remainder:
        counts.append(remainder)

    plan: list[list[tuple[int, int, int]]] = []
    for _ in range(args.series):
        chunks = []
        ts = args.start_ms
        for count in counts:
            chunks.append((ts, count, 0))
            ts += count * args.interval_ms
        plan.append(chunks)

    written = write_chunks(out / "chunks" / "000001", plan, args.samples,
                           args.chunk_samples, args.interval_ms)
    if written != args.samples:
        raise SystemExit(f"wrote {written}, expected {args.samples}")

    symbols = ["__name__", "synthetic_metric", "series"]
    for index in range(args.series):
        symbols.append(f"s{index:06d}")
    symbol_refs = {symbol: index for index, symbol in enumerate(symbols)}
    symbol_body = bytearray(struct.pack(">I", len(symbols)))
    for symbol in symbols:
        raw = symbol.encode()
        symbol_body += uvarint(len(raw)) + raw

    index = bytearray(struct.pack(">IB", MAGIC_INDEX, 2))
    index += b"\x00" * 11
    symbols_offset = len(index)
    index += struct.pack(">I", len(symbol_body)) + symbol_body
    index += struct.pack(">I", crc32c(symbol_body))
    while len(index) % 16:
        index.append(0)
    series_offset = len(index)
    for index_no, chunks in enumerate(plan):
        body = build_series_body(
            [(symbol_refs["__name__"], symbol_refs["synthetic_metric"]),
             (symbol_refs["series"], symbol_refs[f"s{index_no:06d}"])],
            chunks, args.interval_ms)
        entry = uvarint(len(body)) + body + struct.pack(">I", crc32c(body))
        index += entry
        while len(index) % 16:
            index.append(0)
    series_end = len(index)
    toc = struct.pack(">6Q", symbols_offset, series_offset, series_end, 0, 0, 0)
    index += toc + struct.pack(">I", crc32c(toc))
    (out / "index").write_bytes(index)

    max_time = args.start_ms + per_series * args.interval_ms
    meta = {
        "ulid": "01J0000000000000000000000",
        "minTime": args.start_ms,
        "maxTime": max_time,
        "stats": {
            "numSamples": args.samples,
            "numSeries": args.series,
            "numChunks": args.series * len(counts),
        },
        "compaction": {"level": 1, "sources": ["synthetic"]},
    }
    (out / "meta.json").write_text(json.dumps(meta) + "\n")
    print(json.dumps({
        "block": str(out),
        "samples": args.samples,
        "series": args.series,
        "chunks": args.series * len(counts),
        "bytes": sum(p.stat().st_size for p in out.rglob("*") if p.is_file()),
    }))


if __name__ == "__main__":
    main()
