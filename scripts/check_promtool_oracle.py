#!/usr/bin/env python3
"""Compare `promtool tsdb dump` output with the native reader's TSV dump.

promtool line:  {label="value",...} <float> <timestamp_ms>
native line:    <labels-json>\t<timestamp_ms>\t<f64-bits-hex>
"""

import json
import struct
import sys


def parse_promtool(line):
    labels_text, value_text, ts_text = line.rsplit(" ", 2)
    labels = {}
    assert labels_text.startswith("{") and labels_text.endswith("}")
    body = labels_text[1:-1]
    i = 0
    while i < len(body):
        while i < len(body) and body[i] == " ":
            i += 1
        eq = body.index("=", i)
        key = body[i:eq].strip()
        i = eq + 1
        assert body[i] == '"'
        i += 1
        value = []
        while body[i] != '"':
            if body[i] == "\\":
                i += 1
            value.append(body[i])
            i += 1
        i += 1
        labels[key] = "".join(value)
        if i < len(body):
            assert body[i] == ","
            i += 1
    return labels, float(value_text), int(ts_text)


def main():
    promtool_path, native_path = sys.argv[1], sys.argv[2]
    checked = 0
    with open(promtool_path, encoding="utf-8") as prom, open(native_path, encoding="utf-8") as native:
        for prom_line, native_line in zip(prom, native):
            labels, value, ts = parse_promtool(prom_line.rstrip("\n"))
            labels_json, native_ts, bits_hex = native_line.rstrip("\n").split("\t")
            native_labels = json.loads(labels_json)
            native_ts = int(native_ts)
            native_bits = int(bits_hex, 16)
            native_value = struct.unpack(">d", struct.pack(">Q", native_bits))[0]
            if labels != native_labels or ts != native_ts:
                raise SystemExit(
                    f"series mismatch at sample {checked}: promtool={labels, ts} native={native_labels, native_ts}"
                )
            if value != native_value and not (
                value != value and native_value != native_value
            ):
                raise SystemExit(
                    f"value mismatch at sample {checked}: promtool={value!r} native={native_value!r}"
                )
            checked += 1
        extra_prom = prom.readline()
        extra_native = native.readline()
        if extra_prom or extra_native:
            raise SystemExit("sample count mismatch")
    print(f"oracle OK: {checked} samples match promtool sample-for-sample")


if __name__ == "__main__":
    main()
