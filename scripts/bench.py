#!/usr/bin/env python3
"""Convert the repository fixture and compare the result with a baseline.

Usage: scripts/bench.py <binary> <fixture> <baseline.json> [--update]

Fails when throughput drops below 70% of the baseline or when bytes per sample
grows by more than 30%, so a slow regression cannot ship unnoticed.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

THROUGHPUT_FLOOR = 0.70
BYTES_CEILING = 1.30


def run(binary: str, fixture: str) -> dict[str, float]:
    with tempfile.TemporaryDirectory() as out:
        started = time.perf_counter()
        completed = subprocess.run(
            [binary, "convert", fixture, "--out", out, "--preset", "series"],
            capture_output=True,
            text=True,
            check=False,
        )
        elapsed = time.perf_counter() - started
        if completed.returncode != 0:
            print(completed.stdout, completed.stderr, sep="\n", file=sys.stderr)
            raise SystemExit(f"convert exited {completed.returncode}")
        manifest = json.loads(Path(out, "manifest.json").read_text())
        rows = manifest["totals"]["rows"]
        byte_count = manifest["totals"]["bytes"]
    return {
        "rows": rows,
        "bytes": byte_count,
        "seconds": round(elapsed, 3),
        "samples_per_second": round(rows / elapsed),
        "bytes_per_sample": round(byte_count / rows, 3),
    }


def main(argv: list[str]) -> int:
    if len(argv) < 4:
        print(__doc__, file=sys.stderr)
        return 2
    binary, fixture, baseline_path = argv[1], argv[2], Path(argv[3])
    update = "--update" in argv[4:]
    measured = run(binary, fixture)
    measured["tool"] = subprocess.run(
        [binary, "--version"], capture_output=True, text=True, check=False
    ).stdout.strip()

    if update or not baseline_path.exists():
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        baseline_path.write_text(json.dumps(measured, indent=2) + "\n")
        print(json.dumps(measured, indent=2))
        return 0

    baseline = json.loads(baseline_path.read_text())
    print(json.dumps({"measured": measured, "baseline": baseline}, indent=2))

    failures = []
    throughput = measured["samples_per_second"] / baseline["samples_per_second"]
    if throughput < THROUGHPUT_FLOOR:
        failures.append(
            f"throughput {measured['samples_per_second']}/s is {throughput:.0%} of the baseline"
        )
    bytes_ratio = measured["bytes_per_sample"] / baseline["bytes_per_sample"]
    if bytes_ratio > BYTES_CEILING:
        failures.append(
            f"{measured['bytes_per_sample']} B/sample is {bytes_ratio:.0%} of the baseline"
        )
    for failure in failures:
        print(f"regression: {failure}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
