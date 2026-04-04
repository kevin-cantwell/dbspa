#!/usr/bin/env python3
"""
Print a formatted table from the latest (or a specified) stress result JSON.

Usage:
    python3 scripts/stress-report.py                       # latest result
    python3 scripts/stress-report.py stress/results/X.json # specific file
"""

import json
import sys
import os
import glob


def find_latest():
    pattern = os.path.join(os.path.dirname(__file__), "../stress/results/[0-9]*.json")
    results = sorted(glob.glob(pattern))
    if not results:
        print("No stress results found in stress/results/", file=sys.stderr)
        sys.exit(1)
    return results[-1]


def fmt_throughput(n):
    if n is None or n == 0:
        return "—"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))


def fmt_rss(mb):
    if mb is None:
        return "—"
    return f"{mb:.1f} MB"


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else find_latest()
    with open(path) as f:
        data = json.load(f)

    print(f"Stress results: {os.path.basename(path)}")
    print(f"  dbspa:  {data.get('dbspa_version', '?')}")
    print(f"  go:     {data.get('go_version', '?')}")
    print(f"  cpu:    {data.get('cpu', '?')}")
    print(f"  os:     {data.get('os', '?')}/{data.get('arch', '?')}")
    print()

    col_w = [38, 6, 12, 10, 10, 11]
    headers = ["scenario", "pass", "rec/sec", "peak RSS", "final RSS", "duration"]
    sep = "  "
    row_fmt = sep.join(f"{{:<{w}}}" for w in col_w)

    print(row_fmt.format(*headers))
    print(sep.join("-" * w for w in col_w))

    for r in data["results"]:
        row = [
            r["name"],
            "✓" if r["passed"] else "✗",
            fmt_throughput(r.get("throughput_avg")),
            fmt_rss(r.get("peak_rss_mb")),
            fmt_rss(r.get("final_rss_mb")),
            r.get("duration", "?"),
        ]
        print(row_fmt.format(*row))

    total = len(data["results"])
    passed = sum(1 for r in data["results"] if r["passed"])
    print()
    print(f"  {passed}/{total} scenarios passed")


if __name__ == "__main__":
    main()
