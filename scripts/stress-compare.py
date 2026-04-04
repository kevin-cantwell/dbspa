#!/usr/bin/env python3
"""
Compare two stress result JSON files, showing delta in throughput and RSS.

Usage:
    python3 scripts/stress-compare.py <baseline.json> <new.json>
    make stress-compare BASELINE=stress/results/old.json NEW=stress/results/new.json

Positive throughput delta = improvement. Positive RSS delta = regression.
"""

import json
import sys


def fmt_throughput(n):
    if n is None or n == 0:
        return "—"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))


def fmt_pct(new_val, old_val, invert=False):
    """Format a percentage change. invert=True means higher is worse (e.g., RSS)."""
    if old_val is None or old_val == 0:
        return "N/A"
    pct = (new_val - old_val) / old_val * 100
    sign = "+" if pct >= 0 else ""
    label = f"{sign}{pct:.1f}%"
    if invert:
        # For RSS: increase is bad, shown with warning marker
        marker = " !" if pct > 10 else ""
    else:
        # For throughput: decrease is bad
        marker = " !" if pct < -10 else ""
    return label + marker


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <baseline.json> <new.json>", file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1]) as f:
        baseline = json.load(f)
    with open(sys.argv[2]) as f:
        new = json.load(f)

    print(f"Baseline: {sys.argv[1]}")
    print(f"          {baseline.get('dbspa_version', '?')}  go={baseline.get('go_version', '?')}")
    print(f"New:      {sys.argv[2]}")
    print(f"          {new.get('dbspa_version', '?')}  go={new.get('go_version', '?')}")
    print()

    b_map = {r["name"]: r for r in baseline["results"]}
    n_map = {r["name"]: r for r in new["results"]}

    # Preserve order: baseline first, then any new-only scenarios
    all_names = list(dict.fromkeys(
        [r["name"] for r in baseline["results"]] +
        [r["name"] for r in new["results"]]
    ))

    col_w = [38, 12, 12, 9, 10, 10, 9]
    headers = ["scenario", "rec/sec (B)", "rec/sec (N)", "Δ thr", "RSS (B)", "RSS (N)", "Δ RSS"]
    sep = "  "
    row_fmt = sep.join(f"{{:<{w}}}" for w in col_w)

    print(row_fmt.format(*headers))
    print(sep.join("-" * w for w in col_w))

    regressions = []

    for name in all_names:
        b = b_map.get(name)
        n = n_map.get(name)

        if b is None:
            thr_b = "—"
            rss_b = "—"
            thr_delta = "NEW"
            rss_delta = ""
        else:
            thr_b = fmt_throughput(b.get("throughput_avg"))
            rss_b = f"{b['peak_rss_mb']:.1f} MB" if b.get("peak_rss_mb") else "—"

        if n is None:
            thr_n = "—"
            rss_n = "—"
            thr_delta = "GONE"
            rss_delta = ""
        else:
            thr_n = fmt_throughput(n.get("throughput_avg"))
            rss_n = f"{n['peak_rss_mb']:.1f} MB" if n.get("peak_rss_mb") else "—"

        if b is not None and n is not None:
            thr_delta = fmt_pct(n.get("throughput_avg", 0), b.get("throughput_avg", 0))
            rss_delta = fmt_pct(n.get("peak_rss_mb", 0), b.get("peak_rss_mb", 0), invert=True)
            if "!" in thr_delta or "!" in rss_delta:
                regressions.append(name)

        print(row_fmt.format(name, thr_b, thr_n, thr_delta, rss_b, rss_n, rss_delta))

    print()
    if regressions:
        print(f"  Possible regressions (>10% change): {', '.join(regressions)}")
    else:
        print("  No regressions detected (all within 10%).")


if __name__ == "__main__":
    main()
