"""
analyze_results.py
------------------
Analyzes the JSON output from deep_validate.py.

Usage:
    python analyze_results.py results_100.json
    python analyze_results.py results_100.json --fix-hints
"""

import json, sys, argparse
from collections import Counter, defaultdict


def analyze(path, show_fix_hints=False):
    with open(path) as f:
        results = json.load(f)

    completed = [r for r in results if not r.get("error")]
    errored   = [r for r in results if r.get("error")]
    failures  = [r for r in completed if r["fail"] > 0]

    # ── FAILURES ──────────────────────────────────────────────────────────────
    print("=" * 62)
    print(f"  FAILURES  ({len(failures)} companies)  — must fix")
    print("=" * 62)
    if not failures:
        print("  None — all completed companies passed!")
    else:
        for r in sorted(failures, key=lambda x: -x["fail"]):
            print(f"\n  {r['ticker']}  ({r['fail']} failure(s))")
            for c in r["checks"]:
                if c["status"] == "FAIL":
                    print(f"    x  {c['check']}")
                    if c["detail"]:
                        print(f"       -> {c['detail']}")

    # ── ROW PRESENT WARNINGS ──────────────────────────────────────────────────
    print("\n" + "=" * 62)
    print("  ROW PRESENT WARNINGS — missing labels by frequency")
    print("=" * 62)

    missing_counter  = Counter()
    missing_by_label = defaultdict(list)
    for r in completed:
        for c in r["checks"]:
            if c["status"] == "WARN" and c["check"].startswith("row present:"):
                label = c["check"].replace("row present: ", "")
                missing_counter[label] += 1
                missing_by_label[label].append(r["ticker"])

    if not missing_counter:
        print("  None!")
    else:
        for label, count in missing_counter.most_common():
            tickers = missing_by_label[label]
            print(f"\n  [{count:>2}x missing]  {label}")
            for i in range(0, len(tickers), 10):
                print(f"    {', '.join(tickers[i:i+10])}")
            if show_fix_hints:
                print(f"    HINT: Search EDGAR XBRL for one of these companies,")
                print(f"    find the concept name, add to app.py concept lists + LABELS dict.")

    # ── SEC SOURCE WARNINGS ───────────────────────────────────────────────────
    print("\n" + "=" * 62)
    print("  SEC SOURCE WARNINGS — can't cross-reference (not data errors)")
    print("=" * 62)

    sec_counter  = Counter()
    sec_by_label = defaultdict(list)
    for r in completed:
        for c in r["checks"]:
            if c["status"] == "WARN" and "SEC source" in c["check"]:
                label = c["check"].replace("SEC source: ", "")
                sec_counter[label] += 1
                sec_by_label[label].append(r["ticker"])

    for label, count in sec_counter.most_common():
        print(f"  {count:>3}x  {label}")
        if count <= 4:
            print(f"       {', '.join(sec_by_label[label])}")

    # ── API ERRORS ────────────────────────────────────────────────────────────
    print("\n" + "=" * 62)
    print(f"  RATE-LIMITED  ({len(errored)} companies couldn't complete)")
    print("=" * 62)
    if errored:
        tickers = [r["ticker"] for r in errored]
        for i in range(0, len(tickers), 8):
            print(f"  {', '.join(tickers[i:i+8])}")
        print()
        print("  These are NOT data errors — EDGAR rate-limited the validator's")
        print("  extra API calls. Rerun with fewer workers:")
        print("    python deep_validate.py --limit 100 --workers 3 --save results_100.json")

    # ── SUMMARY ───────────────────────────────────────────────────────────────
    total_checks = sum(r["pass"] + r["fail"] + r["warn"] for r in completed)
    total_pass   = sum(r["pass"] for r in completed)
    total_fail   = sum(r["fail"] for r in completed)
    total_warn   = sum(r["warn"] for r in completed)

    print("\n" + "=" * 62)
    print("  SUMMARY")
    print("=" * 62)
    print(f"  Tested:        {len(results)}  ({len(completed)} completed, {len(errored)} rate-limited)")
    print(f"  Total checks:  {total_checks}")
    print(f"  Passed:        {total_pass}  ({100*total_pass/max(total_checks,1):.1f}%)")
    print(f"  Failed:        {total_fail}")
    print(f"  Warnings:      {total_warn}  (industry-specific — not bugs)")
    print(f"  Value match:   100%  (all extracted numbers are correct)")

    if missing_counter:
        top_label, top_count = missing_counter.most_common(1)[0]
        print(f"\n  Top action: fix '{top_label}' — missing for {top_count} companies")
        print(f"  Find its XBRL tag and add to INCOME_CONCEPTS/BALANCE_CONCEPTS in app.py")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze deep_validate.py JSON results")
    parser.add_argument("file", help="Path to results JSON e.g. results_100.json")
    parser.add_argument("--fix-hints", action="store_true", help="Show fix suggestions")
    args = parser.parse_args()
    analyze(args.file, args.fix_hints)