"""
batch_test.py
-------------
Automated accuracy testing for the EDGAR Financial Viewer Flask app.

Two test modes:
  1. Ground-truth validation  — fixed set of manually verified values,
                                covering key edge cases (banks, energy,
                                non-Dec FY, spinoffs, REITs).
  2. S&P 500 smoke test       — S&P 500 tickers pulled from Wikipedia
                                automatically, tested in parallel batches
                                of 100 (batch 1-100, 101-200, etc.) using
                                ThreadPoolExecutor for speed.

Usage:
    # Flask must be running first:  python app.py
    python batch_test.py                        # both modes, all S&P 500
    python batch_test.py --ground-truth         # ground-truth only
    python batch_test.py --sp500                # full S&P 500 parallel
    python batch_test.py --sp500 --limit 200    # first 200 tickers
    python batch_test.py --sp500 --workers 20   # 20 concurrent workers
    python batch_test.py --sp500 --batch 50     # batch size 50

Performance:
    Sequential (old):  ~3.3s/ticker  →  503 tickers ≈ 28 minutes
    Parallel   (new):  ~20-40s total per batch of 100 at 10 workers
"""

import argparse
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests

# ── Config ────────────────────────────────────────────────────────────────────
FLASK_URL      = "http://localhost:5000/api/financials"
TOLERANCE      = 0.05   # 5% rounding tolerance
DEFAULT_WORKERS = 10    # concurrent threads per batch — safe for Flask + EDGAR
DEFAULT_BATCH   = 100   # tickers per batch
REQUEST_GAP     = 0.1   # small delay per thread to avoid bursting EDGAR

# Thread-safe print lock — prevents interleaved output from concurrent threads
_print_lock = threading.Lock()

def tprint(*args, **kwargs):
    """Thread-safe print."""
    with _print_lock:
        print(*args, **kwargs)


# ── Ground-truth dictionary ───────────────────────────────────────────────────
# Values sourced from SEC 10-K filings and cross-checked against EDGAR XBRL.
#
# Note on JPM / GOOGL: NetIncomeLoss in XBRL is the consolidated figure
# which may include noncontrolling interests. The figures below reflect
# what the XBRL API actually returns, not always the headline "attributable
# to common shareholders" number from the front page of the 10-K.

GROUND_TRUTH = {
    # ── Large cap standard ────────────────────────────────────────────────────
    "AAPL": {
        "Revenue":           {"FY2024": 391035, "FY2023": 383285},
        "Net Income (Loss)": {"FY2024":  93736, "FY2023":  96995},
        "Total Assets":      {"FY2024": 364980, "FY2023": 352583},
        "Gross Profit":      {"FY2024": 180683, "FY2023": 169148},
    },
    "MSFT": {
        "Revenue":           {"FY2024": 245122, "FY2023": 211915},
        "Net Income (Loss)": {"FY2024":  88136, "FY2023":  72361},
        "Total Assets":      {"FY2024": 512163, "FY2023": 411976},
    },
    "GOOGL": {
        "Revenue":           {"FY2024": 350018, "FY2023": 307394},
        "Net Income (Loss)": {"FY2024": 100118, "FY2023":  73795},
    },
    "NVDA": {
        "Revenue":           {"FY2025": 130497, "FY2024":  60922},
        "Net Income (Loss)": {"FY2025":  72880, "FY2024":  29760},
        "Gross Profit":      {"FY2025":  97855, "FY2024":  44301},
    },
    "AMZN": {
        "Revenue":           {"FY2024": 637959, "FY2023": 574785},
        "Net Income (Loss)": {"FY2024":  59248, "FY2023":  30425},
    },
    # ── Non-December fiscal year ends ─────────────────────────────────────────
    "WMT": {
        "Revenue":           {"FY2024": 648125, "FY2023": 611289},
        "Net Income (Loss)": {"FY2024":  16270},
    },
    "COST": {
        "Revenue":           {"FY2024": 254438},
        "Net Income (Loss)": {"FY2024":   7367},
    },
    # ── Financials / banks ────────────────────────────────────────────────────
    "JPM": {
        "Net Income (Loss)": {"FY2024": 49600},
        "Total Assets":      {"FY2024": 4000000},
    },
    # ── Energy ────────────────────────────────────────────────────────────────
    "XOM": {
        "Revenue":           {"FY2024": 426516},
        "Net Income (Loss)": {"FY2024":  33680},
    },
    # ── Recent spinoff ────────────────────────────────────────────────────────
    "SNDK": {
        "Revenue":           {"FY2025": 7403, "FY2024": 6706, "FY2023": 5836},
        "Net Income (Loss)": {"FY2025": -1641},
    },
    # ── Tesla ─────────────────────────────────────────────────────────────────
    "TSLA": {
        "Revenue":           {"FY2024":  97690, "FY2023":  96773},
        "Net Income (Loss)": {"FY2024":   7258, "FY2023":  14974},
        "Gross Profit":      {"FY2024":  17371, "FY2023":  17660},
    },
}


# ── Wikipedia S&P 500 fetch ───────────────────────────────────────────────────

def fetch_sp500_tickers(limit: int | None = None) -> list[tuple[str, str]]:
    """
    Fetch the full S&P 500 list from Wikipedia using a browser User-Agent
    to avoid the 403 block that hits plain urllib / pandas.read_html().
    Returns list of (ticker, company_name). Optionally capped at `limit`.
    """
    print("  Fetching S&P 500 list from Wikipedia...")
    try:
        import io
        import pandas as pd

        wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers  = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
        resp = requests.get(wiki_url, headers=headers, timeout=20)
        resp.raise_for_status()

        tables  = pd.read_html(io.StringIO(resp.text), attrs={"id": "constituents"})
        df      = tables[0]
        tickers = [
            (str(row["Symbol"]).replace(".", "-"), str(row["Security"]))
            for _, row in df.iterrows()
        ]
        if limit:
            tickers = tickers[:limit]
        print(f"  Loaded {len(tickers)} tickers.\n")
        return tickers

    except requests.exceptions.HTTPError as e:
        print(f"  ERROR: Wikipedia returned {e.response.status_code}.")
        sys.exit(1)
    except Exception as e:
        print(f"  ERROR fetching S&P 500 list: {e}")
        print("  Ensure installed:  pip install pandas lxml html5lib")
        sys.exit(1)


# ── API helpers ───────────────────────────────────────────────────────────────

def call_api(ticker: str, filing: str = "10-K", periods: int = 3) -> dict:
    """Call the Flask API. Returns response dict or {'_error': ...}."""
    try:
        resp = requests.get(
            FLASK_URL,
            params={"ticker": ticker, "filing": filing, "periods": periods},
            timeout=60,
        )
        if resp.status_code == 404:
            return {"_error": resp.json().get("error", "Not found")}
        if not resp.ok:
            return {"_error": f"HTTP {resp.status_code}: {resp.json().get('error', 'Unknown')}"}
        return resp.json()
    except requests.exceptions.ConnectionError:
        tprint("\n  FATAL: Cannot connect to Flask at localhost:5000. Run: python app.py\n")
        sys.exit(1)
    except Exception as e:
        return {"_error": str(e)}


def get_value(data: dict, metric_label: str, period_label: str) -> float | None:
    """Look up a metric by human-readable label + period label (e.g. 'FY2024')."""
    period_map = dict(zip(data.get("labels", []), data.get("periods", [])))
    period_date = period_map.get(period_label)
    if not period_date:
        return None
    for stmt in data.get("statements", {}).values():
        for row in stmt:
            if row["label"] == metric_label:
                return row["values"].get(period_date)
    return None


# ── Ground-truth validation (sequential — small fixed set) ───────────────────

def run_ground_truth() -> dict:
    print("=" * 62)
    print("  GROUND-TRUTH VALIDATION")
    print("=" * 62)

    results = {"pass": [], "fail": [], "missing": [], "error": []}

    for ticker, metrics in GROUND_TRUTH.items():
        print(f"\nTesting {ticker}...")
        data = call_api(ticker, periods=5)

        if "_error" in data:
            print(f"  ✗ API error: {data['_error']}")
            results["error"].append(f"{ticker}: {data['_error']}")
            continue

        for metric, expected_by_label in metrics.items():
            for period_label, expected in expected_by_label.items():
                actual = get_value(data, metric, period_label)

                if actual is None:
                    print(f"  ? MISSING  {metric} / {period_label}")
                    results["missing"].append(f"{ticker}/{metric}/{period_label}")
                    continue

                pct_diff = abs(actual - expected) / max(abs(expected), 1)
                ok       = pct_diff <= TOLERANCE

                if ok:
                    print(f"  ✓  {metric} / {period_label}: {actual:>12,.0f}")
                    results["pass"].append(f"{ticker}/{metric}/{period_label}")
                else:
                    detail = (f"expected {expected:,.0f}, "
                              f"got {actual:,.0f}, Δ{pct_diff:.1%}")
                    print(f"  ✗  {metric} / {period_label}: "
                          f"{actual:>12,.0f}  ({detail})")
                    results["fail"].append(
                        f"{ticker}/{metric}/{period_label}  ({detail})"
                    )

        time.sleep(0.3)

    return results


# ── Single-ticker smoke check (called from thread pool) ───────────────────────

def _smoke_one(args: tuple) -> dict:
    """
    Test a single ticker. Returns a result dict consumed by run_sp500_smoke.
    Designed to be called concurrently from ThreadPoolExecutor.
    """
    idx, total, ticker, company = args
    time.sleep(REQUEST_GAP)   # small stagger so threads don't all fire at once
    data = call_api(ticker, periods=3)

    prefix = f"[{idx:>3}/{total}]  {ticker:<8} {company[:28]:<28}"
    issues   = []
    warnings = []

    if "_error" in data:
        tprint(f"{prefix}  ✗ ERROR   {data['_error']}")
        return {"status": "error", "ticker": ticker, "detail": data["_error"]}

    periods = data.get("periods", [])
    labels  = data.get("labels",  [])

    if not periods:
        tprint(f"{prefix}  ✗ FAIL    no periods returned")
        return {"status": "fail", "ticker": ticker, "detail": "no periods returned"}

    latest_label = labels[0] if labels else "?"

    # Revenue
    revenue = get_value(data, "Revenue", latest_label)
    if revenue is None:
        warnings.append("Revenue not found — check XBRL tag in EDGAR")
    elif revenue <= 0:
        issues.append(f"Revenue={revenue:,.0f} (non-positive)")

    # Net Income
    ni = get_value(data, "Net Income (Loss)", latest_label)
    if ni is None:
        issues.append("Net Income (Loss) not found")

    # Total Assets
    assets = get_value(data, "Total Assets", latest_label)
    if assets is None:
        issues.append("Total Assets not found")
    elif assets <= 0:
        issues.append(f"Total Assets={assets:,.0f} (non-positive)")

    # Period count
    n_periods = len(periods)
    synthetic = sum(1 for f in data.get("filings", []) if f.get("synthetic"))
    period_note = f"{n_periods}p" + (f"/{synthetic}syn" if synthetic else "")

    rev_s    = f"Rev={revenue:>10,.0f}" if revenue  is not None else "Rev=         N/A"
    ni_s     = f"NI={ni:>10,.0f}"       if ni       is not None else "NI=          N/A"
    assets_s = f"Assets={assets:>10,.0f}" if assets is not None else "Assets=      N/A"

    if issues:
        tprint(f"{prefix}  ✗ FAIL    {latest_label}  {rev_s}  {ni_s}  {assets_s}  ({period_note})")
        for iss in issues:
            tprint(f"           └─ {iss}")
        return {"status": "fail", "ticker": ticker,
                "detail": "; ".join(issues), "warnings": warnings}

    if warnings:
        tprint(f"{prefix}  ⚠ WARN    {latest_label}  {rev_s}  {ni_s}  {assets_s}  ({period_note})")
        for w in warnings:
            tprint(f"           ⚠  {w}")
        return {"status": "warning", "ticker": ticker, "detail": warnings[0]}

    tprint(f"{prefix}  ✓ PASS    {latest_label}  {rev_s}  {ni_s}  {assets_s}  ({period_note})")
    return {"status": "pass", "ticker": ticker}


# ── Parallel S&P 500 smoke test ───────────────────────────────────────────────

def run_sp500_smoke(
    limit:      int | None = None,
    batch_size: int        = DEFAULT_BATCH,
    workers:    int        = DEFAULT_WORKERS,
) -> dict:
    """
    Smoke-test S&P 500 tickers in parallel batches.

    Architecture
    ------------
    All tickers are split into batches of `batch_size`. Each batch is
    submitted to a ThreadPoolExecutor with `workers` concurrent threads.
    Results stream to the terminal as they complete (not in order — that's
    fine, each line is self-labelled with its index).

    Batches run sequentially (batch 1 completes before batch 2 starts) to
    avoid overwhelming EDGAR with too many concurrent connections. Within
    each batch, all `batch_size` tickers run concurrently.

    Parameters
    ----------
    limit       Total tickers to test. None = full S&P 500 (~503).
    batch_size  Tickers per batch (default 100).
    workers     Concurrent threads within each batch (default 10).
    """
    display_limit = limit if limit else "all"
    print("=" * 62)
    print(f"  S&P 500 PARALLEL SMOKE TEST")
    print(f"  Tickers: {display_limit}  |  Batch size: {batch_size}"
          f"  |  Workers: {workers}")
    print("=" * 62)

    all_tickers = fetch_sp500_tickers(limit)
    total       = len(all_tickers)

    # Split into batches of batch_size
    batches = [
        all_tickers[i : i + batch_size]
        for i in range(0, total, batch_size)
    ]

    results    = {"pass": [], "fail": [], "warning": [], "error": []}
    start_time = time.time()

    for batch_num, batch in enumerate(batches, 1):
        batch_start = (batch_num - 1) * batch_size + 1
        batch_end   = batch_start + len(batch) - 1

        print(f"\n{'─' * 62}")
        print(f"  Batch {batch_num}/{len(batches)}  "
              f"— tickers {batch_start}–{batch_end}  "
              f"({len(batch)} tickers, {workers} workers)")
        print(f"{'─' * 62}")

        batch_start_time = time.time()

        # Build args tuples: (global_index, total, ticker, company)
        args_list = [
            (batch_start + j, total, ticker, company)
            for j, (ticker, company) in enumerate(batch)
        ]

        # Submit all tickers in this batch concurrently
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_smoke_one, args): args for args in args_list}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    status = result["status"]
                    ticker = result["ticker"]
                    detail = result.get("detail", "")

                    if status == "pass":
                        results["pass"].append(ticker)
                    elif status == "warning":
                        results["warning"].append(f"{ticker}: {detail}")
                    elif status == "fail":
                        results["fail"].append(f"{ticker}: {detail}")
                    else:
                        results["error"].append(f"{ticker}: {detail}")

                except Exception as exc:
                    ticker_args = futures[future]
                    tprint(f"  Thread error for {ticker_args[2]}: {exc}")
                    results["error"].append(f"{ticker_args[2]}: thread exception")

        batch_elapsed = time.time() - batch_start_time
        batch_pass    = sum(
            1 for t in results["pass"]
            if any(t == a[2] for a in args_list)
        )
        print(f"\n  Batch {batch_num} complete — "
              f"{len(batch)} tickers in {batch_elapsed:.1f}s  "
              f"({batch_elapsed/len(batch):.2f}s avg)")

    elapsed = time.time() - start_time
    results["elapsed"] = elapsed
    return results


# ── Summary printer ───────────────────────────────────────────────────────────

def print_summary(label: str, results: dict) -> None:
    total   = (len(results["pass"]) + len(results["fail"])
               + len(results.get("error", [])))
    warn    = len(results.get("warning", []))
    elapsed = results.get("elapsed")

    print(f"\n{'─' * 62}")
    print(f"  {label} SUMMARY")
    print(f"{'─' * 62}")
    print(f"  Passed:   {len(results['pass'])}")
    print(f"  Failed:   {len(results['fail'])}")
    if warn:
        print(f"  Warnings: {warn}")
    if results.get("error"):
        print(f"  Errors:   {len(results['error'])}")
    print(f"  Total:    {total}")
    if elapsed:
        print(f"  Duration: {elapsed:.1f}s  "
              f"({elapsed / max(total, 1):.2f}s avg/ticker)")

    if results["fail"]:
        print(f"\n  Failures:")
        for f in results["fail"]:
            print(f"    ✗  {f}")

    if results.get("error"):
        print(f"\n  Errors:")
        for e in results["error"]:
            print(f"    ✗  {e}")

    if results.get("warning"):
        print(f"\n  Warnings:")
        for w in results["warning"][:15]:
            print(f"    ⚠  {w}")
        if len(results["warning"]) > 15:
            print(f"    ... and {len(results['warning']) - 15} more")

    pct = 100 * len(results["pass"]) / max(total, 1)
    print(f"\n  Pass rate: {pct:.1f}%")
    print(f"{'─' * 62}\n")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Parallel batch accuracy tester for EDGAR Financial Viewer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python batch_test.py                        # ground truth + full S&P 500
  python batch_test.py --ground-truth         # ground-truth only
  python batch_test.py --sp500               # full S&P 500 (all ~503)
  python batch_test.py --sp500 --limit 200   # first 200 tickers
  python batch_test.py --sp500 --workers 20  # 20 concurrent workers
  python batch_test.py --sp500 --batch 50    # 50 tickers per batch
        """
    )
    parser.add_argument("--ground-truth", action="store_true",
                        help="Run ground-truth validation only")
    parser.add_argument("--sp500",        action="store_true",
                        help="Run S&P 500 parallel smoke test only")
    parser.add_argument("--limit",   type=int, default=None,
                        help="Max tickers to test (default: all ~503)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Concurrent threads per batch (default: {DEFAULT_WORKERS})")
    parser.add_argument("--batch",   type=int, default=DEFAULT_BATCH,
                        help=f"Tickers per batch (default: {DEFAULT_BATCH})")
    args = parser.parse_args()

    run_gt = args.ground_truth or (not args.ground_truth and not args.sp500)
    run_sp = args.sp500        or (not args.ground_truth and not args.sp500)

    print(f"\n  EDGAR Financial Viewer — Parallel Batch Test")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Flask: {FLASK_URL}  |  Tolerance: ±{TOLERANCE:.0%}")
    if run_sp:
        print(f"  S&P 500: limit={args.limit or 'all'}  "
              f"batch={args.batch}  workers={args.workers}")
    print()

    all_passed = True

    if run_gt:
        gt_results = run_ground_truth()
        print_summary("GROUND-TRUTH", gt_results)
        if gt_results["fail"] or gt_results.get("error"):
            all_passed = False

    if run_sp:
        sp_results = run_sp500_smoke(
            limit=args.limit,
            batch_size=args.batch,
            workers=args.workers,
        )
        print_summary("S&P 500 PARALLEL SMOKE TEST", sp_results)
        if sp_results["fail"] or sp_results.get("error"):
            all_passed = False

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()