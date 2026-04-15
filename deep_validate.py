"""
deep_validate.py
----------------
Cross-references your Flask app's extracted financial data against the SEC's
own companyconcept API — a completely independent EDGAR endpoint.

For each company it checks:
  1. Completeness — are all expected line items present?
  2. Value accuracy — do extracted values match the raw SEC source within 5%?
  3. Period correctness — are period labels (FY2024 etc.) aligned correctly?
  4. Scaling — are values correctly converted to USD millions?
  5. No duplicates — are any period columns duplicated?
  6. No phantom periods — are synthetic periods leaking into real companies?

Usage:
    # Flask must be running:  python app.py
    python deep_validate.py                      # 20 random S&P 500 stocks
    python deep_validate.py --limit 100          # first 100 S&P 500 stocks
    python deep_validate.py --ticker GOOG        # single company deep-dive
    python deep_validate.py --ticker AAPL,MSFT,GOOG  # specific tickers
    python deep_validate.py --workers 8          # parallel workers
    python deep_validate.py --save report.json   # save full results to JSON
"""

import argparse
import io
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date

import requests

# ── Config ────────────────────────────────────────────────────────────────────
FLASK_URL    = "http://localhost:5000/api/financials"
EDGAR_BASE   = "https://data.sec.gov"
SEC_HEADERS  = {
    "User-Agent": "Deep Validator (educational) validator@example.com",
    "Accept-Encoding": "identity",
}
TOLERANCE    = 0.05   # 5% value tolerance
WORKERS      = 8      # parallel threads
REQUEST_GAP  = 0.3    # seconds between SEC API calls per thread

_print_lock = threading.Lock()

def tprint(*args, **kwargs):
    with _print_lock:
        print(*args, **kwargs)


# ── What we expect per statement ─────────────────────────────────────────────
# Maps label -> SEC concepts to cross-reference.
# "optional" labels are only flagged as WARN (not FAIL) when missing,
# because some industries genuinely don't report them (REITs have no COGS,
# insurance companies have no current assets split, etc.)

EXPECTED_INCOME = {
    "Revenue":                  {
        "concepts": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
                     "RevenueFromContractWithCustomerIncludingAssessedTax", "SalesRevenueNet"],
        "optional": False,
    },
    "Gross Profit":             {
        "concepts": ["GrossProfit"],
        "optional": True,   # REITs, insurance, banks don't have COGS structure
        "derived_ok": True, # acceptable if computed from Revenue - COGS
    },
    "Operating Income (Loss)":  {
        "concepts": ["OperatingIncomeLoss"],
        "optional": True,   # some industries use different operating concepts
    },
    "Net Income (Loss)":        {
        "concepts": ["NetIncomeLoss", "NetIncomeLossAttributableToParent",
                     "IncomeLossFromContinuingOperations",
                     "NetIncomeLossAvailableToCommonStockholdersBasic"],
        "optional": False,
    },
    "Income Tax Expense (Benefit)": {
        "concepts": ["IncomeTaxExpenseBenefit"],
        "optional": True,
    },
}

EXPECTED_BALANCE = {
    "Total Assets":              {
        "concepts": ["Assets"],
        "optional": False,
    },
    "Total Current Assets":      {
        "concepts": ["AssetsCurrent"],
        "optional": True,   # REITs and some financials don't split current/non-current
    },
    "Total Current Liabilities": {
        "concepts": ["LiabilitiesCurrent"],
        "optional": True,   # same as above
    },
    "Total Liabilities":         {
        "concepts": ["Liabilities"],
        "optional": True,   # acceptable if derived from L&E - Equity
        "derived_ok": True,
    },
    "Total Stockholders' Equity": {
        "concepts": ["StockholdersEquity",
                     "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"],
        "optional": False,
    },
    "Cash & Equivalents":        {
        "concepts": ["CashAndCashEquivalentsAtCarryingValue",
                     "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"],
        "optional": True,   # financial companies carry cash differently
    },
}

EXPECTED_CASHFLOW = {
    "Net Cash – Operating": {
        "concepts": ["NetCashProvidedByUsedInOperatingActivities"],
        "optional": False,
    },
    "Net Cash – Investing": {
        "concepts": ["NetCashProvidedByUsedInInvestingActivities"],
        "optional": False,
    },
    "Net Cash – Financing": {
        "concepts": ["NetCashProvidedByUsedInFinancingActivities"],
        "optional": False,
    },
}


# ── Ticker/CIK resolution ─────────────────────────────────────────────────────

_ticker_cache: dict = {}
_ticker_cache_lock = threading.Lock()

def resolve_cik(ticker: str) -> str | None:
    """Resolve ticker → zero-padded CIK using SEC company_tickers.json."""
    with _ticker_cache_lock:
        if _ticker_cache:
            entry = _ticker_cache.get(ticker.upper())
            return str(entry["cik_str"]).zfill(10) if entry else None

    try:
        resp = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=SEC_HEADERS, timeout=20
        )
        resp.raise_for_status()
        raw = resp.json()
        with _ticker_cache_lock:
            for v in raw.values():
                t = (v.get("ticker") or "").upper()
                if t:
                    _ticker_cache[t] = v
        entry = _ticker_cache.get(ticker.upper())
        return str(entry["cik_str"]).zfill(10) if entry else None
    except Exception as e:
        tprint(f"  WARNING: Could not fetch ticker list: {e}")
        return None


# ── SEC companyconcept API ────────────────────────────────────────────────────

def fetch_sec_concept(cik: str, concept: str) -> dict:
    """
    Fetch a single XBRL concept's full history from SEC companyconcept API.
    Returns a dict mapping period_end_date -> raw_value_in_usd.
    """
    url = f"{EDGAR_BASE}/api/xbrl/companyconcept/CIK{cik}/us-gaap/{concept}.json"
    try:
        resp = requests.get(url, headers=SEC_HEADERS, timeout=20)
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()
        data = resp.json()
        units = data.get("units", {})
        records = units.get("USD") or units.get("shares") or units.get("pure") or []

        # Build date -> value map (most recently filed wins per period)
        result = {}
        for r in records:
            end   = r.get("end", "")
            start = r.get("start", "")
            val   = r.get("val")
            filed = r.get("filed", "")
            if val is None or not end:
                continue
            # Annual filter: ~330-420 day duration
            if start:
                try:
                    days = (date.fromisoformat(end) - date.fromisoformat(start)).days
                    if not (330 <= days <= 420):
                        continue
                except ValueError:
                    pass
            if end not in result or filed > result[end][1]:
                result[end] = (val, filed)

        return {k: v[0] for k, v in result.items()}

    except Exception:
        return {}


def get_sec_value(cik: str, concepts: list[str], period_date: str) -> float | None:
    """
    Try each concept in order until one returns a value for period_date.
    Returns the raw USD value (not scaled), or None if nothing found.
    """
    for concept in concepts:
        data = fetch_sec_concept(cik, concept)
        if period_date in data:
            return data[period_date]
        # Fuzzy match within 10 days
        if data:
            try:
                target = date.fromisoformat(period_date).toordinal()
                for end_str, val in data.items():
                    try:
                        if abs(date.fromisoformat(end_str).toordinal() - target) <= 10:
                            return val
                    except ValueError:
                        pass
            except ValueError:
                pass
        time.sleep(0.1)  # be polite to SEC API
    return None


# ── Flask API helper ──────────────────────────────────────────────────────────

def call_flask(ticker: str, periods: int = 3) -> dict:
    try:
        resp = requests.get(
            FLASK_URL,
            params={"ticker": ticker, "filing": "10-K", "periods": periods},
            timeout=60
        )
        if not resp.ok:
            return {"_error": resp.json().get("error", f"HTTP {resp.status_code}")}
        return resp.json()
    except requests.exceptions.ConnectionError:
        tprint("\n  FATAL: Flask not running. Start with: python app.py\n")
        sys.exit(1)
    except Exception as e:
        return {"_error": str(e)}


def get_flask_value(data: dict, label: str, period_label: str) -> float | None:
    """Look up a value from Flask response by label and period label (e.g. FY2024)."""
    period_map = dict(zip(data.get("labels", []), data.get("periods", [])))
    period_date = period_map.get(period_label)
    if not period_date:
        return None
    for stmt in data.get("statements", {}).values():
        for row in stmt:
            if row["label"] == label:
                return row["values"].get(period_date)
    return None


# ── Per-company deep validation ───────────────────────────────────────────────

def validate_company(ticker: str, periods: int = 3) -> dict:
    """
    Run a full deep validation for one company.
    Returns a result dict with detailed pass/fail breakdown.
    """
    ticker = ticker.upper()
    result = {
        "ticker":    ticker,
        "checks":    [],   # list of {label, check, status, detail}
        "pass":      0,
        "fail":      0,
        "warn":      0,
        "error":     None,
    }

    def add(label, check, status, detail=""):
        result["checks"].append({"label": label, "check": check,
                                  "status": status, "detail": detail})
        if status == "PASS": result["pass"] += 1
        elif status == "FAIL": result["fail"] += 1
        else: result["warn"] += 1

    # ── Step 1: Flask API call ────────────────────────────────────────────────
    flask_data = call_flask(ticker, periods)
    if "_error" in flask_data:
        result["error"] = flask_data["_error"]
        return result

    flask_labels  = flask_data.get("labels",  [])
    flask_periods = flask_data.get("periods", [])
    statements    = flask_data.get("statements", {})

    # ── Check A: API resolves and returns data ────────────────────────────────
    if not flask_periods:
        result["error"] = "No periods returned from Flask API"
        return result
    add("API", "resolves and returns data", "PASS")

    # ── Check B: No duplicate period labels ──────────────────────────────────
    if len(flask_labels) != len(set(flask_labels)):
        from collections import Counter
        dupes = [l for l, c in Counter(flask_labels).items() if c > 1]
        add("Periods", "no duplicate columns", "FAIL",
            f"Duplicate labels: {dupes}")
    else:
        add("Periods", "no duplicate columns", "PASS",
            f"Periods: {', '.join(flask_labels)}")

    # ── Check C: No synthetic periods for established companies ──────────────
    filings = flask_data.get("filings", [])
    synthetic_count = sum(1 for f in filings if f.get("synthetic"))
    real_count = len(filings) - synthetic_count
    if synthetic_count > 0 and real_count >= 2:
        add("Periods", "no spurious synthetic stubs",
            "WARN", f"{synthetic_count} synthetic period(s) on company with {real_count} real filings")
    else:
        add("Periods", "no spurious synthetic stubs", "PASS")

    # ── Step 2: Get CIK for SEC cross-check ──────────────────────────────────
    cik = flask_data.get("cik") or resolve_cik(ticker)
    if not cik:
        add("SEC Cross-check", "CIK resolution", "WARN", "Could not resolve CIK — skipping value checks")
        return result

    # ── Check D: Completeness — all expected rows present ────────────────────
    all_labels_present = set()
    for stmt in statements.values():
        for row in stmt:
            all_labels_present.add(row["label"])

    most_recent_label = flask_labels[0] if flask_labels else None

    expected_all = {**EXPECTED_INCOME, **EXPECTED_BALANCE, **EXPECTED_CASHFLOW}
    for label, cfg in expected_all.items():
        is_optional   = cfg.get("optional", False)
        derived_ok    = cfg.get("derived_ok", False)
        present       = label in all_labels_present

        if present:
            add("Completeness", f"row present: {label}", "PASS")
        elif is_optional:
            add("Completeness", f"row present: {label}", "WARN",
                f"'{label}' not found — optional for this industry (REIT/bank/insurance)")
        else:
            add("Completeness", f"row present: {label}", "FAIL",
                f"'{label}' not found in any statement")

    if not most_recent_label:
        return result

    # ── Check E: Value accuracy vs SEC companyconcept API ────────────────────
    most_recent_period = flask_periods[0] if flask_periods else None
    if not most_recent_period:
        return result

    for label, cfg in expected_all.items():
        concepts  = cfg["concepts"]
        flask_val = get_flask_value(flask_data, label, most_recent_label)

        if flask_val is None:
            # Already flagged as missing in completeness check
            continue

        # Get raw value from SEC for the same period
        time.sleep(REQUEST_GAP)
        sec_raw = get_sec_value(cik, concepts, most_recent_period)

        if sec_raw is None:
            add("Accuracy", f"SEC source: {label}", "WARN",
                f"Cannot find SEC raw value to compare (may use non-standard tag)")
            continue

        # Flask value is in millions; SEC is in raw dollars
        flask_in_raw = flask_val * 1_000_000
        pct_diff = abs(flask_in_raw - sec_raw) / max(abs(sec_raw), 1)

        if pct_diff <= TOLERANCE:
            add("Accuracy", f"value match: {label}", "PASS",
                f"Flask={flask_val:,.1f}M, SEC={sec_raw/1e6:,.1f}M, Δ={pct_diff:.1%}")
        else:
            add("Accuracy", f"value match: {label}", "FAIL",
                f"Flask={flask_val:,.1f}M, SEC={sec_raw/1e6:,.1f}M, Δ={pct_diff:.1%} — MISMATCH")

    # ── Check F: Scaling sanity — revenue in millions not billions ────────────
    rev_val = get_flask_value(flask_data, "Revenue", most_recent_label)
    if rev_val is not None:
        if rev_val > 10_000_000:
            add("Scaling", "revenue not in raw dollars", "FAIL",
                f"Revenue={rev_val:,.0f} — looks like raw dollars, not millions")
        elif rev_val < 0.1:
            add("Scaling", "revenue not in billions", "FAIL",
                f"Revenue={rev_val} — may be in billions, not millions")
        else:
            add("Scaling", "revenue in correct millions range", "PASS",
                f"Revenue={rev_val:,.1f}M ✓")

    # ── Check G: Period label format ──────────────────────────────────────────
    import re
    for lbl in flask_labels:
        if re.match(r'^FY\d{4}$', lbl) or re.match(r'^Q[1-4]–\d{4}$', lbl):
            add("Periods", f"label format: {lbl}", "PASS")
        else:
            add("Periods", f"label format: {lbl}", "FAIL",
                f"Unexpected period label format: '{lbl}'")

    return result


# ── S&P 500 ticker fetch ──────────────────────────────────────────────────────

def fetch_sp500(limit: int | None = None) -> list[str]:
    try:
        import pandas as pd
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        resp = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers=headers, timeout=20
        )
        resp.raise_for_status()
        tables = pd.read_html(io.StringIO(resp.text), attrs={"id": "constituents"})
        tickers = [str(row["Symbol"]).replace(".", "-") for _, row in tables[0].iterrows()]
        return tickers[:limit] if limit else tickers
    except Exception as e:
        print(f"  ERROR fetching S&P 500: {e}")
        sys.exit(1)


# ── Parallel runner ───────────────────────────────────────────────────────────

def run_validation(tickers: list[str], workers: int = WORKERS, periods: int = 3) -> list[dict]:
    total   = len(tickers)
    results = []
    lock    = threading.Lock()
    counter = [0]

    def task(args):
        i, ticker = args
        tprint(f"\n[{i:>3}/{total}] ── {ticker} ─────────────────────────────")
        r = validate_company(ticker, periods)

        with lock:
            counter[0] += 1
            results.append(r)

        if r["error"]:
            tprint(f"  ERROR: {r['error']}")
        else:
            for c in r["checks"]:
                icon = "✓" if c["status"] == "PASS" else ("⚠" if c["status"] == "WARN" else "✗")
                detail = f"  → {c['detail']}" if c["detail"] else ""
                tprint(f"  {icon} [{c['check']}]{detail}")

        p, f, w = r["pass"], r["fail"], r["warn"]
        tprint(f"  ── {ticker}: {p} pass / {f} fail / {w} warn")
        return r

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(task, (i+1, t)): t for i, t in enumerate(tickers)}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                tprint(f"  Thread exception: {e}")

    return results


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(results: list[dict], elapsed: float) -> None:
    total_pass = sum(r["pass"] for r in results)
    total_fail = sum(r["fail"] for r in results)
    total_warn = sum(r["warn"] for r in results)
    total_checks = total_pass + total_fail + total_warn
    errors = [r for r in results if r["error"]]
    failures = [r for r in results if r["fail"] > 0]

    print(f"\n{'═' * 62}")
    print(f"  DEEP VALIDATION SUMMARY")
    print(f"{'═' * 62}")
    print(f"  Companies tested:  {len(results)}")
    print(f"  Total checks:      {total_checks}")
    print(f"  Passed:            {total_pass}  ({100*total_pass/max(total_checks,1):.1f}%)")
    print(f"  Failed:            {total_fail}")
    print(f"  Warnings:          {total_warn}")
    print(f"  API errors:        {len(errors)}")
    print(f"  Duration:          {elapsed:.1f}s ({elapsed/max(len(results),1):.1f}s/company)")

    if errors:
        print(f"\n  API Errors:")
        for r in errors:
            print(f"    ✗  {r['ticker']}: {r['error']}")

    if failures:
        print(f"\n  Companies with failures:")
        for r in sorted(failures, key=lambda x: -x["fail"]):
            failing = [c for c in r["checks"] if c["status"] == "FAIL"]
            print(f"\n    {r['ticker']}  ({r['fail']} failure(s))")
            for c in failing:
                print(f"      ✗ {c['check']}: {c['detail']}")

    # Check-type breakdown
    check_types = {}
    for r in results:
        for c in r["checks"]:
            cat = c["check"].split(":")[0].strip()
            if cat not in check_types:
                check_types[cat] = {"pass": 0, "fail": 0, "warn": 0}
            check_types[cat][c["status"].lower()] += 1

    print(f"\n  By check category:")
    for cat, counts in sorted(check_types.items()):
        total_cat = counts["pass"] + counts["fail"] + counts["warn"]
        pct = 100 * counts["pass"] / max(total_cat, 1)
        bar_len = int(pct / 5)
        bar = "█" * bar_len + "░" * (20 - bar_len)
        print(f"    {cat:<20} [{bar}] {pct:>5.1f}%  "
              f"({counts['pass']}✓ {counts['fail']}✗ {counts['warn']}⚠)")

    pct_overall = 100 * total_pass / max(total_checks, 1)
    print(f"\n  Overall pass rate: {pct_overall:.1f}%")
    print(f"{'═' * 62}\n")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Deep validation: cross-references Flask output against SEC companyconcept API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python deep_validate.py                         # 20 random S&P 500 stocks
  python deep_validate.py --limit 100             # first 100 S&P 500 stocks
  python deep_validate.py --ticker GOOG           # single company deep-dive
  python deep_validate.py --ticker AAPL,MSFT,GOOG # specific tickers
  python deep_validate.py --workers 8             # 8 parallel workers
  python deep_validate.py --save report.json      # save results to JSON
        """
    )
    parser.add_argument("--ticker",  type=str, default=None,
                        help="Comma-separated tickers (e.g. AAPL,MSFT,GOOG)")
    parser.add_argument("--limit",   type=int, default=20,
                        help="Number of S&P 500 tickers to test (default: 20)")
    parser.add_argument("--workers", type=int, default=WORKERS,
                        help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--periods", type=int, default=3,
                        help="Periods to fetch per company (default: 3)")
    parser.add_argument("--save",    type=str, default=None,
                        help="Save full results to a JSON file")
    args = parser.parse_args()

    print(f"\n  EDGAR Financial Viewer — Deep Validator")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Flask: {FLASK_URL}")
    print(f"  Tolerance: ±{TOLERANCE:.0%}  |  Workers: {args.workers}  "
          f"|  Periods: {args.periods}\n")

    # Determine tickers to test
    if args.ticker:
        tickers = [t.strip().upper() for t in args.ticker.split(",")]
        print(f"  Testing {len(tickers)} specified ticker(s): {', '.join(tickers)}\n")
    else:
        print(f"  Fetching first {args.limit} S&P 500 tickers from Wikipedia...")
        tickers = fetch_sp500(args.limit)
        print(f"  Testing {len(tickers)} tickers\n")

    start = time.time()
    results = run_validation(tickers, workers=args.workers, periods=args.periods)
    elapsed = time.time() - start

    print_summary(results, elapsed)

    if args.save:
        with open(args.save, "w") as f:
            json.dump(results, f, indent=2)
        print(f"  Full results saved to: {args.save}\n")

    # Exit code 1 if any failures
    has_failures = any(r["fail"] > 0 or r["error"] for r in results)
    sys.exit(1 if has_failures else 0)


if __name__ == "__main__":
    main()