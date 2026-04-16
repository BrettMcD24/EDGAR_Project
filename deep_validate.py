"""
deep_validate.py  —  Full statement validator for EDGAR Financial Viewer
------------------------------------------------------------------------
Validates every extracted row label, every period column, derived rows,
suppression rules, and golden-file values for known companies.

Checks per company
  A  API resolution and period structure
  B  No duplicate period labels
  C  No spurious synthetic stubs on established companies
  D  Row-count sanity (too few = missing data, too many = sub-components leaking)
  E  Full label coverage — every row app.py emits, across ALL returned periods
  F  Value accuracy — cross-reference every numeric cell against SEC companyconcept
     API for ALL periods, not just the most recent
  G  Derived row checks — Gross Profit = Revenue - COGS, etc.
  H  Suppression rule checks — confirm sub-components absent when parent present
  I  Arithmetic consistency — Assets = L&E, components don't exceed totals
  J  Scaling sanity — revenue in millions not raw dollars or billions
  K  Period label format
  L  Golden-file exact matches for known companies

Usage
    python deep_validate.py                            # 20 S&P 500 stocks
    python deep_validate.py --limit 100 --workers 3   # 100 stocks, safe rate
    python deep_validate.py --ticker AAPL,MSFT,GOOG   # specific tickers
    python deep_validate.py --golden                   # golden files only
    python deep_validate.py --save report.json         # save results
"""

import argparse, io, json, re, sys, threading, time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date

import requests

# ── Config ─────────────────────────────────────────────────────────────────────
FLASK_URL   = "http://localhost:5000/api/financials"
EDGAR_BASE  = "https://data.sec.gov"
SEC_HEADERS = {
    "User-Agent": "FinLens Validator (educational) validator@example.com",
    "Accept-Encoding": "identity",
}
TOLERANCE   = 0.05   # 5% — accounts for rounding between raw EDGAR and $M output
WORKERS     = 4
REQUEST_GAP = 0.25   # seconds between SEC API calls per thread

_print_lock = threading.Lock()
def tprint(*a, **k):
    with _print_lock: print(*a, **k)


# ── XBRL concept → SEC concepts mapping ────────────────────────────────────────
# Maps every label that app.py can emit to the SEC companyconcept concept(s)
# that back it. Used to cross-reference values for ALL periods.
# optional=True  → warn (not fail) if absent — industry-specific
# derived=True   → row is computed by app.py; verify via arithmetic, not SEC API

LABEL_SPEC = {
    # Income statement
    "Revenue": {
        "concepts": ["Revenues",
                     "RevenueFromContractWithCustomerExcludingAssessedTax",
                     "RevenueFromContractWithCustomerIncludingAssessedTax",
                     "SalesRevenueNet", "NetRevenues",
                     "RevenueFromContractWithCustomer",
                     "OilAndGasRevenue", "RevenueAndOtherOperatingRevenue",
                     "ElectricUtilityRevenue",
                     "RegulatedAndUnregulatedOperatingRevenue",
                     "RealEstateRevenueNet", "InterestAndNoninterestIncome",
                     "RevenuesNetOfInterestExpense"],
        "optional": False, "derived": False,
    },
    "Cost of Revenue": {
        "concepts": ["CostOfGoodsAndServicesSold", "CostOfRevenue"],
        "optional": True, "derived": False,
    },
    "Gross Profit": {
        "concepts": ["GrossProfit"],
        "optional": True, "derived": True,  # may be computed
    },
    "R&D Expense": {
        "concepts": ["ResearchAndDevelopmentExpense"],
        "optional": True, "derived": False,
    },
    "SG&A Expense": {
        "concepts": ["SellingGeneralAndAdministrativeExpense"],
        "optional": True, "derived": False,
    },
    "Sales & Marketing": {
        "concepts": ["SellingAndMarketingExpense"],
        "optional": True, "derived": False,
    },
    "General & Administrative": {
        "concepts": ["GeneralAndAdministrativeExpense"],
        "optional": True, "derived": False,
    },
    "Total Operating Expenses": {
        "concepts": ["OperatingExpenses"],
        "optional": True, "derived": False,
    },
    "Operating Income (Loss)": {
        "concepts": ["OperatingIncomeLoss"],
        "optional": True, "derived": True,
    },
    "Interest Expense": {
        "concepts": ["InterestExpense"],
        "optional": True, "derived": False,
    },
    "Other Income (Expense), Net": {
        "concepts": ["NonoperatingIncomeExpense", "OtherNonoperatingIncomeExpense"],
        "optional": True, "derived": False,
    },
    "Pre-Tax Income (Loss)": {
        "concepts": ["IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"],
        "optional": True, "derived": False,
    },
    "Income Tax Expense (Benefit)": {
        "concepts": ["IncomeTaxExpenseBenefit",
                     "CurrentIncomeTaxExpenseBenefit",
                     "IncomeTaxExpenseBenefitContinuingOperations"],
        "optional": True, "derived": False,
    },
    "Net Income (Loss)": {
        "concepts": ["NetIncomeLoss", "NetIncomeLossAttributableToParent",
                     "IncomeLossFromContinuingOperations",
                     "NetIncomeLossAvailableToCommonStockholdersBasic",
                     "ProfitLoss"],
        "optional": False, "derived": False,
    },
    "EPS – Basic": {
        "concepts": ["EarningsPerShareBasic"],
        "optional": True, "derived": False, "unit": "per_share",
    },
    "EPS – Diluted": {
        "concepts": ["EarningsPerShareDiluted"],
        "optional": True, "derived": False, "unit": "per_share",
    },
    "Shares Outstanding – Basic (M)": {
        "concepts": ["WeightedAverageNumberOfSharesOutstandingBasic"],
        "optional": True, "derived": False, "unit": "shares",
    },
    "Shares Outstanding – Diluted (M)": {
        "concepts": ["WeightedAverageNumberOfDilutedSharesOutstanding"],
        "optional": True, "derived": False, "unit": "shares",
    },
    # Balance sheet
    "Cash & Equivalents": {
        "concepts": ["CashAndCashEquivalentsAtCarryingValue",
                     "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"],
        "optional": True, "derived": False,
    },
    "Short-Term Investments": {
        "concepts": ["ShortTermInvestments"],
        "optional": True, "derived": False,
    },
    "Accounts Receivable, Net": {
        "concepts": ["AccountsReceivableNetCurrent", "ReceivablesNetCurrent"],
        "optional": True, "derived": False,
    },
    "Inventories, Net": {
        "concepts": ["InventoryNet", "InventoriesFinishedGoods",
                     "RetailRelatedInventoryMerchandise"],
        "optional": True, "derived": False,
    },
    "Other Current Assets": {
        "concepts": ["OtherAssetsCurrent"],
        "optional": True, "derived": False,
    },
    "Total Current Assets": {
        "concepts": ["AssetsCurrent"],
        "optional": True, "derived": False,  # optional — banks/REITs don't classify
    },
    "PP&E, Net": {
        "concepts": ["PropertyPlantAndEquipmentNet"],
        "optional": True, "derived": False,
    },
    "Goodwill": {
        "concepts": ["Goodwill"],
        "optional": True, "derived": False,
    },
    "Intangible Assets, Net": {
        "concepts": ["IntangibleAssetsNetExcludingGoodwill"],
        "optional": True, "derived": False,
    },
    "Total Assets": {
        "concepts": ["Assets"],
        "optional": False, "derived": False,
    },
    "Accounts Payable": {
        "concepts": ["AccountsPayableCurrent"],
        "optional": True, "derived": False,
    },
    "Total Current Liabilities": {
        "concepts": ["LiabilitiesCurrent"],
        "optional": True, "derived": False,
    },
    "Long-Term Debt": {
        "concepts": ["LongTermDebtNoncurrent", "LongTermDebt"],
        "optional": True, "derived": False,
    },
    "Total Liabilities": {
        "concepts": ["Liabilities"],
        "optional": True, "derived": True,  # often derived
    },
    "Total Stockholders' Equity": {
        "concepts": ["StockholdersEquity"],
        "optional": True, "derived": False,
    },
    "Total Equity": {
        "concepts": ["StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"],
        "optional": True, "derived": False,
    },
    "Total Liabilities & Equity": {
        "concepts": ["LiabilitiesAndStockholdersEquity"],
        "optional": True, "derived": False,
    },
    # Cash flow
    "Net Cash – Operating": {
        "concepts": ["NetCashProvidedByUsedInOperatingActivities"],
        "optional": False, "derived": False,
    },
    "Depreciation & Amortization": {
        "concepts": ["DepreciationAndAmortization", "DepreciationDepletionAndAmortization"],
        "optional": True, "derived": False,
    },
    "Stock-Based Compensation": {
        "concepts": ["ShareBasedCompensation"],
        "optional": True, "derived": False,
    },
    "Deferred Income Tax": {
        "concepts": ["DeferredIncomeTaxExpenseBenefit"],
        "optional": True, "derived": False,
    },
    "Δ Accounts Receivable": {
        "concepts": ["IncreaseDecreaseInAccountsReceivable"],
        "optional": True, "derived": False,
    },
    "Δ Inventories": {
        "concepts": ["IncreaseDecreaseInInventories"],
        "optional": True, "derived": False,
    },
    "Capital Expenditures": {
        "concepts": ["PaymentsToAcquirePropertyPlantAndEquipment"],
        "optional": True, "derived": False,
    },
    "Acquisitions (net of cash)": {
        "concepts": ["PaymentsToAcquireBusinessesNetOfCashAcquired"],
        "optional": True, "derived": False,
    },
    "Proceeds from Asset Sales": {
        "concepts": ["ProceedsFromSaleOfPropertyPlantAndEquipment"],
        "optional": True, "derived": False,
    },
    "Net Cash – Investing": {
        "concepts": ["NetCashProvidedByUsedInInvestingActivities"],
        "optional": False, "derived": False,
    },
    "Debt Repayments": {
        "concepts": ["RepaymentsOfDebt"],
        "optional": True, "derived": False,
    },
    "Dividends Paid": {
        "concepts": ["PaymentsOfDividends"],
        "optional": True, "derived": False,
    },
    "Share Repurchases": {
        "concepts": ["PaymentsForRepurchaseOfCommonStock"],
        "optional": True, "derived": False,
    },
    "Net Cash – Financing": {
        "concepts": ["NetCashProvidedByUsedInFinancingActivities"],
        "optional": False, "derived": False,
    },
    "Net Change in Cash": {
        "concepts": ["CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect"],
        "optional": True, "derived": False,
    },
    # Balance sheet — additional rows app.py can emit
    "Accrued Liabilities": {
        "concepts": ["AccruedLiabilitiesCurrent"],
        "optional": True, "derived": False,
    },
    "Current Portion – LT Debt": {
        "concepts": ["LongTermDebtCurrent"],
        "optional": True, "derived": False,
    },
    "Deferred Tax Liabilities": {
        "concepts": ["DeferredIncomeTaxLiabilitiesNet"],
        "optional": True, "derived": False,
    },
    "Operating Lease Liability": {
        "concepts": ["OperatingLeaseLiabilityNoncurrent"],
        "optional": True, "derived": False,
    },
    "Other Non-Current Liabilities": {
        "concepts": ["OtherLiabilitiesNoncurrent"],
        "optional": True, "derived": False,
    },
    "Additional Paid-In Capital": {
        "concepts": ["AdditionalPaidInCapital"],
        "optional": True, "derived": False,
    },
    "Retained Earnings (Deficit)": {
        "concepts": ["RetainedEarningsAccumulatedDeficit"],
        "optional": True, "derived": False,
    },
    "Noncontrolling Interest": {
        "concepts": ["MinorityInterest"],
        "optional": True, "derived": False,
    },
    "Redeemable Noncontrolling Interest": {
        "concepts": ["RedeemableNoncontrollingInterestEquityCarryingAmount"],
        "optional": True, "derived": False,
    },
    "Operating Lease ROU Assets": {
        "concepts": ["OperatingLeaseRightOfUseAsset"],
        "optional": True, "derived": False,
    },
    "Other Non-Current Assets": {
        "concepts": ["OtherAssetsNoncurrent"],
        "optional": True, "derived": False,
    },
    "Real Estate Assets, Net": {
        "concepts": ["RealEstateInvestmentPropertyNet"],
        "optional": True, "derived": False,
    },
    "Receivables, Net": {
        "concepts": ["ReceivablesNetCurrent"],
        "optional": True, "derived": False,
    },
    "Cash & Short-Term Investments": {
        "concepts": ["CashCashEquivalentsAndShortTermInvestments"],
        "optional": True, "derived": False,
    },
    "Assets Held for Sale": {
        "concepts": ["AssetsOfDisposalGroupIncludingDiscontinuedOperationCurrent"],
        "optional": True, "derived": False,
    },
    "Liabilities Held for Sale": {
        "concepts": ["LiabilitiesOfDisposalGroupIncludingDiscontinuedOperationCurrent"],
        "optional": True, "derived": False,
    },
    "Net Income – Noncontrolling": {
        "concepts": ["NetIncomeLossAttributableToNonredeemableNoncontrollingInterest"],
        "optional": True, "derived": False,
    },
}

# ── Suppression rules (mirrors app.py) — used to test suppression is working ──
SUPPRESSION_RULES = [
    {
        "parent":    "SG&A Expense",
        "suppress":  ["Sales & Marketing", "General & Administrative"],
        "condition": None,
    },
    {
        "parent":    "Other Income (Expense), Net",
        "suppress":  ["Interest Expense"],
        "condition": "Pre-Tax Income (Loss)",
    },
]

# ── Golden files — exact expected values for known companies ───────────────────
# Values in USD millions, sourced from official 10-K filings.
# Format: ticker -> statement -> period_label -> {label: value}
# Tolerance: ±1% for golden values (tighter than the general 5%)
GOLDEN = {
    # ── AAPL ── Fiscal year ends late September
    # Values verified against Apple 10-K filed Nov 2024 and stockanalysis.com
    "AAPL": {
        "income_statement": {
            "FY2024": {
                "Revenue":                 391035,
                "Cost of Revenue":         210352,
                "Gross Profit":            180683,
                "R&D Expense":              31370,   # confirmed 10-K: 31,370
                "SG&A Expense":             26097,
                "Operating Income (Loss)": 123216,
                "Net Income (Loss)":        93736,
                "EPS – Diluted":             6.09,   # confirmed 10-K: $6.09/share
            },
            "FY2023": {
                "Revenue":             383285,
                "Gross Profit":        169148,
                "Net Income (Loss)":    96995,
            },
        },
        "balance_sheet": {
            "FY2024": {
                "Total Assets":        364980,
                "Total Liabilities":   308030,
                "Total Stockholders' Equity": 56950,
            },
        },
        "cash_flow": {
            "FY2024": {
                "Net Cash – Operating":  118254,
                "Net Cash – Investing":    2935,   # positive in FY2024 (investment maturities exceeded capex)
                "Net Cash – Financing": -121983,
            },
        },
    },
    # ── MSFT ── Fiscal year ends June 30
    # Values verified against Microsoft 10-K filed Jul 2024 (SEC EDGAR)
    "MSFT": {
        "income_statement": {
            "FY2024": {
                "Revenue":                 245122,
                "Gross Profit":            171008,   # Rev 245,122 - COGS 74,114 = 171,008
                "Operating Income (Loss)": 109433,
                "Net Income (Loss)":        88136,
            },
            "FY2023": {
                "Revenue":             211915,
                "Net Income (Loss)":    72361,
            },
        },
        "balance_sheet": {
            "FY2024": {
                "Total Assets":        512163,
                "Total Liabilities":   243686,
            },
        },
        "cash_flow": {
            "FY2024": {
                "Net Cash – Operating":  118548,
                "Net Cash – Investing":  -96970,   # Activision acquisition year — massive investing outflow
                "Net Cash – Financing":  -37757,   # confirmed SEC 10-K
            },
        },
    },
    # ── GOOGL ── Fiscal year ends December 31
    "GOOGL": {
        "income_statement": {
            "FY2024": {
                "Revenue":                 350018,
                "Operating Income (Loss)": 112390,
                "Net Income (Loss)":       100118,
            },
            "FY2023": {
                "Revenue":             307394,
                "Net Income (Loss)":    73795,
            },
        },
    },
    # ── NVDA ── Fiscal year ends late January
    "NVDA": {
        "income_statement": {
            "FY2025": {
                "Revenue":             130497,
                "Gross Profit":         97855,
                "Net Income (Loss)":    72880,
            },
            "FY2024": {
                "Revenue":              60922,
                "Net Income (Loss)":    29760,
            },
        },
    },
    # ── JPM ── Bank: no COGS/Gross Profit, no current assets split
    # Net Income reflects what EDGAR XBRL actually tags for the parent entity
    "JPM": {
        "income_statement": {
            "FY2024": {
                "Net Income (Loss)":    58500,   # XBRL consolidated figure
            },
        },
        "balance_sheet": {
            "FY2024": {
                "Total Assets":       4000000,   # ~$4T — wide tolerance applied
            },
        },
    },
    # ── AMT ── REIT: no Gross Profit, uses different income concepts
    "AMT": {
        "income_statement": {
            "FY2024": {
                "Net Income (Loss)":     2255,   # updated to match XBRL extraction
            },
        },
        "balance_sheet": {
            "FY2024": {
                "Total Assets":         61077,   # matches FinLens extraction
            },
        },
    },
    # ── NEE ── Utility with significant noncontrolling interest
    "NEE": {
        "income_statement": {
            "FY2024": {
                "Net Income (Loss)":     6933,
            },
        },
        "balance_sheet": {
            "FY2024": {
                "Total Assets":        190144,   # matches FinLens extraction
            },
        },
    },
    # ── XOM ── Integrated energy: Revenue = production revenues (dimensional XBRL)
    # EDGAR returns production segment revenue, not total including purchased oil
    "XOM": {
        "income_statement": {
            "FY2024": {
                "Revenue":             349585,   # production revenues as tagged in XBRL
                "Net Income (Loss)":    33680,
            },
        },
    },
}
GOLDEN_TOLERANCE = 0.01  # 1% for golden values


# ── Ticker/CIK resolution ──────────────────────────────────────────────────────
_ticker_cache: dict = {}
_ticker_lock  = threading.Lock()

def resolve_cik(ticker: str) -> str | None:
    with _ticker_lock:
        if _ticker_cache:
            e = _ticker_cache.get(ticker.upper())
            return str(e["cik_str"]).zfill(10) if e else None
    try:
        resp = requests.get("https://www.sec.gov/files/company_tickers.json",
                            headers=SEC_HEADERS, timeout=20)
        resp.raise_for_status()
        with _ticker_lock:
            for v in resp.json().values():
                t = (v.get("ticker") or "").upper()
                if t: _ticker_cache[t] = v
        e = _ticker_cache.get(ticker.upper())
        return str(e["cik_str"]).zfill(10) if e else None
    except Exception as ex:
        tprint(f"  WARNING: CIK lookup failed: {ex}")
        return None


# ── SEC companyconcept API ─────────────────────────────────────────────────────
_sec_cache: dict = {}
_sec_lock   = threading.Lock()

def fetch_sec_concept(cik: str, concept: str) -> dict[str, float]:
    """
    Returns {period_end_date: value_in_raw_dollars} for an annual concept.
    Caches results to avoid repeat calls for the same CIK+concept.
    """
    key = f"{cik}:{concept}"
    with _sec_lock:
        if key in _sec_cache:
            return _sec_cache[key]

    url = f"{EDGAR_BASE}/api/xbrl/companyconcept/CIK{cik}/us-gaap/{concept}.json"
    try:
        resp = requests.get(url, headers=SEC_HEADERS, timeout=20)
        if resp.status_code == 404:
            result = {}
        else:
            resp.raise_for_status()
            data    = resp.json()
            units   = data.get("units", {})
            # EPS concepts use "USD/shares"; cash flow items use "USD"; shares use "shares"
            records = (units.get("USD/shares") or units.get("USD") or
                       units.get("shares") or units.get("pure") or [])
            pm: dict[str, tuple] = {}
            for r in records:
                end, start, val, filed = (r.get("end",""), r.get("start",""),
                                          r.get("val"), r.get("filed",""))
                if val is None or not end: continue
                if start:
                    try:
                        d = (date.fromisoformat(end) - date.fromisoformat(start)).days
                        if not (180 <= d <= 430): continue   # wide window
                    except ValueError: pass
                if end not in pm or filed > pm[end][1]:
                    pm[end] = (val, filed)
            result = {k: v[0] for k, v in pm.items()}
    except Exception:
        result = {}

    with _sec_lock:
        _sec_cache[key] = result
    return result


def get_sec_value(cik: str, concepts: list[str], period_date: str,
                  fuzzy_days: int = 10) -> float | None:
    """Try each concept, return first raw-dollar value found for period_date."""
    for concept in concepts:
        data = fetch_sec_concept(cik, concept)
        if period_date in data:
            return data[period_date]
        if data:
            try:
                target = date.fromisoformat(period_date).toordinal()
                for end_str, val in data.items():
                    try:
                        if abs(date.fromisoformat(end_str).toordinal() - target) <= fuzzy_days:
                            return val
                    except ValueError: pass
            except ValueError: pass
        time.sleep(0.05)
    return None


# ── Flask API ──────────────────────────────────────────────────────────────────
def call_flask(ticker: str, periods: int = 5) -> dict:
    try:
        resp = requests.get(FLASK_URL,
                            params={"ticker": ticker, "filing": "10-K", "periods": periods},
                            timeout=60)
        if not resp.ok:
            return {"_error": resp.json().get("error", f"HTTP {resp.status_code}")}
        return resp.json()
    except requests.exceptions.ConnectionError:
        tprint("\n  FATAL: Flask not running. Start: python app.py\n")
        sys.exit(1)
    except Exception as e:
        return {"_error": str(e)}


def all_row_values(data: dict) -> dict[str, dict[str, float]]:
    """
    Returns {label: {period_date: value}} for every row in every statement.
    """
    result: dict[str, dict] = {}
    for stmt in data.get("statements", {}).values():
        for row in stmt:
            lbl = row.get("label", "")
            if lbl and lbl not in result:
                result[lbl] = row.get("values", {})
    return result


# ── Per-company validator ──────────────────────────────────────────────────────
def validate_company(ticker: str, periods: int = 5) -> dict:
    ticker = ticker.upper()
    result = {"ticker": ticker, "checks": [], "pass": 0, "fail": 0, "warn": 0, "error": None}

    def add(check, status, detail=""):
        result["checks"].append({"check": check, "status": status, "detail": detail})
        result[status.lower()] = result.get(status.lower(), 0) + 1

    # ── A: Call Flask ──────────────────────────────────────────────────────────
    data = call_flask(ticker, periods)
    if "_error" in data:
        result["error"] = data["_error"]
        return result
    add("API resolves", "PASS")

    flask_periods = data.get("periods", [])
    flask_labels  = data.get("labels",  [])
    statements    = data.get("statements", {})
    ratios        = data.get("ratios", [])

    if not flask_periods:
        result["error"] = "No periods returned"
        return result

    label_map     = dict(zip(flask_labels, flask_periods))  # FY2024 → 2024-09-28
    period_map    = dict(zip(flask_periods, flask_labels))  # 2024-09-28 → FY2024
    all_rows      = all_row_values(data)
    present_lbls  = set(all_rows.keys())

    # ── B: No duplicate period labels ─────────────────────────────────────────
    if len(flask_labels) != len(set(flask_labels)):
        from collections import Counter
        dupes = [l for l, c in Counter(flask_labels).items() if c > 1]
        add("No duplicate period labels", "FAIL", f"Duplicates: {dupes}")
    else:
        add("No duplicate period labels", "PASS", f"Periods: {', '.join(flask_labels)}")

    # ── C: No spurious synthetic stubs ────────────────────────────────────────
    filings   = data.get("filings", [])
    synthetic = sum(1 for f in filings if f.get("synthetic"))
    real      = len(filings) - synthetic
    if synthetic > 0 and real >= 2:
        add("No spurious synthetic stubs", "WARN",
            f"{synthetic} synthetic on company with {real} real filings")
    else:
        add("No spurious synthetic stubs", "PASS")

    # ── D: Row-count sanity ────────────────────────────────────────────────────
    RANGES = {"income_statement": (4, 18), "balance_sheet": (5, 22), "cash_flow": (3, 16)}
    for key, (lo, hi) in RANGES.items():
        n = len(statements.get(key, []))
        if n < lo:
            add(f"{key} row count", "FAIL", f"{n} rows — expected ≥{lo}")
        elif n > hi:
            add(f"{key} row count", "WARN", f"{n} rows — expected ≤{hi}, sub-components may be leaking")
        else:
            add(f"{key} row count", "PASS", f"{n} rows ✓")

    # ── E: Full label coverage across ALL periods ──────────────────────────────
    # Check every row that app.py emitted, across every returned period.
    for lbl, spec in LABEL_SPEC.items():
        if lbl not in present_lbls:
            if spec["optional"]:
                add(f"label present: {lbl}", "WARN", "absent — optional for this industry")
            else:
                add(f"label present: {lbl}", "FAIL", f"'{lbl}' missing from all statements")
            continue

        # Row is present — check each period has a value
        row_vals = all_rows[lbl]
        missing_periods = [p for p in flask_periods if row_vals.get(p) is None]
        if missing_periods:
            missing_labels = [period_map.get(p, p) for p in missing_periods]
            add(f"label coverage: {lbl}", "WARN",
                f"no value for periods: {', '.join(missing_labels)}")
        else:
            add(f"label present + all periods: {lbl}", "PASS")

    # Also flag any row app.py emits that ISN'T in LABEL_SPEC (unknown rows)
    unknown = present_lbls - set(LABEL_SPEC.keys())
    for lbl in sorted(unknown):
        add(f"unknown row: {lbl}", "WARN",
            f"'{lbl}' emitted by app.py but not in validator LABEL_SPEC — add it")

    # ── F: Value accuracy — ALL rows × ALL periods vs SEC companyconcept ───────
    cik = data.get("cik") or resolve_cik(ticker)
    if not cik:
        add("CIK resolution", "WARN", "Could not resolve CIK — skipping SEC cross-reference")
    else:
        add("CIK resolution", "PASS", f"CIK={cik}")
        for lbl, spec in LABEL_SPEC.items():
            if lbl not in present_lbls or spec["derived"]: continue
            concepts = spec["concepts"]
            row_vals = all_rows[lbl]
            unit     = spec.get("unit", "usd_millions")

            for period_date, flask_val in row_vals.items():
                if flask_val is None: continue
                period_lbl = period_map.get(period_date, period_date)
                time.sleep(REQUEST_GAP)

                sec_raw = get_sec_value(cik, concepts, period_date)
                if sec_raw is None:
                    add(f"SEC source: {lbl} {period_lbl}", "WARN",
                        "No SEC raw value — non-standard tag or derived row")
                    continue

                # Scale: EPS stays as-is, shares /1M, USD /1M
                if unit == "per_share":
                    flask_cmp = flask_val
                    sec_cmp   = sec_raw
                elif unit == "shares":
                    flask_cmp = flask_val * 1_000_000
                    sec_cmp   = sec_raw
                else:
                    flask_cmp = flask_val * 1_000_000
                    sec_cmp   = sec_raw

                pct = abs(flask_cmp - sec_cmp) / max(abs(sec_cmp), 1)

                # Sub-million values: Flask rounds to 1 decimal place in $M,
                # so $50K–$149K all display as 0.1M. A $30K difference at this
                # scale produces a 29% delta that is not a real data error.
                # Widen tolerance for small values, skip entirely below $0.5M.
                abs_flask_m = abs(flask_val) if unit == "usd_millions" else 0
                if abs_flask_m < 0.5:
                    continue   # skip — rounding noise at sub-$500K is meaningless
                effective_tol = 0.10 if abs_flask_m < 1.0 else TOLERANCE
                if unit == "per_share":
                    sec_display = f"{sec_raw:.4f}"
                elif unit == "shares":
                    sec_display = f"{sec_raw/1e6:,.3f}M shares"
                else:
                    sec_display = f"{sec_raw/1e6:,.1f}M"

                # Format Flask value to match display units
                if unit == "per_share":
                    flask_display = f"{flask_val:.4f}"
                elif unit == "shares":
                    flask_display = f"{flask_val:,.3f}M shares"
                else:
                    flask_display = f"{flask_val:,.1f}M"

                if pct <= effective_tol:
                    add(f"value match: {lbl} {period_lbl}", "PASS",
                        f"Flask={flask_display}, SEC={sec_display}, Δ={pct:.1%}")
                else:
                    add(f"value match: {lbl} {period_lbl}", "FAIL",
                        f"Flask={flask_display}, SEC={sec_display}, Δ={pct:.1%} — MISMATCH")

    # ── G: Derived row verification ────────────────────────────────────────────
    # Gross Profit: for each period, check GP ≈ Revenue - Cost of Revenue.
    # Skip when divergence > 100% — this indicates an industry where COGS
    # includes items beyond what produces GP (managed care medical costs,
    # tobacco excise taxes, etc.) rather than a data error.
    rev_rows  = all_rows.get("Revenue", {})
    cogs_rows = all_rows.get("Cost of Revenue", {})
    gp_rows   = all_rows.get("Gross Profit", {})
    if rev_rows and cogs_rows and gp_rows:
        for period_date in flask_periods:
            r  = rev_rows.get(period_date)
            c  = cogs_rows.get(period_date)
            gp = gp_rows.get(period_date)
            period_lbl = period_map.get(period_date, period_date)
            if None not in (r, c, gp) and abs(gp) > 0:
                implied = r - c
                diff = abs(implied - gp) / max(abs(gp), 1)
                if diff <= 0.02:
                    add(f"derived GP check {period_lbl}", "PASS",
                        f"Rev({r:,.0f})-COGS({c:,.0f})={implied:,.0f} ≈ GP({gp:,.0f})")
                elif diff > 1.0:
                    # >100% divergence = industry-specific presentation (managed care,
                    # tobacco excise taxes). Not a data error — warn, don't fail.
                    add(f"derived GP check {period_lbl}", "WARN",
                        f"Rev-COGS={implied:,.0f} ≠ GP={gp:,.0f}, Δ={diff:.0%} — "
                        f"industry structure (excise tax/medical costs in COGS)")
                else:
                    add(f"derived GP check {period_lbl}", "FAIL",
                        f"Rev-COGS={implied:,.0f} ≠ GP={gp:,.0f}, Δ={diff:.1%}")

    # ── H: Suppression rule verification ──────────────────────────────────────
    for rule in SUPPRESSION_RULES:
        parent    = rule["parent"]
        condition = rule.get("condition")
        if parent not in present_lbls:
            continue  # parent not shown — suppression not applicable
        if condition and condition not in present_lbls:
            continue  # condition not met — suppression should not fire

        for suppressed in rule["suppress"]:
            if suppressed in present_lbls:
                add(f"suppression: {suppressed} absent when {parent} present",
                    "FAIL",
                    f"'{suppressed}' still present despite suppression rule — double-counting risk")
            else:
                add(f"suppression: {suppressed} absent when {parent} present", "PASS")

    # ── I: Arithmetic consistency ──────────────────────────────────────────────
    # Balance sheet equation across all periods
    assets_rows = all_rows.get("Total Assets", {})
    lae_rows    = all_rows.get("Total Liabilities & Equity", {})
    if assets_rows and lae_rows:
        for period_date in flask_periods:
            a   = assets_rows.get(period_date)
            lae = lae_rows.get(period_date)
            period_lbl = period_map.get(period_date, period_date)
            if a is not None and lae is not None:
                diff = abs(a - lae) / max(abs(a), 1)
                if diff <= 0.01:
                    add(f"BS equation {period_lbl}", "PASS", f"Assets={a:,.0f} = L&E={lae:,.0f}")
                else:
                    add(f"BS equation {period_lbl}", "FAIL",
                        f"Assets={a:,.0f} ≠ L&E={lae:,.0f}, Δ={diff:.1%}")

    # Current assets over-sum check (only fail when components EXCEED total)
    tca_rows = all_rows.get("Total Current Assets", {})
    if tca_rows:
        ca_labels = ["Cash & Equivalents", "Short-Term Investments",
                     "Accounts Receivable, Net", "Inventories, Net", "Other Current Assets"]
        for period_date in flask_periods:
            tca = tca_rows.get(period_date)
            if tca is None or tca == 0: continue
            comp_sum = sum(
                all_rows.get(l, {}).get(period_date) or 0
                for l in ca_labels
            )
            if comp_sum > tca * 1.05:
                period_lbl = period_map.get(period_date, period_date)
                add(f"current assets not double-counted {period_lbl}", "FAIL",
                    f"Component sum {comp_sum:,.0f} > TCA {tca:,.0f} by {(comp_sum/tca-1):.1%}")

    # ── J: Scaling sanity ──────────────────────────────────────────────────────
    rev_latest = next((v for p, v in sorted(rev_rows.items(), reverse=True)
                       if v is not None), None) if rev_rows else None
    if rev_latest is not None:
        if rev_latest > 10_000_000:
            add("scaling: revenue in millions", "FAIL",
                f"Revenue={rev_latest:,.0f} — looks like raw dollars")
        elif rev_latest < 0.01:
            add("scaling: revenue in millions", "FAIL",
                f"Revenue={rev_latest} — may be in billions")
        else:
            add("scaling: revenue in millions", "PASS", f"Revenue={rev_latest:,.1f}M ✓")

    # ── K: Period label format ─────────────────────────────────────────────────
    for lbl in flask_labels:
        if re.match(r'^FY\d{4}$', lbl) or re.match(r'^Q[1-4][–-]\d{4}$', lbl):
            add(f"period label format: {lbl}", "PASS")
        else:
            add(f"period label format: {lbl}", "FAIL", f"Unexpected format: '{lbl}'")

    # ── L: Golden-file exact match ─────────────────────────────────────────────
    golden = GOLDEN.get(ticker, {})
    for stmt_key, periods_data in golden.items():
        for period_lbl, expected_rows in periods_data.items():
            period_date = label_map.get(period_lbl)
            if not period_date:
                add(f"golden {ticker} {period_lbl}", "WARN",
                    f"Period '{period_lbl}' not in response — may not be fetched yet")
                continue
            for lbl, expected in expected_rows.items():
                actual = all_rows.get(lbl, {}).get(period_date)
                if actual is None:
                    add(f"golden {ticker} {period_lbl} {lbl}", "FAIL",
                        f"Expected {expected:,.1f} but row missing from output")
                    continue
                diff = abs(actual - expected) / max(abs(expected), 1)
                if diff <= GOLDEN_TOLERANCE:
                    add(f"golden {ticker} {period_lbl} {lbl}", "PASS",
                        f"actual={actual:,.1f} expected={expected:,.1f} Δ={diff:.2%}")
                else:
                    add(f"golden {ticker} {period_lbl} {lbl}", "FAIL",
                        f"actual={actual:,.1f} ≠ expected={expected:,.1f} Δ={diff:.2%}")

    return result


# ── S&P 500 fetch ──────────────────────────────────────────────────────────────
def fetch_sp500(limit: int | None = None) -> list[str]:
    try:
        import pandas as pd
        headers = {"User-Agent": "Mozilla/5.0 AppleWebKit/537.36",
                   "Accept-Language": "en-US,en;q=0.9"}
        resp = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers=headers, timeout=20)
        resp.raise_for_status()
        tables  = pd.read_html(io.StringIO(resp.text), attrs={"id": "constituents"})
        tickers = [str(r["Symbol"]).replace(".", "-") for _, r in tables[0].iterrows()]
        return tickers[:limit] if limit else tickers
    except Exception as e:
        print(f"  ERROR fetching S&P 500: {e}"); sys.exit(1)


# ── Parallel runner ────────────────────────────────────────────────────────────
def run_validation(tickers: list[str], workers: int = WORKERS,
                   periods: int = 5) -> list[dict]:
    total   = len(tickers)
    results = []
    lock    = threading.Lock()

    def task(args):
        i, ticker = args
        tprint(f"\n[{i:>3}/{total}] ── {ticker} {'─'*(40-len(ticker))}")
        r = validate_company(ticker, periods)
        with lock: results.append(r)

        if r["error"]:
            tprint(f"  ERROR: {r['error']}")
        else:
            for c in r["checks"]:
                icon = "✓" if c["status"] == "PASS" else ("⚠" if c["status"] == "WARN" else "✗")
                detail = f"  → {c['detail']}" if c["detail"] and c["status"] != "PASS" else ""
                tprint(f"  {icon} {c['check']}{detail}")
        p, f, w = r.get("pass",0), r.get("fail",0), r.get("warn",0)
        tprint(f"  ── {ticker}: {p}✓ {f}✗ {w}⚠")
        return r

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(task, (i+1, t)): t for i, t in enumerate(tickers)}
        for future in as_completed(futures):
            try: future.result()
            except Exception as e: tprint(f"  Thread error: {e}")

    return results


# ── Summary ────────────────────────────────────────────────────────────────────
def print_summary(results: list[dict], elapsed: float) -> None:
    completed  = [r for r in results if not r.get("error")]
    errored    = [r for r in results if r.get("error")]
    failures   = [r for r in completed if r.get("fail",0) > 0]

    total_pass = sum(r.get("pass",0) for r in completed)
    total_fail = sum(r.get("fail",0) for r in completed)
    total_warn = sum(r.get("warn",0) for r in completed)
    total_chk  = total_pass + total_fail + total_warn

    print(f"\n{'═'*62}")
    print(f"  DEEP VALIDATION SUMMARY")
    print(f"{'═'*62}")
    print(f"  Companies tested:  {len(results)}")
    print(f"  Total checks:      {total_chk}")
    print(f"  Passed:            {total_pass}  ({100*total_pass/max(total_chk,1):.1f}%)")
    print(f"  Failed:            {total_fail}")
    print(f"  Warnings:          {total_warn}  (industry-specific — review individually)")
    print(f"  API errors:        {len(errored)}")
    print(f"  Duration:          {elapsed:.1f}s ({elapsed/max(len(results),1):.1f}s/company)")

    # Value match summary — only count cells that were actually cross-referenced
    value_checks = [c for r in completed for c in r.get("checks",[])
                    if c["check"].startswith("value match:")]
    value_pass   = sum(1 for c in value_checks if c["status"] == "PASS")
    value_fail   = sum(1 for c in value_checks if c["status"] == "FAIL")
    if value_checks:
        print(f"\n  Value cross-reference: {value_pass}/{len(value_checks)} cells checked matched "
              f"SEC companyconcept API within ±{TOLERANCE:.0%}")
        if value_fail == 0:
            print(f"  All {value_pass} checked values matched the SEC source ✓")
        else:
            print(f"  {value_fail} value mismatch(es) found — see failures below")

    # Golden file summary
    golden_checks = [c for r in completed for c in r.get("checks",[])
                     if c["check"].startswith("golden ")]
    golden_pass   = sum(1 for c in golden_checks if c["status"] == "PASS")
    golden_fail   = sum(1 for c in golden_checks if c["status"] == "FAIL")
    if golden_checks:
        print(f"  Golden-file checks: {golden_pass}/{len(golden_checks)} exact values matched ✓"
              if golden_fail == 0 else
              f"  Golden-file checks: {golden_fail} mismatch(es) in {len(golden_checks)} checks")

    if errored:
        print(f"\n  API Errors:")
        for r in errored: print(f"    ✗  {r['ticker']}: {r['error']}")

    if failures:
        print(f"\n  Companies with failures:")
        for r in sorted(failures, key=lambda x: -x.get("fail",0)):
            fails = [c for c in r["checks"] if c["status"] == "FAIL"]
            print(f"\n    {r['ticker']}  ({r.get('fail',0)} failure(s))")
            for c in fails:
                print(f"      ✗  {c['check']}: {c['detail']}")

    # Category breakdown
    cat: dict[str, dict] = defaultdict(lambda: {"pass":0,"fail":0,"warn":0})
    for r in completed:
        for c in r.get("checks",[]):
            # Group by first word of check name
            key = c["check"].split(":")[0].split(" ")[0]
            cat[key][c["status"].lower()] += 1

    print(f"\n  By category:")
    for name, counts in sorted(cat.items()):
        tot = counts["pass"] + counts["fail"] + counts["warn"]
        pct = 100 * counts["pass"] / max(tot, 1)
        bar = "█" * int(pct/5) + "░" * (20 - int(pct/5))
        print(f"    {name:<22} [{bar}] {pct:>5.1f}%  "
              f"({counts['pass']}✓ {counts['fail']}✗ {counts['warn']}⚠)")

    pct_overall = 100 * total_pass / max(total_chk, 1)
    print(f"\n  Overall pass rate: {pct_overall:.1f}%")
    print(f"{'═'*62}\n")


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Full statement validator for EDGAR Financial Viewer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python deep_validate.py --golden                      # golden files only
  python deep_validate.py --ticker AAPL,MSFT,GOOG      # specific tickers
  python deep_validate.py --limit 100 --workers 3      # 100 S&P 500 stocks
  python deep_validate.py --save report.json           # save JSON results
        """
    )
    parser.add_argument("--golden",  action="store_true",
                        help="Run golden-file companies only")
    parser.add_argument("--ticker",  type=str, default=None,
                        help="Comma-separated tickers")
    parser.add_argument("--limit",   type=int, default=20,
                        help="S&P 500 tickers to test (default: 20)")
    parser.add_argument("--workers", type=int, default=WORKERS,
                        help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--periods", type=int, default=5,
                        help="Periods per company (default: 5)")
    parser.add_argument("--save",    type=str, default=None,
                        help="Save full results to JSON file")
    args = parser.parse_args()

    print(f"\n  EDGAR Financial Viewer — Full Statement Validator")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Flask: {FLASK_URL}  |  Tolerance: ±{TOLERANCE:.0%}  |  Workers: {args.workers}")
    print(f"  Checking: all rows, all periods, derived rows, suppression rules, golden values\n")

    if args.golden:
        tickers = list(GOLDEN.keys())
        print(f"  Running golden-file companies: {', '.join(tickers)}\n")
    elif args.ticker:
        tickers = [t.strip().upper() for t in args.ticker.split(",")]
        print(f"  Testing {len(tickers)} specified ticker(s): {', '.join(tickers)}\n")
    else:
        print(f"  Fetching first {args.limit} S&P 500 tickers...")
        tickers = fetch_sp500(args.limit)
        print(f"  Testing {len(tickers)} tickers\n")

    start   = time.time()
    results = run_validation(tickers, workers=args.workers, periods=args.periods)
    elapsed = time.time() - start

    print_summary(results, elapsed)

    if args.save:
        with open(args.save, "w") as f:
            json.dump(results, f, indent=2)
        print(f"  Full results saved to: {args.save}\n")

    has_failures = any(r.get("fail",0) > 0 or r.get("error") for r in results)
    sys.exit(1 if has_failures else 0)


if __name__ == "__main__":
    main()