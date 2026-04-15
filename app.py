"""
app.py — EDGAR Financial Viewer (Flask)
---------------------------------------
Runs the EDGAR data pipeline entirely on the server, eliminating
the CORS problem that blocks browser-to-EDGAR requests.

Usage:
    pip install flask requests
    python app.py

Then open: http://localhost:5000
"""

import json
import logging
import time
import os
from datetime import date, timedelta
from pathlib import Path

import requests
from flask import Flask, jsonify, render_template, request

# ── Flask setup ───────────────────────────────────────────────────────────────

app = Flask(__name__, template_folder="templates", static_folder="static")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ── EDGAR API constants ───────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": "EDGAR Financial Viewer (educational use) viewer@example.com",
    "Accept-Encoding": "identity",
    "Accept": "application/json",
}

TICKERS_URL     = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
FACTS_URL       = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# Simple in-memory cache to avoid hammering EDGAR
_cache: dict = {}
CACHE_TTL = 300  # seconds


# ── HTTP helper ───────────────────────────────────────────────────────────────

def edgar_get(url: str, retries: int = 3) -> dict:
    """Fetch a URL from EDGAR with retry/backoff. Caches results for CACHE_TTL seconds."""
    now = time.time()
    if url in _cache:
        data, ts = _cache[url]
        if now - ts < CACHE_TTL:
            logger.info("Cache hit: %s", url)
            return data

    delay = 2
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 429:
                wait = delay * (2 ** attempt)
                logger.warning("Rate limited (429). Waiting %ds…", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            _cache[url] = (data, now)
            return data
        except requests.exceptions.ConnectionError:
            if attempt == retries - 1:
                raise ConnectionError("Could not reach SEC EDGAR. Check your internet connection.")
            time.sleep(delay)
        except requests.exceptions.Timeout:
            if attempt == retries - 1:
                raise ConnectionError("SEC EDGAR request timed out.")
            time.sleep(delay)
        except requests.exceptions.HTTPError as e:
            raise ValueError(f"EDGAR returned an error: {e}")

    raise ConnectionError("Failed to fetch from SEC EDGAR after retries.")


# ── Ticker resolution ─────────────────────────────────────────────────────────

def resolve_ticker(ticker: str) -> tuple[str, str]:
    """
    Map a ticker symbol to (cik_padded, company_name).
    Raises ValueError if not found.
    """
    ticker = ticker.strip().upper()
    raw = edgar_get(TICKERS_URL)
    for entry in raw.values():
        if (entry.get("ticker") or "").upper() == ticker:
            cik = str(entry["cik_str"]).zfill(10)
            return cik, entry.get("title", ticker)
    raise ValueError(f"Ticker '{ticker}' not found in SEC EDGAR.")


# ── Filing discovery ──────────────────────────────────────────────────────────

FORM_VARIANTS = {
    "10-K": ["10-K", "10-K/A", "10-KT"],
    "10-Q": ["10-Q", "10-Q/A"],
}


def get_filings(cik: str, form_type: str, n_periods: int) -> list[dict]:
    """
    Return N most recent filings of form_type for the given CIK.
    Appends synthetic prior-period stubs for spinoffs/IPOs with fewer
    real filings than requested (enables comparative-year recovery).
    """
    accepted = FORM_VARIANTS.get(form_type, [form_type])
    subs = edgar_get(SUBMISSIONS_URL.format(cik=cik))

    fd      = subs.get("filings", {}).get("recent", {})
    forms   = fd.get("form", [])
    accs    = fd.get("accessionNumber", [])
    dates   = fd.get("filingDate", [])
    periods = fd.get("reportDate", [])

    matched = []
    for i, form in enumerate(forms):
        if form in accepted and len(matched) < n_periods:
            matched.append({
                "accession_number": (accs[i] if i < len(accs) else "").replace("-", ""),
                "filing_date":       dates[i]   if i < len(dates)   else "",
                "form":              form,
                "report_date":       periods[i] if i < len(periods) else "",
            })

    if not matched:
        raise ValueError(f"No {form_type} filings found for CIK {cik}.")

    # Deduplicate: remove any filings with the same report_date
    # (can happen when a company files both a 10-K and 10-K/A for the same period)
    seen_dates = set()
    deduped = []
    for f in matched:
        rd = f.get("report_date", "")
        if rd not in seen_dates:
            seen_dates.add(rd)
            deduped.append(f)
    matched = deduped

    # Synthetic stubs ONLY for genuine spinoffs/IPOs — companies with fewer
    # than 2 real filings. Established companies (e.g. Alphabet with 4 real
    # filings) should NOT get synthetic stubs; we just return what EDGAR has.
    real_count = len(matched)
    if real_count < 2:
        anchor      = matched[0]
        anchor_date = date.fromisoformat(anchor["report_date"])
        step_days   = 365 if form_type == "10-K" else 91

        for i in range(1, n_periods - real_count + 1):
            prior = anchor_date - timedelta(days=step_days * i)
            prior_str = prior.isoformat()
            if prior_str not in seen_dates:
                matched.append({
                    "accession_number": anchor["accession_number"],
                    "filing_date":      anchor["filing_date"],
                    "form":             anchor["form"],
                    "report_date":      prior_str,
                    "synthetic":        True,
                })
                seen_dates.add(prior_str)

    return matched[:n_periods]


# ── XBRL concept lists ────────────────────────────────────────────────────────

INCOME_CONCEPTS = [
    # ── Revenue — ordered by prevalence across S&P 500 ──────────────────────
    # "Revenues" is listed first because it is the broadest tag used by the
    # widest range of companies including Alphabet, industrials, and energy.
    # More specific tags follow as fallbacks.
    "Revenues",                                             # broadest — Alphabet, industrials, energy
    "RevenueFromContractWithCustomerExcludingAssessedTax",  # tech/consumer (Apple, Microsoft)
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
    "NetRevenues",
    "RevenueFromContractWithCustomer",                      # Alphabet / GOOGL variant
    "OilAndGasRevenue",                                     # pure-play E&P (DVN, OXY)
    "OilAndGasRevenueNetOfRoyalties",                       # some Canadian-listed E&P
    "ExplorationAndProductionRevenue",                      # some E&P filers
    "RevenueFromOilAndGas",                                 # older E&P tag
    "RevenueAndOtherOperatingRevenue",                      # APA Corporation — "Total revenues and other"
    "RevenuesAndOtherIncome",                               # APA alt tag
    "CrudeOilAndNaturalGasRevenue",                         # some upstream E&P
    "OilAndCondensateRevenue",                              # pure upstream producers
    "NaturalGasProductionRevenue",                          # gas-weighted E&P
    "NaturalGasGatheringTransportationMarketingAndProcessingRevenue",  # midstream (TRGP, EQT)
    "GasGatheringTransportationMarketingAndProcessingRevenue",         # midstream alt tag
    "ElectricUtilityRevenue",                               # utilities (AEE, AEP, ETR, ES)
    "RegulatedAndUnregulatedOperatingRevenue",              # diversified utilities
    "ElectricAndGasUtilitiesRevenue",                       # combined electric+gas utilities
    "UtilitiesOperatingRevenue",                            # utility operating revenue
    "RealEstateRevenueNet",                                 # REITs (EQR, AVB)
    "OperatingLeasesIncomeStatementLeaseRevenue",           # leasing REITs
    "HealthCareOrganizationRevenue",                        # healthcare systems
    "InterestAndNoninterestIncome",                         # banks (GS, MS, RF, FITB, SYF, TFC)
    "NetInterestIncome",                                    # some bank filers
    "RevenuesNetOfInterestExpense",                         # Invesco and asset managers
    "ManagementFeesRevenue",                                # asset managers (IVZ)
    "InvestmentBankingRevenue",                             # investment banks
    "BrokerageCommissionsRevenue",                          # broker-dealers
    "AerospaceAndDefenseRevenue",                           # defense contractors (LHX)
    "ContractWithCustomerLiabilityRevenueRecognized",       # defense/aerospace alt
    "SalesRevenueGoodsNet",                                 # older GAAP tag
    "SalesRevenueServicesNet",                              # service companies
    # ── Cost & margin ───────────────────────────────────────────────────────
    "CostOfGoodsAndServicesSold", "CostOfRevenue", "GrossProfit",
    # ── Operating expenses ──────────────────────────────────────────────────
    "ResearchAndDevelopmentExpense",
    "SellingAndMarketingExpense",                           # Alphabet Sales & Marketing
    "SellingGeneralAndAdministrativeExpense",               # combined SG&A (many companies)
    "GeneralAndAdministrativeExpense",                      # G&A standalone
    "OperatingExpenses",
    "OperatingIncomeLoss",
    "OperatingIncomeLossFromContinuingOperations",          # some industrials
    "IncomeLossFromContinuingOperationsBeforeInterestExpenseInterestIncomeIncomeTaxesExtraordinaryItemsNoncontrollingInterestsNet",  # insurance (AFL)
    "RealEstateInvestmentTrustNetOperatingIncomeLoss",      # REITs (ARE)
    "InterestExpense",
    "NonoperatingIncomeExpense",                            # Other income (expense), net — Alphabet
    "OtherNonoperatingIncomeExpense",
    # ── Pre-tax & tax ───────────────────────────────────────────────────────
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
    "IncomeTaxExpenseBenefit",
    "CurrentIncomeTaxExpenseBenefit",                       # some REITs file current tax only
    "IncomeTaxExpenseBenefitContinuingOperations",          # BXP, ARE specific
    # ── Net income — ordered by priority (first match wins per label) ────────
    "NetIncomeLoss",                                        # consolidated (most common)
    "NetIncomeLossAttributableToParent",                    # parent-only (REITs, MLPs)
    "IncomeLossFromContinuingOperations",                   # utilities, some industrials
    "IncomeLossFromContinuingOperationsNetOfTax",           # Parker Hannifin, some industrials
    "IncomeLossFromContinuingOperationsNetOfTaxIncludingPortionAttributableToNoncontrollingInterest",  # Broadcom, utilities
    "NetIncomeLossAvailableToCommonStockholdersBasic",      # after preferred dividends (PNC, ETR, ES)
    "NetIncomeLossIncludingPortionAttributableToNonredeemableNoncontrollingInterest",  # Ford, some industrials
    "NetIncomeLossAttributableToNonredeemableNoncontrollingInterest",  # partnership structures
    "ProfitLoss",                                           # IFRS-adjacent filers
    "ComprehensiveIncomeNetOfTax",                          # last resort fallback
    # ── Per share ───────────────────────────────────────────────────────────
    "EarningsPerShareBasic", "EarningsPerShareDiluted",
    "WeightedAverageNumberOfSharesOutstandingBasic",
    "WeightedAverageNumberOfDilutedSharesOutstanding",
]

BALANCE_CONCEPTS = [
    # ── Cash ────────────────────────────────────────────────────────────────
    "CashAndCashEquivalentsAtCarryingValue",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",  # 3M and many large caps
    "CashAndCashEquivalentsAtCarryingValueIncludingDiscontinuedOperations",
    "CashCashEquivalentsAndShortTermInvestments",   # when cash + ST investments combined
    "CashAndCashEquivalentsPeriodIncreaseDecrease", # some utilities
    "ShortTermInvestments",
    # ── Receivables & current assets ────────────────────────────────────────
    "AccountsReceivableNetCurrent",
    "ReceivablesNetCurrent",                        # broader receivables (banks, insurance)
    "InventoryNet",
    "OtherAssetsCurrent",
    "AssetsCurrent",
    "AssetsOfDisposalGroupIncludingDiscontinuedOperationCurrent",  # spinoffs/discontinued
    "OtherAssets",                                       # some REITs lump everything here
    # ── Non-current assets ───────────────────────────────────────────────────
    "PropertyPlantAndEquipmentNet",
    "RealEstateInvestmentPropertyNet",              # REITs (ARE) — replaces PP&E
    "Goodwill",
    "IntangibleAssetsNetExcludingGoodwill",
    "OperatingLeaseRightOfUseAsset",
    "OtherAssetsNoncurrent",
    "Assets",
    # ── Current liabilities ──────────────────────────────────────────────────
    "AccountsPayableCurrent",
    "AccruedLiabilitiesCurrent",
    "LongTermDebtCurrent",
    "LiabilitiesCurrent",
    "LiabilitiesOfDisposalGroupIncludingDiscontinuedOperationCurrent",
    "AccountsPayableAndAccruedLiabilitiesCurrentAndNoncurrent",  # some REITs
    # ── Non-current liabilities ──────────────────────────────────────────────
    "LongTermDebtNoncurrent",
    "LongTermDebt",
    "OperatingLeaseLiabilityNoncurrent",
    "DeferredIncomeTaxLiabilitiesNet",
    "OtherLiabilitiesNoncurrent",
    # ── Total liabilities — multiple tags used across industries ─────────────
    "Liabilities",
    "LiabilitiesAndRedeemableNoncontrollingInterestAndEquity",  # some insurance companies
    "LiabilitiesAndStockholdersEquity",             # used when Liabilities not separately filed
    # ── Equity ──────────────────────────────────────────────────────────────
    "AdditionalPaidInCapital",
    "RetainedEarningsAccumulatedDeficit",
    "StockholdersEquity",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    "PartnersCapital",                              # MLPs and partnerships
    "MembersEquity",                                # LLCs
]

CASHFLOW_CONCEPTS = [
    # ── Operating ───────────────────────────────────────────────────────────
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",  # APD, some industrials
    "DepreciationAndAmortization",
    "DepreciationDepletionAndAmortization",         # energy/mining companies
    "ShareBasedCompensation",
    "DeferredIncomeTaxExpenseBenefit",
    "IncreaseDecreaseInAccountsReceivable",
    "IncreaseDecreaseInInventories",
    # ── Investing ───────────────────────────────────────────────────────────
    "NetCashProvidedByUsedInInvestingActivities",
    "NetCashProvidedByUsedInInvestingActivitiesContinuingOperations",  # APD
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "ProceedsFromSaleOfPropertyPlantAndEquipment",
    "PaymentsToAcquireBusinessesNetOfCashAcquired",
    # ── Financing ───────────────────────────────────────────────────────────
    "NetCashProvidedByUsedInFinancingActivities",
    "NetCashProvidedByUsedInFinancingActivitiesContinuingOperations",  # APD
    "RepaymentsOfDebt",
    "PaymentsOfDividends",
    "PaymentsForRepurchaseOfCommonStock",
    # ── Net change ──────────────────────────────────────────────────────────
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect",
    "CashAndCashEquivalentsPeriodIncreaseDecrease",  # older filers
]

ALL_CONCEPTS = {
    "income_statement": INCOME_CONCEPTS,
    "balance_sheet":    BALANCE_CONCEPTS,
    "cash_flow":        CASHFLOW_CONCEPTS,
}

LABELS = {
    "RevenueFromContractWithCustomerExcludingAssessedTax":     "Revenue",
    "RevenueFromContractWithCustomerIncludingAssessedTax":     "Revenue",
    "Revenues": "Revenue", "SalesRevenueNet": "Revenue", "NetRevenues": "Revenue",
    "CostOfGoodsAndServicesSold":   "Cost of Revenue",
    "CostOfRevenue":                "Cost of Revenue",
    "GrossProfit":                  "Gross Profit",
    "ResearchAndDevelopmentExpense":        "R&D Expense",
    "SellingAndMarketingExpense":           "Sales & Marketing",
    "SellingGeneralAndAdministrativeExpense": "SG&A Expense",
    "GeneralAndAdministrativeExpense":      "General & Administrative",
    "OperatingExpenses":                    "Total Operating Expenses",
    "NonoperatingIncomeExpense":            "Other Income (Expense), Net",
    "OtherNonoperatingIncomeExpense":       "Other Income (Expense), Net",
    "OperatingIncomeLoss":                    "Operating Income (Loss)",
    "OperatingIncomeLossFromContinuingOperations": "Operating Income (Loss)",
    "IncomeLossFromContinuingOperationsBeforeInterestExpenseInterestIncomeIncomeTaxesExtraordinaryItemsNoncontrollingInterestsNet": "Operating Income (Loss)",
    "RealEstateInvestmentTrustNetOperatingIncomeLoss": "Operating Income (Loss)",
    "InterestExpense":              "Interest Expense",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest": "Pre-Tax Income (Loss)",
    "IncomeTaxExpenseBenefit":                   "Income Tax Expense (Benefit)",
    "CurrentIncomeTaxExpenseBenefit":            "Income Tax Expense (Benefit)",
    "IncomeTaxExpenseBenefitContinuingOperations": "Income Tax Expense (Benefit)",
    "NetIncomeLoss":                                  "Net Income (Loss)",
    "NetIncomeLossAttributableToParent":              "Net Income (Loss)",
    "IncomeLossFromContinuingOperations":             "Net Income (Loss)",
    "IncomeLossFromContinuingOperationsNetOfTax":     "Net Income (Loss)",
    "IncomeLossFromContinuingOperationsNetOfTaxIncludingPortionAttributableToNoncontrollingInterest": "Net Income (Loss)",
    "NetIncomeLossAvailableToCommonStockholdersBasic":"Net Income (Loss)",
    "NetIncomeLossIncludingPortionAttributableToNonredeemableNoncontrollingInterest": "Net Income (Loss)",
    "NetIncomeLossAttributableToNonredeemableNoncontrollingInterest": "Net Income – Noncontrolling",
    "ProfitLoss":                                     "Net Income (Loss)",
    "ComprehensiveIncomeNetOfTax":                    "Net Income (Loss)",
    "OilAndGasRevenueNetOfRoyalties":                 "Revenue",
    "ExplorationAndProductionRevenue":                "Revenue",
    "RevenueFromOilAndGas":                           "Revenue",
    "RevenueAndOtherOperatingRevenue":                "Revenue",
    "RevenuesAndOtherIncome":                         "Revenue",
    "CrudeOilAndNaturalGasRevenue":                   "Revenue",
    "OilAndCondensateRevenue":                        "Revenue",
    "NaturalGasProductionRevenue":                    "Revenue",
    "NaturalGasGatheringTransportationMarketingAndProcessingRevenue": "Revenue",
    "GasGatheringTransportationMarketingAndProcessingRevenue": "Revenue",
    "ElectricAndGasUtilitiesRevenue":                 "Revenue",
    "UtilitiesOperatingRevenue":                      "Revenue",
    "OperatingLeasesIncomeStatementLeaseRevenue":     "Revenue",
    "NetInterestIncome":                              "Revenue",
    "RevenuesNetOfInterestExpense":                   "Revenue",
    "ManagementFeesRevenue":                          "Revenue",
    "InvestmentBankingRevenue":                       "Revenue",
    "BrokerageCommissionsRevenue":                    "Revenue",
    "AerospaceAndDefenseRevenue":                     "Revenue",
    "ContractWithCustomerLiabilityRevenueRecognized": "Revenue",
    "SalesRevenueGoodsNet":                           "Revenue",
    "SalesRevenueServicesNet":                        "Revenue",
    "RevenueFromContractWithCustomer":                "Revenue",
    "OilAndGasRevenue":                               "Revenue",
    "ElectricUtilityRevenue":                         "Revenue",
    "RegulatedAndUnregulatedOperatingRevenue":        "Revenue",
    "RealEstateRevenueNet":                           "Revenue",
    "HealthCareOrganizationRevenue":                  "Revenue",
    "InterestAndNoninterestIncome":                   "Revenue",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments": "Pre-Tax Income (Loss)",
    "EarningsPerShareBasic":        "EPS – Basic",
    "EarningsPerShareDiluted":      "EPS – Diluted",
    "WeightedAverageNumberOfSharesOutstandingBasic":    "Shares Outstanding – Basic (M)",
    "WeightedAverageNumberOfDilutedSharesOutstanding":  "Shares Outstanding – Diluted (M)",
    "CashAndCashEquivalentsAtCarryingValue": "Cash & Equivalents",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents": "Cash & Equivalents",
    "CashAndCashEquivalentsAtCarryingValueIncludingDiscontinuedOperations": "Cash & Equivalents",
    "LiabilitiesAndRedeemableNoncontrollingInterestAndEquity": "Total Liabilities & Equity",
    "AccountsPayableAndAccruedLiabilitiesCurrentAndNoncurrent": "Total Current Liabilities",
    "ShortTermInvestments":         "Short-Term Investments",
    "AccountsReceivableNetCurrent": "Accounts Receivable, Net",
    "InventoryNet":                 "Inventories, Net",
    "OtherAssetsCurrent":           "Other Current Assets",
    "AssetsCurrent":                "Total Current Assets",
    "PropertyPlantAndEquipmentNet": "PP&E, Net",
    "Goodwill":                     "Goodwill",
    "IntangibleAssetsNetExcludingGoodwill": "Intangible Assets, Net",
    "OperatingLeaseRightOfUseAsset": "Operating Lease ROU Assets",
    "OtherAssetsNoncurrent":        "Other Non-Current Assets",
    "Assets":                       "Total Assets",
    "AccountsPayableCurrent":       "Accounts Payable",
    "AccruedLiabilitiesCurrent":    "Accrued Liabilities",
    "LongTermDebtCurrent":          "Current Portion – LT Debt",
    "LiabilitiesCurrent":           "Total Current Liabilities",
    "LongTermDebtNoncurrent":       "Long-Term Debt",
    "LongTermDebt":                 "Long-Term Debt",
    "OperatingLeaseLiabilityNoncurrent": "Operating Lease Liability",
    "DeferredIncomeTaxLiabilitiesNet":   "Deferred Tax Liabilities",
    "OtherLiabilitiesNoncurrent":   "Other Non-Current Liabilities",
    "Liabilities":                  "Total Liabilities",
    "AdditionalPaidInCapital":      "Additional Paid-In Capital",
    "RetainedEarningsAccumulatedDeficit": "Retained Earnings (Deficit)",
    "StockholdersEquity":           "Total Stockholders' Equity",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": "Total Stockholders' Equity",
    "PartnersCapital":              "Total Equity",
    "MembersEquity":                "Total Equity",
    "LiabilitiesAndStockholdersEquity": "Total Liabilities & Equity",
    "RealEstateInvestmentPropertyNet": "Real Estate Assets, Net",
    "ReceivablesNetCurrent":        "Receivables, Net",
    "CashCashEquivalentsAndShortTermInvestments": "Cash & Short-Term Investments",
    "AssetsOfDisposalGroupIncludingDiscontinuedOperationCurrent": "Assets Held for Sale",
    "LiabilitiesOfDisposalGroupIncludingDiscontinuedOperationCurrent": "Liabilities Held for Sale",
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations": "Net Cash – Operating",
    "NetCashProvidedByUsedInInvestingActivitiesContinuingOperations": "Net Cash – Investing",
    "NetCashProvidedByUsedInFinancingActivitiesContinuingOperations": "Net Cash – Financing",
    "DepreciationDepletionAndAmortization": "Depreciation & Amortization",
    "PaymentsToAcquireBusinessesNetOfCashAcquired": "Acquisitions (net of cash)",
    "CashAndCashEquivalentsPeriodIncreaseDecrease": "Net Change in Cash",
    "NetCashProvidedByUsedInOperatingActivities": "Net Cash – Operating",
    "DepreciationAndAmortization":  "Depreciation & Amortization",
    "ShareBasedCompensation":       "Stock-Based Compensation",
    "DeferredIncomeTaxExpenseBenefit": "Deferred Income Tax",
    "IncreaseDecreaseInAccountsReceivable": "Δ Accounts Receivable",
    "IncreaseDecreaseInInventories": "Δ Inventories",
    "NetCashProvidedByUsedInInvestingActivities": "Net Cash – Investing",
    "PaymentsToAcquirePropertyPlantAndEquipment": "Capital Expenditures",
    "ProceedsFromSaleOfPropertyPlantAndEquipment": "Proceeds from Asset Sales",
    "NetCashProvidedByUsedInFinancingActivities": "Net Cash – Financing",
    "RepaymentsOfDebt":             "Debt Repayments",
    "PaymentsOfDividends":          "Dividends Paid",
    "PaymentsForRepurchaseOfCommonStock": "Share Repurchases",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect": "Net Change in Cash",
}

PER_SHARE   = {"EarningsPerShareBasic", "EarningsPerShareDiluted"}
SHARE_COUNT = {"WeightedAverageNumberOfSharesOutstandingBasic", "WeightedAverageNumberOfDilutedSharesOutstanding"}


# ── XBRL extraction ───────────────────────────────────────────────────────────

def extract_concept(gaap: dict, concept: str, filings: list[dict], form_type: str) -> dict | None:
    """
    Extract a time-series for one XBRL concept.
    Uses fuzzy ±20-day date matching to align synthetic stubs to real periods.
    Returns {report_date: scaled_value} or None.
    """
    cd = gaap.get(concept)
    if not cd:
        return None

    units   = cd.get("units", {})
    records = units.get("USD") or units.get("shares") or units.get("pure") or []
    if not records:
        return None

    # Build period_end -> (val, filed) keeping most-recently-filed value
    pm: dict[str, tuple] = {}
    for r in records:
        end   = r.get("end", "")
        start = r.get("start", "")
        val   = r.get("val")
        filed = r.get("filed", "")

        if val is None or not end:
            continue

        # Annual: keep only ~12-month duration records.
        # Window is 330-420 days to handle:
        #   - Standard fiscal years (364-366 days)
        #   - 53-week fiscal years (~371 days, common in retail/tech)
        #   - Non-Dec fiscal years with slight calendar variation
        #   - Broadcom (ends late Oct/Nov, ~371-378d)
        #   - Parker Hannifin (ends late June, ~364d)
        if form_type == "10-K" and start:
            try:
                days = (date.fromisoformat(end) - date.fromisoformat(start)).days
                if not (330 <= days <= 420):
                    continue
            except ValueError:
                pass

        if end not in pm or filed > pm[end][1]:
            pm[end] = (val, filed)

    # Fallback 1: wider window 270-430 days
    if not pm and form_type == "10-K":
        for r in records:
            end   = r.get("end", "")
            start = r.get("start", "")
            val   = r.get("val")
            filed = r.get("filed", "")
            if val is None or not end:
                continue
            if start:
                try:
                    days = (date.fromisoformat(end) - date.fromisoformat(start)).days
                    if not (270 <= days <= 430):
                        continue
                except ValueError:
                    pass
            if end not in pm or filed > pm[end][1]:
                pm[end] = (val, filed)

    # Fallback 2: any annual-ish record (180-430 days) — catches companies like APA
    # whose first full year after a corporate restructuring is slightly short,
    # and any other edge cases where normal filters reject all records.
    if not pm and form_type == "10-K":
        for r in records:
            end   = r.get("end", "")
            start = r.get("start", "")
            val   = r.get("val")
            filed = r.get("filed", "")
            if val is None or not end:
                continue
            if start:
                try:
                    days = (date.fromisoformat(end) - date.fromisoformat(start)).days
                    if not (180 <= days <= 430):
                        continue
                except ValueError:
                    pass
            if end not in pm or filed > pm[end][1]:
                pm[end] = (val, filed)

    if not pm:
        return None

    result:    dict[str, float] = {}
    used_ends: set[str]         = set()
    avail = sorted(pm.keys(), reverse=True)

    for filing in filings:
        rd = filing.get("report_date", "")
        if not rd:
            continue

        # Exact match
        if rd in pm and rd not in used_ends:
            result[rd] = _scale(pm[rd][0], concept)
            used_ends.add(rd)
            continue

        # Fuzzy match (±20 days for synthetic stubs)
        try:
            target_ts = date.fromisoformat(rd).toordinal()
        except ValueError:
            continue

        best_key, best_delta = None, 21
        for end_str in avail:
            if end_str in used_ends:
                continue
            try:
                delta = abs(date.fromisoformat(end_str).toordinal() - target_ts)
                if delta < best_delta:
                    best_delta, best_key = delta, end_str
            except ValueError:
                pass

        if best_key:
            result[rd] = _scale(pm[best_key][0], concept)
            used_ends.add(best_key)

    return result if result else None


def _scale(val: float, concept: str) -> float:
    if concept in PER_SHARE:
        return round(val, 4)
    if concept in SHARE_COUNT:
        return round(val / 1_000_000, 3)
    return round(val / 1_000_000, 1)


def _subtract_series(a: dict, b: dict) -> dict:
    """
    Subtract two period-value dicts (a - b).
    Uses fuzzy ±10-day date matching so concepts with slightly different
    period-end dates (e.g. 2024-09-28 vs 2024-09-30) still align correctly.
    """
    result = {}
    for k_a, v_a in a.items():
        if v_a is None:
            continue
        # Exact match first
        if k_a in b and b[k_a] is not None:
            result[k_a] = round(v_a - b[k_a], 1)
            continue
        # Fuzzy match within ±10 days
        try:
            ord_a = date.fromisoformat(k_a).toordinal()
        except ValueError:
            continue
        for k_b, v_b in b.items():
            if v_b is None:
                continue
            try:
                if abs(date.fromisoformat(k_b).toordinal() - ord_a) <= 10:
                    result[k_a] = round(v_a - v_b, 1)
                    break
            except ValueError:
                continue
    return result


def _derive_total_liabilities(bal_rows: list[dict]) -> dict | None:
    """
    Derive Total Liabilities as LiabilitiesAndStockholdersEquity - StockholdersEquity
    for companies that don't file Liabilities as a standalone tag.
    Uses fuzzy period matching so slight date differences don't break the subtraction.
    """
    equity_labels = ("Total Stockholders' Equity", "Total Equity")
    total_eq = next((r["values"] for r in bal_rows
                     if r["label"] in equity_labels), None)
    total_both = next((r["values"] for r in bal_rows
                       if r["label"] in ("Total Liabilities & Equity",
                                         "Total Liabilities & Stockholders Equity")), None)
    if total_eq and total_both:
        result = _subtract_series(total_both, total_eq)
        return result if result else None
    return None


def extract_statements(facts: dict, filings: list[dict], form_type: str) -> dict:
    """Extract all three financial statements. Returns dict of statement -> list of row dicts."""
    gaap   = facts.get("facts", {}).get("us-gaap", {})
    result = {}

    for stmt_name, concepts in ALL_CONCEPTS.items():
        rows = []
        seen_labels: set[str] = set()

        for concept in concepts:
            label = LABELS.get(concept, concept)
            if label in seen_labels:
                continue
            series = extract_concept(gaap, concept, filings, form_type)
            if not series:
                continue
            seen_labels.add(label)
            rows.append({"label": label, "values": series})

        # ── Derived rows (computed when direct XBRL tag is absent) ───────────

        if stmt_name == "income_statement":
            # Gross Profit = Revenue - Cost of Revenue (when GrossProfit tag absent)
            if "Gross Profit" not in seen_labels:
                rev  = next((r["values"] for r in rows if r["label"] == "Revenue"), None)
                cogs = next((r["values"] for r in rows
                             if r["label"] in ("Cost of Revenue", "Cost of Goods Sold")), None)
                if rev and cogs:
                    derived = _subtract_series(rev, cogs)
                    if derived:
                        # Insert after Cost of Revenue row
                        insert_at = next(
                            (i+1 for i, r in enumerate(rows)
                             if r["label"] in ("Cost of Revenue", "Cost of Goods Sold")),
                            len(rows)
                        )
                        rows.insert(insert_at, {
                            "label":  "Gross Profit",
                            "values": derived,
                            "_derived": True,
                        })
                        seen_labels.add("Gross Profit")

            # Operating Income = Gross Profit - Operating Expenses (when tag absent)
            if "Operating Income (Loss)" not in seen_labels:
                gp   = next((r["values"] for r in rows if r["label"] == "Gross Profit"), None)
                opex = next((r["values"] for r in rows
                             if r["label"] == "Total Operating Expenses"), None)
                if gp and opex:
                    derived = _subtract_series(gp, opex)
                    if derived:
                        rows.append({"label": "Operating Income (Loss)",
                                     "values": derived, "_derived": True})
                        seen_labels.add("Operating Income (Loss)")

        elif stmt_name == "balance_sheet":
            # Total Liabilities = Total L&E - Total Equity (when Liabilities tag absent)
            if "Total Liabilities" not in seen_labels:
                derived = _derive_total_liabilities(rows)
                if derived:
                    # Insert just before equity section
                    rows.append({"label": "Total Liabilities",
                                 "values": derived, "_derived": True})
                    seen_labels.add("Total Liabilities")

        result[stmt_name] = rows

    return result


# ── Ratio computation ─────────────────────────────────────────────────────────

def _find(rows: list[dict], *labels) -> dict | None:
    for label in labels:
        for r in rows:
            if r["label"] == label:
                return r["values"]
    return None


def _ratio(num: dict | None, den: dict | None, scale: float = 1.0) -> dict:
    if not num or not den:
        return {}
    result = {}
    for k in num:
        n, d = num.get(k), den.get(k)
        if n is not None and d is not None and d != 0:
            result[k] = round((n / d) * scale, 2)
    return result


def compute_ratios(statements: dict) -> list[dict]:
    inc = statements.get("income_statement", [])
    bal = statements.get("balance_sheet", [])

    rev    = _find(inc, "Revenue")
    gp     = _find(inc, "Gross Profit")
    opinc  = _find(inc, "Operating Income (Loss)")
    ni     = _find(inc, "Net Income (Loss)")
    intexp = _find(inc, "Interest Expense")
    assets = _find(bal, "Total Assets")
    cur_a  = _find(bal, "Total Current Assets")
    cur_l  = _find(bal, "Total Current Liabilities")
    inv    = _find(bal, "Inventories, Net")
    equity = _find(bal, "Total Stockholders' Equity")
    ltd    = _find(bal, "Long-Term Debt")

    quick_a = None
    if cur_a:
        quick_a = {k: (cur_a[k] or 0) - (inv.get(k) or 0 if inv else 0) for k in cur_a}

    abs_int = None
    if intexp:
        abs_int = {k: abs(v) for k, v in intexp.items() if v is not None}

    return [
        {"label": "Gross Margin",      "unit": "%", "values": _ratio(gp,     rev,    100)},
        {"label": "Operating Margin",  "unit": "%", "values": _ratio(opinc,  rev,    100)},
        {"label": "Net Profit Margin", "unit": "%", "values": _ratio(ni,     rev,    100)},
        {"label": "Return on Equity",  "unit": "%", "values": _ratio(ni,     equity, 100)},
        {"label": "Return on Assets",  "unit": "%", "values": _ratio(ni,     assets, 100)},
        {"label": "Current Ratio",     "unit": "x", "values": _ratio(cur_a,  cur_l)},
        {"label": "Quick Ratio",       "unit": "x", "values": _ratio(quick_a, cur_l)},
        {"label": "Debt-to-Equity",    "unit": "x", "values": _ratio(ltd,    equity)},
        {"label": "Interest Coverage", "unit": "x", "values": _ratio(opinc,  abs_int)},
        {"label": "Asset Turnover",    "unit": "x", "values": _ratio(rev,    assets)},
    ]


# ── Period label formatting ───────────────────────────────────────────────────

def period_label(date_str: str, form_type: str) -> str:
    if not date_str or len(date_str) < 7:
        return date_str
    year  = date_str[:4]
    month = int(date_str[5:7])
    if form_type == "10-K":
        return f"FY{year}"
    q = "Q1" if month <= 3 else "Q2" if month <= 6 else "Q3" if month <= 9 else "Q4"
    return f"{q}–{year}"


# ── Flask routes ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/financials")
def api_financials():
    """
    GET /api/financials?ticker=AAPL&filing=10-K&periods=5

    Returns JSON:
    {
      "ticker":     "AAPL",
      "company":    "Apple Inc.",
      "cik":        "0000320193",
      "filing":     "10-K",
      "periods":    ["2024-09-28", "2023-09-30", ...],
      "labels":     ["FY2024", "FY2023", ...],
      "statements": {
        "income_statement": [{"label": "Revenue", "values": {"2024-09-28": 391035.0, ...}}, ...],
        "balance_sheet":    [...],
        "cash_flow":        [...]
      },
      "ratios": [{"label": "Gross Margin", "unit": "%", "values": {...}}, ...]
    }
    """
    ticker    = (request.args.get("ticker") or "").strip().upper()
    form_type = (request.args.get("filing") or "10-K").upper()
    try:
        n_periods = int(request.args.get("periods", 5))
    except ValueError:
        n_periods = 5

    if not ticker:
        return jsonify({"error": "ticker parameter is required"}), 400
    if form_type not in FORM_VARIANTS:
        return jsonify({"error": "filing must be 10-K or 10-Q"}), 400

    try:
        logger.info("Request: ticker=%s filing=%s periods=%d", ticker, form_type, n_periods)

        # 1. Resolve ticker
        cik, company = resolve_ticker(ticker)

        # 2. Get filings
        filings = get_filings(cik, form_type, n_periods)

        # 3. Fetch XBRL facts
        facts = edgar_get(FACTS_URL.format(cik=cik))

        # 4. Extract statements
        statements = extract_statements(facts, filings, form_type)

        # 5. Compute ratios
        ratios = compute_ratios(statements)

        # 6. Build period metadata — deduplicate on computed label
        # (prevents duplicate FY2024 when a company files both 10-K and 10-K/A
        # for the same period, or has two filings with nearly identical dates)
        report_dates_raw = [f["report_date"] for f in filings]
        labels_raw       = [period_label(d, form_type) for d in report_dates_raw]

        # Keep only the first occurrence of each label
        seen_period_labels: set = set()
        report_dates = []
        labels       = []
        for rd, lbl in zip(report_dates_raw, labels_raw):
            if lbl not in seen_period_labels:
                seen_period_labels.add(lbl)
                report_dates.append(rd)
                labels.append(lbl)

        return jsonify({
            "ticker":     ticker,
            "company":    company,
            "cik":        cik,
            "filing":     form_type,
            "filings":    filings,
            "periods":    report_dates,
            "labels":     labels,
            "statements": statements,
            "ratios":     ratios,
        })

    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except ConnectionError as e:
        return jsonify({"error": str(e)}), 502
    except Exception as e:
        logger.exception("Unexpected error for ticker %s", ticker)
        return jsonify({"error": f"Internal error: {e}"}), 500


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n  EDGAR Financial Viewer — Flask")
    print("  ─────────────────────────────────")
    print("  Running at: http://localhost:5000")
    print("  Press Ctrl+C to stop\n")
    app.run(debug=True, port=5000)
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)