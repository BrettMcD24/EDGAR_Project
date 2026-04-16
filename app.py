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
from datetime import date, timedelta
from pathlib import Path

import requests
from flask import Flask, jsonify, render_template, request

# ── Flask setup ───────────────────────────────────────────────────────────────

app = Flask(__name__)
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
    "InventoriesFinishedGoods",                      # apparel/footwear (NKE)
    "RetailRelatedInventoryMerchandise",             # retail inventory
    "InventoryFinishedGoods",                        # alt finished goods tag
    "InventoryRawMaterialsAndSupplies",              # manufacturing raw materials
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
    "StockholdersEquity",                           # parent equity (most common)
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",  # total equity incl. NCI
    "MinorityInterest",                             # noncontrolling interest — NEE, AMT, utilities
    "RedeemableNoncontrollingInterestEquityCarryingAmount",  # redeemable NCI
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
    "InventoryNet":                                  "Inventories, Net",
    "InventoriesFinishedGoods":                      "Inventories, Net",
    "RetailRelatedInventoryMerchandise":             "Inventories, Net",
    "InventoryFinishedGoods":                        "Inventories, Net",
    "InventoryRawMaterialsAndSupplies":              "Inventories, Net",
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
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": "Total Equity",
    "MinorityInterest":             "Noncontrolling Interest",
    "RedeemableNoncontrollingInterestEquityCarryingAmount": "Redeemable Noncontrolling Interest",
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
    # EPS concepts use "USD/shares" unit type in EDGAR (not "pure" or "USD")
    records = (units.get("USD/shares") or units.get("USD") or
               units.get("shares") or units.get("pure") or [])
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
    equity_labels = ("Total Stockholders' Equity", "Total Equity",
                     "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
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
            # ── Presentation suppression rules ────────────────────────────────
            # When a parent/aggregate concept is present on the face of the
            # income statement, suppress its sub-components to avoid double-
            # counting and match the official 10-K presentation format.
            #
            # Structure: {parent_label: ([children_to_suppress], condition)}
            # condition = None means always suppress; a string means only
            # suppress when that additional label is also present (safety guard).
            SUPPRESSION_RULES = [
                # SG&A: Apple, Microsoft and most large-caps show one combined line.
                # Sub-components appear in EDGAR from footnote disclosures.
                {
                    "parent":    "SG&A Expense",
                    "suppress":  ["Sales & Marketing", "General & Administrative"],
                    "condition": None,
                },
                # Interest Expense: Microsoft, Alphabet and many tech/growth companies
                # bundle interest income, interest expense, and FX into a single
                # "Other income (expense), net" line. Interest Expense exists in
                # EDGAR as a note disclosure but not a face-of-statement line.
                # Only suppress when Pre-Tax Income is present (confirms we have
                # a complete statement using the aggregated presentation).
                {
                    "parent":    "Other Income (Expense), Net",
                    "suppress":  ["Interest Expense"],
                    "condition": "Pre-Tax Income (Loss)",
                },
                # Cost sub-components: some companies tag both the total Cost of
                # Revenue and its sub-lines (Cost of Products, Cost of Services).
                # Suppress sub-lines when the parent total is present.
                {
                    "parent":    "Cost of Revenue",
                    "suppress":  ["Cost of Products", "Cost of Services",
                                  "Cost of Goods Sold"],
                    "condition": None,
                },
                # Revenue sub-components: some companies tag both total Revenue
                # and product/service splits. Suppress splits when total present.
                {
                    "parent":    "Revenue",
                    "suppress":  ["Product Revenue", "Service Revenue",
                                  "Product Sales", "Service Revenue Net"],
                    "condition": None,
                },
            ]

            for rule in SUPPRESSION_RULES:
                if rule["parent"] not in seen_labels:
                    continue
                if rule["condition"] and rule["condition"] not in seen_labels:
                    continue
                to_remove = set(rule["suppress"]) & seen_labels
                if to_remove:
                    rows = [r for r in rows if r["label"] not in to_remove]
                    seen_labels -= to_remove
                    logger.debug("Suppressed %s (parent: %s)", to_remove, rule["parent"])

            # ── Arithmetic consistency check ──────────────────────────────────
            # Verify key accounting identities hold. Log warnings when they
            # don't — these flag double-counting or wrong-period matches.
            def _check_identity(name, lhs_label, rhs_labels, rows_list, tolerance=0.03):
                """Check lhs ≈ sum(rhs) for each period. Log if off by >tolerance."""
                lhs = next((r["values"] for r in rows_list if r["label"] == lhs_label), None)
                if not lhs:
                    return
                for period, lhs_val in lhs.items():
                    if lhs_val is None:
                        continue
                    rhs_sum = sum(
                        (next((r["values"].get(period) for r in rows_list
                               if r["label"] == rhs_lbl), None) or 0)
                        for rhs_lbl in rhs_labels
                    )
                    if rhs_sum == 0:
                        continue
                    diff = abs(lhs_val - rhs_sum) / max(abs(lhs_val), 1)
                    if diff > tolerance:
                        logger.warning(
                            "Arithmetic check FAILED: %s %s=%.1f, computed=%.1f, Δ=%.1f%%",
                            name, period, lhs_val, rhs_sum, diff * 100
                        )

            # Revenue - COGS should equal Gross Profit
            _check_identity(
                "Revenue-COGS=GP", "Gross Profit",
                [],   # can't easily check subtraction here, skip
                rows
            )

            # Gross Profit - Operating Expenses should ≈ Operating Income
            # (only check when all three are present)
            gp_vals   = next((r["values"] for r in rows if r["label"] == "Gross Profit"), None)
            opex_vals = next((r["values"] for r in rows if r["label"] == "Total Operating Expenses"), None)
            oi_vals   = next((r["values"] for r in rows if r["label"] == "Operating Income (Loss)"), None)
            if gp_vals and opex_vals and oi_vals:
                for period, gp_v in gp_vals.items():
                    opex_v = opex_vals.get(period)
                    oi_v   = oi_vals.get(period)
                    if None not in (gp_v, opex_v, oi_v) and abs(oi_v) > 0:
                        implied = gp_v - opex_v
                        diff = abs(implied - oi_v) / max(abs(oi_v), 1)
                        if diff > 0.03:
                            logger.warning(
                                "GP-OpEx≠OI for period %s: GP=%.1f OpEx=%.1f implied=%.1f actual=%.1f Δ=%.1f%%",
                                period, gp_v, opex_v, implied, oi_v, diff * 100
                            )

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
            # ── Enforce correct balance sheet row ordering ────────────────────
            # Rows are assigned to numbered sections so that sorting produces
            # the standard balance sheet layout regardless of XBRL filing order:
            #   0  Current asset components (cash, receivables, inventory, other)
            #   1  Total Current Assets  (always last in current assets)
            #   2  Non-current assets
            #   3  Total Assets
            #   4  Current liability components
            #   5  Total Current Liabilities (always last in current liabilities)
            #   6  Non-current liabilities
            #   7  Total Liabilities
            #   8  Equity components
            #   9  Total Stockholders' Equity
            #  10  Total Liabilities & Equity  (always last)
            #  99  Anything unrecognised

            SECTION_MAP = {
                "Cash & Equivalents":           0,
                "Short-Term Investments":        0,
                "Accounts Receivable, Net":      0,
                "Inventories, Net":              0,
                "Other Current Assets":          0,
                "Assets Held for Sale":          0,
                "Total Current Assets":          1,
                "PP&E, Net":                     2,
                "Real Estate Investment, Net":   2,
                "Goodwill":                      2,
                "Intangible Assets, Net":        2,
                "Operating Lease ROU Assets":    2,
                "Other Non-Current Assets":      2,
                "Total Assets":                  3,
                "Accounts Payable":              4,
                "Accrued Liabilities":           4,
                "Current Portion – LT Debt":     4,
                "Total Current Liabilities":     5,
                "Long-Term Debt":                6,
                "Operating Lease Liability":     6,
                "Deferred Tax Liabilities":      6,
                "Other Non-Current Liabilities": 6,
                "Total Liabilities":             7,
                "Additional Paid-In Capital":    8,
                "Retained Earnings (Deficit)":   8,
                "Noncontrolling Interest":        8,
                "Redeemable Noncontrolling Interest": 8,
                "Total Stockholders' Equity":    9,
                "Total Equity":                  9,
                "Total Liabilities & Equity":   10,
            }

            rows.sort(key=lambda r: SECTION_MAP.get(r["label"], 99))

            # ── Suppress double-counted Inventories row ───────────────────────
            # Some companies (e.g. APA) file InventoryNet as an XBRL concept but
            # it is actually a sub-component of "Other Current Assets" on the face
            # of the balance sheet — not a separate line item. Showing both causes
            # double-counting. Detect this by checking whether:
            #   (Cash + Receivables + Inventory + Other) > Total Current Assets
            # If the sum of components significantly exceeds TCA, inventory is
            # embedded inside Other Current Assets and should be suppressed.
            inv_row = next((r for r in rows if r["label"] == "Inventories, Net"), None)
            tca_row = next((r for r in rows if r["label"] == "Total Current Assets"), None)
            if inv_row and tca_row:
                component_labels = {"Cash & Equivalents", "Short-Term Investments",
                                    "Accounts Receivable, Net", "Inventories, Net",
                                    "Other Current Assets"}
                comp_rows = [r for r in rows if r["label"] in component_labels]
                # Check the most recent period with data
                for period_date, tca_val in tca_row["values"].items():
                    if not tca_val:
                        continue
                    comp_sum = sum(
                        r["values"].get(period_date) or 0
                        for r in comp_rows
                        if r["values"].get(period_date) is not None
                    )
                    # If components sum to more than 105% of TCA, inventory
                    # is already included in Other Current Assets — suppress it
                    if comp_sum > tca_val * 1.05:
                        logger.warning(
                            "Suppressing embedded Inventories: component sum %.0f "
                            "exceeds TCA %.0f — inventory is inside Other Current Assets",
                            comp_sum, tca_val
                        )
                        rows = [r for r in rows if r["label"] != "Inventories, Net"]
                        seen_labels.discard("Inventories, Net")
                        break
            # Only derive if not present as a direct XBRL tag
            if "Total Liabilities" not in seen_labels:
                derived = _derive_total_liabilities(rows)
                if derived:
                    rows.append({"label": "Total Liabilities",
                                 "values": derived, "_derived": True})
                    seen_labels.add("Total Liabilities")
                    # Re-sort after adding derived row
                    rows.sort(key=lambda r: SECTION_MAP.get(r["label"], 99))

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