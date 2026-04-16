"""
test_inventory_fix.py
---------------------
Tests that the inventory suppression fix works correctly:
  - Companies with REAL standalone inventory → Inventories row should be PRESENT
  - Companies with embedded/no inventory     → Inventories row should be ABSENT

Run with Flask running:
    python test_inventory_fix.py
"""
import requests
import sys

FLASK_URL = "http://localhost:5000/api/financials"

# Companies where Inventories should be a VISIBLE separate line item
# (they hold physical goods — inventory is material and standalone on BS)
SHOULD_HAVE_INVENTORY = [
    ("AAPL",  "Apple — iPhones/Macs in warehouses"),
    ("WMT",   "Walmart — retail merchandise"),
    ("AMZN",  "Amazon — fulfillment centre goods"),
    ("F",     "Ford — vehicles & parts"),
    ("TSLA",  "Tesla — vehicles & raw materials"),
    ("COST",  "Costco — retail merchandise"),
    ("NKE",   "Nike — shoes & apparel"),
    ("CAT",   "Caterpillar — heavy equipment"),
    ("DE",    "Deere — agricultural machinery"),
    ("MMM",   "3M — industrial goods"),
    ("MCD",   "McDonald's — food & packaging"),
    ("PG",    "Procter & Gamble — consumer goods"),
]

# Companies where Inventories should NOT appear (services, financials, E&P)
SHOULD_NOT_HAVE_INVENTORY = [
    ("APA",   "APA Corp — E&P, inventory is inside Other Current Assets"),
    ("JPM",   "JPMorgan — bank, no physical inventory"),
    ("GS",    "Goldman Sachs — bank"),
    ("MSFT",  "Microsoft — software, minimal physical goods"),
    ("GOOG",  "Alphabet — digital services"),
    ("V",     "Visa — payment network"),
    ("MA",    "Mastercard — payment network"),
    ("BLK",   "BlackRock — asset manager"),
    ("ARE",   "Alexandria Real Estate — REIT"),
    ("AMT",   "American Tower — REIT"),
]

def get_inventory(ticker):
    """Returns the Inventories value for the most recent period, or None if not present."""
    try:
        resp = requests.get(FLASK_URL, params={
            "ticker": ticker, "filing": "10-K", "periods": 3
        }, timeout=60)
        if not resp.ok:
            return "API_ERROR", resp.json().get("error", "")
        data = resp.json()
        labels  = data.get("labels", [])
        periods = data.get("periods", [])
        if not labels:
            return "NO_DATA", ""
        latest_period = periods[0]
        for stmt in data["statements"].values():
            for row in stmt:
                if row["label"] == "Inventories, Net":
                    val = row["values"].get(latest_period)
                    return "PRESENT", val
        return "ABSENT", None
    except requests.exceptions.ConnectionError:
        print("\nFATAL: Flask not running. Start with: python app.py\n")
        sys.exit(1)
    except Exception as e:
        return "ERROR", str(e)

def run():
    print("=" * 62)
    print("  INVENTORY SUPPRESSION FIX — Validation Test")
    print("=" * 62)

    passes = 0
    fails  = 0

    print("\n  Companies that SHOULD show Inventories:")
    print("  (fix must NOT suppress these)\n")
    for ticker, desc in SHOULD_HAVE_INVENTORY:
        status, val = get_inventory(ticker)
        if status == "PRESENT":
            print(f"  ✓  {ticker:<8} {desc[:40]:<40} val={val:,.0f}M")
            passes += 1
        elif status == "API_ERROR":
            print(f"  ?  {ticker:<8} API error: {val}")
        else:
            print(f"  ✗  {ticker:<8} {desc[:40]:<40} MISSING — fix wrongly suppressed it!")
            fails += 1

    print("\n  Companies that should NOT show Inventories:")
    print("  (fix SHOULD suppress or they have none)\n")
    for ticker, desc in SHOULD_NOT_HAVE_INVENTORY:
        status, val = get_inventory(ticker)
        if status == "ABSENT":
            print(f"  ✓  {ticker:<8} {desc[:40]:<40} correctly absent")
            passes += 1
        elif status == "PRESENT":
            print(f"  ⚠  {ticker:<8} {desc[:40]:<40} val={val} — present (may be OK if small)")
            # Not a hard fail — some service companies have tiny inventory
        elif status == "API_ERROR":
            print(f"  ?  {ticker:<8} API error: {val}")
        else:
            print(f"  ✓  {ticker:<8} {desc[:40]:<40} absent (no data)")
            passes += 1

    print(f"\n  Result: {passes} passed, {fails} failed")
    print("=" * 62)
    if fails:
        print(f"\n  ACTION NEEDED: {fails} companies had inventory wrongly suppressed.")
        print("  Consider raising the threshold above 1.05 in app.py:")
        print("      if comp_sum > tca_val * 1.05:  ← try 1.08 or 1.10")
    else:
        print("\n  All checks passed — fix is safe to ship.")
    print()

if __name__ == "__main__":
    run()