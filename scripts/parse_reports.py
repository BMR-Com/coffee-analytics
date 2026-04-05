"""
parse_reports.py
================
Complete ICO Coffee Market Report pipeline.

- Downloads ALL ICO Monthly Coffee Market Report PDFs (Oct 2014 → present)
- Extracts Tables 1, 3, 4, 5 from each PDF using pdfplumber
- Builds 4 master CSVs (append-only, no duplicates)
- Builds per-month JSON summaries
- Updates data/index.json master manifest

Usage:
    python scripts/parse_reports.py                    # full run Oct 2014 → last month
    python scripts/parse_reports.py --from 2023-01     # start from specific month
    python scripts/parse_reports.py --dry-run          # list URLs only
    python scripts/parse_reports.py --skip-existing    # skip already-processed months
    python scripts/parse_reports.py --period 2026-02   # single month only

Requirements:
    pip install pdfplumber requests pandas python-dateutil

No API key needed. Pure PDF parsing — completely free.
"""

import os
import re
import sys
import json
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

import requests
import pdfplumber
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).parent.parent
DATA_DIR    = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
PDFS_DIR    = ROOT / "pdfs"
CSV_DIR     = DATA_DIR / "csv"
INDEX_PATH  = DATA_DIR / "index.json"
LOG_PATH    = DATA_DIR / "pipeline-log.json"

CSV_PRICES        = CSV_DIR / "prices.csv"
CSV_SUPPLY_DEMAND = CSV_DIR / "supply_demand.csv"
CSV_EXPORTS       = CSV_DIR / "exports.csv"
CSV_STOCKS        = CSV_DIR / "certified_stocks.csv"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("coffee")

# ── ICO URL Builder ───────────────────────────────────────────────────────────

def get_coffee_year(year, month):
    if month >= 10:
        return year, year + 1
    else:
        return year - 1, year

def build_ico_url(year, month):
    cy_start, cy_end = get_coffee_year(year, month)
    cy_str   = f"cy{cy_start}-{str(cy_end)[-2:]}"
    mm       = str(month).zfill(2)
    yy       = str(year)[-2:]
    filename = f"cmr-{mm}{yy}-e.pdf"
    return f"https://www.ico.org/documents/{cy_str}/{filename}"

def all_periods(from_year=2014, from_month=10):
    periods = []
    today = date.today()
    last_month = today.replace(day=1) - relativedelta(months=1)
    y, m = from_year, from_month
    while (y, m) <= (last_month.year, last_month.month):
        periods.append({
            "period": f"{y}-{str(m).zfill(2)}",
            "year": y,
            "month": m,
            "url": build_ico_url(y, m),
        })
        m += 1
        if m > 12:
            m = 1
            y += 1
    return periods

# ── PDF Downloader ─────────────────────────────────────────────────────────────

def download_pdf(url, dest, retries=3):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; coffee-analytics/1.0; +https://github.com/BMR-Com/coffee-analytics)",
        "Accept": "application/pdf,*/*",
    }
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=30, stream=True)
            if resp.status_code == 404:
                log.warning(f"  404 Not Found: {url}")
                return None
            resp.raise_for_status()
            content = resp.content
            if not content.startswith(b"%PDF"):
                log.warning(f"  Response is not a PDF")
                return None
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(content)
            log.info(f"  ✅ PDF saved ({len(content)//1024} KB): {dest.name}")
            return content
        except requests.RequestException as e:
            log.warning(f"  Attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(3 * attempt)
    return None

# ── PDF Text Extractor ────────────────────────────────────────────────────────

def extract_text_pages(pdf_path):
    pages = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                pages.append(text)
    except Exception as e:
        log.error(f"  pdfplumber error: {e}")
    return pages

def find_number(text, pattern):
    m = re.search(pattern, text, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except (ValueError, IndexError):
            pass
    return None

# ── Table 1: Prices ───────────────────────────────────────────────────────────

def extract_prices(pages, period):
    full_text = "\n".join(pages[:4])
    row = {"period": period}
    found_any = False

    # I-CIP with MoM
    icip_match = re.search(
        r"(?:I-CIP|composite indicator)[^\d]*([\d.]+)\s*US\s*cents[^,]*,\s*a\s*([\d.]+)%\s*(decrease|declin|increase|grew|rose)",
        full_text, re.IGNORECASE
    )
    if icip_match:
        row["i_cip"] = float(icip_match.group(1))
        mom_val = float(icip_match.group(2))
        if "decreas" in icip_match.group(3).lower() or "declin" in icip_match.group(3).lower():
            mom_val = -mom_val
        row["i_cip_mom"] = mom_val
        found_any = True

    # Group prices
    patterns = {
        "colombian_milds":    r"Colombian Milds[^\d]*([\d.]+)\s*US\s*cents",
        "other_milds":        r"Other Milds[^\d]*([\d.]+)\s*US\s*cents",
        "brazilian_naturals": r"Brazilian Naturals[^\d]*([\d.]+)\s*US\s*cents",
        "robustas":           r"Robustas[^\d]*([\d.]+)\s*US\s*cents",
        "new_york_ice":       r"New York[^\d]*([\d.]+)\s*US\s*cents",
        "london_ice":         r"London[^\d]*([\d.]+)\s*US\s*cents",
        "arbitrage":          r"arbitrage[^\d]*([\d.]+)\s*US\s*cents",
    }
    for field, pattern in patterns.items():
        val = find_number(full_text, pattern)
        row[field] = val
        if val:
            found_any = True

    # MoM changes for groups
    mom_patterns = {
        "colombian_milds_mom":    r"Colombian Milds[^.]*?([\d.]+)%",
        "other_milds_mom":        r"Other Milds[^.]*?([\d.]+)%",
        "brazilian_naturals_mom": r"Brazilian Naturals[^.]*?([\d.]+)%",
        "robustas_mom":           r"Robustas[^.]*?([\d.]+)%",
        "new_york_ice_mom":       r"New York[^.]*?([\d.]+)%",
        "london_ice_mom":         r"London[^.]*?([\d.]+)%",
    }
    for field, pattern in mom_patterns.items():
        m = re.search(pattern, full_text, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            context = full_text[max(0, m.start()-80):m.start()]
            if re.search(r"decreas|declin|fell|drop|reduc|shrink|retract", context, re.I):
                val = -val
            row[field] = val

    return row if found_any else None

# ── Table 3: Supply/Demand ────────────────────────────────────────────────────

def extract_supply_demand(pages, period):
    rows = []
    full_text = "\n".join(pages)

    t3_match = re.search(
        r"Table 3[:\s]+(.*?)(?=Table 4|\Z)", full_text, re.DOTALL | re.IGNORECASE
    )
    if not t3_match:
        return rows

    section = t3_match.group(1)
    years_found = re.findall(r"(\d{4}/\d{2})", section)
    prod_match    = re.search(r"[Pp]roduction[^\d]*([\d.]+)", section)
    cons_match    = re.search(r"[Cc]onsumption[^\d]*([\d.]+)", section)
    surplus_match = re.search(r"(?:surplus|deficit)[^\d]*([\d.]+)", section, re.IGNORECASE)

    if years_found and (prod_match or cons_match):
        coffee_year = years_found[0]
        sd_val = None
        if surplus_match:
            sd_val = float(surplus_match.group(1))
            if re.search(r"deficit", section[:surplus_match.start()+100], re.IGNORECASE):
                sd_val = -abs(sd_val)
        rows.append({
            "report_period":     period,
            "coffee_year":       coffee_year,
            "production":        float(prod_match.group(1)) if prod_match else None,
            "total_consumption": float(cons_match.group(1)) if cons_match else None,
            "surplus_deficit":   sd_val,
            "note":              "",
        })
    return rows

# ── Table 4: Exports ──────────────────────────────────────────────────────────

def extract_exports(pages, period):
    full_text = "\n".join(pages[:6])

    y, m = int(period[:4]), int(period[5:7])
    m -= 1
    if m == 0:
        m = 12
        y -= 1
    export_month = f"{y}-{str(m).zfill(2)}"

    row = {"report_period": period, "export_month": export_month}
    found_any = False

    patterns = {
        "colombian_milds":    r"Colombian Milds[^\d]*([\d.]+)\s*million",
        "other_milds":        r"Other Milds[^\d]*([\d.]+)\s*million",
        "brazilian_naturals": r"Brazilian Naturals[^\d]*([\d.]+)\s*million",
        "robustas":           r"Robustas[^\d]*([\d.]+)\s*million",
        "total_green_beans":  r"[Gg]reen bean[s]?[^\d]*([\d.]+)\s*million",
        "soluble":            r"[Ss]oluble[^\d]*([\d.]+)\s*million",
        "roasted":            r"[Rr]oasted[^\d]*([\d.]+)\s*million",
        "total_all_forms":    r"[Tt]otal[^\d]*([\d.]+)\s*million\s*bags",
    }
    for field, pattern in patterns.items():
        val = find_number(full_text, pattern)
        row[field] = val
        if val:
            found_any = True

    region_patterns = {
        "south_america":          r"South America[^\d]*([\d.]+)\s*million",
        "asia_oceania":           r"Asia\s*[&and]+\s*Oceania[^\d]*([\d.]+)\s*million",
        "africa":                 r"Africa[^\d]*([\d.]+)\s*million",
        "mexico_central_america": r"(?:Mexico|Central America)[^\d]*([\d.]+)\s*million",
    }
    for field, pattern in region_patterns.items():
        row[field] = find_number(full_text, pattern)

    yoy = re.search(
        r"(?:total|all forms)[^%]*([\d.]+)%\s*(?:higher|lower|increase|decrease)",
        full_text, re.IGNORECASE
    )
    if yoy:
        val = float(yoy.group(1))
        if re.search(r"lower|decreas|declin|fell", yoy.group(0), re.I):
            val = -val
        row["total_all_forms_yoy_pct"] = val
    else:
        row["total_all_forms_yoy_pct"] = None

    return row if found_any else None

# ── Table 5: Certified Stocks ─────────────────────────────────────────────────

def extract_certified_stocks(pages, period):
    rows = []
    full_text = "\n".join(pages)

    t5_match = re.search(
        r"Table 5[:\s]+(.*?)(?=Explanatory|\Z)", full_text, re.DOTALL | re.IGNORECASE
    )
    section = t5_match.group(1) if t5_match else full_text

    month_labels = re.findall(r"([A-Z][a-z]{2}-\d{2})", section)
    number_rows  = re.findall(r"((?:[\d.]+\s+){3,}[\d.]+)", section)

    if month_labels and len(number_rows) >= 2:
        ny_vals  = number_rows[0].split()
        lon_vals = number_rows[1].split()
        for i, label in enumerate(month_labels):
            try:
                dt = datetime.strptime(label, "%b-%y")
                if dt.year < 2000:
                    dt = dt.replace(year=dt.year + 100)
                stock_month = dt.strftime("%Y-%m")
            except ValueError:
                stock_month = label
            rows.append({
                "report_period":          period,
                "stock_month":            stock_month,
                "new_york_bags_millions": float(ny_vals[i])  if i < len(ny_vals)  else None,
                "london_bags_millions":   float(lon_vals[i]) if i < len(lon_vals) else None,
            })
    else:
        ny  = re.search(r"(?:New York|Arabica)[^\d]*([\d.]+)\s*million", full_text, re.IGNORECASE)
        lon = re.search(r"(?:London|Robusta)[^\d]*([\d.]+)\s*million",   full_text, re.IGNORECASE)
        if ny or lon:
            rows.append({
                "report_period":          period,
                "stock_month":            period,
                "new_york_bags_millions": float(ny.group(1))  if ny  else None,
                "london_bags_millions":   float(lon.group(1)) if lon else None,
            })
    return rows

# ── JSON Summary ──────────────────────────────────────────────────────────────

def build_json_summary(period, prices, exports, sd_rows, stock_rows, pdf_url):
    mom = prices.get("i_cip_mom") if prices else None
    if mom is None:       sentiment = "neutral"
    elif mom > 2:         sentiment = "bullish"
    elif mom < -2:        sentiment = "bearish"
    else:                 sentiment = "neutral"

    drivers = []
    if prices and prices.get("i_cip_mom"):
        direction = "fell" if prices["i_cip_mom"] < 0 else "rose"
        drivers.append(f"I-CIP {direction} {abs(prices['i_cip_mom']):.1f}% MoM")
    if exports and exports.get("total_all_forms_yoy_pct"):
        direction = "up" if exports["total_all_forms_yoy_pct"] > 0 else "down"
        drivers.append(f"Global exports {direction} {abs(exports['total_all_forms_yoy_pct']):.1f}% YoY")
    if sd_rows and sd_rows[-1].get("surplus_deficit") is not None:
        val   = sd_rows[-1]["surplus_deficit"]
        label = "surplus" if val > 0 else "deficit"
        drivers.append(f"Market {label} of {abs(val):.1f}M bags ({sd_rows[-1].get('coffee_year','')})")

    return {
        "period":          period,
        "source_url":      pdf_url,
        "pdf_path":        f"pdfs/{period}.pdf",
        "summarized_at":   datetime.utcnow().isoformat() + "Z",
        "composite_price": {
            "value":          prices.get("i_cip")     if prices else None,
            "mom_change_pct": prices.get("i_cip_mom") if prices else None,
        },
        "prices_by_group": {
            "colombian_milds":    {"value": prices.get("colombian_milds"),    "mom_change_pct": prices.get("colombian_milds_mom")}    if prices else {},
            "other_milds":        {"value": prices.get("other_milds"),        "mom_change_pct": prices.get("other_milds_mom")}        if prices else {},
            "brazilian_naturals": {"value": prices.get("brazilian_naturals"), "mom_change_pct": prices.get("brazilian_naturals_mom")} if prices else {},
            "robustas":           {"value": prices.get("robustas"),           "mom_change_pct": prices.get("robustas_mom")}           if prices else {},
        },
        "futures": {
            "new_york_ice": {"value": prices.get("new_york_ice"), "mom_change_pct": prices.get("new_york_ice_mom")} if prices else {},
            "london_ice":   {"value": prices.get("london_ice"),   "mom_change_pct": prices.get("london_ice_mom")}   if prices else {},
            "arbitrage":    {"value": prices.get("arbitrage")}    if prices else {},
        },
        "exports":          exports   or {},
        "supply_demand":    sd_rows,
        "certified_stocks": stock_rows,
        "key_drivers":      drivers,
        "sentiment":        sentiment,
        "data_quality":     "complete" if (prices and exports) else ("partial" if (prices or exports) else "minimal"),
    }

# ── CSV Append ────────────────────────────────────────────────────────────────

def append_csv(csv_path, new_rows, dedup_keys):
    if not new_rows:
        return
    new_df = pd.DataFrame(new_rows)
    if csv_path.exists():
        existing = pd.read_csv(csv_path, dtype=str)
        new_str  = new_df.astype(str)
        merged   = pd.merge(existing, new_str[dedup_keys], on=dedup_keys, how="left", indicator=True)
        existing = existing[merged["_merge"] == "left_only"]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df
        csv_path.parent.mkdir(parents=True, exist_ok=True)
    if dedup_keys[0] in combined.columns:
        combined = combined.sort_values(dedup_keys[0])
    combined.to_csv(csv_path, index=False)
    log.info(f"  📊 {csv_path.name} → {len(combined)} rows")

# ── Index ─────────────────────────────────────────────────────────────────────

def update_index(period, summary):
    if INDEX_PATH.exists():
        index = json.loads(INDEX_PATH.read_text())
    else:
        index = {
            "reports": [], "last_updated": None, "total_reports": 0,
            "schema_version": "1.0",
            "data_source": "ICO Monthly Coffee Market Reports",
            "data_source_url": "https://ico.org/resources/coffee-market-report-statistics-section/",
        }
    index["reports"] = [r for r in index["reports"] if r["period"] != period]
    index["reports"].append({
        "period":         period,
        "composite_price": summary["composite_price"].get("value"),
        "mom_change_pct": summary["composite_price"].get("mom_change_pct"),
        "sentiment":      summary["sentiment"],
        "source_url":     summary["source_url"],
        "pdf_path":       summary["pdf_path"],
        "data_quality":   summary["data_quality"],
        "summarized_at":  summary["summarized_at"],
    })
    index["reports"].sort(key=lambda r: r["period"], reverse=True)
    index["last_updated"]  = datetime.utcnow().isoformat() + "Z"
    index["total_reports"] = len(index["reports"])
    INDEX_PATH.write_text(json.dumps(index, indent=2))

# ── Per-Period Processor ──────────────────────────────────────────────────────

def process_period(period, year, month, skip_existing=False):
    pdf_url   = build_ico_url(year, month)
    pdf_path  = PDFS_DIR / f"{period}.pdf"
    json_path = REPORTS_DIR / f"{period}.json"

    log.info(f"  URL: {pdf_url}")

    if skip_existing and json_path.exists():
        log.info(f"  ⏭️  Already processed")
        return True

    if pdf_path.exists():
        log.info(f"  📄 Using cached PDF")
        pdf_bytes = pdf_path.read_bytes()
    else:
        pdf_bytes = download_pdf(pdf_url, pdf_path)
        if pdf_bytes is None:
            return False

    pages = extract_text_pages(pdf_path)
    if not pages:
        log.warning(f"  ❌ No text extracted")
        return False

    prices  = extract_prices(pages, period)
    sd_rows = extract_supply_demand(pages, period)
    exports = extract_exports(pages, period)
    stocks  = extract_certified_stocks(pages, period)

    log.info(f"  Prices:{'✅' if prices and prices.get('i_cip') else '⚠️'} "
             f"S&D:{'✅' if sd_rows else '⚠️'} "
             f"Exports:{'✅' if exports else '⚠️'} "
             f"Stocks:{'✅' if stocks else '⚠️'}")

    summary = build_json_summary(period, prices, exports, sd_rows, stocks, pdf_url)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(summary, indent=2))
    log.info(f"  ✅ JSON: {json_path.name}")

    if prices:
        append_csv(CSV_PRICES, [prices], ["period"])
    if sd_rows:
        append_csv(CSV_SUPPLY_DEMAND, sd_rows, ["report_period", "coffee_year"])
    if exports:
        append_csv(CSV_EXPORTS, [exports], ["report_period"])
    if stocks:
        append_csv(CSV_STOCKS, stocks, ["report_period", "stock_month"])

    update_index(period, summary)
    return True

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ICO Coffee Market Report Pipeline")
    parser.add_argument("--from",    dest="from_period", default="2014-10")
    parser.add_argument("--period",  help="Single period YYYY-MM")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--delay",   type=float, default=2.0)
    args = parser.parse_args()

    print("☕ Coffee Analytics — ICO Report Pipeline")
    print("─" * 60)

    if args.period:
        if not re.match(r"^\d{4}-\d{2}$", args.period):
            print(f"❌ Invalid period: {args.period}")
            sys.exit(1)
        y, m = int(args.period[:4]), int(args.period[5:])
        periods = [{"period": args.period, "year": y, "month": m, "url": build_ico_url(y, m)}]
    else:
        fy = int(args.from_period[:4])
        fm = int(args.from_period[5:])
        periods = all_periods(fy, fm)

    print(f"  Periods: {len(periods)}  |  {periods[0]['period']} → {periods[-1]['period']}")
    print(f"  Delay: {args.delay}s  |  Skip existing: {args.skip_existing}")
    print("─" * 60)

    if args.dry_run:
        for p in periods:
            exists = (REPORTS_DIR / f"{p['period']}.json").exists()
            print(f"  {'✅' if exists else '⬜'} {p['period']} → {p['url']}")
        return

    pipeline_log = load_log() if LOG_PATH.exists() else {"processed": [], "failed": [], "skipped": []}
    results = {"processed": 0, "failed": 0, "skipped": 0}
    start_time = time.time()

    for i, p in enumerate(periods):
        period   = p["period"]
        progress = f"[{i+1}/{len(periods)}]"
        print(f"\n{progress} ☕ {period}")

        if args.skip_existing and (REPORTS_DIR / f"{period}.json").exists():
            print(f"  ⏭️  Skipping")
            results["skipped"] += 1
            continue

        try:
            ok = process_period(period, p["year"], p["month"], args.skip_existing)
            if ok:
                results["processed"] += 1
                pipeline_log["processed"].append({"period": period, "at": datetime.utcnow().isoformat()})
                pipeline_log["failed"] = [f for f in pipeline_log["failed"] if f["period"] != period]
            else:
                results["failed"] += 1
                pipeline_log["failed"].append({"period": period, "error": "download_failed", "at": datetime.utcnow().isoformat()})
        except Exception as e:
            log.error(f"  ❌ {e}")
            results["failed"] += 1
            pipeline_log["failed"].append({"period": period, "error": str(e), "at": datetime.utcnow().isoformat()})

        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        LOG_PATH.write_text(json.dumps(pipeline_log, indent=2))

        elapsed   = time.time() - start_time
        avg       = elapsed / (i + 1)
        remaining = (len(periods) - i - 1) * avg
        print(f"  ⏱  {elapsed/60:.1f}m elapsed | {remaining/60:.1f}m remaining")

        if i < len(periods) - 1:
            time.sleep(args.delay)

    total = time.time() - start_time
    print("\n" + "═" * 60)
    print(f"☕ Done  ✅ {results['processed']}  ⏭️  {results['skipped']}  ❌ {results['failed']}")
    print(f"⏱  Total: {total/60:.1f} minutes")
    print(f"📁 data/csv/  |  data/reports/  |  pdfs/")

def load_log():
    if LOG_PATH.exists():
        return json.loads(LOG_PATH.read_text())
    return {"processed": [], "failed": [], "skipped": []}

if __name__ == "__main__":
    main()
