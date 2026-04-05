"""
parse_reports.py
================
ICO Coffee Market Report pipeline.
Extracts: Table 1 (prices), Table 4 (exports), Table 5 (certified stocks)
Appends to CSVs with deduplication — safe to re-run.

Usage:
    python scripts/parse_reports.py                    # last month only
    python scripts/parse_reports.py --period 2026-02   # specific month
    python scripts/parse_reports.py --from 2015-10     # full backfill
    python scripts/parse_reports.py --skip-existing    # skip already done
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

import requests
import pdfplumber
import pandas as pd
from dateutil.relativedelta import relativedelta

# ── Paths ─────────────────────────────────────────────────────────
ROOT        = Path(__file__).parent.parent
DATA_DIR    = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
PDFS_DIR    = ROOT / "pdfs"
CSV_DIR     = DATA_DIR / "csv"
INDEX_PATH  = DATA_DIR / "index.json"
LOG_PATH    = DATA_DIR / "pipeline-log.json"

CSV_PRICES  = CSV_DIR / "prices.csv"
CSV_EXPORTS = CSV_DIR / "exports.csv"
CSV_STOCKS  = CSV_DIR / "certified_stocks.csv"

CATEGORIES = ["total", "arabicas", "colombian_milds",
               "other_milds", "brazilian_naturals", "robustas"]

PRICE_COLS = ["i_cip", "colombian_milds", "other_milds",
              "brazilian_naturals", "robustas", "new_york_ice", "london_ice"]

T4_HEADERS = [
    "Table 4: Total exports from exporting countries",
    "Table 4: Total exports by exporting countries",
    "Table 4:",
]
T4_ROW_PATTERNS = [
    ("total",              r"^TOTAL\b"),
    ("arabicas",           r"^Arabicas?\b"),
    ("colombian_milds",    r"Colombian\s+Milds"),
    ("other_milds",        r"Other\s+Milds"),
    ("brazilian_naturals", r"Brazilian\s+Naturals"),
    ("robustas",           r"^Robustas?\b"),
]
T5_HEADERS = [
    "Table 5: Certified stocks on the New York and London futures markets",
    "Certified stocks on the New York and London futures markets",
    "Table 5:",
]

# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("coffee")

# ── URL Builder ───────────────────────────────────────────────────
def build_ico_url(year: int, month: int) -> str:
    cy = year - 1 if month < 10 else year
    return (f"https://www.ico.org/documents/cy{cy}-{str(cy+1)[-2:]}"
            f"/cmr-{str(month).zfill(2)}{str(year)[-2:]}-e.pdf")

def all_periods(from_year: int, from_month: int) -> list[dict]:
    periods = []
    today = date.today()
    last_month = today.replace(day=1) - relativedelta(months=1)
    y, m = from_year, from_month
    while (y, m) <= (last_month.year, last_month.month):
        periods.append({
            "period": f"{y}-{str(m).zfill(2)}",
            "year": y, "month": m,
            "url": build_ico_url(y, m),
        })
        m += 1
        if m > 12: m = 1; y += 1
    return periods

# ── PDF Download ──────────────────────────────────────────────────
def download_pdf(url: str, dest: Path, retries: int = 3) -> bytes | None:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; coffee-analytics/1.0)",
        "Accept": "application/pdf,*/*",
    }
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=45, verify=False)
            if r.status_code == 404:
                log.warning(f"  404: {url}"); return None
            r.raise_for_status()
            if not r.content.startswith(b"%PDF"):
                log.warning("  Not a PDF"); return None
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(r.content)
            log.info(f"  Downloaded {len(r.content)//1024} KB")
            return r.content
        except Exception as e:
            log.warning(f"  Attempt {attempt}/{retries}: {e}")
            if attempt < retries: time.sleep(3 * attempt)
    return None

# ── Text Extraction ───────────────────────────────────────────────
def get_full_text(pdf_path: Path) -> list[str]:
    pages = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                pages.append(page.extract_text() or "")
    except Exception as e:
        log.error(f"  pdfplumber error: {e}")
    return pages

def get_text_after(full_text: str, header: str, stop_at: str | None = None) -> str:
    words   = header.split()
    pattern = r"\s+".join(re.escape(w) for w in words)
    m = re.search(pattern, full_text, re.IGNORECASE)
    if not m: return ""
    section = full_text[m.end():]
    if stop_at:
        stop_p = r"\s+".join(re.escape(w) for w in stop_at.split())
        sm = re.search(stop_p, section, re.IGNORECASE)
        if sm: section = section[:sm.start()]
    return section

def clean_num(val) -> float | None:
    try: return float(str(val).strip().replace(",", "").replace(" ", ""))
    except: return None

def parse_month_label(label: str) -> str | None:
    for fmt in ("%b-%y", "%b-%Y", "%B-%y", "%B-%Y"):
        try:
            dt = datetime.strptime(str(label).strip(), fmt)
            if dt.year < 2000: dt = dt.replace(year=dt.year + 100)
            return dt.strftime("%Y-%m")
        except: pass
    return None

# ── Table 1: Prices ───────────────────────────────────────────────
def extract_prices(pages_text: list[str], period: str, pdf_path: Path) -> list[dict]:
    rows_out = []
    t1_pi = next(
        (pi for pi, t in enumerate(pages_text)
         if "table 1" in t.lower() or "indicator prices" in t.lower()),
        None
    )
    if t1_pi is None:
        log.warning("  Table 1: page not found"); return rows_out

    with pdfplumber.open(pdf_path) as pdf:
        tables = pdf.pages[t1_pi].extract_tables(
            table_settings={"vertical_strategy": "text",
                            "horizontal_strategy": "text"}) or []
    if not tables:
        log.warning("  Table 1: no tables found"); return rows_out

    pending_month = None
    for row in tables[0]:
        if not row: continue
        first = str(row[0] or "").strip()
        if "table 2" in first.lower() or "differential" in first.lower(): break
        month = parse_month_label(first)
        nums  = []
        for cell in row[1:]:
            for token in str(cell or "").split("\n"):
                v = clean_num(token.strip())
                if v and v > 50: nums.append(v); break
        if month and len(nums) >= 5:
            rec = {"price_month": month, "report_period": period}
            for i, col in enumerate(PRICE_COLS):
                rec[col] = nums[i] if i < len(nums) else None
            rows_out.append(rec); pending_month = None
        elif month:
            pending_month = month
        elif pending_month:
            all_nums = []
            for cell in row:
                for token in str(cell or "").split("\n"):
                    v = clean_num(token.strip())
                    if v and v > 50: all_nums.append(v); break
            if len(all_nums) >= 5:
                rec = {"price_month": pending_month, "report_period": period}
                for i, col in enumerate(PRICE_COLS):
                    rec[col] = all_nums[i] if i < len(all_nums) else None
                rows_out.append(rec)
            pending_month = None

    log.info(f"  Table 1: {len(rows_out)} months")
    return rows_out

# ── Table 4: Exports (wide format) ───────────────────────────────
def collect_all_nums_in_order(line: str) -> list[float]:
    """
    Walk tokens left-to-right collecting numbers in order.
    Handles both plain (892) and space-thousands (1 147) formats.
    This correctly separates monthly cols from cumulative YTD cols.
    """
    label_m  = re.match(r"^[A-Za-z\s]+", line.strip())
    num_part = line.strip()[label_m.end():].strip() if label_m else line.strip()

    # Comma-thousands format (newer reports)
    if re.search(r"\d{1,3},\d{3}", num_part):
        vals = []
        for t in re.findall(r"\d{1,3}(?:,\d{3})+", num_part):
            v = float(t.replace(",", ""))
            if v > 100: vals.append(v)
        if vals: return vals

    # Space-thousands format (older reports) — greedy left-to-right merge
    tokens = num_part.split()
    nums   = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if "%" in tok or tok.startswith("-"):
            i += 1; continue
        if re.match(r"^\d+$", tok):
            if (i + 1 < len(tokens)
                    and re.match(r"^\d{3}$", tokens[i + 1])
                    and "%" not in tokens[i + 1]):
                merged = float(tok + tokens[i + 1])
                if merged > 100: nums.append(merged)
                i += 2
            else:
                v = float(tok)
                if v > 100: nums.append(v)
                i += 1
        else:
            i += 1
    return nums

def extract_exports(full_text: str, period: str) -> dict:
    """Returns one wide-format row: export_month + 6 category columns."""
    y, m = int(period[:4]), int(period[5:])
    m -= 1
    if m == 0: m, y = 12, y - 1
    export_month = f"{y}-{str(m).zfill(2)}"
    row = {"export_month": export_month}
    for cat in CATEGORIES: row[cat] = None

    section = ""
    for hdr in T4_HEADERS:
        pat   = r"\s+".join(re.escape(w) for w in hdr.split())
        match = re.search(pat, full_text, re.IGNORECASE)
        if match:
            rest  = full_text[match.end():]
            stop  = re.search(r"Table\s+5", rest, re.IGNORECASE)
            section = rest[:stop.start()] if stop else rest[:2000]
            break
    if not section:
        log.warning(f"  Table 4: not found for {export_month}")
        return row

    # col index 1 = current month (second number in row)
    for line in section.split("\n"):
        s   = line.strip()
        if not s: continue
        cat = next((c for c, p in T4_ROW_PATTERNS if re.match(p, s, re.I)), None)
        if not cat: continue
        nums = collect_all_nums_in_order(s)
        if not nums: continue
        row[cat] = nums[1] if len(nums) > 1 else nums[0]

    filled = sum(1 for c in CATEGORIES if row[c] is not None)
    log.info(f"  Table 4: {filled}/6 categories for {export_month}")
    return row

# ── Table 5: Certified Stocks ─────────────────────────────────────
def extract_stocks(full_text: str, period: str) -> list[dict]:
    rows_out = []
    section  = ""
    for hdr in T5_HEADERS:
        section = get_text_after(full_text, hdr, stop_at="In million")
        if section: break
    if not section:
        log.warning("  Table 5: not found"); return rows_out

    month_order = []
    for line in section.split("\n"):
        months = re.findall(r"[A-Z][a-z]{2}-\d{2,4}", line)
        if len(months) >= 6:
            month_order = [parse_month_label(m) for m in months]
            month_order = [m for m in month_order if m]
            break
    if not month_order:
        log.warning("  Table 5: no month header"); return rows_out

    ny_nums = []; lon_nums = []; found_ny = found_lon = False
    for line in section.split("\n"):
        ls    = line.strip()
        valid = [float(n) for n in re.findall(r"\d+\.\d+", ls)
                 if 0.1 <= float(n) <= 10.0]
        is_ny  = "new york" in ls.lower() or ("york" in ls.lower() and not found_ny)
        is_lon = "london" in ls.lower()
        if   is_ny  and not found_ny  and len(valid) >= 3: ny_nums  = valid; found_ny  = True
        elif is_lon and not found_lon and len(valid) >= 3: lon_nums = valid; found_lon = True
        elif not found_ny             and len(valid) >= 6: ny_nums  = valid; found_ny  = True
        elif found_ny and not found_lon and len(valid) >= 6: lon_nums = valid; found_lon = True
        if found_ny and found_lon: break

    for i, sm in enumerate(month_order):
        ny_v  = ny_nums[i]  if i < len(ny_nums)  else None
        lon_v = lon_nums[i] if i < len(lon_nums) else None
        if ny_v is not None or lon_v is not None:
            rows_out.append({
                "report_period":          period,
                "stock_month":            sm,
                "new_york_bags_millions": ny_v,
                "london_bags_millions":   lon_v,
            })

    log.info(f"  Table 5: {len(rows_out)} months")
    return rows_out

# ── CSV Append with Dedup ─────────────────────────────────────────
def append_csv(csv_path: Path, new_rows: list[dict],
               dedup_keys: list[str], sort_key: str | None = None):
    if not new_rows: return
    new_df = pd.DataFrame(new_rows)
    if csv_path.exists() and csv_path.stat().st_size > 50:
        existing = pd.read_csv(csv_path, dtype=str)
        if len(existing) > 0:
            new_str  = new_df.astype(str)
            merged   = pd.merge(existing,
                                new_str[dedup_keys].drop_duplicates(),
                                on=dedup_keys, how="left", indicator=True)
            existing = existing[merged["_merge"] == "left_only"].copy()
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
    else:
        combined = new_df
        csv_path.parent.mkdir(parents=True, exist_ok=True)
    sk = sort_key or dedup_keys[0]
    if sk in combined.columns:
        combined = combined.sort_values(sk)
    combined.to_csv(csv_path, index=False)

# ── Index Update ──────────────────────────────────────────────────
def update_index(period: str, prices_count: int,
                 exports_filled: int, stocks_count: int):
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
        "period":          period,
        "prices_months":   prices_count,
        "exports_filled":  exports_filled,
        "stocks_months":   stocks_count,
        "processed_at":    datetime.utcnow().isoformat() + "Z",
    })
    index["reports"].sort(key=lambda r: r["period"], reverse=True)
    index["last_updated"]  = datetime.utcnow().isoformat() + "Z"
    index["total_reports"] = len(index["reports"])
    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    INDEX_PATH.write_text(json.dumps(index, indent=2))

# ── Process One Period ────────────────────────────────────────────
def process_period(period: str, year: int, month: int,
                   skip_existing: bool = False) -> bool:
    pdf_url  = build_ico_url(year, month)
    pdf_path = PDFS_DIR / f"{period}.pdf"

    log.info(f"  URL: {pdf_url}")

    if skip_existing and (CSV_PRICES.exists() and
            period in pd.read_csv(CSV_PRICES, dtype=str).get("report_period",
                                  pd.Series(dtype=str)).values):
        log.info("  Already processed — skipping"); return True

    if not pdf_path.exists():
        if download_pdf(pdf_url, pdf_path) is None:
            return False
    else:
        log.info(f"  Using cached PDF ({pdf_path.stat().st_size//1024} KB)")

    pages_text = get_full_text(pdf_path)
    if not pages_text:
        log.warning("  No text extracted"); return False

    full_text = "\n".join(pages_text)

    price_rows  = extract_prices(pages_text, period, pdf_path)
    export_row  = extract_exports(full_text, period)
    stock_rows  = extract_stocks(full_text, period)

    append_csv(CSV_PRICES,  price_rows,  ["price_month"],  "price_month")
    append_csv(CSV_EXPORTS, [export_row], ["export_month"], "export_month")
    append_csv(CSV_STOCKS,  stock_rows,  ["stock_month"],  "stock_month")

    exports_filled = sum(1 for c in CATEGORIES if export_row.get(c) is not None)
    update_index(period, len(price_rows), exports_filled, len(stock_rows))

    log.info(f"  Done — prices:{len(price_rows)} "
             f"exports:{exports_filled}/6 stocks:{len(stock_rows)}")
    return True

# ── Main ──────────────────────────────────────────────────────────
def main():
    import urllib3
    urllib3.disable_warnings()

    parser = argparse.ArgumentParser(description="ICO Coffee Market Report Pipeline")
    parser.add_argument("--from",    dest="from_period", default=None,
                        help="Start period YYYY-MM (default: last month only)")
    parser.add_argument("--period",  help="Single period YYYY-MM")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--delay",   type=float, default=2.0)
    args = parser.parse_args()

    print("☕ Coffee Analytics — ICO Report Pipeline")
    print("─" * 60)

    # Determine periods to process
    if args.period:
        if not re.match(r"^\d{4}-\d{2}$", args.period):
            print(f"❌ Invalid period: {args.period}"); sys.exit(1)
        y, m = int(args.period[:4]), int(args.period[5:])
        periods = [{"period": args.period, "year": y, "month": m,
                    "url": build_ico_url(y, m)}]
    elif args.from_period:
        fy = int(args.from_period[:4])
        fm = int(args.from_period[5:])
        periods = all_periods(fy, fm)
    else:
        # Default: last month only
        today = date.today()
        last  = today.replace(day=1) - relativedelta(months=1)
        periods = all_periods(last.year, last.month)

    print(f"  Periods: {len(periods)}  |  "
          f"{periods[0]['period']} → {periods[-1]['period']}")
    print(f"  Delay: {args.delay}s  |  Skip existing: {args.skip_existing}")
    print("─" * 60)

    if args.dry_run:
        for p in periods:
            print(f"  {p['period']} → {p['url']}")
        return

    pipeline_log = {"processed": [], "failed": [], "skipped": []}
    if LOG_PATH.exists():
        pipeline_log = json.loads(LOG_PATH.read_text())

    results    = {"ok": 0, "failed": 0, "skipped": 0}
    start_time = time.time()

    for i, p in enumerate(periods):
        period = p["period"]
        print(f"\n[{i+1}/{len(periods)}] ☕ {period}")

        try:
            ok = process_period(period, p["year"], p["month"], args.skip_existing)
            if ok:
                results["ok"] += 1
                pipeline_log["processed"].append({
                    "period": period, "at": datetime.utcnow().isoformat()
                })
                pipeline_log["failed"] = [
                    f for f in pipeline_log["failed"] if f["period"] != period
                ]
            else:
                results["failed"] += 1
                pipeline_log["failed"].append({
                    "period": period, "error": "download_or_parse_failed",
                    "at": datetime.utcnow().isoformat()
                })
        except Exception as e:
            log.error(f"  Exception: {e}")
            results["failed"] += 1
            pipeline_log["failed"].append({
                "period": period, "error": str(e),
                "at": datetime.utcnow().isoformat()
            })

        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        LOG_PATH.write_text(json.dumps(pipeline_log, indent=2))

        elapsed   = time.time() - start_time
        remaining = (elapsed / (i + 1)) * (len(periods) - i - 1)
        print(f"  ⏱  {elapsed/60:.1f}m elapsed | ~{remaining/60:.1f}m remaining")

        if i < len(periods) - 1:
            time.sleep(args.delay)

    total = time.time() - start_time
    print("\n" + "═" * 60)
    print(f"☕ Done  ✅ {results['ok']}  ❌ {results['failed']}  "
          f"⏱ {total/60:.1f}min")

if __name__ == "__main__":
    main()
