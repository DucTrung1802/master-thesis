"""
Scrape a Vietnamese listed company's full quarterly financial statements from CafeF.

    python scrape_financials.py                 # default: VCB
    python scrape_financials.py --symbol FPT    # any ticker on HOSE / HNX / UPCOM / OTC

Works for **any sector** — the endpoints and sub-types below are identical for banks and
non-banks; only the line items differ, and they're read from each ticker's own template, so
the CSV columns adapt automatically (VCB starts with interest income, FPT with revenue;
VCB reports cash flow by the direct method, FPT the indirect — all handled).

Exchange is resolved from CafeF's master list (`Search/company.json`, 2,556 codes) via its
`CenterId`: 1=HOSE, 2=HNX, 8=OTC, 9=UPCOM.

Source — the ticker's `…-tai-chinh.chn` page renders each tab via
`/du-lieu/Ajax/FinancialAjax.aspx?tab=<candoi|ketqua|luuchuyen>`, which in turn calls a
JSON API on apiweb.cafef.vn. Hitting that API directly gets **all history in one request**
— no clicking the period-pager (`#table_HDKD thead th[2]`), which is just pagination:

  balance_sheet    GET api/v2/BCTC/GetReportCDKT   reportType=TN | NV
  income_statement GET api/v1/BCTC/GetReportDetail reportType=KQKD
  cash_flow        GET api/v1/BCTC/GetReportLCTT   reportType=HDKD | HDDT | HDTC
  …&symbol=<TICKER>&pageIndex=1&pageSize=<count>&TypeTime=QUY

`value.count` is the number of periods available (70 quarters / 18 years), so we read it
first and then request exactly that many — the whole history in a single call per report.

Report naming (Vietnamese → the standard accounting English used here)
  CDKT  Cân đối kế toán      → balance_sheet     sections: TN=assets, NV=liabilities_and_equity
  KQKD  Kết quả kinh doanh   → income_statement  (a.k.a. P&L / statement of profit or loss)
  LCTT  Lưu chuyển tiền tệ   → cash_flow         sections: HDKD=operating, HDDT=investing,
                                                           HDTC=financing
(`HDKD` in the xpath is the cash-flow *operating* section, not a separate report.)

Two JSON shapes are returned and both are normalised here:
  nested (CDKT, LCTT): value.templace[0].data = line items; value.data[0].data = periods
  flat   (KQKD):       value.templace         = line items; value.data         = periods

Self-contained, stdlib only.

Output — **6 generic files**, shared by every ticker (the ticker is a COLUMN, not a filename):

    balance_sheet.csv   income_statement.csv   cash_flow.csv     <- data
    balance_sheet_manual.csv  income_statement_manual.csv  cash_flow_manual.csv   <- fill by hand

Scraping a ticker **accumulates** into these files: it replaces only that symbol's rows and
leaves every other ticker's alone, so you can build a multi-ticker panel one run at a time.
Rows = (symbol, quarter), oldest first; columns = the union of all scraped tickers' line
items, named `<item_code>__<slug>`.

Line items are keyed by that full name, NOT the bare code — the same code means different
things across sectors (code `1` is interest income for a bank, revenue for a non-bank), so
they stay separate columns and a bank's row is simply blank in the non-bank columns.
"""

from __future__ import annotations

import csv
import json
import re
import sys
import time
import unicodedata
import urllib.request
from pathlib import Path

HERE = Path(__file__).parent
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
REFERER = "https://cafef.vn/du-lieu/"

# Ticker to scrape:  python scrape_financials.py --symbol FPT   (default VCB)
SYMBOL = (sys.argv[sys.argv.index("--symbol") + 1].upper()
          if "--symbol" in sys.argv else "VCB")

API = "https://apiweb.cafef.vn/api/{ver}/BCTC/{ep}"
COMPANY_URL = "https://cafefnew.mediacdn.vn/Search/company.json"   # all 2,556 listed codes
EXCHANGES = {1: "HOSE", 2: "HNX", 8: "OTC", 9: "UPCOM"}            # CafeF CenterId

REPORTS = {
    "balance_sheet": {
        "url": API.format(ver="v2", ep="GetReportCDKT"),
        "shape": "nested",
        "sections": {"TN": "assets", "NV": "liabilities_and_equity"},
    },
    "income_statement": {
        "url": API.format(ver="v1", ep="GetReportDetail"),
        "shape": "flat",
        "sections": {"KQKD": "income_statement"},
    },
    "cash_flow": {
        "url": API.format(ver="v1", ep="GetReportLCTT"),
        "shape": "nested",
        "sections": {"HDKD": "operating", "HDDT": "investing", "HDTC": "financing"},
    },
}

TYPE_TIME = "QUY"   # quarterly only — annual rows are dropped (they're derivable / duplicative)


def lookup_company(symbol: str) -> tuple[str, str]:
    """-> (exchange, company name) from CafeF's master list. Exchange comes from CenterId."""
    req = urllib.request.Request(COMPANY_URL, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r:
        companies = json.loads(r.read().decode("utf-8"))
    for c in companies:
        if str(c.get("Symbol", "")).upper() == symbol:
            return EXCHANGES.get(c.get("CenterId"), ""), c.get("Title", "")
    raise SystemExit(f"symbol {symbol!r} not found in CafeF's company list")


def _get(url: str, report_type: str, type_time: str, page_size: int) -> dict:
    q = (f"{url}?symbol={SYMBOL}&pageIndex=1&pageSize={page_size}"
         f"&reportType={report_type}&TypeTime={type_time}")
    req = urllib.request.Request(q, headers={"User-Agent": UA, "Referer": REFERER})
    with urllib.request.urlopen(req, timeout=60) as r:
        payload = json.loads(r.read().decode("utf-8"))
    time.sleep(0.2)
    if not payload.get("isSuccess"):
        raise RuntimeError(f"API error for {report_type}/{type_time}: {payload.get('errors')}")
    return payload["value"]


def _split(value: dict, shape: str) -> tuple[list, list]:
    """Return (line_items, period_blocks) for either JSON shape."""
    if shape == "nested":
        items = value["templace"][0]["data"] if value.get("templace") else []
        periods = value["data"][0]["data"] if value.get("data") else []
    else:
        items = value.get("templace") or []
        periods = value.get("data") or []
    return items, periods


def _slug(name: str, maxlen: int = 44) -> str:
    """'I. Tiền mặt, vàng bạc' -> 'tien_mat_vang_bac' (ASCII snake_case, no numbering)."""
    s = name.replace("đ", "d").replace("Đ", "D")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))          # strip diacritics
    s = re.sub(r"^\s*[IVXLC]+\.\s*|^\s*\d+\s*[.)]\s*|^\s*[a-z]\.\s*", "", s, flags=re.I)
    s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").lower()
    return s[:maxlen].rstrip("_")


def scrape_report(report: str, cfg: dict) -> tuple[list[str], dict[str, str], dict[str, dict]]:
    """-> (ordered item codes, code->column name, period -> {code: value})."""
    columns: list[str] = []                       # item codes, in statement order
    colname: dict[str, str] = {}
    rows: dict[str, dict] = {}                    # "Q1-2026" -> {code: value, ...}

    for code, section in cfg["sections"].items():
        # 1st call: how many periods exist; 2nd: pull them all at once
        count = _get(cfg["url"], code, TYPE_TIME, 1).get("count") or 0
        if not count:
            print(f"  !! no periods for {report}/{section}")
            continue
        items, periods = _split(_get(cfg["url"], code, TYPE_TIME, count), cfg["shape"])

        for it in items:                          # register columns in statement order
            c = it["code"]
            if not c:                             # UI section-header row (no code, always 0)
                continue
            if c not in colname:
                colname[c] = f"{c}__{_slug(it.get('name', ''))}"
                columns.append(c)

        for blk in periods:
            row = rows.setdefault(blk["time"], {"year": blk["year"], "quarter": blk["quater"]})
            for cell in blk.get("data", []):
                if cell["code"] in colname:
                    row[cell["code"]] = cell.get("value", "")

        print(f"  {report:<17} {section:<22} {len(items):>3} items x {len(periods):>2} periods "
              f"({periods[-1]['time']} .. {periods[0]['time']})")
    return columns, colname, rows


def _is_empty(row: dict, columns: list[str]) -> bool:
    """An all-zero/blank period = a genuine source gap (CafeF returns literal 0s)."""
    return all(str(row.get(c, "")) in ("", "0", "None") for c in columns)


def _quarter_grid(rows: dict) -> list[tuple[str, int, int]]:
    """Full contiguous (period, year, quarter) grid — the API skips quarters (e.g. Q2-2024)."""
    ys = [(r["year"], r["quarter"]) for r in rows.values()]
    lo, hi = min(ys), max(ys)
    out, y, q = [], lo[0], lo[1]
    while (y, q) <= hi:
        out.append((f"Q{q}-{y}", y, q))
        y, q = (y + 1, 1) if q == 4 else (y, q + 1)
    return out


DATA_COLS = ["symbol", "exchange", "period", "year", "quarter", "source"]
TMPL_COLS = ["symbol", "period", "year", "quarter"]


def _read_csv(path: Path) -> tuple[list[str], list[dict]]:
    """-> (header, rows). Existing tickers in the shared file are preserved."""
    if not path.exists():
        return [], []
    with path.open(encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        return list(r.fieldnames or []), list(r)


def _merge_head(old_head: list[str], index: list[str], new_items: list[str]) -> list[str]:
    """Union of line-item columns, existing order first, new ones appended.

    Line items are keyed by the full `<code>__<slug>` name, NOT the bare code: the same
    code means different things across sectors (code `1` = interest income for a bank,
    revenue for a non-bank), so they must stay separate columns.
    """
    items = [c for c in old_head if c not in index]
    for c in new_items:
        if c not in items:
            items.append(c)
    return index + items


def _sort_key(r: dict) -> tuple:
    return (r["symbol"], int(r["year"] or 0), int(r["quarter"] or 0))


def _write(path: Path, head: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(head)
        for r in rows:
            w.writerow([r.get(c, "") for c in head])


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    exchange, name = lookup_company(SYMBOL)
    print(f"scraping {SYMBOL} ({exchange}) — {name}\n")

    for report, cfg in REPORTS.items():
        codes, colname, scraped = scrape_report(report, cfg)
        if not scraped:
            print(f"  !! {report}: no data for {SYMBOL}, skipped\n")
            continue
        items = [colname[c] for c in codes]              # column names for THIS ticker
        grid = _quarter_grid(scraped)

        data_path = HERE / f"{report}.csv"
        tmpl_path = HERE / f"{report}_manual.csv"
        old_data_head, old_data = _read_csv(data_path)
        old_tmpl_head, old_tmpl = _read_csv(tmpl_path)

        # hand-filled values for THIS ticker, keyed by period (blanks ignored)
        manual = {r["period"]: {k: v.strip() for k, v in r.items()
                                if k not in TMPL_COLS and (v or "").strip()}
                  for r in old_tmpl if r.get("symbol") == SYMBOL}
        manual = {p: v for p, v in manual.items() if v}

        new_data, new_tmpl, missing, n_manual = [], [], [], 0
        for period, y, q in grid:
            src = scraped.get(period)
            ok = src is not None and not _is_empty(src, codes)
            row = {colname[c]: v for c, v in (src or {}).items() if c in colname} if ok else {}
            filled = manual.get(period, {})
            row.update(filled)                            # MANUAL WINS over scraped
            n_manual += bool(filled)

            if not ok:
                # every scrape gap stays in the template — filled or not — so hand-entered
                # rows are never dropped on a re-run
                new_tmpl.append({"symbol": SYMBOL, "period": period, "year": y,
                                 "quarter": q, **filled})
                if not filled:
                    missing.append(period)
            new_data.append({"symbol": SYMBOL, "exchange": exchange, "period": period,
                             "year": y, "quarter": q,
                             "source": "manual" if filled else ("scraped" if ok else "missing"),
                             **row})

        # accumulate: replace only THIS ticker's rows, keep every other ticker's
        data_rows = [r for r in old_data if r.get("symbol") != SYMBOL] + new_data
        tmpl_rows = [r for r in old_tmpl if r.get("symbol") != SYMBOL] + new_tmpl
        data_rows.sort(key=_sort_key)
        tmpl_rows.sort(key=_sort_key)

        _write(data_path, _merge_head(old_data_head, DATA_COLS, items), data_rows)
        _write(tmpl_path, _merge_head(old_tmpl_head, TMPL_COLS, items), tmpl_rows)

        others = sorted({r["symbol"] for r in data_rows} - {SYMBOL})
        print(f"  -> {data_path.name}: {SYMBOL} {len(new_data)} quarters x {len(items)} items "
              f"({n_manual} manual, {len(missing)} missing)"
              + (f"; also holds {', '.join(others)}" if others else ""))
        print(f"  -> {tmpl_path.name}: {len(new_tmpl)} gap rows for {SYMBOL}, "
              f"{len(missing)} to fill [{', '.join(missing[:5])}{' …' if len(missing) > 5 else ''}]\n")


if __name__ == "__main__":
    main()
