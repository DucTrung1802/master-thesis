"""
Quarterly financial statements for any Vietnamese listed company — one script.

    python financials.py                              # scrape VCB (default)
    python financials.py scrape --symbol FPT          # any code on HOSE / HNX / UPCOM / OTC
    python financials.py pdf --period Q2-2014         # read the filing into the pdf layer
    python financials.py pdf --period Q2-2014 --render   # rasterise a SCANNED filing's pages
    python financials.py docs --period Q2-2014        # list the filings CafeF has

Six generic files — the ticker is a COLUMN, not a filename, so scraping another ticker
ACCUMULATES (it replaces only that symbol's rows and is idempotent):

    balance_sheet.csv   income_statement.csv   cash_flow.csv          <- deliverables
    balance_sheet_manual.csv  income_statement_manual.csv  cash_flow_manual.csv
                                                                      <- FILL THESE BY HAND
(+ a `<report>_pdf.csv` layer written by the `pdf` command.)

Three layers, merged cell by cell — **manual > pdf > scraped** — and the `source` column
records which one won (`scraped` / `pdf` / `manual` / `missing`).

THE MANUAL TEMPLATES
--------------------
Every quarter CafeF cannot supply gets a pre-seeded row in `<report>_manual.csv`, keyed by
`symbol` + `period`, with the same line-item columns as the deliverable and blank values.
Fill any cells you can source from the actual filing, re-run `scrape`, and they win over
everything. Gap rows persist across re-runs, so a hand-entered value is never dropped.

WHERE THE DATA COMES FROM
-------------------------
The ticker's `…-tai-chinh.chn` page renders each tab via `Ajax/FinancialAjax.aspx`, which
calls a JSON API. Hitting it directly returns the whole history in one request — the
period-pager (`#table_HDKD thead th[2]`) is just pagination:

    balance_sheet    GET api/v2/BCTC/GetReportCDKT    reportType=TN | NV
    income_statement GET api/v1/BCTC/GetReportDetail  reportType=KQKD
    cash_flow        GET api/v1/BCTC/GetReportLCTT    reportType=HDKD | HDDT | HDTC
    …&symbol=<TICKER>&pageIndex=1&pageSize=<count>&TypeTime=QUY

`value.count` = the quarters available, so we read it then ask for exactly that many. Two
JSON shapes come back and both are normalised: nested (CDKT/LCTT) puts the line items at
`templace[0].data` and the periods at `data[0].data`; flat (KQKD) has them at the top level.

Sector-agnostic: banks and non-banks use the same endpoints and differ only in their line
items, which are read from each ticker's own template — so the columns adapt themselves.
Line items are keyed by the full `<code>__<slug>` NAME, never the bare code: code `1` is
interest income for a bank but revenue for a non-bank, so they must stay separate columns.

⚠️  THREE TRAPS A RECONCILIATION CHECK CANNOT CATCH — all three still balance internally:
  1. CUMULATIVE vs STANDALONE. A Q2/Q3/Q4 report prints both the quarter and the
     year-to-date column. The income statement needs the STANDALONE quarter; cash flow
     needs the CUMULATIVE one (that is CafeF's convention, so a Q4 report's full-year cash
     flow *is* the Q4 value). A semi-annual filing may print ONLY 6-month figures — then the
     standalone quarter must be derived as 6M − Q1.
  2. UNITS. Most filings are in Triệu VNĐ (×1e6); some (VCB's 2009 and Q1-2010 ones) are in
     plain đồng. A 10^6 error reconciles perfectly.
  3. SIGNS. CafeF stores income-statement expenses as POSITIVE magnitudes while the filing
     prints them in parentheses. (Balance sheet and cash flow keep the filing's signs.)
Only a MAGNITUDE CHECK against the neighbouring quarters catches (1) and (2); `pdf` runs one
before writing and normalises (3). Sanity-check any hand-fill the same way.

Stdlib only, except PyMuPDF (`pip install pymupdf`) for the `pdf` command.
"""

from __future__ import annotations

import argparse
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
MILLION = 1_000_000

API = "https://apiweb.cafef.vn/api/{ver}/BCTC/{ep}"
COMPANY_URL = "https://cafefnew.mediacdn.vn/Search/company.json"   # all 2,556 listed codes
DOCS_URL = "https://cafef.vn/du-lieu/Ajax/PageNew/FileBCTC.ashx?Symbol={sym}&Type=1&Year=0"
EXCHANGES = {1: "HOSE", 2: "HNX", 8: "OTC", 9: "UPCOM"}            # CafeF CenterId
TYPE_TIME = "QUY"          # quarterly only — annual is derivable and duplicative

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

DATA_COLS = ["symbol", "exchange", "period", "year", "quarter", "source"]
IDX_COLS = ["symbol", "period", "year", "quarter"]          # manual + pdf layers


# ═══════════════════════════════════════════════════════════════════ shared helpers
def norm(s: str) -> str:
    s = s.replace("đ", "d").replace("Đ", "D")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^A-Za-z0-9]+", " ", s).strip().lower()
    return re.sub(r"\s+", " ", s)


def slug(name: str, maxlen: int = 44) -> str:
    """'I. Tiền mặt, vàng bạc' -> 'tien_mat_vang_bac' (ASCII snake_case, numbering stripped)."""
    s = name.replace("đ", "d").replace("Đ", "D")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"^\s*[IVXLC]+\.\s*|^\s*\d+\s*[.)]\s*|^\s*[a-z]\.\s*", "", s, flags=re.I)
    s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").lower()
    return s[:maxlen].rstrip("_")


def read_csv(path: Path) -> tuple[list[str], list[dict]]:
    if not path.exists():
        return [], []
    with path.open(encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        return list(r.fieldnames or []), list(r)


def write_csv(path: Path, head: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(head)
        for r in rows:
            w.writerow([r.get(c, "") for c in head])


def merge_head(old: list[str], index: list[str], new_items: list[str]) -> list[str]:
    """Union of line-item columns: existing order first, new ones appended."""
    items = [c for c in old if c not in index]
    for c in new_items:
        if c not in items:
            items.append(c)
    return index + items


def sort_key(r: dict) -> tuple:
    return (r["symbol"], int(r["year"] or 0), int(r["quarter"] or 0))


def columns_of(report: str) -> list[str]:
    head, _ = read_csv(HERE / f"{report}.csv")
    return [c for c in head if c not in DATA_COLS]


def lookup_company(symbol: str) -> tuple[str, str]:
    """-> (exchange, name) from CafeF's master list; exchange comes from CenterId."""
    req = urllib.request.Request(COMPANY_URL, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r:
        for c in json.loads(r.read().decode("utf-8")):
            if str(c.get("Symbol", "")).upper() == symbol:
                return EXCHANGES.get(c.get("CenterId"), ""), c.get("Title", "")
    raise SystemExit(f"symbol {symbol!r} not found in CafeF's company list")


# ═══════════════════════════════════════════════════════════════════ 1. scrape (the API)
def api(url: str, symbol: str, report_type: str, page_size: int) -> dict:
    q = (f"{url}?symbol={symbol}&pageIndex=1&pageSize={page_size}"
         f"&reportType={report_type}&TypeTime={TYPE_TIME}")
    req = urllib.request.Request(q, headers={"User-Agent": UA, "Referer": REFERER})
    with urllib.request.urlopen(req, timeout=60) as r:
        payload = json.loads(r.read().decode("utf-8"))
    time.sleep(0.2)
    if not payload.get("isSuccess"):
        raise RuntimeError(f"API error for {report_type}: {payload.get('errors')}")
    return payload["value"]


def split_shape(value: dict, shape: str) -> tuple[list, list]:
    """(line items, period blocks) for either JSON shape."""
    if shape == "nested":
        items = value["templace"][0]["data"] if value.get("templace") else []
        periods = value["data"][0]["data"] if value.get("data") else []
    else:
        items = value.get("templace") or []
        periods = value.get("data") or []
    return items, periods


def scrape_report(symbol: str, report: str, cfg: dict):
    """-> (item codes in statement order, code->column name, period -> {code: value})."""
    codes: list[str] = []
    colname: dict[str, str] = {}
    rows: dict[str, dict] = {}

    for code, section in cfg["sections"].items():
        count = api(cfg["url"], symbol, code, 1).get("count") or 0   # how many quarters exist
        if not count:
            print(f"  !! no periods for {report}/{section}")
            continue
        items, periods = split_shape(api(cfg["url"], symbol, code, count), cfg["shape"])

        for it in items:
            c = it["code"]
            if not c:                       # UI section-header row (no code, always 0)
                continue
            if c not in colname:
                colname[c] = f"{c}__{slug(it.get('name', ''))}"
                codes.append(c)

        for blk in periods:
            row = rows.setdefault(blk["time"], {"year": blk["year"], "quarter": blk["quater"]})
            for cell in blk.get("data", []):
                if cell["code"] in colname:
                    row[cell["code"]] = cell.get("value", "")

        print(f"  {report:<17} {section:<22} {len(items):>3} items x {len(periods):>2} periods "
              f"({periods[-1]['time']} .. {periods[0]['time']})")
    return codes, colname, rows


def is_empty(row: dict, codes: list[str]) -> bool:
    """All-zero/blank = a genuine source gap — CafeF returns literal 0s, never a null."""
    return all(str(row.get(c, "")) in ("", "0", "None") for c in codes)


def quarter_grid(rows: dict) -> list[tuple[str, int, int]]:
    """Contiguous (period, year, quarter) grid — the API SKIPS quarters (e.g. Q2-2024), so the
    grid is rebuilt rather than taken from the response, and a gap becomes a `missing` row."""
    ys = [(r["year"], r["quarter"]) for r in rows.values()]
    lo, hi = min(ys), max(ys)
    out, y, q = [], lo[0], lo[1]
    while (y, q) <= hi:
        out.append((f"Q{q}-{y}", y, q))
        y, q = (y + 1, 1) if q == 4 else (y, q + 1)
    return out


def cmd_scrape(symbol: str) -> None:
    exchange, name = lookup_company(symbol)
    print(f"scraping {symbol} ({exchange}) — {name}\n")

    for report, cfg in REPORTS.items():
        codes, colname, scraped = scrape_report(symbol, report, cfg)
        if not scraped:
            print(f"  !! {report}: no data for {symbol}, skipped\n")
            continue
        items = [colname[c] for c in codes]
        grid = quarter_grid(scraped)

        data_path = HERE / f"{report}.csv"
        tmpl_path = HERE / f"{report}_manual.csv"
        old_data_head, old_data = read_csv(data_path)
        old_tmpl_head, old_tmpl = read_csv(tmpl_path)
        _, old_pdf = read_csv(HERE / f"{report}_pdf.csv")

        def layer(rows: list[dict]) -> dict[str, dict]:
            out = {r["period"]: {k: v.strip() for k, v in r.items()
                                 if k not in IDX_COLS and (v or "").strip()}
                   for r in rows if r.get("symbol") == symbol}
            return {p: v for p, v in out.items() if v}

        manual, from_pdf = layer(old_tmpl), layer(old_pdf)     # manual > pdf > scraped

        new_data, new_tmpl, missing = [], [], []
        n = {"manual": 0, "pdf": 0}
        for period, y, q in grid:
            src = scraped.get(period)
            ok = src is not None and not is_empty(src, codes)
            row = {colname[c]: v for c, v in (src or {}).items() if c in colname} if ok else {}

            pdf_p, man_p = from_pdf.get(period, {}), manual.get(period, {})
            row.update(pdf_p)                                  # pdf beats scraped
            row.update(man_p)                                  # manual beats both
            n["pdf"] += bool(pdf_p)
            n["manual"] += bool(man_p)

            source = ("manual" if man_p else "pdf" if pdf_p
                      else "scraped" if ok else "missing")
            if not ok and not pdf_p:
                # a scrape gap stays in the template forever — filled or not — so a
                # hand-entered row is never dropped on a re-run
                new_tmpl.append({"symbol": symbol, "period": period, "year": y,
                                 "quarter": q, **man_p})
                if not man_p:
                    missing.append(period)
            new_data.append({"symbol": symbol, "exchange": exchange, "period": period,
                             "year": y, "quarter": q, "source": source, **row})

        # accumulate: replace only THIS ticker's rows, keep every other ticker's
        data_rows = sorted([r for r in old_data if r.get("symbol") != symbol] + new_data,
                           key=sort_key)
        tmpl_rows = sorted([r for r in old_tmpl if r.get("symbol") != symbol] + new_tmpl,
                           key=sort_key)
        write_csv(data_path, merge_head(old_data_head, DATA_COLS, items), data_rows)
        write_csv(tmpl_path, merge_head(old_tmpl_head, IDX_COLS, items), tmpl_rows)

        others = sorted({r["symbol"] for r in data_rows} - {symbol})
        print(f"  -> {data_path.name}: {symbol} {len(new_data)} quarters x {len(items)} items "
              f"({n['manual']} manual, {n['pdf']} pdf, {len(missing)} missing)"
              + (f"; also holds {', '.join(others)}" if others else ""))
        print(f"  -> {tmpl_path.name}: {len(new_tmpl)} gap rows for {symbol}, "
              f"{len(missing)} to fill "
              f"[{', '.join(missing[:5])}{' …' if len(missing) > 5 else ''}]\n")


# ═══════════════════════════════════════════════════════════════════ 2. pdf (the filings)
PDF_CACHE = HERE / "pdf_cache"
PAGES_DIR = HERE / "pdf_pages"
Y_TOL = 3.0

HEADING = {
    "balance_sheet": r"B[ảa]ng c[âa]n [đd][ốo]i k[ếe] to[áa]n|B[áa]o c[áa]o t[ìi]nh h[ìi]nh t[àa]i ch[íi]nh",
    "income_statement": r"k[ếe]t qu[ảa] ho[ạa]t [đd][ộo]ng kinh doanh",
    "cash_flow": r"l[ưu]u chuy[ểe]n ti[ềe]n t[ệe]",
}
NOTES_PAGE = r"Thuy[ếe]t minh b[áa]o c[áa]o"

# Income-statement lines CafeF stores as a POSITIVE magnitude even though the filing prints
# them in parentheses. Getting this wrong makes the row sign-inconsistent with every scraped
# quarter — which is exactly what happened on an early pass.
IS_EXPENSES = {"2__chi_phi_lai_va_cac_chi_phi_tuong_tu", "5__chi_phi_hoat_dong_dich_vu",
               "11__chi_phi_hoat_dong_khac", "14__chi_phi_hoat_dong",
               "16__chi_phi_du_phong_rui_ro_tin_dung", "18__chi_phi_thue_tndn_hien_hanh",
               "20__chi_phi_thue_tndn", "22__loi_ich_cua_co_dong_thieu_so",
               "25__chi_phi_hoat_dong_khac"}

# Labels that wrap across lines: the text layer only keeps the tail ("…thu nhập lãi và các
# khoản | thu nhập tương tự" -> "thu nhap tuong tu"). Accept those fragments explicitly rather
# than loosening the matcher — fuzzy matching once put "Chi phí hoạt động khác" into
# "Chi phí hoạt động".
ALIASES: dict[str, list[str]] = {
    "1__thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu": ["thu nhap tuong tu"],
    "2__chi_phi_lai_va_cac_chi_phi_tuong_tu": ["khoan chi phi tuong tu", "chi phi tuong tu"],
    "5__chi_phi_hoat_dong_dich_vu": ["chi phi tu hoat dong dich vu"],
    "6__lai_lo_thuan_tu_hoat_dong_dich_vu": ["lai thuan tu hoat dong dich vu"],
    "7__lai_lo_thuan_tu_hoat_dong_kinh_doanh_ngoai_h": ["lai thuan tu hoat dong kinh doanh ngoai hoi"],
    "8__lai_lo_thuan_tu_mua_ban_chung_khoan_kinh_doa": ["lai thuan tu mua ban chung khoan kinh doanh"],
    "12__lai_lo_thuan_tu_hoat_dong_khac": ["lai thuan tu hoat dong khac"],
    "24__tong_thu_nhap_hoat_dong": ["tong thu nhap hoat dong"],
    "26__loi_nhuan_sau_thue_cua_co_dong_cua_ngan_hang": ["loi nhuan thuan trong ky"],
    "23__lai_co_ban_tren_co_phieu_dong_1_co_phieu": ["lai co ban tren co phieu"],
}

_NUM = re.compile(r"^\(?-?[\d][\d.,]*\)?$|^[-–—]$")


def parse_num(t: str):
    t = t.strip()
    if t in ("-", "–", "—"):
        return 0
    neg = t.startswith("(") and t.endswith(")")
    t = t.strip("()")
    if not re.fullmatch(r"-?[\d.,]+", t):
        return None
    d = re.sub(r"[.,]", "", t)
    if not d.lstrip("-").isdigit() or not d.strip("-"):
        return None
    return -int(d) if neg else int(d)


def label_key(lab: str) -> str:
    k, prev = norm(lab), None
    while prev != k:                       # strip stacked prefixes: "VIII", "1.", "a)"
        prev = k
        k = re.sub(r"^(?:[ivxlc]+|\d+|[a-z])\s+", "", k)
    return k.strip()


def documents(symbol: str) -> list[dict]:
    req = urllib.request.Request(
        DOCS_URL.format(sym=symbol.lower()),
        headers={"User-Agent": UA,
                 "Referer": f"https://cafef.vn/du-lieu/hose/{symbol.lower()}-tai-lieu.chn"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))["Data"]


def open_report(symbol: str, period: str):
    """The consolidated ('hợp nhất') filing for a quarter; reviewed/audited preferred."""
    import fitz
    q, year = int(period[1]), int(period.split("-")[1])
    cands = [d for d in documents(symbol)
             if d["Year"] == year and d["Quarter"] == q and "hợp nhất" in d["Name"].lower()]
    if not cands:
        return None, None
    cands.sort(key=lambda d: ("soát xét" in d["Name"].lower()
                              or "kiểm toán" in d["Name"].lower()), reverse=True)
    PDF_CACHE.mkdir(exist_ok=True)
    dest = PDF_CACHE / f"{symbol}_{period}.pdf"
    for c in cands:
        # the API advertises cafefnew.mediacdn.vn but older files live on cafef1
        for url in (c["Link"], c["Link"].replace("cafefnew.mediacdn.vn", "cafef1.mediacdn.vn")):
            try:
                if not dest.exists():
                    req = urllib.request.Request(url, headers={"User-Agent": UA})
                    with urllib.request.urlopen(req, timeout=240) as r:
                        dest.write_bytes(r.read())
                return fitz.open(dest), c["Name"]
            except Exception:
                continue
    return None, None


def statement_pages(doc, report: str) -> list[int]:
    head, note = re.compile(HEADING[report], re.I), re.compile(NOTES_PAGE, re.I)
    return [i for i, p in enumerate(doc)
            if head.search(p.get_text()) and not note.search(p.get_text())]


def is_scanned(doc) -> bool:
    return sum(len(p.get_text().strip()) for p in doc) < 3000


def _clusters(centres: list[float]) -> list[list[float]]:
    out: list[list[float]] = []
    for c in centres:
        if out and c - out[-1][-1] <= 18:
            out[-1].append(c)
        else:
            out.append([c])
    return out


def n_value_columns(doc, pages) -> int:
    """How many period columns — derived from the DATA, not from counting unit labels.

    Counting the repeated "Triệu VNĐ" header is unreliable: some filings print the unit once
    for a 4-column table, and guessing 2 there silently reads the CUMULATIVE column.
    """
    nums = [w for p in pages for w in doc[p].get_text("words")
            if _NUM.match(w[4]) and parse_num(w[4]) is not None]
    if not nums:
        return 2
    cl = _clusters(sorted((w[0] + w[2]) / 2 for w in nums))
    biggest = max(len(c) for c in cl)
    dense = [c for c in cl if len(c) >= 0.45 * biggest]
    return len(dense) if 2 <= len(dense) <= 5 else 2


def table_rows(doc, pages, ncol: int) -> list[tuple[str, list[int]]]:
    """Rebuild rows from WORD COORDINATES, not the raw text stream.

    PyMuPDF emits label fragments out of order — a line-based read silently mis-assigns
    values. The period columns are found by clustering the x-positions of every number and
    keeping the densest N; that is what separates a real value from the row's item number on
    the far left and the "Thuyết minh" note reference in the middle. Clustering is done ONCE
    across the whole statement, because per-page it drifts on a continuation page and the
    label column then falls on the wrong side of the threshold.
    """
    nums = [w for p in pages for w in doc[p].get_text("words")
            if _NUM.match(w[4]) and parse_num(w[4]) is not None]
    if not nums:
        return []
    clusters = _clusters(sorted((w[0] + w[2]) / 2 for w in nums))
    if len(clusters) < ncol:
        return []
    keep = sorted(sorted(clusters, key=len, reverse=True)[:ncol], key=lambda c: c[0])
    lo = min(keep[0]) - 25                  # left of this = label / item no. / note ref

    out = []
    for p in pages:
        lines: dict[float, list] = {}
        for w in doc[p].get_text("words"):
            k = next((k for k in lines if abs(k - w[1]) <= Y_TOL), w[1])
            lines.setdefault(k, []).append(w)
        carry: list[str] = []               # a label that wrapped onto its own line
        for y in sorted(lines):
            label, vals = [], []
            for w in sorted(lines[y], key=lambda w: w[0]):
                cx = (w[0] + w[2]) / 2
                v = parse_num(w[4]) if _NUM.match(w[4]) else None
                if v is not None and cx >= lo:
                    vals.append(v)
                elif cx < lo and not _NUM.match(w[4]):
                    label.append(w[4])
            if vals:
                out.append((" ".join(carry + label), vals[-ncol:]))
                carry = []
            elif label:
                carry = (carry + label)[-12:]
            else:
                carry = []
    return out


def unit_scale(doc, pages) -> int:
    """×1e6 if the statement is in 'Triệu VNĐ', else ×1 (plain đồng)."""
    txt = "\n".join(doc[p].get_text() for p in pages[:1])
    return MILLION if re.search(r"Tri[ệe]u\s*(?:VN[ĐD]|[đd][ồo]ng)", txt, re.I) else 1


def to_cafef_signs(report: str, d: dict[str, int]) -> dict[str, int]:
    """Normalise to CafeF's convention: income-statement expenses are stored POSITIVE.

    The deferred-tax line is DERIVED rather than parsed — the filing labels it
    "Thu nhập/(chi phí)", so its printed sign flips depending on which it is, while CafeF
    always keeps `20 = 18 + 19`. Balance sheet and cash flow keep the filing's signs.
    """
    if report != "income_statement":
        return d
    out = {k: (abs(v) if k in IS_EXPENSES else v) for k, v in d.items()}
    cur, tot = out.get("18__chi_phi_thue_tndn_hien_hanh"), out.get("20__chi_phi_thue_tndn")
    if cur is not None and tot is not None:
        out["19__chi_phi_thue_tndn_hoan_lai"] = tot - cur
    return out


def reconcile(report: str, d: dict) -> str | None:
    """None if the statement balances against its own printed subtotals, else why not."""
    g = d.get
    if report == "balance_sheet":
        tot, tot2 = g("300__tong_tai_san"), g("800__tong_no_phai_tra_va_von_chu_so_huu")
        if tot is None or tot2 is None:
            return "missing a total"
        if tot != tot2:
            return "assets != liabilities + equity"
        liab, eq = g("400__tong_no_phai_tra"), g("500__viii_von_chu_so_huu")
        minority = g("700__loi_ich_cua_co_dong_thieu_so") or 0
        # Both conventions are in use — the older one keeps minority interest OUTSIDE equity
        # (400+500+700 = 800), TT49/2014 folds it INSIDE (400+500 = 800). CafeF itself
        # switched mid-2024, so accept either rather than reject a good filing.
        if None not in (liab, eq) and tot2 not in (liab + eq, liab + eq + minority):
            return "liabilities + equity (+/- minority) != total"
    if report == "income_statement":
        # values are already in CafeF signs (expenses positive) -> every identity subtracts
        for out, a, b in [
            ("3__thu_nhap_lai_thuan", "1__thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu",
             "2__chi_phi_lai_va_cac_chi_phi_tuong_tu"),
            ("6__lai_lo_thuan_tu_hoat_dong_dich_vu", "4__thu_nhap_tu_hoat_dong_dich_vu",
             "5__chi_phi_hoat_dong_dich_vu"),
            ("12__lai_lo_thuan_tu_hoat_dong_khac", "10__thu_nhap_tu_hoat_dong_khac",
             "11__chi_phi_hoat_dong_khac"),
            ("17__tong_loi_nhuan_truoc_thue", "15__loi_nhuan_thuan_tu_hoat_dong_kinh_doanh_truo",
             "16__chi_phi_du_phong_rui_ro_tin_dung"),
            ("21__loi_nhuan_sau_thue", "17__tong_loi_nhuan_truoc_thue", "20__chi_phi_thue_tndn"),
        ]:
            if None not in (g(out), g(a), g(b)) and abs(g(a) - g(b) - g(out)) > 2:
                return f"{out.split('__')[0]} != {a.split('__')[0]} - {b.split('__')[0]}"
        if g("17__tong_loi_nhuan_truoc_thue") is None:
            return "no PBT"
    if report == "cash_flow":
        op = g("HDKD_27__luu_chuyen_tien_thuan_tu_hoat_dong_kinh_doan")
        inv = g("HDDT_38__luu_chuyen_tien_thuan_tu_hd_dau_tu")
        fin = g("HDTC_46__luu_chuyen_tien_thuan_tu_hd_tai_chinh")
        net = g("HDTC_47__luu_chuyen_tien_thuan_trong_ky")
        if None in (op, inv, fin, net):
            return "missing a cash-flow subtotal"
        if abs(op + inv + fin - net) > 2:
            return "operating + investing + financing != net change"
        o = g("HDTC_48__tien_va_cac_khoan_tuong_duong_tien_tai_thoi")
        c = g("HDTC_50__tien_va_cac_khoan_tuong_duong_tien_tai_thoi")
        fx = g("HDTC_49__dieu_chinh_anh_huong_cua_thay_doi_ty_gia") or 0
        if None not in (o, c) and abs(o + net + fx - c) > 2:
            return "opening + net (+fx) != closing"
    return None


PROBE = {"balance_sheet": "300__tong_tai_san",
         "income_statement": "17__tong_loi_nhuan_truoc_thue",
         "cash_flow": "HDTC_50__tien_va_cac_khoan_tuong_duong_tien_tai_thoi"}


def sane(report: str, symbol: str, values: dict[str, int]) -> str | None:
    """Magnitude guard against the already-scraped quarters.

    Reconciliation is ratio-based, so it cannot see a UNITS error or a CUMULATIVE-vs-
    STANDALONE mix-up — both balance perfectly. This is the only thing that catches them.
    """
    col = PROBE[report]
    path = HERE / f"{report}.csv"
    if col not in values or not path.exists():
        return None
    with path.open(encoding="utf-8-sig") as f:
        ref = sorted(abs(float(r[col])) for r in csv.DictReader(f)
                     if r["symbol"] == symbol and r["source"] == "scraped"
                     and r.get(col) and float(r[col]) != 0)
    if not ref:
        return None
    med, got = ref[len(ref) // 2], abs(values[col])
    if got and not (med / 20 <= got <= med * 20):
        return f"magnitude {got:.3g} vs typical {med:.3g} (units? cumulative column?)"
    return None


def write_pdf_layer(report: str, symbol: str, period: str, values: dict[str, int]) -> None:
    """Upsert one (symbol, period) row into <report>_pdf.csv, preserving every other row."""
    path = HERE / f"{report}_pdf.csv"
    head, rows = read_csv(path)
    head = head or IDX_COLS[:]
    for c in values:
        if c not in head:
            head.append(c)
    q, year = int(period[1]), int(period.split("-")[1])
    row = {"symbol": symbol, "period": period, "year": year, "quarter": q,
           **{k: str(v) for k, v in values.items()}}
    rows = [r for r in rows if not (r["symbol"] == symbol and r["period"] == period)] + [row]
    write_csv(path, head, sorted(rows, key=sort_key))
    print(f"  -> {path.name}: {symbol} {period}, {len(values)} line items")


def cmd_pdf(symbol: str, period: str, report: str | None, render: bool) -> None:
    doc, name = open_report(symbol, period)
    if doc is None:
        sys.exit(f"no consolidated report on CafeF for {symbol} {period}")
    scanned = is_scanned(doc)
    print(f"{symbol} {period}: {name}\n  pages={doc.page_count}  "
          f"{'SCANNED (no text layer)' if scanned else 'text layer'}\n")

    reports = [report] if report else list(REPORTS)

    if scanned or render:
        PAGES_DIR.mkdir(exist_ok=True)
        found = {r: statement_pages(doc, r) for r in reports}
        pages = sorted({p for v in found.values() for p in v}) or list(range(min(20, doc.page_count)))
        for p in pages[:20]:
            doc[p].get_pixmap(dpi=200).save(PAGES_DIR / f"{symbol}_{period}_p{p + 1}.png")
        print(f"  rendered {min(len(pages), 20)} page(s) -> {PAGES_DIR.name}/")
        print("  This filing has no text layer. Read the figures off the images and put them")
        print(f"  in <report>_pdf.csv (or the manual template). Check the statement's own")
        print("  subtotals AND the magnitude against neighbouring quarters before trusting them.")
        return

    for rep in reports:
        pg = statement_pages(doc, rep)
        if not pg:
            print(f"  {rep:<17} not found in this document")
            continue
        ncol, scale = n_value_columns(doc, pg), unit_scale(doc, pg)
        idx: dict[str, int] = {}
        for lab, vals in table_rows(doc, pg, ncol):
            idx.setdefault(label_key(lab), vals[0])          # column 0 = the current period

        got: dict[str, int] = {}
        for col in columns_of(rep):
            for want in [col.split("__", 1)[1].replace("_", " ")] + ALIASES.get(col, []):
                if want in idx:
                    got[col] = idx[want]
                    break
                hit = [k for k in idx if k.endswith(" " + want)]   # heading glued in front
                if len(hit) == 1:
                    got[col] = idx[hit[0]]
                    break
        got = {k: v * (1 if k.startswith("23__") else scale) for k, v in got.items()}
        got = to_cafef_signs(rep, got)

        why = (reconcile(rep, got) or sane(rep, symbol, got)) if got else "no rows parsed"
        if why:
            print(f"  {rep:<17} REJECTED — {why}  ({len(got)} items, x{scale:.0e})")
            continue
        print(f"  {rep:<17} OK — {len(got)} items, {ncol} cols, x{scale:.0e}")
        write_pdf_layer(rep, symbol, period, got)


# ═══════════════════════════════════════════════════════════════════ entry point
def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", nargs="?", default="scrape", choices=["scrape", "pdf", "docs"])
    ap.add_argument("--symbol", default="VCB")
    ap.add_argument("--period", help="e.g. Q2-2014 (required by `pdf` and `docs`)")
    ap.add_argument("--report", choices=list(REPORTS), help="pdf: just one report")
    ap.add_argument("--render", action="store_true",
                    help="pdf: rasterise the statement pages (for a scanned filing)")
    a = ap.parse_args()
    sym = a.symbol.upper()

    if a.command == "scrape":
        cmd_scrape(sym)
    elif a.command == "docs":
        if not a.period:
            sys.exit("--period is required, e.g. --period Q2-2014")
        q, yr = int(a.period[1]), int(a.period.split("-")[1])
        for d in documents(sym):
            if d["Year"] == yr and d["Quarter"] == q:
                print(f"  {d['Name']}\n     {d['Link']}")
    else:
        if not a.period:
            sys.exit("--period is required, e.g. --period Q2-2014")
        cmd_pdf(sym, a.period, a.report, a.render)


if __name__ == "__main__":
    main()
