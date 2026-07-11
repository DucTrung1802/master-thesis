"""
Read a quarter's financial statements out of the company's actual filing (PDF) and write
them to the `pdf` layer: `<report>_pdf.csv`.

    python read_pdf.py --period Q2-2014                 # auto-extract (text-layer PDFs)
    python read_pdf.py --period Q2-2014 --render        # rasterise the pages (scanned PDFs)
    python read_pdf.py --period Q2-2014 --list          # just show the available documents

Precedence in `scrape_financials.py` is **manual > pdf > scraped**: the API is the base,
this module overrides it from the filing, and a hand-entered value beats both.

Two kinds of PDF
----------------
* **text layer** — parsed automatically here. Rows are rebuilt from WORD COORDINATES, not
  from the raw text stream: PyMuPDF emits label fragments out of order, and a line-based
  read silently mis-assigns values (it once put "Chi phí hoạt động khác" into
  "Chi phí hoạt động"). The period columns are found by clustering the x-positions of every
  number and keeping the densest N — that is what separates a real value from the row's item
  number on the far left and the "Thuyết minh" note reference in the middle.
* **scanned image** (no text layer) — cannot be parsed. `--render` writes each statement page
  to `pdf_pages/<SYM>_<period>_p<n>.png` so the figures can be transcribed by eye/OCR and
  pasted into `<report>_pdf.csv`.

Whatever the route, a statement is only accepted if it RECONCILES against its own printed
subtotals (assets = liabilities + equity, PBT = operating profit − provisions, …).

Two traps that reconciliation CANNOT catch — both still balance internally:
  1. **cumulative vs standalone** — a Q2/Q3/Q4 report prints both the quarter and the
     year-to-date column. The income statement must take the STANDALONE quarter; cash flow
     must take the CUMULATIVE one (that is CafeF's convention). Getting this wrong yields a
     number that reconciles perfectly and is still wrong.
  2. **units** — most reports are in Triệu VNĐ (×1e6) but some (e.g. VCB's 2009 ones) are in
     plain đồng. A 10^6 error reconciles perfectly too.
Both are only caught by comparing magnitudes with the neighbouring quarters, which
`_sane()` does before anything is written.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
import urllib.request
from pathlib import Path

try:
    import fitz  # PyMuPDF
except ImportError:
    sys.exit("PyMuPDF is required:  pip install pymupdf")

HERE = Path(__file__).parent
PDF_CACHE = HERE / "pdf_cache"
PAGES = HERE / "pdf_pages"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
MILLION = 1_000_000
Y_TOL = 3.0

DOCS = ("https://cafef.vn/du-lieu/Ajax/PageNew/FileBCTC.ashx"
        "?Symbol={sym}&Type=1&Year=0")

HEADING = {
    "balance_sheet": r"B[ảa]ng c[âa]n [đd][ốo]i k[ếe] to[áa]n",
    "income_statement": r"k[ếe]t qu[ảa] ho[ạa]t [đd][ộo]ng kinh doanh",
    "cash_flow": r"l[ưu]u chuy[ểe]n ti[ềe]n t[ệe]",
}
NOTES_PAGE = r"Thuy[ếe]t minh b[áa]o c[áa]o"


# --------------------------------------------------------------------------- helpers
def norm(s: str) -> str:
    s = s.replace("đ", "d").replace("Đ", "D")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^A-Za-z0-9]+", " ", s).strip().lower()
    return re.sub(r"\s+", " ", s)


def label_key(lab: str) -> str:
    """Normalised label with its numbering stripped ('VIII Chi phí hoạt động' -> ...)."""
    k, prev = norm(lab), None
    while prev != k:
        prev = k
        k = re.sub(r"^(?:[ivxlc]+|\d+|[a-z])\s+", "", k)
    return k.strip()


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


# --------------------------------------------------------------------------- documents
def documents(symbol: str) -> list[dict]:
    req = urllib.request.Request(
        DOCS.format(sym=symbol.lower()),
        headers={"User-Agent": UA,
                 "Referer": f"https://cafef.vn/du-lieu/hose/{symbol.lower()}-tai-lieu.chn"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))["Data"]


def open_report(symbol: str, period: str) -> tuple[fitz.Document, str] | tuple[None, None]:
    """Consolidated ('hợp nhất') report for the quarter; reviewed/audited preferred."""
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
        # the API advertises cafefnew.mediacdn.vn but older files are served from cafef1
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


# --------------------------------------------------------------------------- table parse
def n_value_columns(doc, pages) -> int:
    """Number of period columns — derived from the DATA, not from counting unit labels.

    Counting the repeated "Triệu VNĐ" header is unreliable: some reports print the unit once
    for a 4-column table, and guessing 2 there silently reads the *cumulative* column.
    """
    nums = [w for p in pages for w in doc[p].get_text("words")
            if _NUM.match(w[4]) and parse_num(w[4]) is not None]
    if not nums:
        return 2
    cl = _cluster(sorted((w[0] + w[2]) / 2 for w in nums))
    biggest = max(len(c) for c in cl)
    dense = [c for c in cl if len(c) >= 0.45 * biggest]
    return len(dense) if 2 <= len(dense) <= 5 else 2


def _cluster(centres: list[float]) -> list[list[float]]:
    out: list[list[float]] = []
    for c in centres:
        if out and c - out[-1][-1] <= 18:
            out[-1].append(c)
        else:
            out.append([c])
    return out


def table_rows(doc, pages, ncol: int) -> list[tuple[str, list[int]]]:
    """Rebuild rows from word coordinates: [(label, [value per period column])]."""
    nums = [w for p in pages for w in doc[p].get_text("words")
            if _NUM.match(w[4]) and parse_num(w[4]) is not None]
    if not nums:
        return []
    clusters = _cluster(sorted((w[0] + w[2]) / 2 for w in nums))
    if len(clusters) < ncol:
        return []
    keep = sorted(sorted(clusters, key=len, reverse=True)[:ncol], key=lambda c: c[0])
    lo = min(keep[0]) - 25                      # left of this = label / item no. / note ref

    out = []
    for p in pages:
        lines: dict[float, list] = {}
        for w in doc[p].get_text("words"):
            k = next((k for k in lines if abs(k - w[1]) <= Y_TOL), w[1])
            lines.setdefault(k, []).append(w)
        carry: list[str] = []                   # a label that wrapped onto its own line
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
    """×1e6 when the statement is denominated in 'Triệu VNĐ', else ×1 (plain đồng)."""
    txt = "\n".join(doc[p].get_text() for p in pages[:1])
    return MILLION if re.search(r"Tri[ệe]u\s*(?:VN[ĐD]|[đd][ồo]ng)", txt, re.I) else 1


# --------------------------------------------------------------------------- checks
def reconcile(report: str, d: dict) -> str | None:
    """None if the statement balances against its own printed subtotals, else the reason."""
    g = d.get
    if report == "balance_sheet":
        tot, tot2 = g("300__tong_tai_san"), g("800__tong_no_phai_tra_va_von_chu_so_huu")
        if tot is None or tot2 is None:
            return "missing a total"
        if abs(tot - tot2) != 0:
            return "assets != liabilities + equity"
        liab, eq = g("400__tong_no_phai_tra"), g("500__viii_von_chu_so_huu")
        minority = g("700__loi_ich_cua_co_dong_thieu_so") or 0
        # Two conventions are both in use — the older one reports minority interest OUTSIDE
        # equity (400+500+700 = 800), the TT49/2014 one folds it INSIDE (400+500 = 800).
        # CafeF itself switched mid-2024, so accept either rather than reject a good filing.
        if None not in (liab, eq) and tot2 not in (liab + eq, liab + eq + minority):
            return "liabilities + equity (+/- minority) != total"
    if report == "income_statement":
        # values are already normalised to CafeF's convention (expenses POSITIVE), so every
        # identity is a SUBTRACTION
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
        p = [g("HDKD_27__luu_chuyen_tien_thuan_tu_hoat_dong_kinh_doan"),
             g("HDDT_38__luu_chuyen_tien_thuan_tu_hd_dau_tu"),
             g("HDTC_46__luu_chuyen_tien_thuan_tu_hd_tai_chinh"),
             g("HDTC_47__luu_chuyen_tien_thuan_trong_ky")]
        if None in p:
            return "missing a cash-flow subtotal"
        if abs(sum(p[:3]) - p[3]) > 2:
            return "operating + investing + financing != net change"
    return None


PROBE = {"balance_sheet": "300__tong_tai_san",
         "income_statement": "17__tong_loi_nhuan_truoc_thue",
         "cash_flow": "HDTC_50__tien_va_cac_khoan_tuong_duong_tien_tai_thoi"}

# Income-statement lines CafeF stores as a POSITIVE magnitude even though the filing prints
# them in parentheses (an expense). Get this wrong and the row is sign-inconsistent with every
# scraped quarter — which is exactly what happened to Q4-2010 / Q1-2011 on the first pass.
IS_EXPENSES = {"2__chi_phi_lai_va_cac_chi_phi_tuong_tu", "5__chi_phi_hoat_dong_dich_vu",
               "11__chi_phi_hoat_dong_khac", "14__chi_phi_hoat_dong",
               "16__chi_phi_du_phong_rui_ro_tin_dung", "18__chi_phi_thue_tndn_hien_hanh",
               "20__chi_phi_thue_tndn", "22__loi_ich_cua_co_dong_thieu_so",
               "25__chi_phi_hoat_dong_khac"}


def to_cafef_signs(report: str, d: dict[str, int]) -> dict[str, int]:
    """Normalise a parsed statement to CafeF's sign convention.

    Income statement: expenses are stored POSITIVE (the filing prints them negative). The
    deferred-tax line is *derived* rather than parsed, because the filing labels it
    "Thu nhập/(chi phí)" — its printed sign flips depending on whether it is income or an
    expense, while CafeF always keeps `20 = 18 + 19`.
    Balance sheet / cash flow keep the filing's signs (provisions and outflows stay negative).
    """
    if report != "income_statement":
        return d
    out = {k: (abs(v) if k in IS_EXPENSES else v) for k, v in d.items()}
    cur, tot = out.get("18__chi_phi_thue_tndn_hien_hanh"), out.get("20__chi_phi_thue_tndn")
    if cur is not None and tot is not None:
        out["19__chi_phi_thue_tndn_hoan_lai"] = tot - cur
    return out


def sane(report: str, symbol: str, values: dict[str, int]) -> str | None:
    """Magnitude guard vs the already-scraped quarters.

    Reconciliation is ratio-based, so it cannot see a units error or a cumulative-vs-
    standalone mix-up — both balance perfectly. This is what catches them.
    """
    col = PROBE[report]
    if col not in values:
        return None
    path = HERE / f"{report}.csv"
    if not path.exists():
        return None
    with path.open(encoding="utf-8-sig") as f:
        ref = sorted(abs(float(r[col])) for r in csv.DictReader(f)
                     if r["symbol"] == symbol and r["source"] == "scraped"
                     and r.get(col) and float(r[col]) != 0)
    if not ref:
        return None
    med = ref[len(ref) // 2]
    got = abs(values[col])
    if got and not (med / 20 <= got <= med * 20):
        return f"magnitude {got:.3g} vs typical {med:.3g} (units? cumulative column?)"
    return None


# --------------------------------------------------------------------------- pdf layer io
IDX = ["symbol", "period", "year", "quarter"]


def write_pdf_layer(report: str, symbol: str, period: str, values: dict[str, int]) -> None:
    """Upsert one (symbol, period) row into <report>_pdf.csv, preserving other rows."""
    path = HERE / f"{report}_pdf.csv"
    head, rows = [], []
    if path.exists():
        with path.open(encoding="utf-8-sig") as f:
            rd = csv.DictReader(f)
            head, rows = list(rd.fieldnames or []), list(rd)
    if not head:
        head = IDX[:]
    for c in values:
        if c not in head:
            head.append(c)
    q, year = int(period[1]), int(period.split("-")[1])
    row = {"symbol": symbol, "period": period, "year": year, "quarter": q,
           **{k: str(v) for k, v in values.items()}}
    rows = [r for r in rows if not (r["symbol"] == symbol and r["period"] == period)] + [row]
    rows.sort(key=lambda r: (r["symbol"], int(r["year"]), int(r["quarter"])))
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=head)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in head})
    print(f"  -> {path.name}: {symbol} {period}, {len(values)} line items")


# Some labels wrap across lines in the filing and the text layer only preserves the tail
# ("…thu nhập lãi và các khoản | thu nhập tương tự" -> "thu nhap tuong tu"). Accept those
# fragments explicitly rather than loosening the matcher: fuzzy matching once put
# "Chi phí hoạt động khác" into "Chi phí hoạt động".
ALIASES: dict[str, list[str]] = {
    "1__thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu": ["thu nhap tuong tu"],
    "2__chi_phi_lai_va_cac_chi_phi_tuong_tu": ["khoan chi phi tuong tu", "chi phi tuong tu"],
    "5__chi_phi_hoat_dong_dich_vu": ["chi phi tu hoat dong dich vu"],
    "6__lai_lo_thuan_tu_hoat_dong_dich_vu": ["lai thuan tu hoat dong dich vu"],
    "7__lai_lo_thuan_tu_hoat_dong_kinh_doanh_ngoai_h": ["lai lo thuan tu hoat dong kinh doanh ngoai hoi", "lai thuan tu hoat dong kinh doanh ngoai hoi"],
    "8__lai_lo_thuan_tu_mua_ban_chung_khoan_kinh_doa": ["lai lo thuan tu mua ban chung khoan kinh doanh", "lai thuan tu mua ban chung khoan kinh doanh"],
    "9__lai_lo_thuan_tu_mua_ban_chung_khoan_dau_tu": ["lai lo thuan tu mua ban chung khoan dau tu", "lai lo thuan tu mua ban chung khoan dau tu"],
    "12__lai_lo_thuan_tu_hoat_dong_khac": ["lai thuan tu hoat dong khac", "lai lo thuan tu hoat dong khac"],
    "15__loi_nhuan_thuan_tu_hoat_dong_kinh_doanh_truo": ["loi nhuan thuan tu hoat dong kinh doanh truoc chi phi du phong rui ro tin dung", "phong rui ro tin dung"],
    "16__chi_phi_du_phong_rui_ro_tin_dung": ["chi phi du phong rui ro tin dung", "tin dung"],
    "24__tong_thu_nhap_hoat_dong": ["tong thu nhap hoat dong"],
    "26__loi_nhuan_sau_thue_cua_co_dong_cua_ngan_hang": ["loi nhuan thuan trong ky", "loi nhuan thuan cua co dong ngan hang"],
    "23__lai_co_ban_tren_co_phieu_dong_1_co_phieu": ["lai co ban tren co phieu", "lai co ban tren co phieu vnd co phieu"],
}


def columns_of(report: str) -> list[str]:
    with (HERE / f"{report}.csv").open(encoding="utf-8-sig") as f:
        cols = list(next(csv.DictReader(f)).keys())
    return [c for c in cols
            if c not in ("symbol", "exchange", "period", "year", "quarter", "source")]


# --------------------------------------------------------------------------- main
def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="VCB")
    ap.add_argument("--period", required=True, help="e.g. Q2-2014")
    ap.add_argument("--report", choices=list(HEADING), help="default: all three")
    ap.add_argument("--render", action="store_true",
                    help="rasterise the statement pages (for scanned PDFs)")
    ap.add_argument("--list", action="store_true", help="list the available documents")
    a = ap.parse_args()
    sym = a.symbol.upper()

    if a.list:
        q, yr = int(a.period[1]), int(a.period.split("-")[1])
        for d in documents(sym):
            if d["Year"] == yr and d["Quarter"] == q:
                print(f"  {d['Name']}\n     {d['Link']}")
        return

    doc, name = open_report(sym, a.period)
    if doc is None:
        sys.exit(f"no consolidated report on CafeF for {sym} {a.period}")
    print(f"{sym} {a.period}: {name}\n  pages={doc.page_count}  "
          f"{'SCANNED (no text layer)' if is_scanned(doc) else 'text layer'}\n")

    reports = [a.report] if a.report else list(HEADING)

    if is_scanned(doc) or a.render:
        PAGES.mkdir(exist_ok=True)
        # a scan has no searchable headings, so fall back to rendering every page
        todo = {r: statement_pages(doc, r) for r in reports}
        pages = sorted({p for v in todo.values() for p in v}) or list(range(doc.page_count))
        for p in pages[:16]:
            out = PAGES / f"{sym}_{a.period}_p{p + 1}.png"
            doc[p].get_pixmap(dpi=300).save(out)
        print(f"  rendered {min(len(pages), 16)} page(s) -> {PAGES.name}/")
        print("  This PDF has no text layer: transcribe the figures (by eye or OCR) and put")
        print(f"  them in <report>_pdf.csv. Every total must reconcile before you trust it.")
        return

    for report in reports:
        pg = statement_pages(doc, report)
        if not pg:
            print(f"  {report:<17} not found in this document")
            continue
        ncol = n_value_columns(doc, pg)
        scale = unit_scale(doc, pg)
        rows = table_rows(doc, pg, ncol)
        idx: dict[str, int] = {}
        for lab, vals in rows:
            idx.setdefault(label_key(lab), vals[0])       # column 0 = the current period
        # map by exact / suffix key onto this ticker's columns (never fuzzy — a fuzzy match
        # once put "chi phi hoat dong khac" into "chi phi hoat dong")
        got: dict[str, int] = {}
        for col in columns_of(report):
            for want in [col.split("__", 1)[1].replace("_", " ")] + ALIASES.get(col, []):
                if want in idx:
                    got[col] = idx[want]
                    break
                hit = [k for k in idx if k.endswith(" " + want)]
                if len(hit) == 1:
                    got[col] = idx[hit[0]]
                    break
        got = {k: v * (1 if k.startswith("23__") else scale) for k, v in got.items()}
        got = to_cafef_signs(report, got)

        why = reconcile(report, got) if got else "no rows parsed"
        why = why or sane(report, sym, got)
        if why:
            print(f"  {report:<17} REJECTED — {why}  ({len(got)} items, x{scale:.0e})")
            continue
        print(f"  {report:<17} OK — {len(got)} items, {ncol} cols, x{scale:.0e}")
        write_pdf_layer(report, sym, a.period, got)


if __name__ == "__main__":
    main()
