"""
Scrape VCB financial-statement PUBLISH (disclosure) dates, one date per quarter,
2009 -> present, into a single CSV.  Self-contained: no login, stdlib only.

Per the reporting cadence VCB actually uses, each quarter maps to:
    Q1 -> quarterly report                       (unaudited)
    Q2 -> semi-annual report      "soát xét"     (reviewed)      <- audited half-year
    Q3 -> quarterly report                       (unaudited)
    Q4 -> WHOLE-YEAR report       "kiểm toán"    (audited)       <- annual audited
So Q4 of year Y uses the audited FY-Y report (disclosed early Y+1), e.g.
    Q4/2025 -> "BCTC hợp nhất năm 2025 (đã kiểm toán)" published 2026-03-27.

Best source per era (each is the true disclosure day):
  * 2009-2014  Vietstock NEWS feed  -> the HOSE disclosure announcement article,
               whose article date is the disclosure date.  This is the only place
               the 2009-2012 dates survive (the file APIs only kept later
               bulk-reupload dates).
  * 2013-2021  Vietstock getdocument `LastUpdate` (upload = disclosure in this era).
  * 2022+      CafeF PDF-filename date (exact disclosure day; most precise).
When several sources cover a cell, the more precise/earlier one wins
(cafef-filename > news > vietstock-lastupdate).

Endpoints (all GET/POST, no auth):
  CafeF     GET  cafef.vn/du-lieu/Ajax/PageNew/FileBCTC.ashx?Symbol=vcb&Type=1&Year=0
  Vietstock POST finance.vietstock.vn/data/getdocument  (needs page cookie+token)
  Vietstock POST finance.vietstock.vn/View/PagingNewsContent  (news feed)

Output: vcb_quarter_publish_dates.csv  (year, quarter, publish_date, report_used,
        confidence, source, evidence)
Raw caches (for reproducibility): vcb_cafef_bctc.json, vcb_vietstock_docs.json,
        vcb_vietstock_news.json
"""

from __future__ import annotations

import csv
import http.cookiejar
import json
import re
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

HERE = Path(__file__).parent
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
ROMAN = {"i": 1, "ii": 2, "iii": 3, "iv": 4}
# Pass --refresh to re-hit the network; otherwise cached raw JSON is reused.
import sys as _sys
REFRESH = "--refresh" in _sys.argv


def _cache(name):
    p = HERE / name
    if p.exists() and not REFRESH:
        return json.loads(p.read_text(encoding="utf-8"))
    return None


def _epoch_date(s):
    m = re.search(r"/Date\((\d+)\)/", s or "")
    return datetime.fromtimestamp(int(m.group(1)) / 1000, tz=timezone.utc).date() if m else None


# Vietnamese signing line "…ngày DD tháng MM năm YYYY", tolerant of the legacy
# TCVN3 font that garbles the letters (e.g. "ngμy 20 th¸ng 07 n¨m 2009") — the
# digits stay ASCII, which is all we need.
_SIGN_RE = re.compile(
    r"ng[^ ]{0,3}y\s*(\d{1,2})\s*th[^ ]{0,3}ng\s*(\d{1,2})\s*n[^ ]{0,3}m\s*(\d{4})", re.I)


def pdf_signing_date(url, period_end, cap_days):
    """Download a report PDF and return its signing date = the LATEST printed date
    strictly after period_end (within cap_days) — the report is signed after all
    of its reporting/comparative dates.  None if the file is missing (404),
    scanned (no text layer), or has no such date."""
    try:
        import fitz  # PyMuPDF; optional dependency, only needed for PDF recovery
    except ImportError:
        return None
    parts = urllib.parse.urlsplit(url)              # encode spaces in the path
    url = urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, urllib.parse.quote(parts.path),
         parts.query, parts.fragment))
    raw = None
    for attempt in range(3):                        # retry transient network errors
        try:
            raw = urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent": UA}), timeout=90).read()
            break
        except urllib.error.HTTPError:
            return None                             # 404 etc. -> file genuinely absent
        except Exception:
            continue
    if raw is None:
        return None
    try:
        doc = fitz.open(stream=raw, filetype="pdf")
    except Exception:
        return None
    hi = date.fromordinal(period_end.toordinal() + cap_days)
    hits = []
    for pg in doc:
        for m in _SIGN_RE.finditer(pg.get_text()):
            try:
                d = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except ValueError:
                continue
            if period_end < d <= hi:
                hits.append(d)
    return max(hits) if hits else None  # latest date = the signing date

# ---------------------------------------------------------------- HTTP helpers


def _open(req, timeout=40):
    return urllib.request.urlopen(req, timeout=timeout)


def get(url, headers=None):
    req = urllib.request.Request(url, headers={"User-Agent": UA, **(headers or {})})
    return _open(req).read().decode("utf-8", "replace")


def post(url, data, headers=None, opener=None):
    body = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(
        url, data=body,
        headers={"User-Agent": UA, "Content-Type": "application/x-www-form-urlencoded",
                 "X-Requested-With": "XMLHttpRequest", **(headers or {})},
    )
    fn = opener.open if opener else _open
    return fn(req, timeout=40).read().decode("utf-8", "replace")


# ---------------------------------------------------------------- 1) CafeF


def scrape_cafef() -> list[dict]:
    """Financial-statement docs from CafeF; date = YYYYMMDD / DDMMYYYY in filename."""
    data = _cache("vcb_cafef_bctc.json")
    if data is None:
        url = "https://cafef.vn/du-lieu/Ajax/PageNew/FileBCTC.ashx?Symbol=vcb&Type=1&Year=0"
        data = json.loads(get(url, {"Referer": "https://cafef.vn/du-lieu/hose/vcb-tai-lieu.chn"}))["Data"]
        (HERE / "vcb_cafef_bctc.json").write_text(json.dumps(data, ensure_ascii=False, indent=0), "utf-8")
    pre = re.compile(r"/BCTC/(\d{8})_")
    emb = re.compile(r"(\d{2})(\d{2})(\d{4})-?\d{6}")
    out = []
    for r in data:
        link = r.get("Link", "") or ""
        d = None
        m = pre.search(link)
        if m:
            try:
                d = datetime.strptime(m.group(1), "%Y%m%d").date()
            except ValueError:
                d = None
        if d is None:
            for m in emb.finditer(link):
                dd, mm, yyyy = m.groups()
                try:
                    d = date(int(yyyy), int(mm), int(dd))
                    break
                except ValueError:
                    continue
        if d:
            out.append({"quarter": r.get("Quarter"), "year": r.get("Year"),
                        "name": (r.get("Name") or "").strip(), "date": d})
    return out


# ---------------------------------------------------------------- 2) Vietstock docs


def scrape_vietstock_docs() -> list[dict]:
    """type=1 documents; date = LastUpdate epoch (reliable ~2013+)."""
    all_rows = _cache("vcb_vietstock_docs.json")
    if all_rows is not None:
        return [{"title": (r.get("Title") or "").strip(),
                 "date": _epoch_date(r.get("LastUpdate"))} for r in all_rows]
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    page = opener.open(urllib.request.Request(
        "https://finance.vietstock.vn/VCB/tai-tai-lieu.htm", headers={"User-Agent": UA})
    ).read().decode("utf-8", "replace")
    m = re.search(r'__RequestVerificationToken[^>]*?value=([A-Za-z0-9_-]+)', page)
    token = m.group(1) if m else ""
    all_rows = []
    for p in range(1, 40):
        txt = post("https://finance.vietstock.vn/data/getdocument",
                   {"code": "VCB", "page": p, "type": 1, "__RequestVerificationToken": token},
                   {"Referer": "https://finance.vietstock.vn/VCB/tai-tai-lieu.htm"}, opener)
        try:
            rows = json.loads(txt)
        except json.JSONDecodeError:
            break
        if not rows:
            break
        all_rows.extend(rows)
        if len(all_rows) >= (rows[0].get("TotalRow") or 0):
            break
    (HERE / "vcb_vietstock_docs.json").write_text(json.dumps(all_rows, ensure_ascii=False, indent=0), "utf-8")
    return [{"title": (r.get("Title") or "").strip(),
             "date": _epoch_date(r.get("LastUpdate"))} for r in all_rows]


# ---------------------------------------------------------------- 3) Vietstock news


def scrape_vietstock_news(start_year=2009) -> list[dict]:
    """All VCB news since start_year (each item carries its disclosure date).

    The feed caps total results for a wide date range, so we query one year at a
    time (that returns the full history for the window) and merge.
    """
    cached = _cache("vcb_vietstock_news.json")
    if cached is not None:
        return cached
    url = "https://finance.vietstock.vn/View/PagingNewsContent"
    hdr = {"Referer": "https://finance.vietstock.vn/VCB/tin-moi-nhat.htm"}
    blk = re.compile(r'(\d{2}/\d{2}/\d{4})[\s\S]*?href="([^"]+\.htm)"[^>]*>([^<]+)')
    import html as _html
    seen, items = set(), []
    for yr in range(start_year, date.today().year + 1):
        for p in range(1, 30):
            h = post(url, {"view": 1, "code": "VCB", "type": 1,
                           "fromDate": f"01/01/{yr}", "toDate": f"31/12/{yr}",
                           "channelID": -1, "page": p, "pageSize": 50}, hdr)
            found = blk.findall(h)
            if not found:
                break
            for d, u, t in found:
                if u in seen:
                    continue
                seen.add(u)
                items.append({"date": d, "title": _html.unescape(t).strip(), "url": u})
    (HERE / "vcb_vietstock_news.json").write_text(json.dumps(items, ensure_ascii=False, indent=0), "utf-8")
    return items


# ---------------------------------------------------------------- news classifier

def _year4(s):
    m = re.search(r"(20\d{2})", s)
    return int(m.group(1)) if m else None


def _quarter_num(t: str):
    m = re.search(r"quý\s*(iv|iii|ii|i|[1-4])\b", t)
    if m:
        g = m.group(1)
        return ROMAN.get(g) or (int(g) if g.isdigit() else None)
    m = re.search(r"\bq\s*([1-4])\b", t)  # Q1 / Q2.2013 / Q4-2012
    return int(m.group(1)) if m else None


def classify_news(title: str, art_date: date):
    """news title -> (year, cell, tier, pref).  Cells:
      Q1/Q3 quarterly (unaudited), Q2 reviewed half-year, Q4 quarterly (unaudited),
      FY whole-year audited report.
    tier 1 = official BCTC disclosure article, 2 = earnings-result (approx).
    pref 0 = exactly the report we want for that cell, 1 = weaker fallback."""
    t = title.lower()
    if any(x in t for x in ["giải trình", "nghị quyết", "nhắc nhở", "không thực hiện",
                            "không nộp", "thường niên", "quản trị", "chênh lệch",
                            "sẽ kiểm toán", "ký kết hợp đồng kiểm toán"]):
        return None  # meta / non-report announcements

    is_bctc = ("báo cáo tài chính" in t) or re.search(r"\bbctc\b", t)
    audited = ("kiểm toán" in t) or re.search(r"\bkt\b", t)
    reviewed = ("soát xét" in t) or re.search(r"\bsx\b", t)

    if is_bctc:
        # whole-year report ("BCTC [Hợp nhất] năm YYYY", no quý / no 6 tháng) is the
        # audited final-year report -> FY cell.  The word "kiểm toán" is often omitted.
        if "quý" not in t and "6 tháng" not in t and ("năm" in t or audited):
            y = _year4(t)
            if y:
                return (y, "FY", 1, 0)
        # semi-annual REVIEWED -> Q2 cell
        if reviewed:
            y = _year4(t) or art_date.year
            return (y, "Q2", 1, 0)
        q = _quarter_num(t)
        if q in (1, 3, 4):  # unaudited quarterly (Q4 is the Q4 report, not the audit)
            return (_year4(t) or art_date.year, f"Q{q}", 1, 0)
        if q == 2:  # preliminary Q2 (no review) -> only a fallback for Q2
            return (_year4(t) or art_date.year, "Q2", 1, 1)
        return None

    # Tier-2 earnings-result fallback (fills 2009-2011 where no clean BCTC article).
    # These dates are approximate (media report ≈ disclosure day).  Use specific
    # earnings phrases only (bare "lãi" also means "lãi suất" = interest rate).
    earn = any(x in t for x in ["lãi sau thuế", "lãi ròng", "lãi trước thuế",
                                "lợi nhuận", "kết quả kinh doanh"])
    if earn:
        y = art_date.year
        if "6 tháng" in t or re.search(r"quý\s*(2|ii)\b", t):
            return (y, "Q2", 2, 1)
        if "9 tháng" in t or re.search(r"quý\s*(3|iii)\b", t):
            return (y, "Q3", 2, 1)
        if "3 tháng" in t or re.search(r"quý\s*(1|i)\b", t):
            return (y, "Q1", 2, 1)
        if re.search(r"quý\s*(4|iv)\b", t):
            return (_year4(t) or y, "Q4", 2, 1)
        # preliminary full-year results reported early next year -> Q4 (unaudited)
        if art_date.month in (1, 2, 3) and "năm" in t:
            return (_year4(t) or (y - 1), "Q4", 2, 1)
        # else infer from month, for figure-quoting articles only
        if re.search(r"\btỷ\b", t):
            if art_date.month in (4, 5):
                return (y, "Q1", 2, 1)
            if art_date.month in (10, 11):
                return (y, "Q3", 2, 1)
    return None


# ---------------------------------------------------------------- doc classifier

def classify_doc_title(title: str):
    """CafeF/Vietstock document title -> (year, cell, pref).  Same cell rules:
    Q1/Q3/Q4 unaudited quarterly, Q2 reviewed half-year, FY audited final-year."""
    t = title.lower()
    audited = "kiểm toán" in t
    reviewed = "soát xét" in t
    if audited and "quý" not in t and "năm" in t:      # audited full-year -> FY
        y = _year4(t)
        return (y, "FY", 0) if y else None
    if reviewed and ("6 tháng" in t or re.search(r"quý\s*2", t)):  # reviewed H1 -> Q2
        y = _year4(t)
        return (y, "Q2", 0) if y else None
    q = _quarter_num(t)
    if q in (1, 3, 4):
        y = _year4(t)
        return (y, f"Q{q}", 0) if y else None
    if q == 2:                                          # preliminary Q2 -> fallback
        y = _year4(t)
        return (y, "Q2", 1) if y else None
    return None


# -------------------------------------------------- 4) in-PDF signing dates

# The file/news trail is thin for the earliest years, so for this range we read
# the signing date printed inside the CafeF PDF itself (authoritative). Newer
# years already have exact disclosure dates from news/CafeF filenames.
PDF_YEARS = range(2009, 2013)


def scrape_pdf_signing(cafef_raw, vdocs_raw) -> dict:
    """(year, cell) -> ISO signing date read from the report PDF (2009-2012).

    CafeF lost most 2010-2012 files (404) and its 2009 audited annuals are scanned,
    so we also try Vietstock's hosted copies (static2.vietstock.vn)."""
    cached = _cache("vcb_pdf_signing.json")
    if cached is not None:
        return {tuple(k.split("|")): v for k, v in cached.items()}
    # gather every candidate PDF per cell, ranked by report type (pref) then
    # consolidated-first, and try each until one yields a signing date (many old
    # PDFs 404 on the CDN or are scanned image-only files with no text layer).
    cands: dict[tuple, list] = {}

    def offer(title, link, y=None):
        c = classify_doc_title(title)
        if not c:
            return
        yy, cell, pref = c
        yy = yy or y
        if yy not in PDF_YEARS or not link or not link.lower().endswith(".pdf"):
            return
        cons = "hợp nhất" in title.lower()
        cands.setdefault((yy, cell), []).append(((pref, 0 if cons else 1), link))

    for r in cafef_raw:
        offer(r.get("Name", ""), r.get("Link", ""), r.get("Year"))
    for r in vdocs_raw:
        offer(r.get("Title", ""), r.get("Url", ""))
    out = {}
    for (y, cell), lst in cands.items():
        cap = 500 if cell == "FY" else 150   # audited annuals can be badly delayed
        seen = set()
        for _score, link in sorted(lst):
            if link in seen:
                continue
            seen.add(link)
            d = pdf_signing_date(link, _period_end(y, cell), cap)
            if d:
                out[(y, cell)] = d.isoformat()
                break
    (HERE / "vcb_pdf_signing.json").write_text(
        json.dumps({f"{y}|{c}": v for (y, c), v in out.items()}, ensure_ascii=False, indent=0),
        "utf-8")
    return out


# -------------------------------------------------- 5) manual overrides

MANUAL_FILE = "vcb_manual_overrides.csv"


def load_manual_overrides() -> list:
    """Read year,cell,publish_date[,note] from vcb_manual_overrides.csv (if present).
    Any row with a publish_date wins over every scraped source. `cell` accepts
    Q1/Q2/Q3/Q4 and FY / final_year.  Rows with a blank date are ignored."""
    p = HERE / MANUAL_FILE
    if not p.exists():
        return []
    out = []
    with p.open(encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            raw = (r.get("cell") or "").strip().lower()
            cell = "FY" if raw in ("fy", "final_year", "final year", "annual") else raw.upper()
            ds = (r.get("publish_date") or "").strip()
            if not ds or cell not in CELLS:
                continue
            try:
                out.append((int(r["year"]), cell, date.fromisoformat(ds),
                            (r.get("note") or "").strip()))
            except (ValueError, KeyError, TypeError):
                continue
    return out


# ---------------------------------------------------------------- merge

def parse_ddmmyyyy(s):
    return datetime.strptime(s, "%d/%m/%Y").date()


def _period_end(y, cell):
    return {"Q1": date(y, 3, 31), "Q2": date(y, 6, 30), "Q3": date(y, 9, 30),
            "Q4": date(y, 12, 31), "FY": date(y, 12, 31)}[cell]


# plausible disclosure lag (days after period end) used to trust a vts upload date.
# Q4 = the quarterly Q4 report (disclosed ~late Jan/early Feb); FY = the audited
# final-year report (disclosed ~late Mar/Apr).
_LAG = {"Q1": (15, 80), "Q2": (15, 135), "Q3": (15, 80),
        "Q4": (15, 95), "FY": (40, 220)}

CELLS = ("Q1", "Q2", "Q3", "Q4", "FY")


def build():
    from collections import Counter
    cafef = scrape_cafef()  # also writes/refreshes vcb_cafef_bctc.json
    cafef_raw = json.loads((HERE / "vcb_cafef_bctc.json").read_text(encoding="utf-8"))
    vdocs = scrape_vietstock_docs()  # also writes vcb_vietstock_docs.json
    vdocs_raw = json.loads((HERE / "vcb_vietstock_docs.json").read_text(encoding="utf-8"))
    news = scrape_vietstock_news()

    cand: dict[tuple[int, str], list[dict]] = {}

    def add(y, cell, d, source, conf, pref, evid):
        if not y or not d:
            return
        cand.setdefault((y, cell), []).append(
            {"date": d, "source": source, "confidence": conf, "pref": pref, "evidence": evid})

    # --- Vietstock news: authoritative HOSE disclosure-article dates (all years)
    for n in news:
        d = parse_ddmmyyyy(n["date"])
        c = classify_news(n["title"], d)
        if not c:
            continue
        y, cell, tier, pref = c
        lo, hi = _LAG[cell]
        if not (lo - 10 <= (d - _period_end(y, cell)).days <= hi + 40):
            continue  # article date not in a plausible disclosure window -> skip
        add(y, cell, d, "vietstock-news", "high" if tier == 1 else "approx", pref,
            f'{n["title"]}  <{n["url"]}>')

    # --- CafeF filename dates (exact; 2022+)
    for r in cafef:
        c = classify_doc_title(r["name"])
        if c:
            add(c[0], c[1], r["date"], "cafef-filename", "high", c[2], r["name"])

    # --- Vietstock getdocument LastUpdate: trust only when the lag is plausible and
    #     the date is not a shared bulk-reupload (many quarters same day).
    doc_hits = []
    for r in vdocs:
        c = classify_doc_title(r["title"])
        if c and r["date"]:
            doc_hits.append((c[0], c[1], c[2], r["date"], r["title"]))
    bulk = {d for d, n in Counter(h[3] for h in doc_hits).items() if n > 2}
    for y, cell, pref, d, title in doc_hits:
        lo, hi = _LAG[cell]
        plausible = lo <= (d - _period_end(y, cell)).days <= hi and d not in bulk
        add(y, cell, d, "vietstock-lastupdate", "high" if plausible else "low", pref, title)

    # --- in-PDF signing dates (2009-2012): the date printed in the report itself
    for (y, cell), iso in scrape_pdf_signing(cafef_raw, vdocs_raw).items():
        add(int(y), cell, date.fromisoformat(iso), "pdf-signing", "high", 0,
            "in-document signing date (ngày…tháng…năm)")

    # --- manual overrides: user-supplied dates win over everything
    for y, cell, d, note in load_manual_overrides():
        add(y, cell, d, "manual", "manual", 0, note or "manual override")

    # priority: confidence -> report type (pref) -> source -> earliest date
    SRC_RANK = {"manual": -1, "pdf-signing": 0, "vietstock-news": 1,
                "cafef-filename": 2, "vietstock-lastupdate": 3}
    CONF_RANK = {"manual": -1, "high": 0, "approx": 1, "low": 2}

    def choose(y, cell):
        lst = cand.get((y, cell))
        if not lst:
            return None
        # confidence first: never prefer a known bad (backfill) date over a plausible
        # real one, even if the latter is a weaker report-type match (pref).
        return sorted(lst, key=lambda r: (
            CONF_RANK[r["confidence"]], r["pref"], SRC_RANK[r["source"]], r["date"]))[0]

    # report each cell uses, and its assurance level (Unaudited < Reviewed < Audited)
    REPORT_USED = {
        "Q1": ("quarterly Q1", "Unaudited"),
        "Q2": ("semi-annual (H1)", "Reviewed"),
        "Q3": ("quarterly Q3", "Unaudited"),
        "Q4": ("quarterly Q4", "Unaudited"),
        "FY": ("whole-year", "Audited"),
    }
    detail = []
    for y in range(2009, date.today().year + 1):
        for cell in CELLS:
            ch = choose(y, cell)
            report, level = REPORT_USED[cell]
            if ch and ch["confidence"] == "low":
                # only a bulk-reupload (backfill) date exists -> not a real disclosure
                # day; report it as unavailable rather than emit a wrong date.
                detail.append({"year": y, "cell": cell, "publish_date": "",
                               "report_used": report, "assurance": level,
                               "confidence": "unavailable", "source": ch["source"],
                               "evidence": f"no disclosure found; backfill upload "
                                           f"{ch['date'].isoformat()} ({ch['evidence']})"})
            else:
                detail.append({"year": y, "cell": cell,
                               "publish_date": ch["date"].isoformat() if ch else "",
                               "report_used": report, "assurance": level,
                               "confidence": ch["confidence"] if ch else "missing",
                               "source": ch["source"] if ch else "",
                               "evidence": ch["evidence"] if ch else ""})

    # --- wide deliverable: year, Q1, Q2, Q3, Q4, final_year -------------------
    by = {(d["year"], d["cell"]): d for d in detail}
    wide = HERE / "vcb_quarter_publish_dates.csv"
    with wide.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["year", "Q1", "Q2", "Q3", "Q4", "final_year"])
        for y in range(2009, date.today().year + 1):
            w.writerow([y] + [by[(y, c)]["publish_date"] for c in CELLS])

    # --- companion detail (provenance + confidence + assurance level) ---------
    dfile = HERE / "vcb_quarter_publish_dates_detail.csv"
    with dfile.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["year", "cell", "publish_date", "report_used",
                                          "assurance", "confidence", "source", "evidence"])
        w.writeheader()
        w.writerows(detail)

    filled = sum(1 for d in detail if d["publish_date"])
    print(f"wrote {wide.name} (year × Q1..Q4,final_year) and {dfile.name}; "
          f"{filled}/{len(detail)} cells dated")
    return detail


if __name__ == "__main__":
    build()
