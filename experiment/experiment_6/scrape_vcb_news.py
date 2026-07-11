"""
Scrape VCB company news from CafeF into a single table:

    order, timestamp, type, headline, category, content, url, pdf_url

Two stages, one script:
  1. LIST  — the CafeF news tabs (#a0..#a5) all call one AJAX endpoint that returns an
             HTML fragment of headlines:
               Events_RelatedNews_New.aspx?symbol=VCB&floorID=0&configID=<0-5>
                                          &PageIndex=<n>&PageSize=30&Type=2
             configID is the news category; PageSize caps at 30 → paginate until empty.
             Scrape categories 1..5 (true category), then backfill 0 (uncategorised);
             dedup by article URL.
  2. FETCH — open each headline (both page types are static, both carry a JSON-LD
             NewsArticle block for metadata):
               editorial  (/<slug>-<id>.chn)      content = full <p> body text
               disclosure (/du-lieu/VCB-<id>/…)    content = on-page summary; pdf_url =
                          the attached HOSE filing PDF (recorded, NOT downloaded).
             Disclosure URLs 301-redirect uppercase→lowercase; urllib follows it.

Columns
  type      editorial (journalist article) | disclosure (official HOSE filing) | error (404)
  category  general_uncategorized | business_results_and_analysis | dividends_and_record_date
            | personnel_changes | capital_increase_and_treasury_shares
            | major_and_insider_shareholder_transactions
  timestamp article's own published/modified time, else the listing time (YYYY-MM-DD HH:MM:SS)

Self-contained, stdlib only. `--limit N` samples a few rows. Output: vcb_news.csv
"""

from __future__ import annotations

import csv
import html
import json
import re
import sys
import time
import urllib.request
from datetime import date
from pathlib import Path

HERE = Path(__file__).parent
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
SYMBOL = "VCB"
LIST_ENDPOINT = "https://cafef.vn/du-lieu/Ajax/Events_RelatedNews_New.aspx"
LIST_REFERER = f"https://cafef.vn/du-lieu/tin-doanh-nghiep/{SYMBOL.lower()}/event.chn"
PAGE_SIZE = 30
MAX_PAGES = 300  # safety stop (~9000 items)

LIMIT = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else None
OUTPUT = "vcb_news.csv"

# configID (CafeF tab) -> english snake_case category
CATEGORY_EN = {
    0: "general_uncategorized",
    1: "business_results_and_analysis",
    2: "dividends_and_record_date",
    3: "personnel_changes",
    4: "capital_increase_and_treasury_shares",
    5: "major_and_insider_shareholder_transactions",
}

# ---------------------------------------------------------------------------
# stage 1 — headline listing
# ---------------------------------------------------------------------------
# <li>: <span class="timeTitle">DD/MM/YYYY HH:MM</span> ... <a ...>TITLE</a>
# Title from the anchor inner text (the title="" attribute breaks on legacy quotes).
_ITEM_RE = re.compile(
    r'timeTitle">\s*(?P<dt>\d{2}/\d{2}/\d{4}(?:\s+\d{2}:\d{2})?)\s*</span>'
    r'.*?<a\s+class=[\'"]docnhanhTitle[\'"]\s+href="(?P<url>[^"]+)"[^>]*>(?P<title>.*?)</a>',
    re.S,
)
_TAG_RE = re.compile(r"<[^>]+>")


def _text(s: str) -> str:
    s = re.sub(r"<(script|style)\b.*?</\1>", " ", s, flags=re.S | re.I)
    return re.sub(r"\s+", " ", html.unescape(_TAG_RE.sub(" ", s))).strip()


def _fetch_list(config_id: int, page: int) -> str:
    url = (f"{LIST_ENDPOINT}?symbol={SYMBOL}&floorID=0&configID={config_id}"
           f"&PageIndex={page}&PageSize={PAGE_SIZE}&Type=2")
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": LIST_REFERER})
    with urllib.request.urlopen(req, timeout=30) as r:
        html_txt = r.read().decode("utf-8")
    time.sleep(0.2)
    return html_txt


def _clean_url(u: str) -> str:
    u = re.sub(r"[?&]utm_source=[^&]*", "", u)
    return "https://cafef.vn" + u if u.startswith("/") else u


def _parse_list(html_txt: str, config_id: int) -> list[dict]:
    out = []
    for m in _ITEM_RE.finditer(html_txt):
        dt = re.sub(r"\s+", " ", m.group("dt")).strip()
        out.append({
            "datetime": dt,
            "category_id": config_id,
            "title": _text(m.group("title")),
            "url": _clean_url(m.group("url")),
        })
    return out


def _scrape_category(config_id: int) -> list[dict]:
    items, seen = [], set()
    for page in range(1, MAX_PAGES + 1):
        rows = _parse_list(_fetch_list(config_id, page), config_id)
        fresh = [r for r in rows if r["url"] not in seen]
        if not rows or not fresh:
            break
        for r in fresh:
            seen.add(r["url"])
            items.append(r)
        if len(rows) < PAGE_SIZE:
            break
    return items


def build_headlines() -> list[dict]:
    by_url: dict[str, dict] = {}
    for cid in (1, 2, 3, 4, 5):          # specific categories first -> true category
        for r in _scrape_category(cid):
            by_url.setdefault(r["url"], r)
    for r in _scrape_category(0):         # backfill general/uncategorised
        by_url.setdefault(r["url"], r)

    def key(r):                           # chronological sort: yyyy, mm, dd, hh:mm
        d = r["datetime"].split(" ")
        dd, mm, yy = d[0].split("/")
        return (yy, mm, dd, d[1] if len(d) > 1 else "")

    return sorted(by_url.values(), key=key, reverse=True)


# ---------------------------------------------------------------------------
# stage 2 — article content
# ---------------------------------------------------------------------------
def _fetch_article(url: str) -> tuple[str, str]:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": LIST_REFERER})
    with urllib.request.urlopen(req, timeout=30) as r:   # follows the 301 redirect
        body = r.read().decode("utf-8", "replace")
        final = r.geturl()
    time.sleep(0.2)
    return body, final


def _balanced_div(h: str, attr: str, val: str) -> str | None:
    m = re.search(r'<div[^>]*' + attr + r'="[^"]*' + re.escape(val) + r'[^"]*"[^>]*>', h, re.I)
    if not m:
        return None
    i, depth = m.end(), 1
    for mm in re.finditer(r"<div\b|</div>", h[i:], re.I):
        depth += 1 if mm.group().lower() == "<div" else -1
        if depth == 0:
            return h[i:i + mm.start()]
    return h[i:]


def _jsonld(h: str) -> dict:
    for ld in re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', h, re.S):
        try:
            d = json.loads(ld)
        except Exception:
            continue
        for it in (d if isinstance(d, list) else [d]):
            if isinstance(it, dict) and "NewsArticle" in str(it.get("@type", "")):
                return it
    return {}


def _norm_ts(pub: str, mod: str, listing_dt: str) -> str:
    for s in (pub, mod):
        if s:
            s = s[:19].replace("T", " ")
            return s if len(s) == 19 else (s + ":00")
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})(?:\s+(\d{2}):(\d{2}))?", listing_dt or "")
    if m:
        d, mo, y, hh, mm = m.groups()
        return f"{y}-{mo}-{d} {hh or '00'}:{mm or '00'}:00"
    return listing_dt or ""


def extract(row: dict) -> dict:
    url = row["url"]
    h, final = _fetch_article(url)
    meta = _jsonld(h)
    is_disc = "/du-lieu/vcb-" in final.lower()
    headline = html.unescape(html.unescape(meta.get("headline", ""))) or row["title"]

    pdf_url = ""
    if is_disc:
        content = _text(_balanced_div(h, "id", "divContent") or "")
        pdfs = sorted(set(re.findall(r'https?://[^"]*mediacdn[^"]*\.pdf', h, re.I)))
        pdf_url = pdfs[0] if pdfs else ""
    else:
        inner = _balanced_div(h, "class", "detail-content") or ""
        paras = [_text(p) for p in re.findall(r"<p\b[^>]*>(.*?)</p>", inner, re.S)]
        content = " ".join(p for p in paras if p)
        if len(content) < 50:  # legacy articles: body is <br>-separated, not in <p>
            full = _text(inner)
            m = re.search(r"TIN M[ỚO]I\s+", full)  # cut the leading price-quote widget
            content = full[m.end():].strip() if m else full

    return {
        "timestamp": _norm_ts(meta.get("datePublished", ""), meta.get("dateModified", ""),
                              row.get("datetime", "")),
        "type": "disclosure" if is_disc else "editorial",
        "headline": headline,
        "category": CATEGORY_EN.get(row["category_id"], "general_uncategorized"),
        "content": content,
        "url": url,
        "pdf_url": pdf_url,
    }


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    headlines = build_headlines()          # newest first
    print(f"listed {len(headlines)} unique headlines")
    if LIMIT:
        headlines = headlines[:LIMIT]      # sample the most-recent N
    headlines = headlines[::-1]            # oldest first -> order 1 = oldest news

    cols = ["order", "timestamp", "type", "headline", "category", "content", "url", "pdf_url"]
    results, n_pdf = [], 0
    for i, r in enumerate(headlines, 1):
        try:
            rec = extract(r)
            rec["order"] = i
            n_pdf += bool(rec["pdf_url"])
        except Exception as e:
            rec = {"order": i, "timestamp": _norm_ts("", "", r.get("datetime", "")),
                   "type": "error", "headline": r.get("title", ""),
                   "category": CATEGORY_EN.get(r["category_id"], "general_uncategorized"),
                   "content": f"[ERROR: {e}]", "url": r["url"], "pdf_url": ""}
        results.append(rec)
        if i % 50 == 0 or i == len(headlines):
            print(f"[{i}/{len(headlines)}] pdf_urls={n_pdf}  last={rec['type']}: {rec['headline'][:50]}")

    with (HERE / OUTPUT).open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(results)
    print(f"\nwrote {OUTPUT}: {len(results)} rows, {n_pdf} with pdf_url")


if __name__ == "__main__":
    main()
