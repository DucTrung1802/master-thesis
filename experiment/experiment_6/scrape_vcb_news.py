"""
Scrape VCB company-news / disclosure headlines from CafeF, categorised, into a
single CSV.  Self-contained: no login, stdlib only.

Source
------
Page:  https://cafef.vn/du-lieu/tin-doanh-nghiep/vcb/event.chn
The category tabs (#a0..#a5) all call one AJAX endpoint that returns an HTML
fragment (a <ul> of <li> headlines):

    GET cafef.vn/du-lieu/Ajax/Events_RelatedNews_New.aspx
        ?symbol=VCB&floorID=0&configID=<0-5>&PageIndex=<n>&PageSize=30&Type=2

`configID` is the tab / news category; `floorID` 0=all,1=HSX,2=HNX; PageSize is
capped at 30, so we paginate PageIndex until a page comes back empty.

Categories (configID -> tab label)
    0  Tất cả                              (all — union of the rest, + a few uncategorised)
    1  Tình hình SXKD & Phân tích khác     (business results & analysis)
    2  Trả cổ tức - Chốt quyền             (dividends / record date)
    3  Thay đổi nhân sự                    (personnel changes)
    4  Tăng vốn - Cổ phiếu quỹ            (capital increase / treasury shares)
    5  GD cổ đông lớn & Cổ đông nội bộ    (major & insider shareholder transactions)

Method: scrape 1..5 first (each item gets its true category), then scrape 0 and add
any headline not already seen (tagged category 0). Dedup by article URL.

This is the third point-in-time orthogonal-data piece (after experiment_4's
disclosure calendar and experiment_5's shares-outstanding): a dated, categorised
event/news stream for VCB, joinable without look-ahead.

Output
------
    vcb_news.csv             datetime, date, category_id, category, news_id, title, url
    vcb_news_categories.csv  category_id, category, n_items
    raw_html/cfg<c>_p<n>.html   raw fragment cache (reproducibility)
"""

from __future__ import annotations

import csv
import html
import re
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).parent
RAW = HERE / "raw_html"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
REFRESH = "--refresh" in sys.argv

SYMBOL = "VCB"
ENDPOINT = "https://cafef.vn/du-lieu/Ajax/Events_RelatedNews_New.aspx"
PAGE_SIZE = 30
MAX_PAGES = 300  # safety stop (~9000 items)

CATEGORIES = {
    0: "Tất cả",
    1: "Tình hình SXKD & Phân tích khác",
    2: "Trả cổ tức - Chốt quyền",
    3: "Thay đổi nhân sự",
    4: "Tăng vốn - Cổ phiếu quỹ",
    5: "GD cổ đông lớn & Cổ đông nội bộ",
}

# one <li>: <span class="timeTitle">DD/MM/YYYY HH:MM</span> ... <a ... href="URL" ...>TITLE</a>
# Title is read from the anchor's inner text (the title="" attribute is unreliable —
# some legacy headlines embed unescaped double-quotes that truncate the attribute).
_ITEM_RE = re.compile(
    r'timeTitle">\s*(?P<dt>\d{2}/\d{2}/\d{4}(?:\s+\d{2}:\d{2})?)\s*</span>'
    r'.*?<a\s+class=[\'"]docnhanhTitle[\'"]\s+href="(?P<url>[^"]+)"[^>]*>(?P<title>.*?)</a>',
    re.S,
)
_TAG_RE = re.compile(r"<[^>]+>")


def _clean_title(t: str) -> str:
    return html.unescape(_TAG_RE.sub("", t)).strip()


def _fetch(config_id: int, page: int) -> str:
    RAW.mkdir(exist_ok=True)
    cache = RAW / f"cfg{config_id}_p{page}.html"
    if cache.exists() and not REFRESH:
        return cache.read_text(encoding="utf-8")
    url = (
        f"{ENDPOINT}?symbol={SYMBOL}&floorID=0&configID={config_id}"
        f"&PageIndex={page}&PageSize={PAGE_SIZE}&Type=2"
    )
    req = urllib.request.Request(
        url,
        headers={"User-Agent": UA,
                 "Referer": f"https://cafef.vn/du-lieu/tin-doanh-nghiep/{SYMBOL.lower()}/event.chn"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        html = r.read().decode("utf-8")
    cache.write_text(html, encoding="utf-8")
    time.sleep(0.2)  # be polite
    return html


def _clean_url(u: str) -> str:
    u = re.sub(r"[?&]utm_source=[^&]*", "", u)
    return "https://cafef.vn" + u if u.startswith("/") else u


def _news_id(url: str) -> str:
    m = re.search(r"-(\d{6,})\.chn", url)
    return m.group(1) if m else ""


def _parse(html: str, config_id: int) -> list[dict]:
    out = []
    for m in _ITEM_RE.finditer(html):
        dt = re.sub(r"\s+", " ", m.group("dt")).strip()
        date = dt.split(" ")[0]
        url = _clean_url(m.group("url"))
        out.append(
            {
                "datetime": dt,
                "date": date,
                "category_id": config_id,
                "category": CATEGORIES[config_id],
                "news_id": _news_id(url),
                "title": _clean_title(m.group("title")),
                "url": url,
            }
        )
    return out


def _scrape_category(config_id: int) -> list[dict]:
    """Paginate a category until a page is empty or repeats already-seen items."""
    items, seen = [], set()
    for page in range(1, MAX_PAGES + 1):
        rows = _parse(_fetch(config_id, page), config_id)
        fresh = [r for r in rows if r["url"] not in seen]
        if not rows or not fresh:  # empty page, or clamped/repeated last page -> stop
            break
        for r in fresh:
            seen.add(r["url"])
            items.append(r)
        if len(rows) < PAGE_SIZE:  # last partial page
            break
    return items


def _sort_key(r: dict):
    d = r["date"].split("/")
    t = (r["datetime"].split(" ") + [""])[1]
    return (d[2], d[1], d[0], t)  # yyyy, mm, dd, hh:mm -> chrono


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    by_url: dict[str, dict] = {}

    # 1..5 first: assign the true category
    for cid in [1, 2, 3, 4, 5]:
        rows = _scrape_category(cid)
        for r in rows:
            by_url.setdefault(r["url"], r)
        print(f"configID={cid} {CATEGORIES[cid]:<38} {len(rows):>4} items")

    # 0 (all): backfill headlines not present in any specific category
    all_rows = _scrape_category(0)
    added = 0
    for r in all_rows:
        if r["url"] not in by_url:
            by_url[r["url"]] = r  # keep category 0 = uncategorised/other
            added += 1
    print(f"configID=0 {CATEGORIES[0]:<38} {len(all_rows):>4} items ({added} not in 1..5)")

    rows = sorted(by_url.values(), key=_sort_key, reverse=True)

    with (HERE / "vcb_news.csv").open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["datetime", "date", "category_id", "category", "news_id", "title", "url"],
        )
        w.writeheader()
        w.writerows(rows)

    counts = {cid: sum(r["category_id"] == cid for r in rows) for cid in CATEGORIES}
    with (HERE / "vcb_news_categories.csv").open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["category_id", "category", "n_items"])
        for cid, name in CATEGORIES.items():
            w.writerow([cid, name, counts[cid]])

    span = f"{rows[-1]['date']} .. {rows[0]['date']}" if rows else "-"
    print(f"\nTOTAL {len(rows)} unique headlines, {span}")
    print("wrote vcb_news.csv, vcb_news_categories.csv")


if __name__ == "__main__":
    main()
