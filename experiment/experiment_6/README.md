# Experiment 6 — VCB company-news / disclosure headlines (categorised)

**Goal.** Scrape VCB's full **company-news & disclosure headline stream** from CafeF,
tagged by category and dated, 2008 → present. Third point-in-time orthogonal-data
piece after experiment_4 (disclosure *calendar*) and experiment_5 (shares-outstanding):
a dated, categorised event feed that can be joined to prices without look-ahead
(headline counts, event flags, sentiment features, earnings/dividend-announcement dates).

## Source

Page:
[du-lieu/tin-doanh-nghiep/vcb/event.chn](https://cafef.vn/du-lieu/tin-doanh-nghiep/vcb/event.chn).
The category tabs (`#a0..#a5`) all call one AJAX endpoint returning an HTML fragment
(a `<ul>` of `<li>` headlines):

```
GET cafef.vn/du-lieu/Ajax/Events_RelatedNews_New.aspx
    ?symbol=VCB&floorID=0&configID=<0-5>&PageIndex=<n>&PageSize=30&Type=2
```

- `configID` = the tab / news category (below); `floorID` 0=all, 1=HSX, 2=HNX.
- `PageSize` is **capped at 30** → paginate `PageIndex` until a page comes back empty.
- Each `<li>`: a `timeTitle` span (`DD/MM/YYYY HH:MM`) + a `docnhanhTitle` anchor
  (headline + article URL). The title is read from the anchor **inner text**, not the
  `title=""` attribute — some legacy headlines embed unescaped quotes that truncate it.

## Categories (the tabs)

| configID | tab id | label | meaning |
|---|---|---|---|
| 0 | a0 | Tất cả | all (union of the rest + uncategorised general news) |
| 1 | a1 | Tình hình SXKD & Phân tích khác | business results & analysis |
| 2 | a2 | Trả cổ tức - Chốt quyền | dividends / record date |
| 3 | a3 | Thay đổi nhân sự | personnel changes |
| 4 | a4 | Tăng vốn - Cổ phiếu quỹ | capital increase / treasury shares |
| 5 | a5 | GD cổ đông lớn & Cổ đông nội bộ | major & insider shareholder transactions |

**Method:** scrape 1..5 first so each headline gets its true category, then scrape 0
and backfill any headline not in a specific category (tagged category 0). Dedup by
article URL.

## Result (as of 2026-07-10)

**1,629 unique headlines, 2008-01-01 → 2026-07-09.**

| category | n |
|---|---:|
| 1 · Business results & analysis | 770 |
| 5 · Major & insider shareholder txns | 101 |
| 3 · Personnel changes | 88 |
| 4 · Capital increase / treasury | 71 |
| 2 · Dividends / record date | 38 |
| 0 · Uncategorised (general news) | 561 |

Categories 2/4 line up with experiment_5's corporate actions and experiment_4's
disclosure cadence — the three feeds cross-validate each other.

## Files

- `scrape_vcb_news.py` — one stdlib script. `--refresh` re-hits CafeF; otherwise the
  cached `raw_html/` fragments are reused.
- `vcb_news.csv` — `datetime, date, category_id, category, news_id, title, url`.
- `vcb_news_categories.csv` — `category_id, category, n_items`.
- `raw_html/` — raw fragment cache (gitignored; regenerate with `--refresh`).

## Notes / extending

- `symbol=VCB` and the `event.chn` referer are the only ticker-specific bits — set
  `SYMBOL` to scrape any listed code.
- Headlines link to full articles (the `.chn` URL); article-body scraping (for NLP /
  sentiment) is a downstream step, not done here.
- `news_id` is the numeric id in the article URL where present (some disclosure-filing
  links use a `VCB-<id>` path instead and have no standalone news_id).
