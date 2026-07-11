# Experiment 6 — VCB company-news / disclosure feed with content

**Goal.** Scrape VCB's full **company-news & disclosure stream** from CafeF — headline,
category, and **article content** — 2008 → present. Third point-in-time orthogonal-data
piece after experiment_4 (disclosure *calendar*) and experiment_5 (shares-outstanding):
a dated, categorised event feed with text, joinable to prices without look-ahead
(event flags, headline counts, sentiment / NLP features, announcement dates).

One script, one table:
- `scrape_vcb_news.py` — stdlib only, no login. `--limit N` samples a few rows.
- `vcb_news.csv` — the deliverable (**1,629 rows**, 2008-01 → 2026-07).

## How it works (two stages, one script)

**1. List** — the CafeF news tabs (`#a0..#a5`) all call one AJAX endpoint returning an
HTML fragment of headlines:

```
Events_RelatedNews_New.aspx?symbol=VCB&floorID=0&configID=<0-5>&PageIndex=<n>&PageSize=30&Type=2
```

`configID` = the news category; `PageSize` caps at 30 → paginate until empty. Scrape
categories 1..5 first (true category), backfill 0 (uncategorised), dedup by URL.

**2. Fetch** — open each headline. Both page types are static and carry a JSON-LD
`NewsArticle` block for metadata:
- **editorial** (`/<slug>-<id>.chn`) — `content` = full `<p>` body text (a `<br>`-body
  fallback recovers pre-2015 articles).
- **disclosure** (`/du-lieu/VCB-<id>/…`) — `content` = the on-page summary; `pdf_url` =
  the attached HOSE filing PDF (recorded, **not** downloaded). These URLs 301-redirect
  uppercase→lowercase; urllib follows it.

## Columns

| column | meaning |
|---|---|
| `order` | 1-based chronological, **oldest = 1** (rows sorted oldest → newest) |
| `timestamp` | article's own published/modified time, else the listing time (`YYYY-MM-DD HH:MM:SS`) |
| `type` | `editorial` (journalist article) · `disclosure` (official HOSE filing) · `error` (dead 404 link) |
| `headline` | title (JSON-LD, entity-decoded; falls back to the listing title) |
| `category` | one of the six below |
| `content` | article body (editorial) or filing summary (disclosure) |
| `url` | the CafeF article URL |
| `pdf_url` | attached filing PDF link (disclosures only; empty otherwise) |

### Categories (`configID` → english snake_case)

| category | tab | meaning |
|---|---|---|
| `general_uncategorized` | Tất cả (residual) | general market/news mentions of VCB not in a specific event type |
| `business_results_and_analysis` | Tình hình SXKD & Phân tích khác | operating/earnings results + catch-all for many official filings |
| `dividends_and_record_date` | Trả cổ tức - Chốt quyền | dividend payments + ex-/record-date (GDKHQ) |
| `personnel_changes` | Thay đổi nhân sự | board / supervisory / senior-management appointments & resignations |
| `capital_increase_and_treasury_shares` | Tăng vốn - Cổ phiếu quỹ | charter-capital increases, share issuance/listing changes, treasury/buyback |
| `major_and_insider_shareholder_transactions` | GD cổ đông lớn & Cổ đông nội bộ | trades by major (>5%) and insider/related-party shareholders |

`type` and `category` are **orthogonal** (provenance vs topic) — the cross-tab shows
neither is derivable from the other (e.g. `business_results_and_analysis` splits
242 editorial / 528 disclosure), so both are kept. Filter `type == disclosure` for the
clean, look-ahead-safe official-event stream, then bucket by `category`.

## Result (as of 2026-07)

**1,629 rows** — 896 editorial · 727 disclosure · 6 error (dead 404 links). 702 rows carry
a `pdf_url`. Content populated for all but 3 editorials (video/photo pages with no text).
Categories: business_results_and_analysis 770 · general_uncategorized 561 ·
major_and_insider_shareholder_transactions 101 · personnel_changes 88 ·
capital_increase_and_treasury_shares 71 · dividends_and_record_date 38.

## Notes / extending

- `symbol=VCB` + the `event.chn` referer are the only ticker-specific bits → set `SYMBOL`
  to scrape any listed code.
- No local cache: a re-run re-fetches all pages (~a few minutes). PDFs are referenced by
  URL only — download separately (PyMuPDF, as experiment_4 does) if the filing text is needed.
