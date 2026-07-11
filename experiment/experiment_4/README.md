# Experiment 4 — VCB financial-report publish (disclosure) dates

Goal: VCB financial-statement **publish/disclosure dates**, **2009 → present**, for
point-in-time / look-ahead-safe modelling.

Produced by a single script [scrape_vcb_publish_dates.py](scrape_vcb_publish_dates.py):
- [vcb_quarter_publish_dates.csv](vcb_quarter_publish_dates.csv) — **6-column deliverable**
  `year, Q1, Q2, Q3, Q4, final_year`.
- [vcb_quarter_publish_dates_detail.csv](vcb_quarter_publish_dates_detail.csv) — long form
  with `assurance`, `confidence`, `source`, `evidence` per cell.

## Which report each column uses (Unaudited < Reviewed < Audited)

The **Q4 quarterly report** and the **audited final-year report** are *different documents*:
the Q4 report is unaudited and comes out ~late Jan; the audited annual comes months later
and its figures can differ. So they are separate columns:

| Column | Report | Assurance | Typically disclosed |
|---|---|---|---|
| Q1 | quarterly Q1 | Unaudited | late Apr |
| **Q2** | semi-annual (H1), *soát xét* | **Reviewed** | mid-Aug |
| Q3 | quarterly Q3 | Unaudited | late Oct |
| Q4 | quarterly Q4 | Unaudited | late Jan / early Feb (next yr) |
| **final_year** | whole-year (năm), *kiểm toán* | **Audited** | late Mar / Apr (next yr) |

e.g. for FY-2025: `Q4 = "BCTC hợp nhất quý 4 năm 2025"` → **2026-01-30**, while
`final_year = "BCTC hợp nhất năm 2025 (đã kiểm toán)"` → **2026-03-27**.

## Sources (no login) and why more than one is needed

| # | Source | Endpoint | Gives | Reliable era |
|---|---|---|---|---|
| 1 | **Vietstock news** | `POST finance.vietstock.vn/View/PagingNewsContent` (per-year windows) | the HOSE disclosure-announcement article; its **article date = disclosure date** | **2009 → now** (authoritative) |
| 2 | **In-PDF signing date** | the report PDF itself (CafeF `/YYYY/…pdf` or Vietstock `static2.vietstock.vn`) | the printed **"Hà Nội, ngày DD tháng MM năm YYYY"** signing line | any year the PDF has a text layer |
| 3 | Vietstock docs | `POST finance.vietstock.vn/data/getdocument` (cookie + `__RequestVerificationToken`) | doc `LastUpdate` (upload≈disclosure) | ~2013 → now |
| 4 | CafeF | `GET cafef.vn/du-lieu/Ajax/PageNew/FileBCTC.ashx?Symbol=vcb&Type=1&Year=0` | exact `YYYYMMDD` in the PDF filename | 2022 → now |

The file/document APIs only keep a *later re-upload* date for old reports (many 2009–2012
quarters share one bulk date like `2012-08-28`), so they **cannot** date the early period.
Two things recover it: the **news feed** (dated HOSE announcement) and, best of all, the
**date printed inside the report PDF** — read with PyMuPDF, tolerant of the legacy TCVN3
font that garbles the letters (`ngμy 20 th¸ng 07 n¨m 2009`) while the digits stay ASCII.
The signing date is taken as the **latest** printed date after the period-end (the report is
signed after all its reporting/comparative dates). The script merges all sources, preferring
per cell: highest confidence → correct report type → most authoritative source (in-PDF >
news > CafeF > Vietstock upload) → earliest date. A known bulk-backfill never wins over a
plausible real date.

## Confidence (`source` column shows which)

- **high** — an in-PDF signing date (`pdf-signing`), a real disclosure article
  (`vietstock-news`), an exact CafeF filename, or a plausible Vietstock upload.
- **approx*** — from the earnings-result news article (media report ≈ disclosure day, ±days),
  used only for a few 2010–2012 quarters whose PDF is a `.rar` / not republished as an article.
- **n/a** — unrecoverable: VCB listed on HOSE only 30/06/2009 and its 2009–2010 audited
  annuals (State-Audited, long delays) survive only as **scanned image PDFs with no text
  layer** — would need OCR.

## Result (`*` approximate · `ᴾ` read from the PDF signing line · `n/a` unavailable · `—` future)

| Year | Q1 | Q2 | Q3 | Q4 | final_year |
|---|---|---|---|---|---|
| 2009 | 2009-06-17 | 2009-07-20ᴾ | n/a | 2010-02-09* | n/a |
| 2010 | 2010-04-26* | 2010-07-27* | 2010-10-20ᴾ | 2011-02-18ᴾ | n/a |
| 2011 | 2011-05-12ᴾ | 2011-08-19ᴾ | 2011-11-15ᴾ | 2012-02-20ᴾ | 2012-03-30 |
| 2012 | 2012-04-24 | 2012-07-20* | 2012-10-22 | 2013-01-21 | 2013-03-25 |
| 2013 | 2013-04-25 | 2013-08-15 | 2013-11-14 | 2014-02-18 | 2014-03-21 |
| 2014 | 2014-05-14 | 2014-08-13 | 2014-11-13 | 2015-02-15 | 2015-04-01 |
| 2015 | 2015-05-15 | 2015-08-14 | 2015-11-13 | 2016-01-20 | 2016-03-17 |
| 2016 | 2016-05-04 | 2016-08-15 | 2016-10-20 | 2017-01-20 | 2017-03-30 |
| 2017 | 2017-04-21 | 2017-08-14 | 2017-10-20 | 2018-01-23 | 2018-04-02 |
| 2018 | 2018-04-20 | 2018-07-24 | 2018-10-23 | 2019-01-22 | 2019-04-01 |
| 2019 | 2019-04-22 | 2019-08-15 | 2019-10-22 | 2020-01-21 | 2020-03-16 |
| 2020 | 2020-04-22 | 2020-08-14 | 2020-10-22 | 2021-01-21 | 2021-04-01 |
| 2021 | 2021-05-04 | 2021-08-13 | 2021-10-29 | 2022-02-07 | 2022-03-31 |
| 2022 | 2022-05-04 | 2022-09-07 | 2022-10-28 | 2023-01-31 | 2023-04-03 |
| 2023 | 2023-05-04 | 2023-08-14 | 2023-10-30 | 2024-01-31 | 2024-04-01 |
| 2024 | 2024-05-02 | 2024-08-14 | 2024-10-31 | 2025-01-24 | 2025-03-31 |
| 2025 | 2025-04-29 | 2025-10-06 | 2025-10-30 | 2026-01-30 | 2026-03-27 |
| 2026 | 2026-04-29 | — | — | — | — |

**79 high-confidence · 4 approximate · 3 unavailable** (+4 future 2026 cells). The 3 `n/a`
are the 2009 Q3 audited quarterly and the 2009/2010 audited annuals — scanned image PDFs
with no text layer (would need OCR).

## Adding dates manually

For cells that are `n/a` (or any date you want to correct), edit
[vcb_manual_overrides.csv](vcb_manual_overrides.csv) and re-run the script — manual dates
**win over every scraped source**. Columns: `year, cell, publish_date, note`.

```csv
year,cell,publish_date,note
2009,Q3,2009-10-30,from PDF cover date
2009,final_year,2010-05-28,audited annual
```

- `cell` is one of `Q1 Q2 Q3 Q4 FY` (`final_year` also accepted).
- `publish_date` is `YYYY-MM-DD`; rows with a blank date are ignored (the file ships with
  the 3 unavailable cells pre-listed so you only fill the date).
- In the detail CSV such rows show `source = manual`, `confidence = manual`.

```bash
python scrape_vcb_publish_dates.py   # picks up the overrides (cached scrape, fast)
```

## Files & dependency
- `scrape_vcb_publish_dates.py` — the one scraper. Stdlib only **except PyMuPDF**
  (`pip install pymupdf`) for the in-PDF signing dates; if it is absent the PDF step is
  skipped and everything else still runs. It fetches from the network and writes raw JSON
  caches (`vcb_*.json`, gitignored); a later run reuses those caches unless `--refresh`.
- `vcb_quarter_publish_dates.csv` — **deliverable** (wide): `year, Q1, Q2, Q3, Q4, final_year`.
- `vcb_quarter_publish_dates_detail.csv` — long: `year, cell, publish_date, report_used,
  assurance, confidence, source, evidence`.
- `vcb_manual_overrides.csv` — hand-entered dates (top priority); see above.

## Reproduce
```bash
pip install pymupdf
python scrape_vcb_publish_dates.py            # fetches (writes local vcb_*.json caches)
python scrape_vcb_publish_dates.py --refresh  # re-scrape everything
```
The raw `vcb_*.json` caches are regenerated locally on first run and are not tracked; the
committed deliverable CSVs already contain the final dates.
