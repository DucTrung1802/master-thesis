# Experiment 7 — Financial statements (balance sheet / income statement / cash flow)

**Goal.** Scrape a listed company's **full quarterly financial-statement history** from
CafeF — every line item of all three statements, 2008 → present. Fourth orthogonal-data
piece (after exp_4 disclosure calendar, exp_5 shares-outstanding, exp_6 news/events): the
fundamentals themselves, which — joined to exp_4's publish dates — become point-in-time safe.

## Any ticker, any sector — 6 generic files

```bash
python scrape_financials.py                 # default: VCB
python scrape_financials.py --symbol FPT    # any code on HOSE / HNX / UPCOM / OTC
```

Nothing is named after a ticker — **the ticker is a column, not a filename**. There are
exactly **6 files**, shared by every ticker:

| file | purpose |
|---|---|
| `balance_sheet.csv` · `income_statement.csv` · `cash_flow.csv` | **deliverables** |
| `balance_sheet_manual.csv` · `income_statement_manual.csv` · `cash_flow_manual.csv` | **fill in by hand** (missing quarters) |
| `scrape_financials.py` | the one scraper (stdlib only, no login) |

**Scraping a ticker accumulates.** It replaces only that symbol's rows and leaves every
other ticker's alone, so you build a multi-ticker panel one run at a time. Re-running a
ticker is idempotent (no duplicate rows).

**Exchange** is resolved from CafeF's master list (`Search/company.json`, 2,556 codes) via
its `CenterId`: **1=HOSE, 2=HNX, 8=OTC, 9=UPCOM**.

### Sector-agnostic — and why columns are keyed by name, not code

The endpoints and sub-types are identical for banks and non-banks; only the *line items*
differ, and they're read from each ticker's own template, so columns adapt automatically:

| | VCB (bank) | FPT (non-bank) |
|---|---|---|
| balance-sheet items | 85 | 132 |
| income statement starts with | interest income | revenue |
| cash-flow method | **direct** | **indirect** |
| quarters | 71 (from Q3-2008) | 81 (from Q1-2006) |

The same `item_code` means **different things across sectors** — code `1` is *interest
income* for a bank but *revenue* for a non-bank. So line items are keyed by the full
`<code>__<slug>` **column name**, never the bare code: they stay separate columns, and a
bank's row is simply blank in the non-bank columns (and vice-versa). Scraping VCB + FPT
yields 152 rows × 213 line items, sparse across sectors — that's by design.

> Only VCB is committed here. Add any ticker with `--symbol`.

**Quarterly only** — annual rows are dropped (derivable from the quarters, and duplicative).
Every quarter in a ticker's range is present as a row (VCB: **71**, Q3-2008 → Q1-2026) — a
contiguous grid including the ones CafeF omits, so the index has no holes.

## Three layers: manual > pdf > scraped

| layer | file | what it is |
|---|---|---|
| **scraped** | — (CafeF BCTC API) | the base; fast, complete where CafeF has it |
| **pdf** | `<report>_pdf.csv` | read off the company's **actual filing** by `read_pdf.py` |
| **manual** | `<report>_manual.csv` | hand-entered; **beats everything** |

Values merge **cell by cell** in that order, and the `source` column records which layer won:
`scraped` · `pdf` · `manual` · `missing`.

### `read_pdf.py` — reading the filing

```bash
python read_pdf.py --period Q2-2014            # auto-extract (text-layer PDFs)
python read_pdf.py --period Q2-2014 --render   # rasterise pages (scanned PDFs)
python read_pdf.py --period Q2-2014 --list     # list the available documents
```

It finds the consolidated ("hợp nhất") report on CafeF, locates each statement, and — for a
**text-layer** PDF — parses it and writes `<report>_pdf.csv`. Rows are rebuilt from **word
coordinates**, not the raw text stream (PyMuPDF emits label fragments out of order; a
line-based read once silently put *"Chi phí hoạt động khác"* into *"Chi phí hoạt động"*), and
the period columns are found by clustering the x-positions of every number.

Roughly **two-thirds of VCB's older filings are scanned images** with no text layer. For
those, `--render` writes the statement pages to `pdf_pages/` so the figures can be transcribed
by eye/OCR and pasted into `<report>_pdf.csv`.

**Nothing is written unless it reconciles** against the statement's own printed subtotals
(assets = liabilities + equity, PBT = operating profit − provisions, op+inv+fin = net change…).

> ⚠️ **Two traps reconciliation cannot catch** — both balance internally:
> 1. **cumulative vs standalone.** A Q2/Q3/Q4 report prints *both* the quarter and the
>    year-to-date column. The income statement needs the **standalone quarter**; cash flow
>    needs the **cumulative** one. Take the wrong one and you get a number that reconciles
>    perfectly and is still wrong.
> 2. **units.** Most reports are in *Triệu VNĐ* (×10⁶) — but some (VCB's 2009 ones) are in
>    plain *đồng*. A 10⁶ error also reconciles perfectly.
>
> Only a **magnitude check against the neighbouring quarters** catches these. `read_pdf.py`
> runs one before writing; do the same for any hand-fill.

## Manual overrides — hand-filled data always wins

Each report has a `<report>_manual.csv` template, **pre-seeded with exactly the quarters
CafeF is missing** for each ticker you've scraped (keyed by `symbol` + `period`, values blank,
same line-item columns as the deliverable). Fill in any cells you can source from the actual
filings and re-run:

```bash
python scrape_financials.py
```

- **Manual beats scraped, cell by cell** — a non-blank cell in the template always overrides
  whatever the API returned (you can also add a row for a quarter that *was* scraped, to
  correct it).
- **Your entries are never lost**: the template keeps every gap row across re-runs, filled or
  not, so re-running only refreshes the scrape and re-applies your data on top.
- The `source` column in each deliverable says where the row came from:
  `scraped` · `manual` · `missing` (still un-filled).

### VCB: what has been filled, and what cannot be

Read off VCB's own consolidated filings, each statement reconciled against its printed
subtotals **and** magnitude-checked against neighbouring quarters:

| report | source mix (71 quarters) | still missing |
|---|---|---|
| balance_sheet | 66 scraped · 2 pdf · 1 manual | **2** — Q3-2008, Q4-2008 |
| income_statement | 64 scraped · 6 pdf · 1 manual | **0 — COMPLETE** |
| cash_flow | 54 scraped · 14 pdf · 1 manual | **2** — Q3-2008, Q4-2008 |

**Every fillable gap is closed.** The only two left are Q3-2008 and Q4-2008, for which CafeF
has **no document at all** (audited against all 206 documents it lists) — permanently
unfillable from this source.

Each of the 21 quarters read from a filing was reconciled against the statement's own printed
subtotals *and* magnitude-checked against its neighbours. A full re-check over the finished
dataset: **0 continuity breaks** in the cash-flow series and **0 subtotal breaks** among the
PDF-sourced quarters. (One subtotal break remains at **Q4-2023 — in CafeF's own *scraped*
data**, where op+inv+fin does not equal the reported net change. Left as-is rather than
silently "fixed".)

**The auto-parser is not trusted on the older filings.** Their text layer fragments labels, so
it extracts only ~40% of the lines and sometimes mis-assigns one — the reconcile gate rejects
those. Every quarter above was transcribed from the rendered pages instead, which has been
exact every time. Q4-2010/Q1-2011 were auto-filled on an earlier pass and had to be **withdrawn**:
they carried raw-PDF signs (negative expenses) where CafeF stores expenses **positive**.

> ⚠️ **Three traps, all invisible to a reconciliation check** — every one of them still balances
> internally:
> 1. **cumulative vs standalone.** The semi-annual filing prints *only* a 6-month column, with
>    no standalone quarter. Q2-2024's income statement had to be derived as **6M − Q1**; taking
>    the printed figure would have doubled the quarter (PBT 20,835 instead of 10,116).
>    Cash flow is the opposite — CafeF *wants* the cumulative figure.
> 2. **units.** Most filings are in Triệu VNĐ (×10⁶); VCB's 2009 ones are in plain đồng.
> 3. **signs.** CafeF stores income-statement expenses as **positive magnitudes**; the filing
>    prints them in parentheses. (Balance sheet and cash flow keep the filing's signs.)
>
> Only a **magnitude check against neighbouring quarters** catches (1) and (2); `read_pdf.py`
> runs one before writing, and normalises (3).

## Report naming (Vietnamese → standard accounting English)

| CafeF code | Vietnamese | **name used here** | sections |
|---|---|---|---|
| `CDKT` | Cân đối kế toán | **`balance_sheet`** | `assets` (TN), `liabilities_and_equity` (NV) |
| `KQKD` | Kết quả kinh doanh | **`income_statement`** | — (a.k.a. P&L / statement of profit or loss) |
| `LCTT` | Lưu chuyển tiền tệ | **`cash_flow`** | `operating` (HDKD), `investing` (HDDT), `financing` (HDTC) |

> Naming notes: `income_statement` is the standard term for *Kết quả kinh doanh* (P&L).
> For *Lưu chuyển tiền tệ* the correct accounting term is **`cash_flow`** (cash flow
> statement), not "money_flow". `HDKD` — the id in the `#table_HDKD` pager xpath — is the
> cash-flow **operating** section, not a separate report.

## How it works — all history in one call

The page renders each tab via `/du-lieu/Ajax/FinancialAjax.aspx?tab=<candoi|ketqua|luuchuyen>`,
which calls a JSON API. Hitting that API directly avoids the period-pager
(`//*[@id="table_HDKD"]/thead/tr/th[2]` is just pagination):

```
balance_sheet     GET apiweb.cafef.vn/api/v2/BCTC/GetReportCDKT    reportType=TN | NV
income_statement  GET apiweb.cafef.vn/api/v1/BCTC/GetReportDetail  reportType=KQKD
cash_flow         GET apiweb.cafef.vn/api/v1/BCTC/GetReportLCTT    reportType=HDKD | HDDT | HDTC
…&symbol=<TICKER>&pageIndex=1&pageSize=<count>&TypeTime=QUY
```

`value.count` = how many quarters exist for that ticker (VCB 70, FPT 77). The script reads
it first, then requests exactly that many → the whole history in a single call per report.

Two JSON shapes are returned; both are normalised:
- **nested** (CDKT, LCTT): `value.templace[0].data` = line items, `value.data[0].data` = periods
- **flat** (KQKD): `value.templace` = line items, `value.data` = periods

## Layout — wide, one CSV per report

Each file is **rows = periods, columns = line items**:

| column | meaning |
|---|---|
| `symbol` | ticker, e.g. `VCB` |
| `exchange` | `HOSE` · `HNX` · `UPCOM` · `OTC` |
| `period` | e.g. `Q1-2026` |
| `year`, `quarter` | numeric parts |
| `source` | `scraped` · `manual` (hand-filled, wins) · `missing` (still empty) |
| *(the rest)* | one column per line item — values in **VND** |

**Line-item columns are named `<item_code>__<slug>`**, e.g.
`110__tien_mat_vang_bac_da_quy`, `3__thu_nhap_lai_thuan`, `HDKD_2__thu_nhap_lai_…`.
Within one ticker's report the `item_code` is unique, so the header is **stable** (safe to
join on across periods) *and* readable — no separate data dictionary needed. The slug is the
ASCII snake_case of the Vietnamese label with its numbering stripped. Columns stay in
statement order, so a report's natural hierarchy (heading → components) reads left to right.
Across *sectors* the bare code is **not** unique (see above), which is exactly why the full
name — code **and** slug — is the column key.

The `section` is recoverable from the code prefix: balance sheet `1xx…`=assets vs
`3xx/4xx…`=liabilities_and_equity; cash flow `HDKD_`=operating, `HDDT_`=investing,
`HDTC_`=financing.

## Coverage (VCB — the committed example)

**71 quarterly rows**, **Q3-2008 → Q1-2026**, oldest first — a contiguous grid (gaps appear
as `source=missing` rows rather than being skipped). Range and item counts are derived
per ticker.

| file | sections | VCB line items |
|---|---|--:|
| `balance_sheet.csv` | assets + liabilities_and_equity | **85** |
| `income_statement.csv` | — | **26** |
| `cash_flow.csv` | operating + investing + financing | **47** |

**Validated** against the annual figures at scrape time: the 2025 identities hold —
net interest income 58,771 = 105,216 − 46,445; PBT 44,020 = 47,212 − 3,192;
PAT 35,198 = 44,020 − 8,822 (bn VND); and the four quarterly income statements sum exactly
to the annual figure (2022/2023/2025).

## ⚠️ Data caveats (verified against the API — read before modelling)

**1. Quarterly cash flow is CUMULATIVE year-to-date, not standalone.** It resets each
January, so `Q4` = the full year. Verified on `HDKD_2` (interest received, bn VND):

| | Q1 | Q2 | Q3 | Q4 |
|---|--:|--:|--:|--:|
| 2023 | 26,870 | 56,751 | 83,233 | 108,116 |

→ To get a standalone quarter, **difference consecutive quarters within a year**
(`Qn − Qn−1`, with `Q1` as-is). The **income statement is already standalone** (its four
quarters sum exactly to the annual figure — checked for 2022/2023/2025), and the
**balance sheet is a stock** (point-in-time), so neither needs this treatment.

**2. Gaps are real source gaps, not parse bugs.** CafeF omits `Q2-2024` entirely and returns
literal all-zero rows for other quarters (re-checked against the live API). Both are emitted
as **blank rows with `source=missing`** — never as `0` — and seeded into the manual templates.
Dense usable history effectively starts ~**2012**.

(UI-only section-header rows — no item_code, always 0 — are dropped: `HDKD_1`, `HDDT_28`,
`HDTC_39`, and the balance sheet's "Nợ phải trả và vốn chủ sở hữu" heading.)

## Point-in-time use

Fundamentals are **not** knowable on the period-end date — only when disclosed. Join
`report`/`period` to **experiment_4's publish dates** (`vcb_quarter_publish_dates.csv`) and
use the *publish* date as the as-of date to stay look-ahead-safe.

## Notes / extending

- `symbol=VCB` is the only ticker-specific bit → set `SYMBOL` to scrape any listed code.
  (Line items differ by sector: these are the **bank** templates; `isBank=true` on the page.)
- `TypeTime` also accepts `LUYKE` (cumulative/YTD) — not scraped here.
- Financial ratios (Chỉ số tài chính) live on a separate endpoint,
  `api/v2/BCTC/FinancialIndicators` — not part of this experiment.
