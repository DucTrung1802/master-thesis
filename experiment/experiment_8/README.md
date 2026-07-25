# experiment_8 — PaddleOCR-DB + VietOCR on a filing the production parser cannot read

Reads **ACB's FY-2013 audited consolidated report** — balance sheet, income statement, cash flow
— with the pipeline from **https://github.com/bmd1905/vietnamese-ocr**: PaddleOCR's DB detector
finds the text, and **VietOCR** (a Vietnamese CNN+Transformer) reads it.

## Why this document

`raw_data/cafef/financials/statements/bank/*/HOSE_ACB.csv` records all three FY-2013 statements
as `source=cafef`. That is the pipeline saying *the PDF defeated me*: the figures had to be taken
from CafeF's transcription instead of from the filing. The document is 105 pages of **page scan**
carrying a **legacy-font text layer** that is neither usable nor short enough to be ignored —

| what the embedded text layer says | what the page says |
|---|---|
| `Bine can ddi k6loAn hqD nhil tli rqdy` | Bảng cân đối kế toán hợp nhất tại ngày |
| `t5l.l6l` / `r.0?8.109` / `(227.t4E'` | 851.161 / 1.078.309 / (227.148) |

The production parser gates OCR on `_native_garbled`, a ≤2-char-token fraction ≥ 0.40. These
pages score **0.16-0.24** — the mojibake keeps enough long ASCII runs to pass for prose — so they
were never OCR'd at all. That is the case this experiment attacks.

## What was built

```
vietnamese_ocr.py    the engine   — PaddleOCR DB detection + VietOCR recognition
ocr_pipeline.py      everything downstream, SHARED WITH experiment_9
run_acb_2013.py      the CLI
out/                 statements, comparisons, the OCR read itself, report.md
```

**`ocr_pipeline.py` is deliberately not new code.** It feeds the engine into
`src/web_scraper/cafef_pdf_parser.PdfParser` — the parser already in production — as a drop-in
replacement for its Tesseract seam, so the question being answered is "what would the existing
pipeline produce if the OCR were better?". experiment_9 imports the same module, which is what
makes the two experiments comparable: the engine is the only thing that differs.

Two departures from the 40-line `predict.py` in the source repo, both necessary rather than
decorative: recognition is **batched** (one `predict()` per box is ~3,000 sequential forward
passes for this filing) and low-confidence recognitions are **dropped** (scan speckle detects as
text, and a junk token in a row can capture a value column).

## Running it

```bash
python -m venv --system-site-packages ocr_env8            # from the repo root
ocr_env8/Scripts/python -m pip install paddlepaddle==3.0.0 paddleocr==3.1.0 paddlex==3.0.3 vietocr
cd experiment/experiment_8
../../ocr_env8/Scripts/python run_acb_2013.py --max-pages 16
```

`paddlex` must be pinned: `paddleocr==3.1.0` accepts any `paddlex>=3.1.0`, and 3.7 changed
`PaddlePredictorOption.__init__` so the detector cannot be constructed at all. Model weights
(PP-OCRv5 det, VietOCR `vgg_transformer`) download on first use.

The OCR is cached in `out/ocr_cache.json`, keyed on engine + file + DPI, so re-running to iterate
on the table logic takes a second instead of the four minutes the read itself costs (**234 s for
15 pages, 15.6 s/page** — experiment_9's detector does the same pages in 27 s). Delete the cache
to force a re-read.

## Result

| statement | pages | rows parsed | mapped | reconciles | agree | differ | missed |
|---|---|---|---|---|---|---|---|
| balance_sheet | 8-10 | 63 | 43 | **yes** | 22 | 18 | 31 |
| income_statement | 11-12 | 25 | 19 | **yes** | 7 | 10 | 0 |
| cash_flow | 13-14 | 38 | 11 | **yes** | 6 | 4 | 26 |

`agree`/`differ` compare canonical columns against CafeF's own transcription of the same filing
(`<report>_vs_cafef.csv`); `missed` are lines CafeF has that this parse did not map. The balance
sheet is compared at 31 Dec, the income statement against the SUM of CafeF's four quarters (the
annual report prints the year, its quarterly rows are standalone), and the cash flow against Q4
alone (already cumulative).

**All three statements reconcile against their own printed subtotals** — the gate that decides
whether the production pipeline would accept a parse. On this document it currently accepts none.

### The OCR is not the bottleneck

The recognition is close to exact. Against the balance sheet's own figures:

| line | this parse | the page (independent, from the broken text layer) |
|---|---|---|
| TỔNG NỢ PHẢI TRẢ | 154,094,787 | `l5{,09{.787` |
| TỔNG VỐN CHỦ SỞ HỮU | 12,504,202 | `12.504.202` |
| TỔNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU | 166,598,989 | `166.598.989` |

and the income statement's arithmetic closes on the parsed values alone: interest income
15,205,073 − interest expense 10,818,660 = **NII 4,386,413**, and operating profit 1,890,190 −
credit provisions 854,630 = **PBT 1,035,560**.

**Some `differ` rows are CafeF being wrong, not the OCR.** CafeF's Q4-2013 tab puts total assets
at 166,737,706; the audited filing prints **166,598,989**, and the difference (140,751) is one
line — *Các khoản nợ khác* — that propagates into liabilities and the grand total. Both sides
balance internally, so this is two different vintages of the same statement, not a misread digit.
CafeF's interest-expense quarters even carry inconsistent signs, which is why its FY sum
(−6,089,848) cannot reproduce a net interest income that the filing states directly.

The real loss is **schema mapping**: `map_to_schema` matches the printed line name against the
chart of accounts, and the chart of accounts is built from CafeF's tabs, which abbreviate
(`tien_gui_tai_cac_tctd_khac`) where the filing spells out (*Tiền gửi tại các tổ chức tín dụng
khác*). Below the 0.80 threshold the line is dropped — 31 of the balance sheet's lines, 26 of the
cash flow's — and where a subtotal and its first sub-item are near-identical the ordered walk can
take the wrong one (*Vay các TCTD khác* received 7,793,776, which is the section subtotal
5,842,936 + 1,950,840). Every one of those values is present and correct in
`<report>_rows.csv`; only its column is wrong.

### Three parser findings, reproduced in `ocr_pipeline.OcrPdfParser`

Each is a real failure of the production classifier that only surfaces once the OCR is good
enough to reach it. They are implemented as overrides here, not in `src/`, and are candidates for
promotion:

1. **A table of contents is not a statement.** The filing's "NỘI DUNG" page lists every statement
   with its form code (`Mẫu B02/TCTD-HN`, `B03`, `B04`). The classifier trusts a form code
   absolutely, so that page came back as the balance sheet, anchored the run six pages from the
   real statement, and fed its PAGE NUMBERS into the period-column clustering. A statement
   carries one form code, its own; two or more means the page is talking *about* the statements.
2. **The best title must win, not the first.** The cash-flow page prints `Mẫu BO4/TCTD-HN` — a
   letter O for the zero — so `B\d{2}` misses and the page falls through to title matching.
   `PdfParser._titled` returns the first of the three titles to clear 0.80 in dict order, and a
   window of the page's boilerplate scored 0.80+ for *"kết quả hoạt động kinh doanh"*. The page
   was declared the income statement, the cash-flow run never started, and **the whole cash-flow
   statement was lost**, even though *"lưu chuyển tiền tệ"* appears in the header verbatim.
   Scoring all three and taking the best recovered it.
3. **The "Thuyết minh" note column must be dropped by magnitude, not by position.** The parser
   keeps period columns to the right 60% of the page to avoid the note references. That works for
   word-level OCR, which scatters them; a line-level detector emits one tight, well-populated
   column of them that sits inside the value zone, becomes column 1, and — via
   `Statement._first_value` — makes every line's figure its NOTE NUMBER. The first run mapped 0
   values because of it. A period column's numbers are 4-9 digits; a note reference is 1-2.

## Batch — a whole range of quarters (`run_batch_acb.py`)

The single-doc run proves one hard scan can be read. `run_batch_acb.py` runs the range
**ACB Q1-2014 … Q4-2016** — 12 filings of three shapes (plain quarterlies, semi-annual reviews,
audited annuals) — and scores every statement against the production reference in
`raw_data/cafef/financials/`. This is the model called **`paddle`** in the head-to-head; the code
lives in `batch.py` (shared with experiment_9, exactly like `ocr_pipeline.py`).

```bash
../../ocr_env8/Scripts/python run_batch_acb.py     # writes out_batch/, cache under out_batch/cache/
python ../compare_models.py                        # after both models have run
```

What counts as correct changes with the filing, and the scorer handles it (see `batch.py`): the
balance sheet is a stock; the income statement is standalone for Q1/Q3 but **cumulative** for the
Q2 review (Jan-Jun) and Q4 annual (full year), so the expected value is reconstructed from the
reference quarters; the cash flow is cumulative YTD in every filing. Income-statement **expense
signs** are a CafeF-vs-filing convention (CafeF stores positive magnitudes, the filing prints
parentheses) — a magnitude match that only flips sign is counted as a match, not an OCR error.

Result (`out_batch/report.md`): **paddle reconciles 34/36 statements** (all 12 income statements
and all 12 cash flows; 10/12 balance sheets — Q1-2015 and Q3-2016 rejected on a schema-mapping
collision) and matches **734/879** figures (0.835). OCR cost **2001 s for 144 pages (13.9
s/page)**. As in the single-doc run, the residual `differ` is dominated by **schema mapping** on
the bank sub-item lines (a value landing on an adjacent sub-line) and by **CafeF vintage** (the
tabs store 0 where the filing reports a figure), not by misread digits. See
[`../model_comparison.md`](../model_comparison.md) for paddle-vs-onnx — they tie on accuracy and
onnx is ~10× faster.

## Files

| file | what it is |
|---|---|
| `vietnamese_ocr.py` | the engine: DB detection, padded crops, batched VietOCR, confidence floor |
| `ocr_pipeline.py` | rasterising, the `PdfParser` subclass, the statement builder, CafeF comparison, the shared single-doc runner — **experiment_9 imports this** |
| `batch.py` | the multi-quarter driver + period-aware reference scoring — **experiment_9 imports this** |
| `run_acb_2013.py` | single-doc CLI (`--dpi`, `--box-thresh`, `--unclip-ratio`, `--min-prob`, `--weights`, …) |
| `run_batch_acb.py` | batch CLI over ACB Q1-2014…Q4-2016 |
| `out/…` | single-doc outputs (raw rows, canonical, vs-CafeF, words, OCR text, report) |
| `out_batch/cells.csv` | scorecard: one row per quarter × statement |
| `out_batch/detail.csv` | every figure vs the reference |
| `out_batch/report.md` | the batch summary |
