# Workflow — summarize the OCR backlog

> **Goal:** print the top `X` rows of the OCR coverage table — which tickers are most worth
> parsing next, most liquid first. **Cost: one read-only command, ~1 s.** Default `X` is 10.
>
> ⚠️ **THIS WORKFLOW RUNS NOTHING AND OPENS NOTHING ELSE.** It reads one CSV that
> `RUN__pdf_ocr_summary.ipynb` already wrote. That is the entire point: the same answer costs a
> notebook run (`F3`, seconds but a kernel and 784 index reads) or one `Get-Content`. **Do not open
> the notebook, `ISSUES.md`, `PDF_OCR.md` or any package file to answer this** — none of them
> carries the table, and opening one costs more tokens than the answer is worth.

---

## 1. Print the rows — **F4**

```powershell
$n=<X>; Get-Content src\kaggle_gpu\pdf_ocr_coverage.csv -TotalCount ($n+1)
$c=(Get-Item src\kaggle_gpu\pdf_ocr_coverage.csv).LastWriteTime
$s=(Get-ChildItem raw_data\cafef\financials\statements -Recurse -Filter *.csv |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1).LastWriteTime
"table: $c | newest statement: $s | STALE: $($s -gt $c)"
```

`X` is the number of DATA rows; the `+1` is the header. Render what comes back as a markdown
table and stop — no commentary per ticker.

| column | means |
|---|---|
| `total` | quarters with a filing, `Q1-2008` onward, one filing per quarter |
| `scraped` | of those, quarters whose PDF is on disk |
| `ocred` | quarters where **all three** statements carry a `pdf` row |
| `ocred_percentage` | `100 × ocred / total` |

## 2. Say what the order is

⚠️ **Rows are sorted by median matched turnover over the trailing year, DESCENDING — not by
market capitalisation, which this repo does not have** for 773 of its 784 tickers. Say "most
liquid first", never "largest first". ⚠️ **The sort key is NOT a column**, so the row order is the
only thing carrying it: never re-sort the file and expect to get it back.

## 3. Only if `STALE: True` — or the file is missing

Say so in one line, then offer to regenerate. **Do not regenerate without being asked.**
Regenerating is running the last cell of `src/kaggle_gpu/RUN__pdf_ocr_summary.ipynb` (`F3`),
which reads `raw_data/` and writes only that CSV.

---

## Done when

- [ ] `X` rows printed, with the header
- [ ] the sort key is named as **liquidity**, and the sort key's absence from the file is said once
- [ ] `STALE` reported when true, and **not** silently fixed

## Traps

⚠️ **`ocred` counts quarters where all three statements landed.** A ticker part-way through one
quarter reads as not done, deliberately — this table exists to order work, and §5 rule 2 says a
missing measurement is missing.

⚠️ **`total` counts QUARTERS, not index rows.** The PDF index carries 56,002 rows over 784 tickers
because a quarter has several filings; `documents()` picks one per quarter and returns 26,040.
A percentage computed against the index is wrong by more than 2×.

⚠️ **A high `ocred_percentage` on a small `total` is a short filing history, not a finished
ticker.** Read the two columns together.

⚠️ **`scraped < total` means a filing CafeF advertises has no file on disk and cannot be
re-parsed** — measured 2026-09-09 at exactly one quarter across all 784 tickers (ACB 2009-Q3).
