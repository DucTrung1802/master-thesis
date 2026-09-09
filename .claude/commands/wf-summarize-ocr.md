---
description: Print the first X rows of the OCR coverage table, most liquid ticker first (default 10).
argument-hint: "[X]  — how many rows (default 10)"
---

⚠️ **CHEAPEST WORKFLOW IN THE REPO, AND IT STAYS THAT WAY.** Run exactly the command below and
render its output. **Do NOT** open `../workflows/summarize-ocr.md`, the notebook, `ISSUES.md`,
`CLAUDE.md` §6 or any package file first — the answer is one CSV, and reading around it costs
more tokens than it is worth. The workflow file is the reference for when something is wrong.

Run this from the repo root, with `X` = `$ARGUMENTS` if given, otherwise **10** (runbook **F4**):

```powershell
$n=10; Get-Content src\kaggle_gpu\pdf_ocr_coverage.csv -TotalCount ($n+1)
$c=(Get-Item src\kaggle_gpu\pdf_ocr_coverage.csv).LastWriteTime
$s=(Get-ChildItem raw_data\cafef\financials\statements -Recurse -Filter *.csv |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1).LastWriteTime
"table: $c | newest statement: $s | STALE: $($s -gt $c)"
```

Then, in one reply and nothing more:

1. Render the rows as a markdown table. `total` = quarters with a filing; `scraped` = of those,
   quarters whose PDF is on disk; `ocred` = quarters where all three statements carry a `pdf`
   row; `ocred_percentage` = `100 × ocred / total`.
2. Say once that the order is **median matched turnover over the trailing year, descending** —
   ⚠️ **liquidity, not market capitalisation**, which this repo does not have for 773 of 784
   tickers — and that the key is **not a column**, so the row order is the only thing carrying it.
3. If `STALE: True`, or the CSV is missing, say so in one line and **offer** to regenerate by
   running the last cell of `src/kaggle_gpu/RUN__pdf_ocr_summary.ipynb` (runbook **F3**).
   ⚠️ **Do not regenerate unless I ask.**

No per-ticker commentary, no recommendation of what to OCR next unless I ask for one.

Arguments: $ARGUMENTS
