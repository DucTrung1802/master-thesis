# Workflow — OCR a ticker's filings

> **Goal:** answer the standing request **"OCR ticker `<SYM>` LOCAL"** or **"OCR ticker `<SYM>`
> KAGGLE"**.
>
> ⚠️ **THE REQUEST IS FOR A PREPARED NOTEBOOK, NEVER FOR A RUN.** Standing since 2026-09-04. You
> clone, edit one cell, resolve the parameters read-only, report what they resolved to, and
> **STOP**. The wait is the point and is not a step to optimise away: a whole-ticker parse is
> 60-70 filings and hours of GPU — one ticker was **185 minutes over 71 filings on a T4**.
>
> **The authority is [`src/kaggle_gpu/kgpu/PDF_OCR.md`](../../src/kaggle_gpu/kgpu/PDF_OCR.md) §1a.**
> This file is the order of operations; that one is how each parameter is decided.

---

## 0. The rule that governs every step — `CLAUDE.md` §5 rule 24

**A financial statement value comes from the filing PDF and from nothing else.** No HTML tab, no
JSON endpoint, no web table, no transcription — **not as a fallback, not "for the quarters OCR
cannot read", not to close a gap.**

⚠️ **A quarter no readable PDF can produce is `missing`, and `missing` is the CORRECT ANSWER.**

⚠️ **The code still disagrees with the rule**: `CafefFinancialsBuilder` takes `use_api: bool =
True`, which is why **34 report-rows on disk carry `source='cafef'`** rather than `pdf` (`FIN-1`).
The `source` column is what makes this auditable — **D6** is the query, and **anything but `pdf`
or `missing` is a defect, not a data point.**

## 1. Clone the control notebook — do not edit the original

```
src/kaggle_gpu/RUN__pdf_ocr_control.ipynb  →  RUN__pdf_ocr_control_<sym>.ipynb
```

**Edit cell 2 and nothing else.** Strip the outputs.

## 2. Resolve the parameters against disk — read-only, no OCR, no network, no quota

`pdf_ocr_batch.plan_batch` + `job.seed_history`. This is the whole preparation: it says which
quarters exist, which are already `pdf`, which are settled absences, and which the run would
attempt.

⚠️ **`ENVIRONMENT` decides which machine parses** — that is what `LOCAL` vs `KAGGLE` in the
request selects. `MODE` takes `"auto" | "local" | "kgpu"`; an explicit `"kgpu"` with no payload
mounted **raises** rather than quietly parsing the repo's own `raw_data/` and reporting a Kaggle
run that never touched the payload.

## 3. Decide the two judgement calls — they have no safe default

| parameter | when it is right |
|---|---|
| **`FORCE_EMPTY_BAND`** | ⚠️ **only when BOOTSTRAPPING a ticker with no history.** `BND-1`: `seed_history` rebuilds the sanity band from the `pdf` rows on disk, an empty band fails open, and the merge then refuses every statement the run produced — **the loop closes on itself.** This flag is the escape and **it lifts a real guard**; the arithmetic screens (`web_scraper/statement_screens.py`) are what stands in for it |
| **`OVERWRITE`** | only for a deliberate, scoped repair of a known-bad cell. Merging a quarter that DIFFERS from a good `pdf` row is a decision, not a default |

⚠️ **Neither is decidable from the ticker symbol alone** — both are judgements about *that
ticker's* history, which is why this workflow ends in a report rather than a run.

## 4. Report, and STOP

Say what the parameters resolved to: how many filings, how many already `pdf`, how many settled,
which quarters the run would attempt, and which of the two flags you set and why.

**⛔ Do not run it.** The user starts it.

---

## After a run — reading the result

## 5. Recompute coverage — **F3**, seconds, no OCR

`RUN__pdf_ocr_summary.ipynb` measures `statements/**/*.csv` against `documents(allow_parent=True)`
and `settled_absences`.

⚠️ **`complete` is CONTINUITY from the start of the filing chain, NOT coverage, and the two
disagree.** One ticker reads ✅ at 210/210 and another reads ✅ at 40/51, while a third reads ❌ at
210/213. **Read the cell count for coverage and `complete` for continuity.**

⚠️ **Do not read a missing count as work available.** Of one day's 130 missing cells, **66 (51 %)
were quarters the company never filed** — where `missing` is correct and no run can change it —
against 56 winnable, **46 of them one ticker**, and 27 of those waiting on a de-cumulation operand
rather than on OCR.

## 6. Check the figures the gates cannot check

⚠️ **The recurring defect class here is `SLD-1`'s family: a WRONG FIGURE THAT PASSES EVERY GATE.**
Recorded six-plus times — a slid row, a lost bracket, a comparative column, a merged label, a seal
over the digits. `reconcile` and `sane` are the only gates, and **on a cash flow accepted at a
STRICT layer the arithmetic identity never runs at all** (`CFV-1`).

**The method that keeps working, and it is free:** ⚠️ **a VAS filing prints several of its figures
twice, and checking one against the other costs no OCR and no network.**

- the cash flow's closing balance **is** the balance sheet's cash line
- a Q1 income statement's two columns are the same three months
- a balance sheet's two grand totals are one number
- a cumulative cash flow prints one opening per year

Four defects were found that way in a single session.

⚠️ **Before quoting any BID fundamental, read `CFB-1`** — 7 BID quarters carry the 1-Jan opening
in the CLOSING slot, and Q3-2011 holds a **negative** closing cash balance.

⚠️ **Before any non-bank parse, read `TPL-1` / `CRP-1`** — two of the seven reconcile anchors
return the OPENING cash balance as the closing one on `corp` and `insurance`, and a `corp` balance
sheet reconciles on the trivial `assets == resources` rather than on `A = L + E`. **Nothing from a
non-bank template may be quoted as a fundamental.**

## 7. Delete the clone — on `complete = True` **AND** `outstanding = 0`

⚠️ **Never on `complete` alone**, which measures continuity from the start of the chain and reads
`True` on tickers with open cells.

---

## Done when

**For the request itself:** the clone exists, cell 2 is edited, the parameters are resolved and
reported, and you have stopped.

**For a finished run:** coverage recomputed (**F3**), the double-printed figures cross-checked,
`source` audited (**D6**), the numbers written into
[docs/OCR_PARSER_LOG.md](../../docs/OCR_PARSER_LOG.md) or the hub's §6-2 summary, and the clone
deleted.

## Traps

⚠️ **`SET-2`: a SETTLED absence is a verdict on the page CLASSIFIER wearing the words of one on
the document.** Re-test every settled cell after any `_page_kind` change — six of one ticker's
eight turned out winnable that way.

⚠️ **`TPX-1`: `templates.csv` names only three tickers.** Every other ticker resolves its template
by a NETWORK call, and a ticker absent from **both** `CAFEF_FINANCIALS_TICKERS` and
`orchestration/config.json` is silently unmaterialisable.

⚠️ **`$env:PYTHONUTF8 = "1"` is REQUIRED on the `pdf-ocr` Kaggle job.** The parse logs Vietnamese
account labels; without it the run COMPLETES on Kaggle and the **download** raises. Re-pull with
the variable set — the run is not lost.

⚠️ **Do not quote a LOCAL-vs-T4 speedup.** Four runs of the identical easy document on this
machine came in at 100.6, 113.3, 50.8 and 50.3 s — a **2.25× swing** with the T4's 69.0 s between
them. To compare two machines, INTERLEAVE the runs. What a T4 buys is a second machine running in
parallel, free — not a multiplier.
