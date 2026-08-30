# docs/INDEX.md — the documentation map

> **AUTO-LOADED.** `CLAUDE.md` imports this file with `@docs/INDEX.md`, so every session
> starts holding this map. Nothing else here is loaded until you ask for it.
>
> ⚠️ **NEVER BULK-LOAD THIS CORPUS.** 127 `.md` files, **~583k tokens** (re-measured
> 2026-08-27) — about 3× a full context window. The routing below is the whole point: **open ONE file, when you
> touch that thing.** Every row carries its measured cost so you can budget before you read.
>
> Written 2026-08-22, when the root registers moved into `docs/`. Costs are `chars/4`,
> re-measured that day — the figures previously in `CLAUDE.md` §7 were stale by up to 1.8×.

---

> ### ⚠️ ONE STANDING RULE IS REPEATED HERE BECAUSE THIS FILE IS AUTO-LOADED (2026-08-24)
>
> **A financial statement value comes from the filing PDF and from nothing else.** No HTML
> tab, no JSON endpoint, no web table, no transcription — not as a fallback, not "for the
> quarters OCR cannot read", not to close a gap. **A quarter no readable PDF can produce is
> `missing`, and `missing` is the correct answer.** CLAUDE.md §5 rule 24 · `ISSUES.md`
> `FIN-1` · TODO `P37`.
>
> ⚠️ This is the one exception to rule 4 below ("this index is routing, never content"),
> and it is deliberate: the rule has to be in context before a session opens anything.

## Tier 0 — already in your context, free

| file | ~tokens | what it answers |
|---|---|---|
| [../CLAUDE.md](../CLAUDE.md) | **103.2k** | *what is this project, and what has it PROVED?* The map and the verdict. §2 is the headline negative, §6 the current state |
| **docs/INDEX.md** *(this file)* | ~2k | *where is everything else, and what does it cost to open?* |

---

## Tier 1 — the four registers + the result write-ups (`docs/`)

**One job each, no overlap.** Movement between them is one-way: a TODO item that turns out
to be a defect **graduates to ISSUES.md with a permanent code**; a fixed ISSUES entry keeps
its row and is struck through; a done TODO item leaves its measurement in `CLAUDE.md` or a
`CONTEXT.md` and is **deleted, not ticked**.

| file | ~tokens | open it when |
|---|---|---|
| [RUNBOOK.md](RUNBOOK.md) | **19.6k** | *how do I RUN it?* 8 stages with measured runtimes, stage order, the two flags that destroy things, §8 rule 1 (the gate on quoting any number), **§8c the before-you-commit state check** |
| [ISSUES.md](ISSUES.md) | **33.8k** | *what is BROKEN?* 45 open / 38 resolved, codes permanent, never renumbered or reused. ⚠️ **Read before quoting any number — four entries change how a number may be READ** (`NUL-1`, `NUL-3`, `RPR-1`, `OUT-1`), and `TPL-1` before any non-bank financials parse |
| [TODO.md](TODO.md) | **63.3k** ⚠️ | *what is NEXT?* ⚠️ **THE NUMBERS ARE FROZEN AS OF 2026-08-23** — a `P<n>` is a permanent NAME and **PRIORITY IS THE ROW ORDER**, so read top-down and cite the number. **42 open tasks** in seven groups — ⭐ **top row `P46`, in group 0 · PARSER** (✅ `P41` and `P42` DONE 2026-08-30 — the two cost items: a hard filing fell **64.6 min → 9.5 min** with every row it reads identical on `rows_sha`) (⚠️ `P48` added 2026-08-30 — a bracket OCR damaged reads as a POSITIVE number and reconciles, and **six such cells are measured on disk in TCB Q4-2013**, a ticker nobody was looking at; the code already reads all six correctly, so it is a data repair; ⚠️ `P46`/`P47` added 2026-08-29 from the TCB run: the unit repair sits at layer 41 of 47 and cannot reach the statements that need it, and the OCR job cannot bootstrap a ticker at all; the rest added 2026-08-27 — `P41`-`P45` plus `P39`, a review of the PDF-parse module against the three parsed tickers on disk — two items are wrong numbers every gate passed, two are the cost `P38`/`P6` are budgeted on; ✅ `P39` DONE 2026-08-27, two of its pieces moved into `P43`); **A data `P2`, B OCR `P38`/`P6`/`P5`/`P4`** (`P3` closed by DECISION, archived unmeasured), C output `P7`-`P8`, D model `P9`-`P17`, E honesty `P18`-`P21`, F backlog `P22`-`P36`; `P1` closed 2026-08-23. ⚠️ A HYPHENATED code is RETIRED, and a bare `P<n>` written BEFORE 2026-08-23 still means a different item — the two crosswalks resolve it and are the last two needed. **The largest file here: read the top, not the whole thing** |
| [pipeline.md](pipeline.md) | **5.5k** | *which ticker, on which date?* What the chain OUTPUTS — `(date, ticker, weight)`, 4,720 picks across 236 books. §6 is why there is no book for today |
| [PIPELINE_h10_CAGR74.md](PIPELINE_h10_CAGR74.md) | **7.3k** | *how does ONE number get made, end to end?* The h=10 chain returning CAGR +74.0 %/yr. **§12 is the caveat section and is why the file exists** |
| [feature_groups.md](feature_groups.md) | **0.8k** | naming a feature group — the canonical taxonomy |

### Thesis deliverables — `docs/thesis/`

| file | ~tokens | |
|---|---|---|
| [thesis/THESIS_PROGRESS_2026.md](thesis/THESIS_PROGRESS_2026.md) | 7.2k | progress write-up (EN) |
| [thesis/THESIS_PROGRESS_2026_VI.md](thesis/THESIS_PROGRESS_2026_VI.md) | 7.6k | progress write-up (VI) |
| [thesis/THESIS_SUMMARY_2026_VI.md](thesis/THESIS_SUMMARY_2026_VI.md) | 4.3k | summary (VI) |

### The folder's own tooling

| script | what it does |
|---|---|
| **`docs/state_check.py`** | ⚠️ **run this before you commit.** Six drift checks across `CLAUDE.md` and `docs/`; reports, never rewrites; exits 1 on drift. [RUNBOOK.md §8c](RUNBOOK.md) explains each row |
| `docs/check_index.py` | the narrower check: fails if any `.md` in the repo is unrouted by this index. Called by `state_check.py` |

⚠️ These are **deliverables, not filings** — written for a reader outside the repo. They
are not the research record; `CLAUDE.md` and the `CONTEXT.md` files are.

---

## Tier 2 — package evidence (`src/*/CONTEXT.md`) — **open ONE, only when you touch it**

These stay beside their package on purpose: they are the evidence behind `CLAUDE.md`'s
claims, and locality is what keeps them true. **~212k tokens total — never open more than
one or two.**

| open this | ~tokens | when you are… |
|---|---|---|
| [../src/feature_selection/CONTEXT.md](../src/feature_selection/CONTEXT.md) | **45.0k** | running or reading a selection, or quoting any IC / null / bar. §15a the country-sweep guide, §16 the GPU conversion, §19 the ranker measurement |
| [../src/orchestration/CONTEXT.md](../src/orchestration/CONTEXT.md) | **47.0k** | touching Dagster, `config.json`, any asset, any bronze/silver/gold table, the browser budget, a scrape, or ⚠️ **the FILTER layer** (§"FILTER") |
| [../src/web_scraper/CONTEXT.md](../src/web_scraper/CONTEXT.md) | **39.7k** | touching a scraper, the PDF/OCR statement parser, or `raw_data/` layout |
| [../src/orchestration/preprocessor/CONTEXT.md](../src/orchestration/preprocessor/CONTEXT.md) | **25.8k** | changing HOW a table is built — the `_ingest_*` / `_helper_*` transform library |
| [../src/walkforward/CONTEXT.md](../src/walkforward/CONTEXT.md) | **16.0k** | asking whether a result survives more than ONE split, or which MODEL to use. §8 is PRF-8 (three architectures, 101× capacity, all tied) |
| [../src/model/CONTEXT.md](../src/model/CONTEXT.md) | **12.0k** | training, adding a model type, or quoting a run's numbers. §1a is the RUN STANDARD |
| [../src/backtest/CONTEXT.md](../src/backtest/CONTEXT.md) | **8.1k** | asking whether a signal is TRADABLE. §3 the cost identity that decides the horizon; §5 the single-stock answer ("no trade") |
| [../src/final_features/CONTEXT.md](../src/final_features/CONTEXT.md) | 6.8k | building or rebuilding a `__final__` table |
| [../src/pipeline/CONTEXT.md](../src/pipeline/CONTEXT.md) | 7.6k | the chain, staleness, `--root`/`--scope`, `--rescrape`, adding a stage |
| [../src/train_test_creator/CONTEXT.md](../src/train_test_creator/CONTEXT.md) | 4.9k | building a dataset, or the purge/impute/scale/window steps |
| [../src/result_evaluator/CONTEXT.md](../src/result_evaluator/CONTEXT.md) | 4.1k | scoring, the metric set, panel-vs-series grain. ⚠️ **STALE** — predates `index.py` and `NUL-3` |
| [../src/sentiment/CONTEXT.md](../src/sentiment/CONTEXT.md) | 3.4k | anything news / text / PhoBERT |

### Package sub-docs

| open this | ~tokens | when you are… |
|---|---|---|
| [../src/kaggle_gpu/README.md](../src/kaggle_gpu/README.md) | 7.5k | running a repo notebook on a Kaggle T4. §7b PANEL MODE, §7's five measured traps |
| [../src/kaggle_gpu/kgpu/PDF_OCR.md](../src/kaggle_gpu/kgpu/PDF_OCR.md) | 8.8k | **running the FILING OCR on a T4** — the control notebook, the `QUARTERS`/`PERIODS` filter, what a verdict means, and the five things that have actually gone wrong. ⚠️ §6 is when a recovered quarter may be merged, and it is a four-step procedure; the merge's refusal table is where `months` — the span a row covers — is explained. **§8 is the other direction — what is still MISSING across every ticker at once** (`RUN__pdf_ocr_summary.ipynb`), and why the start mark must be CONTIGUOUS backwards from the newest filing |
| [../src/feature_selection/docs/RANKER_COMPARISON.md](../src/feature_selection/docs/RANKER_COMPARISON.md) | 4.5k | asking which ranker to keep, drop or add — the scorecard behind `feature_selection` §19 |
| [../src/feature_selection/docs/NULL_DRAWS_VI.md](../src/feature_selection/docs/NULL_DRAWS_VI.md) | 3.8k | how many null draws, and why (VI) |
| [../src/feature_selection/docs/NULL_DRAWS.md](../src/feature_selection/docs/NULL_DRAWS.md) | 3.1k | how many null draws, and why (EN) |
| [../src/orchestration/preprocessor/FUNDAMENTAL_INDICATORS.md](../src/orchestration/preprocessor/FUNDAMENTAL_INDICATORS.md) | 3.1k | the fundamental indicator definitions |

---

## Tier 3 — the research record (`experiment/`) — ~95k tokens

| open this | ~tokens | when you are… |
|---|---|---|
| [../experiment/experiment_10/CONTEXT.md](../experiment/experiment_10/CONTEXT.md) | **44.0k** ⚠️ | writing the literature chapter. **Read the `"Combined reading"` section alone** unless you need a specific paper — 23 papers, and not one reports a naive baseline |
| [../experiment/CONTEXT.md](../experiment/CONTEXT.md) | 9.2k | the 9 exploratory experiments — signal discovery, tradability, point-in-time data, VN OCR |
| [../experiment/experiment_10/guidance.md](../experiment/experiment_10/guidance.md) | 7.8k | the paper-analysis method |
| [../experiment/experiment_10/news_result_examples.md](../experiment/experiment_10/news_result_examples.md) | 6.0k | worked news examples |
| [../experiment/experiment_10/conclusion.md](../experiment/experiment_10/conclusion.md) | 5.1k | the literature distillate |
| `../experiment/experiment_{1,3,4,5,6,7,8,9}/README.md` | 0.6–3.6k each | one experiment's own write-up |
| [../experiment/model_comparison.md](../experiment/model_comparison.md) | 0.6k | the model comparison note |

---

## Tier 4 — generated, **not hand-written** — do not edit, rarely read

| what | files | ~tokens | |
|---|---|---|---|
| `../reports/feature_selection*/**/README.md` | 71 | ~45k | one per archived selection run — **an artefact, written by `feature_selection.report`.** Read a single run's when you are auditing that run |
| [../reports/feature_selection/unified_schema_vcb__pool_columns.md](../reports/feature_selection/unified_schema_vcb__pool_columns.md) | 1 | 7.5k | the VCB pool column dump |
| [../README.md](../README.md) | 1 | 0.3k | the front door; routes to `CLAUDE.md` |
| `../experiment/experiment_{8,9}/out*/*.md` | 7 | ~3k | OCR probe output — `report.md`, `tsr_page*.md` |
| `../experiment/experiment_9/vendor/**/*.md` | 3 | ~5k | **third-party**, not ours |

---

## The rules that keep this map honest

1. **Costs above are measured, not guessed** (`chars/4`, 2026-08-22). `CLAUDE.md` §7 once
   carried figures that had gone stale by 1.8× as files grew — a stale cost is worse than
   no cost, because it gets budgeted against.
2. ⚠️ **BEFORE YOU COMMIT, RUN `python docs/state_check.py`** and resolve what it reports —
   stale `CLAUDE.md` §6 date, a `CONTEXT.md` changed without the hub, disagreeing issue
   counts, an unrouted `.md`, a drifted token cost, a broken link. **It REPORTS and never
   rewrites**: these counts are a SCAN, not a decrement, and a confidently wrong number is
   worse than none. Nothing enforces it at commit time by choice — no git hook — so running
   it is the discipline. **[RUNBOOK.md §8c](RUNBOOK.md) is the procedure and says where each
   kind of change is recorded.** (`check_index.py` is the narrower completeness check, and
   `state_check.py` already calls it.)
3. **`CLAUDE.md` stays at the repo root** and is not moved into `docs/`. Claude Code
   auto-loads it from the root only; moving it would silently switch off the one file that
   is guaranteed to be read.
4. **This index is routing, never content.** If you find yourself explaining a measurement
   here, it belongs in `CLAUDE.md` or the relevant `CONTEXT.md` instead.
