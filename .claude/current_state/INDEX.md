# .claude/current_state/INDEX.md — the documentation map

> **AUTO-LOADED.** `CLAUDE.md` imports this file with `@.claude/current_state/INDEX.md`, so every session starts
> holding this map. Nothing else here is loaded until you ask for it.
>
> ⚠️ **NEVER BULK-LOAD THIS CORPUS.** 117 `.md` files, **~659k tokens** — about 3× a full context
> window. The routing below is the whole point: **open ONE file, when you touch that thing.** Every
> row carries its measured cost so you can budget before you read.
>
> ⚠️ **THE ALWAYS-LOADED HALF FELL FROM 165.7k TO 47.2k ON 2026-09-06**, and the corpus total did
> not move: `CLAUDE.md` §6-2-septies…undeseptuagies was **relocated verbatim** to
> a separate file. The hub had grown to
> 165.3k tokens of content while its own header called this index "routing and not content" — so
> the move is the rule being obeyed, not a change of policy. **No measurement was condensed or
> dropped**, and all ~196 `§6-2-*` citations across the repo still resolve, because they cite by
> section NAME rather than by anchor.
>
> Costs are `chars/4000`, re-measured 2026-09-03 (the five `SGN-1` touched). ⚠️ **Ten of them had drifted** — `ISSUES.md`
> was quoted at 40.6k in CLAUDE.md §7 against a measured 29.1k, a 40 % overstatement, which is
> the direction that makes a session refuse to open a file it could afford.

> ### ⚠️ ONE STANDING RULE IS REPEATED HERE BECAUSE THIS FILE IS AUTO-LOADED
>
> **A financial statement value comes from the filing PDF and from nothing else.** No HTML tab, no
> JSON endpoint, no web table, no transcription — not as a fallback, not "for the quarters OCR cannot
> read", not to close a gap. **A quarter no readable PDF can produce is `missing`, and `missing` is
> the correct answer.** CLAUDE.md §5 rule 24 · `ISSUES.md` `FIN-1` · TODO `P37`.
>
> ⚠️ This is the one exception to rule 4 below ("this index is routing, never content"), and it is
> deliberate: the rule has to be in context before a session opens anything.

## Tier 0 — already in your context, free

| file | ~tokens | what it answers |
|---|---|---|
| [../CLAUDE.md](../../CLAUDE.md) | **44.2k** | *what is this project, and what has it PROVED?* The map and the verdict. §2 is the headline negative, §6 the current state. ⚠️ **Was 162.7k until 2026-09-06** — the filings/OCR chronicle moved to Tier 1 |
| **.claude/current_state/INDEX.md** *(this file)* | **3.7k** | *where is everything else, and what does it cost to open?* |
| [../.claude/rules/common.md](../rules/common.md) | **0.9k** | *what rules hold in EVERY session, whatever the task?* Added 2026-09-06; auto-loaded via `@.claude/rules/common.md` in `CLAUDE.md`. **R1: everything written into a file is English** (the conversation stays Vietnamese; `*_VI.md` and Vietnamese DATA are the two named exceptions). ⚠️ **A new file in `.claude/rules/` is loaded only if `CLAUDE.md` imports it** — add the `@` line in the same commit |

## Tier 1 — the four registers + the result write-ups (`docs/`)

**One job each, no overlap.** Movement between them is one-way: a TODO item that turns out to be a
defect **graduates to ISSUES.md with a permanent code**; a fixed ISSUES entry keeps its row; a done
TODO item leaves its measurement in `CLAUDE.md` or a `CONTEXT.md` and is **deleted, not ticked**.

| file | ~tokens | open it when |
|---|---|---|
| [ISSUES.md](ISSUES.md) | **42.2k** | *what is BROKEN?* 97 open / 38 resolved, codes permanent. ⚠️ **Read before quoting any number — four entries change how a number may be READ** (`NUL-1`, `NUL-3`, `RPR-1`, `OUT-1`), `CFB-1` before quoting a BID cash flow, `CFV-1` before believing any cash flow accepted at a STRICT layer, and `TPL-1`/`CRP-1` before any non-bank financials parse |
| [TODO.md](TODO.md) | **30.8k** | *what is NEXT?* ⚠️ **NUMBERS FROZEN 2026-08-23** — a `P<n>` is a permanent NAME and **PRIORITY IS THE ROW ORDER**, so read top-down and cite the number. Seven groups — ⭐ top rows are group **0 · PARSER** (`P54`, `P55`, `P51`, `P46`, `P47`(a), `P43`, `P48`, `P44`, `P45`), then **A data `P2`**, **B OCR `P37`/`P38`/`P6`/`P5`/`P4`**, C output `P7`-`P8`, D model `P9`-`P17`, E honesty `P18`-`P21`, F backlog `P22`-`P36`. ⚠️ A HYPHENATED code is RETIRED, and a bare `P<n>` written BEFORE 2026-08-23 means a different item — the crosswalks resolve it |
| [pipeline.md](../docs/pipeline.md) | **4.8k** | *which ticker, on which date?* What the chain OUTPUTS — `(date, ticker, weight)`, 4,720 picks across 236 books. §6 is why there is no book for today; **§9d is the tradability gate that takes the CAGR from +181 % to +36.5 %** |
| [PIPELINE_h10_CAGR74.md](../docs/PIPELINE_h10_CAGR74.md) | **7.2k** | *how does ONE number get made, end to end?* The h=10 chain returning CAGR +74.0 %/yr. **§12 is the caveat section and is why the file exists** |
| [feature_groups.md](../docs/feature_groups.md) | **0.8k** | naming a feature group — the canonical taxonomy |

### Thesis deliverables — `.claude/docs/`

⚠️ These are **deliverables, not filings** — written for a reader outside the repo, and not the
research record (`CLAUDE.md` and the `CONTEXT.md` files are).

| file | ~tokens | |
|---|---|---|
| [THESIS_PROGRESS_2026.md](../docs/THESIS_PROGRESS_2026.md) | 7.2k | progress write-up (EN) |
| [THESIS_PROGRESS_2026_VI.md](../docs/THESIS_PROGRESS_2026_VI.md) | 7.6k | progress write-up (VI) |
| [THESIS_SUMMARY_2026_VI.md](../docs/THESIS_SUMMARY_2026_VI.md) | 4.3k | summary (VI) |

### The folder's own tooling

| script | what it does |
|---|---|
| **`.claude/tools/state_check.py`** | ⚠️ **run this before you commit.** Six drift checks across `CLAUDE.md` and `.claude/`; reports, never rewrites; exits 1 on drift. the [runbook](../runbook/RUNBOOK.md) has the commands it checks |
| `.claude/tools/check_index.py` | the narrower check: fails if any `.md` in the repo is unrouted by this index. Called by `state_check.py` |

## Tier 2 — package evidence (`src/*/CONTEXT.md`) — **open ONE, only when you touch it**

These stay beside their package on purpose: they are the evidence behind `CLAUDE.md`'s claims, and
locality is what keeps them true. **~246k tokens total — never open more than one or two.**

| open this | ~tokens | when you are… |
|---|---|---|
| [../src/web_scraper/CONTEXT.md](../../src/web_scraper/CONTEXT.md) | **61.9k** | touching a scraper, the PDF/OCR statement parser, or `raw_data/` layout |
| [../src/orchestration/CONTEXT.md](../../src/orchestration/CONTEXT.md) | **47.5k** | touching Dagster, `config.json`, any asset, any bronze/silver/gold table, a scrape, or ⚠️ **the FILTER layer** (§"FILTER") |
| [../src/feature_selection/CONTEXT.md](../../src/feature_selection/CONTEXT.md) | **45.0k** | running or reading a selection, or quoting any IC / null / bar. §15a the country-sweep guide, §16 the GPU conversion, §19 the ranker measurement |
| [../src/orchestration/preprocessor/CONTEXT.md](../../src/orchestration/preprocessor/CONTEXT.md) | **26.1k** | changing HOW a table is built — the `_ingest_*` / `_helper_*` transform library |
| [../src/walkforward/CONTEXT.md](../../src/walkforward/CONTEXT.md) | **16.0k** | asking whether a result survives more than ONE split, or which MODEL to use. §8 is PRF-8 (three architectures, 101× capacity, all tied) |
| [../src/model/CONTEXT.md](../../src/model/CONTEXT.md) | **12.0k** | training, adding a model type, or quoting a run's numbers. §1a is the RUN STANDARD |
| [../src/backtest/CONTEXT.md](../../src/backtest/CONTEXT.md) | **8.1k** | asking whether a signal is TRADABLE. §3 the cost identity that decides the horizon; §5 the single-stock answer ("no trade") |
| [../src/pipeline/CONTEXT.md](../../src/pipeline/CONTEXT.md) | 7.4k | the chain, staleness, `--root`/`--scope`, `--rescrape`, adding a stage, `pipeline.freshness` |
| [../src/final_features/CONTEXT.md](../../src/final_features/CONTEXT.md) | 6.8k | building or rebuilding a `__final__` table |
| [../src/train_test_creator/CONTEXT.md](../../src/train_test_creator/CONTEXT.md) | 4.9k | building a dataset, or the purge/impute/scale/window steps |
| [../src/result_evaluator/CONTEXT.md](../../src/result_evaluator/CONTEXT.md) | 4.1k | scoring, the metric set, panel-vs-series grain. ⚠️ **STALE** — predates `index.py` and `NUL-3` |
| [../src/sentiment/CONTEXT.md](../../src/sentiment/CONTEXT.md) | 3.4k | anything news / text / PhoBERT |

### Session tooling — `.claude/` (started 2026-09-06)

⚠️ **How to WORK, as opposed to what the project knows.** `.claude/rules/common.md` is the only
auto-loaded part (Tier 0); everything below is **lazily loaded — open the one file for the job in
front of you.** ⚠️ **The runbook here is the only runbook.**

| open this | ~tokens | when you are… |
|---|---|---|
| [../.claude/workflows/README.md](../workflows/README.md) | **0.9k** | starting any recurring JOB — it routes the eight step-by-step guides below |
| `.claude/workflows/*.md` | **13.6k** total | *start a session* (1.2k) · *run the chain* (1.6k) · *run a selection* (1.8k) · *refresh the data* (1.6k) · *OCR a ticker* (1.8k) · *quote a number* (1.8k) · *record a finding* (1.5k) · *finish and commit* (1.4k). ⚠️ **Each is the ORDER; the commands are cited by runbook row ID** so a flag changes in one place |
| [.claude/runbook/RUNBOOK.md](../runbook/RUNBOOK.md) | **4.0k** | you want the COMMAND — ~40 templates as one table (`O`/`C`/`W`/`D`/`E`/`F` row IDs), each with what it writes, its measured cost, and the step that must follow it |
| [../.claude/current_state/README.md](README.md) | **1.3k** | writing a measured SNAPSHOT (freshness, coverage, what exists). ⚠️ **Not a fifth register** — `CLAUDE.md` §6 holds what the state MEANS; this holds what a command PRINTED, with the command named |
| `.claude/current_state/*.md` | — | the snapshots themselves. ⚠️ **Empty until someone measures something, and that is the correct state** |

### Module descriptions — `.claude/module_descriptions/` (started 2026-09-06)

⚠️ **One file per `src/` module, named after the module.** These answer *"what is in this folder,
what is actually USED, and what will bite me"* — a structural read of the code with measured
reference counts, which is a different question from what a `CONTEXT.md` answers (*"what did we
measure, and what did it prove"*). **Lazily loaded — open the one for the module you are touching.**

| open this | ~tokens | when you are… |
|---|---|---|
| [../.claude/module_descriptions/dtos.md](../module_descriptions/dtos.md) | **3.3k** | touching any dataclass under `src/dtos/`. ⚠️ **The package is a mixed-age attic**: `tabular_database_driver_dtos` carries ~232 reference sites, `Task` 36, and `ModelConfigDto` / `ConfigDto` have **0** — "it is in `dtos/`" does not mean it is used |
| `.claude/module_descriptions/*.md` | — | the rest, as they are written |

### Package sub-docs

| open this | ~tokens | when you are… |
|---|---|---|
| [../src/kaggle_gpu/kgpu/PDF_OCR.md](../../src/kaggle_gpu/kgpu/PDF_OCR.md) | 14.3k | **running the FILING OCR on a T4** — the control notebook, the `QUARTERS` filter, what a verdict means, and the five things that have gone wrong. ⚠️ **§1a is what *"OCR ticker `<SYM>` LOCAL\|KAGGLE"* means** — a prepared per-ticker clone that WAITS, and how each parameter is decided from disk; §6 is when a recovered quarter may be merged; **§8 is what is still MISSING across every ticker at once** — 130 cells, of which **56 winnable and 46 of those VIC** |
| [../src/kaggle_gpu/README.md](../../src/kaggle_gpu/README.md) | 8.0k | running a repo notebook on a Kaggle T4 — the payload dataset, the parameter patcher, `rehearse`, **§7b PANEL MODE**, and §7's five measured traps |
| [../src/feature_selection/docs/RANKER_COMPARISON.md](../../src/feature_selection/docs/RANKER_COMPARISON.md) | 4.5k | asking which ranker to keep, drop or add — the scorecard behind `feature_selection` §19 |
| [../src/orchestration/preprocessor/FUNDAMENTAL_INDICATORS.md](../../src/orchestration/preprocessor/FUNDAMENTAL_INDICATORS.md) | 3.4k | the fundamental indicator definitions |
| [../src/feature_selection/docs/NULL_DRAWS_VI.md](../../src/feature_selection/docs/NULL_DRAWS_VI.md) · [NULL_DRAWS.md](../../src/feature_selection/docs/NULL_DRAWS.md) | 3.2k · 3.1k | how many null draws, and why (VI · EN) |

## Tier 3 — generated, **not hand-written** — do not edit, rarely read

| what | files | ~tokens | |
|---|---|---|---|
| `../reports/feature_selection*/**/README.md` | 71 | ~45k | one per archived selection run — **an artefact, written by `feature_selection.report`.** Read a single run's when auditing that run |
| [../reports/feature_selection/unified_schema_vcb__pool_columns.md](../../reports/feature_selection/unified_schema_vcb__pool_columns.md) | 1 | 7.5k | the VCB pool column dump |
| [../README.md](../../README.md) | 1 | 0.3k | the front door; routes to `CLAUDE.md` |

## The rules that keep this map honest

1. **Costs above are measured, not guessed.** `CLAUDE.md` §7 once carried figures that had gone stale
   by 1.8× as files grew — a stale cost is worse than no cost, because it gets budgeted against.
2. ⚠️ **BEFORE YOU COMMIT, RUN `python .claude/tools/state_check.py`** and resolve what it reports — stale
   `CLAUDE.md` §6 date, a `CONTEXT.md` changed without the hub, disagreeing issue counts, an unrouted
   `.md`, a drifted token cost, a broken link. **It REPORTS and never rewrites**: these counts are a
   SCAN, not a decrement, and a confidently wrong number is worse than none. Nothing enforces it at
   commit time by choice, so running it is the discipline. **this file's tooling row is the
   procedure and says where each kind of change is recorded.**
3. **`CLAUDE.md` stays at the repo root** and is not moved into `docs/` — Claude Code auto-loads it
   from the root only, so moving it would silently switch off the one file guaranteed to be read.
4. **This index is routing, never content.** If you find yourself explaining a measurement here, it
   belongs in `CLAUDE.md` or the relevant `CONTEXT.md` instead.
