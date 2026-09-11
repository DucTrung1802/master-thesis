# .claude/current_state/INDEX.md — the documentation map

> **AUTO-LOADED.** `CLAUDE.md` imports this file with `@.claude/current_state/INDEX.md`, so every session starts
> holding this map. Nothing else here is loaded until you ask for it.
>
> ⚠️ **NEVER BULK-LOAD THIS CORPUS.** 131 `.md` files, **~529k tokens** — about 2.5× a full context
> window. The routing below is the whole point: **open ONE file, when you touch that thing.** Every
> row carries its measured cost so you can budget before you read.
>
> ⚠️ **THE ALWAYS-LOADED HALF FELL FROM 165.7k TO 49.4k AND THEN TO 11.7k, ALL ON 2026-09-06.**
> The hub had grown to 165.3k tokens of content while its own header called this index "routing and
> not content", so `CLAUDE.md` §6-2-septies…undeseptuagies came out of it — **the move is the rule
> being obeyed, not a change of policy.** ~196 `§6-2-*` citations across the repo still name those
> sections, and they cite by section NAME rather than by anchor.
>
> ⚠️ **AND THE SECOND CUT MADE THE RULE ENFORCEABLE.** The hub was still **2,549 lines** after the
> first; §2-§6's evidence moved VERBATIM to [`../findings/`](../findings/) and §5's rule bodies to
> [`../rules/standing-rules.md`](../rules/standing-rules.md), **each keeping its original heading**,
> so a `§2b-bis` / `§6-1` / `§5 rule 24` citation still resolves — to those files.
> [`common.md`](../rules/common.md) **R2 now caps `CLAUDE.md` at 300 lines** and
> `state_check.py`'s first check fails when it is over. ⚠️ **A cap that is only written down is the
> cap the hub already broke twice**; this one is measured on every run.
>
> ⚠️ **AND THIS PARAGRAPH SAID THE CHRONICLE WAS "RELOCATED VERBATIM TO A SEPARATE FILE" AND THE
> CORPUS TOTAL "DID NOT MOVE". BOTH WERE WRONG, measured 2026-09-06.** `docs/OCR_PARSER_LOG.md`
> was **deleted** in commit `4348eb5d`, not relocated — `CLAUDE.md` §7 says so in as many words —
> so the corpus fell **659k → 520k** and this header went on budgeting against a file that was not
> there. Recoverable with `git show 4348eb5d^:docs/OCR_PARSER_LOG.md`, and read as `RPR-1`'s shape:
> a claim whose evidence is one `git checkout` away.
>
> ⚠️ **THE PACKAGE DOCS MOVED OUT OF `src/` ON 2026-09-06** — twelve `src/<pkg>/CONTEXT.md` to
> `.claude/context/<pkg>.md` (one file per `src/` subfolder) and seven long-form guides to
> `.claude/docs/`. **No `.md` remains under `src/`.** Tier 2 has what that cost.
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
| [../CLAUDE.md](../../CLAUDE.md) | **6.6k** | *what is this project, and what has it PROVED?* **The map and the verdict, and nothing else** — §2 the headline negative, §6 the current state, **§7 the module→file routing you are meant to lazy-load from**. ⚠️ **Was 162.7k, then 2,549 lines, both on 2026-09-06**; the evidence is in Tier 1's `findings/` rows and the hub is capped at 300 lines by R2 |
| **.claude/current_state/INDEX.md** *(this file)* | **4.0k** | *where is everything else, and what does it cost to open?* |
| [../.claude/rules/common.md](../rules/common.md) | **1.4k** | *what rules hold in EVERY session, whatever the task?* Added 2026-09-06; auto-loaded via `@.claude/rules/common.md` in `CLAUDE.md`. **R1: everything written into a file is English** (the conversation stays Vietnamese; `*_VI.md` and Vietnamese DATA are the two named exceptions). **R2: `CLAUDE.md` is at most 300 lines**, checked by `state_check.py`, with the table saying where displaced prose goes. ⚠️ **A new file in `.claude/rules/` is loaded only if `CLAUDE.md` imports it** — add the `@` line in the same commit |

## Tier 1 — the four registers + the result write-ups (`docs/`)

**One job each, no overlap.** Movement between them is one-way: a TODO item that turns out to be a
defect **graduates to ISSUES.md with a permanent code**; a fixed ISSUES entry keeps its row; a done
TODO item leaves its measurement in `CLAUDE.md` or a `.claude/context/` file and is **deleted, not ticked**.

| file | ~tokens | open it when |
|---|---|---|
| [ISSUES.md](ISSUES.md) | **60.6k** | *what is BROKEN?* 97 open / 38 resolved, codes permanent. ⚠️ **Read before quoting any number — four entries change how a number may be READ** (`NUL-1`, `NUL-3`, `RPR-1`, `OUT-1`), `CFB-1` before quoting a BID cash flow, `CFV-1` before believing any cash flow accepted at a STRICT layer, and `TPL-1`/`CRP-1` before any non-bank financials parse |
| [TODO.md](TODO.md) | **30.8k** | *what is NEXT?* ⚠️ **NUMBERS FROZEN 2026-08-23** — a `P<n>` is a permanent NAME and **PRIORITY IS THE ROW ORDER**, so read top-down and cite the number. Seven groups — ⭐ top rows are group **0 · PARSER** (`P54`, `P55`, `P51`, `P46`, `P47`(a), `P43`, `P48`, `P44`, `P45`), then **A data `P2`**, **B OCR `P37`/`P38`/`P6`/`P5`/`P4`**, C output `P7`-`P8`, D model `P9`-`P17`, E honesty `P18`-`P21`, F backlog `P22`-`P36`. ⚠️ A HYPHENATED code is RETIRED, and a bare `P<n>` written BEFORE 2026-08-23 means a different item — the crosswalks resolve it |
| [pipeline.md](../docs/pipeline.md) | **4.8k** | *which ticker, on which date?* What the chain OUTPUTS — `(date, ticker, weight)`, 4,720 picks across 236 books. §6 is why there is no book for today; **§9d is the tradability gate that takes the CAGR from +181 % to +36.5 %** |
| [PIPELINE_h10_CAGR74.md](../docs/PIPELINE_h10_CAGR74.md) | **7.2k** | *how does ONE number get made, end to end?* The h=10 chain returning CAGR +74.0 %/yr. **§12 is the caveat section and is why the file exists** |
| [feature_groups.md](../docs/feature_groups.md) | **0.8k** | naming a feature group — the canonical taxonomy |

### The research record — `.claude/findings/` (**this is `CLAUDE.md` §2-§6's evidence**)

⚠️ **Moved out of the hub on 2026-09-06 when R2 capped it at 300 lines, VERBATIM and with every
heading unchanged**, so a `§2b-bis`, `§3a-bis`, `§5c` or `§6-1-quater` citation written anywhere in
this repo still resolves — **to one of these six files**. `CLAUDE.md` carries the headline number
and a 📂 link; **the table it came from is here.** ⚠️ These are the *narrative* — what a measurement
MEANS — and are not [`current_state/`](README.md) snapshots, which hold what a command PRINTED.

| open this | ~tokens | when you are… |
|---|---|---|
| [verdict.md](../findings/verdict.md) | **2.3k** | ⚠️ **about to propose any modelling work.** §2 in full — the five defeats, the horizon nobody controlled for, the width ladder that survived, its caveats, and §2d's one remaining lever |
| [data-state.md](../findings/data-state.md) | **22.7k** | asking what the DATA looks like today. §6-2-§6-3 — the `pool__ta` prune, the 2026-08-23 re-scrape and carry-up, the freshness distribution, `DEP-1`, the filings/OCR summary, **MSN 2026-09-07 (the gaps were REFUSALS: `JVW-2` and `MSO-5` fixed, `PBT-1`/`SPN-2`/`BSP-1` open)**, the audit |
| [cross-sectional.md](../findings/cross-sectional.md) | **8.5k** | quoting the headline result. §6-0-§6-0-ter — the walk-forward, seven architectures, the dataset sweep. ⚠️ **§6-0-c is the caveat list and is why the rest is readable** |
| [model-chain.md](../findings/model-chain.md) | **6.9k** | running the chain end to end. §3b-§3d-bis + §4 — the eight stages, the two selection layers, Kaggle panel mode. ⚠️ the COMMANDS are the [runbook](../runbook/RUNBOOK.md)'s; this is the reasoning around them |
| [data-layers.md](../findings/data-layers.md) | **5.6k** | asking where a column comes from. §3a-§3a-bis — the 83 assets, all ten `pool__*` tables and what each measured, the forex ingest, the FILTER layer |
| [single-stock.md](../findings/single-stock.md) | **4.2k** | asked for a signal on ONE ticker. §5c, §5d, §6-1-§6-1-quater — eleven architectures inside one error bar, the BANK panel, the five-ticker h=10 run, the VN30 run whose POOLED answer flips |

### Thesis deliverables — `.claude/docs/`

⚠️ These are **deliverables, not filings** — written for a reader outside the repo, and not the
research record (`CLAUDE.md` and the `.claude/context/` files are).

| file | ~tokens | |
|---|---|---|
| [THESIS_PROGRESS_2026.md](../docs/THESIS_PROGRESS_2026.md) | 7.2k | progress write-up (EN) |
| [THESIS_PROGRESS_2026_VI.md](../docs/THESIS_PROGRESS_2026_VI.md) | 7.6k | progress write-up (VI) |
| [THESIS_SUMMARY_2026_VI.md](../docs/THESIS_SUMMARY_2026_VI.md) | 4.3k | summary (VI) |

### The folder's own tooling

| script | what it does |
|---|---|
| **`.claude/tools/state_check.py`** | ⚠️ **run this before you commit.** Six drift checks across `CLAUDE.md` and `.claude/`; reports, never rewrites; exits 1 on drift. the [runbook](../runbook/RUNBOOK.md) has the commands it checks |
| `.claude/tools/replay_duplicate_column.py` | ⚠️ **`DPC-3`'s measurement, and it needs no OCR.** Replays `_duplicate_period` over the stored `row_dump` of every accepted income statement in `reports/pdf_ocr/` and reports how many Q1 filings print a duplicate column that CONTRADICTS itself — the cells today's code takes from column 0 without checking. Read-only; re-run it before deciding whether `duplicate_period` belongs on the default path |
| `.claude/tools/replay_duplicate_arbitration.py` | The other half of `DPC-3`: of the contradicted cells the tool above counts, **how many can `_resolve_duplicate_identity` actually decide?** Measured 2026-09-09 — **6 of 34**; the other 28 land on no `OP_IDENTITY` term and nothing on the page can arbitrate them. Read-only |
| `.claude/tools/replay_cash_close.py` | ⚠️ **THE TOOL THAT TALKED THE CHANGE IT WAS BUILT FOR OUT OF SHIPPING, and it is kept for that.** Replays the closing-cash anchor over every cash flow in `reports/pdf_ocr/` — no OCR. Adding GAS's `Tiền tồn cuối năm` spelling to `CASH_CLOSE` moves **0** accepted anchors and recovers **24** refused cash flows with the filing's own `opening + net + fx == closing` confirming each, and was still REVERTED: a needle satisfies `reconcile`'s GATE and fills NO COLUMN, so it writes `pdf` rows with a blank closing-cash cell. `GCW-1`'s `cash_wording` schema ALIAS is the right route. Read-only |
| `.claude/tools/check_index.py` | the narrower check: fails if any `.md` in the repo is unrouted by this index. Called by `state_check.py` |
| `.claude/tools/open_claude_tab.py` | runbook `O8` — opens a new Claude tab by firing the key bound to `claude-vscode.editor.open`, after checking `remoteControlAtStartup` and that binding. ⚠️ **The FOURTH route**: the `code` CLI, the `vscode://` URI and the IDE WebSocket were all measured dead 2026-09-06 and stay dead. Windows only (`SendInput`, and a FIFTH route added 2026-09-06 — a posted chord that needs no foreground window, **1-for-7**, `FGD-1`). ⚠️ It verifies by the WINDOW TITLE and exits 1 when it cannot show a tab opened |

## Tier 2 — package evidence (`.claude/context/`) — **open ONE, only when you touch it**

⚠️ **THESE MOVED OUT OF `src/` ON 2026-09-06, AND THIS SECTION ARGUED AGAINST THE MOVE UNTIL
THEN.** It read *"These stay beside their package **on purpose**: they are the evidence behind
`CLAUDE.md`'s claims, and **locality is what keeps them true**"*. The twelve
`src/<package>/CONTEXT.md` are now `.claude/context/<package>.md` — **one file per `src/`
subfolder, named after it** — and the seven long-form guides that sat beside them are in
`.claude/docs/`.

⚠️ **THE COST IS THAT A PACKAGE FOLDER NO LONGER ADVERTISES ITS OWN EVIDENCE**, so this index is
the only routing left: `src/feature_selection/` used to hold the file that explained it, and now
holds only code. **`.claude/tools/state_check.py`'s check 2 was keyed on `endswith("CONTEXT.md")`
and passed silently through the move** — a monitor that stops monitoring — and was re-keyed on
the directory in the same commit.

**~247k tokens across the twelve — never open more than one or two.**

| open this | ~tokens | when you are… |
|---|---|---|
| [web_scraper.md](../context/web_scraper.md) | **64.9k** | touching a scraper, the PDF/OCR statement parser, or `raw_data/` layout |
| [orchestration.md](../context/orchestration.md) | **47.6k** | touching Dagster, `config.json`, any asset, any bronze/silver/gold table, a scrape, or ⚠️ **the FILTER layer** (§"FILTER") |
| [feature_selection.md](../context/feature_selection.md) | **45.4k** | running or reading a selection, or quoting any IC / null / bar. §15a the country-sweep guide, §16 the GPU conversion, §19 the ranker measurement |
| [orchestration-preprocessor.md](../context/orchestration-preprocessor.md) | **26.1k** | changing HOW a table is built — the `_ingest_*` / `_helper_*` transform library. ⚠️ It is `src/orchestration/preprocessor/`, a NESTED package, which is why the file name carries both halves |
| [walkforward.md](../context/walkforward.md) | **16.0k** | asking whether a result survives more than ONE split, or which MODEL to use. §8 is PRF-8 (three architectures, 101× capacity, all tied) |
| [model.md](../context/model.md) | **12.0k** | training, adding a model type, or quoting a run's numbers. §1a is the RUN STANDARD |
| [backtest.md](../context/backtest.md) | **8.1k** | asking whether a signal is TRADABLE. §3 the cost identity that decides the horizon; §5 the single-stock answer ("no trade") |
| [pipeline.md](../context/pipeline.md) | 7.4k | the chain, staleness, `--root`/`--scope`, `--rescrape`, adding a stage, `pipeline.freshness` |
| [final_features.md](../context/final_features.md) | 6.8k | building or rebuilding a `__final__` table |
| [train_test_creator.md](../context/train_test_creator.md) | 5.0k | building a dataset, or the purge/impute/scale/window steps |
| [result_evaluator.md](../context/result_evaluator.md) | 4.1k | scoring, the metric set, panel-vs-series grain. ⚠️ **STALE** — predates `index.py` and `NUL-3` |
| [sentiment.md](../context/sentiment.md) | 3.4k | anything news / text / PhoBERT |

### Session tooling — `.claude/` (started 2026-09-06)

⚠️ **How to WORK, as opposed to what the project knows.** `.claude/rules/common.md` is the only
auto-loaded part (Tier 0); everything below is **lazily loaded — open the one file for the job in
front of you.** ⚠️ **The runbook here is the only runbook.**

| open this | ~tokens | when you are… |
|---|---|---|
| [../.claude/rules/standing-rules.md](../rules/standing-rules.md) | **4.9k** | ⚠️ **about to lean on one of the 24 numbered rules.** `CLAUDE.md` §5 carries them as one-liners and is auto-loaded; **this is the EVIDENCE behind each**, plus §5a what was RETIRED (`src/main.py`, `switch_config.json`, …), §5b what was REMOVED, and §8's conventions. ⚠️ **Not auto-loaded** — `common.md` is the only rules file `CLAUDE.md` imports, and that is deliberate: the one-liners fit the always-loaded budget and the war stories do not |
| [../.claude/workflows/README.md](../workflows/README.md) | **1.1k** | starting any recurring JOB — it routes the nine step-by-step guides below |
| `.claude/workflows/*.md` | **13.6k** total | *start a session* (0.7k — **one command, `O8`, then stop**; it takes NO arguments, ⚠️ **and §1b is the 2026-09-06 measurement that closed that question** — `TAB-1`) · *run the chain* (1.7k) · *run a selection* (1.8k) · *refresh the data* (1.7k) · *OCR a ticker* (1.8k) · *summarize ocr* (0.8k — reads ONE csv, opens nothing else) · *quote a number* (1.8k) · *record a finding* (1.7k) · *finish and commit* (1.5k). Re-measured 2026-09-06. ⚠️ **Each is the ORDER; the commands are cited by runbook row ID** so a flag changes in one place |
| `.claude/commands/*.md` | **4.0k** total | you want to LAUNCH a workflow rather than read it. Ten Claude Code slash commands, one per job plus `/wf-list` — `/wf-<workflow-name>` reads `.claude/workflows/<name>.md` and executes it in order. ⚠️ **A launcher copies NOTHING**: it names the workflow, the way a workflow names a runbook row, so a step changes in one place. ⚠️ **Added 2026-09-06** — a file dropped in this folder becomes a slash command with no registration step, which is also how a stale one keeps being offered |
| [.claude/runbook/RUNBOOK.md](../runbook/RUNBOOK.md) | **5.2k** | you want the COMMAND — ~40 templates as one table (`O`/`C`/`W`/`D`/`E`/`F` row IDs), each with what it writes, its measured cost, and the step that must follow it |
| [../.claude/current_state/README.md](README.md) | **1.3k** | writing a measured SNAPSHOT (freshness, coverage, what exists). ⚠️ **Not a fifth register** — `CLAUDE.md` §6 holds what the state MEANS; this holds what a command PRINTED, with the command named |
| `.claude/current_state/*.md` | — | the snapshots themselves. ⚠️ **Empty until someone measures something, and that is the correct state** |

### Module descriptions — the OTHER kind of file in `.claude/context/`

⚠️ **Same folder, same naming rule, different question.** A module description answers *"what is
in this folder, what is actually USED, and what will bite me"* — a structural read of the code
with measured reference counts — where a package doc above answers *"what did we measure, and
what did it prove"*. They share `.claude/context/` because they share the **one file per `src/`
subfolder** rule; a module with both would carry them in one file.

| open this | ~tokens | when you are… |
|---|---|---|
| [dtos.md](../context/dtos.md) | **3.2k** | touching any dataclass under `src/dtos/`. ⚠️ **The package is a mixed-age attic**: `tabular_database_driver_dtos` carries ~232 reference sites, `Task` 36, and `ModelConfigDto` / `ConfigDto` have **0** — "it is in `dtos/`" does not mean it is used |
| `.claude/context/*.md` | — | the rest, as they are written |

### Long-form guides — `.claude/docs/`

⚠️ **Moved out of `src/` on 2026-09-06 with the package docs.** These are depth behind ONE section
of a Tier 2 file, not evidence in their own right — which is why they are not in `.claude/context/`
and do not follow its one-file-per-package rule.

| open this | ~tokens | when you are… |
|---|---|---|
| [PDF_OCR.md](../docs/PDF_OCR.md) | 14.4k | **running the FILING OCR on a T4** — the control notebook, the `QUARTERS` filter, what a verdict means, and the five things that have gone wrong. ⚠️ **§1a is what *"OCR ticker `<SYM>` LOCAL\|KAGGLE"* means** — a prepared per-ticker clone that WAITS, and how each parameter is decided from disk; §6 is when a recovered quarter may be merged; **§8 is what is still MISSING across every ticker at once** — 130 cells, of which **56 winnable and 46 of those VIC** |
| [kaggle_gpu.md](../docs/kaggle_gpu.md) | 8.1k | running a repo notebook on a Kaggle T4 — the payload dataset, the parameter patcher, `rehearse`, **§7b PANEL MODE**, and §7's five measured traps. ⚠️ It was `src/kaggle_gpu/README.md`, renamed on the move because a bare `README.md` cannot sit in a shared folder |
| [RANKER_COMPARISON.md](../docs/RANKER_COMPARISON.md) | 4.6k | asking which ranker to keep, drop or add — the scorecard behind `feature_selection` §19 |
| [FUNDAMENTAL_INDICATORS.md](../docs/FUNDAMENTAL_INDICATORS.md) | 3.5k | the fundamental indicator definitions |
| [NULL_DRAWS_VI.md](../docs/NULL_DRAWS_VI.md) · [NULL_DRAWS.md](../docs/NULL_DRAWS.md) | 3.2k · 3.1k | how many null draws, and why (VI · EN) |
| [model_lstm_legacy_configs.md](../docs/model_lstm_legacy_configs.md) | 0.4k | about to run one of the 27 configs in `src/model/lstm/configs/_legacy/`. ⚠️ **They still resolve and must NOT be used for a new result** — no purge, `ffill().bfill()`, no null. The warning used to sit in that folder as `README.md`; it is here now, so read it before you reach for one |

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
   `CLAUDE.md` §6 date, a `.claude/context/` file changed without the hub, disagreeing issue counts, an unrouted
   `.md`, a drifted token cost, a broken link. **It REPORTS and never rewrites**: these counts are a
   SCAN, not a decrement, and a confidently wrong number is worse than none. Nothing enforces it at
   commit time by choice, so running it is the discipline. **this file's tooling row is the
   procedure and says where each kind of change is recorded.**
3. **`CLAUDE.md` stays at the repo root** and is not moved into `docs/` — Claude Code auto-loads it
   from the root only, so moving it would silently switch off the one file guaranteed to be read.
4. **This index is routing, never content.** If you find yourself explaining a measurement here, it
   belongs in `CLAUDE.md` or the relevant `.claude/context/` file instead.
