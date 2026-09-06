# Standing rules — the cross-cutting ones, learned expensively

> **Moved out of `CLAUDE.md` on 2026-09-06, VERBATIM.** The hub had grown to 2,549 lines while its
> own header called itself a map; [`.claude/rules/common.md`](../rules/common.md) R2 now caps it at
> **300 lines**, so the evidence lives here and the hub routes to it.
>
> ⚠️ **THE SECTION HEADINGS BELOW ARE UNCHANGED ON PURPOSE.** ~196 `§6-2-*` citations across this
> repo were already pointing at sections deleted from the hub earlier the same day; a `§5 rule 24`
> citation written before this move still resolves — **to this file**. Nothing was rewritten but
> the relative links, which climb one directory less.
>
> **`CLAUDE.md` §5, §5a, §5b, §5b-bis and §8 in full** — the 24 numbered rules with the evidence
> behind each, what was RETIRED, what was REMOVED, and the repo-wide conventions.
> ⚠️ **`CLAUDE.md` §5 carries all 24 as one-liners and is auto-loaded**; this file is the WHY,
> and a rule you are about to lean on is worth reading here first.

---

## 5. ⚠️ Standing rules — the cross-cutting ones, learned expensively

**Evidence**

1. **Every selection needs its own null, re-run whenever the pool or representation
   changes.** `zscore` moved its own bar 43% with no change to the data. A bar computed
   for one configuration says nothing about another.
2. **An absent null is recorded as absent, never implied to be a pass.** `"null": null`
   means no bar was computed. `evidence=no_null` is an *unknown*; `failed_null` is a
   *measurement*.
3. **`clears_bar` is the wrong summary whenever the null MAX exceeds the observed** —
   quote the max beside it. `pool__ta` cleared its p95 and one of 20 shuffled draws still
   beat the real data.
4. **Every single-score holdout needs a shuffled-label control.** With one score there is
   no fold spread, so the control IS the error bar — and it has reached +0.169 against
   real results of at most +0.071.
5. **Report the IC trend beside the mean.** A mean built from folds decaying to negative
   is not a signal.

⚠️ **HOW MANY DRAWS: 10 to FAIL something, 20 to PASS it** (decided 2026-08-18, on the
measurements below — not on taste). `p` is **not** the criterion: until a draw beats the
observed it is pinned at the `1/(n+1)` floor either way, 0.0909 at 10 draws and 0.0476 at
20, which says "no draw beat it" and never "p is small". **`z` is the statistic, and its
denominator is the null's `sd`.**

| measured | 10 draws | 20 draws |
|---|---|---|
| the p95 BAR | stable — P0-1's two independent 10-draw seeds gave **+0.0573 and +0.0565** | — |
| **`SE(sd)`** — the denominator of `z` | **0.0083** | **0.0051** |

So a 10-draw bar is already trustworthy; what 20 buys is a `z` you can defend. The rule
follows from the asymmetry: **when the observed lands below or near the null's mean, 10
draws settle it** — the 2026-08-17 six-pool sweep failed all six on 10 and needed nothing
more. **When it lands far above, the whole claim is *how far*, and that is `z`.** Rule 3
points the same way: quoting the null MAX beside the bar is a statement about the tail,
and 10 draws sample the tail half as well.

**Leakage & sample size**

6. **THE PURGE GAP IS `d + h − 1`, NOT `h`.** At `d=20, h=5` that is 24 rows. Purging only
   `h` leaves 19 rows of the test sample's own input window in training — the easiest way
   to make a windowed model look predictive. `feature_selection.PurgedWalkForward.gap`
   computes it and `train_test_creator` applies the same one at each split boundary.
7. **`n_eff` is `n/h` for a series and `n_dates/h` for a PANEL.** 635 test samples ≈ 127
   independent observations; 13,028 panel rows over 653 dates ≈ 130.6, not 2,606. Both
   figures are still optimistic — they price in label overlap, not input overlap.
8. **Imputation is the TRAIN-slice median, never `ffill().bfill()`.** `bfill` fills a
   leading gap with the first *future* observation. And the "train slice" is the rows
   carried by a train SAMPLE — the cut *minus* the purge gap, not `date < val_start`.
9. **Never standardise a 0/1 label.** A classification dataset must be built with
   `scale_target=False`; `_verify` raises otherwise.

**The pipeline lies in specific ways**

10. **A green asset is NOT evidence of fresh data.** `landed()` answers "is this folder
    empty?", not "did THIS run produce anything". `skip_existing=True` means a scrape can
    go green in 500 ms having fetched nothing — or worse, having fetched *some* things
    (one forex run refreshed 29 series and left 328 stale). **The per-series max date is
    the only honest freshness check.**
11. **A scrape and its ingests are separate assets, so "re-scraped" never implies
    "re-ingested".** Bronze once sat a full day behind a completed scrape — 5 countries
    against 19 on disk — with nothing raising. The check is one query:
    `COUNT(DISTINCT ticker)` in bronze vs the file count in `raw_data/`.
12. **Absent = OFF in `config.json`, and every asset must be listed** — the loader
    **raises** on an unlisted asset, so silence is never how something gets disabled. A
    group gate (`{"enabled": false, …}`) switches off a layer while modules stay visible.
13. **Selection IS the run plan.** `--select` is the whole mechanism; there is no switch
    file left to veto with — see §5a for what was retired and what replaced it.
14. **Disabling an asset does NOT disable downstream** — Dagster keeps the edge and the
    downstream still reads the folder from disk. To stop a chain, disable the downstream.

**Data types & I/O**

15. **`CREATE TABLE AS`, never a pandas round-trip.** psycopg2 returns `numeric` as
    `Decimal` → DataFrame dtype `object` → writer maps `object` to VARCHAR. A read-then-write
    silently turns every price column into TEXT. This trap is documented in three places
    because it has nearly fired in three directions.
16. **`driver.select` used to swallow exceptions and return an empty DataFrame.** It raises
    now, but prefer `UnifiedSchemaReader.read`, which also raises on empty.
17. **SQL `LIKE '_'` is a single-char wildcard** — `'pool__%'` also matches `poolXX`.
18. **Windows/cp1252**: open `metadata.json` and friends with `encoding="utf-8"` (the
    provenance comments carry `⚠️`); PowerShell 5.1's `Out-File -Encoding utf8` writes a
    BOM; and **never put `⚠️` in matplotlib chart text** — Segoe UI has no glyph and it
    renders as a box. ⚠️ **AND A ONE-OFF EDIT SCRIPT THAT `print`s THE TEXT IT IS EDITING
    DIES ON THE PRINT, HALFWAY THROUGH** — measured three times in one session on
    2026-08-23, each time after some replacements had been made and before the file was
    written, so the edit was silently partial. Start such a script with
    **`sys.stdout.reconfigure(encoding="utf-8")`**, or print counts rather than content.
    The write is the work; the print is the thing that kills it.
19. **Ephemeral scripts must load `.env` by explicit path** — they live in the scratchpad
    outside the repo, so `find_dotenv()` misses it and `PostgreSQLConnectionDto` raises
    "Password cannot be empty". Use `load_dotenv(os.path.abspath(".env"), override=True)`
    with cwd = repo root.
20. **Line-buffer anything long and write each unit to disk as it finishes.** A 4-hour null
    run was lost entirely to a `TextIOWrapper` that re-buffered on top of `python -u`.

**A finished run is not a run that checked itself** (added 2026-08-14, all three
measured on one `pool__forex` selection — `.claude/context/feature_selection.md` §17)

21. **A metric that CANNOT FAIL is not a pass — withdraw it.** `hit_rate` is
    `sign(pred) == sign(y)`, and on a price-LEVEL target (`close_adjust_{h}day`) every
    label is positive, so it is **1.0 by construction**. One run reported `+1.0000`
    beside `ic_mean −0.1638`. It is `NaN` → `—` now. The same target makes R² −24.9,
    which is a *measurement* of a bad target and stays.
22. **Coverage is a scalar and a scalar cannot see a FROZEN SOURCE.** A late starter and
    a channel dead since June both score 0.67. Read `trailing_null_sessions` beside it —
    328 of 357 forex channels had carried no value for 40 sessions. This is §5 rule 10's
    per-series max date, one level down, at the feature.
23. **An all-NaN train slice is imputed to the constant `0.0` and then RANKED.** There is
    no median to take, so `_impute` invents one in a unit the channel never had. **197 of
    357 channels in fold 1**, and **44 of the 66 SELECTED**. `validation.csv` carries
    `n_dead_train`/`n_dead_test` per fold now. ⚠️ A rising `ic_trend_per_fold` on a ragged
    pool measures **data arrival**, not a strengthening signal.

**⚠️ WHERE A NUMBER IS ALLOWED TO COME FROM** (added 2026-08-24, a standing DECISION,
not a measurement)

24. ⚠️ **FINANCIALS COME FROM THE FILING PDF AND FROM NOTHING ELSE. EVER.** Every
    balance-sheet, income-statement and cash-flow value must be OCR-parsed out of the
    company's own filed PDF in `raw_data/cafef/pdfs/`. **An HTML tab, a JSON endpoint, a
    web table or any other transcription is FORBIDDEN as a source** — not as a fallback,
    not "for the quarters OCR cannot read", not to fill a gap. **A quarter with no
    readable PDF is recorded as `missing`, and `missing` is the correct answer.** A
    transcription is somebody else's parse of the document, with their rounding, their
    omissions and their sentinels (CafeF's "not reported" is a literal `-1`, which reads
    as −1 dong in a column of billions), and once it is in the table nothing downstream
    can tell it from the filing. ⚠️ **THE CODE STILL DISAGREES WITH THIS RULE**:
    `CafefFinancialsBuilder` takes `use_api: bool = True` (`cafef_financials.py:485`,
    `:1629`) and its `from_api` docstring argues the opposite in as many words — *"This is
    not a lesser source … it is a BETTER one"*. That default is why **34 report-rows on
    disk today carry `source='cafef'`** rather than `pdf` (`FIN-1`). ⚠️ The `source`
    column of `bronze.cafef_financial_reports` is what makes this auditable at all —
    **keep it, and read it before quoting any fundamental.**

### 5a. ⚠️ RETIRED — do not go looking for these, and do not follow old advice about them

Phase 5 (2026-08-05/06) removed the second way to run things. **All four are gone from
disk**, verified 2026-08-10:

| gone | was | replaced by |
|---|---|---|
| `src/main.py` | the run plan — 8 `scraper.scrape()` calls + 3 ingest entry points | `--select` |
| `src/switch_config.json` | 676 flag keys gating every stage | `config.json`'s `parameters` (295 leaves) for what a scrape *enumerates*; `--select` for what *runs*. **A leftover copy now RAISES** |
| `src/data_preprocessor/` | the ETL library | **moved** to `src/orchestration/preprocessor/` — same code, now inside its only caller |
| `src/data_postprocessor/` | 652 lines joining macro + market columns | `gold.economy`, `gold.stock_market`, the unified schema |
| `DataPreprocessor.ingest_{bronze,silver,gold}_data`, `_run_layer` | hard-coded leaf lists that **deliberately did not raise** | one asset per `_ingest_*`, and a failed asset fails the run |

⚠️ **`SwitchHandler` the CLASS still exists and is still used** — but with an explicit
`switches` dict handed in, and **no default path**. Seeing it in the code is not evidence
that a config file drives anything.

⚠️ **Two package context files gave deleted-file instructions until 2026-08-10** — both §5s,
in `web_scraper` and `orchestration/preprocessor`, opened with *"edit
`src/switch_config.json`, then `python src\main.py`"*. Both are now rewritten with the
old text quoted as history. If you find a third, fix it the same way rather than deleting
it: the old mechanism explains the shape of what replaced it.

### 5b. Folders removed 2026-08-10 — ~3.1 GB

| removed | was | recoverable? |
|---|---|---|
| `charts/`, `pdfs/` | 2025-era chart PNGs and report PDFs — outputs of the viz notebooks below | ✅ tracked, `git checkout` |
| `src/visualization/`, `src/visualizer/`, `src/test/` | 4 viz notebooks + their helper + `compare.ipynb`. **Verified dead**: `visualizer` was imported only by those notebooks; they were imported by nothing | ✅ tracked |
| `ocr_env8/`, `ocr_env9/` | the two OCR venvs for experiments 8/9 (1.9 GB) | ❌ rebuild — recipe in each experiment's README. Production parsing is unaffected (`CAFEF_OCR_ENGINE=onnx` in `mt_env`) |
| `raw_data/_archive/` | the pre-2026-08-05 TradingView CSVs (803 MB) | ❌ **re-scrape only** — see `.claude/context/orchestration.md`. Budget ~2 h for forex |
| `src/logs/`, 38 × `__pycache__` | stale duplicate log dir; bytecode | n/a — regenerable |

⚠️ **`src/utils/constants.py` still defines `{SILVER,GOLD,ARIMA}_VISUALIZATION_LOG_FILE_BASE`.**
Now dead — the notebooks that used them are gone. Harmless, unreferenced, left alone
because this was a folder cleanup and not a code change.

### ⚠️ 5b-bis. `experiment/` REMOVED 2026-09-06 — and the CITATIONS TO IT STAYED

Commit `4cf907f0` ("CLEAN: clean up /experiment") deleted the whole folder: 10 experiment
sub-folders, `experiment/CONTEXT.md` (9.2k) and `experiment/experiment_10/CONTEXT.md` (44.0k,
the 23-paper literature record), ~95k tokens in all. ✅ **Tracked, so `git checkout 4cf907f0^ --
experiment` restores every byte** — unlike `RPR-1`'s run folders, which were gitignored and are
gone for good.

The routing was removed the same day: `.claude/current_state/INDEX.md`'s Tier 3 (7 rows) and its two Tier 4 rows,
plus the two rows in §7 of this file. **`Tier 4` was renumbered to `Tier 3`**, and the corpus
line fell from **128 files / ~705k tokens to 106 / ~639k**.

⚠️ **THREE CITATIONS IN THIS FILE NOW POINT AT A DELETED FOLDER, AND THEY WERE LEFT ON PURPOSE:**
§2a's literature row (*"23 papers … not one reports a naive baseline"*, `experiment_10`), §2c's
costed walk-forward (*"`model` §11, `experiment` exp_3"*) and §5b's `ocr_env8/9` row. **Each is a
measurement, and deleting the citation would delete the record of where it came from** — §8's rule
is to record what was measured. Read them as `RPR-1`'s shape: **a claim whose evidence is one
`git checkout` away**, not a live pointer.

---

## 8. Conventions that hold across the repo

- ⚠️ **BEFORE YOU COMMIT, RECORD THE STATE.** Run **`python .claude/tools/state_check.py`** and
  resolve what it reports. A commit that changes what this project KNOWS must also change
  where that knowledge is read: a new measurement lands in `CLAUDE.md` (§6 "State today",
  and its date is bumped) or in the package's own file under `.claude/context/`; a new defect gets a
  permanent code in `.claude/current_state/ISSUES.md`; a finished item leaves its number behind and is
  deleted from `.claude/current_state/TODO.md`; a new `.md` file gets a row in `.claude/current_state/INDEX.md`. ⚠️ **The
  script REPORTS and never rewrites** — the counts here are a SCAN, not a decrement
  (`ISSUES.md` keeps four fixed rows struck-through inside its Open table, so a row-counter
  returns 17 where the truth is 16), and a confidently wrong number is worse than none.
  ⚠️ **Nothing enforces this at commit time by choice** (2026-08-22): no git hook, so
  running it is the discipline.
- **`⚠️` marks a claim that cost something to learn.** Do not strip them; add them when you
  measure a new one.
- ⚠️ **"OCR ticker `<SYM>` LOCAL" / "OCR ticker `<SYM>` KAGGLE" IS A REQUEST FOR A PREPARED
  NOTEBOOK, NEVER FOR A RUN** (standing since 2026-09-04). Clone
  `src/kaggle_gpu/RUN__pdf_ocr_control.ipynb` to `RUN__pdf_ocr_control_<sym>.ipynb`, edit
  **cell 2 and nothing else**, strip the outputs, resolve the parameters against disk with
  `pdf_ocr_batch.plan_batch` + `job.seed_history` (read-only — no OCR, no quota), report what
  they resolved to, and **stop**. ⚠️ **The wait is the point and is not an extra step to
  optimise away**: a whole-ticker parse is 60-70 filings and hours of GPU — HOSE_FPT was 185
  min over 71 filings on a T4 — and `FORCE_EMPTY_BAND` and `OVERWRITE` are judgement calls
  about THAT ticker's history that a default cannot make. ⚠️ **A clone is deleted when the
  ticker is done, on `complete = True` AND `outstanding = 0`** — never on `complete` alone,
  which measures continuity from the start of the filing chain and reads `True` on tickers
  with open cells. The full procedure, and how each parameter is decided, is
  `.claude/docs/PDF_OCR.md` §1a.
- ⚠️ **ANYTHING THAT REPORTS PROGRESS PRINTS `xx.x% - task - sub-task - detail`, LEADING WITH
  THE OVERALL %** — the fraction of the WHOLE thing the reader started, never of the current
  file, step or cell. Standing since 2026-09-04, and formatted by **`src/utils/progress.py`
  and by nothing else**: two writers of one shape drift, and then a reader parsing the log
  gets one shape from one stage and another from the next. **A per-step percentage is what the
  three nested readouts already were** (§6-2-undequadragies), and a bare `print()` beside them
  is worse — the reader cannot tell a session 3 % in from one 96 % in. Composing a plan out of
  parts that each already report has two supported shapes and neither needs a new formatter:
  `Stages(..., final=False)` where a routine drives only a SEGMENT of the plan and its
  `done()` must not complete it, and `capture(nested=True)` where the captured code already
  leads with a percentage of its own denominator. ⚠️ **The number is a POSITION IN A PLAN and
  never a fraction of the time left** — say so where it is printed, and keep the weights
  NOMINAL and labelled (§5 rule 2 applies to a progress bar too). ⚠️ **A reader that matched
  the START of a log line is broken by this**: `progress.detail_of(line)` is the segment that
  used to BE the line.
- ⚠️ **NO STRIKETHROUGH, ANYWHERE — 108 markers were removed on 2026-08-23.** A closed
  item is marked with words (`✅ FIXED <date>`, `DONE`, `SUPERSEDED`, `RETIRED`) and its
  text stays in ordinary type. Struck-out text renders as damaged and reads as *"ignore
  this"*, which is the opposite of what a closed row is for here: **the measurement it
  leaves behind is the point**, and `ISSUES.md` rows are cited BY CODE from this file and
  from the package context files. Nothing was lost in the removal — every struck row already
  carried its status in words beside the markup.
- ⚠️ **A `TODO.md` number is a permanent NAME and priority is the ROW ORDER** (frozen
  2026-08-23). Codes are never renumbered or reused, exactly as in `ISSUES.md`; the list
  therefore starts at `P2` and need not stay monotonic. **Read the order, cite the
  number.** Three renumbers in two days preceded the freeze, and each one silently
  repointed every `P<n>` written before it.
- **Dates on findings, always.** A number without a date cannot be told from a stale one.
- **Record what was measured, not what was concluded** — the tables in these files are
  reproducible checks, which is why they are still trusted months later.
- **Nothing in `feature_selection` writes to the database**; `final_features` is the only
  stage that does. That boundary is enforced by the package split, not a comment.
- **Notebooks named `RUN__*.ipynb` are meant to be run.** Everything else (`study_*`,
  legacy `lstm_*`) is a finished write-up kept for the record.
- **A run folder is immutable.** Re-scoring rewrites metrics from `predictions_*.csv`;
  editing a built dataset's `metadata.json` in place is how a folder stops describing its
  own tensors.
- **git**: `src/model/runs/*/` and `src/train_test_set/` are ignored (only `index.csv` is
  tracked), so a fresh checkout has 27 runs stripped to `results/`. `reports/feature_selection/`
  IS tracked. `raw_data/` is ignored except `raw_data/cafef/financials/`.
