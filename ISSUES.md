# Issue register — pipeline (2026-08-09)

Stable codes for the
`feature_selection → final_features → train_test_creator → model → result_evaluator`
chain. **Codes are permanent**: a resolved issue keeps its code and its row, never
renumbered and never reused, so an old message that says "BNK-1" still resolves.

## Open (5)

| code | severity | issue | lives in |
|---|---|---|---|
| **EVD-1** | substantive | Nothing upstream ever cleared a null. **Partly measured 2026-08-09**: the bare `pool__basic` run was reproduced bit-identically and given the 20-draw null it never had — it **fails at `z = +1.46`** (§10b), so the archive now reads `no_null=18, failed_null=2` instead of `no_null=19, failed_null=1`. The remaining 18 are not in that price class: `basic+economy_usa` alone spends **12,255 s on `permutation` per pass** at 1,458 channels, so one 20-draw null is ~68 CPU-hours and all 18 is over 1,000. **Still not fixable downstream** — everything below inherits it. Next lever is §12c (`permutation_repeats` 10→3, CUDA, 10 draws), with the observed re-run at the same settings per §8. | `feature_selection/CONTEXT.md` §14b, §10b |
| **NUL-1** | substantive | The evaluator's null shuffles outcomes against a *finished* score vector, so it prices in nothing about feature selection, architecture search or early stopping. It can kill a run, never confirm one. Structural — the evaluator cannot re-run the selection. | `result_evaluator/metrics.py` §3b |
| **DRF-1** | substantive | **Worse after the STL-1 rebuild**: 126 of 721 VCB channels put >1% of the test set beyond 5 train-sigmas and **18 put 100% of it there** (was 48 and 4 at 203 channels). The tail is also far more extreme than the old "+9 to +13 sigma" — `european_union__…__euestr` sits at **+885 sigma** and `usa__…__resppllopnww` at **−282**. Monotone macro levels. Reported in `drift.csv`, acted on by nothing. The fix belongs upstream — differenced channels, not a silent drop here. | `train_test_creator` → `drift.csv` |
| **COV-1** | substantive | Channels were *selected* despite barely existing. **Far wider than first recorded**: 26 on the VCB table have zero TRAIN coverage (all four `prop_*` plus 22 late-starting macro series), and across the archive **248 of 952 shortlisted rows — 26% — sit below 0.95 coverage**, the worst at **0.024**. **Partly addressed 2026-08-09**: `outstanding.csv` now carries `coverage` + a `PARTIAL` flag (§14d), so the fetch list states the risk instead of a later stage discovering it. It **flags, it does not filter** — the archive cannot see where the train/test cut falls, and dropping 248 rows would change the fingerprint set and trigger the STL-1 domino. The defect itself is unchanged: the selection still ranks these channels on the fraction of history where they exist. | `train_test_creator/dataset.py::_screen`, `feature_selection/outstanding.py` |
| **RPR-1** | reproducibility | `src/train_test_set/` and `src/model/runs/*/` are git-ignored (they are large). A fresh clone has no datasets and 27 runs stripped to `results/`. Most project history is re-derivable, not reproducible. A design trade-off, not a bug — recorded so it is a choice rather than a surprise. | `.gitignore` |

## Resolved (18)

| code | issue and fix | pinned by |
|---|---|---|
| ~~**PIP-1**~~ | Feature selection is manual by design, so `--apply` cannot cold-rebuild — but **`Stage.manual` was set and never read**. The plan decided "MANUAL" from `stage.apply is None`, and the selection stage *has* an apply (it refreshes shortlists), so the flag was dead and the plan printed a bare `ran`. The design is unchanged; it is now **stated**: a `manual` column in the plan, `ran (refresh only)` when a manual stage applies, and `MANUAL — cannot be produced here` when one is not ready. | `python -m pipeline` → `manual` column reads `True` on `selection` only |
| ~~**STL-1**~~ | The VCB table had drifted 26 columns from its own shortlists and nothing noticed, because "the table exists" was the only check. Now every table's `COMMENT` carries a **fingerprint** — a sha256 over its sorted `(source_table, channel)` set — and `status_final_features` compares it against what the current reports would produce. `max_features` was dropped from `SETUP_KEYS` (it said `12` while runs kept 10–236) and the real cut parameters are stamped into `outstanding.csv`. Both tables dropped and rebuilt: **VCB 203 → 750 channels**, bank 10 → 14. | `status_final_features` → `current — fingerprint 505fbe21a1f0 matches` |
| ~~**CMP-1**~~ | Classifier core metrics were measured against the 0/1 label. The old fallback *could not* have worked — a classification dataset's `y_test` **is** the label, so `load_dataset` returned the thing it was called to replace, silently. Now read from **`pool__targets`** on `(date, ticker)`; when no schema is recorded, `ic` and `long_short` are **NaN** rather than a number in the wrong units, while `dir_auc`/`hit_rate` survive (they need only the up/down label). | all 18 classification rows: `ic` NaN, `dir_auc` present, `return_source = unavailable (no schema recorded)` |
| ~~**BNK-1**~~ | The bank table could not enter the pipeline: the reader trusted the table NAME for the target. Whole chain now runs — 26,964 / 12,524 / 13,028 windows, **no skill on either split**. | `test_a_rank_table_reads_the_column_it_actually_stores` |
| ~~**TGT-1**~~ | The first BNK-1 fix rebuilt `return_{h}day` from the horizon, duplicating `final_features._stored_target` where it could drift. Replaced by two authoritative records: `outstanding.csv` says what is a **channel**, `pool__targets` says what is a **label**. No horizon string is constructed anywhere. | `test_no_horizon_string_is_ever_constructed` |
| ~~**PNL-1**~~ | An N-ticker panel scored as one series was wrong three ways, each flattering the model: `n_eff = n/h` counted 20 banks on a date as 20 observations (2,606 vs 130.6); a pooled Spearman mixed "which bank beats which" with "is today a good day"; the row-block null tore each date's cross-section apart. `evaluate_panel` fixes all three; grain is detected from the `ticker` column, never a config flag. | 5 tests incl. `test_panel_n_eff_counts_dates_not_rows` |
| ~~**NUL-2**~~ | `block_shuffle` pads with NaN; permuted forward they landed in the retained slice, so **10 of 200 draws survived** while the p-value was still divided by 201 — understating every p-value ~20×. Bar `+0.046 → +0.113`, `p = 0.025 → 0.522` on the VCB run. | `test_every_draw_is_usable_despite_the_shuffle_padding_with_nan` |
| ~~**EFF-1**~~ | `n_eff` priced in label overlap only. `n_eff_windowed = n/(d+h-1)` — the purge-gap count, the point at which two windowed samples truly share nothing — is now reported beside it (127.0 vs **26.5** on 635 test samples). Both are kept: diverging from `feature_selection.effective_sample` silently would be worse than being optimistic openly. | reported per split |
| ~~**NAM-1**~~ | `dir_accuracy` named two quantities — the legacy sign hit rate at 0 and the core's hit rate at the score's median. The core metric is now **`hit_rate`**; `dir_accuracy` unambiguously means the legacy one. | `CORE_METRICS = ("ic", "dir_auc", "hit_rate", "long_short")` |
| ~~**IDX-1**~~ | `index.csv` had accumulated two metric eras and a naive `dropna()` silently picked one. `python -m result_evaluator --rebuild-index` regenerates it in one pass from every run's predictions — one column set, one definition. | 29 rows × 52 columns, single era |
| ~~**DUP-1**~~ | Three superseded run folders removed; they referenced datasets that no longer exist after the STL-1 rebuild. | 5 `d20_h5` folders → 2 |
| ~~**PKG-1**~~ | `src/evaluator/result_evaluator.py` deleted — unused, and its name collided with the package. `data_evaluator.ipynb` kept; it is a different tool. | `src/evaluator/` holds one notebook |
| ~~**OLD-1**~~ | `unified_schema_creator.ipynb` (1.2 MB) deleted — superseded by `orchestration/assets/unified.py`, imported by nothing, recoverable from git. | gone from `src/train_test_creator/` |
| ~~**OLD-2**~~ | 27 legacy configs moved to `configs/_legacy/` with a README stating what changed and warning that a legacy run lands in `index.csv` with the core columns blank until `--rescore`. | `configs/` holds the 2 current files |
| ~~**TCK-1**~~ | `tickers_*.npy` was a dtype-`object` array, so `np.save` pickled it and every reader uses `allow_pickle=False` — the file existed and was unreadable, which is exactly how a panel gets silently scored as one series. | `test_the_ticker_array_is_unicode_not_object` |
| ~~**HDR-1**~~ | `csv.DictWriter` writes fields in the code's order; appending under a differently-ordered header silently misaligns every new row. `append_run` now migrates the header. | migration verified on the live `index.csv` |
| ~~**ENC-1**~~ | `model/common/data.py` opened `metadata.json` with a bare `open()` — cp1252 on Windows — and the provenance `COMMENT` carries a `⚠️`. | notebook executes end to end |
| ~~**GIT-1**~~ | Dropping the `reports/feature_selection/*/` rule was not enough, and the 384 already-indexed files hid it: a **new** run's `.csv` was still caught by the blanket `*.csv`. Negations added after it. | exit-code test: new run folder tracked, controls still ignored |

## Priority

No blocking issues remain. The four substantive ones are all upstream of the code:
**EVD-1** and **NUL-1** are about what a null can prove, **DRF-1** and **COV-1** are
defects in the *selection*, not in the stages that consume it.

⚠️ **What is left open is what is EXPENSIVE or STRUCTURAL, not what is unknown.**
The 2026-08-09 pass took every cheap thing: EVD-1's one affordable null was measured
(it failed, `z = +1.46`), COV-1's true extent was measured and stamped into the
deliverable (248 of 952 rows), and PIP-1 turned out to be dead code and closed. The
remainder is priced: EVD-1's other 18 nulls are 1,000+ CPU-hours, NUL-1 cannot be
fixed by an evaluator that does not re-run the selection, DRF-1 needs differenced
channels upstream, and RPR-1 is a deliberate trade-off.

⚠️ Note what none of the 18 fixes changed: **every run in the archive still sits
inside its own null.** The STL-1 rebuild made the VCB result worse, not better —
`test ic −0.011 → −0.072`, `r2 −0.08 → −0.90` — which is the expected consequence of
handing an LSTM 724 channels instead of 202 on 2,918 training windows.
