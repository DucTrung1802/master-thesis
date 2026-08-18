# Context — `src/train_test_creator`

> 🗺️ **Project hub: [CLAUDE.md](../../CLAUDE.md)** — the whole project in ~5k tokens
> (verdict, chain, standing rules, routing). Read that first; this file is the depth
> behind one stage.

> Reads **`unified_schema_vcb.return_5day__final__d20_h5`** and writes windowed
> train/val/test tensors under `src/train_test_set/`. Rebuilt 2026-08-09.
>
> This is the stage between `final_features` (which writes the table) and `model`
> (which reads the tensors). It **selects nothing, tunes nothing and joins nothing** —
> the table already holds exactly the channels chosen upstream, so all that is left is
> *split → impute → scale → window → save*.

```
python -m train_test_creator                     # print the plan, write nothing
python -m train_test_creator --save              # write the dataset folder
python -m train_test_creator --save --replace    # ⚠️ overwrite an existing one
python -m pytest train_test_creator/test_dataset.py -q
```

## 1. What it built (2026-08-09)

`vcb__return_5day__final__d20_h5__tr70_val15_test15__std`

| split | samples | shape | first label | last label |
|---|---|---|---|---|
| train | 2,918 | `(2918, 20, 724)` | 2009-07-27 | 2021-04-05 |
| val | 610 | `(610, 20, 724)` | 2021-05-13 | 2023-10-17 |
| test | 635 | `(635, 20, 724)` | 2023-11-21 | 2026-06-18 |

4,235 rows read, 5 dropped as the unlabelled tail (`h=5` sessions with no complete
forward window), 4,230 labelled. Of the 4,211 windows those rows can form, **4,163
survive and 48 are purged** — 24 at each boundary, which is §3.

**750 channels in the table, 724 kept and 26 dropped** (§5). Of the kept, 721 are
standardised and 3 are bounded. Dataset hash `686ff164619b29d9`.

⚠️ **These widths are post-STL-1 and this section read `202` of `203` until
2026-08-09.** The table grew from 203 channels to 750 when the measured cut replaced
`max_features=12` upstream (`feature_selection/CONTEXT.md` §14c) — so the screen now
drops 26 channels rather than 1, and every shape here is 3.6× wider.

## 2. Why the old notebook could not be patched

⚠️ **`train_test_creator.ipynb` was deleted 2026-08-09** and is recoverable from
commit `d2f9771`. It read a VIEW called `<target>__lb<L>__final` that
`unified_schema_creator.ipynb` used to build. **That view does not exist any more** —
selection moved to `feature_selection` and its result is materialised by
`final_features` as `<target>__final__d<d>_h<h>`. The notebook was also carrying two
dead cells (7–10) that referenced an undefined `VOLUME_COL` and `add_*` imports that
were never made, and cell 12 silently recomputed the `df_clean` that cells 8–10 had
just built — so the TA-tuning half had no effect even when the view existed.

Four things follow from reading the new table, and each is a behaviour change:

| | old notebook | now |
|---|---|---|
| `d`, `h` | free parameters (`LOOKBACK_DAY = 5` against an `lb20` view) | **parsed from the table name** |
| read | `driver.select` | `UnifiedSchemaReader.read` (typed) |
| selection | XGBoost gain + SHAP + permutation, re-run here | **upstream only** |
| split | `d-1` rows of overlap, no purge | **purged by `d + h - 1`** |

## 3. ⚠️ The purge — the substantive fix

Sample `i` reads rows `[i-d+1, i]` and carries the label for `[i, i+h]`. For a train
sample at `i` to share nothing with a val sample at `j`:

```
i + h < j - d + 1     ⟹     j - i > d + h - 1
```

so **24 samples**, not 5, are dropped from the end of train and the end of val. That
is exactly `feature_selection.PurgedWalkForward.gap` — the same purge the channels
were *selected* under, so the split here and the CV upstream agree about what a leak
is. Verified on the real table: the last train label period ends at row 2941 and the
first val window opens at row 2942.

The old notebook instead started each split `d-1` rows early and justified it in a
docstring: *"no leakage — features are scaled with train-only statistics, targets only
look forward"*. **Both halves of that are true and neither is the issue.** The leak is
the train LABEL that reaches `h` days into the val window; scaling has nothing to do
with it, and "targets only look forward" is the reason there is a leak, not the reason
there is not.

⚠️ **Windows still warm up across the boundary** — a val window may read train ROWS.
That is not leakage: those rows are the past and a live model would have them. Only
labels are purged. This is also why val loses no `d-1` warmup samples.

⚠️ **`--no-purge` exists and is for comparison only.** It reproduces the old
behaviour so the cost of the leak can be measured; a result must not be produced with
it.

## 4. ⚠️ Imputation is the TRAIN median, never `ffill().bfill()`

The old notebook forward- then back-filled. `bfill` fills a **leading** gap with the
first **future** observation, and on this table that is not a rounding error:

| | channels | worst |
|---|---|---|
| incomplete coverage | **199 of 750** | `germany__…__deelpc`, coverage **0.024** |
| **zero coverage in TRAIN** | **26 of 750** | all 26 are dropped by §5 |

`prop_buy_vol` remains the clearest case: 3,382 of 4,230 rows missing, so under `bfill`
80% of it — the entire training set — would be a value first observed in 2023.

So the rule is **the median of the train slice**, matching
`FeatureSelector._impute` line for line, including its `fillna(0.0)` for a column that
is NaN throughout. That is the imputation the ranking which chose these channels was
computed under; using a different one here would mean the model sees a column the
selector never scored.

⚠️ **The "train slice" is not `date < val_start_date`.** It is the rows carried by a
train SAMPLE — everything up to the last train label, i.e. the cut *minus the purge
gap*. The purged tail belongs to no train sample, so letting it into a median or a
scaler would put val-adjacent rows into the very statistics the purge exists to keep
out. On this dataset that is 2,937 rows ending 2021-04-05, not 2,961 ending
2021-05-12.

## 5. ⚠️ 26 channels are dropped, and that is a finding not a cleanup

Each has **zero coverage across the whole train slice** — empty until partway through
the sample, live afterwards. Imputed, such a channel is a constant through training and
a varying signal at test: the model cannot fit a response to it and is handed one
anyway.

⚠️ **It was 1 channel at 203 and it is 26 at 750** (post-STL-1). The set is **all four
`prop_*` channels** — `prop_buy_vol`, `prop_buy_val`, `prop_sell_vol`, `prop_sell_val`
— plus 22 macro series that begin after the train cut, the worst reaching only 2.4%
coverage overall. `prop_buy_vol` is still the clearest case: zero in train, 20%
overall, empty until 2023.

`on_untrainable="drop"` (default) removes it and records the reason in
`metadata.json`; `"keep"` and `"raise"` are there for when that is not wanted. The
same test catches a channel that is constant in train for any other reason
(`train_nunique <= 1`).

⚠️ It was **selected by a feature-selection run**, which is the part worth noticing.
A run that ranks a channel highly on the full panel can be ranking it on the 20% of
history where it exists. `coverage.csv` ships beside the tensors so this is checkable
per channel rather than discovered once.

## 6. ⚠️ Scaling is train-fit, and 126 channels leave the train range anyway

`StandardScaler` on the continuous columns, fit on the train slice of §4 only, applied
to all three splits. Bounded columns — `_sin`/`_cos` and 0/1 flags — are passed
through unscaled. **On this table that finds 3**: `usa__economy__business__fred__usrec`,
`usrecd` and `usrecm`, the US recession indicator as 0/1. (At 203 channels it found
zero, which is why the classifier exists.)

The panel is non-stationary by construction, so a train-fitted scaler necessarily puts
part of the test set outside the range the model saw. `drift.csv` measures how much:

| | at 203 channels | **now, at 750** |
|---|---|---|
| scaled channels | 202 | **721** |
| >1% of TEST beyond 5 train-sigmas | 48 | **126** |
| **100%** of TEST beyond 5 train-sigmas | 4 | **18** |

⚠️ **The STL-1 rebuild made this worse in both absolute and relative terms** — 17.5% of
channels now drift past 1% against 23.8% before, but the count fully outside more than
quadrupled, and the tail got far more extreme. The old worst four sat at +9 to +13
sigma; the current worst two are
`european_union__economy__money__economics__euestr` at **+885 sigma** and
`usa__economy__money__fred__resppllopnww` at **−282**, with
`united_kingdom__…__gbmr` (+12.6) and `european_union__…__euppi` (+12.4) behind them.
These are macro **level** series that trend monotonically; standardising
them maps the test period to a region the training set never occupied. This is
reported, not filtered — `feature_selection/CONTEXT.md` makes the same argument about
raw levels in a window, and the fix belongs upstream (a differenced channel) rather
than in a silent drop here.

## 7. The split is cut on DATES, and windows are built per TICKER

⚠️ A row-index cut works only because the VCB table has one ticker. On
`unified_schema_bank.rank_5day__final__d20_h5` (20 tickers, 53,921 rows) it would put
the same date in train for one ticker and in val for another, and the two would then
share a label period across the boundary. So the cut is on the **date axis** — on a
one-ticker table the two are identical — and windows are stacked **per (exchange,
ticker)** before being concatenated in date order. A single global stride would build
windows whose first days belong to one company and last days to another.

### 7c. The table name may carry a `__<scope>` suffix (2026-08-10)

`FINAL_TABLE` now accepts an optional trailing segment:
`return_5day__final__d20_h5__basic` parses to exactly the same `("return_5day", 20, 5)`
as the unsuffixed table. The suffix names the **feature block** a table was built from —
`pool__basic` alone, rather than the archive's union of 19 shortlists — and it exists
because `final_features` groups on `(schema, target, setup)`, a key with no term for
*which pools* (`final_features/CONTEXT.md` §0a). Without it the narrow build and the wide
build collide on one name and the narrow one can only be created by DROPPING the wide.

⚠️ **Nothing in this module branches on it.** `d` and `h` still come from the same place,
the channels are still whatever columns the table has, and `resolve_target` still reads
the stored target off the table rather than off the name (§7b). The scope is a label on
the input, and `dataset_name` carries the whole table name verbatim, so the dataset
folder states which one it read.

### ⚠️ 7b. The target column is resolved against the TABLE, not the name

`rank_5day__final__d20_h5` **stores `return_5day`**: a rank's value for a stock-date
depends on which other names are in the panel, so `final_features` refuses to freeze
one into a table (`final_features/CONTEXT.md` §5). Reading the name and demanding that
column made the entire bank schema unreachable — the table was fine, the reader was
wrong (issue **BNK-1**, fixed 2026-08-09).

⚠️ **There is no `return_{h}day` fallback, and the table's NAME is not consulted.**
The first fix built the column name from the horizon whenever the name's target was
absent — which duplicated `final_features.builder._stored_target` in a second place
that could drift from it. Two records already answer this exactly:

| record | says |
|---|---|
| `reports/feature_selection/*/outstanding.csv` | every **channel** the table was built from |
| `pool__targets` | every **label** the schema has |

So the stored target is *the one column that is neither a key, nor a channel, nor
absent from `pool__targets`*. Exactly one on both current tables; `resolve_target`
raises rather than choosing when it is not.

⚠️ **`pool__targets` gained `close_adjust_5day` / `close_adjust_10day` on 2026-08-12** —
the forward adjusted close, `LEAD(close_adjust, h)`. The label set this method matches
against is therefore six columns wide, not four. It does not change any current
resolution: a `__final__` table carries ONE stored target, and the forward price cannot
arrive as a channel because the selection excludes it (`feature_selection.run.
ALL_TARGETS`, which now verifies itself against the table). It DOES mean a table built
for `close_adjust_5day` resolves through the same path with no special case.

⚠️ **`outstanding.csv`'s own `target` column is NOT the stored column and cannot be.**
It reads `cs_rank_5day` for the bank runs — a rank is computed within a date across a
chosen universe and is deliberately never stored. That value is what the channels were
*selected for*, so it is read from there into `target.selected_for` (better than
rebuilding it from the table name, which drops the `cs_` prefix). `target.column` is
what was read; `target.derived` flags when the two differ.

⚠️ **The same rule detects a STALE table.** A column that is in no current shortlist
and is not a label means the table predates the last
`python -m feature_selection.outstanding`. That is reported, not raised — the table is
still readable. **It fired on 26 VCB columns until the STL-1 rebuild** (2026-08-09),
when the shortlists were regenerated after `selection_cut` replaced `max_features=12`
with a measured cut and the table went 203 → 750 channels. **It now fires on none**,
and `python -m pipeline` confirms the fingerprint matches on both tables.

| | rows | tickers | samples (train/val/test) | features |
|---|---|---|---|---|
| `unified_schema_vcb.return_5day__final__d20_h5` | 4,235 | 1 | 2,918 / 610 / 635 | **724** of 750 |
| `unified_schema_bank.rank_5day__final__d20_h5` | 53,921 | 20 | 26,964 / 12,524 / 13,028 | **13** of 14 |

⚠️ **The bank panel is ragged and stays that way** — 13,028 of 13,060 test cells are
populated (99.8%). Nothing is filled to make it rectangular; the missing cells are
carried as NaN into `result_evaluator`, which masks them per date.

⚠️ **`tickers_*.npy` is written with an explicit unicode dtype.** `.astype(str)` on a
pandas column gives dtype `object`, `np.save` then writes a *pickled* array, and every
reader in the repo uses `allow_pickle=False` — the file would exist and be unreadable,
which is precisely how a 20-ticker panel gets silently scored as one series. Pinned by
`test_the_ticker_array_is_unicode_not_object`.

## 6a. ⚠️ THE LABEL IS RE-RANKED HERE — `RNK-1`, fixed 2026-08-18

`final_features` does **not** store `cs_rank_{h}day`. A rank is computed within a date
across a chosen universe, so its value depends on which names are in the panel and on
`min_width` — properties of the RUN, not of the row (`final_features/CONTEXT.md` §5). It
stores `return_{h}day` instead, *"and the reader re-ranks"*.

⚠️ **No reader re-ranked.** `y` was the stored return while the shortlist above it had
been chosen against the rank, so the model was aimed at a label the selection never
scored. CLAUDE.md §2b measured exactly that swap, on the same panel and the same folds:
**the IC drops 4× and the hit rate falls below a coin.** It never produced a wrong NUMBER
— `result_evaluator.daily_ic` is a per-date Spearman, so scoring stayed cross-sectional —
which is why it survived two universes and a model sweep, and it is a candidate
explanation for §5d's *"the selection cleared its bar; the model did not clear its own"*.

`_label()` now reconstitutes it, and three things about how:

* **One definition, not two.** It calls `cross_sectional.cross_sectional_rank`, the same
  function the selection used. A test asserts the two agree at **atol = 0**.
* **`rank_min_width` is part of the LABEL, not a cleaning knob.** A date with fewer
  labelled names than this gets no rank at all. It must match the `min_ic_width` the
  selection ran with, and it is recorded in `metadata.json` under `target.label_recipe`.
* **Thin dates are DROPPED and counted separately** as `rows_unrankable`, not folded into
  the unlabelled tail — one is the end of the panel, the other a thin session inside it.

⚠️ **`metadata.json → target.column` CHANGED MEANING.** It is now what `y` IS
(`cs_rank_20day`); `target.stored_target` carries what was read (`return_20day`). Before
the fix there was one field, it held the stored column, and `y` was that column.

⚠️ **The universe is still the table's own ticker set** — a table holding 781 names ranks
over 781 even when the shortlist was selected over 150. That is `UNI-1`, a separate defect
in a separate package.

## 7a. Where this sits in the chain

```
final_features  →  THIS  →  model.lstm  →  result_evaluator
```

`python -m pipeline` prints the state of all five stages and runs the stale ones; see
`src/pipeline/CONTEXT.md`. Downstream, `model/lstm/train.py` **asserts** its config
against the `metadata.json` written here — `lookback`, `n_features` and (for a
classifier) the absence of a target scaler — and copies §1's source `COMMENT` into
every run's `lineage`, so the provenance travels one more hop.

## 8. The output folder is a contract with `model/`

`model/common/data.py` loads `X_/y_{train,val,test}.npy`, `dates_*.npy`,
`feature_scaler.pkl`, `target_scaler.pkl` and `metadata.json` **by name**, and hashes
the six tensors. Renaming one returns `None` there instead of raising here, so the
names are fixed. Verified: `load_dataset` reads this dataset unchanged
(hash `686ff164619b29d9`, `n_features=724`, `lookback=20`).

Written beside them, and ignored by the loader: `tickers_*.npy` (which name each
sample belongs to), `coverage.csv` (§5) and `drift.csv` (§6).

⚠️ **The folder names its INPUT.** `vcb__return_5day__final__d20_h5__tr70_val15_test15
__std` contains the source table verbatim. The old scheme
(`vcb_return_5day_lb20_h5_final_...`) rebuilt a table name out of parts and so could
describe a table that was never read — the same argument as the report-folder rename
in `feature_selection` (commit `9f8f5b0`).

⚠️ **`save` refuses to overwrite without `replace=True`.** A half-rewritten folder
still loads: the model stage hashes the tensors, so a stale `metadata.json` beside
fresh tensors passes every check it makes.

## 9. ⚠️ What this stage does NOT assert

That the features are worth having. **18 of this table's 19 source runs computed no
null, and the 19th failed its own** (`feature_selection/CONTEXT.md` §14b), and 725 of
the 750 channels were chosen by exactly one run (`final_features/CONTEXT.md` §6) — the
table is a union of disjoint shortlists, not a consensus.

⚠️ **This dataset's `metadata.json` still says "runs that computed no null", which is
now imprecise and is deliberately NOT hand-edited.** It was written when the dataset
was built; its claim that *no bar was cleared* remains true, and editing a built
dataset's metadata in place is how a folder stops describing its own tensors. `metadata.json` carries the source table's `COMMENT`
verbatim so the provenance travels one more hop with the data. This module reshapes
those channels. It does not vouch for them.

## 10. Files

```
dataset.py        the whole pipeline — one module, because there is no branch in it
test_dataset.py   11 tests, no database, ~6 s
RUN__train_test_creator.ipynb   the notebook meant to be run
```

One other file, `model/common/data.py`, changed with this rebuild: it opened
`metadata.json` with a bare `open()`, which is cp1252 on Windows, and the provenance
`COMMENT` copied from the source table carries a `⚠️`. Every other `metadata.json`
reader in the repo already passed `encoding="utf-8"`; now this one does too.

`unified_schema_creator.ipynb` is still in this folder. It is the **superseded**
builder of the old `unified_schema_*` layer — that job belongs to
`orchestration/assets/unified.py` now, and the `<target>__lb<L>__final` views it made
are gone. It is kept for the record and nothing imports it.
