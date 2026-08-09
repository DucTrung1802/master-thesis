# Context — `src/final_features`

> Collects every run's `outstanding.csv` and materialises **one `<target>__final__*`
> table per (schema, target, setup)**. Built 2026-08-09.
>
> ⚠️ **This is the package that WRITES TO THE DATABASE.** `feature_selection`
> deliberately does not — "a selection is a result object and a set of figures, not a
> table" (`feature_selection/CONTEXT.md` §1). That rule is not bent here; the writing
> step was made a **separate package** so the boundary is visible in the import graph
> instead of living in a comment.

```
python -m final_features                     # print the plan, touch nothing
python -m final_features --apply             # create the tables
python -m final_features --apply --replace   # ⚠️ DROP an existing table first
```

## 1. What it built (2026-08-09)

| table | runs | pools | features | rows | cols | fingerprint |
|---|---|---|---|---|---|---|
| `unified_schema_vcb.return_5day__final__d20_h5` | 19 | 19 | 750 | 4,235 | 754 | `505fbe21a1f0` |
| `unified_schema_bank.rank_5day__final__d20_h5` | 1 | 1 | 14 | 53,921 | 18 | `f5615a68f556` |

⚠️ **Rebuilt 2026-08-09 from the current shortlists** (issue STL-1). The VCB table was
203 channels, built when the cut was a flat `max_features=12`; the measured cut keeps
10–236 per run, so the union is now 750. The old table was not even a subset — 26 of
its columns are in no current shortlist. Both tables now carry a fingerprint.

⚠️ **`d=20, h=5` is the whole study now.** A third table,
`unified_schema_vcb.return_5day__final__d1_h5`, was built from the `pool__fa` and
`pool__ta` runs and **both runs and the table were removed 2026-08-09** — those two
are `lookback=1` by the nature of their data (§11a, §12c), which makes them a
different setup rather than a cheaper one. They are recoverable from commit
`5813342`.

⚠️ **The bank table holds the WHOLE bank universe, verified not assumed**: 53,921
rows, 20 tickers, `industry_code = '401010'` and nothing else, and a LEFT JOIN from
`pool__basic` finds **0 rows missing**. The inner join to `pool__targets` drops
nothing because both pools are on one calendar.

Each is `(date, exchange, ticker)` + the target + the union of the group's outstanding
channels. **Primary key `(date, exchange, ticker)`, `date` leading** — same contract as
every `pool__*`, and only a leading `date` lets the index serve a range scan.

## 2. The grouping rule — one table per (schema, target, setup)

**Same schema in, same schema out.** `unified_schema_vcb`'s runs produce a table in
`unified_schema_vcb`; nothing crosses schemas, because a VCB feature and a BANK
feature are not the same column even when they share a name.

**Same target AND same setup → one table.** Runs sharing all three describe the same
experiment on different feature blocks, so their channels belong together. `SETUP_KEYS`
is `lookback_d, horizon_h, normalize, feature_normalize, corr_threshold, n_splits,
min_train, random_state, selector_class`, plus `CUT_KEYS` = `cut_fdr_q,
cut_corr_threshold`.

⚠️ **Most of the setup is read from `metadata.json`; the CUT keys are read from
`outstanding.csv`.** The shortlist used to carry only `lookback_d` and `horizon_h` —
enough to read a row, not enough to decide two runs are the same experiment, and
grouping on what it happened to carry would silently merge runs differing in
`normalize` or `random_state` (`feature_selection/CONTEXT.md` §8 lists what that
costs). But `metadata.json` describes the SELECTOR RUN, and the shortlist is now
rebuilt afterwards by `selection_cut` — so the two parameters that determine the cut
have to come from the file the cut wrote. See §5a.

⚠️ **A `None` setup value is a real value** (`feature_normalize` is `None` on every
non-cross-sectional run) and `groupby` drops `None` keys, so it is carried as the
`NOT_SET` sentinel. ASCII, because this prints to a cp1252 console on Windows.

## 3. The name — `<target>__final__d<lookback>_h<horizon>`, minus any `cs_`

⚠️ **A `cs_` target prefix is DROPPED from the table name**, so `cs_rank_5day`
becomes `rank_5day__final__d20_h5`. The `cs_` marks a CROSS-SECTIONAL target and a
cross-section is a set of tickers — which is exactly what the SCHEMA already says.
`unified_schema_bank.cs_rank_5day__final__*` states "across a cross-section" twice
and names neither one; `unified_schema_bank.rank_5day__final__*` reads as "the 5-day
rank, across the banks", which is the fact. It cannot collide with a time-series
table: the stems differ (`rank_5day` vs `return_5day`).

⚠️ **The setup is IN the name, not only in the grouping, and it was forced.** Before
the `d=1` runs were dropped, two groups shared a schema AND a target and differed only
in `lookback_d`; a bare `return_5day__final` would have had to hold both or silently
lose one. Only one setup survives today — **keep the discriminator anyway**, because
the second setup is one run away and a rename is worse than a long name. If two groups
ever collide on a name, `plan_from_reports` **raises** rather than picking a winner.

⚠️ **Identifiers are validated, not trusted.** Schema, table and column names are
interpolated into SQL — an identifier cannot be a bound parameter — so every one is
matched against `IDENTIFIER` first, and against PostgreSQL's 63-byte limit, because
past it PostgreSQL truncates **silently** and two truncated names collide into one
table.

## 4. ⚠️ CREATE TABLE AS, never a pandas round-trip

psycopg2 returns `numeric` as `Decimal`, a DataFrame carries that as dtype `object`,
and a writer maps `object` to VARCHAR — **a read-then-write would silently turn every
price column into TEXT.** `orchestration/assets/unified.py` documents the same trap
for `pool__basic`, and `feature_selection/unified_reader.py` documents the other
direction. So the join runs **server-side** and the source types are inherited:
verified on the rebuilt 754-column table as 723 `real`, 15 `numeric`, 12 `bigint`,
1 `double precision`, 1 `date` and **2 `character varying` — `exchange` and `ticker`,
which are the only text columns there should be.** (The 18-column bank table: 9
`numeric`, 5 `bigint`, 1 `double precision`, 1 `date`, 2 `character varying`.)

⚠️ **The join is INNER on all three keys**, matching the panel the selection actually
ran on — every `metadata.json`'s `join_log` records the same keys and `how="inner"`.
A LEFT join would invent rows the ranking never saw. The build asserts the column
count equals `features + 3 keys + target` and that the result is non-empty; an empty
result means the pools do not share a calendar.

## 5. ⚠️ `cs_rank_5day` is NOT stored, and that is not an omission

`cs_rank_{h}day` is `(rank − 1)/(n − 1) − 0.5` computed **within each date across a
chosen universe** (`cross_sectional.cross_sectional_rank`). Its value for a stock-date
depends on which other names are in the panel and on `min_width` — **properties of the
RUN, not of the row.** Freezing it into a table would bake one universe into data that
outlives it.

So that group stores **`return_5day`**, the quantity the rank is computed from, and the
reader re-ranks. The plan still records `target = cs_rank_5day`, and the table's
`COMMENT` says all of this.

⚠️ **The note is keyed on `target_derived`, not on `stored_target is None`** — a
derived target still stores its base column, so the second test is False exactly when
the warning is most needed. That bug shipped in the first build and is why the flag
exists.

## 5a. ⚠️ The shortlist fingerprint — how a stale table is detected

Every table's `COMMENT` carries `Shortlist fingerprint: <digest> over <n> channels` —
a sha256 over the sorted `(source_table, channel)` SET it was built from.
`pipeline.status_final_features` recomputes it from the current reports and compares.

**This exists because "the table exists" was the only check anything made**, and under
it the VCB table drifted 26 columns away from its own shortlists unnoticed (issue
STL-1). A *parameter* cannot do this job: the cut is measured per run, so the same
knobs on a re-run archive can legitimately produce a different set. Only the set is
the fact.

⚠️ **`max_features` was REMOVED from `SETUP_KEYS` (2026-08-09).** It is still `12` in
every `metadata.json` and has not determined a shortlist since `selection_cut`
replaced the fixed cap with a measured one — the same runs now keep 10 to 236
channels. Grouping and naming on it recorded a number that was no longer true of
anything. The parameters that DO determine the cut (`cut_fdr_q`,
`cut_corr_threshold`) are stamped into `outstanding.csv` by
`feature_selection.outstanding` and read from there.

## 6. ⚠️ Provenance travels with the table, and it is not flattering

Every table carries a `COMMENT ON TABLE` naming the source runs, the setup and the
**`evidence`** of each run. On the largest table that reads:

> Run evidence: `no_null=19`. ⚠️ evidence=no_null means no bar was computed for that
> run — a ranking without a null is descriptive, not evidence.

⚠️ **All 750 features in the VCB table come from runs that computed NO NULL**, and
the bank table's one run **failed** its own — so **not one surviving run in the archive
clears anything** (`feature_selection/CONTEXT.md` §14b) — 19 with no bar, 1 that failed its
own. A row in one of these tables is a channel some run ranked highly. **That is all
it is.** §10 of the feature-selection context records an absent null as absent and
never as a pass; this propagates the same rule into the database rather than letting a
table name imply a verdict.

⚠️ **And the selections do not agree — but read that carefully.** 725 of the 750
channels were chosen by exactly one run, and only 25 by more than one. That looks like
instability and is mostly ARITHMETIC: each run is `pool__basic + one
pool__economy_<country>`, so 18 of the 19 pools appear in a single run and **a macro
channel can never be a candidate more than once**. Measured: `pool__basic`'s 27
channels appear in up to 19 runs; all 723 economy channels appear in exactly 1.

⚠️ So the table is a **UNION of 19 disjoint candidate sets**, not a consensus and not
a ranking — and a consensus threshold cannot fix it. Requiring `≥2 runs` collapses 750
to 25, all of them `pool__basic`, discarding every macro channel by construction
rather than by evidence. The width is a function of how the pools were sharded: add a
20th economy pool and the union grows again. The coherent alternative is ONE selection
run over the joined pool, where the channels actually compete — expensive, and not
what the archive contains.

## 6a. Where this sits in the chain

```
feature_selection  →  THIS  →  train_test_creator  →  model.lstm  →  result_evaluator
```

`python -m pipeline` prints the state of all five and runs the stale ones
(`src/pipeline/CONTEXT.md`). Two things downstream depend on decisions made here:

- ⚠️ **The `d` and `h` in the table NAME are the only source of the window length and
  horizon downstream.** `train_test_creator.parse_final_table` reads them off it, so
  §3's "keep the discriminator anyway" is load-bearing, not tidiness.
- ⚠️ **§6's provenance `COMMENT` is copied verbatim** into the dataset's
  `metadata.json` and from there into every run's `lineage`. The `evidence=no_null`
  sentence therefore reaches the model run that was trained on these channels.

## 7. Re-running

`--apply` refuses to overwrite: an existing table raises unless `--replace` is passed,
which **drops it first**. The plan is printed before anything is written, and
`python -m final_features` with no flags writes nothing at all — so the safe move is
always to look at the plan first.

After adding a run: `python -m feature_selection.outstanding` to refresh the
shortlists, then `python -m final_features` to see how the grouping changed.

⚠️ **Refreshing the shortlists makes every existing table STALE, and now says so.**
`python -m pipeline` compares each table's stored fingerprint (§5a) against what the
current reports would build and reports `STALE — table <a> vs shortlists <b>`. That is
not automatic: rebuilding drops the table, changes every dataset hash below it and
orphans the runs that referenced them. It is a decision, and the check exists so it is
a decision rather than a surprise.
