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

| table | runs | pools | features | rows | cols |
|---|---|---|---|---|---|
| `unified_schema_vcb.return_5day__final__d20_h5` | 19 | 19 | 203 | 4,235 | 207 |
| `unified_schema_bank.rank_5day__final__d20_h5` | 1 | 1 | 10 | 53,921 | 14 |

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
is the full list — `lookback_d, horizon_h, normalize, feature_normalize, max_features,
corr_threshold, n_splits, min_train, random_state, selector_class`.

⚠️ **The setup is read from `metadata.json`, NOT from `outstanding.csv`.** The
shortlist carries only `lookback_d` and `horizon_h` — enough to read a row, not enough
to decide two runs are the same experiment. Grouping on what it happens to carry would
silently merge runs differing in `normalize` or `random_state`, and
`feature_selection/CONTEXT.md` §8 is a list of what that costs.

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
verified on the 207-column table as 189 `real`, 7 `bigint`, 7 `numeric`, 1
`double precision`, 1 `date` and **2 `character varying` — `exchange` and `ticker`,
which are the only text columns there should be.**

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

## 6. ⚠️ Provenance travels with the table, and it is not flattering

Every table carries a `COMMENT ON TABLE` naming the source runs, the setup and the
**`evidence`** of each run. On the largest table that reads:

> Run evidence: `no_null=19`. ⚠️ evidence=no_null means no bar was computed for that
> run — a ranking without a null is descriptive, not evidence.

⚠️ **All 203 features in the VCB table come from runs that computed NO NULL**, and
since the `pool__ta` run was removed **not one surviving run in the archive clears
anything** (`feature_selection/CONTEXT.md` §14b) — 19 with no bar, 1 that failed its
own. A row in one of these tables is a channel some run ranked highly. **That is all
it is.** §10 of the feature-selection context records an absent null as absent and
never as a pass; this propagates the same rule into the database rather than letting a
table name imply a verdict.

⚠️ **And the selections do not agree with each other.** 199 of 203 channels were
chosen by exactly one run, while all 19 runs saw the same 27 `pool__basic` channels
(§14a). The 203-column table is therefore a UNION of 19 unstable shortlists, not a
consensus.

## 7. Re-running

`--apply` refuses to overwrite: an existing table raises unless `--replace` is passed,
which **drops it first**. The plan is printed before anything is written, and
`python -m final_features` with no flags writes nothing at all — so the safe move is
always to look at the plan first.

After adding a run: `python -m feature_selection.outstanding` to refresh the
shortlists, then `python -m final_features` to see how the grouping changed.
