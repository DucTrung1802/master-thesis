# Context — `src/final_features`

> 🗺️ **Project hub: [CLAUDE.md](../../CLAUDE.md)** — the whole project in ~5k tokens
> (verdict, chain, standing rules, routing). Read that first; this file is the depth
> behind one stage.

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
python -m final_features --apply --scope basic   # root: reports/feature_selection
python -m final_features --apply --shape shortlist   # the layer-2 INPUT pool — §8
```

## 0. ⚠️ THE INTERFACE — the only two files this module may open

> **This section is normative and it is MIRRORED in
> [`feature_selection/CONTEXT.md` §0](../feature_selection/CONTEXT.md).** That file is
> the PRODUCER's half: what a selection promises to write. This is the CONSUMER's
> half: what this module promises to read, and nothing beyond it. Neither may be
> changed alone — the definition they both import is
> [`feature_selection/contract.py`](../feature_selection/contract.py), and a column
> added on one side and not the other is an **import error**, not a discovery three
> stages later.

`plan_from_reports` walks every folder under `--root` that holds a `metadata.json`, and
opens **two files in each**:

```
reports/feature_selection/<run_id>/
  ⭐ metadata.json     → setup[SETUP_KEYS]        the grouping key
  ⭐ outstanding.csv   → REQUIRED_SHORTLIST_COLUMNS + CUT_KEYS
     everything else — feature_importance.csv, channel_correlation.csv, validation.csv,
     coverage.csv, stability.csv, figures/ — IS NOT READ HERE AND MUST NOT BE
```

⚠️ **Reaching into the rest would be re-deciding the cut.** How many channels a run
supports is measured *per run* by `feature_selection.selection_cut`, from a
shuffled-methods null and a per-method knee, with the parameters it used stamped into
the shortlist. This module's job starts after that decision. A second opinion formed
here from `feature_importance.csv` would be a second cut with no null behind it.

### The shortlist — what is read out of `outstanding.csv`

`REQUIRED_SHORTLIST_COLUMNS` is **10 of the 25 columns the producer writes** (verified
2026-08-16), and being a subset is deliberate: the other 15 are diagnostics
(`coverage`, `kept_by`, `beat_in_tie`, `consensus_p`, `tie_group_size`,
`source_table_from`, …) that must stay free to change without breaking the handoff.

| column | what this module does with it |
|---|---|
| `channel` | one column of the generated `SELECT` |
| `source_table` | the `FROM` it is read out of — §4's server-side join |
| `schema` | the schema in AND out; **nothing crosses schemas** (§2) |
| `target` | the grouping key and the table NAME (§3) |
| `lookback_d`, `horizon_h` | the `__d<d>_h<h>` in that name — ⚠️ the ONLY source of the window and horizon downstream (§6a) |
| `run_id`, `evidence` | the provenance `COMMENT ON TABLE` (§6) |
| `cut_fdr_q`, `cut_corr_threshold` | `CUT_KEYS`, part of the grouping setup — ⚠️ they are **in no other file** (§5a) |

⚠️ **`source_table == 'unknown'` RAISES, and a guess would be worse than the raise.**
These names are interpolated into SQL, so a wrong table that *exists* produces SQL that
runs and reads a channel out of the wrong pool — silent bad data, discovered nowhere.
Every `channel`, `source_table` and `schema` is matched against `contract.IDENTIFIER`
and the 63-byte limit first, because past 63 bytes PostgreSQL truncates **silently**
and two truncated names collide into one table.

### The setup — what is read out of `metadata.json`

`setup[SETUP_KEYS]` — all nine of `lookback_d, horizon_h, normalize,
feature_normalize, corr_threshold, n_splits, min_train, random_state,
selector_class`. A run missing any of them **raises**: it predates those keys and
cannot be grouped with runs that have them.

⚠️ **The setup comes from `metadata.json`, the CUT keys from `outstanding.csv`, and
that split is not an accident** (§5a). The shortlist carries only `lookback_d` and
`horizon_h` — enough to read a row, not enough to decide two runs are the same
experiment; grouping on what it happens to carry would silently merge runs differing in
`normalize` or `random_state`. But `metadata.json` describes the SELECTOR run, and the
shortlist is rebuilt after it by `selection_cut` — so the two parameters that determine
the cut have to come from the file the cut wrote. `max_features` is in neither list, on
purpose (§5a).

### A run folder with no `outstanding.csv` RAISES

⚠️ **It used to be skipped, and that is the failure this interface exists to prevent.**
Measured 2026-08-15: the two newest runs — both merged back from `kaggle_gpu`, which at
the time wrote the report folder and printed a reminder instead of the shortlist —
carried none. `plan_from_reports` planned **19 runs, reported no error, and was wrong
about which experiment it described.** `contract.missing_shortlists` now finds them and
`_read_outstanding` raises, naming the folders and the command that fixes them. Same
rule as CLAUDE.md §5 rule 12: silence is never how something gets left out.

The repair is always the same one command, and it is safe to re-run:

```powershell
python -m feature_selection.outstanding      # rebuild every shortlist under the root
python -m final_features                     # then look at the plan
```

## 0a. ⚠️ `--root` and `--scope` — the grouping key has no term for "which pools"

Added 2026-08-10. §2's rule is one table per `(schema, target, setup)`, and **none of
those three says which feature blocks a run ranked**. That is correct for the archive,
where 19 runs sharding one candidate set are deliberately UNIONED (§6). It is wrong the
moment a narrower pool is the experiment: a `pool__basic`-only run at `d=20, h=5` lands
in the same group as those 19 and wants the same table name, so building it means
`--replace`, which drops the 750-channel table, changes every dataset hash below it and
orphans the runs that referenced them (§7).

- **`--root`** decides which runs exist as far as `plan_from_reports` is concerned. A
  scoped run archived under its own root cannot join the archive's group. ⚠️ **It is no
  longer separating anything by default (2026-08-10)**: the `_basic`, `_economy` and
  `_superseded` roots were merged into `reports/feature_selection/` and all 22 archived
  runs deleted, so every run lands in one root at one seed and `--scope` is the only
  thing left keeping two experiments off one table name.
- **`--scope`** appends `__<scope>` to the table name — `return_5day__final__d20_h5__basic`.
  `train_test_creator.FINAL_TABLE` parses it and ignores it: `d` and `h` still come from
  the same place, and the scope names the feature BLOCK, not the setup.

⚠️ **`--scope` is deliberately NOT a `SETUP_KEY`.** It is chosen per build, beside the
`--root` that already decided the group. Putting it in the grouping key would encode
"which pools" into a fingerprint that is over `(source_table, channel)` by design — and
the fingerprint would then stop being a statement about the channel SET.

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
reader re-ranks — ⚠️ **which nothing did until `RNK-1` was fixed on 2026-08-18**; it is
`train_test_creator._label` that does it now, and §5b records what else had to travel
with the label. The plan still records `target = cs_rank_5day`, and the table's
`COMMENT` says all of this.

⚠️ **The note is keyed on `target_derived`, not on `stored_target is None`** — a
derived target still stores its base column, so the second test is False exactly when
the warning is most needed. That bug shipped in the first build and is why the flag
exists.

## 5b. ⚠️ THE UNIVERSE TRAVELS NOW — `UNI-1`, fixed 2026-08-18

§5 says a rank depends on which other names are in the panel. That is the argument for
not storing it — and it is equally an argument that the **names themselves** are part of
the label. They were not carried: this module built from the pools of
`unified_schema_<ticker>`, which on `ALL` is **781 names**, so a table built from a
150-name shortlist would have been ranked over 781 by whoever read it.

Three changes, and the second is the one that matters:

1. `_read_outstanding` reads `metadata.json → input.universe` (recorded by
   `feature_selection.report` since 2026-08-18) onto every shortlist row.
2. **It is a GROUP KEY.** Two runs at the same target and the same knobs over different
   populations are two experiments; unioning them would produce a table whose label is a
   rank across a population **neither run used**. They now collide on the table name and
   `plan_from_reports` **raises** — pass `--scope`, exactly as for any other collision.
3. `build_sql` emits `WHERE base.ticker IN (…)`, and `comment()` records the first eight
   names plus a sha1 of the sorted set, so a later reader can ask *"is this the same
   population?"* without diffing prose.

⚠️ **Appended to the group keys here rather than added to `contract.SETUP_KEYS`.** A new
SETUP_KEY raises on every archived run that predates it (`MTH-1`), and a universe is a
fact about a CROSS-SECTIONAL run only — this keeps two universes apart without
invalidating 30 single-ticker runs that never had one.

⚠️ **NOT in the fingerprint.** Adding it would report every existing table STALE at once.
The collision guard above is what stops the wrong population being built; the fingerprint
stays a statement about the CHANNEL SET, which is what it has always meant.

⚠️ An empty universe means *"every name in the source pools"* — right for a single-ticker
chain, and right for `unified_schema_bank`, which IS its universe.

## 5a. ⚠️ The shortlist fingerprint — how a stale table is detected

Every table's `COMMENT` carries `Shortlist fingerprint: <digest> over <n> channels` —
a sha256 over the sorted `(source_table, channel)` SET it was built from.
`pipeline.status_final_features` recomputes it from the current reports and compares.

**This exists because "the table exists" was the only check anything made**, and under
it the VCB table drifted 26 columns away from its own shortlists unnoticed (issue
STL-1). A *parameter* cannot do this job: the cut is measured per run, so the same
knobs on a re-run archive can legitimately produce a different set. Only the set is
the fact.

⚠️ **`max_features` was REMOVED from `SETUP_KEYS` (2026-08-09) and from the last live
default on 2026-08-10.** It was `12` in the 20 pre-2026-08-10 `metadata.json` files and
`null` in the two uncapped ones, and in NEITHER case did it determine the
shortlist — `selection_cut` replaced the fixed cap with a measured one and those same
runs kept 10 to 236 channels. (All 22 were deleted with the root merge later that day;
the rule stands for the runs that replace them.) Grouping and naming on it recorded a number that was no
longer true of anything. The parameters that DO determine the cut (`cut_fdr_q`,
`cut_corr_threshold`) are stamped into `outstanding.csv` by
`feature_selection.outstanding` and read from there.

## 6. ⚠️ Provenance travels with the table, and it is not flattering

Every table carries a `COMMENT ON TABLE` naming the source runs, the setup and the
**`evidence`** of each run. On the largest table that reads:

> Run evidence: `failed_null=1, no_null=18`. ⚠️ evidence=no_null means no bar was
> computed for that run — a ranking without a null is descriptive, not evidence.

⚠️ **18 of the VCB table's 19 source runs computed NO NULL, and the 19th FAILED its
own** (`pool__basic`, z = +1.46, measured 2026-08-09 — `feature_selection/CONTEXT.md`
§10b). The bank table's single run also failed. So **not one surviving run in the
archive clears anything** — 18 with no bar, 2 that failed their own. A row in one of
these tables is a channel some run ranked highly. **That is all it is.** §10 of the
feature-selection context records an absent null as absent and never as a pass; this
propagates the same rule into the database rather than letting a table name imply a
verdict.

⚠️ **That tally changed WITHOUT the table changing, and no check would have caught
it.** The fingerprint is over `(source_table, channel)` — adding a null to a run moves
its `evidence` and not one channel, so `status_final_features` still read `current`
while the stored sentence said `no_null=19`. The `COMMENT` was refreshed in place with
`COMMENT ON TABLE`, using `plan.comment()` so the text cannot drift from what a
rebuild would write. **Rebuilding to correct a sentence would have been wrong** — it
drops the table, changes every dataset hash below it and orphans the runs that
referenced them (§7).

⚠️ **A regenerated comment is NOT safe to write blindly.** `plan_from_reports()`
leaves `target_derived=False` — only `build_all` can see the database and set it — so
regenerating the BANK table's comment silently **drops the `cs_rank_5day` is-not-stored
warning** of §5. The refresh therefore writes only when the evidence tally is the sole
difference, and refuses otherwise.

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

## 8. ⚠️ TWO SHAPES, because there are TWO SELECTION LAYERS (2026-08-16)

> ⚠️ **This section was cited by CLAUDE.md §3b/§3c from 2026-08-13 and did not exist.**
> The machinery it described was written, RUN — `unified_schema_vcb.pool__shortlist__close_adjust_5day__d20_h5`
> (4,266 × 892) is still in the database, built from 20 runs, and its `COMMENT` names
> them — and then **never committed**. `git log --all -S"Pre-final shortlist"` finds no
> commit; the module on disk had no `--shape` flag. Rewritten 2026-08-16 from that
> table's own `COMMENT` and from §3c. ⚠️ **A documented feature is not a shipped one:
> the check that would have caught this is `grep`, and it costs seconds.**

```
shape=shortlist   pool__shortlist__<target>__d<d>_h<h>   keys + channels, NO label
shape=final       <target>__final__d<d>_h<h>             + the target column
```

**Why a second layer at all — §6 is the argument.** The layer-1 union is not a
consensus and cannot be made into one: each run ranks `pool__basic + one` other pool,
so a macro channel is offered to exactly one run and agreement was never available to
measure. Requiring `≥2 runs` collapses 750 channels to 25, all of them `pool__basic`,
**by construction rather than by evidence**. §6 named the coherent alternative — "ONE
selection run over the joined pool" — and called it expensive. The shortlist pool is
the cheap version of it: one run over the few hundred **survivors** rather than over
every candidate.

```
layer 1   N runs over pool__basic + <each pool>   →  shortlist pool   (this module)
layer 2   1 run  over the shortlist pool          →  final table      (this module)
```

### The rules, and what each one is preventing

- ⚠️ **The `pool__` prefix is load-bearing.** `feature_selection.run --pools`,
  `UnifiedSchemaReader.pools()`, the pipeline's calendar check and the run-folder scope
  all key on it — so the pool needed **no new code anywhere** to be selectable over. It
  simply *is* a pool as far as every consumer is concerned.
- ⚠️ **The target is IN the name because this pool is TARGET-CONDITIONED.** Its
  channels were kept *using* that label at that window. A raw `pool__basic` may be
  selected over for anything; this one may not, and doing so is leakage. The `COMMENT`
  says so in the same words.
- ⚠️ **It stores NO label**, exactly like every raw `pool__*` — `pool__targets` is
  joined by whoever reads it. Storing one would make this the single pool that could
  hand a selector its own answer through `--pools`.
- ⚠️ **Never name a shape with a `__final__` SEGMENT.** `train_test_creator.FINAL_TABLE`
  permits underscores in the target group, so `…__pre__final__d20_h5` PARSES, yielding
  `target='…__pre'` — a column that exists nowhere, discovered stages later.
  `__prefinal__` was rejected for this, and `SHAPES` is a closed set so the mistake is
  not reachable from the CLI.
- ⚠️ **A run is layer 2 iff its `outstanding.csv` says `source_table=pool__shortlist__*`**
  — a fact about what it RANKED, not a flag. A flag would let a layer-1 run claim
  layer 2 and the claim would travel into the `COMMENT` unchallenged.

### Which layer each shape reads, and why it is not symmetric

| shape | reads | because |
|---|---|---|
| `shortlist` | layer-1 runs **only** | feeding layer 2's output back into its own input is circular — the pool would be conditioned on a selection made over itself. Raises if every run is layer 2 |
| `final` | layer-2 runs **whenever any exists**, else every run | the competing run is strictly better evidence than the union that fed it |

⚠️ **`final` does not UNION the layers.** A layer-1 shortlist and a layer-2 one are not
two shards of one candidate set — the second *is* the first, re-ranked with the channels
competing. Unioning them would put back exactly the channels the competing run
**rejected**, and the table would be the union again wearing layer 2's name. The switch
shows up in the printed plan, in the `COMMENT` (`Selection layer N:`) and — because the
channel sets differ — in the **fingerprint**, so a stale table reports STALE rather than
being accepted.

⚠️ **`--apply --shape shortlist` cannot be followed straight by `--shape final`.** The
layer-2 run has to happen in between and it is manual:

```powershell
python -m final_features --apply --shape shortlist
python -m feature_selection.run --pools pool__shortlist__<target>__d<d>_h<h> `
       --target <target> --lookback 20 --horizon 5 --null-draws 10
python -m final_features --apply --shape final --replace
```

`python -m pipeline` reports both as stages (`shortlist_pool`, `selection_2`), and
`selection_2` prints `MANUAL — cannot be produced here` rather than doing something
cheaper that looks the same.
