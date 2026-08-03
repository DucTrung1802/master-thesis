# Context — `src/orchestration` (Dagster migration of `src/main.py`)

> # ▶️ THIS IS THE ENTRY POINT. Work happens here.
>
> **📁 Moved 2026-08-01: this package lives under `src/`, not at the repo root.** It is a
> module of the codebase like every other, so it sits beside the ones it wraps. The only
> thing that changes for a user is the path in every command — **`-f
> src/orchestration/definitions.py`** — and `workspace.yaml`, which was updated with it.
> One thing changes for a reader, and it is in §5 *Gotchas*: `definitions.py` now repeats
> `_bootstrap`'s two-line `sys.path` insert INLINE, because the package can no longer be
> imported until `src/` is on the path.
>
> Handoff notes. **Status (2026-08-03): the LANDING layer and the whole BRONZE layer are
> assets and have both been materialised green; silver has 8, gold 7, and there is now a
> fifth layer — `unified` (1).** 55 assets. What is left is the rest of silver (~7
> leaves) and the rest of the unified schema — see §4. Verify anything before acting on
> it: the code is the source of truth.
>
> **[`src/data_preprocessor`](../data_preprocessor/CONTEXT.md) is ARCHIVED as of
> 2026-08-01** — as a *way to run things*. It is still the implementation library every
> asset wraps (`resources.py:23` imports `DataPreprocessor`, and all the transform logic
> lives there), so the directory must not be moved or deleted; what is retired is
> `main.py` + the three `ingest_*_data()` entry points + `switch_config.json` as a run
> plan. **Selection is the run plan now.** Read that file for how a table is BUILT; add
> new pipeline steps as assets HERE.
>
> ⚠️ `src/main.py` still exists and still works — nothing was ripped out from under it.
> But it is no longer maintained as a run path: two of its gold leaves (`stocks`,
> `indices`) already fail or are gone (§"Gold housekeeping"), and nothing has been done
> about it, because the assets are what run.

## 1. Why this is worth doing

`main.py` is already a DAG, written as `if switch: call()`. The evidence is
[data_preprocessor.py:3495-3525](../data_preprocessor/data_preprocessor.py#L3495-L3525),
a hand-written list of `(leaf_name, callable)` pairs iterated against the switch
config, and [cafef_scraper.py:516-534](../web_scraper/cafef_scraper.py#L516-L534),
the same shape for the scrapers. The dependency edges (TV links → CafeF/Simplize
universe; `raw_data/<folder>` → one bronze table; bronze → silver → gold) exist only
as source-code ORDER plus comments.

What Dagster adds that is not already there:

| today | after |
|---|---|
| run plan = hand-edit two lines of a 382-key JSON | asset selection, from the UI or `--select` |
| a failed ingest logs one line and **returns normally** | the asset fails and the run is red |
| "what ran" = read `logs/app.log` | per-asset run history with timestamps |
| row counts maintained by hand in CONTEXT.md tables | materialisation metadata, recorded per run |
| ticker scoping = edit `CAFEF_FINANCIALS_TICKERS` in constants.py | a partition per ticker, re-runnable one at a time |

**Scope is smaller than the switch count suggests.** Of 382 keys, **320 are
TradingView** `(asset_class, country, type, sector)` combinations — those are
PARTITIONS of two assets, not 320 assets. The remaining 61 map to ~55-65 assets.

## 2. What exists — LANDING + BRONZE complete, silver started (2026-08-01)

**55 assets: 19 landing + 20 bronze + 8 silver + 7 gold + 1 unified.** Every scraper in
`main.py` lands to `raw_data/` as an asset ([assets/scrape.py](assets/scrape.py)); every
bronze ingest leaf is an asset ([assets/bronze.py](assets/bronze.py), 20 leaves → 25
tables); silver has eight ([assets/silver.py](assets/silver.py)), gold seven
([assets/gold.py](assets/gold.py)) and the per-ticker unified schema one
([assets/unified.py](assets/unified.py)).
They are separate modules on purpose: the
landing layer is correct-on-disk and re-runnable with no database at all.

```
trading_view_links[9 asset classes]
   ├─(same partition)─► trading_view_data[9 asset classes]
   ├─► trading_view_collected_links        ⚠️ NOTHING READS THIS
   └─(the `stocks` partition ONLY = the ticker UNIVERSE)
           ├─► cafef_{price,order_stats,foreign,prop_trading,insider_txn}
           ├─► cafef_news
           └─► simplize_{stocks,industry}

cafef_financials_templates ─► cafef_financials[2 tickers]     (no TradingView dep)
cafef_pdfs[100 tickers]                                        (no TradingView dep)
cafef_index_{price,order_stats,foreign,prop_trading}           (no TradingView dep)
gics_structure                                                 (no TradingView dep)

── and every landing asset feeds the bronze table whose folder it writes ──

raw/trading_view_data[economy] ─► bronze/trading_view_economy ─┬─► silver/economy ──────┐
                                                               │                        ├─► gold/economy
                                                               └─► silver/economy_series┘   (wide, as-of)

raw/cafef_index_{price,order_stats,foreign,prop_trading}
        └─► bronze/cafef_index_* (x4) ─► silver/stock_market ─► gold/stock_market
                                         (4 tabs joined)        (wide, unfilled)

raw/cafef_financials[t] ─► bronze/cafef_financials ─► silver/cafef_financials_bank
                                                              │  (quarterly, 180 cols)
   silver/stocks_basic ──────────────────────────────────┐    │
                                                         ▼    ▼
                              silver/stocks_basic_financials_bank   (daily, as-of)
                                              └─► …_fa   (+26 indicators)
                                                    └─► gold/stocks_financials_bank_fa
                                                            (+ the TA battery)
```

| group | assets | partitions |
|---|---|---|
| `trading_view` | links, collected_links, data | **9** — one per asset class |
| `cafef` | price, order_stats, foreign, prop_trading, insider_txn, news | — |
| `cafef_index` | price, order_stats, foreign, prop_trading | — |
| `cafef_filings` | pdfs, financials_templates, financials | **100** / **2** — one per ticker |
| `simplize` | stocks, industry | — |
| `gics` | structure | — |
| `bronze` | **all 20 ingest leaves** (25 tables) | — |
| `silver` | `economy` (long fact), `economy_series` (dimension), `stock_market` (4 index tabs), `stocks_basic` (6 sources), `cafef_financials_bank` (quarterly), `stocks_basic_financials_bank` (as-of daily), `…_fa` (+26 indicators) | — |
| `gold` | `economy` (wide, as-of), `stock_market` (wide, unfilled), `stocks` (price panel, no features), `stocks_ta` (+ the ~900-column TA block), `stocks_financials_bank_fa` (feature panel), `news_{weekly,daily}_panel` | — |
| `unified_vcb` | `pool__basic` (one ticker, every column of `silver.stocks_basic`) | — |

### ⚠️ The edges, read out of the code (2026-07-31 correction)

The first version of this graph took "TradingView is the universe authority" from
`web_scraper/CONTEXT.md` §1 and hung CafeF/Simplize/pdfs off
`trading_view_collected_links`. **Three of those edges were wrong.** What each consumer
actually opens:

| consumer | reads | depends on |
|---|---|---|
| `CafeFScraper.get_stock_symbols` | `raw_data/trading_view/links/**stocks**/**/*.csv` | `trading_view_links`, partition **`stocks`** |
| `SimplizeScraper.get_stock_symbols` | the same directory | the same |
| `CafeFNewsScraper.scrape_all_news` | `get_stock_symbols()` | the same |
| `_add_generic_link_data_tasks` | `links/<its own asset class>/…` | `trading_view_links`, **same partition** |
| `CafeFPdfScraper` | `CAFEF_PDF_TICKERS` (repo-root `vn100.csv`) | **nothing** |
| `CafeFIndexScraper` | its own `INDEXES` class constant | **nothing** |

1. **`collected_links` IS NOT THE UNIVERSE.** The only references to it anywhere in the
   repo are the code that writes it and its switch key — **nothing reads it**. It is a
   leaf, not a hub. The universe is `links/stocks/`. The asset is kept because `main.py`
   still runs the aggregation phase and the CSV is on disk.
2. **The universe is ONE PARTITION, not the whole asset.** CafeF and Simplize glob
   `links/stocks/` and are indifferent to the other eight classes, so the dep uses
   `SpecificPartitionsPartitionMapping(["stocks"])`. The default for an unpartitioned
   asset depending on a partitioned one is ALL partitions, which would claim CafeF needs
   forex and bonds links to run.
3. **`trading_view_data` depends on `trading_view_links` per partition**, identity — the
   `forex` data partition needs the `forex` links partition and nothing else. It never
   touches the aggregate.
4. **`cafef_pdfs` has no TradingView dep at all.** Alone among the CafeF scrapers it
   does not call `get_stock_symbols()`; its universe is `vn100.csv`.
5. **`cafef_index_*` and `gics_structure` have no upstream** — a fixed six-entry list and
   a standalone reference download.
6. **`cafef_financials_templates` is a separate, unpartitioned asset upstream of the
   per-ticker parse.** `build_templates_index` REWRITES `templates.csv` from exactly
   the symbols handed to it rather than upserting, so building it inside a partition
   would leave a one-row file naming only the ticker that ran last — which is how VCB
   lost its row when ACB was parsed alone.

### Full landing-layer audit (every asset traced to the file it opens)

| asset | opens | upstream |
|---|---|---|
| `trading_view_links[c]` | switch leaves only | — |
| `trading_view_data[c]` | `links/<c>/…` (`_add_generic_link_data_tasks`) | `trading_view_links`, **same partition** |
| `trading_view_collected_links` | `os.walk(links/)` — **all** classes | `trading_view_links`, all partitions |
| `cafef_{price,order_stats,foreign,prop_trading,insider_txn}` | `get_stock_symbols()` → `links/stocks/**` | `trading_view_links[stocks]` |
| `cafef_news` | `get_stock_symbols()` | `trading_view_links[stocks]` |
| `simplize_{stocks,industry}` | `get_stock_symbols()` | `trading_view_links[stocks]` |
| `cafef_index_*` | `self.INDEXES` (class constant) | — |
| `cafef_pdfs[t]` | `CAFEF_PDF_TICKERS` ← `vn100.csv` | — |
| `cafef_financials_templates` | network fingerprint **+ `simplize/industry.csv`** | `simplize_industry` |
| `cafef_financials[t]` | `templates.csv`, `pdfs/files/<t>/`, **`financials/schema/`** | `cafef_financials_templates` + 2 runtime checks |
| `gics_structure` | MSCI xlsx over HTTP | — |

Two further gaps this audit turned up, beyond the three wrong edges above:

7. **`cafef_financials_templates` reads `raw_data/simplize/industry.csv`.** The
   load-bearing part (the template) is fingerprinted over the network, but the
   `sector` / `industry_group` columns come from Simplize behind an
   `if os.path.exists(...)`. Without it the file is still written with those columns
   **blank** — losing precisely the GICS-vs-fingerprint disagreement `templates.csv`
   exists to expose. Now a declared dep.
8. **⚠️ THE 12 CHART-OF-ACCOUNTS CSVs HAVE NO PRODUCER IN THE PIPELINE.**
   `FinancialsBuilder.schema_of` read
   `raw_data/cafef/financials/schema/<template>_<report>.csv` behind an
   `if os.path.exists(...)` and returned an **empty list** when absent — so a missing
   schema file did not raise, it mapped nothing, and every statement was rejected for
   figures it read correctly. `cafef_schema.save()` can write them, but **nothing in
   `src/` calls it**: they are a git-TRACKED repo input (`raw_data/cafef/financials/`
   is the one exception in `.gitignore`), not something a run regenerates — so it is a
   precondition, not an upstream asset (an asset would invite overwriting a curated,
   versioned file on every materialisation). **Fixed at source — see §3a.**

## 3a. The input-declaration mechanism (`utils.inputs`)

Finding (7) and (8) are the same bug wearing two costumes, and the costume is
`if os.path.exists(path):`. From the outside that guard cannot be told from its
opposite:

* *"optional; without it I lose a named, minor thing"* — fine, but the loss must reach
  the log rather than be inferred months later from blank columns;
* *"required; without it I silently produce nothing"* — a bug dressed as a guard.

The second hides best under expensive work, which is exactly where it was found.

[src/utils/inputs.py](../utils/inputs.py) makes the choice explicit at the read
site: **`require_file` / `require_dir`** raise `MissingSourceDataError` with *what* is
missing, *what breaks*, and *how to fix it*; **`optional_file`** returns a bool and logs
a WARNING that must name the degradation (`degrades=` is not decoration — "sector and
industry_group columns are left blank" is actionable, "some data missing" is not).

Applied:

| site | was | now |
|---|---|---|
| `schema_of` | `exists()` → `[]` → everything rejected after 2.4 h | `require_file` |
| `_from_api` code map | `exists()` → `continue` → that report loses its CafeF fallback | `require_file` |
| `build_templates_index` ← Simplize | `exists()` → columns silently blank | `optional_file` + WARNING |

**And the ordering fix that matters more than any of them:**
`FinancialsBuilder.preflight(exchange, symbol)` validates the template, the three charts
of accounts, the PDF index and the archive — and **`build()` calls it as its first
statement**, so `main.py`, a notebook and Dagster are all covered. The Dagster asset
deliberately does NOT keep its own copy of those checks: a duplicated precondition would
have left the orchestrator as the only caller that failed early, with the silent path
still open everywhere else.

> Both reads are now also documented in `web_scraper/CONTEXT.md` §3a. Neither appeared in
> any CONTEXT.md before this audit — which is the point: they were invisible in the code
> *and* in the prose, and only a "what does this actually open?" pass found them.

**The lesson for the rest of the migration:** derive every edge from what the code
opens, not from the prose. The prose was written for humans reading in order, so
"TradingView must run first" was true while "CafeF reads the aggregate" was never
stated and never checked — and neither the Simplize read nor the schema-file read
appears in any CONTEXT.md at all.

### Why only these two things are partitioned

`cafef_pdfs` and `cafef_financials` are the only steps whose **cost is per ticker and
large** — ~1.0-1.7 GB of download each, ~2.4 h of OCR each. Scoping them today means
editing `CAFEF_PDF_TICKERS` / `CAFEF_FINANCIALS_TICKERS` in constants.py and reading
`app.log` to find out what happened; as partitions, one ticker is one re-runnable unit
with its own success record.

The CafeF/Simplize tabs are deliberately **not** ticker-partitioned: the scrapers
already fan out over ~777 tickers on a 16-thread pool and `skip_existing=True` makes a
re-run cheap, so partitioning would only take that parallelism away.

TradingView is partitioned by asset class (**9**), not by its 320 switch leaves — the
leaves below an asset class (country / stock_type / sector) are *parameters* the
scraper's own task adders read from `switch_config.json`. 320 partitions would
re-encode that JSON in a second place and be unusable in the UI.

### ⚠️ `SwitchConfig.build_unblocked` — the one place switches still matter

`is_enabled` requires EVERY ancestor to be true, and the committed config has
`"web_scraper": false` plus `".../links": false`. A TradingView asset would therefore
enumerate zero countries and scrape nothing, **silently**, whatever Dagster was asked
to do. `build_unblocked` forces the run-plan ancestors true and leaves the parameter
leaves exactly as the JSON has them. Only TradingView and GICS need it — every other
scraper is driven by calling its per-tab method directly, which consults no switch.

⚠️ **It has one edge, found 2026-07-31: forcing an ancestor true can make a LEAF true.**
`crypto` and `options` have no children in `switch_config.json`, so
`web_scraper/trading_view/links/options` is itself a leaf — forcing it hands the adder a
4-part path where it expects 5, and `_add_options_links_tasks` had no guard:
`trading_view_links` partition `options` raised `IndexError` before queueing a task.
Guarded now (crypto always was). Both classes legitimately queue **0 tasks**, so those
two partitions are `landed()`-red on an empty folder — neither has ever produced links.

### ⚠️ Every landing asset verifies what landed

`_common.landed()` counts files/rows in the target folder afterwards and **raises if a
scraper wrote nothing**. The scrapers still swallow their own failures — Phase 0 fixed
the preprocessor, not this layer (`GicsScraper.scrape` does
`if not self._download_structure(...): log_error(...); return`) — so without the check
a failed download is a green asset. `require=False` is used for TradingView's crypto
and options DATA steps, which are documented no-ops.

⚠️ **`landed()` did not catch the 2026-07-31 links breakage, and could not have.** All
140 tasks wrote header-only CSVs, but `landed()` rglobs the whole asset-class folder and
the *previous* dated files are still in it — so `files` and `rows` both looked healthy
and the asset went green. The check answers "is this folder empty?", not "did THIS run
produce anything". The fix belongs in the scraper (it now raises — see
`web_scraper/CONTEXT.md` §3 TradingView); if a per-run check is ever wanted here, it has
to compare against the run's own output file, not the folder.

### The original prototype slice

The first proof was the CafeF market indices: 4 tabs × (scrape + bronze) = 8 assets.
Chosen because it spans both halves of `main.py`, needs no TradingView links, and costs
seconds rather than the 2.4 h of the financials parse. Those 8 are still here, now
split across the two modules.

### Verified

| claim | how it was checked | result |
|---|---|---|
| the flat `src/` layout can be imported by Dagster | `dagster definitions validate` | passes, **55 assets** (19 landing + 20 bronze + 8 silver + 7 gold + 1 unified), all code locations OK |
| partitions resolve | reading the definitions back | TV **9**, pdfs **100**, financials **2** (`HOSE_VCB`, `HOSE_ACB`) |
| the index scrape assets run | `--select "group:cafef_index"` | 4/4 green |
| the TradingView path works incl. `build_unblocked` | `--select "raw/trading_view_collected_links"` | green, rewrote `all_links_2026-06-26.csv` (313 KB) |
| scrape → bronze chains correctly | `--select "+bronze/cafef_index_price"` | both steps green, 3.5 s |
| the numbers are right | asset metadata vs the CONTEXT tables | 24,962 / 22,863 / 20,547 / 1,494 — **exact** |
| re-materialising is safe | `app.log` | `Inserted: 0, Updated: 24962` — pure upsert |
| **an empty landing FAILS the asset** | `landed()` on a missing folder | `MissingSourceDataError` raised; `require=False` tolerates |
| **a DB error FAILS the run** | synthetic asset querying a missing table | `STEP_FAILURE`, `run.success == False` |
| dependencies resolve on this machine | `pip install` into `mt_env` (py3.12.10, Windows) | dagster 1.13.15, 36 packages, **0 conflicts** |

⚠️ **The heavy assets are wired but have NOT been run end-to-end here** (`trading_view_links`
excepted — it has now run for real, see the crash log below):
`trading_view_links` / `trading_view_data` (Selenium, hours), the five CafeF stock tabs
and `cafef_news` (full-universe network), `cafef_pdfs` (GB per partition) and
`cafef_financials` (~2.4 h per partition). They are the same per-tab/per-ticker methods
`main.py` already drives, called the same way — but "validated and wired" is not
"observed to complete".

The only pre-existing versions touched by the install are `grpcio` 1.78→1.83 and
`coloredlogs` 15.0.1→14.0 (only `onnxruntime-gpu` requires it, unpinned — safe).

### ⚠️ What crashed in the first real materialize (2026-07-31)

The landing layer was run for real and `logs/app.log` is the record. **Everything below
was found in one run**, and the two that mattered most were both GREEN at the time —
which is the recurring lesson of this file: a failure that does not raise is not a
failure the orchestrator can see.

| # | symptom in the log | what it really was | status |
|---|---|---|---|
| 1 | 140/140 links tasks: `Total links extracted: 0`, `Saved 0 links`, `Failed Tasks (0)` | TradingView rotated the build hash in `.scrollContainer-<hash>` (`FSX6AatX` → `Q9nrHY0X`). The rows were on the page; the JS did `if (!container) return {symbols: []}` and threw them away | **FIXED** — prefix match + scrollable-ancestor fallback, and it now **raises `PipelineError`** when rows rendered but nothing was read |
| 2 | `IndexError: list index out of range` in `_add_options_links_tasks` | `build_unblocked` + a leaf switch node — see §`build_unblocked` above. **Reachable only through Dagster**, so `main.py` never hit it: partition `options` could never run | **FIXED** — guarded like crypto |
| 3 | 140 header-only CSVs on disk, asset green | `landed()` rglobs the folder, where the PREVIOUS dated files still live — see below | open, by design; the scraper raises instead |
| 4 | 2 × `failed to change window state to 'minimized'` | Chrome transient; the retry wrapper caught both | no action |
| 5 | `Skipping incomplete crypto links path` | correct guard doing its job | no action |

Two things this run did **not** prove and are still open:

* `trading_view_links` partitions **`crypto` and `options` queue 0 tasks** (no children
  in `switch_config.json`) and neither folder has ever existed, so `landed(require=True)`
  makes both **red with nothing wrong**. Either give them `require=False` like the DATA
  steps, or accept two permanently-red partitions.
* A **0-link result is legitimate** for some leaves (`futures/vietnam/*`,
  `bonds/vietnam/corporate`, `economy/*/health` — header-only since June), so "0 rows"
  can never be the failure test on its own. What separates empty from broken is whether
  symbol ROWS RENDERED, which is why the 20-second item wait is load-bearing, not a
  convenience.

Verified after the fixes, one leaf per asset class through the real adders: stocks 20,
funds 21, forex 48, indices 50, economy 32 links; futures and bonds 0 and legitimately
so; crypto and options 0 tasks, no crash.

### Run it

**Every command below runs from the repo root**, in the `mt_env` venv, with
`DAGSTER_HOME` set to an ABSOLUTE path. `.dagster/` is gitignored (run history is
regenerable).

```powershell
# once per shell
.\mt_env\Scripts\Activate.ps1
$env:DAGSTER_HOME = "D:\GIT\master-thesis\.dagster"

# A. the UI — browse the graph, click to materialise, read run history
dagster dev                       # http://localhost:3000 ; Ctrl-C to stop

# B. headless, one asset
dagster asset materialize -f src/orchestration/definitions.py --select "bronze/cafef_index_price"

# C. headless, a cheap known-good slice (4 scrapes, seconds — files already on disk)
dagster asset materialize -f src/orchestration/definitions.py --select "group:cafef_index"

# D. one partition of a per-ticker asset
dagster asset materialize -f src/orchestration/definitions.py `
    --select "raw/cafef_financials" --partition "HOSE_VCB"

# E. the WHOLE bronze layer, 20 assets — ~9 min, 10.6 M rows (raw_data must be populated)
dagster asset materialize -f src/orchestration/definitions.py --select "group:bronze"

# F. a table AND everything upstream of it — scrape, then ingest, in order
dagster asset materialize -f src/orchestration/definitions.py --select "+bronze/trading_view_economy"
```

**Bringing the landing layer up from nothing**, in dependency order — the universe
first, because CafeF and Simplize read it:

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "raw/trading_view_links"        --partition stocks
dagster asset materialize -f src/orchestration/definitions.py --select "raw/trading_view_collected_links"
dagster asset materialize -f src/orchestration/definitions.py --select "group:cafef"        # ⚠️ hours
dagster asset materialize -f src/orchestration/definitions.py --select "group:simplize"
dagster asset materialize -f src/orchestration/definitions.py --select "group:cafef_index"  # independent
dagster asset materialize -f src/orchestration/definitions.py --select "group:gics"         # independent
```

**Then the database layers**, which need only `raw_data/` on disk:

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "group:bronze"   # 20 assets, ~9 min
dagster asset materialize -f src/orchestration/definitions.py --select "group:silver"
dagster asset materialize -f src/orchestration/definitions.py --select "group:gold"     # ⚠️ incl. stocks_ta: hours
dagster asset materialize -f src/orchestration/definitions.py --select "group:unified_vcb"
```

Or in the UI: select the graph and hit Materialize — Dagster walks the edges itself,
which is the point of having them.

Truncate `logs/app.log` first (`Clear-Content logs\app.log`) — the repo `Logger` still
writes there and it stays the record of what the underlying scrapers/ingests did, per
memory `feedback-clean-app-log-before-run`. Dagster's own per-run logs are separate and
live in `.dagster/`.

### Turning things off

There are three levels, and the first is almost always the right one.

**1. Don't select it.** Selection IS the run plan — an asset you never name never runs.
Nothing to configure.

**2. Exclude it from a bigger selection**, with the `and not` syntax (verified working
in 1.13):

```powershell
--select "group:cafef and not key:\"raw/cafef_news\""
--select "* and not group:cafef_filings"
```

**3. Hard-disable it** — set it `false` in
[assets_enabled.json](assets_enabled.json). No Python edit. It then vanishes from the
UI, from `*`, and from every selection. Reserve this for "must never load in this repo".

```jsonc
"// raw/cafef_news": "HEAVY - ~2 h, ~405k rows over the full universe",
"raw/cafef_news": false
```

All 55 keys are listed in the file as a menu, grouped by source, with `//` comment keys
marking the expensive ones (same comment convention as `switch_config.json`).
`true` or **absent** = loaded, so a newly added asset is on by default.

Behaviour, all verified:

| case | result |
|---|---|
| one key `false` | that asset not loaded (45 → 44); `//` comment keys ignored |
| a key matching no asset | **raises**, listing the valid keys |
| malformed JSON | **raises** — never read as "disable everything" |
| file absent | all 55 assets — absent means "no opinion", not "all off" |
| file with a **BOM** | handled (`utf-8-sig`) |

The last three are direct lessons from `switch_config.json`:
`SwitchHandler._load_config` swallows a read error and returns `{}` — every switch
false, `main.py` a complete no-op, one ERROR line in a log nobody reads. The equivalent
slip here would silently disable the whole pipeline, so it raises instead. And
PowerShell 5.1's `Out-File -Encoding utf8` writes a BOM, which is what caused that
no-op in the first place.

- ⚠️ **Disabling does NOT disable downstream.** Verified: with `raw/cafef_index_price`
  disabled, `bronze/cafef_index_price` still resolves and still reads the folder from
  disk — Dagster keeps the edge and shows the removed node as an unexecutable external
  asset. Usually right (the data is on disk either way), but it means disabling is not a
  way to stop a chain: to stop a chain, disable the downstream too.

**Selection syntax** (`--select`, and the same strings work in the UI's search box):

| pattern | means |
|---|---|
| `bronze/cafef_index_price` | that one asset (the `/` is the key prefix, not a path) |
| `group:cafef_index` | every asset in the group |
| `+bronze/cafef_index_price` | it **and everything upstream** — the scrape then the ingest |
| `raw/cafef_index_price_raw+` | it and everything downstream |
| `*` | everything in the code location |

⚠️ **`switch_config.json` is NOT consulted.** Assets call the per-tab / per-table
methods directly, so `--select` is the whole run plan. A leaf set `false` in the JSON
still materialises here, and that is intended (§3).

**Expected output of C**, which doubles as the acceptance test — the four counts match
`data_preprocessor/CONTEXT.md` §8 exactly:

```
index_price:        6 files, 24962 rows   →  bronze.cafef_index_price:        24962 rows
index_order_stats:  6 files, 22863 rows   →  bronze.cafef_index_order_stats:  22863 rows
index_foreign:      6 files, 20547 rows   →  bronze.cafef_index_foreign:      20547 rows
index_prop_trading: 6 files,  1494 rows   →  bronze.cafef_index_prop_trading:  1494 rows
RUN_SUCCESS
```

**Sanity check without running anything:** `dagster definitions validate` (it should
report 55 assets and "All code locations passed validation").

### ✅ Phase 1a — the whole BRONZE layer, 20 assets (2026-08-01)

[assets/bronze.py](assets/bronze.py) now covers **all 20 ingest leaves**, generated from
an `INGESTS` spec table (name, method, tables, upstream). The four `cafef_index_*` keys
are unchanged, so their run history survives.

**Bronze has no cross-table dependency** — each ingest reads its own `raw_data/` folder —
so the layer is flat and every edge points UP at the landing asset that writes that
folder. ⚠️ The six TradingView edges are **per partition**
(`SpecificPartitionsPartitionMapping(["economy"])` etc.): `_ingest_bronze_economy` globs
`data/economy/**` only, and the default all-partitions mapping would claim it needs forex
and crypto to build. `cafef_financials` is ONE asset writing SIX tables (it is one
method) and reports a row count per table.

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "group:bronze"
```

**20/20 green, ~9 minutes**, 10.6 M rows re-ingested. This run doubled as the acceptance
test for the `symbol` → `ticker` refactor in `src/` (see `data_preprocessor/CONTEXT.md`
§4-bronze): **22 of 25 tables reproduced their row count EXACTLY.** The three that moved
were stale bronze catching up with raw data the scrapers had already written —
`cafef_news` 5,599 → 405,320 (3 tickers → the full 777), `cafef_order_stats` 351,373 →
2,523,196, `cafef_prop_trading` 64,139 → 73,810 — and each now equals its raw folder
row-for-row.

### SILVER + GOLD — the economy chain (2026-08-01)

Three assets, and the split between them is the whole point:

```
bronze/trading_view_economy
   ├─► silver/economy          LONG fact, PK (exchange, ticker, date), 579,459 rows, 0 nulls
   └─► silver/economy_series   DIMENSION, PK (exchange, ticker), 1,034 rows + derived frequency
            └──────┬──────────►
   silver/economy ─┴─► gold/economy   WIDE, 1 row per BUSINESS DAY, 1,034 columns
```

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "group:silver,group:gold"
```

| asset | result |
|---|---|
| `silver/economy` | 579,459 rows / 1,034 series — **exactly the bronze row count**, 0 nulls, 17.6 s |
| `silver/economy_series` | 1,034 series: 500 monthly, 226 quarterly, 206 annual, 66 daily, 32 weekly, 4 irregular, 3.8 s |
| `gold/economy` | **6,935 business days × 1,034 series, 88.6% filled** (long form is 5.8% of that grid), 13.1 s |

⚠️ **The wide panel is in GOLD, not silver, and that is a layering decision made on
measurements.** As silver it was 5.8% filled — but the nulls cost ~1 bit each, so the
real arguments were: a column-per-series table makes the **schema a function of the
data** (new series ⇒ DDL, 65% of PostgreSQL's 1,600-column ceiling), it **mixed
frequencies on one calendar** (67 daily series imposed a 9,719-day grid on 500 monthly
ones; on their own grids the buckets are 76-93% filled), and every step that makes the
panel dense is a **modelling** decision that silver must not take.

⚠️ **The look-ahead guard is the reason gold owns it.** The source `date` is the
reference period, not the release date, so each observation is shifted by a per-frequency
publication lag, carried forward as-of with a staleness cap, and the calendar stops
TODAY. Verified: VNGDPYY's Q1-2026 figure first appears at ref + 45 days.

⚠️ **A weekend nearly ate a quarter of GDP.** `2025-12-31 + 45 days` is a Saturday, and
reindexing onto a business-day calendar dropped that observation SILENTLY — the series
jumped Q3 → Q1. Availability dates are rolled forward to the next business day now, and
an invariant check raises if the reindex loses anything. 565,171 observations in range,
**0 missing**.

> **Retired the same day: `silver/trading_view_economy`**, an earlier version of the same
> pivot under its own table name. Code at `fa74ad3`.

### `silver/stock_market` — four bronze tables into one (2026-08-01)

The four `bronze.cafef_index_*` tabs are four MEASURES of the same entity (index × day),
so they join on the full key into one table:

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "silver/stock_market"
```

| check | result |
|---|---|
| materialised | **RUN_SUCCESS, 3.0 s** |
| shape | **25,935 index-days × 30 columns**, PK `(exchange, ticker, date)` unique |
| indices | 6 — `VNINDEX`, `VN30INDEX`, `VN100-INDEX`, `HNX-INDEX`, `HNX30-INDEX`, `UPCOM-INDEX` |
| **every source value preserved** | **532,188 cells compared vs the four bronze tables, 0 mismatches** |
| coverage | 24,962 price / 22,863 order stats / 20,547 foreign / 1,494 prop — each exactly its bronze row count |

⚠️ **OUTER join, unlike `stocks_basic`'s left-join-on-price.** The key union is 25,935
against price's 24,962, so a left join would have dropped **973 index-days** that carry
order-stats (930), foreign (539) or prop (6) data with no price row — 842 of them
VN100-INDEX. An invariant check raises if the join count ever differs from the key union.

⚠️ **`ticker` here is an INDEX CODE, not a company.** This table must never be unioned
into `silver.stocks_basic`.

### `gold/stock_market` — the index panel, 1 row per trading day (2026-08-01)

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "gold/stock_market"
```

| check | result |
|---|---|
| materialised | **RUN_SUCCESS, 4.2 s** |
| shape | **6,339 trading days × 162 columns** (6 indices × 27 measures), PK `date` unique |
| naming | `{exchange}__{ticker}__{measure}`, max 42 bytes, all identifier-safe |
| **exact round-trip** | 532,188 observations compared vs silver — **0 missing, 0 mismatches** |

⚠️ **Hyphens.** `HNX-INDEX` / `VN100-INDEX` / `HNX30-INDEX` / `UPCOM-INDEX` cannot be
unquoted PostgreSQL identifiers (`hnx-index` parses as subtraction) and
`_helper_build_upsert_sql` does not quote. They are sanitised to underscores and the
result is **collision-checked**, because merging two indices into one column would be
silent.

⚠️ **No as-of fill here, unlike `gold/economy`.** Macro series are stale-but-valid
between releases; an index either traded or it did not. Filling would invent prices for
days the market was shut — so the panel is 34-71% filled per index, which is just each
index starting on a different day (VNINDEX 2000-07, VN100-INDEX 2014-02).

**DECIMAL, not REAL:** at 162 columns there is no row-size pressure, and `value_matched`
reaches ~1e12 where REAL would lose thousands. Hence the exact round-trip above.

### `silver/stocks_basic` — six bronze tables into the per-stock panel (2026-08-01)

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "silver/stocks_basic"
```

| check | result |
|---|---|
| materialised | **RUN_SUCCESS, 7m36s** — the biggest silver build so far |
| shape | **2,388,368 stock-days × 38 columns**, 781 tickers, = the spine's row count exactly |
| **values** | all four joined blocks vs their bronze sources over the full table — **0 mismatches** |
| coverage gain | order stats **347,841 → 2,323,351**, prop **63,389 → 72,607** |

The gain is not from this asset: `bronze.cafef_order_stats` grew 351,373 → 2,523,196 in
the layer-wide re-ingest, and `stocks_basic` had never been rebuilt against it.

⚠️ **The `cafef_price` spine drops ~200k order-stat days.** 199,845 order-stats rows have
no price row — 193,116 of them INSIDE the price date range (all 781 tickers), 6,729 newer
than the price scrape. `silver/stock_market` made the opposite call (outer join) for the
same reason; if those stock-days matter here, it is the same one-word change.

⚠️ **Six sources, two different keys.** Four CafeF tabs on `(exchange, ticker, date)`,
plus `simplize_industry × gics` on `(exchange, ticker)` for the GICS tree — so the asset
declares six bronze deps, not four.

### The FINANCIALS chain — 3 silver assets + 1 gold (2026-08-01)

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "gold/stocks_financials_bank_fa"
dagster asset materialize -f src/orchestration/definitions.py --select "+gold/stocks_financials_bank_fa"   # incl. upstream
```

Four assets, and the chain is the only silver→silver one in the layer — each step reads
the table the step before it wrote, which is why they are four assets and not one:

| asset | result |
|---|---|
| `silver/cafef_financials_bank` | **152 quarters × 180 columns** (136 with a `publish_date`) — both halves of `main.py`'s `financials` leaf: the per-report carry-ups, then the wide per-quarter join |
| `silver/stocks_basic_financials_bank` | **8,265 stock-days × 216 columns**, 45 s — daily × quarterly, as-of on `publish_date`. **The asset counts rows with `publish_date > date` and RAISES** — the look-ahead guard is an assertion, not a comment |
| `silver/stocks_basic_financials_bank_fa` | **8,265 × 242**, 11 s — + the 26 fundamental indicators |
| `gold/stocks_financials_bank_fa` | **8,265 stock-days × 1,150 columns** (908 added), 65 MB, ~9 s |

All four green, **VCB 4,235 + ACB 4,030**. The chain was first built VCB-only (silver
predated ACB's statements landing in bronze on 2026-07-30); re-running it end to end is
what picked ACB up — 4,235 → 8,265 rows at every step.

⚠️ **The gold asset asserts its own grain**: it fails if its row count differs from
silver's. A feature build must add columns, never rows, and a TA layer that silently
duplicated a stock-day would otherwise look like a bigger, better table.

⚠️ **`gold` had to REBUILD THE PRICE SERIES, and this is the interesting part.** The
CafeF panel has no usable OHLC set: `open`/`high`/`low` are RAW while only the close is
adjusted (`close_adjust`). VCB on 2009-06-30 is the whole problem in one row —
`open`=`high`=`low`=`close_raw`=60,000, `close_adjust`=9,130. TA on the adjusted close
with raw high/low puts two price scales inside one indicator; TA on the raw close
re-introduces every split as a fake overnight crash. `_helper_adjust_ohlc` applies the
standard factor `close_adjust / close_raw` to that same day's open/high/low, and the
adjusted set takes the canonical names `open`/`high`/`low`/`close` (what TA-Lib's
defaults read), with the source values kept as `open_raw`/`high_raw`/`low_raw`.
**So gold's `open`/`high`/`low` are NOT silver's** — same names, adjusted values.

Verified: 0 mismatching cells over 163 financial columns AND over all 26 indicators
(DOUBLE PRECISION, so the round-trip is exact — REAL would round VND figures of ~1e15 to
the nearest ~1e8); `close` = `close_adjust` and `high` = `high_raw × factor` on every
row; 0 rows with `publish_date > date`; SMA-50 reproduces to 0.0 and RSI-14 to 6.8e-6
against an independent pandas computation.

⚠️ **One genuine bad row, and it is CafeF's.** ACB 2018-07-31 has `high` 35,800 < `low`
36,500 in `bronze.cafef_price` — one of **262 such rows in that 2.4 M-row table** —
which surfaces in gold as a negative `range_hl`. The adjustment scales both legs by the
same positive factor, so it preserves the inversion rather than causing it. The fix
belongs in a bronze data-quality screen, not here.

### ⚠️ Three things this build broke on, all of them pre-existing

**1. `sys.path` does not survive into a step.** `bootstrap()` runs when the asset module
is imported — but Dagster loads a code location inside a context manager that RESTORES
`sys.path` afterwards, so by the time a step executes, the entry it added is gone.
Modules imported during the load survive in `sys.modules` and hide it completely; a
module imported LAZILY at run time does not. `_build_transform_func_map` imports
`ta.ta_functions` on the first `_helper_transform` call, so this asset died with
`ModuleNotFoundError: No module named 'ta'` in the step subprocess while all 45 earlier
assets passed — none of them had ever reached the TA path. **Fixed**:
`PreprocessorResource.session` calls `bootstrap()` again on entry (it is idempotent and
free). Any future asset that touches a lazily-imported repo module is covered.

**2. No gold table built through `_ingest_gold_table` could be materialised TWICE.**
The COPY writer assumes an empty table, so the second run died on the primary key —
`duplicate key value violates unique constraint … Key (exchange, ticker, date)=(HOSE,
VCB, 2009-06-30) already exists`. Re-materialising is the normal life of an asset, so
"drop the gold table first", which is what `data_preprocessor/CONTEXT.md` §7 told a
human to do, was never going to survive contact with an orchestrator. **Fixed**:
`_ingest_gold_table` drops the table itself, as late as possible so an earlier failure
leaves the old one intact — the same thing `_ingest_gold_economy` and
`_ingest_gold_stock_market` always did. This also un-breaks re-runs of gold
`bonds`/`forex`/`funds`/`indices`.

**3. `_ingest_gold_stocks` is stale and raises.** `gold.stocks` in the database predates
the 2026-07-19 rewrite of `silver.stocks_basic` and still carries that era's columns
(`close`, `volume`, `f_buy_vol`, `own_pct`). The current source has neither `close` nor
`volume`, so its first TA layer dies exactly the way this asset's did. **Not fixed at the
time, on purpose** — the remedy is the `prepare_fn` + `volume_col="volume_matched"` pair
the `_fa` asset already uses, but switching it on re-defines `gold.stocks`'
`open`/`high`/`low` as adjusted and commits to a ~2.4 M-row × ~900-column rebuild. That
is a decision to take on its own, not a side effect of adding a different table.
✅ **Taken on its own on 2026-08-03 — see "The per-stock panel, split in two" below.**

### The per-stock panel, split in two — `gold/stocks` + `gold/stocks_ta` (2026-08-03)

One silver source, two gold tables, split by whether a column is **carried** or
**computed**:

```
silver/stocks_basic ─┬─► gold/stocks      2,388,368 × 42    adjusted OHLC + flow, NO features
                     └─► gold/stocks_ta   2,388,368 × ~940  the same + the ~900-column TA block
```

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "gold/stocks"
dagster asset materialize -f src/orchestration/definitions.py --select "gold/stocks_ta"   # ⚠️ hours, ~11 GB
```

⚠️ **What the split buys is one thing, and it is not tidiness.** PostgreSQL reads the
whole row, so every query that wanted OHLC out of the old 935-column table paid for 905
TA columns it did not want — ~11 GB against ~200 MB.
[unified_schema_creator.ipynb](../train_test_creator/unified_schema_creator.ipynb) was
already splitting them by hand (`GOLD_NON_TA` vs its `pool__ta` group); this makes the
split a table boundary instead of a convention two notebooks have to agree on.

⚠️ **This is where the deferred OHLC decision got taken.** `_ingest_gold_stocks` had been
raising since 2026-07-19 (§"Three things this build broke on", item 3) and the fix was
held back because it re-defines `open`/`high`/`low` as adjusted. It does — and it is not
lossy: `_helper_adjust_ohlc` keeps the source legs beside them as
`open_raw`/`high_raw`/`low_raw`/`close_raw`, so both scales are present and neither is
implicit. 2,388,368 rows got a factor, 524,633 of them exactly 1.0.

⚠️ **Neither table is built from the other**, for the same reason
`stocks_financials_bank_fa` is not built from `gold.stocks`: a table carrying its base
from another gold table could disagree with it about a stock-day while looking identical.
Each recomputes from silver — the cost is one extra read of a 2.4 M-row table.

⚠️ **`gold.stocks_ta` in the database is OLDER than the code that builds it, and
rebuilding it COSTS HISTORY.** It is the **rename** of the pre-rewrite `gold.stocks` —
2,678,167 rows on the old column names (`close`, `volume`, `f_buy_vol`, `own_pct`).
Renaming does not rebuild, and the rebuild was deliberately not run. Measured against
today's silver:

| | rows |
|---|---|
| in `stocks_ta` only, **before 2009-01-02** | **98,464** — current silver starts 2009-01-02; CafeF's price history does not go back further |
| in `stocks_ta` only, 2009 onward | **197,852** — stock-days the `cafef_price` spine drops |
| in current silver only | 6,517 — data newer than the old build (to 2026-07-08) |
| shared | 2,381,851 |

So materialising `gold/stocks_ta` replaces a 2,678,167-row table with a 2,388,368-row
one and **loses 296,316 stock-days, including every day before 2009**. That history came
from the source-priority merge the 2026-07-19 silver rewrite removed, and no current
asset can rebuild it. Dump the table first if those years matter. The row-count assertion
in the asset is what makes the change visible rather than silent.

⚠️ **Carried numerics are DOUBLE PRECISION in this pair, not gold's default REAL.** The
default exists because ~900 float8 columns cannot fit PostgreSQL's ~8160-byte row limit;
at 42 columns there is no such pressure, and `value_matched` reaches ~1e12 where REAL
rounds to the nearest ~1e5.

⚠️ **`_ingest_gold_table(standard_features=False)` is the new mechanism, and it disables
a guard on purpose.** The empty-layer `PipelineError` exists to catch a gold table that
came out a copy of its input BY ACCIDENT; with this flag that is the stated intent, so
the two cases have to be distinguishable. It also needs its own write path —
`_helper_transform` returns the frame untouched when no layer resolves and **never
reaches `checkpoint_fn`**, so a featureless build routed through it would log success and
write nothing.

### Gold housekeeping — what the schema holds, and what it is allowed to hold

**Ten tables**, and every one of them is something the code can still build — the
schema and the pipeline agree, which is the point of the housekeeping.

| table | shape | built by | state |
|---|---|---|---|
| `economy` | 6,935 × 1,035 | **asset** + leaf | current (wide, as-of filled) |
| `stock_market` | 6,339 × 163 | **asset** only | current (wide, unfilled) |
| `stocks` | 2,388,368 × 42 | **asset** + leaf | current — the price panel, no features (2026-08-03) |
| `stocks_ta` | 2,678,167 × 935 | **asset** only | ⚠️ the RENAME of the old `stocks`; the builder is current, the TABLE is not |
| `stocks_financials_bank_fa` | 8,265 × 1,150 | **asset** only | current |
| `news_weekly_panel` | 429,052 × 28 | **asset** only | current |
| `news_daily_panel` | 2,058,604 × 26 | **asset** only | current |
| `bonds` | 66,100 × 16 | leaf | unknown age |
| `forex` | 1,324,940 × 16 | leaf | unknown age |
| `funds` | 18,662 × 22 | leaf | unknown age |
| ~~`indices`~~ | ~~24,095 × 22~~ | — | **RETIRED + DROPPED 2026-08-01** |

⚠️ **`gold.indices` is retired because it was a duplicate.** It was `silver.indices`
(the TradingView index series) through the generic single-series feature build —
`value` plus returns/volatility/rolling, 22 columns. `gold.stock_market` already covers
the same six Vietnamese indices from CafeF at **27 measures apiece** (OHLC, order stats,
foreign flow, prop trading, matched/negotiated split) instead of one. Two gold tables
for one asset is one too many — the same call made for `economy` vs `economy_panel` on
2026-08-01. `_ingest_gold_indices`, the `data_quality_gold/indices` leaf and the switch
key are all gone, and the table itself was **dropped** the same day (24,095 rows, 6
tickers, 2000-07-28 → 2026-06-09, 4064 kB).

**Nothing upstream was touched** — `bronze_schema.trading_view_indices` and
`silver_schema.indices` (24,095 rows) are both intact, so no history is lost and the
reversal is one line (`_ingest_gold_table("indices")` + its leaf). ⚠️ Note the bronze
table is `trading_view_indices`, not `indices`: silver renames it, and only silver and
gold used the short name.

⚠️ **The two per-stock FEATURE panels share an identical 888-column TA block** — 337
overlap studies, 293 momentum, 90 cycle, 60 price transform, 58 volatility, 50 volume,
from 43 indicators via `_helper_stock_ta_layers`. 207 of them are boolean signals. That
is the point of the shared helper: `gold.stocks_ta` and
`gold.stocks_financials_bank_fa` cannot drift into different feature sets while looking
identical. `gold.stocks` has none of them by design — that is the split.

### UNIFIED — the fourth layer, and the first scoped to ONE TICKER (2026-08-03)

`unified_schema_<ticker>` is not a fourth copy of the pipeline. It is one company's
slice, cut into the **feature groups a model selects over**:

```
silver/stocks_basic ──► unified_vcb/pool__basic     4,235 × 38, PK (exchange, ticker, date)
                        pool__ta / pool__macro / pool__calendar / pool__targets   ⚠️ NOT ASSETS YET
```

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "unified_vcb/pool__basic"
```

| check | result |
|---|---|
| materialised | **RUN_SUCCESS, 611 ms**; re-run green (the asset REPLACES its table) |
| shape | **4,235 rows × 38 columns**, 1 ticker, 2009-06-30 → 2026-06-25 |
| column name + type + ORDER vs `silver.stocks_basic` | **identical, all 38** |
| every value vs silver, 35 non-key columns | **0 mismatches** over 4,235 rows |

⚠️ **The schema is created BY THE ASSET.** `PreprocessorResource.session` already issues
`CREATE SCHEMA` for whatever schema it is handed — the same preamble
`ingest_bronze_data` runs — so naming `unified_schema_vcb` there is what brings it into
existence. `_ingest_unified_pool_basic` creates it too, so the method is still correct
from a notebook or `main.py`.

⚠️ **`CREATE TABLE AS`, not a pandas round-trip, and that is type fidelity not taste.**
psycopg2 returns a PostgreSQL `numeric` as a Python `Decimal`, which lands in a DataFrame
as dtype `object`, and `_helper_infer_sql_type` maps `object` → VARCHAR. Reading this
table out and writing it back would turn every price and value column into TEXT **while
looking like it worked** — the same "degraded VARCHAR" the silver carry-ups have. A
server-side CTAS never materialises a Python value, so the types are silver's by
construction, which is what the check above confirms.

⚠️ **The ticker is an IDENTIFIER, not a value.** It is interpolated into a schema NAME,
and a name cannot be a bound parameter. `_helper_unified_schema` validates it against
`UNIFIED_TICKER_PATTERN` and raises otherwise — it is the only thing between a ticker
(which arrives from a CSV, a config or a partition key) and arbitrary SQL.

⚠️ **`UNIFIED_TICKER = "VCB"` is a constant, not a partition**, deliberately. A partition
would imply the other four `pool__*` tables are per-ticker assets too, and they are not
assets at all — `train_test_creator/unified_schema_creator.ipynb` still builds them.

⚠️ **THE REST OF THE SCHEMA WAS DROPPED ON 2026-08-03 AND NOTHING REBUILDS IT.**
`unified_schema_vcb` held **140 objects — 113 tables and 27 views, 126 MB**: the five
`pool__*` groups and 135 `<target>__lb<N>__<group>__<n>` feature-selection outputs across
three targets × ten lookbacks. All of it was dropped at the user's explicit instruction
(`DROP SCHEMA … CASCADE`), and only `pool__basic` has been rebuilt. The selection outputs
were notebook work — the `pool__ta` ensemble alone cost ~6.6 h at lookback=20 (memory
`project-feature-selection-ta-cost`) — so re-running the notebook is the only way back,
and its `GOLD_NON_TA` / `pool__ta` split now wants rewriting against `gold.stocks` and
`gold.stocks_ta` instead of the one 935-column table it was written for.

⚠️ **`pool__basic` is now 4,235 rows where every dropped sibling had 4,242**, because it
is built from `silver.stocks_basic` (which ends 2026-06-25) rather than from the old
`gold.stocks` (2026-06-26). Anything rebuilt to join against it has to be built from the
same source or it will silently lose the difference.

## 2a. Cost of a full materialize (2026-07-31)

| | before | after |
|---|---|---|
| warm re-run (disk populated) | ~17-20 h | **~15-40 min** |
| cold run (empty `raw_data/`) | ~33-47 h | ~20-30 h |

Two changes, both measured:

**1. TradingView data now skips what is on disk.** It had no skip at all — the data path
never checked, and the links path explicitly does `File exists: deleting to re-fetch`.
Every navigation passes a **global** 8-second gate (`SCRAPER_NAV_STAGGER`, a monotonic
lock), so 4,675 links cost ≥10.4 h *of stagger alone*, re-paid every run. Verified on the
current disk: **3,887 → 0 tasks queued**, ~8.6 h of stagger removed. The check is at
task-ADD time, because a task skipped later still pays its 8 s. Matched by glob on the
symbol prefix — the filename carries today's date, so equality would skip nothing.

**2. The statement parse skips complete YEARS.** Verified: VCB **17 of 21 years skipped,
7 of 72 quarters left** (all 2006-2009, which have no filing or no readable one); ACB
**17/17 years, 0 of 65 quarters left** — it is complete, so a re-run is now a no-op
instead of ~2.4 h.

> ⚠️ **The unit is a YEAR, not a quarter, and that is correctness, not convenience.**
> `_decumulate` turns a cumulative income statement into a standalone quarter as
> `YTD − (Q1..Q(q-1))`, taking the priors from **this run's** `data`; a quarter whose
> priors are absent is **dropped**. Skipping Q1-Q3 of a year while Q4 still needs
> parsing would therefore delete the very Q4 the run exists to fix — every time. Keeping
> a year whole guarantees the priors are present. Cross-year is already safe: a Q1's
> `open_ref` is read back from disk.

> ⚠️ **Skipping forces `merge=True`.** `_write` rebuilds the grid from what the run
> holds, so without merging the skipped (complete!) years would lose their `pdf` rows to
> the CafeF tabs — the documented way a 6-minute run destroys a 4-hour one.

> ⚠️ **`skip_existing=False` is the AUTHORITATIVE run, not just the slow one.** Skipping
> makes every run a subset run, which switches `sane` — the magnitude guard comparing a
> figure against neighbouring quarters — to failing open. That guard is what caught ACB's
> Q1-2024 carrying Q1-2023's PBT. Default for "fill the gaps"; `false` whenever the
> PARSER has changed. Settable per-run from the UI (`FinancialsConfig`).

### Executor: multiprocess, with two hard caps

`multiprocess_executor`, `max_concurrent` 4 (override with `DAGSTER_MAX_CONCURRENT`).
Verified running four steps in four PIDs.

**The tag limits are not tuning — without them this is worse than sequential.** Two
resources are physical and capped *inside a single process*, so a second process silently
doubles them:

| tag | limit | why |
|---|---|---|
| `resource: browser` | 1 | `SCRAPER_MAX_CONCURRENT_BROWSERS = 8` is an in-process semaphore — 4 processes is 32 Chrome instances, and 4× the global stagger against TradingView |
| `resource: gpu` | 1 | OCR runs onnxruntime-gpu on a 4 GB RTX 3050; two partitions is VRAM exhaustion |

**So a materialize opens at most 8 Chrome instances**, and only while a TradingView
step runs — nothing else in the repo imports Selenium. The 8 is per PROCESS and the
`browser` tag keeps exactly one browser step running, so the two multiply to 8, not 8×N.

⚠️ **Fixed 2026-07-31: the links phase ignored the semaphore.**
`_scrape_links_attempt` created its driver outside it, so the effective cap there was
the 16-thread pool — 16 browsers (~100-160 `chrome.exe` processes), twice what this
table and `web_scraper/CONTEXT.md` both claimed. It now takes the permit before
`webdriver.Chrome()` and holds it until `quit()`. Verified with a fake driver: 24 tasks
(what the `stocks` links partition queues), 16 workers → **peak 8**.

⚠️ **A multi-run backfill still escapes this.** `tag_concurrency_limits` is EXECUTOR
config, i.e. per run. Backfilling the 9 TradingView partitions the default way launches
9 runs, and `.dagster/dagster.yaml` is empty — 9 × 8 = 72 browsers. Use a single-run
backfill, or give the instance a real concurrency pool, if you ever backfill TV.

Everything else is `requests`-bound and safe to overlap — 4 assets × a 16-thread pool is
~64 in-flight requests, the concurrency the news scraper already runs at.

⚠️ **`logs/app.log` now has several writers.** `Logger` calls `basicConfig` on the ROOT
logger, so every step process appends to the same file and records may interleave out of
order. The file is kept (that was the requirement) and each line still names its class and
method, but it is no longer a strict chronology; Dagster's own per-step logs are in
`.dagster/`. If it becomes a problem the fix is a per-process filename in `Logger`, not a
return to sequential execution.

## 3. Design decisions, and why

- **No pipeline logic lives here.** Every asset is a thin wrapper over a method that
  already exists in `src/`. Delete `src/orchestration/` and nothing is lost but the
  scheduling; `main.py` keeps working the whole time. This is what makes the
  migration incremental rather than a rewrite.
- **Assets are generated from a spec table**, not copy-pasted — `TABS` in
  [assets/cafef_index.py](assets/cafef_index.py) is four rows and produces eight
  assets. At ~60 assets this is the difference between maintainable and not.
- **Assets call the per-tab method (`scrape_all_index_price`), never `scrape()`.**
  `scrape()` re-consults the switch config, which would let `switch_config.json`
  silently veto a materialisation the user explicitly asked for. **Selection is
  Dagster's job now.**
- **`PreprocessorResource.session` has no `except` clause.** This is the point of the
  migration — see §4.1.
- **The repo `Logger` is kept as-is.** It calls
  `logging.basicConfig(filename="logs/app.log")` on the ROOT logger and is threaded
  through every scraper/preprocessor constructor, so replacing it with `context.log` is
  a wide refactor and a separate decision.
  > ⚠️ **This bullet used to end "and the executor is pinned `in_process`" — that is no
  > longer true.** The executor is MULTIPROCESS (§2a), which is exactly why several step
  > processes now interleave writes into that one file. The trade was made deliberately:
  > the file stays (that was the requirement), each line still names its class and
  > method, and Dagster's own per-step logs are in `.dagster/`.
- **`_bootstrap.py` sets `sys.path` and the CWD.** The repo's modules import each
  other flat (`from web_scraper.x import y`), which works today only because
  `python src/main.py` puts `src/` at `sys.path[0]`; `pytest.ini` solves the same
  problem with `pythonpath = src`. The CWD matters just as much: `SwitchHandler`
  defaults to the *relative* `src/switch_config.json`, `Logger` to a relative
  `logs/app.log`, and the `*_RAW_DATA_DIR` constants are relative — and a wrong CWD
  fails **quietly** (an unreadable switch config returns `{}`, i.e. every switch off).
  > ⚠️ **Since the move into `src/`, that path entry is also what makes THIS package
  > importable** — so `definitions.py` cannot reach `_bootstrap` through an import and
  > repeats the insert inline instead. See §5 *Gotchas*.
- **Counts are read through a raw cursor, not `driver.select`.** Written before Phase 0
  fixed `select` (which used to return an empty DataFrame on error, so a missing table
  read as a legitimate 0 rows). `select` now raises, so this is belt-and-braces rather
  than load-bearing — but a `COUNT(*)` through a cursor is also one less layer between
  the asset's metadata and the database.

## 4. The migration plan

### 4.1 ✅ Phase 0 — exception propagation (DONE, 2026-07-31)

A stage that did not do its work now raises. New module
[src/utils/exceptions.py](../utils/exceptions.py): `PipelineError` base,
`MissingSourceDataError` (input absent/empty), `DatabaseQueryError` (query failed).

| was | now |
|---|---|
| `driver.select` → `except: return pd.DataFrame()` | **raises `DatabaseQueryError`.** The most dangerous swallow in the repo: a typo'd column, a missing table or a dropped connection all became "0 rows", indistinguishable from an empty table. A genuinely empty table still returns an empty frame through the normal path, so the two are now distinguishable |
| `_helper_load_csvs` / `_helper_load_cafef_folder` → `return None` | **raise `MissingSourceDataError`**; the ~23 `if x is None: log_error(...); return` call sites are gone |
| 20 ingest bodies → `log_error(...); return` | **raise `MissingSourceDataError`** naming the folder and the file count |
| unknown financial-schema template → `continue` | **raises** — it silently dropped a whole template's chart of accounts |
| gold `_helper_build_feature_layers` returns `[]` → `log_error; return` | **raises `PipelineError`** — the gold table would have been a copy of its input |
| `ingest_{bronze,silver,gold}_data`: ONE `try/except` around every leaf | **`_run_layer`**, shared by all three: per-leaf isolation, and a **summary** naming exactly what failed |
| `FinancialsBuilder.build_all`: per-ticker `except` → `{}` | per-ticker isolation KEPT (a ticker costs ~2.4 h), plus an unmissable failure summary |

**The three entry points still do not raise, deliberately.** They are the `main.py`
compatibility shim: `main.py` calls them unconditionally and has always run to
completion. What changed is that a failure is no longer invisible — and that the
FIRST failure no longer silently skips every leaf after it, which the single
`try/except` used to do. They return the list of failed leaves, so a caller can act on
it. **Orchestration does not come through them** — assets call `_ingest_*` directly.

Verified:

| check | result |
|---|---|
| missing `raw_data/` folder | `MissingSourceDataError` raised (was: log + return) |
| `select` on a missing table | `DatabaseQueryError` raised (was: empty DataFrame) |
| `select` on a real table | unchanged — 24,962 rows |
| `_run_layer` with a failing middle leaf | `['b']` returned, `a` and `c` still ran, summary logged |
| `ingest_bronze_data()` on a real leaf | `[]`, `` `bronze` finished: 1 leaves OK ['cafef_index_price'] `` |
| prototype re-materialised | both steps green, same 24,962 rows |

⚠️ **What Phase 0 does NOT cover.** The driver's other methods
(`create_table`/`insert`/`update`/`upsert`/`delete`) still return a
`DatabaseExecutionStatus.ERROR` enum that no caller checks. They are left alone
because the live write path does not use them — `_helper_save_pandas_table_to_database`
builds its own SQL and **already re-raises** from its worker threads
([data_preprocessor.py:529-531](../data_preprocessor/data_preprocessor.py#L529-L531))
— and because a failed `create_table` surfaces immediately as a failed insert anyway.

⚠️ **Expect red on the first wide run.** Turning on a bronze leaf whose scraper has
never run is now a *failure* rather than a silent no-op. That is intended, and the
per-leaf isolation means it costs you one leaf, not the layer.

### 4.2 Phase 1 — the preprocessor (~41 assets, low risk)

> **✅ BRONZE IS DONE (2026-08-01)** — all 20 leaves are assets and have been
> materialised green; see §"Phase 1a" above. What remains of Phase 1 is silver (7 of
> ~15 done) and gold (3 of 7 — `stocks_financials_bank_fa` is a table `main.py` cannot
> build at all, like `stock_market` before it: new gold work lands as an asset and gets
> no switch leaf, since phase 5 retires those anyway).

Pure DB work, fast to iterate, no network. One asset per bronze leaf (20), per silver
ingest (~15 — the multi-ingest leaves like `cafef_carry_ups` and `stocks_financials`
split naturally, which is an improvement), and per gold leaf (6).

Bronze has **no cross-table dependency** — each ingest reads its own `raw_data/`
folder — so those 20 are a flat layer. Silver and gold edges are already documented in
[data_preprocessor/CONTEXT.md](../data_preprocessor/CONTEXT.md) §4 and can be
transcribed directly.

Note `data_quality_unified` is a **dead switch** — no code reads it. Drop it.

### 4.3 ✅ Phase 2 — the cheap scrapers (BUILT)

`cafef_index`, `cafef/{price,foreign,order_stats,prop_trading,insider_txn}`,
`cafef_news`, `simplize/{stocks,industry}`, `gics/structure` are all assets in
[assets/scrape.py](assets/scrape.py).

⚠️ **The planned `deps=[tv_collected_links]` edge was WRONG and is not what was built.**
The audit in §2 found that CafeF and Simplize read `links/stocks/` directly, not the
aggregate — nothing reads `collected_links` at all. The real edge is
`trading_view_links` partition `stocks`, via `SpecificPartitionsPartitionMapping`.

### 4.4 ✅ Phase 3 — TradingView (BUILT, but partitioned differently than planned)

`trading_view_links` / `trading_view_data` are partitioned by **asset class (9)**, not
by the 320 `(asset_class, country, type, sector)` switch leaves as this plan proposed.
320 partitions would re-encode `switch_config.json` in a second place and be unusable in
the UI; the sub-leaves stay in the JSON as *parameters* that each scraper's own task
adder reads. `trading_view_collected_links` sits between them, unpartitioned.

⚠️ **The "keep TV in-process" instruction was superseded.** The executor IS multiprocess
now; what keeps the in-process browser semaphore meaningful is the
`resource: browser` tag limit of 1 (§2a), not sequential execution.

### 4.5 ✅ Phase 4 — the heavy per-filing pipelines (BUILT)

`cafef_pdfs` (100 partitions) and `cafef_financials` (2) are ticker-partitioned, and
`CAFEF_PDF_TICKERS` / `CAFEF_FINANCIALS_TICKERS` no longer scope a run — selection does.

The `build_templates_index` hazard was handled exactly as this plan demanded:
`cafef_financials_templates` is a SEPARATE unpartitioned asset upstream of the
partitioned parse, because the builder REWRITES `templates.csv` from exactly the symbols
handed to it (which is how VCB lost its row when ACB was parsed alone).

⚠️ **Built is not run.** These two, plus `trading_view_data` and the full-universe CafeF
tabs, have never been materialised end-to-end here — see the warning in §2.

### 4.6 Phase 5 — retire the switch config's run-plan role

Delete every leaf that only ever meant "run this". Keep only genuine parameters (the
TradingView leaves). Until this happens there are two sources of truth, which is worse
than either alone.

### Effort

| phase | scope | estimate |
|---|---|---|
| 0 | exception propagation | ✅ **done** |
| 1 | preprocessor, ~41 assets | bronze ✅ **done**; silver 7/15, gold 3/7 |
| 2 | cheap scrapers, ~13 assets | ✅ **done** |
| 3 | TradingView + partitions | ✅ **done** (9 partitions, not 320 — see 4.4) |
| 4 | pdfs/financials, ticker partitions | ✅ **built**, not yet run end-to-end |
| 5 | retire switch config | half a day — started: `src/data_preprocessor` is archived as a RUN path (2026-08-01), the keys are still there |

`main.py` ends up empty and `switch_config.json` shrinks to ~320 parameter keys.

**Where it actually stands (2026-08-03):** phases 0-4 are built; every landing and bronze
asset has been materialised green, silver has 8 assets, gold 7, and a fifth `unified`
layer has 1. What remains is the rest of silver (~7 leaves), the four remaining
`pool__*` groups of the unified schema, phase 5, and end-to-end runs of the four heavy
assets.

⚠️ **Phase 5 is now half-true in the documentation and not at all in the code.**
`src/data_preprocessor/CONTEXT.md` carries an ARCHIVED banner and this file says
selection is the run plan — but every `data_preprocessor/data_quality_*` key is still in
`switch_config.json`, and `_run_layer` still consults them. Nothing reads them on the
asset path (assets call `_ingest_*` directly), so they are inert rather than dangerous;
finishing phase 5 means deleting the keys and the three entry points, which is a
separate, mechanical change.

## 5. Gotchas

- **`DAGSTER_HOME` must be an absolute path** and must exist. `.dagster/dagster.yaml`
  may be empty but silences a warning on every command.
- **`dagster asset materialize` needs `-f src/orchestration/definitions.py`** even though
  `workspace.yaml` exists — the workspace file is honoured by `dagster dev`, not by the
  one-shot asset commands.
- **⚠️ `definitions.py` REPEATS `_bootstrap`'s `sys.path` insert INLINE, and that is not
  redundancy** (2026-08-01, when this package moved into `src/`). `dagster -f <file>`
  loads the file as a **top-level module** — named `definitions`, with no package context
  — and puts only the **working directory** (the repo root) on `sys.path`. `src/` is not
  on it, so `from orchestration._bootstrap import bootstrap` would fail on the very line
  that exists to add `src/`: the bootstrap cannot bootstrap its own package. Relative
  imports (`from ._bootstrap import …`) are not the way out either — a file loaded by
  path has no parent package. Hence four lines of prelude in that ONE file, above its
  first `orchestration` import. Every other module here is reached through the package
  and needs nothing. **Do not "tidy" it away**, and do not move `definitions.py` deeper
  without re-checking the `parent.parent` it computes.
  - The same applies to anything else that loads a module here BY PATH. Importing it
    normally (`sys.path.insert(0, "src"); import orchestration.definitions`) is fine —
    `orchestration/__init__.py` calls `bootstrap()` and the package import already
    required `src/` to be reachable.
  - `working_directory` in `workspace.yaml` stays the **repo root**, not `src/`: it is
    the CWD that the relative `raw_data/`, `logs/app.log` and `src/switch_config.json`
    paths resolve against.
- **Dagster loads `.env` itself** on startup (confirmed: it reported loading
  `POSTGRES_*`), so the `load_dotenv()` calls in the repo are belt-and-braces here.
  Do NOT rely on that for scripts run outside Dagster.
- **The CLI warns that `dagster definitions validate` / `asset materialize` are
  superseded** by `dg check defs` / `dg launch`. Both still work in 1.13; if they are
  removed, the replacements are drop-in.
- **`skip_existing=True` still applies inside the scrapers**, so a scrape asset can
  materialise "successfully" in 500 ms having fetched nothing. That is correct
  behaviour, but it means a green materialisation is NOT evidence of fresh data — the
  row-count metadata is what to read. (Cf. the short-page pagination bug in
  `web_scraper/CONTEXT.md §7`: the per-stock CSVs on disk predate the fix and
  `skip_existing` will not refresh them.)
- **The OCR venvs (`ocr_env8`/`ocr_env9`) are irrelevant here** — the production
  financials parse runs in `mt_env` with `CAFEF_OCR_ENGINE=onnx` against the
  `onnxruntime-gpu` already installed there. Only the experiments need those venvs, and
  they stay outside Dagster.
