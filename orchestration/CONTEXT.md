# Context — `orchestration` (Dagster migration of `src/main.py`)

> Handoff notes. **Status (2026-08-01): the LANDING layer and the whole BRONZE layer are
> assets and have both been materialised green; silver has its first asset.** 40 assets.
> `src/main.py` is untouched and still runs the pipeline the old way. What is left is
> silver (14 of 15) and gold (6) — see §4. Verify anything before acting on it: the code
> and `src/switch_config.json` are still the sources of truth.

## 1. Why this is worth doing

`main.py` is already a DAG, written as `if switch: call()`. The evidence is
[data_preprocessor.py:3495-3525](../src/data_preprocessor/data_preprocessor.py#L3495-L3525),
a hand-written list of `(leaf_name, callable)` pairs iterated against the switch
config, and [cafef_scraper.py:516-534](../src/web_scraper/cafef_scraper.py#L516-L534),
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

**40 assets: 19 landing + 20 bronze + 1 silver.** Every scraper in `main.py` lands to
`raw_data/` as an asset ([assets/scrape.py](assets/scrape.py)); every bronze ingest leaf
is an asset ([assets/bronze.py](assets/bronze.py), 20 leaves → 25 tables); silver has its
first ([assets/silver.py](assets/silver.py)). They are separate modules on purpose: the
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
cafef_index_{price,order_stats,foreign,prop_trading} ─► bronze/cafef_index_*
gics_structure
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
| `silver` | `economy` (canonical long) | — |

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

[src/utils/inputs.py](../src/utils/inputs.py) makes the choice explicit at the read
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
| the flat `src/` layout can be imported by Dagster | `dagster definitions validate` | passes, **40 assets** (19 landing + 20 bronze + 1 silver), all code locations OK |
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
dagster asset materialize -f orchestration/definitions.py --select "bronze/cafef_index_price"

# C. headless, a cheap known-good slice (4 scrapes, seconds — files already on disk)
dagster asset materialize -f orchestration/definitions.py --select "group:cafef_index"

# D. one partition of a per-ticker asset
dagster asset materialize -f orchestration/definitions.py `
    --select "raw/cafef_financials" --partition "HOSE_VCB"

# E. the WHOLE bronze layer, 20 assets — ~9 min, 10.6 M rows (raw_data must be populated)
dagster asset materialize -f orchestration/definitions.py --select "group:bronze"

# F. a table AND everything upstream of it — scrape, then ingest, in order
dagster asset materialize -f orchestration/definitions.py --select "+bronze/trading_view_economy"
```

**Bringing the landing layer up from nothing**, in dependency order — the universe
first, because CafeF and Simplize read it:

```powershell
dagster asset materialize -f orchestration/definitions.py --select "raw/trading_view_links"        --partition stocks
dagster asset materialize -f orchestration/definitions.py --select "raw/trading_view_collected_links"
dagster asset materialize -f orchestration/definitions.py --select "group:cafef"        # ⚠️ hours
dagster asset materialize -f orchestration/definitions.py --select "group:simplize"
dagster asset materialize -f orchestration/definitions.py --select "group:cafef_index"  # independent
dagster asset materialize -f orchestration/definitions.py --select "group:gics"         # independent
```

**Then the database layers**, which need only `raw_data/` on disk:

```powershell
dagster asset materialize -f orchestration/definitions.py --select "group:bronze"   # 20 assets, ~9 min
dagster asset materialize -f orchestration/definitions.py --select "group:silver"
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

All 40 keys are listed in the file as a menu, grouped by source, with `//` comment keys
marking the expensive ones (same comment convention as `switch_config.json`).
`true` or **absent** = loaded, so a newly added asset is on by default.

Behaviour, all verified:

| case | result |
|---|---|
| one key `false` | that asset not loaded (40 → 39); `//` comment keys ignored |
| a key matching no asset | **raises**, listing the valid keys |
| malformed JSON | **raises** — never read as "disable everything" |
| file absent | 40 assets — absent means "no opinion", not "all off" |
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
report 8 assets and "All code locations passed validation").

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
dagster asset materialize -f orchestration/definitions.py --select "group:bronze"
```

**20/20 green, ~9 minutes**, 10.6 M rows re-ingested. This run doubled as the acceptance
test for the `symbol` → `ticker` refactor in `src/` (see `data_preprocessor/CONTEXT.md`
§4-bronze): **22 of 25 tables reproduced their row count EXACTLY.** The three that moved
were stale bronze catching up with raw data the scrapers had already written —
`cafef_news` 5,599 → 405,320 (3 tickers → the full 777), `cafef_order_stats` 351,373 →
2,523,196, `cafef_prop_trading` 64,139 → 73,810 — and each now equals its raw folder
row-for-row.

### The first SILVER asset — `silver/economy` (2026-08-01)

`bronze.trading_view_economy` → `silver.economy`, PK `(exchange, ticker, date)` — the
canonical LONG panel, one row per series per date. Thin wrapper over
`DataPreprocessor._ingest_silver_economy` ([assets/silver.py](assets/silver.py)).

⚠️ **This ingest had never once succeeded.** It re-derived the key with
`df["symbol"].str.split(":")` against a frame that has no `symbol` — bronze splits it on
read — so it raised `KeyError('symbol')` every run; the `silver.economy` table on disk
predated the bronze change. Fixed with the rest of the `symbol` → `ticker` work, and it
now also RAISES on empty bronze instead of logging and returning (its four siblings —
`bonds`/`forex`/`funds`/`indices` — still take the silent path and should follow).

```powershell
dagster asset materialize -f orchestration/definitions.py --select "silver/economy"
```

| check | result |
|---|---|
| materialised | **RUN_SUCCESS, 25.6 s** |
| rows | **579,459** — exactly the bronze row count, a true 1:1 lift |
| tickers | 1,034 |
| the dep is real | `deps=[bronze/trading_view_economy]`, expressible only now that every bronze leaf is an asset |

**Retired the same day: `silver/trading_view_economy`.** The same bronze table pivoted to
one row per DATE × one column per ticker (9,719 × 1,035 columns, 5.8% filled). It was
built and verified on 2026-07-31 — non-null cells = 579,459 = the bronze row count
exactly, 0 value mismatches on 525 spot-checked cells — then dropped in favour of the
canonical long grain. **Nothing was lost:** every observation it held is in
`silver.economy`, and the wide shape is one `pivot` away whenever a model wants it.
The asset and `_ingest_silver_trading_view_economy` are in git history at `fa74ad3`.

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
  already exists in `src/`. Delete `orchestration/` and nothing is lost but the
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
- **The repo `Logger` is kept as-is** and the executor is pinned `in_process`.
  `Logger` calls `logging.basicConfig(filename="logs/app.log")` on the ROOT logger and
  is threaded through every scraper/preprocessor constructor, so replacing it with
  `context.log` is a wide refactor and a separate decision. Under the default
  multiprocess executor, several step processes would interleave writes into that one
  file. Every scraper already parallelises internally with its own `ThreadManager`, so
  step-level multiprocessing buys little here anyway.
- **`_bootstrap.py` sets `sys.path` and the CWD.** The repo's modules import each
  other flat (`from web_scraper.x import y`), which works today only because
  `python src/main.py` puts `src/` at `sys.path[0]`; `pytest.ini` solves the same
  problem with `pythonpath = src`. The CWD matters just as much: `SwitchHandler`
  defaults to the *relative* `src/switch_config.json`, `Logger` to a relative
  `logs/app.log`, and the `*_RAW_DATA_DIR` constants are relative — and a wrong CWD
  fails **quietly** (an unreadable switch config returns `{}`, i.e. every switch off).
- **Counts are read through a raw cursor, not `driver.select`.** Written before Phase 0
  fixed `select` (which used to return an empty DataFrame on error, so a missing table
  read as a legitimate 0 rows). `select` now raises, so this is belt-and-braces rather
  than load-bearing — but a `COUNT(*)` through a cursor is also one less layer between
  the asset's metadata and the database.

## 4. The migration plan

### 4.1 ✅ Phase 0 — exception propagation (DONE, 2026-07-31)

A stage that did not do its work now raises. New module
[src/utils/exceptions.py](../src/utils/exceptions.py): `PipelineError` base,
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
([data_preprocessor.py:529-531](../src/data_preprocessor/data_preprocessor.py#L529-L531))
— and because a failed `create_table` surfaces immediately as a failed insert anyway.

⚠️ **Expect red on the first wide run.** Turning on a bronze leaf whose scraper has
never run is now a *failure* rather than a silent no-op. That is intended, and the
per-leaf isolation means it costs you one leaf, not the layer.

### 4.2 Phase 1 — the preprocessor (~41 assets, low risk)

> **✅ BRONZE IS DONE (2026-08-01)** — all 20 leaves are assets and have been
> materialised green; see §"Phase 1a" above. What remains of Phase 1 is silver (~15,
> one done) and gold (6).

Pure DB work, fast to iterate, no network. One asset per bronze leaf (20), per silver
ingest (~15 — the multi-ingest leaves like `cafef_carry_ups` and `stocks_financials`
split naturally, which is an improvement), and per gold leaf (6).

Bronze has **no cross-table dependency** — each ingest reads its own `raw_data/`
folder — so those 20 are a flat layer. Silver and gold edges are already documented in
[data_preprocessor/CONTEXT.md](../src/data_preprocessor/CONTEXT.md) §4 and can be
transcribed directly.

Note `data_quality_unified` is a **dead switch** — no code reads it. Drop it.

### 4.3 Phase 2 — the cheap scrapers (~13 assets)

`cafef_index` (done), `cafef/{price,foreign,order_stats,prop_trading,insider_txn}`,
`simplize/{stocks,industry}`, `gics/structure`. Same wrapper shape as the prototype.

The CafeF and Simplize assets take a `deps=[tv_collected_links]` edge — they read the
TradingView link CSVs for their universe.

### 4.4 Phase 3 — TradingView (2 assets + partitions)

`tv_links` and `tv_data` become **partitioned** by the enabled
`(asset_class, country, type, sector)` leaves — a `StaticPartitionsDefinition` built
from the same 320 switch keys, which stay in `switch_config.json` as *parameters*.
`tv_collected_links` sits between them, unpartitioned.

Selenium under Dagster needs care: `SCRAPER_MAX_CONCURRENT_BROWSERS=8` is enforced by
an in-process semaphore, so if steps ever move to multiprocess that cap silently stops
applying. Keep TV in-process.

### 4.5 Phase 4 — the heavy per-filing pipelines (2 assets, partitioned by ticker)

`cafef/pdfs` (~1.0-1.7 GB/ticker) and `cafef/financials` (~2.4 h/ticker) become
**ticker-partitioned**, replacing `CAFEF_PDF_TICKERS` / `CAFEF_FINANCIALS_TICKERS` in
constants.py. This is the largest single ergonomic win: re-parsing one ticker becomes
one partition with its own success/failure record, instead of editing a constant and
reading `app.log` to find out what happened.

⚠️ `build_templates_index` **rewrites `templates.csv` from exactly the symbols handed
to it** — it does not upsert (this is how VCB lost its row when ACB was parsed alone).
Under per-ticker partitions this hazard gets *worse*, not better. The index must be a
SEPARATE unpartitioned asset built from the full ticker list, upstream of the
partitioned parse.

### 4.6 Phase 5 — retire the switch config's run-plan role

Delete every leaf that only ever meant "run this". Keep only genuine parameters (the
TradingView leaves). Until this happens there are two sources of truth, which is worse
than either alone.

### Effort

| phase | scope | estimate |
|---|---|---|
| 0 | exception propagation | ✅ **done** |
| 1 | preprocessor, ~41 assets | bronze ✅ **done**; silver 1/15, gold 0/6 |
| 2 | cheap scrapers, ~13 assets | 1 day |
| 3 | TradingView + partitions | 1 day |
| 4 | pdfs/financials, ticker partitions | 1 day |
| 5 | retire switch config | half a day |

`main.py` ends up empty and `switch_config.json` shrinks to ~320 parameter keys.

## 5. Gotchas

- **`DAGSTER_HOME` must be an absolute path** and must exist. `.dagster/dagster.yaml`
  may be empty but silences a warning on every command.
- **`dagster asset materialize` needs `-f orchestration/definitions.py`** even though
  `workspace.yaml` exists — the workspace file is honoured by `dagster dev`, not by the
  one-shot asset commands.
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
