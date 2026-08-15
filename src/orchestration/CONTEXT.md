# Context — `src/orchestration` (THE pipeline entry point)

> 🗺️ **Project hub: [CLAUDE.md](../../CLAUDE.md)** — the whole project in ~5k tokens
> (verdict, chain, standing rules, routing). Read that first; this file is the depth
> behind one stage.

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
> ## ⚙️ ONE CONFIG FILE: `config.json` (2026-08-05, completed 2026-08-06)
>
> `assets_enabled.json`, `tv_full_refresh.yaml` **and `src/switch_config.json`** are all
> **gone**, folded into [config.json](config.json) with four sections — and
> [enabled.py](enabled.py) is the only thing that reads it:
>
> ```jsonc
> {
>   "assets": {
>     "trading_view": { "enabled": true,  "raw/trading_view_links": true,
>                                         "raw/trading_view_data": false },
>     "bronze":       { "enabled": false, "bronze/trading_view_economy": false, … }
>   },
>   "partitions": { "raw/trading_view": { "economy": true, "forex": false, … } },
>   "parameters": { "trading_view": { "economy": { "vietnam": { "gdp": true, … } } } },
>   "run":        { "skip_existing": false, "max_browsers": 4 }
> }
> ```
>
> ## 🌏 THE ECONOMY CHAIN IS COMPLETE AT 19 COUNTRIES (2026-08-06)
>
> **All four stages, end to end: scrape → bronze → silver → gold.** The scrape had been
> complete since earlier that day; **the three database layers were still holding the
> 5-country universe from 2026-08-01** and nothing said so — see §*…AND THE DATABASE
> LAYERS DID NOT NOTICE FOR A DAY*. Rebuilt: **1,877,742 rows / 3,784 series / 19
> countries**, identical at bronze, silver and gold.
>
> ⚠️ **`gold.economy` is now NINETEEN TABLES, `gold.economy_<country>`** — 3,852 columns
> is over PostgreSQL's 1,600 limit. They share one calendar (6,939 rows each) and keep
> globally unique column names, so joining them on `date` rebuilds the single panel.
> The old 5-country `gold.economy` was **dropped** the same day, after checking that all
> 1,034 of its series appear in the nineteen (they do — 0 missing).
>
> ## 🔚 `src/switch_config.json` IS DELETED (2026-08-06)
>
> **The last config file outside this package is gone**, and with it the last way to run
> something the orchestrator could not see. Its 676 keys split cleanly in two:
>
> | what was in it | where it went |
> |---|---|
> | **654 TradingView parameter keys** — which countries, sectors, brokers, categories | the new **`parameters`** section, as **295 leaves** |
> | 19 `cafef` / `cafef_index` / `simplize` / `gics` run-plan keys | **nothing** — dead since Phase 5 made selection the run plan |
> | 3 `web_scraper/…` run-plan ancestors | **nothing** — `build_trading_view` forces them |
>
> ⚠️ **THE 654 WERE TWO IDENTICAL COPIES.** `…/links/…` and `…/data/…` held the same 326
> keys and differed on exactly ONE value (`crypto`) — which is not a feature, it is the
> drift you get from a tree that has to be edited twice. They are **one tree** now, for
> the same reason the two assets share one `PartitionsDefinition`: the data step reads
> the link CSV its own leaf wrote, so they cannot legitimately disagree.
>
> ⚠️ **The flat path format survives inside `enabled.py`, deliberately.** The fifteen
> `get_enabled_paths` call sites in `trading_view_scraper.py` index positionally —
> `parts[4]` is the country, `parts[6]` the sector — so `trading_view_switches()`
> rebuilds `web_scraper/trading_view/<phase>/…` from the tree and hands it to an ordinary
> `SwitchHandler`. **Not one line of the scraper changed.** Verified by golden test
> against the deleted file: 17 of 19 (phase × asset class) selections **byte-identical**.
>
> ⚠️ **The two that differ are the bug being fixed — see `build_trading_view` below.**
>
> ⚠️ **A leftover `src/switch_config.json` now RAISES**, like the other two superseded
> files. `SwitchHandler` has **no default path** any more: it takes an explicit
> `switches` dict, an explicit file, or neither. "Neither" is what CafeF, Simplize and
> `DataPreprocessor` get — they all require a handler in their constructor and **none of
> them ever calls it**, which an empty handler states honestly.
>
> ⚠️ **ABSENT MEANS OFF, and every module must be LISTED (2026-08-05).** The old default
> was the friendly one — absent = loaded — and it is also the default under which a
> 777-ticker scrape or a 2.4-hour OCR parse joins a run because nobody wrote a line about
> it. Nothing loads now unless the file says `true`. The obvious danger of that — an
> asset missing from the file vanishing from the UI with nothing saying so — is closed by
> the loader **raising** on any asset in the graph with no entry here. Adding an asset
> forces a yes or no.
>
> ⚠️ **THE GROUP GATE IS THE HIERARCHY.** A module loads only if BOTH its group's
> `enabled` and its own value are true, so one line switches off a whole layer while
> every module stays visible — the same rule `switch_config.json` uses, where every
> ancestor must be true. A bare `"cafef": false` is **rejected**: write
> `{"enabled": false, …}` so the modules stay listed rather than disappearing.
>
> ⚠️ **One asymmetry, and it is deliberate.** Absent-inside-a-listed partition owner is
> OFF; an owner mentioned NOWHERE is "no opinion" and keeps all its partitions. Without
> that, `raw/cafef_pdfs`' 100 unlisted tickers would resolve to zero partitions and raise
> at import time for an asset that is switched off anyway.
>
> ⚠️ **The `run` block is the reason this mattered.** It used to live only in a YAML file
> that the CLI read and **the UI never did**, so the same job launched with
> `skip_existing: false` from a terminal and `true` from a button. One file, one answer.
>
> ⚠️ **A leftover `assets_enabled.json` or `tv_full_refresh.yaml` now RAISES** rather than
> being ignored — a stale config that still looks live is this package's recurring
> failure mode, not a hypothetical one.
>
> Handoff notes. **Status (2026-08-05): ✅ PHASE 5 IS DONE — THERE IS NO LONGER A SECOND
> WAY TO RUN ANYTHING.** `src/main.py`, `DataPreprocessor.ingest_{bronze,silver,gold}_data`,
> `_run_layer` and all 41 `data_quality_*` switch keys were **deleted on 2026-08-05**, and
> `src/data_postprocessor/` with them. **75 assets** (19 landing + 20 bronze + 20 silver +
> 10 gold + 5 unified + 1 analysis), every DB table in the four schemas covered. Selection IS the run
> plan, and now it is the only one. Verify anything before acting on it: the code is the
> source of truth.
>
> ## ✅ `src/data_preprocessor` NO LONGER EXISTS (2026-08-05)
>
> It was **moved into this package** — `src/orchestration/preprocessor/` — and the old
> directory deleted. Nothing about the code changed; what changed is that it now lives
> inside the only thing that calls it. That was the whole distance between "orchestration
> is a thin wrapper" and "orchestration is the pipeline": the package had exactly ONE
> real importer (`resources.py`) plus a notebook, so removing it was a RELOCATION, not
> the multi-week rewrite the 6,181-line count suggests.
>
> ```
> src/orchestration/
>   definitions.py  resources.py  _bootstrap.py  enabled.py  config.json
>   assets/         scrape · bronze · silver · gold · unified · selection  ← the 75 assets
>   preprocessor/   preprocessor.py + CONTEXT.md                ← the transform library
> ```
>
> `from orchestration.preprocessor import DataPreprocessor`. Read
> [preprocessor/CONTEXT.md](preprocessor/CONTEXT.md) for how a table is BUILT; add new
> pipeline steps as assets in [assets/](assets/).
>
> ⚠️ **It is still a LIBRARY and still has no entry point.** The move did not turn it
> into an orchestrator, and the assets did not absorb its logic — they still wrap it.
> What is gone is the second *package*, not the separation of concerns, and that
> separation is deliberate: an asset stays two or three lines, and the transform is
> testable without Dagster.
>
> ⚠️ **`src/main.py` IS GONE** (deleted 2026-08-05, code at `f4bc4a2`). So is
> `src/data_postprocessor/` — 652 lines only `main.py` imported, and the call was already
> commented out; its job of joining macro and market columns into one frame is done by
> `gold.economy`, `gold.stock_market` and the unified schema. `switch_config.json` was
> reduced to **TradingView PARAMETERS** here — which countries and sectors to scrape, and
> nothing about what runs — and then **deleted outright on 2026-08-06**, its 295
> parameter leaves moving into `config.json`'s `parameters` section.

## 1. Why this is worth doing

> **Historical, kept because it explains the shape of what is here.** `main.py` was a
> DAG written as `if switch: call()`; it is gone now, but every design decision below
> was taken against it.

`main.py` was already a DAG, written as `if switch: call()`. The evidence is
[preprocessor/preprocessor.py:3495-3525](preprocessor/preprocessor.py#L3495-L3525),
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

**80 assets: 19 landing + 20 bronze + 20 silver + 10 gold + 10 unified + 1 analysis** (was 75
until the four date-broadcast pools were added 2026-08-13). Every scraper
lands to `raw_data/` as an asset ([assets/scrape.py](assets/scrape.py)); every
bronze ingest leaf is an asset ([assets/bronze.py](assets/bronze.py), 20 leaves → 25
tables); silver has twenty ([assets/silver.py](assets/silver.py)), gold ten
([assets/gold.py](assets/gold.py)) and the per-ticker unified schema two
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
                                                               │                        ├─► gold/economy_<country>
                                                               └─► silver/economy_series┘   (wide, as-of, ×19)

raw/trading_view_data[c]       ─► bronze/trading_view_<c>   ─► silver/<c>   ─► gold/<c>
   for c in {bonds, funds, forex}                               (long)          (wide, unfilled)

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
| `silver` | `economy` (long fact), `economy_series` (dimension), `bonds`/`funds`/`forex` (TradingView projections), `stock_market` (4 index tabs), `stocks_basic` (6 sources), `cafef_financials_bank` (quarterly), `stocks_basic_financials_bank` (as-of daily), `…_fa` (+26 indicators) | — |
| `gold` | `economy` (wide, as-of), `bonds`/`funds`/`forex` (wide, unfilled), `stock_market` (wide, unfilled), `stocks` (price panel, no features), `stocks_ta` (+ the ~900-column TA block), `stocks_financials_bank_fa` (feature panel), `news_{weekly,daily}_panel` | — |
| `unified` | `pool__basic`, `pool__targets`, `pool__economy` (19 country tables), `pool__forex` (**48 exchange tables** since 2026-08-14), `pool__funds` + `pool__bonds` + `pool__stock_market` (one spec table), `pool__basic_bank` (pivoted on the fly), `pool__ta`, `pool__fa` | **3** — `VCB` / `BANK` / `ALL`, the universe |

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
scraper's own task adders read from `config.json`'s `parameters`. 295 partitions would
re-encode that tree in a second place and be unusable in the UI.

### ⚠️ `build_trading_view` — how the parameters reach the scraper (2026-08-06)

`is_enabled` requires EVERY ancestor to be true, and the run-plan ancestors
(`web_scraper`, `web_scraper/trading_view`, `web_scraper/trading_view/<phase>`) mean
"run this stage" — which is Dagster's decision now, not a file's. `build_trading_view`
forces exactly those three and takes every leaf below them from `parameters`.

`build_unblocked` survives for **GICS only**: `GicsScraper.scrape` gates itself on
`web_scraper/gics/structure`, so the asset forces that one path. It is force-only now,
with no file underneath.

> **Historical, and it is why the signature changed.** `build_unblocked` used to load
> `switch_config.json` — whose committed state was `"web_scraper": false` plus
> `".../links": false`, so without the forcing a TradingView asset enumerated zero
> countries and scraped nothing, silently. That file is deleted; the forcing that
> remains is three lines, not a workaround for a config fighting the orchestrator.

⚠️ **THE OLD FORM FORCED A FOURTH PREFIX AND THAT WAS A BUG — fixed 2026-08-06.** It also
forced `web_scraper/trading_view/<phase>/<asset_class>`, and for a class with **no
children** that made the class itself a LEAF: `get_enabled_paths` returned a 4-part path
where the adder indexes `parts[4]`, and `_add_options_links_tasks` raised `IndexError`
before queueing anything (partition `options`, 2026-07-31 — reachable only through
Dagster, so `main.py` never hit it). The class prefix is **no longer forced**: a class
the tree does not list enumerates nothing, quietly and correctly. This is the one
behavioural difference the golden test found, and it is the intended one —

| selection | old | new |
|---|---|---|
| `links/options`, `data/options` | 1 path (the bare 4-part artefact) | **0** |
| every other phase × class (17) | — | **byte-identical** |

The guards in the `crypto` and `options` adders stay as defence, but this path can no
longer trip them. `crypto` is still a real leaf (`"crypto": true`) and still returns its
4-part path, so `Skipping incomplete crypto links path` remains the correct guard doing
its job. Both classes legitimately queue **0 tasks**, so those two partitions are
`landed()`-red on an empty folder — neither has ever produced links.

**Verified after the move**, every adder through the asset's own `_tv()` path:

| class | tasks queued | | class | tasks queued |
|---|---|---|---|---|
| stocks | 21 | | indices | 1 |
| funds | 4 | | bonds | 2 |
| futures | 10 | | economy | **209** |
| forex | 47 | | crypto / options | 0 / 0 |

and the data adder for economy: **2,388** at `skip_existing=True`, **3,847** at `False` —
matching the 1,459 files on disk against the 3,847-series universe exactly.

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
| the flat `src/` layout can be imported by Dagster | `dagster definitions validate` | passes, **80 assets** (19 landing + 20 bronze + 20 silver + 10 gold + 10 unified + 1 analysis), all code locations OK — 75 until the four date-broadcast pools landed 2026-08-13 |
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

* `trading_view_links` partitions **`crypto` and `options` queue 0 tasks** (bare leaves
  under `parameters`, no countries beneath them) and neither folder has ever existed, so `landed(require=True)`
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
dagster asset materialize -f src/orchestration/definitions.py --select "group:unified" --partition VCB
```

Or in the UI: select the graph and hit Materialize — Dagster walks the edges itself,
which is the point of having them.

Truncate `logs/app.log` first (`Clear-Content logs\app.log`) — the repo `Logger` still
writes there and it stays the record of what the underlying scrapers/ingests did, per
memory `feedback-clean-app-log-before-run`. Dagster's own per-run logs are separate and
live in `.dagster/`.

### ✅ The economy scrape is COMPLETE — 3,851 series, 19/19 countries (2026-08-06)

Both halves, links and data, verified against `raw_data/` file counts:

| | links (the universe) | data (the series) |
|---|---|---|
| 19 countries, 209 leaves | **3,851** | **3,851 — 100%** |

It took two runs and the gap between them is the lesson. The first (2026-08-05) was
**data only and was interrupted**, leaving 1,459 of 3,847 on disk — 10 countries whole,
Philippines at 46/100, and **8 countries including the USA never started** — while
`landed()` went green because the folder was not empty. Nothing said so; the count had to
be read off the disk. The second run was the `trading_view_full_refresh` JOB with
`skip_existing: true`, which queued exactly **2,388** tasks (3,847 − 1,459) and finished
the layer. Its links pass also picked up 4 more USA symbols, which is where 3,847 → 3,851
came from.

⚠️ **`landed()` cannot see a partial run and never could** — it answers "is this folder
empty?", not "did this run produce everything?". For the record, the check that DOES
answer it is one query per leaf against TradingView's own symbol-search API (§"Two
link-scrape defects"); on 2026-08-06 that put **206 of 209 leaves exactly on the API's
count**, the remaining three being USA `gdp`/`labor`/`money` short by 6 symbols total
out of 3,853 — the tail of the lazy scroll on the three longest lists.

#### ⚠️ …AND THE DATABASE LAYERS DID NOT NOTICE FOR A DAY (found + fixed 2026-08-06)

**"The scrape is complete" was true and meant less than it looks.** On 2026-08-06 the
disk held 3,851 series across 19 countries and **bronze held 1,034 across 5** — the
2026-08-01 state, untouched by either the 19-country expansion or the two silent
link-scrape fixes. Fourteen countries were absent from `bronze.trading_view_economy`
entirely, and the USA sat at **461 of 1,460**, i.e. bronze predated even the
`minimize_window` 50-row truncation fix. Silver and gold inherited all of it.

Nothing was broken and nothing raised. `landed()` was green because the folders are
full; every asset that ran, ran fine; and this file's own tables said *579,459 rows /
1,034 series*, which was accurate — for the table, not for the data on disk. **A scrape
and its ingests are separate assets, so "re-scraped" never implies "re-ingested."**

| | disk | bronze/silver/gold, before | after the rebuild |
|---|---|---|---|
| countries | 19 | **5** | **19** |
| series | 3,851 | **1,034** | **3,784** |
| `silver.economy` rows | — | 579,459 | **1,877,742** |

⚠️ **3,784, not 3,851, and the difference is data not loss.** 105 of the CSVs on disk
are **header-only** — the legitimately-empty leaves this file already documents
(`futures/vietnam/*`, `bonds/vietnam/corporate`, `economy/*/health`), which is why "0
rows" can never be the failure test on its own. Bronze, silver and gold all agree on
3,784 exactly.

⚠️ **The check that would have caught this is one query, and it is not `landed()`:**
compare `COUNT(DISTINCT ticker)` in bronze against the file count in
`raw_data/trading_view/data/<class>/`. Same shape as the per-series max-date check §5
*Gotchas* prescribes for freshness — the folder is never the answer, the table is.

### How it got to 19 COUNTRIES, from 5 (2026-08-05)

Vietnam, USA, China, Japan and South Korea were joined by fourteen chosen for their
direct effect on Vietnam — the FDI sources and supply chain (Taiwan, Singapore, Hong
Kong), the export markets (Euro Area, Germany, Netherlands, United Kingdom), the ASEAN
peer group (Thailand, Malaysia, Indonesia, Philippines) and three other trade partners
(India, Australia, Russia). `switch_config.json` went **347 → 711 keys**; the links adder
queues **209 tasks** (19 countries × 11 categories), verified by running it.

⚠️ **`COUNTRY_CODE.get(country, "")` WAS A SILENT-WRONG-DATA BUG and is now
`country_code()`, which raises.** The code is injected into the screener's localStorage,
and an EMPTY string there is not an error — it is a valid state meaning *no country
filter*. A country present in the `parameters` tree but missing from the map would have
scraped the unfiltered global list into that country's folder, where every row looks like
a successful scrape. That is exactly what the UK bug below did with a code that was
merely WRONG rather than missing.

| | series | | | series |
|---|---|---|---|---|
| Taiwan | 107 | | Thailand | 111 |
| Singapore | 108 | | Malaysia | 113 |
| Hong Kong | 93 | | Indonesia | 114 |
| Euro Area | 126 | | Philippines | 103 |
| Germany | 184 | | India | 127 |
| Netherlands | 151 | | Australia | 183 |
| United Kingdom | 209 | | Russia | 126 |

**3,853 series across the 19** (2,035 for the original five — the USA alone is 1,462),
against the 1,037 files the pre-fix runs had on disk.

### ⚠️ Two link-scrape defects found and fixed on 2026-08-05 — both SILENT

Both were found the same way: comparing what landed against TradingView's own
symbol-search API, which is authoritative and costs no browser —

```
https://symbol-search.tradingview.com/symbol_search/v3/
    ?text=&search_type=economic&country=<CC>&lang=en&domain=production
    [&economic_category=<gdp|lbr|prce|hlth|mny|trd|gov|bsnss|cnsm|hse|txs>]
```

It returns `symbols_remaining`, so a country's or a leaf's true count is one request.
**17 of 19 countries matched their count EXACTLY** (Australia 180/180, Germany 177/177,
Singapore 105/105). The two that did not were both real bugs, and neither raised anything.

**1. `minimize_window()` truncated every leaf to exactly 50 rows.** A minimized window
has no layout, so TradingView renders its first page of 50 and the lazy loader never
fires again — however far the container is scrolled. Only leaves with MORE than 50
symbols were affected, and almost every leaf has fewer, which is why it survived: the USA
was the only country with categories over 50 and it came back at **464 of 1,462**, with
`health` 4/4 and `taxes` 10/10 complete beside `labor` 50/276 and `money` 50/320.

> ⚠️ **The first fix was wrong and is worth recording.** The scroll loop gave up after
> 3 idle iterations x 0.3 s = 0.9 s, which looked like too little patience for a network
> page. Widening it to 4.8 s changed *nothing* — the run still produced exactly 50. Only
> a DOM probe settled it: the container was correct, the scroll worked, and the rows
> simply never arrived because nothing was visible. **Verified: same leaf, minimized 50
> rows, sized window 276.** ⚠️ **Headless is NOT a substitute** — `--headless=new` also
> stops at 50. The window has to be real and sized; `set_window_size(1400, 1000)`.
> The occlusion flags (`--disable-backgrounding-occluded-windows` and friends) are there
> for the same reason: over a run of hours the windows WILL end up behind something.

**2. The United Kingdom scraped the whole world.** `Country.UNITED_KINGDOM` mapped to
`gb`, and the screener does not reject an unknown country code — **it drops the filter**.
So all 11 UK leaves returned a global list capped at 50: `united_kingdom/health` held
Austrian, Australian, Hungarian, Israeli and Polish series. The correct code is **`uk`**,
verified by probe (with `uk` that leaf returns `GBHB`, `GBHOSP` and nothing else).

> ⚠️ **THE SCREENER AND THE SEARCH API DISAGREE, AND ONLY FOR THIS ONE COUNTRY.** The API
> wants `country=GB` (209 series; `UK` returns 0) while the panel wants `uk`. So verifying
> a code against the API proves the country EXISTS, not that the panel accepts it. The
> check that catches both classes of bug is the row count: **a leaf at exactly 50 is
> suspicious, and a country whose total does not match its `COUNTRY_CODE` comment is
> either truncated or unfiltered.**

Verified end to end through `_scrape_economy_links` (the asset's own path), not the probe:

| leaf | before | after |
|---|---|---|
| `usa/labor` | 50 | **275** (API says 276; the collector de-duplicates) |
| `united_kingdom/health` | 50, world | **2, both GB** |
| `united_kingdom/money` | 50, world | **23, GB** |

### The `trading_view_full_refresh` JOB — a button that means "trust nothing on disk"

**The CLI form, which cannot be mis-clicked** — one asset class per command, run them in
whatever order you like:

```powershell
$env:DAGSTER_HOME = "D:\GIT\master-thesis\.dagster"
$cfg = '{\"ops\":{\"raw__trading_view_links\":{\"config\":{\"max_browsers\":4}},\"raw__trading_view_data\":{\"config\":{\"skip_existing\":false,\"max_browsers\":4}}}}'
foreach ($c in "economy","forex","bonds","funds") {
  dagster asset materialize -f src/orchestration/definitions.py `
      --select "raw/trading_view_links,raw/trading_view_data" `
      --partition $c --config-json $cfg
}
```

⚠️ **The CLI has to repeat the config; the JOB does not.** `dagster asset materialize`
builds the implicit asset job and never sees `trading_view_full_refresh`, so it cannot
pick up the `run` block from `config.json` — which is the argument for launching the job
instead. `-c src/orchestration/tv_full_refresh.yaml` used to serve this and is **gone**:
that file was folded into `config.json` on 2026-08-05.

Or in the UI:

```
dagster dev  →  Jobs  →  trading_view_full_refresh  →  Launchpad
             →  pick ONE partition  →  Launch Run
```

⚠️ **`DAGSTER_HOME` MUST BE SET IN THE SHELL THAT STARTS `dagster dev`, and forgetting it
is not a warning you can ignore.** Without it Dagster creates
`.tmp_dagster_home_<random>/` and uses it as the whole instance — so
`.dagster/dagster.yaml` is **not read**, `max_concurrent_runs: 1` is **not applied**, and
a backfill launches every partition AT ONCE. Three such directories accumulated in the
repo root before anyone noticed, one of them still holding runs stuck in `STARTED` from
2026-07-31 because the process that owned them was killed. Run history in a temp home is
also thrown away, which is why the UI can look like it has never run anything.

⚠️ **"Materialize all" / a backfill over a partitioned asset takes EVERY partition** —
`stocks` (777 tickers, ~10 h), `futures`, `indices` and the two that legitimately queue 0
tasks included. It cannot be fixed in the JOB: `define_asset_job(partitions_def=<subset>)`
is rejected — *"Partitioning is inferred from the selected assets"*. It is fixed in
[config.json](config.json)'s `partitions` section instead, one key per sub-source — see
*Turning things off*, level 3.

Two assets (`raw/trading_view_links` then `raw/trading_view_data`, same partition, one
run) carrying this config, so nothing has to be typed into the launchpad:

```yaml
ops:
  raw__trading_view_links: {config: {max_browsers: 4}}
  raw__trading_view_data:  {config: {skip_existing: false, max_browsers: 4}}
```

⚠️ **THE POINT OF THE JOB IS THE CONFIG, NOT THE SELECTION.** Materialising
`raw/trading_view_data` from the asset graph runs it with its DEFAULTS —
`skip_existing=True` — which fetches only symbols absent from disk and leaves every
existing series at whatever date it already had. That default is right for "pick up
what is new" and it has already produced one green, two-hour, mostly-stale forex run
(see the table above). A job carries the override with it, so a full refresh is a
button rather than a YAML snippet somebody has to remember.

⚠️ **THE LINKS ARE IN THE JOB BECAUSE A STALE LINK CSV SILENTLY SHRINKS THE DATA RUN.**
`_add_generic_link_data_tasks` reads only the NEWEST link CSV per leaf. Measured
2026-08-05: **every `economy` leaf's newest file is a header-only casualty of the
2026-07-31 breakage** (`trading_view_links_2026-07-31.csv`, 0 rows) except
`vietnam/gdp` (16) and `japan/business` (32) — the two leaves re-run to verify that
fix. So a data-only refresh of economy would have queued **48 symbols against the 1,037
series on disk** and gone green. Links first, in the same run.

⚠️ **ONE RUN IS ONE ASSET CLASS.** Launch the four partitions as a BACKFILL of this
job; `max_concurrent_runs: 1` makes them queue, which is what keeps the browser cap
honest (§2a).

⚠️ **`skip_existing=False` DOES NOT DELETE THE OLD FILE.** The name carries today's date
(`<SYM>_<start>_<today>.csv`), so a re-fetch lands BESIDE the previous one — forex was
carrying 947 such twins before this run. The bronze ingests glob the whole folder and
dedupe `keep="first"` in **glob order**, which sorts the older date first: **where an old
and a new file disagree on a date's value, the STALE one wins.** "Overwrite" therefore
means clearing the folder, and on 2026-08-05 the four folders were MOVED, not deleted:

```
raw_data/_archive/trading_view_data_{bonds,funds,forex,economy}_2026-08-05/
   18 / 21 / 2,307 / 1,037 CSVs — 795 MB
```

⚠️ **THAT ARCHIVE WAS DELETED ON 2026-08-10 AND THERE IS NO LONGER A RESTORE PATH.** This
paragraph used to read *"move them back if a re-scrape comes back short of what was
there"*, which is exactly what the 2026-07-31 links breakage did to the whole layer. The
folder is gone (803 MB reclaimed), so **if a re-scrape now comes back short, the only way
back is another scrape** — budget ~2 h for forex alone. The decision was taken against a
verified-complete current state: 3,851 series / 19 of 19 countries on disk, with bronze,
silver and gold all agreeing at 3,784.

**The rule the archive existed to serve is unchanged, and matters more now:** clearing a
folder before a `skip_existing=False` re-fetch is still the only way to make "overwrite"
mean overwrite. **Move the folder aside rather than deleting it**, and keep it until the
rebuilt layer has been counted.

### ⚠️ Progress now reaches the log — `Progress [####----] 45.6% 1459/3199` (2026-08-05)

`ThreadManager.execute()` logs one line per finished task:

```
Progress [#########-----------]  45.6%  1459/3199 | ok 1450 fail 9 | 4.5/min
       | elapsed 5h22m | ETA 6h48m | last: data_economy_usa_uslabor
```

⚠️ **Dagster reports NOTHING until a step ends**, and a TradingView step is thousands of
tasks over many hours inside one step — so before this the only way to tell a 7-hour run
from a hung one was counting files in `raw_data/`. Both forms are deliberate: the
percentage answers "are we nearly there", the counts answer "how many series did we
actually get", and they diverge exactly when tasks are failing.

⚠️ It also removed a misleading `wait(futures, timeout=120)`. That timeout cut nothing
short — the `future.result()` after it blocks with no timeout, and the pool is joined by
its `with` block regardless — so a run longer than 120 s simply stopped reporting.
`as_completed` reports as they land.

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

**3. Hard-disable it** — set it `false` under `assets` in [config.json](config.json). No
Python edit. It then vanishes from the UI, from `*`, and from every selection. Reserve
this for "must never load in this repo".

```jsonc
"assets": {
    "// raw/cafef_news": "HEAVY - ~2 h, ~405k rows over the full universe",
    "raw/cafef_news": false
}
```

All 80 assets are listed in the file as a menu, grouped by the asset's own Dagster group
(trading_view, cafef, cafef_index, cafef_filings, simplize, gics, bronze, silver, gold,
unified), with `//` comment keys
marking the expensive ones (same comment convention as `switch_config.json`).
**Absent = OFF and the loader raises on an unlisted asset** — the note that used to sit
here said "`true` or absent = loaded", which was true until 2026-08-05 and is the exact
default the change was made to remove.

### ⚠️ Everything is ON as of 2026-08-06, and that is the intended resting state

**All 80 assets and every partition are `true`.** The file spent 2026-08-05 with one
group and one partition enabled, which is a fine way to keep a specific run honest and a
bad way to leave a repo: the UI showed one asset, and "which module do I want today" had
no answer without a config edit first. Level 1 is the lever — *selection is the run
plan*, and what you do not select does not run.

⚠️ **It re-arms the backfill footgun, so it is worth saying plainly.** With every
partition live, `Materialize all` / `*` / a backfill over a partitioned asset takes
**every** partition: `raw/cafef_pdfs` is 100 tickers at ~1-1.7 GB each,
`raw/trading_view@stocks` is 777 tickers at ~10 h, `raw/cafef_financials` is ~2.4 h per
ticker. **Materialise a group or a named asset, never the whole graph.** If one does run
away, setting the partitions `false` here is still the only lever that stops it *before*
any work starts — it removes them from the `PartitionsDefinition` entirely.

**…and one SUB-SOURCE — the `partitions` section (2026-08-05).** Half the sources here
are not assets: TradingView's nine classes, CafeF's 100 filing tickers and the three
unified universes are PARTITIONS, and "turn off TradingView stocks" had no expression in
this file at all. The only lever was typing the right `--partition` at launch — which is
the lever that failed when a backfill took all 100 `cafef_pdfs` tickers at once.

```jsonc
"partitions": {
    "raw/trading_view": {
        "economy": true,
        "stocks": false          // 777 tickers, ~10 h
    }
}
```

⚠️ **A disabled partition is REMOVED from the `PartitionsDefinition`**, which is stronger
than not selecting it: it cannot be materialised, cannot be backfilled, is not in the UI,
and `--partition stocks` fails with `DagsterUnknownPartitionError` before anything runs.

⚠️ **THE KEY UNDER `partitions` IS THE SET'S OWNER, WHICH IS NOT ALWAYS AN ASSET.**
Where several assets share one `PartitionsDefinition` OBJECT they must share its toggles:
`raw/trading_view_data` cannot offer a class `raw/trading_view_links` does not, because
the data step reads the link CSV that same partition wrote. Those sets are registered
under a group name; sets owned by one asset keep that asset's key.

| owner | partitions | shared by |
|---|---|---|
| `raw/trading_view` | the 9 asset classes | `trading_view_links` + `trading_view_data` |
| `unified` | `VCB` / `ALL` / `BANK` | all four `pool__*` assets |
| `raw/cafef_pdfs` | 100 `<EX>_<TICKER>` | itself |
| `raw/cafef_financials` | `HOSE_VCB`, `HOSE_ACB` | itself |

#### ⚠️ `parameters.data_only` — the ONE place links and data may disagree (2026-08-14)

```jsonc
"parameters": {
    "trading_view": { "forex": { "saxo": true, "oanda": true, /* …all 47… */ } },
    "data_only":    { "forex": [ "saxo", "oanda", /* …10 of them… */ ] }
}
```

`trading_view_switches` emits ONE tree under both phase prefixes, deliberately — two
trees is exactly how `switch_config.json` drifted to a one-value difference nobody
noticed. **That rule still holds for the dangerous direction: data may never enable what
links does not**, because the data adder reads the links CSV its own leaf wrote
(`_add_generic_link_data_tasks` derives `links/<sub_parts>` from the enabled DATA path),
and a leaf with no CSV is a `log_warning` and a silent no-op.

**The safe direction turned out to be a real requirement.** Enumerating forex is cheap —
47 brokers, 5.5 MB, minutes — and fetching it is not: ~50 s per symbol behind a global
8-second navigation gate. Wanting the whole universe LISTED and a subset FETCHED had no
expression at all before this; the only lever was switching a broker off in the shared
tree, which also stopped its links being collected, so **the recorded universe silently
shrank to whatever was being fetched.**

- absent class → no restriction, every leaf its tree enables is fetched
- **empty list → raises.** "Fetch nothing" is what `assets/raw/trading_view_data: false`
  is for; an empty list here reads as an accident
- a leaf not enabled under `parameters/trading_view/<class>` → **raises at
  definition-validation time**, listing the valid ones, before a browser starts
- it filters the FIRST level below the class only. `forex/<broker>` is one level and is
  what this was built for; `stocks/<country>/<type>/<sector>` would filter on
  `<country>`. Deeper selection needs a path prefix, and inventing that before something
  needs it is how the last config reached 676 keys

The loading moved out of `definitions.py` into [enabled.py](enabled.py), and that is not
tidiness: a partition toggle has to be applied where the `PartitionsDefinition` is BUILT
— inside `assets/scrape.py` and `assets/unified.py`, at import time — which is long
before `definitions.py` has an asset list to filter. `definitions.py` keeps the
validation pass, which can only run once every asset module has registered its
partitions.

Behaviour, all verified:

| case | result |
|---|---|
| one key `false` | that asset not loaded; `//` comment keys ignored |
| **a group's `enabled: false`** | **every module under it off**, however each is set |
| **an asset absent from the file** | **raises** — absent is OFF, so silence must not be how it happens |
| **a bare `"cafef": false` instead of a group object** | **raises**, naming what to write instead |
| **a group with no `enabled` key** | **raises** — its absence would read as OFF for the whole group |
| a key matching no asset | **raises**, listing the valid keys |
| **an unknown asset class under `parameters`** | **raises** at definition-validation time, listing the nine |
| **a `parameters` node that is neither bool nor object** | **raises**, quoting the path *as authored* |
| **`data_only` naming a leaf `trading_view` does not enable** | **raises** — data without links is a silent no-op |
| **an empty `data_only` list** | **raises** — "fetch nothing" belongs in `assets`, not here |
| **`data_only` under an unknown asset class** | **raises**, listing the nine |
| **a resurrected `src/switch_config.json`** | **raises** — superseded, like the other two |
| **`"raw/trading_view": {"bnods": false}`** (bad partition) | **raises**, listing that owner's valid partitions |
| **a partition under the wrong owner** | **raises** — "not a partition set" |
| **every partition of one owner `false`** | **raises** — zero partitions is unmaterialisable; disable the ASSET instead |
| **`--partition stocks` when `@stocks` is false** | `DagsterUnknownPartitionError`, before any work |
| malformed JSON | **raises** — never read as "disable everything" |
| file absent | all 80 assets, all partitions — absent means "no opinion", not "all off" |
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

⚠️ **NO SWITCH FILE IS CONSULTED — there isn't one.** Assets call the per-tab /
per-table methods directly, so `--select` is the whole run plan. `config.json`'s
`parameters` narrows what a RUNNING TradingView asset enumerates; it can never veto a
materialisation the way `switch_config.json` could (§3).

**Expected output of C**, which doubles as the acceptance test — the four counts match
`preprocessor/CONTEXT.md` §8 exactly:

```
index_price:        6 files, 24962 rows   →  bronze.cafef_index_price:        24962 rows
index_order_stats:  6 files, 22863 rows   →  bronze.cafef_index_order_stats:  22863 rows
index_foreign:      6 files, 20547 rows   →  bronze.cafef_index_foreign:      20547 rows
index_prop_trading: 6 files,  1494 rows   →  bronze.cafef_index_prop_trading:  1494 rows
RUN_SUCCESS
```

**Sanity check without running anything:** `dagster definitions validate` (it should
report 80 assets and "All code locations passed validation").

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
test for the `symbol` → `ticker` refactor in `src/` (see `preprocessor/CONTEXT.md`
§4-bronze): **22 of 25 tables reproduced their row count EXACTLY.** The three that moved
were stale bronze catching up with raw data the scrapers had already written —
`cafef_news` 5,599 → 405,320 (3 tickers → the full 777), `cafef_order_stats` 351,373 →
2,523,196, `cafef_prop_trading` 64,139 → 73,810 — and each now equals its raw folder
row-for-row.

### SILVER + GOLD — the economy chain (2026-08-01, rebuilt at 19 countries 2026-08-06)

Three assets, and the split between them is the whole point:

```
bronze/trading_view_economy      1,877,742 rows, 3,784 series, 19 countries
   ├─► silver/economy          LONG fact, PK (exchange, ticker, date), 1,877,742 rows, 0 nulls
   └─► silver/economy_series   DIMENSION, PK (exchange, ticker), 3,784 rows + derived frequency
            └──────┬──────────►
   silver/economy ─┴─► gold/economy_<country>   WIDE, 1 row per BUSINESS DAY, ×19
```

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "group:silver,group:gold"
```

| asset | result (2026-08-06) | was (2026-08-01, 5 countries) |
|---|---|---|
| `silver/economy` | **1,877,742 rows / 3,784 series** — exactly the bronze row count, 0 nulls | 579,459 / 1,034 |
| `silver/economy_series` | **3,784 series** | 1,034 |
| `gold/economy_<country>` | **19 tables × 6,939 business days**, 3,784 series total, 73-93% filled | one table, 6,935 × 1,034 |

### ⚠️ `gold.economy` IS NINETEEN TABLES — a hard limit, not a preference (2026-08-06)

The 19-country expansion asked the wide panel for **3,852 columns**. PostgreSQL's
maximum is **1,600**, and at REAL a 3,851-value row is **15,404 bytes** against a
usable width of ~8,160. **Both ceilings, at once** — and the lever `gold.forex` pulled
at 328 series (carry one measure instead of 13) was already spent here, because economy
has only ever carried `value`. So the table splits on `country`, the one key that
divides the series set cleanly.

```
gold_schema.economy_usa               1,438 series   ← 1,439 of 1,600 columns
gold_schema.economy_united_kingdom      182
…17 more…
gold_schema.economy_vietnam              88
                                      ─────
                                      3,784 = silver.economy_series exactly
```

⚠️ **TWO CHOICES KEEP THIS A STORAGE DETAIL RATHER THAN A SHAPE CHANGE**, and without
either one it would be a downgrade:

1. **ONE SHARED BUSINESS-DAY CALENDAR**, computed before the split, so all 19 tables
   have an identical `date` index — **6,939 rows each, verified**. Joining them is an
   inner join on one key, not an outer join across 19 ranges. `gold_economy` **asserts
   this**: nineteen tables that disagree on their calendar cannot be rejoined, and the
   disagreement would be invisible table by table.
2. **THE COLUMN NAMES ARE UNCHANGED and still lead with the country**
   (`vietnam__economy__gdp__economics__vngdpyy`). They stay globally unique, so joining
   all 19 on `date` reproduces exactly the panel this used to be, plus the fourteen
   countries it never had. Dropping the now-redundant country prefix would have saved 9
   characters and made `gdp__economics__usgdp` collide with its Vietnamese namesake the
   first time anyone joined two panels. The longest name measures **57 bytes** against
   the 63-byte identifier limit, so nothing had to be shortened.

⚠️ **THE CEILING IS NOW CHECKED IN CODE, BEFORE THE WRITE.**
`_helper_gold_economy_country_panel` raises if a country needs more than
`PG_MAX_COLUMNS`, naming the country and the remedy (split by `category` next). The
width of this table **is a function of the data** — that is exactly how the single-table
version broke — so the next time it happens it fails named, not as a bare `tables can
have at most 1600 columns` from the driver halfway through a layer build. **The USA is
the one to watch: 1,439 of 1,600.**

✅ **THE PRE-SPLIT `gold.economy` WAS DROPPED (2026-08-06)** — 6,935 × 1,035, ending
2026-07-31, the five-country universe. **Checked before dropping, not assumed:** all
**1,034 of its series appear among the 3,784** in the nineteen panels (0 missing) and
its date range is covered, so it was a strict subset and nothing was lost. A stale table
that still looks live is this package's recurring failure mode, which is why it went
rather than staying as a "just in case".

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

### The carry-ups — 11 assets that closed the table gap (2026-08-05)

The prerequisite for phase 5. Before this, **17 of 65 tables had no asset** and were
reachable only by calling a `DataPreprocessor` method through a `main.py` leaf:

| asset | tables | leaf it replaces |
|---|---|---|
| `silver/cafef_{price,order_stats,foreign,prop_trading,insider_shareholder_transactions}` | 5 | `cafef_carry_ups` |
| `silver/gics`, `silver/indices`, `silver/cafef_news_sentiment` | 3 | one leaf each |
| `silver/cafef_financials` | **3** (the per-report bank statements) | half of `financials` |
| `unified/pool__ta`, `unified/pool__fa` | 2 | **no leaf — notebook only** |

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "silver/cafef_price,silver/cafef_order_stats,silver/cafef_foreign,silver/cafef_prop_trading,silver/cafef_insider_shareholder_transactions,silver/gics,silver/indices,silver/cafef_financials"
dagster asset materialize -f src/orchestration/definitions.py --select "unified/pool__ta,unified/pool__fa" --partition VCB
```

Measured: gics 163, indices 24,095, insider 13,607, prop_trading **64,139 → 73,810**,
foreign 1,772,666, order_stats **351,373 → 2,523,196**, price 2,388,368, financials
3 × 152; `pool__fa` 4,235 × 207 and `pool__ta` 4,235 × 924, both on `pool__basic`'s
calendar with `(date, exchange, ticker)` read back from `pg_index`.

⚠️ **`silver.cafef_order_stats` is why this mattered.** It was 351,373 rows against a
bronze table of 2,523,196 — a **86% shortfall**, stale since the layer-wide re-ingest
grew bronze, and nothing re-ran it because the leaf was something a person had to
remember. That is the failure mode an orchestrator exists to remove, and it was sitting
in the database the whole time.

⚠️ **A carry-up CLEANS as it goes** (drops rows null on the key or null throughout), so
silver ≤ bronze and the row count is a **floor** check, not an equality like the
`bonds`/`funds`/`forex` projections get. What IS hard-asserted is that the table came
back non-empty — a clean pass that eats every row is a failure wearing the costume of a
small table.

⚠️ **THE BRONZE ASSET KEY IS NOT ALWAYS A BRONZE TABLE NAME**, and this asset found out
the expensive way: `bronze/cafef_financials` is ONE asset writing SIX tables, and
`bronze_schema.cafef_financials` does not exist. Reading the dep as a table name is how
`silver/cafef_financials` failed its first run (`UndefinedTable`). The spec table now
carries the dep and the row-count tables as separate fields.

### The BONDS / FUNDS / FOREX chains — three wide panels (2026-08-05)

Three TradingView asset classes got a full chain on the same day, and they are **one
spec table each in silver and gold** rather than six hand-written assets
([silver.py](assets/silver.py) `PROJECTIONS`, [gold.py](assets/gold.py) `WIDE_PANELS`) —
because the ASSERTIONS are the valuable part, and a copy-pasted panel gets the ones
whoever wrote it remembered.

```
raw/trading_view_links[c] ─► raw/trading_view_data[c] ─► bronze/trading_view_<c>
        └─► silver/<c>   LONG, one row per entity-day, a straight projection of bronze
                └─► gold/<c>   WIDE, one row per TRADING DAY, PK date
```

```powershell
# everything, scrape included
dagster asset materialize -f src/orchestration/definitions.py --select "*gold/bonds,*gold/funds,*gold/forex"
# DB layers only, from raw_data/ already on disk
dagster asset materialize -f src/orchestration/definitions.py --select "bronze/trading_view_bonds,silver/bonds,gold/bonds,bronze/trading_view_funds,silver/funds,gold/funds,bronze/trading_view_forex,silver/forex,gold/forex"
```

**Scraped and built end to end on 2026-08-05** — all 9 DB assets green in 3m55s, the
scrape 2h05m before it (bonds 1m32s / funds 4m46s / forex 1h59m, 0 ERROR lines):

| | silver | gold | measures each | round-trip vs silver |
|---|---|---|---|---|
| `bonds` | 66,100 × 4, 18 tenor spellings | **4,642 × 118** | 13 | 33,050 cells, **0 mismatches** |
| `funds` | 18,731 × 8, 21 HOSE ETFs | **2,921 × 390** | up to 19 | 93,655 cells, **0 mismatches**, 0 fund-days absent |
| `forex` | 1,415,390 × 4, **357** series | **7,958 × 358** | **1** | **1,415,390 cells, 0 mismatches** |

All three: rows == distinct dates == silver's distinct dates, PK `date` read back from
`pg_index`. The column algebra closes exactly — bonds 9 collapsed tenors × 13 = 117,
forex 357 × 1 = 357, funds 21 × 19 = 399 **minus 10 never written** because FUEBFVND's
3 rows cannot fill a 5- or 21-day window and an all-NULL column is not created.
`gold.bonds`' other 33,050 silver rows are the twin spellings the builder collapses.

⚠️ **THE SCRAPE REFRESHED ONLY NEW SYMBOLS, AND THE TABLES SHOW IT.** `skip_existing`
globs the symbol prefix at task-ADD time, so a symbol scraped once is never fetched
again. Measured immediately after the run:

| | series ending 2026-08-03/04 | series still ending 2026-06-08/09 or earlier |
|---|---|---|
| `forex` | 29 | **328** |
| `funds` | 2 | **19** |
| `bonds` | 0 | **18** (0 data tasks queued at all) |

So `gold.forex`'s 48 rows after 2026-06-09 and `gold.funds`' 40 carry only the new
symbols and are nearly empty; `gold.bonds` gained no new days whatever. **This is the
documented default doing its job** (it is what removed ~8.6 h of navigation stagger from
a warm run) and the tables are correct — but a green scrape is NOT evidence of fresh
data, which is the standing warning in §5 *Gotchas* made concrete.

**The fix is now one flag:** `TradingViewDataConfig.skip_existing=False`
([assets/scrape.py](assets/scrape.py)) re-fetches every symbol. Budget ~50 s per symbol
(the 8-second global gate dominates), so ~396 symbols across the three classes is
roughly **5.5 hours**.

```yaml
ops:
  raw__trading_view_data:
    config:
      skip_existing: false
```

#### ✅ THE FOREX RE-SCRAPE — all 47 brokers enumerated, 10 fetched (2026-08-14)

Two runs, both green, **0 ERROR lines**:

| phase | select | result |
|---|---|---|
| links | `raw/trading_view_links --partition forex` | 47 tasks, **1h09m**, 47 CSVs |
| data | `raw/trading_view_data --partition forex` | **668 queued, 229 skipped**, **2h15m** |

**Verified symbol-by-symbol afterwards, not taken from the green run** (§5 rule 10):
all **897** symbols the ten brokers' newest links CSVs list have a file, **0 missing, 0
empty**, thinnest series 270 rows, each folder single-exchange with folder name ==
exchange.

| broker | symbols | last date | | broker | symbols | last date |
|---|---|---|---|---|---|---|
| `saxo` | 169 | 2026-08-14 | | `tastyfx` | 81 | 2026-08-13 |
| `b2prime` | 107 | *2026-08-07* | | `fxpro` | 74 | *2026-08-07* |
| `ig` | 97 | 2026-08-13 | | `skilling` | 74 | 2026-08-13 |
| `pepperstone` | 94 | 2026-08-14 | | `activtrades` | 48 | *2026-08-07* |
| `swissquote` | 85 | 2026-08-13 | | `oanda` | 68 | 2026-08-13 |

⚠️ **The three at 2026-08-07 are the 229 `skip_existing=True` skipped** — and they are
exactly the three folders whose filter had worked in the earlier scrape, so their files
were already in the right place. The other seven had their data filed under
`fp_markets/` and `capital_com/`, which the per-folder glob cannot see, so they
refetched in full. **`skip_existing` is a per-FOLDER check, not a per-series one.**

⚠️ **THE SELECTION IS `parameters.data_only`, AND THE 10 ARE A MEASUREMENT.** Ranked by
symbols TradingView lists for each broker whose filter works: saxo 169, b2prime 107, ig
97, pepperstone 94, swissquote 85, tastyfx 81, fxpro 74, skilling 74, activtrades 48,
oanda 68. **Every one of the 19 contaminated brokers is excluded by construction** —
fetching one is 8,500–16,700 symbols of `FX_IDC` that is not that broker's book.

⚠️ **THE LINKS RE-SCRAPE COLLECTED NOTHING NEW, AND A ROW COUNT SAYS OTHERWISE.** Each
broker folder accumulates one dated CSV per run (5 now), and summing rows across them
reads as growth — saxo "269 → 438" — while the newest snapshot holds **169** and the
union of all five holds **169**. Across the ten: newest 897, union 898, the difference
being one symbol in an older snapshot. **Count symbols from the newest CSV; never sum
rows across a leaf's history.**

**State of the class after both runs** (measured 2026-08-14):

| | |
|---|---|
| brokers enumerated | **47 of 47**; 27 clean, 19 contaminated (`FLT-1`), 1 empty |
| symbols fetched, clean | **897 of 1,722** addressable = **52%**, 10 of 27 brokers |
| files on disk | **6,189 / 2.19 GB**, 48% of them duplicate copies |
| **distinct series** | **3,129 across 48 exchanges** — bronze's count, which is authoritative |
| **all of it ingested** | bronze 13,662,058 rows, 2000-01-02 → 2026-08-14 |

⚠️ **THE 2,177 SERIES IN MIS-NAMED FOLDERS INGESTED FINE.** `FLT-1` puts a broker's data
under another broker's folder, but bronze splits the `symbol` column (`SAXO:EURUSD`) on
`:`, so the exchange is always right whatever the folder is called. The folder name is
cosmetic to the database and load-bearing only for `skip_existing`.

📄 **[`reports/forex_exchanges.csv`](../../reports/forex_exchanges.csv)** — the 48
exchanges ranked by ticker count, generated from bronze 2026-08-14:
`rank, exchange, n_tickers, n_rows, first_date, last_date, gold_table, gold_series,
unified_pool`. `gold_series` is a CROSS-CHECK, not a copy — it is read from
`information_schema` for that exchange's gold panel and equals `n_tickers` on all 48.
Regenerate with:

```sql
SELECT exchange, COUNT(DISTINCT ticker) AS n_tickers, COUNT(*) AS n_rows,
       MIN(date) AS first_date, MAX(date) AS last_date
FROM bronze_schema.trading_view_forex GROUP BY exchange
ORDER BY n_tickers DESC, exchange ASC;
```

⚠️ It needed its own `.gitignore` negation (`!reports/*.csv`). The two pairs above it
re-include SUBTREES and neither matches a file sitting loose beside them, so without it
the blanket `*.csv` takes the file and `git add` reports success while committing
nothing — issue `GIT-1` exactly.

⚠️ **3,129 SERIES, NOT THE 3,074 A FILENAME SCAN REPORTS.** Parsing
`<EXCHANGE>_<SYMBOL>_<start>_<end>.csv` on the first underscore splits `FX_IDC_EURUSD`
into exchange `FX` and symbol `IDC_EURUSD`, merging one real exchange into a phantom.
**Count series from bronze, never from filenames.**

#### ✅ THE SPLIT THAT UNBLOCKED IT — `WID-1`, opened and cleared 2026-08-14

`gold.forex` needed **3,130 columns against PostgreSQL's 1,600**. It now splits per
exchange exactly as `gold.economy` splits per country:

| stage | result | time |
|---|---|---|
| `bronze/trading_view_forex` | 357 → **3,129 series**, 1.4 M → **13,662,058 rows**, 21 batches | 27m16s |
| `silver/forex` | 13,662,058 rows, 3,129 series, 2000-01-02 → 2026-08-14 | 14m38s |
| `gold/forex` | **48 panels summing to 3,129 series**, widest `forex_fx_idc` at **648 cols** | 3m56s |
| `unified/pool__forex` (VCB) | **48 pools × 4,266 rows**, 71.3% panel-row coverage | 4.0s |

Verified independently: gold's series total equals silver's exactly, `saxo__eurusd`
round-trips gold↔silver and pool↔gold with **0 mismatches**, 0 unaligned keys against
`pool__basic`, and **the pre-split `gold.forex` and `pool__forex` are both gone**.

⚠️ **THE UN-SUFFIXED TABLES ARE DROPPED LAST, ON SUCCESS ONLY.** Two answers to "what is
gold forex" is the STL-1 shape, and the name with no suffix is the one every
pre-2026-08-14 consumer reads. Dropping only after every panel is written means a failed
rebuild leaves the old table intact. **Anything naming `gold.forex` or `pool__forex` is
naming a dropped table.**

⚠️ **`gold/forex` LEFT `WIDE_PANELS`.** That generic builder asserts one row count, one
column count and one date range — all true of a single table and false of a family.

⚠️ **THESE PANELS DO NOT SHARE A CALENDAR, AND THAT IS NOT ASSERTED — the opposite of
`gold.economy`.** Economy reindexes every country onto one business-day range, so equal
row counts are a real invariant there. Brokers quote what they quote: `forex_saxo` runs
to 2026-08-14 and `forex_fx_idc` to 2026-08-06. Equal row counts here would be a bug.

#### ⚠️ AND THE BLOCK WAS HIDING A SILENT DROP — `SHP-1`

The scraper writes **two file shapes**, and only one was ever ingested: OHLC detection in
`bulk_js` produces either `(date, open, high, low, close, volume)` or `(date, value)` —
**4,402 files against 1,787**. Every clean layer in `_ingest_bronze_forex` filters on
`value`, so the old all-files `pd.concat` produced a `value` column from the OTHER files
and `REMOVE_RECORD_IF_COLUMN_IS_NULL("value")` dropped every OHLC row **without a word**.
**That is the whole reason bronze held 357 series while the disk held 3,129.**

It surfaced only because batching turned the silent drop into a `KeyError` on the first
batch containing no value-shaped file. ⚠️ **The fix is justified by the extraction code,
not by overlap**: no series on disk carries both shapes, so nothing could be compared —
but `bulk_js` pushes `[date, v[1], v[2], v[3], v[4], v[5]]` when a series is OHLC and
`[date, v[4]]` when it is not, i.e. **the same slot 4 of the same array**, labelled
`close` in one branch and `value` in the other.

⚠️ **`bonds`, `funds`, `economy` and `indices` carry the same `value`-only filter and
have never been counted for this.**

⚠️ **THE BRONZE FOREX INGEST READS IN BATCHES OF 300 FILES**, and only forex does. 6,189
files / 2.19 GB / ~29.6 M rows needs 10-15 GB unbatched, against **3.6 GB free**; batched
it held **0.55 GB**. ⚠️ It also inverts which duplicate wins: `keep="first"` in glob
order let the **stale** file win (the trap §"`skip_existing=False` DOES NOT DELETE THE
OLD FILE" documents), while batched upserts in name order — the name ends in the fetch
date — let the **newest** win.

⚠️ **THE MEASURE SET SHRINKS AS THE ENTITY COUNT GROWS, and that is a ceiling talking,
not taste.** PostgreSQL allows 1,600 columns per table. 9 tenors and 19 ETFs carry the
full 13-measure feature block comfortably; forex's **328 series would need 4,264
columns** to do the same. So `gold.forex` carries `value` alone — which is the identical
trade `gold.economy` already makes at 1,034 series, and it is why that table has no
features either. Anything derived is one `_helper_transform` away from `silver.forex`,
which keeps the long grain and every column. ⚠️ **At 3,074 series even `value` alone no
longer fits** — see `WID-1` above.

⚠️ **`gold.forex`'s columns are `{exchange}__{ticker}` with NO measure suffix.** At one
measure, `saxo__eurusd__value` says "value" 328 times.
`_helper_gold_wide_panel(include_measure=False)` **raises if the frame has more than one
measure**, because every measure would otherwise compete for the same column name.

⚠️ **THE 9 FOREX "EXCHANGES" ARE 9 BROKERS, AND MUST NOT BE COLLAPSED.** It is tempting
to treat 99 pairs quoted 328 times the way `_helper_bonds_drop_duplicate_tenors` treats
`VN01`/`VN01Y` — but those twins agree on **100%** of shared dates, and these do not.
Measured 2026-08-05: SAXO vs JFX disagree on **160,781 of 161,816** shared ticker-days
(**99.4%**), SAXO vs SWISSQUOTE 95.6%, B2PRIME vs SWISSQUOTE 99.9%. They are different
feeds snapshotted at different times, so each is a real series and picking one would be
picking a number, not removing a duplicate.

| check | result |
|---|---|
| both materialised | **RUN_SUCCESS, 16 s**; re-run green (gold drops its own table first) |
| `silver/funds` | **18,662 rows × 8 columns**, 19 HOSE ETFs, 2014-10-06 → 2026-06-26 — bronze's row count exactly |
| `gold/funds` | **2,894 trading days × 351 measure columns**, 352,314 observations = 34.7% of cells (pre-scrape; 2,921 × 389 after 2026-08-05) |
| grain | rows == distinct dates == **2,894**, `date` unique, and equal to silver's distinct dates |
| **exact round-trip** | 93,310 carried OHLCV cells vs silver — **0 mismatches**; **0 of 18,662** fund-days absent from the panel |
| a feature, independently | E1VFVN30 `return_simple` vs pandas `pct_change` — **max abs diff 0.0** over 2,893 rows |

⚠️ **The wide panel REPLACED the long one under the same name**, which is the call
`gold.economy` took on 2026-08-01. `gold.funds` was 18,662 × 22 — one row per fund-day
through the generic `_ingest_gold_table("funds")`. Nothing is lost that cannot be
rebuilt: the long table is one line (`self._ingest_gold_table("funds")`) from
`silver.funds`, which is untouched.

⚠️ **Why wide.** A fund panel is read ACROSS funds on one day — FUEVFVND against
E1VFVN30 is the VN-Diamond-versus-VN30 spread, and every rotation or relative-strength
feature is a comparison between two funds on the same date. Long form makes that a
self-join per pair; one row per date makes it a subtraction.

⚠️ **351 columns, not 361, and the shortfall is DATA not a bug.** 19 funds × 19 measures
is 361. The ten absentees are all **FUEBFVND's** rolling and volatility columns, and
FUEBFVND has **3 rows** (2023-08-11 → 2023-08-18) — a 5-day window and a 21-day
volatility cannot produce one non-null value from three observations, so the melt's
`dropna` removes them and the column is never created. Correct (an all-NULL column is
noise), but it means **the column COUNT is a function of the data**: a fund gaining
history gains columns, i.e. a DDL change. That is tolerable in gold, and is precisely
the argument that kept `silver.economy` long.

⚠️ **No as-of fill**, the same call `gold.stock_market` and `gold.bonds` make. A missing
fund-day means that ETF had not listed yet — FUETPVND lists in 2025, eleven years after
E1VFVN30 — and carrying a NAV forward would invent a price. Hence 34.7% filled, which is
listing history rather than a defect.

⚠️ **The features are computed BEFORE the pivot, per fund and in date order**
(`_helper_transform` groups by `(exchange, ticker)`). A return computed after pivoting is
a row-wise difference across the wide frame, which is the same arithmetic only if no fund
has a gap — FUEBFVND has 3 dates against E1VFVN30's 2,894, so it is not.

⚠️ **`_ingest_silver_funds` used to return SILENTLY on an empty bronze table** —
`log_info("No bronze funds data found."); return` — which would have marked the new asset
green over whatever the previous run left in `silver.funds`. It raises
`MissingSourceDataError` now. **Its `bonds` / `forex` / `indices` / `gics` / `cafef_price`
siblings still have the same swallow** (grep `No bronze .* data found`); each is a
one-line fix and belongs with that table's own asset, not here.

> **New helper: `_helper_gold_wide_panel`** — melt → name → check the names → pivot →
> check nothing was lost, shared by any wide panel. `_ingest_gold_stock_market` and
> `_ingest_gold_bonds` still INLINE the same steps because each has a published exact
> round-trip check that would have to be re-run to prove a move was value-preserving;
> that is a separate change, not a side effect of adding funds.

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
"drop the gold table first", which is what `preprocessor/CONTEXT.md` §7 told a
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

**Twenty-eight tables — nine plus the nineteen economy panels** — and every one of them
is something the code can still build. The schema and the pipeline agree, which is the
point of the housekeeping.

| table | shape | built by | state |
|---|---|---|---|
| `economy_<country>` ×19 | 6,939 × (89…1,439) | **asset** | current — wide, as-of filled, 3,784 series (2026-08-06) |
| ~~`economy`~~ | ~~6,935 × 1,035~~ | — | **SUPERSEDED + DROPPED 2026-08-06** — the 5-country panel; every series of it is in the nineteen |
| `stock_market` | 6,339 × 163 | **asset** only | current (wide, unfilled) |
| `stocks` | 2,388,368 × 42 | **asset** + leaf | current — the price panel, no features (2026-08-03) |
| `stocks_ta` | 2,678,167 × 935 | **asset** only | ⚠️ the RENAME of the old `stocks`; the builder is current, the TABLE is not |
| `stocks_financials_bank_fa` | 8,265 × 1,150 | **asset** only | current |
| `news_weekly_panel` | 429,052 × 28 | **asset** only | current |
| `news_daily_panel` | 2,058,604 × 26 | **asset** only | current |
| `bonds` | 4,642 × 118 | **asset** + leaf | current (wide, unfilled) |
| ~~`forex`~~ → `forex_<exchange>` | **48 panels, 3,129 series** | **asset** + leaf | current — SPLIT per exchange 2026-08-14 (`WID-1`); widest `forex_fx_idc` 648 cols. The un-suffixed table is DROPPED |
| `funds` | **2,894 × 352** | **asset** + leaf | current — WIDE, 1 row per trading day (2026-08-05) |
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

### UNIFIED — the fourth layer, PARTITIONED BY UNIVERSE (2026-08-03, partitioned 2026-08-05)

`unified_schema_<ticker>` is not a fourth copy of the pipeline. It is one company's
slice, cut into the **feature groups a model selects over**:

```
silver/stocks_basic ──► unified_vcb/pool__basic     4,266 × 96, PK (date, exchange, ticker)
                                                    ⚠️ 38 silver + 58 drv_* since 2026-08-16
                              └──► unified_vcb/pool__targets   4,235 × 7, PK (date, exchange, ticker)
gold/economy_<country> ──► unified_vcb/pool__economy_<country> ×19, PK (date, exchange, ticker)
                        pool__ta / pool__macro / pool__calendar   ⚠️ NOT ASSETS YET
```

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "group:unified" --partition VCB
```

| check | result |
|---|---|
| `pool__basic` materialised | **RUN_SUCCESS, 611 ms**; re-run green (the asset REPLACES its table) |
| shape | **4,235 rows × 38 columns**, 1 ticker, 2009-06-30 → 2026-06-25 |
| column name + type + ORDER vs `silver.stocks_basic` | **identical, all 38** |
| every value vs silver, 35 non-key columns | **0 mismatches** over 4,235 rows |
| `pool__targets` materialised | **RUN_SUCCESS**, 4,235 rows × 7 columns, PK `(date, exchange, ticker)` |
| `return_5day` vs an independent pandas `close.shift(-5)/close - 1` | **max abs diff 1.5e-16**, null pattern identical |
| label coverage | 4,230 labelled + a **5-row NULL tail**; range -18.8% → +29.1%, mean +0.32% |

⚠️ **The schema is created BY THE ASSET.** `PreprocessorResource.session` already issues
`CREATE SCHEMA` for whatever schema it is handed — the same preamble
`ingest_bronze_data` runs — so naming `unified_schema_vcb` there is what brings it into
existence. `_ingest_unified_pool_basic` creates it too, so the method is still correct
from a notebook or `main.py`.

#### ⚠️ `pool__basic` CARRIES DERIVED FEATURES NOW, AND STOPPED BEING A COPY (2026-08-16)

The table above — *"column name + type + ORDER vs `silver.stocks_basic`: identical, all
38"* — **is history**. `pool__basic` is still `SELECT *` over silver, but now with a
block of **58 trailing derived channels** beside it (63 on a universe partition), all
named `drv_*` and all computed in SQL inside the same `CREATE TABLE AS`.

| partition | rows × columns | silver | derived | cross-sectional | build |
|---|---|---|---|---|---|
| `VCB` | **4,266 × 96** | 38 | 58 | ✗ | 1.2 s |
| `BANK` | **54,528 × 101** | 38 | 63 | ✓ | 1.4 s |
| `ALL` | 2,388,975 × 101 | 38 | 63 | ✓ | see below |

**The contract that survives is the SUBSET one, and it is still asserted:** every silver
column present, silver's type, silver's value. The derived set is asserted as an
**equality** in the asset, in declared order — an extra name there is a CTE helper
(`_o`, `_h`, `_val_vnd`, `_gk_var`, `_m3_63`) that leaked past the explicit select list
and is about to be offered to a selector as a feature.

**The seven blocks**, `DataPreprocessor.UNIFIED_DERIVED_L1` / `_L2` / `_L3` / `_CS`:

| block | n | what | why it is not already in `pool__ta` |
|---|---|---|---|
| A. bar shape | 7 | `range_hl_pct`, `body_pct`, `clv`, the two shadows, **`gap_open_pct`**, `intraday_pct` | 0 hits for "gap" or "clv" across `gold.stocks_ta`'s 935 columns; the gap is the only overnight information a daily bar has |
| B. range volatility | 9 | Parkinson / Garman-Klass / Rogers-Satchell at 5 and 21, `realized_vol_{10,63}`, `downside_vol_21` | none exist anywhere in the repo; they use the whole bar, so they are several times more efficient per observation than close-to-close — which is what matters where `n_eff` is `n_dates/h` |
| C. normalisation | 13 | `close_z_{21,63}`, `close_pos_{21,63,252}`, `dist_from_{high,low}_*`, `volume_z_21`, `volume_pos_63`, `value_z_21`, `ret_skew_63`, `ret_kurt_63` | `pool__ta` has only `close_roll_{mean,max,min,std}_{5,21}` and normalises no volume series at all |
| D. order flow | 10 | `order_{count,vol}_imb` + 5/21 means + z, `log_order_size_ratio`, `avg_order_size`, `order_fill_ratio` | **0 hits for "order" or "imbalance"** in 935 columns — and this is hub §2d's top lever, at daily grain |
| E. foreign / prop | 8 | `foreign_net_value_ratio`, `foreign_participation`, `foreign_flow_ratio_{5,21}`, `foreign_own_chg_{5,21}`, 2 prop | `pool__ta` has 3 thin foreign columns and **no prop at all** |
| F. liquidity | 7 | **`amihud_{21,63}`**, **`vwap_raw`**, `close_vs_vwap`, `negotiated_value_share`, `no_trade_days_21` | 0 hits for amihud / vwap / turnover / illiq |
| G. cross-sectional | 5 | `cs_pct_{ret_1d,turnover,range}`, `cs_ret_demeaned`, `cs_ret_vs_industry` | **universe partitions only** — and per hub §2b the only block anything has ever survived a null in |

##### ⚠️ The five traps this cost, all measured

1. **Silver's `open`/`high`/`low` are RAW; `close_adjust` is not.** `close_raw BETWEEN
   low AND high` holds on **4,266 of 4,266** VCB rows, `close_adjust` on **248**;
   market-wide 2,383,827 against 546,358. `_helper_adjust_ohlc` says the same for gold
   and names the row that proves it (VCB 2009-06-30, `open=high=low=close_raw=60,000`
   against `close_adjust=9,130`). Every expression reads the OHLC set rebuilt with
   `close_adjust/close_raw`. Same-day ratios are factor-invariant either way; **anything
   spanning days is not**, and `gap_open_pct` on raw prices reads every split as a crash.
2. **`value_matched` is in BILLIONS of VND and `foreign_*_value` / `prop_*_val` are
   not.** VCB 2026-08-07: `value_matched` = **392.54** against `volume_matched ×
   close_raw` = **389,375,340,000** — ratio 1.0e9, market-wide median 1.0007e9. The
   first draft divided one by the other and reported a foreign participation ratio of
   **215,150,099**. The 1e9 now lives in exactly one place, `_val_vnd` in the `px` CTE.
3. **bigint / bigint is integer division.** `drv_volume_pos_63` returned a flat **0**
   where the float form gives **0.2038**. The block-wide `::double precision` cast in
   `render()` is applied *after* the division and cannot save it — such an expression
   casts its own numerator.
4. **`STDDEV_SAMP` over a bigint returns `numeric`**, which psycopg2 hands back as
   `Decimal` and pandas carries as dtype `object` — rule 15's degraded-VARCHAR trap
   arriving through the derived half of a table written as a CTAS to avoid exactly that.
   Every finished expression is cast to `double precision`; all 58 columns read back
   `float64`.
5. **A PARTIAL FRAME IS A MISLABELLED CHANNEL, and PostgreSQL hands you one by
   default.** `ROWS BETWEEN 251 PRECEDING AND CURRENT ROW` computes over whatever rows
   exist, so `drv_close_pos_252` was non-NULL from the **second row of every series** —
   a "position in the trailing 252 days" measured over ten days. **188,737 of `ALL`'s
   2,388,975 rows** (7.9%) carried a 252-day channel computed on a shorter window, and
   every series was affected for its own first year. ⚠️ **The pandas cross-check could
   not see this** — `rolling(w)` defaults to `min_periods=w`, so the comparison was
   silently restricted to the region where both were defined. It took a separate
   question ("is this NULL before it can be defined?") to find it. Every level-2
   channel now carries a `COUNT(*) OVER wN = N` guard, and the frame is read back out
   of the SQL with exactly one frame per expression asserted. ⚠️ The guard asserts a
   full **frame** (N rows), not N non-NULL inputs — so the nine channels built on a
   lagged return are defined from row N, computed from N−1 returns, where pandas'
   `min_periods` starts them at N+1. One row per series, deliberate: frame fullness is
   the only property well defined for a multi-column expression like `drv_amihud_63`,
   which reads two.

##### ✅ How it was verified — 20 columns against pandas, and one causality test

The SQL was cross-checked against an independent pandas/numpy recomputation of the same
definitions on VCB, so an error had to be duplicated in numpy to pass:

| | |
|---|---|
| 16 of 20 columns | max **relative** diff ≤ 9.5e-13 |
| `drv_clv`, `drv_gap_open_pct` | flagged on relative error, **absolute** diff 6.2e-14 / 2.2e-16 — the artifact of a denominator crossing zero, not a defect |
| `drv_ret_skew_63`, `drv_ret_kurt_63` | match `scipy.stats.{skew,kurtosis}(bias=True)` to **2.2e-15 / 1.6e-14**. ⚠️ They are the **population** moment estimators, and differ from pandas' sample-corrected `rolling().skew()/.kurt()` by 0.067 / 1.23 — a definition, not a bug |
| **causality** | the whole block rebuilt on data **truncated at 2026-06-15** reproduces all 58 columns on the 4,227 shared rows with **max abs diff exactly 0.0** |

That last row is the one that matters. A feature reaching forward would *change* when
the future is deleted; none of them move. There is also no `FOLLOWING` anywhere in the
generated SQL — grep for it.

##### ⚠️ OUT-1 — one corrupt source row manufactures a finding

Probing the block for forward-looking correlation flagged `drv_prop_net_value_ratio`
and `drv_prop_participation` at **|corr| = 0.266** against the forward 5-day return,
with `corr(net, participation)` **exactly +1.0**. Neither is signal:

`silver.stocks_basic`, **VCB 2026-01-05**, carries `prop_buy_val = 4.001e17` — 400
quadrillion VND — against that day's whole turnover of 2.06e11, on `prop_buy_vol =
697,000` shares at a close of 57,100. **Implied 5.7e11 VND per share, ten million times
the real price.** One cell. Remove it and both numbers collapse.

Market-wide, **77 of 73,044** `prop_buy_val` and **1,182 of 1,240,032**
`foreign_buy_value` rows exceed ten times their own day's turnover (~0.1% each) — so
`foreign_*` carries the same defect. ⚠️ The scale itself is fine — implied price on the
*normal* rows is 87,596 against a `close_raw` of 87,500, and the foreign block matches
too; this is outliers, not units.

✅ **FIXED 2026-08-16 in SILVER, not here** — `_helper_screen_flow_outliers`. Cleaning
belongs to the layer that owns the column; clipping a source value inside a feature
expression is how a data defect stops being visible to every other consumer of it.

⚠️ **It took THREE rules, and the two wrong intermediate versions are the lesson.**

| # | rule | what it alone misses |
|---|---|---|
| 1 | implied price `\|value\|/\|volume\|` off `close_raw` by >100× | rows where value AND volume are corrupt together — `STB 2025-12-30` carries **1.5e13 shares** with a value to match, so its implied price is a plausible 9.9× |
| 2 | flow volume >100× the day's total volume | rows with a huge value and a NULL/zero volume — neither rule works without a volume. `SHB 2025-10-30` slipped through and left `drv_prop_participation` at **57,644** against a p99 of 0.269 |
| 3 | flow value >100× total turnover (VND) | nothing — flow is a SUBSET of trading, so this is true by definition and 100× is enormous slack |

**Thresholds were read off the distribution, not chosen:** 99.5% of flow rows imply a
price within **2×** of `close_raw`, 99.8% within 10×, **99.98% within 100×**. Result:
**611 of 2,388,975 rows (0.0256%)** NULLed — never winsorised, because the corruption
factor is not constant (1e7 on VCB, 1e8 on HPG/TPB) so there is nothing to divide out
and NULL is the only honest encoding. Row count unchanged.

⚠️ **Two intermediate versions each deleted the wrong data, and neither was caught by the
run going green — only by re-measuring.** Without `total_vol > 0` on rule 2, a no-trade
day makes the right-hand side 0 and any flow trips it: **2,818 rows**. Without
`value > 0` on rule 1, a zero value gives ratio 0 and trips the low side: **2,849
pairs**, five times the real defect, mostly ordinary trades the source rounds to 0
(`PVC 2025-11-25` sells **4 shares**). A cleaning step flagging 5× more than the defect
it targets is not being conservative — it is a second, unexamined rule wearing the
first one's justification.

⚠️ **TWO CLASSES REMAIN, REPORTED AND NOT SCREENED**, because for both, which side is
wrong is undetermined: **2,844 pairs hold a real volume with a ZERO value** — and this
class is **MIXED, not rounding**, which an earlier draft of this note got wrong: 362 of
857 `prop_sell` rows imply <100 M VND but **305 imply ≥1 BN**, the largest 1.73e13. VCB
2026-01-05 is one (buy pair NULLed, sell pair survives as `val=0, vol=165,300`). And
**196 rows carry flow volume on a day with no traded volume at all.**

**What it bought, on VCB:**

| | before | after |
|---|---|---|
| `corr(prop_net, prop_participation)` | **+1.000000** | +0.3270 |
| `pearson(prop_net, forward 5d)` | **+0.2658** | +0.0280 |
| `spearman(prop_net, forward 5d)` | +0.0008 | −0.0049 |
| sd of the channel | 65,385 | 0.165 |

##### ⚠️ And a defect in THIS block, found in the same pass

The flow ratios divided by **matched** turnover. Foreign and proprietary desks trade in
the **negotiated** channel too, so a block trade lands in `value_negotiated` while
`value_matched` stays small — `ABB 2026-06-26` has **19.07 bn matched against 392.62 bn
negotiated**, `LPB 2026-06-19` **75 bn against 1,558 bn**. Those rows were inflated ~20×.
`_val_tot_vnd` (matched + negotiated) is now the denominator for every flow ratio, for
`drv_amihud_*` and for `drv_cs_pct_turnover`. ⚠️ `drv_vwap_raw` and `drv_close_vs_vwap`
deliberately keep **matched** — a matched VWAP is exactly what
`value_matched / volume_matched` means.

| channel, BANK panel | matched denominator | total denominator |
|---|---|---|
| `drv_foreign_net_value_ratio` | [−239.6, +75.0], 63 rows outside ±2 | **[−4.87, +2.27]**, 4 rows |
| `drv_prop_net_value_ratio` | [−115,288, +8,023] | **[−84.95, +2.08]**, p1/p99 ∓0.29 |
| `drv_prop_participation` | max 57,644 | **max 42.5**, p99 **0.269** |

⚠️ **The residual tail is REAL, not corruption, and it does not go away.** On `ALL`,
p1/p99 sit inside ±1 for every flow channel while the extremes reach **|85| on
`drv_foreign_net_value_ratio` and 1,469 on `drv_foreign_flow_ratio_21`** — the latter a
21-day sum of flow over a 21-day sum of turnover, on a name that barely traded for a
month. A ratio with a near-zero denominator is a large number, not a wrong one.
Winsorising it is a **modelling** choice belonging to `train_test_creator` (whose
`StandardScaler` is the thing that suffers), not a data fix belonging here — and the
distinction matters, because the same instinct applied one layer lower is what the two
wrong intermediate screens above did.

##### ⚠️ Two things that are deliberately not uniform across partitions

- **The cross-sectional block is universe-only.** On `unified_schema_vcb` there is one
  row per date, so `PERCENT_RANK` is 0.0 and a cross-sectional demean is 0.0, on every
  row forever. `_ingest_unified_pool_basic_bank` excludes the GICS identity columns for
  the same reason — a column known to be constant before it is written costs selection
  budget and buys nothing. This is a considered exception to *"the column set must not
  depend on which partition is being built"*; the asset reports which side it took in
  `cross_sectional_block`.
- **On `BANK`, `drv_cs_ret_vs_industry` IS `drv_cs_ret_demeaned`** — that universe is a
  single GICS industry, so demeaning by date and by (date, industry) are the same
  operation. On `ALL` they differ. Expected, not a fault.

##### Coverage is per channel, not one scalar

Rule 22, one level down. The block spans four source blocks with very different
histories — OHLCV from 2009, order stats from 2010 (96.4% / 97.3%), foreign from 2012
(72.6% / 74.2%), prop from 2023 (**20.7% VCB / 3.1% market-wide**). The asset reports
`derived_min_coverage_pct`, names the three thinnest channels, and lists any that are
**entirely** NULL. An all-empty channel is a WARNING and never a raise: it is the
`pool__fa` coverage decision of 2026-08-05 reached again — raising would make a correct
table unbuildable for a universe whose date range predates a source block.

✅ **`pool__economy` is a Dagster asset (2026-08-07).** It materialises all nineteen
`pool__economy_<country>` tables from the matching causal, as-of-filled
`gold.economy_<country>` panels on `pool__basic`'s trading-day spine. A single table
would recreate the 3,784-column panel gold split to remain under PostgreSQL's 1,600-column
limit. Each table carries the same `(date, exchange, ticker)` primary key and deliberately
does not forward-fill again: publication lag and staleness are gold's contract.

#### ✅ `pool__forex` — the FX block, the SIXTH pool (2026-08-13)

`gold.forex` → `unified_schema_<universe>.pool__forex`. **357 broker-quoted pairs, one
`value` column each**, named `{exchange}__{ticker}` (`saxo__eurusd`, `jfx__usdjpy`) with
no measure suffix — gold carries one measure, so a suffix would say "value" 357 times.

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "unified/pool__forex" --partition VCB
```

**Built for `VCB` 2026-08-13, 852 ms:**

| check | result |
|---|---|
| shape | **4,266 rows × 360 columns**, 1 ticker, 2009-06-30 → 2026-08-07 |
| PK, read back from `pg_index.indkey` | `(date, exchange, ticker)` — order asserted |
| types | **357 `numeric`** + 2 varchar + 1 date — CTAS, no VARCHAR degradation |
| every cell vs `gold.forex` on the same date, all 357 series | **0 mismatches** |
| rows vs `pool__basic` | 4,266 = 4,266, and the symmetric `EXCEPT` is 0 |
| rows carrying ≥1 quote | 4,260 of 4,266 (**99.9%**) — the 6 are spine dates `gold.forex` has no row for at all |

⚠️ **IT IS THE `pool__economy` SHAPE, NOT THE `pool__ta` ONE, and the source's key is
what decides that.** `gold.forex` is keyed on `date` ALONE, so the pool LEFT JOINs on
`date` and **broadcasts** one FX row across every ticker of that day. `pool__ta` /
`pool__fa` come from tables already keyed `(date, exchange, ticker)` and INNER JOIN on
all three. The consequence is a different assertion: a date-broadcast pool must hold
**exactly** the spine's row count, where a per-ticker pool is allowed to cover a subset
(the one-sided check §"Two LIBRARY bugs" relaxed).

⚠️ **ONE table where economy is 19.** 357 series + 3 key columns = 360, comfortably
inside PostgreSQL's 1,600 — and a broker is not a country, so there is no natural key to
split on anyway.

⚠️ **NOT FORWARD-FILLED, and the NULLs carry information.** `gold.forex` is unfilled by
construction: a NULL means that broker did not quote that pair that day, and filling it
would invent a price. Per-series coverage on VCB's spine, measured 2026-08-13:
**min 4.3%, median 67.0%, max 97.5%**; 250 of 357 series above 50%, only 1 above 90%,
**none all-NULL**. The imputation decision belongs to `train_test_creator` (TRAIN-slice
median), not here.

⚠️ **328 OF 357 SERIES STOP AT 2026-06-08/09, AND `MAX(date)` HIDES IT.** That is the
`skip_existing=True` scrape of 2026-08-05 (§"The BONDS / FUNDS / FOREX chains"): 29
series reach 2026-08-04 and the rest did not move. On VCB's spine that is **43 trading
days after 2026-06-09 on which 328 of 357 columns are NULL**, under a table whose
`MAX(date)` reads 2026-08-07. The asset reports `last_date_with_fx_row` beside
`date_range` for exactly this reason. Re-scrape with
`TradingViewDataConfig.skip_existing=False` before leaning on the tail.

⚠️ **THE 9 EXCHANGES ARE 9 BROKERS AND MUST NOT BE COLLAPSED** — 99 distinct pairs
quoted 357 times between them. SAXO vs JFX disagree on **160,781 of 161,816** shared
ticker-days (measured 2026-08-05), so deduplicating by pair name is picking one broker's
book at random. Mean spine coverage by broker: `saxo` 77.6%, `jfx` 75.2%,
`swissquote` 68.8%, `b2prime` 60.7%, `fxtf` 60.2%, `matsui` 59.7%, `easymarkets` 44.0%,
`esafx` 34.5%, `forexcom` 78.8% (8 series).

⚠️ **These are LEVELS**, and an FX level against a forward stock return is the trap
`close_adjust` already documented — a level "predicts" a level at ρ≈0.996. The
representation (`diff` / `zscore` / returns) is the selection step's decision, exactly as
it is for `pool__economy`; this asset copies values as-is.

⚠️ **`ALL` would be 2,388,368 × 360.** The date-broadcast means every ticker-day carries
the same 357 numbers — ~860 M cells of a 357-column daily series repeated 781 times.
Built for `VCB` only so far; `BANK` (53,921 rows) is cheap, `ALL` is not.

#### ✅ `pool__funds` — the VN ETF block, and the pair became a SPEC TABLE (2026-08-13)

`gold.funds` → `unified_schema_<universe>.pool__funds`. **21 HOSE-listed ETFs × up to 19
measures = 389 columns**, named `{exchange}__{ticker}__{measure}`
(`hose__e1vfvn30__close`). ⚠️ **The measure suffix is present here and absent on
`pool__forex`** — that is gold's naming: `gold.funds` carries 19 measures per fund,
`gold.forex` carries one. 21 × 19 = 399 minus **10 never written**, because FUEBFVND has
3 rows and cannot fill a 5- or 21-day window (it lands with 9 columns, not 19 — the
subtraction closes exactly).

**Built for `VCB` 2026-08-13, 1.01 s:**

| check | result |
|---|---|
| shape | **4,266 rows × 392 columns**, 1 ticker, 2009-06-30 → 2026-08-07 |
| PK, read back from `pg_index.indkey` | `(date, exchange, ticker)` |
| types | **389 `numeric`** + 2 varchar + 1 date |
| every cell vs `gold.funds` on the same date, all 389 columns | **0 mismatches** |
| rows vs `pool__basic`, symmetric `EXCEPT` | 4,266 = 4,266, 0 unaligned |
| rows carrying ≥1 value | 2,915 of 4,266 (**68.3%**) |

⚠️ **THE TWO DATE-BROADCAST POOLS ARE ONE SPEC TABLE NOW** (`DATE_SPINE_POOLS` in
`assets/unified.py`, `_helper_unified_pool_on_date_spine` in the library). `pool__forex`
was refactored into it the same day it landed, and the refactor is a **proven no-op**:
the table's content md5 is `5837f3bb…` before and after. Same reason `bronze.py` and
`gold.py` use spec tables — two near-identical bodies drift, and the differences that
matter (the source, and what each ⚠️ carries) belong in a row.

⚠️ **EVERY MEASURE IS TRAILING — verified, not assumed.** `add_returns` is
`pct_change()`, `add_return_volatility` is `log(p/p.shift(1)).rolling(w).std()`, and
`add_rolling_statistics` is a bare `series.rolling(w)` with **no `center=True`**
(`ta/ta_functions.py:2685-2745`). This is checked because a forward-looking measure in a
feature pool is a label wearing a feature's name, and `return_simple` is exactly the
column name that would hide one.

⚠️ **31.7% OF THE POOL IS NULL BY CONSTRUCTION, NOT BY A MISSING SCRAPE.** `gold.funds`
starts **2014-10-06**; the VCB spine starts 2009-06-30, so **1,351 of 4,266 rows have no
fund row at all**. With most VN ETFs listing after 2020 the per-column coverage is far
thinner than `pool__forex`'s: **min 0.05%, median 17.7%, max 67.7%**, none all-NULL. A
selection over this pool is mostly a selection over the last five years.

| fund | rows | cov | first | last |
|---|---|---|---|---|
| `hose__e1vfvn30` | 2,888 | 67.7% | 2014-10-06 | 2026-06-26 |
| `hose__fuessv50` | 2,158 | 50.6% | 2017-10-24 | 2026-06-26 |
| `hose__fuessvfl` | 1,561 | 36.6% | 2020-03-18 | 2026-06-26 |
| `hose__fuevfvnd` | 1,527 | 35.8% | 2020-05-12 | 2026-06-26 |
| … 13 more, 2020-2025 listings | 1,477 → 312 | 34.6% → 7.3% | | 2026-06-26 |
| `hose__fuevn50g`, `hose__fuemitec` | 35, 34 | 0.8% | 2026-06-16 | **2026-08-04** |
| `hose__fuebfvnd` | **3** | 0.1% | 2023-08-11 | 2023-08-18 |

⚠️ **`last_date_with_data` READS 2026-08-04 AND THAT IS TWO FUNDS OUT OF 21.** The same
`skip_existing=True` scrape that froze forex froze this: **19 of 21 funds stop at
2026-06-26**, and the only two reaching August are the two NEW listings the scrape
picked up. Of the 30 spine days after 2026-06-26, **38 of 389 columns carry a value**.
The metadata field is honest about the table and still flattering about the data —
on this pool, read the per-fund last dates above.

⚠️ **E1VFVN30 IS THE VN30 INDEX WEARING A TICKER**, and it is the widest-covered column
here. An ETF's same-day close is the market factor — the very thing
`pool__targets.return_rel_{h}day` SUBTRACTS — so a `close` column against an absolute
forward return is the market predicting the market. Causally clean (nothing is
forward-looking); economically the same "level predicts level" trap `pool__forex`
carries, and `return_simple` / `volatility_*` are the columns to reach for instead.

⚠️ **`gold.funds` is live at 2,921 × 390**, not the `2,894 × 352` the table listing in
§"gold tables" still records (measured 2026-08-13). The 390 is 1 date + 389, which is
the 399-minus-10 above; the older row predates a rebuild.

#### ✅ `pool__bonds` — the yield curve, and the spec table is now THREE (2026-08-13)

`gold.bonds` → `unified_schema_<universe>.pool__bonds`. **9 VN government tenors × 13
measures = 117 columns**, named `{exchange}__{ticker}__{measure}` (`tvc__vn10y__value`,
`tvc__vn02y__volatility_21`). One more row in `DATE_SPINE_POOLS` and one wrapper method
— which is the spec table paying for itself the day after it was written.

**Built for `VCB` 2026-08-13, 672 ms:**

| check | result |
|---|---|
| shape | **4,266 rows × 120 columns**, 1 ticker, 2009-06-30 → 2026-08-07 |
| PK, read back from `pg_index.indkey` | `(date, exchange, ticker)` |
| types | **117 `numeric`** + 2 varchar + 1 date |
| every cell vs `gold.bonds` on the same date, all 117 columns | **0 mismatches** |
| rows vs `pool__basic`, symmetric `EXCEPT` | 4,266 = 4,266, 0 unaligned |
| rows carrying ≥1 value | 3,249 of 4,266 (**76.2%**) |
| coverage per column | min **37.1%**, median **75.9%**, max **76.1%**, none all-NULL |

⚠️ **THE SLOPE IS THE SIGNAL AND IT IS NOT A COLUMN.** A yield CURVE is read ACROSS
tenors on one day — `tvc__vn10y__value − tvc__vn02y__value` is the 10s2s slope, and the
slope is what carries macro information, not any single tenor's level. The wide shape is
what makes that a subtraction instead of a self-join (`gold.bonds`' own reason for
existing), but **nothing computes it**: `FeatureSelector` scores the columns it is given.
Measured on the pool as built, the 10s2s runs **mean +1.204, min −0.828, max +3.618** —
it inverts, so it is a real series and not a constant offset. A consumer wanting it must
derive it.

⚠️ **9 TENORS FROM 18 SPELLINGS, COLLAPSED UPSTREAM.** TradingView exposes `TVC:VN01`
and `TVC:VN01Y` as separate symbols and the scraper collected both — 66,100 silver rows
for 33,050 observations. Gold collapses them having ASSERTED per pair that they agree
(**0 differing values**, 2026-08-05). Nothing to redo here; worth knowing the tenor set
is 9 and not 18 before counting columns.

⚠️ **THE WHOLE SOURCE STOPS 2026-06-08, UNIFORMLY — and that is the cleanest staleness
of the three.** The 2026-08-05 scrape queued **0 bond data tasks**: not "some series
moved" as with forex (29 of 357) and funds (2 of 21), but none at all. So every tenor
ends the same day and the last **44 spine rows are entirely NULL**. There is no
per-series staleness to unpick here — one date compares the whole table.

⚠️ **The 15y/20y/30y tenors begin in 2018**, and `gold.bonds` has no row for **1,017 of
the 4,266 VCB spine dates** (VN traded, TVC did not quote) — that is the 76.1% ceiling,
not a gap in the join. The long end simply does not exist before 2018, so a slope built
across it is a 2018-onward feature whatever its NULL policy.

⚠️ **These are LEVELS, in percent** — same trap as `pool__forex` and `pool__economy`.

#### ✅ `pool__stock_market` — the index panel, and it contains the TARGET'S BENCHMARK (2026-08-13)

`gold.stock_market` → `unified_schema_<universe>.pool__stock_market`. **6 VN indices ×
27 measures = 162 columns**, named `{exchange}__{index}__{measure}`: `hose__vnindex`,
`hose__vn30index`, `hose__vn100_index`, `hnx__hnx_index`, `hnx__hnx30_index`,
`upcom__upcom_index`.

⚠️ **THE PIVOT ALREADY HAPPENED IN GOLD.** `gold.stock_market` is the four CafeF index
tabs (price / order stats / foreign / prop trading) joined and pivoted to **one row per
date, 6,339 × 163** — every index is already its own set of channels. The pool copies
them onto the spine; there is no pivot to redo.

**Built for `VCB` 2026-08-13, 569 ms:**

| check | result |
|---|---|
| shape | **4,266 rows × 165 columns**, 1 ticker, 2009-06-30 → 2026-08-07 |
| PK, read back from `pg_index.indkey` | `(date, exchange, ticker)` |
| types | **162 `numeric`** + 2 varchar + 1 date |
| every cell vs `gold.stock_market`, all 162 columns | **0 mismatches** |
| rows vs `pool__basic`, symmetric `EXCEPT` | 4,266 = 4,266, 0 unaligned |
| rows carrying ≥1 value | 4,260 of 4,266 (**99.9%**) |
| coverage per column | min 0.02%, median **83.1%**, max 99.8%, none all-NULL |

##### ⚠️ `hose__vnindex__close_adjust` IS `UNIFIED_BENCHMARK_COLUMN`

It is the series `pool__targets` subtracts to build the relative target:

```
return_rel_h[t] = return_h[t] − (bm[t+h] / bm[t] − 1)
```

**This pool carries `bm[t]` and its trailing history, NEVER `bm[t+h]`.** Checked, not
assumed, 2026-08-13: the benchmark column matches gold on its own date with **0
mismatches**, and **0 rows** hold a future benchmark value in place of their own. So
there is no leakage — nothing here is dated after the row it sits on.

**What IS true is that the target's own DENOMINATOR is now a feature.** A model fitting
`return_rel_{h}day` with this pool joined can see `bm[t]`, the quantity its label is
divided by. That is legitimate and it is not nothing; quote it beside any result this
pool contributes to.

⚠️ **AND FOR THE ABSOLUTE TARGET THE INDEX CLOSE IS THE MARKET FACTOR ITSELF.** §2's
whole reason for `return_rel_{h}day` existing is that a single stock's absolute forward
return is dominated by the market. Handing a model the index's contemporaneous level is
the level-predicts-level trap in its purest form.

⚠️ **THE ORDER-FLOW MEASURES ARE THE HALF OF THIS POOL THAT IS NOT THAT**, and they are
the most interesting thing in it. `n_buy_orders`, `n_sell_orders`,
`avg_vol_per_{buy,sell}_order`, `buy_order_vol`, `sell_order_vol`,
`foreign_net_{value,volume}`, `prop_{buy,sell}_{val,vol}` are market-wide FLOW rather
than price — **the closest anything already in this database gets to §2d's top-ranked
lever** (aggressor buy/sell imbalance, which properly needs intraday tick). ⚠️ The four
`prop_*` measures cover only **5.8%** of the spine — that CafeF tab starts late — so
they are effectively a recent-years feature.

⚠️ **Coverage per index is the launch date, not a gap**: `vnindex` 84.1%, `hnx_index`
80.7%, `upcom_index` 80.2%, `vn30index` 67.9%, `hnx30_index` 62.0%, `vn100_index`
51.3%.

⚠️ **The source ends 2026-07-30 against a 2026-08-07 spine — 6 NULL rows — and the cause
is DIFFERENT from the TradingView trio.** This is a CafeF chain
(`raw/cafef_index_*` → `bronze.cafef_index_*` → `silver.stock_market` → gold), so it is
refreshed by materialising that chain, not by a `skip_existing=False` TradingView
scrape.

**The date-broadcast family, side by side (VCB, all measured 2026-08-13):**

| pool | source | shape | ≥1 value | col coverage | source ends |
|---|---|---|---|---|---|
| `pool__forex` | `gold.forex`, 357 pairs | 4,266 × 360 | 99.9% | median 67.0% | 29 of 357 at 2026-08-04, **328 at 2026-06-08/09** |
| `pool__funds` | `gold.funds`, 21 ETFs | 4,266 × 392 | 68.3% | median 17.7% | 2 of 21 at 2026-08-04, **19 at 2026-06-26** |
| `pool__bonds` | `gold.bonds`, 9 tenors | 4,266 × 120 | 76.2% | median 75.9% | **all 9 at 2026-06-08** |
| `pool__stock_market` | `gold.stock_market`, 6 indices | 4,266 × 165 | 99.9% | median **83.1%** | 2026-07-30 (CafeF chain, not TV) |

All four: PK `(date, exchange, ticker)` verified from `pg_index`, 0 round-trip
mismatches against gold, 0 unaligned keys, row count equal to the spine's. Adding the
fourth was **one row in `DATE_SPINE_POOLS` plus a wrapper method**.

#### ✅ `pool__basic_bank` — PEER CHANNELS, and the first pool with no table behind it (2026-08-14)

`silver.stocks_basic` filtered to GICS `industry_code = 401010`, **pivoted** to
`{exchange}__{ticker}__{measure}` — one column per bank per measure, one row per date —
and broadcast onto `pool__basic`'s spine. **20 banks × 27 measures = 540 channels.**

⚠️ **THERE IS NO `gold.stocks_bank_wide`, SO THE PIVOT IS BUILT ON THE FLY**: one
`MAX(CASE WHEN exchange = … AND ticker = … THEN <measure> END)` per channel, grouped by
date, handed to `_helper_unified_pool_on_date_spine` as a subquery. The helper grew a
`relation` + `feature_columns` pair for exactly this — a derived panel has no table for
`_helper_column_types` to introspect — and they must be passed together, because a
relation whose columns were guessed is the failure the pair exists to prevent.

⚠️ **`MAX(CASE …)` is only correct because `(date, exchange, ticker)` is
`silver.stocks_basic`'s PRIMARY KEY.** At most one row matches each CASE, so `MAX`
picks a value rather than choosing between two; a source with duplicate keys would
silently publish the larger.

**Built for `VCB` 2026-08-14, 8.0 s:**

| check | result |
|---|---|
| shape | **4,266 rows × 543 columns**, 1 ticker, 2009-06-30 → 2026-08-07 |
| PK, read back from `pg_index.indkey` | `(date, exchange, ticker)` |
| types | 300 `numeric` + **240 `bigint`** + 2 varchar + 1 date — silver's own types |
| every cell vs `silver.stocks_basic`, all 540 channels | **0 mismatches** |
| rows vs `pool__basic`, symmetric `EXCEPT` | 4,266 = 4,266, 0 unaligned |
| rows carrying ≥1 value | **100.0%** |
| coverage per channel | min 0.87%, median 47.5%, max 100.0%, none all-NULL |

##### ⚠️ IT FOUND THAT `pool__basic` IS 12 COLUMNS BEHIND ITS OWN SOURCE

The channel count came out **540, not the 300** predicted from `pool__basic`'s 15
measures — because `silver.stocks_basic` has **38 columns and
`unified_schema_vcb.pool__basic` has 26**. Missing, all of them flow:

```
foreign_buy_value  foreign_buy_volume  foreign_net_value  foreign_net_volume
foreign_own        foreign_room_left   foreign_sell_value foreign_sell_volume
prop_buy_val       prop_buy_vol        prop_sell_val      prop_sell_vol
```

The `pool__basic` asset asserts *"every column of `silver.stocks_basic` is present"* and
would pass on a rebuild — it CTASes `SELECT *`. So the table on disk simply predates
silver gaining those 12, and **`unified/pool__basic --partition VCB` would widen it 26 →
38 without changing a single row.** Nothing downstream is wrong; it is narrower than it
should be. ⚠️ Until that rebuild, `pool__basic_bank` is the ONLY place in
`unified_schema_vcb` carrying VCB's own foreign flow and prop trading —
`hose__vcb__foreign_net_value` exists, `pool__basic.foreign_net_value` does not.

✅ **RESOLVED 2026-08-16.** All three partitions were re-materialised as part of the
derived-feature work below, and all three now carry silver's full 38. The paragraph
above stands as the history of how the gap was found — by a sibling asset counting
channels, not by any check on `pool__basic` itself.

##### ⚠️ THE SCHEMA'S OWN TICKER IS ONE OF THE CHANNELS

`hose__vcb__*` is in the 20. On `unified_schema_vcb` those channels ARE `pool__basic`'s
own columns — **asserted by the asset, not assumed**: 15 mirrored measures (the 15
`pool__basic` still has), **0 mismatches**, and a non-zero count raises, because it
would mean the pivot is reading the wrong row.

It is kept rather than dropped because **the column set of a date-broadcast pool must
not depend on which partition is being built.** Dropping "self" is meaningless on `BANK`
(every row has a different self) and on `ALL`. ⚠️ The consequence is real and belongs to
the consumer: **`pool__basic ⋈ pool__basic_bank` on this schema holds each VCB measure
twice**, and the correlation prune will spend budget rediscovering that unless the
duplicate is excluded up front — the same trap `pool__fa` avoids by excluding the TA
block by name intersection.

##### ⚠️ MEMBERSHIP IS DERIVED, AND IT IS NOT POINT-IN-TIME

The predicate is `UNIFIED_MEMBER_FILTERS[UNIFIED_BANK]` — the same GICS code
`unified_schema_bank` uses — so a bank listing or being reclassified is picked up by a
rebuild rather than by editing a list. **But `silver.stocks_basic` carries today's GICS
code on every historical row and holds no delisted name**, so these 20 channels are the
banks that SURVIVED to 2026, carried back to 2009. A bank that listed in 2018 is NULL
before it; a bank that was delisted is absent entirely. Per-member coverage is that
listing date and nothing else — `stb`/`vcb`/`ctg` 79.3% down to `abb` 26.5%.

⚠️ **`pool__basic_vn30` was DEFERRED on the same reasoning (2026-08-14 decision).**
Its only membership source is repo-root `vn30.csv`, which is today's list with **no
history at all** — strictly worse than a derived GICS predicate. It waits for
point-in-time membership, which is §2d's #4 lever.

⚠️ **`pool__financials` was NOT built — it is `pool__fa`.** `gold.stocks_financials_bank_fa`
is already pooled at 196 columns; a second table over the same statements would be a
duplicate wearing a different name.

⚠️ **The 8 GICS identity columns are excluded**, as they are from `pool__ta` / `pool__fa`:
constant per ticker, so pivoting them would write **160 constant strings** that
`FeatureSelector._prepare` drops after paying to read.

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

#### ✅ THE UNIVERSE IS A PARTITION NOW — 2026-08-05

`unified_schema_all` and `unified_schema_bank` used to exist in the database and
nowhere in the graph: built by calling the methods by hand, so
`--select "group:unified" --partition VCB` rebuilt VCB and left 4.9 M rows stale. The comment on
`UNIFIED_TICKER` said this became a partition "when the rest of the schema moves
here". It has.

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "group:unified" --partition VCB
dagster asset materialize -f src/orchestration/definitions.py --select "group:unified" --partition BANK
dagster asset materialize -f src/orchestration/definitions.py --select "group:unified" --partition ALL
```

| partition | what it is | `pool__basic` |
|---|---|---|
| `VCB` | one company | 4,235 × 38 |
| `BANK` | GICS `industry_code = 401010`, **20 tickers** | 53,921 × 38 |
| `ALL` | every ticker silver holds, **781** | 2,388,368 × 38 |

⚠️ **THE ASSET KEYS CHANGED: `unified_vcb/pool__x` → `unified/pool__x`**, and the group
with them (`unified_vcb` → `unified`). Five assets × three partitions where there were
two assets. Run history under the old keys does not carry over.

⚠️ **`ALL` and `BANK` are SENTINELS, and the library already knew.**
`UNIFIED_MEMBER_FILTERS` maps each to its `silver.stocks_basic` predicate (`None` for
`ALL` — no predicate at all), so membership is DERIVED: `BANK` reads the GICS
classification silver already carries, and a bank listing or being reclassified is
picked up by a rebuild rather than by editing a list. Adding a universe is one entry
in that dict plus one partition key.

⚠️ **The single-company assertions are GATED, not deleted.**
`COUNT(DISTINCT ticker) = 1` is right for `VCB` and flatly wrong for `ALL`, so each such
check asks `_helper_unified_is_universe` first — the change this file previously
flagged as the blocker. Its mirror is also asserted: a SENTINEL resolving to one series
is a failure, because a schema whose name promises a cross-section and whose contents
are one time series is the §9h failure mode arrived at silently.

⚠️ **`pool__targets`' unlabelled tail is PER SERIES.** The old check was `tail == h`,
which is only right on one company; on 781 tickers the correct figure is `h × tickers`.
The two coincide at one ticker, which is exactly why the old check looked right.

#### ⚠️ Two LIBRARY bugs the partitioning exposed, both pre-existing — 2026-08-05

Neither was in the assets. Both had been invisible because the only caller was ever a
single company.

**1. `_ingest_unified_pool_basic` could not build ANY ordinary ticker.**

```python
predicate, params = self._helper_unified_member_filter(ticker)  # "VCB" → ("ticker = %s", ("VCB",))
if predicate and series < 2:            # ← truthy for a real company too
    raise PipelineError("... a sector schema is a CROSS-SECTION ...")
```

The guard exists for SECTOR sentinels, but it tests whether a predicate exists — and an
ordinary ticker has one. So `_ingest_unified_pool_basic("VCB")` raised *"a sector schema
is a CROSS-SECTION"* about a schema that is one company on purpose. Now
`if universe and predicate and series < 2`, which is what its own docstring described in
prose. ⚠️ This means `unified_schema_vcb.pool__basic` had been **unbuildable since the
`BANK` sentinel landed**, and the "RUN_SUCCESS, 611 ms" recorded below predates it.

**2. A feature pool had to match the spine KEY FOR KEY, which no multi-ticker universe
can.** `_helper_unified_pool_from_source` used a symmetric `EXCEPT`. But a feature pool
is as wide as its SOURCE, and `gold.stocks_financials_bank_fa` is built from the CafeF
*bank* chart of accounts — **VCB and ACB alone**. So `pool__fa` disagreed with the spine
on 45,656 keys on `BANK` and **2,380,103** on `ALL`, and `pool__ta` on 6,517 (those are
`gold.stocks_ta` being the stale pre-rewrite table). Demanding equality did not protect
anything; it made the tables unbuildable.

**The check is now one-sided**: every pool row must be a spine row — an orphaned row
cannot be joined to anything and is still a hard error — but a spine row needs no pool
row. **Coverage is REPORTED instead**, by the builder and in each asset's
`spine_coverage_pct` metadata, so a pool that silently shrank stays visible. Consumers
LEFT JOIN a feature pool onto the spine.

⚠️ **The relaxation keeps a check it would otherwise have lost**: on a SINGLE-COMPANY
schema a pool must still cover the whole spine. The subset allowance is about a source
covering fewer TICKERS than the universe — it is not a licence to lose days for a
company the source does have.

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
same source or it will silently lose the difference. **This is why `pool__targets` reads
`pool__basic` and not `gold.stocks`** — the two share one calendar by construction, and
the asset asserts it (a symmetric `EXCEPT` in both directions, not just a row count).

#### ⚠️ EVERY unified table is keyed `(date, exchange, ticker)` — 2026-08-04

`DataPreprocessor.UNIFIED_PRIMARY_KEY` is the contract, and **the ORDER is part of
it**. `_helper_unified_primary_key` applies it and reads the key back from
`pg_index.indkey`; both assets assert it independently and fail the run on drift.

⚠️ **Read from `pg_index`, not `information_schema.key_column_usage`.** The latter
reports position within the CONSTRAINT, which PostgreSQL does not guarantee matches
the index's own column order — and the index order is the thing that decides what a
range scan can use.

Three reasons, and the third is why it was worth changing:

1. **A join needs no special case.** `pool__targets` was keyed on `date` alone, so
   joining it to `pool__basic` meant intersecting key sets and hoping. Every pool now
   joins to every other on the same three columns.
2. **`date` FIRST.** Every access pattern here is time-ordered — walk-forward folds,
   purge gaps, as-of joins — and only a leading `date` lets the PK's index serve a
   range scan. `(exchange, ticker, date)`, the previous order, could not.
3. **The cross-sectional panel.** A multi-ticker pool is keyed this way by necessity.
   Keying the single-ticker pools identically makes that move a wider table rather
   than a different convention, and turns `COUNT(DISTINCT ticker) = 1` from a
   structural assumption into the assertion it always was.

⚠️ `pool__targets` therefore carries `exchange` and `ticker` even though neither
varies in a one-company schema. Verified 2026-08-04: both tables
`PK = (date, exchange, ticker)` read back from the index.

#### `pool__targets` — the key + one column per horizon (2026-08-04: now **5 AND 10**)

⚠️ **The shapes recorded in this section are the 2026-08-04 measurement and the table
is now 9 columns wide, not 7** — `close_adjust_{h}day` was added 2026-08-12 and has its
own subsection at the end of this one. The `7`s and the `4,235`s below are history.

`return_{h}day = close[t+h] / close[t] - 1`, the forward simple return, computed
server-side with `LEAD(close_adjust, h) OVER (ORDER BY date)`. `pool__basic` is one row
per session, so a ROW offset is a trading-day offset and no calendar arithmetic is
involved — which is what `close.shift(-5)` meant in the notebook too.

⚠️ **`UNIFIED_TARGET_HORIZON` (scalar) became `UNIFIED_TARGET_HORIZONS = (5, 10)`
(tuple) on 2026-08-04**, so the table now holds `date`, `return_5day` **and**
`return_10day`. A model comparing horizons needs both labels **on one calendar**, and
deriving the second one anywhere else would put the label definition in two places —
which is the same argument that put the first one here rather than in a notebook.
Adding a horizon adds a column; changing one still renames rather than silently
re-defines.

⚠️ **The two columns have DIFFERENT usable ranges** — `return_5day` 4,230 labelled + a
5-row tail, `return_10day` 4,225 + a 10-row tail. The NULL check is therefore **per
column against that column's own `h`**; a single check against the longest horizon
would have let a genuine hole in `return_5day` through. Anything fitting on both must
drop each target's own tail, not a shared one, or the h=5 run silently loses 5 sessions
it had every right to use.

Verified 2026-08-04: **RUN_SUCCESS, 681 ms** — `return_5day` range -0.1876 → 0.2914,
mean 0.00319; `return_10day` range -0.2742 → 0.3310, mean 0.00646.

⚠️ **`close_adjust`, not `close_raw`.** A return on the raw close reads every split and
stock dividend as a real overnight loss (VCB 2009-06-30: raw 60,000, adjusted 9,130).
That is a corporate action, not a label.

⚠️ **The last 5 rows are NULL and are KEPT.** Their future does not exist yet. Dropping
them here would break the `date` join against the feature pools that the notebook's
`<target>__final` views rely on; drop them when fitting instead. The asset asserts the
tail is **exactly** 5 rows — more would mean NULL or zero closes putting silent holes in
the labels.

⚠️ **Each column name is derived from its horizon** (`UNIFIED_TARGET_HORIZONS = (5, 10)`
→ `return_5day`, `return_10day`), so changing a horizon renames its column rather than
silently re-defining it.

⚠️ **Only one of the notebook's four targets is built here.** `return_rel_5day`,
`direction_5day` and `probability_gain_5pct_5day` are not — and `return_rel_5day` cannot
be, as written: it subtracted the VNINDEX return read from `gold.indices`, which was
**retired and dropped on 2026-08-01**. Its replacement is
`gold.stock_market.hose__vnindex__close_adjust`.

#### `close_adjust_{h}day` — the forward PRICE (added 2026-08-12), so the table is **9** columns

`close_adjust_{h}day = close_adjust[t+h]`, plain `LEAD(close_adjust, h) OVER w` — the
same lead `return_{h}day` divides by `close[t]`, kept in price units for a model asked to
predict a LEVEL rather than a return. Built from the same `horizons` tuple as the two
return families and emitted last, so the column order is now keys, `return_{h}day`×2,
`return_rel_{h}day`×2, `close_adjust_{h}day`×2. The asset's column check is an EQUALITY,
so a family appended anywhere else fails a correct table.

⚠️ **IT IS A LABEL AND IT DOES NOT LOOK LIKE ONE.** `return_5day` announces itself; a
column named `close_adjust_5day` sitting beside `pool__basic`'s `close_adjust` reads like
a price feature. `UnifiedSchemaReader.join` brings the whole label table in, so anything
that treats every non-key column of the joined panel as a candidate feature has been
handed the answer — it would not fail, it would report an IC near 1.
**`feature_selection.run.ALL_TARGETS` and the `OTHER_TARGETS` cell of
`RUN__feature_importance_report.ipynb` both name it, and must stay in step.**

⚠️ **No `NULLIF` on it, deliberately.** `return_{h}day` guards `close[t] = 0` because it
divides by it; a forward level has no denominator, so a zero or negative close comes
through as the number it is. That is visible in the numbers below — and it is the
`close_adjust` defect the return columns were hiding inside a ratio.

Verified 2026-08-12, all three partitions, against an independently computed
`ROW_NUMBER()`-offset self-join of `pool__basic` (`rn = rn + h`):

| schema | rows | `h=5` rows with a future | mismatches | NULL tail | `close_adjust_5day` range |
|---|---|---|---|---|---|
| `unified_schema_vcb` | 4,266 | 4,261 | **0** | 5 = 5×1 | 4,400 → 76,000 |
| `unified_schema_bank` | 54,528 | 54,428 | **0** | 100 = 5×20 | 960 → 76,800 |
| `unified_schema_all` | 2,388,975 | 2,385,070 | **0** | 3,905 = 5×781 | **−10** → 377,040 |

⚠️ **That `−10` is real and it is `VNX`.** Its silver `close_adjust` is NEGATIVE for 968
sessions — a data defect already known (`feature_selection.cross_sectional` excludes
`VNX` by default because it makes `return_5day` reach −781). The forward-price column
puts it in plain sight instead of inside a ratio. Anything fitting on
`close_adjust_{h}day` over `ALL` must exclude `VNX` the same way.

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
| `resource: browser` | 1 | `SCRAPER_MAX_CONCURRENT_BROWSERS` (**4**) is an in-process semaphore — 4 processes is 16 Chrome instances, and 4× the global stagger against TradingView |
| `resource: gpu` | 1 | OCR runs onnxruntime-gpu on a 4 GB RTX 3050; two partitions is VRAM exhaustion |

**So a materialize opens at most 4 Chrome instances**, and only while a TradingView
step runs — nothing else in the repo imports Selenium. The 4 is per PROCESS and the
`browser` tag keeps exactly one browser step running, so the two multiply to 4, not 4×N.

⚠️ **Fixed 2026-07-31: the links phase ignored the semaphore.**
`_scrape_links_attempt` created its driver outside it, so the effective cap there was
the 16-thread pool — 16 browsers (~100-160 `chrome.exe` processes), twice what this
table and `web_scraper/CONTEXT.md` both claimed. It now takes the permit before
`webdriver.Chrome()` and holds it until `quit()`. Verified with a fake driver: 24 tasks
(what the `stocks` links partition queues), 16 workers → **peak 8**.

✅ **The multi-run backfill escape hatch is SHUT (2026-08-05).** `tag_concurrency_limits`
is EXECUTOR config, i.e. per run — backfilling the 9 TradingView partitions the default
way launches 9 runs, and `.dagster/dagster.yaml` used to be empty, so the real figure was
9 × 4 = 36 browsers. That file now carries a `QueuedRunCoordinator` with
**`max_concurrent_runs: 1`**, so runs queue and the browser budget is 4 whatever is
launched. ⚠️ It is a GLOBAL, INSTANCE-level limit: two cheap DB assets no longer overlap
across runs either (they still do inside one run, via the executor's `max_concurrent`).
Raise it only for work with no browser in it.

### The browser budget is ONE number, and it is a parameter

`SCRAPER_MAX_CONCURRENT_BROWSERS` in [utils/constants.py](../utils/constants.py) —
**default 4** — is the whole cap. Three ways to change it, in ascending precedence:

```powershell
# 1. the file            src/utils/constants.py
# 2. the environment
$env:SCRAPER_MAX_CONCURRENT_BROWSERS = "2"
# 3. per run, in the launchpad or a YAML file
ops:
  raw__trading_view_data:
    config:
      max_browsers: 2
```

⚠️ **It sizes the THREAD POOL as well as the semaphore, and that is the fix, not a
detail.** Every TradingView task opens a browser, so the two numbers can only ever
disagree in one of two useless ways: a pool wider than the cap buys threads that block
on a semaphore, and a pool narrower than the cap makes the cap unreachable. The second is
what was actually shipped — `SCRAPER_MAX_CONCURRENT_BROWSERS = 1` in the code,
`SCRAPER_MAX_WORKERS = 2` for the pool, and **8** in this file and in
`web_scraper/CONTEXT.md`. The effective concurrency was 1.

⚠️ **`_browser_slot()` replaced `with self._browser_semaphore:`** and counts live and
peak browsers, so "at most N" is a number in `logs/app.log` and in the asset's
`browsers_peak` metadata rather than a claim about a semaphore. It raises if a driver is
ever created outside it. Verified with a fake driver: 40 tasks → **peak exactly 4**;
with the env var at 2 → **peak exactly 2**.

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
  already exists in `src/`. This is what made the migration incremental rather than a
  rewrite. ⚠️ **The corollary is now load-bearing: `src/data_preprocessor` CANNOT be
  deleted.** Since `main.py` went, this package is the only caller of those methods —
  but it is still only the *caller*. Deleting `data_preprocessor` would leave 75 assets
  wrapping nothing. Making orchestration self-contained means MOVING ~6,200 lines into
  it, not removing a directory.
- **Assets are generated from a spec table**, not copy-pasted — `TABS` in
  [assets/cafef_index.py](assets/cafef_index.py) is four rows and produces eight
  assets. At ~60 assets this is the difference between maintainable and not.
- **Assets call the per-tab method (`scrape_all_index_price`), never `scrape()`.**
  `scrape()` re-consults the switch config, which used to let `switch_config.json`
  silently veto a materialisation the user explicitly asked for. **Selection is
  Dagster's job now**, and since 2026-08-06 there is no file left to veto with.
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
  problem with `pythonpath = src`. The CWD matters just as much: `Logger` writes a
  relative `logs/app.log` and the `*_RAW_DATA_DIR` constants are relative — and a wrong
  CWD fails **quietly**, with a scraper writing its CSVs somewhere else and the asset
  still going green. (The worst case of this is gone: `SwitchHandler` used to default to
  a relative `src/switch_config.json`, where a wrong CWD returned `{}` — every switch
  off. `config.json` resolves from `__file__`.)
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
([preprocessor/preprocessor.py:529-531](preprocessor/preprocessor.py#L529-L531))
— and because a failed `create_table` surfaces immediately as a failed insert anyway.

⚠️ **Expect red on the first wide run.** Turning on a bronze leaf whose scraper has
never run is now a *failure* rather than a silent no-op. That is intended, and the
per-leaf isolation means it costs you one leaf, not the layer.

### 4.2 Phase 1 — the preprocessor (~41 assets, low risk)

> **✅ BRONZE IS DONE (2026-08-01)** — all 20 leaves are assets and have been
> materialised green; see §"Phase 1a" above. What remains of Phase 1 is silver (9 of
> ~15 done) and gold (**10 assets; every gold leaf now has one**). Several gold tables — `stock_market`, `stocks_ta`,
> `stocks_financials_bank_fa`, both news panels — are things `main.py` cannot build at
> all: new gold work lands as an asset and gets no switch leaf, since phase 5 retires
> those anyway.

Pure DB work, fast to iterate, no network. One asset per bronze leaf (20), per silver
ingest (~15 — the multi-ingest leaves like `cafef_carry_ups` and `stocks_financials`
split naturally, which is an improvement), and per gold leaf (6).

Bronze has **no cross-table dependency** — each ingest reads its own `raw_data/`
folder — so those 20 are a flat layer. Silver and gold edges are already documented in
[data_preprocessor/CONTEXT.md](preprocessor/CONTEXT.md) §4 and can be
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
295 partitions would re-encode the parameter tree in a second place and be unusable in
the UI; the sub-leaves stay in `config.json` as *parameters* that each scraper's own task
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

### 4.6 ✅ Phase 5 — retire the second run path (DONE, 2026-08-05)

Every leaf that only ever meant "run this" is deleted; only genuine parameters (the
TradingView country/sector leaves) remain. Two sources of truth is worse than either
alone, and there is now one.

| deleted | was |
|---|---|
| `src/main.py` | the run plan: 8 `scraper.scrape()` calls + the 3 ingest entry points |
| `DataPreprocessor.ingest_{bronze,silver,gold}_data` | hard-coded leaf lists, switch-gated |
| `DataPreprocessor._run_layer` | their shared body — **deliberately did not raise**, so a failed table returned normally |
| 41 `data_quality_*` switch keys (+10 comments) | `switch_config.json` 398 → **347 keys** |
| `src/data_postprocessor/` | 652 lines, imported only by `main.py`, call already commented out |

`preprocessor.py` went 6,389 → 6,181 lines and is now **a library with no entry
point**: 61 `_ingest_*` methods, each called directly by the asset that wraps it.

⚠️ **THE PREREQUISITE WAS CLOSING THE TABLE GAP, and it was not small.** An audit of
what each side could build found **65 tables, 48 with an asset and 17 without** —
9,513,514 rows reachable only by calling a method. Deleting the run path first would
have orphaned every one of them. The 11 assets that closed it are in §"The carry-ups".
The standing example: `silver.cafef_order_stats` sat at **351,373 rows against a bronze
table of 2,523,196**, stale since the layer-wide re-ingest, because "remember to run the
`cafef_carry_ups` leaf" was a person's job. Its new asset took it to 2,523,196.

✅ **`switch_config.json` IS DELETED (2026-08-06)** — the "separate, smaller cleanup"
this section used to defer. Its TradingView parameters are `config.json`'s `parameters`
section (295 leaves, one tree instead of two identical ones); its other 22 keys were
run-plan ancestors this phase had already made dead. `build_unblocked` no longer reads a
file and survives for GICS alone. See §`build_trading_view`.

⚠️ **A THIRD writer survives: `train_test_creator/unified_schema_creator.ipynb`**
imports `DataPreprocessor` directly. It still works — the library is intact — but a
notebook that writes tables is not a run plan, and it is what built the unified pools
that vanished in the 2026-08-03 drop. `pool__ta` and `pool__fa` are assets now; the
`_all` and `_bank` universes are not (see §"unified_schema_all").

### Effort

| phase | scope | estimate |
|---|---|---|
| 0 | exception propagation | ✅ **done** |
| 1 | preprocessor, ~41 assets | bronze ✅ **done**; silver 11/15, gold 10 assets (every leaf covered) |
| 2 | cheap scrapers, ~13 assets | ✅ **done** |
| 3 | TradingView + partitions | ✅ **done** (9 partitions, not 320 — see 4.4) |
| 4 | pdfs/financials, ticker partitions | ✅ **built**, not yet run end-to-end |
| 5 | retire switch config | ✅ **done 2026-08-05** — keys, entry points, `_run_layer`, `main.py` and `data_postprocessor` all deleted |

✅ `main.py` is deleted, and `switch_config.json` — down to 347 parameter keys here —
was itself deleted on 2026-08-06, its parameters folded into `config.json`.

**Where it actually stands (2026-08-05):** phases 0-4 are built; every landing and bronze
asset has been materialised green, silver has 11 assets, gold 10, and a fifth `unified`
layer has 2. What remains is the rest of silver (~4 leaves), the three remaining
`pool__*` groups of the unified schema, phase 5, and end-to-end runs of the four heavy
assets.

✅ **Phase 5 is now true in the code, not just the documentation** (2026-08-05). The
keys, the three entry points and `_run_layer` are deleted; `main.py` and
`src/data_postprocessor/` are deleted. What remains of the original plan is the ~4
silver leaves that were never separate tables and the two unified universes
(`_all`, `_bank`) that still need a partitioned asset.

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
    the CWD that the relative `raw_data/` and `logs/app.log` paths resolve against.
    (`config.json` is resolved from `__file__` and does not care.)
- **Dagster loads `.env` itself** on startup (confirmed: it reported loading
  `POSTGRES_*`), so the `load_dotenv()` calls in the repo are belt-and-braces here.
  Do NOT rely on that for scripts run outside Dagster.
- **The CLI warns that `dagster definitions validate` / `asset materialize` are
  superseded** by `dg check defs` / `dg launch`. Both still work in 1.13; if they are
  removed, the replacements are drop-in.
- **`skip_existing=True` still applies inside the scrapers**, so a scrape asset can
  materialise "successfully" in 500 ms having fetched nothing. That is correct
  behaviour, but it means a green materialisation is NOT evidence of fresh data — the
  row-count metadata is what to read.
  > ⚠️ **Demonstrated 2026-08-05, and it is worse than "fetched nothing": it fetches
  > SOME things.** The forex data partition queued 120 tasks, ran 1h59m, went green —
  > and refreshed 29 series while leaving **328** ending 2026-06-08. A per-series max
  > date is the only honest freshness check; `landed()` and the row count both look
  > healthy. `TradingViewDataConfig.skip_existing=False` forces the full re-fetch. (Cf. the short-page pagination bug in
  `web_scraper/CONTEXT.md §7`: the per-stock CSVs on disk predate the fix and
  `skip_existing` will not refresh them.)
- **The OCR venvs (`ocr_env8`/`ocr_env9`) are irrelevant here** — the production
  financials parse runs in `mt_env` with `CAFEF_OCR_ENGINE=onnx` against the
  `onnxruntime-gpu` already installed there. Only the experiments needed those venvs, and
  they stayed outside Dagster. ⚠️ **Both were DELETED on 2026-08-10** (1.9 GB); nothing in
  `src/` is affected, but re-running experiment 8 or 9 now needs a venv rebuild — the
  recipe is in each experiment's own README.

---

## The `analysis` group — the fifth layer writes no table (2026-08-10)

[assets/selection.py](assets/selection.py), **one asset**:
`analysis/feature_selection_economy`, partitioned by **COUNTRY (19 keys)**. It runs
`feature_selection.run.run_selection` over `pool__basic + pool__economy_<country>` and
archives a folder under `reports/feature_selection/` — ⚠️ **the SINGLE report root since
2026-08-10**, where the config default was `reports/feature_selection_economy` before the
four roots were merged. It shares that root, and its seed 18, with every hand-run
selection, so `final_features` groups a country run WITH a `pool__basic` run unless each
build passes `--scope` (`feature_selection/CONTEXT.md` §15a-after).

⚠️ **It is the only asset in this code location that writes no database table.**
`feature_selection` is read-only by package design (CLAUDE.md §8) and `final_features`
is the one stage that writes, so the "output" this asset is checked against is a run
folder on disk. Nothing about the Dagster graph changes: it still declares its upstreams
(`unified/pool__basic`, `pool__targets`, `pool__economy`) and still fails the run when it
fails.

⚠️ **THE PARTITION KEYS ARE DERIVED, NOT DECLARED.** They come from
`parameters.trading_view.economy` — the same tree that decides which countries get
scraped — so a country enabled for the scrape is a country you can select over, and the
two cannot drift. That is why the partition owner is deliberately absent from the
`partitions` section: listing it would be the second hard-coded copy `enabled.py` exists
to prevent. (`raw/cafef_pdfs` is absent for the related reason: 100 lines of `true`
bury the settings that matter.)

⚠️ **Two guards, both of which fire on real conditions rather than hypotheses.** The
country pool must share `pool__basic`'s calendar — an INNER join makes a stale macro pool
silently truncate the study window, and on 2026-08-10 all 19 VCB economy pools were 31
sessions behind. And the cost estimate must fit `budget_minutes` — the curve is fitted to
this repo's own archived timings and is quadratic in channels, so `usa` at 1,458 channels
is 7.2 h with no null and 6.3 days at the default 20 draws.

**The depth, the config knobs and the exact commands are
[feature_selection/CONTEXT.md §15](../feature_selection/CONTEXT.md).** Do not duplicate
them here; this section exists so the group is discoverable from the orchestration side.
