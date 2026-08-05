# src\orchestration\definitions.py
"""The Dagster code location — what `dagster dev` loads. **56 assets.**

Five layers, kept in separate modules on purpose:

  * `assets/scrape.py`  — THE LANDING LAYER: every scraper, network → `raw_data/`.
                          No database at all, so it is correct-on-disk and re-runnable
                          without one. This is all of `main.py`'s scraping half (19).
  * `assets/bronze.py`  — raw_data/ → `bronze_schema`. ALL 20 ingest leaves (25 tables),
                          generated from a spec table. A flat layer: bronze has no
                          cross-table dependency, so every edge points up at the landing
                          asset whose folder that ingest reads.
  * `assets/silver.py`  — `bronze_schema` → `silver_schema` (8): the canonical long
                          facts and dimensions — the economy fact table and its
                          dimension, the four CafeF index tabs joined into one
                          `stock_market`, the per-stock spine, the bank financials.
  * `assets/gold.py`    — `silver_schema` → `gold_schema` (7): the wide, model-ready
                          panels. Every assumption that makes a panel dense — publication
                          lag, as-of carry, staleness cap — lives HERE and never in
                          silver.
  * `assets/unified.py` — `silver_schema` → `unified_schema_<ticker>` (2): ONE ticker,
                          cut into the feature groups a model selects over. The first
                          layer scoped to a single company rather than the universe.

`src/main.py` is untouched and still runs the whole pipeline the old way.
"""

import os
import sys
from pathlib import Path

from dagster import (
    AssetKey,
    AssetSelection,
    Definitions,
    define_asset_job,
    multiprocess_executor,
)

# ⚠️ THIS PRELUDE IS LOAD-BEARING AND MUST STAY ABOVE THE FIRST `orchestration` IMPORT.
# `dagster ... -f src/orchestration/definitions.py` loads this file as a TOP-LEVEL module
# (named `definitions`, no package context) and puts only the WORKING DIRECTORY — the repo
# root — on `sys.path`. Since this package now lives under `src/`, `import orchestration`
# would fail before `_bootstrap` ever got the chance to add `src/`. Relative imports are
# not an option either: a file loaded by path has no parent package. So the two lines
# `_bootstrap.bootstrap()` would run are repeated here, for this one file.
_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from orchestration._bootstrap import bootstrap

bootstrap()

from utils.constants import SCRAPER_MAX_CONCURRENT_BROWSERS

from orchestration import enabled
from orchestration.assets import bronze, gold, scrape, silver, unified
from orchestration.resources import PreprocessorResource, RepoLogger, SwitchConfig

# ── Which assets are loaded — src/orchestration/config.json ───────────────────
# `false` there means NOT LOADED: gone from the UI, from `*`, and from every selection.
# `true` or ABSENT means loaded, so a newly added asset is on by default.
#
# A key may also name ONE PARTITION — `"partitions": {"raw/trading_view": {"stocks": false}}` — which removes
# it from the PartitionsDefinition itself, so it cannot be materialised, backfilled or
# even seen. That is what makes a sub-source (a TradingView asset class, a CafeF filing
# ticker, a unified universe) switchable without editing Python. See `enabled.py`.
#
# ⚠️ THIS IS NOT A RUN PLAN, and that is the difference from switch_config.json. The
# ordinary way to not run something is to not select it; to skip one inside a bigger
# run use the exclusion syntax:
#
#     --select "group:cafef and not key:\"raw/cafef_news\""
#     --select "* and not group:cafef_filings"
#
# Reach for this file only for "must never load in this repo".
#
# ⚠️ Disabling an asset does NOT disable its downstream. Dagster keeps the dependency
# and shows the removed node as an unexecutable external asset, so `bronze/cafef_index_price`
# still resolves (and still reads the folder from disk) with its `raw/` parent disabled.
# To stop a chain, disable the DOWNSTREAM too.
# ⚠️ THE LOADING LIVES IN `enabled.py`, NOT HERE, AND THAT IS NOT TIDINESS. A partition
# toggle has to be applied where the `PartitionsDefinition` is BUILT — inside
# `assets/scrape.py` and `assets/unified.py`, at import time — which is long before this
# file has an asset list to filter. Both halves therefore read the same module, and this
# file's remaining job is the validation pass, which can only run once every asset module
# has registered its partitions.
def _enabled(asset_defs):
    """Drop anything disabled in the JSON, and fail loudly on a key that matches nothing.

    ⚠️ Validation is against the FULL asset list, before the drop — otherwise every
    legitimately-disabled key would report itself as unknown.
    """
    all_keys = {k.to_user_string() for a in asset_defs for k in a.keys}
    enabled.validate(all_keys)

    disabled = enabled.disabled_assets()
    return [
        a for a in asset_defs if not ({k.to_user_string() for k in a.keys} & disabled)
    ]


# ── Executor ──────────────────────────────────────────────────────────────────
# MULTIPROCESS: assets that do not contend for the same physical resource now run at
# the same time. `DAGSTER_MAX_CONCURRENT` overrides the default of 4.
#
# ⚠️ THE TAG LIMITS ARE NOT TUNING — WITHOUT THEM THIS IS WORSE THAN SEQUENTIAL.
# Two limits exist because two resources are physical and are capped INSIDE a single
# process, so a second process silently doubles them:
#
#   * `browser` (limit 1) — `SCRAPER_MAX_CONCURRENT_BROWSERS` (4) is an in-process
#     semaphore. Four TradingView partitions in four processes is 16 Chrome instances,
#     not 4, on a machine tuned for 4. It also multiplies the global 8-second
#     navigation stagger by four against TradingView, which is the thing the stagger
#     exists to avoid. ⚠️ This tag limit is per RUN; the multi-run escape hatch is shut
#     by `max_concurrent_runs: 1` in `.dagster/dagster.yaml`, not here.
#   * `gpu` (limit 1) — the OCR parse runs onnxruntime-gpu on a 4 GB RTX 3050. Two
#     partitions is VRAM exhaustion, and `sentiment/CONTEXT.md` already records stale
#     GPU processes fragmenting that card.
#
# Everything else is `requests`-bound and safe to overlap: each scraper fans out on its
# own 16-thread pool, so 4 concurrent assets is ~64 in-flight requests — the same
# concurrency the news scraper already runs at (8 tickers x 8 article workers).
#
# ⚠️ `logs/app.log` NOW HAS SEVERAL WRITERS. The repo `Logger` calls
# `logging.basicConfig(filename=...)` on the ROOT logger, so each step process appends
# to the same file; records may interleave out of order. The file is kept (that was the
# explicit requirement) and each line still identifies its class and method — but it is
# no longer a strict chronology. Dagster's own per-step logs, which ARE per-step, are in
# `.dagster/`. If interleaving becomes a problem the fix is a per-process filename in
# `Logger`, not a return to sequential execution.
EXECUTOR = multiprocess_executor.configured(
    {
        "max_concurrent": int(os.getenv("DAGSTER_MAX_CONCURRENT", "4")),
        "tag_concurrency_limits": [
            {"key": "resource", "value": "browser", "limit": 1},
            {"key": "resource", "value": "gpu", "limit": 1},
        ],
    }
)

# ── Jobs — a selection PLUS its run config, so the UI needs no typing ─────────
# ⚠️ THE POINT OF THIS JOB IS THE CONFIG, NOT THE SELECTION. Materialising
# `raw/trading_view_data` from the asset graph runs it with its DEFAULTS —
# `skip_existing=True` — which refreshes only symbols absent from disk and leaves every
# existing series at whatever date it already had. That is the documented trap in
# `CONTEXT.md` §5 and it has already produced a green, two-hour, mostly-stale forex run.
# A job carries `skip_existing=False` with it, so a full refresh is a button rather than
# a YAML snippet somebody has to remember to paste into the launchpad.
#
# Both TradingView assets are in the selection because THE LINKS ARE THE UNIVERSE and the
# data adder reads only the NEWEST link CSV per leaf: if that file is stale — or is one of
# the header-only casualties of the 2026-07-31 breakage, which is exactly what every
# `economy` leaf still holds — the data step queues a fraction of the symbols and reports
# success. Links first, same partition, same run.
#
# ⚠️ ONE RUN IS ONE ASSET CLASS. Launch the four partitions as a BACKFILL and keep
# `max_concurrent_runs: 1` in `.dagster/dagster.yaml`: `max_browsers` is an in-process
# semaphore, so N concurrent runs is 4N Chrome instances (§2a).
TV_REFRESH_ASSETS = AssetSelection.assets(
    AssetKey(["raw", "trading_view_links"]),
    AssetKey(["raw", "trading_view_data"]),
)

# ⚠️ THE VALUES COME FROM config.json, NOT FROM LITERALS HERE. They used to live in
# `tv_full_refresh.yaml`, which only the CLI ever read — so the UI launched this job with
# the assets' own defaults (`skip_existing=True`, i.e. refresh nothing already on disk)
# while the CLI launched it with `false`. Same job, same name, two behaviours.
_RUN = enabled.run_config()
_TV_SKIP_EXISTING = _RUN.get("skip_existing", False)
_TV_MAX_BROWSERS = _RUN.get("max_browsers", SCRAPER_MAX_CONCURRENT_BROWSERS)

_TV_JOB_CONFIG = {
    "ops": {
        "raw__trading_view_links": {"config": {"max_browsers": _TV_MAX_BROWSERS}},
        "raw__trading_view_data": {
            "config": {
                "skip_existing": _TV_SKIP_EXISTING,
                "max_browsers": _TV_MAX_BROWSERS,
            }
        },
    }
}

trading_view_full_refresh = define_asset_job(
    name="trading_view_full_refresh",
    selection=TV_REFRESH_ASSETS,
    description=(
        "Re-scrape ONE TradingView asset class from scratch: links (the universe) then "
        "every symbol's OHLCV, with skip_existing=False so nothing on disk is trusted. "
        "Pick the partition — bonds / funds / forex / economy — and launch; the config "
        "is baked in. Budget ~8-12 s per symbol at 4 browsers (the 8-second global "
        "navigation gate is the floor, not the browser count)."
    ),
    config=_TV_JOB_CONFIG,
)

_repo_logger = RepoLogger()
_switches = SwitchConfig()

defs = Definitions(
    assets=_enabled(
        [
            *scrape.assets,
            *bronze.assets,
            *silver.assets,
            *gold.assets,
            *unified.assets,
        ]
    ),
    jobs=[trading_view_full_refresh],
    resources={
        "repo_logger": _repo_logger,
        "switches": _switches,
        "preprocessor": PreprocessorResource(
            logger=_repo_logger, switches=_switches
        ),
    },
    executor=EXECUTOR,
)
