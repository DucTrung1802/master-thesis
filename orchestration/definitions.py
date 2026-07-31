# orchestration\definitions.py
"""The Dagster code location — what `dagster dev` loads.

Two layers, kept in separate modules on purpose:

  * `assets/scrape.py`  — THE LANDING LAYER: every scraper, network → `raw_data/`.
                          No database. This is all of `main.py`'s scraping half.
  * `assets/bronze.py`  — the first database layer, and only the 4 index tables:
                          the proof that landing connects to PostgreSQL. The other
                          16 bronze tables are Phase 1 (see CONTEXT.md §4.2).

`src/main.py` is untouched and still runs the whole pipeline the old way.
"""

import os

from dagster import Definitions, multiprocess_executor

from orchestration._bootstrap import bootstrap

bootstrap()

from orchestration.assets import bronze, gold, scrape, silver
from orchestration.resources import PreprocessorResource, RepoLogger, SwitchConfig

import json
from pathlib import Path

# ── Which assets are loaded — orchestration/assets_enabled.json ───────────────
# `false` there means NOT LOADED: gone from the UI, from `*`, and from every selection.
# `true` or ABSENT means loaded, so a newly added asset is on by default.
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
ENABLED_CONFIG = Path(__file__).resolve().parent / "assets_enabled.json"


def _load_disabled() -> set[str]:
    """Asset keys explicitly set to `false`.

    ⚠️ FAILS LOUDLY ON A BAD FILE, ON PURPOSE. `SwitchHandler._load_config` swallows a
    read error and returns `{}` — every switch false, `main.py` a complete no-op, one
    ERROR line in a log nobody reads (see `data_preprocessor/CONTEXT.md` §5). The
    equivalent slip here would silently DISABLE THE WHOLE PIPELINE, so a malformed file
    raises instead. An ABSENT file is different and is fine: it means "no opinion", i.e.
    everything enabled.

    Read as `utf-8-sig`, because this file is hand-edited on Windows and PowerShell 5.1's
    `Out-File -Encoding utf8` writes a BOM — the same trap that cost this repo a silent
    no-op run.
    """
    if not ENABLED_CONFIG.exists():
        return set()

    try:
        raw = json.loads(ENABLED_CONFIG.read_text(encoding="utf-8-sig"))
    except Exception as e:
        raise ValueError(
            f"{ENABLED_CONFIG} is not valid JSON: {e}. Refusing to load — an unreadable "
            f"config must never be read as 'disable everything'."
        ) from e

    if not isinstance(raw, dict):
        raise ValueError(f"{ENABLED_CONFIG} must be a JSON object, got {type(raw).__name__}.")

    # Keys beginning `//` are comments, matching switch_config.json's convention.
    return {
        key
        for key, value in raw.items()
        if not key.startswith("//") and value is False
    }


def _enabled(asset_defs):
    """Drop anything disabled in the JSON, and fail loudly on a key that matches nothing."""
    disabled = _load_disabled()
    kept, dropped = [], set()
    for a in asset_defs:
        names = {k.to_user_string() for k in a.keys}
        if names & disabled:
            dropped |= names & disabled
        else:
            kept.append(a)

    unknown = disabled - dropped
    if unknown:
        # A misspelled key would silently disable nothing — the exact failure mode
        # switch_config.json's trailing-slash bug had, and worth a hard error here.
        raise ValueError(
            f"{ENABLED_CONFIG}: {sorted(unknown)} match no asset. Known keys: "
            f"{sorted(k.to_user_string() for a in asset_defs for k in a.keys)}"
        )
    return kept


# ── Executor ──────────────────────────────────────────────────────────────────
# MULTIPROCESS: assets that do not contend for the same physical resource now run at
# the same time. `DAGSTER_MAX_CONCURRENT` overrides the default of 4.
#
# ⚠️ THE TAG LIMITS ARE NOT TUNING — WITHOUT THEM THIS IS WORSE THAN SEQUENTIAL.
# Two limits exist because two resources are physical and are capped INSIDE a single
# process, so a second process silently doubles them:
#
#   * `browser` (limit 1) — `SCRAPER_MAX_CONCURRENT_BROWSERS = 8` is an in-process
#     semaphore. Four TradingView partitions in four processes is 32 Chrome instances,
#     not 8, on a machine tuned for 8. It also multiplies the global 8-second
#     navigation stagger by four against TradingView, which is the thing the stagger
#     exists to avoid.
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

_repo_logger = RepoLogger()
_switches = SwitchConfig()

defs = Definitions(
    assets=_enabled([*scrape.assets, *bronze.assets, *silver.assets, *gold.assets]),
    resources={
        "repo_logger": _repo_logger,
        "switches": _switches,
        "preprocessor": PreprocessorResource(
            logger=_repo_logger, switches=_switches
        ),
    },
    executor=EXECUTOR,
)
