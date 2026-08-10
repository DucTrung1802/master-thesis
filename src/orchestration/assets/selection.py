# src\orchestration\assets\selection.py
"""The ANALYSIS layer — `unified_schema_<universe>` → `reports/feature_selection*/`.

ONE asset, `analysis/feature_selection_economy`, **partitioned by COUNTRY**: it runs
`feature_selection.run.run_selection` on `pool__basic + pool__economy_<country>` and
archives a run folder. It is the first asset in this code location that writes no
database table — `feature_selection` is read-only by package design (CLAUDE.md §8), so
its output is a folder under `reports/` and nothing else.

⚠️ **THIS EXISTS BECAUSE THE 18 ARCHIVED COUNTRY RUNS WERE LAUNCHED BY HAND, ONE AT A
TIME, AND IT SHOWS.** `taiwan_china` was simply never run, every one of the 18 was
launched with `--null-draws 0` so all 18 record `evidence=no_null`, and all 18 read pools
that had since fallen 31 sessions behind `pool__basic` without anything noticing. A
partition set fixes the first, a default fixes the second, and the freshness assertion
below fixes the third.

## The two guards, and why each one is here rather than in a comment

⚠️ **1. THE COUNTRY POOL MUST SHARE `pool__basic`'s CALENDAR.** `UnifiedSchemaReader.join`
is an INNER join on `(date, exchange, ticker)`, so a `pool__economy_<country>` that is
older than `pool__basic` does not fail — it silently truncates the panel to its own last
date and the run reports the smaller row count as if it were the universe. Measured
2026-08-10: `unified_schema_vcb.pool__basic` was re-materialised to 2026-08-07 and all 19
economy pools stayed at 2026-06-25, so any run launched that day would have quietly
studied a panel ending six weeks early. `_assert_same_calendar` raises instead.

⚠️ **2. THE COST IS QUADRATIC IN CHANNELS AND LINEAR IN NULL DRAWS.** From the archive's
own `timings_seconds`, wall clock ≈ `(channels / 207)² × 8.7 min`, which reproduces
`usa` (1,458 ch) at **428 min** and `vietnam` (113 ch) at 3.7 min. `lasso` and
`permutation` are 98% of the `usa` run and neither has a GPU path. A null re-runs the
whole selection per draw, so the estimate is multiplied by `1 + null_draws`:

    country          channels   no null   × 20 draws
    vietnam               113    3.7 min      1.3 h
    united_kingdom        207    8.7 min      3.0 h
    usa                 1,458    428 min    **6.2 days**

`ESTIMATED_MINUTES_PER_207CH` and `budget_minutes` turn that into a raise rather than a
discovery. The default budget of 240 minutes admits 18 countries with a full 20-draw null
and stops `usa`, which is the correct place for a human decision — not a number to raise
until it stops complaining.

## What this asset deliberately does NOT do

* **It does not accept a `cs_` target.** `run_selection` reads `pool__basic ⋈
  pool__targets` only on the cross-sectional path, so an economy pool cannot reach a
  cross-sectional run at all; asking for one here would produce a run that silently
  ignored its own partition key. It raises with that sentence.
* **It does not cap the features.** `max_features` is not offered — the per-run measured
  cut (`selection_cut`) is the only width rule since STL-1, and the flat 12 is gone from
  every live default as of 2026-08-10.
* **It does not write to the database.** `final_features` remains the one stage that
  does, and it reads the `outstanding.csv` this asset leaves behind.
"""

import json
import os
import time
from typing import Callable, List, Optional

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    MaterializeResult,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
)
from pydantic import Field

from orchestration._bootstrap import bootstrap

bootstrap()

from orchestration import enabled
from orchestration.assets.unified import UNIFIED_PARTITIONS, _schema_of
from orchestration.resources import PreprocessorResource

# ── The partition set: the countries, read from the SCRAPE's own config ──────────
#
# ⚠️ NOT A LITERAL LIST AND NOT A DATABASE QUERY. A `PartitionsDefinition` is built at
# import time, long before any connection exists (`unified.py` says the same about its
# universe sentinels) — but a second hard-coded copy of the 19 countries is exactly the
# drift `enabled.py` was written to stop. `parameters.trading_view.economy` is already
# the list that decides which countries get SCRAPED, so a country enabled there and
# absent here would be a country whose data lands and is never selected over.
#
# The keys are lower-case with underscores because that is what the scraper writes to
# `raw_data/trading_view/data/economy/<country>/` and what survives all the way into
# `gold.economy_<country>` and `unified_schema_<u>.pool__economy_<country>`.
def _countries_from_config() -> List[str]:
    tree = enabled.config().get("parameters", {}).get("trading_view", {})
    economy = tree.get("economy", {})
    if not isinstance(economy, dict) or not economy:
        raise ValueError(
            "config.json: parameters/trading_view/economy must be an object naming the "
            "countries. It is what both the economy SCRAPE and "
            "analysis/feature_selection_economy enumerate, so an empty tree here would "
            "mean an asset with zero partitions."
        )
    return sorted(k for k in economy if not k.startswith("//"))


ECONOMY_COUNTRIES = _countries_from_config()

ECONOMY_PARTITIONS = StaticPartitionsDefinition(
    enabled.register("analysis/feature_selection_economy", ECONOMY_COUNTRIES)
)

# ── The cost model, fitted to the archive's own timings ─────────────────────────
#
# `united_kingdom` at 207 channels took 8.7 min; `usa` at 1,458 took 428.4. Solving
# `(1458/207)**k = 428.4/8.7` gives k = 2.00, so the exponent is not a guess — it is what
# two measured runs three orders of magnitude apart actually imply. Re-fit these two
# numbers if the selector's method set changes; `lasso` and `permutation` are 98% of the
# wide-run cost and either one becoming GPU-capable would invalidate the fit entirely.
ESTIMATED_MINUTES_PER_207CH = 8.7
COST_REFERENCE_CHANNELS = 207
COST_EXPONENT = 2.0

# `pool__basic` contributes this many channels on top of the country's macro series. It
# is measured (27 in `unified_schema_vcb`) and only ever feeds the ESTIMATE, so being a
# few out costs nothing; the assertion that matters is the budget, not this.
BASIC_CHANNELS = 27


def estimated_minutes(channels: int, null_draws: int) -> float:
    """Wall clock for one run, from the archive's fitted `channels² × draws` curve."""
    base = ESTIMATED_MINUTES_PER_207CH * (
        max(channels, 1) / COST_REFERENCE_CHANNELS
    ) ** COST_EXPONENT
    return base * (1 + max(null_draws, 0))


class EconomySelectionConfig(Config):
    """The knobs, with the DEFAULTS being the argument this asset exists to make.

    ⚠️ `null_draws` defaults to 20 and not to 0. Every one of the 18 hand-launched
    country runs used 0 and every one of them records `evidence=no_null` — an unknown,
    never an implied pass (CLAUDE.md §5 rule 2). A selection with no bar is the cheap
    output, so it must be the one you have to ask for.
    """

    universe: str = Field(
        default="VCB",
        description="unified_schema_<universe>. VCB is the single-name study.",
    )
    target: str = Field(
        default="return_5day",
        description="a cs_ target is REJECTED: the cross-sectional path reads "
        "pool__basic only and cannot see an economy pool.",
    )
    lookback: int = Field(default=20, description="d, in sessions")
    horizon: int = Field(default=5, description="h; must match the target")
    normalize: str = Field(default="none", description="none | zscore | window_relative")
    corr_threshold: float = Field(default=0.9)
    n_splits: int = Field(default=5)
    min_train: int = Field(default=500)
    device: str = Field(default="cpu", description="part of the setup; cpu | cuda | auto")
    random_state: int = Field(default=42)
    stability: bool = Field(default=True, description="per-fold SHAP ranking; cheap")
    null_draws: int = Field(
        default=20,
        description="block-shuffled draws. 0 records evidence=no_null, which is an "
        "UNKNOWN and not a pass.",
    )
    holdout_start: Optional[str] = Field(default=None, description="e.g. 2024-06-01")
    root: Optional[str] = Field(
        default=None,
        description="report root. None = reports/feature_selection. ⚠️ A root is a "
        "GROUP for final_features, which keys on (schema, target, setup) with no term "
        "for which pools — see pipeline/CONTEXT.md §5c.",
    )
    budget_minutes: float = Field(
        default=240.0,
        description="raise if the ESTIMATE exceeds this. Raise it deliberately, per "
        "run, for usa.",
    )
    notes: str = Field(default="")


def _assert_same_calendar(cur, schema: str, table: str) -> tuple:
    """`pool__economy_<country>` and `pool__basic` must end on the same date.

    ⚠️ Raises rather than warns, and the reason is that the failure is INVISIBLE
    downstream: the reader inner-joins, so a stale macro pool produces a shorter panel
    and a perfectly ordinary-looking report about it. There is no column, row count or
    metric in the archived run that says "this studied six fewer weeks than it could
    have" — the only place it can be caught is before the join.
    """
    cur.execute(f'SELECT COUNT(*), MIN(date), MAX(date) FROM {schema}."{table}"')
    econ_rows, econ_min, econ_max = cur.fetchone()
    cur.execute(f"SELECT COUNT(*), MIN(date), MAX(date) FROM {schema}.pool__basic")
    basic_rows, basic_min, basic_max = cur.fetchone()

    if econ_max != basic_max or econ_min != basic_min:
        raise ValueError(
            f"{schema}.{table} covers {econ_min}..{econ_max} ({econ_rows:,} rows) but "
            f"{schema}.pool__basic covers {basic_min}..{basic_max} ({basic_rows:,}). "
            f"The panel reader INNER-joins, so this run would silently study the "
            f"narrower window and report it as the universe. Re-materialise "
            f"unified/pool__economy on partition {schema.rsplit('_', 1)[-1].upper()} "
            f"first — every one of its 19 country siblings moves together."
        )
    return int(basic_rows), basic_min, basic_max


def _channel_estimate(cur, schema: str, table: str) -> int:
    """Roughly how many channels the joined panel will carry, for the cost guard."""
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    macro_columns = int(cur.fetchone()[0])
    # minus (date, exchange, ticker); plus pool__basic's own channels.
    return max(macro_columns - 3, 0) + BASIC_CHANNELS


@asset(
    name="feature_selection_economy",
    key_prefix=["analysis"],
    group_name="analysis",
    compute_kind="python",
    partitions_def=ECONOMY_PARTITIONS,
    deps=[
        AssetKey(["unified", "pool__basic"]),
        AssetKey(["unified", "pool__targets"]),
        AssetKey(["unified", "pool__economy"]),
    ],
    description=(
        "unified_schema_<universe>.pool__basic + pool__economy_<country> → one "
        "feature-selection run folder under reports/. Partitioned by COUNTRY. "
        "Writes NO database table — feature_selection is read-only by package design; "
        "final_features is the stage that writes. Defaults to a 20-draw null, and "
        "REFUSES to start when the country pool is behind pool__basic's calendar or "
        "when the fitted cost estimate exceeds budget_minutes (usa needs ~6 days at "
        "20 draws, so it raises on purpose)."
    ),
)
def feature_selection_economy(
    context: AssetExecutionContext,
    config: EconomySelectionConfig,
    preprocessor: PreprocessorResource,
) -> MaterializeResult:
    country = context.partition_key
    table = f"pool__economy_{country}"

    if config.universe not in UNIFIED_PARTITIONS.get_partition_keys():
        raise ValueError(
            f"universe={config.universe!r} is not an enabled unified partition. "
            f"Valid: {', '.join(UNIFIED_PARTITIONS.get_partition_keys())}."
        )
    # ⚠️ See the module docstring: `run_selection` takes the cross-sectional path on any
    # `cs_` target, and that path reads `pool__basic ⋈ pool__targets` ONLY. The run would
    # succeed and the partition key would have had no effect on it whatsoever.
    if config.target.startswith("cs_"):
        raise ValueError(
            f"target={config.target!r} is cross-sectional, and the cross-sectional "
            f"path reads pool__basic + pool__targets only — it cannot see {table}. "
            f"This run would ignore its own partition key. Use a series target here, "
            f"and run cs_ targets through `python -m feature_selection.run` on "
            f"pool__basic."
        )

    schema = _schema_of(config.universe)

    with preprocessor.session(schema=schema) as prep:
        with prep._database_driver._cursor_ctx() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s",
                (schema, table),
            )
            if cur.fetchone() is None:
                raise ValueError(
                    f"{schema}.{table} does not exist. Materialise "
                    f"unified/pool__economy on partition {config.universe} first."
                )
            rows, first_date, last_date = _assert_same_calendar(cur, schema, table)
            channels = _channel_estimate(cur, schema, table)

    minutes = estimated_minutes(channels, config.null_draws)
    context.log.info(
        f"{schema}.{table}: ~{channels} channels over {rows:,} rows "
        f"({first_date}..{last_date}); estimated {minutes:.0f} min at "
        f"null_draws={config.null_draws}"
    )
    if minutes > config.budget_minutes:
        raise ValueError(
            f"estimated {minutes:.0f} min ({minutes / 60:.1f} h) for {country} at "
            f"~{channels} channels x {config.null_draws} null draws, over the "
            f"{config.budget_minutes:.0f} min budget. The curve is fitted to this "
            f"repo's own archive and is quadratic in channels (usa: 1,458 channels, "
            f"428 min with NO null). Either drop null_draws, or raise budget_minutes "
            f"deliberately for this one partition — do not raise the default."
        )

    # ⚠️ IMPORTED HERE, NOT AT MODULE LEVEL. `feature_selection.run` pulls in xgboost,
    # sklearn and scipy; at module scope that cost lands on every `dagster dev` reload
    # and every `dagster definitions validate`, for a code location whose other 74
    # assets do not need any of it.
    from feature_selection.run import run_selection

    started = time.time()
    written = run_selection(
        ticker=config.universe,
        pools=["pool__basic", table],
        target=config.target,
        lookback=config.lookback,
        horizon=config.horizon,
        normalize=config.normalize,
        corr_threshold=config.corr_threshold,
        n_splits=config.n_splits,
        min_train=config.min_train,
        device=config.device,
        random_state=config.random_state,
        stability=config.stability,
        null_draws=config.null_draws,
        holdout_start=config.holdout_start,
        notes=config.notes or f"dagster analysis/feature_selection_economy/{country}",
        **({"root": config.root} if config.root else {}),
    )
    elapsed = (time.time() - started) / 60.0

    # Read the run's OWN metadata rather than re-deriving anything: the folder is the
    # artefact and the asset's metadata must not be able to disagree with it.
    with open(
        os.path.join(written.path, "metadata.json"), encoding="utf-8"
    ) as handle:
        archived = json.load(handle)

    ic = archived["results"].get("ic_summary", {}) or {}
    null = archived.get("null")
    kept = archived["results"].get("kept", []) or []
    economy_kept = [c for c in kept if "__economy__" in c]

    # ⚠️ `evidence` is the honest headline and `clears_bar` is not, for the two reasons
    # CLAUDE.md §5 rules 2-3 give: an absent null is an UNKNOWN, and a bar cleared while
    # one shuffled draw beat the real data is not a pass worth reporting as a boolean.
    if null is None:
        evidence = "no_null — NO bar was computed; this is an unknown, not a pass"
    else:
        evidence = (
            f"{'clears' if null.get('clears_bar') else 'FAILS'} its own bar "
            f"(observed {null.get('observed_ic'):+.4f}, p95 bar "
            f"{null.get('null_p95_BAR'):+.4f}, null MAX {null.get('null_max'):+.4f}, "
            f"z {null.get('z_vs_null'):+.2f}, p {null.get('p_value'):.4f})"
        )
        if null.get("null_max", 0) >= null.get("observed_ic", 0):
            evidence += " ⚠️ a shuffled draw reached or beat the observed value"

    context.log.info(f"{country}: {evidence}")

    return MaterializeResult(
        metadata={
            "country": MetadataValue.text(country),
            "universe": MetadataValue.text(config.universe),
            "run_id": MetadataValue.text(archived["run_id"]),
            "report_path": MetadataValue.path(written.path),
            "channels": archived["results"]["n_channels"],
            "kept": len(kept),
            "kept_economy": len(economy_kept),
            "panel_rows": archived["input"]["panel_rows"],
            "last_date": MetadataValue.text(str(archived["input"]["last_date"])),
            "ic_mean": MetadataValue.float(round(float(ic.get("ic_mean", 0.0)), 4)),
            "ic_fold_sd": MetadataValue.float(
                round(float(ic.get("ic_fold_sd", 0.0)), 4)
            ),
            "se_ic_per_fold": MetadataValue.float(
                round(float(ic.get("se_ic_per_fold", 0.0)), 4)
            ),
            "null_draws": config.null_draws,
            "evidence": MetadataValue.text(evidence),
            "estimated_minutes": MetadataValue.float(round(minutes, 1)),
            "actual_minutes": MetadataValue.float(round(elapsed, 1)),
        }
    )


assets: List[Callable] = [feature_selection_economy]
