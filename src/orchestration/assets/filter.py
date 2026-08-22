# src\orchestration\assets\filter.py
"""The FILTER layer — `silver_schema` / `gold_schema` → `filter_schema.universe__<screen>`.

ONE asset, partitioned by SCREEN. It sits between gold and unified and produces no time
series at all: one row per `(exchange, ticker)` carrying every condition's measured
value, its verdict, the conjunction `passes`, and `first_failed`.

    dagster asset materialize -f src/orchestration/definitions.py \\
      --select "filter/universe" --partition PRICE10K
    dagster asset materialize -f src/orchestration/definitions.py \\
      --select "group:unified" --partition PRICE10K      # then the schema it gates

⚠️ **THE DEFINITIONS LIVE IN `preprocessor/filters.py`, NOT HERE.** This module knows
how to run a screen and nothing about what any screen means — adding a condition or a
universe is an entry in that registry plus one partition key in `assets/unified.py` and
two lines in `config.json`.

⚠️ **THE EDGE INTO `unified` IS NOT DECLARED, AND THAT IS DELIBERATE.** `filter/universe`
is partitioned by SCREEN while `unified/pool__basic` is partitioned by UNIVERSE — a set
that also holds `VCB`, `ALL`, `BANK` and 29 single names, for which no screen exists. A
Dagster dependency is per-ASSET, not per-partition, so declaring it would claim that
`unified_schema_vcb` needs a screen. The check moved to where it can be exact instead:
`_helper_unified_member_filter` raises with the materialize command in the message when
a screen's universe table is absent. ⚠️ The cost is real and is the ordinary Dagster one
(CLAUDE.md §5 rule 14, from the other side): **re-running a screen does NOT mark the
unified schema stale.** Rebuild `group:unified` for that partition yourself.

⚠️ **A GREEN RUN HERE IS NOT EVIDENCE THE MEMBERSHIP IS WHAT YOU THINK** — rule 10's
shape, one layer along. Read `selected` against `candidates` and, for every condition,
`measured` against `candidates`: a condition that measured nothing and a condition
everything cleared BOTH report a 100 % pass rate, and only the first number tells them
apart. `debt_to_equity_max_12` is exactly that case at **2 of 781**.
"""

from typing import Callable, List

from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
)

from orchestration._bootstrap import bootstrap

bootstrap()

from orchestration import enabled
from orchestration.preprocessor import filters as filter_registry
from orchestration.resources import PreprocessorResource
from utils.constants import FILTER_SCHEMA

# ⚠️ THE PARTITION SET IS `filters.SCREENS`, READ AT IMPORT TIME. Every other partition
# list in this package is a literal, because a `PartitionsDefinition` must be known
# before any database connection exists — that constraint is satisfied here too, since
# `SCREENS` is a plain Python dict in a module with no I/O. Deleting a screen from the
# registry removes its partition; nothing else has to be edited.
FILTER_PARTITIONS = StaticPartitionsDefinition(
    enabled.register("filter", sorted(filter_registry.SCREENS))
)


def _source_asset_keys() -> List[AssetKey]:
    """Every asset a screen could read, as an upstream edge.

    The UNION over all screens, not per-partition, because a Dagster dependency belongs
    to the asset. `silver_schema.stocks_basic` → `silver/stocks_basic`, i.e. the schema
    with its `_schema` suffix dropped — the naming every asset in this package already
    follows.
    """
    sources = {filter_registry.CANDIDATE_SOURCE}
    sources |= {c.source for c in filter_registry.CONDITIONS.values()}
    keys = []
    for source in sorted(sources):
        schema, table = source.split(".", 1)
        keys.append(AssetKey([schema.removesuffix("_schema"), table]))
    return keys


@asset(
    name="universe",
    key_prefix=["filter"],
    group_name="filter",
    compute_kind="postgres",
    partitions_def=FILTER_PARTITIONS,
    deps=_source_asset_keys(),
    description=(
        "One SCREEN → filter_schema.universe__<screen>: the membership table that "
        "gates unified_schema_<screen>. ⚠️ EVERY CANDIDATE IS WRITTEN, NOT ONLY THE "
        "SURVIVORS — one row per (exchange, ticker) silver holds, with val__<cond> and "
        "pass__<cond> for every condition, the conjunction `passes`, and `first_failed` "
        "naming the first condition that rejected the name. A table of survivors "
        "answers 'who is in'; this one answers 'why is HPG out', which is the question "
        "anyone moving a threshold actually has. ⚠️ A SCREEN IS NOT POINT-IN-TIME: "
        "membership is decided from the windows in each condition and applied to the "
        "WHOLE history of every pool built on it, so a 2012 row exists because of "
        "something measured in 2026. That is CLAUDE.md §2c's defect — benign for a "
        "within-date shuffle null, fatal for any CAGR read off the universe. Every "
        "window is written into the table COMMENT. ⚠️ Built with CREATE TABLE AS, not "
        "a pandas round-trip (rule 15), PK (exchange, ticker) asserted so a condition "
        "CTE that returns two rows for one ticker fails instead of doubling it. "
        "⚠️ Re-running this does NOT mark unified stale — rebuild group:unified for the "
        "same partition yourself."
    ),
)
def filter_universe(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    name = context.partition_key
    scr = filter_registry.screen(name)

    # ⚠️ `session(schema=...)` is what CREATES the schema — same contract as the unified
    # assets. `filter_schema` comes into existence by being named here.
    with preprocessor.session(schema=FILTER_SCHEMA) as prep:
        result = prep._ingest_filter_universe(name)
        members = prep._helper_filter_universe_members(name)

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s",
                (FILTER_SCHEMA, scr.table),
            )
            columns = int(cur.fetchone()[0])
            cur.execute(
                "SELECT obj_description(%s::regclass, 'pg_class')",
                (scr.qualified_table,),
            )
            stored_comment = cur.fetchone()[0]

    candidates = result["candidates"]
    selected = result["selected"]
    conditions = scr.resolve()

    # ⚠️ THE MEMBER LIST IS COUNTED TWICE, from two different queries, because they
    # answer slightly different questions: `selected` counts rows the CTAS wrote with
    # `passes` true, `members` re-reads them through the same sub-select
    # `_helper_unified_member_filter` will use. A disagreement means the predicate and
    # the table have drifted, which is the one failure that would build a unified schema
    # nobody could explain.
    if len(members) != selected:
        raise ValueError(
            f"{scr.qualified_table} reports {selected} passing row(s) but the member "
            f"query returns {len(members)}. The screen table and the membership "
            f"predicate disagree."
        )
    # The comment is the only copy of the definition that travels WITH the data; a
    # missing one makes a later staleness check impossible rather than merely awkward.
    if filter_registry.definition_fingerprint(
        filter_registry.parse_comment(stored_comment)
    ) != filter_registry.definition_fingerprint(
        filter_registry.parse_comment(result["comment"])
    ):
        raise ValueError(
            f"{scr.qualified_table}'s stored COMMENT does not match the screen "
            f"definition that built it. The definition is the table's own provenance "
            f"and must be readable back."
        )

    # ⚠️ MEASURED, NOT ASSUMED, per condition — rule 22 at the filter. A pass rate alone
    # cannot tell "everything cleared this" from "nothing was measured", and this layer
    # has a live example of the second: `gold.stocks_financials_bank_fa` holds 2 of 781
    # tickers, so `debt_to_equity_max_12` abstains on 779 and reports a 100% pass rate.
    coverage = {
        c.name: 100.0 * result["measured"][c.name] / max(candidates, 1)
        for c in conditions
    }
    thinnest = min(coverage.items(), key=lambda kv: kv[1])

    context.log.info(
        f"{scr.qualified_table}: {selected} of {candidates} ticker(s) pass "
        f"({100.0 * selected / max(candidates, 1):.1f}%), {columns} columns. "
        + " | ".join(
            f"{c.name} passed {result['passed'][c.name]}, "
            f"measured {result['measured'][c.name]}/{candidates}"
            for c in conditions
        )
    )
    if thinnest[1] < 100.0:
        context.log.warning(
            f"{scr.qualified_table}: condition {thinnest[0]!r} could only be measured "
            f"on {thinnest[1]:.1f}% of candidates "
            f"(on_missing={filter_registry.CONDITIONS[thinnest[0]].on_missing}). A pass "
            f"on an unmeasured name is an ABSENT measurement, never a pass - "
            f"CLAUDE.md section 5 rule 2."
        )

    return MaterializeResult(
        metadata={
            "screen": MetadataValue.text(scr.name),
            "table": MetadataValue.text(scr.qualified_table),
            "candidates": candidates,
            "selected": selected,
            "selected_pct": MetadataValue.float(
                round(100.0 * selected / max(candidates, 1), 2)
            ),
            "columns": columns,
            "conditions": MetadataValue.text(
                " | ".join(c.describe() for c in conditions)
            ),
            "condition_coverage_pct": MetadataValue.text(
                ", ".join(f"{n} {v:.1f}%" for n, v in coverage.items())
            ),
            "rejected_by_first_failure": MetadataValue.text(
                ", ".join(f"{n} {k}" for n, k in result["rejected_by"].items())
                or "nothing rejected"
            ),
            "members_head": MetadataValue.text(
                ", ".join(f"{e}:{t}" for e, t in members[:25])
                + (f" … (+{len(members) - 25})" if len(members) > 25 else "")
            ),
            "unified_schema": MetadataValue.text(f"unified_schema_{scr.slug}"),
            "point_in_time": MetadataValue.bool(False),
            "screen_windows": MetadataValue.text(
                " | ".join(f"{c.name}: {c.window()}" for c in conditions)
            ),
        }
    )


assets: List[Callable] = [filter_universe]
