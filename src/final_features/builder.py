# src\final_features\builder.py
"""Collect every `outstanding.csv` and materialise one final feature table per setup.

    python -m final_features                 # print the plan and the DDL, write nothing
    python -m final_features --apply         # create the tables
    python -m final_features --apply --replace   # ⚠️ drop an existing table first

## The grouping rule

One table per **(schema, target, setup)**. Runs that share all three describe the
same experiment on different feature blocks, so their chosen channels belong in one
table; runs that differ in any of them do not, and merging them would produce exactly
the artefact `feature_selection/CONTEXT.md` §8 is a list of — two runs that look
comparable and are not.

    unified_schema_vcb  / return_5day  / d=20 h=5   ← 19 runs (basic + each economy block)
    unified_schema_vcb  / return_5day  / d=1  h=5   ← 2 runs (pool__fa, pool__ta)
    unified_schema_bank / cs_rank_5day / d=20 h=5   ← 1 run

**Same schema in, same schema out.** `unified_schema_vcb`'s runs produce a table in
`unified_schema_vcb`. Nothing crosses schemas — a VCB feature and a BANK feature are
not the same column even when they share a name.

## The name

    <target>__final__d<lookback>_h<horizon>          e.g. return_5day__final__d20_h5

⚠️ **The setup is IN the name, not only in the grouping.** Two of the three groups
above share a schema AND a target and differ only in `lookback_d`; a bare
`return_5day__final` would have to hold both or silently lose one. If two groups ever
collide on a name anyway, `plan_from_reports` raises rather than picking a winner.

## ⚠️ CREATE TABLE AS, never a pandas round-trip

psycopg2 returns `numeric` as `Decimal`, a DataFrame carries that as dtype `object`,
and a writer maps `object` to VARCHAR — a read-then-write would silently turn every
price column into TEXT. `orchestration/assets/unified.py` documents the same trap for
`pool__basic`. So the join happens **server-side** and the source types are inherited.

## ⚠️ The target column, and the one case where it cannot be stored

The table carries its target beside the features, so the next stage needs one object.
`return_5day` is a column of `pool__targets` and is copied.

**`cs_rank_5day` is NOT a stored column and is not written.** It is
`(rank − 1)/(n − 1) − 0.5` computed *within each date across a chosen universe*
(`cross_sectional.cross_sectional_rank`), so its value for a stock-date depends on
which other names are in the panel and on `min_width` — properties of the RUN, not of
the row. Freezing it into a table would bake one universe into data that outlives it.
Those groups store **`return_5day`**, the quantity the rank is computed FROM, and the
reader re-ranks. The `target` column of the plan still records what was selected for.

## ⚠️ What this does NOT assert

That the features are worth having. 19 of the 22 runs computed no null at all, and no
run in the archive is a clean pass (`feature_selection/CONTEXT.md` §14b). Every table
gets a `COMMENT` naming its source runs and their `evidence`, so the provenance
travels with the data — but a row in one of these tables is a channel some run ranked
highly, nothing more.
"""

import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from feature_selection.outstanding import OUTSTANDING_FILENAME
from feature_selection.report import DEFAULT_REPORT_ROOT
from feature_selection.unified_reader import KEY_COLS, UnifiedSchemaReader

# The table the label is read from. Every `unified_schema_*` has one.
TARGETS_TABLE = "pool__targets"

# What makes two runs "the same setup". ⚠️ Every knob here can move a selection, so a
# difference in any of them means the two runs are not the same experiment and their
# chosen channels do not belong in one table.
SETUP_KEYS: Tuple[str, ...] = (
    "lookback_d", "horizon_h", "normalize", "feature_normalize", "max_features",
    "corr_threshold", "n_splits", "min_train", "random_state", "selector_class",
)

# Anything interpolated into SQL as an identifier is matched against this first —
# schema, table and column names cannot be bound parameters. Same reasoning as
# `unified_reader.TICKER_PATTERN`.
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# How a `None` setup value is carried through `groupby`, which drops None keys.
NOT_SET = "not_set"

# PostgreSQL truncates identifiers past this silently, which would collide two tables
# into one. Checked rather than trusted.
MAX_IDENTIFIER_BYTES = 63


def _identifier(name: str, what: str) -> str:
    if not IDENTIFIER.match(name or ""):
        raise ValueError(f"{what} {name!r} is not a bare SQL identifier.")
    if len(name.encode()) > MAX_IDENTIFIER_BYTES:
        raise ValueError(
            f"{what} {name!r} is {len(name.encode())} bytes; PostgreSQL truncates "
            f"past {MAX_IDENTIFIER_BYTES} and two truncated names can collide."
        )
    return name


def table_name(target: str, lookback: int, horizon: int) -> str:
    """`<target>__final__d<lookback>_h<horizon>`."""
    return _identifier(f"{target}__final__d{int(lookback)}_h{int(horizon)}", "table name")


@dataclass
class FinalTablePlan:
    """One table to build: where it goes, what it holds, and where that came from."""

    schema: str
    table: str
    target: str
    stored_target: Optional[str]
    setup: Dict
    columns_by_table: Dict[str, List[str]]
    runs: List[str] = field(default_factory=list)
    evidence: Dict[str, int] = field(default_factory=dict)

    @property
    def n_features(self) -> int:
        return sum(len(c) for c in self.columns_by_table.values())

    @property
    def source_tables(self) -> List[str]:
        return sorted(self.columns_by_table)

    def comment(self) -> str:
        """The provenance sentence attached to the table with `COMMENT ON`."""
        evidence = ", ".join(f"{k}={v}" for k, v in sorted(self.evidence.items()))
        note = ""
        # ⚠️ Keyed on whether the TARGET is derived, not on `stored_target is None` —
        # a derived target still stores its base column, so the second test is False
        # exactly when the warning is most needed.
        if self.target_derived:
            note = (
                f" ⚠️ Target {self.target!r} is NOT stored: it is ranked within a date "
                f"across a chosen universe, so it depends on which other names are in "
                f"the panel rather than on this row — {self.stored_target!r} is stored "
                f"instead and the reader re-ranks with "
                f"cross_sectional.cross_sectional_rank."
            )
        if self.evidence.get("no_null"):
            note += (
                " ⚠️ evidence=no_null means no bar was computed for that run — a "
                "ranking without a null is descriptive, not evidence "
                "(feature_selection/CONTEXT.md §14b)."
            )
        # ⚠️ `.item()` on the numpy scalars pandas leaves in the setup — without it
        # the comment reads `'lookback_d': np.int64(20)`, which is the repr of the
        # container rather than the value anyone wants to read off a table.
        setup = ", ".join(
            f"{k}={v.item() if hasattr(v, 'item') else v}"
            for k, v in ((k, self.setup[k]) for k in SETUP_KEYS)
        )
        return (
            f"Final feature table built by final_features from {len(self.runs)} "
            f"feature-selection run(s) sharing target={self.target!r} and setup "
            f"[{setup}]. "
            f"{self.n_features} channels from {len(self.columns_by_table)} pool(s). "
            f"Run evidence: {evidence}.{note} Source runs: "
            f"{'; '.join(sorted(self.runs))}"
        )

    # Set by `build_all`, which is the only place that can see the database and so
    # the only place that knows whether the target exists as a column.
    target_derived: bool = False


def _read_outstanding(root: str) -> pd.DataFrame:
    """Every `outstanding.csv` under `root`, with the full setup joined from metadata.

    ⚠️ **The setup comes from `metadata.json`, not from `outstanding.csv`.** The
    shortlist carries only `lookback_d` and `horizon_h` — enough to read a row, not
    enough to decide that two runs are the same experiment. Grouping on what the
    shortlist happens to carry would silently merge runs differing in `normalize`,
    `max_features` or `random_state`, and §8 of `feature_selection/CONTEXT.md` is a
    list of what that costs. `metadata.json` is the authority and records all 27 knobs.
    """
    frames = []
    for name in sorted(os.listdir(root)):
        path = os.path.join(root, name, OUTSTANDING_FILENAME)
        meta_path = os.path.join(root, name, "metadata.json")
        if not (os.path.exists(path) and os.path.exists(meta_path)):
            continue
        frame = pd.read_csv(path)
        setup = json.load(open(meta_path, encoding="utf-8"))["setup"]
        missing = [k for k in SETUP_KEYS if k not in setup]
        if missing:
            raise ValueError(f"{name}/metadata.json setup is missing {missing}.")
        for key in SETUP_KEYS:
            # None is a real value here (`feature_normalize` on a non-cross-sectional
            # run), and `groupby` DROPS None keys even with dropna=False on some
            # pandas versions — so it is stringified into a stable sentinel. ASCII,
            # because this prints to a cp1252 console on Windows.
            frame[key] = NOT_SET if setup[key] is None else setup[key]
        frame["folder"] = name
        frames.append(frame)
    if not frames:
        raise FileNotFoundError(
            f"no {OUTSTANDING_FILENAME} under {root} — run "
            f"`python -m feature_selection.outstanding` first."
        )
    return pd.concat(frames, ignore_index=True)


def _stored_target(target: str, available: Sequence[str], horizon: int) -> Optional[str]:
    """The column to copy for `target`, or None when the target is derived.

    A `cs_rank_*` target is a rank WITHIN a date across a universe, so it is not a
    column of any per-ticker table — see the module docstring.
    """
    if target in available:
        return target
    if target.startswith("cs_rank_"):
        return None
    raise ValueError(
        f"target {target!r} is neither a column of {TARGETS_TABLE} {list(available)} "
        f"nor a recognised derived target."
    )


def plan_from_reports(root: str = DEFAULT_REPORT_ROOT) -> List[FinalTablePlan]:
    """Group every run's `outstanding.csv` into one plan per (schema, target, setup)."""
    rows = _read_outstanding(root)
    for column in ("channel", "source_table", "schema"):
        for value in rows[column].unique():
            _identifier(str(value), column)

    plans: List[FinalTablePlan] = []
    group_keys = ["schema", "target"] + [k for k in SETUP_KEYS if k in rows.columns]
    for key, group in rows.groupby(group_keys, dropna=False):
        setup = dict(zip(group_keys, key))
        columns_by_table: Dict[str, List[str]] = {}
        for source, sub in group.groupby("source_table"):
            if source == "unknown":
                raise ValueError(
                    f"{sorted(set(sub['channel']))} have source_table='unknown' — "
                    f"feature_selection.outstanding could not map them to a pool."
                )
            columns_by_table[source] = sorted(set(sub["channel"]))
        plans.append(
            FinalTablePlan(
                schema=setup["schema"],
                table=table_name(setup["target"], setup["lookback_d"], setup["horizon_h"]),
                target=setup["target"],
                stored_target=None,  # resolved in build_all, which can see the database
                setup={k: setup.get(k) for k in SETUP_KEYS},
                columns_by_table=columns_by_table,
                runs=sorted(set(group["run_id"])),
                evidence=group.groupby("evidence")["run_id"].nunique().to_dict(),
            )
        )

    seen: Dict[Tuple[str, str], List[str]] = {}
    for plan in plans:
        seen.setdefault((plan.schema, plan.table), []).extend(plan.runs)
    for (schema, table), runs in seen.items():
        if len([p for p in plans if (p.schema, p.table) == (schema, table)]) > 1:
            raise ValueError(
                f"two different setups both want {schema}.{table} — the name does not "
                f"separate them. Runs: {sorted(set(runs))}"
            )
    return sorted(plans, key=lambda p: (p.schema, p.table))


def build_sql(plan: FinalTablePlan) -> str:
    """The `CREATE TABLE AS` that materialises one plan.

    ⚠️ The join is INNER on `(date, exchange, ticker)`, matching the panel the
    selection actually ran on — `join_log` in every `metadata.json` records the same
    keys and `how="inner"`. A LEFT join would invent rows the ranking never saw.
    """
    schema = _identifier(plan.schema, "schema")
    keys = ", ".join(f"base.{k}" for k in KEY_COLS)
    tables = plan.source_tables
    base = tables[0]

    selected = [keys]
    if plan.stored_target:
        selected.append(f"tgt.{_identifier(plan.stored_target, 'target column')}")
    for table in tables:
        alias = "base" if table == base else _alias(table)
        selected += [
            f"{alias}.{_identifier(c, 'channel')}" for c in plan.columns_by_table[table]
        ]

    using = ", ".join(KEY_COLS)
    joins = [
        f"JOIN {schema}.{_identifier(t, 'table')} AS {_alias(t)} USING ({using})"
        for t in tables[1:]
    ]
    if plan.stored_target:
        joins.append(f"JOIN {schema}.{TARGETS_TABLE} AS tgt USING ({using})")

    body = ",\n       ".join(selected)
    join_sql = "\n".join(joins)
    return (
        f"CREATE TABLE {schema}.{plan.table} AS\n"
        f"SELECT {body}\n"
        f"FROM   {schema}.{_identifier(base, 'table')} AS base\n"
        f"{join_sql}\n"
        f"ORDER BY {keys};"
    )


def _alias(table: str) -> str:
    """A short, unique, identifier-safe alias for a pool table."""
    return "t_" + re.sub(r"[^A-Za-z0-9_]", "_", table)


def build_all(
    root: str = DEFAULT_REPORT_ROOT, apply: bool = False, replace: bool = False
) -> pd.DataFrame:
    """Plan every table and, with `apply=True`, create it. Returns one row per plan."""
    plans = plan_from_reports(root)
    results = []

    by_schema: Dict[str, List[FinalTablePlan]] = {}
    for plan in plans:
        by_schema.setdefault(plan.schema, []).append(plan)

    for schema, schema_plans in by_schema.items():
        ticker = schema.replace("unified_schema_", "")
        with UnifiedSchemaReader(ticker) as reader:
            available = list(reader.column_types(TARGETS_TABLE))
            existing = set(reader.tables())
            for plan in schema_plans:
                plan.stored_target = _stored_target(
                    plan.target, available, plan.setup["horizon_h"]
                )
                if plan.stored_target is None:
                    plan.target_derived = True
                    fallback = f"return_{int(plan.setup['horizon_h'])}day"
                    if fallback not in available:
                        raise ValueError(
                            f"derived target {plan.target!r} needs {fallback!r} in "
                            f"{TARGETS_TABLE}, which has {available}."
                        )
                    plan.stored_target = fallback

                sql = build_sql(plan)
                row = {
                    "schema": plan.schema,
                    "table": plan.table,
                    "target": plan.target,
                    "stored_target": plan.stored_target,
                    "derived_target": plan.target
                    if plan.target not in available
                    else None,
                    "runs": len(plan.runs),
                    "pools": len(plan.columns_by_table),
                    "features": plan.n_features,
                    "exists": plan.table in existing,
                    "created": False,
                    "rows": None,
                    "columns": None,
                }

                if apply:
                    if plan.table in existing and not replace:
                        raise ValueError(
                            f"{schema}.{plan.table} already exists. Pass --replace to "
                            f"drop and rebuild it — this DESTROYS the existing table."
                        )
                    with reader.driver._cursor_ctx() as cur:
                        if plan.table in existing and replace:
                            cur.execute(f"DROP TABLE {schema}.{plan.table}")
                        cur.execute(sql)
                        # ⚠️ The key ORDER is part of the contract, same as
                        # `unified.py`: only a leading `date` lets the index serve the
                        # time-range scan every consumer of this table will do.
                        cur.execute(
                            f"ALTER TABLE {schema}.{plan.table} "
                            f"ADD PRIMARY KEY ({', '.join(KEY_COLS)})"
                        )
                        cur.execute(
                            f"COMMENT ON TABLE {schema}.{plan.table} IS %s",
                            (plan.comment(),),
                        )
                        cur.execute(f"SELECT COUNT(*) FROM {schema}.{plan.table}")
                        row["rows"] = int(cur.fetchone()[0])
                        cur.execute(
                            "SELECT COUNT(*) FROM information_schema.columns "
                            "WHERE table_schema = %s AND table_name = %s",
                            (schema, plan.table),
                        )
                        row["columns"] = int(cur.fetchone()[0])
                    row["created"] = True
                    expected = plan.n_features + len(KEY_COLS) + (1 if plan.stored_target else 0)
                    if row["columns"] != expected:
                        raise ValueError(
                            f"{schema}.{plan.table} has {row['columns']} columns, "
                            f"expected {expected} — a channel was lost or duplicated."
                        )
                    if not row["rows"]:
                        raise ValueError(
                            f"{schema}.{plan.table} is EMPTY — the inner join matched "
                            f"nothing, which means the pools do not share a calendar."
                        )
                results.append(row)
    return pd.DataFrame(results)


def main(argv: Optional[Sequence[str]] = None) -> pd.DataFrame:
    argv = list(sys.argv[1:] if argv is None else argv)
    apply = "--apply" in argv
    replace = "--replace" in argv

    plans = plan_from_reports()
    for plan in plans:
        print(f"\n{'=' * 78}\n{plan.schema}.{plan.table}")
        print(f"  target   {plan.target}")
        print(f"  setup    " + ", ".join(f"{k}={plan.setup[k]}" for k in SETUP_KEYS))
        print(f"  runs     {len(plan.runs)}  evidence={plan.evidence}")
        print(f"  features {plan.n_features} from {len(plan.columns_by_table)} pool(s)")
        for table in plan.source_tables:
            print(f"      {len(plan.columns_by_table[table]):>3}  {table}")

    result = build_all(apply=apply, replace=replace)
    print(f"\n{'=' * 78}")
    pd.set_option("display.width", 200)
    print(result.to_string(index=False))
    if not apply:
        print("\nplan only — pass --apply to create the tables")
    return result


if __name__ == "__main__":
    main()
