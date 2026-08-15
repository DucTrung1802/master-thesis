# src\final_features\builder.py
"""Collect every `outstanding.csv` and materialise one final feature table per setup.

    python -m final_features                 # print the plan and the DDL, write nothing
    python -m final_features --apply         # create the tables
    python -m final_features --apply --replace   # ⚠️ drop an existing table first
    python -m final_features --apply --scope basic   # root: reports/feature_selection

## The grouping rule

One table per **(schema, target, setup)**. Runs that share all three describe the
same experiment on different feature blocks, so their chosen channels belong in one
table; runs that differ in any of them do not, and merging them would produce exactly
the artefact `feature_selection/CONTEXT.md` §8 is a list of — two runs that look
comparable and are not.

    unified_schema_vcb  / return_5day  / d=20 h=5   ← 19 runs (basic + each economy block)
    unified_schema_bank / cs_rank_5day / d=20 h=5   ← 1 run

⚠️ The study keeps only `d=20, h=5` as of 2026-08-09; a `d=1` group existed and was
removed with its two runs.

**Same schema in, same schema out.** `unified_schema_vcb`'s runs produce a table in
`unified_schema_vcb`. Nothing crosses schemas — a VCB feature and a BANK feature are
not the same column even when they share a name.

## The name

    <target>__final__d<lookback>_h<horizon>          e.g. return_5day__final__d20_h5

⚠️ **A `cs_` target prefix is dropped**: `cs_rank_5day` → `rank_5day__final__d20_h5`.
The `cs_` says "cross-sectional", which is what the SCHEMA already says. See
`table_name`.

⚠️ **The setup is IN the name, not only in the grouping.** Two groups used to share a
schema AND a target and differ only in `lookback_d`; a bare `return_5day__final` would
have had to hold both or silently lose one. The discriminator stays even now that one
setup survives — a second is one run away, and a rename is worse than a long name. If
two groups ever collide on a name, `plan_from_reports` raises rather than picking a
winner.

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

That the features are worth having. 19 of the 20 runs computed no null at all and the
twentieth failed its own, so **no surviving run in the archive clears anything**
(`feature_selection/CONTEXT.md` §14b). Every table
gets a `COMMENT` naming its source runs and their `evidence`, so the provenance
travels with the data — but a row in one of these tables is a channel some run ranked
highly, nothing more.
"""

import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from feature_selection import contract
from feature_selection.contract import (
    CUT_KEYS,
    IDENTIFIER,
    MAX_IDENTIFIER_BYTES,
    SETUP_FROM_SHORTLIST,
    SETUP_KEYS,
)
from feature_selection.outstanding import OUTSTANDING_FILENAME
from feature_selection.report import DEFAULT_REPORT_ROOT
from feature_selection.unified_reader import KEY_COLS, UnifiedSchemaReader
from utils import runtime

# The table the label is read from. Every `unified_schema_*` has one.
TARGETS_TABLE = "pool__targets"

# ⚠️ **`SETUP_KEYS`, `CUT_KEYS`, `IDENTIFIER` AND THE 63-BYTE LIMIT NOW LIVE IN
# `feature_selection/contract.py`** (2026-08-15) and are imported above, unchanged
# and under their old names. They describe a HANDOFF — what a selection run must
# write for this module to read it — and they were previously declared here alone,
# where the module that has to satisfy them could not see them. `contract.py` carries
# the reasoning that used to sit in these comments, including why `max_features` is
# not a setup key and why the two cut parameters come from the shortlist rather than
# from `metadata.json`.

# How many hex characters of the shortlist digest go into the table COMMENT. 12 is
# ~48 bits — far past collision risk for a few dozen tables, and short enough to read.
FINGERPRINT_CHARS = 12

# The sentence the fingerprint is written in, and the pattern that reads it back.
FINGERPRINT_LABEL = "Shortlist fingerprint"
FINGERPRINT_RE = re.compile(
    rf"{FINGERPRINT_LABEL}: (?P<digest>[0-9a-f]{{{FINGERPRINT_CHARS}}}) "
    r"over (?P<n>\d+) channel"
)

# How a `None` setup value is carried through `groupby`, which drops None keys.
NOT_SET = "not_set"

# The marker a cross-sectional target carries. Dropped from the TABLE name — the
# schema already names the cross-section. See `table_name`.
CS_PREFIX = "cs_"


def _identifier(name: str, what: str) -> str:
    """`name`, or a `ValueError`. The rule is `contract.identifier`'s — one copy, and
    the producer checks its own output against the same one before writing it."""
    return contract.identifier(name, what)


def fingerprint(columns_by_table: Dict[str, List[str]]) -> str:
    """A digest of the exact `(source_table, channel)` SET a table was built from.

    ⚠️ **This is the only thing that can tell a stale table from a current one.**
    `pipeline.status_final_features` used to report a stage "ready" whenever the table
    EXISTED, and that is how the VCB table drifted 26 columns away from its own
    shortlists without anything noticing (issue STL-1). A parameter cannot do this job:
    the cut is measured per run, so the same knobs on a re-run archive can legitimately
    produce a different set. The set itself is the fact.

    Sorted before hashing, so the digest is a property of the SET and not of the order
    `groupby` happened to return it in.
    """
    payload = "\n".join(
        f"{table}:{channel}"
        for table in sorted(columns_by_table)
        for channel in sorted(columns_by_table[table])
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:FINGERPRINT_CHARS]


def fingerprint_of_comment(comment: Optional[str]) -> Optional[str]:
    """The digest a live table records, or None if it predates fingerprinting."""
    match = FINGERPRINT_RE.search(comment or "")
    return match["digest"] if match else None


def table_name(
    target: str, lookback: int, horizon: int, scope: Optional[str] = None
) -> str:
    """`<target>__final__d<lookback>_h<horizon>[__<scope>]`, minus any `cs_` prefix.

    ⚠️ **`cs_rank_5day` names its table `rank_5day__final__*`.** The `cs_` marks a
    CROSS-SECTIONAL target, and a cross-section is a set of tickers — which is what the
    SCHEMA already says. `unified_schema_bank.cs_rank_5day__final__*` states "ranked
    across a cross-section" twice and names neither one; `unified_schema_bank.rank_5day
    __final__*` reads as "the 5-day rank, across the banks", which is the fact.

    ⚠️ This cannot collide a cross-sectional table with a time-series one: the stems
    differ (`rank_5day` vs `return_5day`), and `plan_from_reports` raises on any name
    two setups both want.

    ## ⚠️ `scope` names the FEATURE BLOCK, and it exists because the setup does not

    Two runs over *different pools* at the same target and the same knobs are the same
    `(schema, target, setup)` and so want the same table — `pool__basic` alone and
    `pool__basic + 19 macro blocks` both resolve to `return_5day__final__d20_h5`. That
    is correct for the archive, where 19 runs sharding one candidate set are deliberately
    UNIONED into one table (§6). It is wrong when the narrower pool is the experiment:
    building it into the same name means `--replace`, which drops the wide table, moves
    every dataset hash below it and orphans the runs that referenced them (§7).

    So a scoped build gets its own name. `scope` is a bare identifier describing the
    block — `basic`, `ta`, `fa` — and is NOT part of the grouping key: it is chosen per
    build, alongside the `--root` that decides which runs are in scope at all. A build
    with no `--scope` behaves exactly as before.

    ⚠️ **`scope` CARRIES THE SEPARATION ALONE SINCE 2026-08-10.** The `_basic` and
    `_economy` roots were merged into `reports/feature_selection/` and every archived
    run dropped, so `--root` no longer partitions anything by default: a `pool__basic`
    run and a `basic+economy_<country>` run now land in one root at one seed and are
    UNIONED. Name each build's block (`--scope basic`, `--scope economy_japan`) or
    accept that union deliberately.
    """
    stem = target[len(CS_PREFIX):] if target.startswith(CS_PREFIX) else target
    name = f"{stem}__final__d{int(lookback)}_h{int(horizon)}"
    if scope:
        # Validated separately so the error names the SCOPE rather than the whole
        # table it was interpolated into.
        name = f"{name}__{_identifier(scope, 'scope')}"
    return _identifier(name, "table name")


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
    def fingerprint(self) -> str:
        """The digest of this plan's exact channel set. See `fingerprint`."""
        return fingerprint(self.columns_by_table)

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
            # ⚠️ The fingerprint is what lets a later `status` call tell whether the
            # shortlists still describe this table. Without it, "the table exists" was
            # the only check anything made — see `fingerprint`.
            f"{FINGERPRINT_LABEL}: {self.fingerprint} over {self.n_features} channels. "
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

    ⚠️ **A RUN FOLDER WITH NO `outstanding.csv` NOW RAISES** (2026-08-15). It used to
    be skipped, which meant a finished selection could be absent from every plan this
    module makes and nothing would say so — measured that day on the two newest runs,
    both merged back from `kaggle_gpu`, which writes the folder and prints a reminder
    rather than the shortlist. The plan came out over 19 runs, reported no error, and
    was wrong about which experiment it described. Same rule as CLAUDE.md §5 rule 12:
    silence is never how something gets left out.
    """
    absent = contract.missing_shortlists(root)
    if absent:
        raise FileNotFoundError(
            f"{len(absent)} run folder(s) under {root} have a metadata.json and no "
            f"{OUTSTANDING_FILENAME}, so they would be SILENTLY excluded from every "
            f"plan: {absent}. Run `python -m feature_selection.outstanding` first."
        )

    frames = []
    for name in sorted(os.listdir(root)):
        path = os.path.join(root, name, OUTSTANDING_FILENAME)
        meta_path = os.path.join(root, name, "metadata.json")
        if not (os.path.exists(path) and os.path.exists(meta_path)):
            continue
        frame = pd.read_csv(path)
        setup = json.load(open(meta_path, encoding="utf-8"))["setup"]
        # ⚠️ `CUT_KEYS` describe the cut and are stamped into `outstanding.csv` by
        # `feature_selection.outstanding`; they are NOT in `metadata.json`, which
        # records the selector run. A shortlist written before they existed is
        # rejected rather than silently grouped with one that has them.
        missing_cut = [k for k in SETUP_FROM_SHORTLIST if k not in frame.columns]
        if missing_cut:
            raise ValueError(
                f"{name}/{OUTSTANDING_FILENAME} is missing {missing_cut} — it "
                f"predates the cut parameters. Re-run "
                f"`python -m feature_selection.outstanding`."
            )
        missing = [k for k in SETUP_KEYS if k not in setup]
        if missing:
            raise ValueError(f"{name}/metadata.json setup is missing {missing}.")
        for key in (k for k in SETUP_KEYS if k not in SETUP_FROM_SHORTLIST):
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


def plan_from_reports(
    root: str = DEFAULT_REPORT_ROOT, scope: Optional[str] = None
) -> List[FinalTablePlan]:
    """Group every run's `outstanding.csv` into one plan per (schema, target, setup).

    ⚠️ **`root` decides which runs exist as far as this function is concerned**, and
    that is the whole mechanism for building a narrower table: the grouping key has no
    term for "which pools", so two runs over different feature blocks at the same target
    and knobs are one group. Keeping a scoped run under its own root is what keeps it
    out of the archive's union (`table_name`). `scope` then names the table it builds.
    """
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
                table=table_name(
                    setup["target"], setup["lookback_d"], setup["horizon_h"], scope
                ),
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
    root: str = DEFAULT_REPORT_ROOT,
    apply: bool = False,
    replace: bool = False,
    scope: Optional[str] = None,
) -> pd.DataFrame:
    """Plan every table and, with `apply=True`, create it. Returns one row per plan."""
    plans = plan_from_reports(root, scope)
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
                    # Set when an existing table is left alone because it is current.
                    "skipped": "",
                }

                if apply:
                    # ⚠️ **A TABLE THAT IS ALREADY CURRENT IS SKIPPED, NOT AN ERROR.**
                    # A root holding several runs produces several plans, and the moment
                    # a second experiment joined the narrow root (then
                    # `reports/feature_selection_basic/`, merged away 2026-08-10) a
                    # plain `--apply` raised on the FIRST table — which was finished and
                    # correct — before reaching the new one. That made the flag unusable
                    # for adding a table to an existing root. "Current" means the live
                    # table's stored fingerprint equals what this plan would build, the
                    # same test `pipeline.status_final_features` makes (issue STL-1); a
                    # table that exists and does NOT match is still a hard error, because
                    # rebuilding it drops it and orphans every dataset below (§7).
                    if plan.table in existing and not replace:
                        with reader.driver._cursor_ctx() as cur:
                            cur.execute(
                                "SELECT obj_description(%s::regclass)",
                                (f"{schema}.{plan.table}",),
                            )
                            found = cur.fetchone()
                        stored = fingerprint_of_comment(found[0] if found else "")
                        if stored == plan.fingerprint:
                            row["skipped"] = "current — fingerprint matches"
                            results.append(row)
                            continue
                        raise ValueError(
                            f"{schema}.{plan.table} already exists and its fingerprint "
                            f"{stored} != this plan's {plan.fingerprint}. Pass --replace "
                            f"to drop and rebuild it — this DESTROYS the existing table "
                            f"and every dataset hash below it."
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


def _flag_value(argv: Sequence[str], flag: str) -> Optional[str]:
    """`--flag value` or `--flag=value`, or None. Kept tiny and local — this module's
    CLI is three flags and an argparse would be more machinery than the whole main."""
    for i, token in enumerate(argv):
        if token == flag and i + 1 < len(argv):
            return argv[i + 1]
        if token.startswith(f"{flag}="):
            return token.split("=", 1)[1]
    return None


def main(argv: Optional[Sequence[str]] = None) -> pd.DataFrame:
    argv = list(sys.argv[1:] if argv is None else argv)
    apply = "--apply" in argv
    replace = "--replace" in argv
    root = _flag_value(argv, "--root") or DEFAULT_REPORT_ROOT
    scope = _flag_value(argv, "--scope")

    # ⚠️ `show_gpu=False`: this stage is SQL from end to end. `runtime.gpu_report`
    # would answer from `nvidia-smi`, truthfully and irrelevantly, and a banner that
    # reports hardware no step here can use teaches the reader to skip banners.
    with runtime.RunTimer(
        f"final_features  root={os.path.basename(root)}"
        f"{f' scope={scope}' if scope else ''}"
        f"{'  --apply' if apply else '  (plan only)'}"
        f"{'  --replace' if replace else ''}",
        show_gpu=False,
    ):
        return _main(root=root, scope=scope, apply=apply, replace=replace)


def _main(root: str, scope: Optional[str], apply: bool, replace: bool) -> pd.DataFrame:
    plans = plan_from_reports(root, scope)
    for plan in plans:
        print(f"\n{'=' * 78}\n{plan.schema}.{plan.table}")
        print(f"  target   {plan.target}")
        print(f"  setup    " + ", ".join(f"{k}={plan.setup[k]}" for k in SETUP_KEYS))
        print(f"  runs     {len(plan.runs)}  evidence={plan.evidence}")
        print(f"  features {plan.n_features} from {len(plan.columns_by_table)} pool(s)")
        for table in plan.source_tables:
            print(f"      {len(plan.columns_by_table[table]):>3}  {table}")

    result = build_all(root=root, apply=apply, replace=replace, scope=scope)
    print(f"\n{'=' * 78}")
    pd.set_option("display.width", 200)
    print(result.to_string(index=False))
    if not apply:
        print("\nplan only — pass --apply to create the tables")
    return result


if __name__ == "__main__":
    main()
