# src\pipeline\freshness.py
"""Per-ticker freshness — the thing `MAX(date)` cannot say (TODO `P1`, `FRZ-1`).

`MAX(date)` over a ticker-keyed table is ONE number, and one number cannot see how many
tickers produce it. Measured 2026-08-22 on `silver_schema.stocks_basic`: the scalar read
**2026-08-19** while **five** tickers produced that date and **757 of 781 were stale**,
599 of them frozen on one day in June. Every consumer that sanity-checks a table reads
exactly that scalar, so the whole non-bank universe carried no price for ~37 sessions with
nothing raising. That is CLAUDE.md §5 rule 10 at the top of the chain, and `FRZ-1` records
what it cost.

⚠️ **AND EVERY NARROWLY-SCOPED RE-SCRAPE MAKES THE SCALAR MORE WRONG.** Refreshing the 20
banks moved it to 2026-08-07; refreshing five tickers for the single-stock track moved it
to 2026-08-19. The number advanced six weeks while the tradeable cross-section stayed dead
at 2026-06-25. So this module reports a DISTRIBUTION and never a maximum.

## ⚠️ The one design decision that matters: WHOSE calendar

`sessions_behind` is counted against a **reference calendar taken from the price spine**
(`silver_schema.stocks_basic`), never against the measured table's own dates — and that is
not a detail. A table's own dates cannot contain the sessions it is missing, so a
**completely frozen table would report every ticker 0 sessions behind**: the same lie as
the scalar, one level down, and harder to see because it wears a per-ticker shape. Aging a
laggard against a fresher reference is the entire point of the module.

## ⚠️ A cliff is a scrape failure; scatter is delisting

Both look like "stale tickers" to a counter, and they call for opposite responses. The
SHAPE tells them apart, and this repo has measured both regimes six weeks apart:

| what happened | stale tickers | largest same-date group | as a share |
|---|---|---|---|
| the `FRZ-1` freeze (2026-08-22) | **757 of 781** | **599 on 2026-06-26** | **77 %** |
| after the re-scrape (2026-08-23) | **13 of 784** | **5 on 2026-07-08** | **0.6 %** |

A scrape that fails, fails for everything it was covering, so its tickers pile onto ONE
date. Names that stop trading stop on their own days. `summarise` therefore reports the
largest same-date group (`cliff_n`), and only that group is allowed to make a stage
`not ready` — otherwise a handful of permanently-delisted names hold the gate red forever.

⚠️ **THE SHARE IS THE STATISTIC, NOT THE COUNT, AND MEASURING IT CORRECTED THIS FILE'S
FIRST DRAFT.** An absolute floor of 5 tickers was written here first and fired immediately
on the 2026-08-23 corpus — DSE, DZM, KOS, SIP and VPI all stop on 2026-07-08, and they are
delistings, not a failure. What separates the two regimes is two orders of magnitude of
SHARE (0.6 % against 77 %), not a count, and a share also behaves correctly on a
one-ticker schema, where a single stale name is 100 % and genuinely is the whole story.

⚠️ **AND IT CORRECTED CLAUDE.md §6-2-bis / `FRZ-1` IN PASSING.** Both record the 13
survivors of the re-scrape as carrying *"thirteen distinct dates, the signature of
individual delistings"*. They carry **seven** — the parenthetical list in `FRZ-1` says so
itself (`SSN/STL` share one, `DSE/KOS/SIP/VPI/DZM` share another) while the prose counts
tickers. The CONCLUSION survives and was re-verified here another way: each of the 13
raw CafeF price CSVs ends on exactly the date silver holds, so the 2026-08-23 incremental
scrape did attempt them and the source returned nothing after that date. It is the
diagnostic NUMBER that was wrong, which is the argument for this view existing at all.

## Use

    python -m pipeline.freshness                  # every layer's distribution
    python -m pipeline.freshness --layer silver   # one layer
    python -m pipeline.freshness --install        # (re)create the SQL functions

    SELECT * FROM health_schema.ticker_freshness('silver') WHERE NOT is_current;

⚠️ **PASS A LAYER OR PAY FOR ALL OF THEM.** Each layer costs what its table costs —
measured 2026-08-23: silver **1.0 s**, `gold.stocks` **2.9 s**,
`unified_schema_all.pool__basic` **5.2 s**, and `gold.stocks_ta` **26.5 s** because that
table is **17 GB** across 946 columns. `ticker_freshness(NULL)` walks every layer.

## ⚠️ IT IS A FUNCTION AND NOT A VIEW, AND THAT COST ONE FAILED REBUILD TO LEARN

The first version shipped `health_schema.ticker_freshness` as a `UNION ALL` **view** over
every layer. PostgreSQL records a view's dependency on the tables under it, so the very
next rebuild died:

    psycopg2.errors.DependentObjectsStillExist: cannot drop table
    unified_schema_vnm.pool__basic because other objects depend on it
    DETAIL: view health_schema.ticker_freshness depends on table ...

**Every builder in this repo drops and recreates its table** — `_ingest_unified_pool_basic`
opens with `DROP TABLE IF EXISTS`, and so do the silver and gold builders. So a view over
those tables **blocks every repair it exists to recommend**, including the `gold.stocks_ta`
rebuild that had run hours earlier. A monitor that has to be uninstalled before the system
can be fixed is worse than no monitor.

A `plpgsql` function body is not parsed for dependencies, so `DROP TABLE` is unaffected —
and two things fall out for free:

1. **Layers are discovered at CALL time**, not frozen at install time, so a schema built
   later appears on its own with no `--install` re-run.
2. **The layer filter is an ARGUMENT**, so only the requested layer is ever touched — no
   `UNION ALL`, and no reliance on the planner pruning branches on a constant.

⚠️ It also skips a layer whose table is missing (`to_regclass IS NULL`) rather than
raising, so the tool still answers **while a rebuild is mid-flight** — which is exactly
when somebody is watching it.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

# The schema the views live in, named like the layers it measures. It holds no data of its
# own — both objects are VIEWS, so nothing here can go stale behind its own source, which
# is the failure mode this module exists to report.
HEALTH_SCHEMA = "health_schema"
CALENDAR_FUNCTION = "session_calendar"
FRESHNESS_FUNCTION = "ticker_freshness"
LAYERS_FUNCTION = "freshness_layers"

# ⚠️ The reference calendar. Every layer is aged against THIS table's sessions, for the
# reason in the module docstring. `silver_schema.stocks_basic` is the price spine — CafeF
# only, verified in `_ingest_silver_stocks_basic` — and the source of every chain here.
SPINE_SCHEMA = "silver_schema"
SPINE_TABLE = "stocks_basic"

# ⚠️ What SHARE of a table's tickers must share one stale date before it reads as a scrape
# scope rather than as delistings. The two measured regimes are 0.6 % and 77 % (module
# docstring), so any threshold between them is defensible and the exact value is not
# load-bearing; 2 % catches a partition-scoped re-scrape of any size — the freeze is
# always the COMPLEMENT of the scope, so it is the large group — while leaving a normal
# trickle of delistings below the line. ⚠️ A share, not a count: on a one-ticker schema a
# single stale name is 100 % and no absolute floor can express that.
CLIFF_MIN_SHARE = 0.02

# Identifiers are interpolated into SQL — an identifier cannot be a bound parameter — so
# they are validated the way `unified_reader.unified_schema_name` validates a schema.
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def _ident(value: str, what: str) -> str:
    """Return `value` if it is a bare SQL identifier, else raise."""
    if not _IDENT.match(value or ""):
        raise ValueError(
            f"{what} {value!r} is not a bare identifier - it is interpolated into SQL, "
            f"so it must match {_IDENT.pattern}."
        )
    return value


# ---------------------------------------------------------------- the layer registry

# `(layer, schema, table)`. The layer name is what you filter the view on, and it is a
# CONSTANT in that branch's SELECT — which is what lets PostgreSQL prune the other
# branches when you filter on it.
CORE_LAYERS: Tuple[Tuple[str, str, str], ...] = (
    ("silver", SPINE_SCHEMA, SPINE_TABLE),
    ("gold", "gold_schema", "stocks"),
    ("gold_ta", "gold_schema", "stocks_ta"),
)


def discover_layers(driver, database: Optional[str] = None) -> List[Tuple[str, str, str]]:
    """`CORE_LAYERS` plus every `unified_schema_*.pool__basic` that exists.

    ⚠️ The unified half is discovered at INSTALL time and frozen into the view, so a
    schema built later is invisible until `--install` runs again. That is why the view
    records the list it was built from in its `COMMENT`: a reader can then tell "this
    layer is current" apart from "this layer was never in the view".
    """
    layers = list(CORE_LAYERS)
    with driver._cursor_ctx(database) as cur:
        cur.execute(
            """
            SELECT table_schema FROM information_schema.tables
            WHERE table_name = 'pool__basic'
              AND table_schema LIKE 'unified\\_schema\\_%'
            ORDER BY 1
            """
        )
        for (schema,) in cur.fetchall():
            layers.append(
                (schema.replace("unified_schema_", "unified_"), schema, "pool__basic")
            )
    return layers


# ---------------------------------------------------------------------------- the SQL


def calendar_sql() -> str:
    """The reference session calendar: every distinct spine date, ranked backwards.

    `sessions_behind = 0` is the newest session, 1 the one before it. Ranking the dates
    once here is what keeps the per-ticker join cheap — the obvious alternative, counting
    later dates per ticker, is a correlated subquery, and it timed out at 5 minutes the
    first time this was written that way.
    """
    return (
        "SELECT date, (ROW_NUMBER() OVER (ORDER BY date DESC) - 1)::int AS sessions_behind\n"
        f"FROM (SELECT DISTINCT date FROM {_ident(SPINE_SCHEMA, 'schema')}."
        f"{_ident(SPINE_TABLE, 'table')}) d"
    )


def layers_function_sql() -> str:
    """`health_schema.freshness_layers()` — the registry, discovered at CALL time.

    ⚠️ The core rows are literals and the unified rows come from `information_schema`, so a
    schema built after this was installed appears on its own. The first version froze the
    list into a view at install time and had to say so in the view's `COMMENT`; nothing
    has to say so now.
    """
    core = "\n    UNION ALL ".join(
        f"SELECT '{lay}'::text, '{sch}'::text, '{tbl}'::text" for lay, sch, tbl in CORE_LAYERS
    )
    return f"""
CREATE OR REPLACE FUNCTION {HEALTH_SCHEMA}.{LAYERS_FUNCTION}()
RETURNS TABLE (layer text, schema_name text, table_name text)
LANGUAGE sql STABLE AS $fn$
    {core}
    UNION ALL
    SELECT replace(table_schema, 'unified_schema_', 'unified_')::text,
           table_schema::text, 'pool__basic'::text
    FROM information_schema.tables
    WHERE table_name = 'pool__basic' AND table_schema LIKE 'unified\\_schema\\_%'
    ORDER BY 1
$fn$;
"""


def calendar_function_sql() -> str:
    """`health_schema.session_calendar()` — the reference calendar as a function.

    ⚠️ A FUNCTION and not a view, for the reason in the module docstring: the spine is
    dropped and recreated by its own builder, and a view over it would block that.
    """
    return f"""
CREATE OR REPLACE FUNCTION {HEALTH_SCHEMA}.{CALENDAR_FUNCTION}()
RETURNS TABLE (session_date date, sessions_behind int)
LANGUAGE plpgsql STABLE AS $fn$
BEGIN
    RETURN QUERY EXECUTE format(
        'SELECT date, (ROW_NUMBER() OVER (ORDER BY date DESC) - 1)::int
         FROM (SELECT DISTINCT date FROM %I.%I) d',
        {_ident(SPINE_SCHEMA, 'schema')!r}, {_ident(SPINE_TABLE, 'table')!r});
END;
$fn$;
"""


def freshness_function_sql() -> str:
    """`health_schema.ticker_freshness(layer)` — one row per (layer, exchange, ticker).

    ⚠️ The calendar is inlined as a CTE per layer rather than called, so one layer is one
    statement. ⚠️ The join to it is an equality LEFT JOIN with the counting form only as a
    COALESCE fallback — PostgreSQL short-circuits COALESCE, so the expensive form runs only
    for a `last_date` the spine does not carry (a bronze table fresher than silver, say)
    instead of once per ticker.

    ⚠️ A layer whose table has gone missing is SKIPPED, not raised on. A rebuild drops its
    table before it writes one, and that is precisely when someone is watching this.
    """
    return f"""
CREATE OR REPLACE FUNCTION {HEALTH_SCHEMA}.{FRESHNESS_FUNCTION}(layer_filter text DEFAULT NULL)
RETURNS TABLE (layer text, source text, exchange text, ticker text,
               last_date date, sessions_behind int, is_current boolean)
LANGUAGE plpgsql STABLE AS $fn$
DECLARE
    lay record;
BEGIN
    FOR lay IN
        SELECT * FROM {HEALTH_SCHEMA}.{LAYERS_FUNCTION}() f
        WHERE layer_filter IS NULL OR f.layer = layer_filter
    LOOP
        CONTINUE WHEN to_regclass(lay.schema_name || '.' || lay.table_name) IS NULL;
        RETURN QUERY EXECUTE format($q$
            WITH cal AS (
                SELECT date, (ROW_NUMBER() OVER (ORDER BY date DESC) - 1)::int AS sessions_behind
                FROM (SELECT DISTINCT date FROM %I.%I) d
            ), per AS (
                SELECT exchange::text AS exchange, ticker::text AS ticker, MAX(date) AS last_date
                FROM %I.%I GROUP BY 1, 2
            )
            SELECT %L::text, %L::text, p.exchange, p.ticker, p.last_date,
                   COALESCE(c.sessions_behind,
                            (SELECT COUNT(*)::int FROM cal c2 WHERE c2.date > p.last_date)),
                   COALESCE(c.sessions_behind, -1) = 0
            FROM per p LEFT JOIN cal c ON c.date = p.last_date
        $q$, {_ident(SPINE_SCHEMA, 'schema')!r}, {_ident(SPINE_TABLE, 'table')!r},
             lay.schema_name, lay.table_name,
             lay.layer, lay.schema_name || '.' || lay.table_name);
    END LOOP;
END;
$fn$;
"""


def per_ticker_sql(schema: str, table: str) -> str:
    """The same measurement for ONE table, without the views being installed.

    This is what `pipeline.status_data` runs, and it is standalone on purpose: the gate on
    quoting a number must not depend on a DDL step somebody may not have run.
    """
    schema, table = _ident(schema, "schema"), _ident(table, "table")
    return f"""
WITH cal AS ({calendar_sql()}),
     per AS (SELECT exchange, ticker, MAX(date) AS last_date
             FROM {schema}.{table} GROUP BY 1, 2)
SELECT p.exchange, p.ticker, p.last_date,
       COALESCE(c.sessions_behind,
                (SELECT COUNT(*)::int FROM cal c2 WHERE c2.date > p.last_date))
           AS sessions_behind
FROM per p LEFT JOIN cal c ON c.date = p.last_date
"""


# ------------------------------------------------------------------------- reading it


def read_per_ticker(
    driver, schema: str, table: str, database: Optional[str] = None
) -> pd.DataFrame:
    """`(exchange, ticker, last_date, sessions_behind)` for one ticker-keyed table."""
    with driver._cursor_ctx(database) as cur:
        cur.execute(per_ticker_sql(schema, table))
        rows = cur.fetchall()
    return pd.DataFrame(
        rows, columns=["exchange", "ticker", "last_date", "sessions_behind"]
    )


def summarise(frame: pd.DataFrame, cliff_share: float = CLIFF_MIN_SHARE) -> Dict:
    """Reduce a per-ticker frame to the numbers a caller can act on.

    Returns `n_tickers`, `n_current`, `n_stale`, `max_sessions_behind`, `cliff_date`,
    `cliff_n`, `cliff_share` and `is_cliff`. ⚠️ `is_cliff` — never `n_stale` — is the
    alarm: see the module docstring for why a handful of delisted names must not hold a
    gate red, and why the alarm is a SHARE rather than a count.
    """
    if frame is None or frame.empty:
        return {
            "n_tickers": 0,
            "n_current": 0,
            "n_stale": 0,
            "max_sessions_behind": 0,
            "cliff_date": None,
            "cliff_n": 0,
            "cliff_share": 0.0,
            "is_cliff": False,
        }
    behind = frame["sessions_behind"].fillna(0).astype(int)
    stale = frame.loc[behind > 0]
    cliff_date, cliff_n = None, 0
    if not stale.empty:
        groups = stale.groupby("last_date").size().sort_values(ascending=False)
        cliff_date, cliff_n = groups.index[0], int(groups.iloc[0])
    share = cliff_n / len(frame)
    return {
        "n_tickers": int(len(frame)),
        "n_current": int((behind == 0).sum()),
        "n_stale": int(len(stale)),
        "max_sessions_behind": int(behind.max()),
        "cliff_date": cliff_date,
        "cliff_n": cliff_n,
        "cliff_share": round(share, 4),
        "is_cliff": share >= cliff_share,
    }


def describe(summary: Dict) -> str:
    """One ASCII line for a console (CLAUDE.md §5 rule 18 — this prints to cp1252)."""
    if not summary["n_tickers"]:
        return "no tickers"
    text = (
        f"{summary['n_current']}/{summary['n_tickers']} tickers current, "
        f"{summary['n_stale']} stale (worst {summary['max_sessions_behind']} sessions)"
    )
    if summary["is_cliff"] and summary["cliff_share"] >= 1.0:
        text += (
            f"; WARNING FROZEN - EVERY ticker stops on {summary['cliff_date']}, "
            f"so the whole table is behind, not a few names"
        )
    elif summary["is_cliff"]:
        text += (
            f"; WARNING CLIFF - {summary['cliff_n']} tickers ({summary['cliff_share']:.1%})"
            f" all stop on {summary['cliff_date']}, which is a scrape SCOPE, not delistings"
        )
    elif summary["n_stale"]:
        text += (
            f"; largest same-date group {summary['cliff_n']} "
            f"({summary['cliff_share']:.1%}), so it reads as delistings"
        )
    return text


# ------------------------------------------------------------------------------- DDL


def install_functions(driver, database: Optional[str] = None) -> List[str]:
    """Create `health_schema` and its three functions. Returns the function names.

    ⚠️ **Functions, never views** — see the module docstring. A view here blocks every
    `DROP TABLE` in the repo's builders, which is every repair this tool recommends. There
    is also nothing to re-install when a schema is added: the layer list is a query.
    """
    with driver._cursor_ctx(database) as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {HEALTH_SCHEMA}")
        # ⚠️ Drop any view left by the first version, or `CREATE FUNCTION` succeeds while
        # a same-named view goes on blocking rebuilds with nothing pointing at it.
        cur.execute(f"DROP VIEW IF EXISTS {HEALTH_SCHEMA}.{FRESHNESS_FUNCTION}")
        cur.execute(f"DROP VIEW IF EXISTS {HEALTH_SCHEMA}.{CALENDAR_FUNCTION}")
        cur.execute(layers_function_sql())
        cur.execute(calendar_function_sql())
        cur.execute(freshness_function_sql())
        cur.execute(
            f"COMMENT ON FUNCTION {HEALTH_SCHEMA}.{FRESHNESS_FUNCTION}(text) IS "
            f"'Per-ticker freshness, one row per (layer, exchange, ticker). Pass a layer "
            f"or pay for all of them - gold_ta alone costs ~27s (17 GB). A cliff of "
            f"tickers sharing one stale date is a scrape SCOPE; scattered dates are "
            f"delistings. A FUNCTION and not a view, because a view blocks the DROP TABLE "
            f"every builder here opens with. TODO P1 / FRZ-1.'"
        )
        cur.connection.commit()
    return [LAYERS_FUNCTION, CALENDAR_FUNCTION, FRESHNESS_FUNCTION]


# ------------------------------------------------------------------------------- CLI


def main(argv=None) -> int:
    import os
    import sys

    from dotenv import load_dotenv

    argv = list(sys.argv[1:] if argv is None else argv)
    load_dotenv()

    from dtos.tabular_database_driver_dtos.postgre_sql_connection_dto import (
        PostgreSQLConnectionDto,
    )
    from logger.logger import Logger
    from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
    from utils import runtime
    from utils.constants import DATABASE_MAIN_V2

    logger = Logger(file_name="logs/pipeline_freshness")
    driver = PostgreSQLDriver(logger=logger)
    with runtime.RunTimer("pipeline.freshness", show_gpu=False):
        driver.connect(
            PostgreSQLConnectionDto(
                logger=logger,
                host=os.getenv("POSTGRES_HOST", "localhost"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", ""),
                port=int(os.getenv("POSTGRES_PORT", 5432)),
                database=DATABASE_MAIN_V2,
            )
        )
        try:
            if "--install" in argv:
                for name in install_functions(driver):
                    print(f"installed function {HEALTH_SCHEMA}.{name}")
                print()
                print("  SELECT * FROM health_schema.ticker_freshness('silver')")
                print("  WHERE NOT is_current ORDER BY sessions_behind DESC;")
                print()
                print("layers it will walk (discovered at CALL time, not frozen here):")
                for name, schema, table in discover_layers(driver):
                    print(f"  {name:<20} {schema}.{table}")
                return 0

            wanted = argv[argv.index("--layer") + 1] if "--layer" in argv else None
            layers = discover_layers(driver)
            if wanted:
                layers = [lay for lay in layers if lay[0] == wanted]
                if not layers:
                    print(f"no layer named {wanted!r}")
                    return 1
            rows = []
            for name, schema, table in layers:
                summary = summarise(read_per_ticker(driver, schema, table))
                print(f"{name:<20} {schema}.{table}")
                print(f"                     {describe(summary)}")
                rows.append({"layer": name, **summary})
            print()
            print(pd.DataFrame(rows).to_string(index=False))
            return 0
        finally:
            driver.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
