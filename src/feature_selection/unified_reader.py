# src\feature_selection\unified_reader.py
"""Read `unified_schema_<ticker>` and join its pools on the keys they share.

One class, `UnifiedSchemaReader`. It knows three things the rest of the package
does not have to:

1. **The schema name is a template, not a value.** `unified_schema_{ticker}` is
   interpolated into SQL as an IDENTIFIER, and an identifier cannot be a bound
   parameter — so `unified_schema_name` validates the ticker against the same
   pattern `DataPreprocessor._helper_unified_schema` uses and raises rather than
   interpolating anything else.

2. **`numeric` comes back as `Decimal`.** psycopg2 maps PostgreSQL `numeric` to
   Python `Decimal`, which pandas carries as dtype `object` — so `close_adjust`
   would arrive looking like a string column and every correlation, model fit and
   `.mean()` downstream would either fail or silently coerce. `read()` casts by
   the SQL type read from `information_schema`, not by guessing from the values.

3. **Every pool is keyed `(date, exchange, ticker)`** — `UNIFIED_PRIMARY_KEY`, in
   that order, as of 2026-08-04. It was not always so: `pool__targets` used to be
   keyed on `date` alone, because a one-company label table has nowhere to put a
   ticker, and that made every join to it a special case. So `join()` still uses the
   INTERSECTION of `KEY_COLS` present in both frames, requires `date` to be in it,
   and records what it used in `join_log` — a pool that predates the change joins
   correctly rather than silently wrongly.

⚠️ **The join is validated one-to-one, not merged hopefully.** A duplicated key on
either side turns a 4,235-row panel into a longer one that still looks like a
panel, and a feature-selection run on it would be fitting on repeated rows. Both
sides are checked for duplicate keys before the merge and the row count is
asserted after it.
"""

import os
import re
from typing import Dict, List, Optional, Sequence

import pandas as pd
from dotenv import load_dotenv

from dtos.tabular_database_driver_dtos.postgre_sql_connection_dto import (
    PostgreSQLConnectionDto,
)
from logger.logger import Logger
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from utils.constants import DATABASE_MAIN_V2

# The unified layer's schema template. `src/orchestration/assets/unified.py` builds
# `unified_schema_vcb`; anything else that follows the template is readable here
# with no change but the ticker.
UNIFIED_SCHEMA_TEMPLATE = "unified_schema_{ticker}"

# Same pattern as `DataPreprocessor.UNIFIED_TICKER_PATTERN` — see the module
# docstring for why a regex sits between a ticker and a schema name.
TICKER_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,20}$")

# The candidate join keys, in the order a composite key is written — the same order
# as `DataPreprocessor.UNIFIED_PRIMARY_KEY`, and `date` leads for the same reason:
# every access pattern here is time-ordered.
#
# ⚠️ As of 2026-08-04 EVERY `pool__*` table carries all three, so this is the join
# key rather than a menu of candidates. It is still intersected per pair, because a
# pool that predates the change (or a hand-built one) would otherwise join wrongly
# instead of failing — `join()` raises if the intersection loses `date`.
KEY_COLS = ("date", "exchange", "ticker")

# information_schema.data_type values that must land as float64 rather than object.
_NUMERIC_SQL_TYPES = frozenset(
    {
        "numeric",
        "decimal",
        "double precision",
        "real",
        "integer",
        "bigint",
        "smallint",
    }
)
_DATE_SQL_TYPES = frozenset(
    {"date", "timestamp without time zone", "timestamp with time zone"}
)

# ⚠️ Anchored to the repo root, not to the CWD. `Logger` takes a RELATIVE path and
# creates the directory, so a CWD-relative default drops a stray `logs/` beside
# whatever notebook happened to run — which is exactly what it did the first time.
DEFAULT_LOG_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "logs",
    "feature_selection",
)


def unified_schema_name(ticker: str) -> str:
    """`unified_schema_<ticker>`, lowercased, with the ticker validated.

    Raises:
        ValueError: if `ticker` is not a bare alphanumeric identifier. It is
            interpolated into a schema NAME, which cannot be parameterised.
    """
    if not TICKER_PATTERN.match(ticker or ""):
        raise ValueError(
            f"ticker {ticker!r} is not a valid identifier — it is interpolated "
            f"into a schema name, so it must match {TICKER_PATTERN.pattern}."
        )
    return UNIFIED_SCHEMA_TEMPLATE.format(ticker=ticker.lower())


class UnifiedSchemaReader:
    """Read and join the `pool__*` tables of one ticker's unified schema.

    Usage:
        reader = UnifiedSchemaReader("VCB").connect()
        panel = reader.join(["pool__basic", "pool__targets"])
        reader.close()

    Or as a context manager, which closes the connection on the way out:
        with UnifiedSchemaReader("VCB") as reader:
            panel = reader.join(["pool__basic", "pool__targets"])
    """

    def __init__(
        self,
        ticker: str,
        database: str = DATABASE_MAIN_V2,
        logger: Optional[Logger] = None,
        log_file: str = DEFAULT_LOG_FILE,
    ):
        self.ticker = ticker.upper()
        self.schema = unified_schema_name(ticker)
        self.database = database
        self._logger = logger or Logger(file_name=log_file)
        self._driver = PostgreSQLDriver(logger=self._logger)
        self._connected = False
        # What `join()` actually joined on, one row per merge step. Read it — the
        # keys are derived from the frames, so this is the only record of whether
        # `ticker` took part in a given join or `date` carried it alone.
        self.join_log: List[Dict] = []
        # ⚠️ **WHICH POOL EACH CHANNEL CAME FROM — the only place that knows.**
        # `{table: [non-key columns it contributed]}`, filled by `join()`. Downstream
        # this is the difference between a fact and a guess: `feature_selection.
        # outstanding` used to infer the pool from the channel NAME, which returns
        # `unknown` for every pool added since 2026-08-10 (forex, funds, bonds,
        # stock_market, basic_bank) and, worse, silently names `pool__ta` for a forex
        # channel in a run that joined both. `feature_selection/contract.py` has the
        # measured table. `report.write_report` records this into `metadata.json`.
        self.columns_by_table: Dict[str, List[str]] = {}

    # ---------------------------------------------------------------- lifecycle

    def connect(self) -> "UnifiedSchemaReader":
        """Open the connection from the `.env` credentials. Returns self."""
        load_dotenv()
        self._driver.connect(
            PostgreSQLConnectionDto(
                logger=self._logger,
                host=os.getenv("POSTGRES_HOST", "localhost"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", ""),
                port=int(os.getenv("POSTGRES_PORT", 5432)),
                database=self.database,
            )
        )
        self._connected = True
        return self

    def close(self) -> None:
        if self._connected:
            self._driver.disconnect()
            self._connected = False

    def __enter__(self) -> "UnifiedSchemaReader":
        return self.connect()

    def __exit__(self, *exc) -> None:
        self.close()

    @property
    def driver(self) -> PostgreSQLDriver:
        """The underlying driver, for the odd query this class does not wrap."""
        return self._driver

    # ------------------------------------------------------------- introspection

    def tables(self, prefix: str = "") -> List[str]:
        """Base tables and views in the schema, optionally filtered by prefix.

        ⚠️ Filtered in Python, not with SQL `LIKE`: `LIKE 'pool__%'` treats `_` as
        a single-character wildcard, so it would also match `poolXX...`.
        """
        with self._driver._cursor_ctx() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s ORDER BY table_name",
                (self.schema,),
            )
            names = [row[0] for row in cur.fetchall()]
        if not names:
            raise ValueError(
                f"schema {self.schema!r} holds no tables — materialise them first "
                f'(dagster asset materialize --select "group:unified_'
                f'{self.ticker.lower()}").'
            )
        return [n for n in names if n.startswith(prefix)]

    def pools(self) -> List[str]:
        """The `pool__*` tables — the pre-selection feature groups."""
        return self.tables(prefix="pool__")

    def column_types(self, table: str) -> Dict[str, str]:
        """`{column: information_schema.data_type}`, in ordinal position order."""
        with self._driver._cursor_ctx() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (self.schema, table),
            )
            types = {name: dtype for name, dtype in cur.fetchall()}
        if not types:
            raise ValueError(
                f"{self.schema}.{table} does not exist. Available: {self.tables()}"
            )
        return types

    def overview(self) -> pd.DataFrame:
        """One row per table: row count, column count, key columns, date range.

        The orientation table for a schema you have not opened before — it is what
        tells you `pool__targets` has no `ticker` before a join silently decides
        that for you.
        """
        rows = []
        for table in self.tables():
            types = self.column_types(table)
            keys = [k for k in KEY_COLS if k in types]
            with self._driver._cursor_ctx() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.schema}.{table}")
                n_rows = int(cur.fetchone()[0])
                first = last = None
                if "date" in types:
                    cur.execute(
                        f"SELECT MIN(date), MAX(date) FROM {self.schema}.{table}"
                    )
                    first, last = cur.fetchone()
            rows.append(
                {
                    "table": table,
                    "rows": n_rows,
                    "columns": len(types),
                    "key_columns": ", ".join(keys),
                    "first_date": first,
                    "last_date": last,
                }
            )
        return pd.DataFrame(rows)

    # -------------------------------------------------------------------- read

    def read(
        self,
        table: str,
        columns: Optional[Sequence[str]] = None,
        order_by: Sequence[str] = ("date",),
    ) -> pd.DataFrame:
        """Read one table, typed from `information_schema` rather than inferred.

        `numeric` → float64 and `date` → datetime64, because psycopg2 hands both
        back as Python objects that pandas stores as dtype `object`.
        """
        types = self.column_types(table)
        if columns is not None:
            unknown = [c for c in columns if c not in types]
            if unknown:
                raise ValueError(f"{self.schema}.{table} has no column(s) {unknown}")
        order = [c for c in order_by if c in types] or None

        frame = self._driver.select(
            schema_name=self.schema,
            table_name=table,
            columns=list(columns) if columns else None,
            order_by=order,
        )
        if frame.empty:
            raise ValueError(f"{self.schema}.{table} is empty.")

        for column in frame.columns:
            sql_type = types.get(column, "")
            if sql_type in _NUMERIC_SQL_TYPES:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
            elif sql_type in _DATE_SQL_TYPES:
                frame[column] = pd.to_datetime(frame[column])
        return frame

    # -------------------------------------------------------------------- join

    def join(
        self,
        tables: Sequence[str],
        how: str = "inner",
    ) -> pd.DataFrame:
        """Join the given tables on the keys each PAIR shares, left to right.

        The keys are the intersection of `KEY_COLS` present in both frames — for
        two current pools that is all three of `(date, exchange, ticker)`. Every
        join is checked one-to-one on both sides first, so a duplicated key raises
        instead of quietly multiplying rows.

        Args:
            tables: pool names, e.g. `["pool__basic", "pool__targets"]`.
            how: passed to `DataFrame.merge`. `"inner"` keeps only fully-joined
                rows; `"left"` keeps the first table's spine.

        Raises:
            ValueError: no shared key, `date` absent from the shared key,
                duplicate keys on either side, or an empty join result.
        """
        if len(tables) < 1:
            raise ValueError("join() needs at least one table.")
        self.join_log = []
        self.columns_by_table = {}

        panel = self.read(tables[0])
        self.columns_by_table[tables[0]] = [
            c for c in panel.columns if c not in KEY_COLS
        ]
        for table in tables[1:]:
            right = self.read(table)
            keys = [k for k in KEY_COLS if k in panel.columns and k in right.columns]
            if "date" not in keys:
                raise ValueError(
                    f"{table} shares no `date` column with the frame built from "
                    f"{tables[0]}… — shared keys were {keys or 'none'}. Every "
                    f"unified pool is a daily panel; a join without `date` would "
                    f"be a cross product."
                )
            for name, frame in ((tables[0], panel), (table, right)):
                duplicated = int(frame.duplicated(subset=keys).sum())
                if duplicated:
                    raise ValueError(
                        f"{name} has {duplicated} duplicate row(s) on {keys} — a "
                        f"merge would multiply rows and the selection would fit on "
                        f"repeats."
                    )
            # Overlapping non-key columns would become `_x`/`_y` and silently
            # double a feature. Drop the right-hand copy and say so.
            overlap = [
                c
                for c in right.columns
                if c in panel.columns and c not in keys
            ]
            if overlap:
                right = right.drop(columns=overlap)

            # ⚠️ Recorded AFTER the overlap drop, so a column two pools both carry is
            # attributed to the pool it is actually READ FROM — the left one. Recording
            # it before would name a table whose copy of the column the panel does not
            # hold, and `final_features` would then SELECT the wrong pool's version.
            self.columns_by_table[table] = [
                c for c in right.columns if c not in keys
            ]

            before = len(panel)
            panel = panel.merge(right, on=keys, how=how, validate="one_to_one")
            self.join_log.append(
                {
                    "table": table,
                    "join_keys": ", ".join(keys),
                    "how": how,
                    "rows_before": before,
                    "rows_after": len(panel),
                    "columns_added": right.shape[1] - len(keys),
                    "duplicate_columns_dropped": ", ".join(overlap),
                }
            )

        if panel.empty:
            raise ValueError(
                f"joining {list(tables)} produced 0 rows — the pools do not share "
                f"a calendar."
            )
        if "date" in panel.columns:
            panel = panel.sort_values("date").reset_index(drop=True)
        return panel
