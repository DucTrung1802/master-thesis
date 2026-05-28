from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import reduce
import os
import threading
from typing import List

import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values

from dtos.tabular_database_driver_dtos.postgre_sql_connection_dto import (
    PostgreSQLConnectionDto,
)
from dtos.tabular_database_driver_dtos.tabular_database_driver_dtos import *
from logger.logger import Logger
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from ta.ta_functions import add_one_for_all_ta
from utils.enums import *
from utils.switch_handler import SwitchHandler
from utils.utils import *

load_dotenv()


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG DATACLASSES
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class MarketIndexConfig:
    """
    Describes one market index row from ``stock_market.g_stock_market``
    that should be joined into the unified stock table.

    Joined columns follow the template: ``<prefix>_<original_column>``

    Example
    -------
    ``MarketIndexConfig(index_code="VNINDEX", prefix="vnindex")``
    produces columns like ``vnindex_close``, ``vnindex_open``, …

    Parameters
    ----------
    index_code : str
        The ``code`` value stored in ``g_stock_market``,
        e.g. ``"VNINDEX"``, ``"HNX-INDEX"``, ``"UPCOM-INDEX"``.
    prefix : str
        Prefix applied to every joined column,
        e.g. ``"vnindex"`` → ``vnindex_close``, ``vnindex_high``, …
    columns : list[str] | None
        Explicit allow-list of source columns to keep (do not include
        ``date`` or ``code``).
        ``None`` → keep every numeric column automatically.
    """

    index_code: str
    prefix: str
    columns: List[str] | None = field(default=None)


# ──────────────────────────────────────────────────────────────────────────────
# DATA POSTPROCESSOR
# ──────────────────────────────────────────────────────────────────────────────


class DataPostprocessor:

    def __init__(
        self,
        logger: Logger,
        switch_handler: SwitchHandler,
        stock_list: List[str],
        # ── enrichment toggles / configs ──────────────────────────────────────
        include_macroeconomics: bool = True,
        market_index_configs: List[MarketIndexConfig] | None = None,
    ):
        self._logger = logger
        self._switch_handler: SwitchHandler = switch_handler
        self._database_driver = PostgreSQLDriver(logger=logger)
        self._connect_to_database(database_name=os.getenv("GOLD_POSTGRES_DATABASE"))

        self._stock_list: List[str] = stock_list

        # Whether to left-join the unified_macroeconomic table on date.
        self._include_macroeconomics: bool = include_macroeconomics

        # List of market index codes to left-join from g_stock_market on date.
        # Pass an empty list (or None) to skip stock-market enrichment.
        self._market_index_configs: List[MarketIndexConfig] = market_index_configs or []

    # ──────────────────────────────────────────────────────────────────────────
    # DATABASE HELPERS
    # ──────────────────────────────────────────────────────────────────────────

    def _connect_to_database(self, database_name: str = "postgres") -> None:
        connection_model = PostgreSQLConnectionDto(
            logger=self._logger,
            host=os.getenv("POSTGRES_HOST"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("POSTGRES_PORT"),
            database=database_name,
        )
        return self._database_driver.connect(connection_model)

    def _infer_sql_type(self, dtype) -> str:
        dtype_str = str(dtype)
        if dtype_str.startswith("int32") or dtype_str == "Int32":
            return DataType.INT()
        elif dtype_str.startswith("int") or dtype_str == "Int64":
            return DataType.BIGINT()
        elif dtype_str.startswith("float"):
            return DataType.DECIMAL()
        elif dtype_str == "bool":
            return DataType.BOOLEAN()
        elif dtype_str.startswith("datetime"):
            return DataType.TIMESTAMP()
        else:
            return DataType.VARCHAR()  # covers "object" and unknown

    def _ensure_table_exists(
        self,
        schema_name: str,
        table_name: str,
        primary_keys: List[str],
        df: pd.DataFrame,
        dtype_overrides: dict[str, str] | None = None,
    ) -> None:
        columns = [
            Column(
                name=col,
                data_type=(
                    dtype_overrides[col]
                    if dtype_overrides and col in dtype_overrides
                    else self._infer_sql_type(df[col].dtype)
                ),
                nullable=(col not in primary_keys),
            )
            for col in df.columns
        ]
        # create_table uses IF NOT EXISTS — safe to call every time
        self._database_driver.create_table(
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            primary_keys=primary_keys,
        )

    def _build_upsert_sql(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        has_update_date: bool = False,
    ) -> str:
        col_str = ", ".join(columns)
        pk_str = ", ".join(primary_keys)

        update_parts = [f"{c} = EXCLUDED.{c}" for c in columns if c not in primary_keys]
        if has_update_date:
            update_parts.append("update_date = now()")
        update_str = ", ".join(update_parts)

        return f"""
    WITH upserted AS (
        INSERT INTO {schema_name}.{table_name} ({col_str})
        VALUES %s
        ON CONFLICT ({pk_str})
        DO UPDATE SET {update_str}
        RETURNING xmax
    )
    SELECT
        COUNT(*) FILTER (WHERE xmax = 0)  AS inserted,
        COUNT(*) FILTER (WHERE xmax <> 0) AS updated
    FROM upserted
    """

    def _to_python(self, v):
        """Convert numpy scalars to native Python types psycopg2 can serialise."""
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass  # pd.isna raises on some types (e.g. lists) — just pass through
        if hasattr(v, "item"):  # catches all numpy scalars: uint32, int64, float32 …
            return v.item()
        return v

    def _save_pandas_table_to_database(
        self,
        schema_name: str,
        table_name: str,
        primary_keys: List[str],
        df: pd.DataFrame,
        dtype_overrides: dict[str, str] | None = None,
        chunk_size: int = 5_000,
        max_workers: int | None = None,
    ) -> None:
        self._logger.log_info(
            f'Saving dataframe to table "{schema_name}.{table_name}".'
        )

        df = df.dropna(how="all")
        if df.empty:
            self._logger.log_info("DataFrame is empty after cleaning. Nothing to save.")
            return

        self._ensure_table_exists(
            schema_name=schema_name,
            table_name=table_name,
            primary_keys=primary_keys,
            df=df,
            dtype_overrides=dtype_overrides,
        )

        with self._database_driver._cursor_ctx() as _probe_cur:
            available_columns = self._database_driver._get_table_columns(
                _probe_cur, schema_name, table_name
            )
        has_create_date = "create_date" in available_columns
        has_update_date = "update_date" in available_columns

        df_columns = list(df.columns)
        insert_columns = list(df_columns)
        if has_create_date and "create_date" not in insert_columns:
            insert_columns.append("create_date")

        sql = self._build_upsert_sql(
            schema_name=schema_name,
            table_name=table_name,
            columns=insert_columns,
            primary_keys=primary_keys,
            has_update_date=has_update_date,
        )

        now = datetime.now(timezone.utc) if has_create_date else None
        records: list[tuple] = [
            tuple(self._to_python(v) for v in row)
            + ((now,) if has_create_date and "create_date" not in df_columns else ())
            for row in df.itertuples(index=False, name=None)
        ]

        chunks: list[list[tuple]] = [
            records[start : start + chunk_size]
            for start in range(0, len(records), chunk_size)
        ]

        inserted_total = 0
        updated_total = 0
        counter_lock = threading.Lock()

        def _upsert_chunk(chunk: list[tuple]) -> None:
            nonlocal inserted_total, updated_total
            with self._database_driver._cursor_ctx() as cur:
                execute_values(cur, sql, chunk, page_size=len(chunk))
                row = cur.fetchone()
            if row:
                with counter_lock:
                    inserted_total += row[0] or 0
                    updated_total += row[1] or 0

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_upsert_chunk, chunk) for chunk in chunks]
            for future in futures:
                future.result()

        self._logger.log_info(
            f"Saved {inserted_total + updated_total}/{len(df)} records into "
            f"'{schema_name}.{table_name}'. "
            f"(Inserted: {inserted_total}, Updated: {updated_total})"
        )

    def _select(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str] = None,
        join_model_list: List[JoinModel] = None,
        conditions: List[Condition] = None,
        order_by: List[str] = None,
        limit: int = None,
    ) -> pd.DataFrame:
        return self._database_driver.select(
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            join_model_list=join_model_list,
            conditions=conditions,
            order_by=order_by,
            limit=limit,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # SHARED UTILITY — date normalisation before any join
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _normalise_date_column(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
        """
        Convert *col* to timezone-naive, midnight-only Timestamps.

        The ``g_stock_market`` table stores dates as TIMESTAMP
        (``"2024-01-15 00:00:00"``), while ``g_enterprise`` may store them as
        DATE (``"2024-01-15"``).  Without this normalisation the pandas merge
        would fail to match rows even when the calendar date is identical.
        """
        df = df.copy()
        s = pd.to_datetime(df[col], utc=False)
        if s.dt.tz is not None:
            s = s.dt.tz_localize(None)  # strip timezone
        df[col] = s.dt.normalize()  # zero out the time component
        return df

    # ──────────────────────────────────────────────────────────────────────────
    # region MACROECONOMICS JOIN
    # ──────────────────────────────────────────────────────────────────────────

    def _join_macroeconomics_columns(self, stock_df: pd.DataFrame) -> pd.DataFrame:
        """
        Left-join every column from ``macroeconomics.unified_macroeconomic``
        into *stock_df* on ``date``.

        The unified_macroeconomic table already has descriptive column names
        so no prefix is applied here.  Rows in *stock_df* whose date has no
        matching macroeconomic record will receive ``NULL`` for those columns.

        Parameters
        ----------
        stock_df : pd.DataFrame
            The per-stock dataframe being built.  Must contain a ``date``
            column already normalised by ``_normalise_date_column``.

        Returns
        -------
        pd.DataFrame
            *stock_df* enriched with macroeconomic columns.
        """
        self._logger.log_info(
            "  [MACROECONOMICS] Fetching from "
            f"{Schema.MACROECONOMICS.value}.{Table.UNIFIED_MACROECONOMIC.name} …"
        )

        macro_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.UNIFIED_MACROECONOMIC.name,
            order_by=[Table.UNIFIED_MACROECONOMIC.Column.DATE.value],
        )

        if macro_df.empty:
            self._logger.log_info(
                "  [MACROECONOMICS] unified_macroeconomic is empty — skipping."
            )
            return stock_df

        macro_df = self._normalise_date_column(macro_df)

        # Drop the primary-key column; we join on date only
        macro_df = macro_df.drop(
            columns=[c for c in macro_df.columns if c == "code"],
            errors="ignore",
        )

        before_cols = len(stock_df.columns)
        merged = stock_df.merge(macro_df, on="date", how="left")

        new_cols = [c for c in macro_df.columns if c != "date"]
        matched = merged[new_cols].notna().all(axis=1).sum()

        self._logger.log_info(
            f"  [MACROECONOMICS] Joined {len(new_cols)} columns, "
            f"{matched}/{len(stock_df)} rows matched "
            f"({len(merged.columns) - before_cols} net new columns added)."
        )
        return merged

    # endregion MACROECONOMICS JOIN

    # ──────────────────────────────────────────────────────────────────────────
    # region STOCK MARKET INDEX JOIN
    # ──────────────────────────────────────────────────────────────────────────

    def _fetch_single_index_df(self, config: MarketIndexConfig) -> pd.DataFrame:
        """
        Fetch one market index from ``stock_market.g_stock_market``,
        filter to *config.index_code*, keep the requested columns, then
        rename every non-date column using the template::

            <prefix>_<original_column>

        Example
        -------
        ``config = MarketIndexConfig("VNINDEX", "vnindex")``

        produces columns: ``vnindex_open``, ``vnindex_high``, ``vnindex_low``,
        ``vnindex_close``, …

        Parameters
        ----------
        config : MarketIndexConfig
            Which index to load, which prefix to apply, and optionally which
            source columns to keep.

        Returns
        -------
        pd.DataFrame
            Columns: ``date`` + ``<prefix>_<col>`` for every kept column.
            Empty DataFrame (only ``date`` column) when no data is found.
        """
        self._logger.log_info(
            f"  [STOCK MARKET] Fetching index '{config.index_code}' "
            f"(prefix='{config.prefix}') …"
        )

        raw_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,  # "stock_market"
            table_name=Table.G_STOCK_MARKET.name,  # "g_stock_market"
            conditions=[
                Condition(
                    column=Table.G_STOCK_MARKET.Column.CODE.value,  # "code"
                    operator=SqlOperator.EQUAL_TO,  # "="
                    value=config.index_code,
                    data_type=DataType.VARCHAR,
                )
            ],
            order_by=[Table.G_STOCK_MARKET.Column.DATE.value],
        )

        if raw_df.empty:
            self._logger.log_info(
                f"  [STOCK MARKET] No rows found for '{config.index_code}' — skipping."
            )
            return pd.DataFrame(columns=["date"])

        # ── decide which source columns to keep ───────────────────────────────
        _pk_cols = {
            Table.G_STOCK_MARKET.Column.DATE.value,  # "date"
            Table.G_STOCK_MARKET.Column.CODE.value,  # "code"
        }

        # psycopg2 returns PostgreSQL NUMERIC/DECIMAL columns as Python Decimal
        # objects, which pandas stores as object dtype.  pd.api.types.is_numeric_dtype()
        # returns False for object columns, so price/value columns (open, high, low,
        # close, adjust, matching_value …) would be silently dropped.
        # Fix: coerce every non-PK object column to float before the dtype check.
        for c in raw_df.columns:
            if c not in _pk_cols and raw_df[c].dtype == object:
                converted = pd.to_numeric(raw_df[c], errors="coerce")
                if converted.notna().sum() / len(raw_df) >= 0.9:
                    raw_df[c] = converted

        if config.columns:
            # Caller supplied an explicit allow-list
            keep_cols = [c for c in config.columns if c in raw_df.columns]
            missing = [c for c in config.columns if c not in raw_df.columns]
            if missing:
                self._logger.log_info(
                    f"  [STOCK MARKET] Warning — columns not found in "
                    f"g_stock_market for '{config.index_code}': {missing}"
                )
        else:
            # Default: every numeric column that is not a primary-key column
            keep_cols = [
                c
                for c in raw_df.columns
                if c not in _pk_cols and pd.api.types.is_numeric_dtype(raw_df[c])
            ]

        self._logger.log_info(
            f"  [STOCK MARKET] Keeping {len(keep_cols)} columns for "
            f"prefix '{config.prefix}': {keep_cols}"
        )

        # ── apply prefix template: <prefix>_<original_column> ────────────────
        index_df = raw_df[["date"] + keep_cols].copy()
        index_df.rename(
            columns={col: f"{config.prefix}_{col}" for col in keep_cols},
            inplace=True,
        )

        index_df = self._normalise_date_column(index_df)
        index_df.sort_values("date", inplace=True)
        index_df.reset_index(drop=True, inplace=True)

        self._logger.log_info(
            f"  [STOCK MARKET] '{config.index_code}' → "
            f"{len(index_df)} rows, {len(index_df.columns) - 1} prefixed columns."
        )
        return index_df

    def _join_stock_market_columns(self, stock_df: pd.DataFrame) -> pd.DataFrame:
        """
        Left-join every configured market index into *stock_df* on ``date``.

        Each index in ``self._market_index_configs`` is fetched individually
        from ``g_stock_market`` and joined in sequence.  Column names follow
        the template ``<prefix>_<original_column>`` defined per index.

        Parameters
        ----------
        stock_df : pd.DataFrame
            The per-stock dataframe being built.  Must contain a ``date``
            column already normalised by ``_normalise_date_column``.

        Returns
        -------
        pd.DataFrame
            *stock_df* enriched with market index columns.
        """
        if not self._market_index_configs:
            self._logger.log_info(
                "  [STOCK MARKET] No MarketIndexConfig provided — skipping."
            )
            return stock_df

        for config in self._market_index_configs:
            index_df = self._fetch_single_index_df(config)

            if len(index_df.columns) <= 1:
                # Only 'date' column → fetch returned nothing useful
                continue

            before_cols = len(stock_df.columns)
            stock_df = stock_df.merge(index_df, on="date", how="left")

            new_cols = [c for c in index_df.columns if c != "date"]
            matched = stock_df[new_cols].notna().all(axis=1).sum()
            self._logger.log_info(
                f"  [STOCK MARKET] Joined '{config.index_code}' "
                f"(prefix='{config.prefix}'): "
                f"{len(new_cols)} new columns, "
                f"{matched}/{len(stock_df)} rows matched "
                f"({len(stock_df.columns) - before_cols} net new columns added)."
            )

        return stock_df

    # endregion STOCK MARKET INDEX JOIN

    # ──────────────────────────────────────────────────────────────────────────
    # PUBLIC API
    # ──────────────────────────────────────────────────────────────────────────

    def export_unified_dataframe(self) -> pd.DataFrame:

        if not self._switch_handler.is_enabled(
            "data_postprocessor", "export_unified_dataframe"
        ):
            return

        if not self._stock_list:
            self._logger.log_info("Stock list is empty. Nothing to export.")
            return

        self._logger.log_info(
            f"Exporting unified dataframe of stocks: {self._stock_list}"
        )

        self._connect_to_database(database_name=os.getenv("GOLD_POSTGRES_DATABASE"))

        stock_list = [str.lower(item) for item in self._stock_list]

        all_stock_code_df = self._select(
            schema_name=Schema.ENTERPRISE.value,
            table_name=Table.G_ENTERPRISE.name,
            conditions=[
                Condition(
                    column=Table.G_ENTERPRISE.Column.CODE.value,
                    operator=SqlOperator.IN,
                    value=stock_list,
                    data_type=DataType.VARCHAR,
                    column_func="lower",
                )
            ],
        )

        for stock_code in stock_list:

            self._logger.log_info(f"─── Processing '{stock_code}' ───")

            # ── base enterprise data ───────────────────────────────────────────
            stock_df = all_stock_code_df[
                all_stock_code_df["code"].str.lower() == stock_code.lower()
            ].copy()

            # Drop columns with too much missing data
            data_lack_threshold = 0.01
            missing_data_columns = [
                col
                for col in stock_df.columns
                if col != "date"
                and stock_df[col].isna().sum() / len(stock_df) > data_lack_threshold
            ]
            self._logger.log_info(
                f"Dropping {len(missing_data_columns)} columns with >"
                f"{data_lack_threshold * 100}% missing data: {missing_data_columns}"
            )
            stock_df = stock_df.drop(columns=missing_data_columns)

            # Drop columns with only one unique value
            single_value_columns = [
                col
                for col in stock_df.columns
                if col not in ["date", "code"]
                and stock_df[col].nunique(dropna=False) <= 1
            ]
            self._logger.log_info(
                f"Dropping {len(single_value_columns)} single-value columns: "
                f"{single_value_columns}"
            )
            stock_df = stock_df.drop(columns=single_value_columns)

            stock_df = stock_df.dropna()
            self._logger.log_info(
                f"Base enterprise data: {len(stock_df)} rows, "
                f"{len(stock_df.columns)} columns."
            )

            # Normalise date column once — ensures clean DATE-level merges below
            stock_df = self._normalise_date_column(stock_df)

            # ── region MACROECONOMICS JOIN ─────────────────────────────────────
            if self._include_macroeconomics:
                stock_df = self._join_macroeconomics_columns(stock_df)
            # endregion MACROECONOMICS JOIN

            # ── region STOCK MARKET INDEX JOIN ────────────────────────────────
            stock_df = self._join_stock_market_columns(stock_df)
            # endregion STOCK MARKET INDEX JOIN

            self._logger.log_info(
                f"Final unified dataframe for '{stock_code}': "
                f"{len(stock_df)} rows, {len(stock_df.columns)} columns."
            )

            # Drop existing unified table so schema is always rebuilt from scratch
            self._database_driver.drop_table(
                schema_name=Schema.ENTERPRISE.value,
                table_name=f"unified_{stock_code}",
            )

            self._save_pandas_table_to_database(
                schema_name=Schema.ENTERPRISE.value,
                table_name=f"unified_{stock_code}",
                primary_keys=["code", "date"],
                df=stock_df,
            )
