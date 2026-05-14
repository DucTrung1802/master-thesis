from concurrent.futures import ThreadPoolExecutor
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


class DataPostprocessor:
    def __init__(
        self, logger: Logger, switch_handler: SwitchHandler, stock_list: List[str]
    ):
        self._logger = logger
        self._switch_handler: SwitchHandler = switch_handler
        self._database_driver = PostgreSQLDriver(logger=logger)
        self._connect_to_database(database_name=os.getenv("GOLD_POSTGRES_DATABASE"))

        self._stock_list: List[str] = stock_list

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
        # create_table now uses IF NOT EXISTS internally — safe to call every time
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
        """Convert numpy scalars to native Python types psycopg2 can serialize."""
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass  # pd.isna raises on some types (e.g. lists) — just pass them through
        if hasattr(
            v, "item"
        ):  # catches all numpy scalars: uint32, int64, float32, etc.
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
        max_workers: int | None = None,  # None → let ThreadPoolExecutor decide
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

        # ── schema introspection (uses driver cache — no extra DB round-trip) ──
        # We need a real cursor for _get_table_columns; open a short-lived one.
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

        # ── build all tuples once, before spawning threads ────────────────────
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

        # ── shared counters, protected by a lock ─────────────────────────────
        inserted_total = 0
        updated_total = 0
        counter_lock = threading.Lock()

        def _upsert_chunk(chunk: list[tuple]) -> None:
            """Run one chunk in its own cursor. Thread-safe — no shared state."""
            nonlocal inserted_total, updated_total

            with self._database_driver._cursor_ctx() as cur:
                execute_values(cur, sql, chunk, page_size=len(chunk))
                row = cur.fetchone()

            if row:
                with counter_lock:
                    inserted_total += row[0] or 0
                    updated_total += row[1] or 0

        # ── fan out — one thread per chunk ────────────────────────────────────
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_upsert_chunk, chunk) for chunk in chunks]

            for future in futures:
                # Re-raise any exception from the worker thread
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

    def export_unified_dataframe(self) -> pd.DataFrame:

        if self._switch_handler.is_enabled(
            "data_postprocessor", "export_unified_dataframe"
        ):
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

                self._logger.log_info(f"Processing {stock_code}...")

                stock_code_df = all_stock_code_df[
                    all_stock_code_df["code"].str.lower() == stock_code.lower()
                ]

                # Drop existng unified table if any
                self._database_driver.drop_table(
                    schema_name=Schema.ENTERPRISE.value,
                    table_name=f"unified_{stock_code}",  # Template name in database
                )

                # Drop lack data columns
                data_lack_threshold = 0.01
                columns_to_drop = [
                    col
                    for col in stock_code_df.columns
                    if col != "date"
                    and stock_code_df[col].isna().sum() / len(stock_code_df)
                    > data_lack_threshold
                ]

                self._logger.log_info(
                    f"Dropping {len(columns_to_drop)} columns with more than {data_lack_threshold * 100}% missing data: {columns_to_drop}"
                )
                stock_code_df = stock_code_df.drop(columns=columns_to_drop)
                self._logger.log_info(
                    f"Filtered 'stock_market_df' has {len(stock_code_df)} rows and {len(stock_code_df.columns)} columns after filter."
                )

                stock_code_df = stock_code_df.dropna()
                self._logger.log_info(
                    f"Unified dataframe after dropping NA has {len(stock_code_df)} rows and {len(stock_code_df.columns)} columns."
                )

                self._save_pandas_table_to_database(
                    schema_name=Schema.ENTERPRISE.value,
                    table_name=f"unified_{stock_code}",
                    primary_keys=["code", "date"],
                    df=stock_code_df,
                )
