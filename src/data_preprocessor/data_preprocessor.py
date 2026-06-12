from dotenv import load_dotenv
import os
import pandas as pd
import re
from glob import glob
import numpy as np
from datetime import date, datetime, timezone
from psycopg2.extras import execute_values
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import threading

from dtos.thread_manager_dtos.task import Task
from logger.logger import Logger
from dtos.tabular_database_driver_dtos.postgre_sql_connection_dto import (
    PostgreSQLConnectionDto,
)
from dtos.tabular_database_driver_dtos.tabular_database_driver_dtos import *
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from thread_manager.thread_manager import ThreadManager
from utils.constants import (
    SCRAPER_RAW_DATA_DIR,
    DATABASE_MAIN_V2,
    BRONZE_SCHEMA,
    SILVER_SCHEMA,
    GOLD_SCHEMA,
)
from utils.enums import *
from utils.utils import *
from utils.switch_handler import SwitchHandler

load_dotenv()


class DataPreprocessor:

    def __init__(
        self,
        logger: Logger,
        switch_handler: SwitchHandler,
        power: int = THREAD_MANAGER_POWER,
    ):
        self._logger = logger
        self._switch_handler: SwitchHandler = switch_handler
        self._database_driver = PostgreSQLDriver(logger=logger)
        self._thread_manager = ThreadManager(logger=self._logger, power=power)

    # region Helper functions
    def _helper_connect_to_database(self, data_quality: DataQuality) -> None:
        self._logger.log_info(f'Connecting to "{data_quality.value}" database...')

        database = None
        match (data_quality):
            case DataQuality.BRONZE:
                database = os.getenv("BRONZE_POSTGRES_DATABASE")

            case DataQuality.SILVER:
                database = os.getenv("SILVER_POSTGRES_DATABASE")

            case DataQuality.GOLD:
                database = os.getenv("GOLD_POSTGRES_DATABASE")

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        connection_model = PostgreSQLConnectionDto(
            logger=self._logger,
            host=os.getenv("POSTGRES_HOST"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("POSTGRES_PORT"),
            database=database,
        )

        self._database_driver.connect(connection_model)

    def _helper_select_database(self, database_name: str) -> None:
        self._logger.log_info(f'Selecting database "{database_name}"...')

        self._database_driver.change_database(database_name)

    def _helper_select(
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

    def _helper_clean(
        self, df: pd.DataFrame, clean_layer_list: List[CleanLayer]
    ) -> pd.DataFrame:

        if not clean_layer_list:
            return df

        df = df.copy()

        for layer in clean_layer_list:
            match layer.action:
                case CleanAction.REMOVE_RECORD_IF_COLUMN_IS_NULL:
                    if col := layer.params.get("column_name"):
                        df = df[df[col].notnull()]

                case CleanAction.REMOVE_IF_ALL_COLUMNS_ARE_NULL:
                    keep_cols = ["year", "quarter", "month", "day", "date"]
                    df = df.dropna(
                        axis="index",
                        how="all",
                        subset=[col for col in df.columns if col not in keep_cols],
                    )

                case CleanAction.ORDER_BY:
                    if col_list := layer.params.get("column_list"):
                        df = df.sort_values(by=col_list).reset_index(drop=True)

                case CleanAction.REMOVE_COLUMN:
                    if col_list := layer.params.get("column_list"):
                        df = df.drop(columns=col_list).reset_index(drop=True)

                case CleanAction.REMOVE_DUPLICATE_COLUMNS:
                    keep = layer.params.get("keep", "first")

                    if keep == "first":
                        df = df.loc[:, ~df.columns.duplicated(keep="first")]

                    elif keep == "last":
                        df = df.loc[:, ~df.columns.duplicated(keep="last")]

                    else:
                        raise ValueError("keep must be either 'first' or 'last'")

                    df = df.reset_index(drop=True)

                case _:
                    # Optional: handle unknown layer or skip
                    pass

        return df

    def _helper_infer_sql_type(self, dtype) -> str:
        dtype_str = str(dtype).lower()
        if dtype_str in ("int32", "int16", "int8", "uint32", "uint16", "uint8"):
            return DataType.INT()
        elif dtype_str in ("int64", "int", "uint64", "int_"):
            return DataType.BIGINT()
        elif dtype_str.startswith("float") or dtype_str.startswith("decimal"):
            return DataType.DECIMAL()
        elif dtype_str == "bool":
            return DataType.BOOLEAN()
        elif dtype_str.startswith("datetime"):
            return DataType.TIMESTAMP()
        else:
            return DataType.VARCHAR()  # covers "object" and unknown

    def _helper_ensure_table_exists(
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
                    else self._helper_infer_sql_type(df[col].dtype)
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

    def _helper_build_upsert_sql(
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

    def _helper_to_python(self, v):
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

    def _helper_save_pandas_table_to_database(
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

        self._helper_ensure_table_exists(
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

        sql = self._helper_build_upsert_sql(
            schema_name=schema_name,
            table_name=table_name,
            columns=insert_columns,
            primary_keys=primary_keys,
            has_update_date=has_update_date,
        )

        # ── build all tuples once, before spawning threads ────────────────────
        now = datetime.now(timezone.utc) if has_create_date else None
        records: list[tuple] = [
            tuple(self._helper_to_python(v) for v in row)
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

    def _helper_load_csvs(self, folder_path: str) -> tuple[pd.DataFrame, list] | None:
        file_paths = get_all_file_names_with_extensions(
            logger=self._logger,
            folder_path=folder_path,
            extensions=[FileExtension.CSV],
        )

        if not file_paths:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return None

        dataframes = []

        for fp in file_paths:
            df = pd.read_csv(fp, encoding="utf-8")

            # Skip completely empty or all-NA DataFrames
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            self._logger.log_error(f'No valid CSV data found in "{folder_path}".')
            return None

        df = pd.concat(dataframes, ignore_index=True).drop_duplicates()

        return df, file_paths

    def _helper_cast_columns(
        self,
        df: pd.DataFrame,
        decimal_cols: list[str],
        bigint_cols: list[str],
    ) -> pd.DataFrame:

        def _clean_numeric(series: pd.Series) -> pd.Series:
            return (
                series.astype("string")  # preserves missing values as <NA>
                .str.replace(",", "", regex=False)
                .str.strip()
                .replace(
                    {
                        "": pd.NA,
                        "nan": pd.NA,
                        "None": pd.NA,
                        "NULL": pd.NA,
                        "null": pd.NA,
                        "N/A": pd.NA,
                    }
                )
            )

        for col in decimal_cols:
            cleaned = _clean_numeric(df[col])

            df[col] = pd.to_numeric(cleaned, errors="raise").astype("Float64")

        for col in bigint_cols:
            cleaned = _clean_numeric(df[col])

            df[col] = pd.to_numeric(cleaned, errors="raise").astype("Int64")

        return df

    def _helper_remove_duplicates(
        self,
        df: pd.DataFrame,
        primary_keys: List[str],
        sort_by: Optional[List[str]] = None,
        ascending: Optional[List[bool]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Remove duplicates based on primary keys with optional sorting and filtering.

        Only applies filtering, sorting, and deduplication
        if duplicates exist on primary_keys.
        """

        result = df.copy()

        # Check duplicates first
        has_duplicates = result.duplicated(subset=primary_keys).any()

        # If no duplicates, return original dataframe unchanged
        if not has_duplicates:
            return result

        # Apply filters only when duplicates exist
        if filters:
            operator_map = {
                ">": lambda x, y: x > y,
                ">=": lambda x, y: x >= y,
                "<": lambda x, y: x < y,
                "<=": lambda x, y: x <= y,
                "==": lambda x, y: x == y,
                "!=": lambda x, y: x != y,
            }

            for column, (operator, value) in filters.items():

                if operator not in operator_map:
                    raise ValueError(f"Unsupported operator: {operator}")

                result = result[operator_map[operator](result[column], value)]

        # Sort before deduplication
        if sort_by:
            result = result.sort_values(
                by=sort_by,
                ascending=ascending if ascending is not None else True,
            )

        # Remove duplicates
        result = result.drop_duplicates(
            subset=primary_keys,
            keep="first",
        )

        return result

    def _ingest_bronze_economy(self) -> None:
        self._logger.log_info("Ingesting bronze economy data...")

        economy_dir = os.path.join(SCRAPER_RAW_DATA_DIR, "data", "economy")
        csv_files = glob(os.path.join(economy_dir, "**", "*.csv"), recursive=True)

        if not csv_files:
            self._logger.log_error(f'No economy CSV files found in "{economy_dir}".')
            return

        dataframes = []
        for fp in csv_files:
            df = pd.read_csv(fp, encoding="utf-8")
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            self._logger.log_error("No valid economy CSV data found.")
            return

        df = pd.concat(dataframes, ignore_index=True).drop_duplicates()

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("symbol"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("value"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["symbol", "date"]),
            ],
        )

        df = self._helper_cast_columns(df, decimal_cols=["value"], bigint_cols=[])

        df["date"] = pd.to_datetime(df["date"]).dt.date

        df = self._helper_remove_duplicates(df, primary_keys=["symbol", "date"])

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="economy",
            primary_keys=["symbol", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_bronze_forex(self) -> None:
        self._logger.log_info("Ingesting bronze forex data...")

        forex_dir = os.path.join(SCRAPER_RAW_DATA_DIR, "data", "forex")
        csv_files = glob(os.path.join(forex_dir, "**", "*.csv"), recursive=True)

        if not csv_files:
            self._logger.log_error(f'No forex CSV files found in "{forex_dir}".')
            return

        dataframes = []
        for fp in csv_files:
            df = pd.read_csv(fp, encoding="utf-8")
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            self._logger.log_error("No valid forex CSV data found.")
            return

        df = pd.concat(dataframes, ignore_index=True).drop_duplicates()

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("symbol"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("value"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["symbol", "date"]),
            ],
        )

        df = self._helper_cast_columns(df, decimal_cols=["value"], bigint_cols=[])

        df["date"] = pd.to_datetime(df["date"]).dt.date

        df = self._helper_remove_duplicates(df, primary_keys=["symbol", "date"])

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="forex",
            primary_keys=["symbol", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_bronze_funds(self) -> None:
        self._logger.log_info("Ingesting bronze funds data...")

        funds_dir = os.path.join(SCRAPER_RAW_DATA_DIR, "data", "funds")
        csv_files = glob(os.path.join(funds_dir, "**", "*.csv"), recursive=True)

        if not csv_files:
            self._logger.log_error(f'No funds CSV files found in "{funds_dir}".')
            return

        dataframes = []
        for fp in csv_files:
            df = pd.read_csv(fp, encoding="utf-8")
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            self._logger.log_error("No valid funds CSV data found.")
            return

        df = pd.concat(dataframes, ignore_index=True).drop_duplicates()

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("symbol"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("close"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["symbol", "date"]),
            ],
        )

        df = self._helper_cast_columns(
            df,
            decimal_cols=["open", "high", "low", "close"],
            bigint_cols=["volume"],
        )

        df["date"] = pd.to_datetime(df["date"]).dt.date

        df = self._helper_remove_duplicates(df, primary_keys=["symbol", "date"])

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="funds",
            primary_keys=["symbol", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_bronze_indices(self) -> None:
        self._logger.log_info("Ingesting bronze indices data...")

        indices_dir = os.path.join(SCRAPER_RAW_DATA_DIR, "data", "indices")
        csv_files = glob(os.path.join(indices_dir, "**", "*.csv"), recursive=True)

        if not csv_files:
            self._logger.log_error(f'No indices CSV files found in "{indices_dir}".')
            return

        dataframes = []
        for fp in csv_files:
            df = pd.read_csv(fp, encoding="utf-8")
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            self._logger.log_error("No valid indices CSV data found.")
            return

        df = pd.concat(dataframes, ignore_index=True).drop_duplicates()

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("symbol"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("close"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["symbol", "date"]),
            ],
        )

        df = self._helper_cast_columns(
            df,
            decimal_cols=["open", "high", "low", "close"],
            bigint_cols=["volume"],
        )

        df["date"] = pd.to_datetime(df["date"]).dt.date

        df = self._helper_remove_duplicates(df, primary_keys=["symbol", "date"])

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="indices",
            primary_keys=["symbol", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_bronze_stocks(self) -> None:
        self._logger.log_info("Ingesting bronze stocks data...")

        stocks_dir = os.path.join(SCRAPER_RAW_DATA_DIR, "data", "stocks")
        csv_files = glob(os.path.join(stocks_dir, "**", "*.csv"), recursive=True)

        if not csv_files:
            self._logger.log_error(f'No stocks CSV files found in "{stocks_dir}".')
            return

        dataframes = []
        for fp in csv_files:
            df = pd.read_csv(fp, encoding="utf-8")
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            self._logger.log_error("No valid stocks CSV data found.")
            return

        df = pd.concat(dataframes, ignore_index=True).drop_duplicates()

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("symbol"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("close"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["symbol", "date"]),
            ],
        )

        df = self._helper_cast_columns(
            df,
            decimal_cols=["open", "high", "low", "close"],
            bigint_cols=["volume"],
        )

        df["date"] = pd.to_datetime(df["date"]).dt.date

        df = self._helper_remove_duplicates(df, primary_keys=["symbol", "date"])

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="stocks",
            primary_keys=["symbol", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_bronze_bonds(self) -> None:
        self._logger.log_info("Ingesting bronze bonds data...")

        bonds_dir = os.path.join(SCRAPER_RAW_DATA_DIR, "data", "bonds")
        csv_files = glob(os.path.join(bonds_dir, "**", "*.csv"), recursive=True)

        if not csv_files:
            self._logger.log_error(f'No bonds CSV files found in "{bonds_dir}".')
            return

        dataframes = []
        for fp in csv_files:
            df = pd.read_csv(fp, encoding="utf-8")
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            self._logger.log_error("No valid bonds CSV data found.")
            return

        df = pd.concat(dataframes, ignore_index=True).drop_duplicates()

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("symbol"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("value"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["symbol", "date"]),
            ],
        )

        df = self._helper_cast_columns(df, decimal_cols=["value"], bigint_cols=[])

        df["date"] = pd.to_datetime(df["date"]).dt.date

        df = self._helper_remove_duplicates(df, primary_keys=["symbol", "date"])

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="bonds",
            primary_keys=["symbol", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_silver_bonds(self) -> None:
        self._logger.log_info("Ingesting silver bonds data...")

        df = self._helper_select(schema_name=BRONZE_SCHEMA, table_name="bonds")

        if df.empty:
            self._logger.log_info("No bronze bonds data found.")
            return

        df["exchange"] = df["symbol"].str.split(":").str[0]
        df["ticker"] = df["symbol"].str.split(":").str[1]

        df = df[["exchange", "ticker", "date", "value"]]

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="bonds",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_silver_economy(self) -> None:
        self._logger.log_info("Ingesting silver economy data...")

        df = self._helper_select(schema_name=BRONZE_SCHEMA, table_name="economy")

        if df.empty:
            self._logger.log_info("No bronze economy data found.")
            return

        df["exchange"] = df["symbol"].str.split(":").str[0]
        df["ticker"] = df["symbol"].str.split(":").str[1]

        df = df[["exchange", "ticker", "date", "value"]]

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="economy",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_silver_forex(self) -> None:
        self._logger.log_info("Ingesting silver forex data...")

        df = self._helper_select(schema_name=BRONZE_SCHEMA, table_name="forex")

        if df.empty:
            self._logger.log_info("No bronze forex data found.")
            return

        df["exchange"] = df["symbol"].str.split(":").str[0]
        df["ticker"] = df["symbol"].str.split(":").str[1]

        df = df[["exchange", "ticker", "date", "open", "high", "low", "close", "volume"]]
        df = self._helper_cast_columns(df, decimal_cols=["open", "high", "low", "close"], bigint_cols=["volume"])

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="forex",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_silver_funds(self) -> None:
        self._logger.log_info("Ingesting silver funds data...")

        df = self._helper_select(schema_name=BRONZE_SCHEMA, table_name="funds")

        if df.empty:
            self._logger.log_info("No bronze funds data found.")
            return

        df["exchange"] = df["symbol"].str.split(":").str[0]
        df["ticker"] = df["symbol"].str.split(":").str[1]

        df = df[["exchange", "ticker", "date", "open", "high", "low", "close", "volume"]]
        df = self._helper_cast_columns(df, decimal_cols=["open", "high", "low", "close"], bigint_cols=["volume"])

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="funds",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_silver_indices(self) -> None:
        self._logger.log_info("Ingesting silver indices data...")

        df = self._helper_select(schema_name=BRONZE_SCHEMA, table_name="indices")

        if df.empty:
            self._logger.log_info("No bronze indices data found.")
            return

        df["exchange"] = df["symbol"].str.split(":").str[0]
        df["ticker"] = df["symbol"].str.split(":").str[1]

        df = df[["exchange", "ticker", "date", "open", "high", "low", "close", "volume"]]
        df = self._helper_cast_columns(df, decimal_cols=["open", "high", "low", "close"], bigint_cols=["volume"])

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="indices",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_silver_stocks(self) -> None:
        self._logger.log_info("Ingesting silver stocks data...")

        df = self._helper_select(schema_name=BRONZE_SCHEMA, table_name="stocks")

        if df.empty:
            self._logger.log_info("No bronze stocks data found.")
            return

        df["exchange"] = df["symbol"].str.split(":").str[0]
        df["ticker"] = df["symbol"].str.split(":").str[1]

        df = df[["exchange", "ticker", "date", "open", "high", "low", "close", "volume"]]
        df = self._helper_cast_columns(df, decimal_cols=["open", "high", "low", "close"], bigint_cols=["volume"])

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="stocks",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _helper_transform(
        self,
        df: pd.DataFrame,
        transform_layer_list: List[TransformLayer],
        checkpoint_fn=None,
        checkpoint_size: int = 10_000,
    ) -> pd.DataFrame:
        """
        Apply transform layers to df.

        If checkpoint_fn is provided, instead of returning a single concatenated
        DataFrame the method flushes accumulated groups to checkpoint_fn(chunk)
        every checkpoint_size rows and returns an empty DataFrame.
        This keeps memory bounded and persists progress incrementally.
        """
        from ta.ta_functions import (
            add_bbands, add_dema, add_ema, add_kama, add_midpoint, add_midprice,
            add_sar, add_sma, add_t3, add_tema, add_trima, add_wma,
            add_adx, add_aroon, add_bop, add_cci, add_cmo, add_macd,
            add_mfi, add_mom, add_ppo, add_roc, add_rsi, add_stoch,
            add_stoch_rsi, add_trix, add_ultosc, add_willr,
            add_ad, add_adosc, add_obv,
            add_ht_dcperiod, add_ht_dcphase, add_ht_phasor, add_ht_sine, add_ht_trendmode,
            add_avgprice, add_medprice, add_typprice, add_wclprice,
            add_atr, add_natr, add_trange,
        )

        _TA_FUNC_MAP = {
            TransformAction.TA_ADD_BBANDS: add_bbands,
            TransformAction.TA_ADD_DEMA: add_dema,
            TransformAction.TA_ADD_EMA: add_ema,
            TransformAction.TA_ADD_KAMA: add_kama,
            TransformAction.TA_ADD_MIDPOINT: add_midpoint,
            TransformAction.TA_ADD_MIDPRICE: add_midprice,
            TransformAction.TA_ADD_SAR: add_sar,
            TransformAction.TA_ADD_SMA: add_sma,
            TransformAction.TA_ADD_T3: add_t3,
            TransformAction.TA_ADD_TEMA: add_tema,
            TransformAction.TA_ADD_TRIMA: add_trima,
            TransformAction.TA_ADD_WMA: add_wma,
            TransformAction.TA_ADD_ADX: add_adx,
            TransformAction.TA_ADD_AROON: add_aroon,
            TransformAction.TA_ADD_BOP: add_bop,
            TransformAction.TA_ADD_CCI: add_cci,
            TransformAction.TA_ADD_CMO: add_cmo,
            TransformAction.TA_ADD_MACD: add_macd,
            TransformAction.TA_ADD_MFI: add_mfi,
            TransformAction.TA_ADD_MOM: add_mom,
            TransformAction.TA_ADD_PPO: add_ppo,
            TransformAction.TA_ADD_ROC: add_roc,
            TransformAction.TA_ADD_RSI: add_rsi,
            TransformAction.TA_ADD_STOCH: add_stoch,
            TransformAction.TA_ADD_STOCH_RSI: add_stoch_rsi,
            TransformAction.TA_ADD_TRIX: add_trix,
            TransformAction.TA_ADD_ULTOSC: add_ultosc,
            TransformAction.TA_ADD_WILLR: add_willr,
            TransformAction.TA_ADD_AD: add_ad,
            TransformAction.TA_ADD_ADOSC: add_adosc,
            TransformAction.TA_ADD_OBV: add_obv,
            TransformAction.TA_ADD_HT_DCPERIOD: add_ht_dcperiod,
            TransformAction.TA_ADD_HT_DCPHASE: add_ht_dcphase,
            TransformAction.TA_ADD_HT_PHASOR: add_ht_phasor,
            TransformAction.TA_ADD_HT_SINE: add_ht_sine,
            TransformAction.TA_ADD_HT_TRENDMODE: add_ht_trendmode,
            TransformAction.TA_ADD_AVGPRICE: add_avgprice,
            TransformAction.TA_ADD_MEDPRICE: add_medprice,
            TransformAction.TA_ADD_TYPPRICE: add_typprice,
            TransformAction.TA_ADD_WCLPRICE: add_wclprice,
            TransformAction.TA_ADD_ATR: add_atr,
            TransformAction.TA_ADD_NATR: add_natr,
            TransformAction.TA_ADD_TRANGE: add_trange,
        }

        ta_layers = [l for l in transform_layer_list if l.action in _TA_FUNC_MAP]
        general_layers = [l for l in transform_layer_list if l.action not in _TA_FUNC_MAP]

        # Apply whole-df transforms first
        df = df.copy()
        for layer in general_layers:
            if layer.action == TransformAction.EXTRACT_DATETIME_FEATURE:
                col = layer.params.get("column_name", "date")
                dt = pd.to_datetime(df[col])

                # Calendar basics
                df["year"]          = dt.dt.year.astype("int32")
                df["quarter"]       = dt.dt.quarter.astype("int32")
                df["month"]         = dt.dt.month.astype("int32")
                df["week_of_year"]  = dt.dt.isocalendar().week.astype("int32")
                df["day_of_year"]   = dt.dt.day_of_year.astype("int32")
                df["day"]           = dt.dt.day.astype("int32")
                df["day_of_week"]   = dt.dt.day_of_week.astype("int32")  # 0=Mon … 6=Sun

                # Boundary flags (bool → int for DB compatibility)
                df["is_month_start"]   = dt.dt.is_month_start.astype("int32")
                df["is_month_end"]     = dt.dt.is_month_end.astype("int32")
                df["is_quarter_start"] = dt.dt.is_quarter_start.astype("int32")
                df["is_quarter_end"]   = dt.dt.is_quarter_end.astype("int32")
                df["is_year_start"]    = dt.dt.is_year_start.astype("int32")
                df["is_year_end"]      = dt.dt.is_year_end.astype("int32")

                # Cyclical encodings — let the model see that Dec→Jan and Fri→Mon wrap around
                df["month_sin"]       = np.sin(2 * np.pi * dt.dt.month / 12)
                df["month_cos"]       = np.cos(2 * np.pi * dt.dt.month / 12)
                df["day_of_week_sin"] = np.sin(2 * np.pi * dt.dt.day_of_week / 7)
                df["day_of_week_cos"] = np.cos(2 * np.pi * dt.dt.day_of_week / 7)
                df["day_of_year_sin"] = np.sin(2 * np.pi * dt.dt.day_of_year / 365)
                df["day_of_year_cos"] = np.cos(2 * np.pi * dt.dt.day_of_year / 365)

        # Apply TA transforms per (exchange, ticker) group, sorted by date
        if ta_layers:
            if checkpoint_fn:
                buffer: list[pd.DataFrame] = []
                buffer_rows = 0
                grouped = list(df.groupby(["exchange", "ticker"], sort=False))
                total = len(grouped)
                for i, ((_, _), group) in enumerate(grouped):
                    group = group.sort_values("date").reset_index(drop=True)
                    for layer in ta_layers:
                        func = _TA_FUNC_MAP.get(layer.action)
                        if func:
                            group = func(group, **layer.params)
                    buffer.append(group)
                    buffer_rows += len(group)
                    if buffer_rows >= checkpoint_size:
                        checkpoint_fn(pd.concat(buffer, ignore_index=True))
                        self._logger.log_info(
                            f"Checkpoint saved: {i + 1}/{total} tickers ({buffer_rows} rows)"
                        )
                        buffer = []
                        buffer_rows = 0
                if buffer:
                    checkpoint_fn(pd.concat(buffer, ignore_index=True))
                    self._logger.log_info(
                        f"Checkpoint saved: {total}/{total} tickers ({buffer_rows} rows)"
                    )
                return pd.DataFrame()
            else:
                groups = []
                for (_, _), group in df.groupby(["exchange", "ticker"], sort=False):
                    group = group.sort_values("date").reset_index(drop=True)
                    for layer in ta_layers:
                        func = _TA_FUNC_MAP.get(layer.action)
                        if func:
                            group = func(group, **layer.params)
                    groups.append(group)
                df = pd.concat(groups, ignore_index=True)

        return df

    def _ingest_gold_stocks(self) -> None:
        self._logger.log_info("Ingesting gold stocks data...")

        df = self._helper_select(
            schema_name=SILVER_SCHEMA,
            table_name="stocks",
            order_by=["exchange", "ticker", "date"],
        )

        if df.empty:
            self._logger.log_info("No silver stocks data found.")
            return

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        def _checkpoint(chunk: pd.DataFrame) -> None:
            # Use REAL (4-byte float) for all float columns to stay within
            # PostgreSQL's 8160-byte row size limit given the large number of TA columns.
            overrides: dict[str, str] = {"date": DataType.DATE()}
            for col in chunk.columns:
                if str(chunk[col].dtype).lower().startswith("float"):
                    overrides[col] = "REAL"
            self._helper_save_pandas_table_to_database(
                schema_name=GOLD_SCHEMA,
                table_name="stocks",
                primary_keys=["exchange", "ticker", "date"],
                df=chunk,
                dtype_overrides=overrides,
            )

        self._helper_transform(
            df,
            [
                # Overlap Studies
                TransformLayer.TA_ADD_BBANDS(),
                TransformLayer.TA_ADD_DEMA(),
                TransformLayer.TA_ADD_EMA(),
                TransformLayer.TA_ADD_KAMA(),
                TransformLayer.TA_ADD_MIDPOINT(),
                TransformLayer.TA_ADD_MIDPRICE(),
                TransformLayer.TA_ADD_SAR(),
                TransformLayer.TA_ADD_SMA(),
                TransformLayer.TA_ADD_T3(),
                TransformLayer.TA_ADD_TEMA(),
                TransformLayer.TA_ADD_TRIMA(),
                TransformLayer.TA_ADD_WMA(),
                # Momentum Indicators
                TransformLayer.TA_ADD_ADX(),
                TransformLayer.TA_ADD_AROON(),
                TransformLayer.TA_ADD_BOP(),
                TransformLayer.TA_ADD_CCI(),
                TransformLayer.TA_ADD_CMO(),
                TransformLayer.TA_ADD_MACD(),
                TransformLayer.TA_ADD_MFI(volume_col="volume"),
                TransformLayer.TA_ADD_MOM(),
                TransformLayer.TA_ADD_PPO(),
                TransformLayer.TA_ADD_ROC(),
                TransformLayer.TA_ADD_RSI(),
                TransformLayer.TA_ADD_STOCH(),
                TransformLayer.TA_ADD_STOCH_RSI(),
                TransformLayer.TA_ADD_TRIX(),
                TransformLayer.TA_ADD_ULTOSC(),
                TransformLayer.TA_ADD_WILLR(),
                # Volume Indicators
                TransformLayer.TA_ADD_AD(volume_col="volume"),
                TransformLayer.TA_ADD_ADOSC(volume_col="volume"),
                TransformLayer.TA_ADD_OBV(volume_col="volume"),
                # Cycle Indicators
                TransformLayer.TA_ADD_HT_DCPERIOD(),
                TransformLayer.TA_ADD_HT_DCPHASE(),
                TransformLayer.TA_ADD_HT_PHASOR(),
                TransformLayer.TA_ADD_HT_SINE(),
                TransformLayer.TA_ADD_HT_TRENDMODE(),
                # Price Transform
                TransformLayer.TA_ADD_AVGPRICE(),
                TransformLayer.TA_ADD_MEDPRICE(),
                TransformLayer.TA_ADD_TYPPRICE(),
                TransformLayer.TA_ADD_WCLPRICE(),
                # Volatility Indicators
                TransformLayer.TA_ADD_ATR(),
                TransformLayer.TA_ADD_NATR(),
                TransformLayer.TA_ADD_TRANGE(),
            ],
            checkpoint_fn=_checkpoint,
            checkpoint_size=10_000,
        )

    # endregion Helper functions

    def ingest_bronze_data(self) -> None:

        if self._switch_handler.is_enabled("data_preprocessor", "data_quality_bronze"):
            try:
                connection_model = PostgreSQLConnectionDto(
                    logger=self._logger,
                    host=os.getenv("POSTGRES_HOST"),
                    user=os.getenv("POSTGRES_USER"),
                    password=os.getenv("POSTGRES_PASSWORD"),
                    port=os.getenv("POSTGRES_PORT"),
                    database="postgres",
                )
                self._database_driver.connect(connection_model)

                self._database_driver.create_database(DATABASE_MAIN_V2)

                self._database_driver.create_schema(BRONZE_SCHEMA)

                if self._switch_handler.is_enabled("data_preprocessor", "data_quality_bronze", "bonds"):
                    self._ingest_bronze_bonds()
                if self._switch_handler.is_enabled("data_preprocessor", "data_quality_bronze", "economy"):
                    self._ingest_bronze_economy()
                if self._switch_handler.is_enabled("data_preprocessor", "data_quality_bronze", "forex"):
                    self._ingest_bronze_forex()
                if self._switch_handler.is_enabled("data_preprocessor", "data_quality_bronze", "funds"):
                    self._ingest_bronze_funds()
                if self._switch_handler.is_enabled("data_preprocessor", "data_quality_bronze", "indices"):
                    self._ingest_bronze_indices()
                if self._switch_handler.is_enabled("data_preprocessor", "data_quality_bronze", "stocks"):
                    self._ingest_bronze_stocks()

            except Exception as e:
                self._logger.log_error(
                    f"Error preprocessing `{DataQuality.BRONZE.value}` data: {e}"
                )

            finally:
                self._database_driver.disconnect()

    def ingest_silver_data(self) -> None:

        if self._switch_handler.is_enabled("data_preprocessor", "data_quality_silver"):
            try:
                connection_model = PostgreSQLConnectionDto(
                    logger=self._logger,
                    host=os.getenv("POSTGRES_HOST"),
                    user=os.getenv("POSTGRES_USER"),
                    password=os.getenv("POSTGRES_PASSWORD"),
                    port=os.getenv("POSTGRES_PORT"),
                    database="postgres",
                )
                self._database_driver.connect(connection_model)

                self._database_driver.create_database(DATABASE_MAIN_V2)

                self._database_driver.create_schema(SILVER_SCHEMA)

                if self._switch_handler.is_enabled("data_preprocessor", "data_quality_silver", "bonds"):
                    self._ingest_silver_bonds()
                if self._switch_handler.is_enabled("data_preprocessor", "data_quality_silver", "economy"):
                    self._ingest_silver_economy()
                if self._switch_handler.is_enabled("data_preprocessor", "data_quality_silver", "forex"):
                    self._ingest_silver_forex()
                if self._switch_handler.is_enabled("data_preprocessor", "data_quality_silver", "funds"):
                    self._ingest_silver_funds()
                if self._switch_handler.is_enabled("data_preprocessor", "data_quality_silver", "indices"):
                    self._ingest_silver_indices()
                if self._switch_handler.is_enabled("data_preprocessor", "data_quality_silver", "stocks"):
                    self._ingest_silver_stocks()

            except Exception as e:
                self._logger.log_error(
                    f"Error preprocessing `{DataQuality.SILVER.value}` data: {e}"
                )

            finally:
                self._database_driver.disconnect()

    def ingest_gold_data(self) -> None:

        if self._switch_handler.is_enabled("data_preprocessor", "data_quality_gold"):
            try:
                connection_model = PostgreSQLConnectionDto(
                    logger=self._logger,
                    host=os.getenv("POSTGRES_HOST"),
                    user=os.getenv("POSTGRES_USER"),
                    password=os.getenv("POSTGRES_PASSWORD"),
                    port=os.getenv("POSTGRES_PORT"),
                    database="postgres",
                )
                self._database_driver.connect(connection_model)

                self._database_driver.create_database(DATABASE_MAIN_V2)

                self._database_driver.create_schema(GOLD_SCHEMA)

                if self._switch_handler.is_enabled("data_preprocessor", "data_quality_gold", "stocks"):
                    self._ingest_gold_stocks()

            except Exception as e:
                self._logger.log_error(
                    f"Error preprocessing `{DataQuality.GOLD.value}` data: {e}"
                )

            finally:
                self._database_driver.disconnect()
