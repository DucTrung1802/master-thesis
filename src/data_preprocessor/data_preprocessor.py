from dotenv import load_dotenv
import os
import hashlib
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
    TRADING_VIEW_RAW_DATA_DIR,
    CAFEF_RAW_DATA_DIR,
    SIMPLIZE_RAW_DATA_DIR,
    GICS_RAW_DATA_DIR,
    SIMPLIZE_GROUP_TO_GICS_SUB_INDUSTRY,
    DATABASE_MAIN_V2,
    BRONZE_SCHEMA,
    SILVER_SCHEMA,
    GOLD_SCHEMA,
)
from utils.enums import *
from utils.utils import *
from utils.switch_handler import SwitchHandler

load_dotenv()


def _build_transform_func_map() -> dict:
    """Map each per-group TransformAction to its implementation.

    Defined at module level so it can be rebuilt inside ProcessPoolExecutor
    worker processes (which re-import this module under the spawn start method).
    """
    from ta.ta_functions import (
        add_bbands,
        add_dema,
        add_ema,
        add_kama,
        add_midpoint,
        add_midprice,
        add_sar,
        add_sma,
        add_t3,
        add_tema,
        add_trima,
        add_wma,
        add_adx,
        add_aroon,
        add_bop,
        add_cci,
        add_cmo,
        add_macd,
        add_mfi,
        add_mom,
        add_ppo,
        add_roc,
        add_rsi,
        add_stoch,
        add_stoch_rsi,
        add_trix,
        add_ultosc,
        add_willr,
        add_ad,
        add_adosc,
        add_obv,
        add_ht_dcperiod,
        add_ht_dcphase,
        add_ht_phasor,
        add_ht_sine,
        add_ht_trendmode,
        add_avgprice,
        add_medprice,
        add_typprice,
        add_wclprice,
        add_atr,
        add_natr,
        add_trange,
        add_returns,
        add_intraday_range,
        add_return_volatility,
        add_rolling_statistics,
        add_foreign_buy_pressure,
        add_foreign_net_val_ratio,
        add_negotiated_vol_ratio,
    )

    return {
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
        # Feature engineering (non-TA) — also applied per (exchange, ticker) group
        TransformAction.ADD_RETURNS: add_returns,
        TransformAction.ADD_INTRADAY_RANGE: add_intraday_range,
        TransformAction.ADD_RETURN_VOLATILITY: add_return_volatility,
        TransformAction.ADD_ROLLING_STATISTICS: add_rolling_statistics,
        TransformAction.ADD_FOREIGN_BUY_PRESSURE: add_foreign_buy_pressure,
        TransformAction.ADD_FOREIGN_NET_VAL_RATIO: add_foreign_net_val_ratio,
        TransformAction.ADD_NEGOTIATED_VOL_RATIO: add_negotiated_vol_ratio,
    }


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

    def _helper_copy_insert_to_database(
        self,
        schema_name: str,
        table_name: str,
        df: pd.DataFrame,
        has_create_date: bool,
    ) -> None:
        """
        Fast bulk insert via PostgreSQL COPY FROM STDIN.

        Much faster than the execute_values upsert (vectorized serialization +
        server-side bulk load, no per-cell Python conversion). Plain insert with
        NO conflict handling — only safe when the target rows are known to be new
        (e.g. a freshly created/empty table, as in the gold ingest).

        CSV serialization uses pyarrow's multi-threaded write_csv when available
        (~10x faster than pandas to_csv on wide frames); falls back to pandas.
        """
        from io import BytesIO, StringIO

        df = df.copy()
        if has_create_date and "create_date" not in df.columns:
            df["create_date"] = datetime.now(timezone.utc)

        columns = list(df.columns)
        col_str = ", ".join(f'"{c}"' for c in columns)
        copy_sql = (
            f'COPY {schema_name}."{table_name}" ({col_str}) '
            f"FROM STDIN WITH (FORMAT csv, NULL '')"
        )

        try:
            import pyarrow as pa
            from pyarrow import csv as pacsv

            table = pa.Table.from_pandas(df, preserve_index=False)
            sink = pa.BufferOutputStream()
            pacsv.write_csv(
                table, sink, write_options=pacsv.WriteOptions(include_header=False)
            )
            buf = BytesIO(sink.getvalue().to_pybytes())
        except Exception as e:
            self._logger.log_warning(
                f"pyarrow CSV serialization unavailable ({e}); falling back to pandas."
            )
            buf = StringIO()
            df.to_csv(buf, index=False, header=False, na_rep="")
            buf.seek(0)

        with self._database_driver._cursor_ctx() as cur:
            cur.copy_expert(copy_sql, buf)

        self._logger.log_info(
            f"COPY-inserted {len(df)} records into '{schema_name}.{table_name}'."
        )

    def _helper_save_pandas_table_to_database(
        self,
        schema_name: str,
        table_name: str,
        primary_keys: List[str],
        df: pd.DataFrame,
        dtype_overrides: dict[str, str] | None = None,
        chunk_size: int = 5_000,
        max_workers: int | None = None,  # None → let ThreadPoolExecutor decide
        use_copy: bool = False,
    ) -> None:
        self._logger.log_info(
            f'Saving dataframe to table "{schema_name}.{table_name}".'
        )

        df = df.dropna(how="all")
        if df.empty:
            self._logger.log_info("DataFrame is empty after cleaning. Nothing to save.")
            return

        # Sanitize values destined for REAL columns. PostgreSQL REAL rejects floats
        # outside its range: ±inf and subnormals with |x| below ~1e-38 (a few TA
        # features can emit e.g. -5.7e-46). Map ±inf → NaN and tiny magnitudes → 0.0.
        if dtype_overrides:
            real_cols = [
                c
                for c, t in dtype_overrides.items()
                if str(t).upper().startswith("REAL") and c in df.columns
            ]
            if real_cols:
                df = df.copy()
                for c in real_cols:
                    s = pd.to_numeric(df[c], errors="coerce")
                    s = s.replace([np.inf, -np.inf], np.nan)
                    s = s.where(s.abs() <= 3.4e38, np.nan)  # above REAL max → NaN
                    df[c] = s.mask(s.abs() < 1e-37, 0.0)  # subnormal → 0.0

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

        # ── fast bulk path: COPY (no upsert) for known-new rows ───────────────
        if use_copy:
            self._helper_copy_insert_to_database(
                schema_name=schema_name,
                table_name=table_name,
                df=df,
                has_create_date=has_create_date,
            )
            return

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

        economy_dir = os.path.join(TRADING_VIEW_RAW_DATA_DIR, "data", "economy")
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
        df = self._helper_split_symbol_column(df)

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="trading_view_economy",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_bronze_forex(self) -> None:
        self._logger.log_info("Ingesting bronze forex data...")

        forex_dir = os.path.join(TRADING_VIEW_RAW_DATA_DIR, "data", "forex")
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
        df = self._helper_split_symbol_column(df)

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="trading_view_forex",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_bronze_funds(self) -> None:
        self._logger.log_info("Ingesting bronze funds data...")

        funds_dir = os.path.join(TRADING_VIEW_RAW_DATA_DIR, "data", "funds")
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
        df = self._helper_split_symbol_column(df)

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="trading_view_funds",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_bronze_indices(self) -> None:
        self._logger.log_info("Ingesting bronze indices data...")

        indices_dir = os.path.join(TRADING_VIEW_RAW_DATA_DIR, "data", "indices")
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
        df = self._helper_split_symbol_column(df)

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="trading_view_indices",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_bronze_stocks_trading_view(self) -> None:
        """Bronze table for TradingView per-stock data — the universe source and
        the only source carrying `sector`; dividend-adjusted OHLCV (volume is
        split-adjusted, so superseded by Simplize in silver). Kept as a separate
        bronze table per source; merged in silver."""
        self._logger.log_info("Ingesting bronze TradingView stocks data...")

        stocks_dir = os.path.join(TRADING_VIEW_RAW_DATA_DIR, "data", "stocks")
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
        df = self._helper_split_symbol_column(df)

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="trading_view_stocks",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    # ── CafeF: one bronze table per scraper link-folder ──────────────────────
    # The CafeF scraper writes five per-link folders under raw_data/cafef/
    # (price, foreign, order_stats, prop_trading, insider_txn). Each lands as its
    # OWN raw-faithful bronze table (rather than the former single merged
    # `cafef_stocks`), mirroring the scraper's one-folder-per-link design; the
    # price+foreign merge now happens in silver. Key is normalised to
    # `symbol = "<EXCHANGE>:<TICKER>"` to match the TradingView / Simplize
    # convention so the silver merge can split it the same way.

    def _helper_load_cafef_folder(self, folder: str) -> pd.DataFrame | None:
        """Concat every CSV under raw_data/cafef/<folder>/ (one per ticker),
        dropping empty / all-NA frames. Returns None if nothing valid is found."""
        files = glob(
            os.path.join(CAFEF_RAW_DATA_DIR, folder, "**", "*.csv"), recursive=True
        )
        frames = [
            df
            for fp in files
            if not (df := pd.read_csv(fp, encoding="utf-8")).empty
            and not df.dropna(how="all").empty
        ]
        return (
            pd.concat(frames, ignore_index=True).drop_duplicates() if frames else None
        )

    def _helper_split_symbol_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Split the `"<EXCHANGE>:<TICKER>"` colon key into separate `exchange` and
        `ticker` columns (dropping `symbol`). The inverse of the colon convention —
        used by the sources that keep a single colon `symbol` (TradingView).

        Splits on the FIRST `:` only, because TradingView's provider-prefixed symbols
        are still `PREFIX:REST` (`ECONOMICS:CN14RRR`, `B2PRIME:AUDCAD`) and REST may
        itself contain no further colon. A value with no colon at all keeps the whole
        string as `exchange` and leaves `ticker` null (then dropped by the key
        null-check), which never happens for the current universe but must not crash."""
        df = df.copy()
        parts = df["symbol"].astype("string").str.split(":", n=1, expand=True)
        if parts.shape[1] == 1:
            parts[1] = pd.NA
        df["exchange"] = parts[0].str.strip()
        df["ticker"] = parts[1].str.strip()
        return df.drop(columns=["symbol"])

    def _helper_normalise_cafef_symbol(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fold CafeF's split (exchange, symbol) into the bronze convention
        `symbol = "<EXCHANGE>:<TICKER>"` and drop the redundant `exchange` column."""
        df = df.copy()
        df["symbol"] = (
            df["exchange"].astype("string").str.strip()
            + ":"
            + df["symbol"].astype("string").str.strip()
        )
        return df.drop(columns=["exchange"])

    def _ingest_bronze_cafef_daily(
        self,
        folder: str,
        table_name: str,
        decimal_cols: List[str],
        bigint_cols: List[str],
        required_col: Optional[str] = None,
        split_key: bool = False,
    ) -> None:
        """Ingest one DAILY-series CafeF folder as its own bronze table
        (raw-faithful). Drives price / foreign / order_stats / prop_trading.

        `split_key` selects the key convention:
        - `False` (default) — fold CafeF's raw (exchange, symbol) columns into the
          `symbol = "<EXCHANGE>:<TICKER>"` colon key so the source merges uniformly
          with Simplize/TradingView in silver. PK (symbol, date).
        - `True` — keep the key SPLIT as separate `exchange` + `ticker` columns
          (raw-faithful to the CSV, which already stores the two apart). PK
          (exchange, ticker, date)."""
        self._logger.log_info(f"Ingesting bronze CafeF {folder} data...")

        df = self._helper_load_cafef_folder(folder)
        if df is None:
            self._logger.log_error(f'No valid CafeF "{folder}" CSV data found.')
            return

        if split_key:
            df = df.rename(columns={"symbol": "ticker"})
            key_cols = ["exchange", "ticker"]
        else:
            df = self._helper_normalise_cafef_symbol(df)
            key_cols = ["symbol"]

        clean_layers = [
            CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL(col) for col in key_cols
        ]
        clean_layers.append(CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"))
        if required_col:
            clean_layers.append(
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL(required_col)
            )
        clean_layers += [
            CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
            CleanLayer.ORDER_BY([*key_cols, "date"]),
        ]
        df = self._helper_clean(df, clean_layers)

        df = self._helper_cast_columns(
            df, decimal_cols=decimal_cols, bigint_cols=bigint_cols
        )
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = self._helper_remove_duplicates(df, primary_keys=[*key_cols, "date"])

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name=table_name,
            primary_keys=[*key_cols, "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_bronze_cafef_price(self) -> None:
        """CafeF price tab — OHLC (raw + dividend-adjusted close) and the matched
        vs negotiated (block) volume/value split. Key kept SPLIT as
        (exchange, ticker); PK (exchange, ticker, date)."""
        self._ingest_bronze_cafef_daily(
            folder="price",
            table_name="cafef_price",
            decimal_cols=[
                "open",
                "high",
                "low",
                "close_raw",
                "close_adjust",
                "value_matched",
                "value_negotiated",
            ],
            bigint_cols=["volume_matched", "volume_negotiated"],
            required_col="close_adjust",
            split_key=True,
        )

    def _ingest_bronze_cafef_foreign(self) -> None:
        """CafeF foreign tab — foreign buy/sell/net flow (volume + value), remaining
        room and foreign ownership %. Key kept SPLIT; PK (exchange, ticker, date)."""
        self._ingest_bronze_cafef_daily(
            folder="foreign",
            table_name="cafef_foreign",
            decimal_cols=[
                "foreign_buy_value",
                "foreign_sell_value",
                "foreign_net_value",
                "foreign_own",
            ],
            bigint_cols=[
                "foreign_buy_volume",
                "foreign_sell_volume",
                "foreign_net_volume",
                "foreign_room_left",
            ],
            split_key=True,
        )

    def _ingest_bronze_cafef_order_stats(self) -> None:
        """CafeF order-placement stats — number + volume of buy vs sell orders and
        the average volume per order. Key kept SPLIT; PK (exchange, ticker, date)."""
        self._ingest_bronze_cafef_daily(
            folder="order_stats",
            table_name="cafef_order_stats",
            decimal_cols=["avg_vol_per_buy_order", "avg_vol_per_sell_order"],
            bigint_cols=[
                "n_buy_orders",
                "buy_order_vol",
                "n_sell_orders",
                "sell_order_vol",
            ],
            split_key=True,
        )

    def _ingest_bronze_cafef_prop_trading(self) -> None:
        """CafeF proprietary-desk trades — brokers' own-account buy/sell volume and
        value. Key kept SPLIT; PK (exchange, ticker, date)."""
        self._ingest_bronze_cafef_daily(
            folder="prop_trading",
            table_name="cafef_prop_trading",
            decimal_cols=["prop_buy_val", "prop_sell_val"],
            bigint_cols=["prop_buy_vol", "prop_sell_vol"],
            split_key=True,
        )

    def _ingest_bronze_cafef_insider_shareholder_transactions(self) -> None:
        """CafeF insider & major-shareholder transactions — registered (planned) vs
        actually-executed buy/sell by insiders, related persons and major
        shareholders. EVENT-based (one row per transaction, not a daily series), so
        there is no natural (symbol, date) key. Loaded raw-faithful from the
        `insider_txn/` scraper folder with a deterministic md5 `row_id` surrogate PK
        (hash of the full raw row) so re-ingests stay idempotent."""
        self._logger.log_info(
            "Ingesting bronze CafeF insider/shareholder transactions data..."
        )

        df = self._helper_load_cafef_folder("insider_txn")
        if df is None:
            self._logger.log_error('No valid CafeF "insider_txn" CSV data found.')
            return

        # Keep the key split (exchange + ticker); the raw CSV stores them apart.
        df = df.rename(columns={"symbol": "ticker"})
        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
            ],
        )

        # Deterministic surrogate PK = md5 of the full raw row (computed on the
        # string form, before casting, so it is stable across re-ingests).
        hash_src = df.astype("string").fillna("")
        df["row_id"] = [
            hashlib.md5("|".join(row).encode("utf-8")).hexdigest()
            for row in hash_src.itertuples(index=False, name=None)
        ]

        df = self._helper_cast_columns(
            df,
            decimal_cols=["ownership_pct"],
            bigint_cols=[
                "vol_before",
                "plan_buy_vol",
                "plan_sell_vol",
                "real_buy_vol",
                "real_sell_vol",
                "vol_after",
            ],
        )

        date_cols = [
            "plan_begin_date",
            "plan_end_date",
            "real_end_date",
            "order_date",
            "published_date",
        ]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        df = self._helper_remove_duplicates(df, primary_keys=["row_id"])

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="cafef_insider_shareholder_transactions",
            primary_keys=["row_id"],
            df=df,
            dtype_overrides={
                **{c: DataType.DATE() for c in date_cols},
                "row_id": DataType.VARCHAR(32),
                "transaction_man": DataType.TEXT(),
                "position": DataType.TEXT(),
                "related_man": DataType.TEXT(),
                "related_position": DataType.TEXT(),
                "note": DataType.TEXT(),
                "profile_url": DataType.TEXT(),
            },
        )

    def _ingest_bronze_cafef_news(self) -> None:
        """CafeF company-news / disclosure feed — one row per article, from the
        `news/` scraper folder. A point-in-time EVENT stream (headline, body, event
        type and topic category, plus the filing PDF link for disclosures), not a
        daily series, so there is no (symbol, date) key.

        Three shape differences from the other CafeF folders:
        - the CSV carries NO exchange/ticker column — they exist only in the filename
          (`<EXCHANGE>_<SYMBOL>.csv`), so the key is rebuilt from the path;
        - the key is stored SPLIT as (exchange, ticker) rather than folded into the
          `symbol = "<EXCHANGE>:<TICKER>"` convention the daily CafeF tables use — the
          colon key exists so the three price sources merge uniformly in silver, and
          news has nothing to merge with; `simplize_industry` keys the same way;
        - `order` is a reserved SQL word → stored as `news_order`.

        PK = md5 of (exchange, ticker, url). The URL is the article's identity and is
        already unique per ticker (the scraper dedups on it), so hashing it — rather
        than the whole row, as the insider-transaction table does — keeps a re-scrape
        whose body text changed (a corrected article, a filled-in `type=error` row) an
        UPDATE of the same row instead of a second copy of the same article."""
        self._logger.log_info("Ingesting bronze CafeF news data...")

        files = glob(
            os.path.join(CAFEF_RAW_DATA_DIR, "news", "**", "*.csv"), recursive=True
        )

        frames = []
        for file_path in files:
            df = pd.read_csv(file_path, encoding="utf-8")
            if df.empty or df.dropna(how="all").empty:
                continue
            # The ticker lives ONLY in the filename: `<EXCHANGE>_<SYMBOL>.csv`.
            exchange, _, ticker = os.path.splitext(os.path.basename(file_path))[
                0
            ].partition("_")
            df["exchange"] = exchange
            df["ticker"] = ticker
            frames.append(df)

        if not frames:
            self._logger.log_error('No valid CafeF "news" CSV data found.')
            return

        df = pd.concat(frames, ignore_index=True).drop_duplicates()

        df = df.rename(columns={"order": "news_order"})

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("url"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["exchange", "ticker", "timestamp"]),
            ],
        )

        df["row_id"] = [
            hashlib.md5(f"{exchange}|{ticker}|{url}".encode("utf-8")).hexdigest()
            for exchange, ticker, url in zip(df["exchange"], df["ticker"], df["url"])
        ]

        df = self._helper_cast_columns(df, decimal_cols=[], bigint_cols=["news_order"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        df = self._helper_remove_duplicates(df, primary_keys=["row_id"])

        df = df[
            [
                "row_id",
                "exchange",
                "ticker",
                "news_order",
                "timestamp",
                "type",
                "category",
                "headline",
                "content",
                "url",
                "pdf_url",
            ]
        ]

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="cafef_news",
            primary_keys=["row_id"],
            df=df,
            dtype_overrides={
                "row_id": DataType.VARCHAR(32),
                "timestamp": DataType.TIMESTAMP(),
                "headline": DataType.TEXT(),
                "content": DataType.TEXT(),
                "url": DataType.TEXT(),
                "pdf_url": DataType.TEXT(),
            },
        )

    # ── CafeF financials: the parsed quarterly statements ────────────────────
    # raw_data/cafef/financials/ is built OFFLINE (src/web_scraper/cafef_*.py reads
    # the local PDF archive), and its folder layout is the schema:
    #
    #   schema/<template>_<report>.csv                  the 4 charts of accounts x 3
    #   statements/<template>/<report>/<EXCH>_<SYM>.csv  the quarterly panels
    #   templates.csv                                    ticker -> template + cf method
    #
    # THE TEMPLATE IS A FOLDER, NOT A COLUMN. Vietnam has four charts of accounts
    # among listed companies (bank / corp / securities / insurance) and they share no
    # line items — every one has a "code 1" and it means a different thing in each —
    # so their columns must never meet in one table. That gives 12 statement tables
    # (4 templates x 3 reports), each schema-homogeneous, + 3 reference tables:
    #
    #   cafef_financial_templates   ticker -> which of the 12 tables to look in
    #   cafef_financial_schema      the line-item dictionary (column -> printed name)
    #   cafef_financial_reports     per-DOCUMENT metadata, incl. publish_date
    #   cafef_financials_<template>_<report>   the figures
    #
    # Only the templates that have actually been parsed get a table; today that is
    # `bank` (VCB), so 3 of the 12 exist.

    CAFEF_FINANCIAL_TEMPLATES = ("bank", "corp", "securities", "insurance")
    CAFEF_FINANCIAL_REPORTS = ("balance_sheet", "income_statement", "cash_flow")

    # The 14 non-line-item columns every statement CSV carries. They describe the
    # DOCUMENT a quarter was read from, not the accounts, and are split off into
    # `cafef_financial_reports`; `source` is kept on the statement table too so a
    # `missing` quarter is identifiable without a join.
    #
    # ⚠️ A COLUMN ADDED TO THE STATEMENT CSV MUST BE LISTED HERE OR IT IS TREATED AS A
    # LINE ITEM — `line_cols` is defined as "everything that is not meta", and a text
    # column reaching `_helper_cast_columns(decimal_cols=…)` is not a wrong number, it
    # is a failed cast. That is what `method` (the OCR parse layer: "onnx@200",
    # "tesseract@200") would have hit.
    CAFEF_FINANCIAL_META_COLS = (
        "exchange",
        "ticker",
        "template",
        "period",
        "year",
        "quarter",
        "method",
        "source",
        "publish_date",
        "assurance",
        "cash_flow_method",
        "unit",
        "n_columns",
        "document",
    )
    CAFEF_FINANCIAL_KEY_COLS = (
        "exchange",
        "ticker",
        "template",
        "period",
        "year",
        "quarter",
        "source",
    )
    # Share counts read from the filing's "Vốn cổ phần" note. A per-DOCUMENT fact like
    # publish_date, but — unlike the other meta — kept on EACH statement table (not split into
    # `cafef_financial_reports`), so a consumer of one statement has the share count without a
    # join. They ride through as line columns; listed here only so they are cast as whole-share
    # BIGINT rather than decimal (a share count is an integer, never a fraction).
    CAFEF_FINANCIAL_SHARE_COLS = (
        "shares_authorized",
        "shares_issued",
        "shares_outstanding",
    )

    @staticmethod
    def _helper_sql_safe_line_id(line_id: str) -> str:
        """133 of the 753 line items are named with the flat ARABIC numbering the
        filing prints (`1_thu_nhap_lai…`, `10_chi_phi_quan_ly_doanh_nghiep`), and
        PostgreSQL only accepts an identifier starting with a digit if it is QUOTED —
        which this driver never does (it interpolates bare names into its DDL/DML).

        So a leading digit takes an `n` prefix, and ONLY that: everything else is left
        exactly as the parser named it. The mapping is injective (no `n<digit>…` name
        exists) and is recorded in `cafef_financial_schema` as `sql_column` beside the
        untouched `line_id`, so the printed line is always recoverable."""
        return f"n{line_id}" if re.match(r"^\d", str(line_id)) else line_id

    def _ingest_bronze_cafef_financials(self) -> None:
        """Ingest the whole `raw_data/cafef/financials/` tree — the 3 reference
        tables + one wide table per (template, report) that has been parsed."""
        self._ingest_bronze_cafef_financial_templates()
        self._ingest_bronze_cafef_financial_schema()
        self._ingest_bronze_cafef_financial_statements()

    def _ingest_bronze_cafef_financial_templates(self) -> None:
        """`templates.csv` — the map from a ticker to WHICH of the statement tables
        holds it, plus the cash-flow method its filings use.

        Load-bearing, not a convenience: a consumer holding a ticker cannot otherwise
        tell which of the four schema-homogeneous tables to read. It also carries the
        GICS sector/industry group beside the fingerprinted template so the two can be
        seen to disagree — HVA sits in the securities industry group and files on the
        CORPORATE template. The template is fingerprinted from the filing's own chart
        of accounts, never classified from the sector (see web_scraper/CONTEXT.md)."""
        self._logger.log_info("Ingesting bronze CafeF financial templates...")

        file_path = os.path.join(CAFEF_RAW_DATA_DIR, "financials", "templates.csv")
        if not os.path.exists(file_path):
            self._logger.log_error(
                f'No CafeF financials "templates.csv" at {file_path}.'
            )
            return

        df = pd.read_csv(file_path, encoding="utf-8")
        df = df.rename(columns={"symbol": "ticker"})

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["exchange", "ticker"]),
            ],
        )
        df = self._helper_remove_duplicates(df, primary_keys=["exchange", "ticker"])

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="cafef_financial_templates",
            primary_keys=["exchange", "ticker"],
            df=df,
            dtype_overrides={"sector": DataType.TEXT()},
        )

    def _ingest_bronze_cafef_financial_schema(self) -> None:
        """The 12 `schema/<template>_<report>.csv` charts of accounts, concatenated
        into ONE dictionary table — this is the only place the four templates may
        meet, because here a line item is a ROW (a fact about the template), not a
        column. PK (template, report, line_id).

        It is what lets a consumer go from a column name in a statement table back to
        the Vietnamese line the filing actually printed (`as_printed`), and to CafeF's
        own item code. Two columns are renamed off reserved SQL words:
        `order` → `line_order`, `column` → `line_id`."""
        self._logger.log_info("Ingesting bronze CafeF financial schema...")

        files = sorted(
            glob(os.path.join(CAFEF_RAW_DATA_DIR, "financials", "schema", "*.csv"))
        )

        frames = []
        for file_path in files:
            df = pd.read_csv(file_path, encoding="utf-8")
            if df.empty:
                continue
            # `<template>_<report>.csv` — the report is the part after the template,
            # and both the template and the report names contain underscores
            # (`income_statement`), so split on the KNOWN template prefix.
            stem = os.path.splitext(os.path.basename(file_path))[0]
            template = next(
                (t for t in self.CAFEF_FINANCIAL_TEMPLATES if stem.startswith(f"{t}_")),
                None,
            )
            if template is None:
                self._logger.log_error(f"Unknown financial schema template: {stem}")
                continue
            df["report"] = stem[len(template) + 1 :]
            frames.append(df)

        if not frames:
            self._logger.log_error("No CafeF financial schema CSVs found.")
            return

        df = pd.concat(frames, ignore_index=True)
        df = df.rename(columns={"order": "line_order", "column": "line_id"})
        df["sql_column"] = df["line_id"].map(self._helper_sql_safe_line_id)

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("template"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("line_id"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["template", "report", "line_order"]),
            ],
        )
        df = self._helper_cast_columns(
            df, decimal_cols=[], bigint_cols=["line_order", "level"]
        )
        df = self._helper_remove_duplicates(
            df, primary_keys=["template", "report", "line_id"]
        )

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="cafef_financial_schema",
            primary_keys=["template", "report", "line_id"],
            df=df,
            dtype_overrides={
                "line_id": DataType.TEXT(),
                "sql_column": DataType.TEXT(),
                "as_printed": DataType.TEXT(),
                "cafef_code": DataType.TEXT(),
            },
        )

    def _ingest_bronze_cafef_financial_statements(self) -> None:
        """The quarterly panels — one wide table per (template, report) that exists on
        disk, plus the `cafef_financial_reports` metadata table built from the same
        pass.

        ⚠️ `publish_date` is THE column downstream must join on — it is the day the
        figures became public, read from inside the filing, and it is not the period
        end: VCB's Q4-2025 covers the quarter ending 31 Dec 2025 but was not published
        until 27 Mar 2026. Joining fundamentals to prices on the period end hands a
        model twelve weeks of look-ahead every year.

        A quarter that could not be read is written as a blank `source='missing'` row,
        never zero-filled, and the panel is a contiguous quarter grid — so the
        null-drop layers deliberately gate on the KEY columns only, never on the line
        items. Figures are already absolute VND (the parser applied the filing's unit),
        so `unit` is provenance, not a scale factor to re-apply."""
        report_rows = []

        for template in self.CAFEF_FINANCIAL_TEMPLATES:
            for report in self.CAFEF_FINANCIAL_REPORTS:
                folder = os.path.join(
                    CAFEF_RAW_DATA_DIR, "financials", "statements", template, report
                )
                files = sorted(glob(os.path.join(folder, "*.csv")))
                if not files:
                    continue

                self._logger.log_info(
                    f"Ingesting bronze CafeF financials: {template}/{report}..."
                )

                frames = [
                    df
                    for fp in files
                    if not (df := pd.read_csv(fp, encoding="utf-8")).empty
                ]
                if not frames:
                    self._logger.log_error(
                        f"No valid CafeF financial statement CSVs in {folder}."
                    )
                    continue

                df = pd.concat(frames, ignore_index=True)
                df = df.rename(columns={"symbol": "ticker"})
                df["report"] = report

                df = self._helper_clean(
                    df,
                    [
                        CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                        CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                        CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("period"),
                        CleanLayer.ORDER_BY(["exchange", "ticker", "year", "quarter"]),
                    ],
                )

                df["publish_date"] = pd.to_datetime(
                    df["publish_date"], errors="coerce"
                ).dt.date

                # Split the document metadata off into the shared reports table…
                report_rows.append(
                    df[["report", *self.CAFEF_FINANCIAL_META_COLS]].copy()
                )

                # …and keep the figures, keyed, on the per-template table. The line
                # items are renamed only where PostgreSQL forces it (leading digit).
                line_cols = [
                    c
                    for c in df.columns
                    if c not in self.CAFEF_FINANCIAL_META_COLS and c != "report"
                ]
                df = df[[*self.CAFEF_FINANCIAL_KEY_COLS, *line_cols]]
                df = df.rename(
                    columns={c: self._helper_sql_safe_line_id(c) for c in line_cols}
                )
                line_cols = [self._helper_sql_safe_line_id(c) for c in line_cols]
                # The share counts ride through as line columns but are whole shares, so they are
                # cast BIGINT, not decimal like the đồng figures.
                share_cols = [c for c in self.CAFEF_FINANCIAL_SHARE_COLS if c in line_cols]
                decimal_line_cols = [c for c in line_cols if c not in share_cols]
                df = self._helper_cast_columns(
                    df, decimal_cols=decimal_line_cols, bigint_cols=[]
                )
                df = self._helper_cast_columns(
                    df, decimal_cols=[], bigint_cols=["year", "quarter", *share_cols]
                )
                df = self._helper_remove_duplicates(
                    df, primary_keys=["exchange", "ticker", "year", "quarter"]
                )

                self._helper_save_pandas_table_to_database(
                    schema_name=BRONZE_SCHEMA,
                    table_name=f"cafef_financials_{template}_{report}",
                    primary_keys=["exchange", "ticker", "year", "quarter"],
                    df=df,
                )

        if not report_rows:
            self._logger.log_error("No CafeF financial statement CSVs found.")
            return

        self._logger.log_info("Ingesting bronze CafeF financial reports...")

        reports = pd.concat(report_rows, ignore_index=True)
        reports = self._helper_cast_columns(
            reports,
            decimal_cols=["unit"],
            bigint_cols=["year", "quarter", "n_columns"],
        )
        reports = self._helper_remove_duplicates(
            reports, primary_keys=["exchange", "ticker", "report", "year", "quarter"]
        )

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="cafef_financial_reports",
            primary_keys=["exchange", "ticker", "report", "year", "quarter"],
            df=reports,
            dtype_overrides={
                "publish_date": DataType.DATE(),
                "document": DataType.TEXT(),
            },
        )

    def _ingest_bronze_stocks_simplize(self) -> None:
        """Bronze table for Simplize per-stock daily data — the validated backbone
        of the daily panel: fully dividend-adjusted OHLC (CafeF only adjusts the
        close; TradingView's volume is split-inflated), true total traded volume,
        and foreign buy/sell/net flow (volume + value) plus remaining room.

        Kept as a separate bronze table because its schema differs from the
        TradingView `stocks` and CafeF `stocks_cafef` tables; the sources are merged
        in silver. Key is normalised to `symbol = "<EXCHANGE>:<TICKER>"` to match the
        TradingView convention so the silver merge can split it the same way.
        """
        self._logger.log_info("Ingesting bronze Simplize stocks data...")

        stocks_dir = os.path.join(SIMPLIZE_RAW_DATA_DIR, "stocks")
        csv_files = glob(os.path.join(stocks_dir, "**", "*.csv"), recursive=True)

        if not csv_files:
            self._logger.log_error(
                f'No Simplize stocks CSV files found in "{stocks_dir}".'
            )
            return

        dataframes = []
        for fp in csv_files:
            df = pd.read_csv(fp, encoding="utf-8")
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            self._logger.log_error("No valid Simplize stocks CSV data found.")
            return

        df = pd.concat(dataframes, ignore_index=True).drop_duplicates()

        # Simplize stores the key split already; keep it that way (exchange + ticker).
        df = df.rename(columns={"symbol": "ticker"})

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("close"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["exchange", "ticker", "date"]),
            ],
        )

        df = self._helper_cast_columns(
            df,
            decimal_cols=[
                "open",
                "high",
                "low",
                "close",
                "net_change",
                "percentage_change",
                "foreign_buy_value",
                "foreign_sell_value",
                "foreign_net_value",
            ],
            bigint_cols=[
                "volume",
                "foreign_room",
                "foreign_buy_volume",
                "foreign_sell_volume",
                "foreign_net_volume",
            ],
        )

        df["date"] = pd.to_datetime(df["date"]).dt.date

        df = self._helper_remove_duplicates(
            df, primary_keys=["exchange", "ticker", "date"]
        )

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="simplize_stocks",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_bronze_simplize_industry(self) -> None:
        """Bronze table for Simplize per-ticker industry (GICS-based VN taxonomy:
        10 economic sectors / 50 industry groups, accurate per ticker). Loaded
        as-is from raw_data/simplize/industry.csv; the source for GICS
        classification (merged with bronze.gics in silver). PK (exchange, ticker)."""
        self._logger.log_info("Ingesting bronze Simplize industry data...")

        path = os.path.join(SIMPLIZE_RAW_DATA_DIR, "industry.csv")
        if not os.path.exists(path):
            self._logger.log_error(f'Simplize industry CSV not found at "{path}".')
            return

        df = pd.read_csv(path, encoding="utf-8", dtype=str)
        if df.empty:
            self._logger.log_error("No Simplize industry data found.")
            return

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["exchange", "ticker"]),
            ],
        )
        df = self._helper_remove_duplicates(df, primary_keys=["exchange", "ticker"])

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="simplize_industry",
            primary_keys=["exchange", "ticker"],
            df=df,
        )

    def _ingest_bronze_gics(self) -> None:
        """Bronze table for the official MSCI GICS structure (reference taxonomy):
        11 sectors / 25 industry groups / 74 industries / 163 sub-industries, each
        with code + name + snake_case, plus the sub-industry definition. Loaded
        as-is from raw_data/gics; one row per sub-industry (PK sub_industry_code)."""
        self._logger.log_info("Ingesting bronze GICS structure...")

        path = os.path.join(GICS_RAW_DATA_DIR, "gics_2023_official.csv")
        if not os.path.exists(path):
            self._logger.log_error(f'GICS structure CSV not found at "{path}".')
            return

        df = pd.read_csv(path, encoding="utf-8", dtype=str)
        if df.empty:
            self._logger.log_error("No GICS structure data found.")
            return

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("sub_industry_code"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["sub_industry_code"]),
            ],
        )
        df = self._helper_remove_duplicates(df, primary_keys=["sub_industry_code"])

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="gics",
            primary_keys=["sub_industry_code"],
            df=df,
            # Sub-industry definitions are full sentences — exceed VARCHAR(255).
            dtype_overrides={"sub_industry_definition": DataType.TEXT()},
        )

    def _ingest_bronze_bonds(self) -> None:
        self._logger.log_info("Ingesting bronze bonds data...")

        bonds_dir = os.path.join(TRADING_VIEW_RAW_DATA_DIR, "data", "bonds")
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
        df = self._helper_split_symbol_column(df)

        self._helper_save_pandas_table_to_database(
            schema_name=BRONZE_SCHEMA,
            table_name="trading_view_bonds",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_silver_bonds(self) -> None:
        self._logger.log_info("Ingesting silver bonds data...")

        df = self._helper_select(
            schema_name=BRONZE_SCHEMA, table_name="trading_view_bonds"
        )

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

        df = self._helper_select(
            schema_name=BRONZE_SCHEMA, table_name="trading_view_economy"
        )

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

        df = self._helper_select(
            schema_name=BRONZE_SCHEMA, table_name="trading_view_forex"
        )

        if df.empty:
            self._logger.log_info("No bronze forex data found.")
            return

        df["exchange"] = df["symbol"].str.split(":").str[0]
        df["ticker"] = df["symbol"].str.split(":").str[1]

        # Forex is a single-value series (OHLC columns are null in bronze;
        # the price lives in `value`), so treat it like bonds/economy.
        df = df[["exchange", "ticker", "date", "value"]]
        df = self._helper_cast_columns(df, decimal_cols=["value"], bigint_cols=[])

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="forex",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_silver_funds(self) -> None:
        self._logger.log_info("Ingesting silver funds data...")

        df = self._helper_select(
            schema_name=BRONZE_SCHEMA, table_name="trading_view_funds"
        )

        if df.empty:
            self._logger.log_info("No bronze funds data found.")
            return

        df["exchange"] = df["symbol"].str.split(":").str[0]
        df["ticker"] = df["symbol"].str.split(":").str[1]

        df = df[
            ["exchange", "ticker", "date", "open", "high", "low", "close", "volume"]
        ]
        df = self._helper_cast_columns(
            df, decimal_cols=["open", "high", "low", "close"], bigint_cols=["volume"]
        )

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="funds",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_silver_indices(self) -> None:
        self._logger.log_info("Ingesting silver indices data...")

        df = self._helper_select(
            schema_name=BRONZE_SCHEMA, table_name="trading_view_indices"
        )

        if df.empty:
            self._logger.log_info("No bronze indices data found.")
            return

        df["exchange"] = df["symbol"].str.split(":").str[0]
        df["ticker"] = df["symbol"].str.split(":").str[1]

        df = df[
            ["exchange", "ticker", "date", "open", "high", "low", "close", "volume"]
        ]
        df = self._helper_cast_columns(
            df, decimal_cols=["open", "high", "low", "close"], bigint_cols=["volume"]
        )

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="indices",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_silver_gics(self) -> None:
        """Silver `gics` — the bronze MSCI GICS taxonomy carried up with a basic
        clean pass (drop rows null on the key or all columns, order by key). It is
        a reference table, not per-source, so there is nothing to merge; PK stays
        `sub_industry_code`. The old silver table is dropped first so a schema
        change re-materialises cleanly past the driver's IF NOT EXISTS create."""
        self._logger.log_info("Ingesting silver GICS structure...")

        df = self._helper_select(schema_name=BRONZE_SCHEMA, table_name="gics")

        if df.empty:
            self._logger.log_info("No bronze gics data found.")
            return

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("sub_industry_code"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["sub_industry_code"]),
            ],
        )

        self._database_driver.drop_table(SILVER_SCHEMA, "gics")

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="gics",
            primary_keys=["sub_industry_code"],
            df=df,
            dtype_overrides={"sub_industry_definition": DataType.TEXT()},
        )

    def _ingest_silver_cafef_price(self) -> None:
        """Silver `cafef_price` — the bronze CafeF price table carried up with a
        basic clean pass (drop rows null on the key or all columns, order by key).
        Bronze is already keyed `(exchange, ticker)`, so there is no symbol to split.
        The existing silver table is dropped first so a schema change re-materialises
        cleanly rather than colliding with the `IF NOT EXISTS` create. PK
        `(exchange, ticker, date)`."""
        self._logger.log_info("Ingesting silver CafeF price data...")

        df = self._helper_select(schema_name=BRONZE_SCHEMA, table_name="cafef_price")

        if df.empty:
            self._logger.log_info("No bronze cafef_price data found.")
            return

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["exchange", "ticker", "date"]),
            ],
        )

        # Drop the old silver table first (the user's explicit request, and it lets a
        # changed schema re-materialise cleanly past the driver's IF NOT EXISTS create).
        self._database_driver.drop_table(SILVER_SCHEMA, "cafef_price")

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="cafef_price",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_silver_cafef_daily(self, table_name: str) -> None:
        """Carry a DAILY CafeF bronze table (order_stats / foreign / prop_trading)
        up to silver with the same basic clean pass as `cafef_price`: drop rows null
        on the key or all columns, order by key, drop the old silver table first.
        Same name and shape as bronze; PK `(exchange, ticker, date)`."""
        self._logger.log_info(f"Ingesting silver CafeF {table_name} data...")

        df = self._helper_select(schema_name=BRONZE_SCHEMA, table_name=table_name)

        if df.empty:
            self._logger.log_info(f"No bronze {table_name} data found.")
            return

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["exchange", "ticker", "date"]),
            ],
        )

        self._database_driver.drop_table(SILVER_SCHEMA, table_name)

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name=table_name,
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_silver_cafef_order_stats(self) -> None:
        self._ingest_silver_cafef_daily("cafef_order_stats")

    def _ingest_silver_cafef_foreign(self) -> None:
        self._ingest_silver_cafef_daily("cafef_foreign")

    def _ingest_silver_cafef_prop_trading(self) -> None:
        self._ingest_silver_cafef_daily("cafef_prop_trading")

    def _ingest_silver_cafef_insider_shareholder_transactions(self) -> None:
        """Carry the CafeF insider / major-shareholder transactions up to silver.
        EVENT-based (one row per transaction), so it keeps bronze's md5 `row_id`
        surrogate PK — there is no `(exchange, ticker, date)` key. Basic clean only;
        the five date columns and the long free-text columns keep their bronze type
        overrides (a default VARCHAR(255) would truncate `note` / `profile_url`)."""
        table_name = "cafef_insider_shareholder_transactions"
        self._logger.log_info(f"Ingesting silver CafeF {table_name} data...")

        df = self._helper_select(schema_name=BRONZE_SCHEMA, table_name=table_name)

        if df.empty:
            self._logger.log_info(f"No bronze {table_name} data found.")
            return

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("row_id"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["exchange", "ticker"]),
            ],
        )

        date_cols = [
            "plan_begin_date", "plan_end_date", "real_end_date",
            "order_date", "published_date",
        ]

        self._database_driver.drop_table(SILVER_SCHEMA, table_name)

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name=table_name,
            primary_keys=["row_id"],
            df=df,
            dtype_overrides={
                **{c: DataType.DATE() for c in date_cols},
                "row_id": DataType.VARCHAR(32),
                "transaction_man": DataType.TEXT(),
                "position": DataType.TEXT(),
                "related_man": DataType.TEXT(),
                "related_position": DataType.TEXT(),
                "note": DataType.TEXT(),
                "profile_url": DataType.TEXT(),
            },
        )

    # Non-line-item columns on every bronze `cafef_financials_<template>_<report>`
    # table (the surviving subset of the key cols; the document-metadata columns were
    # split off into `cafef_financial_reports` at bronze time). Everything on the
    # statement table that is NOT one of these is a numeric line item.
    CAFEF_FINANCIAL_STATEMENT_TEXT_COLS = ("exchange", "ticker", "template", "period", "source")
    CAFEF_FINANCIAL_STATEMENT_KEY = ["exchange", "ticker", "year", "quarter"]

    def _list_bronze_financial_statement_tables(self) -> list[str]:
        """Return the bronze `cafef_financials_<template>_<report>` statement tables
        that actually exist (only parsed templates get a table — today just `bank`,
        so 3 of the 12). Discovered from `information_schema.tables` so this grows
        automatically as more templates are parsed. The `cafef_financials_` prefix
        (note the trailing `s`) uniquely selects the statement tables and EXCLUDES the
        three metadata tables `cafef_financial_reports` / `_schema` / `_templates`
        (which start with `cafef_financial_`, no `s`)."""
        tables = self._helper_select(
            schema_name="information_schema",
            table_name="tables",
            columns=["table_name"],
            conditions=[
                Condition(
                    column="table_schema",
                    operator=SqlOperator.EQUAL_TO,
                    value=BRONZE_SCHEMA,
                    data_type=DataType.VARCHAR(),
                )
            ],
        )
        if tables.empty:
            return []
        names = tables["table_name"].astype("string")
        return sorted(names[names.str.startswith("cafef_financials_")].tolist())

    def _ingest_silver_cafef_financials(self) -> None:
        """Carry every bronze `cafef_financials_<template>_<report>` STATEMENT table
        up to silver, one-to-one, same name and PK `(exchange, ticker, year, quarter)`.
        The three metadata tables (`cafef_financial_reports` / `_schema` / `_templates`)
        are deliberately NOT carried — they describe the filings / chart of accounts,
        not the figures.

        Same basic-clean-then-cast pattern as the other per-source CafeF carry-ups,
        with one financials-specific rule (see `_ingest_bronze_cafef_financial_statements`):
        a quarter that could not be read is a blank `source='missing'` row on a
        contiguous quarter grid, never zero-filled — so the null-drop gates on the KEY
        columns ONLY, never on the line items, and `REMOVE_IF_ALL_COLUMNS_ARE_NULL` is
        omitted (a missing-quarter row is legitimately empty apart from its keys).
        Line items are cast back to decimal, `year`/`quarter` to bigint; the old silver
        table is dropped first so a schema change re-materialises cleanly."""
        statement_tables = self._list_bronze_financial_statement_tables()
        if not statement_tables:
            self._logger.log_info(
                "No bronze cafef_financials_<template>_<report> tables found."
            )
            return

        for table_name in statement_tables:
            self._logger.log_info(f"Ingesting silver CafeF {table_name} data...")

            df = self._helper_select(schema_name=BRONZE_SCHEMA, table_name=table_name)
            if df.empty:
                self._logger.log_info(f"No bronze {table_name} data found.")
                continue

            # Null-drop on the KEY columns only — never the line items (a `missing`
            # quarter is a legitimate blank row and must survive).
            df = self._helper_clean(
                df,
                [
                    CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                    CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                    CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("year"),
                    CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("quarter"),
                    CleanLayer.ORDER_BY(self.CAFEF_FINANCIAL_STATEMENT_KEY),
                ],
            )

            # Everything that is not a text/key column is a numeric line item — except the
            # share counts, which are whole shares and stay BIGINT (as in bronze).
            line_cols = [
                c
                for c in df.columns
                if c not in self.CAFEF_FINANCIAL_STATEMENT_TEXT_COLS
                and c not in ("year", "quarter")
            ]
            share_cols = [c for c in self.CAFEF_FINANCIAL_SHARE_COLS if c in line_cols]
            decimal_line_cols = [c for c in line_cols if c not in share_cols]
            df = self._helper_cast_columns(
                df,
                decimal_cols=decimal_line_cols,
                bigint_cols=["year", "quarter", *share_cols],
            )

            self._database_driver.drop_table(SILVER_SCHEMA, table_name)

            self._helper_save_pandas_table_to_database(
                schema_name=SILVER_SCHEMA,
                table_name=table_name,
                primary_keys=self.CAFEF_FINANCIAL_STATEMENT_KEY,
                df=df,
            )

    def _ingest_silver_cafef_financials_template(self, template: str) -> None:
        """Combine one template's three per-report silver statement tables
        (`cafef_financials_<template>_{balance_sheet,income_statement,cash_flow}`)
        into ONE wide table `cafef_financials_<template>`, OUTER-joined on
        `(exchange, ticker, year, quarter)`.

        Every NON-KEY column is prefixed with its report — `balance_sheet_…`,
        `income_statement_…`, `cash_flow_…` — so line items from the three statements
        never collide and provenance is explicit. The shared metadata columns
        (`template`/`period`/`source`) are prefixed too, so each report keeps its own
        (a quarter can be `source='missing'` in one statement but present in another,
        and the three statements of one quarter often come from different documents).

        A single, unprefixed **`publish_date`** column is joined on from
        `bronze.cafef_financial_reports` (which is per-report): the day the figures
        became public — ⚠️ NOT the period end (VCB's Q4 covers the quarter ending 31 Dec
        but is published the following March, so joining fundamentals to prices on the
        period end hands a model ~12 weeks of look-ahead). It is unprefixed because all
        three reports of a quarter publish on the same day (verified: 0 of the quarters
        with a non-null date disagree across reports), so one column suffices.

        OUTER join so a quarter present in any statement survives even if another
        statement lacks it; on the contiguous `missing`-row grid the three tables cover
        the same quarter set, so in practice the join is 1:1. PK
        `(exchange, ticker, year, quarter)`; the old combined table is dropped first."""
        reports = ("balance_sheet", "income_statement", "cash_flow")
        key = self.CAFEF_FINANCIAL_STATEMENT_KEY
        out_table = f"cafef_financials_{template}"

        merged: pd.DataFrame | None = None
        decimal_cols: list[str] = []
        shares_df: pd.DataFrame | None = None
        for report in reports:
            src_table = f"cafef_financials_{template}_{report}"
            df = self._helper_select(schema_name=SILVER_SCHEMA, table_name=src_table)
            if df.empty:
                self._logger.log_info(
                    f"No silver {src_table} data found; skipping it in {out_table}."
                )
                continue

            # The share counts are a per-DOCUMENT fact, identical across the three statements
            # of a quarter (one filing, one capital note), so they are NOT report-prefixed —
            # that would mint 9 duplicate columns. They are dropped here and re-attached once,
            # unprefixed, after the join (like publish_date). Kept from the balance_sheet
            # statement so a single source wins deterministically.
            present_shares = [c for c in self.CAFEF_FINANCIAL_SHARE_COLS if c in df.columns]
            if report == "balance_sheet" and present_shares:
                shares_df = df[key + present_shares].copy()
            df = df.drop(columns=present_shares)

            # Prefix every non-key column with the report; keys stay bare for the join.
            non_key = [c for c in df.columns if c not in key]
            df = df.rename(columns={c: f"{report}_{c}" for c in non_key})

            # Track which prefixed columns are numeric line items (everything except
            # the prefixed metadata) so the combined frame can be cast in one pass.
            meta = set(self.CAFEF_FINANCIAL_STATEMENT_TEXT_COLS)  # exchange/ticker excl. below
            for c in non_key:
                if c not in meta:
                    decimal_cols.append(f"{report}_{c}")

            merged = df if merged is None else merged.merge(df, on=key, how="outer")

        if merged is None:
            self._logger.log_info(
                f"No silver statement tables found for template '{template}'; "
                f"{out_table} not built."
            )
            return

        self._logger.log_info(f"Ingesting silver {out_table} (3 statements combined)...")

        # ── One publish_date per quarter, joined on from bronze.cafef_financial_reports
        #    (per-report). The 3 reports agree on it, so collapse to one value per key
        #    with max() (== the shared value; NULLs from a missing report drop out). ──
        reports_meta = self._helper_select(
            schema_name=BRONZE_SCHEMA, table_name="cafef_financial_reports"
        )
        if not reports_meta.empty and "publish_date" in reports_meta.columns:
            pub = reports_meta[reports_meta["template"] == template].copy()
            # Normalise to datetime so groupby().max() skips missing values cleanly
            # (the driver may hand publish_date back as object/date with None mixed in).
            pub["publish_date"] = pd.to_datetime(pub["publish_date"], errors="coerce")
            pub = pub.groupby(key, as_index=False)["publish_date"].max()
            # Back to plain dates for the DATE column.
            pub["publish_date"] = pub["publish_date"].dt.date
            merged = merged.merge(pub, on=key, how="left")
        else:
            self._logger.log_warning(
                "cafef_financial_reports has no publish_date; "
                f"{out_table}.publish_date will be null."
            )
            merged["publish_date"] = pd.NaT

        # Re-attach the share counts once, unprefixed — a per-document fact shared by the three
        # statements, exactly like publish_date. Kept from the balance_sheet statement.
        share_cols: list[str] = []
        if shares_df is not None:
            share_cols = [c for c in self.CAFEF_FINANCIAL_SHARE_COLS if c in shares_df.columns]
            merged = merged.merge(shares_df, on=key, how="left")

        # Place publish_date, then the share counts, right after the keys.
        lead = key + ["publish_date"] + share_cols
        merged = merged[lead + [c for c in merged.columns if c not in lead]]

        merged = self._helper_clean(
            merged,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("year"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("quarter"),
                CleanLayer.ORDER_BY(key),
            ],
        )
        merged = self._helper_cast_columns(
            merged,
            decimal_cols=[c for c in decimal_cols if c in merged.columns],
            bigint_cols=["year", "quarter", *share_cols],
        )

        self._database_driver.drop_table(SILVER_SCHEMA, out_table)
        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name=out_table,
            primary_keys=key,
            df=merged,
            dtype_overrides={"publish_date": DataType.DATE()},
        )

    def _ingest_silver_cafef_financials_bank(self) -> None:
        """Combine the three `bank` statements into silver `cafef_financials_bank`."""
        self._ingest_silver_cafef_financials_template("bank")

    def _helper_column_types(
        self, schema_name: str, table_name: str
    ) -> dict[str, str]:
        """Return `{column_name: data_type}` for a table, straight from
        `information_schema.columns`. Used to derive numeric/bigint cast lists from the
        live schema rather than hand-listing columns (the financials tables have ~180
        columns, and grow as more templates are parsed)."""
        info = self._helper_select(
            schema_name="information_schema",
            table_name="columns",
            columns=["column_name", "data_type"],
            conditions=[
                Condition(
                    column="table_schema",
                    operator=SqlOperator.EQUAL_TO,
                    value=schema_name,
                    data_type=DataType.VARCHAR(),
                ),
                Condition(
                    column="table_name",
                    operator=SqlOperator.EQUAL_TO,
                    value=table_name,
                    data_type=DataType.VARCHAR(),
                ),
            ],
        )
        if info.empty:
            return {}
        return dict(zip(info["column_name"], info["data_type"]))

    def _ingest_silver_stocks_basic_financials_bank(self) -> None:
        """Silver `stocks_basic_financials_bank` — the **daily** `silver.stocks_basic`
        panel joined to the **quarterly** `silver.cafef_financials_bank`, keeping **all
        columns of both** (no computed indicators — this is a straight join).

            silver.stocks_basic            (daily,  (exchange, ticker, date))
              INNER JOIN (as-of, backward on publish_date)
            silver.cafef_financials_bank   (quarterly, + publish_date + shares + lines)

        ⚠️ **The join is an as-of merge on `publish_date`, NOT the period end.** Each
        price day carries the financials of the most-recently-*published* quarter (the
        greatest `publish_date <= date`), so every financials column **steps** on its
        publish date and holds flat until the next filing drops — zero look-ahead. A
        quarter whose `publish_date` is NULL (the 6 earliest un-dated ones) is excluded
        from the as-of key: a fact with no public date can't be pinned to a day. Days
        before a ticker's first publish would get NULL financials, but this is scoped as
        an **INNER** join (only tickers with financials, only days on/after the first
        publish survive), so those pre-publish days drop out — the table is dense and
        grows as more bank tickers are parsed. Today: HOSE:VCB only.

        The two tables share no non-key column name, so the merge needs no suffixes:
        every output row is a `stocks_basic` day with the 38 price/GICS/flow columns,
        plus the as-of quarter's 177 financials columns (its `exchange`/`ticker` fold
        into the join keys). Numeric columns are re-cast from the driver's Decimal→object
        read so they don't degrade to VARCHAR (bigint stays bigint, numeric→Float64), the
        cast lists derived from each source table's live `information_schema` types. PK
        `(exchange, ticker, date)`; the old table is dropped first so a schema change
        re-materialises cleanly past the driver's IF NOT EXISTS."""
        self._logger.log_info(
            "Ingesting silver stocks_basic_financials_bank "
            "(stocks_basic × cafef_financials_bank, as-of on publish_date)..."
        )
        key = ["exchange", "ticker"]
        keys_out = ["exchange", "ticker", "date"]

        price = self._helper_select(
            schema_name=SILVER_SCHEMA, table_name="stocks_basic"
        )
        if price.empty:
            self._logger.log_info(
                "No silver stocks_basic data found; "
                "stocks_basic_financials_bank not built."
            )
            return

        fin = self._helper_select(
            schema_name=SILVER_SCHEMA, table_name="cafef_financials_bank"
        )
        if fin.empty:
            self._logger.log_info(
                "No silver cafef_financials_bank data found; "
                "stocks_basic_financials_bank not built."
            )
            return

        # Column types of both sources, to rebuild the numeric/bigint cast lists from the
        # live schema (union across the two tables; the join keys are excluded — they are
        # text and don't need casting).
        price_types = self._helper_column_types(SILVER_SCHEMA, "stocks_basic")
        fin_types = self._helper_column_types(SILVER_SCHEMA, "cafef_financials_bank")

        # Only keep the financials row with a public date — the as-of key can't use a
        # NULL publish_date. Sort both sides globally on the "on" key (merge_asof needs it).
        fin["publish_date"] = pd.to_datetime(fin["publish_date"], errors="coerce")
        fin = fin[fin["publish_date"].notna()].copy()
        if fin.empty:
            self._logger.log_warning(
                "cafef_financials_bank has no dated quarters; "
                "stocks_basic_financials_bank not built."
            )
            return

        # Restrict the price side to tickers that have financials (INNER scope) so the
        # as-of merge doesn't carry along 620 tickers of NULL financials.
        fin_keys = fin[key].drop_duplicates()
        price = price.merge(fin_keys, on=key, how="inner")
        if price.empty:
            self._logger.log_info(
                "No stocks_basic rows for the financials tickers; "
                "stocks_basic_financials_bank not built."
            )
            return

        price["date"] = pd.to_datetime(price["date"], errors="coerce")
        price = price.sort_values("date").reset_index(drop=True)
        fin = fin.sort_values("publish_date").reset_index(drop=True)

        merged = pd.merge_asof(
            price,
            fin,
            left_on="date",
            right_on="publish_date",
            by=key,
            direction="backward",
        )

        # Drop pre-first-publish days (no quarter was public yet → INNER scope drops them).
        merged = merged[merged["publish_date"].notna()].copy()
        if merged.empty:
            self._logger.log_info(
                "No price days on/after a publish_date; "
                "stocks_basic_financials_bank not built."
            )
            return

        # publish_date back to a plain date for the DATE column.
        merged["publish_date"] = merged["publish_date"].dt.date

        # Lay out columns: keys, date, publish_date, then the rest (stocks_basic block
        # followed by the financials block, both in their source order).
        lead = keys_out + ["publish_date"]
        merged = merged[lead + [c for c in merged.columns if c not in lead]]

        merged = self._helper_clean(
            merged,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.ORDER_BY(keys_out),
            ],
        )

        # Rebuild the numeric types the driver's Decimal→object read dropped. Derive the
        # cast lists from the two source schemas (a column is bigint iff it is bigint in
        # its source, else numeric→Float64); keys/date/publish_date/text pass through.
        col_types = {**price_types, **fin_types}
        skip = set(keys_out) | {"publish_date"}
        decimal_cols = [
            c
            for c in merged.columns
            if c not in skip and col_types.get(c) == "numeric"
        ]
        bigint_cols = [
            c
            for c in merged.columns
            if c not in skip and col_types.get(c) == "bigint"
        ]
        merged = self._helper_cast_columns(
            merged, decimal_cols=decimal_cols, bigint_cols=bigint_cols
        )

        self._database_driver.drop_table(
            SILVER_SCHEMA, "stocks_basic_financials_bank"
        )
        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="stocks_basic_financials_bank",
            primary_keys=keys_out,
            df=merged,
            dtype_overrides={
                "date": DataType.DATE(),
                "publish_date": DataType.DATE(),
            },
        )

    # ── Canonical bank line-item columns on `stocks_basic_financials_bank` (they carry
    #    the same report-prefixed names as `cafef_financials_bank`), keyed to the
    #    fundamental catalog in FUNDAMENTAL_INDICATORS.md §1. A future `corp` /
    #    `securities` / `insurance` FA table would supply its own map against these
    #    roles. ──
    BANK_FA_NET_INCOME = "income_statement_xiii_loi_nhuan_sau_thue"
    BANK_FA_PRETAX = "income_statement_xi_tong_loi_nhuan_truoc_thue"
    BANK_FA_NII = "income_statement_i_thu_nhap_lai_thuan"
    BANK_FA_OP_INCOME = "income_statement_tong_thu_nhap_hoat_dong"
    BANK_FA_OP_EXPENSE = "income_statement_viii_chi_phi_hoat_dong"
    BANK_FA_EQUITY = "balance_sheet_viii_von_chu_so_huu"
    BANK_FA_CHARTER = "balance_sheet_viii_1_a_von_dieu_le"
    BANK_FA_ASSETS = "balance_sheet_tong_tai_san"
    BANK_FA_LOANS = "balance_sheet_vi_cho_vay_khach_hang"
    BANK_FA_DEPOSITS = "balance_sheet_iii_tien_gui_cua_khach_hang"

    # VN listed shares have a fixed par value of ₫10,000, so charter capital / par is a
    # safe backfill for the share count where the scanned column is null (see
    # FUNDAMENTAL_INDICATORS.md §0). Prefer the scanned count; this only fills gaps.
    VN_PAR_VALUE = 10_000.0

    # Indicators the FA table adds, split by what they need:
    #  - *_QUARTERLY: computed on the quarterly grain (no price) and mapped back to every
    #    day of the publish window — they hold flat across the window.
    #  - *_VALUATION: price ÷ fundamental, computed row-wise on the daily panel from
    #    `close_adjust` × the (already as-of) fundamentals.
    BANK_FA_QUARTERLY_COLS = [
        "shares_used",
        "ttm_net_income",
        "ttm_op_income",
        "eps_ttm",
        "bvps",
        "roe",
        "roa",
        "nim",
        "net_profit_margin",
        "pretax_margin",
        "effective_tax_rate",
        "cost_to_income",
        "equity_multiplier",
        "equity_to_assets",
        "ldr",
        "loans_to_assets",
        "deposits_to_assets",
        "earnings_growth_yoy",
        "opincome_growth_yoy",
        "equity_growth_yoy",
        "asset_growth_yoy",
    ]
    BANK_FA_VALUATION_COLS = [
        "market_cap",
        "pe_ttm",
        "pb",
        "ps_ttm",
        "earnings_yield",
    ]

    def _helper_build_bank_fundamental_indicators(
        self, q: pd.DataFrame
    ) -> pd.DataFrame:
        """Compute the price-independent bank fundamental indicators on the **quarterly**
        grain and return them keyed by `(exchange, ticker, year, quarter)`.

        Input `q` is one row per `(exchange, ticker, year, quarter)` carrying the bank
        line items (report-prefixed) + `shares_*` — i.e. the distinct quarters of
        `stocks_basic_financials_bank`. TTM flows are trailing-4-quarter sums (NaN unless
        all 4 quarters are present, so a gap makes the window NULL rather than wrong);
        ROE/ROA/NIM average the balance-sheet stock over the flow's span (this quarter
        and the year-ago quarter). Per `(exchange, ticker)`, ordered by `(year, quarter)`.
        Formulas map 1:1 to FUNDAMENTAL_INDICATORS.md §1.

        Shares: prefer the scanned `shares_outstanding`, fall back to the published
        `shares_issued`, then to the par-value estimate `charter_capital / 10_000`."""
        key = ["exchange", "ticker"]
        num_cols = [
            self.BANK_FA_NET_INCOME,
            self.BANK_FA_PRETAX,
            self.BANK_FA_NII,
            self.BANK_FA_OP_INCOME,
            self.BANK_FA_OP_EXPENSE,
            self.BANK_FA_EQUITY,
            self.BANK_FA_CHARTER,
            self.BANK_FA_ASSETS,
            self.BANK_FA_LOANS,
            self.BANK_FA_DEPOSITS,
            "shares_outstanding",
            "shares_issued",
        ]
        q = q.copy()
        for c in num_cols:
            q[c] = pd.to_numeric(q.get(c), errors="coerce")

        q = q.sort_values(key + ["year", "quarter"]).reset_index(drop=True)
        g = q.groupby(key, sort=False)

        # Share count: scanned outstanding → published issued → par-value estimate.
        shares = q["shares_outstanding"]
        shares = shares.where(shares.notna(), q["shares_issued"])
        shares = shares.where(
            shares.notna(), q[self.BANK_FA_CHARTER] / self.VN_PAR_VALUE
        )
        q["shares_used"] = shares

        # Trailing-twelve-month flows (all 4 quarters required).
        def _ttm(col: str) -> pd.Series:
            return g[col].transform(lambda s: s.rolling(4, min_periods=4).sum())

        q["ttm_net_income"] = _ttm(self.BANK_FA_NET_INCOME)
        q["ttm_op_income"] = _ttm(self.BANK_FA_OP_INCOME)
        ttm_nii = _ttm(self.BANK_FA_NII)

        # Period-average balances (this quarter + year-ago quarter) for ratios that put a
        # TTM flow over a balance-sheet stock.
        def _avg_yoy(col: str) -> pd.Series:
            return g[col].transform(lambda s: (s + s.shift(4)) / 2)

        avg_equity = _avg_yoy(self.BANK_FA_EQUITY)
        avg_assets = _avg_yoy(self.BANK_FA_ASSETS)

        # Profitability / returns.
        q["roe"] = q["ttm_net_income"] / avg_equity
        q["roa"] = q["ttm_net_income"] / avg_assets
        q["nim"] = ttm_nii / avg_assets
        q["net_profit_margin"] = q[self.BANK_FA_NET_INCOME] / q[self.BANK_FA_OP_INCOME]
        q["pretax_margin"] = q[self.BANK_FA_PRETAX] / q[self.BANK_FA_OP_INCOME]
        q["effective_tax_rate"] = 1 - (
            q[self.BANK_FA_NET_INCOME] / q[self.BANK_FA_PRETAX]
        )
        # Op-expense is filed negative → negate so CIR is a positive cost fraction.
        q["cost_to_income"] = -q[self.BANK_FA_OP_EXPENSE] / q[self.BANK_FA_OP_INCOME]

        # Balance-sheet structure / bank health.
        q["equity_multiplier"] = q[self.BANK_FA_ASSETS] / q[self.BANK_FA_EQUITY]
        q["equity_to_assets"] = q[self.BANK_FA_EQUITY] / q[self.BANK_FA_ASSETS]
        q["ldr"] = q[self.BANK_FA_LOANS] / q[self.BANK_FA_DEPOSITS]
        q["loans_to_assets"] = q[self.BANK_FA_LOANS] / q[self.BANK_FA_ASSETS]
        q["deposits_to_assets"] = q[self.BANK_FA_DEPOSITS] / q[self.BANK_FA_ASSETS]

        # Per-share bases (feed the valuation ratios after mapping back to days).
        q["eps_ttm"] = q["ttm_net_income"] / q["shares_used"]
        q["bvps"] = q[self.BANK_FA_EQUITY] / q["shares_used"]

        # Growth (YoY, t vs t-4q).
        def _yoy(col: str) -> pd.Series:
            return g[col].transform(lambda s: s / s.shift(4) - 1)

        q["earnings_growth_yoy"] = _yoy(self.BANK_FA_NET_INCOME)
        q["opincome_growth_yoy"] = _yoy(self.BANK_FA_OP_INCOME)
        q["equity_growth_yoy"] = _yoy(self.BANK_FA_EQUITY)
        q["asset_growth_yoy"] = _yoy(self.BANK_FA_ASSETS)

        # ±inf from a zero denominator (early sparse quarters) is not a real ratio → NaN.
        out_cols = key + ["year", "quarter"] + self.BANK_FA_QUARTERLY_COLS
        result = q[out_cols].copy()
        result[self.BANK_FA_QUARTERLY_COLS] = result[
            self.BANK_FA_QUARTERLY_COLS
        ].replace([np.inf, -np.inf], np.nan)
        return result

    def _ingest_silver_stocks_basic_financials_bank_fa(self) -> None:
        """Silver `stocks_basic_financials_bank_fa` — `stocks_basic_financials_bank`
        (the daily price × as-of bank financials panel) with the **full fundamental
        indicator catalog** (FUNDAMENTAL_INDICATORS.md §1) appended: it carries **all
        source columns plus** the profitability / return / leverage / growth ratios, the
        per-share bases (EPS_ttm, BVPS) and the price-dependent valuation ratios (market
        cap, P/E, P/B, P/S, earnings yield).

        The source already has the as-of merge baked in (every price day carries its
        most-recently-published quarter, `publish_date` ≤ `date`), so no re-join is
        needed. The price-independent indicators are computed once on the **quarterly
        grain** — the distinct `(exchange, ticker, year, quarter)` rows of the source,
        where trailing-4-quarter TTM sums and year-ago-balance averages are well defined —
        then mapped back onto every day of the publish window (so they step on
        `publish_date` and hold flat, preserving the source's zero-look-ahead property).
        The valuation ratios are then computed row-wise from `close_adjust` × those
        as-of fundamentals. PK `(exchange, ticker, date)`; the old table is dropped first
        so a schema change re-materialises cleanly past the driver's IF NOT EXISTS.

        Bank-template only (reads `…_bank`). `corp`/`securities`/`insurance` would each
        get an analogous `…_<template>_fa` once their financials are parsed."""
        self._logger.log_info(
            "Ingesting silver stocks_basic_financials_bank_fa "
            "(fundamental indicators on the price × financials panel)..."
        )
        src_table = "stocks_basic_financials_bank"
        out_table = "stocks_basic_financials_bank_fa"
        keys_out = ["exchange", "ticker", "date"]
        qkey = ["exchange", "ticker", "year", "quarter"]

        df = self._helper_select(schema_name=SILVER_SCHEMA, table_name=src_table)
        if df.empty:
            self._logger.log_info(
                f"No silver {src_table} data found; {out_table} not built."
            )
            return

        # Column types of the source, to re-cast its numeric columns (the driver reads
        # `numeric` back as Decimal→object; without this they'd degrade to VARCHAR again).
        src_types = self._helper_column_types(SILVER_SCHEMA, src_table)

        # ── 1. Quarterly grain: one row per (exchange, ticker, year, quarter). Every
        #       financials column is constant within a quarter (it was as-of joined), so
        #       taking the first row per quarter loses nothing. ──
        quarterly = (
            df.sort_values(keys_out)
            .drop_duplicates(subset=qkey, keep="first")
            .reset_index(drop=True)
        )
        ind = self._helper_build_bank_fundamental_indicators(quarterly)

        # ── 2. Map the quarterly indicators back onto every day by (…, year, quarter). ──
        merged = df.merge(ind, on=qkey, how="left")

        # ── 3. Price-dependent valuation ratios, row-wise from the as-of fundamentals. ──
        close = pd.to_numeric(merged["close_adjust"], errors="coerce")
        merged["market_cap"] = close * merged["shares_used"]
        merged["pe_ttm"] = close / merged["eps_ttm"]
        merged["pb"] = close / merged["bvps"]
        merged["ps_ttm"] = merged["market_cap"] / merged["ttm_op_income"]
        merged["earnings_yield"] = merged["eps_ttm"] / close
        merged[self.BANK_FA_VALUATION_COLS] = merged[
            self.BANK_FA_VALUATION_COLS
        ].replace([np.inf, -np.inf], np.nan)

        merged = self._helper_clean(
            merged,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.ORDER_BY(keys_out),
            ],
        )

        # ── 4. Cast: the source's numeric columns re-typed from its live schema, plus
        #       every computed indicator (all clean floats) → Float64 so NaN → SQL NULL. ──
        indicator_cols = self.BANK_FA_QUARTERLY_COLS + self.BANK_FA_VALUATION_COLS
        skip = set(keys_out) | {"publish_date"}
        src_decimal = [
            c
            for c in df.columns
            if c not in skip and src_types.get(c) == "numeric"
        ]
        src_bigint = [
            c
            for c in df.columns
            if c not in skip and src_types.get(c) == "bigint"
        ]
        merged = self._helper_cast_columns(
            merged,
            decimal_cols=src_decimal + indicator_cols,
            bigint_cols=src_bigint,
        )

        self._database_driver.drop_table(SILVER_SCHEMA, out_table)
        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name=out_table,
            primary_keys=keys_out,
            df=merged,
            dtype_overrides={
                "date": DataType.DATE(),
                "publish_date": DataType.DATE(),
            },
        )

    def _ingest_silver_cafef_news_sentiment(self) -> None:
        """Silver `cafef_news_sentiment` — Vietnamese sentiment scored over
        `bronze.cafef_news`, one row per news `row_id`.

        The actual scoring is the `src/sentiment` module (a PhoBERT-based VN
        sentiment model), imported here so `data_preprocessor` owns only the ETL:
        read bronze → `score_news_frame` → save. The text handed to the model is the
        headline (always) plus, for `editorial` rows, a lead slice of the body
        (`build_scored_text`) — disclosures are short filing stubs whose headline is
        the whole story. ⚠️ The news text is **Vietnamese**; an English sentiment
        model would be wrong (see `sentiment/CONTEXT.md`).

        Output carries the bronze event keys and provenance — `row_id` (md5 PK,
        inherited so a re-score UPDATEs in place), `exchange`/`ticker`, `timestamp`,
        `type`/`category` — plus `sentiment_label`
        (negative/neutral/positive), a signed `sentiment_score` in [-1, 1]
        (`p(pos) − p(neg)`), the three class probabilities, and `model_version`.
        This is the EVENT grain (like the bronze table); a later step can aggregate
        it to a daily per-ticker signal that as-of-joins onto `stocks_basic`. PK
        `row_id`; the old table is dropped first so a schema change re-materialises
        past the driver's IF NOT EXISTS."""
        # Lazy import so a switch-gated-off run never pulls torch/transformers.
        from sentiment.sentiment_functions import score_news_frame

        self._logger.log_info(
            "Ingesting silver cafef_news_sentiment (Vietnamese sentiment scoring)..."
        )
        out_table = "cafef_news_sentiment"
        pk = ["row_id"]
        meta_cols = [
            "row_id",
            "exchange",
            "ticker",
            "timestamp",
            "type",
            "category",
        ]

        news = self._helper_select(schema_name=BRONZE_SCHEMA, table_name="cafef_news")
        if news.empty:
            self._logger.log_info(
                f"No bronze cafef_news data found; {out_table} not built."
            )
            return

        # Score (pure text → sentiment columns, aligned by index), then attach the keys.
        scored = score_news_frame(news)
        keep = [c for c in meta_cols if c in news.columns]
        result = pd.concat([news[keep].reset_index(drop=True), scored.reset_index(drop=True)], axis=1)

        result = self._helper_clean(
            result,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("row_id"),
                CleanLayer.ORDER_BY(["exchange", "ticker", "timestamp"]),
            ],
        )
        result = self._helper_cast_columns(
            result,
            decimal_cols=[
                "sentiment_score",
                "prob_negative",
                "prob_neutral",
                "prob_positive",
            ],
            bigint_cols=[],
        )

        self._database_driver.drop_table(SILVER_SCHEMA, out_table)
        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name=out_table,
            primary_keys=pk,
            df=result,
            dtype_overrides={
                "timestamp": DataType.TIMESTAMP(),
                "type": DataType.VARCHAR(),
                "category": DataType.VARCHAR(),
                "sentiment_label": DataType.VARCHAR(),
                "model_version": DataType.VARCHAR(),
            },
        )

    def _ingest_silver_stocks_basic(self) -> None:
        """Silver `stocks_basic` — the four daily CafeF bronze tables joined into one
        wide per-stock-day panel, `cafef_price` as the base (spine):

            cafef_price
              LEFT JOIN cafef_order_stats  ON (exchange, ticker, date)
              LEFT JOIN cafef_foreign      ON (exchange, ticker, date)
              LEFT JOIN cafef_prop_trading ON (exchange, ticker, date)

        All four are already keyed `(exchange, ticker, date)` in bronze and share no
        non-key column names, so this is a clean left-merge with no suffixes: every
        row is a `cafef_price` day, with order-stats / foreign / prop-trading columns
        filled where that source has the day and NULL where it does not (their history
        is shorter — foreign_own from 2012, order_stats from 2010, prop_trading from
        2023). Join on the FULL `(exchange, ticker, date)` key, not `(ticker, date)`:
        a ticker can list on more than one exchange, and dropping `exchange` from the
        join would fan the base rows out. PK `(exchange, ticker, date)`.

        Basic clean + cast (matching the per-source CafeF carry-ups) — this is a
        CafeF-faithful merge, NOT the old Simplize-primary canonical spine; no price /
        volume / foreign source fallback. It DOES, however, carry the full per-ticker
        GICS classification tree (the 8 GICS_CLASS_COLS), merged on (exchange, ticker)
        via `_helper_build_gics_classification` (bronze `simplize_industry` × `gics`,
        constant per ticker) and placed right after the keys. The old silver table is
        dropped first so a schema change re-materialises cleanly past the driver's
        IF NOT EXISTS create.
        """
        self._logger.log_info(
            "Ingesting silver stocks data (CafeF price + order_stats + foreign + prop_trading)..."
        )

        KEYS = ["exchange", "ticker", "date"]

        base = self._helper_select(schema_name=BRONZE_SCHEMA, table_name="cafef_price")
        if base.empty:
            self._logger.log_info("No bronze cafef_price data found.")
            return

        df = base
        for table_name in ["cafef_order_stats", "cafef_foreign", "cafef_prop_trading"]:
            right = self._helper_select(
                schema_name=BRONZE_SCHEMA, table_name=table_name
            )
            if right.empty:
                self._logger.log_info(
                    f"No bronze {table_name} data found; skipping its columns."
                )
                continue
            df = df.merge(right, on=KEYS, how="left")

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(KEYS),
            ],
        )

        # Cast the joined columns back to numeric (the driver reads bronze `numeric`
        # columns as Decimal -> pandas object; without this they would land as VARCHAR
        # in silver, as the earlier per-source carry-ups already do). Filter to columns
        # actually present so a missing optional source (skipped above) can't KeyError.
        decimal_cols = [
            # cafef_price
            "open",
            "high",
            "low",
            "close_raw",
            "close_adjust",
            "value_matched",
            "value_negotiated",
            # cafef_order_stats
            "avg_vol_per_buy_order",
            "avg_vol_per_sell_order",
            # cafef_foreign
            "foreign_buy_value",
            "foreign_sell_value",
            "foreign_net_value",
            "foreign_own",
            # cafef_prop_trading
            "prop_buy_val",
            "prop_sell_val",
        ]
        bigint_cols = [
            # cafef_price
            "volume_matched",
            "volume_negotiated",
            # cafef_order_stats
            "n_buy_orders",
            "buy_order_vol",
            "n_sell_orders",
            "sell_order_vol",
            # cafef_foreign
            "foreign_buy_volume",
            "foreign_sell_volume",
            "foreign_net_volume",
            "foreign_room_left",
            # cafef_prop_trading
            "prop_buy_vol",
            "prop_sell_vol",
        ]
        df = self._helper_cast_columns(
            df,
            decimal_cols=[c for c in decimal_cols if c in df.columns],
            bigint_cols=[c for c in bigint_cols if c in df.columns],
        )

        # Attach the full GICS classification tree, merged per ticker (constant per
        # (exchange, ticker)) from bronze `simplize_industry` × `gics`. Placed right
        # after the keys; left-join so a ticker with no GICS crosswalk keeps its rows
        # (class columns NULL). Done after the cast so the categorical string columns
        # are never coerced to numeric.
        cls = self._helper_build_gics_classification()
        if not cls.empty:
            df = df.merge(cls, on=["exchange", "ticker"], how="left")
        else:
            for c in self.GICS_CLASS_COLS:
                df[c] = pd.NA

        lead = KEYS + self.GICS_CLASS_COLS
        df = df[lead + [c for c in df.columns if c not in lead]]

        # Drop the old silver table first so a changed schema re-materialises cleanly
        # rather than colliding with the driver's IF NOT EXISTS create.
        self._database_driver.drop_table(SILVER_SCHEMA, "stocks_basic")

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="stocks_basic",
            primary_keys=KEYS,
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    # Full GICS hierarchy columns carried on every silver.stocks_basic row (English
    # snake_case names + codes, sourced entirely from bronze.gics).
    GICS_CLASS_COLS = [
        "sector",
        "sector_code",
        "industry_group",
        "industry_group_code",
        "industry",
        "industry_code",
        "sub_industry",
        "sub_industry_code",
    ]

    def _helper_build_gics_classification(self) -> pd.DataFrame:
        """Build the per-ticker FULL GICS classification tree (merged into
        silver.stocks_basic), sourced entirely from the official GICS taxonomy.

        • Simplize (bronze `simplize_industry`) — per-ticker industry group
          (accurate for VN, e.g. VHM=real estate, VCB=bank).
        • GICS (bronze `gics`) — the official taxonomy.

        Each ticker's Simplize industry group is crosswalked to a GICS sub-industry
        (leaf) via SIMPLIZE_GROUP_TO_GICS_SUB_INDUSTRY; joining bronze.gics on that
        leaf yields all four levels — sector / industry_group / industry /
        sub_industry — as English snake_case names + codes. `sector` is the GICS
        sector. Returns one row per (exchange, ticker); empty if bronze is missing.
        """
        out_cols = ["exchange", "ticker"] + self.GICS_CLASS_COLS

        ind = self._helper_select(
            schema_name=BRONZE_SCHEMA, table_name="simplize_industry"
        )
        gics = self._helper_select(schema_name=BRONZE_SCHEMA, table_name="gics")
        if ind.empty or gics.empty:
            self._logger.log_warning(
                "GICS classification: missing bronze simplize_industry / gics; "
                "stocks will have no GICS classification."
            )
            return pd.DataFrame(columns=out_cols)

        # GICS taxonomy keyed by sub-industry leaf -> the full snake_case hierarchy.
        # Select code + *_snake columns only (bronze.gics also has title-case name
        # columns), then rename the snake columns to the canonical output names.
        gics = gics.copy()
        gics["sub_industry_code"] = (
            gics["sub_industry_code"].astype("string").str.strip()
        )
        snake_map = {
            "sub_industry_code": "sub_industry_code",
            "sector_code": "sector_code",
            "sector_snake": "sector",
            "industry_group_code": "industry_group_code",
            "industry_group_snake": "industry_group",
            "industry_code": "industry_code",
            "industry_snake": "industry",
            "sub_industry_snake": "sub_industry",
        }
        gics_tree = gics.drop_duplicates("sub_industry_code")[list(snake_map)].rename(
            columns=snake_map
        )

        ind = ind.copy()
        ind["industry_group_code"] = (
            ind["industry_group_code"].astype("string").str.strip()
        )
        ind["sub_industry_code"] = ind["industry_group_code"].map(
            SIMPLIZE_GROUP_TO_GICS_SUB_INDUSTRY
        )

        unmapped = ind[ind["sub_industry_code"].isna()]
        if not unmapped.empty:
            self._logger.log_warning(
                f"GICS classification: {len(unmapped)} tickers unmapped; "
                f"unknown Simplize industry groups: "
                f"{sorted(unmapped['industry_group_code'].dropna().unique())}"
            )

        # Join only on the leaf code; GICS supplies all hierarchy columns (avoids
        # colliding with Simplize's own industry_group_code on `ind`).
        merged = ind[["exchange", "ticker", "sub_industry_code"]].merge(
            gics_tree, on="sub_industry_code", how="left"
        )
        return (
            merged[out_cols]
            .drop_duplicates(subset=["exchange", "ticker"])
            .reset_index(drop=True)
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
        _TA_FUNC_MAP = _build_transform_func_map()

        ta_layers = [l for l in transform_layer_list if l.action in _TA_FUNC_MAP]

        df = df.copy()

        # Apply TA transforms per (exchange, ticker) group, sorted by date.
        # (Compute is only ~12% of ingest wall-time — the DB insert dominates —
        # so this stays a simple sequential loop; parallelizing it is not worth
        # the complexity. See _helper_save_pandas_table_to_database use_copy.)
        if ta_layers:

            def _process_group(group: pd.DataFrame) -> pd.DataFrame:
                group = group.sort_values("date").reset_index(drop=True)
                for layer in ta_layers:
                    func = _TA_FUNC_MAP.get(layer.action)
                    if func:
                        group = func(group, **layer.params)
                return group

            if checkpoint_fn:
                buffer: list[pd.DataFrame] = []
                buffer_rows = 0
                grouped = list(df.groupby(["exchange", "ticker"], sort=False))
                total = len(grouped)
                for i, ((_, _), group) in enumerate(grouped):
                    buffer.append(_process_group(group))
                    buffer_rows += len(buffer[-1])
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
                groups = [
                    _process_group(group)
                    for _, group in df.groupby(["exchange", "ticker"], sort=False)
                ]
                df = pd.concat(groups, ignore_index=True)

        return df

    def _helper_build_feature_layers(self, df: pd.DataFrame) -> List[TransformLayer]:
        """
        Build the standard gold feature-engineering layers based on the table's
        price representation:

        • OHLC tables (open/high/low/close) → returns, intraday range,
          return volatility and rolling statistics on `close`.
        • Single-value tables (`value`)     → returns, return volatility and
          rolling statistics on `value` (intraday range is not applicable).
        """
        cols = set(df.columns)
        if {"open", "high", "low", "close"}.issubset(cols):
            price_col = "close"
            return [
                TransformLayer.ADD_RETURNS(column_name=price_col),
                TransformLayer.ADD_INTRADAY_RANGE(),
                TransformLayer.ADD_RETURN_VOLATILITY(column_name=price_col),
                TransformLayer.ADD_ROLLING_STATISTICS(column_name=price_col),
            ]
        if "value" in cols:
            price_col = "value"
            return [
                TransformLayer.ADD_RETURNS(column_name=price_col),
                TransformLayer.ADD_RETURN_VOLATILITY(column_name=price_col),
                TransformLayer.ADD_ROLLING_STATISTICS(column_name=price_col),
            ]
        self._logger.log_error(
            f"No recognizable price columns (OHLC or 'value') found in "
            f"columns: {sorted(cols)}"
        )
        return []

    def _ingest_gold_table(
        self,
        table_name: str,
        ta_layers: Optional[List[TransformLayer]] = None,
        silver_table_name: Optional[str] = None,
    ) -> None:
        """
        Generic gold ingest: read a silver table, coerce numeric source columns,
        categorize as OHLC vs single-value, apply the standard feature-engineering
        layers (plus any table-specific TA layers), and checkpoint-save to gold.

        `silver_table_name` is the source table to READ from silver; it defaults to
        `table_name` (the gold table WRITTEN). They differ only where the silver and
        gold table names diverge — e.g. gold `stocks` is built from silver
        `stocks_basic`.
        """
        source_table = silver_table_name or table_name
        self._logger.log_info(
            f"Ingesting gold {table_name} data (from silver {source_table})..."
        )

        df = self._helper_select(
            schema_name=SILVER_SCHEMA,
            table_name=source_table,
            order_by=["exchange", "ticker", "date"],
        )

        if df.empty:
            self._logger.log_info(f"No silver {source_table} data found.")
            return

        # psycopg2 returns DECIMAL as Python Decimal — coerce numeric source columns
        # to float before feature engineering (covers OHLCV/value and the CafeF
        # fields carried through from silver: foreign flow, volume breakdown, etc.).
        # The GICS classification columns are categorical strings and are passed
        # through untouched (coercing them would wipe them to NaN).
        _non_numeric = {"exchange", "ticker", "date", *self.GICS_CLASS_COLS}
        for col in df.columns:
            if col not in _non_numeric:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        transform_layers = list(ta_layers or []) + self._helper_build_feature_layers(df)
        if not transform_layers:
            self._logger.log_error(
                f"No transform layers resolved for gold {table_name}; skipping."
            )
            return

        def _checkpoint(chunk: pd.DataFrame) -> None:
            # Use REAL (4-byte float) for all float columns to stay within
            # PostgreSQL's 8160-byte row size limit given the large number of columns.
            overrides: dict[str, str] = {"date": DataType.DATE()}
            for col in chunk.columns:
                if str(chunk[col].dtype).lower().startswith("float"):
                    overrides[col] = "REAL"
            self._helper_save_pandas_table_to_database(
                schema_name=GOLD_SCHEMA,
                table_name=table_name,
                primary_keys=["exchange", "ticker", "date"],
                df=chunk,
                dtype_overrides=overrides,
                use_copy=True,
            )

        self._helper_transform(
            df,
            transform_layers,
            checkpoint_fn=_checkpoint,
            checkpoint_size=100_000,
        )

    def _ingest_gold_stocks(self) -> None:
        # Reads silver `stocks_basic` (the CafeF panel + GICS tree), writes gold `stocks`.
        self._ingest_gold_table(
            "stocks",
            silver_table_name="stocks_basic",
            ta_layers=[
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
                # Stock microstructure (foreign flow / volume breakdown)
                TransformLayer.ADD_FOREIGN_BUY_PRESSURE(),
                TransformLayer.ADD_FOREIGN_NET_VAL_RATIO(),
                TransformLayer.ADD_NEGOTIATED_VOL_RATIO(),
            ],
        )

    def _ingest_gold_bonds(self) -> None:
        self._ingest_gold_table("bonds")

    def _ingest_gold_economy(self) -> None:
        self._ingest_gold_table("economy")

    def _ingest_gold_forex(self) -> None:
        self._ingest_gold_table("forex")

    def _ingest_gold_funds(self) -> None:
        self._ingest_gold_table("funds")

    def _ingest_gold_indices(self) -> None:
        self._ingest_gold_table("indices")

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

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_bronze", "bonds"
                ):
                    self._ingest_bronze_bonds()

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_bronze", "economy"
                ):
                    self._ingest_bronze_economy()

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_bronze", "forex"
                ):
                    self._ingest_bronze_forex()

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_bronze", "funds"
                ):
                    self._ingest_bronze_funds()

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_bronze", "indices"
                ):
                    self._ingest_bronze_indices()

                # ONE LEAF PER SOURCE TABLE, not one `stocks` leaf for all ten. Bronze is
                # raw-faithful and each of these reads a different raw_data folder, so they
                # share nothing but the schema — yet lumped together the cheap ones could not
                # be run without the expensive ones (re-ingesting the financials CSVs, ~2 s,
                # meant also re-reading 2.4 M price rows + 2.7 M Simplize rows). The leaves
                # are independent: bronze has no cross-table dependency, so any subset is a
                # valid run. Order is only convention (universe, then daily, then event, then
                # reference).
                bronze_ingests = [
                    ("trading_view_stocks", self._ingest_bronze_stocks_trading_view),
                    ("cafef_price", self._ingest_bronze_cafef_price),
                    ("cafef_foreign", self._ingest_bronze_cafef_foreign),
                    ("cafef_order_stats", self._ingest_bronze_cafef_order_stats),
                    ("cafef_prop_trading", self._ingest_bronze_cafef_prop_trading),
                    (
                        "cafef_insider_txn",
                        self._ingest_bronze_cafef_insider_shareholder_transactions,
                    ),
                    ("cafef_news", self._ingest_bronze_cafef_news),
                    ("cafef_financials", self._ingest_bronze_cafef_financials),
                    ("simplize_stocks", self._ingest_bronze_stocks_simplize),
                    ("simplize_industry", self._ingest_bronze_simplize_industry),
                    ("gics", self._ingest_bronze_gics),
                ]
                for leaf, ingest in bronze_ingests:
                    if self._switch_handler.is_enabled(
                        "data_preprocessor", "data_quality_bronze", leaf
                    ):
                        ingest()

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

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "bonds"
                ):
                    self._ingest_silver_bonds()

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "economy"
                ):
                    self._ingest_silver_economy()

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "forex"
                ):
                    self._ingest_silver_forex()

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "funds"
                ):
                    self._ingest_silver_funds()

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "indices"
                ):
                    self._ingest_silver_indices()

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "gics"
                ):
                    self._ingest_silver_gics()

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "financials"
                ):
                    self._ingest_silver_cafef_financials()
                    self._ingest_silver_cafef_financials_bank()

                # The one-to-one source carry-ups. Split off `stocks_basic`'s leaf because
                # they are not its inputs — `stocks_basic` joins the BRONZE tables directly,
                # so neither needs the other and rebuilding the 2.4 M-row panel to refresh a
                # carry-up (or vice versa) was pure cost.
                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "cafef_carry_ups"
                ):
                    self._ingest_silver_cafef_price()
                    self._ingest_silver_cafef_order_stats()
                    self._ingest_silver_cafef_foreign()
                    self._ingest_silver_cafef_prop_trading()
                    self._ingest_silver_cafef_insider_shareholder_transactions()

                # News sentiment reads bronze.cafef_news only (independent of the
                # stocks/financials tables), so it gets its own leaf.
                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "news_sentiment"
                ):
                    self._ingest_silver_cafef_news_sentiment()

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "stocks_basic"
                ):
                    self._ingest_silver_stocks_basic()

                # Depends on BOTH silver.stocks_basic and silver.cafef_financials_bank,
                # so it runs after both are (re)built. The _fa step then reads the
                # plain-join table just built and appends the fundamental indicators.
                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "stocks_financials"
                ):
                    self._ingest_silver_stocks_basic_financials_bank()
                    self._ingest_silver_stocks_basic_financials_bank_fa()

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

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_gold", "bonds"
                ):
                    self._ingest_gold_bonds()
                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_gold", "economy"
                ):
                    self._ingest_gold_economy()
                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_gold", "forex"
                ):
                    self._ingest_gold_forex()
                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_gold", "funds"
                ):
                    self._ingest_gold_funds()
                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_gold", "indices"
                ):
                    self._ingest_gold_indices()
                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_gold", "stocks"
                ):
                    self._ingest_gold_stocks()

            except Exception as e:
                self._logger.log_error(
                    f"Error preprocessing `{DataQuality.GOLD.value}` data: {e}"
                )

            finally:
                self._database_driver.disconnect()
