from dotenv import load_dotenv
import os
import hashlib
import pandas as pd
import re
from glob import glob
import numpy as np
from datetime import date, datetime, timezone
from psycopg2.extras import execute_values
from typing import List, Optional, Dict, Any, Sequence, Tuple
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
    UNIFIED_SCHEMA,
)
from utils.enums import *
from utils.exceptions import MissingSourceDataError, PipelineError
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

    def _helper_load_csvs(self, folder_path: str) -> tuple[pd.DataFrame, list]:
        """⚠️ RAISES `MissingSourceDataError` — it used to return None.

        Every caller answered that None with `log_error(...); return`, i.e. the ingest
        finished normally having written nothing. The return type is now unconditional:
        either a frame, or an exception.
        """
        file_paths = get_all_file_names_with_extensions(
            logger=self._logger,
            folder_path=folder_path,
            extensions=[FileExtension.CSV],
        )

        if not file_paths:
            raise MissingSourceDataError(f'Data in "{folder_path}" does not exist.')

        dataframes = []

        for fp in file_paths:
            df = pd.read_csv(fp, encoding="utf-8")

            # Skip completely empty or all-NA DataFrames
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            raise MissingSourceDataError(
                f'No valid CSV data found in "{folder_path}".'
            )

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
            raise MissingSourceDataError(
                f'No economy CSV files found in "{economy_dir}".'
            )

        dataframes = []
        for fp in csv_files:
            df = pd.read_csv(fp, encoding="utf-8")
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            raise MissingSourceDataError("No valid economy CSV data found.")

        df = pd.concat(dataframes, ignore_index=True).drop_duplicates()

        # ⚠️ SPLIT ON READ. `symbol` ("ECONOMICS:VNCPI") is the RAW CSV's key; the bronze
        # convention is (exchange, ticker), so it is split here and every clean, order
        # and dedupe below keys on the real stored columns. It used to be split LAST,
        # which left `symbol` as the working key through the whole method — and is why
        # five silver ingests still reach for a column no bronze table has ever held.
        df = self._helper_split_symbol_column(df)

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("value"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["exchange", "ticker", "date"]),
            ],
        )

        df = self._helper_cast_columns(df, decimal_cols=["value"], bigint_cols=[])

        df["date"] = pd.to_datetime(df["date"]).dt.date

        df = self._helper_remove_duplicates(
            df, primary_keys=["exchange", "ticker", "date"]
        )

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
            raise MissingSourceDataError(
                f'No forex CSV files found in "{forex_dir}".'
            )

        dataframes = []
        for fp in csv_files:
            df = pd.read_csv(fp, encoding="utf-8")
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            raise MissingSourceDataError("No valid forex CSV data found.")

        df = pd.concat(dataframes, ignore_index=True).drop_duplicates()

        # ⚠️ SPLIT ON READ. `symbol` ("ECONOMICS:VNCPI") is the RAW CSV's key; the bronze
        # convention is (exchange, ticker), so it is split here and every clean, order
        # and dedupe below keys on the real stored columns. It used to be split LAST,
        # which left `symbol` as the working key through the whole method — and is why
        # five silver ingests still reach for a column no bronze table has ever held.
        df = self._helper_split_symbol_column(df)

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("value"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["exchange", "ticker", "date"]),
            ],
        )

        df = self._helper_cast_columns(df, decimal_cols=["value"], bigint_cols=[])

        df["date"] = pd.to_datetime(df["date"]).dt.date

        df = self._helper_remove_duplicates(
            df, primary_keys=["exchange", "ticker", "date"]
        )

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
            raise MissingSourceDataError(
                f'No funds CSV files found in "{funds_dir}".'
            )

        dataframes = []
        for fp in csv_files:
            df = pd.read_csv(fp, encoding="utf-8")
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            raise MissingSourceDataError("No valid funds CSV data found.")

        df = pd.concat(dataframes, ignore_index=True).drop_duplicates()

        # ⚠️ SPLIT ON READ. `symbol` ("ECONOMICS:VNCPI") is the RAW CSV's key; the bronze
        # convention is (exchange, ticker), so it is split here and every clean, order
        # and dedupe below keys on the real stored columns. It used to be split LAST,
        # which left `symbol` as the working key through the whole method — and is why
        # five silver ingests still reach for a column no bronze table has ever held.
        df = self._helper_split_symbol_column(df)

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
            decimal_cols=["open", "high", "low", "close"],
            bigint_cols=["volume"],
        )

        df["date"] = pd.to_datetime(df["date"]).dt.date

        df = self._helper_remove_duplicates(
            df, primary_keys=["exchange", "ticker", "date"]
        )

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
            raise MissingSourceDataError(
                f'No indices CSV files found in "{indices_dir}".'
            )

        dataframes = []
        for fp in csv_files:
            df = pd.read_csv(fp, encoding="utf-8")
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            raise MissingSourceDataError("No valid indices CSV data found.")

        df = pd.concat(dataframes, ignore_index=True).drop_duplicates()

        # ⚠️ SPLIT ON READ. `symbol` ("ECONOMICS:VNCPI") is the RAW CSV's key; the bronze
        # convention is (exchange, ticker), so it is split here and every clean, order
        # and dedupe below keys on the real stored columns. It used to be split LAST,
        # which left `symbol` as the working key through the whole method — and is why
        # five silver ingests still reach for a column no bronze table has ever held.
        df = self._helper_split_symbol_column(df)

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
            decimal_cols=["open", "high", "low", "close"],
            bigint_cols=["volume"],
        )

        df["date"] = pd.to_datetime(df["date"]).dt.date

        df = self._helper_remove_duplicates(
            df, primary_keys=["exchange", "ticker", "date"]
        )

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
            raise MissingSourceDataError(
                f'No stocks CSV files found in "{stocks_dir}".'
            )

        dataframes = []
        for fp in csv_files:
            df = pd.read_csv(fp, encoding="utf-8")
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            raise MissingSourceDataError("No valid stocks CSV data found.")

        df = pd.concat(dataframes, ignore_index=True).drop_duplicates()

        # ⚠️ SPLIT ON READ. `symbol` ("ECONOMICS:VNCPI") is the RAW CSV's key; the bronze
        # convention is (exchange, ticker), so it is split here and every clean, order
        # and dedupe below keys on the real stored columns. It used to be split LAST,
        # which left `symbol` as the working key through the whole method — and is why
        # five silver ingests still reach for a column no bronze table has ever held.
        df = self._helper_split_symbol_column(df)

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
            decimal_cols=["open", "high", "low", "close"],
            bigint_cols=["volume"],
        )

        df["date"] = pd.to_datetime(df["date"]).dt.date

        df = self._helper_remove_duplicates(
            df, primary_keys=["exchange", "ticker", "date"]
        )

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

    def _helper_load_cafef_folder(self, folder: str) -> pd.DataFrame:
        """Concat every CSV under raw_data/cafef/<folder>/ (one per ticker),
        dropping empty / all-NA frames.

        ⚠️ RAISES `MissingSourceDataError` when nothing valid is found — it used to
        return None, which every caller turned into `log_error(...); return`, i.e. an
        ingest that wrote no table and reported success anyway."""
        files = glob(
            os.path.join(CAFEF_RAW_DATA_DIR, folder, "**", "*.csv"), recursive=True
        )
        frames = [
            df
            for fp in files
            if not (df := pd.read_csv(fp, encoding="utf-8")).empty
            and not df.dropna(how="all").empty
        ]
        if not frames:
            raise MissingSourceDataError(
                f'No valid CafeF "{folder}" CSV data found '
                f"({len(files)} file(s) under {CAFEF_RAW_DATA_DIR}/{folder})."
            )
        return pd.concat(frames, ignore_index=True).drop_duplicates()

    def _helper_split_symbol_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Split the RAW CSV's `"<EXCHANGE>:<TICKER>"` colon key into separate
        `exchange` and `ticker` columns (dropping `symbol`).

        ⚠️ **THE ONLY PLACE `symbol` MAY APPEAR, and it is called ON READ.** `symbol` is
        TradingView's CSV column, never a bronze column: every bronze table stores
        `(exchange, ticker)`. Calling this at the END of an ingest — as all six
        TradingView ingests did until 2026-08-01 — makes `symbol` look like the layer's
        key, which is how five silver ingests came to split a column that no bronze
        table has ever stored (`KeyError('symbol')`, all five).

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

    def _ingest_bronze_cafef_daily(
        self,
        folder: str,
        table_name: str,
        decimal_cols: List[str],
        bigint_cols: List[str],
        required_col: Optional[str] = None,
    ) -> None:
        """Ingest one DAILY-series CafeF folder as its own bronze table
        (raw-faithful). Drives price / foreign / order_stats / prop_trading and the
        four index tables. PK `(exchange, ticker, date)`.

        The CSV already stores the key apart, so `symbol` is only ever the CSV's name
        for the ticker and is renamed on read.

        > Until 2026-08-01 a `split_key` flag also offered the opposite convention:
        > fold `(exchange, symbol)` into `symbol = "<EXCHANGE>:<TICKER>"`, then split it
        > back apart before saving. **All 8 callers passed `split_key=True`**, so that
        > round-trip was dead code that nonetheless kept `symbol` alive in the layer's
        > vocabulary — and cost the silver ingests a column that never reaches disk."""
        self._logger.log_info(f"Ingesting bronze CafeF {folder} data...")

        df = self._helper_load_cafef_folder(folder)

        df = df.rename(columns={"symbol": "ticker"})
        key_cols = ["exchange", "ticker"]

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
        )

    def _ingest_bronze_cafef_prop_trading(self) -> None:
        """CafeF proprietary-desk trades — brokers' own-account buy/sell volume and
        value. Key kept SPLIT; PK (exchange, ticker, date)."""
        self._ingest_bronze_cafef_daily(
            folder="prop_trading",
            table_name="cafef_prop_trading",
            decimal_cols=["prop_buy_val", "prop_sell_val"],
            bigint_cols=["prop_buy_vol", "prop_sell_vol"],
        )

    # ── CafeF MARKET INDICES — the same four daily tabs, for the six indices ──────
    # `CafeFIndexScraper` writes `index_<tab>/` folders that are COLUMN-IDENTICAL to the
    # per-stock ones (it subclasses the stock scraper and reuses its column constants),
    # so these reuse `_ingest_bronze_cafef_daily` unchanged — same cast lists, same
    # (exchange, ticker, date) PK. Only the folder and the table name differ.
    #
    # ⚠️ THEY GET THEIR OWN TABLES AND MUST NEVER BE UNIONED INTO THE STOCK ONES.
    # `ticker` here holds an INDEX CODE (`VNINDEX`, `VN30INDEX`, `VN100-INDEX`,
    # `HNX-INDEX`, `HNX30-INDEX`, `UPCOM-INDEX`), not a company. Appended to
    # `cafef_price` the six would surface as phantom stocks in `silver.stocks_basic`,
    # pick up NULL GICS classes, and flow into every downstream cross-sectional model as
    # if they were tradeable names. An index is a different GRAIN, not another ticker.
    #
    # ⚠️ The values are ALREADY correctly scaled — the scraper neutralises its `_mul`
    # because an index level is a point, not '000 VND (VNINDEX's first row is the base,
    # 100.0, on HOSE's opening day). Nothing here re-scales; don't add it.
    #
    # ⚠️ Three of the four series carry holes that are CAFEF'S, not ingest failures, and
    # bronze is faithful to them (see web_scraper/CONTEXT.md §3, *CafeF indices*):
    # VN100-INDEX's price stops at 2025-04-29; `order_stats` is literally zero-filled for
    # VN30INDEX/VN100-INDEX and leaves `sell_order_vol` 0 on the HNX/UPCOM indices;
    # `prop_trading` is effectively exchange-level (VN100-INDEX has ONE row). A consumer
    # that reads those zeros as data gets a breadth signal made of CafeF's padding.

    def _ingest_bronze_cafef_index_price(self) -> None:
        """Index price tab — OHLC + both closes as index POINTS, plus matched/negotiated
        volume and value. On HNX/UPCOM `close_raw` carries full precision where
        `close_adjust` is rounded to 2dp, so the two are not redundant there.
        PK (exchange, ticker, date)."""
        self._ingest_bronze_cafef_daily(
            folder="index_price",
            table_name="cafef_index_price",
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
        )

    def _ingest_bronze_cafef_index_foreign(self) -> None:
        """Index foreign-trading tab. `foreign_room_left` / `foreign_own` are always 0 —
        an index has no ownership limit; the columns exist for layout parity with
        `cafef_foreign`. PK (exchange, ticker, date)."""
        self._ingest_bronze_cafef_daily(
            folder="index_foreign",
            table_name="cafef_index_foreign",
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
        )

    def _ingest_bronze_cafef_index_order_stats(self) -> None:
        """Index order-placement stats — the whole market's order book aggregated.
        PK (exchange, ticker, date)."""
        self._ingest_bronze_cafef_daily(
            folder="index_order_stats",
            table_name="cafef_index_order_stats",
            decimal_cols=["avg_vol_per_buy_order", "avg_vol_per_sell_order"],
            bigint_cols=[
                "n_buy_orders",
                "buy_order_vol",
                "n_sell_orders",
                "sell_order_vol",
            ],
        )

    def _ingest_bronze_cafef_index_prop_trading(self) -> None:
        """Index proprietary-desk trades. Effectively an EXCHANGE-level series — the
        three sub-indices hold almost nothing. PK (exchange, ticker, date)."""
        self._ingest_bronze_cafef_daily(
            folder="index_prop_trading",
            table_name="cafef_index_prop_trading",
            decimal_cols=["prop_buy_val", "prop_sell_val"],
            bigint_cols=["prop_buy_vol", "prop_sell_vol"],
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
            raise MissingSourceDataError('No valid CafeF "news" CSV data found.')

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
            raise MissingSourceDataError(
                f'No CafeF financials "templates.csv" at {file_path}.'
            )

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
                # Not skippable: an unrecognised template means CAFEF_FINANCIAL_TEMPLATES
                # is out of date, and the chart of accounts would be silently missing a
                # whole template's line items.
                raise PipelineError(
                    f"Unknown financial schema template: {stem} "
                    f"(known: {sorted(self.CAFEF_FINANCIAL_TEMPLATES)})"
                )
            df["report"] = stem[len(template) + 1 :]
            frames.append(df)

        if not frames:
            raise MissingSourceDataError("No CafeF financial schema CSVs found.")

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
                    # `files` was non-empty to get here, so the CSVs exist and are all
                    # unreadable/empty — a real failure, not an unparsed template (that
                    # case `continue`s a few lines above, on `if not files`).
                    raise MissingSourceDataError(
                        f"No valid CafeF financial statement CSVs in {folder} "
                        f"({len(files)} file(s) present but all empty)."
                    )

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
            raise MissingSourceDataError("No CafeF financial statement CSVs found.")

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
            raise MissingSourceDataError(
                f'No Simplize stocks CSV files found in "{stocks_dir}".'
            )

        dataframes = []
        for fp in csv_files:
            df = pd.read_csv(fp, encoding="utf-8")
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            raise MissingSourceDataError("No valid Simplize stocks CSV data found.")

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
            raise MissingSourceDataError(
                f'Simplize industry CSV not found at "{path}".'
            )

        df = pd.read_csv(path, encoding="utf-8", dtype=str)
        if df.empty:
            raise MissingSourceDataError("No Simplize industry data found.")

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
            raise MissingSourceDataError(f'GICS structure CSV not found at "{path}".')

        df = pd.read_csv(path, encoding="utf-8", dtype=str)
        if df.empty:
            raise MissingSourceDataError("No GICS structure data found.")

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
            raise MissingSourceDataError(
                f'No bonds CSV files found in "{bonds_dir}".'
            )

        dataframes = []
        for fp in csv_files:
            df = pd.read_csv(fp, encoding="utf-8")
            if not df.empty and not df.dropna(how="all").empty:
                dataframes.append(df)

        if not dataframes:
            raise MissingSourceDataError("No valid bonds CSV data found.")

        df = pd.concat(dataframes, ignore_index=True).drop_duplicates()

        # ⚠️ SPLIT ON READ. `symbol` ("ECONOMICS:VNCPI") is the RAW CSV's key; the bronze
        # convention is (exchange, ticker), so it is split here and every clean, order
        # and dedupe below keys on the real stored columns. It used to be split LAST,
        # which left `symbol` as the working key through the whole method — and is why
        # five silver ingests still reach for a column no bronze table has ever held.
        df = self._helper_split_symbol_column(df)

        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("value"),
                CleanLayer.REMOVE_IF_ALL_COLUMNS_ARE_NULL(),
                CleanLayer.ORDER_BY(["exchange", "ticker", "date"]),
            ],
        )

        df = self._helper_cast_columns(df, decimal_cols=["value"], bigint_cols=[])

        df["date"] = pd.to_datetime(df["date"]).dt.date

        df = self._helper_remove_duplicates(
            df, primary_keys=["exchange", "ticker", "date"]
        )

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

        df = df[["exchange", "ticker", "date", "value"]]

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="bonds",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_silver_economy(self) -> None:
        """`bronze.trading_view_economy` -> `silver.economy`, PK
        `(exchange, ticker, date)`. **LONG**: one row per series per date, the same grain
        as its four siblings (`bonds`/`forex`/`funds`/`indices`) and the grain
        `_ingest_gold_economy` reads.

        WARNING - IT WAS BRIEFLY WIDE (2026-08-01) AND THAT WAS THE WRONG LAYER FOR IT.
        One row per date x 1,034 columns measured **5.8% filled**, and the nulls were the
        least of it - three things were wrong:

        * **the schema became a function of the data.** Every new series TradingView
          publishes is a DDL change, every retired one leaves a dead column, and each
          upsert chunk carries a 1,034-term `ON CONFLICT DO UPDATE`. In long form a new
          series is ROWS. (It also sat at 65% of PostgreSQL's 1,600-column ceiling.)
        * **it mixed frequencies on one calendar.** 67 daily series hold 63% of all
          observations and imposed a 9,719-day grid on 500 monthly and 226 quarterly
          series that can never fill it. On their own grids those buckets are 76-93%
          filled - the sparsity was an artefact of the shape, not of the data.
        * **`date` is the REFERENCE period, not the release date.** Vietnam's Q1 GDP is
          dated 2026-03-31 and published in April, so any wide panel joined on `date`
          hands a model a number a week before it existed. That needs a publication lag,
          which is a MODELLING decision - see `_ingest_gold_economy`, where it
          lives.

        Nulls were never the problem: a NULL costs ~1 bit in the row's null bitmap, so
        9.4 M of them was ~1.2 MB. Shape and layer were the problem.

        WARNING - RAISES on empty bronze rather than logging and returning. Its four
        siblings still take the silent path - an empty source there reads as a successful
        ingest, the exact Phase 0 failure mode - and should follow when next touched.
        """
        self._logger.log_info("Ingesting silver economy data...")

        df = self._helper_select(
            schema_name=BRONZE_SCHEMA, table_name="trading_view_economy"
        )

        if df.empty:
            raise MissingSourceDataError(
                f"{BRONZE_SCHEMA}.trading_view_economy is empty - run the bronze "
                f"economy ingest first."
            )

        # `exchange` and `ticker` come STRAIGHT OUT OF BRONZE. Until 2026-08-01 this
        # re-derived them with `df["symbol"].str.split(":")`, against a frame that has
        # no `symbol` column - bronze splits it on read - so this method raised
        # `KeyError('symbol')` every time it ran.
        df = self._helper_clean(
            df,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("exchange"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("ticker"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("date"),
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("value"),
                CleanLayer.ORDER_BY(["exchange", "ticker", "date"]),
            ],
        )

        df = self._helper_cast_columns(df, decimal_cols=["value"], bigint_cols=[])
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = self._helper_remove_duplicates(
            df, primary_keys=["exchange", "ticker", "date"]
        )

        # ONLY the canonical four columns, matching the siblings - the dimensions
        # (`country`/`category`/`scrape_main_type`) live in `silver.economy_series`, a
        # proper dimension table. Carrying them here would break `_ingest_gold_table`,
        # which coerces every column outside {exchange, ticker, date, GICS} with
        # `pd.to_numeric` and would wipe all three to NaN.
        df = df[["exchange", "ticker", "date", "value"]]

        # The grain changed back from `date` (the brief wide version) and `create_table`
        # is IF NOT EXISTS, so the stale shape has to go before the upsert.
        self._database_driver.drop_table(SILVER_SCHEMA, "economy")

        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="economy",
            primary_keys=["exchange", "ticker", "date"],
            df=df,
            dtype_overrides={"date": DataType.DATE()},
        )

    # Median days between consecutive observations -> frequency class. The boundaries are
    # generous because real macro calendars are ragged: a "monthly" series that skips a
    # month still has a median gap near 30. Measured over the live table: 67 daily,
    # 32 weekly, 500 monthly, 226 quarterly, 206 annual, 4 single-observation.
    ECONOMY_FREQUENCY_BOUNDS = (
        (4, "daily"),
        (10, "weekly"),
        (45, "monthly"),
        (135, "quarterly"),
        (400, "annual"),
    )

    def _classify_economy_frequency(self, median_gap_days: float) -> str:
        if pd.isna(median_gap_days):
            return "single"
        for bound, label in self.ECONOMY_FREQUENCY_BOUNDS:
            if median_gap_days <= bound:
                return label
        return "irregular"

    def _ingest_silver_economy_series(self) -> None:
        """`silver.economy_series` - the DIMENSION table for the economy fact table.
        One row per series (1,034), PK `(exchange, ticker)`.

        Holds what a one-row-per-date panel cannot carry and what `silver.economy` must
        not carry (see there): `country`, `scrape_main_type`, `category`, plus the
        observed `frequency`, `observations`, `first_date`/`last_date`.

        `frequency` is DERIVED, not given - TradingView publishes no frequency field -
        and it is what sets the publication lag in `_ingest_gold_economy`.
        """
        self._logger.log_info("Ingesting silver economy series dimension...")

        df = self._helper_select(
            schema_name=BRONZE_SCHEMA, table_name="trading_view_economy"
        )

        if df.empty:
            raise MissingSourceDataError(
                f"{BRONZE_SCHEMA}.trading_view_economy is empty - run the bronze "
                f"economy ingest first."
            )

        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values(["exchange", "ticker", "date"])

        gaps = (
            df.groupby(["exchange", "ticker"])["date"]
            .apply(lambda s: s.diff().dt.days.median())
            .rename("median_gap_days")
        )
        agg = df.groupby(["exchange", "ticker"]).agg(
            country=("country", "first"),
            scrape_main_type=("scrape_main_type", "first"),
            category=("category", "first"),
            observations=("value", "size"),
            first_date=("date", "min"),
            last_date=("date", "max"),
        )
        dim = agg.join(gaps).reset_index()
        dim["frequency"] = dim["median_gap_days"].map(self._classify_economy_frequency)
        dim["first_date"] = dim["first_date"].dt.date
        dim["last_date"] = dim["last_date"].dt.date

        dim = dim[
            [
                "exchange",
                "ticker",
                "country",
                "scrape_main_type",
                "category",
                "frequency",
                "observations",
                "median_gap_days",
                "first_date",
                "last_date",
            ]
        ]

        self._logger.log_info(
            "economy_series: "
            + f"{len(dim)} series - "
            + ", ".join(f"{n} {f}" for f, n in dim["frequency"].value_counts().items())
        )

        self._database_driver.drop_table(SILVER_SCHEMA, "economy_series")
        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="economy_series",
            primary_keys=["exchange", "ticker"],
            df=dim,
            dtype_overrides={
                "first_date": DataType.DATE(),
                "last_date": DataType.DATE(),
            },
        )

    # The four CafeF index tabs, and how each column must be cast coming out of bronze.
    # Names are taken straight from the bronze ingests - VERIFIED to be collision-free
    # across the four tabs, so the merge needs no suffixes and no renaming.
    INDEX_TABS = {
        "cafef_index_price": (
            ["open", "high", "low", "close_raw", "close_adjust",
             "value_matched", "value_negotiated"],
            ["volume_matched", "volume_negotiated"],
        ),
        "cafef_index_order_stats": (
            ["avg_vol_per_buy_order", "avg_vol_per_sell_order"],
            ["n_buy_orders", "buy_order_vol", "n_sell_orders", "sell_order_vol"],
        ),
        "cafef_index_foreign": (
            ["foreign_buy_value", "foreign_sell_value", "foreign_net_value",
             "foreign_own"],
            ["foreign_buy_volume", "foreign_sell_volume", "foreign_net_volume",
             "foreign_room_left"],
        ),
        "cafef_index_prop_trading": (
            ["prop_buy_val", "prop_sell_val"],
            ["prop_buy_vol", "prop_sell_vol"],
        ),
    }

    def _ingest_silver_stock_market(self) -> None:
        """The four `bronze.cafef_index_*` tabs -> ONE `silver.stock_market`,
        PK `(exchange, ticker, date)`. 6 market indices, 30 columns.

        The four tabs are four MEASURES of the same entity (index x day) split across
        tables only because the scraper writes one folder per CafeF tab. Joined on the
        full key they become one row per index per day: price + order stats + foreign
        flow + proprietary-desk trades.

        ⚠️ **`ticker` HERE IS AN INDEX CODE, NOT A COMPANY** - `VNINDEX`, `VN30INDEX`,
        `VN100-INDEX`, `HNX-INDEX`, `HNX30-INDEX`, `UPCOM-INDEX`. This table must never
        be unioned into `silver.stocks_basic`; that is why it is its own table with its
        own name rather than extra rows in the per-stock one.

        ⚠️ **OUTER JOIN, and that is a deliberate divergence from
        `_ingest_silver_stocks_basic`,** which left-joins everything onto `cafef_price`.
        Measured on the live tables: the union of keys is **25,935** while `price` alone
        has **24,962** - so a left join would silently DROP 973 index-days that have
        order-stats, foreign or prop-trading data but no price row (VN100-INDEX is the
        worst: 3,088 order-stats days against 2,273 price days). For the per-stock table
        a price day is the natural spine; for six indices, discarding a thousand days of
        real observations to keep that convention is the wrong trade.

        The tabs have very different histories - price from 2000-07, foreign 2007-01,
        order stats 2007-11, prop trading only 2022-11 - so NULL in a measure column
        means "that tab has no record for this index-day", never "zero".

        The old silver table is dropped first so a schema change re-materialises past
        the driver's IF NOT EXISTS create.
        """
        self._logger.log_info(
            "Ingesting silver stock_market data (4 CafeF index tabs -> 1 table)..."
        )

        KEYS = ["exchange", "ticker", "date"]

        frames: dict[str, pd.DataFrame] = {}
        for table_name in self.INDEX_TABS:
            frame = self._helper_select(
                schema_name=BRONZE_SCHEMA, table_name=table_name
            )
            if frame.empty:
                raise MissingSourceDataError(
                    f"{BRONZE_SCHEMA}.{table_name} is empty - run the bronze index "
                    f"ingests first (--select group:bronze)."
                )
            frame["date"] = pd.to_datetime(frame["date"]).dt.date
            frames[table_name] = frame

        df = None
        for table_name, frame in frames.items():
            df = frame if df is None else df.merge(frame, on=KEYS, how="outer")

        # The join must not invent or lose keys: the result is exactly the UNION of the
        # four key sets. Checked rather than assumed - a duplicate key in any input
        # would fan the merge out silently.
        expected = set()
        for frame in frames.values():
            expected |= set(map(tuple, frame[KEYS].itertuples(index=False, name=None)))
        if len(df) != len(expected):
            raise PipelineError(
                f"silver.stock_market: joined {len(df)} rows but the union of the four "
                f"tabs' keys is {len(expected)}. A duplicate (exchange, ticker, date) "
                f"in one of the bronze index tables is the usual cause."
            )

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

        # The driver reads bronze `numeric` as Decimal -> pandas object; without this
        # they would land as VARCHAR in silver, the way the earlier per-source carry-ups
        # already do.
        decimal_cols, bigint_cols = [], []
        for dec, big in self.INDEX_TABS.values():
            decimal_cols += dec
            bigint_cols += big
        df = self._helper_cast_columns(
            df,
            decimal_cols=[c for c in decimal_cols if c in df.columns],
            bigint_cols=[c for c in bigint_cols if c in df.columns],
        )

        df = self._helper_remove_duplicates(df, primary_keys=KEYS)
        df = df[KEYS + [c for c in df.columns if c not in KEYS]]

        self._logger.log_info(
            f"stock_market: {len(df)} index-days x {len(df.columns)} columns, "
            f"{df['ticker'].nunique()} indices."
        )

        self._database_driver.drop_table(SILVER_SCHEMA, "stock_market")
        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name="stock_market",
            primary_keys=KEYS,
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

    def _ingest_silver_cafef_news(self) -> None:
        """Silver `cafef_news` — bronze news cleaned, de-duplicated and **aligned to a
        trading session**. One row per surviving `row_id`; PK `row_id`.

        The transform itself is `sentiment.news_clean` (pure pandas), imported here so
        this module owns only the ETL: read bronze → read the session calendar →
        `clean_news` → assert → save. Same split as `_ingest_silver_cafef_news_sentiment`
        and `src/ta`.

        What changes relative to bronze:

        * `type='error'` rows (scrape failures) and empty articles are dropped;
        * republished stories collapse on `(ticker, trading_date, normalised headline)` —
          the URL differs, so the bronze `row_id` does NOT catch them;
        * Word-export residue (`Normal 0 false false false EN-US …`), the
          `- File đính kèm: x.pdf` stub and the `Theo HOSE` sign-off are stripped;
        * `ts_is_date_only` flags the 22.2% of bronze rows stamped exactly `00:00:00`
          (89,639 disclosures, 137 errors, **59 editorials**);
        * `relevance_score` counts how often the ticker is actually named.

        ⚠️ **`trading_date` is the look-ahead guard and the reason this table exists.**
        An article is assigned the first session whose OPEN comes after it, on a 09:00 ICT
        boundary; a date-only stamp is treated as end-of-day, i.e. the NEXT session, which
        can only ever delay information rather than advance it. 65.5% of this corpus
        publishes outside 09:00-15:00 (the mode is 17:00), so the calendar-day assignment
        every paper in `experiment/experiment_10` except one uses would put post-close news
        in the same row as that day's close. `leakage_violations` is asserted to be EMPTY
        before anything is written — that is the defect which disqualifies papers 46, 47
        and 50.

        The old table is dropped first so a schema change re-materialises past the
        driver's IF NOT EXISTS create.
        """
        from sentiment.news_clean import clean_news, leakage_violations

        self._logger.log_info(
            "Ingesting silver cafef_news (clean + de-dup + session alignment)..."
        )
        out_table = "cafef_news"

        news = self._helper_select(schema_name=BRONZE_SCHEMA, table_name="cafef_news")
        if news.empty:
            raise MissingSourceDataError(
                f"{BRONZE_SCHEMA}.cafef_news is empty - run the bronze news ingest "
                f"first (--select bronze/cafef_news)."
            )

        # ⚠️ The session calendar comes from a DISTINCT query, never a full read:
        # silver.stocks_basic is ~2.4M rows and fetching it whole has stalled runs before.
        with self._database_driver._cursor_ctx() as cur:
            cur.execute(
                f"SELECT DISTINCT date FROM {SILVER_SCHEMA}.stocks_basic ORDER BY date"
            )
            sessions = [row[0] for row in cur.fetchall()]
        if not sessions:
            raise MissingSourceDataError(
                f"{SILVER_SCHEMA}.stocks_basic has no dates - the trading calendar is "
                f"required to align news to sessions (--select silver/stocks_basic)."
            )
        session_arr = pd.to_datetime(pd.Series(sessions)).to_numpy(dtype="datetime64[D]")

        result = clean_news(news, session_arr)
        dropped = result.attrs.get("dropped", {})
        self._logger.log_info(
            f"cafef_news: {len(news)} bronze -> {len(result)} silver "
            f"(dropped {dropped})"
        )

        # ⚠️ The invariant, asserted rather than commented.
        violations = leakage_violations(result)
        if len(violations) > 0:
            raise PipelineError(
                f"{SILVER_SCHEMA}.{out_table}: {len(violations)} rows would let an "
                f"article inform the session it was published into. First offenders: "
                f"{violations.head(3)[['row_id', 'ts_resolved', 'trading_date']].to_dict('records')}"
            )

        result = self._helper_clean(
            result,
            [
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL("row_id"),
                CleanLayer.ORDER_BY(["exchange", "ticker", "trading_date", "news_order"]),
            ],
        )
        result = self._helper_cast_columns(
            result,
            decimal_cols=["relevance_score"],
            bigint_cols=["news_order", "content_len", "ticker_hits"],
        )

        self._database_driver.drop_table(SILVER_SCHEMA, out_table)
        self._helper_save_pandas_table_to_database(
            schema_name=SILVER_SCHEMA,
            table_name=out_table,
            primary_keys=["row_id"],
            df=result,
            dtype_overrides={
                "timestamp": DataType.TIMESTAMP(),
                "ts_resolved": DataType.TIMESTAMP(),
                "trading_date": DataType.DATE(),
                "type": DataType.VARCHAR(),
                "category": DataType.VARCHAR(),
                "headline": DataType.TEXT(),
                "content_clean": DataType.TEXT(),
                "url": DataType.TEXT(),
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
            # ⚠️ RAISES, where this used to log and return. `cafef_price` is the SPINE:
            # without it the method produced no table at all and still reported success,
            # which under Dagster would be a green asset over a missing table.
            raise MissingSourceDataError(
                f"{BRONZE_SCHEMA}.cafef_price is empty — it is the spine of "
                f"silver.stocks_basic, so there is nothing to build. Run the bronze "
                f"ingests first (--select group:bronze)."
            )

        df = base
        for table_name in ["cafef_order_stats", "cafef_foreign", "cafef_prop_trading"]:
            right = self._helper_select(
                schema_name=BRONZE_SCHEMA, table_name=table_name
            )
            if right.empty:
                # An OPTIONAL source, unlike the spine: the table still builds, one
                # measure block short. WARNING rather than INFO because the loss is
                # silent in the output — those columns are simply absent.
                self._logger.log_warning(
                    f"No bronze {table_name} data found; silver.stocks_basic will be "
                    f"built WITHOUT its columns."
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
        passthrough_cols: Optional[List[str]] = None,
        exact_float_cols: Optional[List[str]] = None,
        prepare_fn: Optional[Any] = None,
        standard_features: bool = True,
    ) -> None:
        """
        Generic gold ingest: read a silver table, coerce numeric source columns,
        categorize as OHLC vs single-value, apply the standard feature-engineering
        layers (plus any table-specific TA layers), and checkpoint-save to gold.

        `silver_table_name` is the source table to READ from silver; it defaults to
        `table_name` (the gold table WRITTEN). They differ only where the silver and
        gold table names diverge — e.g. gold `stocks` is built from silver
        `stocks_basic`.

        `passthrough_cols` — extra source columns to carry through UNTOUCHED, beyond
        the keys and the GICS class columns. ⚠️ Everything else is
        `pd.to_numeric(errors="coerce")`d, which turns a text or date column into a
        column of NULLs **without raising**, so any non-numeric column a source adds
        must be named here (`stocks_basic_financials_bank_fa` brings `publish_date`
        and the nine per-report `template`/`period`/`source` provenance columns).
        ⚠️ It is NOT derived from `information_schema` here, and must not be: several
        silver carry-ups store real numbers as VARCHAR (`silver.bonds.value` is the
        live example) and those MUST still be coerced. A caller whose source is known
        to be properly typed may derive it — see
        `_ingest_gold_stocks_financials_bank_fa`.

        `exact_float_cols` — float columns to store as `DOUBLE PRECISION` instead of
        gold's default `REAL`. REAL's ~7 significant digits are ample for prices and
        ratios but not for VND balance-sheet lines, which reach ~1e15-1e17: at that
        magnitude REAL's step is ~1e8-1e10, so the figure would come back off by
        hundreds of millions of dong. Reserve it for columns carried from silver
        (the computed feature block stays REAL — 900 of them at 8 bytes is what the
        8160-byte row limit cannot take).

        `prepare_fn(df) -> df` — a last reshape after the numeric coercion and BEFORE
        the feature layers, for sources whose columns are not yet in the shape the TA
        functions expect (`_helper_adjust_ohlc` is the one caller: it rebuilds a
        split-adjusted OHLC set, because TA-Lib's defaults read `open`/`high`/`low`/
        `close` and silver's `open`/`high`/`low` are RAW while `close_adjust` is not).

        `standard_features=False` — build the table with NO derived columns at all:
        neither the `ta_layers` (pass none) nor the standard
        `_helper_build_feature_layers` block. The gold table is then the prepared
        silver frame, written as-is. ⚠️ **This deliberately disables the empty-layer
        guard below**, which exists to catch a table that came out a copy of its input
        BY ACCIDENT — with this flag that is the stated intent, so the two cases must
        be distinguishable. `gold.stocks` is the one caller: it is the plain price
        panel, and its feature-bearing twin is `gold.stocks_ta`.
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
        _non_numeric = {
            "exchange",
            "ticker",
            "date",
            *self.GICS_CLASS_COLS,
            *(passthrough_cols or []),
        }
        for col in df.columns:
            if col not in _non_numeric:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if prepare_fn is not None:
            df = prepare_fn(df)

        transform_layers = list(ta_layers or [])
        if standard_features:
            transform_layers += self._helper_build_feature_layers(df)
            if not transform_layers:
                raise PipelineError(
                    f"No transform layers resolved for gold {table_name} — the silver "
                    f"table's columns matched no feature layer, so the gold table would "
                    f"be a copy of its input."
                )

        _exact = set(exact_float_cols or [])

        # A pass-through DATE column arrives as `object` (psycopg2 hands back
        # `datetime.date`), and `_helper_infer_sql_type` maps object → VARCHAR — so
        # `publish_date` would land in gold as text unless it is named here.
        _passthrough_dates = [
            c
            for c in (passthrough_cols or [])
            if c in df.columns
            and df[c].notna().any()
            and df[c].dropna().map(lambda v: isinstance(v, date)).all()
        ]

        def _checkpoint(chunk: pd.DataFrame) -> None:
            # Use REAL (4-byte float) for all float columns to stay within
            # PostgreSQL's 8160-byte row size limit given the large number of columns.
            # `exact_float_cols` opts individual columns back up to DOUBLE PRECISION.
            overrides: dict[str, str] = {"date": DataType.DATE()}
            overrides.update({c: DataType.DATE() for c in _passthrough_dates})
            for col in chunk.columns:
                if str(chunk[col].dtype).lower().startswith("float"):
                    overrides[col] = (
                        "DOUBLE PRECISION" if col in _exact else "REAL"
                    )
            self._helper_save_pandas_table_to_database(
                schema_name=GOLD_SCHEMA,
                table_name=table_name,
                primary_keys=["exchange", "ticker", "date"],
                df=chunk,
                dtype_overrides=overrides,
                use_copy=True,
            )

        # ⚠️ REBUILD, don't append. `_checkpoint` saves with `use_copy=True`, and the
        # COPY path assumes an EMPTY table — re-running over an existing gold table
        # dies on the primary key (`duplicate key value violates unique constraint …
        # Key (exchange, ticker, date)=(HOSE, VCB, 2009-06-30) already exists`), which
        # is exactly what a second materialisation of an ASSET is. A gold table is a
        # pure function of its silver source, so replacing it is the correct semantic
        # and a merge would be meaningless; dropping also lets the column set change
        # when the feature list does. `_ingest_gold_economy` and
        # `_ingest_gold_stock_market` already do this — the generic builder was the
        # one path where "drop the gold table first" stayed a manual instruction.
        # Kept as late as possible, so a failure earlier in the build leaves the old
        # table intact.
        self._database_driver.drop_table(GOLD_SCHEMA, table_name)

        # ⚠️ `_helper_transform` returns the frame UNTOUCHED when no layer resolves —
        # it never reaches `checkpoint_fn`. That is right for its own contract and
        # wrong here: with `standard_features=False` there is nothing to compute but
        # there is still a table to write, so a `standard_features=False` build routed
        # through it would log success and write NOTHING. Write the prepared frame
        # directly instead, in the same 100k chunks the checkpoint path uses.
        if not transform_layers:
            for start in range(0, len(df), 100_000):
                _checkpoint(df.iloc[start : start + 100_000].copy())
                self._logger.log_info(
                    f"Checkpoint saved: {min(start + 100_000, len(df))}/{len(df)} rows"
                )
            return

        self._helper_transform(
            df,
            transform_layers,
            checkpoint_fn=_checkpoint,
            checkpoint_size=100_000,
        )

    # ── gold.economy — the WIDE macro panel ─────────────────────────────────────
    #
    #     {country}__{scrape_main_type}__{category}__{exchange}__{ticker}
    #     vietnam__economy__prices__economics__vncpi
    #
    # THE SEPARATOR IS TWO UNDERSCORES BECAUSE THE VALUES CONTAIN ONE.
    # `mainland_china`, `south_korea` - with a single `_` no rule can split a name back
    # into its fields; with `__`, `split("__")` is exact and
    # `split_part(column_name,'__',1)` is a usable GROUP BY. Coarse -> fine, so
    # alphabetical column order groups a country's series together. Lowercase, because
    # `_helper_build_upsert_sql` interpolates column names UNQUOTED.
    ECONOMY_PANEL_NAME_PARTS = (
        "country",
        "scrape_main_type",
        "category",
        "exchange",
        "ticker",
    )
    ECONOMY_PANEL_NAME_SEP = "__"

    # PostgreSQL truncates identifiers over 63 BYTES silently, and two names that
    # truncate alike collide. Today's longest is 57, but the vocabulary allows
    # 14 + 7 + 10 + 9 + 20 + 8 separators = 68 - the longest ticker simply does not
    # co-occur with the longest country and category. That is luck, so this is checked.
    #
    # THE LIMIT CANNOT BE RAISED ON THIS SERVER. It is `NAMEDATALEN - 1`, and NAMEDATALEN
    # is a COMPILE-TIME constant in `src/include/pg_config_manual.h`: changing it means
    # building PostgreSQL from source and `initdb`-ing a fresh cluster (the catalogue
    # layout changes, so an existing data directory is unreadable by the rebuilt binary).
    # It is not a GUC and `ALTER` cannot reach it. Shorten the NAME, never the server.
    PG_IDENTIFIER_LIMIT = 63

    # ⚠️ BOTH DICTS BELOW ARE ASSUMPTIONS, NOT DATA. TradingView gives a reference period
    # and no release date, so the lag cannot be read off the source - it is imposed.
    #
    # `date` in the source is the period the figure DESCRIBES. Vietnam's Q1 GDP is dated
    # 2026-03-31 and published in the first week of April, so joining a panel on `date`
    # would hand a model a number ~a week before it existed - look-ahead bias, straight
    # into a backtest. Shifting each observation forward by its frequency's typical
    # publication lag is the conservative fix; tighten per series if release dates are
    # ever scraped.
    ECONOMY_PUBLICATION_LAG_DAYS = {
        "daily": 0,
        "weekly": 3,
        "monthly": 30,
        "quarterly": 45,
        "annual": 90,
        "single": 0,
        "irregular": 30,
    }

    # How long a value stays "the current known value" before it goes stale, in BUSINESS
    # DAYS (~1.5-2x the natural period). Without a cap, a series that stopped reporting
    # in 2010 would carry its last value forward to 2036 and read as live data.
    ECONOMY_MAX_STALENESS_BDAYS = {
        "daily": 5,
        "weekly": 10,
        "monthly": 45,
        "quarterly": 130,
        "annual": 400,
        "single": 0,
        "irregular": 45,
    }

    def _build_economy_panel_columns(self, df: pd.DataFrame) -> pd.Series:
        """The composite column name for every row, verified fit for PostgreSQL.

        RAISES rather than let a name be truncated into a collision - the failure would
        otherwise surface as two series quietly sharing one column.
        """
        parts = [
            df[part].astype("string").str.strip().str.lower()
            for part in self.ECONOMY_PANEL_NAME_PARTS
        ]
        names = parts[0]
        for part in parts[1:]:
            names = names + self.ECONOMY_PANEL_NAME_SEP + part

        unique_names = list(pd.unique(names.dropna()))
        too_long = sorted(
            n for n in unique_names if len(n.encode()) > self.PG_IDENTIFIER_LIMIT
        )
        if too_long:
            raise PipelineError(
                f"{len(too_long)} gold.economy column name(s) exceed PostgreSQL's "
                f"{self.PG_IDENTIFIER_LIMIT}-byte identifier limit and would be "
                f"TRUNCATED SILENTLY, e.g. {too_long[:3]}. Shorten the template - drop "
                f"`scrape_main_type` (constant, -9 chars) or use ISO country codes "
                f"(-12) - rather than let two series share a column."
            )

        truncated = {n[: self.PG_IDENTIFIER_LIMIT] for n in unique_names}
        if len(truncated) != len(unique_names):
            raise PipelineError(
                f"gold.economy column names collide after truncation: "
                f"{len(unique_names)} names -> {len(truncated)} distinct."
            )
        return names

    def _ingest_gold_economy(self) -> None:
        """`silver.economy` + `silver.economy_series` -> `gold.economy`:
        **one row per business day**, one column per series, AS-OF filled.

        ⚠️ **THIS IS THE ONLY GOLD ECONOMY TABLE.** Until 2026-08-01 `gold.economy` was
        the generic `_ingest_gold_table("economy")` output instead — the LONG grain with
        per-series TA features (579,459 rows x 16 columns: returns, volatility, rolling
        stats), and the wide panel lived beside it as `gold.economy_panel`. Two gold
        tables for one asset is one too many, so the wide panel took the name. Restoring
        the feature table is one line (`self._ingest_gold_table("economy")`) — the
        generic builder is untouched and still drives bonds/forex/funds/indices/stocks.

        This is the wide macro panel a model joins on `date` alone - the shape
        `DataPostprocessor._join_macroeconomics_columns` expects. It lives in GOLD, not
        silver, because every step below is a modelling decision:

        1. **publication lag** - each observation becomes visible only
           `ECONOMY_PUBLICATION_LAG_DAYS[frequency]` days after the period it describes,
           because `date` in the source is the REFERENCE period, not the release date;
        2. **as-of carry** - between releases the last published value IS the current
           known value, so it is carried forward on a business-day calendar;
        3. **staleness cap** - but only for `ECONOMY_MAX_STALENESS_BDAYS[frequency]`, so
           a series that stopped reporting in 2010 does not read as live data in 2026;
        4. **business-day roll-forward** - an availability date landing on a weekend
           becomes the next business day, because the panel is indexed by business day
           and a plain reindex would DROP that observation (see the code comment: this
           silently lost Vietnam's Q4-2025 GDP);
        5. **the calendar ends TODAY** - projection rows dated out to 2036 are raw data
           in bronze/silver, but in a model panel they are 2,685 near-empty rows that
           only make a look-ahead join possible.

        Silver stays long and raw-faithful; none of this touches it.

        Density: the long source is 5.8% of a date x series grid; after the as-of carry
        the panel is ~91% filled, which is the point of building it.

        ⚠️ **`REAL`, not `DOUBLE PRECISION`.** 1,034 float8 columns is 8,272 bytes and
        PostgreSQL's row limit is ~8,160; float4 halves it to ~4.1 kB. Macro series carry
        far fewer than REAL's ~7 significant digits, so nothing is lost - the same reason
        `_ingest_gold_table` casts its feature columns.
        """
        self._logger.log_info("Ingesting gold economy (wide, as-of filled)...")

        fact = self._helper_select(schema_name=SILVER_SCHEMA, table_name="economy")
        dim = self._helper_select(
            schema_name=SILVER_SCHEMA, table_name="economy_series"
        )

        if fact.empty:
            raise MissingSourceDataError(
                f"{SILVER_SCHEMA}.economy is empty - run the silver economy ingest first."
            )
        if dim.empty:
            raise MissingSourceDataError(
                f"{SILVER_SCHEMA}.economy_series is empty - run the silver economy "
                f"series ingest first. Without it the panel cannot name its columns or "
                f"pick a publication lag."
            )

        df = fact.merge(
            dim[
                [
                    "exchange",
                    "ticker",
                    "country",
                    "scrape_main_type",
                    "category",
                    "frequency",
                ]
            ],
            on=["exchange", "ticker"],
            how="inner",
            validate="many_to_one",
        )
        if len(df) != len(fact):
            # An inner join that loses rows means the dimension is stale - rebuild it
            # rather than silently publish a panel missing whole series.
            raise PipelineError(
                f"{len(fact) - len(df)} of {len(fact)} silver.economy rows have no "
                f"matching row in silver.economy_series. Re-run the dimension ingest."
            )

        df["date"] = pd.to_datetime(df["date"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])
        df["series"] = self._build_economy_panel_columns(df)

        lag = df["frequency"].map(self.ECONOMY_PUBLICATION_LAG_DAYS).fillna(30)

        # ⚠️ ROLL FORWARD TO THE NEXT BUSINESS DAY, DO NOT JUST ADD THE LAG.
        # The panel is indexed by business day, so an availability date landing on a
        # weekend would be dropped outright by the reindex below — silently. Found the
        # hard way: Vietnam's Q4-2025 GDP is dated 2025-12-31, and 2025-12-31 + 45 days
        # is Saturday 2026-02-14, so that figure VANISHED from the panel entirely (the
        # series jumped straight from Q3 to Q1). Rolling forward also matches reality:
        # a release nobody can act on until Monday becomes known on Monday.
        df["available_from"] = (df["date"] + pd.to_timedelta(lag, unit="D")).map(
            pd.offsets.BusinessDay().rollforward
        )

        # A lag can push two observations of one series onto the same day; the later
        # REFERENCE period wins, which is what a reader on that day would see.
        df = df.sort_values(["series", "available_from", "date"])
        expected_cells = df.groupby(["series", "available_from"], sort=False).ngroups

        wide = df.pivot_table(
            index="available_from", columns="series", values="value", aggfunc="last"
        )

        # ⚠️ THE PANEL STOPS AT TODAY. 47 series carry projections with reference dates
        # out to 2036; on a business-day calendar those became 2,685 rows that were 2.3%
        # filled — a decade of near-empty rows whose only real effect is to make a
        # look-ahead join possible. Projections stay in bronze and silver, where they are
        # raw data; the MODEL panel is "what was knowable by then", so it ends now.
        last_day = pd.offsets.BusinessDay().rollback(pd.Timestamp.today().normalize())
        calendar = pd.bdate_range(wide.index.min(), last_day, name="date")
        wide = wide.reindex(calendar)

        # Every observation the lag made visible on or before `last_day` must survive the
        # reindex. This is the invariant the weekend bug broke, so it is checked, not
        # assumed.
        expected_in_range = df.loc[
            df["available_from"] <= last_day, ["series", "available_from"]
        ].drop_duplicates().shape[0]
        landed = int(wide.notna().sum().sum())
        if landed != expected_in_range:
            raise PipelineError(
                f"gold.economy lost {expected_in_range - landed} observation(s) "
                f"in the reindex: {expected_in_range} distinct (series, available_from) "
                f"pairs fall on or before {last_day.date()}, but only {landed} cells "
                f"landed. An availability date that is not a business day is the usual "
                f"cause."
            )
        dropped_future = expected_cells - expected_in_range
        if dropped_future:
            self._logger.log_info(
                f"gold economy: {dropped_future} observation(s) have an availability "
                f"date after {last_day.date()} (projections) and are not in the panel; "
                f"they remain in silver.economy."
            )

        # Per-frequency staleness cap, so the carry cannot outlive the series.
        staleness = (
            dim.assign(series=self._build_economy_panel_columns(dim))
            .set_index("series")["frequency"]
            .map(self.ECONOMY_MAX_STALENESS_BDAYS)
            .fillna(45)
            .astype(int)
        )
        raw_cells = int(wide.notna().sum().sum())
        for limit, cols in staleness.groupby(staleness):
            present = [c for c in cols.index if c in wide.columns]
            if present and limit > 0:
                wide[present] = wide[present].ffill(limit=int(limit))

        wide = wide.reset_index()
        wide.columns.name = None
        wide["date"] = wide["date"].dt.date

        filled = int(wide.drop(columns=["date"]).notna().sum().sum())
        cells = len(wide) * (len(wide.columns) - 1)
        self._logger.log_info(
            f"gold economy: {len(wide)} business days x {len(wide.columns) - 1} series "
            f"- {raw_cells} observations visible after the publication lag, "
            f"{filled} cells after the as-of carry ({100.0 * filled / max(cells, 1):.1f}% "
            f"filled, from {100.0 * raw_cells / max(cells, 1):.1f}%)."
        )

        overrides: dict[str, str] = {"date": DataType.DATE()}
        for col in wide.columns:
            if col != "date":
                overrides[col] = "REAL"

        self._database_driver.drop_table(GOLD_SCHEMA, "economy")
        self._helper_save_pandas_table_to_database(
            schema_name=GOLD_SCHEMA,
            table_name="economy",
            primary_keys=["date"],
            df=wide,
            dtype_overrides=overrides,
            use_copy=True,
        )

    # ── gold.stock_market — the WIDE index panel ─────────────────────────────────
    #
    #     {exchange}__{ticker}__{measure}
    #     hose__vnindex__close_adjust,  hnx__hnx_index__n_buy_orders
    #
    # Same `__` convention as gold.economy: the values contain single underscores
    # (`close_adjust`, `n_buy_orders`), so only a double underscore can be split back.
    #
    # ⚠️ THE TICKERS CONTAIN HYPHENS AND POSTGRES CANNOT. `HNX-INDEX`, `VN100-INDEX`,
    # `HNX30-INDEX` and `UPCOM-INDEX` are real index codes, but `hnx-index` unquoted
    # parses as `hnx MINUS index` - and `_helper_build_upsert_sql` interpolates column
    # names UNQUOTED. Hyphens therefore become underscores, and the result is checked
    # for collisions: sanitising two different tickers into one column name would merge
    # two indices silently.
    GOLD_MARKET_NAME_SEP = "__"

    def _sanitize_identifier(self, value: str) -> str:
        """Lowercase, and turn anything PostgreSQL cannot take unquoted into `_`."""
        out = "".join(
            ch if (ch.isalnum() or ch == "_") else "_" for ch in str(value).strip().lower()
        )
        return out if (out and not out[0].isdigit()) else f"i_{out}"

    def _ingest_gold_stock_market(self) -> None:
        """`silver.stock_market` -> `gold.stock_market`: **one row per DATE**, PK `date`,
        one column per (index x measure) named `{exchange}__{ticker}__{measure}`.

        6 indices x 27 measures = 162 columns on the trading-day calendar the data
        itself defines (the distinct dates in silver), not a synthetic business-day
        range - Vietnamese exchange holidays are not weekends.

        ⚠️ **NO as-of fill here, unlike `gold.economy`, and the difference is the
        source.** Macro series are published on a lag and are *stale but valid* between
        releases, so carrying them forward is what a reader would know. An index either
        traded on a day or it did not: a gap means VN100-INDEX did not exist yet
        (it starts 2014) or that tab has no record, and forward-filling would invent
        prices for days the market was shut. NULL stays NULL.

        ⚠️ **Column types are inferred (DECIMAL), not forced to REAL.** `gold.economy`
        uses REAL because 1,034 float8 columns would exceed PostgreSQL's ~8 kB row limit;
        at 162 columns there is no such pressure, and `value_matched` reaches ~1e12 where
        REAL's ~7 significant digits would start losing whole thousands.
        """
        self._logger.log_info("Ingesting gold stock_market (wide, 1 row per date)...")

        KEYS = ["exchange", "ticker", "date"]

        df = self._helper_select(schema_name=SILVER_SCHEMA, table_name="stock_market")
        if df.empty:
            raise MissingSourceDataError(
                f"{SILVER_SCHEMA}.stock_market is empty - run the silver stock_market "
                f"ingest first."
            )

        df["date"] = pd.to_datetime(df["date"]).dt.date
        measures = [c for c in df.columns if c not in KEYS]

        # Cast before melting: the driver hands back `numeric` as Decimal (object
        # dtype), and a melted object column would land in gold as VARCHAR.
        decimal_cols, bigint_cols = [], []
        for dec, big in self.INDEX_TABS.values():
            decimal_cols += dec
            bigint_cols += big
        df = self._helper_cast_columns(
            df,
            decimal_cols=[c for c in decimal_cols if c in df.columns],
            bigint_cols=[c for c in bigint_cols if c in df.columns],
        )

        long = df.melt(
            id_vars=KEYS, value_vars=measures, var_name="measure", value_name="value"
        ).dropna(subset=["value"])

        sep = self.GOLD_MARKET_NAME_SEP
        long["column"] = (
            long["exchange"].map(self._sanitize_identifier)
            + sep
            + long["ticker"].map(self._sanitize_identifier)
            + sep
            + long["measure"].map(self._sanitize_identifier)
        )

        # Sanitising must not merge two indices into one column, and the result must fit
        # PostgreSQL's identifier limit. Both are checked, not assumed.
        pairs = long[["exchange", "ticker", "measure", "column"]].drop_duplicates()
        collided = pairs.groupby("column").size()
        collided = collided[collided > 1]
        if len(collided):
            raise PipelineError(
                f"gold.stock_market: {len(collided)} column name(s) are produced by more "
                f"than one (exchange, ticker, measure) - sanitising merged distinct "
                f"indices, e.g. {list(collided.index[:3])}. Rename before publishing."
            )
        too_long = [
            c for c in pairs["column"].unique()
            if len(c.encode()) > self.PG_IDENTIFIER_LIMIT
        ]
        if too_long:
            raise PipelineError(
                f"{len(too_long)} gold.stock_market column name(s) exceed PostgreSQL's "
                f"{self.PG_IDENTIFIER_LIMIT}-byte identifier limit and would be "
                f"TRUNCATED SILENTLY, e.g. {sorted(too_long)[:3]}."
            )

        wide = long.pivot(index="date", columns="column", values="value")
        wide = wide.sort_index().reset_index()
        wide.columns.name = None

        # Nothing may be lost on the way through: one non-null cell per observation.
        landed = int(wide.drop(columns=["date"]).notna().sum().sum())
        if landed != len(long):
            raise PipelineError(
                f"gold.stock_market: {len(long)} observations went into the pivot but "
                f"{landed} cells came out. A duplicate (exchange, ticker, date, measure) "
                f"in silver.stock_market is the usual cause."
            )

        cells = len(wide) * (len(wide.columns) - 1)
        self._logger.log_info(
            f"gold stock_market: {len(wide)} trading days x {len(wide.columns) - 1} "
            f"columns, {landed} observations "
            f"({100.0 * landed / max(cells, 1):.1f}% of cells filled)."
        )

        self._database_driver.drop_table(GOLD_SCHEMA, "stock_market")
        self._helper_save_pandas_table_to_database(
            schema_name=GOLD_SCHEMA,
            table_name="stock_market",
            primary_keys=["date"],
            df=wide,
            dtype_overrides={"date": DataType.DATE()},
            chunk_size=1_000,
        )

    def _helper_stock_ta_layers(self, volume_col: str) -> List[TransformLayer]:
        """The per-stock TA battery: ~40 TA-Lib indicators (overlap studies, momentum,
        volume, cycle, price transform, volatility) plus the three microstructure
        features built from the CafeF foreign-flow / volume-breakdown columns.

        Shared by every gold table built on a daily per-stock OHLCV panel
        (`gold.stocks` from `silver.stocks_basic`, `gold.stocks_financials_bank_fa`
        from `silver.stocks_basic_financials_bank_fa`) so the two cannot drift into
        different feature sets — which would make them incomparable while looking
        identical.

        Everything price-based takes TA-Lib's default `open`/`high`/`low`/`close`, so
        the caller must hand `_ingest_gold_table` a frame that HAS those columns —
        see `_helper_adjust_ohlc`. `volume_col` is explicit because the CafeF panel
        splits volume into `volume_matched` / `volume_negotiated` and only the matched
        side belongs in a money-flow indicator.

        ⚠️ `volume_col="volume"` (what `_ingest_gold_stocks` still passes) has not
        existed on `silver.stocks_basic` since the 2026-07-19 rewrite — see the note
        on `_ingest_gold_stocks`."""
        return [
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
            TransformLayer.TA_ADD_MFI(volume_col=volume_col),
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
            TransformLayer.TA_ADD_AD(volume_col=volume_col),
            TransformLayer.TA_ADD_ADOSC(volume_col=volume_col),
            TransformLayer.TA_ADD_OBV(volume_col=volume_col),
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
        ]

    # ── gold.stocks / gold.stocks_ta — the per-stock panel, SPLIT IN TWO ────────
    #
    # One source (`silver.stocks_basic`), two gold tables, and the split is by whether
    # a column is CARRIED or COMPUTED:
    #
    #   gold.stocks     — the price/flow panel and nothing else (~42 columns)
    #   gold.stocks_ta  — the same panel plus the ~900-column feature block
    #
    # ⚠️ **They are not built from each other.** `stocks_ta` recomputes its own base
    # from silver rather than joining `stocks`, for the same reason
    # `stocks_financials_bank_fa` does not join it either: a table that carried its
    # base from another gold table could disagree with it about a stock-day while
    # looking identical. The cost is one extra read of a 2.4 M-row table.
    #
    # ⚠️ **A reader wanting only prices should read `stocks`.** `stocks_ta` is ~11 GB
    # against ~200 MB, and PostgreSQL still reads the whole row.
    #
    # The pair replaces the single `gold.stocks` that existed until 2026-08-03 — 935
    # columns of which 30 were the panel. `unified_schema_creator.ipynb` already split
    # them by hand (`GOLD_NON_TA` vs `pool__ta`); this makes the split a table boundary.

    def _helper_gold_stocks_column_types(self) -> tuple[list[str], list[str]]:
        """`(passthrough_cols, exact_float_cols)` for a gold table built on
        `silver.stocks_basic`, derived from the source's own declared types.

        Deriving is safe HERE and is not in general (see `_ingest_gold_table`):
        `silver.stocks_basic` is a typed join of six bronze tables, not one of the
        carry-ups that store numbers as VARCHAR.

        ⚠️ Every carried numeric column is DOUBLE PRECISION, not gold's default REAL.
        `value_matched` reaches ~1e12 and `value_negotiated` further, where REAL's ~7
        significant digits round to the nearest ~1e5 — the gold copy would differ from
        silver by hundreds of thousands of dong. Gold defaults to REAL only because a
        ~900-column feature block cannot fit PostgreSQL's ~8160-byte row limit at 8
        bytes a column; the carried block is ~40 columns and has no such pressure.
        """
        source_types = self._helper_column_types(SILVER_SCHEMA, "stocks_basic")
        if not source_types:
            raise MissingSourceDataError(
                f"`{SILVER_SCHEMA}.stocks_basic` does not exist — build the silver "
                f"`stocks_basic` leaf first."
            )

        passthrough = [
            col
            for col, sql_type in source_types.items()
            if sql_type in ("character varying", "text", "date")
            and col not in ("exchange", "ticker", "date")
        ]
        exact = [
            col
            for col, sql_type in source_types.items()
            if sql_type in ("numeric", "double precision", "real", "bigint", "integer")
        ]
        # `_helper_adjust_ohlc` renames the three raw legs and mints `open`/`high`/
        # `low`/`close`; all seven are carried (or a carried value times a factor), so
        # they keep the exact type too.
        exact += ["open_raw", "high_raw", "low_raw", "open", "high", "low", "close"]
        return passthrough, exact

    def _ingest_gold_stocks(self) -> None:
        """Silver `stocks_basic` → gold `stocks`: the per-stock price/flow panel with
        a SPLIT-ADJUSTED OHLC set and **no derived columns at all**.

        PK `(exchange, ticker, date)`, one row per stock-day, same row count as its
        source. The technicals live in `gold.stocks_ta`.

        ⚠️ **`open`/`high`/`low`/`close` here are ADJUSTED, and they are not silver's.**
        The CafeF panel has raw `open`/`high`/`low` beside `close_raw`/`close_adjust`,
        which is two price scales in one row (VCB 2009-06-30: raw 60,000, adjusted
        9,130) — see `_helper_adjust_ohlc`. The source values are kept beside them as
        `open_raw`/`high_raw`/`low_raw`/`close_raw`, so nothing is lost and neither
        scale is implicit: a reader who wants the unadjusted print can still have it.
        """
        passthrough, exact = self._helper_gold_stocks_column_types()
        self._ingest_gold_table(
            "stocks",
            silver_table_name="stocks_basic",
            passthrough_cols=passthrough,
            exact_float_cols=exact,
            prepare_fn=self._helper_adjust_ohlc,
            standard_features=False,
        )

    def _ingest_gold_stocks_ta(self) -> None:
        """Silver `stocks_basic` → gold `stocks_ta`: everything `gold.stocks` carries,
        plus the full per-stock TA battery (`_helper_stock_ta_layers`) and the standard
        return/volatility/rolling layers. ~900 added columns, ~2.4 M rows.

        ⚠️ **This is the fixed version of what used to be `_ingest_gold_stocks`**, which
        had been stale since the 2026-07-19 rewrite of `silver.stocks_basic` and raised
        `ValueError: Column 'close' not found` — it still asked for `volume` and a
        `close`, neither of which has existed on that table since. The two changes are
        the ones `_ingest_gold_stocks_financials_bank_fa` already made:
        `prepare_fn=self._helper_adjust_ohlc` and `volume_col="volume_matched"` (only
        the matched side belongs in a money-flow indicator).

        ⚠️ **`gold.stocks_ta` in the database is OLDER than this method.** It is the
        2026-08-03 rename of the pre-rewrite `gold.stocks` — 2,678,167 rows on the old
        column names (`close`, `volume`, `f_buy_vol`, `own_pct`). Running this REPLACES
        it with the current schema on ~2.39 M rows, which is a ~900-column rebuild over
        the full universe. Renaming did not run it; that was deliberate.
        """
        passthrough, exact = self._helper_gold_stocks_column_types()
        self._ingest_gold_table(
            "stocks_ta",
            silver_table_name="stocks_basic",
            ta_layers=self._helper_stock_ta_layers(volume_col="volume_matched"),
            passthrough_cols=passthrough,
            exact_float_cols=exact,
            prepare_fn=self._helper_adjust_ohlc,
        )

    # ── gold.stocks_financials_bank_fa ──────────────────────────────────────────
    #
    # `silver.stocks_basic_financials_bank_fa` (the daily price panel × the as-of bank
    # quarter × the 26 fundamental indicators) with the same TA battery `gold.stocks`
    # gets. The result is the one table a model can read end to end: price, flow,
    # GICS, fundamentals and technicals on one row per stock-day.
    #
    # ⚠️ It is deliberately NOT a join against `gold.stocks`. The TA columns are
    # recomputed from this table's own OHLCV, so the panel is self-contained and the
    # two gold tables cannot disagree about a stock-day. The duplication costs
    # ~900 columns over a few thousand rows (bank tickers only), which is nothing
    # next to `gold.stocks`' 2.4 M.
    #
    # ⚠️ Look-ahead: none is introduced here. Every fundamental column already steps
    # on `publish_date <= date` in silver, and TA-Lib indicators are backward-looking
    # by construction.

    def _helper_adjust_ohlc(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rebuild a SPLIT-ADJUSTED OHLC set, and hand it the canonical names.

        ⚠️ **The CafeF panel does not have one price series, it has two halves of two.**
        `open`/`high`/`low` are RAW (they track `close_raw`); only the close comes in
        both flavours, as `close_raw` and `close_adjust`. VCB on 2009-06-30 is the whole
        problem in one row: `open`=`high`=`low`=`close_raw`=60,000 while
        `close_adjust`=9,130. So:

        * running TA on `close_adjust` with the source `high`/`low` mixes two price
          scales inside a single indicator — ATR, Stochastic, MFI, Williams %R and every
          price transform would read a 6.6× gap between the close and the day's range;
        * running it on `close_raw` keeps one scale but re-introduces every split and
          stock dividend as a fake overnight crash.

        Neither is a defensible feature. The fix is the standard one: today's adjustment
        factor is `close_adjust / close_raw`, and the same factor applies to the same
        day's open, high and low.

        The adjusted set takes the canonical names `open`/`high`/`low`/`close` — that is
        what TA-Lib's defaults, `add_intraday_range` and `_helper_build_feature_layers`
        all read, and passing 40 column kwargs to say the same thing would be worse. The
        source values are kept beside them as `open_raw`/`high_raw`/`low_raw` +
        the untouched `close_raw`. ⚠️ **So gold's `open`/`high`/`low` are NOT silver's**
        — same name, adjusted values, which is a modelling decision and therefore gold's
        to make. `close_adjust` is dropped: it would be an exact duplicate of `close`.
        """
        required = {"open", "high", "low", "close_raw", "close_adjust"}
        missing = required - set(df.columns)
        if missing:
            raise PipelineError(
                f"Cannot build an adjusted OHLC set: {sorted(missing)} missing. "
                f"Present: {sorted(df.columns)[:20]}…"
            )

        df = df.rename(columns={"open": "open_raw", "high": "high_raw", "low": "low_raw"})

        # A zero/absent raw close gives no factor — NaN, never a silently wrong price.
        factor = df["close_adjust"] / df["close_raw"].replace(0, np.nan)
        for col in ("open", "high", "low"):
            df[col] = df[f"{col}_raw"] * factor
        df["close"] = df["close_adjust"]
        df = df.drop(columns=["close_adjust"])

        unadjusted = int((factor.round(10) == 1).sum())
        self._logger.log_info(
            f"Adjusted OHLC rebuilt: {len(df)} rows, {factor.notna().sum()} with a "
            f"factor ({unadjusted} of them 1.0 — no corporate action since)."
        )
        return df

    def _ingest_gold_stocks_financials_bank_fa(self) -> None:
        """Silver `stocks_basic_financials_bank_fa` → gold `stocks_financials_bank_fa`:
        every source column, plus the standard return/volatility/rolling features and
        the full per-stock TA battery (`_helper_stock_ta_layers`).

        PK `(exchange, ticker, date)`, same grain and same row count as the source —
        this adds columns, never rows.

        ⚠️ **The carried financial lines are DOUBLE PRECISION, not gold's usual REAL.**
        VND balance-sheet figures reach ~1e15-1e17 (VCB's total assets are ~2.6e15),
        where REAL's ~7 significant digits round to the nearest ~1e8-1e10 — the gold
        copy of a line item would differ from silver by hundreds of millions of dong
        while looking fine. The computed TA/feature block stays REAL: ~900 columns at
        8 bytes each is exactly what PostgreSQL's ~8160-byte row limit cannot take.
        """
        source_table = "stocks_basic_financials_bank_fa"

        source_types = self._helper_column_types(SILVER_SCHEMA, source_table)
        if not source_types:
            raise MissingSourceDataError(
                f"`{SILVER_SCHEMA}.{source_table}` does not exist — build the silver "
                f"`stocks_financials` leaf first "
                f"(_ingest_silver_stocks_basic_financials_bank then …_fa)."
            )

        # Pass-through = the source's own text/date columns beyond the keys and the
        # GICS block: `publish_date` and the nine per-report provenance columns.
        passthrough = [
            col
            for col, sql_type in source_types.items()
            if sql_type in ("character varying", "text", "date")
            and col not in ("exchange", "ticker", "date")
        ]
        # Exact = every numeric column CARRIED from silver (the ~200 statement lines
        # and the fundamental indicators). Feature columns are not in `source_types`,
        # so they keep gold's default REAL.
        exact = [
            col
            for col, sql_type in source_types.items()
            if sql_type in ("numeric", "double precision", "real", "bigint", "integer")
        ]
        # `_helper_adjust_ohlc` renames the three raw legs and mints `close`; all four
        # are carried values, so they keep the exact type too.
        exact += ["open_raw", "high_raw", "low_raw", "close"]

        self._ingest_gold_table(
            "stocks_financials_bank_fa",
            silver_table_name=source_table,
            ta_layers=self._helper_stock_ta_layers(volume_col="volume_matched"),
            passthrough_cols=passthrough,
            exact_float_cols=exact,
            prepare_fn=self._helper_adjust_ohlc,
        )

    def _ingest_gold_news_weekly_panel(self) -> None:
        """Gold `news_weekly_panel` — one row per `(exchange, ticker, week)`, PK the same.

        The MINIMAL panel: news COUNTS and event counts, **no sentiment**. It exists so
        the costed walk-forward can be run on `if_news`/`n_docs` alone before any NLP work
        (`experiment/experiment_10/guidance.md` §8, steps 5-6) — that run is the baseline
        a sentiment block would later have to beat, and it is cheap enough to answer the
        question before the expensive part starts.

        ⚠️ **Weekly, not daily, and neither reason is a taste call.** Paper 57 measures
        daily news predicting 1-2 days (Day 3 t=1.2, gone) against weekly news predicting
        13 weeks; and on this corpus editorials cover **1.6% of ticker-DAYS** but **8.7%
        of ticker-WEEKS** (top-30: 12.2% → 51.7%). A daily feature is not weak, it is
        absent.

        ⚠️ **The spine is PRICE, not news.** Every week a stock traded gets a row, with
        `if_news = 0` where nothing was published. Paper 28 dropped no-news rows and broke
        series continuity; and `if_news` is itself the effect paper 57 measures (covered
        stocks beat uncovered ones by 2.24%/week in small caps, regardless of tone).

        Both GROUP BYs run in SQL: `silver.stocks_basic` is ~2.4M rows and pulling it into
        pandas whole has stalled runs before. The news read is column-scoped for the same
        reason — `content_clean` is ~300 MB of text this step never looks at.

        The old table is dropped first, as every gold ingest must: the COPY writer assumes
        an empty table, so a second materialisation would otherwise die on the PK.
        """
        from sentiment.news_panel import (
            aggregate_news_weekly,
            build_news_weekly_panel,
            grain_violations,
        )

        self._logger.log_info(
            "Ingesting gold news_weekly_panel (per ticker-week news counts)..."
        )
        out_table = "news_weekly_panel"

        # ── price spine, aggregated server-side ──────────────────────────────────────
        with self._database_driver._cursor_ctx() as cur:
            cur.execute(
                f"""
                SELECT exchange, ticker,
                       date_trunc('week', date)::date            AS week_start,
                       COUNT(*)                                  AS sessions,
                       SUM(COALESCE(value_matched, 0))           AS value_w,
                       (ARRAY_AGG(close_adjust ORDER BY date))[1]      AS close_first,
                       (ARRAY_AGG(close_adjust ORDER BY date DESC))[1] AS close_last
                FROM {SILVER_SCHEMA}.stocks_basic
                WHERE close_adjust IS NOT NULL
                GROUP BY 1, 2, 3
                """
            )
            price_weekly = pd.DataFrame(
                cur.fetchall(),
                columns=[
                    "exchange", "ticker", "week_start",
                    "sessions", "value_w", "close_first", "close_last",
                ],
            )
        if price_weekly.empty:
            raise MissingSourceDataError(
                f"{SILVER_SCHEMA}.stocks_basic produced no weekly rows - build it first "
                f"(--select silver/stocks_basic)."
            )

        news = self._helper_select(
            schema_name=SILVER_SCHEMA,
            table_name="cafef_news",
            columns=[
                "row_id", "exchange", "ticker", "trading_date",
                "type", "category", "is_editorial", "has_ticker", "relevance_score",
            ],
        )
        if news.empty:
            raise MissingSourceDataError(
                f"{SILVER_SCHEMA}.cafef_news is empty - build it first "
                f"(--select silver/cafef_news)."
            )

        news_weekly = aggregate_news_weekly(news)
        result = build_news_weekly_panel(price_weekly, news_weekly)

        # ⚠️ The grain invariant, asserted. A feature build adds COLUMNS, never ROWS.
        duplicates = grain_violations(result)
        if duplicates:
            raise PipelineError(
                f"{GOLD_SCHEMA}.{out_table}: {duplicates} duplicate "
                f"(exchange, ticker, week_start) rows - the news merge fanned out."
            )
        # The join must preserve the spine exactly — bar the PANEL_START cut, which is
        # the one place rows are deliberately removed (a six-month news hole in 2012).
        from sentiment.news_panel import PANEL_START

        spine_in_range = int(
            (pd.to_datetime(price_weekly["week_start"]) >= pd.Timestamp(PANEL_START)).sum()
        )
        if len(result) != spine_in_range:
            raise PipelineError(
                f"{GOLD_SCHEMA}.{out_table}: {len(result)} rows against a price spine of "
                f"{spine_in_range} from {PANEL_START} - the join must preserve the spine."
            )

        self._logger.log_info(
            f"news_weekly_panel: {len(result)} ticker-weeks, "
            f"{int(result['if_news'].sum())} with news "
            f"({result['if_news'].mean():.1%})"
        )

        result = self._helper_clean(
            result,
            [
                CleanLayer.ORDER_BY(["exchange", "ticker", "week_start"]),
            ],
        )
        result = self._helper_cast_columns(
            result,
            decimal_cols=[
                "value_w", "close_first", "close_last", "relevance_max",
                "ret_w", "log_value_w",
                *[f"mom_{w}w" for w in (1, 4, 12, 26)],
            ],
            bigint_cols=[
                "sessions", "n_docs", "n_days", "n_editorial", "n_disclosure",
                "n_docs_named", "n_earnings", "n_insider_txn", "n_dividend",
                "n_personnel", "n_capital", "n_uncategorized",
                "if_news", "if_editorial", "if_earnings_week",
            ],
        )

        self._database_driver.drop_table(GOLD_SCHEMA, out_table)
        self._helper_save_pandas_table_to_database(
            schema_name=GOLD_SCHEMA,
            table_name=out_table,
            primary_keys=["exchange", "ticker", "week_start"],
            df=result,
            dtype_overrides={"week_start": DataType.DATE()},
        )

    def _ingest_gold_news_daily_panel(self) -> None:
        """Gold `news_daily_panel` — one row per `(exchange, ticker, date)`, PK the same.

        The DAILY-formation twin of `news_weekly_panel`, built because the thesis target
        is `rel5`/`rel10` (experiment_3.3) — 5 and 10 TRADING DAYS — and weekly formation
        answers a slightly different question: it rebalances once a week, where `rel5`
        forms every session.

        News enters as **trailing 5- and 10-session windows**, matched to the horizons.

        ⚠️ **The trailing window includes the formation day, and that is not a leak.** An
        article's `trading_date` is the first session whose OPEN follows it, so news dated
        `d` is public before `d`'s close, which is where the position forms.

        ⚠️ **Daily formation OVERLAPS**: consecutive rows share `h − 1` days of forward
        path (paper 9's flaw #3, the most likely source of its 75-80% figures). Harmless
        for training, fatal for a random split — which is why the folds purge and embargo
        `h` sessions around every cut. It also means the row count is ~5× the weekly
        panel's without 5× the information; read the per-fold spread, not the n.

        The price read is column-scoped (5 of 38) rather than a `_helper_select` on the
        whole 2.4M-row table, which has stalled runs before. Old table dropped first.
        """
        from sentiment.news_panel import (
            PANEL_START,
            aggregate_news_daily,
            build_news_daily_panel,
            grain_violations,
        )

        self._logger.log_info(
            "Ingesting gold news_daily_panel (trailing 5/10-session news windows)..."
        )
        out_table = "news_daily_panel"

        with self._database_driver._cursor_ctx() as cur:
            cur.execute(
                f"""
                SELECT exchange, ticker, date, close_adjust, value_matched
                FROM {SILVER_SCHEMA}.stocks_basic
                WHERE close_adjust IS NOT NULL
                ORDER BY exchange, ticker, date
                """
            )
            price_daily = pd.DataFrame(
                cur.fetchall(),
                columns=["exchange", "ticker", "date", "close_adjust", "value_matched"],
            )
        if price_daily.empty:
            raise MissingSourceDataError(
                f"{SILVER_SCHEMA}.stocks_basic is empty (--select silver/stocks_basic)."
            )

        news = self._helper_select(
            schema_name=SILVER_SCHEMA,
            table_name="cafef_news",
            columns=[
                "row_id", "exchange", "ticker", "trading_date",
                "category", "is_editorial", "has_ticker", "relevance_score",
            ],
        )
        if news.empty:
            raise MissingSourceDataError(
                f"{SILVER_SCHEMA}.cafef_news is empty (--select silver/cafef_news)."
            )

        result = build_news_daily_panel(price_daily, aggregate_news_daily(news))

        duplicates = grain_violations(result, key="date")
        if duplicates:
            raise PipelineError(
                f"{GOLD_SCHEMA}.{out_table}: {duplicates} duplicate "
                f"(exchange, ticker, date) rows - the news merge fanned out."
            )
        spine_in_range = int(
            (pd.to_datetime(price_daily["date"]) >= pd.Timestamp(PANEL_START)).sum()
        )
        if len(result) != spine_in_range:
            raise PipelineError(
                f"{GOLD_SCHEMA}.{out_table}: {len(result)} rows against a price spine of "
                f"{spine_in_range} from {PANEL_START} - the join must preserve the spine."
            )

        self._logger.log_info(
            f"news_daily_panel: {len(result)} stock-days, "
            f"{int(result['if_news_5d'].sum())} with news in the trailing week "
            f"({result['if_news_5d'].mean():.1%})"
        )

        result = self._helper_clean(
            result, [CleanLayer.ORDER_BY(["exchange", "ticker", "date"])]
        )
        numeric = [c for c in result.columns if c not in ("exchange", "ticker", "date")]
        result = self._helper_cast_columns(
            result,
            decimal_cols=[c for c in numeric if not c.startswith(("if_", "n_"))],
            bigint_cols=[c for c in numeric if c.startswith(("if_", "n_"))],
        )

        self._database_driver.drop_table(GOLD_SCHEMA, out_table)
        self._helper_save_pandas_table_to_database(
            schema_name=GOLD_SCHEMA,
            table_name=out_table,
            primary_keys=["exchange", "ticker", "date"],
            df=result,
            dtype_overrides={"date": DataType.DATE()},
        )

    def _ingest_gold_bonds(self) -> None:
        self._ingest_gold_table("bonds")

    def _ingest_gold_forex(self) -> None:
        self._ingest_gold_table("forex")

    def _ingest_gold_funds(self) -> None:
        self._ingest_gold_table("funds")

    # ⚠️ `_ingest_gold_indices` was REMOVED (2026-08-01) along with its
    # `data_quality_gold/indices` leaf and switch key. `gold.indices` was the
    # TradingView index series (24,095 x 22, the generic single-series feature build)
    # and it duplicated `gold.stock_market`, which covers the same six Vietnamese
    # indices from CafeF with 27 measures apiece instead of OHLCV. `silver.indices` and
    # `bronze.trading_view_indices` are untouched — only the gold table is retired, so
    # nothing upstream loses its history and restoring it is one line. The
    # `gold_schema.indices` table was dropped the same day, so the schema matches the
    # code.

    # ── UNIFIED — the per-ticker modelling schema ───────────────────────────────
    #
    # `unified_schema_<ticker>` is the fourth layer, and it is not a fourth copy of the
    # pipeline: it is ONE TICKER's slice, cut into the FEATURE GROUPS a model consumes
    # (`pool__basic`, `pool__ta`, `pool__macro`, `pool__calendar`, `pool__targets`), so
    # a feature-selection run can be scoped to a group. Built until now only by
    # `train_test_creator/unified_schema_creator.ipynb`.
    #
    # ⚠️ The schema name embeds the ticker, so the ticker is an IDENTIFIER, not a value —
    # it cannot be parameterised into the SQL and must be validated instead. Hence
    # `_helper_unified_schema`.

    # A ticker that reaches an identifier position must match this exactly. CafeF/HOSE
    # tickers are 3 letters today, but the pattern allows what PostgreSQL allows so a
    # future index or foreign listing does not need a code change — what it does NOT
    # allow is anything that could close the identifier and continue the statement.
    UNIFIED_TICKER_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,20}$")

    # ⚠️ THE ONE TICKER THAT IS NOT A TICKER. `"ALL"` names the WHOLE UNIVERSE, and
    # `unified_schema_all` therefore holds every ticker of `silver.stocks_basic`
    # rather than one company's slice.
    #
    # Why a sentinel and not a separate pair of methods: the two builders differ by
    # a `WHERE` clause and a `PARTITION BY`, and nothing else. Forking them would
    # give the label definition two homes — the exact drift
    # `UNIFIED_TARGET_HORIZONS` exists to prevent — and the cross-sectional schema
    # would silently stop matching the single-ticker one it is compared against.
    #
    # ⚠️ It is safe as an identifier by the same rule everything else here is:
    # `"ALL"` matches `UNIFIED_TICKER_PATTERN`, so the schema name is validated on
    # the same path. And it cannot collide with a real listing — no VN ticker is
    # `ALL` (checked: `silver.stocks_basic` holds none), and if one ever lists, the
    # universe build would notice because its assertions count series, not rows.
    UNIFIED_UNIVERSE = "ALL"

    # ⚠️ THE SECOND SENTINEL — a GICS SECTOR SLICE. `unified_schema_bank` holds every
    # company GICS files under industry `401010` (Financials → Banks → Banks), which
    # is 20 tickers today.
    #
    # Why a sentinel and not a ticker list: the same reason `"ALL"` is one. A list
    # would have to be maintained against the taxonomy, and the day a bank lists or
    # is reclassified the schema would quietly stop matching its own name. The
    # predicate reads the classification that `silver.stocks_basic` already carries,
    # so membership is DERIVED and a rebuild tracks GICS by construction.
    #
    # ⚠️ `industry_code`, NOT `sub_industry_code`. `401010` covers `40101010`
    # (diversified banks) and `40101015` (regional banks); today every VN bank is
    # diversified, and pinning the sub-industry would silently drop the first name
    # reclassified as regional.
    UNIFIED_BANK = "BANK"

    # ⚠️ EVERY SENTINEL THAT NAMES A SET OF COMPANIES RATHER THAN ONE, mapped to the
    # `silver.stocks_basic` predicate selecting its members. `None` = no predicate at
    # all, i.e. the whole universe.
    #
    # ⚠️ Membership is a `pool__basic` concern ONLY, and that is not an omission.
    # `pool__targets` reads `pool__basic` and counts the series it finds there;
    # `pool__ta` / `pool__fa` INNER JOIN the source to `pool__basic` on the whole key.
    # Both therefore inherit the filter from the spine, so a new sentinel needs one
    # entry here and no change anywhere else — which is what stops the three builders
    # from drifting apart the way a forked universe method would.
    UNIFIED_MEMBER_FILTERS = {
        UNIFIED_UNIVERSE: (None, ()),
        UNIFIED_BANK: ("industry_code = %s", ("401010",)),
    }

    def _helper_unified_is_universe(self, ticker: str) -> bool:
        """Does this name a SET of companies rather than one?

        ⚠️ True for `"ALL"` and for every sector sentinel, because what every caller
        actually asks is "may this schema hold more than one series" — not "is this
        the whole market". A sentinel that answered False here would be filtered by
        `WHERE ticker = 'BANK'` and produce a real, empty, correctly-typed table.
        """
        return (ticker or "").upper() in self.UNIFIED_MEMBER_FILTERS

    def _helper_unified_member_filter(self, ticker: str) -> Tuple[str, tuple]:
        """`(predicate, params)` over `silver.stocks_basic` for a ticker or sentinel.

        A bare boolean expression with no `WHERE`/`AND`, so the caller decides how to
        attach it; `("", ())` means "select everything".

        ⚠️ The predicate is PARAMETERISED even though the sentinel that chose it is
        interpolated into a schema name. Those are two different trust boundaries: a
        schema name cannot be bound and is validated against
        `UNIFIED_TICKER_PATTERN` instead, while a GICS code is an ordinary value and
        has no business being interpolated.
        """
        key = (ticker or "").upper()
        if key in self.UNIFIED_MEMBER_FILTERS:
            predicate, params = self.UNIFIED_MEMBER_FILTERS[key]
            return (predicate or "", params)
        return ("ticker = %s", (ticker,))

    def _helper_unified_schema(self, ticker: str) -> str:
        """`unified_schema_vcb` for `VCB`. Raises if the ticker is not identifier-safe.

        ⚠️ **This validation is the only thing between a ticker and arbitrary SQL.**
        Every other user-supplied value in this class reaches the database as a bound
        parameter; a schema NAME cannot be bound, so it is interpolated — and a ticker
        arrives from a CSV, a config or a Dagster partition key, none of which this
        class controls.
        """
        if not self.UNIFIED_TICKER_PATTERN.match(ticker or ""):
            raise PipelineError(
                f"Ticker {ticker!r} is not a valid SQL identifier and cannot name a "
                f"schema. Expected e.g. 'VCB' — letters, digits and underscores only, "
                f"starting with a letter."
            )
        return f"{UNIFIED_SCHEMA}_{ticker.lower()}"

    # ⚠️ THE KEY OF EVERY `unified_schema_<ticker>` TABLE, IN THIS ORDER.
    #
    # Every `pool__*` table carries all three columns and is keyed on all three, even
    # where a table holds one company and two of them never vary. Three reasons, and
    # the third is the one that made it worth changing:
    #
    # 1. **A join needs no special cases.** `pool__targets` used to be keyed on `date`
    #    alone, so joining it to `pool__basic` meant intersecting key sets and hoping
    #    the result was right. Now every pool joins to every other pool on the same
    #    three columns.
    # 2. **`date` FIRST is deliberate.** Every access pattern here is time-ordered —
    #    walk-forward folds, purge gaps, as-of joins — and a leading `date` lets the
    #    PK's own index serve a range scan. `(exchange, ticker, date)` cannot, because
    #    a b-tree can only range-scan on its leading column.
    # 3. **The cross-sectional panel is the point.** A multi-ticker pool is keyed
    #    `(date, exchange, ticker)` by necessity, and a cross-sectional model groups by
    #    date. Keying the single-ticker pools the same way means the move is a wider
    #    table, not a different schema convention — and `COUNT(DISTINCT ticker) = 1`
    #    stops being a structural assumption and becomes the assertion it always was.
    UNIFIED_PRIMARY_KEY = ("date", "exchange", "ticker")

    def _helper_unified_primary_key(self, cur, schema: str, table: str) -> None:
        """Add `UNIFIED_PRIMARY_KEY` to a freshly-created unified table, and verify it.

        ⚠️ Verified rather than assumed, because `ADD PRIMARY KEY` succeeds on any
        column ORDER — `(exchange, ticker, date)` and `(date, exchange, ticker)` are
        both valid keys over the same three columns and differ only in which queries
        their index can serve. A silently-reordered key would be invisible until
        someone measured a scan.
        """
        columns = ", ".join(self.UNIFIED_PRIMARY_KEY)
        cur.execute(f"ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({columns})")
        cur.execute(
            """SELECT a.attname
               FROM pg_index i
               JOIN pg_class c ON c.oid = i.indrelid
               JOIN pg_namespace n ON n.oid = c.relnamespace
               JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
               JOIN pg_attribute a
                 ON a.attrelid = c.oid AND a.attnum = k.attnum
               WHERE i.indisprimary AND n.nspname = %s AND c.relname = %s
               ORDER BY k.ord""",
            (schema, table),
        )
        actual = tuple(row[0] for row in cur.fetchall())
        if actual != self.UNIFIED_PRIMARY_KEY:
            raise PipelineError(
                f"{schema}.{table} primary key is {actual}, expected "
                f"{self.UNIFIED_PRIMARY_KEY} — column ORDER is part of the contract."
            )

    def _ingest_unified_pool_basic(self, ticker: str) -> None:
        """`silver.stocks_basic` (one ticker) → `unified_schema_<ticker>.pool__basic`.

        **Every column of `silver.stocks_basic`, with silver's own types**, PK
        `(date, exchange, ticker)` — see `UNIFIED_PRIMARY_KEY` for why in that order.

        ⚠️ **`CREATE TABLE AS`, not a pandas round-trip, and the reason is type
        fidelity.** psycopg2 hands a PostgreSQL `numeric` back as a Python `Decimal`,
        which lands in a DataFrame as dtype `object` — and `_helper_infer_sql_type`
        maps `object` to VARCHAR. Reading this table out and writing it back would
        therefore turn every price and value column into TEXT while looking like it
        worked: exactly the "degraded VARCHAR" the silver carry-ups suffer from (§4).
        A server-side CTAS never materialises a Python value at all, so `numeric` stays
        `numeric` and `bigint` stays `bigint`, and it never holds 4k rows in memory.

        ⚠️ **The schema is created if absent and the table is REPLACED**, so this is
        re-runnable — which is what an orchestrated asset needs. It is scoped to the one
        table: sibling `pool__*` tables in the same schema are left alone.

        ⚠️ **`ticker=UNIFIED_UNIVERSE` ("ALL") drops the filter entirely** and copies
        the whole of `silver.stocks_basic` into `unified_schema_all.pool__basic` — the
        cross-sectional panel. Same columns, same types, same key; only the row count
        and `COUNT(DISTINCT ticker)` differ. That is the point: a cross-sectional run
        must read the same table shape a single-ticker run does, or the two are not
        comparable.
        """
        schema = self._helper_unified_schema(ticker)
        universe = self._helper_unified_is_universe(ticker)
        predicate, params = self._helper_unified_member_filter(ticker)
        # What was selected, for the log and for every error message below — one
        # phrase, so the assertion that fails names the same scope the ingest logged.
        scope = (
            "the whole universe"
            if not predicate
            else f"{predicate.replace('%s', repr(*params))}"
        )
        self._logger.log_info(
            f"Ingesting unified {schema}.pool__basic "
            f"(from {SILVER_SCHEMA}.stocks_basic, {scope})..."
        )

        # The source must exist AND hold this ticker — an empty result would otherwise
        # create a real, empty, correctly-typed table, which is the failure that looks
        # most like success.
        source_types = self._helper_column_types(SILVER_SCHEMA, "stocks_basic")
        if not source_types:
            raise MissingSourceDataError(
                f"`{SILVER_SCHEMA}.stocks_basic` does not exist — build the silver "
                f"`stocks_basic` leaf first."
            )

        self._database_driver.create_schema(schema)

        # One `WHERE`, built once and reused by the count and the CTAS, so the row the
        # assertion counts and the row the table gets can never come from different
        # predicates.
        where = f" WHERE {predicate}" if predicate else ""

        with self._database_driver._cursor_ctx() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {SILVER_SCHEMA}.stocks_basic{where}", params
            )
            available = int(cur.fetchone()[0])
            if not available:
                raise MissingSourceDataError(
                    f"`{SILVER_SCHEMA}.stocks_basic` holds no rows for {scope}, so "
                    f"{schema}.pool__basic would be empty. Check the ticker or the "
                    f"classification, or rebuild silver `stocks_basic`."
                )

            # Dropped as late as possible, so a failure above leaves the old table intact
            # — the same ordering `_ingest_gold_table` uses.
            cur.execute(f"DROP TABLE IF EXISTS {schema}.pool__basic")
            cur.execute(
                f"CREATE TABLE {schema}.pool__basic AS "
                f"SELECT * FROM {SILVER_SCHEMA}.stocks_basic{where}",
                params,
            )
            # CTAS copies types but never constraints, so the grain is asserted here.
            self._helper_unified_primary_key(cur, schema, "pool__basic")
            cur.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT ticker) FROM {schema}.pool__basic"
            )
            written, series = (int(x) for x in cur.fetchone())

        if written != available:
            raise PipelineError(
                f"{schema}.pool__basic wrote {written} rows but silver holds "
                f"{available} for {scope}."
            )
        # ⚠️ The single-ticker contract is asserted HERE too, not only in the Dagster
        # asset. `unified_schema_vcb` holding two companies would break every
        # `COUNT(DISTINCT ticker) = 1` assumption downstream, and a notebook calling
        # this method directly has no asset to catch it.
        if not universe and series != 1:
            raise PipelineError(
                f"{schema}.pool__basic holds {series} tickers — a single-ticker "
                f"unified schema is one company by definition."
            )
        # ⚠️ And the MIRROR of it for a sector sentinel. A classification predicate
        # that matched one company would build a schema whose name promises a
        # cross-section and whose contents are a single time series — the §9h failure
        # mode, arrived at silently. `"ALL"` is exempt only because it has no
        # predicate to be wrong about.
        if predicate and series < 2:
            raise PipelineError(
                f"{schema}.pool__basic holds {series} ticker(s) for {scope} — a "
                f"sector schema is a CROSS-SECTION, and one company is not one. "
                f"Check the classification in `{SILVER_SCHEMA}.stocks_basic`."
            )
        self._logger.log_info(
            f"{schema}.pool__basic: {written} rows x {len(source_types)} columns, "
            f"{series} ticker(s)."
        )

    # The label horizons, in TRADING DAYS — `pool__basic` is one row per session, so a
    # row offset IS a trading-day offset and no calendar arithmetic is involved.
    # ⚠️ Each target COLUMN NAME is derived from its horizon (`return_5day`), so the two
    # cannot drift: changing a horizon renames the column instead of silently
    # re-defining it, and ADDING one adds a column rather than replacing the table's
    # meaning. That is why this is a tuple and not the scalar it started as — a model
    # comparing h=5 against h=10 needs both labels on one calendar, and deriving the
    # second one anywhere else would put the label definition in two places.
    UNIFIED_TARGET_HORIZONS = (5, 10)

    # The market benchmark for the RELATIVE targets, `return_rel_{h}day`.
    #
    # ⚠️ `gold_schema.stock_market`, NOT the retired `gold.indices` — the old
    # notebook's `return_rel_5day` read `gold.indices`, which was dropped on
    # 2026-08-01 (see `orchestration/CONTEXT.md` §"Gold housekeeping"), so that
    # column could not be rebuilt as written. This is its replacement.
    #
    # ⚠️ Why a relative target exists at all: a single stock's ABSOLUTE forward
    # return is dominated by the market factor, which no company-level feature
    # predicts. Subtracting the index leaves the part a stock-specific feature could
    # plausibly explain. See memory `project-cross-sectional-strategy`.
    UNIFIED_BENCHMARK_TABLE = "gold_schema.stock_market"
    UNIFIED_BENCHMARK_COLUMN = "hose__vnindex__close_adjust"

    def _ingest_unified_pool_targets(self, ticker: str) -> None:
        """`unified_schema_<ticker>.pool__basic` → `…​.pool__targets`.

        `UNIFIED_PRIMARY_KEY` — `(date, exchange, ticker)` — plus **two columns per
        horizon in `UNIFIED_TARGET_HORIZONS`**: `return_{h}day`, the forward simple
        return `close[t+h] / close[t] - 1` on the SPLIT-ADJUSTED close, and
        `return_rel_{h}day`, the same minus the benchmark's return over that window.

        ⚠️ **`exchange` and `ticker` are carried even though they never vary here.**
        This table was keyed on `date` alone, which made every join to it a special
        case — the reader had to intersect key sets and hope. Every pool now shares one
        key.

        ⚠️ **The source is `pool__basic`, not `gold.stocks`, and that is what keeps the
        two tables joinable.** They are joined on `date` by every downstream selection
        table, so a target built from a different source would silently contribute its
        own calendar — which is exactly what happened before: the dropped `pool__targets`
        had 4,242 rows against silver's 4,235 because it came from a `gold.stocks` that
        ran one session longer.

        ⚠️ **`close_adjust`, not `close_raw`.** A return computed on the raw close reads
        every split and stock dividend as a real overnight loss — VCB's 2009-06-30 raw
        close is 60,000 against an adjusted 9,130. That is not a label, it is a corporate
        action.

        ⚠️ **Each column's tail is NULL for exactly its own horizon, and that is
        correct.** `return_5day` loses 5 rows, `return_10day` loses 10 — their futures do
        not exist yet. Drop them when fitting, per target; keeping them means the table
        still joins cleanly against the feature pools, which is what the notebook's
        `<target>__final` views rely on. ⚠️ It also means **the two labels do not have
        the same usable range**: a run comparing h=5 against h=10 must drop each
        target's own tail, not a shared one, or the h=5 comparison silently loses 5
        sessions it had every right to use.

        ⚠️ **No look-ahead is introduced by the LEAD itself** — a forward-looking LABEL is
        the point of a target table. The look-ahead that matters is a FEATURE that peeks,
        and nothing here feeds a feature.

        ⚠️ **THE LEAD IS PARTITIONED BY `(exchange, ticker)`, ALWAYS.** On a
        single-ticker pool that partition is a no-op; on `unified_schema_all` it is the
        difference between a label and garbage — an unpartitioned `LEAD` would hand the
        last row of AAA the first row of AAM as its future price. Writing it once, for
        both, is why there is one method here and not two: a universe-only variant would
        be the copy that drifts.

        ⚠️ **The unlabelled tail is `h` rows PER SERIES**, not `h` rows in total, so the
        assertion counts series. Every series must be longer than the longest horizon or
        it would be entirely unlabelled; that is checked rather than assumed.
        """
        schema = self._helper_unified_schema(ticker)
        universe = self._helper_unified_is_universe(ticker)
        horizons = tuple(self.UNIFIED_TARGET_HORIZONS)
        if not horizons or any(h < 1 for h in horizons):
            raise PipelineError(
                f"UNIFIED_TARGET_HORIZONS must be one or more positive trading-day "
                f"offsets, got {horizons}."
            )
        target_cols = {h: f"return_{h}day" for h in horizons}
        relative_cols = {h: f"return_rel_{h}day" for h in horizons}
        longest = max(horizons)
        self._logger.log_info(
            f"Ingesting unified {schema}.pool__targets (from {schema}.pool__basic)..."
        )

        source_types = self._helper_column_types(schema, "pool__basic")
        if not source_types:
            raise MissingSourceDataError(
                f"`{schema}.pool__basic` does not exist — build it first "
                f"(`_ingest_unified_pool_basic`)."
            )
        if "close_adjust" not in source_types:
            raise MissingSourceDataError(
                f"`{schema}.pool__basic` has no `close_adjust` column, so a return "
                f"cannot be computed on an adjusted price. Present: "
                f"{sorted(source_types)[:20]}…"
            )

        with self._database_driver._cursor_ctx() as cur:
            # ⚠️ Row count, SERIES count and the SHORTEST series in one pass. On a
            # universe pool the binding constraint is the shortest series, not the
            # total: a freshly-listed ticker with 3 sessions would be entirely
            # unlabelled and would quietly shift every tail assertion below.
            cur.execute(
                f"SELECT COUNT(*), MIN(rows) "
                f"FROM (SELECT COUNT(*) AS rows FROM {schema}.pool__basic "
                f"      GROUP BY exchange, ticker) s"
            )
            series, shortest = (int(x) for x in cur.fetchone())
            cur.execute(f"SELECT COUNT(*) FROM {schema}.pool__basic")
            available = int(cur.fetchone()[0])
            if available <= longest:
                raise MissingSourceDataError(
                    f"`{schema}.pool__basic` holds {available} rows, which is not more "
                    f"than the longest {longest}-day horizon — every label would be "
                    f"NULL."
                )
            if shortest <= longest:
                raise MissingSourceDataError(
                    f"`{schema}.pool__basic` has a series with only {shortest} rows, "
                    f"which is not more than the longest {longest}-day horizon — that "
                    f"series would be entirely unlabelled. Filter the universe before "
                    f"building the pool."
                )

            # How many pool__basic SESSIONS (distinct dates) have no benchmark close. A
            # gap there propagates into `return_rel_*` and is counted below rather than
            # absorbed into a loosened assertion.
            #
            # ⚠️ Counted as DATES, not rows: on a universe pool one missing index close
            # costs every series in the cross-section, so a row count would be the same
            # number multiplied by the width and the bound would stop meaning anything.
            cur.execute(
                f"SELECT COUNT(DISTINCT b.date) FROM {schema}.pool__basic b "
                f"LEFT JOIN {self.UNIFIED_BENCHMARK_TABLE} m ON m.date = b.date "
                f"WHERE m.{self.UNIFIED_BENCHMARK_COLUMN} IS NULL"
            )
            benchmark_gaps = int(cur.fetchone()[0])

            # One LEAD per horizon over the same ORDER BY, so every label is computed
            # from the same row ordering and no two of them can disagree about which
            # session follows which.
            #
            # ⚠️ The relative return is `P[t+h]/P[t] − B[t+h]/B[t]`, which is the
            # simple stock return MINUS the simple benchmark return (the −1 terms
            # cancel). Written this way rather than as a difference of two ratios
            # minus one so that the cancellation is visible instead of assumed.
            columns = ", ".join(
                [
                    f"(LEAD(px, {h}) OVER w / NULLIF(px, 0) - 1.0)"
                    f"::double precision AS {col}"
                    for h, col in target_cols.items()
                ]
                + [
                    f"(LEAD(px, {h}) OVER w / NULLIF(px, 0)"
                    f" - LEAD(bm, {h}) OVER w / NULLIF(bm, 0))"
                    f"::double precision AS {col}"
                    for h, col in relative_cols.items()
                ]
            )
            # Dropped as late as possible: a failure above leaves the old table intact.
            #
            # ⚠️ `exchange` and `ticker` are carried through from `pool__basic` even
            # where they never vary. They are what make this table join to every other
            # pool on `UNIFIED_PRIMARY_KEY` instead of on `date` alone — and on the
            # universe pool they are also the PARTITION the LEAD runs over.
            #
            # ⚠️ `PARTITION BY exchange, ticker` is not optional and not universe-only.
            # Without it the LEAD walks off the end of one company's history into the
            # next one's, and the label at each series boundary becomes another
            # company's price. On a single-ticker pool the partition is a no-op, so the
            # same statement is correct for both and there is no second code path to
            # keep in step.
            cur.execute(f"DROP TABLE IF EXISTS {schema}.pool__targets")
            cur.execute(
                f"CREATE TABLE {schema}.pool__targets AS "
                f"WITH joined AS ("
                f"  SELECT b.date, b.exchange, b.ticker, b.close_adjust AS px, "
                f"         m.{self.UNIFIED_BENCHMARK_COLUMN} AS bm "
                f"  FROM {schema}.pool__basic b "
                f"  LEFT JOIN {self.UNIFIED_BENCHMARK_TABLE} m ON m.date = b.date"
                f") "
                f"SELECT date, exchange, ticker, {columns} FROM joined "
                f"WINDOW w AS (PARTITION BY exchange, ticker ORDER BY date)"
            )
            self._helper_unified_primary_key(cur, schema, "pool__targets")

            counts = ", ".join(
                f"COUNT({col})"
                for col in list(target_cols.values()) + list(relative_cols.values())
            )
            cur.execute(f"SELECT COUNT(*), {counts} FROM {schema}.pool__targets")
            row = [int(x) for x in cur.fetchone()]
            written = row[0]
            n = len(horizons)
            labelled = dict(zip(horizons, row[1 : 1 + n]))
            labelled_rel = dict(zip(horizons, row[1 + n :]))

        if written != available:
            raise PipelineError(
                f"{schema}.pool__targets wrote {written} rows but pool__basic holds "
                f"{available} — the two must share one calendar."
            )
        # ⚠️ Checked PER COLUMN, and the expected tail is that column's OWN horizon
        # TIMES THE NUMBER OF SERIES. Every partition loses its own last `h` rows, so
        # on the universe pool the expected NULL count is `h × series`, not `h`. A
        # check against `h` alone would pass only on a single-ticker schema and would
        # have failed the universe build for being correct.
        for h, col in target_cols.items():
            unlabelled = written - labelled[h]
            expected_tail = h * series
            if unlabelled != expected_tail:
                raise PipelineError(
                    f"{schema}.pool__targets has {unlabelled} NULL {col} values; "
                    f"exactly {expected_tail} ({h} per series × {series} series, the "
                    f"tail with no future) were expected. Check "
                    f"`pool__basic.close_adjust` for NULLs or zeros."
                )
        # ⚠️ The relative column loses its own tail PLUS every row a benchmark gap
        # touches: a missing `B[t]` kills row `t`, and a missing `B[t+h]` kills row
        # `t-h`. One gap DATE therefore costs up to `h + 1` rows IN EVERY SERIES. The
        # bound is asserted rather than the exact count, because two gaps within `h` of
        # each other overlap — but it still fails loudly if the benchmark is broadly
        # absent instead of merely pitted.
        for h, col in relative_cols.items():
            unlabelled = written - labelled_rel[h]
            floor = h * series
            allowed = floor + benchmark_gaps * (h + 1) * series
            if not floor <= unlabelled <= allowed:
                raise PipelineError(
                    f"{schema}.pool__targets has {unlabelled} NULL {col} values; "
                    f"between {floor} and {allowed} were expected ({h} tail × "
                    f"{series} series + at most {h + 1} per series per each of "
                    f"{benchmark_gaps} benchmark gap date(s) in "
                    f"{self.UNIFIED_BENCHMARK_TABLE}.{self.UNIFIED_BENCHMARK_COLUMN})."
                )
        summary = "; ".join(
            f"{target_cols[h]} {labelled[h]} labelled / {written - labelled[h]} null, "
            f"{relative_cols[h]} {labelled_rel[h]} / {written - labelled_rel[h]} null"
            for h in horizons
        )
        self._logger.log_info(
            f"{schema}.pool__targets: {written} rows over {series} series, "
            f"{benchmark_gaps} benchmark gap date(s) — {summary}."
        )

    # ── UNIFIED — the other two feature pools ───────────────────────────────────
    #
    # `pool__basic` is the price/flow panel and `pool__targets` the labels. These two
    # are the remaining groups a model consumes: the technical block and the
    # fundamental one. Both follow `pool__basic`'s contract exactly — CTAS for type
    # fidelity, PK `(date, exchange, ticker)`, re-runnable, schema created if absent.

    UNIFIED_TA_SOURCE = f"{GOLD_SCHEMA}.stocks_ta"
    UNIFIED_FA_SOURCE = f"{GOLD_SCHEMA}.stocks_financials_bank_fa"

    # ⚠️ COLUMNS THE FEATURE POOLS MUST NOT RE-CARRY. Identity and taxonomy belong to
    # `pool__basic`; duplicating them here would make every join produce `_x`/`_y`
    # pairs, and `UnifiedSchemaReader.join` would silently drop one copy. The keys
    # themselves are added back explicitly — they are what the pools join ON.
    UNIFIED_POOL_IDENTITY = (
        "sector", "sector_code", "industry_group", "industry_group_code",
        "industry", "industry_code", "sub_industry", "sub_industry_code",
    )

    # ⚠️ PRICE COLUMNS `pool__basic` ALREADY OWNS, under these names or others.
    # `gold.stocks_ta` calls them `open/high/low/close`, the FA table adds
    # `open_raw/high_raw/low_raw` — same prices, different spellings, and a model
    # given both would see one series twice and the correlation prune would spend
    # its budget discovering that.
    UNIFIED_POOL_PRICE_DUPES = (
        "open", "high", "low", "open_raw", "high_raw", "low_raw",
    )

    def _helper_unified_pool_columns(
        self, source_schema: str, source_table: str, exclude: Sequence[str]
    ) -> List[str]:
        """Source columns for a pool: the key, then everything not excluded.

        Order matters only for readability — the key leads, as it does in every
        other unified table.
        """
        types = self._helper_column_types(source_schema, source_table)
        if not types:
            raise MissingSourceDataError(
                f"`{source_schema}.{source_table}` does not exist — build it first."
            )
        missing = [k for k in self.UNIFIED_PRIMARY_KEY if k not in types]
        if missing:
            raise MissingSourceDataError(
                f"`{source_schema}.{source_table}` has no {missing} column(s), so it "
                f"cannot be keyed {self.UNIFIED_PRIMARY_KEY}."
            )
        dropped = set(exclude) | set(self.UNIFIED_PRIMARY_KEY)
        return list(self.UNIFIED_PRIMARY_KEY) + [
            c for c in types if c not in dropped
        ]

    def _helper_unified_pool_from_source(
        self,
        ticker: str,
        pool: str,
        source: str,
        exclude: Sequence[str],
    ) -> Tuple[int, int]:
        """Shared body of `_ingest_unified_pool_ta` / `_ingest_unified_pool_fa`.

        ⚠️ **The source is INNER JOINED to `pool__basic` on the whole key, not read
        on its own.** `gold.stocks_ta` runs to 2026-06-26 where `pool__basic` stops
        at 2026-06-25, so a straight copy would produce a pool with 4,242 rows
        against `pool__basic`'s 4,235 — the exact mismatch that made the dropped
        `pool__targets` unjoinable (see `_ingest_unified_pool_targets`). Joining to
        the spine makes one calendar structural instead of hoped for.

        Returns `(rows, columns)`.
        """
        schema = self._helper_unified_schema(ticker)
        universe = self._helper_unified_is_universe(ticker)
        source_schema, source_table = source.split(".", 1)

        if not self._helper_column_types(schema, "pool__basic"):
            raise MissingSourceDataError(
                f"`{schema}.pool__basic` does not exist — it is the calendar spine "
                f"every other pool is joined to. Build it first."
            )

        columns = self._helper_unified_pool_columns(source_schema, source_table, exclude)
        selected = ", ".join(f"s.{c}" for c in columns)
        where = "" if universe else " AND s.ticker = %s"
        params = () if universe else (ticker,)

        with self._database_driver._cursor_ctx() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {source} s "
                f"JOIN {schema}.pool__basic b ON b.date = s.date "
                f" AND b.exchange = s.exchange AND b.ticker = s.ticker"
                f"{'' if universe else ' WHERE TRUE' + where}",
                params,
            )
            available = int(cur.fetchone()[0])
            if not available:
                raise MissingSourceDataError(
                    f"`{source}` shares no (date, exchange, ticker) rows with "
                    f"{schema}.pool__basic"
                    + ("" if universe else f" for {ticker}")
                    + f", so {schema}.{pool} would be empty."
                )

            # Dropped as late as possible: a failure above leaves the old table intact.
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{pool}")
            cur.execute(
                f"CREATE TABLE {schema}.{pool} AS "
                f"SELECT {selected} FROM {source} s "
                f"JOIN {schema}.pool__basic b ON b.date = s.date "
                f" AND b.exchange = s.exchange AND b.ticker = s.ticker"
                f"{'' if universe else ' WHERE TRUE' + where}",
                params,
            )
            self._helper_unified_primary_key(cur, schema, pool)

            cur.execute(f"SELECT COUNT(*) FROM {schema}.{pool}")
            written = int(cur.fetchone()[0])
            cur.execute(f"SELECT COUNT(*) FROM {schema}.pool__basic")
            spine = int(cur.fetchone()[0])
            # ⚠️ Symmetric EXCEPT, not a row count. Two tables can agree on how many
            # rows they hold and disagree about WHICH — the same check
            # `unified_vcb_pool_targets` makes, for the same reason.
            cur.execute(
                f"SELECT COUNT(*) FROM ("
                f"  SELECT date, exchange, ticker FROM {schema}.{pool}"
                f"  EXCEPT SELECT date, exchange, ticker FROM {schema}.pool__basic"
                f"  UNION ALL"
                f"  SELECT date, exchange, ticker FROM {schema}.pool__basic"
                f"  EXCEPT SELECT date, exchange, ticker FROM {schema}.{pool}"
                f") d"
            )
            unaligned = int(cur.fetchone()[0])

        if written != available:
            raise PipelineError(
                f"{schema}.{pool} wrote {written} rows against {available} joinable."
            )
        if unaligned:
            raise PipelineError(
                f"{schema}.{pool} and pool__basic disagree on {unaligned} key(s). "
                f"Every pool must sit on the spine's calendar or the join silently "
                f"drops rows."
            )
        if written != spine:
            raise PipelineError(
                f"{schema}.{pool} has {written} rows against pool__basic's {spine}."
            )
        return written, len(columns)

    def _ingest_unified_pool_ta(self, ticker: str) -> None:
        """`gold.stocks_ta` → `unified_schema_<ticker>.pool__ta` — the technical block.

        **~920 indicator columns** — Bollinger/MACD/RSI/Hilbert families and their
        slopes, crossings and boolean flags — keyed `(date, exchange, ticker)` and on
        `pool__basic`'s calendar.

        ⚠️ **Identity, taxonomy and the duplicated OHLC are dropped.** `gold.stocks_ta`
        repeats the 8 GICS columns and `open/high/low`, all of which `pool__basic`
        already carries; keeping them would give a joined panel two copies of one
        price and eight constant strings.

        ⚠️ **~207 of these columns are BOOLEAN** (`*_gt_prev`, `*_valid`, crossing
        flags). `FeatureSelector._prepare` excludes bool dtypes explicitly, so they
        are stored but will not be scored until someone decides how to encode them —
        which is a modelling decision, not an ingest one.

        ⚠️ **This is the pool §7 of `feature_selection/CONTEXT.md` says to widen to
        LAST**, after a target has cleared its own null. On a single ticker it buys a
        longer list of nothing, more slowly, and with a higher bar.
        """
        schema = self._helper_unified_schema(ticker)
        self._logger.log_info(
            f"Ingesting unified {schema}.pool__ta (from {self.UNIFIED_TA_SOURCE})..."
        )
        rows, columns = self._helper_unified_pool_from_source(
            ticker, "pool__ta", self.UNIFIED_TA_SOURCE,
            exclude=self.UNIFIED_POOL_IDENTITY + self.UNIFIED_POOL_PRICE_DUPES,
        )
        self._logger.log_info(f"{schema}.pool__ta: {rows} rows x {columns} columns.")

    def _ingest_unified_pool_fa(self, ticker: str) -> None:
        """`gold.stocks_financials_bank_fa` → `…​.pool__fa` — the fundamental block.

        **~204 columns**: the balance sheet (93), cash flow (50) and income statement
        (29) line items, share counts, `eps`/`bvps`/`ttm_*`, and the period metadata
        — keyed `(date, exchange, ticker)`, forward-filled to a DAILY grain on
        `pool__basic`'s calendar.

        ## ⚠️ `publish_date` IS THE ONLY THING STOPPING THIS BEING A TIME MACHINE

        A quarterly statement is not knowable on the last day of its quarter — VCB's
        Q1 is published around 29 April, a median **54 days** later. Attaching a
        figure to the period it describes rather than to the day it was announced
        would let a model read Q1's profit throughout Q1, and it would look like the
        best feature ever found. The source already carries `publish_date` and is
        expanded so that each row holds the most recent statement **published on or
        before that row's date**; this method ASSERTS that rather than trusting it,
        because it is the one property that decides whether the pool is usable.

        ⚠️ **The lag reaches 0 days.** On a publication day the figures are attached
        to that same session. If a statement was released after the close, a model
        trading that close has seen tomorrow's news — a half-day leak this layer
        cannot detect, and a reason to shift `publish_date` forward by one session
        before trusting any result that leans on the FA pool.

        ⚠️ **Bank template only, so only two tickers exist**: `VCB` and `ACB`.
        `gold.stocks_financials_bank_fa` is built from the CafeF *bank* chart of
        accounts; a non-bank ticker raises here rather than producing an empty table.
        """
        schema = self._helper_unified_schema(ticker)
        self._logger.log_info(
            f"Ingesting unified {schema}.pool__fa (from {self.UNIFIED_FA_SOURCE})..."
        )

        # ⚠️ The TA block is excluded by NAME INTERSECTION, not by a prefix guess.
        # `gold.stocks_financials_bank_fa` is the FA block merged onto the TA one —
        # 906 of its 1,150 columns are `gold.stocks_ta` columns — and letting those
        # through would make `pool__fa` and `pool__ta` 906-way duplicates of each
        # other, which the correlation prune would then spend its whole budget on.
        ta_columns = tuple(self._helper_column_types(*self.UNIFIED_TA_SOURCE.split(".", 1)))
        basic_columns = tuple(self._helper_column_types(SILVER_SCHEMA, "stocks_basic"))
        exclude = (
            self.UNIFIED_POOL_IDENTITY
            + self.UNIFIED_POOL_PRICE_DUPES
            + ta_columns
            + basic_columns
        )

        rows, columns = self._helper_unified_pool_from_source(
            ticker, "pool__fa", self.UNIFIED_FA_SOURCE, exclude=exclude
        )

        with self._database_driver._cursor_ctx() as cur:
            # THE ASSERTION THIS METHOD EXISTS FOR.
            cur.execute(
                f"SELECT COUNT(*) FROM {schema}.pool__fa WHERE publish_date > date"
            )
            ahead = int(cur.fetchone()[0])
            cur.execute(
                f"SELECT COUNT(*) FROM {schema}.pool__fa WHERE publish_date IS NULL"
            )
            unpublished = int(cur.fetchone()[0])
            cur.execute(
                f"SELECT MIN(date - publish_date), "
                f"       PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY date - publish_date), "
                f"       MAX(date - publish_date) FROM {schema}.pool__fa"
            )
            lag_min, lag_median, lag_max = cur.fetchone()

        if ahead:
            raise PipelineError(
                f"{schema}.pool__fa has {ahead} row(s) whose publish_date is AFTER "
                f"the row's own date. Every one of them lets a model read a "
                f"statement before it was announced. Fix the expansion in "
                f"{self.UNIFIED_FA_SOURCE}; do not filter them out here."
            )
        if unpublished:
            raise PipelineError(
                f"{schema}.pool__fa has {unpublished} row(s) with a NULL "
                f"publish_date, so their look-ahead cannot be checked at all."
            )
        self._logger.log_info(
            f"{schema}.pool__fa: {rows} rows x {columns} columns; "
            f"publish lag min {lag_min} / median {lag_median} / max {lag_max} days, "
            f"0 rows published after their own date."
        )

    # endregion Helper functions

    def _run_layer(
        self,
        data_quality: DataQuality,
        schema: str,
        switch_branch: str,
        ingests: List[tuple],
    ) -> List[str]:
        """Shared body of the three public entry points: connect, run each ENABLED
        leaf, disconnect, and report which leaves failed.

        ⚠️ THIS IS THE `main.py` COMPATIBILITY SHIM, AND IT DELIBERATELY DOES NOT
        RAISE. `main.py` calls the three entry points unconditionally and has always
        run to completion; keeping that means a failing table must not abort the
        others or kill the process. What changed is that the failure is no longer
        INVISIBLE — it used to be one ERROR line among thousands, from a single
        `try/except` wrapped around every leaf at once, which also meant the FIRST
        failure silently skipped every leaf after it. Now each leaf is isolated, and
        the run ends with an unmissable summary naming exactly what failed.

        ⚠️ ORCHESTRATION MUST NOT COME THROUGH HERE. Dagster assets call the
        `_ingest_*` methods DIRECTLY so the exception propagates and the asset goes
        red — see `src/orchestration/resources.py`. If a future caller needs a hard
        failure from this path, act on the returned list of failed leaves; it is
        returned rather than raised precisely so the choice belongs to the caller.

        `ingests` is a list of `(leaf, callable | [callable, ...])`. A leaf holding
        several callables runs them IN ORDER and stops that leaf at the first failure
        — they are chained (`stocks_financials`'s `_fa` step reads the table the step
        before it writes), so continuing past a failure would build on a stale input.
        """
        label = data_quality.value

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
            self._database_driver.create_schema(schema)
        except Exception as e:
            # Nothing can run without a connection, so this is the one case that
            # ends the layer outright.
            self._logger.log_error(
                f"`{label}`: could not connect / create schema — layer SKIPPED: {e}"
            )
            return ["<connection>"]

        ran: List[str] = []
        failed: List[str] = []

        try:
            for leaf, steps in ingests:
                if not self._switch_handler.is_enabled(
                    "data_preprocessor", switch_branch, leaf
                ):
                    continue

                try:
                    for step in steps if isinstance(steps, (list, tuple)) else [steps]:
                        step()
                    ran.append(leaf)
                except Exception as e:
                    failed.append(leaf)
                    self._logger.log_error(
                        f"`{label}` leaf '{leaf}' FAILED: {type(e).__name__}: {e}"
                    )
        finally:
            self._database_driver.disconnect()

        if failed:
            self._logger.log_error(
                f"`{label}` finished with {len(failed)} FAILED leaf/leaves: "
                f"{failed} (succeeded: {ran})"
            )
        else:
            self._logger.log_info(f"`{label}` finished: {len(ran)} leaves OK {ran}")

        return failed

    def ingest_bronze_data(self) -> List[str]:
        """Returns the list of leaves that failed (empty = all clean). See `_run_layer`."""
        if not self._switch_handler.is_enabled(
            "data_preprocessor", "data_quality_bronze"
        ):
            return []

        # ONE LEAF PER SOURCE TABLE, not one `stocks` leaf for all ten. Bronze is
        # raw-faithful and each of these reads a different raw_data folder, so they
        # share nothing but the schema — yet lumped together the cheap ones could not
        # be run without the expensive ones (re-ingesting the financials CSVs, ~2 s,
        # meant also re-reading 2.4 M price rows + 2.7 M Simplize rows). The leaves
        # are independent: bronze has no cross-table dependency, so any subset is a
        # valid run. Order is only convention (universe, then daily, then event, then
        # reference).
        bronze_ingests = [
            ("bonds", self._ingest_bronze_bonds),
            ("economy", self._ingest_bronze_economy),
            ("forex", self._ingest_bronze_forex),
            ("funds", self._ingest_bronze_funds),
            ("indices", self._ingest_bronze_indices),
            ("trading_view_stocks", self._ingest_bronze_stocks_trading_view),
            ("cafef_price", self._ingest_bronze_cafef_price),
            ("cafef_foreign", self._ingest_bronze_cafef_foreign),
            ("cafef_order_stats", self._ingest_bronze_cafef_order_stats),
            ("cafef_prop_trading", self._ingest_bronze_cafef_prop_trading),
            ("cafef_index_price", self._ingest_bronze_cafef_index_price),
            ("cafef_index_foreign", self._ingest_bronze_cafef_index_foreign),
            ("cafef_index_order_stats", self._ingest_bronze_cafef_index_order_stats),
            ("cafef_index_prop_trading", self._ingest_bronze_cafef_index_prop_trading),
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

        return self._run_layer(
            DataQuality.BRONZE, BRONZE_SCHEMA, "data_quality_bronze", bronze_ingests
        )

    def ingest_silver_data(self) -> List[str]:
        """Returns the list of leaves that failed (empty = all clean). See `_run_layer`."""
        if not self._switch_handler.is_enabled(
            "data_preprocessor", "data_quality_silver"
        ):
            return []

        silver_ingests = [
            ("bonds", self._ingest_silver_bonds),
            ("economy", self._ingest_silver_economy),
            ("forex", self._ingest_silver_forex),
            ("funds", self._ingest_silver_funds),
            ("indices", self._ingest_silver_indices),
            ("gics", self._ingest_silver_gics),
            (
                "financials",
                [
                    self._ingest_silver_cafef_financials,
                    self._ingest_silver_cafef_financials_bank,
                ],
            ),
            # The one-to-one source carry-ups. Split off `stocks_basic`'s leaf because
            # they are not its inputs — `stocks_basic` joins the BRONZE tables directly,
            # so neither needs the other and rebuilding the 2.4 M-row panel to refresh a
            # carry-up (or vice versa) was pure cost.
            (
                "cafef_carry_ups",
                [
                    self._ingest_silver_cafef_price,
                    self._ingest_silver_cafef_order_stats,
                    self._ingest_silver_cafef_foreign,
                    self._ingest_silver_cafef_prop_trading,
                    self._ingest_silver_cafef_insider_shareholder_transactions,
                ],
            ),
            # News sentiment reads bronze.cafef_news only (independent of the
            # stocks/financials tables), so it gets its own leaf.
            ("news_sentiment", self._ingest_silver_cafef_news_sentiment),
            ("stocks_basic", self._ingest_silver_stocks_basic),
            # Depends on BOTH silver.stocks_basic and silver.cafef_financials_bank,
            # so it runs after both are (re)built. The _fa step then reads the
            # plain-join table just built and appends the fundamental indicators.
            (
                "stocks_financials",
                [
                    self._ingest_silver_stocks_basic_financials_bank,
                    self._ingest_silver_stocks_basic_financials_bank_fa,
                ],
            ),
        ]

        return self._run_layer(
            DataQuality.SILVER, SILVER_SCHEMA, "data_quality_silver", silver_ingests
        )

    def ingest_gold_data(self) -> List[str]:
        """Returns the list of leaves that failed (empty = all clean). See `_run_layer`."""
        if not self._switch_handler.is_enabled(
            "data_preprocessor", "data_quality_gold"
        ):
            return []

        gold_ingests = [
            ("bonds", self._ingest_gold_bonds),
            ("economy", self._ingest_gold_economy),
            ("forex", self._ingest_gold_forex),
            ("funds", self._ingest_gold_funds),
            # `indices` retired 2026-08-01 — duplicated `gold.stock_market`.
            ("stocks", self._ingest_gold_stocks),
            # `stocks_ta` gets NO switch leaf, per the convention every gold table added
            # since `stock_market`: new gold work lands as a Dagster asset, and phase 5
            # of the migration retires these keys anyway. It is also ~900 columns over
            # 2.4 M rows — not something a layer-wide run should pull in by default.
        ]

        return self._run_layer(
            DataQuality.GOLD, GOLD_SCHEMA, "data_quality_gold", gold_ingests
        )
