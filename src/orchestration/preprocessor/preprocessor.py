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
    FILTER_SCHEMA,
    UNIFIED_SCHEMA,
)
from utils.enums import *
from utils.exceptions import MissingSourceDataError, PipelineError
from utils.utils import *
from utils.switch_handler import SwitchHandler

# ⚠️ THE FILTER LAYER'S REGISTRY, imported as a MODULE rather than by name. Every screen
# and every condition lives in `filters.py`; this file executes them and owns nothing
# about what they mean. Adding a filter must never require editing a 9,000-line module.
from orchestration.preprocessor import filters as filter_registry

# ⚠️ **RE-BOUND HERE ON PURPOSE, AFTER THE STAR IMPORTS ABOVE — do not move it back up.**
# Line 6 says `from glob import glob`, which binds the FUNCTION; `from utils.enums import *`
# and `from utils.utils import *` then re-export their own `import glob` and rebind the name
# to the MODULE, silently. Every one of this file's 11 `glob(...)` call sites then raises
# `TypeError: 'module' object is not callable` — measured 2026-08-24, when
# `_ingest_bronze_cafef_financial_schema` failed on it and took the whole
# `bronze/cafef_financials` materialisation with it.
#
# ⚠️ The star imports are what make this possible at all: `utils.utils` and `utils.enums`
# declare no `__all__`, so `import *` carries their imported MODULES into this namespace
# alongside their own functions. A one-line re-bind is the minimal repair; the real fix is an
# `__all__` in those two modules, which is `GLB-1`.
from glob import glob  # noqa: E402  — must come AFTER the star imports; see above

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

    # ⚠️ HOW MANY CSVs ARE READ INTO MEMORY AT ONCE BY `_ingest_bronze_forex`.
    #
    # The forex folder reached **6,189 files / 2.19 GB / ~29.6 M rows** on 2026-08-14,
    # and the read-everything-then-concat shape every other bronze ingest uses needs
    # ~10-15 GB for that — against 3.6 GB free on this machine. Batching bounds it: 300
    # files is ~1.4 M rows, and the writer is an UPSERT (`ON CONFLICT`) over
    # `CREATE TABLE IF NOT EXISTS`, so calling it once per batch accumulates exactly as
    # one call would.
    #
    # ⚠️ Only FOREX is batched. Its siblings read tens or hundreds of files and the
    # unbatched shape is easier to read; converting them for a problem they do not have
    # would be five more places to keep in step.
    BRONZE_FOREX_BATCH_FILES = 300

    def _ingest_bronze_forex(self) -> None:
        """`raw_data/trading_view/data/forex/**` → `bronze.trading_view_forex`.

        ⚠️ **READ IN BATCHES, NOT ALL AT ONCE** — see `BRONZE_FOREX_BATCH_FILES`.

        ⚠️ **AND THE BATCHING CHANGES WHICH DUPLICATE WINS, DELIBERATELY.** The folder
        holds the same series many times over: 6,189 files carry 3,074 distinct series
        (2026-08-14), one of them stored 12 times, because `skip_existing=False` lands a
        re-fetch BESIDE the previous file and a broken broker filter wrote the same
        symbols under several folders. The unbatched shape deduped with `keep="first"`
        in glob order, which sorts the OLDER end-date first — so **where an old and a new
        file disagreed on a date's value, the STALE one won** (documented in
        `orchestration/CONTEXT.md`). Upserting batch by batch makes the LAST write win,
        so the files are sorted by name — the name ends in the fetch date — and **the
        NEWEST file now wins**. That is the behaviour the old comment warned about,
        inverted on purpose.
        """
        self._logger.log_info("Ingesting bronze forex data...")

        forex_dir = os.path.join(TRADING_VIEW_RAW_DATA_DIR, "data", "forex")
        # ⚠️ SORTED, and the sort is load-bearing: `<EXCHANGE>_<SYMBOL>_<start>_<end>.csv`
        # ends in the fetch date, so ascending order writes the oldest first and lets
        # every newer file overwrite it.
        csv_files = sorted(glob(os.path.join(forex_dir, "**", "*.csv"), recursive=True))

        if not csv_files:
            raise MissingSourceDataError(
                f'No forex CSV files found in "{forex_dir}".'
            )

        batch_size = self.BRONZE_FOREX_BATCH_FILES
        batches = [
            csv_files[start : start + batch_size]
            for start in range(0, len(csv_files), batch_size)
        ]
        self._logger.log_info(
            f"Forex: {len(csv_files)} CSV file(s) in {len(batches)} batch(es) of "
            f"{batch_size}."
        )

        total_rows = 0
        for index, batch in enumerate(batches, start=1):
            dataframes = []
            for fp in batch:
                df = pd.read_csv(fp, encoding="utf-8")
                if not df.empty and not df.dropna(how="all").empty:
                    dataframes.append(df)
            if not dataframes:
                continue

            df = pd.concat(dataframes, ignore_index=True).drop_duplicates()
            del dataframes

            # ⚠️ SPLIT ON READ. `symbol` ("ECONOMICS:VNCPI") is the RAW CSV's key; the
            # bronze convention is (exchange, ticker), so it is split here and every
            # clean, order and dedupe below keys on the real stored columns. It used to
            # be split LAST, which left `symbol` as the working key through the whole
            # method — and is why five silver ingests still reach for a column no bronze
            # table has ever held.
            df = self._helper_split_symbol_column(df)

            # ⚠️ THE SCRAPER WRITES TWO FILE SHAPES AND ONLY ONE WAS EVER INGESTED.
            # `_scrape_data_trading_view_link` detects OHLC per series and writes either
            # `(date, open, high, low, close, volume)` or `(date, value)` — 4,402 files
            # against 1,787 on 2026-08-14. Every clean below filters on `value`, so for
            # years **71% of the forex folder was read and silently discarded**: the
            # unbatched concat produced a `value` column from the OTHER files, and
            # `REMOVE_RECORD_IF_COLUMN_IS_NULL` then dropped every OHLC row. Batching
            # turned that into a visible `KeyError` on the first batch with no
            # value-shaped file in it, which is how it was found.
            #
            # ⚠️ `value` IS `close` — not approximately, identically. The extraction JS
            # pushes `[date, v[1], v[2], v[3], v[4], v[5]]` when a series is OHLC and
            # `[date, v[4]]` when it is not (`trading_view_scraper.py`, the `bulk_js`
            # block): **the same slot 4 of the same array**, labelled `close` in one
            # branch and `value` in the other. No series on disk carries both shapes, so
            # this cannot be confirmed by overlap — it is confirmed by the code that
            # wrote them.
            if "value" not in df.columns:
                df["value"] = pd.NA
            if "close" in df.columns:
                df["value"] = df["value"].where(df["value"].notna(), df["close"])

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
            total_rows += len(df)
            self._logger.log_info(
                f"Forex batch {index}/{len(batches)}: {len(df)} row(s) upserted "
                f"({total_rows} so far)."
            )
            del df

        if not total_rows:
            raise MissingSourceDataError("No valid forex CSV data found.")

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
        # ⚠️ WHICH ENTITY THE ROW DESCRIBES — "True" consolidated (hợp nhất), "False"
        # standalone (công ty mẹ), blank for a `missing` row. Added 2026-08-24 with
        # `FinancialsBuilder.documents(allow_parent=…)`, and it MUST be listed here: this
        # tuple is what separates document metadata from LINE ITEMS, and a column missing
        # from it falls through to `line_cols` and is coerced to numeric —
        # `ValueError: Unable to parse string "False"`, which is how it was found. The
        # writer's `DATA_COLS` and this reader's list are two halves of one contract with
        # nothing enforcing the match; adding a column to one means adding it to the other.
        "consolidated",
        # ⚠️ HOW MANY MONTHS OF ACTIVITY THE ROW COVERS — 3 for a standalone quarter,
        # 6/9/12 for a year-to-date figure that could not be split, blank for a balance
        # sheet (a stock) and for a `missing` row. Added 2026-08-30 with
        # `FinancialsBuilder._decumulate`'s keep-and-label branch. ⚠️ **It must be listed
        # here AND in `CAFEF_FINANCIAL_KEY_COLS`**: here so it is not treated as a line
        # item, there so it stays ON the statement table beside `source` — a consumer
        # summing a flow column has to know the span without a join, and it is the one
        # piece of metadata that changes what the FIGURES mean rather than where they
        # came from.
        "months",
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
        # ⚠️ The SPAN of a flow row — see CAFEF_FINANCIAL_META_COLS."months". Kept here, not
        # only in `cafef_financial_reports`, for the same reason `source` is: a reader of one
        # statement must be able to tell a quarter from a year without joining another table,
        # and summing four rows of which one is already a year double-counts that year.
        "months",
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
                    df,
                    decimal_cols=[],
                    # `months` is a whole number of months (3/6/9/12), never a fraction —
                    # bigint like year/quarter, not decimal like the đồng figures.
                    bigint_cols=["year", "quarter", "months", *share_cols],
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

        # RAISES rather than returning — see `_ingest_silver_funds`.
        if df.empty:
            raise MissingSourceDataError(
                f"{BRONZE_SCHEMA}.trading_view_bonds is empty — run the bronze bonds "
                f"ingest first (it reads raw_data/trading_view/data/bonds/)."
            )

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

        # RAISES rather than returning — a silent return leaves `silver.forex` holding
        # the last successful run's rows while reporting success. See
        # `_ingest_silver_funds` and `src/orchestration/CONTEXT.md` §4.1.
        if df.empty:
            raise MissingSourceDataError(
                f"{BRONZE_SCHEMA}.trading_view_forex is empty — run the bronze forex "
                f"ingest first (it reads raw_data/trading_view/data/forex/)."
            )

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

        # ⚠️ RAISES rather than returning, unlike its `bonds`/`forex`/`indices`
        # siblings. A silent `return` here leaves `silver.funds` holding whatever the
        # LAST successful run wrote, and the caller — an orchestrator included —
        # cannot tell that from a table that was just rebuilt. That is the exact
        # failure Phase 0 exists to close (`src/orchestration/CONTEXT.md` §4.1); the
        # siblings still have it, and each is a one-line fix when its own asset lands.
        if df.empty:
            raise MissingSourceDataError(
                f"{BRONZE_SCHEMA}.trading_view_funds is empty — run the bronze funds "
                f"ingest first (it reads raw_data/trading_view/data/funds/)."
            )

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
                and c not in ("year", "quarter", "months")
            ]
            share_cols = [c for c in self.CAFEF_FINANCIAL_SHARE_COLS if c in line_cols]
            decimal_line_cols = [c for c in line_cols if c not in share_cols]
            df = self._helper_cast_columns(
                df,
                decimal_cols=decimal_line_cols,
                bigint_cols=["year", "quarter", "months", *share_cols],
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

        # ⚠️ **A P&L ROW THAT IS NOT A QUARTER MAY NOT ENTER A TRAILING-4-QUARTER SUM.**
        # `income_statement_months` says how many months of activity the row covers, and
        # since 2026-08-30 it can legitimately read 6 or 12: `_decumulate` KEEPS a
        # year-to-date figure — labelled — where the quarters that would have been
        # subtracted from it were never filed (BID before 2012, BSR before H2-2018, VCB
        # Q4-2008). Summing such a row with three quarterly ones counts that year twice,
        # and the result would reconcile against nothing that could catch it.
        #
        # NaN is the right way to exclude it, not a row drop: `_ttm` requires all four
        # quarters (`min_periods=4`), so every window touching this quarter goes NULL —
        # which is the docstring's own rule that *a gap makes the window NULL rather than
        # wrong*. The BALANCE SHEET is untouched: a stock at 31 December is the Q4 stock
        # whatever span the P&L beside it covers.
        #
        # ⚠️ A BLANK `months` IS LEFT ALONE, and it has to be. Every income-statement row
        # written before the column existed was either de-cumulated or read from an
        # ordinary quarterly filing — both 3 months — so blanking them would delete the
        # whole bank history to guard against a case none of them is in. The column is the
        # measurement; its absence is not evidence of the opposite (§5 rule 2).
        span = pd.to_numeric(q.get("income_statement_months"), errors="coerce")
        not_a_quarter = span.notna() & (span != 3)
        if not_a_quarter.any():
            for c in (self.BANK_FA_NET_INCOME, self.BANK_FA_PRETAX, self.BANK_FA_NII,
                      self.BANK_FA_OP_INCOME, self.BANK_FA_OP_EXPENSE):
                q.loc[not_a_quarter, c] = np.nan
            self._logger.log_info(
                f"  {int(not_a_quarter.sum())} quarter(s) carry a non-quarterly income "
                f"statement (months != 3) — their P&L flows are excluded from the TTM sums."
            )

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
        # ⚠️ **THE OP-EXPENSE IS NOT ALWAYS FILED NEGATIVE, AND THIS LINE ASSUMED IT WAS.**
        # The comment here read "op-expense is filed negative → negate", which is true of 222
        # of the 267 bank rows that carry one and FALSE of 45 — the sign is the FILING's
        # bracket convention, not a fact about the number (`SGN-1`/`SGN-2`), and CTG alone
        # stores 35 of its 63 POSITIVE. Every one of those 45 produced a NEGATIVE
        # cost-to-income ratio. A cost fraction is a magnitude over a magnitude, so `abs()` is
        # the convention-independent form and the only one that is right under both.
        # ⚠️ Measured 2026-09-03; `gold.stocks_financials_bank_fa` still holds the old values
        # until it is rebuilt (§5 rule 14 — a code fix does not mark the table stale).
        q["cost_to_income"] = (q[self.BANK_FA_OP_EXPENSE].abs()
                               / q[self.BANK_FA_OP_INCOME])

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
        sentiment model), imported here so this module owns only the ETL:
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

        # ⚠️ Run AFTER the cast — the screen does arithmetic, and before the cast these
        # columns are `object`-dtype Decimals.
        df = self._helper_screen_flow_outliers(df)

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

    # ── The flow-plausibility screen (issue OUT-1, 2026-08-16) ───────────────────
    #
    # ⚠️ **ONE CORRUPT CELL IS ENOUGH TO MANUFACTURE A FINDING.** `VCB 2026-01-05`
    # arrived from CafeF with `prop_buy_val = 4.001e17` — 400 quadrillion VND —
    # against that day's whole turnover of 2.06e11, on `prop_buy_vol = 697,000` shares
    # at a close of 57,100: an implied **5.7e11 VND per share**, ten million times the
    # real price. That single cell drove `corr(prop_net_ratio, prop_participation)` to
    # exactly +1.0 and manufactured a **+0.266** correlation against the forward
    # 5-day return.
    #
    # ⚠️ **THE DAMAGE PATH IS `StandardScaler`, NOT THE RANKERS.** Selection is
    # rank-based end to end and never saw it (Spearman +0.0008 against the same label
    # where Pearson read +0.266). But `train_test_creator` standardises with mean/std,
    # which is not robust: one such cell took a channel's sd from 0.165 to 65,385 and
    # squeezed all 877 genuine observations into a z-range of **0.0001** instead of
    # **21.6**. The channel becomes a constant plus one spike.
    #
    # ⚠️ **NULL, NOT WINSORISED, and not repaired.** The corruption factor is not
    # constant (1e7 on VCB, 1e8 on HPG/TPB), so there is nothing to divide out — the
    # true value is simply unknown, and NULL is the only honest encoding of that.
    # Every pool downstream already handles NULLs.
    #
    # ⚠️ **AND IT IS DONE HERE, IN SILVER, NOT IN A FEATURE EXPRESSION.** Silver is the
    # canonical cross-source layer, so a cross-field invariant (`value ≈ volume ×
    # price`) is exactly its business. Clipping inside a derived feature — the
    # alternative considered on 2026-08-16 — would have hidden the defect from every
    # other consumer of the same column while looking clean.
    #
    # **The thresholds were read off the distribution, not chosen.** Measured over
    # 2,388,975 rows: 99.5% of flow rows imply a price within **2×** of `close_raw`,
    # 99.8% within 10×, **99.98% within 100×**. The cliff is far below the cut, so
    # 100× flags only the unambiguous corruption.
    #
    # **TWO rules, because one is not enough.** Some rows have value AND volume
    # corrupt by similar factors — `STB 2025-12-30` carries 1.5e13 shares (the whole
    # market's daily volume is ~1e9) with a value to match, so its implied price is a
    # plausible 9.9× and the price test misses it entirely. The volume rule catches it.
    FLOW_PLAUSIBILITY_PRICE_FACTOR = 100.0
    FLOW_PLAUSIBILITY_VOLUME_FACTOR = 100.0
    # ⚠️ Flow is a SUBSET of the day's trading, so a ratio above 1.0 is already
    # impossible. 100x is chosen to match its siblings and to stay far away from any
    # legitimate reporting slack — it is not a claim that 99x would be plausible.
    FLOW_PLAUSIBILITY_VALUE_FACTOR = 100.0
    # If more than this fraction is flagged, the SOURCE changed and a screen tuned to
    # 0.016% is the wrong tool — raise instead of silently deleting a scrape.
    FLOW_PLAUSIBILITY_MAX_FLAGGED = 0.01

    # (value column, volume column) — the five independently-scraped flow pairs.
    FLOW_VALUE_VOLUME_PAIRS = (
        ("foreign_buy_value", "foreign_buy_volume"),
        ("foreign_sell_value", "foreign_sell_volume"),
        ("foreign_net_value", "foreign_net_volume"),
        ("prop_buy_val", "prop_buy_vol"),
        ("prop_sell_val", "prop_sell_vol"),
    )

    def _helper_screen_flow_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """NULL the flow value/volume pairs that cannot be reconciled with the day's
        own price and volume. Returns the frame; logs a count per column.

        A pair is implausible when EITHER
          * its implied price `|value| / |volume|` differs from `close_raw` by more
            than `FLOW_PLAUSIBILITY_PRICE_FACTOR` in either direction, or
          * its volume exceeds the day's total traded volume (matched + negotiated)
            by more than `FLOW_PLAUSIBILITY_VOLUME_FACTOR`.

        ⚠️ **BOTH members of a failing pair are NULLed**, because the test cannot say
        which of the two is wrong — only that they disagree with the price. `net` is
        screened on absolute values (it is signed on both legs) and independently of
        buy/sell, since CafeF scrapes it as its own field rather than deriving it.
        """
        close = pd.to_numeric(df.get("close_raw"), errors="coerce")
        if close is None or close.isna().all():
            return df
        total_vol = pd.to_numeric(
            df.get("volume_matched"), errors="coerce"
        ).fillna(0) + pd.to_numeric(df.get("volume_negotiated"), errors="coerce").fillna(0)
        # ⚠️ In VND. `value_matched` / `value_negotiated` are BILLIONS of VND while
        # every flow value column is plain VND — the same source inconsistency
        # `ta.ta_functions.VALUE_MATCHED_VND_SCALE` documents. Comparing the two
        # without this factor would make the value rule 1e9 times too loose, i.e. dead.
        total_vol_value = (
            pd.to_numeric(df.get("value_matched"), errors="coerce").fillna(0)
            + pd.to_numeric(df.get("value_negotiated"), errors="coerce").fillna(0)
        ) * 1e9

        flagged: Dict[str, int] = {}
        any_flag = pd.Series(False, index=df.index)
        for value_col, volume_col in self.FLOW_VALUE_VOLUME_PAIRS:
            if not {value_col, volume_col}.issubset(df.columns):
                continue
            value = pd.to_numeric(df[value_col], errors="coerce").abs()
            volume = pd.to_numeric(df[volume_col], errors="coerce").abs()

            # ⚠️ `value > 0` IS LOAD-BEARING TOO, and it is the second thing this
            # screen got wrong before it was measured. A row with a real volume and a
            # value of ZERO gives ratio 0, which trips the low side — and there are
            # **2,849** of them, against the 576 the screen is actually for. They are
            # not all corruption: `PVC 2025-11-25` sells **4 shares** at 10,800, which
            # the source rounds to 0 in its own value unit. The 99.98%-within-100x
            # cliff these thresholds come from was measured on strictly-positive
            # pairs, so the rule must be too.
            #
            # ⚠️ **AND THEY ARE NOT ALL ROUNDING — an earlier version of this comment
            # said they were, and the data says otherwise.** Of the 857 `prop_sell`
            # rows with a zero value and a real volume, **362 imply under 100 M VND
            # (rounding) but 305 imply 1 BN VND or more**, the largest 1.73e13. VCB
            # 2026-01-05 — the row that opened OUT-1 — is one of them: its buy pair was
            # NULLed and its sell pair survives as `val=0, vol=165,300`. So this class
            # is MIXED, it is still unscreened, and the counter below reports it rather
            # than characterising it.
            plausible = (value > 0) & (volume > 0) & (close > 0)
            implied = value / volume.where(volume > 0)
            ratio = implied / close.where(close > 0)
            bad_price = plausible & (
                (ratio > self.FLOW_PLAUSIBILITY_PRICE_FACTOR)
                | (ratio < 1.0 / self.FLOW_PLAUSIBILITY_PRICE_FACTOR)
            )
            # ⚠️ `total_vol > 0` IS LOAD-BEARING, and leaving it out cost a wrong
            # number on the first run. Without it a day with NO traded volume at all
            # makes the right-hand side 0, so any non-zero flow volume trips the rule:
            # the screen NULLed **2,818 rows (0.118%)** against the 2,818−378 = 2,440
            # it was documented to. Those extra rows are a DIFFERENT defect —
            # 14,056 rows across 457 tickers carry flow on a day the price table
            # records no turnover — and folding a second, unexamined invariant into a
            # screen justified by a price-distribution cliff is how a cleaning step
            # starts deleting data nobody agreed to delete. Counted below, not NULLed.
            bad_volume = (total_vol > 0) & (
                volume > total_vol * self.FLOW_PLAUSIBILITY_VOLUME_FACTOR
            )
            # ⚠️ **THE THIRD RULE, AND THE ONLY ONE THAT WORKS WITHOUT A VOLUME.**
            # Both rules above need `volume > 0`, so a huge VALUE carrying a NULL or
            # zero volume passes them untouched — which is exactly what `SHB
            # 2025-10-30` did, surviving the first fix and leaving
            # `drv_prop_participation` with a maximum of **57,644** against a p99 of
            # 0.269. A desk's flow is a SUBSET of the day's trading, so flow value
            # cannot exceed total turnover at all; the same 100x factor is therefore
            # enormously generous and still catches it.
            bad_value = (total_vol_value > 0) & (
                value > total_vol_value * self.FLOW_PLAUSIBILITY_VALUE_FACTOR
            )
            bad = (bad_price | bad_volume | bad_value).fillna(False)
            if not bad.any():
                continue
            df.loc[bad, [value_col, volume_col]] = np.nan
            flagged[f"{value_col}/{volume_col}"] = int(bad.sum())
            any_flag |= bad

        # ⚠️ REPORTED, NOT NULLED — a separate invariant this screen deliberately does
        # not enforce. Flow recorded on a day whose price table shows no turnover at
        # all is internally inconsistent, but whether the fault is the flow tab or the
        # price tab has not been established, and 14,056 rows is far too many to
        # delete on a guess. Surfaced here so it stays visible until someone decides.
        no_trade_with_flow = 0
        zero_value_with_volume = 0
        flow_present = pd.Series(False, index=df.index)
        for value_col, volume_col in self.FLOW_VALUE_VOLUME_PAIRS:
            if volume_col not in df.columns:
                continue
            vol = pd.to_numeric(df[volume_col], errors="coerce").fillna(0).abs()
            flow_present |= vol != 0
            if value_col in df.columns:
                val = pd.to_numeric(df[value_col], errors="coerce").abs()
                zero_value_with_volume += int(((val == 0) & (vol > 0)).sum())
        no_trade_with_flow = int(((total_vol <= 0) & flow_present).sum())
        if no_trade_with_flow or zero_value_with_volume:
            self._logger.log_warning(
                f"Flow plausibility screen, REPORTED NOT SCREENED: "
                f"{no_trade_with_flow} row(s) carry flow volume on a day with no "
                f"traded volume; {zero_value_with_volume} pair(s) carry a real volume "
                f"with a ZERO value — a MIXED class, part source rounding of a "
                f"few-share trade and part genuinely missing value on a large one "
                f"(measured 2026-08-16: 362 of 857 prop_sell rows imply <100M VND, "
                f"305 imply >=1BN). Both are separate invariants from the one this "
                f"screen enforces. See ISSUES.md OUT-1."
            )

        rows = int(any_flag.sum())
        if not rows:
            self._logger.log_info(
                "Flow plausibility screen: 0 implausible value/volume pairs."
            )
            return df

        share = rows / max(len(df), 1)
        # ⚠️ A ceiling, not a target. 0.016% is the measured rate; a jump to 1% means
        # the SOURCE changed shape and this screen would be quietly deleting a scrape.
        if share > self.FLOW_PLAUSIBILITY_MAX_FLAGGED:
            raise PipelineError(
                f"Flow plausibility screen flagged {rows} of {len(df)} rows "
                f"({100 * share:.3f}%), above the {100 * self.FLOW_PLAUSIBILITY_MAX_FLAGGED:.1f}% "
                f"ceiling. That is a source change, not outliers — inspect "
                f"bronze.cafef_foreign / cafef_prop_trading before re-running. "
                f"Per pair: {flagged}"
            )
        self._logger.log_warning(
            f"Flow plausibility screen: NULLed {rows} row(s) of {len(df)} "
            f"({100 * share:.4f}%) whose flow value/volume disagrees with the day's "
            f"price by >{self.FLOW_PLAUSIBILITY_PRICE_FACTOR:.0f}x or whose flow "
            f"volume exceeds the day's total by >"
            f"{self.FLOW_PLAUSIBILITY_VOLUME_FACTOR:.0f}x. Per pair: {flagged}. "
            f"See ISSUES.md OUT-1."
        )
        return df

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

    # ── gold.economy is NINETEEN TABLES, one per country (2026-08-06) ────────────
    #
    # `gold_schema.economy_<country>`, all on ONE shared business-day calendar and
    # carrying the SAME column names the single table used.
    #
    # ⚠️ IT IS SPLIT BECAUSE 3,851 SERIES CANNOT BE 3,852 COLUMNS. The economy scrape
    # went from 5 countries to 19 on 2026-08-06 and the wide panel broke BOTH of
    # PostgreSQL's ceilings at once: 3,852 columns against a hard limit of 1,600, and
    # a REAL-typed row of 15,404 bytes against a ~8,160-byte usable row width. The
    # lever `gold.forex` pulled at 328 series — carry one measure instead of 13 — was
    # already spent here, because economy has only ever carried `value`.
    #
    # ⚠️ THE SHARED CALENDAR AND THE UNCHANGED COLUMN NAMES ARE WHAT KEEP THIS FROM
    # BEING A DOWNGRADE. Every table has an identical `date` index, and the column
    # name still LEADS WITH THE COUNTRY, so the names stay globally unique and
    # joining all nineteen on `date` reproduces exactly the panel this used to be —
    # plus the fourteen countries it never had. Per-country calendars would have made
    # that an outer join across nineteen different date ranges, and dropping the now
    # redundant country prefix would have made `gdp__economics__usgdp` collide with
    # its Vietnamese namesake the moment anyone joined two of them.
    GOLD_ECONOMY_TABLE_PREFIX = "economy_"

    # PostgreSQL's hard ceiling: a table cannot be created with more columns than this,
    # whatever the types. For a wide REAL panel the ~8,160-byte usable ROW width bites
    # at 2,040 columns, so staying under this is necessary but not sufficient.
    PG_MAX_COLUMNS = 1600

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
        """`silver.economy` + `silver.economy_series` -> `gold.economy_<country>`:
        **one table per country**, one row per business day, one column per series,
        AS-OF filled.

        ⚠️ **NINETEEN TABLES SINCE 2026-08-06, AND THE REASON IS A HARD LIMIT, NOT A
        PREFERENCE.** This was one `gold.economy` table while the scrape covered five
        countries and 1,034 series. The 19-country expansion took it to 3,851 series,
        i.e. 3,852 columns against PostgreSQL's hard maximum of 1,600 — and, at REAL,
        a 15,404-byte row against a usable width of ~8,160. Both ceilings, at once.
        See `GOLD_ECONOMY_TABLE_PREFIX` for why the calendar is shared and the column
        names still carry the country.

        ⚠️ **THE OLD SINGLE `gold.economy` IS NOT WRITTEN ANY MORE, AND THIS METHOD DOES
        NOT DROP IT.** The pre-split table — five countries, 1,034 series, a strict
        subset of what the nineteen now hold — was dropped by hand on 2026-08-06. A
        rebuild will not recreate it. If it reappears, something is calling the
        pre-split code path; drop it deliberately rather than leaving a stale table that
        still looks live.

        ⚠️ Until 2026-08-01 `gold.economy` was
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

        # ⚠️ THE PANEL STOPS AT TODAY. 47 series carry projections with reference dates
        # out to 2036; on a business-day calendar those became 2,685 rows that were 2.3%
        # filled — a decade of near-empty rows whose only real effect is to make a
        # look-ahead join possible. Projections stay in bronze and silver, where they are
        # raw data; the MODEL panel is "what was knowable by then", so it ends now.
        last_day = pd.offsets.BusinessDay().rollback(pd.Timestamp.today().normalize())

        # ⚠️ ONE CALENDAR FOR EVERY COUNTRY, COMPUTED BEFORE THE SPLIT. Reindexing each
        # panel onto this same range is what makes the nineteen tables joinable on
        # `date` alone — see GOLD_ECONOMY_TABLE_PREFIX.
        calendar = pd.bdate_range(df["available_from"].min(), last_day, name="date")

        # Per-frequency staleness cap, so the carry cannot outlive the series. Built
        # once over the whole dimension; each panel takes the columns it holds.
        staleness = (
            dim.assign(series=self._build_economy_panel_columns(dim))
            .set_index("series")["frequency"]
            .map(self.ECONOMY_MAX_STALENESS_BDAYS)
            .fillna(45)
            .astype(int)
        )

        countries = sorted(df["country"].dropna().unique())
        if not countries:
            raise PipelineError(
                f"{SILVER_SCHEMA}.economy carries no country, so the per-country "
                f"panels cannot be named. Re-run the silver economy series dimension."
            )

        totals = {"series": 0, "raw": 0, "filled": 0}
        for country in countries:
            written = self._helper_gold_economy_country_panel(
                df[df["country"] == country],
                country=country,
                calendar=calendar,
                last_day=last_day,
                staleness=staleness,
            )
            for key in totals:
                totals[key] += written[key]

        self._logger.log_info(
            f"gold economy: {len(countries)} country panel(s), {totals['series']} "
            f"series total on {len(calendar)} business days - {totals['raw']} "
            f"observations visible after the publication lag, {totals['filled']} cells "
            f"after the as-of carry."
        )

    def _helper_gold_economy_country_panel(
        self,
        df: pd.DataFrame,
        country: str,
        calendar: pd.DatetimeIndex,
        last_day: pd.Timestamp,
        staleness: pd.Series,
    ) -> dict:
        """Pivot ONE country's rows into `gold.economy_<country>` and write it.

        `df` arrives already merged with the dimension, named, lagged and sorted by
        `_ingest_gold_economy`; this is only the part that has to happen per table.
        Returns the counts that method sums into its summary line.
        """
        table = f"{self.GOLD_ECONOMY_TABLE_PREFIX}{self._sanitize_identifier(country)}"

        expected_cells = df.groupby(["series", "available_from"], sort=False).ngroups
        wide = df.pivot_table(
            index="available_from", columns="series", values="value", aggfunc="last"
        )

        # ⚠️ CHECK THE CEILING HERE, BEFORE WRITING. This table is one column per
        # series, so ITS WIDTH IS A FUNCTION OF THE DATA — which is precisely how the
        # single-table version broke when the scrape went 5 countries -> 19 and asked
        # for 3,852 columns. A country that outgrows the limit has to fail here, named
        # and with the remedy attached, rather than as a bare `tables can have at most
        # 1600 columns` from the driver halfway through a layer build. The USA is the
        # one to watch: it is already 1,461 of the 1,600.
        if len(wide.columns) + 1 > self.PG_MAX_COLUMNS:
            raise PipelineError(
                f"{GOLD_SCHEMA}.{table} needs {len(wide.columns) + 1} columns (date + "
                f"{len(wide.columns)} series) and PostgreSQL allows "
                f"{self.PG_MAX_COLUMNS}. Split this country further — by `category`, "
                f"the next natural key — or carry it long."
            )

        wide = wide.reindex(calendar)

        # Every observation the lag made visible on or before `last_day` must survive the
        # reindex. This is the invariant the weekend bug broke, so it is checked, not
        # assumed.
        expected_in_range = (
            df.loc[df["available_from"] <= last_day, ["series", "available_from"]]
            .drop_duplicates()
            .shape[0]
        )
        landed = int(wide.notna().sum().sum())
        if landed != expected_in_range:
            raise PipelineError(
                f"{GOLD_SCHEMA}.{table} lost {expected_in_range - landed} observation(s) "
                f"in the reindex: {expected_in_range} distinct (series, available_from) "
                f"pairs fall on or before {last_day.date()}, but only {landed} cells "
                f"landed. An availability date that is not a business day is the usual "
                f"cause."
            )
        dropped_future = expected_cells - expected_in_range
        if dropped_future:
            self._logger.log_info(
                f"gold.{table}: {dropped_future} observation(s) have an availability "
                f"date after {last_day.date()} (projections) and are not in the panel; "
                f"they remain in {SILVER_SCHEMA}.economy."
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
            f"gold.{table}: {len(wide)} business days x {len(wide.columns) - 1} series "
            f"- {raw_cells} observations visible after the publication lag, {filled} "
            f"cells after the as-of carry "
            f"({100.0 * filled / max(cells, 1):.1f}% filled, from "
            f"{100.0 * raw_cells / max(cells, 1):.1f}%)."
        )

        overrides: dict[str, str] = {"date": DataType.DATE()}
        for col in wide.columns:
            if col != "date":
                overrides[col] = "REAL"

        self._database_driver.drop_table(GOLD_SCHEMA, table)
        self._helper_save_pandas_table_to_database(
            schema_name=GOLD_SCHEMA,
            table_name=table,
            primary_keys=["date"],
            df=wide,
            dtype_overrides=overrides,
            use_copy=True,
        )
        return {
            "table": table,
            "series": len(wide.columns) - 1,
            "raw": raw_cells,
            "filled": filled,
        }

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

    def _ingest_gold_market_breadth(self) -> None:
        """The whole cross-section compressed to ONE ROW PER SESSION — `gold.market_breadth`.

        `silver.stocks_basic` over every ticker → 8 date-keyed channels describing what
        the market as a whole was doing. It exists so a SINGLE-COMPANY study can carry
        market information without carrying the market's COLUMNS.

        ⚠️ **THIS IS A DELIBERATE REJECTION OF THE PIVOT, AND THE ARITHMETIC IS WHY.**
        The obvious way to give VCB "the whole market" is `pool__basic_bank`'s shape —
        pivot each ticker's measures into columns. At 781 tickers × 27 measures that is
        **21,087 columns against PostgreSQL's 1,600 limit** (issue `WID-1`, met before
        on forex), so it is not merely wide, it is impossible. And it would be wrong
        even if it fit: VCB has 4,261 labels, i.e. `n_eff = 852` independent
        observations, and CLAUDE.md §5c measured what width costs on exactly that
        sample — 202 channels gave test IC −0.011, 724 channels gave **−0.072** on the
        same ticker, target and splits. Compression is not a shortcut here; it is the
        only shape the sample size permits.

        ⚠️ **THE CHANNEL SET IS THE ONE THAT MEASURED NON-ZERO, not the one that is
        conventional.** Seven candidate market features were scored against VCB's
        forward 5-day return on 826 NON-OVERLAPPING observations (2026-08-16):

        | feature | Spearman | t | kept |
        |---|---|---|---|
        | `xs_skew5` | −0.0794 | **−2.29** | ✅ |
        | `xs_disp5` | +0.0571 | +1.64 | ✅ |
        | `turnover_z` | −0.0507 | −1.46 | ✅ |
        | `n_active` | +0.0119 | +0.34 | ❌ |
        | `xs_mean5` | +0.0109 | +0.31 | ❌ |
        | `above_ma20` | +0.0101 | +0.29 | ❌ |
        | `breadth_pos5` | +0.0072 | +0.21 | ❌ |

        The four dropped are the BREADTH family, and they were dropped for a reason
        beyond their t-stats: they are near-duplicates of the index level, which
        `pool__stock_market` already carries as `hose__vnindex__*`. The three kept are
        the DISPERSION/FLOW family — they describe how the cross-section is spread and
        where the money went, which no index level contains.

        ⚠️ **NOT ONE OF THEM CLEARS MULTIPLE TESTING.** Seven tests puts the Bonferroni
        bar at |t| > 2.69 and the best is −2.29. These are kept because they are the
        least-bad candidates and because they cost 8 columns, NOT because anything here
        was demonstrated. The honest reading of that table is in its last row: VCB's own
        past 5-day return scored **t = −0.31** — the market tells you at least as much
        about VCB as VCB's own history does, which is a statement about how little
        either says.

        ⚠️ **EVERY CHANNEL IS TRAILING OR CONTEMPORANEOUS.** `xs_*` are computed from
        the 5-day return ENDING at the row's date; `turnover_z` is a 5-day log change
        ending at it. Nothing reads `t+1`. This is asserted the only way it can be —
        by construction, since every window closes on the row's own date.

        ⚠️ **Survivorship, unavoidable and stated**: `silver.stocks_basic` holds no
        delisted name, so a breadth number for 2012 is computed over the companies that
        survived to 2026. Dispersion is biased DOWNWARD by that (the failures are
        missing from the tails); §2c records the same limitation for the universe.
        """
        self._logger.log_info("Ingesting gold.market_breadth (from silver.stocks_basic)...")

        with self._database_driver._cursor_ctx() as cur:
            cur.execute("DROP TABLE IF EXISTS gold_schema.market_breadth")
            cur.execute(
                """
                CREATE TABLE gold_schema.market_breadth AS
                WITH base AS (
                    SELECT date, ticker,
                           close_adjust::double precision AS px,
                           COALESCE(value_matched, 0)::double precision AS turnover
                    FROM silver_schema.stocks_basic
                    WHERE close_adjust IS NOT NULL AND close_adjust > 0
                ),
                -- ⚠️ PARTITION BY ticker: a LAG down a (date, ticker) frame without it
                -- reads the previous COMPANY, not the previous day. Same defect class
                -- as PNL-2 one layer down.
                lagged AS (
                    SELECT date, ticker, turnover,
                           px / NULLIF(LAG(px, 5) OVER (
                               PARTITION BY ticker ORDER BY date), 0) - 1.0 AS ret5
                    FROM base
                ),
                -- ⚠️ Screened at ±50%: silver carries a handful of corrupt cells whose
                -- implied 5-day return reaches -781 (VNX, close_adjust negative for 968
                -- sessions). One such row would set the skew for its whole date.
                clean AS (
                    SELECT * FROM lagged WHERE ret5 IS NOT NULL AND ABS(ret5) <= 0.5
                ),
                -- ⚠️ TWO PASSES, and it is not a style choice: PostgreSQL rejects
                -- `AVG(POWER(x - AVG(x) OVER (...), 3))` with "aggregate function
                -- calls cannot contain window function calls". The centre has to be
                -- computed and joined back before the third and fourth moments can
                -- be taken against it.
                per_date AS (
                    SELECT date,
                           COUNT(*)                AS n_names,
                           STDDEV_SAMP(ret5)       AS xs_disp5,
                           AVG(ret5)               AS xs_mean5,
                           STDDEV_POP(ret5)        AS sd_pop,
                           SUM(turnover)           AS turnover_total,
                           SUM(POWER(turnover, 2)) AS turnover_sq
                    FROM clean GROUP BY date
                ),
                -- Fisher-Pearson skew and EXCESS kurtosis from the population
                -- moments, because PostgreSQL has no SKEW/KURT aggregate.
                moments AS (
                    SELECT c.date,
                           AVG(POWER(c.ret5 - p.xs_mean5, 3)) AS m3,
                           AVG(POWER(c.ret5 - p.xs_mean5, 4)) AS m4
                    FROM clean c JOIN per_date p ON p.date = c.date
                    GROUP BY c.date
                ),
                shaped AS (
                    SELECT p.date, p.n_names, p.xs_disp5, p.xs_mean5, p.turnover_total,
                           CASE WHEN p.sd_pop > 0 THEN m.m3 / POWER(p.sd_pop, 3) END AS xs_skew5,
                           CASE WHEN p.sd_pop > 0 THEN m.m4 / POWER(p.sd_pop, 4) - 3.0 END AS xs_kurt5,
                           CASE WHEN p.turnover_total > 0
                                THEN p.turnover_sq / POWER(p.turnover_total, 2) END AS hhi_turnover
                    FROM per_date p JOIN moments m ON m.date = p.date
                )
                SELECT date,
                       n_names                                       AS mkt_n_names,
                       xs_disp5                                      AS mkt_xs_disp5,
                       xs_skew5                                      AS mkt_xs_skew5,
                       xs_kurt5                                      AS mkt_xs_kurt5,
                       xs_mean5                                      AS mkt_xs_mean5,
                       hhi_turnover                                  AS mkt_hhi_turnover,
                       LN(1.0 + turnover_total)                      AS mkt_log_turnover,
                       LN(1.0 + turnover_total) - LAG(LN(1.0 + turnover_total), 5)
                           OVER (ORDER BY date)                      AS mkt_turnover_z
                FROM shaped ORDER BY date
                """
            )
            cur.execute(
                "ALTER TABLE gold_schema.market_breadth ADD PRIMARY KEY (date)"
            )
            cur.execute("SELECT COUNT(*), MIN(date), MAX(date) FROM gold_schema.market_breadth")
            rows, first, last = cur.fetchone()
            cur.execute(
                "SELECT MIN(mkt_n_names), PERCENTILE_DISC(0.5) WITHIN GROUP "
                "(ORDER BY mkt_n_names) FROM gold_schema.market_breadth"
            )
            narrowest, median_width = cur.fetchone()

        if not rows:
            raise PipelineError(
                "gold.market_breadth is EMPTY — silver.stocks_basic produced no "
                "(date, ret5) pair. Build silver first."
            )
        self._logger.log_info(
            f"gold.market_breadth: {rows} sessions ({first} → {last}), "
            f"cross-section width min {narrowest} / median {median_width}."
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

    # ── gold.bonds — the WIDE yield-curve panel ─────────────────────────────────
    #
    #     {exchange}__{ticker}__{measure}
    #     tvc__vn10y__value,  tvc__vn10y__volatility_21
    #
    # Same `__` convention and the same reason as `gold.stock_market` and
    # `gold.economy`: the measures contain single underscores (`return_simple`,
    # `value_roll_mean_21`), so only a double underscore can be split back.
    #
    # ⚠️ WHY WIDE AT ALL. A yield CURVE is read across tenors on one day — 10y minus
    # 2y is the slope, and the slope is the series that carries macro information.
    # In the long shape that is a self-join per tenor pair; one row per date makes it
    # a subtraction. It is also what a `pool__macro` needs: a feature panel is keyed
    # by date, and every consumer of the long table had to pivot it first.
    GOLD_BONDS_NAME_SEP = "__"

    # ⚠️ EVERY TENOR IS PRESENT TWICE, ALL THE WAY FROM BRONZE. TradingView exposes
    # `TVC:VN01` and `TVC:VN01Y` as separate symbols and the scraper collected both,
    # so silver holds 18 "tickers" that are 9 tenors: 66,100 rows for 33,050
    # observations. Measured — all 9 pairs agree on every shared date, **0 differing
    # values** — but agreement is ASSERTED here per pair rather than trusted, because
    # the day the two spellings diverge is the day one of them is wrong and silently
    # picking either would publish it.
    #
    # The `Y` spelling is the survivor: `VN10Y` reads as the 10-YEAR yield, which is
    # what the series is, and it is TradingView's own canonical government-yield
    # symbol. Set this to `None` to publish both spellings unchanged.
    GOLD_BONDS_DUPLICATE_SUFFIX = "Y"

    def _helper_bonds_drop_duplicate_tenors(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop `VN10` where `VN10Y` carries the same series. Asserts they agree.

        ⚠️ Compared on the RAW `value` and on date COVERAGE, before any feature is
        computed. The derived columns are a deterministic function of `value` within
        a series, so two series with the same values on the same dates cannot
        disagree downstream — checking the input is both sufficient and the only
        place a divergence has a readable cause.
        """
        suffix = self.GOLD_BONDS_DUPLICATE_SUFFIX
        if not suffix:
            return df

        present = set(zip(df["exchange"], df["ticker"]))
        twins = [
            (exchange, plain)
            for exchange, plain in sorted(present)
            if (exchange, f"{plain}{suffix}") in present
        ]
        if not twins:
            return df

        for exchange, plain in twins:
            long_name = f"{plain}{suffix}"
            a = df[(df["exchange"] == exchange) & (df["ticker"] == plain)]
            b = df[(df["exchange"] == exchange) & (df["ticker"] == long_name)]
            a = a.set_index("date")["value"].sort_index()
            b = b.set_index("date")["value"].sort_index()
            if not a.index.equals(b.index):
                only_a, only_b = a.index.difference(b.index), b.index.difference(a.index)
                raise PipelineError(
                    f"gold.bonds: {exchange}:{plain} and {exchange}:{long_name} cover "
                    f"different dates ({len(only_a)} only in {plain}, {len(only_b)} "
                    f"only in {long_name}) — they are not the same series, so "
                    f"dropping either would lose data. Set "
                    f"GOLD_BONDS_DUPLICATE_SUFFIX = None to publish both."
                )
            # NaN == NaN is False, so compare on the null pattern too rather than
            # letting a pair of all-NULL series pass as "differing".
            differing = int(
                ((a != b) & ~(a.isna() & b.isna())).sum()
            )
            if differing:
                raise PipelineError(
                    f"gold.bonds: {exchange}:{plain} and {exchange}:{long_name} "
                    f"disagree on {differing} of {len(a)} dates. They were the same "
                    f"series when this was written; one of them is now wrong and "
                    f"this code cannot tell which. Investigate before publishing."
                )

        dropped = {(e, t) for e, t in twins}
        keep = ~pd.Series(
            list(zip(df["exchange"], df["ticker"])), index=df.index
        ).isin(dropped)
        self._logger.log_info(
            f"gold.bonds: dropped {len(dropped)} duplicate tenor spelling(s) "
            f"({', '.join(t for _, t in twins[:5])}"
            f"{', …' if len(twins) > 5 else ''}) — each verified identical to its "
            f"'{suffix}' twin on every date."
        )
        return df[keep].reset_index(drop=True)

    def _ingest_gold_bonds(self) -> None:
        """`silver.bonds` -> `gold.bonds`: **one row per DATE**, PK `date`, one column
        per (tenor x measure) named `{exchange}__{ticker}__{measure}`.

        9 government tenors x 13 measures = 117 columns on the calendar the data
        itself defines — the distinct dates in silver, not a synthetic business-day
        range.

        ⚠️ **NO as-of fill, the same choice `gold.stock_market` makes and for the same
        reason.** A missing tenor-day means that tenor did not quote (VN15/VN20/VN30
        begin in 2018, thirteen years after VN01), and carrying a yield forward would
        invent a quote. `gold.economy` fills because a macro series is *stale but
        valid* between releases; a quote is not. NULL stays NULL.

        ⚠️ **The features are computed BEFORE the pivot, per series and in date
        order** — `_helper_transform` groups by `(exchange, ticker)`. Computing a
        return after pivoting would be a row-wise difference across the wide frame,
        which is the same arithmetic only as long as no tenor has a gap; VN15 has
        2,089 dates against VN01's 4,441, so it is not the same arithmetic.
        """
        self._logger.log_info("Ingesting gold bonds (wide, 1 row per date)...")

        KEYS = ["exchange", "ticker", "date"]

        df = self._helper_select(
            schema_name=SILVER_SCHEMA,
            table_name="bonds",
            order_by=KEYS,
        )
        if df.empty:
            raise MissingSourceDataError(
                f"{SILVER_SCHEMA}.bonds is empty — run the silver bonds ingest first."
            )

        df["date"] = pd.to_datetime(df["date"]).dt.date
        # ⚠️ `silver.bonds.value` is VARCHAR — the live example the generic builder's
        # docstring names. It must be coerced, not inferred.
        for col in df.columns:
            if col not in KEYS:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = self._helper_bonds_drop_duplicate_tenors(df)

        layers = self._helper_build_feature_layers(df)
        if not layers:
            raise PipelineError(
                "gold.bonds: no feature layer matched silver.bonds' columns, so the "
                "table would be a bare pivot of `value`."
            )
        df = self._helper_transform(df, layers)

        measures = [c for c in df.columns if c not in KEYS]
        # ⚠️ `value_name="observation"`, not `"value"`. One of the MEASURES is itself
        # called `value` (the yield), and pandas refuses a `value_name` that collides
        # with an existing column. `gold.stock_market` never meets this because its
        # measures are `close_adjust`/`n_buy_orders`; a single-value silver table
        # always will.
        long = df.melt(
            id_vars=KEYS,
            value_vars=measures,
            var_name="measure",
            value_name="observation",
        ).dropna(subset=["observation"])

        sep = self.GOLD_BONDS_NAME_SEP
        long["column"] = (
            long["exchange"].map(self._sanitize_identifier)
            + sep
            + long["ticker"].map(self._sanitize_identifier)
            + sep
            + long["measure"].map(self._sanitize_identifier)
        )

        # Sanitising must not merge two tenors, and the result must fit PostgreSQL's
        # identifier limit. Both checked, not assumed — same as `gold.stock_market`.
        pairs = long[["exchange", "ticker", "measure", "column"]].drop_duplicates()
        collided = pairs.groupby("column").size()
        collided = collided[collided > 1]
        if len(collided):
            raise PipelineError(
                f"gold.bonds: {len(collided)} column name(s) are produced by more than "
                f"one (exchange, ticker, measure) — sanitising merged distinct tenors, "
                f"e.g. {list(collided.index[:3])}. Rename before publishing."
            )
        too_long = [
            c for c in pairs["column"].unique()
            if len(c.encode()) > self.PG_IDENTIFIER_LIMIT
        ]
        if too_long:
            raise PipelineError(
                f"{len(too_long)} gold.bonds column name(s) exceed PostgreSQL's "
                f"{self.PG_IDENTIFIER_LIMIT}-byte identifier limit and would be "
                f"TRUNCATED SILENTLY, e.g. {sorted(too_long)[:3]}."
            )

        wide = long.pivot(index="date", columns="column", values="observation")
        wide = wide.sort_index().reset_index()
        wide.columns.name = None

        # Nothing may be lost on the way through: one non-null cell per observation.
        landed = int(wide.drop(columns=["date"]).notna().sum().sum())
        if landed != len(long):
            raise PipelineError(
                f"gold.bonds: {len(long)} observations went into the pivot but "
                f"{landed} cells came out. A duplicate (exchange, ticker, date, "
                f"measure) in silver.bonds is the usual cause."
            )

        cells = len(wide) * (len(wide.columns) - 1)
        self._logger.log_info(
            f"gold bonds: {len(wide)} trading days x {len(wide.columns) - 1} columns, "
            f"{landed} observations "
            f"({100.0 * landed / max(cells, 1):.1f}% of cells filled)."
        )

        self._database_driver.drop_table(GOLD_SCHEMA, "bonds")
        self._helper_save_pandas_table_to_database(
            schema_name=GOLD_SCHEMA,
            table_name="bonds",
            primary_keys=["date"],
            df=wide,
            dtype_overrides={"date": DataType.DATE()},
            chunk_size=1_000,
        )

    # ── gold.forex — the currency panel, one row per date ────────────────────────
    #
    # ⚠️ WHY WIDE, and why it carries NO FEATURES while `bonds` and `funds` do.
    # A currency panel is read ACROSS pairs on one day — USDVND against DXY, or one
    # broker's EURUSD against another's — so one row per date is the shape every
    # cross-rate and carry feature needs. But there are **328 series** (99 pairs
    # quoted by 9 brokers), and at the 13 measures `gold.funds` carries that is 4,264
    # columns against PostgreSQL's hard ceiling of 1,600. When the entity count is
    # large the MEASURE SET is what has to give — which is exactly the trade
    # `gold.economy` already makes: 1,034 series, one `value` column apiece, no
    # features. Anything derived is one `_helper_transform` away from `silver.forex`,
    # which keeps the long grain and every column.
    #
    # ⚠️ THE 9 EXCHANGES ARE 9 BROKERS, NOT DUPLICATE SPELLINGS — do not collapse
    # them the way `_helper_bonds_drop_duplicate_tenors` collapses VN01/VN01Y. Those
    # twins agreed on 100% of shared dates; measured 2026-08-05, SAXO and JFX disagree
    # on **160,781 of 161,816** shared ticker-days (99.4%), and every other broker pair
    # is 95.6-99.9%. They are different feeds taken at different snapshot times, so
    # each is its own series and picking one would be picking a number, not a fix.
    GOLD_FOREX_NAME_SEP = "__"

    # ⚠️ ONE TABLE PER EXCHANGE, AS `gold.economy` IS ONE PER COUNTRY (2026-08-14).
    # The 2026-08-14 re-scrape took the folder from 357 series to **3,074**, and a
    # single wide panel would need 3,075 columns against PostgreSQL's 1,600 — issue
    # `WID-1`. The lever this table already spent (carry `value` alone instead of 13
    # measures) bought room for 328 series and cannot be spent twice, so the split is
    # the same one economy made at 1,034 series and for the identical reason.
    #
    # ⚠️ The EXCHANGE is the split key because it is the only one that divides the
    # series set cleanly and is already in the name (`saxo__eurusd`), so a column keeps
    # its meaning whichever table it lands in.
    GOLD_FOREX_TABLE_PREFIX = "forex_"

    # PostgreSQL's hard maximum columns per table. Named rather than inlined because
    # it is the number three separate designs here have had to bend around —
    # `gold.economy` (3,852 series), `gold.forex` (3,074), `gold.funds`' measure set.
    GOLD_MAX_TABLE_COLUMNS = 1_600

    def _ingest_gold_forex(self) -> List[dict]:
        """`silver.forex` -> `gold.forex_<exchange>`: **one row per DATE**, PK `date`,
        one column per series named `{exchange}__{ticker}`. Returns one dict per panel.

        ⚠️ **ONE TABLE PER EXCHANGE** — see `GOLD_FOREX_TABLE_PREFIX`. 47 panels /
        3,074 series on 2026-08-14, the widest being `forex_fx_idc` at 697 series.
        The un-suffixed `gold.forex` is DROPPED at the end of a successful run, because
        two answers to "what is gold forex" is worse than one migration.

        ⚠️ **No measure in the column name**, unlike `gold.bonds`/`gold.funds`. The
        panel carries exactly one measure, so `saxo__eurusd__value` would repeat
        "value" for every series; `_helper_gold_wide_panel(include_measure=False)`
        enforces that the frame really does have only one.

        ⚠️ **NO as-of fill**, the same call `stock_market`/`bonds`/`funds` make. A gap
        means that broker did not quote that pair that day — B2PRIME starts in 2015,
        fifteen years after SAXO — and carrying a rate forward would invent a quote.
        `gold.economy` fills because a macro release is *stale but valid* until the
        next one; an FX quote is not.

        ⚠️ **DECIMAL, not REAL.** 329 numeric columns is ~3.3 kB, comfortably inside
        PostgreSQL's ~8 kB row limit, so there is no reason to take REAL's ~7
        significant digits — JPY crosses run to five decimal places on values of
        ~1e2, and rounding them would change the rate.
        """
        self._logger.log_info("Ingesting gold forex (wide, 1 row per date)...")

        KEYS = ["exchange", "ticker", "date"]
        panels: List[dict] = []

        with self._database_driver._cursor_ctx() as cur:
            cur.execute(
                f"SELECT exchange, COUNT(DISTINCT ticker) FROM {SILVER_SCHEMA}.forex "
                f"GROUP BY exchange ORDER BY exchange"
            )
            members = [(str(row[0]), int(row[1])) for row in cur.fetchall()]

        if not members:
            raise MissingSourceDataError(
                f"{SILVER_SCHEMA}.forex is empty — run the silver forex ingest first."
            )

        # ⚠️ THE CEILING IS CHECKED BEFORE A SINGLE TABLE IS WRITTEN, not discovered by
        # PostgreSQL halfway through. The split buys a lot of room (widest exchange is
        # FX_IDC at 697 series on 2026-08-14, against 3,074 in total) but it is not
        # unlimited, and the next thing to breach it should say so in these terms.
        over = [(e, n) for e, n in members if n + 1 > self.GOLD_MAX_TABLE_COLUMNS]
        if over:
            raise PipelineError(
                f"{SILVER_SCHEMA}.forex exchange(s) {[e for e, _ in over]} need "
                f"{[n + 1 for _, n in over]} columns against PostgreSQL's "
                f"{self.GOLD_MAX_TABLE_COLUMNS}. The per-exchange split is already "
                f"spent; the next lever is a per-(exchange, base-currency) split or a "
                f"long gold table."
            )

        self._logger.log_info(
            f"Gold forex: {len(members)} exchange panel(s), "
            f"{sum(n for _, n in members)} series total."
        )

        for exchange, series_count in members:
            if not self.UNIFIED_TICKER_PATTERN.match(exchange):
                raise PipelineError(
                    f"{SILVER_SCHEMA}.forex holds exchange {exchange!r}, which cannot "
                    f"safely name a gold table."
                )
            table = f"{self.GOLD_FOREX_TABLE_PREFIX}{exchange.lower()}"

            # ⚠️ ONE EXCHANGE AT A TIME, and that is the memory shape as much as the
            # column shape: the old build read all of `silver.forex` — 12 M rows once
            # the 2026-08-14 scrape landed — into one frame before pivoting.
            df = self._helper_select(
                schema_name=SILVER_SCHEMA,
                table_name="forex",
                conditions=[
                    Condition(
                        column="exchange",
                        operator=SqlOperator.EQUAL_TO,
                        value=exchange,
                        data_type=DataType.VARCHAR(),
                    )
                ],
                order_by=KEYS,
            )
            if df.empty:
                raise MissingSourceDataError(
                    f"{SILVER_SCHEMA}.forex returned no rows for exchange "
                    f"{exchange!r}, which its own DISTINCT said has {series_count} "
                    f"series."
                )

            df["date"] = pd.to_datetime(df["date"]).dt.date
            # The driver hands `numeric` back as `Decimal` (object dtype); an object
            # column melted into the panel would land in gold as VARCHAR.
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

            wide = self._helper_gold_wide_panel(
                df[KEYS + ["value"]],
                table_name=table,
                sep=self.GOLD_FOREX_NAME_SEP,
                keys=KEYS,
                include_measure=False,
            )

            self._database_driver.drop_table(GOLD_SCHEMA, table)
            self._helper_save_pandas_table_to_database(
                schema_name=GOLD_SCHEMA,
                table_name=table,
                primary_keys=["date"],
                df=wide,
                dtype_overrides={"date": DataType.DATE()},
                chunk_size=1_000,
            )
            panels.append(
                {"table": table, "series": series_count, "rows": len(wide)}
            )
            del df, wide

        # ⚠️ THE OLD SINGLE TABLE IS DROPPED LAST, after every panel is written. It is
        # the same name minus a suffix, so leaving it would give two answers to "what is
        # gold forex" — and the one with no suffix is the one every pre-2026-08-14
        # consumer reaches for.
        self._database_driver.drop_table(GOLD_SCHEMA, "forex")
        self._logger.log_info(
            f"gold.{self.GOLD_FOREX_TABLE_PREFIX}*: {len(panels)} panel(s), "
            f"{sum(p['series'] for p in panels)} series."
        )
        return panels

    def _helper_gold_wide_panel(
        self,
        df: pd.DataFrame,
        table_name: str,
        sep: str,
        keys: Optional[List[str]] = None,
        include_measure: bool = True,
    ) -> pd.DataFrame:
        """`(exchange, ticker, date, *measures)` -> **one row per DATE**, one column
        per `{exchange}{sep}{ticker}{sep}{measure}`.

        `include_measure=False` drops the measure from the NAME, for a panel that
        carries exactly one (`gold.forex` is 328 quotes of a single `value`, so
        `saxo__eurusd__value` says "value" 328 times and `saxo__eurusd` does not).
        It is rejected below if the frame actually has more than one measure, because
        the names would silently collide.

        The shared middle of a wide gold panel: melt, name, CHECK the names, pivot,
        and check that nothing was lost on the way through.

        ⚠️ `value_name="observation"`, not `"value"`. A single-series silver table has a
        MEASURE called `value` (`bonds` holds the yield under that name), and pandas
        refuses a `value_name` that collides with an existing column.

        ⚠️ `_ingest_gold_stock_market` and `_ingest_gold_bonds` still INLINE these same
        steps — they were written before this helper. Moving them onto it is a separate
        change, because each has a published exact round-trip check
        (`src/orchestration/CONTEXT.md`) that has to be re-run to prove the move
        preserved every value; a refactor that is only *probably* value-preserving is
        worth less than the duplication it removes.
        """
        keys = keys or ["exchange", "ticker", "date"]
        measures = [c for c in df.columns if c not in keys]

        if not include_measure and len(measures) != 1:
            raise PipelineError(
                f"gold.{table_name}: include_measure=False names a column "
                f"`exchange{sep}ticker`, which is unique only for ONE measure — this "
                f"frame has {len(measures)}: {measures[:5]}. Every measure would "
                f"compete for the same column name."
            )

        long = df.melt(
            id_vars=keys,
            value_vars=measures,
            var_name="measure",
            value_name="observation",
        ).dropna(subset=["observation"])

        long["column"] = (
            long["exchange"].map(self._sanitize_identifier)
            + sep
            + long["ticker"].map(self._sanitize_identifier)
        )
        if include_measure:
            long["column"] += sep + long["measure"].map(self._sanitize_identifier)

        # Sanitising must not merge two entities into one column, and the result must
        # fit PostgreSQL's identifier limit. Both are checked, not assumed: a merge
        # would publish one series under another's name, and an over-long name is
        # TRUNCATED SILENTLY by the server.
        pairs = long[["exchange", "ticker", "measure", "column"]].drop_duplicates()
        collided = pairs.groupby("column").size()
        collided = collided[collided > 1]
        if len(collided):
            raise PipelineError(
                f"gold.{table_name}: {len(collided)} column name(s) are produced by "
                f"more than one (exchange, ticker, measure) — sanitising merged "
                f"distinct series, e.g. {list(collided.index[:3])}. Rename before "
                f"publishing."
            )
        too_long = [
            c for c in pairs["column"].unique()
            if len(c.encode()) > self.PG_IDENTIFIER_LIMIT
        ]
        if too_long:
            raise PipelineError(
                f"{len(too_long)} gold.{table_name} column name(s) exceed PostgreSQL's "
                f"{self.PG_IDENTIFIER_LIMIT}-byte identifier limit and would be "
                f"TRUNCATED SILENTLY, e.g. {sorted(too_long)[:3]}."
            )

        wide = long.pivot(index="date", columns="column", values="observation")
        wide = wide.sort_index().reset_index()
        wide.columns.name = None

        # Nothing may be lost on the way through: one non-null cell per observation.
        landed = int(wide.drop(columns=["date"]).notna().sum().sum())
        if landed != len(long):
            raise PipelineError(
                f"gold.{table_name}: {len(long)} observations went into the pivot but "
                f"{landed} cells came out. A duplicate (exchange, ticker, date, "
                f"measure) in the silver source is the usual cause."
            )

        cells = len(wide) * (len(wide.columns) - 1)
        self._logger.log_info(
            f"gold {table_name}: {len(wide)} trading days x {len(wide.columns) - 1} "
            f"columns, {landed} observations "
            f"({100.0 * landed / max(cells, 1):.1f}% of cells filled)."
        )
        return wide

    # ── gold.funds — the ETF panel, one row per date ─────────────────────────────
    #
    # ⚠️ WHY WIDE. A fund panel is read ACROSS funds on one day: FUEVFVND against
    # E1VFVN30 is the VN-Diamond-versus-VN30 spread, and every rotation, relative-
    # strength or tracking-error feature is a comparison between two funds on the
    # same date. In the long shape that is a self-join per pair; one row per date
    # makes it a subtraction. It is also the shape a feature panel takes — keyed by
    # date — so every consumer of the long table had to pivot it first.
    #
    # This REPLACES the generic `_ingest_gold_table("funds")` output (18,662 x 22,
    # one row per fund-day), which is the same decision `gold.economy` took on
    # 2026-08-01 when the wide panel took the name from the long feature table.
    GOLD_FUNDS_NAME_SEP = "__"

    def _ingest_gold_funds(self) -> None:
        """`silver.funds` -> `gold.funds`: **one row per DATE**, PK `date`, one column
        per (fund x measure) named `{exchange}__{ticker}__{measure}`.

        19 HOSE ETFs x up to 19 measures (OHLCV + the 14 standard feature columns) on
        the calendar the data itself defines — the distinct dates in silver, not a
        synthetic business-day range. Vietnamese exchange holidays are not weekends.

        ⚠️ **"UP TO" IS LITERAL: the column COUNT is a function of the data, not of the
        measure list.** 19 x 19 is 361; the table has 351. The ten absentees are all
        FUEBFVND's rolling and volatility columns, and FUEBFVND has **3 rows** — a
        5-day window and a 21-day volatility cannot yield one non-null value from
        three observations, so the melt's `dropna` removes them and the column is
        never created. That is right (an all-NULL column is noise a model has to
        learn to ignore), but it means a fund gaining history GAINS COLUMNS, i.e. a
        DDL change. Acceptable in gold, which is where the wide shape is allowed to
        live; it is exactly the argument that kept `silver.economy` long.

        ⚠️ **NO as-of fill, the same choice `gold.stock_market` and `gold.bonds` make
        and for the same reason.** A missing fund-day means that ETF did not exist yet
        (FUETPVND lists in 2025, eleven years after E1VFVN30) or did not trade, and
        carrying a NAV forward would invent a price. `gold.economy` fills because a
        macro series is *stale but valid* between releases; a quote is not. NULL stays
        NULL — which is why this panel is ~34% filled, and that number is the listing
        history, not a defect.

        ⚠️ **The features are computed BEFORE the pivot, per fund and in date order** —
        `_helper_transform` groups by `(exchange, ticker)`. Computing a return after
        pivoting would be a row-wise difference across the wide frame, which is the
        same arithmetic only as long as no fund has a gap; FUEBFVND has 3 dates
        against E1VFVN30's 2,894, so it is emphatically not the same arithmetic.
        """
        self._logger.log_info("Ingesting gold funds (wide, 1 row per date)...")

        KEYS = ["exchange", "ticker", "date"]

        df = self._helper_select(
            schema_name=SILVER_SCHEMA,
            table_name="funds",
            order_by=KEYS,
        )
        if df.empty:
            raise MissingSourceDataError(
                f"{SILVER_SCHEMA}.funds is empty — run the silver funds ingest first."
            )

        df["date"] = pd.to_datetime(df["date"]).dt.date
        # The driver hands `numeric` back as `Decimal` (object dtype) and `bigint` as
        # object too; an object column melted into the panel would land as VARCHAR.
        for col in df.columns:
            if col not in KEYS:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        layers = self._helper_build_feature_layers(df)
        if not layers:
            raise PipelineError(
                "gold.funds: no feature layer matched silver.funds' columns, so the "
                "table would be a bare pivot of the OHLCV block."
            )
        df = self._helper_transform(df, layers)

        wide = self._helper_gold_wide_panel(
            df, table_name="funds", sep=self.GOLD_FUNDS_NAME_SEP, keys=KEYS
        )

        self._database_driver.drop_table(GOLD_SCHEMA, "funds")
        self._helper_save_pandas_table_to_database(
            schema_name=GOLD_SCHEMA,
            table_name="funds",
            primary_keys=["date"],
            df=wide,
            dtype_overrides={"date": DataType.DATE()},
            chunk_size=1_000,
        )

    # ⚠️ `_ingest_gold_indices` was REMOVED (2026-08-01) along with its
    # `data_quality_gold/indices` leaf and switch key. `gold.indices` was the
    # TradingView index series (24,095 x 22, the generic single-series feature build)
    # and it duplicated `gold.stock_market`, which covers the same six Vietnamese
    # indices from CafeF with 27 measures apiece instead of OHLCV. `silver.indices` and
    # `bronze.trading_view_indices` are untouched — only the gold table is retired, so
    # nothing upstream loses its history and restoring it is one line. The
    # `gold_schema.indices` table was dropped the same day, so the schema matches the
    # code.

    # ── FILTER — the universe screen, gold/silver -> filter_schema ─────────────
    #
    # ⚠️ THE FIFTH LAYER, AND THE ONLY ONE THAT PRODUCES NO TIME SERIES. It answers the
    # one question sitting between gold and unified: WHICH TICKERS ARE ALLOWED IN. The
    # WHAT — every condition, every threshold, every window — lives in
    # `orchestration/preprocessor/filters.py`; this method is only the HOW, and that
    # split is the point. Adding a filter is an entry in a registry, never a change here.
    #
    # ⚠️ A SCREEN IS NOT POINT-IN-TIME. Membership is decided from a window and applied
    # to the whole history of every pool built on it — CLAUDE.md 2c's defect in its
    # purest form, benign for a within-date shuffle null and fatal for any CAGR. The
    # windows are written into the table COMMENT so a later reader can see which one
    # chose the basket. `filters.py`'s docstring is the full argument.

    def _ingest_filter_universe(self, screen: str) -> dict:
        """A screen -> `filter_schema.universe__<screen>`. Returns a metadata dict.

        ⚠️ **EVERY CANDIDATE IS WRITTEN, NOT ONLY THE SURVIVORS.** One row per
        `(exchange, ticker)` in `silver.stocks_basic` — 781 today — carrying every
        condition's measured `val__*`, its `pass__*`, the conjunction `passes`, and
        `first_failed`. A table of survivors answers "who is in" and nothing else; this
        one answers "why is HPG out", which is the question anyone moving a threshold
        actually has.

        ⚠️ **`CREATE TABLE AS`, not a pandas round-trip** — CLAUDE.md 5 rule 15.
        psycopg2 hands `numeric` back as `Decimal`, a DataFrame carries that as dtype
        `object`, and the writer maps `object` to VARCHAR; a read-then-write would turn
        every measurement into TEXT while looking like it worked.

        ⚠️ **THE TABLE IS DROPPED AS LATE AS POSSIBLE**, so a failing screen leaves the
        previous universe intact — the same ordering `_ingest_gold_table` and
        `_ingest_unified_pool_basic` use. A half-built screen is worse than a stale one
        because `pool__basic` would silently build on it.
        """
        scr = filter_registry.screen(screen)
        conditions = scr.resolve()
        self._logger.log_info(
            f"Ingesting filter {scr.qualified_table} "
            f"({len(conditions)} condition(s): "
            f"{', '.join(c.name for c in conditions)})..."
        )

        # Every source must exist before anything is dropped. A screen naming a table
        # that is not built yet would otherwise fail INSIDE the CTAS, after the old
        # universe had already gone.
        for condition in conditions:
            source_schema, source_table = condition.source.split(".", 1)
            if not self._helper_column_types(source_schema, source_table):
                raise MissingSourceDataError(
                    f"Screen {scr.name!r} condition {condition.name!r} reads "
                    f"`{condition.source}`, which does not exist. Build that layer "
                    f"first."
                )

        self._database_driver.create_schema(FILTER_SCHEMA)
        sql, params = filter_registry.build_universe_sql(scr)
        comment = filter_registry.build_comment(scr)

        with self._database_driver._cursor_ctx() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM ("
                f"  SELECT DISTINCT exchange, ticker "
                f"  FROM {filter_registry.CANDIDATE_SOURCE}) s"
            )
            candidates = int(cur.fetchone()[0])
            if not candidates:
                raise MissingSourceDataError(
                    f"`{filter_registry.CANDIDATE_SOURCE}` holds no tickers, so "
                    f"{scr.qualified_table} would be empty. Build silver "
                    f"`stocks_basic` first."
                )

            cur.execute(f"DROP TABLE IF EXISTS {scr.qualified_table}")
            cur.execute(sql, params)
            # CTAS copies types but never constraints. `(exchange, ticker)` is the grain
            # and asserting it is what proves the LEFT JOINs stayed one-to-one — a
            # condition CTE returning two rows for one ticker would otherwise duplicate
            # that ticker silently and hand `pool__basic` a doubled member list.
            cur.execute(
                f"ALTER TABLE {scr.qualified_table} ADD PRIMARY KEY (exchange, ticker)"
            )
            cur.execute(f"COMMENT ON TABLE {scr.qualified_table} IS %s", (comment,))

            cur.execute(
                f"SELECT COUNT(*), COUNT(*) FILTER (WHERE passes), "
                f"       COUNT(*) FILTER (WHERE passes IS NULL) "
                f"FROM {scr.qualified_table}"
            )
            written, selected, unknown = (int(x) for x in cur.fetchone())

            # Per condition: how many were MEASURED at all, and how many passed. Rule 22
            # one level down — a condition that measured nothing and a condition that
            # everything cleared both report 100% pass, and only the first number tells
            # them apart. `debt_to_equity_max_12` is exactly that case at 2 of 781.
            measured: dict = {}
            passed: dict = {}
            for condition in conditions:
                cur.execute(
                    f'SELECT COUNT("{condition.value_column}"), '
                    f'       COUNT(*) FILTER (WHERE "{condition.pass_column}") '
                    f"FROM {scr.qualified_table}"
                )
                measured[condition.name], passed[condition.name] = (
                    int(x) for x in cur.fetchone()
                )

            # Why each rejected name was rejected, in screen order. The audit the
            # `first_failed` column exists for, summarised so it reaches a log line.
            cur.execute(
                f"SELECT first_failed, COUNT(*) FROM {scr.qualified_table} "
                f"WHERE NOT passes GROUP BY 1 ORDER BY 2 DESC"
            )
            rejected_by = {row[0]: int(row[1]) for row in cur.fetchall()}

        if written != candidates:
            raise PipelineError(
                f"{scr.qualified_table} wrote {written} rows against "
                f"{candidates} candidate ticker(s). A screen may only mark a name, "
                f"never add or lose one — a mismatch means a condition CTE is not "
                f"one row per (exchange, ticker)."
            )
        # ⚠️ `passes` MUST BE TOTAL. A NULL there reads downstream as "not selected"
        # while meaning "not known", and `_pass_expression`'s IS NULL / IS NOT NULL
        # guards exist precisely so three-valued logic cannot reach this column.
        if unknown:
            raise PipelineError(
                f"{scr.qualified_table} has {unknown} row(s) with `passes` NULL. Every "
                f"condition must resolve to TRUE or FALSE — check `on_missing`."
            )
        if not selected:
            raise MissingSourceDataError(
                f"Screen {scr.name!r} selected 0 of {written} tickers, so "
                f"`unified_schema_{scr.slug}` would be empty. Rejections by first "
                f"failed condition: {rejected_by or 'none'}."
            )

        self._logger.log_info(
            f"{scr.qualified_table}: {selected} of {written} ticker(s) pass "
            f"({100.0 * selected / max(written, 1):.1f}%); rejected by "
            + (
                ", ".join(f"{name} {n}" for name, n in rejected_by.items())
                or "nothing"
            )
        )
        return {
            "screen": scr.name,
            "table": scr.qualified_table,
            "candidates": written,
            "selected": selected,
            "conditions": [c.name for c in conditions],
            "measured": measured,
            "passed": passed,
            "rejected_by": rejected_by,
            "comment": comment,
        }

    def _helper_filter_universe_exists(self, screen: str) -> bool:
        """Has this screen been materialised?

        Asked BEFORE a unified build rather than after, so the error names the command
        to run instead of surfacing as `relation "filter_schema.universe__x" does not
        exist` from inside a 200-line CTAS.
        """
        scr = filter_registry.screen(screen)
        return bool(self._helper_column_types(FILTER_SCHEMA, scr.table))

    def _helper_filter_universe_members(self, screen: str) -> List[Tuple[str, str]]:
        """`[(exchange, ticker), ...]` this screen selected, for logs and assertions."""
        scr = filter_registry.screen(screen)
        with self._database_driver._cursor_ctx() as cur:
            cur.execute(
                f"SELECT exchange, ticker FROM {scr.qualified_table} "
                f"WHERE passes ORDER BY exchange, ticker"
            )
            return [(row[0], row[1]) for row in cur.fetchall()]

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
    # ⚠️ **`VN30` IS TODAY'S MEMBERSHIP AND IS NOT POINT-IN-TIME.** `vn30.csv` is a
    # single snapshot with no history, so this predicate selects the 30 companies that
    # turned out to be index members in 2026 — SSB listed in 2021-03 and is in the list;
    # every name that was in the VN30 during the sample and later dropped out is NOT.
    # That is survivorship AND look-ahead in the form CLAUDE.md §2c calls the worst,
    # and it is why `pool__basic_vn30` was previously deferred rather than built.
    # It is materialised anyway because a VN30 experiment was asked for explicitly; the
    # honest comparator is a liquidity-ranked top-30 chosen with `liquidity_before`,
    # which is what `kgpu.export` already does for the top-150 panel.
    # ⚠️ Anything quoting a CAGR off this universe is quoting a biased number. A `z`
    # against a within-date shuffle is protected (every draw sees the same basket).
    UNIFIED_VN30 = "VN30"

    #: The VN30 constituents as of `vn30.csv`. Frozen here rather than read from the CSV
    #: at call time so a partition's membership cannot change under an already-built
    #: table without a code change that shows up in `git diff`.
    UNIFIED_VN30_TICKERS = (
        "ACB", "BCM", "BID", "BVH", "CTG", "FPT",
        "GAS", "GVR", "HDB", "HPG", "MBB", "MSN",
        "MWG", "PLX", "POW", "SAB", "SHB", "SSB",
        "SSI", "STB", "TCB", "TPB", "VCB", "VHM",
        "VIB", "VIC", "VJC", "VNM", "VPB", "VRE",
    )

    UNIFIED_MEMBER_FILTERS = {
        UNIFIED_UNIVERSE: (None, ()),
        UNIFIED_BANK: ("industry_code = %s", ("401010",)),
        UNIFIED_VN30: ("ticker = ANY(%s)", (list(UNIFIED_VN30_TICKERS),)),
        # ⚠️ EVERY SCREEN IS A SENTINEL TOO, and they are ADDED HERE rather than written
        # out, so `filters.SCREENS` is the single place a universe is defined. The
        # predicate is a SUB-SELECT over the screen's table, not an inlined ticker list:
        # re-materialise the filter layer, rebuild `pool__basic`, and membership tracks
        # it. The three above stay literal because they are not screens — `ALL` has no
        # predicate at all, `BANK` reads a GICS code silver already carries, and `VN30`
        # is a frozen list whose staleness is the thing being made visible.
        **{
            name: filter_registry.member_predicate(name)
            for name in filter_registry.SCREENS
        },
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
        # ⚠️ A SCREEN'S PREDICATE READS A TABLE, so the table has to be there. Checked
        # HERE, at the one place every unified builder resolves membership, because the
        # alternative is PostgreSQL raising `relation does not exist` from inside a CTAS
        # — a message that names neither the screen nor the command that builds it.
        if filter_registry.is_screen(key) and not self._helper_filter_universe_exists(
            key
        ):
            scr = filter_registry.screen(key)
            raise MissingSourceDataError(
                f"Screen {key!r} has no universe table - `{scr.qualified_table}` does "
                f"not exist, so `unified_schema_{scr.slug}` cannot know its members. "
                f"Build the filter layer first:\n  dagster asset materialize -f "
                f"src/orchestration/definitions.py --select \"filter/universe\" "
                f"--partition {key}"
            )
        if key in self.UNIFIED_MEMBER_FILTERS:
            predicate, params = self.UNIFIED_MEMBER_FILTERS[key]
            return (predicate or "", params)
        return ("ticker = %s", (ticker,))

    @staticmethod
    def _helper_unified_describe_predicate(predicate: str, params: tuple) -> str:
        """A member predicate with its bound values inlined, FOR LOGGING ONLY.

        ⚠️ Never used to build SQL — the real statement binds every one of these, and
        this exists so the assertion that fails names the same scope the ingest logged.

        ⚠️ **ANY NUMBER OF PARAMETERS, INCLUDING ZERO.** A screen's predicate is a
        sub-select over `filter_schema.universe__<screen>` and binds nothing, so the
        one-parameter version this replaced raised on it.
        """
        text = predicate
        for value in params or ():
            text = text.replace("%s", repr(value), 1)
        return text

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

    # Gold keeps one macro panel per country because all 19 panels together exceed
    # PostgreSQL's 1,600-column limit.  Unified preserves that storage shape: one
    # `pool__economy_<country>` table per gold panel, all on the same stock-date spine.
    # A single combined `pool__economy` table would recreate the 3,784-column failure
    # that made gold split its panel in the first place.
    UNIFIED_ECONOMY_SOURCE_PREFIX = "economy_"
    UNIFIED_ECONOMY_POOL_PREFIX = "pool__economy_"

    def _ingest_unified_pool_economy(self, ticker: str) -> List[dict]:
        """All `gold.economy_<country>` panels → matching unified economy pools.

        Each target is `pool__economy_<country>`, keyed `(date, exchange, ticker)` on
        `pool__basic`'s calendar.  Gold already applied the publication lag, as-of carry
        and staleness cap, so values are copied as-is rather than forward-filled again.
        Returns one metadata dict per country panel.
        """
        schema = self._helper_unified_schema(ticker)
        if not self._helper_column_types(schema, "pool__basic"):
            raise MissingSourceDataError(
                f"`{schema}.pool__basic` does not exist — build it first; it is the "
                "calendar spine for every unified feature pool."
            )

        with self._database_driver._cursor_ctx() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                "AND LEFT(table_name, CHAR_LENGTH(%s)) = %s ORDER BY table_name",
                (
                    GOLD_SCHEMA,
                    self.UNIFIED_ECONOMY_SOURCE_PREFIX,
                    self.UNIFIED_ECONOMY_SOURCE_PREFIX,
                ),
            )
            source_tables = [str(row[0]) for row in cur.fetchall()]
            cur.execute(f"SELECT COUNT(*) FROM {schema}.pool__basic")
            spine_rows = int(cur.fetchone()[0])

        if not source_tables:
            raise MissingSourceDataError(
                f"`{GOLD_SCHEMA}.economy_<country>` panels do not exist — build gold "
                "`economy` first."
            )
        if not spine_rows:
            raise MissingSourceDataError(
                f"`{schema}.pool__basic` is empty, so the economy pools would have no "
                "calendar to join."
            )

        # Fully preflight every source before replacing any target. This catches a
        # missing, empty, widened, or calendar-drifted gold panel without leaving a
        # half-refreshed collection of unified economy tables behind.
        plans: List[dict] = []
        reference_source = None
        for source_table in source_tables:
            if not source_table.startswith(self.UNIFIED_ECONOMY_SOURCE_PREFIX):
                raise PipelineError(
                    f"Unexpected gold economy table {source_table!r}; expected names "
                    f"starting with {self.UNIFIED_ECONOMY_SOURCE_PREFIX!r}."
                )
            country = source_table.removeprefix(self.UNIFIED_ECONOMY_SOURCE_PREFIX)
            if not self.UNIFIED_TICKER_PATTERN.match(country):
                raise PipelineError(
                    f"Gold economy table {source_table!r} has an unsafe country suffix "
                    f"{country!r}; it cannot safely name a unified pool."
                )

            source = f"{GOLD_SCHEMA}.{source_table}"
            source_types = self._helper_column_types(GOLD_SCHEMA, source_table)
            if "date" not in source_types:
                raise MissingSourceDataError(
                    f"`{source}` has no `date` column, so it cannot join the unified "
                    "trading-day spine."
                )
            macro_columns = [column for column in source_types if column != "date"]
            if not macro_columns:
                raise MissingSourceDataError(f"`{source}` contains no macro columns.")
            duplicate_keys = set(macro_columns) & set(self.UNIFIED_PRIMARY_KEY)
            if duplicate_keys:
                raise PipelineError(
                    f"`{source}` reuses unified key column(s) {sorted(duplicate_keys)}. "
                    "A macro panel may contribute only feature columns beside `date`."
                )

            with self._database_driver._cursor_ctx() as cur:
                cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT date) FROM {source}")
                source_rows, source_dates = (int(value) for value in cur.fetchone())
                if not source_rows:
                    raise MissingSourceDataError(f"`{source}` is empty.")
                if source_rows != source_dates:
                    raise PipelineError(
                        f"`{source}` has {source_rows} rows over {source_dates} dates. "
                        "It must be one row per date before joining a unified pool."
                    )
                if reference_source:
                    cur.execute(
                        f"SELECT COUNT(*) FROM ("
                        f"  (SELECT date FROM {source} EXCEPT SELECT date FROM {reference_source}) "
                        f"  UNION ALL "
                        f"  (SELECT date FROM {reference_source} EXCEPT SELECT date FROM {source})"
                        f") calendar_drift"
                    )
                    calendar_drift = int(cur.fetchone()[0])
                    if calendar_drift:
                        raise PipelineError(
                            f"`{source}` disagrees with `{reference_source}` on "
                            f"{calendar_drift} calendar date(s). Gold economy panels must "
                            "share one calendar before unified pools can be comparable."
                        )
                else:
                    reference_source = source

                cur.execute(
                    f"SELECT COUNT(*) FROM {schema}.pool__basic b "
                    f"JOIN {source} e ON e.date = b.date"
                )
                matched_rows = int(cur.fetchone()[0])
                if not matched_rows:
                    raise MissingSourceDataError(
                        f"`{source}` shares no dates with `{schema}.pool__basic`."
                    )

            plans.append(
                {
                    "source": source,
                    "source_table": source_table,
                    "table": f"{self.UNIFIED_ECONOMY_POOL_PREFIX}{country}",
                    "macro_columns": macro_columns,
                }
            )

        self._logger.log_info(
            f"Ingesting {len(plans)} unified economy pools in {schema} from "
            f"{GOLD_SCHEMA}.economy_<country>..."
        )
        panels: List[dict] = []
        for plan in plans:
            source = plan["source"]
            table = plan["table"]
            macro_columns = plan["macro_columns"]
            selected_macro_columns = ", ".join(
                f"e.{column}" for column in macro_columns
            )
            any_value = " OR ".join(
                f"{column} IS NOT NULL" for column in macro_columns
            )

            with self._database_driver._cursor_ctx() as cur:
                # Drop only after every source has passed preflight. Each rebuild is
                # scoped to its own table, leaving unrelated unified pools untouched.
                cur.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
                cur.execute(
                    f"CREATE TABLE {schema}.{table} AS "
                    f"SELECT b.date, b.exchange, b.ticker, {selected_macro_columns} "
                    f"FROM {schema}.pool__basic b "
                    f"LEFT JOIN {source} e ON e.date = b.date"
                )
                self._helper_unified_primary_key(cur, schema, table)
                cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                written = int(cur.fetchone()[0])
                cur.execute(f"SELECT COUNT(*) FROM {schema}.{table} WHERE {any_value}")
                populated_rows = int(cur.fetchone()[0])
                cur.execute(
                    f"SELECT COUNT(*) FROM ("
                    f"  (SELECT date, exchange, ticker FROM {schema}.{table} "
                    f"   EXCEPT SELECT date, exchange, ticker FROM {schema}.pool__basic) "
                    f"  UNION ALL "
                    f"  (SELECT date, exchange, ticker FROM {schema}.pool__basic "
                    f"   EXCEPT SELECT date, exchange, ticker FROM {schema}.{table})"
                    f") unaligned"
                )
                unaligned = int(cur.fetchone()[0])

            if written != spine_rows:
                raise PipelineError(
                    f"{schema}.{table} wrote {written} rows against pool__basic's "
                    f"{spine_rows}. A macro pool adds columns, never rows."
                )
            if unaligned:
                raise PipelineError(
                    f"{schema}.{table} disagrees with pool__basic on {unaligned} "
                    "unified key(s). Every pool must share the spine exactly."
                )
            if not populated_rows:
                raise MissingSourceDataError(
                    f"{schema}.{table} has no populated macro value on pool__basic's "
                    "calendar."
                )
            panels.append(
                {
                    "source": source,
                    "table": table,
                    "rows": written,
                    "features": len(macro_columns),
                    "populated_rows": populated_rows,
                }
            )

        self._logger.log_info(
            f"{schema}.pool__economy_*: {len(panels)} country panels, "
            f"{sum(panel['features'] for panel in panels)} macro series total."
        )
        return panels

    # ── The DATE-BROADCAST pools: pool__forex, pool__funds, pool__bonds ─────────
    #
    # ⚠️ THESE ARE THE ECONOMY SHAPE, NOT THE TA/FA ONE, AND THE SOURCE'S KEY IS WHAT
    # DECIDES THAT. `gold.forex` / `gold.funds` / `gold.bonds` are keyed on `date`
    # ALONE — one row per trading day, one column per `{exchange}__{ticker}[__{measure}]`
    # — so they join the spine on `date` and BROADCAST across tickers, exactly like a
    # macro panel and unlike `gold.stocks_ta`, which already carries
    # `(date, exchange, ticker)` and is INNER JOINed on all three by
    # `_helper_unified_pool_from_source`.
    #
    # The assertion differs with the shape, which is the reason the two families do not
    # share a body: a broadcast pool must hold EXACTLY the spine's rows, where a
    # per-ticker pool is allowed the one-sided subset (its source may cover fewer
    # TICKERS than the universe).
    #
    # All three stay ONE table where economy is 19 — 360, 392 and 120 columns, well
    # inside PostgreSQL's 1,600 — and none has a natural split key anyway: a broker is
    # not a country, and neither is an ETF or a tenor.
    UNIFIED_FOREX_SOURCE = f"{GOLD_SCHEMA}.forex"
    UNIFIED_FUNDS_SOURCE = f"{GOLD_SCHEMA}.funds"
    UNIFIED_BONDS_SOURCE = f"{GOLD_SCHEMA}.bonds"
    UNIFIED_STOCK_MARKET_SOURCE = f"{GOLD_SCHEMA}.stock_market"
    UNIFIED_MARKET_BREADTH_SOURCE = f"{GOLD_SCHEMA}.market_breadth"

    # ⚠️ Columns of `gold.market_breadth` that are DIAGNOSTICS, not candidate features.
    # `mkt_n_names` is the width each date's statistics were computed over; it rises
    # ~380 → ~771 across the sample purely because tickers listed and because silver
    # holds no delisted name, so it is a CALENDAR PROXY — `close_adjust`'s trap in a new
    # column. Kept in gold (a reader needs the width), blocked from the pool.
    UNIFIED_MARKET_BREADTH_NOT_FEATURES = ("mkt_n_names",)

    def _ingest_unified_pool_forex(self, ticker: str) -> List[dict]:
        """`gold.forex_<exchange>` → `…​.pool__forex_<exchange>` — the FX block.

        **One pool per exchange**, each holding that broker's pairs as
        `{exchange}__{ticker}` columns (e.g. `saxo__eurusd`) with no measure suffix,
        keyed `(date, exchange, ticker)` on `pool__basic`'s calendar. Returns one
        metadata dict per panel.

        ⚠️ **IT WAS ONE TABLE UNTIL 2026-08-14** (`pool__forex`, 357 pairs). The
        re-scrape took forex to 3,074 series, gold split per exchange to stay under
        PostgreSQL's 1,600 columns (`WID-1`), and this followed — exactly as
        `pool__economy_<country>` follows `gold.economy_<country>`, and for the same
        reason. **A caller still naming `pool__forex` is naming a table that no longer
        exists**; `UnifiedSchemaReader.join(["pool__forex_saxo", …])` is the new shape.

        ⚠️ **NOT FORWARD-FILLED, and the NULLs are the honest answer.** `gold.forex` is
        unfilled by construction — a NULL means that broker did not quote that pair that
        day — and filling one here would invent a price. Median coverage on VCB's
        4,266-row spine is **67%** (min 4.3%, max 97.5%; 250 of 357 series above 50%),
        so a consumer must decide how to impute rather than discover the table already
        did. `train_test_creator` imputes with the TRAIN-slice median for exactly this.

        ⚠️ **THE 9 EXCHANGES ARE 9 BROKERS AND MUST NOT BE COLLAPSED.** 99 distinct
        pairs are quoted 357 times between them, and SAXO vs JFX disagree on 160,781 of
        161,816 shared ticker-days (measured 2026-08-05). Deduplicating by pair name
        would be picking one broker's book at random.

        ⚠️ **MOST OF THE SOURCE IS STALE, and the table's MAX(date) hides it.** The
        2026-08-05 scrape ran with `skip_existing=True`: 29 series reach 2026-08-04 and
        **328 stop at 2026-06-08/09**. On VCB's spine that leaves 43 trading days on
        which 328 of 357 columns are NULL. The pool is correct; the source is behind.
        Re-scrape with `TradingViewDataConfig.skip_existing=False` before leaning on the
        recent tail.

        ⚠️ **These are LEVELS.** An FX rate against a forward stock return is the same
        trap `close_adjust` is — a level "predicts" a level at ρ≈0.996 — so the
        representation (`diff` / `zscore` / returns) belongs to the selection step, the
        way it does for `pool__economy`. This method copies values as-is.
        """
        prefix = self.GOLD_FOREX_TABLE_PREFIX
        with self._database_driver._cursor_ctx() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                "AND LEFT(table_name, CHAR_LENGTH(%s)) = %s ORDER BY table_name",
                (GOLD_SCHEMA, prefix, prefix),
            )
            sources = [str(row[0]) for row in cur.fetchall()]

        if not sources:
            raise MissingSourceDataError(
                f"`{GOLD_SCHEMA}.{prefix}<exchange>` panels do not exist — build gold "
                f"`forex` first. ⚠️ The single `{GOLD_SCHEMA}.forex` table was SPLIT "
                f"per exchange on 2026-08-14 (issue WID-1); a chain still expecting it "
                f"is reading a name that no longer exists."
            )

        panels: List[dict] = []
        for source_table in sources:
            exchange = source_table.removeprefix(prefix)
            if not self.UNIFIED_TICKER_PATTERN.match(exchange):
                raise PipelineError(
                    f"Gold forex table {source_table!r} has an unsafe exchange suffix "
                    f"{exchange!r}; it cannot safely name a unified pool."
                )
            panels.append(
                self._helper_unified_pool_on_date_spine(
                    ticker,
                    f"pool__forex_{exchange}",
                    f"{GOLD_SCHEMA}.{source_table}",
                    noun="FX pair",
                )
            )
        # ⚠️ THE PRE-SPLIT `pool__forex` IS DROPPED LAST, after every panel is written,
        # for the reason `_ingest_gold_forex` drops its own: the un-suffixed name is
        # what every pre-2026-08-14 consumer reaches for, and a stale 357-column table
        # sitting beside 48 fresh ones answers the same question two ways. Dropping it
        # only on success means a failed rebuild leaves the old table intact.
        schema = self._helper_unified_schema(ticker)
        with self._database_driver._cursor_ctx() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {schema}.pool__forex")

        self._logger.log_info(
            f"{schema}.pool__forex_*: {len(panels)} exchange panels, "
            f"{sum(p['features'] for p in panels)} pairs total."
        )
        return panels

    def _ingest_unified_pool_funds(self, ticker: str) -> dict:
        """`gold.funds` → `unified_schema_<ticker>.pool__funds` — the VN ETF block.

        **21 HOSE-listed ETFs × up to 19 measures = 389 columns**, named
        `{exchange}__{ticker}__{measure}` (e.g. `hose__e1vfvn30__close`,
        `hose__fuevfvnd__volatility_21`), keyed `(date, exchange, ticker)` on
        `pool__basic`'s calendar. Returns a metadata dict.

        ⚠️ **THE MEASURE SUFFIX IS PRESENT HERE AND ABSENT IN `pool__forex`**, and that
        is gold's naming, not a choice made here: `gold.funds` carries 19 measures per
        fund (OHLC, volume, `return_simple`/`return_log`, `range_hl`/`body_oc`,
        `volatility_{5,21}`, `close_roll_{mean,std,min,max}_{5,21}`) where `gold.forex`
        carries one. 21 × 19 = 399 minus **10 never written** — FUEBFVND has 3 rows and
        cannot fill a 5- or 21-day window, and an all-NULL column is not created.

        ⚠️ **EVERY MEASURE IS TRAILING — verified, not assumed.** `add_returns` is
        `pct_change()`, `add_return_volatility` is `log(p/p.shift(1)).rolling(w).std()`
        and `add_rolling_statistics` is a bare `series.rolling(w)` with no `center=True`
        (`ta/ta_functions.py`). A forward-looking measure here would be a label wearing a
        feature's name, so it is checked rather than trusted.

        ⚠️ **THE SOURCE STARTS 2014-10-06 AND THE VCB SPINE STARTS 2009-06-30**, so
        **1,351 of 4,266 spine rows (31.7%) get no fund row at all** — those are NULL by
        construction, not by a missing scrape. With most VN ETFs listing after 2020 the
        per-column coverage is far thinner than `pool__forex`'s: **min 0.02%, median
        17.7%, max 67.7%**, none all-NULL. A selection over this pool is mostly a
        selection over the last five years.

        ⚠️ **E1VFVN30 IS THE VN30 INDEX WEARING A TICKER.** An ETF's same-day close is
        the market factor — the very thing `pool__targets.return_rel_{h}day` SUBTRACTS —
        so a `close` column here against an absolute forward return is the market
        predicting the market. Causally clean (nothing is forward-looking); economically
        the same "level predicts level" trap `pool__forex` carries.
        """
        return self._helper_unified_pool_on_date_spine(
            ticker, "pool__funds", self.UNIFIED_FUNDS_SOURCE, noun="fund series"
        )

    def _ingest_unified_pool_bonds(self, ticker: str) -> dict:
        """`gold.bonds` → `unified_schema_<ticker>.pool__bonds` — the yield curve.

        **9 VN government tenors × 13 measures = 117 columns**, named
        `{exchange}__{ticker}__{measure}` (`tvc__vn10y__value`,
        `tvc__vn02y__volatility_21`), keyed `(date, exchange, ticker)` on
        `pool__basic`'s calendar. Returns a metadata dict.

        ⚠️ **THE SLOPE IS THE SIGNAL AND IT IS NOT A COLUMN.** A yield CURVE is read
        ACROSS tenors on one day — `tvc__vn10y__value − tvc__vn02y__value` is the
        10s2s slope, and the slope is the series that carries macro information, not
        any single tenor's level. The wide shape is what makes that a subtraction
        instead of a self-join (`gold.bonds`' own reason for existing), but nothing
        here computes it: `FeatureSelector` scores the columns it is given. A pool of
        levels is the raw material for that feature, not the feature.

        ⚠️ **9 TENORS FROM 18 SPELLINGS, COLLAPSED UPSTREAM.** TradingView exposes
        `TVC:VN01` and `TVC:VN01Y` as separate symbols and the scraper collected both —
        66,100 silver rows for 33,050 observations. `_helper_bonds_drop_duplicate_tenors`
        collapses them in gold, having ASSERTED per pair that they agree (measured
        2026-08-05: **0 differing values** on every shared date). Nothing to redo here;
        it is worth knowing the tenor set is 9 and not 18 before counting columns.

        ⚠️ **THE WHOLE SOURCE STOPS 2026-06-08, UNIFORMLY.** The 2026-08-05 scrape
        queued **0 data tasks** for bonds — not "some series moved" as with forex and
        funds, but none — so every tenor ends on the same day and the last **43 spine
        rows are entirely NULL**. That uniformity is the one mercy: there is no
        per-series staleness to unpick, just one date to compare against.

        ⚠️ **The 15y/20y/30y tenors begin in 2018**, and `gold.bonds` has no row at all
        for 1,017 of the 4,266 VCB spine dates (VN traded, TVC did not quote), so
        per-column coverage runs **min 37.1%, median 75.9%, max 76.1%**. The long end
        of the curve simply does not exist before 2018 — a slope built across it is a
        2018-onward feature whatever its NULL policy.

        ⚠️ **These are LEVELS, in percent.** Same trap as `pool__forex` and
        `pool__economy`: a yield level against a forward stock return. The
        representation is the selection step's decision; this method copies as-is.
        """
        return self._helper_unified_pool_on_date_spine(
            ticker, "pool__bonds", self.UNIFIED_BONDS_SOURCE, noun="tenor series"
        )

    def _ingest_unified_pool_market_breadth(self, ticker: str) -> dict:
        """`gold.market_breadth` → `…​.pool__market_breadth` — the market, COMPRESSED.

        **8 date-keyed channels** describing the whole 781-name cross-section, LEFT
        JOINed onto `pool__basic` and broadcast across its tickers. Returns a metadata
        dict.

        ⚠️ **THIS POOL IS THE ANSWER TO "PUT THE WHOLE MARKET IN" THAT ACTUALLY FITS.**
        The instinct is `pool__basic_bank`'s shape — pivot every ticker's measures into
        columns. At 781 × 27 that is **21,087 columns against PostgreSQL's 1,600 limit**
        (issue `WID-1`), so it cannot be built; and VCB has `n_eff = 852` independent
        observations, where §5c measured 202 channels at test IC −0.011 against 724 at
        **−0.072** on the same ticker and splits. Eight columns is not a compromise
        here, it is the only shape the sample size allows.

        ⚠️ **THE CHANNELS WERE CHOSEN BY MEASUREMENT, and the ones that were dropped
        are the informative part of the story.** Seven candidates were scored against
        VCB's forward 5-day return over 826 NON-OVERLAPPING observations (2026-08-16).
        The dispersion/flow family survived — `xs_skew5` t = −2.29, `xs_disp5`
        t = +1.64, `turnover_z` t = −1.46 — and the BREADTH family did not:
        `breadth_pos5` t = +0.21, `above_ma20` t = +0.29, `n_active` t = +0.34. Breadth
        was dropped for two reasons, not one: it measured ≈ 0, AND it is a restatement
        of the index level that `pool__stock_market` already carries as
        `hose__vnindex__*`.

        ⚠️ **NOTHING HERE CLEARS MULTIPLE TESTING.** Seven tests puts the Bonferroni
        bar at |t| > 2.69 and the best is −2.29. These are the least-bad candidates at a
        cost of 8 columns, not a demonstrated signal. The row of that table that matters
        most is the baseline: **VCB's own past 5-day return scored t = −0.31**, so the
        market says at least as much about VCB as VCB's own history does — which is a
        statement about how little either says.

        ⚠️ **`mkt_n_names` STAYS IN GOLD AND IS BLOCKED FROM THIS POOL (TODO P0-4,
        2026-08-17).** It is the WIDTH the other seven were computed over — a fact about
        the DATA, not about the market. It rises ~380 (2009) → ~771 (today) because
        tickers were listed and because `silver.stocks_basic` holds no delisted name, so
        a tree splitting on it is **reading the calendar**: exactly the trap
        `close_adjust` sets on a level target, wearing a new name. Kept in
        `gold.market_breadth` because a reader needs to know how wide each date's
        cross-section was; blocked here because a candidate FEATURE is a different thing
        from a DIAGNOSTIC.

        ⚠️ It did not bite on the run that prompted this: on 2026-08-17's `return_5day`
        chain **no `mkt_*` channel survived layer 2 at all** — 4 of 208 reached the
        shortlist pool, 0 of 66 reached the final table. This is a guard against the next
        run, not a repair of that one.
        """
        columns = self._helper_column_types(
            *self.UNIFIED_MARKET_BREADTH_SOURCE.split(".", 1)
        )
        if not columns:
            raise MissingSourceDataError(
                f"`{self.UNIFIED_MARKET_BREADTH_SOURCE}` does not exist — build "
                f"gold/market_breadth first."
            )
        blocked = set(self.UNIFIED_MARKET_BREADTH_NOT_FEATURES)
        missing = blocked - set(columns)
        if missing:
            # ⚠️ A block-list that silently matches nothing is how an excluded column
            # comes back: rename it upstream and the guard evaporates without a word.
            raise PipelineError(
                f"UNIFIED_MARKET_BREADTH_NOT_FEATURES names {sorted(missing)}, which "
                f"`{self.UNIFIED_MARKET_BREADTH_SOURCE}` does not have."
            )
        features = [c for c in columns if c != "date" and c not in blocked]
        return self._helper_unified_pool_on_date_spine(
            ticker,
            "pool__market_breadth",
            self.UNIFIED_MARKET_BREADTH_SOURCE,
            noun="market series",
            relation=self.UNIFIED_MARKET_BREADTH_SOURCE,
            feature_columns=features,
        )

    def _ingest_unified_pool_stock_market(self, ticker: str) -> dict:
        """`gold.stock_market` → `…​.pool__stock_market` — the index panel.

        **6 indices × 27 measures = 162 columns**, named
        `{exchange}__{index}__{measure}` — `hose__vnindex`, `hose__vn30index`,
        `hose__vn100_index`, `hnx__hnx_index`, `hnx__hnx30_index`,
        `upcom__upcom_index` — keyed `(date, exchange, ticker)` on `pool__basic`'s
        calendar. Returns a metadata dict.

        ⚠️ **THE PIVOT ALREADY HAPPENED IN GOLD.** `gold.stock_market` is the four
        CafeF index tabs (price / order stats / foreign / prop trading) joined and
        pivoted to one row per date, so every index is already its own set of channels
        and this method copies them. There is no pivot to redo here — asking for one
        would mean re-deriving a table that exists.

        ## ⚠️ `hose__vnindex__close_adjust` IS THE TARGET'S OWN BENCHMARK

        It is `UNIFIED_BENCHMARK_COLUMN` — the series `_ingest_unified_pool_targets`
        subtracts to build `return_rel_{h}day`:

            return_rel_h[t] = return_h[t] − (bm[t+h] / bm[t] − 1)

        This pool carries **bm[t] and its trailing history, never bm[t+h]**, so there
        is NO leakage: nothing here is dated after the row it sits on. What IS true is
        that the target's own DENOMINATOR is now a feature, and a model fitting
        `return_rel` can see it. That is legitimate and it is not nothing — quote it
        beside any result this pool contributes to.

        ⚠️ **AND FOR THE ABSOLUTE TARGET IT IS THE MARKET FACTOR ITSELF.** A single
        stock's absolute forward return is dominated by the market, which is the whole
        reason `return_rel_{h}day` exists. Handing a model the index's contemporaneous
        level is the level-predicts-level trap in its purest form; the order-flow and
        foreign-flow measures below are the part of this pool that is not that.

        ⚠️ **THE ORDER-FLOW MEASURES ARE THE INTERESTING HALF.** `n_buy_orders`,
        `n_sell_orders`, `avg_vol_per_{buy,sell}_order`, `buy_order_vol`,
        `sell_order_vol`, `foreign_net_{value,volume}`, `prop_{buy,sell}_{val,vol}` are
        market-wide flow, not price — the closest anything already in this database gets
        to the top-ranked lever in the hub's §2d (aggressor buy/sell imbalance, which
        properly needs intraday tick). ⚠️ The four `prop_*` measures cover **5.8%** of
        the spine: that CafeF tab starts late, so they are effectively a recent-years
        feature.

        ⚠️ **Coverage is the WIDEST of the date-broadcast pools — median 83.1%** (min
        0.02%, max 99.8%, none all-NULL), because an index quotes when the market is
        open and the spine is VN trading days. Per index the mean runs `vnindex` 84.1%,
        `hnx_index` 80.7%, `upcom_index` 80.2%, `vn30index` 67.9%, `hnx30_index` 62.0%,
        `vn100_index` 51.3% — the later launches, not gaps.

        ⚠️ **The source ends 2026-07-30 against a 2026-08-07 spine**, so the last **6
        rows are NULL**. Different cause from the TradingView trio: this is a CafeF
        chain (`raw/cafef_index_*` → `bronze.cafef_index_*` → `silver.stock_market`),
        so it is re-freshed by materialising that chain, not by a `skip_existing=False`
        TradingView scrape.
        """
        return self._helper_unified_pool_on_date_spine(
            ticker,
            "pool__stock_market",
            self.UNIFIED_STOCK_MARKET_SOURCE,
            noun="index series",
        )

    # ⚠️ THE FIRST DATE-BROADCAST POOL WITH NO TABLE BEHIND IT. Every other one copies
    # a wide gold panel that already exists; there is no `gold.stocks_bank_wide`, so
    # this one PIVOTS `silver.stocks_basic` on the fly — one `MAX(CASE WHEN ticker = …)`
    # per (ticker × measure), grouped by date — and hands the subquery to the shared
    # helper. Server-side throughout: a pandas round-trip would return every `numeric`
    # as `Decimal`, land it as dtype `object`, and write the whole panel back as VARCHAR.
    UNIFIED_PEER_SOURCE = f"{SILVER_SCHEMA}.stocks_basic"

    def _ingest_unified_pool_basic_bank(self, ticker: str) -> dict:
        """The BANK sector's `pool__basic` measures as PEER CHANNELS on this spine.

        `silver.stocks_basic` filtered to GICS `industry_code = 401010`, pivoted to
        `{exchange}__{ticker}__{measure}` — one column per bank per measure, one row
        per date — and LEFT JOINed onto `pool__basic`. **20 banks × 15 measures = 300
        columns.**

        ⚠️ **MEMBERSHIP IS DERIVED, AND IT IS THE SAME PREDICATE `unified_schema_bank`
        USES.** It comes from `UNIFIED_MEMBER_FILTERS[UNIFIED_BANK]` — the GICS
        classification `silver.stocks_basic` already carries — not from a ticker list.
        A bank listing or being reclassified is picked up by a rebuild.

        ⚠️ **AND IT IS NOT POINT-IN-TIME.** `silver.stocks_basic` holds today's GICS
        code on every historical row and **no delisted name at all**, so these 20
        channels are the banks that survived to 2026 carried back to 2009. A bank that
        listed in 2018 is simply NULL before it; a bank that was delisted is ABSENT.
        Same survivorship the hub's §2c records for the universe as a whole, and the
        reason `pool__basic_vn30` was deferred rather than built beside this
        (2026-08-14 decision — `vn30.csv` is today's list with no history at all,
        which is strictly worse than a derived GICS predicate).

        ⚠️ **THE SCHEMA'S OWN TICKER IS ONE OF THE CHANNELS.** On
        `unified_schema_vcb`, `hose__vcb__close_adjust` IS `pool__basic.close_adjust` —
        verified 2026-08-14, 0 mismatches on all 15 measures. It is kept rather than
        dropped because the column set of a date-broadcast pool must not depend on
        which partition is being built: dropping "self" is meaningless on `BANK` (every
        row has a different self) and on `ALL`. **A consumer joining
        `pool__basic ⋈ pool__basic_bank` on `unified_schema_vcb` holds each VCB measure
        twice**, and the correlation prune will spend budget rediscovering that unless
        the duplicate is excluded up front.

        ⚠️ **IDENTITY COLUMNS ARE EXCLUDED**, as they are from `pool__ta` / `pool__fa`.
        The 8 GICS columns are constant per ticker, so pivoting them would write 160
        constant strings — and `FeatureSelector._prepare` would drop every one after
        paying to read it.
        """
        schema = self._helper_unified_schema(ticker)
        predicate, params = self._helper_unified_member_filter(self.UNIFIED_BANK)
        source_schema, source_table = self.UNIFIED_PEER_SOURCE.split(".", 1)

        source_types = self._helper_column_types(source_schema, source_table)
        if not source_types:
            raise MissingSourceDataError(
                f"`{self.UNIFIED_PEER_SOURCE}` does not exist — build silver first."
            )
        measures = [
            column
            for column in source_types
            if column not in set(self.UNIFIED_PRIMARY_KEY) | set(self.UNIFIED_POOL_IDENTITY)
        ]
        if not measures:
            raise MissingSourceDataError(
                f"`{self.UNIFIED_PEER_SOURCE}` has no measure columns outside the key "
                f"and the GICS identity block."
            )

        with self._database_driver._cursor_ctx() as cur:
            cur.execute(
                f"SELECT DISTINCT exchange, ticker FROM {self.UNIFIED_PEER_SOURCE} "
                f"WHERE {predicate} ORDER BY exchange, ticker",
                params,
            )
            members = [(str(row[0]), str(row[1])) for row in cur.fetchall()]

        if not members:
            raise MissingSourceDataError(
                f"The {self.UNIFIED_BANK} predicate ({predicate!r} {params}) matches no "
                f"row of `{self.UNIFIED_PEER_SOURCE}`, so the peer pool would be empty."
            )
        # ⚠️ EVERY PART OF EVERY COLUMN NAME IS AN IDENTIFIER READ OUT OF THE DATABASE,
        # so it is validated before interpolation — the same rule `_helper_unified_schema`
        # applies to a ticker, for the same reason. A measure name comes from
        # information_schema and an exchange/ticker from a data row; neither can be bound.
        for exchange, member in members:
            for part in (exchange, member):
                if not self.UNIFIED_TICKER_PATTERN.match(part):
                    raise PipelineError(
                        f"`{self.UNIFIED_PEER_SOURCE}` holds {part!r} in an identifier "
                        f"position; it cannot safely name a pool column."
                    )

        # {exchange}__{ticker}__{measure} — the naming every wide gold panel uses, and
        # the reason the separator is DOUBLE: measures carry single underscores
        # (`avg_vol_per_buy_order`), so only `__` can be split back.
        channels: List[str] = []
        projections: List[str] = []
        for exchange, member in members:
            for measure in measures:
                column = f"{exchange.lower()}__{member.lower()}__{measure}"
                if len(column) > 63:
                    raise PipelineError(
                        f"Pool column {column!r} is {len(column)} characters; "
                        f"PostgreSQL truncates identifiers at 63, which would silently "
                        f"collide two channels."
                    )
                channels.append(column)
                projections.append(
                    f"MAX(CASE WHEN s.exchange = '{exchange}' AND s.ticker = "
                    f"'{member}' THEN s.{measure} END) AS {column}"
                )

        # ⚠️ `MAX(CASE …)` is the pivot, and it is only correct because
        # `(date, exchange, ticker)` is `silver.stocks_basic`'s PRIMARY KEY — at most
        # one row can match each CASE, so MAX picks a value rather than choosing
        # between two. A source with duplicate keys would silently publish the larger.
        #
        # ⚠️ The WHERE narrows to the SAME members the CASEs already select, so it is a
        # scan optimisation and not the filter — dropping it would give an identical
        # table from 2.4 M rows instead of ~55 k. It restricts on the resolved member
        # list rather than re-stating the GICS predicate, because the predicate is
        # PARAMETERISED (`_helper_unified_member_filter` keeps a GICS code a value) and
        # this string reaches `cur.execute` with no params of its own. The literals
        # below are the same ticker/exchange values already interpolated into the CASE
        # expressions, and they passed `UNIFIED_TICKER_PATTERN` above — there is no way
        # to parameterise a CASE-per-member anyway.
        members_in = ", ".join(
            f"('{exchange}', '{member}')" for exchange, member in members
        )
        relation = (
            f"(SELECT s.date, {', '.join(projections)} "
            f"FROM {self.UNIFIED_PEER_SOURCE} s "
            f"WHERE (s.exchange, s.ticker) IN ({members_in}) "
            f"GROUP BY s.date)"
        )

        self._logger.log_info(
            f"Ingesting unified {schema}.pool__basic_bank: {len(members)} member(s) × "
            f"{len(measures)} measures = {len(channels)} channels."
        )
        result = self._helper_unified_pool_on_date_spine(
            ticker,
            "pool__basic_bank",
            f"{self.UNIFIED_PEER_SOURCE} (industry_code {params[0]}, pivoted)",
            noun="peer channel",
            relation=relation,
            feature_columns=channels,
        )
        result["members"] = len(members)
        result["measures"] = len(measures)
        result["channels"] = channels
        return result

    def _helper_unified_pool_on_date_spine(
        self,
        ticker: str,
        pool: str,
        source: str,
        noun: str,
        relation: Optional[str] = None,
        feature_columns: Optional[Sequence[str]] = None,
    ) -> dict:
        """Shared body of every DATE-BROADCAST pool.

        A wide panel keyed on `date` ALONE, LEFT JOINed onto `pool__basic` and
        broadcast across its tickers. Returns a metadata dict; `noun` names the source's
        entity in log lines and error messages.

        `source` is a `schema.table` and, by default, both the thing joined and the
        thing whose columns are read. ⚠️ **`relation` + `feature_columns` override that
        for a DERIVED panel** — `_ingest_unified_pool_basic_bank` pivots
        `silver.stocks_basic` on the fly and has no table for `_helper_column_types` to
        introspect, so it passes the subquery and the column list it just generated.
        Both or neither: a relation whose columns were guessed is the failure this pair
        exists to prevent. `source` stays the human-readable name in every message.

        ⚠️ **EVERY QUERY ALIASES THE RELATION `f`**, which is what lets a bare
        `schema.table` and a parenthesised subquery both sit in FROM position. A
        subquery without an alias is a syntax error in PostgreSQL, and one WITH its own
        alias would collide.

        ⚠️ **LEFT JOIN, never INNER.** An INNER JOIN would silently DROP every spine
        date the source does not cover — 1,351 of 4,266 rows for `gold.funds`, which
        starts in 2014 — and produce a pool that looks clean and has quietly changed the
        calendar under its own primary key.
        """
        schema = self._helper_unified_schema(ticker)
        derived = relation is not None or feature_columns is not None
        if derived and (relation is None or feature_columns is None):
            raise PipelineError(
                f"{pool}: `relation` and `feature_columns` must be given together. A "
                f"derived panel has no table to introspect, so a relation without an "
                f"explicit column list would be reading columns off the wrong object."
            )
        relation = relation if derived else source

        if not self._helper_column_types(schema, "pool__basic"):
            raise MissingSourceDataError(
                f"`{schema}.pool__basic` does not exist — build it first; it is the "
                "calendar spine for every unified feature pool."
            )

        if derived:
            source_types = {"date": "date", **{c: "" for c in feature_columns}}
        else:
            source_schema, source_table = source.split(".", 1)
            source_types = self._helper_column_types(source_schema, source_table)
        if not source_types:
            raise MissingSourceDataError(
                f"`{source}` does not exist — build it first."
            )
        if "date" not in source_types:
            raise MissingSourceDataError(
                f"`{source}` has no `date` column, so it cannot join the unified "
                "trading-day spine."
            )
        feature_columns = (
            list(feature_columns)
            if derived
            else [column for column in source_types if column != "date"]
        )
        if not feature_columns:
            raise MissingSourceDataError(f"`{source}` contains no {noun} columns.")
        duplicate_keys = set(feature_columns) & set(self.UNIFIED_PRIMARY_KEY)
        if duplicate_keys:
            raise PipelineError(
                f"`{source}` reuses unified key column(s) {sorted(duplicate_keys)}. "
                f"A date-keyed panel may contribute only feature columns beside `date`."
            )

        self._logger.log_info(f"Ingesting unified {schema}.{pool} (from {source})...")

        with self._database_driver._cursor_ctx() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.pool__basic")
            spine_rows = int(cur.fetchone()[0])
            if not spine_rows:
                raise MissingSourceDataError(
                    f"`{schema}.pool__basic` is empty, so {pool} would have no "
                    "calendar to join."
                )

            cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT date) FROM {relation} f")
            source_rows, source_dates = (int(value) for value in cur.fetchone())
            if not source_rows:
                raise MissingSourceDataError(f"`{source}` is empty.")
            # ⚠️ One row per date is what makes the join a BROADCAST rather than a
            # fan-out. Two rows for one date would silently double every spine row —
            # and the row-count check below would catch it, but only after writing a
            # table twice the size of the spine.
            if source_rows != source_dates:
                raise PipelineError(
                    f"`{source}` has {source_rows} rows over {source_dates} dates. It "
                    "must be one row per date before joining a unified pool."
                )

            cur.execute(
                f"SELECT COUNT(*) FROM {schema}.pool__basic b "
                f"JOIN {relation} f ON f.date = b.date"
            )
            matched_rows = int(cur.fetchone()[0])
            if not matched_rows:
                raise MissingSourceDataError(
                    f"`{source}` shares no dates with `{schema}.pool__basic`."
                )

            selected = ", ".join(f"f.{column}" for column in feature_columns)
            any_value = " OR ".join(
                f"{column} IS NOT NULL" for column in feature_columns
            )

            # Dropped as late as possible: a failure above leaves the old table intact.
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{pool}")
            cur.execute(
                f"CREATE TABLE {schema}.{pool} AS "
                f"SELECT b.date, b.exchange, b.ticker, {selected} "
                f"FROM {schema}.pool__basic b "
                f"LEFT JOIN {relation} f ON f.date = b.date"
            )
            self._helper_unified_primary_key(cur, schema, pool)

            cur.execute(f"SELECT COUNT(*) FROM {schema}.{pool}")
            written = int(cur.fetchone()[0])
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{pool} WHERE {any_value}")
            populated_rows = int(cur.fetchone()[0])
            cur.execute(
                f"SELECT COUNT(*) FROM ("
                f"  (SELECT date, exchange, ticker FROM {schema}.{pool} "
                f"   EXCEPT SELECT date, exchange, ticker FROM {schema}.pool__basic) "
                f"  UNION ALL "
                f"  (SELECT date, exchange, ticker FROM {schema}.pool__basic "
                f"   EXCEPT SELECT date, exchange, ticker FROM {schema}.{pool})"
                f") unaligned"
            )
            unaligned = int(cur.fetchone()[0])

        # ⚠️ EQUALITY, not the one-sided allowance `pool__ta`/`pool__fa` get. Those are
        # LEFT-limited by their SOURCE's ticker coverage; this one is a LEFT JOIN from
        # the spine, so every spine row must survive it. Fewer rows here means the join
        # itself went wrong, not that the source is narrow.
        if written != spine_rows:
            raise PipelineError(
                f"{schema}.{pool} wrote {written} rows against pool__basic's "
                f"{spine_rows}. A date-keyed panel adds columns, never rows."
            )
        if unaligned:
            raise PipelineError(
                f"{schema}.{pool} disagrees with pool__basic on {unaligned} "
                "unified key(s). Every pool must share the spine exactly."
            )
        if not populated_rows:
            raise MissingSourceDataError(
                f"{schema}.{pool} has no populated {noun} value on pool__basic's "
                "calendar."
            )

        coverage = 100.0 * populated_rows / max(written, 1)
        self._logger.log_info(
            f"{schema}.{pool}: {written} rows x {len(feature_columns)} {noun} columns "
            f"({coverage:.1f}% of rows carry at least one value)."
        )
        return {
            "source": source,
            "table": pool,
            "rows": written,
            "features": len(feature_columns),
            "populated_rows": populated_rows,
        }

    # ── DERIVED FEATURES on `pool__basic` (2026-08-16) ───────────────────────────
    #
    # `pool__basic` used to be `SELECT *` and nothing else — a faithful copy of
    # `silver.stocks_basic`. It now carries a block of TRAILING derived channels named
    # `drv_*`, computed in SQL inside the same `CREATE TABLE AS`.
    #
    # ⚠️ **WHY A PREFIX.** These names must not collide with `pool__ta` (935 columns),
    # `pool__fa`, `pool__targets` or any date-broadcast pool — `UnifiedSchemaReader.join`
    # would produce `_x`/`_y` pairs and silently drop one copy. `drv_` guarantees that,
    # makes "silver column or derived column?" answerable by name alone, and lets the
    # asset assert the exact column set. It also keeps `_ingest_unified_pool_fa`'s
    # `basic_columns` exclusion correct without change: that reads `silver.stocks_basic`,
    # which no `drv_*` name is ever in.
    #
    # ⚠️ **THE ADJUSTMENT FACTOR IS APPLIED FIRST, AND THIS IS THE TRAP.**
    # `silver.stocks_basic.open/high/low` are RAW — they track `close_raw`, not
    # `close_adjust`. Measured 2026-08-16: `close_raw BETWEEN low AND high` on 4,266 of
    # VCB's 4,266 rows, `close_adjust` on 248; market-wide 2,383,827 against 546,358.
    # `_helper_adjust_ohlc` says the same thing for gold and names the row that proves
    # it — VCB 2009-06-30, `open=high=low=close_raw=60,000` while `close_adjust=9,130`.
    # So every expression below reads `_o/_h/_l/_c`, the OHLC set rebuilt with today's
    # factor `close_adjust / close_raw`, never the source columns. Same-day RATIOS are
    # factor-invariant and would survive either way; anything spanning days is not, and
    # `drv_gap_open_pct` on raw prices would read every split as an overnight crash.
    #
    # ⚠️ **EVERY WINDOW IS `PARTITION BY exchange, ticker`, AND IT IS NOT OPTIONAL.**
    # The same warning `_ingest_unified_pool_targets` carries: without it a window walks
    # off the end of one company's history into the next one's, and on `ALL` (781
    # tickers) that is 780 corrupted boundaries per column. On a single-ticker pool the
    # partition is a no-op, so one statement is correct for every universe and there is
    # no second code path to keep in step.
    #
    # ⚠️ **EVERY FRAME ENDS AT `CURRENT ROW`. There is no `FOLLOWING` anywhere in this
    # block**, and a reviewer should be able to confirm that by grepping for it. These
    # are features; a frame reaching forward would be a label wearing a feature's name,
    # which is the failure `pool__funds`' docstring records for the TA battery.
    #
    # ⚠️ **NULLIF ON EVERY DIVISOR, because the degenerate rows are not rare.** Measured
    # 2026-08-16 market-wide: 809,252 of 2,388,975 rows have `high = low` (34%) and
    # 511,322 have zero or NULL matched volume (21%). On VCB only 5 rows do — so a
    # divisor that looks safe on the single-name chain divides by zero a third of the
    # time on `ALL`.
    #
    # ⚠️ **FLOATS ONLY, NO BOOLEANS.** `FeatureSelector._prepare` excludes bool dtypes,
    # which is why ~207 of `pool__ta`'s columns are stored but never scored. Anything
    # here that wants to be a flag is emitted as a count or a ratio instead.
    #
    # ⚠️ **TWO VALUE COLUMNS ARE IN BILLIONS AND FOUR ARE NOT.** Measured 2026-08-16 on
    # VCB 2026-08-07: `value_matched` = **392.54** while `volume_matched × close_raw` =
    # **389,375,340,000**, a ratio of 1.0e9 (market-wide median 1.0007e9) — so
    # `value_matched` / `value_negotiated` are **billions of VND**, exactly as
    # `gold.stocks_ta` says with its `val_matched_bn` name. `foreign_buy_value` (VCB
    # same day: 70,742,630,000) and `prop_buy_val` (37,325,880,000) are **plain VND**.
    # The first draft of this block divided one by the other and produced a "foreign
    # participation ratio" of 215,150,099. Every expression below therefore reads
    # `_val_vnd`, one column defined once in the `px` CTE, so the 1e9 appears in exactly
    # one place with this paragraph attached to it.
    #
    # ⚠️ **INTEGER DIVISION IS THE OTHER ONE.** `volume_matched` is `bigint`, so
    # `(volume - MIN(volume) OVER w) / (MAX(volume) OVER w - MIN(volume) OVER w)`
    # divides int by int and returns **0** — measured, against 0.2038 for the float
    # form. `render()` casts every finished expression to `double precision`, but that
    # is applied AFTER the division, so any expression dividing two integer columns
    # casts its own numerator.
    #
    # Coverage of the source blocks, measured 2026-08-16 (VCB / market-wide): OHLCV
    # 100% / 100%, order stats 96.4% / 97.3% (from 2010-01-04), foreign 72.6% / 74.2%
    # (from 2012-01-03), prop 20.7% / 3.1% (from 2023-01-03). ⚠️ The prop block is thin
    # enough that its two channels are mostly NULL on `ALL` — rule 22's
    # `trailing_null_sessions` problem, visible here as a low `drv_*` populated count.
    UNIFIED_DERIVED_PREFIX = "drv_"

    # Level 1 — per-row expressions and one-step LAGs over `wbase`. Helpers first
    # (leading underscore, consumed by later levels and NEVER written to the table),
    # then the output channels.
    UNIFIED_DERIVED_L1_HELPERS: Tuple[Tuple[str, str], ...] = (
        # Parkinson / Garman-Klass / Rogers-Satchell per-bar variance contributions.
        # Guarded rather than NULLIF'd: LN of a non-positive number RAISES in
        # PostgreSQL, where a division by zero here would only produce a NULL.
        ("_ln_hl2", 'CASE WHEN "_h" > 0 AND "_l" > 0 THEN LN("_h"/"_l") * LN("_h"/"_l") END'),
        (
            "_gk_var",
            'CASE WHEN "_h" > 0 AND "_l" > 0 AND "_c" > 0 AND "_o" > 0 THEN '
            '0.5 * LN("_h"/"_l") * LN("_h"/"_l") '
            '- (2.0 * LN(2.0) - 1.0) * LN("_c"/"_o") * LN("_c"/"_o") END',
        ),
        (
            "_rs_var",
            'CASE WHEN "_h" > 0 AND "_l" > 0 AND "_c" > 0 AND "_o" > 0 THEN '
            'LN("_h"/"_c") * LN("_h"/"_o") + LN("_l"/"_c") * LN("_l"/"_o") END',
        ),
        # A no-trade session, as a 0/1 number so it can be SUMmed over a window. NULL
        # volume counts as no trade — on `ALL` the two are the same event.
        (
            "_no_trade",
            'CASE WHEN COALESCE("volume_matched", 0) = 0 THEN 1.0 ELSE 0.0 END',
        ),
    )

    UNIFIED_DERIVED_L1: Tuple[Tuple[str, str], ...] = (
        # ── A. Bar shape and intraday range ──
        ("drv_range_hl_pct", '("_h" - "_l") / NULLIF("_c", 0)'),
        ("drv_body_pct", '("_c" - "_o") / NULLIF("_o", 0)'),
        # Close Location Value, −1 (closed on the low) … +1 (closed on the high). The
        # sibling of `pool__ta`'s `bop`, which divides the OPEN-to-close move by the
        # range; this one places the CLOSE between the extremes and is not in that pool.
        ("drv_clv", '(("_c" - "_l") - ("_h" - "_c")) / NULLIF("_h" - "_l", 0)'),
        ("drv_upper_shadow", '("_h" - GREATEST("_o", "_c")) / NULLIF("_h" - "_l", 0)'),
        ("drv_lower_shadow", '(LEAST("_o", "_c") - "_l") / NULLIF("_h" - "_l", 0)'),
        # ⚠️ The overnight jump — the ONLY piece of non-intraday information the daily
        # bar carries, and `gold.stocks_ta` has nothing like it (0 hits for "gap" across
        # its 935 columns). Adjusted on both sides, so a split is not a crash.
        ("drv_gap_open_pct", '"_o" / NULLIF(LAG("_c") OVER wbase, 0) - 1.0'),
        ("drv_intraday_pct", '"_c" / NULLIF("_o", 0) - 1.0'),
        ("drv_ret_1d", '"_c" / NULLIF(LAG("_c") OVER wbase, 0) - 1.0'),
        (
            "drv_ret_log_1d",
            'CASE WHEN "_c" > 0 AND LAG("_c") OVER wbase > 0 '
            'THEN LN("_c" / LAG("_c") OVER wbase) END',
        ),
        # ── F. Liquidity, per-row half ──
        # ⚠️ RAW ON RAW ON PURPOSE. `_val_vnd` is VND actually paid and
        # `volume_matched` shares actually traded, so their ratio is a RAW average
        # traded price and must be compared against `close_raw`, not `_c`. It is the
        # one derived channel here that does not live on the adjusted scale.
        ("drv_vwap_raw", '"_val_vnd" / NULLIF("volume_matched", 0)'),
        (
            "drv_close_vs_vwap",
            '"close_raw"::double precision '
            '/ NULLIF("_val_vnd" / NULLIF("volume_matched", 0), 0) - 1.0',
        ),
        # Both sides are billions, so the unit cancels — but written against the VND
        # pair anyway so no reader has to work out which columns share a scale.
        (
            "drv_negotiated_value_share",
            '"_val_neg_vnd" / NULLIF("_val_vnd" + "_val_neg_vnd", 0)',
        ),
        # ── D. Order-flow imbalance — §2d's top lever, at daily grain ──
        # ⚠️ `avg_vol_per_buy_order` IS `buy_order_vol / n_buy_orders` — verified
        # 2026-08-16, 0 of 2,071,153 rows differ by more than half a share. So a
        # per-side size channel would be a pure duplicate; only the CROSS-SIDE ratio
        # below is new information.
        (
            "drv_order_count_imb",
            '("n_buy_orders"::double precision - "n_sell_orders") '
            '/ NULLIF("n_buy_orders"::double precision + "n_sell_orders", 0)',
        ),
        (
            "drv_order_vol_imb",
            '("buy_order_vol"::double precision - "sell_order_vol") '
            '/ NULLIF("buy_order_vol"::double precision + "sell_order_vol", 0)',
        ),
        (
            "drv_log_order_size_ratio",
            'CASE WHEN "avg_vol_per_buy_order" > 0 AND "avg_vol_per_sell_order" > 0 '
            'THEN LN("avg_vol_per_buy_order"::double precision '
            '        / "avg_vol_per_sell_order") END',
        ),
        (
            "drv_avg_order_size",
            '("buy_order_vol"::double precision + "sell_order_vol") '
            '/ NULLIF("n_buy_orders"::double precision + "n_sell_orders", 0)',
        ),
        # How much of the day's posted intent actually traded. Above 1 is normal — a
        # share can change hands more than once against standing interest.
        (
            "drv_order_fill_ratio",
            '"volume_matched"::double precision '
            '/ NULLIF(("buy_order_vol"::double precision + "sell_order_vol") / 2.0, 0)',
        ),
        # ── E. Foreign and proprietary flow ──
        (
            "drv_foreign_net_value_ratio",
            '"foreign_net_value"::double precision / NULLIF("_val_tot_vnd", 0)',
        ),
        (
            "drv_foreign_participation",
            '("foreign_buy_value"::double precision + "foreign_sell_value") '
            '/ NULLIF(2.0 * "_val_tot_vnd", 0)',
        ),
        # Ownership DRIFT, not the level — the level is already a silver column and is
        # near-constant week to week.
        (
            "drv_foreign_own_chg_5",
            '"foreign_own"::double precision - LAG("foreign_own", 5) OVER wbase',
        ),
        (
            "drv_foreign_own_chg_21",
            '"foreign_own"::double precision - LAG("foreign_own", 21) OVER wbase',
        ),
        # ⚠️ 20.7% covered on VCB and 3.1% market-wide, from 2023-01-03. Kept because
        # proprietary desks are the other half of the flow story and the block is 4
        # columns wide; read `drv_*` populated counts before trusting either.
        #
        # ⚠️ **AND THE SOURCE HAS CORRUPT ROWS THAT THESE RATIOS AMPLIFY** (measured
        # 2026-08-16, issue OUT-1). `silver.stocks_basic` VCB 2026-01-05 carries
        # `prop_buy_val = 4.001e17` — 400 quadrillion VND — against a whole-day
        # turnover of 2.06e11, on `prop_buy_vol = 697,000` shares at a close of
        # 57,100: an implied 5.7e11 VND per share, ten million times the real price.
        # That ONE row is enough to drive `corr(drv_prop_net_value_ratio,
        # drv_prop_participation)` to exactly +1.0 and to manufacture a +0.266
        # correlation against the forward 5-day return, which is the shape of a
        # finding and is a single bad cell. Market-wide, 77 of 73,044 `prop_buy_val`
        # and 1,182 of 1,240,032 `foreign_buy_value` rows exceed ten times their own
        # day's turnover (~0.1% each) — so `foreign_*` carries the same defect.
        # ⚠️ NOT winsorised here. Cleaning belongs to the layer that owns the column,
        # and silently clipping a source value inside a feature expression is how a
        # data defect stops being visible. Anything selecting on these four channels
        # should look at their extremes first.
        (
            "drv_prop_net_value_ratio",
            '("prop_buy_val"::double precision - "prop_sell_val") '
            '/ NULLIF("_val_tot_vnd", 0)',
        ),
        (
            "drv_prop_participation",
            '("prop_buy_val"::double precision + "prop_sell_val") '
            '/ NULLIF(2.0 * "_val_tot_vnd", 0)',
        ),
    )

    # Level 2 — trailing windows over level 1. `{w}` is substituted with the frame
    # alias, so a window appears in exactly one place per channel.
    #
    # Helpers again first: the four raw moments of the 63-day return distribution,
    # from which level 3 builds skewness and excess kurtosis (PostgreSQL has no window
    # skew/kurt aggregate, and the moment form needs only one pass).
    UNIFIED_DERIVED_L2_HELPERS: Tuple[Tuple[str, str], ...] = (
        ("_m1_63", 'AVG("drv_ret_1d") OVER w63'),
        ("_m2_63", 'AVG("drv_ret_1d" * "drv_ret_1d") OVER w63'),
        ("_m3_63", 'AVG("drv_ret_1d" * "drv_ret_1d" * "drv_ret_1d") OVER w63'),
        (
            "_m4_63",
            'AVG("drv_ret_1d" * "drv_ret_1d" * "drv_ret_1d" * "drv_ret_1d") OVER w63',
        ),
    )

    UNIFIED_DERIVED_L2: Tuple[Tuple[str, str], ...] = (
        # ── B. Range-based volatility estimators ──
        # ⚠️ None of these exist anywhere in the repo. They use the whole bar rather
        # than the close alone, which is why they are several times more efficient per
        # observation than a close-to-close standard deviation — the thing that matters
        # most here, where `n_eff` is `n_dates/h` and never larger (rule 7).
        ("drv_parkinson_5", 'SQRT(AVG("_ln_hl2") OVER w5 / (4.0 * LN(2.0)))'),
        ("drv_parkinson_21", 'SQRT(AVG("_ln_hl2") OVER w21 / (4.0 * LN(2.0)))'),
        # GREATEST(…, 0): both estimators are unbiased but not non-negative in small
        # samples, and SQRT of a negative RAISES.
        ("drv_garman_klass_5", 'SQRT(GREATEST(AVG("_gk_var") OVER w5, 0))'),
        ("drv_garman_klass_21", 'SQRT(GREATEST(AVG("_gk_var") OVER w21, 0))'),
        ("drv_rogers_satchell_5", 'SQRT(GREATEST(AVG("_rs_var") OVER w5, 0))'),
        ("drv_rogers_satchell_21", 'SQRT(GREATEST(AVG("_rs_var") OVER w21, 0))'),
        # `pool__ta` has `volatility_5` / `volatility_21`; these are the two horizons it
        # does NOT have, chosen so the pair also gives a fast/slow ratio at level 3.
        ("drv_realized_vol_10", 'STDDEV_SAMP("drv_ret_log_1d") OVER w10'),
        ("drv_realized_vol_63", 'STDDEV_SAMP("drv_ret_log_1d") OVER w63'),
        (
            "drv_downside_vol_21",
            'STDDEV_SAMP(CASE WHEN "drv_ret_1d" < 0 THEN "drv_ret_1d" END) OVER w21',
        ),
        # ── C. Distributional / normalisation — the "mean, max, quantile" block ──
        # ⚠️ THIS IS THE ANSWER TO THE LEVEL-PREDICTS-LEVEL TRAP. A raw `close_adjust`
        # ranks first in any selection against a price-level target at ρ 0.996 and means
        # nothing (hub §3c). `drv_close_z_*` and `drv_close_pos_*` say where today sits
        # in its OWN trailing distribution, which is bounded, stationary and comparable
        # across tickers and across decades.
        (
            "drv_close_z_21",
            '("_c" - AVG("_c") OVER w21) / NULLIF(STDDEV_SAMP("_c") OVER w21, 0)',
        ),
        (
            "drv_close_z_63",
            '("_c" - AVG("_c") OVER w63) / NULLIF(STDDEV_SAMP("_c") OVER w63, 0)',
        ),
        # ⚠️ A MIN-MAX POSITION, NOT A TRUE PERCENTILE, and the name says so.
        # `PERCENT_RANK` is a rank window function and cannot take a ROWS frame, and
        # `PERCENTILE_CONT` is an ordered-set aggregate and is not windowable at all —
        # so a genuine trailing quantile needs a correlated subquery, which is O(n·w)
        # and would be ~600 M row comparisons on `ALL` per column. This is the bounded
        # 0–1 statistic that IS computable in one pass.
        (
            "drv_close_pos_21",
            '("_c" - MIN("_c") OVER w21) '
            '/ NULLIF(MAX("_c") OVER w21 - MIN("_c") OVER w21, 0)',
        ),
        (
            "drv_close_pos_63",
            '("_c" - MIN("_c") OVER w63) '
            '/ NULLIF(MAX("_c") OVER w63 - MIN("_c") OVER w63, 0)',
        ),
        (
            "drv_close_pos_252",
            '("_c" - MIN("_c") OVER w252) '
            '/ NULLIF(MAX("_c") OVER w252 - MIN("_c") OVER w252, 0)',
        ),
        ("drv_dist_from_high_21", '"_c" / NULLIF(MAX("_h") OVER w21, 0) - 1.0'),
        ("drv_dist_from_high_63", '"_c" / NULLIF(MAX("_h") OVER w63, 0) - 1.0'),
        ("drv_dist_from_high_252", '"_c" / NULLIF(MAX("_h") OVER w252, 0) - 1.0'),
        ("drv_dist_from_low_21", '"_c" / NULLIF(MIN("_l") OVER w21, 0) - 1.0'),
        ("drv_dist_from_low_63", '"_c" / NULLIF(MIN("_l") OVER w63, 0) - 1.0'),
        # Volume and turnover surprise. `gold.stocks_ta` normalises no volume series at
        # all, so a 935-column pool cannot say "today's volume is unusual".
        (
            "drv_volume_z_21",
            '("volume_matched" - AVG("volume_matched") OVER w21) '
            '/ NULLIF(STDDEV_SAMP("volume_matched") OVER w21, 0)',
        ),
        # ⚠️ The numerator casts ITSELF. `volume_matched` is bigint, and bigint/bigint
        # is integer division — this expression returned a flat 0 until 2026-08-16.
        # `render()`'s outer cast happens after the division and cannot save it.
        (
            "drv_volume_pos_63",
            '("volume_matched" - MIN("volume_matched") OVER w63)::double precision '
            '/ NULLIF(MAX("volume_matched") OVER w63 '
            '         - MIN("volume_matched") OVER w63, 0)',
        ),
        (
            "drv_value_z_21",
            '("_val_vnd" - AVG("_val_vnd") OVER w21) '
            '/ NULLIF(STDDEV_SAMP("_val_vnd") OVER w21, 0)',
        ),
        # ── F. Liquidity, windowed half ──
        # ⚠️ Amihud illiquidity — mean over the window of |return| per BILLION VND
        # traded (the ×1e9 undoes `_val_vnd`, giving ~3e-5 for VCB and larger for a
        # thin name). Price impact per unit of money, the standard measure, and absent
        # from every pool in this database.
        (
            "drv_amihud_21",
            'AVG(ABS("drv_ret_1d") / NULLIF("_val_tot_vnd", 0)) OVER w21 * 1e9',
        ),
        (
            "drv_amihud_63",
            'AVG(ABS("drv_ret_1d") / NULLIF("_val_tot_vnd", 0)) OVER w63 * 1e9',
        ),
        # 21% of market-wide rows are no-trade sessions, so staleness is a real feature
        # on `ALL` and a constant 0 on VCB.
        ("drv_no_trade_days_21", 'SUM("_no_trade") OVER w21'),
        # ── E. Foreign flow, windowed ──
        # ⚠️ A RATIO OF SUMS, not a sum of VND. Persistent flow is what matters and a
        # raw cumulative VND figure is neither stationary nor comparable across tickers.
        (
            "drv_foreign_flow_ratio_5",
            'SUM("foreign_net_value") OVER w5 / NULLIF(SUM("_val_tot_vnd") OVER w5, 0)',
        ),
        (
            "drv_foreign_flow_ratio_21",
            'SUM("foreign_net_value") OVER w21 / NULLIF(SUM("_val_tot_vnd") OVER w21, 0)',
        ),
        # ── D. Order flow, windowed. The daily imbalance is noisy (VCB sd 0.225,
        # range −0.59…+0.98, measured 2026-08-16); the trailing mean is the signal
        # anyone would actually trade, and the z-score is today against that. ──
        ("drv_order_count_imb_5", 'AVG("drv_order_count_imb") OVER w5'),
        ("drv_order_count_imb_21", 'AVG("drv_order_count_imb") OVER w21'),
        ("drv_order_vol_imb_5", 'AVG("drv_order_vol_imb") OVER w5'),
        ("drv_order_vol_imb_21", 'AVG("drv_order_vol_imb") OVER w21'),
        (
            "drv_order_count_imb_z21",
            '("drv_order_count_imb" - AVG("drv_order_count_imb") OVER w21) '
            '/ NULLIF(STDDEV_SAMP("drv_order_count_imb") OVER w21, 0)',
        ),
    )

    # Level 3 — plain row expressions over level 2. No windows, so this rides along on
    # the final SELECT and costs no extra sort.
    UNIFIED_DERIVED_L3: Tuple[Tuple[str, str], ...] = (
        # Fast vol against slow vol: above 1 is a volatility expansion.
        (
            "drv_vol_ratio_10_63",
            '"drv_realized_vol_10" / NULLIF("drv_realized_vol_63", 0)',
        ),
        # Skewness and EXCESS kurtosis from the raw moments carried up from level 2.
        (
            "drv_ret_skew_63",
            '("_m3_63" - 3.0 * "_m1_63" * ("_m2_63" - "_m1_63" * "_m1_63") '
            '        - "_m1_63" * "_m1_63" * "_m1_63") '
            '/ NULLIF(POWER(GREATEST("_m2_63" - "_m1_63" * "_m1_63", 0), 1.5), 0)',
        ),
        (
            "drv_ret_kurt_63",
            '("_m4_63" - 4.0 * "_m1_63" * "_m3_63" '
            '        + 6.0 * "_m1_63" * "_m1_63" * "_m2_63" '
            '        - 3.0 * "_m1_63" * "_m1_63" * "_m1_63" * "_m1_63") '
            '/ NULLIF(POWER(GREATEST("_m2_63" - "_m1_63" * "_m1_63", 0), 2), 0) - 3.0',
        ),
    )

    # Level 3, CROSS-SECTIONAL — `PARTITION BY date`, i.e. across the tickers of one
    # session rather than along one ticker's history.
    #
    # ⚠️ **EMITTED ONLY ON A UNIVERSE PARTITION, and that is a deliberate exception to
    # "the column set must not depend on the partition".** On `unified_schema_vcb` there
    # is exactly one row per date, so `PERCENT_RANK` is 0.0 and a cross-sectional
    # demean is 0.0, on every row, forever. `_ingest_unified_pool_basic_bank`'s identity
    # exclusion makes the same call for the same reason — a column known to be constant
    # before it is written costs selection budget and buys nothing. `pool__basic` on VCB
    # is therefore 5 columns narrower than on `ALL`/`BANK`, and the asset reports which.
    #
    # ⚠️ **NULLS ARE NULLED, not ranked.** `PERCENT_RANK` sorts NULLs last, which would
    # hand a ticker with no return that day a rank of 1.0 — the top of the cross-section.
    #
    # ⚠️ **THIS IS THE ONLY BLOCK THE REPO HAS EVER MEASURED SOMETHING IN.** Hub §2b:
    # single-name prediction fails four independent ways, and the cross-sectional
    # relative rank at 100+ names is what survives its null.
    UNIFIED_DERIVED_CS: Tuple[Tuple[str, str], ...] = (
        (
            "drv_cs_pct_ret_1d",
            'CASE WHEN "drv_ret_1d" IS NOT NULL THEN PERCENT_RANK() OVER '
            '(PARTITION BY "date" ORDER BY "drv_ret_1d" NULLS LAST) END',
        ),
        (
            "drv_cs_pct_turnover",
            'CASE WHEN "_val_tot_vnd" IS NOT NULL THEN PERCENT_RANK() OVER '
            '(PARTITION BY "date" ORDER BY "_val_tot_vnd" NULLS LAST) END',
        ),
        (
            "drv_cs_pct_range",
            'CASE WHEN "drv_range_hl_pct" IS NOT NULL THEN PERCENT_RANK() OVER '
            '(PARTITION BY "date" ORDER BY "drv_range_hl_pct" NULLS LAST) END',
        ),
        # The market factor removed by subtraction rather than by regression — the same
        # thing `return_rel_{h}day` does to the label, applied to the feature.
        (
            "drv_cs_ret_demeaned",
            '"drv_ret_1d" - AVG("drv_ret_1d") OVER (PARTITION BY "date")',
        ),
        # And the same against the GICS industry, so a bank is compared to banks.
        # ⚠️ `industry_code` is NOT point-in-time (silver carries today's code on every
        # historical row), so this is the survivors' industry carried backwards.
        (
            "drv_cs_ret_vs_industry",
            '"drv_ret_1d" - AVG("drv_ret_1d") OVER '
            '(PARTITION BY "date", "industry_code")',
        ),
    )

    # The trailing frames, all sharing one PARTITION BY / ORDER BY so PostgreSQL sorts
    # once and reuses it for every frame. ⚠️ `w{n}` spans n rows ENDING AT CURRENT ROW.
    UNIFIED_DERIVED_FRAMES: Tuple[int, ...] = (5, 10, 21, 63, 252)

    def _helper_unified_derived_sql(self, ticker: str) -> Tuple[str, List[str]]:
        """The derived-feature CTE chain and the output column names, in order.

        Returns `(cte_sql, columns)` where `cte_sql` is everything between `WITH` and
        the final `SELECT`, and `columns` is the `drv_*` list the final `SELECT` must
        emit — which is also what the Dagster asset asserts against the built table.

        ⚠️ The cross-sectional block is included only for a universe partition; see
        `UNIFIED_DERIVED_CS` for why a constant column is worse than a missing one.
        """
        universe = self._helper_unified_is_universe(ticker)

        # ⚠️ ONE CAST, APPLIED TO EVERY FINISHED EXPRESSION. Without it the block's
        # output types follow whatever PostgreSQL inferred — `STDDEV_SAMP` over a
        # bigint returns `numeric`, which psycopg2 hands back as `Decimal` and pandas
        # carries as dtype `object`: rule 15's degraded-VARCHAR trap arriving through
        # the derived half of a table that was written as a CTAS precisely to avoid it.
        # ⚠️ It is applied AFTER the expression, so it cannot rescue an integer
        # division inside one — see `drv_volume_pos_63`.
        def render(specs: Sequence[Tuple[str, str]]) -> str:
            return "".join(
                f', ({sql})::double precision AS "{name}"' for name, sql in specs
            )

        # ⚠️ **A PARTIAL FRAME IS A MISLABELLED CHANNEL, and PostgreSQL gives you one
        # by default.** `ROWS BETWEEN 251 PRECEDING AND CURRENT ROW` computes over
        # whatever rows exist, so `drv_close_pos_252` was non-NULL from the SECOND row
        # of every series — a "position in the trailing 252 days" measured over ten
        # days. Measured 2026-08-16 before this guard: **188,737 of `ALL`'s 2,388,975
        # rows** (7.9%) carried a 252-day channel computed on a shorter window, and
        # every series was affected for its own first year. pandas' `rolling(w)`
        # defaults to `min_periods=w` and does NOT do this, which is why the pandas
        # cross-check could not see it.
        #
        # ⚠️ It matters beyond tidiness: a channel whose MEANING changes over the first
        # year of a series is the ragged-pool problem rule 23 records — a fold-over-fold
        # trend that measures data arrival rather than signal.
        #
        # The frame is read back out of the SQL rather than declared a second time in
        # the spec, and **exactly one frame per expression is asserted** — two would
        # make "is the window full?" ambiguous, and the guard would silently pick one.
        #
        # ⚠️ **`COUNT(*)` asserts a full FRAME — N rows — not N non-NULL inputs**, and
        # for the nine channels built on a lagged return that is a visible one-row
        # difference from pandas. `drv_realized_vol_63` is defined from row 63, where
        # the frame holds 63 rows but only 62 returns (row 1 has no predecessor);
        # `rolling(63).std()` needs 63 non-NaN values and starts at row 64. Frame
        # fullness is the property being asserted, and it is the only one that is
        # well defined for a multi-column expression — `drv_amihud_63` reads both
        # `drv_ret_1d` and `_val_vnd`, so "N non-NULL inputs" would have to pick one.
        # One row per series, against the 188,737 the guard removes.
        def render_windowed(specs: Sequence[Tuple[str, str]]) -> str:
            out = []
            for name, sql in specs:
                used = sorted(set(re.findall(r"\bOVER (w\d+)\b", sql)))
                if len(used) != 1:
                    raise PipelineError(
                        f"Derived channel {name!r} uses {len(used)} window frames "
                        f"({used}); the full-frame guard needs exactly one. Split it "
                        f"into two channels or move it to level 3."
                    )
                frame = used[0]
                out.append(
                    f", (CASE WHEN COUNT(*) OVER {frame} = {int(frame[1:])} "
                    f"THEN ({sql}) END)::double precision AS \"{name}\""
                )
            return "".join(out)

        # One base window definition, inherited by every frame. PostgreSQL only allows
        # inheritance from a window that does not itself specify a frame, which is
        # exactly what `wbase` is.
        frames = ", ".join(
            f'w{n} AS (wbase ROWS BETWEEN {n - 1} PRECEDING AND CURRENT ROW)'
            for n in self.UNIFIED_DERIVED_FRAMES
        )
        wbase = 'wbase AS (PARTITION BY "exchange", "ticker" ORDER BY "date")'

        l1 = self.UNIFIED_DERIVED_L1_HELPERS + self.UNIFIED_DERIVED_L1
        l2 = self.UNIFIED_DERIVED_L2_HELPERS + self.UNIFIED_DERIVED_L2

        cte = (
            # `px` rebuilds the SPLIT-ADJUSTED bar before anything reads it — the
            # `_helper_adjust_ohlc` step, expressed in SQL so the CTAS never has to
            # materialise a Python value (rule 15).
            f"px AS ("
            f'  SELECT s.*,'
            f'         ("open"  * ("close_adjust" / NULLIF("close_raw", 0)))'
            f'           ::double precision AS "_o",'
            f'         ("high"  * ("close_adjust" / NULLIF("close_raw", 0)))'
            f'           ::double precision AS "_h",'
            f'         ("low"   * ("close_adjust" / NULLIF("close_raw", 0)))'
            f'           ::double precision AS "_l",'
            f'         "close_adjust"::double precision AS "_c",'
            # ⚠️ THE 1e9 LIVES HERE AND NOWHERE ELSE. `value_matched` /
            # `value_negotiated` are billions of VND; `foreign_*_value` and `prop_*_val`
            # are plain VND. See the block comment above `UNIFIED_DERIVED_PREFIX`.
            f'         ("value_matched" * 1e9)::double precision AS "_val_vnd",'
            f'         ("value_negotiated" * 1e9)::double precision AS "_val_neg_vnd",'
            # ⚠️ **TOTAL TURNOVER IS THE DENOMINATOR FOR EVERY FLOW RATIO, and using
            # matched alone was a real bug** (found 2026-08-16, after the OUT-1 fix
            # had already run). Foreign and proprietary desks trade in BOTH the
            # matched and the negotiated channel, so a block trade lands in
            # `value_negotiated` while `value_matched` stays small: ABB 2026-06-26 has
            # **19.07 bn matched against 392.62 bn negotiated**, LPB 2026-06-19 has
            # **75 bn against 1,558 bn**. Dividing flow by the matched leg alone
            # inflated those rows ~20x. Measured on the BANK panel, switching to
            # total takes `drv_foreign_net_value_ratio` from **[-239.6, +75.0]** with
            # 63 rows outside ±2 to **[-4.87, +2.27]** with 4, and puts
            # `drv_prop_participation`'s p99 at **0.269**.
            f'         (COALESCE("value_matched", 0) * 1e9'
            f'          + COALESCE("value_negotiated", 0) * 1e9)'
            f'           ::double precision AS "_val_tot_vnd"'
            f"  FROM src s"
            f"), "
            f"l1 AS ("
            f"  SELECT px.*{render(l1)} FROM px WINDOW {wbase}"
            f"), "
            f"l2 AS ("
            f"  SELECT l1.*{render_windowed(l2)} FROM l1 WINDOW {wbase}, {frames}"
            f")"
        )

        columns = [name for name, _ in self.UNIFIED_DERIVED_L1]
        columns += [name for name, _ in self.UNIFIED_DERIVED_L2]
        columns += [name for name, _ in self.UNIFIED_DERIVED_L3]
        if universe:
            columns += [name for name, _ in self.UNIFIED_DERIVED_CS]
        return cte, columns

    def _helper_unified_derived_select(self, ticker: str) -> str:
        """The `drv_*` half of the final SELECT list — level 1 and 2 carried through
        from `l2`, level 3 and the cross-section computed here."""
        universe = self._helper_unified_is_universe(ticker)
        specs = list(self.UNIFIED_DERIVED_L3)
        if universe:
            specs += list(self.UNIFIED_DERIVED_CS)
        carried = [name for name, _ in self.UNIFIED_DERIVED_L1]
        carried += [name for name, _ in self.UNIFIED_DERIVED_L2]
        # Same cast as `render()`, for the same reason — level 3 and the cross-section
        # are computed here rather than in a CTE, so they miss that helper.
        return "".join(f', l2."{c}"' for c in carried) + "".join(
            f', ({sql})::double precision AS "{name}"' for name, sql in specs
        )

    def _ingest_unified_pool_basic(self, ticker: str) -> dict:
        """`silver.stocks_basic` (one ticker) → `unified_schema_<ticker>.pool__basic`.

        **Every column of `silver.stocks_basic`, with silver's own types**, PK
        `(date, exchange, ticker)` — see `UNIFIED_PRIMARY_KEY` for why in that order —
        **plus the `drv_*` derived block**.

        ⚠️ **THIS TABLE STOPPED BEING A FAITHFUL COPY ON 2026-08-16.** It was
        `SELECT *` and nothing else for the whole of its life, and three CONTEXT files
        described it that way. It is now `SELECT *` **plus ~58 trailing derived
        channels** computed in SQL — bar shape, range-based volatility, trailing
        normalisation, order-flow imbalance, foreign/prop flow and liquidity — and 5
        more cross-sectional ones on a universe partition. The contract that survives
        is the SUBSET one: every silver column is still present, with silver's type
        and silver's value. `UNIFIED_DERIVED_L1` and its siblings are the spec, and
        the block comment above them carries the six warnings that shaped it.

        ⚠️ **The derived columns are `double precision`, the silver ones are not.**
        Nothing casts `numeric` here; the derived expressions cast their own inputs,
        so `close_adjust` stays `numeric` and `drv_ret_1d` arrives as a float.

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
        # ⚠️ ONE PARAM WAS ASSUMED AND THAT WAS A BUG (fixed 2026-08-22). This was
        # `predicate.replace('%s', repr(*params))`, which works for the three original
        # sentinels — `BANK`, `VN30` and a bare ticker each bind exactly one value —
        # and raises `TypeError: repr() takes exactly one argument (0 given)` for a
        # SCREEN, whose predicate is a self-contained sub-select binding NONE. A
        # display helper taking down a build is the worst kind of failure: nothing was
        # wrong with the data or the SQL.
        scope = (
            "the whole universe"
            if not predicate
            else self._helper_unified_describe_predicate(predicate, params)
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

            # ⚠️ THE SILVER HALF OF THE SELECT LIST IS EXPLICIT, NOT `l2.*`. The CTE
            # chain carries helper columns (`_o`, `_h`, `_gk_var`, `_m3_63`, …) that
            # must never reach the table, and naming the silver columns from
            # `information_schema` is what guarantees both that every one of them
            # survives and that no helper does. Referencing a column preserves its
            # type, so this is still the type-faithful CTAS the docstring describes.
            silver_select = ", ".join(f'l2."{c}"' for c in source_types)
            derived_cte, derived_columns = self._helper_unified_derived_sql(ticker)
            derived_select = self._helper_unified_derived_select(ticker)

            # Dropped as late as possible, so a failure above leaves the old table intact
            # — the same ordering `_ingest_gold_table` uses.
            cur.execute(f"DROP TABLE IF EXISTS {schema}.pool__basic")
            cur.execute(
                f"CREATE TABLE {schema}.pool__basic AS "
                f"WITH src AS ("
                f"  SELECT * FROM {SILVER_SCHEMA}.stocks_basic{where}"
                f"), {derived_cte} "
                f"SELECT {silver_select}{derived_select} FROM l2",
                params,
            )
            # CTAS copies types but never constraints, so the grain is asserted here.
            self._helper_unified_primary_key(cur, schema, "pool__basic")
            cur.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT ticker) FROM {schema}.pool__basic"
            )
            written, series = (int(x) for x in cur.fetchone())

            # ⚠️ Counted, not assumed. A derived channel whose whole source block is
            # absent — `prop_*` before 2023, `foreign_*` before 2012 — is legitimately
            # all-NULL, and the only way that is visible downstream is if somebody
            # writes the number down. This is rule 22 at the feature, one level below
            # the pool-level coverage every other pool reports.
            populated = ", ".join(f'COUNT("{c}")' for c in derived_columns)
            cur.execute(f"SELECT {populated} FROM {schema}.pool__basic")
            counts = dict(zip(derived_columns, (int(x) for x in cur.fetchone())))
            empty = sorted(c for c, n in counts.items() if n == 0)

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
        #
        # ⚠️ `universe and` WAS MISSING UNTIL 2026-08-05, AND IT MADE EVERY ORDINARY
        # TICKER UNBUILDABLE. `_helper_unified_member_filter` returns
        # `("ticker = %s", (ticker,))` for a real company — a perfectly ordinary
        # predicate — so `if predicate and series < 2` fired on exactly the case it
        # was written to allow, and `_ingest_unified_pool_basic("VCB")` raised
        # "a sector schema is a CROSS-SECTION" about a schema that is one company on
        # purpose. `universe` is what distinguishes a SENTINEL from a ticker, and it
        # is the distinction this guard was always describing in prose.
        if universe and predicate and series < 2:
            raise PipelineError(
                f"{schema}.pool__basic holds {series} ticker(s) for {scope} — a "
                f"sector schema is a CROSS-SECTION, and one company is not one. "
                f"Check the classification in `{SILVER_SCHEMA}.stocks_basic`."
            )
        # ⚠️ AN EMPTY DERIVED CHANNEL IS REPORTED, NEVER RAISED. `prop_*` is 3.1%
        # covered market-wide and starts 2023-01-03, so a universe or a date range that
        # predates it produces two legitimately all-NULL columns. Raising would make a
        # correct table unbuildable — the `pool__fa` coverage decision of 2026-08-05,
        # reached again for the same reason. It is a WARNING because the alternative is
        # a channel that reaches the selector as pure imputed constant (rule 23).
        if empty:
            self._logger.log_warning(
                f"{schema}.pool__basic: {len(empty)} derived channel(s) are entirely "
                f"NULL — {empty}. Their source block is absent for this universe or "
                f"date range; they will be imputed to a constant and ranked if they "
                f"reach a selection."
            )
        self._logger.log_info(
            f"{schema}.pool__basic: {written} rows x "
            f"{len(source_types) + len(derived_columns)} columns "
            f"({len(source_types)} from silver + {len(derived_columns)} derived, "
            f"{len(empty)} of them empty), {series} ticker(s)."
        )
        return {
            "rows": written,
            "series": series,
            "source_columns": list(source_types),
            "derived_columns": derived_columns,
            "derived_populated": counts,
            "derived_empty": empty,
            "cross_sectional": universe,
        }

    # The label horizons, in TRADING DAYS — `pool__basic` is one row per session, so a
    # row offset IS a trading-day offset and no calendar arithmetic is involved.
    # ⚠️ Each target COLUMN NAME is derived from its horizon (`return_5day`), so the two
    # cannot drift: changing a horizon renames the column instead of silently
    # re-defining it, and ADDING one adds a column rather than replacing the table's
    # meaning. That is why this is a tuple and not the scalar it started as — a model
    # comparing h=5 against h=10 needs both labels on one calendar, and deriving the
    # second one anywhere else would put the label definition in two places.
    # ⚠️ 20 ADDED 2026-08-17 for the 4-WEEK experiment (TODO P2-1 v2). CLAUDE.md
    # §2a-bis is the one measurement that varied the HORIZON rather than the
    # features, and it found VN giving signal at 4-13 weeks and none at 5-10
    # sessions. Adding a horizon costs 3 columns per h and no recomputation of the
    # others; ⚠️ but it also costs `h` more NULL rows at the tail of the panel, and
    # `n_eff = n/h` falls to 213 on VCB — which is why the experiment this was
    # added for is CROSS-SECTIONAL, where width buys back precision per observation.
    UNIFIED_TARGET_HORIZONS = (5, 10, 20)

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

        `UNIFIED_PRIMARY_KEY` — `(date, exchange, ticker)` — plus **three columns per
        horizon in `UNIFIED_TARGET_HORIZONS`**: `return_{h}day`, the forward simple
        return `close[t+h] / close[t] - 1` on the SPLIT-ADJUSTED close,
        `return_rel_{h}day`, the same minus the benchmark's return over that window,
        and `close_adjust_{h}day`, the forward adjusted close `close[t+h]` itself.

        ⚠️ **`close_adjust_{h}day` IS A LABEL, NOT A FEATURE** (added 2026-08-12). It is
        `LEAD(close_adjust, h)` — the same lead `return_{h}day` divides by `close[t]` —
        so it is the answer in price units rather than in return units, for a model
        asked to predict a level. It reads like a `pool__basic` column and it is not
        one: anything that joins `pool__basic ⋈ pool__targets` and treats every
        non-key column as a candidate feature has just been handed the target.
        `feature_selection.run.ALL_TARGETS` names it for exactly that reason.

        ⚠️ **No `NULLIF` on the price column, deliberately.** `return_{h}day` guards
        `close[t] = 0` because it divides by it; a forward LEVEL has no denominator, so
        a zero close would come through as the zero it is. The tail assertion below is
        still `h × series` exactly, and it is not a weaker check than the return's: a
        return tail of exactly `h × series` already proves no `close_adjust` in the pool
        is NULL or zero outside the tail.

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
        # ⚠️ Derived from the SAME `horizons` tuple as the two return families, so the
        # three cannot drift apart: adding a horizon adds three columns, and there is no
        # way to end up with a `return_5day` whose forward price is the 10-day one.
        price_cols = {h: f"close_adjust_{h}day" for h in horizons}
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
            # ⚠️ **EVERY SERIES' LENGTH, not just the shortest (changed 2026-08-17).**
            # This used to fetch `MIN(rows)` and RAISE if any series was shorter than the
            # longest horizon, on the reasoning that such a series is entirely unlabelled
            # and "would quietly shift every tail assertion below". The reasoning was
            # right and the remedy was wrong: adding `h=20` for the 4-week experiment made
            # **one** series of 781 too short — `SDA`, 19 rows — and that single ticker
            # blocked the whole universe from gaining a horizon it can carry for the other
            # 780.
            #
            # A series shorter than `h` is not an error, it is a young listing. What it
            # breaks is the ASSUMPTION `expected_tail = h × series`, so the assertion is
            # generalised instead of the input being filtered: a series of `n` rows
            # contributes `min(h, n)` unlabelled rows, exactly. That is still an EQUALITY,
            # not a loosened bound — it is the same check, correct for the general case.
            cur.execute(
                f"SELECT COUNT(*) AS rows FROM {schema}.pool__basic "
                f"GROUP BY exchange, ticker"
            )
            series_rows = [int(row[0]) for row in cur.fetchall()]
            series = len(series_rows)
            shortest = min(series_rows) if series_rows else 0
            available = sum(series_rows)
            if available <= longest:
                raise MissingSourceDataError(
                    f"`{schema}.pool__basic` holds {available} rows, which is not more "
                    f"than the longest {longest}-day horizon — every label would be "
                    f"NULL."
                )
            too_short = [n for n in series_rows if n <= longest]
            if len(too_short) == series:
                raise MissingSourceDataError(
                    f"`{schema}.pool__basic`: EVERY one of its {series} series is "
                    f"{longest} rows or shorter, so no row would carry a "
                    f"{longest}-day label at all."
                )
            if too_short:
                # Reported, never silent: these series contribute zero labels at the
                # longest horizon and are simply absent from its cross-sections.
                self._logger.log_info(
                    f"{schema}.pool__targets: {len(too_short)} of {series} series are "
                    f"<= {longest} rows (shortest {shortest}) and carry NO "
                    f"{longest}-day label. They still carry the shorter horizons."
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
                # ⚠️ Cast to `double precision` like its two siblings rather than kept
                # at `pool__basic.close_adjust`'s own type. psycopg2 hands `numeric`
                # back as `Decimal`, which pandas carries as dtype `object` — a
                # forward PRICE is the one column here a reader is most likely to do
                # arithmetic on, so it arrives as a float or not at all.
                + [
                    f"(LEAD(px, {h}) OVER w)::double precision AS {col}"
                    for h, col in price_cols.items()
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

            # ⚠️ Counted by COLUMN NAME, not by position within a per-family slice.
            # With two families the slice arithmetic was already the fiddliest line
            # here; with three it would be the kind of off-by-one that reports another
            # column's NULL count and passes.
            ordered = (
                list(target_cols.values())
                + list(relative_cols.values())
                + list(price_cols.values())
            )
            counts = ", ".join(f"COUNT({col})" for col in ordered)
            cur.execute(f"SELECT COUNT(*), {counts} FROM {schema}.pool__targets")
            row = [int(x) for x in cur.fetchone()]
            written = row[0]
            labelled_by_col = dict(zip(ordered, row[1:]))

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
        #
        # ⚠️ `close_adjust_{h}day` is checked by the SAME rule and the same number. It
        # is the numerator of `return_{h}day` with no denominator, so its NULLs are the
        # tail and nothing else; a disagreement between the two would mean the LEAD ran
        # over a different row ordering, which is the one failure the shared `WINDOW w`
        # is supposed to make impossible.
        # ⚠️ `min(h, n)` PER SERIES, not `h × series` (generalised 2026-08-17). A series
        # of `n` rows with `n <= h` has no future for ANY of its rows, so it contributes
        # `n` NULLs, not `h`. Still an EQUALITY — the same check, correct for a universe
        # that contains a young listing. On `unified_schema_all` at h=20 exactly one
        # series (`SDA`, 19 rows) makes the two formulas differ, and the old one raised.
        for h in horizons:
            expected_tail = sum(min(h, n) for n in series_rows)
            for col in (target_cols[h], price_cols[h]):
                unlabelled = written - labelled_by_col[col]
                if unlabelled != expected_tail:
                    raise PipelineError(
                        f"{schema}.pool__targets has {unlabelled} NULL {col} values; "
                        f"exactly {expected_tail} (sum of min({h}, series length) over "
                        f"{series} series, the tail with no future) were expected. Check "
                        f"`pool__basic.close_adjust` for NULLs or zeros."
                    )
        # ⚠️ The relative column loses its own tail PLUS every row a benchmark gap
        # touches: a missing `B[t]` kills row `t`, and a missing `B[t+h]` kills row
        # `t-h`. One gap DATE therefore costs up to `h + 1` rows IN EVERY SERIES. The
        # bound is asserted rather than the exact count, because two gaps within `h` of
        # each other overlap — but it still fails loudly if the benchmark is broadly
        # absent instead of merely pitted.
        for h, col in relative_cols.items():
            unlabelled = written - labelled_by_col[col]
            floor = sum(min(h, n) for n in series_rows)
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
            f"{col} {labelled_by_col[col]} labelled / "
            f"{written - labelled_by_col[col]} null"
            for col in ordered
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
    UNIFIED_NEWS_SOURCE = f"{GOLD_SCHEMA}.news_daily_panel"

    # ⚠️ COLUMNS `gold.news_daily_panel` CARRIES THAT ARE PRICE, NOT NEWS. The panel
    # was built to be self-contained for the costed walk-forward, so it re-derives its
    # own returns and turnover beside the event counts — `pool__basic` already owns
    # every one of them, under these exact names or as a `drv_*` twin. Letting them
    # through would hand the correlation prune eight duplicates to rediscover, and
    # would put a SECOND `close_adjust` in a panel whose target is derived from the
    # first. What is left is the ~18 columns that are actually news.
    #
    # ⚠️ `ret_5d` IS TRAILING, verified 2026-08-16 on VCB: corr(+1.000000) against
    # `close_adjust.pct_change(5)` and corr(−0.006) against the FORWARD 5-day return.
    # It is excluded as a duplicate, NOT as a leak — the distinction matters, because
    # a leak would mean `gold.news_daily_panel` is unusable rather than merely wide.
    UNIFIED_NEWS_PRICE_DUPES = (
        "close_adjust", "value_matched",
        "ret_1d", "ret_5d", "ret_10d", "ret_20d", "ret_60d",
        "vol_20d", "log_value_20d",
    )

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
        spine_outward: bool = False,
    ) -> Tuple[int, int]:
        """Shared body of `_ingest_unified_pool_ta` / `_ingest_unified_pool_fa`.

        ⚠️ **The source is INNER JOINED to `pool__basic` on the whole key, not read
        on its own.** `gold.stocks_ta` runs to 2026-06-26 where `pool__basic` stops
        at 2026-06-25, so a straight copy would produce a pool with 4,242 rows
        against `pool__basic`'s 4,235 — the exact mismatch that made the dropped
        `pool__targets` unjoinable (see `_ingest_unified_pool_targets`). Joining to
        the spine makes one calendar structural instead of hoped for.

        ⚠️ **`spine_outward=True` FLIPS THE JOIN TO `spine LEFT JOIN source`**, which
        is the right shape when the SOURCE covers only part of the CALENDAR rather
        than only part of the universe. Added 2026-08-16 for `pool__news_daily`: the
        news corpus starts 2013-01-02 against a VCB spine starting 2009-06-30, so the
        INNER join wrote 3,355 rows and the asset's own "a partial calendar means days
        went missing" assertion caught it — correctly. A missing DAY must become a row
        of NULLs, never a missing row, or the pool silently changes the calendar under
        its own primary key. This is the same rule the date-broadcast pools already
        state: LEFT JOIN, never INNER.

        ⚠️ The default stays INNER, because for `pool__ta` / `pool__fa` it is load-
        bearing: `gold.stocks_ta` runs one session PAST `pool__basic`, and an outward
        join there would keep the spine's calendar while an inward one proves the two
        agree. Two sources, two shapes, one flag that says which.

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
        # ⚠️ On the outward join the KEY must come from the spine, not the source: the
        # source row is NULL wherever the corpus does not reach, and a key of NULLs is
        # not a key. Everything else still comes from `s`.
        key = set(self.UNIFIED_PRIMARY_KEY)
        selected = ", ".join(
            (f"b.{c}" if spine_outward and c in key else f"s.{c}") for c in columns
        )
        where = "" if universe else " AND s.ticker = %s"
        params = () if universe else (ticker,)
        if spine_outward:
            join = (
                f"FROM {schema}.pool__basic b LEFT JOIN {source} s "
                f"ON b.date = s.date AND b.exchange = s.exchange "
                f"AND b.ticker = s.ticker"
            )
            filter_sql = "" if universe else " WHERE b.ticker = %s"
        else:
            join = (
                f"FROM {source} s JOIN {schema}.pool__basic b "
                f"ON b.date = s.date AND b.exchange = s.exchange "
                f"AND b.ticker = s.ticker"
            )
            filter_sql = "" if universe else " WHERE TRUE AND s.ticker = %s"

        with self._database_driver._cursor_ctx() as cur:
            cur.execute(f"SELECT COUNT(*) {join}{filter_sql}", params)
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
                f"CREATE TABLE {schema}.{pool} AS SELECT {selected} {join}{filter_sql}",
                params,
            )
            self._helper_unified_primary_key(cur, schema, pool)

            cur.execute(f"SELECT COUNT(*) FROM {schema}.{pool}")
            written = int(cur.fetchone()[0])
            cur.execute(f"SELECT COUNT(*) FROM {schema}.pool__basic")
            spine = int(cur.fetchone()[0])
            # ⚠️ EXCEPT IN ONE DIRECTION ONLY — every pool row must be a spine row,
            # but NOT every spine row needs a pool row (changed 2026-08-05).
            #
            # It was symmetric, on the reasoning that two tables can agree on how many
            # rows they hold and disagree about WHICH. That half is kept: an orphaned
            # pool row is still a hard error, because it cannot be joined to anything
            # and its presence means the join key is wrong.
            #
            # The other half was FALSE FOR EVERY MULTI-TICKER UNIVERSE, and only ever
            # looked right because the sole caller was one company. A feature pool is
            # as wide as its SOURCE: `gold.stocks_financials_bank_fa` is built from the
            # CafeF *bank* chart of accounts and holds VCB and ACB alone, so on
            # `unified_schema_bank`'s 20-ticker spine it covers 8,265 of 53,921 rows
            # and on `unified_schema_all` 8,265 of 2,388,368. Demanding equality there
            # does not protect anything — it just makes the table unbuildable.
            #
            # Coverage is REPORTED instead of assumed, here and in the asset's
            # metadata, so a pool that silently shrank is visible rather than fatal.
            # Consumers LEFT JOIN a feature pool onto the spine.
            cur.execute(
                f"SELECT COUNT(*) FROM ("
                f"  SELECT date, exchange, ticker FROM {schema}.{pool}"
                f"  EXCEPT SELECT date, exchange, ticker FROM {schema}.pool__basic"
                f") d"
            )
            orphaned = int(cur.fetchone()[0])

        if written != available:
            raise PipelineError(
                f"{schema}.{pool} wrote {written} rows against {available} joinable."
            )
        if orphaned:
            raise PipelineError(
                f"{schema}.{pool} has {orphaned} row(s) whose (date, exchange, ticker) "
                f"is not in pool__basic. Every pool row must sit on the spine or it "
                f"cannot be joined to anything."
            )
        self._logger.log_info(
            f"{schema}.{pool}: {written} rows covering "
            f"{100.0 * written / max(spine, 1):.1f}% of pool__basic's {spine}."
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

    def _ingest_unified_pool_news_daily(self, ticker: str) -> None:
        """`gold.news_daily_panel` → `…​.pool__news_daily` — the EVENT block.

        ~18 columns of news and disclosure COUNTS at a daily grain, keyed
        `(date, exchange, ticker)` and on `pool__basic`'s calendar: `n_docs_{5,10}d`,
        `n_editorial_*`, `n_docs_named_*`, `n_earnings_*`, `relevance_max_*`,
        `if_news_*`, `if_editorial_*`.

        ⚠️ **THIS IS NOT THE SENTIMENT THREAD, AND THE DISTINCTION IS THE WHOLE REASON
        IT IS BEING BUILT.** CLAUDE.md §2a records that news *sentiment* found no
        signal on 3 tickers and made price/TA models WORSE (QWK 0.175 → 0.045). This
        pool carries no sentiment at all — it is counts of things that happened and
        when they were disclosed, which §2d lists SEPARATELY as the third-ranked
        remaining lever ("news+disclosure event dates"). A negative result on the
        first says nothing about the second.

        ⚠️ **NINE PRICE COLUMNS ARE DROPPED** — see `UNIFIED_NEWS_PRICE_DUPES`. The
        panel re-derives its own returns and turnover so it can stand alone; joined
        onto `pool__basic` they are duplicates, and one of them is a second
        `close_adjust` in a panel whose label is built from the first.

        ⚠️ **COVERAGE IS PARTIAL BY CONSTRUCTION AND THAT IS NOT A DEFECT.** The
        source starts 2013-01-02 against a VCB spine starting 2009-06-30, and it
        carries a row only where the corpus does — VCB has 3,355 rows against a 4,266
        spine (78.6%). A LEFT JOIN makes the rest NULL rather than dropping them, and
        `feature_selection`'s `coverage` column is where a consumer reads that. ⚠️ But
        read `trailing_null_sessions` beside it (rule 22): the corpus ends 2026-07-08,
        which is ~16 sessions behind the spine's 2026-08-07.

        ⚠️ **A ZERO IS A MEASUREMENT HERE, NOT A GAP.** `n_docs_5d = 0` means the
        corpus was searched and found nothing that window; NULL means the day is
        outside the corpus entirely. `_impute` fills NULL with the train median and
        cannot tell the two apart, so a channel that is mostly zeros will impute to
        zero and look well-covered. That is why the row count is asserted against the
        source, not against the spine.
        """
        schema = self._helper_unified_schema(ticker)
        self._logger.log_info(
            f"Ingesting unified {schema}.pool__news_daily "
            f"(from {self.UNIFIED_NEWS_SOURCE})..."
        )

        exclude = (
            self.UNIFIED_POOL_IDENTITY
            + self.UNIFIED_POOL_PRICE_DUPES
            + self.UNIFIED_NEWS_PRICE_DUPES
        )
        # ⚠️ `spine_outward=True`: the corpus starts 2013 and the spine starts 2009, so
        # an INNER join would DROP 911 VCB rows rather than NULL them — a pool that
        # silently changes the calendar under its own primary key.
        rows, columns = self._helper_unified_pool_from_source(
            ticker,
            "pool__news_daily",
            self.UNIFIED_NEWS_SOURCE,
            exclude=exclude,
            spine_outward=True,
        )

        with self._database_driver._cursor_ctx() as cur:
            # ⚠️ ASSERTED: not one surviving column may be a price. The exclusion list
            # is hand-maintained and `gold.news_daily_panel` is a young table — a
            # column added upstream would otherwise arrive silently, and the one class
            # that must never arrive is another copy of the series the label is
            # derived from.
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = 'pool__news_daily'",
                (schema,),
            )
            present = {r[0] for r in cur.fetchall()}
        leaked = sorted(present & set(self.UNIFIED_NEWS_PRICE_DUPES))
        if leaked:
            raise PipelineError(
                f"{schema}.pool__news_daily carries price column(s) {leaked}, which "
                f"`pool__basic` already owns. Joined together a model sees the series "
                f"twice, and `close_adjust` is the series `pool__targets` derives the "
                f"label from. Add them to UNIFIED_NEWS_PRICE_DUPES."
            )
        self._logger.log_info(
            f"{schema}.pool__news_daily: {rows} rows x {columns} columns."
        )

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

    # ⚠️ `_run_layer` AND THE THREE PUBLIC ENTRY POINTS WERE DELETED 2026-08-05
    # (phase 5 of the Dagster migration). `ingest_bronze_data` / `ingest_silver_data`
    # / `ingest_gold_data` were the `main.py` run plan: they walked a hard-coded leaf
    # list, consulted `switch_config.json` for which leaves to run, and DELIBERATELY
    # DID NOT RAISE so `main.py` could run to completion. Both of those properties are
    # wrong under an orchestrator — a run plan that lives in a JSON file is a second
    # source of truth, and a failure that does not raise is a green run.
    #
    # THIS CLASS IS NOW A LIBRARY, NOT A RUNNER. Every `_ingest_*` method above is
    # called directly by the Dagster asset that wraps it
    # (`src/orchestration/assets/`), so an exception propagates and the asset goes
    # red. Selection is the run plan. To add a table, write the `_ingest_*` method
    # here and the asset there; do not reintroduce a leaf list.
    #
    # `main.py` and `src/data_postprocessor/` were deleted in the same change. Code at
    # `f4bc4a2` if any of it is ever wanted back.
