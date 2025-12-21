from typing import List, Optional, Tuple
from dotenv import load_dotenv
import os
import pandas as pd
from math import ceil


from sklearn.preprocessing import MinMaxScaler


from feature_selector.feature_selector import FeatureSelector
from logger.logger import Logger
from train_test_creator.train_test_set import TrainTestSet
from utils.constants import *
from utils.utils import *


load_dotenv()


class TrainTestCreator:
    def __init__(self, logger: Logger):
        self._logger = logger

        self.train_set: Optional[pd.DataFrame] = None
        self.val_set: Optional[pd.DataFrame] = None
        self.test_set: Optional[pd.DataFrame] = None

    def load_dataframe(self, stock_code: str, file_path: str = None) -> pd.DataFrame:
        if not stock_code:
            raise ValueError("Stock code must be provided.")

        stock_code = stock_code.lower()

        if not file_path:
            file_path = os.path.join(UNIFIED_DATAFRAME_DIR, f"unified_{stock_code}.csv")

        dataframe = pd.read_csv(file_path)
        self._logger.log_info(
            f"Loaded unified dataframe for stock code '{stock_code}' from file with {len(dataframe)} rows and {len(dataframe.columns)} columns."
        )
        return dataframe

    # ------------------------------------------------------------------
    # NORMALIZATION: fit scalers only on TRAINING portion and apply consistently
    # ------------------------------------------------------------------
    def _fit_scalers(
        self, train_df: pd.DataFrame, output_column: str
    ) -> Tuple[MinMaxScaler, MinMaxScaler, List[str]]:
        """Fit feature and target scalers on the training dataframe only.

        Returns: (feature_scaler, target_scaler, numeric_feature_cols)
        """
        df = train_df.copy()
        df = df.drop(columns=["date"]) if "date" in df.columns else df

        # numeric features (exclude output)
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if output_column in numeric_cols:
            numeric_cols.remove(output_column)

        # drop constant columns (computed on train only)
        constant_cols = [c for c in numeric_cols if df[c].nunique() <= 1]
        if constant_cols:
            self._logger.log_info(f"Dropping constant feature columns: {constant_cols}")
            numeric_cols = [c for c in numeric_cols if c not in constant_cols]

        # Fit feature scaler on numeric_cols
        feature_scaler = MinMaxScaler()
        if numeric_cols:
            feature_scaler.fit(df[numeric_cols].values)
        else:
            # empty scaler placeholder
            feature_scaler = None

        # Fit target scaler (1D)
        target_scaler = MinMaxScaler()
        target_values = df[[output_column]].values
        target_scaler.fit(target_values)

        return feature_scaler, target_scaler, numeric_cols

    def _apply_scalers_to_df(
        self,
        df: pd.DataFrame,
        feature_scaler: Optional[MinMaxScaler],
        target_scaler: MinMaxScaler,
        numeric_feature_cols: List[str],
        output_column: str,
    ) -> pd.DataFrame:
        """Apply previously fitted scalers to a dataframe (do NOT re-fit)."""
        out = df.copy()
        if "date" in out.columns:
            out = out.drop(columns=["date"])  # keep order consistent

        if numeric_feature_cols and feature_scaler is not None:
            out[numeric_feature_cols] = feature_scaler.transform(
                out[numeric_feature_cols].values
            )

        # transform target
        out[[output_column]] = target_scaler.transform(out[[output_column]].values)
        return out

    # ------------------------------------------------------------------
    # Main: create_train_test_set (rewritten)
    # ------------------------------------------------------------------
    def create_train_test_set(
        self,
        dataframe: pd.DataFrame,
        output_column: str,
        stock_code: str,
        input_size: int,
        forecast_size: int,
    ) -> TrainTestSet:

        # --- Basic validation (reuse your validator) ---
        self._validate_input(
            dataframe=dataframe,
            output_column=output_column,
            stock_code=stock_code,
            input_size=input_size,
            forecast_size=forecast_size,
        )

        stock_code = str.lower(stock_code)

        # Ensure output column is last in ordering (keeps consistent ordering)
        dataframe = move_column_to_end(dataframe, output_column)

        # Expand date
        dataframe = expand_date_column(dataframe)
        dataframe = (
            dataframe.drop(columns=["date"])
            if "date" in dataframe.columns
            else dataframe
        )

        val_set_size = VAL_SET_SIZE
        test_set_size = TEST_SET_SIZE
        train_set_size = len(dataframe) - (val_set_size + test_set_size)

        self.train_set = dataframe.iloc[0:train_set_size].reset_index(drop=True).copy()
        self.val_set = (
            dataframe.iloc[train_set_size : train_set_size + val_set_size]
            .reset_index(drop=True)
            .copy()
        )
        self.test_set = (
            dataframe.iloc[train_set_size + val_set_size :]
            .reset_index(drop=True)
            .copy()
        )

        self._logger.log_info(f"Train portion:  {len(self.train_set)}   rows")
        self._logger.log_info(f"Val portion:    {len(self.val_set)}     rows")
        self._logger.log_info(f"Test portion:   {len(self.test_set)}    rows")

        # --- Feature selection should be based on training portion only ---
        train_for_fs = (
            self.train_set.drop(columns=["date"])
            if "date" in self.train_set.columns
            else self.train_set
        )
        feature_columns = train_for_fs.columns.tolist()
        if output_column in feature_columns:
            feature_columns.remove(output_column)

        self._feature_selector = FeatureSelector(
            logger=self._logger,
            stock_code=stock_code,
            dataframe=train_for_fs,
            feature_columns=feature_columns,
            target_column=output_column,
        )
        features_to_drop = self._feature_selector.get_features_to_drop()

        selected_train_set = self.train_set.drop(columns=features_to_drop)
        selected_val_set = self.val_set.drop(
            columns=[c for c in features_to_drop if c in self.val_set.columns]
        )
        selected_test_set = self.test_set.drop(
            columns=[c for c in features_to_drop if c in self.test_set.columns]
        )

        # --- Create fit scalers from train set ---
        feature_scaler, target_scaler, numeric_feature_cols = self._fit_scalers(
            train_df=selected_train_set, output_column=output_column
        )

        # --- Apply scalers for train set ---
        normalized_train_set = self._apply_scalers_to_df(
            selected_train_set,
            feature_scaler,
            target_scaler,
            numeric_feature_cols,
            output_column,
        )

        # --- Apply scalers for val set ---
        normalized_val_set = self._apply_scalers_to_df(
            selected_val_set,
            feature_scaler,
            target_scaler,
            numeric_feature_cols,
            output_column,
        )

        # --- Apply scalers for test set ---
        normalized_test_set = self._apply_scalers_to_df(
            selected_test_set,
            feature_scaler,
            target_scaler,
            numeric_feature_cols,
            output_column,
        )

        total_window_size = input_size + forecast_size

        # --- Build Train windows ---
        train_windows: List[pd.DataFrame] = []

        train_window_stride = 1

        for start_idx in range(
            0, train_set_size - total_window_size, train_window_stride
        ):
            end_idx = start_idx + total_window_size
            window_df = normalized_train_set.iloc[start_idx:end_idx].reset_index(
                drop=True
            )
            # Sanity: each window length must equal total_window_size
            if len(window_df) == total_window_size:
                train_windows.append(window_df)

        self._logger.log_info(
            f"Created {len(train_windows)} training windows (stride={train_window_stride})."
        )

        # --- Build Val windows ---
        val_windows: List[pd.DataFrame] = []

        val_window_stride = 1

        for start_idx in range(
            0, input_size + val_set_size - total_window_size, val_window_stride
        ):
            end_idx = start_idx + total_window_size
            window_df = (
                pd.concat(
                    [
                        normalized_train_set.iloc[train_set_size - input_size :],
                        normalized_val_set,
                    ],
                    ignore_index=True,
                )
                .iloc[start_idx:end_idx]
                .reset_index(drop=True)
            )
            # Sanity: each window length must equal total_window_size
            if len(window_df) == total_window_size:
                val_windows.append(window_df)

        # --- Build Test windows ---

        # --- Create TrainTestSet and attach scalers for downstream use ---
        tts = TrainTestSet(
            name=f"{stock_code}_input_size_{input_size}_forecast_size_{forecast_size}_window_stride_{window_stride}",
            data_set=dataframe,
            train_set=selected_train_set,
            val_set=selected_val_set,
            test_set=selected_test_set,
            output_column=output_column,
            input_size=input_size,
            forecast_size=forecast_size,
            train_windows=train_windows,
            val_windows=[normalized_val_set],
            test_windows=[normalized_test_set],
            norm_train_set=normalized_train_set,
            norm_val_set=normalized_val_set,
            norm_test_set=normalized_test_set,
            feature_scaler=feature_scaler,
            target_scaler=target_scaler,
            numeric_feature_cols=numeric_feature_cols,
        )

        self._logger.log_info(
            "TrainTestSet created successfully with scalers fitted on training portion."
        )

        return tts

    # --------------------------
    # Input validator - reuse your original implementation (kept as-is)
    # --------------------------
    def _validate_input(
        self,
        dataframe: pd.DataFrame,
        output_column: str,
        stock_code: str,
        input_size: int,
        forecast_size: int,
    ) -> bool:
        total_required = (input_size + forecast_size) * 3
        if len(dataframe) <= total_required:
            raise ValueError(
                f"Dataframe length ({len(dataframe)}) must be greater than "
                f"3 * (input_size + forecast_size)"
                f"({total_required})."
            )

        if output_column not in dataframe.columns:
            raise ValueError(
                f"Output column '{output_column}' not found in dataframe columns: {list(dataframe.columns)}."
            )

        if not isinstance(stock_code, str) or not stock_code.strip():
            raise ValueError(
                "Stock code must be a non-empty string after stripping whitespace."
            )

        for name, value in {
            "input_size": input_size,
            "forecast_size": forecast_size,
        }.items():
            if not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer (got {value!r}).")

        return True
