from typing import List
from dotenv import load_dotenv
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import xgboost as xgb
import shap

from sklearn.linear_model import LassoCV, ElasticNetCV
from sklearn.cluster import AgglomerativeClustering
from sklearn.preprocessing import StandardScaler

from logger.logger import Logger
from dtos.tabular_database_driver_dtos.tabular_database_driver_dtos import (
    Condition,
    JoinModel,
)
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from dtos.tabular_database_driver_dtos.postgre_sql_connection_dto import (
    PostgreSQLConnectionDto,
)
from utils.constants import *
from utils.enums import *
from utils.utils import *

# Optional: only import if available
try:
    from tsfresh import extract_features
except ImportError:
    extract_features = None


load_dotenv()


def summarize_feature_importances(
    normalized_importances_dict: dict[str, pd.DataFrame],
    weights: dict[str, float],
) -> pd.DataFrame:
    """
    Combine normalized feature importances from multiple methods using specified weights.

    Parameters
    ----------
    normalized_importances_dict : dict[str, pd.DataFrame]
        Dictionary where keys are method names (e.g., 'lasso', 'xgb', 'shap')
        and values are normalized DataFrames with columns ['feature', 'importance'].
        Each DataFrame should already have its 'importance' values normalized (e.g., min-max scaled).
    weights : dict[str, float]
        Dictionary of weights for each method. Keys must match those in normalized_importances_dict.
        Values should sum approximately to 1.

    Returns
    -------
    pd.DataFrame
        A DataFrame with all individual importances and the final weighted importance,
        sorted by 'final_importance' descending.
    """
    if not normalized_importances_dict:
        raise ValueError("normalized_importances_dict cannot be empty.")
    if not weights:
        raise ValueError("weights cannot be empty.")
    if not set(normalized_importances_dict.keys()).issubset(set(weights.keys())):
        raise ValueError(
            "Weights must be provided for all methods in normalized_importances_dict."
        )

    # Collect all unique features
    all_features = set()
    for df in normalized_importances_dict.values():
        if not {"feature", "importance"}.issubset(df.columns):
            raise ValueError(
                "Each DataFrame must contain ['feature', 'importance'] columns."
            )
        all_features.update(df["feature"].tolist())

    combined_df = pd.DataFrame({"feature": sorted(all_features)})

    # Merge all normalized importances
    for method, df in normalized_importances_dict.items():
        temp = df.copy().rename(columns={"importance": method})
        combined_df = combined_df.merge(
            temp[["feature", method]], on="feature", how="left"
        )

    combined_df = combined_df.fillna(0)

    # Apply weights
    for method, weight in weights.items():
        if method not in combined_df.columns:
            print(
                f"Method '{method}' not found in normalized_importances_dict; skipping."
            )
            continue
        combined_df[f"{method}_weighted"] = combined_df[method] * weight

    # Compute final importance
    weighted_cols = [
        f"{m}_weighted"
        for m in weights.keys()
        if f"{m}_weighted" in combined_df.columns
    ]
    combined_df["final_importance"] = combined_df[weighted_cols].sum(axis=1)

    # Sort by final score
    combined_df = combined_df.sort_values(
        "final_importance", ascending=False
    ).reset_index(drop=True)

    return combined_df


def plot_final_feature_importances(
    combined_df: pd.DataFrame, top_n: int = 20, filename: str | None = None
) -> None:
    """
    Plot the top N features from the combined weighted importance DataFrame,
    with numeric importance values displayed next to each bar.
    Optionally save the plot to FEATURE_SELECTION_CHARTS_DIR.
    """
    if not {"feature", "final_importance"}.issubset(combined_df.columns):
        raise ValueError(
            "combined_df must contain 'feature' and 'final_importance' columns."
        )

    # Select and sort top N
    df = (
        combined_df[["feature", "final_importance"]]
        .sort_values(by="final_importance", ascending=False)
        .head(top_n)
    )

    plt.figure(figsize=(14, 8))
    bars = plt.barh(df["feature"], df["final_importance"])

    plt.title("Final Weighted Feature Importances (Combined Methods)")
    plt.xlabel("Weighted Importance Score")
    plt.ylabel("Features")
    plt.gca().invert_yaxis()

    # Annotate bars with values
    for bar in bars:
        width = bar.get_width()
        plt.text(
            width + 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{width:.3f}",
            va="center",
            ha="left",
            fontsize=10,
            color="black",
        )

    plt.tight_layout()

    # Save plot if filename provided
    if filename:
        # Use cross-platform absolute path
        charts_dir = os.path.abspath(FEATURE_SELECTION_CHARTS_DIR)
        os.makedirs(charts_dir, exist_ok=True)

        save_path = os.path.join(charts_dir, filename)
        plt.savefig(save_path, dpi=300, bbox_inches="tight")

        print(f"✅ Plot saved to: {save_path}")

    plt.show()


class FeatureSelector:
    def __init__(self, logger: Logger, feature_selector_type: FeatureSelectorType):
        self._logger = logger
        self.feature_selector_type = feature_selector_type
        self._database_driver = PostgreSQLDriver(logger=logger)
        self.connect_to_database()
        self._model = None
        self.feature_importances_ = None

    def connect_to_database(self, database_name: str = "postgres") -> None:
        connection_model = PostgreSQLConnectionDto(
            logger=self._logger,
            host=os.getenv("POSTGRES_HOST"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("POSTGRES_PORT"),
            database=database_name,
        )
        return self._database_driver.connect(connection_model)

    def select(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str] = None,
        join_model: JoinModel = None,
        conditions: List[Condition] = None,
        order_by: List[str] = None,
        limit: int = None,
    ) -> pd.DataFrame:
        return self._database_driver.select(
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            join_model=join_model,
            conditions=conditions,
            order_by=order_by,
            limit=limit,
        )

    # =========================================================
    #  FEATURE SELECTION LOGIC
    # =========================================================

    def _fit_xgb_regressor(
        self, dataframe: pd.DataFrame, feature_columns: List[str], target_column: str
    ):
        """XGBoost regressor baseline."""
        self._xgb_regressor = xgb.XGBRegressor(
            base_score=0.5,
            booster="gbtree",
            n_estimators=1000,
            early_stopping_rounds=50,
            objective="reg:squarederror",
            max_depth=3,
            learning_rate=0.01,
        )
        self._xgb_regressor.fit(
            dataframe[feature_columns],
            dataframe[target_column],
            eval_set=[(dataframe[feature_columns], dataframe[target_column])],
            verbose=False,
        )
        self.feature_importances_ = pd.Series(
            self._xgb_regressor.feature_importances_, index=feature_columns
        )

    def _fit_lasso(self, X: pd.DataFrame, y: pd.Series):
        """LASSO regression with automatic feature shrinkage."""
        model = LassoCV(cv=5, random_state=42)
        X_scaled = StandardScaler().fit_transform(X)
        model.fit(X_scaled, y)
        self._model = model
        self.feature_importances_ = pd.Series(np.abs(model.coef_), index=X.columns)

    def _fit_elastic_net(self, X: pd.DataFrame, y: pd.Series):
        """ElasticNet regression for correlated features."""
        model = ElasticNetCV(cv=5, random_state=42, l1_ratio=0.5)
        X_scaled = StandardScaler().fit_transform(X)
        model.fit(X_scaled, y)
        self._model = model
        self.feature_importances_ = pd.Series(np.abs(model.coef_), index=X.columns)

    def _fit_xgb_shap(self, X: pd.DataFrame, y: pd.Series):
        """XGBoost model + SHAP for robust importance ranking."""
        self._fit_xgb_regressor(pd.concat([X, y], axis=1), X.columns.tolist(), y.name)
        explainer = shap.TreeExplainer(self._xgb_regressor)
        shap_values = explainer.shap_values(X)
        mean_abs_shap = np.abs(shap_values).mean(axis=0)
        self.feature_importances_ = pd.Series(mean_abs_shap, index=X.columns)

    # =========================================================
    #  MAIN FIT METHOD
    # =========================================================

    def fit(
        self,
        dataframe: pd.DataFrame,
        target_column: str,
        id_column: str = None,
        time_column: str = None,
    ) -> None:
        """Train feature selector according to selected method."""
        if not self.feature_selector_type:
            raise ValueError("Feature selector type not set or not recognized.")
        if target_column not in dataframe.columns:
            raise ValueError(f"Target column '{target_column}' not found in dataframe.")

        feature_columns = dataframe.columns.tolist()
        feature_columns.remove(target_column)
        if not feature_columns:
            raise ValueError("No feature columns available for training.")

        X = dataframe[feature_columns]
        y = dataframe[target_column]

        match self.feature_selector_type:
            case FeatureSelectorType.XGB_REGRESSOR:
                self._fit_xgb_regressor(dataframe, feature_columns, target_column)
            case FeatureSelectorType.LASSO:
                self._fit_lasso(X, y)
            case FeatureSelectorType.ELASTIC_NET:
                self._fit_elastic_net(X, y)
            case FeatureSelectorType.XGB_SHAP:
                self._fit_xgb_shap(X, y)
            case _:
                raise ValueError(
                    f"Unsupported feature selector type: {self.feature_selector_type}"
                )

    # =========================================================
    #  FEATURE IMPORTANCE HANDLING
    # =========================================================

    def get_feature_importances(self, normalize: bool = True) -> pd.DataFrame:
        """
        Return feature importances as a DataFrame with columns ['feature', 'importance'].
        Optionally applies min-max normalization.

        Parameters
        ----------
        normalize : bool, default=True
            If True, applies min-max normalization: (x - min) / (max - min).

        Returns
        -------
        pd.DataFrame
            A DataFrame sorted by importance (descending) with columns ['feature', 'importance'].
        """
        if (
            not hasattr(self, "feature_importances_")
            or self.feature_importances_ is None
        ):
            raise ValueError("Model has not been fitted yet.")

        feature_importances = self.feature_importances_.copy()

        if normalize:
            min_val = feature_importances.min()
            max_val = feature_importances.max()
            if max_val != min_val:
                feature_importances = (feature_importances - min_val) / (
                    max_val - min_val
                )
            else:
                self._logger.warning(
                    "All feature importances are equal; skipping normalization."
                )

        # Convert to DataFrame
        df = feature_importances.reset_index().rename(
            columns={"index": "feature", 0: "importance"}
        )

        # Handle the case when reset_index() creates wrong column names
        if "importance" not in df.columns:
            df.columns = ["feature", "importance"]

        # Ensure feature column is string type
        df["feature"] = df["feature"].astype(str)

        df = df.sort_values(by="importance", ascending=False).reset_index(drop=True)

        return df

    def plot_feature_importances(self, top_n: int = 20) -> None:
        """
        Plot the top N feature importances as a horizontal bar chart,
        with numeric importance values displayed next to each bar.
        """
        df = self.get_feature_importances().head(top_n)

        # Ensure we sort by importance for display
        df = df.sort_values(by="importance", ascending=True)

        plt.figure(figsize=(14, 8))
        bars = plt.barh(df["feature"], df["importance"])

        plt.title(
            f"Using '{self.feature_selector_type.value}' - Top Feature Importances"
        )
        plt.xlabel("Importance Score")
        plt.ylabel("Features")

        # Annotate bars with importance values
        for bar in bars:
            width = bar.get_width()
            plt.text(
                width + 0.01,  # position slightly to the right of bar
                bar.get_y() + bar.get_height() / 2,
                f"{width:.3f}",  # formatted importance value
                va="center",
                ha="left",
                fontsize=10,
                color="black",
            )

        plt.tight_layout()
        plt.show()
