from typing import List
from dotenv import load_dotenv
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import xgboost as xgb
import shap
import seaborn as sns

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
    stock_code: str,
    combined_df: pd.DataFrame,
    top_n: int = 20,
    filename: str | None = None,
) -> None:
    """
    Plot the top N features from the combined weighted importance DataFrame,
    with numeric importance values displayed next to each bar.
    Optionally save the plot to FEATURE_SELECTION_CHARTS_DIR.
    """
    if stock_code is None or stock_code.strip() == "":
        raise ValueError("Stock code must be provided and cannot be empty.")

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

    plt.title(
        f"Stock code: '{stock_code}' - Final Weighted Feature Importances ({", ".join([member.name for member in FeatureSelectorType])})"
    )
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
        charts_dir = os.path.abspath(FEATURE_SELECTION_LOG_FILE_BASE)
        os.makedirs(charts_dir, exist_ok=True)

        save_path = os.path.join(charts_dir, filename)
        plt.savefig(save_path, dpi=300, bbox_inches="tight")

        print(f"✅ Plot saved to: {save_path}")

    plt.show()


def get_important_features_df(
    combined_df: pd.DataFrame, top_n: int = 20, filename: str | None = None
) -> pd.DataFrame:
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

    return df


def plot_feature_correlation_heatmap(
    stock_code: str,
    df: pd.DataFrame,
    feature_columns: List[str],
    target_column: str,
    top_corr_threshold: float | None = None,
    filename: str | None = None,
) -> None:
    """
    Plot a correlation heatmap of the top N most correlated numeric features,
    excluding the target column. Optionally print highly correlated feature pairs.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe containing features and target.
    feature_columns : list[str]
        List of feature columns to include in correlation analysis.
    target_column : str
        The target column name to exclude.
    top_corr_threshold : float, optional
        If provided (e.g., 0.9), prints feature pairs with |correlation| above this threshold.
    top_n : int, default=50
        Maximum number of features (most correlated) to display in the heatmap.
    filename : str, optional
        If provided, saves the heatmap to the specified file path.
    """
    # Validate columns
    if stock_code is None or stock_code.strip() == "":
        raise ValueError("Stock code must be provided and cannot be empty.")

    missing = [col for col in feature_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in dataframe: {missing}")

    if target_column not in df.columns:
        raise ValueError(f"Target column '{target_column}' not found in dataframe.")

    # Extract numeric features (ignore target)
    df_num = df[feature_columns].select_dtypes(include=[np.number])
    if df_num.empty:
        raise ValueError("No numeric features found in feature_columns.")

    # Compute correlation matrix
    corr = df_num.corr()

    top_n = len(feature_columns)

    # Select top N features by average absolute correlation
    avg_corr = corr.abs().mean().sort_values(ascending=False)
    top_features = avg_corr.head(top_n).index
    corr_top = corr.loc[top_features, top_features]

    # ===============================
    # Plot heatmap
    # ===============================
    plt.figure(figsize=(12, 10))
    sns.heatmap(
        corr_top,
        annot=False,
        cmap="coolwarm",
        center=0,
        square=True,
        linewidths=0.5,
        cbar_kws={"shrink": 0.8},
    )
    plt.title(
        f"Stock code: '{stock_code}' - Top {top_n} Feature Correlation Heatmap (excluding target '{target_column}')",
        fontsize=14,
    )
    plt.tight_layout()

    # Save if requested
    if filename:
        os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
        plt.savefig(filename, dpi=300, bbox_inches="tight")
        print(f"✅ Correlation heatmap saved to: {os.path.abspath(filename)}")

    plt.show()

    # ===============================
    # Show highly correlated pairs
    # ===============================
    if top_corr_threshold is not None:
        high_corr = corr_top.where(np.triu(np.ones(corr_top.shape), k=1).astype(bool))
        high_corr_pairs = (
            high_corr.stack()
            .reset_index()
            .rename(columns={"level_0": "feature_1", "level_1": "feature_2", 0: "corr"})
            .query(f"abs(corr) > {top_corr_threshold}")
            .sort_values(by="corr", ascending=False)
        )


def get_feature_correlation_df(
    df: pd.DataFrame,
    feature_columns: List[str],
    target_column: str,
    top_corr_threshold: float | None = None,
    filename: str | None = None,
) -> pd.DataFrame:
    # Validate columns
    missing = [col for col in feature_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in dataframe: {missing}")

    if target_column not in df.columns:
        raise ValueError(f"Target column '{target_column}' not found in dataframe.")

    # Extract numeric features (ignore target)
    df_num = df[feature_columns].select_dtypes(include=[np.number])
    if df_num.empty:
        raise ValueError("No numeric features found in feature_columns.")

    # Compute correlation matrix
    corr = df_num.corr()

    top_n = len(feature_columns)

    # Select top N features by average absolute correlation
    avg_corr = corr.abs().mean().sort_values(ascending=False)
    top_features = avg_corr.head(top_n).index
    corr_top = corr.loc[top_features, top_features]

    high_corr = corr_top.where(np.triu(np.ones(corr_top.shape), k=1).astype(bool))
    high_corr_pairs = (
        high_corr.stack()
        .reset_index()
        .rename(columns={"level_0": "feature_1", "level_1": "feature_2", 0: "corr"})
        .query(f"abs(corr) > {top_corr_threshold}")
        .sort_values(by="corr", ascending=False)
    )

    return high_corr_pairs.reset_index(drop=True)


def drop_low_importance_correlated_features(
    important_features_df: pd.DataFrame,
    correlation_df: pd.DataFrame,
    corr_threshold: float = 0.9,
) -> tuple[list[str], list[str], pd.DataFrame]:
    """
    Remove features that are highly correlated but have lower importance.

    Parameters
    ----------
    important_features_df : pd.DataFrame
        DataFrame with columns ['feature', 'final_importance'].
        Should contain the importance scores for features.
    correlation_df : pd.DataFrame
        DataFrame with columns ['feature_1', 'feature_2', 'corr'].
        Each row represents a pair of correlated features.
    corr_threshold : float, default=0.9
        Correlation threshold above which features are considered highly correlated.

    Returns
    -------
    kept_features : list[str]
        List of kept (more important) features.
    dropped_features : list[str]
        List of dropped (less important) features.
    pruned_importance_df : pd.DataFrame
        Updated importance DataFrame after dropping redundant features.
    """
    # Validate input
    if not {"feature", "final_importance"}.issubset(important_features_df.columns):
        raise ValueError(
            "important_features_df must contain ['feature', 'final_importance']"
        )
    if not {"feature_1", "feature_2", "corr"}.issubset(correlation_df.columns):
        raise ValueError(
            "correlation_df must contain ['feature_1', 'feature_2', 'corr']"
        )

    # Make sure correlation_df uses absolute correlation values
    correlation_df = correlation_df.copy()
    correlation_df["abs_corr"] = correlation_df["corr"].abs()

    # Filter pairs above threshold
    high_corr_pairs = correlation_df[correlation_df["abs_corr"] > corr_threshold].copy()

    dropped_features = set()
    kept_features = set(important_features_df["feature"].tolist())

    # Map importance for fast lookup
    importance_map = dict(
        zip(important_features_df["feature"], important_features_df["final_importance"])
    )

    for _, row in high_corr_pairs.iterrows():
        f1, f2 = row["feature_1"], row["feature_2"]

        # Skip if already dropped
        if f1 in dropped_features or f2 in dropped_features:
            continue

        imp1 = importance_map.get(f1, 0)
        imp2 = importance_map.get(f2, 0)

        # Drop the one with lower importance
        if imp1 >= imp2:
            dropped_features.add(f2)
        else:
            dropped_features.add(f1)

    kept_features = [
        f for f in important_features_df["feature"] if f not in dropped_features
    ]

    pruned_df = important_features_df[
        important_features_df["feature"].isin(kept_features)
    ].reset_index(drop=True)

    print(
        f"🔍 Dropped {len(dropped_features)} highly correlated features (|r| > {corr_threshold}):"
    )
    print(sorted(list(dropped_features)))
    print(
        f"✅ Kept {len(kept_features)} features after pruning correlation redundancy."
    )

    return kept_features, sorted(list(dropped_features)), pruned_df


class BaseFeatureSelector:
    def __init__(self):
        pass

    def get_feature_importances(self, normalize: bool = True) -> pd.DataFrame:
        feature_importances = self._feature_importance_series.copy()

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
            f"Stock code: '{self._stock_code}' - Using '{self._feature_selector_type.value}' - Top Feature Importances"
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


class XGB_FeatureSelector(BaseFeatureSelector):
    def __init__(self):
        self._name = "XGB"
        self._selector = xgb.XGBRegressor(
            base_score=0.5,
            booster="gbtree",
            n_estimators=1000,
            early_stopping_rounds=50,
            objective="reg:squarederror",
            max_depth=3,
            learning_rate=0.01,
        )

    def fit(
        self, dataframe: pd.DataFrame, feature_columns: List[str], target_column: str
    ) -> None:
        self._selector.fit(
            dataframe[feature_columns],
            dataframe[target_column],
            eval_set=[(dataframe[feature_columns], dataframe[target_column])],
            verbose=False,
        )
        self._feature_importance_series = pd.Series(
            self._selector.feature_importances_, index=feature_columns
        )


class LASSO_FeatureSelector(BaseFeatureSelector):
    def __init__(self):
        self._name = "LASSO"
        self._selector = LassoCV(cv=5, random_state=42)

    def fit(
        self, dataframe: pd.DataFrame, feature_columns: List[str], target_column: str
    ) -> None:
        X = dataframe[feature_columns]
        y = dataframe[target_column]

        X_scaled = StandardScaler().fit_transform(X)
        self._selector.fit(X_scaled, y)

        self._feature_importance_series = pd.Series(
            np.abs(self._selector.coef_), index=X.columns
        )


class ELASTC_NET_FeatureSelector(BaseFeatureSelector):
    def __init__(self):
        self._name = "ELASTC_NET"
        self._selector = ElasticNetCV(cv=5, random_state=42, l1_ratio=0.5)

    def fit(
        self, dataframe: pd.DataFrame, feature_columns: List[str], target_column: str
    ) -> None:
        X = dataframe[feature_columns]
        y = dataframe[target_column]

        X_scaled = StandardScaler().fit_transform(X)
        self._selector.fit(X_scaled, y)

        self._feature_importance_series = pd.Series(
            np.abs(self._selector.coef_), index=X.columns
        )


class XGB_SHAP_FeatureSelector(BaseFeatureSelector):
    def __init__(self):
        self._name = "XGB_SHAP"
        self._selector = None

    def fit(
        self, dataframe: pd.DataFrame, feature_columns: List[str], target_column: str
    ) -> None:
        X = dataframe[feature_columns]
        y = dataframe[target_column]

        xgb_regressor = XGB_FeatureSelector()
        xgb_regressor.fit(pd.concat([X, y], axis=1), X.columns.tolist(), y.name)
        explainer = shap.TreeExplainer(xgb_regressor)
        shap_values = explainer.shap_values(X)
        mean_abs_shap = np.abs(shap_values).mean(axis=0)

        self._feature_importance_series = pd.Series(mean_abs_shap, index=X.columns)


class FeatureSelector:
    def __init__(
        self,
        logger: Logger,
        stock_code: str,
        dataframe: pd.DataFrame,
        feature_columns: List[str],
        target_column: str,
        feature_selector_weight: FeatureSelectorWeight,
    ):
        self._logger = logger
        self._stock_code = stock_code
        self._feature_selector_weight = feature_selector_weight

        if stock_code is None or stock_code.strip() == "":
            raise ValueError("Stock code must be provided and cannot be empty.")

        # XGB
        xgb_feature_selector = XGB_FeatureSelector()
        xgb_feature_selector.fit(dataframe, feature_columns, target_column)
        xgb_df = xgb_feature_selector.get_feature_importances()

        # LASSO
        lasso_feature_selector = LASSO_FeatureSelector()
        lasso_feature_selector.fit(dataframe, feature_columns, target_column)
        lasso_df = lasso_feature_selector.get_feature_importances()

        # ELASTC_NET
        elastic_net_selector = ELASTC_NET_FeatureSelector()
        elastic_net_selector.fit(dataframe, feature_columns, target_column)
        elastic_net_df = elastic_net_selector.get_feature_importances()

        # XGB SHAP
        xgb_shap_selector = XGB_SHAP_FeatureSelector()
        xgb_shap_selector.fit(dataframe, feature_columns, target_column)
        xgb_shap_df = xgb_shap_selector.get_feature_importances()
