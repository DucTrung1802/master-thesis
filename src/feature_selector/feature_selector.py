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

    def _fit_feature_clustering(self, X: pd.DataFrame, y: pd.Series):
        """Cluster correlated features and select best representative."""
        corr = X.corr().abs()
        distance = 1 - corr.fillna(0)
        clustering = AgglomerativeClustering(
            n_clusters=None,
            distance_threshold=0.3,
            affinity="precomputed",
            linkage="complete",
        )
        cluster_labels = clustering.fit_predict(distance)
        cluster_map = pd.Series(cluster_labels, index=X.columns)

        selected_features = []
        for label in np.unique(cluster_labels):
            features_in_cluster = X.columns[cluster_map == label]
            corr_with_target = X[features_in_cluster].apply(
                lambda c: np.abs(np.corrcoef(c, y)[0, 1])
            )
            best_feature = corr_with_target.idxmax()
            selected_features.append(best_feature)

        self.selected_features_ = selected_features
        self.feature_importances_ = pd.Series(1.0, index=selected_features)

    def _fit_tsfresh_lasso(self, dataframe: pd.DataFrame, target_column: str):
        """Extract statistical time-series features using tsfresh, then apply LASSO."""
        if extract_features is None:
            raise ImportError("Install `tsfresh` to use this feature selector.")
        X = extract_features(
            dataframe,
            column_id="id",
            column_sort="time",
            disable_progressbar=True,
        ).dropna(axis=1, how="all")

        y = dataframe.groupby("id")[target_column].last().values
        self._fit_lasso(X, pd.Series(y))

    # =========================================================
    #  MAIN FIT METHOD
    # =========================================================

    def fit(self, dataframe: pd.DataFrame, target_column: str) -> None:
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
            case FeatureSelectorType.FEATURE_CLUSTERING:
                self._fit_feature_clustering(X, y)
            case FeatureSelectorType.TSFRESH_LASSO:
                self._fit_tsfresh_lasso(dataframe, target_column)
            case _:
                raise ValueError(
                    f"Unsupported feature selector type: {self.feature_selector_type}"
                )

    # =========================================================
    #  FEATURE IMPORTANCE HANDLING
    # =========================================================

    def get_feature_importances(self) -> pd.Series:
        if (
            not hasattr(self, "feature_importances_")
            or self.feature_importances_ is None
        ):
            raise ValueError("Model has not been fitted yet.")
        return self.feature_importances_.sort_values(ascending=False)

    def plot_feature_importances(self, top_n: int = 20) -> None:
        feature_importances = self.get_feature_importances().head(top_n)
        plt.figure(figsize=(14, 8))
        feature_importances.sort_values(ascending=True).plot(kind="barh")
        plt.title(
            f"Using '{self.feature_selector_type.value}' - Top Feature Importances"
        )
        plt.xlabel("Importance Score")
        plt.ylabel("Features")
        plt.tight_layout()
        plt.show()
