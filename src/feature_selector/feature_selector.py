from typing import List
from dotenv import load_dotenv
import os
import pandas as pd
import matplotlib.pyplot as plt


import xgboost as xgb

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


load_dotenv()


class FeatureSelector:
    def __init__(self, logger: Logger, feature_selector_type: FeatureSelectorType):
        self._logger = logger
        self.feature_selector_type = feature_selector_type
        self._database_driver = PostgreSQLDriver(logger=logger)
        self.connect_to_database()

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

    def _fit_xgb_regressor(
        self, dataframe: pd.DataFrame, feature_columns: List[str], target_column: str
    ) -> None:
        self._xgb_regressor = xgb.XGBRegressor(
            base_score=0.5,
            booster="gbtree",
            n_estimators=1000,
            early_stopping_rounds=50,
            objective="reg:linear",
            max_depth=3,
            learning_rate=0.01,
        )
        self._xgb_regressor.fit(
            dataframe[feature_columns],
            dataframe[target_column],
            eval_set=[(dataframe[feature_columns], dataframe[target_column])],
            verbose=100,
        )

    def fit(self, dataframe: pd.DataFrame, target_column: str) -> None:
        if not self.feature_selector_type:
            raise ValueError("Feature selector type not set or not recognized.")

        if target_column not in dataframe.columns:
            raise ValueError(f"Target column '{target_column}' not found in dataframe.")

        feature_columns = dataframe.columns.tolist()
        feature_columns.remove(target_column)

        if not feature_columns:
            raise ValueError("No feature columns available for training.")

        match self.feature_selector_type:
            case FeatureSelectorType.XGB_REGRESSOR:
                self._fit_xgb_regressor(dataframe, feature_columns, target_column)

    def get_feature_importances(self) -> pd.Series:
        if not self.feature_selector_type:
            raise ValueError("Feature selector type not set or not recognized.")

        match self.feature_selector_type:
            case FeatureSelectorType.XGB_REGRESSOR:
                if not hasattr(self, "_xgb_regressor"):
                    raise ValueError("Model has not been fitted yet.")
                return pd.Series(
                    self._xgb_regressor.feature_importances_,
                    index=self._xgb_regressor.get_booster().feature_names,
                ).sort_values(ascending=False)

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
