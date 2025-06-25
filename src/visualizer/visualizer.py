from dotenv import load_dotenv
import os
import plotly.graph_objects as go
import pandas as pd

from logger.logger import Logger
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from models.tabular_database_driver_models.postgre_sql_connection_model import (
    PostgreSQLConnectionModel,
)

parent_dir = os.path.dirname(os.getcwd())

load_dotenv()

my_logger = Logger(file_name=os.path.join(parent_dir, "logs/visualizer"))


class Visualizer:
    def __init__(self, logger: Logger = my_logger):
        self._logger = logger
        self._database_driver = PostgreSQLDriver(logger=logger)
        self._connect_to_database()

    def _connect_to_database(self) -> None:
        connection_model = PostgreSQLConnectionModel(
            logger=self._logger,
            host=os.getenv("POSTGRES_HOST"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DATABASE"),
        )

        self._database_driver.connect(connection_model)

    def test_data(self) -> pd.DataFrame:
        return self._database_driver.select(
            schema_name="macroeconomics",
            table_name="gdp",
            order_by=["year", "quarter"],
        )

    def test_visualize(self) -> None:
        df = self.test_data()
        df["gdp_true_growth_acc"] = df["gdp_true_growth_acc"].fillna(0)
        df["period"] = df["year"].astype(str) + "-Q" + df["quarter"].astype(str)
        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=df["period"],
                y=df["gdp_true_growth_acc"],
                mode="lines+markers",
                name="GDP Growth",
            )
        )

        fig.update_layout(
            title="GDP True Growth Acc by Quarter",
            xaxis_title="Year-Quarter",
            yaxis_title="GDP Growth (%)",
            xaxis=dict(
                tickangle=30,  # less tilt
                tickmode="auto",
                nticks=30,  # reduce number of ticks shown
                tickfont=dict(size=10),
            ),
            template="plotly_white",
            height=500,
            width=1000,
        )

        fig.show()
