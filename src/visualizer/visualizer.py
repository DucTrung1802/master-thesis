from typing import List, Optional
from dotenv import load_dotenv
import os
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime

from logger.logger import Logger
from models.tabular_database_driver_models.tabular_database_driver_models import (
    Condition,
    JoinModel,
)
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from models.tabular_database_driver_models.postgre_sql_connection_model import (
    PostgreSQLConnectionModel,
)
from utils.enums import GenerateDateTimeType
from utils.constants import SCRAPER_START_DATE, SCRAPER_END_DATE

parent_dir = os.path.dirname(os.getcwd())

load_dotenv()

my_logger = Logger(file_name=os.path.join(parent_dir, "logs/visualizer"))


class Visualizer:
    def __init__(self, logger: Logger = my_logger):
        self._logger = logger
        self._database_driver = PostgreSQLDriver(logger=logger)
        self.connect_to_database()

    def connect_to_database(self, database_name: str = "postgres") -> None:
        connection_model = PostgreSQLConnectionModel(
            logger=self._logger,
            host=os.getenv("POSTGRES_HOST"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("POSTGRES_PORT"),
            database=database_name,
        )

        return self._database_driver.connect(connection_model)

    def generate_date_time_series(
        self,
        generate_date_time_type: GenerateDateTimeType,
        start_date: datetime,
        end_date: datetime,
    ) -> pd.Series:
        match generate_date_time_type:
            case GenerateDateTimeType.YEAR:
                dates = pd.date_range(start=start_date, end=end_date, freq="YE")
            case GenerateDateTimeType.QUARTER:
                dates = pd.date_range(start=start_date, end=end_date, freq="QE")
            case GenerateDateTimeType.MONTH:
                dates = pd.date_range(start=start_date, end=end_date, freq="ME")
            case GenerateDateTimeType.DAY:
                dates = pd.date_range(start=start_date, end=end_date, freq="D")
            case _:
                raise ValueError(f"Unsupported type: {generate_date_time_type}")

        return pd.Series(dates)

    def make_date_time_index_for_dataframe(
        self,
        df: pd.DataFrame,
        start_date: datetime = SCRAPER_START_DATE,
        end_date: datetime = SCRAPER_END_DATE,
    ) -> pd.DataFrame:
        generate_date_time_type = None

        # Detect type based on dataframe columns
        if {"year", "quarter"}.issubset(df.columns):
            generate_date_time_type = GenerateDateTimeType.QUARTER
        elif {"year", "month"}.issubset(df.columns):
            generate_date_time_type = GenerateDateTimeType.MONTH
        elif (
            "year" in df.columns
            and len(df.columns.intersection({"quarter", "month", "date"})) == 0
        ):
            generate_date_time_type = GenerateDateTimeType.YEAR
        elif "date" in df.columns:
            generate_date_time_type = GenerateDateTimeType.DAY
        else:
            raise ValueError("DataFrame does not contain recognizable time columns")

        # Build "date" column from existing fields
        match generate_date_time_type:
            case GenerateDateTimeType.YEAR:
                df["date"] = (
                    pd.to_datetime(df["year"], format="%Y") + pd.offsets.YearEnd(0)
                ).normalize()
                df = df.drop(columns=["year"])
            case GenerateDateTimeType.QUARTER:
                df["date"] = (
                    pd.PeriodIndex.from_fields(
                        year=df["year"], quarter=df["quarter"], freq="Q"
                    )
                    .to_timestamp(how="end")
                    .normalize()
                )
                df = df.drop(columns=["year", "quarter"])
            case GenerateDateTimeType.MONTH:
                df["date"] = (
                    pd.to_datetime(
                        df["year"].astype(str)
                        + "-"
                        + df["month"].astype(str).str.zfill(2),
                        format="%Y-%m",
                    )
                    + pd.offsets.MonthEnd(0)
                ).normalize()
                df = df.drop(columns=["year", "month"])
            case GenerateDateTimeType.DAY:
                df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d").normalize()
            case _:
                raise ValueError("Unsupported generate_date_time_type")

        # --- NEW PART: generate full date range ---
        match generate_date_time_type:
            case GenerateDateTimeType.YEAR:
                full_range = pd.date_range(
                    start=start_date, end=end_date, freq="YE"
                ).normalize()
            case GenerateDateTimeType.QUARTER:
                full_range = pd.date_range(
                    start=start_date, end=end_date, freq="QE"
                ).normalize()
            case GenerateDateTimeType.MONTH:
                full_range = pd.date_range(
                    start=start_date, end=end_date, freq="ME"
                ).normalize()
            case GenerateDateTimeType.DAY:
                full_range = pd.date_range(
                    start=start_date, end=end_date, freq="D"
                ).normalize()
            case _:
                raise ValueError("Unsupported generate_date_time_type")

        full_df = pd.DataFrame({"date": full_range})

        # Merge to ensure full coverage
        df = pd.merge(full_df, df, on="date", how="left")

        # Move "date" column to the first position (already is, but keep consistent)
        cols = ["date"] + [col for col in df.columns if col != "date"]
        df = df[cols]

        return df

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
            schema_name=schema_name, table_name=table_name
        )

    def plot_line_chart(
        self,
        df: pd.DataFrame,
        x_column: str,
        y_columns: List[str],
        title: str = "Line Chart",
        x_axis_title: str = "Timeline",
        y_axis_title: str = "Value",
        y_unit: Optional[str] = None,
        legend_title: str = "Series",
        legend_orientation: str = "h",  # "h" = horizontal, "v" = vertical
        font_size: int = 16,
    ) -> go.Figure:
        """
        Plots a line chart using Plotly Graph Objects.

        Parameters:
            df (pd.DataFrame): Input dataframe.
            x_column (str): Column name for the x-axis.
            y_columns (List[str]): List of column names to plot on the y-axis.
            title (str): Chart title.
            x_axis_title (str): Label for x-axis.
            y_axis_title (str): Label for y-axis.
            y_unit (str, optional): Unit suffix for y-axis values (e.g., "%", "$").
            legend_title (str): Title of the legend.
            legend_orientation (str): "h" (horizontal) or "v" (vertical).

        Returns:
            go.Figure: Plotly Figure object.
        """
        fig = go.Figure()

        # Add traces for each y-column
        for col in y_columns:
            fig.add_trace(
                go.Scatter(
                    x=df[x_column],
                    y=df[col],
                    mode="lines+markers",
                    name=col,  # Legend label
                )
            )

        # Format y-axis title with unit
        if y_unit:
            y_axis_label = f"{y_axis_title} ({y_unit})"
        else:
            y_axis_label = y_axis_title

        # Update layout
        fig.update_layout(
            title=title,
            xaxis_title=x_axis_title,
            yaxis_title=y_axis_label,
            template="plotly_white",
            hovermode="x unified",
            legend=dict(
                title=legend_title,
                orientation=legend_orientation,
                yanchor="bottom",  # adjust position for horizontal legends
                y=1.02 if legend_orientation == "h" else 1,
                xanchor="right",
                x=1,
            ),
            font=dict(size=font_size),
        )

        fig.update_xaxes(automargin=True)
        fig.update_yaxes(automargin=True)

        return fig
