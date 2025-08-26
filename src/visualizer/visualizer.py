from typing import List, Optional
from dotenv import load_dotenv
import os
import pandas as pd
import matplotlib.pyplot as plt
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
from utils.constants import (
    CHARTS_DIR,
    SCRAPER_START_DATE,
    SCRAPER_END_DATE,
)

parent_dir = os.path.dirname(os.getcwd())

load_dotenv()

my_logger = Logger(file_name=os.path.join(parent_dir, "logs/visualizer"))


class Visualizer:
    def __init__(self, logger: Logger = my_logger):
        self._logger = logger
        self._database_driver = PostgreSQLDriver(logger=logger)
        self.connect_to_database()

        os.makedirs(CHARTS_DIR, exist_ok=True)

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
                ).dt.normalize()
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
                ).dt.normalize()
                df = df.drop(columns=["year", "month"])

            case GenerateDateTimeType.DAY:
                df["date"] = pd.to_datetime(
                    df["date"], format="%Y-%m-%d"
                ).dt.normalize()

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
        legend_position: str = "best",
        font_size: int = 16,
        figure_name: str = None,
        prefix_figure_name: str = None,
        dpi: int = 300,
        style: str = "fivethirtyeight",
    ) -> plt.Figure:
        """
        Plots a line chart using Matplotlib.

        Parameters:
            df (pd.DataFrame): Input dataframe.
            x_column (str): Column name for the x-axis.
            y_columns (List[str]): List of column names to plot on the y-axis.
            title (str): Chart title.
            x_axis_title (str): Label for x-axis.
            y_axis_title (str): Label for y-axis.
            y_unit (str, optional): Unit suffix for y-axis values (e.g., "%", "$").
            legend_title (str): Title of the legend.
            legend_position (str): Position of the legend (e.g., "best", "upper left", "upper right").
            font_size (int): Base font size.
            figure_name (str, optional): Name of the saved figure. Defaults to first y_column.
            dpi (int): Resolution of saved figure.
            style (str): Matplotlib style (e.g., "default", "fivethirtyeight", "seaborn", "ggplot")

        Returns:
            plt.Figure: Matplotlib Figure object.
        """
        with plt.style.context(style):
            df[x_column] = pd.to_datetime(df[x_column])

            fig, ax = plt.subplots(figsize=(14, 5))

            # Plot each y-column
            for col in y_columns:
                ax.plot(
                    df[x_column],
                    df[col],
                    marker="o",
                    label=col,
                    markerfacecolor="#1f3b73",  # point fill color
                    markeredgecolor="#1f3b73",  # point border color
                )

            # Format y-axis title with unit
            if y_unit:
                y_axis_label = f"{y_axis_title} ({y_unit})"
            else:
                y_axis_label = y_axis_title

            ax.set_title(title, fontsize=font_size + 2)
            ax.set_xlabel(x_axis_title, fontsize=font_size)
            ax.set_ylabel(y_axis_label, fontsize=font_size)

            x_min = df[x_column].min() - pd.DateOffset(years=1)
            x_max = df[x_column].max() + pd.DateOffset(years=1)
            ax.set_xlim([x_min, x_max])

            # Legend with flexible position
            ax.legend(
                title=legend_title,
                loc=legend_position,
                fontsize=font_size - 2,
                title_fontsize=font_size - 2,
            )

            # Format x-axis (years only)
            ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter("%Y"))
            ax.xaxis.set_major_locator(plt.matplotlib.dates.YearLocator(base=1))

            ax.tick_params(axis="x", rotation=45, labelsize=font_size - 2)
            ax.tick_params(axis="y", labelsize=font_size - 2)

            plt.tight_layout()

            # Default file name if not specified
            if figure_name is None:
                figure_name = f"{f"{prefix_figure_name}_" if prefix_figure_name else ''}{"_".join(title.lower().split())}.png"
            else:
                figure_name = f"{figure_name}.png"

            fig.savefig(os.path.join(CHARTS_DIR, figure_name), dpi=dpi)

        return fig
