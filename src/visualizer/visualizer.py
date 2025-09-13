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
from utils.constants import *
from utils.utils import *

parent_dir = os.path.dirname(os.getcwd())

load_dotenv()

my_logger = Logger(file_name=os.path.join(parent_dir, "logs/visualizer"))


class Visualizer:
    def __init__(self, logger: Logger = my_logger):
        self._logger = logger
        self._database_driver = PostgreSQLDriver(logger=logger)
        self._chart_dir = CHARTS_DIR_BASE
        self.connect_to_database()

        os.makedirs(CHARTS_DIR_BASE, exist_ok=True)

    def set_chart_dir(self, chart_dir: str) -> None:
        os.makedirs(chart_dir, exist_ok=True)
        self._chart_dir = chart_dir

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
        return make_date_time_index_for_dataframe(df, start_date, end_date)

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

    def plot_lines_chart(
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
        dpi: int = 1000,
        style: str = "fivethirtyeight",
        marker: str = "o",
        indicate_number_of_indicators: bool = True,
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
            df = df.sort_values(by=x_column)

            fig, ax = plt.subplots(figsize=(14, 5))

            colors = plt.cm.Set1.colors

            # Plot each y-column
            for i, col in enumerate(y_columns):
                ax.plot(
                    df[x_column],
                    df[col],
                    color=colors[i % len(colors)],  # unique line color
                    marker=marker,
                    label=col,
                    markerfacecolor="#1f3b73",  # keep marker fill
                    markeredgecolor="#1f3b73",  # keep marker border
                )

            # Format y-axis title with unit
            if y_unit:
                y_axis_label = f"{y_axis_title} ({y_unit})"
            else:
                y_axis_label = y_axis_title

            if indicate_number_of_indicators:
                title += f" ({len(remove_time_column_name(df.columns))} indicators)"

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

            fig.savefig(os.path.join(self._chart_dir, figure_name), dpi=dpi)

        return fig

    def plot_lines_bars_chart(
        self,
        df: pd.DataFrame,
        x_column: str,
        y_line_columns: List[str],
        y_bar_columns: List[str],
        title: str = "Lines & Bars Chart",
        x_axis_title: str = "Timeline",
        y_line_axis_title: str = "Line Value",
        y_bar_axis_title: str = "Bar Value",
        y_unit: Optional[str] = None,
        legend_title: str = "Legend",
        legend_position: str = "best",
        font_size: int = 16,
        figure_name: str = None,
        prefix_figure_name: str = None,
        dpi: int = 1000,
        style: str = "fivethirtyeight",
        marker: str = "o",
        line_alpha: float = 1.0,
        bar_alpha: float = 1.0,
        show_line_grid: bool = True,
        show_bar_grid: bool = False,
        bar_height_ratio: float = 0.33,
    ) -> plt.Figure:
        """
        Plots a combined line (top portion) and bar (bottom portion) chart using Matplotlib.

        Parameters:
            df (pd.DataFrame): Input dataframe.
            x_column (str): Column name for the x-axis.
            y_line_columns (List[str]): List of column names to plot as lines (upper area).
            y_bar_columns (List[str]): List of column names to plot as bars (lower area).
            title (str): Chart title.
            x_axis_title (str): Label for x-axis.
            y_line_axis_title (str): Label for line axis.
            y_bar_axis_title (str): Label for bar axis.
            y_unit (str, optional): Unit suffix for y-axis values (e.g., "%", "$“).
            legend_title (str): Title of the legend.
            legend_position (str): Position of the legend (e.g., "best", "upper left").
            font_size (int): Base font size.
            figure_name (str, optional): Name of the saved figure.
            dpi (int): Resolution of saved figure.
            style (str): Matplotlib style (e.g., "default", "fivethirtyeight", "seaborn").
            marker (str): Marker style for line plots.
            line_alpha (float): Transparency of line plots.
            bar_alpha (float): Transparency of bar plots.
            show_line_grid (bool): Toggle gridlines for line axis.
            show_bar_grid (bool): Toggle gridlines for bar axis.
            bar_height_ratio (float): Fraction of total chart height reserved for bar chart (default=0.33).

        Returns:
            plt.Figure: Matplotlib Figure object.
        """
        with plt.style.context(style):
            df[x_column] = pd.to_datetime(df[x_column])
            df = df.sort_values(by=x_column)

            fig = plt.figure(figsize=(14, 10))
            gs = fig.add_gridspec(1, 1)

            # Define grid height ratios
            gs = fig.add_gridspec(
                2, 1, height_ratios=[1 - bar_height_ratio, bar_height_ratio]
            )

            ax_line = fig.add_subplot(gs[0])  # top for lines
            ax_bar = fig.add_subplot(gs[1], sharex=ax_line)  # bottom for bars

            colors = plt.cm.Set1.colors

            # --- Plot bar columns (bottom axis) ---
            if y_bar_columns:
                for i, col in enumerate(y_bar_columns):
                    ax_bar.bar(
                        df[x_column]
                        + pd.DateOffset(
                            days=i * 10
                        ),  # slight offset if multiple bar series
                        df[col],
                        width=20,
                        color=colors[i % len(colors)],
                        alpha=bar_alpha,
                        label=col,
                    )

            # --- Plot line columns (top axis) ---
            offset = len(y_bar_columns)
            for i, col in enumerate(y_line_columns):
                ax_line.plot(
                    df[x_column],
                    df[col],
                    color=colors[(i + offset) % len(colors)],
                    marker=marker,
                    label=col,
                    alpha=line_alpha,
                    markerfacecolor="#1f3b73",
                    markeredgecolor="#1f3b73",
                )

            # Titles and labels
            y_axis_label_left = f"{y_line_axis_title}{f' ({y_unit})' if y_unit else ''}"
            y_axis_label_right = f"{y_bar_axis_title}{f' ({y_unit})' if y_unit else ''}"

            ax_line.set_title(title, fontsize=font_size + 2)
            ax_line.set_ylabel(y_axis_label_left, fontsize=font_size)
            ax_bar.set_ylabel(y_axis_label_right, fontsize=font_size)
            ax_bar.set_xlabel(x_axis_title, fontsize=font_size)

            # X axis range
            x_min = df[x_column].min() - pd.DateOffset(years=1)
            x_max = df[x_column].max() + pd.DateOffset(years=1)
            ax_line.set_xlim([x_min, x_max])

            # Combine legends from both axes
            handles_line, labels_line = ax_line.get_legend_handles_labels()
            handles_bar, labels_bar = ax_bar.get_legend_handles_labels()
            ax_line.legend(
                handles_line + handles_bar,
                labels_line + labels_bar,
                title=legend_title,
                loc=legend_position,
                fontsize=font_size - 2,
                title_fontsize=font_size - 2,
            )

            # Format x-axis (years only)
            ax_bar.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter("%Y"))
            ax_bar.xaxis.set_major_locator(plt.matplotlib.dates.YearLocator(base=1))

            ax_bar.tick_params(axis="x", rotation=45, labelsize=font_size - 2)
            ax_line.tick_params(
                axis="x", rotation=45, labelsize=font_size - 2
            )  # <- added
            ax_line.tick_params(axis="y", labelsize=font_size - 2)
            ax_bar.tick_params(axis="y", labelsize=font_size - 2)

            # Grid toggles
            if show_line_grid:
                ax_line.grid(
                    True, which="major", axis="both", linestyle="--", alpha=0.7
                )
            if show_bar_grid:
                ax_bar.grid(True, which="major", axis="both", linestyle=":", alpha=0.7)

            plt.tight_layout()

            # Default file name if not specified
            if figure_name is None:
                figure_name = f"{f"{prefix_figure_name}_" if prefix_figure_name else ''}{"_".join(title.lower().split())}.png"
            else:
                figure_name = f"{figure_name}.png"

            fig.savefig(os.path.join(self._chart_dir, figure_name), dpi=dpi)

        return fig
