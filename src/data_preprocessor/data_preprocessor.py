from dotenv import load_dotenv
import os
import pandas as pd
import re
from glob import glob
import numpy as np
from datetime import datetime, timedelta

from logger.logger import Logger
from dtos.tabular_database_driver_dtos.postgre_sql_connection_dto import (
    PostgreSQLConnectionDto,
)
from dtos.tabular_database_driver_dtos.tabular_database_driver_dtos import *
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from utils.constants import SCRAPER_BRONZE_DATA_DIR
from utils.enums import *
from utils.utils import *
from utils.switch_handler import SwitchHandler

load_dotenv()


class DataPreprocessor:
    def __init__(self, logger: Logger, switch_handler: SwitchHandler):
        self._logger = logger
        self._switch_handler: SwitchHandler = switch_handler
        self._database_driver = PostgreSQLDriver(logger=logger)

        # Data
        self._market_df = None

    def _connect_to_database(self, data_quality: DataQuality) -> None:
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

    def _select_database(self, database_name: str) -> None:
        self._logger.log_info(f'Selecting database "{database_name}"...')

        self._database_driver.change_database(database_name)

    def _select(
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

    def _clean(
        self, df: pd.DataFrame, clean_layer_list: List[CleanLayer]
    ) -> pd.DataFrame:
        if not clean_layer_list:
            return df

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

                case _:
                    # Optional: handle unknown layer or skip
                    pass

        return df

    def _save_pandas_table_to_database(
        self,
        schema_name: str,
        table_name: str,
        primary_keys: List[str],
        df: pd.DataFrame,
    ) -> None:
        self._logger.log_info(
            f'Saving dataframe to table "{schema_name}.{table_name}".'
        )

        # Drop rows where all values are NaN
        df = df.dropna(how="all")

        if df.empty:
            self._logger.log_info("DataFrame is empty after cleaning. Nothing to save.")
            return

        # Convert entire DataFrame into a list of Records (vectorized)
        column_names = list(df.columns)
        records = []

        for row in df.itertuples(index=False, name=None):
            data_dto_list = [
                DataModel(column_name=col, value=(val if pd.notna(val) else None))
                for col, val in zip(column_names, row)
            ]
            records.append(Record(data_dto_list=data_dto_list))

        # Batch upsert once
        result = self._database_driver.upsert(
            schema_name=schema_name,
            table_name=table_name,
            records=records,
            primary_keys=primary_keys,
        )

        if result[0] == DatabaseExecutionStatus.SUCCESS:
            inserted_count = result[1]
            updated_count = result[2]
        else:
            inserted_count = updated_count = 0

        self._logger.log_info(
            f"Saved {inserted_count + updated_count}/{len(df)} records into table '{schema_name}.{table_name}'."
            f" (Inserted: {inserted_count}, Updated: {updated_count}) successfully."
        )

    # region Helper functions
    def _get_market_df(self) -> pd.DataFrame:
        if not isinstance(self._market_df, pd.DataFrame):
            self._market_df = self._database_driver.select(
                schema_name=Schema.STOCK_MARKET.value,
                table_name=Table.MARKET.name,
            )

        return self._market_df

    def _get_market_id(self, market_code: str) -> int:
        market_df = self._get_market_df()

        market_id = market_df[market_df[Table.MARKET.Column.CODE.value] == market_code][
            Table.MARKET.Column.ID.value
        ].item()

        return market_id

    def _get_year_list_from_start(self, start_date):
        end_year = datetime.today().year
        return list(range(start_date.year, end_year + 1))

    def _melt_dataframe_by_time_format(
        self, df: pd.DataFrame, time_format: TimeFormat, id_vars: list[str]
    ) -> pd.DataFrame:
        match time_format:
            case TimeFormat.YEAR:
                # Melt from wide to long format
                df = df.melt(id_vars=id_vars, var_name="year_str", value_name="value")

                # Clean numeric values
                df["value"] = df["value"].astype(str).str.replace(",", "", regex=False)
                df["value"] = pd.to_numeric(df["value"], errors="coerce")

                # Extract year
                df["year"] = pd.to_datetime(df["year_str"], errors="coerce").dt.year

                # Use pivot_table with first() to handle duplicates
                df = df.pivot_table(
                    index=["year"], columns=id_vars[0], values="value", aggfunc="first"
                ).reset_index()

                # Sort by year and month
                df = df.sort_values(["year"]).reset_index(drop=True)

            case TimeFormat.QUARTER_YEAR:
                # Melt from wide to long format
                df = df.melt(
                    id_vars=id_vars,
                    var_name="quarter_str",
                    value_name="value",
                )

                # Filter out any non-quarter columns
                df = df[df["quarter_str"].str.match(r"Q\d+/\d{4}")]

                # Clean numeric values
                df["value"] = df["value"].astype(str).str.replace(",", "", regex=False)
                df["value"] = pd.to_numeric(df["value"], errors="coerce")

                # Extract year and quarter
                df["quarter"] = df["quarter_str"].str.extract(r"Q(\d+)/")[0].astype(int)
                df["year"] = df["quarter_str"].str.extract(r"/(\d{4})")[0].astype(int)

                # Use pivot_table with first() to handle duplicates
                df = df.pivot_table(
                    index=["year", "quarter"],
                    columns=id_vars[0],
                    values="value",
                    aggfunc="first",
                ).reset_index()

                # Sort by year and quarter
                df = df.sort_values(["year", "quarter"]).reset_index(drop=True)

            case TimeFormat.MONTH_NAME_YEAR:
                # Melt from wide to long format
                df = df.melt(
                    id_vars=id_vars,
                    var_name="month_str",
                    value_name="value",
                )

                # Clean numeric values
                df["value"] = df["value"].astype(str).str.replace(",", "", regex=False)
                df["value"] = pd.to_numeric(df["value"], errors="coerce")

                # Extract year and month
                df["date"] = pd.to_datetime(df["month_str"], errors="coerce")

                # Drop rows where date couldn't be parsed
                df = df.dropna(subset=["date"])

                # Extract numeric year, month
                df["month"] = df["date"].dt.month
                df["year"] = df["date"].dt.year

                # Use pivot_table with first() to handle duplicates
                df = df.pivot_table(
                    index=["year", "month"],
                    columns=id_vars[0],
                    values="value",
                    aggfunc="first",
                ).reset_index()

                # Sort by year and month
                df = df.sort_values(["year", "month"]).reset_index(drop=True)

            case TimeFormat.MONTH_INDEX_YEAR:
                # Melt from wide to long format
                df = df.melt(
                    id_vars=id_vars,
                    var_name="month_str",
                    value_name="value",
                )

                # Filter out any non-month columns
                df = df[df["month_str"].str.match(r"M\d+/\d{4}")]

                # Clean numeric values
                df["value"] = df["value"].astype(str).str.replace(",", "", regex=False)
                df["value"] = pd.to_numeric(df["value"], errors="coerce")

                # Extract year and month
                df["month"] = df["month_str"].str.extract(r"M(\d+)/")[0].astype(int)
                df["year"] = df["month_str"].str.extract(r"/(\d{4})")[0].astype(int)

                # Use pivot_table with first() to handle duplicates
                df = df.pivot_table(
                    index=["year", "month"],
                    columns=id_vars[0],
                    values="value",
                    aggfunc="first",
                ).reset_index()

                # Sort by year and month
                df = df.sort_values(["year", "month"]).reset_index(drop=True)

            case TimeFormat.THREE_MONTH_INDEX_YEAR:
                # Melt from wide to long format
                df = df.melt(
                    id_vars=id_vars,
                    var_name="month_str",
                    value_name="value",
                )

                # Filter out any non-month columns
                df = df[df["month_str"].str.match(r"\d+M/\d{4}")]

                # Clean numeric values
                df["value"] = df["value"].astype(str).str.replace(",", "", regex=False)
                df["value"] = pd.to_numeric(df["value"], errors="coerce")

                # Extract year and month
                df["month"] = df["month_str"].str.extract(r"(\d+)M/")[0].astype(int)
                df["year"] = df["month_str"].str.extract(r"/(\d{4})")[0].astype(int)

                # Use pivot_table with first() to handle duplicates
                df = df.pivot_table(
                    index=["year", "month"],
                    columns=id_vars[0],
                    values="value",
                    aggfunc="first",
                ).reset_index()

                # Sort by year and month
                df = df.sort_values(["year", "month"]).reset_index(drop=True)

            case TimeFormat.DAY_MONTH_YEAR:
                # Melt from wide to long format
                df = df.melt(
                    id_vars=id_vars,
                    var_name="date_str",
                    value_name="value",
                )

                # Filter only valid dd/mm/yyyy
                df = df[df["date_str"].str.match(r"\d{2}/\d{2}/\d{4}")]

                # Clean numeric values
                df["value"] = df["value"].astype(str).str.replace(",", "", regex=False)
                df["value"] = pd.to_numeric(df["value"], errors="coerce")

                # Convert to datetime
                df["date"] = pd.to_datetime(
                    df["date_str"], format="%d/%m/%Y", errors="coerce"
                )

                # Extract year, month, day
                df["year"] = df["date"].dt.year
                df["month"] = df["date"].dt.month
                df["day"] = df["date"].dt.day

                # Pivot table to wide format
                df = df.pivot_table(
                    index=["year", "month", "day"],
                    columns=id_vars[0],
                    values="value",
                    aggfunc="first",
                ).reset_index()

                # Sort by date
                df = df.sort_values(["year", "month", "day"]).reset_index(drop=True)

        return df

    def _standardize_column_name_before_melting(
        self, df: pd.DataFrame, column_name: str = "Chỉ tiêu"
    ) -> pd.DataFrame:
        df[column_name] = (
            df[column_name]
            .str.lower()
            .str.replace(
                r"[^a-z0-9_\s-]", "", regex=True
            )  # remove everything except letters, numbers, underscore, space
            .str.replace(
                r"[\s-]+", "_", regex=True
            )  # replace any whitespace with underscore
        )

        return df

    # endregion Helper functions

    # region Create Schemas
    def _create_schemas(self, data_quality: DataQuality) -> None:
        self._logger.log_info(f'Start creating schemas for "{data_quality.value}".')

        match data_quality:
            case DataQuality.BRONZE:
                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_bronze", "macroeconomics"
                ):
                    self._database_driver.create_schema(Schema.MACROECONOMICS.value)

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_bronze", "stock_market"
                ):
                    self._database_driver.create_schema(Schema.STOCK_MARKET.value)

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_bronze", "enterprise"
                ):
                    self._database_driver.create_schema(Schema.ENTERPRISE.value)

            case DataQuality.SILVER:
                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "macroeconomics"
                ):
                    self._database_driver.create_schema(Schema.MACROECONOMICS.value)

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "stock_market"
                ):
                    self._database_driver.create_schema(Schema.STOCK_MARKET.value)

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "enterprise"
                ):
                    self._database_driver.create_schema(Schema.ENTERPRISE.value)

            case DataQuality.GOLD:
                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_gold", "macroeconomics"
                ):
                    self._database_driver.create_schema(Schema.MACROECONOMICS.value)

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_gold", "stock_market"
                ):
                    self._database_driver.create_schema(Schema.STOCK_MARKET.value)

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_gold", "enterprise"
                ):
                    self._database_driver.create_schema(Schema.ENTERPRISE.value)

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(f'Finish creating schemas for "{data_quality.value}".')

    # endregion Create Schemas

    # region Create Tables
    def _create_macroeconomics_tables(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start creating macroeconomics tables for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                # GDP
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.GDP.name,
                    columns=[
                        Column(name=Table.GDP.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.GDP.Column.QUARTER.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.GDP.Column.AGRICULTURE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GDP.Column.INDUSTRY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GDP.Column.SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GDP.Column.GDP_GROWTH.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GDP.Column.GDP_REAL.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.GDP.primary_key,
                )
                # fmt: on

                # CPI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.CPI.name,
                    columns = [
                        Column(name=Table.CPI.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.CPI.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.CPI.Column.BEVERAGE_AND_CIGARETTE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.CONSUMER_PRICE_INDEX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.CULTURE_ENTERTAINMENT_AND_TOURISM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.EATING_OUTSIDE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.EDUCATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.FOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.FOOD_AND_FOODSTUFF.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.FOODSTUFF.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.GARMENT_FOOTWEAR_HAT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.HOUSEHOLD_APPLIANCES_AND_GOODS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.HOUSING_AND_CONSTRUCTION_MATERIALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.MEDICINE_AND_HEALTH_CARE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.OTHER_GOODS_AND_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.POSTAL_SERVICES_AND_TELECOMMUNICATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.TRAFFIC.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.CPI.primary_key,
                )
                # fmt: on

                # PPI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.PPI.name,
                    columns = [
                        Column(name=Table.PPI.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.PPI.Column.GENERAL_INDEX.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.FORESTRY_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.AGRICULTURAL_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.FORESTRY_AND_RELATED_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.EXPLOITED_FOREST_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.COLLECTED_FOREST_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.AGRICULTURE_AND_RELATED_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.LIVESTOCK_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.ANNUAL_CROP_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.PERENNIAL_CROP_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.EXPLOITED_AQUATIC_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.AQUATIC_PRODUCTS_EXPLOITATION_AND_FARMING.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.AQUATIC_FARMING_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.FOREST_PLANTING_AND_CARE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.PPI.primary_key,
                )
                # fmt: on

                # IPI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.IPI.name,
                    columns=[
                        Column(name=Table.IPI.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IPI.Column.GENERAL_INDEX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.PROFESSIONAL_SCIENTIFIC_AND_TECHNICAL_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.CONSTRUCTION_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.PAPER_AND_PAPER_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.CHEMICALS_AND_CHEMICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.MACHINERY_AND_EQUIPMENT_NOT_ELSEWHERE_CLASSIFIED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.NATURAL_WATER_EXTRACTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.NATURAL_WATER_EXTRACTION_AND_WASTE_MANAGEMENT_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.OTHER_TRANSPORT_EQUIPMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.METAL_ORES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.PROCESSED_FOOD_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.MANUFACTURING_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.TEXTILES_AND_LEATHER_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.MINING_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.OTHER_MINING_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.METAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.FORESTRY_PRODUCTS_AND_RELATED_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.AGRICULTURE_FORESTRY_AND_FISHERY_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.AGRICULTURE_PRODUCTS_AND_RELATED_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.FISHING_AND_AQUACULTURE_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.RUBBER_AND_PLASTIC_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.WOOD_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.OTHER_NON_METALLIC_MINERAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.FABRICATED_METAL_PRODUCTS_EXCEPT_MACHINERY_AND_EQUIPMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.ELECTRONIC_COMPUTER_AND_OPTICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.USED_FOR_MANUFACTURING_INDUSTRY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.USED_FOR_AGRICULTURE_FORESTRY_AND_FISHERY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.USED_FOR_CONSTRUCTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.COKE_AND_REFINED_PETROLEUM_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.HARD_COAL_AND_LIGNITE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.ELECTRICAL_EQUIPMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.PHARMACEUTICALS_AND_MEDICINAL_CHEMICALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.MOTOR_VEHICLES_AND_TRAILERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.ELECTRICITY_GAS_STEAM_AND_AIR_CONDITIONING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.BEVERAGES_AND_TOBACCO.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.IPI.primary_key,
                )
                # fmt: on

                # XPI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.XPI.name,
                    columns=[
                        Column(name=Table.XPI.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.XPI.Column.MONTH.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.XPI.Column.ANIMAL_FEED_AND_RAW_MATERIALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.AQUATIC_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CAMERAS_CAMCORDERS_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CASHEW_NUTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CASSAVA_AND_CASSAVA_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CHEMICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CHEMICALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CLINKER_AND_CEMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.COFFEE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CONFECTIONERY_AND_CEREAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.DOMESTIC_ECONOMIC_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.ELECTRICAL_WIRES_AND_CABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.ELECTRONICS_COMPUTERS_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.FOOTWEAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.FOREIGN_INVESTED_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.FOREIGN_CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.FURNITURE_PRODUCTS_FROM_MATERIALS_OTHER_THAN_WOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.GLASS_AND_GLASS_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.HANDBAGS_WALLETS_SUITCASES_HATS_UMBRELLAS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.IRON_AND_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.IRON_AND_STEEL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.MACHINERY_EQUIPMENT_TOOLS_SPARE_PARTS_OTHER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.MAIN_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.OTHER_BASE_METALS_AND_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.OTHER_GOODS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.PAPER_AND_PAPER_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.PEPPER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.PETROLEUM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.PHONES_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.PLASTIC_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.RAW_PLASTICS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.RICE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.RUBBER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.RUBBER_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.TEA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.TEXTILE_FIBERS_YARNS_OF_ALL_KINDS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.TEXTILE_GARMENT_LEATHER_FOOTWEAR_RAW_MATERIALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.TEXTILES_GARMENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.TOTAL_VALUE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.TOYS_SPORTS_EQUIPMENT_AND_PARTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.TRANSPORTATION_VEHICLES_AND_SPARE_PARTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.VEGETABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.WOOD_AND_WOOD_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.XPI.primary_key,
                )
                # fmt: on

                # MPI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.MPI.name,
                    columns=[
                        Column(name=Table.MPI.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.MPI.Column.MONTH.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.MPI.Column.ANIMAL_FEED_AND_RAW_MATERIALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.AQUATIC_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CAMERAS_CAMCORDERS_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CASHEW_NUTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CASSAVA_AND_CASSAVA_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CHEMICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CHEMICALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CLINKER_AND_CEMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.COFFEE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CONFECTIONERY_AND_CEREAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.DOMESTIC_ECONOMIC_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.ELECTRICAL_WIRES_AND_CABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.ELECTRONICS_COMPUTERS_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.FOOTWEAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.FOREIGN_INVESTED_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.FOREIGN_CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.FURNITURE_PRODUCTS_FROM_MATERIALS_OTHER_THAN_WOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.GLASS_AND_GLASS_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.HANDBAGS_WALLETS_SUITCASES_HATS_UMBRELLAS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.IRON_AND_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.IRON_AND_STEEL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.MACHINERY_EQUIPMENT_TOOLS_SPARE_PARTS_OTHER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.MAIN_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.OTHER_BASE_METALS_AND_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.OTHER_GOODS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.PAPER_AND_PAPER_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.PEPPER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.PETROLEUM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.PHONES_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.PLASTIC_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.RAW_PLASTICS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.RICE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.RUBBER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.RUBBER_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.TEA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.TEXTILE_FIBERS_YARNS_OF_ALL_KINDS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.TEXTILE_GARMENT_LEATHER_FOOTWEAR_RAW_MATERIALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.TEXTILES_GARMENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.TOTAL_VALUE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.TOYS_SPORTS_EQUIPMENT_AND_PARTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.TRANSPORTATION_VEHICLES_AND_SPARE_PARTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.VEGETABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.WOOD_AND_WOOD_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.MPI.primary_key,
                )
                # fmt: on

                # POPULATION
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.POPULATION.name,
                    columns=[
                        Column(name=Table.POPULATION.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.POPULATION.Column.POPULATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.POPULATION.Column.POPULATION_AREA_URBAN_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.POPULATION.Column.POPULATION_DENSITY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.POPULATION.Column.POPULATION_GROWTH_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.POPULATION.primary_key,
                )
                # fmt: on

                # LABOR
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.LABOR.name,
                    columns=[
                        Column(name=Table.LABOR.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.LABOR.Column.AGRICULTURE_FORESTRY_AND_FISHERY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.EMPLOYED_AMOUNT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.FEMALE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.INDUSTRY_CONSTRUCTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.LABOR_FORCE_ANNUAL_CHANGE_PERCENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.LABOR_FORCE_PARTICIPATION_RATE_PERCENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.MALE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.UNEMPLOYED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.URBAN_UNEMPLOYMENT_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.LABOR.primary_key,
                )
                # fmt: on

                # RETAIL
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.RETAIL.name,
                    columns=[
                        Column(name=Table.RETAIL.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.RETAIL.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.RETAIL.Column.ACCOMMODATION_AND_CATERING_SERVICE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.RETAIL.Column.RETAIL_GROWTH.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.RETAIL.Column.RETAIL_SALE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.RETAIL.Column.SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.RETAIL.Column.TRAVELING_SERVICE.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.RETAIL.primary_key,
                )
                # fmt: on

                # PMI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.PMI.name,
                    columns=[
                        Column(name=Table.PMI.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.PMI.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.PMI.Column.PMI.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.PMI.primary_key,
                )
                # fmt: on

                # IIP
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.IIP.name,
                    columns=[
                        Column(name=Table.IIP.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IIP.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IIP.Column.APPAREL_MANUFACTURING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.BEVERAGE_PRODUCTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.COAL_AND_LIGNITE_MINING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.CRUDE_OIL_AND_NATURAL_GAS_EXTRACTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.ENTIRE_INDUSTRIAL_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.FOOD_PRODUCTION_AND_PROCESSING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.LEATHER_AND_RELATED_PRODUCT_MANUFACTURING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_CHEMICALS_AND_CHEMICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_COKE_AND_REFINED_PETROLEUM_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_ELECTRICAL_EQUIPMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_ELECTRONIC_PRODUCTS_COMPUTERS_AND_OPTICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_FABRICATED_METAL_PRODUCTS_EXCLUDING_MACHINERY_AND_EQUIPMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_FURNITURE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_MACHINERY_AND_EQUIPMENT_NOT_ELSEWHERE_CLASSIFIED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_METALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_MOTOR_VEHICLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_OTHER_NON_METALLIC_MINERAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_OTHER_TRANSPORT_EQUIPMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_PHARMACEUTICALS_MEDICINAL_CHEMICALS_AND_BOTANICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_RUBBER_AND_PLASTIC_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURING_INDUSTRY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.METAL_ORE_MINING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MINING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MINING_SUPPORT_SERVICE_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.OTHER_MANUFACTURING_INDUSTRIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.OTHER_MINING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.PAPER_AND_PAPER_PRODUCT_MANUFACTURING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.PRINTING_AND_REPRODUCTION_OF_RECORDED_MEDIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.PRODUCTION_AND_DISTRIBUTION_OF_ELECTRICITY_GAS_HOT_WATER_STEAM_AND_AIR_CONDITIONING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.REPAIR_MAINTENANCE_AND_INSTALLATION_OF_MACHINERY_AND_EQUIPMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.TEXTILE_MANUFACTURING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.TOBACCO_PRODUCT_MANUFACTURING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.WASTE_COLLECTION_TREATMENT_AND_DISPOSAL_ACTIVITIES_RECYCLING_OF_WASTE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.WASTEWATER_COLLECTION_AND_TREATMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.WATER_COLLECTION_TREATMENT_AND_SUPPLY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.WATER_SUPPLY_WASTE_MANAGEMENT_AND_TREATMENT_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.IIP.primary_key,
                )
                # fmt: on

                # IPV
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.IPV.name,
                    columns=[
                        Column(name=Table.IPV.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IPV.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IPV.Column.ALUMINIUM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.ANIMAL_FEED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.AQUATIC_FEED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.BEER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.CARS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.CASUAL_CLOTHES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.CEMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.CHEMICAL_PAINTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.CIGARETTES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.COAL_CLEAN_COAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.COMMERCIAL_TAP_WATER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.ELECTRICITY_PRODUCED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.EXTRACTED_CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.FRESH_MILK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.GASOLINE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.GRANULATED_SUGAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.IRON_CRUDE_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.LEATHER_SHOES_AND_SANDALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.LIQUIDIZED_GAS_LPG.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.MOBILE_PHONES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.MONONATRI_GLUTAMAT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.MOTORCYCLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.NPK_MIXED_FERTILIZERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.NATURAL_FABRICS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.NATURAL_GAS_AIR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.PHONE_ACCESSORIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.POWDERED_MILK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.PROCESSED_SEAFOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.ROLLED_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.STEEL_BARS_ANGLE_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.SYNTHETIC_FABRICS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.TELEVISION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.UREA_FERTILIZER.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.IPV.primary_key,
                )
                # fmt: on

                # MIP
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.MIP.name,
                    columns=[
                        Column(name=Table.MIP.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.MIP.Column.AIR_CONDITIONERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.ANIMAL_AND_POULTRY_FEED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.ANTIMONY_ORE_AND_ANTIMONY_CONCENTRATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.APATITE_ORE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.AQUACULTURE_FEED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.ASSEMBLED_CARS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.ASSEMBLED_MOTORCYCLES_AND_MOPEDS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.ASSEMBLED_TVS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.BATH_MILK_AND_FACIAL_CLEANSER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.BEER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CANNED_FRUITS_AND_NUTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CANNED_MEAT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CANNED_SEAFOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CANNED_VEGETABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CAR_AND_TRACTOR_TIRES_INFLATABLE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CAST_OR_OTHER_ROUGH_IRON_AND_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CASUAL_CLOTHING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CEMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CHEMICAL_FERTILIZERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CLEAN_COAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.COMMERCIAL_TAP_WATER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.COPPER_ORE_AND_COPPER_CONCENTRATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CRUDE_OIL_EXTRACTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.DIGITAL_CAMERAS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.DOMESTIC_CERAMICS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.DOMESTIC_CRUDE_OIL_EXTRACTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.EXTRACTED_STONE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FABRIC.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FABRIC_SHOES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FIBER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FIBER_CEMENT_ROOFING_SHEETS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FIRED_BRICKS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FIRED_TILES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FISH_SAUCE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FRESH_MILK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FROZEN_SEAFOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.GENERATED_ELECTRICITY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.GRANULATED_SUGAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.GRAVEL_AND_PEBBLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.GROUND_COFFEE_AND_INSTANT_COFFEE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.HERBICIDES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.HOUSEHOLD_ELECTRIC_FANS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.HOUSEHOLD_REFRIGERATORS_AND_FREEZERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.HOUSEHOLD_WASHING_MACHINES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.IRON_ORE_AND_IRON_CONCENTRATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.LANDLINE_PHONES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.LAUNDRY_DETERGENT_AND_CLEANING_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.LEATHER_SHOES_AND_BOOTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.LIGHT_BULBS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.MILLED_RICE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.MINERAL_WATER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.MOBILE_PHONES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.MOTORCYCLE_AND_BICYCLE_TIRES_INFLATABLE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.MSG_MONOSODIUM_GLUTAMATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.NATURAL_GAS_IN_GAS_FORM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.NPK_FERTILIZERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.PAPER_AND_CARDBOARD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.PESTICIDES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.PLASTIC_PACKAGING_AND_BAGS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.POWDERED_MILK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.PRINTED_NEWSPAPERS_AND_OTHER_PRINTING_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.PRINTERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.PROCESSED_TEA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.PURIFIED_WATER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.REFINED_VEGETABLE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.ROLLED_STEEL_AND_SHAPED_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.SANITARY_WARE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.SAWN_TIMBER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.SEA_SALT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.SHAMPOO_AND_CONDITIONER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.SPIRITS_AND_WHITE_WINE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.SPORTS_SHOES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.STANDARD_BATTERIES_1_5V.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.THRESHING_MACHINES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.TITANIUM_ORE_AND_TITANIUM_CONCENTRATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.TOBACCO.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.TOOTHPASTE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.TUBES_FOR_BICYCLES_AND_MOTORCYCLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.TUBES_FOR_CARS_AND_AIRCRAFT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.VARIOUS_TYPES_OF_BATTERIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.VARIOUS_TYPES_OF_BICYCLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.VARIOUS_TYPES_OF_SAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.YELLOW_PHOSPHORUS.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.MIP.primary_key,
                )
                # fmt: on

                # FA_BY_HOUSE_TYPES
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.FA_BY_HOUSE_TYPES.name,
                    columns=[
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column._16_20_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column._21_25_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column._26_FLOORS_AND_ABOVE.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column._5_FLOORS_AND_BELOW.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column._6_8_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column._9_15_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column.APARTMENT_BUILDINGS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column.SINGLE_FAMILY_HOMES.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column.SINGLE_FAMILY_HOMES_4_FLOORS_AND_ABOVE.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column.SINGLE_FAMILY_HOMES_BELOW_4_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column.TOTAL.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column.VILLAS.value, data_type=DataType.INT(), nullable=True),
                    ],
                    primary_keys=Table.FA_BY_HOUSE_TYPES.primary_key,
                )
                # fmt: on

                # IT_BOP
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.IT_BOP.name,
                    columns=[
                        Column(name=Table.IT_BOP.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IT_BOP.Column.QUARTER.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.A_CURRENT_ACCOUNT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.B_CAPITAL_ACCOUNT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.BORROWING_AND_EXTERNAL_DEBT_REPAYMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.C_FINANCIAL_ACCOUNT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.CAPITAL_ACCOUNT_PAYMENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.CAPITAL_ACCOUNT_RECEIPTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.CAPITAL_WITHDRAWAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.CURRENT_TRANSFERS_SECONDARY_INCOME_NET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.CURRENT_TRANSFERS_SECONDARY_INCOME_PAYMENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.CURRENT_TRANSFERS_SECONDARY_INCOME_RECEIPTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.D_ERRORS_AND_OMISSIONS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.DIRECT_INVESTMENT_ABROAD_ASSETS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.DIRECT_INVESTMENT_IN_VIETNAM_LIABILITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.DIRECT_INVESTMENT_NET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.E_OVERALL_BALANCE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.F_RESERVES_AND_RELATED_ITEMS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.FINANCIAL_INSTITUTIONS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.GOODS_EXPORTS_FOB.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.GOODS_IMPORTS_FOB.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.GOODS_NET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.GOVERNMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.IMF_CREDITS_AND_LOANS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.INVESTMENT_INCOME_PRIMARY_INCOME_NET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.INVESTMENT_INCOME_PRIMARY_INCOME_PAYMENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.INVESTMENT_INCOME_PRIMARY_INCOME_RECEIPTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.LOANS_AND_EXTERNAL_DEBT_COLLECTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.LONG_TERM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.MONEY_AND_DEPOSITS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.OTHER_INVESTMENT_ASSETS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.OTHER_INVESTMENT_LIABILITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.OTHER_INVESTMENT_NET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.OTHER_RECEIVABLESPAYABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.PORTFOLIO_INVESTMENT_ABROAD_ASSETS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.PORTFOLIO_INVESTMENT_IN_VIETNAM_LIABILITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.PORTFOLIO_INVESTMENT_NET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.PRINCIPAL_REPAYMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.PRIVATE_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.RESERVE_ASSETS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.RESIDENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.SERVICES_EXPORTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.SERVICES_IMPORTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.SERVICES_NET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.SHORT_TERM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.SPECIAL_FINANCING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.TOTAL_CURRENT_AND_CAPITAL_ACCOUNT_BALANCE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.TRADE_CREDITS_AND_ADVANCES.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.IT_BOP.primary_key,
                )
                # fmt: on

                # TSBR
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.TSBR.name,
                    columns=[
                        Column(name=Table.TSBR.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.TSBR.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.TSBR.Column.AGRICULTURAL_LAND_USE_TAX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.AID_REVENUE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.DOMESTIC_REVENUE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.ENVIRONMENTAL_PROTECTION_TAX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.ENVIRONMENTAL_PROTECTION_TAX_ON_IMPORTED_GOODS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.EXPORT_TAX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.FEES_AND_CHARGES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.IMPORT_TAX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.NON_AGRICULTURAL_LAND_USE_TAX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.OTHER_BUDGET_REVENUES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.OTHER_REVENUE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.PERSONAL_INCOME_TAX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.RECOVERY_OF_CAPITAL_DIVIDENDS_POST_TAX_PROFITS_SURPLUS_REVENUE_AND_EXPENDITURE_OF_THE_STATE_BANK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_BALANCE_FROM_IMPORT_EXPORT_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_FOREIGN_INVESTED_ENTERPRISES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_HOUSING_AND_LAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_LAND_AND_WATER_SURFACE_LEASING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_LAND_USE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_LEASING_AND_SALE_OF_STATE_OWNED_HOUSING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_LOTTERY_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_MINING_RIGHTS_LICENSING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_NON_STATE_ECONOMIC_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_PUBLIC_LAND_FUNDS_AND_OTHER_PUBLIC_ASSET_BENEFITS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_STATE_OWNED_ENTERPRISES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.SPECIAL_CONSUMPTION_TAX_ON_IMPORTED_GOODS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.TOTAL_REVENUE_FROM_IMPORT_EXPORT_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.TOTAL_STATE_BUDGET_REVENUE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.VALUE_ADDED_TAX_ON_IMPORTED_GOODS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.VALUE_ADDED_TAX_REFUND.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.TSBR.primary_key,
                )
                # fmt: on

                # TSBE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.TSBE.name,
                    columns=[
                        Column(name=Table.TSBE.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.TSBE.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.TSBE.Column.AID_EXPENDITURE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.DEBT_INTEREST_PAYMENT_EXPENDITURE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.DEVELOPMENT_INVESTMENT_EXPENDITURE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.EXPENDITURE_FOR_EDUCATION_TRAINING_AND_VOCATIONAL_EDUCATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.EXPENDITURE_FOR_SCIENCE_AND_TECHNOLOGY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.EXPENDITURE_FOR_WAGE_REFORM_AND_STREAMLINING_PERSONNEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.REGULAR_EXPENDITURE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.STATE_BUDGET_CONTINGENCY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.SUPPLEMENTARY_EXPENDITURE_FOR_FINANCIAL_RESERVE_FUND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.TOTAL_STATE_BUDGET_EXPENDITURE.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.TSBE.primary_key,
                )
                # fmt: on

                # GD
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.GD.name,
                    columns=[
                        Column(name=Table.GD.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.GD.Column.DEBT_BALANCE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GD.Column.DOMESTIC_DEBT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GD.Column.FOREIGN_DEBT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GD.Column.TOTAL_DEBT_PAYMENTS_DURING_THE_PERIOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GD.Column.TOTAL_INTEREST_AND_FEES_PAID_DURING_THE_PERIOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GD.Column.TOTAL_PRINCIPAL_REPAYMENT_DURING_THE_PERIOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GD.Column.WITHDRAWALS_DURING_THE_PERIOD.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.GD.primary_key,
                )
                # fmt: on

                # BRD
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.BRD.name,
                    columns=[
                        Column(name=Table.BRD.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.BRD.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.BRD.Column.ENTERPRISES_COMPLETING_DISSOLUTION.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.BRD.Column.ENTERPRISES_RESUMING_OPERATIONS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.BRD.Column.ENTERPRISES_TEMPORARILY_SUSPENDED_AWAITING_DISSOLUTION.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.BRD.Column.NEWLY_ESTABLISHED_ENTERPRISES.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.BRD.Column.REGISTERED_CAPITAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.BRD.Column.REGISTERED_LABOR.value, data_type=DataType.INT(), nullable=True),
                    ],
                    primary_keys=Table.BRD.primary_key,
                )
                # fmt: on

                # IISD
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.IISD.name,
                    columns=[
                        Column(name=Table.IISD.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IISD.Column.QUARTER.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IISD.Column.FOREIGN_DIRECT_INVESTMENT_CAPITAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.GOVERNMENT_BOND_CAPITAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.INVESTMENT_CAPITAL_FROM_RESIDENTS_AND_PRIVATE_INDIVIDUALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.INVESTMENT_CAPITAL_FROM_THE_STATE_BUDGET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.INVESTMENT_CAPITAL_OF_STATE_ENTERPRISES_EQUITY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.LOANS_FROM_OTHER_SOURCES_OF_THE_STATE_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.OTHER_MOBILIZED_CAPITAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.PLANNED_STATE_INVESTMENT_CREDIT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.TOTAL.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.IISD.primary_key,
                )
                # fmt: on

                # TREG
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.TREG.name,
                    columns=[
                        Column(name=Table.TREG.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.TREG.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.TREG.Column.INTERNATIONAL_LIQUIDITY_TOTAL_RESERVES_EXCLUDING_GOLD_FOREIGN_EXCHANGE_US_DOLLARS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TREG.Column.INTERNATIONAL_LIQUIDITY_TOTAL_RESERVES_EXCLUDING_GOLD_US_DOLLARS.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.TREG.primary_key,
                )
                # fmt: on

                # CREDIT
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.CREDIT.name,
                    columns=[
                        Column(name=Table.CREDIT.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.CREDIT.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.CREDIT.Column.CREDIT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CREDIT.Column.CREDIT_GROWTH_YTD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CREDIT.Column.MONEY_SUPPLY_GROWTH_M2_YTD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CREDIT.Column.MONEY_SUPPLY_M2.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.CREDIT.primary_key,
                )
                # fmt: on

                # MOBILIZATION
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.MOBILIZATION.name,
                    columns=[
                        Column(name=Table.MOBILIZATION.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.MOBILIZATION.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.MOBILIZATION.Column.DEPOSITS_FROM_ECONOMIC_ORGANIZATIONS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MOBILIZATION.Column.DEPOSITS_FROM_RESIDENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MOBILIZATION.Column.TOTAL_PAYMENT_INSTRUMENTS.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.MOBILIZATION.primary_key,
                )
                # fmt: on

                # EXCHANGE_RATE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.EXCHANGE_RATE.name,
                    columns = [
                        Column(name=Table.EXCHANGE_RATE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.EXCHANGE_RATE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.EXCHANGE_RATE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.EXCHANGE_RATE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.EXCHANGE_RATE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.EXCHANGE_RATE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.EXCHANGE_RATE.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.EXCHANGE_RATE.primary_key,
                )
                # fmt: on

                # IIR
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.IIR.name,
                    columns=[
                        Column(name=Table.IIR.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IIR.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IIR.Column.DAY.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IIR.Column.ONE_MONTH.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIR.Column.ONE_WEEK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIR.Column.TWO_WEEKS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIR.Column.THREE_MONTHS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIR.Column.SIX_MONTHS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIR.Column.NINE_MONTHS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIR.Column.OVERNIGHT.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.IIR.primary_key,
                )
                # fmt: on

                # RRRR
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.RRRR.name,
                    columns=[
                        Column(name=Table.RRRR.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.RRRR.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.RRRR.Column.DAY.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.RRRR.Column.DISCOUNT_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.RRRR.Column.REFINANCING_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.RRRR.primary_key,
                )
                # fmt: on

                # FDI_SECTOR
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.FDI_SECTOR.name,
                    columns=[
                        Column(name=Table.FDI_SECTOR.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.FDI_SECTOR.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.FDI_SECTOR.Column.ACCOMMODATION_AND_FOOD_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.ADMINISTRATIVE_AND_SUPPORT_SERVICE_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.AGRICULTURE_FORESTRY_AND_FISHERY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.ARTS_ENTERTAINMENT_AND_RECREATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.CONSTRUCTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.DOMESTIC_HOUSEHOLD_SERVICE_WORKERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.EDUCATION_AND_TRAINING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.FINANCIAL_BANKING_AND_INSURANCE_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.HEALTHCARE_AND_SOCIAL_ASSISTANCE_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.INFORMATION_AND_COMMUNICATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.MANUFACTURING_AND_PROCESSING_INDUSTRY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.MINING_AND_QUARRYING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.OTHER_SERVICE_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.PRODUCTION_AND_DISTRIBUTION_OF_ELECTRICITY_GAS_WATER_AND_AIR_CONDITIONING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.PROFESSIONAL_SCIENTIFIC_AND_TECHNOLOGICAL_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.REAL_ESTATE_BUSINESS_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.TRANSPORTATION_AND_WAREHOUSING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.WATER_SUPPLY_AND_WASTE_TREATMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.WHOLESALE_AND_RETAIL_REPAIR_OF_MOTOR_VEHICLES_AND_MOTORCYCLES.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.FDI_SECTOR.primary_key,
                )
                # fmt: on

                # FDI_RD
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.FDI_RD.name,
                    columns=[
                        Column(name=Table.FDI_RD.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.FDI_RD.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.FDI_RD.Column.FDI_DISBURSEMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_RD.Column.REGISTER.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.FDI_RD.primary_key,
                )
                # fmt: on

                # EXPORT
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.EXPORT.name,
                    columns=[
                        Column(name=Table.EXPORT.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.EXPORT.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.EXPORT.Column.ARGENTINA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.ASEAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.POLAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.BELARUS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.BRAZIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.BULGARIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.BELGIUM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.PORTUGAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.IVORY_COAST.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CAMEROON.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CAMBODIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CANADA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CHILE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CROATIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.UNITED_ARAB_EMIRATES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.ESTONIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.EU.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.HUNGARY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.GREECE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.NETHERLANDS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SOUTH_KOREA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.HONG_KONG.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.INDONESIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.IRELAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.ISRAEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.KAZAKHSTAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.KUWAIT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.LATVIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.LITHUANIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.LUXEMBOURG.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.LAOS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.MALAYSIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.MALTA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.MEXICO.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.MYANMAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.USA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.NORWAY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SOUTH_AFRICA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.NEW_ZEALAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.RUSSIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.BRUNEI_DARUSSALAM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.JAPAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.OTHER_COUNTRIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.PAKISTAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.PERU.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.PHILIPPINES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.FRANCE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.FINLAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.ROMANIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SENEGAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SINGAPORE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SLOVAKIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SLOVENIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CZECHIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CYPRUS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.THAILAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.TURKEY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SWITZERLAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SWEDEN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CHINA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SPAIN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.UKRAINE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.UNITED_KINGDOM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.AUSTRIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.AUSTRALIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.ITALY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.DENMARK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.TAIWAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.GERMANY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SAUDI_ARABIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.INDIA.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.EXPORT.primary_key,
                )
                # fmt: on

                # IMPORT
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.IMPORT.name,
                    columns=[
                        Column(name=Table.IMPORT.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IMPORT.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IMPORT.Column.ARGENTINA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.ASEAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.POLAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.BELARUS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.BRAZIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.BULGARIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.BELGIUM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.PORTUGAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.IVORY_COAST.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CAMEROON.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CAMBODIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CANADA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CHILE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CROATIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.UNITED_ARAB_EMIRATES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.ESTONIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.EU.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.HUNGARY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.GREECE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.NETHERLANDS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SOUTH_KOREA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.HONG_KONG.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.INDONESIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.IRELAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.ISRAEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.KAZAKHSTAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.KUWAIT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.LATVIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.LITHUANIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.LUXEMBOURG.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.LAOS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.MALAYSIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.MALTA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.MEXICO.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.MYANMAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.USA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.NORWAY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SOUTH_AFRICA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.NEW_ZEALAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.RUSSIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.BRUNEI_DARUSSALAM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.JAPAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.OTHER_COUNTRIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.PAKISTAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.PERU.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.PHILIPPINES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.FRANCE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.FINLAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.ROMANIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SENEGAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SINGAPORE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SLOVAKIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SLOVENIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CZECHIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CYPRUS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.THAILAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.TURKEY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SWITZERLAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SWEDEN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CHINA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SPAIN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.UKRAINE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.UNITED_KINGDOM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.AUSTRIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.AUSTRALIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.ITALY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.DENMARK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.TAIWAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.GERMANY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SAUDI_ARABIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.INDIA.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.IMPORT.primary_key,
                )
                # fmt: on

                # GOLD_PRICE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.GOLD_PRICE.name,
                    columns = [
                        Column(name=Table.GOLD_PRICE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.GOLD_PRICE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.GOLD_PRICE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.GOLD_PRICE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.GOLD_PRICE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.GOLD_PRICE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.GOLD_PRICE.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.GOLD_PRICE.primary_key,
                )
                # fmt: on
                
                # OIL_PRICE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.OIL_PRICE.name,
                    columns = [
                        Column(name=Table.OIL_PRICE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.OIL_PRICE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.OIL_PRICE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.OIL_PRICE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.OIL_PRICE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.OIL_PRICE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.OIL_PRICE.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.OIL_PRICE.primary_key,
                )
                # fmt: on

                # DOW_JONES
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.DOW_JONES.name,
                    columns = [
                        Column(name=Table.DOW_JONES.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.DOW_JONES.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.DOW_JONES.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.DOW_JONES.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.DOW_JONES.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.DOW_JONES.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.DOW_JONES.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.DOW_JONES.primary_key,
                )
                # fmt: on
                
                # NYSE_COMPOSITE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.NYSE_COMPOSITE.name,
                    columns = [
                        Column(name=Table.NYSE_COMPOSITE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.NYSE_COMPOSITE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.NYSE_COMPOSITE.Column.ADJ_CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.NYSE_COMPOSITE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NYSE_COMPOSITE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NYSE_COMPOSITE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NYSE_COMPOSITE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.NYSE_COMPOSITE.primary_key,
                )
                # fmt: on

                # SNP_500
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.SNP_500.name,
                    columns = [
                        Column(name=Table.SNP_500.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.SNP_500.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.SNP_500.Column.ADJ_CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.SNP_500.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.SNP_500.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.SNP_500.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.SNP_500.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.SNP_500.primary_key,
                )
                # fmt: on
                
                # NASDAQ_COMPOSITE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.NASDAQ_COMPOSITE.name,
                    columns = [
                        Column(name=Table.NASDAQ_COMPOSITE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.NASDAQ_COMPOSITE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.NASDAQ_COMPOSITE.Column.ADJ_CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.NASDAQ_COMPOSITE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NASDAQ_COMPOSITE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NASDAQ_COMPOSITE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NASDAQ_COMPOSITE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.NASDAQ_COMPOSITE.primary_key,
                )
                # fmt: on
                
                # NASDAQ_100
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.NASDAQ_100.name,
                    columns = [
                        Column(name=Table.NASDAQ_100.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.NASDAQ_100.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.NASDAQ_100.Column.ADJ_CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.NASDAQ_100.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NASDAQ_100.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NASDAQ_100.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NASDAQ_100.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.NASDAQ_100.primary_key,
                )
                # fmt: on

            case DataQuality.SILVER:
                # GDP
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.GDP.name,
                    columns=[
                        Column(name=Table.GDP.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.GDP.Column.QUARTER.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.GDP.Column.AGRICULTURE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GDP.Column.INDUSTRY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GDP.Column.SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GDP.Column.GDP_GROWTH.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GDP.Column.GDP_REAL.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.GDP.primary_key,
                )
                # fmt: on

                # CPI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.CPI.name,
                    columns = [
                        Column(name=Table.CPI.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.CPI.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.CPI.Column.BEVERAGE_AND_CIGARETTE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.CONSUMER_PRICE_INDEX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.CULTURE_ENTERTAINMENT_AND_TOURISM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.EATING_OUTSIDE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.EDUCATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.FOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.FOOD_AND_FOODSTUFF.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.FOODSTUFF.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.GARMENT_FOOTWEAR_HAT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.HOUSEHOLD_APPLIANCES_AND_GOODS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.HOUSING_AND_CONSTRUCTION_MATERIALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.MEDICINE_AND_HEALTH_CARE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.OTHER_GOODS_AND_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.POSTAL_SERVICES_AND_TELECOMMUNICATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CPI.Column.TRAFFIC.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.CPI.primary_key,
                )
                # fmt: on

                # PPI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.PPI.name,
                    columns = [
                        Column(name=Table.PPI.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.PPI.Column.GENERAL_INDEX.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.FORESTRY_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.AGRICULTURAL_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.FORESTRY_AND_RELATED_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.EXPLOITED_FOREST_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.COLLECTED_FOREST_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.AGRICULTURE_AND_RELATED_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.LIVESTOCK_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.ANNUAL_CROP_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.PERENNIAL_CROP_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.EXPLOITED_AQUATIC_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.AQUATIC_PRODUCTS_EXPLOITATION_AND_FARMING.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.AQUATIC_FARMING_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.PPI.Column.FOREST_PLANTING_AND_CARE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.PPI.primary_key,
                )
                # fmt: on

                # IPI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.IPI.name,
                    columns=[
                        Column(name=Table.IPI.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IPI.Column.GENERAL_INDEX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.PROFESSIONAL_SCIENTIFIC_AND_TECHNICAL_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.CONSTRUCTION_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.PAPER_AND_PAPER_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.CHEMICALS_AND_CHEMICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.MACHINERY_AND_EQUIPMENT_NOT_ELSEWHERE_CLASSIFIED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.NATURAL_WATER_EXTRACTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.NATURAL_WATER_EXTRACTION_AND_WASTE_MANAGEMENT_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.OTHER_TRANSPORT_EQUIPMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.METAL_ORES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.PROCESSED_FOOD_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.MANUFACTURING_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.TEXTILES_AND_LEATHER_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.MINING_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.OTHER_MINING_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.METAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.FORESTRY_PRODUCTS_AND_RELATED_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.AGRICULTURE_FORESTRY_AND_FISHERY_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.AGRICULTURE_PRODUCTS_AND_RELATED_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.FISHING_AND_AQUACULTURE_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.RUBBER_AND_PLASTIC_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.WOOD_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.OTHER_NON_METALLIC_MINERAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.FABRICATED_METAL_PRODUCTS_EXCEPT_MACHINERY_AND_EQUIPMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.ELECTRONIC_COMPUTER_AND_OPTICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.USED_FOR_MANUFACTURING_INDUSTRY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.USED_FOR_AGRICULTURE_FORESTRY_AND_FISHERY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.USED_FOR_CONSTRUCTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.COKE_AND_REFINED_PETROLEUM_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.HARD_COAL_AND_LIGNITE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.ELECTRICAL_EQUIPMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.PHARMACEUTICALS_AND_MEDICINAL_CHEMICALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.MOTOR_VEHICLES_AND_TRAILERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.ELECTRICITY_GAS_STEAM_AND_AIR_CONDITIONING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPI.Column.BEVERAGES_AND_TOBACCO.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.IPI.primary_key,
                )
                # fmt: on

                # XPI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.XPI.name,
                    columns=[
                        Column(name=Table.XPI.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.XPI.Column.MONTH.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.XPI.Column.ANIMAL_FEED_AND_RAW_MATERIALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.AQUATIC_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CAMERAS_CAMCORDERS_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CASHEW_NUTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CASSAVA_AND_CASSAVA_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CHEMICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CHEMICALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CLINKER_AND_CEMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.COFFEE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CONFECTIONERY_AND_CEREAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.DOMESTIC_ECONOMIC_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.ELECTRICAL_WIRES_AND_CABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.ELECTRONICS_COMPUTERS_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.FOOTWEAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.FOREIGN_INVESTED_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.FOREIGN_CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.FURNITURE_PRODUCTS_FROM_MATERIALS_OTHER_THAN_WOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.GLASS_AND_GLASS_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.HANDBAGS_WALLETS_SUITCASES_HATS_UMBRELLAS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.IRON_AND_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.IRON_AND_STEEL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.MACHINERY_EQUIPMENT_TOOLS_SPARE_PARTS_OTHER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.MAIN_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.OTHER_BASE_METALS_AND_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.OTHER_GOODS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.PAPER_AND_PAPER_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.PEPPER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.PETROLEUM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.PHONES_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.PLASTIC_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.RAW_PLASTICS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.RICE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.RUBBER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.RUBBER_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.TEA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.TEXTILE_FIBERS_YARNS_OF_ALL_KINDS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.TEXTILE_GARMENT_LEATHER_FOOTWEAR_RAW_MATERIALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.TEXTILES_GARMENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.TOTAL_VALUE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.TOYS_SPORTS_EQUIPMENT_AND_PARTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.TRANSPORTATION_VEHICLES_AND_SPARE_PARTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.VEGETABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.XPI.Column.WOOD_AND_WOOD_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.XPI.primary_key,
                )
                # fmt: on

                # MPI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.MPI.name,
                    columns=[
                        Column(name=Table.MPI.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.MPI.Column.MONTH.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.MPI.Column.ANIMAL_FEED_AND_RAW_MATERIALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.AQUATIC_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CAMERAS_CAMCORDERS_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CASHEW_NUTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CASSAVA_AND_CASSAVA_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CHEMICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CHEMICALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CLINKER_AND_CEMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.COFFEE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CONFECTIONERY_AND_CEREAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.DOMESTIC_ECONOMIC_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.ELECTRICAL_WIRES_AND_CABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.ELECTRONICS_COMPUTERS_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.FOOTWEAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.FOREIGN_INVESTED_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.FOREIGN_CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.FURNITURE_PRODUCTS_FROM_MATERIALS_OTHER_THAN_WOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.GLASS_AND_GLASS_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.HANDBAGS_WALLETS_SUITCASES_HATS_UMBRELLAS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.IRON_AND_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.IRON_AND_STEEL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.MACHINERY_EQUIPMENT_TOOLS_SPARE_PARTS_OTHER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.MAIN_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.OTHER_BASE_METALS_AND_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.OTHER_GOODS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.PAPER_AND_PAPER_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.PEPPER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.PETROLEUM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.PHONES_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.PLASTIC_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.RAW_PLASTICS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.RICE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.RUBBER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.RUBBER_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.TEA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.TEXTILE_FIBERS_YARNS_OF_ALL_KINDS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.TEXTILE_GARMENT_LEATHER_FOOTWEAR_RAW_MATERIALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.TEXTILES_GARMENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.TOTAL_VALUE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.TOYS_SPORTS_EQUIPMENT_AND_PARTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.TRANSPORTATION_VEHICLES_AND_SPARE_PARTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.VEGETABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MPI.Column.WOOD_AND_WOOD_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.MPI.primary_key,
                )
                # fmt: on

                # POPULATION
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.POPULATION.name,
                    columns=[
                        Column(name=Table.POPULATION.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.POPULATION.Column.POPULATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.POPULATION.Column.POPULATION_AREA_URBAN_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.POPULATION.Column.POPULATION_DENSITY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.POPULATION.Column.POPULATION_GROWTH_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.POPULATION.primary_key,
                )
                # fmt: on

                # LABOR
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.LABOR.name,
                    columns=[
                        Column(name=Table.LABOR.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.LABOR.Column.AGRICULTURE_FORESTRY_AND_FISHERY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.EMPLOYED_AMOUNT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.FEMALE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.INDUSTRY_CONSTRUCTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.LABOR_FORCE_ANNUAL_CHANGE_PERCENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.LABOR_FORCE_PARTICIPATION_RATE_PERCENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.MALE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.UNEMPLOYED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.LABOR.Column.URBAN_UNEMPLOYMENT_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.LABOR.primary_key,
                )
                # fmt: on

                # RETAIL
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.RETAIL.name,
                    columns=[
                        Column(name=Table.RETAIL.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.RETAIL.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.RETAIL.Column.ACCOMMODATION_AND_CATERING_SERVICE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.RETAIL.Column.RETAIL_GROWTH.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.RETAIL.Column.RETAIL_SALE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.RETAIL.Column.SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.RETAIL.Column.TRAVELING_SERVICE.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.RETAIL.primary_key,
                )
                # fmt: on

                # PMI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.PMI.name,
                    columns=[
                        Column(name=Table.PMI.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.PMI.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.PMI.Column.PMI.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.PMI.primary_key,
                )
                # fmt: on

                # IIP
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.IIP.name,
                    columns=[
                        Column(name=Table.IIP.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IIP.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IIP.Column.APPAREL_MANUFACTURING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.BEVERAGE_PRODUCTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.COAL_AND_LIGNITE_MINING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.CRUDE_OIL_AND_NATURAL_GAS_EXTRACTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.ENTIRE_INDUSTRIAL_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.FOOD_PRODUCTION_AND_PROCESSING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.LEATHER_AND_RELATED_PRODUCT_MANUFACTURING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_CHEMICALS_AND_CHEMICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_COKE_AND_REFINED_PETROLEUM_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_ELECTRICAL_EQUIPMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_ELECTRONIC_PRODUCTS_COMPUTERS_AND_OPTICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_FABRICATED_METAL_PRODUCTS_EXCLUDING_MACHINERY_AND_EQUIPMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_FURNITURE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_MACHINERY_AND_EQUIPMENT_NOT_ELSEWHERE_CLASSIFIED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_METALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_MOTOR_VEHICLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_OTHER_NON_METALLIC_MINERAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_OTHER_TRANSPORT_EQUIPMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_PHARMACEUTICALS_MEDICINAL_CHEMICALS_AND_BOTANICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURE_OF_RUBBER_AND_PLASTIC_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MANUFACTURING_INDUSTRY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.METAL_ORE_MINING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MINING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.MINING_SUPPORT_SERVICE_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.OTHER_MANUFACTURING_INDUSTRIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.OTHER_MINING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.PAPER_AND_PAPER_PRODUCT_MANUFACTURING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.PRINTING_AND_REPRODUCTION_OF_RECORDED_MEDIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.PRODUCTION_AND_DISTRIBUTION_OF_ELECTRICITY_GAS_HOT_WATER_STEAM_AND_AIR_CONDITIONING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.REPAIR_MAINTENANCE_AND_INSTALLATION_OF_MACHINERY_AND_EQUIPMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.TEXTILE_MANUFACTURING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.TOBACCO_PRODUCT_MANUFACTURING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.WASTE_COLLECTION_TREATMENT_AND_DISPOSAL_ACTIVITIES_RECYCLING_OF_WASTE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.WASTEWATER_COLLECTION_AND_TREATMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.WATER_COLLECTION_TREATMENT_AND_SUPPLY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIP.Column.WATER_SUPPLY_WASTE_MANAGEMENT_AND_TREATMENT_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.IIP.primary_key,
                )
                # fmt: on

                # IPV
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.IPV.name,
                    columns=[
                        Column(name=Table.IPV.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IPV.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IPV.Column.ALUMINIUM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.ANIMAL_FEED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.AQUATIC_FEED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.BEER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.CARS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.CASUAL_CLOTHES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.CEMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.CHEMICAL_PAINTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.CIGARETTES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.COAL_CLEAN_COAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.COMMERCIAL_TAP_WATER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.ELECTRICITY_PRODUCED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.EXTRACTED_CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.FRESH_MILK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.GASOLINE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.GRANULATED_SUGAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.IRON_CRUDE_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.LEATHER_SHOES_AND_SANDALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.LIQUIDIZED_GAS_LPG.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.MOBILE_PHONES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.MONONATRI_GLUTAMAT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.MOTORCYCLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.NPK_MIXED_FERTILIZERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.NATURAL_FABRICS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.NATURAL_GAS_AIR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.PHONE_ACCESSORIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.POWDERED_MILK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.PROCESSED_SEAFOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.ROLLED_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.STEEL_BARS_ANGLE_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.SYNTHETIC_FABRICS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.TELEVISION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IPV.Column.UREA_FERTILIZER.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.IPV.primary_key,
                )
                # fmt: on

                # MIP
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.MIP.name,
                    columns=[
                        Column(name=Table.MIP.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.MIP.Column.AIR_CONDITIONERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.ANIMAL_AND_POULTRY_FEED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.ANTIMONY_ORE_AND_ANTIMONY_CONCENTRATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.APATITE_ORE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.AQUACULTURE_FEED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.ASSEMBLED_CARS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.ASSEMBLED_MOTORCYCLES_AND_MOPEDS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.ASSEMBLED_TVS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.BATH_MILK_AND_FACIAL_CLEANSER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.BEER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CANNED_FRUITS_AND_NUTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CANNED_MEAT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CANNED_SEAFOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CANNED_VEGETABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CAR_AND_TRACTOR_TIRES_INFLATABLE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CAST_OR_OTHER_ROUGH_IRON_AND_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CASUAL_CLOTHING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CEMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CHEMICAL_FERTILIZERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CLEAN_COAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.COMMERCIAL_TAP_WATER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.COPPER_ORE_AND_COPPER_CONCENTRATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.CRUDE_OIL_EXTRACTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.DIGITAL_CAMERAS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.DOMESTIC_CERAMICS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.DOMESTIC_CRUDE_OIL_EXTRACTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.EXTRACTED_STONE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FABRIC.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FABRIC_SHOES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FIBER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FIBER_CEMENT_ROOFING_SHEETS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FIRED_BRICKS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FIRED_TILES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FISH_SAUCE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FRESH_MILK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.FROZEN_SEAFOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.GENERATED_ELECTRICITY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.GRANULATED_SUGAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.GRAVEL_AND_PEBBLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.GROUND_COFFEE_AND_INSTANT_COFFEE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.HERBICIDES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.HOUSEHOLD_ELECTRIC_FANS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.HOUSEHOLD_REFRIGERATORS_AND_FREEZERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.HOUSEHOLD_WASHING_MACHINES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.IRON_ORE_AND_IRON_CONCENTRATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.LANDLINE_PHONES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.LAUNDRY_DETERGENT_AND_CLEANING_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.LEATHER_SHOES_AND_BOOTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.LIGHT_BULBS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.MILLED_RICE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.MINERAL_WATER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.MOBILE_PHONES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.MOTORCYCLE_AND_BICYCLE_TIRES_INFLATABLE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.MSG_MONOSODIUM_GLUTAMATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.NATURAL_GAS_IN_GAS_FORM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.NPK_FERTILIZERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.PAPER_AND_CARDBOARD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.PESTICIDES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.PLASTIC_PACKAGING_AND_BAGS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.POWDERED_MILK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.PRINTED_NEWSPAPERS_AND_OTHER_PRINTING_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.PRINTERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.PROCESSED_TEA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.PURIFIED_WATER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.REFINED_VEGETABLE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.ROLLED_STEEL_AND_SHAPED_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.SANITARY_WARE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.SAWN_TIMBER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.SEA_SALT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.SHAMPOO_AND_CONDITIONER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.SPIRITS_AND_WHITE_WINE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.SPORTS_SHOES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.STANDARD_BATTERIES_1_5V.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.THRESHING_MACHINES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.TITANIUM_ORE_AND_TITANIUM_CONCENTRATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.TOBACCO.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.TOOTHPASTE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.TUBES_FOR_BICYCLES_AND_MOTORCYCLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.TUBES_FOR_CARS_AND_AIRCRAFT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.VARIOUS_TYPES_OF_BATTERIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.VARIOUS_TYPES_OF_BICYCLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.VARIOUS_TYPES_OF_SAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MIP.Column.YELLOW_PHOSPHORUS.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.MIP.primary_key,
                )
                # fmt: on

                # FA_BY_HOUSE_TYPES
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.FA_BY_HOUSE_TYPES.name,
                    columns=[
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column._16_20_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column._21_25_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column._26_FLOORS_AND_ABOVE.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column._5_FLOORS_AND_BELOW.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column._6_8_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column._9_15_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column.APARTMENT_BUILDINGS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column.SINGLE_FAMILY_HOMES.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column.SINGLE_FAMILY_HOMES_4_FLOORS_AND_ABOVE.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column.SINGLE_FAMILY_HOMES_BELOW_4_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column.TOTAL.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.FA_BY_HOUSE_TYPES.Column.VILLAS.value, data_type=DataType.INT(), nullable=True),
                    ],
                    primary_keys=Table.FA_BY_HOUSE_TYPES.primary_key,
                )
                # fmt: on

                # IT_BOP
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.IT_BOP.name,
                    columns=[
                        Column(name=Table.IT_BOP.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IT_BOP.Column.QUARTER.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.A_CURRENT_ACCOUNT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.BORROWING_AND_EXTERNAL_DEBT_REPAYMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.C_FINANCIAL_ACCOUNT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.CAPITAL_WITHDRAWAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.CURRENT_TRANSFERS_SECONDARY_INCOME_NET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.CURRENT_TRANSFERS_SECONDARY_INCOME_PAYMENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.CURRENT_TRANSFERS_SECONDARY_INCOME_RECEIPTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.D_ERRORS_AND_OMISSIONS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.DIRECT_INVESTMENT_ABROAD_ASSETS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.DIRECT_INVESTMENT_IN_VIETNAM_LIABILITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.DIRECT_INVESTMENT_NET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.E_OVERALL_BALANCE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.F_RESERVES_AND_RELATED_ITEMS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.FINANCIAL_INSTITUTIONS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.GOODS_EXPORTS_FOB.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.GOODS_IMPORTS_FOB.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.GOODS_NET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.GOVERNMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.INVESTMENT_INCOME_PRIMARY_INCOME_NET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.INVESTMENT_INCOME_PRIMARY_INCOME_PAYMENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.INVESTMENT_INCOME_PRIMARY_INCOME_RECEIPTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.LONG_TERM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.MONEY_AND_DEPOSITS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.OTHER_INVESTMENT_ASSETS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.OTHER_INVESTMENT_LIABILITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.OTHER_INVESTMENT_NET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.OTHER_RECEIVABLESPAYABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.PORTFOLIO_INVESTMENT_ABROAD_ASSETS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.PORTFOLIO_INVESTMENT_IN_VIETNAM_LIABILITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.PORTFOLIO_INVESTMENT_NET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.PRINCIPAL_REPAYMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.PRIVATE_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.RESERVE_ASSETS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.RESIDENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.SERVICES_EXPORTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.SERVICES_IMPORTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.SERVICES_NET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.SHORT_TERM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IT_BOP.Column.TOTAL_CURRENT_AND_CAPITAL_ACCOUNT_BALANCE.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.IT_BOP.primary_key,
                )
                # fmt: on

                # TSBR
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.TSBR.name,
                    columns=[
                        Column(name=Table.TSBR.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.TSBR.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.TSBR.Column.AGRICULTURAL_LAND_USE_TAX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.AID_REVENUE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.DOMESTIC_REVENUE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.ENVIRONMENTAL_PROTECTION_TAX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.ENVIRONMENTAL_PROTECTION_TAX_ON_IMPORTED_GOODS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.EXPORT_TAX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.FEES_AND_CHARGES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.IMPORT_TAX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.NON_AGRICULTURAL_LAND_USE_TAX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.OTHER_BUDGET_REVENUES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.OTHER_REVENUE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.PERSONAL_INCOME_TAX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.RECOVERY_OF_CAPITAL_DIVIDENDS_POST_TAX_PROFITS_SURPLUS_REVENUE_AND_EXPENDITURE_OF_THE_STATE_BANK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_BALANCE_FROM_IMPORT_EXPORT_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_FOREIGN_INVESTED_ENTERPRISES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_HOUSING_AND_LAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_LAND_AND_WATER_SURFACE_LEASING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_LAND_USE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_LEASING_AND_SALE_OF_STATE_OWNED_HOUSING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_LOTTERY_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_MINING_RIGHTS_LICENSING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_NON_STATE_ECONOMIC_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_PUBLIC_LAND_FUNDS_AND_OTHER_PUBLIC_ASSET_BENEFITS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.REVENUE_FROM_STATE_OWNED_ENTERPRISES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.SPECIAL_CONSUMPTION_TAX_ON_IMPORTED_GOODS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.TOTAL_REVENUE_FROM_IMPORT_EXPORT_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.TOTAL_STATE_BUDGET_REVENUE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.VALUE_ADDED_TAX_ON_IMPORTED_GOODS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBR.Column.VALUE_ADDED_TAX_REFUND.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.TSBR.primary_key,
                )
                # fmt: on

                # TSBE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.TSBE.name,
                    columns=[
                        Column(name=Table.TSBE.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.TSBE.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.TSBE.Column.AID_EXPENDITURE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.DEBT_INTEREST_PAYMENT_EXPENDITURE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.DEVELOPMENT_INVESTMENT_EXPENDITURE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.EXPENDITURE_FOR_EDUCATION_TRAINING_AND_VOCATIONAL_EDUCATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.EXPENDITURE_FOR_SCIENCE_AND_TECHNOLOGY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.EXPENDITURE_FOR_WAGE_REFORM_AND_STREAMLINING_PERSONNEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.REGULAR_EXPENDITURE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.STATE_BUDGET_CONTINGENCY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.SUPPLEMENTARY_EXPENDITURE_FOR_FINANCIAL_RESERVE_FUND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TSBE.Column.TOTAL_STATE_BUDGET_EXPENDITURE.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.TSBE.primary_key,
                )
                # fmt: on

                # GD
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.GD.name,
                    columns=[
                        Column(name=Table.GD.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.GD.Column.DEBT_BALANCE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GD.Column.DOMESTIC_DEBT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GD.Column.FOREIGN_DEBT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GD.Column.TOTAL_DEBT_PAYMENTS_DURING_THE_PERIOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GD.Column.TOTAL_INTEREST_AND_FEES_PAID_DURING_THE_PERIOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GD.Column.TOTAL_PRINCIPAL_REPAYMENT_DURING_THE_PERIOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.GD.Column.WITHDRAWALS_DURING_THE_PERIOD.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.GD.primary_key,
                )
                # fmt: on

                # BRD
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.BRD.name,
                    columns=[
                        Column(name=Table.BRD.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.BRD.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.BRD.Column.ENTERPRISES_COMPLETING_DISSOLUTION.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.BRD.Column.ENTERPRISES_RESUMING_OPERATIONS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.BRD.Column.ENTERPRISES_TEMPORARILY_SUSPENDED_AWAITING_DISSOLUTION.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.BRD.Column.NEWLY_ESTABLISHED_ENTERPRISES.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.BRD.Column.REGISTERED_CAPITAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.BRD.Column.REGISTERED_LABOR.value, data_type=DataType.INT(), nullable=True),
                    ],
                    primary_keys=Table.BRD.primary_key,
                )
                # fmt: on

                # IISD
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.IISD.name,
                    columns=[
                        Column(name=Table.IISD.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IISD.Column.QUARTER.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IISD.Column.FOREIGN_DIRECT_INVESTMENT_CAPITAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.GOVERNMENT_BOND_CAPITAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.INVESTMENT_CAPITAL_FROM_RESIDENTS_AND_PRIVATE_INDIVIDUALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.INVESTMENT_CAPITAL_FROM_THE_STATE_BUDGET.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.INVESTMENT_CAPITAL_OF_STATE_ENTERPRISES_EQUITY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.LOANS_FROM_OTHER_SOURCES_OF_THE_STATE_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.OTHER_MOBILIZED_CAPITAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.PLANNED_STATE_INVESTMENT_CREDIT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IISD.Column.TOTAL.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.IISD.primary_key,
                )
                # fmt: on

                # TREG
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.TREG.name,
                    columns=[
                        Column(name=Table.TREG.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.TREG.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.TREG.Column.INTERNATIONAL_LIQUIDITY_TOTAL_RESERVES_EXCLUDING_GOLD_FOREIGN_EXCHANGE_US_DOLLARS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.TREG.Column.INTERNATIONAL_LIQUIDITY_TOTAL_RESERVES_EXCLUDING_GOLD_US_DOLLARS.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.TREG.primary_key,
                )
                # fmt: on

                # CREDIT
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.CREDIT.name,
                    columns=[
                        Column(name=Table.CREDIT.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.CREDIT.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.CREDIT.Column.CREDIT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CREDIT.Column.CREDIT_GROWTH_YTD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CREDIT.Column.MONEY_SUPPLY_GROWTH_M2_YTD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.CREDIT.Column.MONEY_SUPPLY_M2.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.CREDIT.primary_key,
                )
                # fmt: on

                # MOBILIZATION
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.MOBILIZATION.name,
                    columns=[
                        Column(name=Table.MOBILIZATION.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.MOBILIZATION.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.MOBILIZATION.Column.DEPOSITS_FROM_ECONOMIC_ORGANIZATIONS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MOBILIZATION.Column.DEPOSITS_FROM_RESIDENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.MOBILIZATION.Column.TOTAL_PAYMENT_INSTRUMENTS.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.MOBILIZATION.primary_key,
                )
                # fmt: on

                # G_EXCHANGE_RATE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_EXCHANGE_RATE.name,
                    columns = [
                        Column(name=Table.G_EXCHANGE_RATE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_EXCHANGE_RATE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.G_EXCHANGE_RATE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_EXCHANGE_RATE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_EXCHANGE_RATE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_EXCHANGE_RATE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_EXCHANGE_RATE.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.G_EXCHANGE_RATE.primary_key,
                )
                # fmt: on

                # IIR
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.IIR.name,
                    columns=[
                        Column(name=Table.IIR.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IIR.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IIR.Column.DAY.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IIR.Column.ONE_MONTH.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIR.Column.ONE_WEEK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIR.Column.TWO_WEEKS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIR.Column.THREE_MONTHS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIR.Column.SIX_MONTHS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIR.Column.NINE_MONTHS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IIR.Column.OVERNIGHT.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.IIR.primary_key,
                )
                # fmt: on

                # RRRR
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.RRRR.name,
                    columns=[
                        Column(name=Table.RRRR.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.RRRR.Column.DISCOUNT_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.RRRR.Column.REFINANCING_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=[Table.RRRR.Column.DATE.value],
                )
                # fmt: on

                # FDI_SECTOR
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.FDI_SECTOR.name,
                    columns=[
                        Column(name=Table.FDI_SECTOR.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.FDI_SECTOR.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.FDI_SECTOR.Column.ACCOMMODATION_AND_FOOD_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.ADMINISTRATIVE_AND_SUPPORT_SERVICE_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.AGRICULTURE_FORESTRY_AND_FISHERY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.ARTS_ENTERTAINMENT_AND_RECREATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.CONSTRUCTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.DOMESTIC_HOUSEHOLD_SERVICE_WORKERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.EDUCATION_AND_TRAINING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.FINANCIAL_BANKING_AND_INSURANCE_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.HEALTHCARE_AND_SOCIAL_ASSISTANCE_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.INFORMATION_AND_COMMUNICATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.MANUFACTURING_AND_PROCESSING_INDUSTRY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.MINING_AND_QUARRYING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.OTHER_SERVICE_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.PRODUCTION_AND_DISTRIBUTION_OF_ELECTRICITY_GAS_WATER_AND_AIR_CONDITIONING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.PROFESSIONAL_SCIENTIFIC_AND_TECHNOLOGICAL_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.REAL_ESTATE_BUSINESS_ACTIVITIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.TRANSPORTATION_AND_WAREHOUSING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.WATER_SUPPLY_AND_WASTE_TREATMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_SECTOR.Column.WHOLESALE_AND_RETAIL_REPAIR_OF_MOTOR_VEHICLES_AND_MOTORCYCLES.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.FDI_SECTOR.primary_key,
                )
                # fmt: on

                # FDI_RD
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.FDI_RD.name,
                    columns=[
                        Column(name=Table.FDI_RD.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.FDI_RD.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.FDI_RD.Column.FDI_DISBURSEMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.FDI_RD.Column.REGISTER.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.FDI_RD.primary_key,
                )
                # fmt: on

                # EXPORT
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.EXPORT.name,
                    columns=[
                        Column(name=Table.EXPORT.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.EXPORT.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.EXPORT.Column.ARGENTINA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.ASEAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.POLAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.BELARUS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.BRAZIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.BULGARIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.BELGIUM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.PORTUGAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.IVORY_COAST.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CAMEROON.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CAMBODIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CANADA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CHILE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CROATIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.UNITED_ARAB_EMIRATES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.ESTONIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.EU.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.HUNGARY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.GREECE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.NETHERLANDS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SOUTH_KOREA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.HONG_KONG.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.INDONESIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.IRELAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.ISRAEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.KAZAKHSTAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.KUWAIT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.LATVIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.LITHUANIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.LUXEMBOURG.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.LAOS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.MALAYSIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.MALTA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.MEXICO.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.MYANMAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.USA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.NORWAY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SOUTH_AFRICA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.NEW_ZEALAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.RUSSIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.BRUNEI_DARUSSALAM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.JAPAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.OTHER_COUNTRIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.PAKISTAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.PERU.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.PHILIPPINES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.FRANCE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.FINLAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.ROMANIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SENEGAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SINGAPORE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SLOVAKIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SLOVENIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CZECHIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CYPRUS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.THAILAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.TURKEY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SWITZERLAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SWEDEN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.CHINA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SPAIN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.UKRAINE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.UNITED_KINGDOM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.AUSTRIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.AUSTRALIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.ITALY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.DENMARK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.TAIWAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.GERMANY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.SAUDI_ARABIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.EXPORT.Column.INDIA.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.EXPORT.primary_key,
                )
                # fmt: on

                # IMPORT
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.IMPORT.name,
                    columns=[
                        Column(name=Table.IMPORT.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IMPORT.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.IMPORT.Column.ARGENTINA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.ASEAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.POLAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.BELARUS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.BRAZIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.BULGARIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.BELGIUM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.PORTUGAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.IVORY_COAST.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CAMEROON.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CAMBODIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CANADA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CHILE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CROATIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.UNITED_ARAB_EMIRATES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.ESTONIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.EU.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.HUNGARY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.GREECE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.NETHERLANDS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SOUTH_KOREA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.HONG_KONG.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.INDONESIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.IRELAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.ISRAEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.KAZAKHSTAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.KUWAIT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.LATVIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.LITHUANIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.LUXEMBOURG.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.LAOS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.MALAYSIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.MALTA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.MEXICO.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.MYANMAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.USA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.NORWAY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SOUTH_AFRICA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.NEW_ZEALAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.RUSSIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.BRUNEI_DARUSSALAM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.JAPAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.OTHER_COUNTRIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.PAKISTAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.PERU.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.PHILIPPINES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.FRANCE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.FINLAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.ROMANIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SENEGAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SINGAPORE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SLOVAKIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SLOVENIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CZECHIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CYPRUS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.THAILAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.TURKEY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SWITZERLAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SWEDEN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.CHINA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SPAIN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.UKRAINE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.UNITED_KINGDOM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.AUSTRIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.AUSTRALIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.ITALY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.DENMARK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.TAIWAN.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.GERMANY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.SAUDI_ARABIA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.IMPORT.Column.INDIA.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.IMPORT.primary_key,
                )
                # fmt: on

                # GOLD_PRICE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.GOLD_PRICE.name,
                    columns = [
                        Column(name=Table.GOLD_PRICE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.GOLD_PRICE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.GOLD_PRICE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.GOLD_PRICE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.GOLD_PRICE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.GOLD_PRICE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.GOLD_PRICE.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.GOLD_PRICE.primary_key,
                )
                # fmt: on
                
                # OIL_PRICE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.OIL_PRICE.name,
                    columns = [
                        Column(name=Table.OIL_PRICE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.OIL_PRICE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.OIL_PRICE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.OIL_PRICE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.OIL_PRICE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.OIL_PRICE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.OIL_PRICE.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.OIL_PRICE.primary_key,
                )
                # fmt: on

                # DOW_JONES
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.DOW_JONES.name,
                    columns = [
                        Column(name=Table.DOW_JONES.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.DOW_JONES.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.DOW_JONES.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.DOW_JONES.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.DOW_JONES.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.DOW_JONES.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.DOW_JONES.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.DOW_JONES.primary_key,
                )
                # fmt: on
                
                # NYSE_COMPOSITE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.NYSE_COMPOSITE.name,
                    columns = [
                        Column(name=Table.NYSE_COMPOSITE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.NYSE_COMPOSITE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NYSE_COMPOSITE.Column.ADJ_CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NYSE_COMPOSITE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NYSE_COMPOSITE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NYSE_COMPOSITE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NYSE_COMPOSITE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.NYSE_COMPOSITE.primary_key,
                )
                # fmt: on

                # SNP_500
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.SNP_500.name,
                    columns = [
                        Column(name=Table.SNP_500.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.SNP_500.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.SNP_500.Column.ADJ_CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.SNP_500.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.SNP_500.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.SNP_500.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.SNP_500.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.SNP_500.primary_key,
                )
                # fmt: on
                
                # NASDAQ_COMPOSITE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.NASDAQ_COMPOSITE.name,
                    columns = [
                        Column(name=Table.NASDAQ_COMPOSITE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.NASDAQ_COMPOSITE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.NASDAQ_COMPOSITE.Column.ADJ_CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.NASDAQ_COMPOSITE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NASDAQ_COMPOSITE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NASDAQ_COMPOSITE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NASDAQ_COMPOSITE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.NASDAQ_COMPOSITE.primary_key,
                )
                # fmt: on
                
                # NASDAQ_100
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.NASDAQ_100.name,
                    columns = [
                        Column(name=Table.NASDAQ_100.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.NASDAQ_100.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.NASDAQ_100.Column.ADJ_CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.NASDAQ_100.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NASDAQ_100.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NASDAQ_100.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.NASDAQ_100.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.NASDAQ_100.primary_key,
                )
                # fmt: on

            case DataQuality.GOLD:
                # G_GDP
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_GDP.name,
                    columns=[
                        Column(name=Table.G_GDP.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_GDP.Column.GDP_GROWTH.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.G_GDP.primary_key,
                )
                # fmt: on

                # G_CPI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_CPI.name,
                    columns = [
                        Column(name=Table.G_CPI.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_CPI.Column.BEVERAGE_AND_CIGARETTE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_CPI.Column.CONSUMER_PRICE_INDEX.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_CPI.Column.CULTURE_ENTERTAINMENT_AND_TOURISM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_CPI.Column.EATING_OUTSIDE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_CPI.Column.EDUCATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_CPI.Column.FOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_CPI.Column.FOOD_AND_FOODSTUFF.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_CPI.Column.FOODSTUFF.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_CPI.Column.GARMENT_FOOTWEAR_HAT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_CPI.Column.HOUSEHOLD_APPLIANCES_AND_GOODS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_CPI.Column.HOUSING_AND_CONSTRUCTION_MATERIALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_CPI.Column.MEDICINE_AND_HEALTH_CARE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_CPI.Column.OTHER_GOODS_AND_SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_CPI.Column.POSTAL_SERVICES_AND_TELECOMMUNICATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_CPI.Column.TRAFFIC.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.G_CPI.primary_key,
                )
                # fmt: on

                # G_PPI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_PPI.name,
                    columns = [
                        Column(name=Table.G_PPI.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_PPI.Column.GENERAL_INDEX.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_PPI.Column.FORESTRY_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_PPI.Column.AGRICULTURAL_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_PPI.Column.FORESTRY_AND_RELATED_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_PPI.Column.EXPLOITED_FOREST_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_PPI.Column.COLLECTED_FOREST_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_PPI.Column.AGRICULTURE_AND_RELATED_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_PPI.Column.LIVESTOCK_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_PPI.Column.ANNUAL_CROP_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_PPI.Column.PERENNIAL_CROP_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_PPI.Column.EXPLOITED_AQUATIC_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_PPI.Column.AQUATIC_PRODUCTS_EXPLOITATION_AND_FARMING.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_PPI.Column.AQUATIC_FARMING_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_PPI.Column.FOREST_PLANTING_AND_CARE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.G_PPI.primary_key,
                )
                # fmt: on

                # G_XPI
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_XPI.name,
                    columns=[
                        Column(name=Table.G_XPI.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_XPI.Column.ANIMAL_FEED_AND_RAW_MATERIALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.AQUATIC_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.CAMERAS_CAMCORDERS_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.CASHEW_NUTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.CASSAVA_AND_CASSAVA_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.CHEMICAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.CHEMICALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.CLINKER_AND_CEMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.COFFEE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.CONFECTIONERY_AND_CEREAL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.DOMESTIC_ECONOMIC_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.ELECTRICAL_WIRES_AND_CABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.ELECTRONICS_COMPUTERS_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.FOOTWEAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.FOREIGN_INVESTED_SECTOR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.FOREIGN_CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.FURNITURE_PRODUCTS_FROM_MATERIALS_OTHER_THAN_WOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.GLASS_AND_GLASS_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.HANDBAGS_WALLETS_SUITCASES_HATS_UMBRELLAS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.IRON_AND_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.IRON_AND_STEEL_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.MACHINERY_EQUIPMENT_TOOLS_SPARE_PARTS_OTHER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.MAIN_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.OTHER_BASE_METALS_AND_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.OTHER_GOODS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.PAPER_AND_PAPER_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.PEPPER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.PETROLEUM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.PHONES_AND_COMPONENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.PLASTIC_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.RAW_PLASTICS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.RICE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.RUBBER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.RUBBER_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.TEA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.TEXTILE_FIBERS_YARNS_OF_ALL_KINDS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.TEXTILE_GARMENT_LEATHER_FOOTWEAR_RAW_MATERIALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.TEXTILES_GARMENTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.TOTAL_VALUE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.TOYS_SPORTS_EQUIPMENT_AND_PARTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.TRANSPORTATION_VEHICLES_AND_SPARE_PARTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.VEGETABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_XPI.Column.WOOD_AND_WOOD_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.G_XPI.primary_key,
                )
                # fmt: on

                # G_POPULATION
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_POPULATION.name,
                    columns=[
                        Column(name=Table.G_POPULATION.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_POPULATION.Column.POPULATION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_POPULATION.Column.POPULATION_AREA_URBAN_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_POPULATION.Column.POPULATION_DENSITY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_POPULATION.Column.POPULATION_GROWTH_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.G_POPULATION.primary_key,
                )
                # fmt: on

                # G_LABOR
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_LABOR.name,
                    columns=[
                        Column(name=Table.G_LABOR.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_LABOR.Column.AGRICULTURE_FORESTRY_AND_FISHERY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_LABOR.Column.EMPLOYED_AMOUNT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_LABOR.Column.FEMALE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_LABOR.Column.INDUSTRY_CONSTRUCTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_LABOR.Column.LABOR_FORCE_ANNUAL_CHANGE_PERCENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_LABOR.Column.LABOR_FORCE_PARTICIPATION_RATE_PERCENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_LABOR.Column.MALE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_LABOR.Column.SERVICES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_LABOR.Column.UNEMPLOYED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_LABOR.Column.URBAN_UNEMPLOYMENT_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.G_LABOR.primary_key,
                )
                # fmt: on

                # G_IPV
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_IPV.name,
                    columns=[
                        Column(name=Table.G_IPV.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_IPV.Column.ALUMINIUM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.ANIMAL_FEED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.AQUATIC_FEED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.BEER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.CARS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.CASUAL_CLOTHES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.CEMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.CHEMICAL_PAINTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.CIGARETTES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.COAL_CLEAN_COAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.COMMERCIAL_TAP_WATER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.ELECTRICITY_PRODUCED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.EXTRACTED_CRUDE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.FRESH_MILK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.GASOLINE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.GRANULATED_SUGAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.IRON_CRUDE_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.LEATHER_SHOES_AND_SANDALS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.LIQUIDIZED_GAS_LPG.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.MOBILE_PHONES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.MONONATRI_GLUTAMAT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.MOTORCYCLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.NPK_MIXED_FERTILIZERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.NATURAL_FABRICS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.NATURAL_GAS_AIR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.PHONE_ACCESSORIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.POWDERED_MILK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.PROCESSED_SEAFOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.ROLLED_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.STEEL_BARS_ANGLE_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.SYNTHETIC_FABRICS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.TELEVISION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_IPV.Column.UREA_FERTILIZER.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.G_IPV.primary_key,
                )
                # fmt: on

                # G_MIP
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_MIP.name,
                    columns=[
                        Column(name=Table.G_MIP.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_MIP.Column.AIR_CONDITIONERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.ANIMAL_AND_POULTRY_FEED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.ANTIMONY_ORE_AND_ANTIMONY_CONCENTRATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.APATITE_ORE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.AQUACULTURE_FEED.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.ASSEMBLED_CARS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.ASSEMBLED_MOTORCYCLES_AND_MOPEDS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.ASSEMBLED_TVS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.BATH_MILK_AND_FACIAL_CLEANSER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.BEER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.CANNED_FRUITS_AND_NUTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.CANNED_MEAT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.CANNED_SEAFOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.CANNED_VEGETABLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.CAR_AND_TRACTOR_TIRES_INFLATABLE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.CAST_OR_OTHER_ROUGH_IRON_AND_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.CASUAL_CLOTHING.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.CEMENT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.CHEMICAL_FERTILIZERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.CLEAN_COAL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.COMMERCIAL_TAP_WATER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.COPPER_ORE_AND_COPPER_CONCENTRATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.CRUDE_OIL_EXTRACTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.DIGITAL_CAMERAS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.DOMESTIC_CERAMICS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.DOMESTIC_CRUDE_OIL_EXTRACTION.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.EXTRACTED_STONE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.FABRIC.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.FABRIC_SHOES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.FIBER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.FIBER_CEMENT_ROOFING_SHEETS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.FIRED_BRICKS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.FIRED_TILES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.FISH_SAUCE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.FRESH_MILK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.FROZEN_SEAFOOD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.GENERATED_ELECTRICITY.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.GRANULATED_SUGAR.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.GRAVEL_AND_PEBBLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.GROUND_COFFEE_AND_INSTANT_COFFEE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.HERBICIDES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.HOUSEHOLD_ELECTRIC_FANS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.HOUSEHOLD_REFRIGERATORS_AND_FREEZERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.HOUSEHOLD_WASHING_MACHINES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.IRON_ORE_AND_IRON_CONCENTRATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.LANDLINE_PHONES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.LAUNDRY_DETERGENT_AND_CLEANING_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.LEATHER_SHOES_AND_BOOTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.LIGHT_BULBS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.MILLED_RICE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.MINERAL_WATER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.MOBILE_PHONES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.MOTORCYCLE_AND_BICYCLE_TIRES_INFLATABLE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.MSG_MONOSODIUM_GLUTAMATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.NATURAL_GAS_IN_GAS_FORM.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.NPK_FERTILIZERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.PAPER_AND_CARDBOARD.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.PESTICIDES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.PLASTIC_PACKAGING_AND_BAGS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.POWDERED_MILK.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.PRINTED_NEWSPAPERS_AND_OTHER_PRINTING_PRODUCTS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.PRINTERS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.PROCESSED_TEA.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.PURIFIED_WATER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.REFINED_VEGETABLE_OIL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.ROLLED_STEEL_AND_SHAPED_STEEL.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.SANITARY_WARE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.SAWN_TIMBER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.SEA_SALT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.SHAMPOO_AND_CONDITIONER.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.SPIRITS_AND_WHITE_WINE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.SPORTS_SHOES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.STANDARD_BATTERIES_1_5V.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.THRESHING_MACHINES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.TITANIUM_ORE_AND_TITANIUM_CONCENTRATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.TOBACCO.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.TOOTHPASTE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.TUBES_FOR_BICYCLES_AND_MOTORCYCLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.TUBES_FOR_CARS_AND_AIRCRAFT.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.VARIOUS_TYPES_OF_BATTERIES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.VARIOUS_TYPES_OF_BICYCLES.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.VARIOUS_TYPES_OF_SAND.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_MIP.Column.YELLOW_PHOSPHORUS.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.G_MIP.primary_key,
                )
                # fmt: on

                # G_FA_BY_HOUSE_TYPES
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_FA_BY_HOUSE_TYPES.name,
                    columns=[
                        Column(name=Table.G_FA_BY_HOUSE_TYPES.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_FA_BY_HOUSE_TYPES.Column._16_20_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.G_FA_BY_HOUSE_TYPES.Column._21_25_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.G_FA_BY_HOUSE_TYPES.Column._26_FLOORS_AND_ABOVE.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.G_FA_BY_HOUSE_TYPES.Column._5_FLOORS_AND_BELOW.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.G_FA_BY_HOUSE_TYPES.Column._6_8_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.G_FA_BY_HOUSE_TYPES.Column._9_15_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.G_FA_BY_HOUSE_TYPES.Column.APARTMENT_BUILDINGS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.G_FA_BY_HOUSE_TYPES.Column.SINGLE_FAMILY_HOMES.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.G_FA_BY_HOUSE_TYPES.Column.SINGLE_FAMILY_HOMES_4_FLOORS_AND_ABOVE.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.G_FA_BY_HOUSE_TYPES.Column.SINGLE_FAMILY_HOMES_BELOW_4_FLOORS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.G_FA_BY_HOUSE_TYPES.Column.TOTAL.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.G_FA_BY_HOUSE_TYPES.Column.VILLAS.value, data_type=DataType.INT(), nullable=True),
                    ],
                    primary_keys=Table.G_FA_BY_HOUSE_TYPES.primary_key,
                )
                # fmt: on

                # G_TREG
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_TREG.name,
                    columns=[
                        Column(name=Table.G_TREG.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_TREG.Column.INTERNATIONAL_LIQUIDITY_TOTAL_RESERVES_EXCLUDING_GOLD_FOREIGN_EXCHANGE_US_DOLLARS.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_TREG.Column.INTERNATIONAL_LIQUIDITY_TOTAL_RESERVES_EXCLUDING_GOLD_US_DOLLARS.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=Table.G_TREG.primary_key,
                )
                # fmt: on

                # G_EXCHANGE_RATE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.EXCHANGE_RATE.name,
                    columns = [
                        Column(name=Table.EXCHANGE_RATE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.EXCHANGE_RATE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=False),
                        Column(name=Table.EXCHANGE_RATE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.EXCHANGE_RATE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.EXCHANGE_RATE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.EXCHANGE_RATE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.EXCHANGE_RATE.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.EXCHANGE_RATE.primary_key,
                )
                # fmt: on

                # G_RRRR
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_RRRR.name,
                    columns=[
                        Column(name=Table.G_RRRR.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_RRRR.Column.DISCOUNT_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                        Column(name=Table.G_RRRR.Column.REFINANCING_RATE.value, data_type=DataType.FLOAT(), nullable=True),
                    ],
                    primary_keys=[Table.G_RRRR.Column.DATE.value],
                )
                # fmt: on

                # G_GOLD_PRICE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_GOLD_PRICE.name,
                    columns = [
                        Column(name=Table.G_GOLD_PRICE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_GOLD_PRICE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_GOLD_PRICE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_GOLD_PRICE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_GOLD_PRICE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_GOLD_PRICE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_GOLD_PRICE.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.G_GOLD_PRICE.primary_key,
                )
                # fmt: on

                # G_OIL_PRICE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_OIL_PRICE.name,
                    columns = [
                        Column(name=Table.G_OIL_PRICE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_OIL_PRICE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_OIL_PRICE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_OIL_PRICE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_OIL_PRICE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_OIL_PRICE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_OIL_PRICE.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.G_OIL_PRICE.primary_key,
                )
                # fmt: on

                # G_DOW_JONES
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_DOW_JONES.name,
                    columns = [
                        Column(name=Table.G_DOW_JONES.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_DOW_JONES.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_DOW_JONES.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_DOW_JONES.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_DOW_JONES.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_DOW_JONES.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_DOW_JONES.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.G_DOW_JONES.primary_key,
                )
                # fmt: on

                # G_NYSE_COMPOSITE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_NYSE_COMPOSITE.name,
                    columns = [
                        Column(name=Table.G_NYSE_COMPOSITE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_NYSE_COMPOSITE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NYSE_COMPOSITE.Column.ADJ_CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NYSE_COMPOSITE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NYSE_COMPOSITE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NYSE_COMPOSITE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NYSE_COMPOSITE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.G_NYSE_COMPOSITE.primary_key,
                )
                # fmt: on

                # G_SNP_500
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_SNP_500.name,
                    columns = [
                        Column(name=Table.G_SNP_500.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_SNP_500.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_SNP_500.Column.ADJ_CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_SNP_500.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_SNP_500.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_SNP_500.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_SNP_500.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.G_SNP_500.primary_key,
                )
                # fmt: on
                
                # G_NASDAQ_COMPOSITE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_NASDAQ_COMPOSITE.name,
                    columns = [
                        Column(name=Table.G_NASDAQ_COMPOSITE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_NASDAQ_COMPOSITE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NASDAQ_COMPOSITE.Column.ADJ_CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NASDAQ_COMPOSITE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NASDAQ_COMPOSITE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NASDAQ_COMPOSITE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NASDAQ_COMPOSITE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.G_NASDAQ_COMPOSITE.primary_key,
                )
                # fmt: on
                
                # G_NASDAQ_100
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.MACROECONOMICS.value,
                    table_name=Table.G_NASDAQ_100.name,
                    columns = [
                        Column(name=Table.G_NASDAQ_100.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_NASDAQ_100.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NASDAQ_100.Column.ADJ_CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NASDAQ_100.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NASDAQ_100.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NASDAQ_100.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_NASDAQ_100.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.G_NASDAQ_100.primary_key,
                )
                # fmt: on

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish creating macroeconomics tables for "{data_quality.value}".'
        )

    def _create_stock_market_tables(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start creating stock market tables for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                # MARKET_TABLE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.MARKET.name,
                    columns = [
                        Column(name=Table.MARKET.Column.ID.value, data_type=DataType.SERIAL(), nullable=False),
                        Column(name=Table.MARKET.Column.CODE.value, data_type=DataType.VARCHAR(), nullable=True),
                        Column(name=Table.MARKET.Column.NAME.value, data_type=DataType.VARCHAR(), nullable=True),
                        Column(name=Table.MARKET.Column.SAVE_PROGRESS_YEAR.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.MARKET.Column.CREATE_DATE.value, data_type=DataType.AUTO_TIMESTAMP(), nullable=True),
                        Column(name=Table.MARKET.Column.UPDATE_DATE.value, data_type=DataType.TIMESTAMP(), nullable=True),
                        Column(name=Table.MARKET.Column.DELETE_DATE.value, data_type=DataType.TIMESTAMP(), nullable=True),
                    ],
                    primary_keys=Table.MARKET.primary_key,
                )
                # fmt: on
                
                # B_STOCK_MARKET_PRICE
                # fmt: off
                stock_market_price_table_list = [item.value.lower() for item in StockMarketSubType if "price" in item.value.lower()]

                stock_market_price_column_list = [
                    Column(name=Table.B_STOCK_MARKET_PRICE.Column.CODE.value, data_type=DataType.VARCHAR(), nullable=False),
                    Column(name=Table.B_STOCK_MARKET_PRICE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                    Column(name=Table.B_STOCK_MARKET_PRICE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                    Column(name=Table.B_STOCK_MARKET_PRICE.Column.ADJUST.value, data_type=DataType.DECIMAL(), nullable=True),
                    Column(name=Table.B_STOCK_MARKET_PRICE.Column.CHANGE.value, data_type=DataType.VARCHAR(), nullable=True),
                    Column(name=Table.B_STOCK_MARKET_PRICE.Column.MATCHING_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                    Column(name=Table.B_STOCK_MARKET_PRICE.Column.MATCHING_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                    Column(name=Table.B_STOCK_MARKET_PRICE.Column.NEGOTIATE_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                    Column(name=Table.B_STOCK_MARKET_PRICE.Column.NEGOTIATE_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                    Column(name=Table.B_STOCK_MARKET_PRICE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                    Column(name=Table.B_STOCK_MARKET_PRICE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                    Column(name=Table.B_STOCK_MARKET_PRICE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                ]

                for table_name in stock_market_price_table_list:
                    self._database_driver.create_table(
                        schema_name=Schema.STOCK_MARKET.value,
                        table_name=table_name,
                        columns=stock_market_price_column_list,
                        primary_keys=Table.B_STOCK_MARKET_PRICE.primary_key,
                    )
                # fmt: on

                # B_STOCK_MARKET_ORDER
                # fmt: off
                stock_market_order_table_list = [item.value.lower() for item in StockMarketSubType if "order" in item.value.lower()]

                stock_market_order_column_list = [
                        Column(name=Table.B_STOCK_MARKET_ORDER.Column.CODE.value, data_type=DataType.VARCHAR(), nullable=False),
                        Column(name=Table.B_STOCK_MARKET_ORDER.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.B_STOCK_MARKET_ORDER.Column.CHANGE.value, data_type=DataType.VARCHAR(), nullable=True),
                        Column(name=Table.B_STOCK_MARKET_ORDER.Column.NUMBER_OF_BUY_ORDERS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.B_STOCK_MARKET_ORDER.Column.BUY_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.B_STOCK_MARKET_ORDER.Column.AVERAGE_VOLUME_PER_BUY_ORDER.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.B_STOCK_MARKET_ORDER.Column.NUMBER_OF_SELL_ORDERS.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.B_STOCK_MARKET_ORDER.Column.SELL_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.B_STOCK_MARKET_ORDER.Column.AVERAGE_VOLUME_PER_SELL_ORDER.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.B_STOCK_MARKET_ORDER.Column.NET_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                    ]

                for table_name in stock_market_order_table_list:
                    self._database_driver.create_table(
                        schema_name=Schema.STOCK_MARKET.value,
                        table_name=table_name,
                        columns=stock_market_order_column_list,
                        primary_keys=Table.B_STOCK_MARKET_ORDER.primary_key,
                    )
                # fmt: on

            case DataQuality.SILVER:
                # MARKET
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.MARKET.name,
                    columns = [
                        Column(name=Table.MARKET.Column.ID.value, data_type=DataType.SERIAL(), nullable=False),
                        Column(name=Table.MARKET.Column.CODE.value, data_type=DataType.VARCHAR(), nullable=True),
                        Column(name=Table.MARKET.Column.NAME.value, data_type=DataType.VARCHAR(), nullable=True),
                        Column(name=Table.MARKET.Column.SAVE_PROGRESS_YEAR.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.MARKET.Column.CREATE_DATE.value, data_type=DataType.AUTO_TIMESTAMP(), nullable=True),
                        Column(name=Table.MARKET.Column.UPDATE_DATE.value, data_type=DataType.TIMESTAMP(), nullable=True),
                        Column(name=Table.MARKET.Column.DELETE_DATE.value, data_type=DataType.TIMESTAMP(), nullable=True),
                    ],
                    primary_keys=Table.MARKET.primary_key,
                )
                # fmt: on
                
                # S_VN_INDEX
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.S_VN_INDEX.name,
                    columns=[
                        Column(name=Table.S_VN_INDEX.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.S_VN_INDEX.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.ADJUST.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.PERCENT_CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.MATCHING_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.MATCHING_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.NEGOTIATE_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.NEGOTIATE_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.NUMBER_OF_BUY_ORDERS.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.BUY_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.AVERAGE_VOLUME_PER_BUY_ORDER.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.NUMBER_OF_SELL_ORDERS.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.SELL_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.AVERAGE_VOLUME_PER_SELL_ORDER.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.S_VN_INDEX.Column.NET_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                    ],
                    primary_keys=Table.S_VN_INDEX.primary_key,
                )
                # fmt: on
                
                # HNX_INDEX
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.HNX_INDEX.name,
                    columns = [
                        Column(name=Table.HNX_INDEX.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.HNX_INDEX.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_INDEX.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_INDEX.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_INDEX.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_INDEX.Column.VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                    ],
                    primary_keys=Table.HNX_INDEX.primary_key,
                )
                # fmt: on
                
                # VN_30_INDEX
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.VN_30_INDEX.name,
                    columns = [
                        Column(name=Table.VN_30_INDEX.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.VN_30_INDEX.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.ADJUSTED_CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.MATCHED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.MATCHED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.NEGOTIATED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.NEGOTIATED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.CHANGE_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.CHANGE_PERCENTAGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.VN_30_INDEX.primary_key,
                )
                # fmt: on
                
                # VN_100_INDEX
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.VN_100_INDEX.name,
                    columns = [
                        Column(name=Table.VN_100_INDEX.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.VN_100_INDEX.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.ADJUSTED_CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.MATCHED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.MATCHED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.NEGOTIATED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.NEGOTIATED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.CHANGE_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.CHANGE_PERCENTAGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.VN_100_INDEX.primary_key,
                )
                # fmt: on
                
                # HNX_30_INDEX
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.HNX_30_INDEX.name,
                    columns = [
                        Column(name=Table.HNX_30_INDEX.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.HNX_30_INDEX.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.ADJUSTED_CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.MATCHED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.MATCHED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.NEGOTIATED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.NEGOTIATED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.CHANGE_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.CHANGE_PERCENTAGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.HNX_30_INDEX.primary_key,
                )
                # fmt: on
                
                # UPCOM_INDEX
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.UPCOM_INDEX.name,
                    columns = [
                        Column(name=Table.UPCOM_INDEX.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.UPCOM_INDEX.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.ADJUSTED_CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.MATCHED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.MATCHED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.NEGOTIATED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.NEGOTIATED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.CHANGE_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.CHANGE_PERCENTAGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.UPCOM_INDEX.primary_key,
                )
                # fmt: on

            case DataQuality.GOLD:
                # MARKET
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.MARKET.name,
                    columns = [
                        Column(name=Table.MARKET.Column.ID.value, data_type=DataType.SERIAL(), nullable=False),
                        Column(name=Table.MARKET.Column.CODE.value, data_type=DataType.VARCHAR(), nullable=True),
                        Column(name=Table.MARKET.Column.NAME.value, data_type=DataType.VARCHAR(), nullable=True),
                        Column(name=Table.MARKET.Column.SAVE_PROGRESS_YEAR.value, data_type=DataType.INT(), nullable=True),
                        Column(name=Table.MARKET.Column.CREATE_DATE.value, data_type=DataType.AUTO_TIMESTAMP(), nullable=True),
                        Column(name=Table.MARKET.Column.UPDATE_DATE.value, data_type=DataType.TIMESTAMP(), nullable=True),
                        Column(name=Table.MARKET.Column.DELETE_DATE.value, data_type=DataType.TIMESTAMP(), nullable=True),
                    ],
                    primary_keys=Table.MARKET.primary_key,
                )
                # fmt: on

                # G_VN_INDEX
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.G_VN_INDEX.name,
                    columns=[
                        Column(name=Table.G_VN_INDEX.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.G_VN_INDEX.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.ADJUST.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.PERCENT_CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.MATCHING_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.MATCHING_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.NEGOTIATE_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.NEGOTIATE_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.NUMBER_OF_BUY_ORDERS.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.BUY_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.AVERAGE_VOLUME_PER_BUY_ORDER.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.NUMBER_OF_SELL_ORDERS.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.SELL_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.AVERAGE_VOLUME_PER_SELL_ORDER.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.G_VN_INDEX.Column.NET_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                    ],
                    primary_keys=Table.G_VN_INDEX.primary_key,
                )
                # fmt: on
                
                # HNX_INDEX
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.HNX_INDEX.name,
                    columns = [
                        Column(name=Table.HNX_INDEX.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.HNX_INDEX.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_INDEX.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_INDEX.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_INDEX.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_INDEX.Column.VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                    ],
                    primary_keys=Table.HNX_INDEX.primary_key,
                )
                # fmt: on
                
                # VN_30_INDEX
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.VN_30_INDEX.name,
                    columns = [
                        Column(name=Table.VN_30_INDEX.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.VN_30_INDEX.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.ADJUSTED_CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.MATCHED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.MATCHED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.NEGOTIATED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.NEGOTIATED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.CHANGE_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_30_INDEX.Column.CHANGE_PERCENTAGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.VN_30_INDEX.primary_key,
                )
                # fmt: on
                
                # VN_100_INDEX
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.VN_100_INDEX.name,
                    columns = [
                        Column(name=Table.VN_100_INDEX.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.VN_100_INDEX.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.ADJUSTED_CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.MATCHED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.MATCHED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.NEGOTIATED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.NEGOTIATED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.CHANGE_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.VN_100_INDEX.Column.CHANGE_PERCENTAGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.VN_100_INDEX.primary_key,
                )
                # fmt: on
                
                # HNX_30_INDEX
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.HNX_30_INDEX.name,
                    columns = [
                        Column(name=Table.HNX_30_INDEX.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.HNX_30_INDEX.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.ADJUSTED_CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.MATCHED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.MATCHED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.NEGOTIATED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.NEGOTIATED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.CHANGE_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.HNX_30_INDEX.Column.CHANGE_PERCENTAGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.HNX_30_INDEX.primary_key,
                )
                # fmt: on
                
                # UPCOM_INDEX
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.UPCOM_INDEX.name,
                    columns = [
                        Column(name=Table.UPCOM_INDEX.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.UPCOM_INDEX.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.ADJUSTED_CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.MATCHED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.MATCHED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.NEGOTIATED_VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.NEGOTIATED_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.CHANGE_VALUE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.UPCOM_INDEX.Column.CHANGE_PERCENTAGE.value, data_type=DataType.DECIMAL(), nullable=True),
                    ],
                    primary_keys=Table.UPCOM_INDEX.primary_key,
                )
                # fmt: on

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish creating stock market tables for "{data_quality.value}".'
        )

    def _create_enterprise_tables(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start creating enterprise tables for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                # STOCK
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.ENTERPRISE.value,
                    table_name=Table.STOCK.name,
                    columns = [
                        Column(name=Table.STOCK.Column.ID.value, data_type=DataType.SERIAL(), nullable=False),
                        Column(name=Table.STOCK.Column.CODE.value, data_type=DataType.VARCHAR(), nullable=False),
                        Column(name=Table.STOCK.Column.LISTED_SHARES.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.STOCK.Column.OUTSTANDING_SHARES.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.STOCK.Column.OUTSTANDING_RATE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.STOCK.Column.MARKET_CAP.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.STOCK.Column.MARKET_ID.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.STOCK.Column.CREATE_DATE.value, data_type=DataType.AUTO_TIMESTAMP(), nullable=False),
                        Column(name=Table.STOCK.Column.UPDATE_DATE.value, data_type=DataType.TIMESTAMP(), nullable=True),
                        Column(name=Table.STOCK.Column.DELETE_DATE.value, data_type=DataType.TIMESTAMP(), nullable=True),
                    ],
                    primary_keys=Table.STOCK.primary_key,
                    foreign_keys=[ForeignKey(
                        column_name=Table.STOCK.Column.MARKET_ID.value, 
                        ref_table=f"{Schema.STOCK_MARKET.value}.{Table.MARKET.name}", 
                        ref_column=Table.MARKET.Column.ID.value,
                    )],
                )
                # fmt: on
                
                # DAILY_PRICE
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.ENTERPRISE.value,
                    table_name=Table.DAILY_PRICE.name,
                    columns = [
                        Column(name=Table.DAILY_PRICE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                        Column(name=Table.DAILY_PRICE.Column.CODE.value, data_type=DataType.VARCHAR(), nullable=False),
                        Column(name=Table.DAILY_PRICE.Column.MARKET_ID.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.DAILY_PRICE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.DAILY_PRICE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.DAILY_PRICE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.DAILY_PRICE.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.DAILY_PRICE.Column.VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
                    ],
                    primary_keys=Table.DAILY_PRICE.primary_key,
                    foreign_keys=[ForeignKey(
                        column_name=Table.DAILY_PRICE.Column.MARKET_ID.value, 
                        ref_table=f"{Schema.STOCK_MARKET.value}.{Table.MARKET.name}", 
                        ref_column=Table.MARKET.Column.ID.value,
                    )],
                )
                # fmt: on

            case DataQuality.SILVER:
                # STOCK
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.ENTERPRISE.value,
                    table_name=Table.STOCK.name,
                    columns = [
                        Column(name=Table.STOCK.Column.ID.value, data_type=DataType.SERIAL(), nullable=False),
                        Column(name=Table.STOCK.Column.CODE.value, data_type=DataType.VARCHAR(), nullable=False),
                        Column(name=Table.STOCK.Column.LISTED_SHARES.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.STOCK.Column.OUTSTANDING_SHARES.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.STOCK.Column.OUTSTANDING_RATE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.STOCK.Column.MARKET_CAP.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.STOCK.Column.MARKET_ID.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.STOCK.Column.CREATE_DATE.value, data_type=DataType.AUTO_TIMESTAMP(), nullable=False),
                        Column(name=Table.STOCK.Column.UPDATE_DATE.value, data_type=DataType.TIMESTAMP(), nullable=True),
                        Column(name=Table.STOCK.Column.DELETE_DATE.value, data_type=DataType.TIMESTAMP(), nullable=True),
                    ],
                    primary_keys=Table.STOCK.primary_key,
                    foreign_keys=[ForeignKey(
                        column_name=Table.STOCK.Column.MARKET_ID.value, 
                        ref_table=f"{Schema.STOCK_MARKET.value}.{Table.MARKET.name}", 
                        ref_column=Table.MARKET.Column.ID.value,
                    )],
                )
                # fmt: on

            case DataQuality.GOLD:
                # STOCK
                # fmt: off
                self._database_driver.create_table(
                    schema_name=Schema.ENTERPRISE.value,
                    table_name=Table.STOCK.name,
                    columns = [
                        Column(name=Table.STOCK.Column.ID.value, data_type=DataType.SERIAL(), nullable=False),
                        Column(name=Table.STOCK.Column.CODE.value, data_type=DataType.VARCHAR(), nullable=False),
                        Column(name=Table.STOCK.Column.LISTED_SHARES.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.STOCK.Column.OUTSTANDING_SHARES.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.STOCK.Column.OUTSTANDING_RATE.value, data_type=DataType.DECIMAL(), nullable=True),
                        Column(name=Table.STOCK.Column.MARKET_CAP.value, data_type=DataType.BIGINT(), nullable=True),
                        Column(name=Table.STOCK.Column.MARKET_ID.value, data_type=DataType.INT(), nullable=False),
                        Column(name=Table.STOCK.Column.CREATE_DATE.value, data_type=DataType.AUTO_TIMESTAMP(), nullable=False),
                        Column(name=Table.STOCK.Column.UPDATE_DATE.value, data_type=DataType.TIMESTAMP(), nullable=True),
                        Column(name=Table.STOCK.Column.DELETE_DATE.value, data_type=DataType.TIMESTAMP(), nullable=True),
                    ],
                    primary_keys=Table.STOCK.primary_key,
                    foreign_keys=[ForeignKey(
                        column_name=Table.STOCK.Column.MARKET_ID.value, 
                        ref_table=f"{Schema.STOCK_MARKET.value}.{Table.MARKET.name}", 
                        ref_column=Table.MARKET.Column.ID.value,
                    )],
                )
                # fmt: on

                for stock_code in STOCK_CODES_TO_BE_EXPORTED_TO_GOLD_DB:
                    # fmt: off
                    self._database_driver.create_table(
                        schema_name=Schema.ENTERPRISE.value,
                        table_name=stock_code,
                        columns = [
                            Column(name="date", data_type=DataType.DATE(), nullable=False),
                            Column(name="code", data_type=DataType.VARCHAR(), nullable=False),
                            Column(name="market_id", data_type=DataType.INT(), nullable=True),
                            Column(name="open", data_type=DataType.DECIMAL(), nullable=True),
                            Column(name="high", data_type=DataType.DECIMAL(), nullable=True),
                            Column(name="low", data_type=DataType.DECIMAL(), nullable=True),
                            Column(name="close", data_type=DataType.DECIMAL(), nullable=True),
                            Column(name="volume", data_type=DataType.DECIMAL(), nullable=True),
                        ],
                        primary_keys=["date"],
                        foreign_keys=[ForeignKey(
                            column_name="market_id", 
                            ref_table=f"{Schema.STOCK_MARKET.value}.{Table.MARKET.name}", 
                            ref_column=Table.MARKET.Column.ID.value,
                        )],
                    )
                    # fmt: on

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish creating enterprise tables for "{data_quality.value}".'
        )

    def _create_tables(self, data_quality: DataQuality) -> None:
        self._logger.log_info(f'Start creating tables for "{data_quality.value}".')

        match data_quality:
            case DataQuality.BRONZE:
                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_bronze", "macroeconomics"
                ):
                    self._create_macroeconomics_tables(data_quality)

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_bronze", "stock_market"
                ):
                    self._create_stock_market_tables(data_quality)

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_bronze", "enterprise"
                ):
                    self._create_enterprise_tables(data_quality)

            case DataQuality.SILVER:
                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "macroeconomics"
                ):
                    self._create_macroeconomics_tables(data_quality)

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "stock_market"
                ):
                    self._create_stock_market_tables(data_quality)

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_silver", "enterprise"
                ):
                    self._create_enterprise_tables(data_quality)

            case DataQuality.GOLD:
                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_gold", "macroeconomics"
                ):
                    self._create_macroeconomics_tables(data_quality)

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_gold", "stock_market"
                ):
                    self._create_stock_market_tables(data_quality)

                if self._switch_handler.is_enabled(
                    "data_preprocessor", "data_quality_gold", "enterprise"
                ):
                    self._create_enterprise_tables(data_quality)

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}".')

        self._logger.log_info(f'Finish creating tables for "{data_quality.value}".')

    # endregion Create Tables

    # region MACROECONOMICS data process

    # region MACROECONOMICS.GDP
    def _ingest_macroeconomics_gdp_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.GDP,
            GdpSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        df = df.iloc[:5, :]

        # Set indicator names as lowercase with underscores
        self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.QUARTER_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        # Rename columns
        df.rename(columns={"total_gdp": "gdp_growth"}, inplace=True)

        # Resort columns
        df = df[
            [
                "year",
                "quarter",
                "agriculture",
                "industry",
                "services",
                "gdp_growth",
                "gdp_real",
            ]
        ]

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.GDP.name,
            primary_keys=Table.GDP.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_gdp_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.GDP,
            GdpSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.GDP.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.REMOVE_RECORD_IF_COLUMN_IS_NULL(
                    Table.GDP.Column.GDP_GROWTH.value
                ),
                CleanLayer.ORDER_BY(
                    [Table.GDP.Column.YEAR.value, Table.GDP.Column.QUARTER.value]
                ),
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.GDP.name,
            primary_keys=Table.GDP.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_gdp_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.GDP,
            GdpSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.GDP.name,
            columns=[
                Table.GDP.Column.YEAR.value,
                Table.GDP.Column.QUARTER.value,
                Table.GDP.Column.GDP_GROWTH.value,
            ],
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        gold_df["gdp_growth"] = gold_df["gdp_growth"].interpolate(method="linear")

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_GDP.name,
            primary_keys=Table.G_GDP.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_gdp(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics GDP data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_gdp_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_gdp_vietstock()

            case DataQuality.GOLD:
                self._transform_macroeconomics_gdp_vietstock()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics GDP data for "{data_quality.value}".'
        )

    # endregion GDP

    # region MACROECONOMICS.CPI
    def _ingest_macroeconomics_cpi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.CPI,
            CpiSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_NAME_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.CPI.name,
            primary_keys=Table.CPI.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_cpi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.CPI,
            CpiSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.CPI.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [Table.CPI.Column.YEAR.value, Table.CPI.Column.MONTH.value]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.CPI.name,
            primary_keys=Table.CPI.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_cpi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.CPI,
            CpiSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.CPI.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_CPI.name,
            primary_keys=Table.G_CPI.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_cpi(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics CPI data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_cpi_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_cpi_vietstock()

            case DataQuality.GOLD:
                self._transform_macroeconomics_cpi_vietstock()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics CPI data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.CPI

    # region MACROECONOMICS.PPI
    def _ingest_macroeconomics_ppi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.PPI,
            PpiSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df["Chỉ tiêu"] = (
            df["Chỉ tiêu"].str.lower().str.replace(",", "").str.replace(" ", "_")
        )

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        col_translation = {
            "year": "year",
            "chỉ_số_chung": "general_index",
            "dịch_vụ_lâm_nghiệp": "forestry_services",
            "dịch_vụ_nông_nghiệp": "agricultural_services",
            "lâm_nghiệp_và_dịch_vụ_có_liên_quan": "forestry_and_related_services",
            "lâm_sản_khai_thác": "exploited_forest_products",
            "lâm_sản_thu_nhặt": "collected_forest_products",
            "nông_nghiệp_và_dịch_vụ_có_liên_quan": "agriculture_and_related_services",
            "sản_phẩm_từ_chăn_nuôi": "livestock_products",
            "sản_phẩm_từ_cây_hàng_năm": "annual_crop_products",
            "sản_phẩm_từ_cây_lâu_năm": "perennial_crop_products",
            "thủy_sản_khai_thác": "exploited_aquatic_products",
            "thủy_sản_khai_thác_nuôi_trồng": "aquatic_products_exploitation_and_farming",
            "thủy_sản_nuôi_trồng": "aquatic_farming_products",
            "trồng_rừng_và_chăm_sóc_rừng": "forest_planting_and_care",
        }

        df = df.rename(columns=col_translation)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.PPI.name,
            primary_keys=Table.PPI.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_ppi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.PPI,
            PpiSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.PPI.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[CleanLayer.ORDER_BY([Table.PPI.Column.YEAR.value])],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.PPI.name,
            primary_keys=Table.PPI.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_ppi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.PPI,
            PpiSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.PPI.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_PPI.name,
            primary_keys=Table.G_PPI.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_ppi(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics PPI data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_ppi_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_ppi_vietstock()

            case DataQuality.GOLD:
                self._transform_macroeconomics_ppi_vietstock()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics PPI data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.PPI

    # region MACROECONOMICS.IPI
    def _ingest_macroeconomics_ipi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IPI,
            IpiSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df["Chỉ tiêu"] = (
            df["Chỉ tiêu"].str.lower().str.replace(",", "").str.replace(" ", "_")
        )

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        col_translation = {
            "year": "year",
            "chỉ_số_chung": "general_index",
            "dịch_vụ_chuyên_môn_khoa_học_công_nghệ": "professional_scientific_and_technical_services",
            "dịch_vụ_xây_dựng": "construction_services",
            "giấy_và_các_sản_phẩm_từ_giấy": "paper_and_paper_products",
            "hóa_chất_và_sản_phẩm_hóa_chất": "chemicals_and_chemical_products",
            "máy_móc_thiết_bị_chưa_được_phân_vào_đâu": "machinery_and_equipment_not_elsewhere_classified",
            "nước_tự_nhiên_khai_thác": "natural_water_extraction",
            "nước_tự_nhiên_khai_thác;_dịch_vụ_quản_lý_và_xử_lý_rác_thải._nước_thải-": "natural_water_extraction_and_waste_management_services",
            "phương_tiện_vận_tải_khác": "other_transport_equipment",
            "quặng_kim_loại": "metal_ores",
            "sản_phẩm_chế_biến_lương_thực_thực_phẩm": "processed_food_products",
            "sản_phẩm_công_nghiệp_chế_biến_chế_tạo": "manufacturing_products",
            "sản_phẩm_dệt_da": "textiles_and_leather_products",
            "sản_phẩm_khai_khoáng": "mining_products",
            "sản_phẩm_khai_khoáng_khác": "other_mining_products",
            "sản_phẩm_kim_loại": "metal_products",
            "sản_phẩm_lâm_nghiệp_và_dịch_vụ_liên_quan": "forestry_products_and_related_services",
            "sản_phẩm_nông_lâm_nghiệp_và_thủy_sản": "agriculture_forestry_and_fishery_products",
            "sản_phẩm_nông_nghiệp_và_dịch_vụ_liên_quan": "agriculture_products_and_related_services",
            "sản_phẩm_thủy_sản_khai_thác_nuôi_trồng": "fishing_and_aquaculture_products",
            "sản_phẩm_từ_cao_su_và_plastic": "rubber_and_plastic_products",
            "sản_phẩm_từ_gỗ": "wood_products",
            "sản_phẩm_từ_khoáng_phi_kim_loại_khác": "other_non_metallic_mineral_products",
            "sản_phẩm_từ_kim_loại_đúc_sẵn_(trừ_máy_móc_thiết_bị)": "fabricated_metal_products_except_machinery_and_equipment",
            "sản_phẩm_điện_tử_máy_tính_quang_học": "electronic_computer_and_optical_products",
            "sử_dụng_cho_sản_xuất_công_nghiệp_chế_biến_chế_tạo": "used_for_manufacturing_industry",
            "sử_dụng_cho_sản_xuất_nông_lâm_nghiệp_và_thủy_sản": "used_for_agriculture_forestry_and_fishery",
            "sử_dụng_cho_xây_dựng": "used_for_construction",
            "than_cốc_sản_phẩm_dầu_mỏ_tinh_chế": "coke_and_refined_petroleum_products",
            "than_cứng_và_than_non": "hard_coal_and_lignite",
            "thiết_bị_điện": "electrical_equipment",
            "thuốc_và_dược_liệu": "pharmaceuticals_and_medicinal_chemicals",
            "xe_có_động_cơ_rơ_moóc": "motor_vehicles_and_trailers",
            "điện_khí_đốt_hơi_nước_và_điều_hòa_không_khí": "electricity_gas_steam_and_air_conditioning",
            "đồ_uống_hút": "beverages_and_tobacco",
        }

        df = df.rename(columns=col_translation)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IPI.name,
            primary_keys=Table.IPI.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_ipi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IPI,
            IpiSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IPI.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[CleanLayer.ORDER_BY([Table.IPI.Column.YEAR.value])],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IPI.name,
            primary_keys=Table.IPI.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_ipi(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics IPI data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_ipi_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_ipi_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics IPI data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.IPI

    # region MACROECONOMICS.XPI
    def _ingest_macroeconomics_xpi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.XPI,
            XpiSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_NAME_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.XPI.name,
            primary_keys=Table.XPI.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_xpi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.XPI,
            XpiSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.XPI.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [Table.XPI.Column.YEAR.value, Table.XPI.Column.MONTH.value]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.XPI.name,
            primary_keys=Table.XPI.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_xpi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.XPI,
            XpiSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.XPI.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_XPI.name,
            primary_keys=Table.G_XPI.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_xpi(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics XPI data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_xpi_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_xpi_vietstock()

            case DataQuality.GOLD:
                self._transform_macroeconomics_xpi_vietstock()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics XPI data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.XPI

    # region MACROECONOMICS.MPI
    def _ingest_macroeconomics_mpi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.MPI,
            MpiSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_NAME_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.MPI.name,
            primary_keys=Table.MPI.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_mpi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.MPI,
            MpiSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.MPI.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [Table.MPI.Column.YEAR.value, Table.MPI.Column.MONTH.value]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.MPI.name,
            primary_keys=Table.MPI.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_mpi(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics MPI data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_mpi_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_mpi_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics MPI data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.MPI

    # region MACROECONOMICS.POPULATION
    def _ingest_macroeconomics_population_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.POPULATION,
            PopulationSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.POPULATION.name,
            primary_keys=Table.POPULATION.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_population_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.POPULATION,
            PopulationSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.POPULATION.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY([Table.POPULATION.Column.YEAR.value])
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.POPULATION.name,
            primary_keys=Table.POPULATION.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_population_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.POPULATION,
            PopulationSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.POPULATION.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_POPULATION.name,
            primary_keys=Table.G_POPULATION.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_population(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics POPULATION data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_population_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_population_vietstock()

            case DataQuality.GOLD:
                self._transform_macroeconomics_population_vietstock()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics POPULATION data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.POPULATION

    # region MACROECONOMICS.LABOR
    def _ingest_macroeconomics_labor_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.LABOR,
            LaborSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        self._standardize_column_name_before_melting(df=df)

        df["Chỉ tiêu"] = df["Chỉ tiêu"].str.replace("employed_a", "employed_amount")

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.LABOR.name,
            primary_keys=Table.LABOR.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_labor_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.LABOR,
            LaborSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.LABOR.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[CleanLayer.ORDER_BY([Table.LABOR.Column.YEAR.value])],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.LABOR.name,
            primary_keys=Table.LABOR.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_labor_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.LABOR,
            LaborSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.LABOR.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_LABOR.name,
            primary_keys=Table.G_LABOR.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_labor(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics LABOR data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_labor_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_labor_vietstock()

            case DataQuality.GOLD:
                self._transform_macroeconomics_labor_vietstock()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics LABOR data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.LABOR

    # region MACROECONOMICS.RETAIL
    def _ingest_macroeconomics_retail_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.RETAIL,
            RetailSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_NAME_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.RETAIL.name,
            primary_keys=Table.RETAIL.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_retail_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.RETAIL,
            RetailSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.RETAIL.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [Table.RETAIL.Column.YEAR.value, Table.RETAIL.Column.MONTH.value]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.RETAIL.name,
            primary_keys=Table.RETAIL.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_retail(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics RETAIL data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_retail_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_retail_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics RETAIL data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.RETAIL

    # region MACROECONOMICS.PMI
    def _ingest_macroeconomics_pmi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.PMI,
            PmiSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_NAME_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.PMI.name,
            primary_keys=Table.PMI.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_pmi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.PMI,
            PmiSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.PMI.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [Table.PMI.Column.YEAR.value, Table.PMI.Column.MONTH.value]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.PMI.name,
            primary_keys=Table.PMI.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_pmi(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics PMI data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_pmi_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_pmi_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics PMI data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.PMI

    # region MACROECONOMICS.IIP
    def _ingest_macroeconomics_iip_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IIP,
            IipSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        df = df.drop(df.index[14])

        # Set indicator names as lowercase with underscores
        self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_INDEX_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IIP.name,
            primary_keys=Table.IIP.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_iip_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IIP,
            IipSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IIP.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [Table.IIP.Column.YEAR.value, Table.IIP.Column.MONTH.value]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IIP.name,
            primary_keys=Table.IIP.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_iip(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics IIP data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_iip_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_iip_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics IIP data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.IIP

    # region MACROECONOMICS.IPV
    def _ingest_macroeconomics_ipv_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IPV,
            IpvSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_NAME_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IPV.name,
            primary_keys=Table.IPV.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_ipv_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IPV,
            IpvSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IPV.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [Table.IPV.Column.YEAR.value, Table.IPV.Column.MONTH.value]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IPV.name,
            primary_keys=Table.IPV.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_ipv_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IPV,
            IpvSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IPV.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_IPV.name,
            primary_keys=Table.G_IPV.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_ipv(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics IPV data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_ipv_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_ipv_vietstock()

            case DataQuality.GOLD:
                self._transform_macroeconomics_ipv_vietstock()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics IPV data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.IPV

    # region MACROECONOMICS.MIP
    def _ingest_macroeconomics_mip_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.MIP,
            MipSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.MIP.name,
            primary_keys=Table.MIP.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_mip_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.MIP,
            MipSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.MIP.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[CleanLayer.ORDER_BY([Table.MIP.Column.YEAR.value])],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.MIP.name,
            primary_keys=Table.MIP.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_mip_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.MIP,
            MipSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.MIP.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_MIP.name,
            primary_keys=Table.G_MIP.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_mip(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics MIP data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_mip_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_mip_vietstock()

            case DataQuality.GOLD:
                self._transform_macroeconomics_mip_vietstock()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics MIP data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.MIP

    # region MACROECONOMICS.FA_BY_HOUSE_TYPES
    def _ingest_macroeconomics_fa_by_house_types_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.FA_BY_HOUSE_TYPES,
            FaByHouseTypeSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        col_translation = {
            "16_20_floors": "_16_20_floors",
            "21_25_floors": "_21_25_floors",
            "26_floors_and_above": "_26_floors_and_above",
            "5_floors_and_below": "_5_floors_and_below",
            "6_8_floors": "_6_8_floors",
            "9_15_floors": "_9_15_floors",
        }

        df = df.rename(columns=col_translation)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.FA_BY_HOUSE_TYPES.name,
            primary_keys=Table.FA_BY_HOUSE_TYPES.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_fa_by_house_types_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.FA_BY_HOUSE_TYPES,
            FaByHouseTypeSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.FA_BY_HOUSE_TYPES.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY([Table.FA_BY_HOUSE_TYPES.Column.YEAR.value])
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.FA_BY_HOUSE_TYPES.name,
            primary_keys=Table.FA_BY_HOUSE_TYPES.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_fa_by_house_types_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.FA_BY_HOUSE_TYPES,
            FaByHouseTypeSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.FA_BY_HOUSE_TYPES.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_FA_BY_HOUSE_TYPES.name,
            primary_keys=Table.G_FA_BY_HOUSE_TYPES.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_fa_by_house_types(
        self, data_quality: DataQuality
    ) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics FA_BY_HOUSE_TYPES data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_fa_by_house_types_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_fa_by_house_types_vietstock()

            case DataQuality.GOLD:
                self._transform_macroeconomics_fa_by_house_types_vietstock()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics FA_BY_HOUSE_TYPES data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.FA_BY_HOUSE_TYPES

    # region MACROECONOMICS.IT_BOP
    def _ingest_macroeconomics_it_bop_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IT_BOP,
            ItBopSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.QUARTER_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IT_BOP.name,
            primary_keys=Table.IT_BOP.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_it_bop_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IT_BOP,
            ItBopSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IT_BOP.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [Table.IT_BOP.Column.YEAR.value, Table.IT_BOP.Column.QUARTER.value]
                ),
                CleanLayer.REMOVE_COLUMN(
                    [
                        Table.IT_BOP.Column.B_CAPITAL_ACCOUNT.value,
                        Table.IT_BOP.Column.CAPITAL_ACCOUNT_PAYMENTS.value,
                        Table.IT_BOP.Column.CAPITAL_ACCOUNT_RECEIPTS.value,
                        Table.IT_BOP.Column.LOANS_AND_EXTERNAL_DEBT_COLLECTION.value,
                        Table.IT_BOP.Column.TRADE_CREDITS_AND_ADVANCES.value,
                        Table.IT_BOP.Column.OTHER_RECEIVABLESPAYABLES.value,
                        Table.IT_BOP.Column.IMF_CREDITS_AND_LOANS.value,
                        Table.IT_BOP.Column.SPECIAL_FINANCING.value,
                    ]
                ),
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IT_BOP.name,
            primary_keys=Table.IT_BOP.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_it_bop(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics IT_BOP data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_it_bop_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_it_bop_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics IT_BOP data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.IT_BOP

    # region MACROECONOMICS.TSBR
    def _ingest_macroeconomics_tsbr_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.TSBR,
            TsbrSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.THREE_MONTH_INDEX_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.TSBR.name,
            primary_keys=Table.TSBR.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_tsbr_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.TSBR,
            TsbrSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.TSBR.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [Table.TSBR.Column.YEAR.value, Table.TSBR.Column.MONTH.value]
                )
            ],
        )

        keep_cols = ["year", "quarter", "month", "day", "date"]

        # Identify the columns to check (exclude date-related columns)
        check_cols = [col for col in silver_df.columns if col not in keep_cols]

        # Drop rows where *all* of those columns are 0
        silver_df = silver_df.drop(
            silver_df[(silver_df[check_cols] == 0).all(axis=1)].index
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.TSBR.name,
            primary_keys=Table.TSBR.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_tsbr(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics TSBR data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_tsbr_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_tsbr_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics TSBR data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.TSBR

    # region MACROECONOMICS.TSBE
    def _ingest_macroeconomics_tsbe_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.TSBE,
            TsbeSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.THREE_MONTH_INDEX_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.TSBE.name,
            primary_keys=Table.TSBE.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_tsbe_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.TSBE,
            TsbeSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.TSBE.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [Table.TSBE.Column.YEAR.value, Table.TSBE.Column.MONTH.value]
                )
            ],
        )

        keep_cols = ["year", "quarter", "month", "day", "date"]

        # Identify the columns to check (exclude date-related columns)
        check_cols = [col for col in silver_df.columns if col not in keep_cols]

        # Drop rows where *all* of those columns are 0
        silver_df = silver_df.drop(
            silver_df[(silver_df[check_cols] == 0).all(axis=1)].index
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.TSBE.name,
            primary_keys=Table.TSBE.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_tsbe(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics TSBE data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_tsbe_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_tsbe_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics TSBE data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.TSBE

    # region MACROECONOMICS.GD
    def _ingest_macroeconomics_gd_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.GD,
            GdSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.GD.name,
            primary_keys=Table.GD.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_gd_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.GD,
            GdSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.GD.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[CleanLayer.ORDER_BY([Table.GD.Column.YEAR.value])],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.GD.name,
            primary_keys=Table.GD.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_gd(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics GD data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_gd_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_gd_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics GD data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.GD

    # region MACROECONOMICS.BRD
    def _ingest_macroeconomics_brd_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.BRD,
            BrdSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_NAME_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.BRD.name,
            primary_keys=Table.BRD.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_brd_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.BRD,
            BrdSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.BRD.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [Table.BRD.Column.YEAR.value, Table.BRD.Column.MONTH.value]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.BRD.name,
            primary_keys=Table.BRD.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_brd(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics BRD data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_brd_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_brd_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics BRD data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.BRD

    # region MACROECONOMICS.IISD
    def _ingest_macroeconomics_iisd_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IISD,
            IisdSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.QUARTER_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IISD.name,
            primary_keys=Table.IISD.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_iisd_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IISD,
            IisdSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IISD.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [Table.IISD.Column.YEAR.value, Table.IISD.Column.QUARTER.value]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IISD.name,
            primary_keys=Table.IISD.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_iisd(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics IISD data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_iisd_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_iisd_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics IISD data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.IISD

    # region MACROECONOMICS.TREG
    def _ingest_macroeconomics_treg_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.TREG,
            TregSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_NAME_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.TREG.name,
            primary_keys=Table.TREG.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_treg_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.TREG,
            TregSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.TREG.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [Table.TREG.Column.YEAR.value, Table.TREG.Column.MONTH.value]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.TREG.name,
            primary_keys=Table.TREG.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_treg_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.TREG,
            TregSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.TREG.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_TREG.name,
            primary_keys=Table.G_TREG.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_treg(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics TREG data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_treg_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_treg_vietstock()

            case DataQuality.GOLD:
                self._transform_macroeconomics_treg_vietstock()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics TREG data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.TREG

    # region MACROECONOMICS.CREDIT
    def _ingest_macroeconomics_credit_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.CREDIT,
            CreditSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_NAME_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.CREDIT.name,
            primary_keys=Table.CREDIT.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_credit_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.CREDIT,
            CreditSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.CREDIT.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [Table.CREDIT.Column.YEAR.value, Table.CREDIT.Column.MONTH.value]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.CREDIT.name,
            primary_keys=Table.CREDIT.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_credit(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics CREDIT data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_credit_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_credit_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics CREDIT data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.CREDIT

    # region MACROECONOMICS.MOBILIZATION
    def _ingest_macroeconomics_mobilization_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.MOBILIZATION,
            MobilizationSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_NAME_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.MOBILIZATION.name,
            primary_keys=Table.MOBILIZATION.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_mobilization_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.MOBILIZATION,
            MobilizationSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.MOBILIZATION.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [
                        Table.MOBILIZATION.Column.YEAR.value,
                        Table.MOBILIZATION.Column.MONTH.value,
                    ]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.MOBILIZATION.name,
            primary_keys=Table.MOBILIZATION.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_mobilization(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics MOBILIZATION data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_mobilization_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_mobilization_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics MOBILIZATION data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.MOBILIZATION

    # region MACROECONOMICS.EXCHANGE_RATE
    def _ingest_macroeconomics_exchange_rate_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.EXCHANGE_RATE,
            ExchangeRateSource.INVESTING,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Rename columns
            rename_map = {
                "Ngày": "date",
                "Lần cuối": "close",
                "Mở": "open",
                "Cao": "high",
                "Thấp": "low",
                "KL": "volume",
                "% Thay đổi": "change",
            }
            df = df.rename(columns=rename_map)

            # Convert 'date' to datetime
            df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["close", "open", "high", "low"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["volume"] = df["volume"].apply(parse_volume)

            # Handle "change" column: remove '%' and convert to float
            df["change"] = df["change"].astype(str).str.replace("%", "").astype(float)

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("date").reset_index(drop=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.EXCHANGE_RATE.name,
            primary_keys=Table.EXCHANGE_RATE.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish ingesting data in "{table_name}".')

    def _clean_macroeconomics_exchange_rate_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.EXCHANGE_RATE,
            ExchangeRateSource.INVESTING,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.EXCHANGE_RATE.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY([Table.EXCHANGE_RATE.Column.DATE.value])
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.EXCHANGE_RATE.name,
            primary_keys=Table.EXCHANGE_RATE.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_exchange_rate_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.EXCHANGE_RATE,
            ExchangeRateSource.INVESTING,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.EXCHANGE_RATE.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )
        gold_df = gold_df.dropna(subset=["close"])

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_EXCHANGE_RATE.name,
            primary_keys=Table.G_EXCHANGE_RATE.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_exchange_rate(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics EXCHANGE_RATE data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_exchange_rate_investing()

            case DataQuality.SILVER:
                self._clean_macroeconomics_exchange_rate_investing()

            case DataQuality.GOLD:
                self._transform_macroeconomics_exchange_rate_investing()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics EXCHANGE_RATE data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.EXCHANGE_RATE

    # region MACROECONOMICS.IIR
    def _ingest_macroeconomics_iir_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IIR,
            IirSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.DAY_MONTH_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        # Rename columns
        df.rename(
            columns={
                "1_months": "_1_months",
                "1_weeks": "_1_weeks",
                "2_weeks": "_2_weeks",
                "3_months": "_3_months",
                "6_months": "_6_months",
                "9_months": "_9_months",
            },
            inplace=True,
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IIR.name,
            primary_keys=Table.IIR.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_iir_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IIR,
            IirSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IIR.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [
                        Table.IIR.Column.YEAR.value,
                        Table.IIR.Column.MONTH.value,
                        Table.IIR.Column.DAY.value,
                    ]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IIR.name,
            primary_keys=Table.IIR.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_iir(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics IIR data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_iir_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_iir_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics IIR data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.IIR

    # region MACROECONOMICS.RRRR
    def _ingest_macroeconomics_rrrr_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.RRRR,
            RrrrSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.DAY_MONTH_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.RRRR.name,
            primary_keys=Table.RRRR.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_rrrr_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.RRRR,
            RrrrSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.RRRR.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [
                        Table.RRRR.Column.YEAR.value,
                        Table.RRRR.Column.MONTH.value,
                        Table.RRRR.Column.DAY.value,
                    ]
                )
            ],
        )

        # Fill timeline
        silver_df = make_date_time_index_for_dataframe(silver_df)

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.RRRR.name,
            primary_keys=[Table.RRRR.Column.DATE.value],
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_rrrr_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.RRRR,
            RrrrSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.RRRR.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_RRRR.name,
            primary_keys=Table.G_RRRR.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_rrrr(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics RRRR data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_rrrr_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_rrrr_vietstock()

            case DataQuality.GOLD:
                self._transform_macroeconomics_rrrr_vietstock()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics RRRR data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.RRRR

    # region MACROECONOMICS.FDI_SECTOR
    def _ingest_macroeconomics_fdi_sector_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.FDI_SECTOR,
            FdiSectorSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Step 1: Find the row that matches both conditions
        mask = (df["Chỉ tiêu"] == "Manufacturing and Processing Industry") & (
            df["Đơn vị tính"] == "Million US Dollars"
        )

        # Step 2: Within that subset, find the row with the max value in Aug-2020
        max_idx = (
            df.loc[mask, "Aug-2020"]
            .str.replace(",", "", regex=False)
            .astype(float)
            .idxmax()
        )

        # Step 3: Slice from that row to the end
        df = df.loc[max_idx:]

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_NAME_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.FDI_SECTOR.name,
            primary_keys=Table.FDI_SECTOR.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_fdi_sector_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.FDI_SECTOR,
            FdiSectorSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.FDI_SECTOR.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [
                        Table.FDI_SECTOR.Column.YEAR.value,
                        Table.FDI_SECTOR.Column.MONTH.value,
                    ]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.FDI_SECTOR.name,
            primary_keys=Table.FDI_SECTOR.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_fdi_sector(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics FDI_SECTOR data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_fdi_sector_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_fdi_sector_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics FDI_SECTOR data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.FDI_SECTOR

    # region MACROECONOMICS.FDI_RD
    def _ingest_macroeconomics_fdi_rd_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.FDI_RD,
            FdiRdSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df = self._standardize_column_name_before_melting(df=df)

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_NAME_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.FDI_RD.name,
            primary_keys=Table.FDI_RD.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_fdi_rd_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.FDI_RD,
            FdiRdSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.FDI_RD.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [
                        Table.FDI_RD.Column.YEAR.value,
                        Table.FDI_RD.Column.MONTH.value,
                    ]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.FDI_RD.name,
            primary_keys=Table.FDI_RD.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_fdi_rd(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics FDI_RD data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_fdi_rd_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_fdi_rd_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics FDI_RD data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.FDI_RD

    # region MACROECONOMICS.EXPORT
    def _ingest_macroeconomics_export_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.EXPORT,
            ExportSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        column_name = "Chỉ tiêu"

        df[column_name] = (
            df[column_name]
            .str.lower()
            .str.replace(
                r"[\s-]+", "_", regex=True
            )  # replace any whitespace with underscore
        )

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_NAME_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        rename_map = {
            "year": "year",
            "month": "month",
            "argentina": "argentina",
            "asean": "asean",
            "ba_lan": "poland",
            "belarus": "belarus",
            "brazil": "brazil",
            "bulgaria": "bulgaria",
            "bỉ": "belgium",
            "bồ_đào_nha": "portugal",
            "bờ_biển_ngà": "ivory_coast",
            "cameroon": "cameroon",
            "campuchia": "cambodia",
            "canada": "canada",
            "chile": "chile",
            "croatia": "croatia",
            "các_tiểu_vương_quốc_ả_rập_thống_nhất": "united_arab_emirates",
            "estonia": "estonia",
            "eu": "eu",
            "hungary": "hungary",
            "hy_lạp": "greece",
            "hà_lan": "netherlands",
            "hàn_quốc": "south_korea",
            "hồng_kông": "hong_kong",
            "indonesia": "indonesia",
            "ireland": "ireland",
            "israel": "israel",
            "kazakhstan": "kazakhstan",
            "kuwait": "kuwait",
            "latvia": "latvia",
            "litva": "lithuania",
            "luxembourg": "luxembourg",
            "lào": "laos",
            "malaysia": "malaysia",
            "malta": "malta",
            "mexico": "mexico",
            "myanmar": "myanmar",
            "mỹ_(hoa_kỳ)": "usa",
            "na_uy": "norway",
            "nam_phi": "south_africa",
            "new_zealand": "new_zealand",
            "nga": "russia",
            "nhà_nước_brunei_darussalam": "brunei_darussalam",
            "nhật_bản": "japan",
            "other_countries": "other_countries",
            "pakistan": "pakistan",
            "peru": "peru",
            "philippines": "philippines",
            "pháp": "france",
            "phần_lan": "finland",
            "romania": "romania",
            "senegal": "senegal",
            "singapore": "singapore",
            "slovakia": "slovakia",
            "slovenia": "slovenia",
            "séc": "czechia",
            "síp": "cyprus",
            "thái_lan": "thailand",
            "thổ_nhĩ_kỳ": "turkey",
            "thụy_sĩ": "switzerland",
            "thụy_điển": "sweden",
            "trung_quốc": "china",
            "tây_ban_nha": "spain",
            "ukraine": "ukraine",
            "vương_quốc_anh": "united_kingdom",
            "áo": "austria",
            "úc": "australia",
            "ý": "italy",
            "đan_mạch": "denmark",
            "đài_loan": "taiwan",
            "đức": "germany",
            "ả_rập_xê_út": "saudi_arabia",
            "ấn_độ": "india",
        }

        df = df.rename(columns=rename_map)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.EXPORT.name,
            primary_keys=Table.EXPORT.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_export_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.EXPORT,
            ExportSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.EXPORT.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [
                        Table.EXPORT.Column.YEAR.value,
                        Table.EXPORT.Column.MONTH.value,
                    ]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.EXPORT.name,
            primary_keys=Table.EXPORT.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_export(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics EXPORT data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_export_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_export_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics EXPORT data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.EXPORT

    # region MACROECONOMICS.IMPORT
    def _ingest_macroeconomics_import_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IMPORT,
            ImportSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        column_name = "Chỉ tiêu"

        df[column_name] = (
            df[column_name]
            .str.lower()
            .str.replace(
                r"[\s-]+", "_", regex=True
            )  # replace any whitespace with underscore
        )

        df = self._melt_dataframe_by_time_format(
            df=df,
            time_format=TimeFormat.MONTH_NAME_YEAR,
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
        )

        rename_map = {
            "year": "year",
            "month": "month",
            "argentina": "argentina",
            "asean": "asean",
            "ba_lan": "poland",
            "belarus": "belarus",
            "brazil": "brazil",
            "bulgaria": "bulgaria",
            "bỉ": "belgium",
            "bồ_đào_nha": "portugal",
            "bờ_biển_ngà": "ivory_coast",
            "cameroon": "cameroon",
            "campuchia": "cambodia",
            "canada": "canada",
            "chile": "chile",
            "croatia": "croatia",
            "các_tiểu_vương_quốc_ả_rập_thống_nhất": "united_arab_emirates",
            "estonia": "estonia",
            "eu": "eu",
            "hungary": "hungary",
            "hy_lạp": "greece",
            "hà_lan": "netherlands",
            "hàn_quốc": "south_korea",
            "hồng_kông": "hong_kong",
            "indonesia": "indonesia",
            "ireland": "ireland",
            "israel": "israel",
            "kazakhstan": "kazakhstan",
            "kuwait": "kuwait",
            "latvia": "latvia",
            "litva": "lithuania",
            "luxembourg": "luxembourg",
            "lào": "laos",
            "malaysia": "malaysia",
            "malta": "malta",
            "mexico": "mexico",
            "myanmar": "myanmar",
            "mỹ_(hoa_kỳ)": "usa",
            "na_uy": "norway",
            "nam_phi": "south_africa",
            "new_zealand": "new_zealand",
            "nga": "russia",
            "nhà_nước_brunei_darussalam": "brunei_darussalam",
            "nhật_bản": "japan",
            "other_countries": "other_countries",
            "pakistan": "pakistan",
            "peru": "peru",
            "philippines": "philippines",
            "pháp": "france",
            "phần_lan": "finland",
            "romania": "romania",
            "senegal": "senegal",
            "singapore": "singapore",
            "slovakia": "slovakia",
            "slovenia": "slovenia",
            "séc": "czechia",
            "síp": "cyprus",
            "thái_lan": "thailand",
            "thổ_nhĩ_kỳ": "turkey",
            "thụy_sĩ": "switzerland",
            "thụy_điển": "sweden",
            "trung_quốc": "china",
            "tây_ban_nha": "spain",
            "ukraine": "ukraine",
            "vương_quốc_anh": "united_kingdom",
            "áo": "austria",
            "úc": "australia",
            "ý": "italy",
            "đan_mạch": "denmark",
            "đài_loan": "taiwan",
            "đức": "germany",
            "ả_rập_xê_út": "saudi_arabia",
            "ấn_độ": "india",
        }

        df = df.rename(columns=rename_map)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IMPORT.name,
            primary_keys=Table.IMPORT.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_macroeconomics_import_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IMPORT,
            ImportSource.VIETSTOCK,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IMPORT.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY(
                    [
                        Table.IMPORT.Column.YEAR.value,
                        Table.IMPORT.Column.MONTH.value,
                    ]
                )
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IMPORT.name,
            primary_keys=Table.IMPORT.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_import(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics IMPORT data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_import_vietstock()

            case DataQuality.SILVER:
                self._clean_macroeconomics_import_vietstock()

            case DataQuality.GOLD:
                pass

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics IMPORT data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.IMPORT

    # region MACROECONOMICS.GOLD_PRICE
    def _ingest_macroeconomics_gold_price_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.GOLD_PRICE,
            GoldPriceSource.INVESTING,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        table_name = Table.GOLD_PRICE.__qualname__.lower()

        self._logger.log_info(f'Start ingesting data in "{table_name}".')

        # Add logic for processing data here
        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Rename columns
            rename_map = {
                "Ngày": "date",
                "Lần cuối": "close",
                "Mở": "open",
                "Cao": "high",
                "Thấp": "low",
                "KL": "volume",
                "% Thay đổi": "change",
            }
            df = df.rename(columns=rename_map)

            # Convert 'date' to datetime
            df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["close", "open", "high", "low"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["volume"] = df["volume"].apply(parse_volume)

            # Handle "change" column: remove '%' and convert to float
            df["change"] = df["change"].astype(str).str.replace("%", "").astype(float)

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("date").reset_index(drop=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.GOLD_PRICE.name,
            primary_keys=Table.GOLD_PRICE.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish ingesting data in "{table_name}".')

    def _clean_macroeconomics_gold_price_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.GOLD_PRICE,
            GoldPriceSource.INVESTING,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.GOLD_PRICE.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY([Table.GOLD_PRICE.Column.DATE.value])
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.GOLD_PRICE.name,
            primary_keys=Table.GOLD_PRICE.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_gold_price_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.GOLD_PRICE,
            GoldPriceSource.INVESTING,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.GOLD_PRICE.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_GOLD_PRICE.name,
            primary_keys=Table.G_GOLD_PRICE.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_gold_price(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics GOLD_PRICE data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_gold_price_investing()

            case DataQuality.SILVER:
                self._clean_macroeconomics_gold_price_investing()

            case DataQuality.GOLD:
                self._transform_macroeconomics_gold_price_investing()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics GOLD_PRICE data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.GOLD_PRICE

    # region MACROECONOMICS.OIL_PRICE
    def _ingest_macroeconomics_oil_price_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.OIL_PRICE,
            OilPriceSource.INVESTING,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        table_name = Table.OIL_PRICE.__qualname__.lower()

        self._logger.log_info(f'Start ingesting data in "{table_name}".')

        # Add logic for processing data here
        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Rename columns
            rename_map = {
                "Ngày": "date",
                "Lần cuối": "close",
                "Mở": "open",
                "Cao": "high",
                "Thấp": "low",
                "KL": "volume",
                "% Thay đổi": "change",
            }
            df = df.rename(columns=rename_map)

            # Convert 'date' to datetime
            df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["close", "open", "high", "low"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["volume"] = df["volume"].apply(parse_volume)

            # Handle "change" column: remove '%' and convert to float
            df["change"] = df["change"].astype(str).str.replace("%", "").astype(float)

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("date").reset_index(drop=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.OIL_PRICE.name,
            primary_keys=Table.OIL_PRICE.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish ingesting data in "{table_name}".')

    def _clean_macroeconomics_oil_price_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.OIL_PRICE,
            OilPriceSource.INVESTING,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.OIL_PRICE.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[CleanLayer.ORDER_BY([Table.OIL_PRICE.Column.DATE.value])],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.OIL_PRICE.name,
            primary_keys=Table.OIL_PRICE.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_oil_price_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.OIL_PRICE,
            OilPriceSource.INVESTING,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.OIL_PRICE.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_OIL_PRICE.name,
            primary_keys=Table.G_OIL_PRICE.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_oil_price(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics OIL_PRICE data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_oil_price_investing()

            case DataQuality.SILVER:
                self._clean_macroeconomics_oil_price_investing()

            case DataQuality.GOLD:
                self._transform_macroeconomics_oil_price_investing()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics OIL_PRICE data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.OIL_PRICE

    # region MACROECONOMICS.DOW_JONES
    def _ingest_macroeconomics_dow_jones_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.DOW_JONES,
            DowJonesSource.INVESTING,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        table_name = Table.DOW_JONES.__qualname__.lower()

        self._logger.log_info(f'Start ingesting data in "{table_name}".')

        # Add logic for processing data here
        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Rename columns
            rename_map = {
                "Ngày": "date",
                "Lần cuối": "close",
                "Mở": "open",
                "Cao": "high",
                "Thấp": "low",
                "KL": "volume",
                "% Thay đổi": "change",
            }
            df = df.rename(columns=rename_map)

            # Convert 'date' to datetime
            df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["close", "open", "high", "low"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["volume"] = df["volume"].apply(parse_volume)

            # Handle "change" column: remove '%' and convert to float
            df["change"] = df["change"].astype(str).str.replace("%", "").astype(float)

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("date").reset_index(drop=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.DOW_JONES.name,
            primary_keys=Table.DOW_JONES.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish ingesting data in "{table_name}".')

    def _clean_macroeconomics_dow_jones_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.DOW_JONES,
            DowJonesSource.INVESTING,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.DOW_JONES.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[CleanLayer.ORDER_BY([Table.DOW_JONES.Column.DATE.value])],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.DOW_JONES.name,
            primary_keys=Table.DOW_JONES.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_dow_jones_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.DOW_JONES,
            DowJonesSource.INVESTING,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.DOW_JONES.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_DOW_JONES.name,
            primary_keys=Table.G_DOW_JONES.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_dow_jones(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics DOW_JONES data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_dow_jones_investing()

            case DataQuality.SILVER:
                self._clean_macroeconomics_dow_jones_investing()

            case DataQuality.GOLD:
                self._transform_macroeconomics_dow_jones_investing()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics DOW_JONES data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.DOW_JONES

    # region MACROECONOMICS.NYSE_COMPOSITE
    def _ingest_macroeconomics_nyse_composite_yahoo_finance(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.NYSE_COMPOSITE,
            NYSECompositeSource.YAHOO_FINANCE,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        table_name = Table.NYSE_COMPOSITE.__qualname__.lower()

        self._logger.log_info(f'Start ingesting data in "{table_name}".')

        # Add logic for processing data here
        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Rename columns
            rename_map = {
                "Date": "date",
                "CloseClose price adjusted for splits.": "close",
                "Adj CloseAdjusted close price adjusted for splits and dividend and/or capital gain distributions.": "adj_close",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Volume": "volume",
            }
            df = df.rename(columns=rename_map)

            # Convert 'date' to datetime
            df["date"] = pd.to_datetime(df["date"], format="%b %d, %Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["close", "open", "high", "low", "adj_close"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["volume"] = df["volume"].astype(str).replace("-", np.nan)

            df["volume"] = df["volume"].apply(parse_volume)

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("date").reset_index(drop=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NYSE_COMPOSITE.name,
            primary_keys=Table.NYSE_COMPOSITE.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish ingesting data in "{table_name}".')

    def _clean_macroeconomics_nyse_composite_yahoo_finance(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.NYSE_COMPOSITE,
            NYSECompositeSource.YAHOO_FINANCE,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NYSE_COMPOSITE.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY([Table.NYSE_COMPOSITE.Column.DATE.value])
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NYSE_COMPOSITE.name,
            primary_keys=Table.NYSE_COMPOSITE.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_nyse_composite_yahoo_finance(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.NYSE_COMPOSITE,
            NYSECompositeSource.YAHOO_FINANCE,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NYSE_COMPOSITE.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_NYSE_COMPOSITE.name,
            primary_keys=Table.G_NYSE_COMPOSITE.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_nyse_composite(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics NYSE_COMPOSITE data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_nyse_composite_yahoo_finance()

            case DataQuality.SILVER:
                self._clean_macroeconomics_nyse_composite_yahoo_finance()

            case DataQuality.GOLD:
                self._transform_macroeconomics_nyse_composite_yahoo_finance()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics NYSE_COMPOSITE data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.NYSE_COMPOSITE

    # region MACROECONOMICS.SNP_500
    def _ingest_macroeconomics_snp_500_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.SNP_500,
            SNP500Source.YAHOO_FINANCE,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        table_name = Table.SNP_500.__qualname__.lower()

        self._logger.log_info(f'Start ingesting data in "{table_name}".')

        # Add logic for processing data here
        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Rename columns
            rename_map = {
                "Date": "date",
                "CloseClose price adjusted for splits.": "close",
                "Adj CloseAdjusted close price adjusted for splits and dividend and/or capital gain distributions.": "adj_close",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Volume": "volume",
            }
            df = df.rename(columns=rename_map)

            # Convert 'date' to datetime
            df["date"] = pd.to_datetime(df["date"], format="%b %d, %Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["close", "open", "high", "low", "adj_close"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["volume"] = df["volume"].astype(str).replace("-", np.nan)

            df["volume"] = df["volume"].apply(parse_volume)

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("date").reset_index(drop=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.SNP_500.name,
            primary_keys=Table.SNP_500.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish ingesting data in "{table_name}".')

    def _clean_macroeconomics_snp_500_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.SNP_500,
            SNP500Source.YAHOO_FINANCE,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.SNP_500.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[CleanLayer.ORDER_BY([Table.SNP_500.Column.DATE.value])],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.SNP_500.name,
            primary_keys=Table.SNP_500.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_snp_500_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.SNP_500,
            SNP500Source.YAHOO_FINANCE,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.SNP_500.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_SNP_500.name,
            primary_keys=Table.G_SNP_500.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_snp_500(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics SNP_500 data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_snp_500_investing()

            case DataQuality.SILVER:
                self._clean_macroeconomics_snp_500_investing()

            case DataQuality.GOLD:
                self._transform_macroeconomics_snp_500_investing()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics SNP_500 data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.SNP_500

    # region MACROECONOMICS.NASDAQ_COMPOSITE
    def _ingest_macroeconomics_nasdaq_composite_yahoo_finance(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.NASDAQ_COMPOSITE,
            NASDAQCompositeSource.YAHOO_FINANCE,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        table_name = Table.NASDAQ_COMPOSITE.__qualname__.lower()

        self._logger.log_info(f'Start ingesting data in "{table_name}".')

        # Add logic for processing data here
        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Rename columns
            rename_map = {
                "Date": "date",
                "CloseClose price adjusted for splits.": "close",
                "Adj CloseAdjusted close price adjusted for splits and dividend and/or capital gain distributions.": "adj_close",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Volume": "volume",
            }
            df = df.rename(columns=rename_map)

            # Convert 'date' to datetime
            df["date"] = pd.to_datetime(df["date"], format="%b %d, %Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["close", "open", "high", "low", "adj_close"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["volume"] = df["volume"].astype(str).replace("-", np.nan)

            df["volume"] = df["volume"].apply(parse_volume)

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("date").reset_index(drop=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NASDAQ_COMPOSITE.name,
            primary_keys=Table.NASDAQ_COMPOSITE.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish ingesting data in "{table_name}".')

    def _clean_macroeconomics_nasdaq_composite_yahoo_finance(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.NASDAQ_COMPOSITE,
            NASDAQCompositeSource.YAHOO_FINANCE,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NASDAQ_COMPOSITE.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY([Table.NASDAQ_COMPOSITE.Column.DATE.value])
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NASDAQ_COMPOSITE.name,
            primary_keys=Table.NASDAQ_COMPOSITE.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_nasdaq_composite_yahoo_finance(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.NASDAQ_COMPOSITE,
            NASDAQCompositeSource.YAHOO_FINANCE,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NASDAQ_COMPOSITE.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_NASDAQ_COMPOSITE.name,
            primary_keys=Table.G_NASDAQ_COMPOSITE.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_nasdaq_composite(
        self, data_quality: DataQuality
    ) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics NASDAQ_COMPOSITE data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_nasdaq_composite_yahoo_finance()

            case DataQuality.SILVER:
                self._clean_macroeconomics_nasdaq_composite_yahoo_finance()

            case DataQuality.GOLD:
                self._transform_macroeconomics_nasdaq_composite_yahoo_finance()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics NASDAQ_COMPOSITE data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.NASDAQ_COMPOSITE

    # region MACROECONOMICS.NASDAQ_100
    def _ingest_macroeconomics_nasdaq_100_yahoo_finance(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.NASDAQ_100,
            NASDAQ100Source.YAHOO_FINANCE,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        table_name = Table.NASDAQ_100.__qualname__.lower()

        self._logger.log_info(f'Start ingesting data in "{table_name}".')

        # Add logic for processing data here
        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Rename columns
            rename_map = {
                "Date": "date",
                "CloseClose price adjusted for splits.": "close",
                "Adj CloseAdjusted close price adjusted for splits and dividend and/or capital gain distributions.": "adj_close",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Volume": "volume",
            }
            df = df.rename(columns=rename_map)

            # Convert 'date' to datetime
            df["date"] = pd.to_datetime(df["date"], format="%b %d, %Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["close", "open", "high", "low", "adj_close"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["volume"] = df["volume"].astype(str).replace("-", np.nan)

            df["volume"] = df["volume"].apply(parse_volume)

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("date").reset_index(drop=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NASDAQ_100.name,
            primary_keys=Table.NASDAQ_100.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish ingesting data in "{table_name}".')

    def _clean_macroeconomics_nasdaq_100_yahoo_finance(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.NASDAQ_100,
            NASDAQ100Source.YAHOO_FINANCE,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NASDAQ_100.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY([Table.NASDAQ_100.Column.DATE.value])
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NASDAQ_100.name,
            primary_keys=Table.NASDAQ_100.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_macroeconomics_nasdaq_100_yahoo_finance(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.NASDAQ_100,
            NASDAQ100Source.YAHOO_FINANCE,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NASDAQ_100.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.G_NASDAQ_100.name,
            primary_keys=Table.G_NASDAQ_100.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_macroeconomics_nasdaq_100(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing macroeconomics NASDAQ_100 data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_macroeconomics_nasdaq_100_yahoo_finance()

            case DataQuality.SILVER:
                self._clean_macroeconomics_nasdaq_100_yahoo_finance()

            case DataQuality.GOLD:
                self._transform_macroeconomics_nasdaq_100_yahoo_finance()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing macroeconomics NASDAQ_100 data for "{data_quality.value}".'
        )

    # endregion MACROECONOMICS.NASDAQ_100

    # endregion MACROECONOMICS data process

    # region STOCK_MARKET data process

    # region STOCK_MARKET.MARKET
    def _process_stock_market_market_add_data(self) -> None:
        self._logger.log_info(
            f'Start ingesting data in "{Table.MARKET.__qualname__.lower()}".'
        )

        market_data = {
            Table.MARKET.Column.ID.value: [1, 2, 3],
            Table.MARKET.Column.CODE.value: ["HSX", "HNX", "UPCOM"],
            Table.MARKET.Column.NAME.value: [
                "Ho Chi Minh City Stock Exchange",
                "Hanoi Stock Exchange",
                "Unlisted Public Company Market",
            ],
        }

        df = pd.DataFrame(market_data)

        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.MARKET.name,
            primary_keys=Table.MARKET.primary_key,
            df=df,
        )

        self._logger.log_info(
            f'Finish ingesting data in "{Table.MARKET.__qualname__.lower()}".'
        )

    def _clean_stock_market_market(self) -> None:

        self._logger.log_info(
            f'Start cleaning data in table "{Table.MARKET.__qualname__.lower()}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.MARKET.name,
        )

        bronze_df.drop(
            columns=[
                Table.MARKET.Column.CREATE_DATE.value,
                Table.MARKET.Column.UPDATE_DATE.value,
                Table.MARKET.Column.DELETE_DATE.value,
            ],
            inplace=True,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[CleanLayer.ORDER_BY([Table.MARKET.Column.ID.value])],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.MARKET.name,
            primary_keys=Table.MARKET.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{Table.MARKET.__qualname__.lower()}".'
        )

    def _transform_stock_market_market(self) -> None:
        self._logger.log_info(
            f'Start transforming data in "{Table.MARKET.__qualname__.lower()}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.MARKET.name,
        )

        gold_df = silver_df.drop(
            columns=[
                Table.MARKET.Column.CREATE_DATE.value,
                Table.MARKET.Column.UPDATE_DATE.value,
                Table.MARKET.Column.DELETE_DATE.value,
            ]
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.MARKET.name,
            primary_keys=Table.MARKET.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in "{Table.MARKET.__qualname__.lower()}".'
        )

    def _process_stock_market_market(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing stock market MARKET data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._process_stock_market_market_add_data()

            case DataQuality.SILVER:
                self._clean_stock_market_market()

            case DataQuality.GOLD:
                self._transform_stock_market_market()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info("Finish processing stock market MARKET data.")

    # endregion STOCK MARKET.MARKET

    # region STOCK_MARKET.VN_INDEX
    def _ingest_stock_market_vn_index_price(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_INDEX_PRICE,
        )

        folder_path = f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}"

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path, encoding="utf-8")

        vn_index_df = df.drop(columns=["code"])

        # Extract percentage
        vn_index_df["percent_change"] = (
            vn_index_df["change"]
            .str.extract(r"\(([+-]?\d+\.\d+)%\)", expand=False)
            .astype(float)
        )

        vn_index_df["change"] = (
            vn_index_df["change"]
            .str.extract(r"([+-]?\d+\.\d+)", expand=False)
            .astype(float)
        )

        vn_index_df["date"] = pd.to_datetime(vn_index_df["date"], format="%d/%m/%Y")
        vn_index_df = vn_index_df.sort_values(by="date").reset_index(drop=True)

        # DATE
        vn_index_df["date"] = pd.to_datetime(vn_index_df["date"]).dt.date

        # DECIMAL columns
        decimal_cols = [
            "adjust",
            "close",
            "change",
            "percent_change",
            "matching_value",
            "negotiate_value",
            "open",
            "high",
            "low",
        ]

        for col in decimal_cols:
            vn_index_df[col] = (
                vn_index_df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .pipe(pd.to_numeric, errors="raise")
                .astype("Float64")
            )

        # BIGINT columns
        bigint_cols = [
            "matching_volume",
            "negotiate_volume",
        ]

        for col in bigint_cols:
            vn_index_df[col] = (
                vn_index_df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .pipe(pd.to_numeric, errors="raise")
                .astype("Int64")
            )

        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.B_STOCK_MARKET_PRICE.name,
            primary_keys=Table.B_STOCK_MARKET_PRICE.primary_key,
            df=vn_index_df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _ingest_stock_market_vn_index_order(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_INDEX_ORDER,
        )

        folder_path = f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}"

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path, encoding="utf-8")
        rename_map = {
            "Mã": "code",
            "Ngày": "date",
            "Thay đổi": "close_change",
            "Số lệnh mua": "number_of_buy_orders",
            "Khối lượng mua": "buy_volume",
            "KLTB/lệnh mua": "average_volume_per_buy_order",
            "Số lệnh bán": "number_of_sell_orders",
            "Khối lượng bán": "sell_volume",
            "KLTB/lệnh bán": "average_volume_per_sell_order",
            "Khối lượng ròng": "net_volume",
        }
        vn_index_df = df.rename(columns=rename_map)
        vn_index_df = vn_index_df.drop(columns=["code"])
        vn_index_df["close"] = (
            vn_index_df["close_change"].str.extract(r"([\d\.]+)").astype("Float64")
        )

        for col in [
            "net_volume",
        ]:
            vn_index_df[col] = (
                vn_index_df[col].str.replace(".", "", regex=False).astype("Int64")
            )

        vn_index_df.drop(columns=["close_change"], inplace=True)
        vn_index_df["date"] = pd.to_datetime(vn_index_df["date"], format="%d/%m/%Y")
        vn_index_df = vn_index_df.sort_values(by="date").reset_index(drop=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.B_VN_INDEX_ORDER.name,
            primary_keys=Table.B_VN_INDEX_ORDER.primary_key,
            df=vn_index_df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_stock_market_vn_index(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_INDEX,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        join_model_list = [
            JoinModel(
                join_type=SqlJoinType.INNER_JOIN,
                schema_left="stock_market",
                schema_right="stock_market",
                table_left=Table.B_STOCK_MARKET_PRICE.name,
                table_right=Table.B_VN_INDEX_ORDER.name,
                column_left="date",
                column_right="date",
            )
        ]

        vn_index_bronze_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.B_STOCK_MARKET_PRICE.name,
            join_model_list=join_model_list,
        )
        vn_index_bronze_df = vn_index_bronze_df.loc[
            :, ~vn_index_bronze_df.columns.duplicated()
        ]

        silver_df = self._clean(
            df=vn_index_bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY([Table.S_VN_INDEX.Column.DATE.value])
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.S_VN_INDEX.name,
            primary_keys=Table.S_VN_INDEX.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_stock_market_vn_index(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_INDEX,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.S_VN_INDEX.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.G_VN_INDEX.name,
            primary_keys=Table.G_VN_INDEX.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_stock_market_vn_index(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing stock market VN_INDEX data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                # self._ingest_stock_market_vn_index_price()
                self._ingest_stock_market_vn_index_order()

            case DataQuality.SILVER:
                self._clean_stock_market_vn_index()

            case DataQuality.GOLD:
                self._transform_stock_market_vn_index()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing stock market VN_INDEX data for "{data_quality.value}".'
        )

    # endregion STOCK_MARKET.VN_INDEX

    # region STOCK_MARKET.HNX_INDEX
    def _process_stock_market_hnx_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_HNX_INDEX,
            VnHnxIndexSource.CAFEF,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path, encoding="utf-8")
        hnx_index_df = df[df["<Ticker>"] == "HNX-INDEX"]
        rename_map = {
            "<Ticker>": "ticker",
            "<DTYYYYMMDD>": "date",
            "<Open>": "open",
            "<High>": "high",
            "<Low>": "low",
            "<Close>": "close",
            "<Volume>": "volume",
        }
        hnx_index_df = hnx_index_df.rename(columns=rename_map)
        hnx_index_df.drop(columns=["ticker"], inplace=True)
        hnx_index_df["date"] = pd.to_datetime(hnx_index_df["date"], format="%Y%m%d")
        hnx_index_df = hnx_index_df.sort_values(by="date").reset_index(drop=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.HNX_INDEX.name,
            primary_keys=Table.HNX_INDEX.primary_key,
            df=hnx_index_df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_stock_market_hnx_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_HNX_INDEX,
            VnHnxIndexSource.CAFEF,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.HNX_INDEX.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[CleanLayer.ORDER_BY([Table.HNX_INDEX.Column.DATE.value])],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.HNX_INDEX.name,
            primary_keys=Table.HNX_INDEX.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_stock_market_hnx_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_HNX_INDEX,
            VnHnxIndexSource.CAFEF,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.HNX_INDEX.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.HNX_INDEX.name,
            primary_keys=Table.HNX_INDEX.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_stock_market_hnx_index(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing stock market HNX_INDEX data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._process_stock_market_hnx_index_cafef()

            case DataQuality.SILVER:
                self._clean_stock_market_hnx_index_cafef()

            case DataQuality.GOLD:
                self._transform_stock_market_hnx_index_cafef()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing stock market HNX_INDEX data for "{data_quality.value}".'
        )

    # endregion STOCK_MARKET.HNX_INDEX

    # region STOCK_MARKET.VN_30_INDEX
    def _process_stock_market_vn_30_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_30_INDEX,
            Vn30IndexSource.CAFEF,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path, encoding="utf-8")

        df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")

        df["adjusted_close"] = df["adjusted_close"].str.replace(",", "", regex=False)
        df["adjusted_close"] = pd.to_numeric(df["adjusted_close"], errors="coerce")

        df[["change_value", "change_percentage"]] = df["change"].str.extract(
            r"([-\d.]+)\s*\(([-\d.]+)\s*%\)"
        )
        df["change_value"] = df["change_value"].astype(float)
        df["change_percentage"] = df["change_percentage"].astype(float)

        df = df.drop(columns=["change"])

        df["matched_volume"] = (
            df["matched_volume"].str.replace(",", "", regex=False).astype(int)
        )
        df["matched_value"] = (
            df["matched_value"].str.replace(",", "", regex=False).astype(float)
        )
        df["negotiated_volume"] = (
            df["negotiated_volume"].str.replace(",", "", regex=False).astype(int)
        )
        df["negotiated_value"] = (
            df["negotiated_value"].str.replace(",", "", regex=False).astype(float)
        )
        df["open"] = df["open"].str.replace(",", "", regex=False).astype(float)
        df["high"] = df["high"].str.replace(",", "", regex=False).astype(float)
        df["low"] = df["low"].str.replace(",", "", regex=False).astype(float)
        df = df.sort_values(by="date", ascending=True, ignore_index=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.VN_30_INDEX.name,
            primary_keys=Table.VN_30_INDEX.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_stock_market_vn_30_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_30_INDEX,
            Vn30IndexSource.CAFEF,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.VN_30_INDEX.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY([Table.VN_30_INDEX.Column.DATE.value])
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.VN_30_INDEX.name,
            primary_keys=Table.VN_30_INDEX.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_stock_market_vn_30_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_30_INDEX,
            Vn30IndexSource.CAFEF,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.VN_30_INDEX.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.VN_30_INDEX.name,
            primary_keys=Table.VN_30_INDEX.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_stock_market_vn_30_index(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing stock market VN_30_INDEX data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._process_stock_market_vn_30_index_cafef()

            case DataQuality.SILVER:
                self._clean_stock_market_vn_30_index_cafef()

            case DataQuality.GOLD:
                self._transform_stock_market_vn_30_index_cafef()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing stock market VN_30_INDEX data for "{data_quality.value}".'
        )

    # endregion STOCK_MARKET.VN_30_INDEX

    # region STOCK_MARKET.VN_100_INDEX
    def _process_stock_market_vn_100_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_100_INDEX,
            Vn100IndexSource.CAFEF,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path, encoding="utf-8")

        df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")

        df["adjusted_close"] = df["adjusted_close"].str.replace(",", "", regex=False)
        df["adjusted_close"] = pd.to_numeric(df["adjusted_close"], errors="coerce")

        df[["change_value", "change_percentage"]] = df["change"].str.extract(
            r"([-\d.]+)\s*\(([-\d.]+)\s*%\)"
        )
        df["change_value"] = df["change_value"].astype(float)
        df["change_percentage"] = df["change_percentage"].astype(float)

        df = df.drop(columns=["change"])

        df["matched_volume"] = (
            df["matched_volume"].str.replace(",", "", regex=False).astype(int)
        )
        df["matched_value"] = (
            df["matched_value"].str.replace(",", "", regex=False).astype(float)
        )
        df["negotiated_volume"] = (
            df["negotiated_volume"].str.replace(",", "", regex=False).astype(int)
        )
        df["negotiated_value"] = (
            df["negotiated_value"].str.replace(",", "", regex=False).astype(float)
        )
        df["open"] = df["open"].str.replace(",", "", regex=False).astype(float)
        df["high"] = df["high"].str.replace(",", "", regex=False).astype(float)
        df["low"] = df["low"].str.replace(",", "", regex=False).astype(float)
        df = df.sort_values(by="date", ascending=True, ignore_index=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.VN_100_INDEX.name,
            primary_keys=Table.VN_100_INDEX.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_stock_market_vn_100_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_100_INDEX,
            Vn100IndexSource.CAFEF,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.VN_100_INDEX.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY([Table.VN_100_INDEX.Column.DATE.value])
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.VN_100_INDEX.name,
            primary_keys=Table.VN_100_INDEX.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_stock_market_vn_100_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_100_INDEX,
            Vn100IndexSource.CAFEF,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.VN_100_INDEX.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.VN_100_INDEX.name,
            primary_keys=Table.VN_100_INDEX.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_stock_market_vn_100_index(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing stock market VN_100_INDEX data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._process_stock_market_vn_100_index_cafef()

            case DataQuality.SILVER:
                self._clean_stock_market_vn_100_index_cafef()

            case DataQuality.GOLD:
                self._transform_stock_market_vn_100_index_cafef()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing stock market VN_100_INDEX data for "{data_quality.value}".'
        )

    # endregion STOCK_MARKET.VN_100_INDEX

    # region STOCK_MARKET.HNX_30_INDEX
    def _process_stock_market_hnx_30_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.HNX_30_INDEX,
            Hnx30IndexSource.CAFEF,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path, encoding="utf-8")

        df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")

        df["adjusted_close"] = df["adjusted_close"].str.replace(",", "", regex=False)
        df["adjusted_close"] = pd.to_numeric(df["adjusted_close"], errors="coerce")

        df[["change_value", "change_percentage"]] = df["change"].str.extract(
            r"([-\d.]+)\s*\(([-\d.]+)\s*%\)"
        )
        df["change_value"] = df["change_value"].astype(float)
        df["change_percentage"] = df["change_percentage"].astype(float)

        df = df.drop(columns=["change"])

        df["matched_volume"] = (
            df["matched_volume"].str.replace(",", "", regex=False).astype(int)
        )
        df["matched_value"] = (
            df["matched_value"].str.replace(",", "", regex=False).astype(float)
        )
        df["negotiated_volume"] = (
            df["negotiated_volume"].str.replace(",", "", regex=False).astype(int)
        )
        df["negotiated_value"] = (
            df["negotiated_value"].str.replace(",", "", regex=False).astype(float)
        )
        df["open"] = (
            df["open"].astype(str).str.replace(",", "", regex=False).astype(float)
        )
        df["high"] = (
            df["high"].astype(str).str.replace(",", "", regex=False).astype(float)
        )
        df["low"] = (
            df["low"].astype(str).str.replace(",", "", regex=False).astype(float)
        )
        df = df.sort_values(by="date", ascending=True, ignore_index=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.HNX_30_INDEX.name,
            primary_keys=Table.HNX_30_INDEX.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_stock_market_hnx_30_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.HNX_30_INDEX,
            Hnx30IndexSource.CAFEF,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.HNX_30_INDEX.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY([Table.HNX_30_INDEX.Column.DATE.value])
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.HNX_30_INDEX.name,
            primary_keys=Table.HNX_30_INDEX.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_stock_market_hnx_30_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.HNX_30_INDEX,
            Hnx30IndexSource.CAFEF,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.HNX_30_INDEX.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.HNX_30_INDEX.name,
            primary_keys=Table.HNX_30_INDEX.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_stock_market_hnx_30_index(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing stock market HNX_30_INDEX data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._process_stock_market_hnx_30_index_cafef()

            case DataQuality.SILVER:
                self._clean_stock_market_hnx_30_index_cafef()

            case DataQuality.GOLD:
                self._transform_stock_market_hnx_30_index_cafef()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing stock market HNX_30_INDEX data for "{data_quality.value}".'
        )

    # endregion STOCK_MARKET.HNX_30_INDEX

    # region STOCK_MARKET.UPCOM_INDEX
    def _process_stock_market_upcom_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.UPCOM_INDEX,
            UpcomIndexSource.CAFEF,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start ingesting data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path, encoding="utf-8")

        df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")

        df["adjusted_close"] = df["adjusted_close"].str.replace(",", "", regex=False)
        df["adjusted_close"] = pd.to_numeric(df["adjusted_close"], errors="coerce")

        df[["change_value", "change_percentage"]] = df["change"].str.extract(
            r"([-\d.]+)\s*\(([-\d.]+)\s*%\)"
        )
        df["change_value"] = df["change_value"].astype(float)
        df["change_percentage"] = df["change_percentage"].astype(float)

        df = df.drop(columns=["change"])

        df["matched_volume"] = (
            df["matched_volume"].str.replace(",", "", regex=False).astype(int)
        )
        df["matched_value"] = (
            df["matched_value"].str.replace(",", "", regex=False).astype(float)
        )
        df["negotiated_volume"] = (
            df["negotiated_volume"].str.replace(",", "", regex=False).astype(int)
        )
        df["negotiated_value"] = (
            df["negotiated_value"].str.replace(",", "", regex=False).astype(float)
        )
        df["open"] = (
            df["open"].astype(str).str.replace(",", "", regex=False).astype(float)
        )
        df["high"] = (
            df["high"].astype(str).str.replace(",", "", regex=False).astype(float)
        )
        df["low"] = (
            df["low"].astype(str).str.replace(",", "", regex=False).astype(float)
        )
        df = df.sort_values(by="date", ascending=True, ignore_index=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.UPCOM_INDEX.name,
            primary_keys=Table.UPCOM_INDEX.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish ingesting data in "{file_path}".')

    def _clean_stock_market_upcom_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.UPCOM_INDEX,
            UpcomIndexSource.CAFEF,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.UPCOM_INDEX.name,
        )

        silver_df = self._clean(
            df=bronze_df,
            clean_layer_list=[
                CleanLayer.ORDER_BY([Table.UPCOM_INDEX.Column.DATE.value])
            ],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.UPCOM_INDEX.name,
            primary_keys=Table.UPCOM_INDEX.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_stock_market_upcom_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.UPCOM_INDEX,
            UpcomIndexSource.CAFEF,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.UPCOM_INDEX.name,
        )

        gold_df = make_date_time_index_for_dataframe(df=silver_df)
        gold_df = standardize_time_frame(df=gold_df)

        cols_to_interpolate = gold_df.columns.difference(["date"])
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].apply(
            pd.to_numeric, errors="coerce"
        )
        gold_df[cols_to_interpolate] = gold_df[cols_to_interpolate].interpolate(
            method="linear"
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.UPCOM_INDEX.name,
            primary_keys=Table.UPCOM_INDEX.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_stock_market_upcom_index(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing stock market UPCOM_INDEX data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._process_stock_market_upcom_index_cafef()

            case DataQuality.SILVER:
                self._clean_stock_market_upcom_index_cafef()

            case DataQuality.GOLD:
                self._transform_stock_market_upcom_index_cafef()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing stock market UPCOM_INDEX data for "{data_quality.value}".'
        )

    # endregion STOCK_MARKET.UPCOM_INDEX

    # endregion STOCK_MARKET data process

    # region ENTERPRISE data process

    # region ENTERPRISE.STOCK_INFORMATION
    def _ingest_enterprise_stock_cafef(self) -> None:
        key_1 = (
            ScrapeMainType.ENTERPRISE,
            EnterpriseSubType.DAILY_PRICE,
            DailyPriceSource.CAFEF,
        )

        key_2 = (
            ScrapeMainType.ENTERPRISE,
            EnterpriseSubType.STOCK_INFORMATION,
            StockInformationSource.CAFEF,
        )

        folder_path_1 = f"{SCRAPER_BRONZE_DATA_DIR}/{key_1[0].value}/{key_1[1].value}/{key_1[2].value}"
        folder_path_2 = f"{SCRAPER_BRONZE_DATA_DIR}/{key_2[0].value}/{key_2[1].value}/{key_2[2].value}"

        # 1. Get file lists
        base_stock_files = get_all_file_names_with_extensions(
            self._logger,
            folder_path=folder_path_1,
            extensions=[FileExtension.CSV],
        )
        stock_information_files = get_all_file_names_with_extensions(
            self._logger,
            folder_path=folder_path_2,
            extensions=[FileExtension.CSV],
        )

        # 2. Find latest stock files per exchange
        pattern = re.compile(r"(HNX|HSX|UPCOM)_upto_(\d{8})\.csv")
        latest_files = {}

        for file in base_stock_files:
            match = pattern.search(os.path.basename(file))
            if match:
                exchange, date_str = match.groups()
                date = datetime.strptime(date_str, "%Y%m%d")
                if (
                    exchange not in latest_files
                    or date > latest_files[exchange]["date"]
                ):
                    latest_files[exchange] = {"file": file, "date": date}

        # 3. Validate and collect file paths
        required_exchanges = ["HSX", "HNX", "UPCOM"]
        file_paths = {}

        for exchange in required_exchanges:
            file_path = latest_files.get(exchange, {}).get("file")
            if not file_path or not os.path.isfile(file_path):
                self._logger.log_error(
                    f'{exchange} data file not found in "{folder_path_1}".'
                )
                return
            file_paths[exchange] = file_path

        table_name = Table.STOCK.__qualname__.lower()
        self._logger.log_info(f'Start ingesting data in "{table_name}".')

        # 4. Load stock information data efficiently
        stock_info_frames = [
            pd.read_csv(file, encoding="utf-8")
            for file in stock_information_files
            if re.search(r"cafef_upto_\d+_\d+\.csv$", file)
        ]
        stock_infomation_df = (
            pd.concat(stock_info_frames, ignore_index=True)
            if stock_info_frames
            else pd.DataFrame()
        )
        stock_infomation_df.columns = (
            stock_infomation_df.columns.str.lower().str.replace(" ", "_")
        )

        # 6. Create base DataFrame for stocks
        base_dfs = []
        for market_code, stock_market_path in file_paths.items():
            base_df = pd.read_csv(stock_market_path)
            base_df["<Ticker>"] = base_df["<Ticker>"].astype("string")

            # Skip derivatives
            base_df = base_df[base_df["<Ticker>"].str.len() == 3]

            base_stock_df = pd.DataFrame(
                {
                    Table.STOCK.Column.CODE.value: base_df["<Ticker>"]
                    .dropna()
                    .unique(),
                    Table.STOCK.Column.MARKET_ID.value: self._get_market_id(
                        market_code
                    ),
                }
            )
            base_dfs.append(base_stock_df)

        overall_df = pd.concat(base_dfs, ignore_index=True).sort_values(
            by=Table.STOCK.primary_key, ignore_index=True
        )

        # 7. Merge with stock information
        overall_df = pd.merge(
            overall_df, stock_infomation_df, on=Table.STOCK.primary_key, how="left"
        )

        # 8. Calculate outstanding_rate safely
        listed = overall_df["listed_shares"].astype(float)
        outstanding = overall_df["outstanding_shares"].astype(float)
        overall_df["outstanding_rate"] = np.where(
            listed > 0, outstanding / listed, np.nan
        )

        # 9. Save to database
        self._save_pandas_table_to_database(
            schema_name=Schema.ENTERPRISE.value,
            table_name=Table.STOCK.name,
            primary_keys=Table.STOCK.primary_key,
            df=overall_df,
        )

        self._logger.log_info(f'Finish ingesting data in "{table_name}".')

    def _clean_enterprise_stock_cafef(self) -> None:
        key = (
            ScrapeMainType.ENTERPRISE,
            EnterpriseSubType.STOCK_INFORMATION,
            StockInformationSource.CAFEF,
        )

        self._logger.log_info(
            f'Start cleaning data in table "{format_key_for_table(key)}".'
        )

        # Add logic for cleaning data here
        self._select_database(DataQuality.BRONZE.value)

        bronze_df = self._select(
            schema_name=Schema.ENTERPRISE.value,
            table_name=Table.STOCK.name,
        )

        silver_df = bronze_df.drop(
            columns=[
                Table.STOCK.Column.ID.value,
                Table.STOCK.Column.CREATE_DATE.value,
                Table.STOCK.Column.UPDATE_DATE.value,
                Table.STOCK.Column.DELETE_DATE.value,
            ]
        )

        silver_df = self._clean(
            df=silver_df,
            clean_layer_list=[CleanLayer.ORDER_BY([Table.STOCK.Column.CODE.value])],
        )

        self._select_database(DataQuality.SILVER.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.ENTERPRISE.value,
            table_name=Table.STOCK.name,
            primary_keys=Table.STOCK.primary_key,
            df=silver_df,
        )

        self._logger.log_info(
            f'Finish cleaning data in table "{format_key_for_table(key)}".'
        )

    def _transform_enterprise_stock_cafef(self) -> None:
        key = (
            ScrapeMainType.ENTERPRISE,
            EnterpriseSubType.STOCK_INFORMATION,
            StockInformationSource.CAFEF,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here
        self._select_database(DataQuality.SILVER.value)
        silver_df = self._select(
            schema_name=Schema.ENTERPRISE.value,
            table_name=Table.STOCK.name,
        )

        gold_df = silver_df.drop(
            columns=[
                Table.STOCK.Column.ID.value,
                Table.STOCK.Column.CREATE_DATE.value,
                Table.STOCK.Column.UPDATE_DATE.value,
                Table.STOCK.Column.DELETE_DATE.value,
            ]
        )

        self._select_database(DataQuality.GOLD.value)
        self._save_pandas_table_to_database(
            schema_name=Schema.ENTERPRISE.value,
            table_name=Table.STOCK.name,
            primary_keys=Table.STOCK.primary_key,
            df=gold_df,
        )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_enterprise_stock(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing enterprise STOCK data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_enterprise_stock_cafef()

            case DataQuality.SILVER:
                self._clean_enterprise_stock_cafef()

            case DataQuality.GOLD:
                self._transform_enterprise_stock_cafef()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing enterprise STOCK data for "{data_quality.value}".'
        )

    # endregion ENTERPRISE.STOCK_INFORMATION

    # region ENTERPRISE.DAILY_PRICE
    def _ingest_enterprise_daily_price_cafef(self) -> None:
        key = (
            ScrapeMainType.ENTERPRISE,
            EnterpriseSubType.DAILY_PRICE,
            DailyPriceSource.CAFEF,
        )

        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_paths = get_all_file_names_with_extensions(
            self._logger,
            folder_path=folder_path,
            extensions=[FileExtension.CSV],
        )

        if len(file_paths) < 3:
            self._logger.log_info(
                "Files found: " + ", ".join(f"`{path}`" for path in file_paths.values())
            )
            self._logger.log_warning(
                f'Not enough data files in "{folder_path}". Expected 03 .csv files, found {len(file_paths)}.'
            )
            return

        self._logger.log_info(f'Start ingesting data in "{folder_path}".')

        for file_path in file_paths:
            df = pd.read_csv(file_path, encoding="utf-8")
            df["<Ticker>"] = df["<Ticker>"].astype("string")
            df["<DTYYYYMMDD>"] = pd.to_datetime(
                df["<DTYYYYMMDD>"], format="%Y%m%d", errors="coerce"
            )
            df["<Open>"] = pd.to_numeric(df["<Open>"], errors="coerce")
            df["<High>"] = pd.to_numeric(df["<High>"], errors="coerce")
            df["<Low>"] = pd.to_numeric(df["<Low>"], errors="coerce")
            df["<Close>"] = pd.to_numeric(df["<Close>"], errors="coerce")
            df["<Volume>"] = pd.to_numeric(df["<Volume>"], errors="coerce")

            market_code = os.path.basename(file_path).split("_")[0]

            daily_price_df = pd.DataFrame(
                {
                    Table.DAILY_PRICE.Column.DATE.value: df["<DTYYYYMMDD>"],
                    Table.DAILY_PRICE.Column.CODE.value: df["<Ticker>"],
                    Table.DAILY_PRICE.Column.MARKET_ID.value: self._get_market_id(
                        market_code
                    ),
                    Table.DAILY_PRICE.Column.OPEN.value: df["<Open>"],
                    Table.DAILY_PRICE.Column.HIGH.value: df["<High>"],
                    Table.DAILY_PRICE.Column.LOW.value: df["<Low>"],
                    Table.DAILY_PRICE.Column.CLOSE.value: df["<Close>"],
                    Table.DAILY_PRICE.Column.VOLUME.value: df["<Volume>"],
                }
            )

            daily_price_df = daily_price_df.dropna(subset=["code"])

            year_list = self._get_year_list_from_start(SCRAPER_START_DATE)

            # Remove current year
            current_year = year_list[-1]
            year_list = year_list[:-1]

            # Remove years already ingested
            market_df = self._get_market_df()
            process_year = market_df[
                market_df[Table.MARKET.Column.CODE.value] == market_code
            ][Table.MARKET.Column.SAVE_PROGRESS_YEAR.value].item()

            if process_year is None:
                process_year = SCRAPER_START_DATE.year - 1

            year_list = [year for year in year_list if year > process_year]

            for year in year_list:
                self._logger.log_info(
                    f'Ingesting data for market "{market_code}" in year "{year}".'
                )
                self._save_pandas_table_to_database(
                    schema_name=Schema.ENTERPRISE.value,
                    table_name=Table.DAILY_PRICE.name,
                    primary_keys=Table.DAILY_PRICE.primary_key,
                    df=daily_price_df[
                        daily_price_df[Table.DAILY_PRICE.Column.DATE.value].dt.year
                        == year
                    ],
                )

                self._database_driver.update(
                    schema_name=Schema.STOCK_MARKET.value,
                    table_name=Table.MARKET.name,
                    update_record=Record(
                        data_dto_list=[
                            DataModel(
                                column_name=Table.MARKET.Column.SAVE_PROGRESS_YEAR.value,
                                value=year,
                                data_type=DataType.INT,
                            )
                        ]
                    ),
                    conditions=[
                        Condition(
                            column=Table.MARKET.Column.CODE.value,
                            operator=SqlOperator.EQUAL_TO,
                            value=market_code,
                            data_type=DataType.VARCHAR,
                        )
                    ],
                )

            # Ingest current year
            self._save_pandas_table_to_database(
                schema_name=Schema.ENTERPRISE.value,
                table_name=Table.DAILY_PRICE.name,
                primary_keys=Table.DAILY_PRICE.primary_key,
                df=daily_price_df[
                    daily_price_df[Table.DAILY_PRICE.Column.DATE.value].dt.year
                    == current_year
                ],
            )

    def _transform_enterprise_daily_price_cafef(self) -> None:
        key = (
            ScrapeMainType.ENTERPRISE,
            EnterpriseSubType.DAILY_PRICE,
            DailyPriceSource.CAFEF,
        )

        self._logger.log_info(
            f'Start transforming data in table "{format_key_for_table(key)}".'
        )

        # Add logic for transforming data here

        # Get stock code from DB
        if len(STOCK_CODES_TO_BE_EXPORTED_TO_GOLD_DB) == 0:
            self.logger.error(
                "No stock code to be exported to gold database. Please check the database."
            )
            return

        available_stock_codes = []
        for required_stock_code in STOCK_CODES_TO_BE_EXPORTED_TO_GOLD_DB:
            self._select_database(DataQuality.GOLD.value)
            stock_gold_df = self._select(
                schema_name=Schema.ENTERPRISE.value,
                table_name=Table.STOCK.name,
                columns=[Table.STOCK.Column.CODE.value],
                conditions=[
                    Condition(
                        column=Table.STOCK.Column.CODE.value,
                        operator=SqlOperator.EQUAL_TO,
                        value=str.upper(required_stock_code),
                        data_type=DataType.VARCHAR,
                    )
                ],
            )

            stock_code = None
            if stock_gold_df is not None and not stock_gold_df.empty:
                stock_code = stock_gold_df.squeeze()
                available_stock_codes.append(stock_code)

            if not stock_code:
                self.logger.error(
                    f"Cannot find {stock_code} stock in database. Please check the database."
                )
                continue

        if len(available_stock_codes) == 0:
            self.logger.error(
                "No stock code to be exported to gold database. Please check the database."
            )
            return

        for stock_code in available_stock_codes:
            self._select_database(DataQuality.BRONZE.value)
            daily_price_bronze_df = self._select(
                schema_name=Schema.ENTERPRISE.value,
                table_name=Table.DAILY_PRICE.name,
                conditions=[
                    Condition(
                        column=Table.STOCK.Column.CODE.value,
                        operator=SqlOperator.EQUAL_TO,
                        value=str.upper(stock_code),
                        data_type=DataType.VARCHAR,
                    )
                ],
            )

            daily_price_gold_df = self._clean(
                df=daily_price_bronze_df,
                clean_layer_list=[
                    CleanLayer.ORDER_BY([Table.DAILY_PRICE.Column.DATE.value])
                ],
            )

            self._select_database(DataQuality.GOLD.value)
            self._save_pandas_table_to_database(
                schema_name=Schema.ENTERPRISE.value,
                table_name=stock_code,
                primary_keys=["date"],
                df=daily_price_gold_df,
            )

        self._logger.log_info(
            f'Finish transforming data in table "{format_key_for_table(key)}".'
        )

    def _process_enterprise_daily_price(self, data_quality: DataQuality) -> None:
        self._logger.log_info(
            f'Start processing enterprise DAILY PRICE data for "{data_quality.value}".'
        )

        match data_quality:
            case DataQuality.BRONZE:
                self._ingest_enterprise_daily_price_cafef()

            case DataQuality.SILVER:
                pass

            case DataQuality.GOLD:
                self._transform_enterprise_daily_price_cafef()

            case _:
                raise ValueError(f'Invalid data quality: "{data_quality.value}"')

        self._logger.log_info(
            f'Finish processing enterprise DAILY PRICE data for "{data_quality.value}".'
        )

    # endregion ENTERPRISE.DAILY_PRICE

    # endregion ENTERPRISE data process

    def _process_data(self, data_quality: DataQuality) -> None:
        self._logger.log_info(f'Start processing data for "{data_quality.value}".')

        # Macroeconomics
        # self._process_macroeconomics_gdp(data_quality)
        # self._process_macroeconomics_cpi(data_quality)
        # self._process_macroeconomics_ppi(data_quality)
        # self._process_macroeconomics_ipi(data_quality)
        # self._process_macroeconomics_xpi(data_quality)
        # self._process_macroeconomics_mpi(data_quality)
        # self._process_macroeconomics_population(data_quality)
        # self._process_macroeconomics_labor(data_quality)
        # self._process_macroeconomics_retail(data_quality)
        # self._process_macroeconomics_pmi(data_quality)
        # self._process_macroeconomics_iip(data_quality)
        # self._process_macroeconomics_ipv(data_quality)
        # self._process_macroeconomics_mip(data_quality)
        # self._process_macroeconomics_fa_by_house_types(data_quality)
        # self._process_macroeconomics_it_bop(data_quality)
        # self._process_macroeconomics_tsbr(data_quality)
        # self._process_macroeconomics_tsbe(data_quality)
        # self._process_macroeconomics_gd(data_quality)
        # self._process_macroeconomics_brd(data_quality)
        # self._process_macroeconomics_iisd(data_quality)
        # self._process_macroeconomics_treg(data_quality)
        # self._process_macroeconomics_credit(data_quality)
        # self._process_macroeconomics_mobilization(data_quality)
        # self._process_macroeconomics_exchange_rate(data_quality)
        # self._process_macroeconomics_iir(data_quality)
        # self._process_macroeconomics_rrrr(data_quality)
        # self._process_macroeconomics_fdi_sector(data_quality)
        # self._process_macroeconomics_fdi_rd(data_quality)
        # self._process_macroeconomics_export(data_quality)
        # self._process_macroeconomics_import(data_quality)
        # self._process_macroeconomics_gold_price(data_quality)
        # self._process_macroeconomics_oil_price(data_quality)
        # self._process_macroeconomics_dow_jones(data_quality)
        # self._process_macroeconomics_nyse_composite(data_quality)
        # self._process_macroeconomics_snp_500(data_quality)
        # self._process_macroeconomics_nasdaq_composite(data_quality)
        # self._process_macroeconomics_nasdaq_100(data_quality)

        # # Stock market
        # self._process_stock_market_market(data_quality)
        self._process_stock_market_vn_index(data_quality)
        # self._process_stock_market_hnx_index(data_quality)
        # self._process_stock_market_vn_30_index(data_quality)
        # self._process_stock_market_vn_100_index(data_quality)
        # self._process_stock_market_hnx_30_index(data_quality)
        # self._process_stock_market_upcom_index(data_quality)

        # # Enterprise
        # self._process_enterprise_stock(data_quality)
        # self._process_enterprise_daily_price(data_quality)

        self._logger.log_info(f'Finish processing data for "{data_quality.value}".')

    def ingest_bronze_data(self) -> None:

        if self._switch_handler.is_enabled("data_preprocessor", "data_quality_bronze"):
            try:
                self._connect_to_database(DataQuality.BRONZE)
                self._create_schemas(DataQuality.BRONZE)
                self._create_tables(DataQuality.BRONZE)
                # self._process_data(DataQuality.BRONZE)

            except Exception as e:
                self._logger.log_error(
                    f"Error preprocessing `{DataQuality.BRONZE.value}` data: {e}"
                )

            finally:
                self._database_driver.disconnect()

    def ingest_silver_data(self) -> None:

        if self._switch_handler.is_enabled("data_preprocessor", "data_quality_silver"):
            try:
                self._connect_to_database(DataQuality.SILVER)
                self._create_schemas(DataQuality.SILVER)
                self._create_tables(DataQuality.SILVER)
                self._process_data(DataQuality.SILVER)

            except Exception as e:
                self._logger.log_error(
                    f"Error preprocessing `{DataQuality.SILVER.value}` data: {e}"
                )

            finally:
                self._database_driver.disconnect()

    def ingest_gold_data(self) -> None:

        if self._switch_handler.is_enabled("data_preprocessor", "data_quality_gold"):
            try:
                self._connect_to_database(DataQuality.GOLD)
                self._create_schemas(DataQuality.GOLD)
                self._create_tables(DataQuality.GOLD)
                self._process_data(DataQuality.GOLD)

            except Exception as e:
                self._logger.log_error(
                    f"Error preprocessing `{DataQuality.GOLD.value}` data: {e}"
                )

            finally:
                self._database_driver.disconnect()
