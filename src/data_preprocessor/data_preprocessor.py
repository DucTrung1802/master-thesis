from dotenv import load_dotenv
import os
import pandas as pd
import re
from glob import glob
import csv
import numpy as np
from datetime import datetime, timedelta

from logger.logger import Logger
from models.tabular_database_driver_models.postgre_sql_connection_model import (
    PostgreSQLConnectionModel,
)
from models.tabular_database_driver_models.tabular_database_driver_models import *
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from utils.constants import SCRAPER_RAW_DATA_DIR
from utils.enums import *
from utils.utils import *

load_dotenv()


class DataPreprocessor:
    def __init__(self, logger: Logger):
        self._logger = logger
        self._database_driver = PostgreSQLDriver(logger=logger)

        # Data
        self._market_df = None

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

    def _save_pandas_table_to_database(
        self,
        schema_name: str,
        table_name: str,
        primary_keys: List[str],
        df: pd.DataFrame,
    ) -> None:
        self._logger.log_info(f'Saving dataframe to table "{schema_name}.{table_name}"')

        # Drop rows where all values are NaN
        df = df.dropna(how="all")

        if df.empty:
            self._logger.log_info("DataFrame is empty after cleaning. Nothing to save.")
            return

        # Convert entire DataFrame into a list of Records (vectorized)
        column_names = list(df.columns)
        records = []

        for row in df.itertuples(index=False, name=None):
            data_model_list = [
                DataModel(column_name=col, value=(val if pd.notna(val) else None))
                for col, val in zip(column_names, row)
            ]
            records.append(Record(data_model_list=data_model_list))

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
            f"Saved {inserted_count + updated_count}/{len(df)} records into table '{schema_name}.{table_name}'"
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

    # endregion Helper functions

    # region Create Schemas
    def _create_schemas(self) -> None:
        self._logger.log_info("Start creating schemas.")

        self._database_driver.create_schema(Schema.MACROECONOMICS.value)
        self._database_driver.create_schema(Schema.STOCK_MARKET.value)
        self._database_driver.create_schema(Schema.ENTERPRISE.value)

        self._logger.log_info("Finish creating schemas.")

    # endregion Create Schemas

    # region Create Tables
    def _create_macroeconomics_tables(self) -> None:
        self._logger.log_info("Start creating macroeconomics tables.")

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

        # EXCHANGE_RATE
        # fmt: off
        self._database_driver.create_table(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.EXCHANGE_RATE.name,
            columns = [
                Column(name=Table.EXCHANGE_RATE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                Column(name=Table.EXCHANGE_RATE.Column.EXCHANGE_RATE.value, data_type=DataType.DECIMAL(), nullable=True),
            ],
            primary_keys=Table.EXCHANGE_RATE.primary_key,
        )
        # fmt: on
        
        # INTEREST_RATE
        # fmt: off
        self._database_driver.create_table(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.INTEREST_RATE.name,
            columns = [
                Column(name=Table.INTEREST_RATE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                Column(name=Table.INTEREST_RATE.Column.ONE_WEEK.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.INTEREST_RATE.Column.TWO_WEEK.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.INTEREST_RATE.Column.ONE_MONTH.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.INTEREST_RATE.Column.THREE_MONTH.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.INTEREST_RATE.Column.SIX_MONTH.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.INTEREST_RATE.Column.NINE_MONTH.value, data_type=DataType.DECIMAL(), nullable=True),
            ],
            primary_keys=Table.INTEREST_RATE.primary_key,
        )
        # fmt: on
        
        # EXPORT
        # fmt: off
        self._database_driver.create_table(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.EXPORT.name,
            columns = [
                Column(name=Table.EXPORT.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.EXPORT.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.EXPORT.Column.TOTAL.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.EXPORT.Column.LEATHER_SHOES.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.EXPORT.Column.TEXTILES.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.EXPORT.Column.WOOD_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.EXPORT.Column.SEAFOOD.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.EXPORT.Column.CRUDE_OIL.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.EXPORT.Column.RICE.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.EXPORT.Column.COFFEE.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.EXPORT.Column.COMPUTER_ELECTRONICS.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.EXPORT.Column.MACHINERY_EQUIPMENT.value, data_type=DataType.DECIMAL(), nullable=True),
            ],
            primary_keys=Table.EXPORT.primary_key,
        )
        # fmt: on
        
        # IMPORT
        # fmt: off
        self._database_driver.create_table(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IMPORT.name,
            columns = [
                Column(name=Table.IMPORT.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.IMPORT.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.IMPORT.Column.TOTAL.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.IMPORT.Column.ELECTRONICS_COMPUTERS_COMPONENTS.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.IMPORT.Column.MACHINERY_EQUIPMENT.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.IMPORT.Column.GASOLINE.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.IMPORT.Column.CHEMICAL.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.IMPORT.Column.CHEMICAL_PRODUCTS.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.IMPORT.Column.IRON_STEEL.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.IMPORT.Column.FABRIC.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.IMPORT.Column.CAR.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.IMPORT.Column.ANIMAL_FEED.value, data_type=DataType.DECIMAL(), nullable=True),
            ],
            primary_keys=Table.IMPORT.primary_key,
        )
        # fmt: on

        # FDI
        # fmt: off
        self._database_driver.create_table(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.FDI.name,
            columns = [
                Column(name=Table.FDI.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.FDI.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.FDI.Column.REGISTERED.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.FDI.Column.DISBURSEMENTED.value, data_type=DataType.DECIMAL(), nullable=True),
            ],
            primary_keys=Table.FDI.primary_key,
        )
        # fmt: on
        
        # M2
        # fmt: off
        self._database_driver.create_table(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.M2.name,
            columns = [
                Column(name=Table.M2.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.M2.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.M2.Column.CREDITS.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.M2.Column.M2_MONEY_SUPPLY.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.M2.Column.CREDITS_GROWTH_YTD.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.M2.Column.M2_MONEY_SUPPLY_GROWTH_YTD.value, data_type=DataType.DECIMAL(), nullable=True),
            ],
            primary_keys=Table.M2.primary_key,
        )
        # fmt: on
        
        # RETAIL
        # fmt: off
        self._database_driver.create_table(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.RETAIL.name,
            columns = [
                Column(name=Table.RETAIL.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.RETAIL.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.RETAIL.Column.TOTAL.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.RETAIL.Column.COMMERCIAL.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.RETAIL.Column.HOTEL_RESTAURANT.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.RETAIL.Column.TOURISM.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.RETAIL.Column.SERVICE.value, data_type=DataType.DECIMAL(), nullable=True),
            ],
            primary_keys=Table.RETAIL.primary_key,
        )
        # fmt: on
        
        # POPULATION_UNEMPLOYMENT
        # fmt: off
        self._database_driver.create_table(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.POPULATION_UNEMPLOYMENT.name,
            columns = [
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.POPULATION.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.POPULATION_DENSITY.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.POPULATION_GROWTH_RATIO.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.URBAN_POPULATION_RATIO.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.LABOR_FORCE_COUNT.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.AGRICULTURE_FORESTRY_AND_FISHERIES.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.INDUSTRY_AND_CONSTRUCTION.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.SERVICE.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.URBAN_UNEMPLOYED_COUNT.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.LABOR_FORCE_GROWTH.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.LABOR_FORCE_RATIO.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.MALE_RATIO.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.FEMALE_RATIO.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.POPULATION_UNEMPLOYMENT.Column.URBAN_UNEMPLOYED_RATIO.value, data_type=DataType.DECIMAL(), nullable=True),
            ],
            primary_keys=Table.POPULATION_UNEMPLOYMENT.primary_key,
        )
        # fmt: on
        
        # GOLD_PRICE
        # fmt: off
        self._database_driver.create_table(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.GOLD_PRICE.name,
            columns = [
                Column(name=Table.GOLD_PRICE.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                Column(name=Table.GOLD_PRICE.Column.PRICE.value, data_type=DataType.DECIMAL(), nullable=False),
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
                Column(name=Table.OIL_PRICE.Column.PRICE.value, data_type=DataType.DECIMAL(), nullable=False),
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
                Column(name=Table.DOW_JONES.Column.PRICE.value, data_type=DataType.DECIMAL(), nullable=False),
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
                Column(name=Table.NYSE_COMPOSITE.Column.PRICE.value, data_type=DataType.DECIMAL(), nullable=False),
                Column(name=Table.NYSE_COMPOSITE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.NYSE_COMPOSITE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.NYSE_COMPOSITE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.NYSE_COMPOSITE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.NYSE_COMPOSITE.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
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
                Column(name=Table.SNP_500.Column.PRICE.value, data_type=DataType.DECIMAL(), nullable=False),
                Column(name=Table.SNP_500.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.SNP_500.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.SNP_500.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.SNP_500.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.SNP_500.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
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
                Column(name=Table.NASDAQ_COMPOSITE.Column.PRICE.value, data_type=DataType.DECIMAL(), nullable=False),
                Column(name=Table.NASDAQ_COMPOSITE.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.NASDAQ_COMPOSITE.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.NASDAQ_COMPOSITE.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.NASDAQ_COMPOSITE.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.NASDAQ_COMPOSITE.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
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
                Column(name=Table.NASDAQ_100.Column.PRICE.value, data_type=DataType.DECIMAL(), nullable=False),
                Column(name=Table.NASDAQ_100.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.NASDAQ_100.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.NASDAQ_100.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.NASDAQ_100.Column.VOLUME.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.NASDAQ_100.Column.CHANGE.value, data_type=DataType.DECIMAL(), nullable=True),
            ],
            primary_keys=Table.NASDAQ_100.primary_key,
        )
        # fmt: on

        self._logger.log_info("Finish creating macroeconomics tables.")

    def _create_stock_market_tables(self) -> None:
        self._logger.log_info("Start creating stock market tables.")

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
        
        # VN_INDEX
        # fmt: off
        self._database_driver.create_table(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.VN_INDEX.name,
            columns = [
                Column(name=Table.VN_INDEX.Column.DATE.value, data_type=DataType.DATE(), nullable=False),
                Column(name=Table.VN_INDEX.Column.OPEN.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.VN_INDEX.Column.HIGH.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.VN_INDEX.Column.LOW.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.VN_INDEX.Column.CLOSE.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.VN_INDEX.Column.VOLUME.value, data_type=DataType.BIGINT(), nullable=True),
            ],
            primary_keys=Table.VN_INDEX.primary_key,
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

        self._logger.log_info("Finish creating stock market tables.")

    def _create_enterprise_tables(self) -> None:
        self._logger.log_info("Start creating enterprise tables.")

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

        self._logger.log_info("Finish creating enterprise tables.")

    def _create_tables(self) -> None:
        self._logger.log_info("Start creating tables.")

        self._create_macroeconomics_tables()
        # self._create_stock_market_tables()
        # self._create_enterprise_tables()

        self._logger.log_info("Finish creating tables.")

    # endregion Create Tables

    # region MACROECONOMICS data process

    # region MACROECONOMICS.GDP
    def _process_macroeconomics_gdp_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.GDP,
            GdpSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        df = df.iloc[:5, :]

        # Set indicator names as lowercase with underscores
        df["Chỉ tiêu"] = df["Chỉ tiêu"].str.lower().str.replace(" ", "_")

        # Melt from wide to long format
        df = df.melt(
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
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
            columns="Chỉ tiêu",
            values="value",
            aggfunc="first",
        ).reset_index()

        # Sort by year and quarter
        df = df.sort_values(["year", "quarter"]).reset_index(drop=True)

        # Fill missing values with 0
        df.fillna(0, inplace=True)

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

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_gdp(self) -> None:
        self._logger.log_info("Start processing macroeconomics GDP data.")

        self._process_macroeconomics_gdp_vietstock()

        self._logger.log_info("Finish processing macroeconomics GDP data.")

    # endregion GDP

    # region MACROECONOMICS.CPI
    def _process_macroeconomics_cpi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.CPI,
            CpiSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df["Chỉ tiêu"] = (
            df["Chỉ tiêu"].str.lower().str.replace(",", "").str.replace(" ", "_")
        )

        # Melt from wide to long format
        df = df.melt(
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
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
            index=["year", "month"], columns="Chỉ tiêu", values="value", aggfunc="first"
        ).reset_index()

        # Sort by year and month
        df = df.sort_values(["year", "month"]).reset_index(drop=True)

        # Fill missing values with 0
        df.fillna(0, inplace=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.CPI.name,
            primary_keys=Table.CPI.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_cpi(self) -> None:
        self._logger.log_info("Start processing macroeconomics CPI data.")

        self._process_macroeconomics_cpi_vietstock()

        self._logger.log_info("Finish processing macroeconomics CPI data.")

    # endregion MACROECONOMICS.CPI

    # region MACROECONOMICS.PPI

    def _process_macroeconomics_ppi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.PPI,
            PpiSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df["Chỉ tiêu"] = (
            df["Chỉ tiêu"].str.lower().str.replace(",", "").str.replace(" ", "_")
        )

        # Melt from wide to long format
        df = df.melt(
            id_vars=["Chỉ tiêu", "Đơn vị tính"], var_name="year_str", value_name="value"
        )

        # Clean numeric values
        df["value"] = df["value"].astype(str).str.replace(",", "", regex=False)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        # Extract year
        df["year"] = pd.to_datetime(df["year_str"], errors="coerce").dt.year

        # Use pivot_table with first() to handle duplicates
        df = df.pivot_table(
            index=["year"], columns="Chỉ tiêu", values="value", aggfunc="first"
        ).reset_index()

        # Sort by year and month
        df = df.sort_values(["year"]).reset_index(drop=True)

        # Fill missing values with 0
        df.fillna(0, inplace=True)

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

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_ppi(self) -> None:
        self._logger.log_info("Start processing macroeconomics PPI data.")

        self._process_macroeconomics_ppi_vietstock()

        self._logger.log_info("Finish processing macroeconomics PPI data.")

    # endregion MACROECONOMICS.PPI

    # region MACROECONOMICS.IPI
    def _process_macroeconomics_ipi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IPI,
            IpiSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        # df["Chỉ tiêu"] = df["Chỉ tiêu"].str.lower().str.replace(" ", "_")
        df["Chỉ tiêu"] = (
            df["Chỉ tiêu"].str.lower().str.replace(",", "").str.replace(" ", "_")
        )

        # Melt from wide to long format
        df = df.melt(
            id_vars=["Chỉ tiêu", "Đơn vị tính"], var_name="year_str", value_name="value"
        )

        # Clean numeric values
        df["value"] = df["value"].astype(str).str.replace(",", "", regex=False)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        # Extract year
        df["year"] = pd.to_datetime(df["year_str"], errors="coerce").dt.year

        # Use pivot_table with first() to handle duplicates
        df = df.pivot_table(
            index=["year"], columns="Chỉ tiêu", values="value", aggfunc="first"
        ).reset_index()

        # Sort by year and month
        df = df.sort_values(["year"]).reset_index(drop=True)

        # Fill missing values with 0
        df.fillna(0, inplace=True)

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

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_ipi(self) -> None:
        self._logger.log_info("Start processing macroeconomics IPI data.")

        self._process_macroeconomics_ipi_vietstock()

        self._logger.log_info("Finish processing macroeconomics IPI data.")

    # endregion MACROECONOMICS.IPI

    # region MACROECONOMICS.XPI
    def _process_macroeconomics_xpi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.XPI,
            XpiSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df["Chỉ tiêu"] = (
            df["Chỉ tiêu"]
            .str.lower()
            .str.replace(",", "")
            .str.replace(" ", "_")
            .str.replace("-", "_")
        )

        # Melt from wide to long format
        df = df.melt(
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
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
            index=["year", "month"], columns="Chỉ tiêu", values="value", aggfunc="first"
        ).reset_index()

        # Sort by year and month
        df = df.sort_values(["year", "month"]).reset_index(drop=True)

        # Fill missing values with 0
        df.fillna(0, inplace=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.XPI.name,
            primary_keys=Table.XPI.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_xpi(self) -> None:
        self._logger.log_info("Start processing macroeconomics XPI data.")

        self._process_macroeconomics_xpi_vietstock()

        self._logger.log_info("Finish processing macroeconomics XPI data.")

    # endregion MACROECONOMICS.XPI

    # region MACROECONOMICS.MPI
    def _process_macroeconomics_mpi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.MPI,
            MpiSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df["Chỉ tiêu"] = (
            df["Chỉ tiêu"]
            .str.lower()
            .str.replace(",", "")
            .str.replace(" ", "_")
            .str.replace("-", "_")
        )

        # Melt from wide to long format
        df = df.melt(
            id_vars=["Chỉ tiêu", "Đơn vị tính"],
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
            index=["year", "month"], columns="Chỉ tiêu", values="value", aggfunc="first"
        ).reset_index()

        # Sort by year and month
        df = df.sort_values(["year", "month"]).reset_index(drop=True)

        # Fill missing values with 0
        df.fillna(0, inplace=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.MPI.name,
            primary_keys=Table.MPI.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_mpi(self) -> None:
        self._logger.log_info("Start processing macroeconomics MPI data.")

        self._process_macroeconomics_mpi_vietstock()

        self._logger.log_info("Finish processing macroeconomics MPI data.")

    # endregion MACROECONOMICS.MPI

    # region MACROECONOMICS.POPULATION
    def _process_macroeconomics_population_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.POPULATION,
            PopulationSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df["Chỉ tiêu"] = (
            df["Chỉ tiêu"].str.lower().str.replace(",", "").str.replace(" ", "_")
        )

        # Melt from wide to long format
        df = df.melt(
            id_vars=["Chỉ tiêu", "Đơn vị tính"], var_name="year_str", value_name="value"
        )

        # Clean numeric values
        df["value"] = df["value"].astype(str).str.replace(",", "", regex=False)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        # Extract year
        df["year"] = pd.to_datetime(df["year_str"], errors="coerce").dt.year

        # Use pivot_table with first() to handle duplicates
        df = df.pivot_table(
            index=["year"], columns="Chỉ tiêu", values="value", aggfunc="first"
        ).reset_index()

        # Sort by year and month
        df = df.sort_values(["year"]).reset_index(drop=True)

        # Fill missing values with 0
        df.fillna(0, inplace=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.POPULATION.name,
            primary_keys=Table.POPULATION.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_population(self) -> None:
        self._logger.log_info("Start processing macroeconomics POPULATION data.")

        self._process_macroeconomics_population_vietstock()

        self._logger.log_info("Finish processing macroeconomics POPULATION data.")

    # endregion MACROECONOMICS.POPULATION

    # region MACROECONOMICS.LABOR
    def _process_macroeconomics_labor_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.LABOR,
            LaborSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Set indicator names as lowercase with underscores
        df["Chỉ tiêu"] = (
            df["Chỉ tiêu"]
            .str.lower()
            .str.replace("&", "")
            .str.replace(r"\s+", " ", regex=True)
            .str.replace(",", "")
            .str.replace(" ", "_")
            .str.replace("employed_a", "employed_amount")
        )

        # Melt from wide to long format
        df = df.melt(
            id_vars=["Chỉ tiêu", "Đơn vị tính"], var_name="year_str", value_name="value"
        )

        # Clean numeric values
        df["value"] = df["value"].astype(str).str.replace(",", "", regex=False)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        # Extract year
        df["year"] = pd.to_datetime(df["year_str"], errors="coerce").dt.year

        # Use pivot_table with first() to handle duplicates
        df = df.pivot_table(
            index=["year"], columns="Chỉ tiêu", values="value", aggfunc="first"
        ).reset_index()

        # Sort by year and month
        df = df.sort_values(["year"]).reset_index(drop=True)

        # Fill missing values with 0
        df.fillna(0, inplace=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.LABOR.name,
            primary_keys=Table.LABOR.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_labor(self) -> None:
        self._logger.log_info("Start processing macroeconomics LABOR data.")

        self._process_macroeconomics_labor_vietstock()

        self._logger.log_info("Finish processing macroeconomics LABOR data.")

    # endregion MACROECONOMICS.LABOR

    # region MACROECONOMICS.EXCHANGE_RATE
    def _process_macroeconomics_exchange_rate_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.EXCHANGE_RATE,
            ExchangeRateSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)
        df = df.drop(df.columns[:2], axis=1).drop(df.columns[-1], axis=1)
        df = df.transpose()
        df.index = pd.to_datetime(df.index, format="%d/%m/%Y")
        df = df.reset_index()
        df = df.rename(columns={"index": "date"})
        df["exchange_rate"] = df[0].combine_first(df[1])
        df = df.drop(columns=[0, 1])
        df["exchange_rate"] = (
            df["exchange_rate"].str.replace(",", ".", regex=True).astype(float)
        )
        df["date"] = df["date"].dt.date

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.EXCHANGE_RATE.name,
            primary_keys=Table.EXCHANGE_RATE.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_exchange_rate(self) -> None:
        self._logger.log_info("Start processing macroeconomics EXCHANGE_RATE data.")

        self._process_macroeconomics_exchange_rate_vietstock()

        self._logger.log_info("Finish processing macroeconomics EXCHANGE_RATE data.")

    # endregion MACROECONOMICS.EXCHANGE_RATE

    # region MACROECONOMICS.INTEREST_RATE
    def _process_macroeconomics_interest_rate_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.INTEREST_RATE,
            InterestRateSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        extension = FileExtension.CSV
        if not folder_contains_files(folder_path, extension):
            self._logger.log_error(
                f'Data{" with extension " + extension if extension else ""} in "{folder_path}" does not exist.'
            )
            return

        self._logger.log_info(f'Start processing data in folder "{folder_path}".')

        # Add logic for processing data here
        file_pattern = os.path.join(folder_path, "vietstock_*.csv")
        files = sorted(glob(file_pattern))

        rename_map = {
            "1 tuần": Table.INTEREST_RATE.Column.ONE_WEEK.value,
            "2 tuần": Table.INTEREST_RATE.Column.TWO_WEEK.value,
            "1 tháng": Table.INTEREST_RATE.Column.ONE_MONTH.value,
            "3 tháng": Table.INTEREST_RATE.Column.THREE_MONTH.value,
            "6 tháng": Table.INTEREST_RATE.Column.SIX_MONTH.value,
            "9 tháng": Table.INTEREST_RATE.Column.NINE_MONTH.value,
        }

        combined_data = []

        for file in files:
            df = pd.read_csv(file, index_col=0)

            # Skip files with only headers or no data
            if df.shape[1] <= 3:
                print(f"Skipping {file} (likely no actual data)")
                continue

            # Clean column names and rows
            df = df.drop(columns=["Đơn vị tính"], errors="ignore")
            df = df.dropna(how="all", axis=1)  # Drop completely empty columns
            df = df.dropna(how="all", axis=0)  # Drop completely empty rows

            df = df.T  # Transpose to have dates as rows
            df["date"] = df.index
            df["date"] = pd.to_datetime(df["date"], dayfirst=True)
            df.reset_index(drop=True, inplace=True)

            combined_data.append(df)

        # Merge all into one big DataFrame
        result_df = pd.concat(combined_data)

        # Reset index to remove old index and get clean integer index
        result_df = result_df.reset_index(drop=True)

        # Sort by date
        result_df = result_df.sort_values(by="date")

        # Reorder columns to have 'date' first
        cols = result_df.columns.tolist()
        cols.insert(0, cols.pop(cols.index("date")))
        result_df = result_df[cols]
        result_df = result_df.drop(columns=["Qua đêm"], errors="ignore")
        result_df = result_df.rename(columns=rename_map)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.INTEREST_RATE.name,
            primary_keys=Table.INTEREST_RATE.primary_key,
            df=result_df,
        )

        self._logger.log_info(f'Finish processing data in folder "{folder_path}".')

    def _process_macroeconomics_interest_rate(self) -> None:
        self._logger.log_info("Start processing macroeconomics INTEREST_RATE data.")

        self._process_macroeconomics_interest_rate_vietstock()

        self._logger.log_info("Finish processing macroeconomics INTEREST_RATE data.")

    # endregion MACROECONOMICS.INTEREST_RATE

    # region MACROECONOMICS.EXPORT
    def _process_macroeconomics_export_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.EXPORT,
            ExportImportSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{"export_import"}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)
        export_df = df.iloc[0:10, :]

        # Drop "Đơn vị tính" only if it exists
        if "Đơn vị tính" in export_df.columns:
            export_df = export_df.drop(columns=["Đơn vị tính"])

        # Transpose and rename
        export_df = export_df.set_index("Chỉ tiêu").T

        rename_map = {
            "Tổng trị giá Xuất khẩu": Table.EXPORT.Column.TOTAL.value,
            "Giày da": Table.EXPORT.Column.LEATHER_SHOES.value,
            "Dệt may": Table.EXPORT.Column.TEXTILES.value,
            "Gỗ và sản phẩm gỗ": Table.EXPORT.Column.WOOD_PRODUCTS.value,
            "Thủy sản": Table.EXPORT.Column.SEAFOOD.value,
            "Dầu thô": Table.EXPORT.Column.CRUDE_OIL.value,
            "Gạo": Table.EXPORT.Column.RICE.value,
            "Café": Table.EXPORT.Column.COFFEE.value,
            "Điện tử máy tính": Table.EXPORT.Column.COMPUTER_ELECTRONICS.value,
            "Máy móc thiết bị": Table.EXPORT.Column.MACHINERY_EQUIPMENT.value,
        }
        export_df = export_df.rename(columns=rename_map).reset_index()

        # Extract month and year
        export_df[["month", "year"]] = (
            export_df["index"].str.extract(r"Tháng (\d+)/(\d+)").astype("Int64")
        )

        # Reorder columns
        new_col_order = ["year", "month"] + list(rename_map.values())
        export_df = export_df[new_col_order]

        # Convert all data columns (except year and month) to float, removing commas
        data_cols = export_df.columns.difference(["year", "month"])
        for col in data_cols:
            export_df[col] = (
                export_df[col].astype(str).str.replace(",", "").astype(float)
            )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.EXPORT.name,
            primary_keys=Table.EXPORT.primary_key,
            df=export_df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_export(self) -> None:
        self._logger.log_info("Start processing macroeconomics EXPORT data.")

        self._process_macroeconomics_export_vietstock()

        self._logger.log_info("Finish processing macroeconomics EXPORT data.")

    # endregion MACROECONOMICS.EXPORT

    # region MACROECONOMICS.IMPORT
    def _process_macroeconomics_import_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IMPORT,
            ExportImportSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{"export_import"}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)
        import_df = df.iloc[10:, :]

        # Drop "Đơn vị tính" only if it exists
        if "Đơn vị tính" in import_df.columns:
            import_df = import_df.drop(columns=["Đơn vị tính"])

        # Transpose and rename
        import_df = import_df.set_index("Chỉ tiêu").T

        rename_map = {
            "Tổng trị giá Nhập khẩu": Table.IMPORT.Column.TOTAL.value,
            "Điện tử, máy tính và linh kiện": Table.IMPORT.Column.ELECTRONICS_COMPUTERS_COMPONENTS.value,
            "Máy móc thiết bị, phụ tùng": Table.IMPORT.Column.MACHINERY_EQUIPMENT.value,
            "Xăng dầu": Table.IMPORT.Column.GASOLINE.value,
            "Hóa chất": Table.IMPORT.Column.CHEMICAL.value,
            "Sản phẩm hóa chất": Table.IMPORT.Column.CHEMICAL_PRODUCTS.value,
            "Sắt thép": Table.IMPORT.Column.IRON_STEEL.value,
            "Vải": Table.IMPORT.Column.FABRIC.value,
            "Ô tô": Table.IMPORT.Column.CAR.value,
            "Thức ăn gia súc": Table.IMPORT.Column.ANIMAL_FEED.value,
        }
        import_df = import_df.rename(columns=rename_map).reset_index()

        # Extract month and year
        import_df[["month", "year"]] = (
            import_df["index"].str.extract(r"Tháng (\d+)/(\d+)").astype("Int64")
        )

        # Reorder columns
        new_col_order = ["year", "month"] + list(rename_map.values())
        import_df = import_df[new_col_order]

        # Convert all data columns (except year and month) to float, removing commas
        data_cols = import_df.columns.difference(["year", "month"])
        for col in data_cols:
            import_df[col] = (
                import_df[col].astype(str).str.replace(",", "").astype(float)
            )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IMPORT.name,
            primary_keys=Table.IMPORT.primary_key,
            df=import_df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_import(self) -> None:
        self._logger.log_info("Start processing macroeconomics IMPORT data.")

        self._process_macroeconomics_import_vietstock()

        self._logger.log_info("Finish processing macroeconomics IMPORT data.")

    # endregion

    # region MACROECONOMICS.FDI
    def _process_macroeconomics_fdi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.FDI,
            FdiSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        to_fix_file_path = os.path.join(folder_path, "vietstock_2016_2020.csv")
        fixed_file_path = os.path.join(folder_path, "vietstock_2016_2020_fixed.csv")
        merged_file_path = os.path.join(folder_path, "vietstock_merged.csv")

        # Delete the old fixed file if it exists
        if os.path.exists(fixed_file_path):
            os.remove(fixed_file_path)

        # Delete the old merged file if it exists
        if os.path.exists(merged_file_path):
            os.remove(merged_file_path)

        # Fix file "vietstock_2016_2020.csv" with redundant data
        with open(to_fix_file_path, "r", encoding="utf-8") as f:
            reader = list(csv.reader(f))

        # Extract header and the two data rows
        header = reader[0]
        registration_row = reader[1]  # "Đăng ký"
        disbursement_row = reader[2]  # "Giải ngân"

        # Normalize row lengths by removing duplicated values
        expected_columns = len(header)

        # Fix the registration row - remove duplicate at index 13 ("0.49")
        if len(registration_row) > expected_columns:
            del registration_row[13]  # corresponds to "Tháng 1/2017"

        # Fix the disbursement row - remove duplicate at index 12 ("1.6")
        if len(disbursement_row) > expected_columns:
            del disbursement_row[12]  # corresponds to "Tháng 12/2016"

        # Write the cleaned data to a new CSV file
        with open(fixed_file_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerow(registration_row)
            writer.writerow(disbursement_row)

        # Process cleaned data
        data_path = os.path.join(folder_path, "vietstock_*.csv")
        file_list = sorted(glob(data_path))

        dfs = []

        for file in file_list:
            if file.endswith("vietstock_2016_2020.csv"):
                continue

            df = pd.read_csv(file)
            dfs.append(df)

        merged_df = dfs[0].copy()

        for df in dfs[1:]:
            merged_df = pd.concat([merged_df, df.iloc[:, 2:]], axis=1)

        merged_df = merged_df.loc[:, ~merged_df.columns.str.contains("Đồ thị")]
        merged_df.drop(merged_df.columns[2], axis=1, inplace=True)

        merged_df.to_csv(merged_file_path, index=False)

        if not merged_file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{merged_file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(merged_file_path)

        # Drop "Đơn vị tính" only if it exists
        if "Đơn vị tính" in df.columns:
            df = df.drop(columns=["Đơn vị tính"])

        # Transpose and rename
        df = df.set_index("Chỉ tiêu").T

        rename_map = {
            "Đăng ký": Table.FDI.Column.REGISTERED.value,
            "Giải ngân": Table.FDI.Column.DISBURSEMENTED.value,
        }
        df = df.rename(columns=rename_map).reset_index()

        # Extract month and year
        df[["month", "year"]] = (
            df["index"].str.extract(r"Tháng (\d+)/(\d+)").astype("Int64")
        )

        # Reorder columns
        new_col_order = ["year", "month"] + list(rename_map.values())
        df = df[new_col_order]

        # Convert all data columns (except year and month) to float, removing commas
        data_cols = df.columns.difference(["year", "month"])
        for col in data_cols:
            df[col] = df[col].astype(str).str.replace(",", "").astype(float)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.FDI.name,
            primary_keys=Table.FDI.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish processing data in "{folder_path}".')

    def _process_macroeconomics_fdi(self) -> None:
        self._logger.log_info("Start processing macroeconomics FDI data.")

        self._process_macroeconomics_fdi_vietstock()

        self._logger.log_info("Finish processing macroeconomics FDI data.")

    # endregion MACROECONOMICS.FDI

    # region MACROECONOMICS.M2
    def _process_macroeconomics_m2_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.M2,
            M2Source.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Drop "Đơn vị tính" only if it exists
        if "Đơn vị tính" in df.columns:
            df = df.drop(columns=["Đơn vị tính"])

        df = df.set_index("Chỉ tiêu").T

        rename_map = {
            "Tín dụng": Table.M2.Column.CREDITS.value,
            "Cung tiền M2": Table.M2.Column.M2_MONEY_SUPPLY.value,
            "Tăng trưởng tín dụng (YTD)*": Table.M2.Column.CREDITS_GROWTH_YTD.value,
            "Tăng trưởng Cung tiền M2 (YTD)*": Table.M2.Column.M2_MONEY_SUPPLY_GROWTH_YTD.value,
        }
        df = df.rename(columns=rename_map).reset_index()

        # Extract month and year
        df[["month", "year"]] = (
            df["index"].str.extract(r"Tháng (\d+)/(\d+)").astype("Int64")
        )

        # Reorder columns
        new_col_order = ["year", "month"] + list(rename_map.values())
        df = df[new_col_order]

        # Convert all data columns (except year and month) to float, removing commas
        data_cols = df.columns.difference(["year", "month"])
        for col in data_cols:
            df[col] = df[col].astype(str).str.replace(",", "").astype(float)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.M2.name,
            primary_keys=Table.M2.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_m2(self) -> None:
        self._logger.log_info("Start processing macroeconomics M2 data.")

        self._process_macroeconomics_m2_vietstock()

        self._logger.log_info("Finish processing macroeconomics M2 data.")

    # endregion MACROECONOMICS.M2

    # region MACROECONOMICS.RETAIL
    def _process_macroeconomics_retail_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.RETAIL,
            RetailSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        # Drop "Đơn vị tính" only if it exists
        if "Đơn vị tính" in df.columns:
            df = df.drop(columns=["Đơn vị tính"])

        df = df.set_index("Chỉ tiêu").T

        rename_map = {
            "Tổng": "total",
            "Thương nghiệp": "commercial",
            "Khách sạn nhà hàng": "hotel_restaurant",
            "Du lịch": "tourism",
            "Dịch vụ": "service",
        }
        df = df.rename(columns=rename_map).reset_index()

        # Extract month and year
        df[["month", "year"]] = (
            df["index"].str.extract(r"Tháng (\d+)/(\d+)").astype("Int64")
        )

        # Reorder columns
        new_col_order = ["year", "month"] + list(rename_map.values())
        df = df[new_col_order]

        # Convert all data columns (except year and month) to float, removing commas
        data_cols = df.columns.difference(["year", "month"])
        for col in data_cols:
            df[col] = df[col].astype(str).str.replace(",", "").astype(float)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.RETAIL.name,
            primary_keys=Table.RETAIL.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_retail(self) -> None:
        self._logger.log_info("Start processing macroeconomics RETAIL data.")

        self._process_macroeconomics_retail_vietstock()

        self._logger.log_info("Finish processing macroeconomics RETAIL data.")

    # endregion MACROECONOMICS.RETAIL

    # region MACROECONOMICS.POPULATION_UNEMPLOYMENT
    def _process_macroeconomics_population_unemployment_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.POPULATION_UNEMPLOYMENT,
            PopulationUnemploymentSource.VIETSTOCK,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path)

        if "Đơn vị tính" in df.columns:
            df = df.drop(columns=["Đơn vị tính"])

        df = df.set_index("Chỉ tiêu").T

        df = df.reset_index()

        rename_map = {
            "index": Table.POPULATION_UNEMPLOYMENT.Column.YEAR.value,
            "Dân số": Table.POPULATION_UNEMPLOYMENT.Column.POPULATION.value,
            "Mật độ dân số": Table.POPULATION_UNEMPLOYMENT.Column.POPULATION_DENSITY.value,
            "Tốc độ tăng dân số": Table.POPULATION_UNEMPLOYMENT.Column.POPULATION_GROWTH_RATIO.value,
            "Tỷ lệ dân thành thị": Table.POPULATION_UNEMPLOYMENT.Column.URBAN_POPULATION_RATIO.value,
            "Số lượng lao động": Table.POPULATION_UNEMPLOYMENT.Column.LABOR_FORCE_COUNT.value,
            "Nông, lâm nghiệp và thủy sản": Table.POPULATION_UNEMPLOYMENT.Column.AGRICULTURE_FORESTRY_AND_FISHERIES.value,
            "Công nghiệp và Xây dựng": Table.POPULATION_UNEMPLOYMENT.Column.INDUSTRY_AND_CONSTRUCTION.value,
            "Dịch vụ": Table.POPULATION_UNEMPLOYMENT.Column.SERVICE.value,
            "Số người thất nghiệp thành thị": Table.POPULATION_UNEMPLOYMENT.Column.URBAN_UNEMPLOYED_COUNT.value,
            "Tăng trưởng lực lượng lao động": Table.POPULATION_UNEMPLOYMENT.Column.LABOR_FORCE_GROWTH.value,
            "Tỷ lệ lao động/dân số": Table.POPULATION_UNEMPLOYMENT.Column.LABOR_FORCE_RATIO.value,
            "Tỷ lệ nam": Table.POPULATION_UNEMPLOYMENT.Column.MALE_RATIO.value,
            "Tỷ lệ nữ": Table.POPULATION_UNEMPLOYMENT.Column.FEMALE_RATIO.value,
            "Tỷ lệ thất nghiệp thành thị": Table.POPULATION_UNEMPLOYMENT.Column.URBAN_UNEMPLOYED_RATIO.value,
        }
        df = df.rename(columns=rename_map)

        df = df[df["year"].str.fullmatch(r"\d{4}")]

        df["year"] = df["year"].astype("Int64")

        for col in df.columns.difference(["year"]):
            df[col] = pd.to_numeric(df[col], errors="coerce")

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.POPULATION_UNEMPLOYMENT.name,
            primary_keys=Table.POPULATION_UNEMPLOYMENT.primary_key,
            df=df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_population_unemployment(self) -> None:
        self._logger.log_info(
            "Start processing macroeconomics POPULATION_UNEMPLOYMENT data."
        )

        self._process_macroeconomics_population_unemployment_vietstock()

        self._logger.log_info(
            "Finish processing macroeconomics POPULATION_UNEMPLOYMENT data."
        )

    # endregion MACROECONOMICS.POPULATION_UNEMPLOYMENT

    # region MACROECONOMICS.GOLD_PRICE
    def _process_macroeconomics_gold_price_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.GOLD_PRICE,
            GoldPriceSource.INVESTING,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        table_name = Table.GOLD_PRICE.__qualname__.lower()

        self._logger.log_info(f'Start processing data in "{table_name}".')

        # Add logic for processing data here
        combined_file_path = os.path.join(folder_path, "Gold_Futures_Combined.csv")
        if os.path.isfile(combined_file_path):
            os.remove(combined_file_path)

        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Convert 'Date' to datetime
            df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["Price", "Open", "High", "Low"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["Vol."] = df["Vol."].apply(parse_volume)

            # Change %: remove '%' and convert to float
            df["Change %"] = (
                df["Change %"].astype(str).str.replace("%", "").astype(float)
            )

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("Date").reset_index(drop=True)

        rename_map = {
            "Vol.": "volume",
            "Change %": "change",
        }

        full_df.rename(columns=rename_map, inplace=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.GOLD_PRICE.name,
            primary_keys=Table.GOLD_PRICE.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish processing data in "{table_name}".')

    def _process_macroeconomics_gold_price(self) -> None:
        self._logger.log_info("Start processing macroeconomics GOLD_PRICE data.")

        self._process_macroeconomics_gold_price_investing()

        self._logger.log_info("Finish processing macroeconomics GOLD_PRICE data.")

    # endregion MACROECONOMICS.GOLD_PRICE

    # region MACROECONOMICS.OIL_PRICE
    def _process_macroeconomics_oil_price_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.OIL_PRICE,
            OilPriceSource.INVESTING,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        table_name = Table.OIL_PRICE.__qualname__.lower()

        self._logger.log_info(f'Start processing data in "{table_name}".')

        # Add logic for processing data here
        combined_file_path = os.path.join(folder_path, "Brent_Oil_Futures_Combined.csv")
        if os.path.isfile(combined_file_path):
            os.remove(combined_file_path)

        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Convert 'Date' to datetime
            df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["Price", "Open", "High", "Low"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["Vol."] = df["Vol."].apply(parse_volume)

            # Change %: remove '%' and convert to float
            df["Change %"] = (
                df["Change %"].astype(str).str.replace("%", "").astype(float)
            )

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("Date").reset_index(drop=True)

        rename_map = {
            "Vol.": "volume",
            "Change %": "change",
        }

        full_df.rename(columns=rename_map, inplace=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.OIL_PRICE.name,
            primary_keys=Table.OIL_PRICE.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish processing data in "{table_name}".')

    def _process_macroeconomics_oil_price(self) -> None:
        self._logger.log_info("Start processing macroeconomics OIL_PRICE data.")

        self._process_macroeconomics_oil_price_investing()

        self._logger.log_info("Finish processing macroeconomics OIL_PRICE data.")

    # endregion MACROECONOMICS.OIL_PRICE

    # region MACROECONOMICS.DOW_JONES
    def _process_macroeconomics_dow_jones_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.DOW_JONES,
            DowJonesSource.INVESTING,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        table_name = Table.DOW_JONES.__qualname__.lower()

        self._logger.log_info(f'Start processing data in "{table_name}".')

        # Add logic for processing data here
        combined_file_path = os.path.join(folder_path, "Brent_Oil_Futures_Combined.csv")
        if os.path.isfile(combined_file_path):
            os.remove(combined_file_path)

        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Convert 'Date' to datetime
            df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["Price", "Open", "High", "Low"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["Vol."] = df["Vol."].apply(parse_volume)

            # Change %: remove '%' and convert to float
            df["Change %"] = (
                df["Change %"].astype(str).str.replace("%", "").astype(float)
            )

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("Date").reset_index(drop=True)

        rename_map = {
            "Vol.": "volume",
            "Change %": "change",
        }

        full_df.rename(columns=rename_map, inplace=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.DOW_JONES.name,
            primary_keys=Table.DOW_JONES.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish processing data in "{table_name}".')

    def _process_macroeconomics_dow_jones(self) -> None:
        self._logger.log_info("Start processing macroeconomics DOW_JONES data.")

        self._process_macroeconomics_dow_jones_investing()

        self._logger.log_info("Finish processing macroeconomics DOW_JONES data.")

    # endregion MACROECONOMICS.DOW_JONES

    # region MACROECONOMICS.NYSE_COMPOSITE
    def _process_macroeconomics_nyse_composite_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.NYSE_COMPOSITE,
            NYSECompositeSource.INVESTING,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        table_name = Table.NYSE_COMPOSITE.__qualname__.lower()

        self._logger.log_info(f'Start processing data in "{table_name}".')

        # Add logic for processing data here
        combined_file_path = os.path.join(folder_path, "Brent_Oil_Futures_Combined.csv")
        if os.path.isfile(combined_file_path):
            os.remove(combined_file_path)

        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Convert 'Date' to datetime
            df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["Price", "Open", "High", "Low"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["Vol."] = df["Vol."].apply(parse_volume)

            # Change %: remove '%' and convert to float
            df["Change %"] = (
                df["Change %"].astype(str).str.replace("%", "").astype(float)
            )

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("Date").reset_index(drop=True)

        rename_map = {
            "Vol.": "volume",
            "Change %": "change",
        }

        full_df.rename(columns=rename_map, inplace=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NYSE_COMPOSITE.name,
            primary_keys=Table.NYSE_COMPOSITE.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish processing data in "{table_name}".')

    def _process_macroeconomics_nyse_composite(self) -> None:
        self._logger.log_info("Start processing macroeconomics NYSE_COMPOSITE data.")

        self._process_macroeconomics_nyse_composite_investing()

        self._logger.log_info("Finish processing macroeconomics NYSE_COMPOSITE data.")

    # endregion MACROECONOMICS.NYSE_COMPOSITE

    # region MACROECONOMICS.SNP_500
    def _process_macroeconomics_snp_500_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.SNP_500,
            SNP500Source.INVESTING,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        table_name = Table.SNP_500.__qualname__.lower()

        self._logger.log_info(f'Start processing data in "{table_name}".')

        # Add logic for processing data here
        combined_file_path = os.path.join(folder_path, "Brent_Oil_Futures_Combined.csv")
        if os.path.isfile(combined_file_path):
            os.remove(combined_file_path)

        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Convert 'Date' to datetime
            df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["Price", "Open", "High", "Low"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["Vol."] = df["Vol."].apply(parse_volume)

            # Change %: remove '%' and convert to float
            df["Change %"] = (
                df["Change %"].astype(str).str.replace("%", "").astype(float)
            )

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("Date").reset_index(drop=True)

        rename_map = {
            "Vol.": "volume",
            "Change %": "change",
        }

        full_df.rename(columns=rename_map, inplace=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.SNP_500.name,
            primary_keys=Table.SNP_500.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish processing data in "{table_name}".')

    def _process_macroeconomics_snp_500(self) -> None:
        self._logger.log_info("Start processing macroeconomics SNP_500 data.")

        self._process_macroeconomics_snp_500_investing()

        self._logger.log_info("Finish processing macroeconomics SNP_500 data.")

    # endregion MACROECONOMICS.SNP_500

    # region MACROECONOMICS.NASDAQ_COMPOSITE
    def _process_macroeconomics_nasdaq_composite_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.NASDAQ_COMPOSITE,
            NASDAQCompositeSource.INVESTING,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        table_name = Table.NASDAQ_COMPOSITE.__qualname__.lower()

        self._logger.log_info(f'Start processing data in "{table_name}".')

        # Add logic for processing data here
        combined_file_path = os.path.join(folder_path, "Brent_Oil_Futures_Combined.csv")
        if os.path.isfile(combined_file_path):
            os.remove(combined_file_path)

        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Convert 'Date' to datetime
            df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["Price", "Open", "High", "Low"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["Vol."] = df["Vol."].apply(parse_volume)

            # Change %: remove '%' and convert to float
            df["Change %"] = (
                df["Change %"].astype(str).str.replace("%", "").astype(float)
            )

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("Date").reset_index(drop=True)

        rename_map = {
            "Vol.": "volume",
            "Change %": "change",
        }

        full_df.rename(columns=rename_map, inplace=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NASDAQ_COMPOSITE.name,
            primary_keys=Table.NASDAQ_COMPOSITE.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish processing data in "{table_name}".')

    def _process_macroeconomics_nasdaq_composite(self) -> None:
        self._logger.log_info("Start processing macroeconomics NASDAQ_COMPOSITE data.")

        self._process_macroeconomics_nasdaq_composite_investing()

        self._logger.log_info("Finish processing macroeconomics NASDAQ_COMPOSITE data.")

    # endregion MACROECONOMICS.NASDAQ_COMPOSITE

    # region MACROECONOMICS.NASDAQ_100
    def _process_macroeconomics_nasdaq_100_investing(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.NASDAQ_100,
            NASDAQ100Source.INVESTING,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        table_name = Table.NASDAQ_100.__qualname__.lower()

        self._logger.log_info(f'Start processing data in "{table_name}".')

        # Add logic for processing data here
        combined_file_path = os.path.join(folder_path, "Brent_Oil_Futures_Combined.csv")
        if os.path.isfile(combined_file_path):
            os.remove(combined_file_path)

        file_paths = glob(os.path.join(folder_path, "*.csv"))

        dfs = []
        for file in file_paths:
            df = pd.read_csv(file)

            # Convert 'Date' to datetime
            df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y", errors="coerce")

            # Clean numeric columns: remove commas and symbols, then convert to float
            for col in ["Price", "Open", "High", "Low"]:
                df[col] = df[col].astype(str).str.replace(",", "").astype(float)

            df["Vol."] = df["Vol."].apply(parse_volume)

            # Change %: remove '%' and convert to float
            df["Change %"] = (
                df["Change %"].astype(str).str.replace("%", "").astype(float)
            )

            dfs.append(df)

        # Combine and sort
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("Date").reset_index(drop=True)

        rename_map = {
            "Vol.": "volume",
            "Change %": "change",
        }

        full_df.rename(columns=rename_map, inplace=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.NASDAQ_100.name,
            primary_keys=Table.NASDAQ_100.primary_key,
            df=full_df,
        )

        self._logger.log_info(f'Finish processing data in "{table_name}".')

    def _process_macroeconomics_nasdaq_100(self) -> None:
        self._logger.log_info("Start processing macroeconomics NASDAQ_100 data.")

        self._process_macroeconomics_nasdaq_100_investing()

        self._logger.log_info("Finish processing macroeconomics NASDAQ_100 data.")

    # endregion MACROECONOMICS.NASDAQ_100

    # endregion MACROECONOMICS data process

    # region STOCK_MARKET data process

    # region STOCK_MARKET.MARKET
    def _process_stock_market_market_add_data(self) -> None:
        self._logger.log_info(
            f'Start processing data in "{Table.MARKET.__qualname__.lower()}".'
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
            f'Finish processing data in "{Table.MARKET.__qualname__.lower()}".'
        )

    def _process_stock_market_market(self) -> None:
        self._logger.log_info("Start processing stock market MARKET data.")

        self._process_stock_market_market_add_data()

        self._logger.log_info("Finish processing stock market MARKET data.")

    # endregion STOCK MARKET.MARKET

    # region STOCK_MARKET.VN_INDEX
    def _process_stock_market_vn_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_HNX_INDEX,
            VnHnxIndexSource.CAFEF,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

        # Add logic for processing data here
        df = pd.read_csv(file_path, encoding="utf-8")
        vn_index_df = df[df["<Ticker>"] == "VNINDEX"]
        rename_map = {
            "<Ticker>": "ticker",
            "<DTYYYYMMDD>": "date",
            "<Open>": "open",
            "<High>": "high",
            "<Low>": "low",
            "<Close>": "close",
            "<Volume>": "volume",
        }
        vn_index_df = vn_index_df.rename(columns=rename_map)
        vn_index_df.drop(columns=["ticker"], inplace=True)
        vn_index_df["date"] = pd.to_datetime(vn_index_df["date"], format="%Y%m%d")
        vn_index_df = vn_index_df.sort_values(by="date").reset_index(drop=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.VN_INDEX.name,
            primary_keys=Table.VN_INDEX.primary_key,
            df=vn_index_df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_stock_market_vn_index(self) -> None:
        self._logger.log_info("Start processing stock market VN_INDEX data.")

        self._process_stock_market_vn_index_cafef()

        self._logger.log_info("Finish processing stock market VN_INDEX data.")

    # endregion STOCK_MARKET.VN_INDEX

    # region STOCK_MARKET.HNX_INDEX
    def _process_stock_market_hnx_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_HNX_INDEX,
            VnHnxIndexSource.CAFEF,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

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

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_stock_market_hnx_index(self) -> None:
        self._logger.log_info("Start processing stock market HNX_INDEX data.")

        self._process_stock_market_hnx_index_cafef()

        self._logger.log_info("Finish processing stock market HNX_INDEX data.")

    # endregion STOCK_MARKET.HNX_INDEX

    # region STOCK_MARKET.VN_30_INDEX
    def _process_stock_market_vn_30_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_30_INDEX,
            Vn30IndexSource.CAFEF,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

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

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_stock_market_vn_30_index(self) -> None:
        self._logger.log_info("Start processing stock market VN_30_INDEX data.")

        self._process_stock_market_vn_30_index_cafef()

        self._logger.log_info("Finish processing stock market VN_30_INDEX data.")

    # endregion STOCK_MARKET.VN_30_INDEX

    # region STOCK_MARKET.VN_100_INDEX
    def _process_stock_market_vn_100_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_100_INDEX,
            Vn100IndexSource.CAFEF,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

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

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_stock_market_vn_100_index(self) -> None:
        self._logger.log_info("Start processing stock market VN_100_INDEX data.")

        self._process_stock_market_vn_100_index_cafef()

        self._logger.log_info("Finish processing stock market VN_100_INDEX data.")

    # endregion STOCK_MARKET.VN_100_INDEX

    # region STOCK_MARKET.HNX_30_INDEX
    def _process_stock_market_hnx_30_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.HNX_30_INDEX,
            Hnx30IndexSource.CAFEF,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

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

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_stock_market_hnx_30_index(self) -> None:
        self._logger.log_info("Start processing stock market HNX_30_INDEX data.")

        self._process_stock_market_hnx_30_index_cafef()

        self._logger.log_info("Finish processing stock market HNX_30_INDEX data.")

    # endregion STOCK_MARKET.HNX_30_INDEX

    # region STOCK_MARKET.UPCOM_INDEX
    def _process_stock_market_upcom_index_cafef(self) -> None:
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.UPCOM_INDEX,
            UpcomIndexSource.CAFEF,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )

        file_path = get_newest_file_path(
            folder_path=folder_path, extension=FileExtension.CSV
        )

        if not file_path:
            self._logger.log_error(f'Data in "{folder_path}" does not exist.')
            return

        self._logger.log_info(f'Start processing data in "{file_path}".')

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

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_stock_market_upcom_index(self) -> None:
        self._logger.log_info("Start processing stock market UPCOM_INDEX data.")

        self._process_stock_market_upcom_index_cafef()

        self._logger.log_info("Finish processing stock market UPCOM_INDEX data.")

    # endregion STOCK_MARKET.UPCOM_INDEX

    # endregion STOCK_MARKET data process

    # region ENTERPRISE data process

    # region ENTERPRISE.STOCK_INFORMATION
    def _process_enterprise_stock_cafef(self) -> None:
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

        folder_path_1 = (
            f"{SCRAPER_RAW_DATA_DIR}/{key_1[0].value}/{key_1[1].value}/{key_1[2].value}"
        )
        folder_path_2 = (
            f"{SCRAPER_RAW_DATA_DIR}/{key_2[0].value}/{key_2[1].value}/{key_2[2].value}"
        )

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
        self._logger.log_info(f'Start processing data in "{table_name}".')

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

        # 9. Update timestamp
        overall_df["update_date"] = datetime.now()

        # 10. Save to database
        self._save_pandas_table_to_database(
            schema_name=Schema.ENTERPRISE.value,
            table_name=Table.STOCK.name,
            primary_keys=Table.STOCK.primary_key,
            df=overall_df,
        )

        self._logger.log_info(f'Finish processing data in "{table_name}".')

    def _process_enterprise_stock(self) -> None:
        self._logger.log_info("Start processing enterprise STOCK data.")

        self._process_enterprise_stock_cafef()

        self._logger.log_info("Finish processing enterprise STOCK data.")

    # endregion ENTERPRISE.STOCK_INFORMATION

    # region ENTERPRISE.DAILY_PRICE
    def _process_enterprise_daily_price_cafef(self) -> None:
        key = (
            ScrapeMainType.ENTERPRISE,
            EnterpriseSubType.DAILY_PRICE,
            DailyPriceSource.CAFEF,
        )

        folder_path = (
            f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
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

        self._logger.log_info(f'Start processing data in "{folder_path}".')

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

            year_list = self._get_year_list_from_start(SCRAPER_START_DATE)

            # Remove current year
            year_list = year_list[:-1]

            # Remove years already processed
            market_df = self._get_market_df()
            process_year = market_df[
                market_df[Table.MARKET.Column.CODE.value] == market_code
            ][Table.MARKET.Column.SAVE_PROGRESS_YEAR.value].item()

            if process_year is None:
                process_year = SCRAPER_START_DATE.year - 1

            year_list = [year for year in year_list if year > process_year]

            for year in year_list:
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
                        data_model_list=[
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

    def _process_enterprise_daily_price(self) -> None:
        self._logger.log_info("Start processing enterprise DAILY_PRICE data.")

        self._process_enterprise_daily_price_cafef()

        self._logger.log_info("Finish processing enterprise DAILY_PRICE data.")

    # endregion ENTERPRISE.DAILY_PRICE

    # endregion ENTERPRISE data process

    def _process_data(self) -> None:
        self._logger.log_info("Start processing data.")

        # Macroeconomics
        self._process_macroeconomics_gdp()
        self._process_macroeconomics_cpi()
        self._process_macroeconomics_ppi()
        self._process_macroeconomics_ipi()
        self._process_macroeconomics_xpi()
        self._process_macroeconomics_mpi()
        self._process_macroeconomics_population()
        self._process_macroeconomics_labor()
        # self._process_macroeconomics_interest_rate()
        # self._process_macroeconomics_export()
        # self._process_macroeconomics_import()
        # self._process_macroeconomics_ipi()
        # self._process_macroeconomics_fdi()
        # self._process_macroeconomics_m2()
        # self._process_macroeconomics_retail()
        # self._process_macroeconomics_population_unemployment()
        # self._process_macroeconomics_gold_price()
        # self._process_macroeconomics_oil_price()
        # self._process_macroeconomics_dow_jones()
        # self._process_macroeconomics_nyse_composite()
        # self._process_macroeconomics_snp_500()
        # self._process_macroeconomics_nasdaq_composite()
        # self._process_macroeconomics_nasdaq_100()

        # Stock market
        # self._process_stock_market_market()
        # self._process_stock_market_vn_index()
        # self._process_stock_market_hnx_index()
        # self._process_stock_market_vn_30_index()
        # self._process_stock_market_vn_100_index()
        # self._process_stock_market_hnx_30_index()
        # self._process_stock_market_upcom_index()

        # Enterprise
        # self._process_enterprise_stock()
        # self._process_enterprise_daily_price()

        self._logger.log_info("Finish processing data.")

    def preprocess_data(self) -> None:
        try:
            self._connect_to_database()
            self._create_schemas()
            self._create_tables()
            self._process_data()
        except Exception as e:
            self._logger.log_error(f"Error preprocessing data: {e}")
        finally:
            self._database_driver.disconnect()
