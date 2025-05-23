from dotenv import load_dotenv
import os
import pandas as pd
import re

from logger.logger import Logger
from models.tabular_database_driver_models.postgre_sql_connection_model import (
    PostgreSQLConnectionModel,
)
from models.tabular_database_driver_models.tabular_database_driver_models import *
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from utils.constants import SCRAPER_RAW_DATA_DIR
from utils.enums import *
from utils.utils import get_newest_file_path

load_dotenv()


class DataPreprocessor:
    def __init__(self, logger: Logger):
        self._logger = logger
        self._database_driver = PostgreSQLDriver(logger=logger)

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
        self, schema_name: str, table_name: str, df: pd.DataFrame
    ) -> None:
        self._logger.log_info(f'Saving dataframe to table "{schema_name}.{table_name}"')

        # Remove all rows that have NaN values in all columns
        df = df.dropna(how="all")

        for row in df.iterrows():
            self._database_driver.insert(
                schema_name=schema_name,
                table_name=table_name,
                records=[
                    Record(
                        data_model_list=[
                            DataModel(
                                column_name=df.columns[index],
                                value=(
                                    row[1].iloc[index]
                                    if not pd.isnull(row[1].iloc[index])
                                    else None
                                ),
                            )
                            for index in range(len(df.columns))
                        ]
                    )
                ],
            )

    def _create_schemas(self) -> None:
        self._logger.log_info("Start creating schemas.")

        self._database_driver.create_schema(Schema.MACROECONOMICS.value)
        self._database_driver.create_schema(Schema.STOCK_MARKET.value)
        self._database_driver.create_schema(Schema.ENTERPRISE.value)

        self._logger.log_info("Finish creating schemas.")

    def _create_macroeconomics_tables(self) -> None:
        self._logger.log_info("Start creating macroeconomics tables.")

        # GDP
        # fmt: off
        self._database_driver.create_table(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.GDP.name,
            columns = [
                Column(name=Table.GDP.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.GDP.Column.QUARTER.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.GDP.Column.AGRICULTURE_SHARE.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.GDP.Column.INDUSTRY_SHARE.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.GDP.Column.SERVICE_SHARE.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.GDP.Column.GDP_TRUE_GROWTH_ACC.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.GDP.Column.AGRICULTURE_TRUE_GROWTH_ACC.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.GDP.Column.INDUSTRY_TRUE_GROWTH_ACC.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.GDP.Column.SERVICE_TRUE_GROWTH_ACC.value, data_type=DataType.DECIMAL(), nullable=True),
            ],
            primary_keys=[Table.GDP.Column.YEAR.value, Table.GDP.Column.QUARTER.value],
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
                Column(name=Table.CPI.Column.CPI.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.CPI.Column.FNB_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.CPI.Column.STAPLE_FOOD.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.CPI.Column.FOOD.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.CPI.Column.FAFH.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.CPI.Column.DRINK_AND_TOBACO.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.CPI.Column.WEARING.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.CPI.Column.HOUSING_AND_BUILDING_MATERIALS.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.CPI.Column.HOUSEHOLD_APPLIANCES_AND_EQUIPMENT.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.CPI.Column.MEDICINES_AND_MEDICAL_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.CPI.Column.TRAFFIC.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.CPI.Column.POST_AND_TELECOMMUNICATIONS.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.CPI.Column.EDUCATION.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.CPI.Column.CULTURE_ENTERTAINMENT_AND_TOURISM.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.CPI.Column.OTHER_SUPPLIES_AND_SERVICES.value, data_type=DataType.DECIMAL(), nullable=True),
            ],
            primary_keys=[Table.CPI.Column.YEAR.value, Table.CPI.Column.MONTH.value],
        )
        # fmt: on

        self._logger.log_info("Finish creating macroeconomics tables.")

    def _create_tables(self) -> None:
        self._logger.log_info("Start creating tables.")

        self._create_macroeconomics_tables()

        self._logger.log_info("Finish creating tables.")

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
        df = df[
            [
                col
                for col in df.columns
                if not col.startswith(("Quý 2", "Quý 3", "Quý 4"))
            ]
        ]

        df = df.transpose().iloc[2:, :7]

        df.columns = [
            Table.GDP.Column.AGRICULTURE_SHARE.value,
            Table.GDP.Column.INDUSTRY_SHARE.value,
            Table.GDP.Column.SERVICE_SHARE.value,
            Table.GDP.Column.GDP_TRUE_GROWTH_ACC.value,
            Table.GDP.Column.AGRICULTURE_TRUE_GROWTH_ACC.value,
            Table.GDP.Column.INDUSTRY_TRUE_GROWTH_ACC.value,
            Table.GDP.Column.SERVICE_TRUE_GROWTH_ACC.value,
        ]

        df = df.reset_index().rename(columns={"index": "period"})
        df = df[df["period"] != "Đồ thị"].copy()

        def extract_year_quarter(period):
            year_match = re.search(r"(\d{4})", period)
            year = int(year_match.group(1)) if year_match else None

            if "Quý 1" in period:
                quarter = 1
            elif "6 tháng" in period:
                quarter = 2
            elif "9 tháng" in period:
                quarter = 3
            else:
                quarter = None

            return pd.Series([year, quarter])

        df[[Table.GDP.Column.YEAR.value, Table.GDP.Column.QUARTER.value]] = df[
            "period"
        ].apply(extract_year_quarter)

        df = df[
            [Table.GDP.Column.YEAR.value, Table.GDP.Column.QUARTER.value]
            + [
                col
                for col in df.columns
                if col
                not in {Table.GDP.Column.YEAR.value, Table.GDP.Column.QUARTER.value}
            ]
        ]

        df.drop(columns="period", inplace=True)

        df[df.columns] = df[df.columns].apply(pd.to_numeric, errors="coerce")

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value, table_name=Table.GDP.name, df=df
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_gdp_worldometer(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.GDP,
            GdpSource.WORLDOMETER,
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
        df = df.iloc[:, [0, 3]]
        df.columns = [
            Table.GDP.Column.YEAR.value,
            Table.GDP.Column.GDP_TRUE_GROWTH_ACC.value,
        ]
        df = df[df[Table.GDP.Column.YEAR.value] >= SCRAPER_START_DATE.year]
        df.insert(1, Table.GDP.Column.QUARTER.value, 4)
        df[Table.GDP.Column.GDP_TRUE_GROWTH_ACC.value] = (
            df[Table.GDP.Column.GDP_TRUE_GROWTH_ACC.value].str.rstrip("%").astype(float)
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value, table_name=Table.GDP.name, df=df
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_gdp_custom(self) -> None:
        # NOTE: At current date (23/05/2025), Worldometer does not have data for 2024. Have to input manually
        self._logger.log_info(f"Start manually input data.")

        # Add logic for processing data here
        df = pd.DataFrame(
            {
                Table.GDP.Column.YEAR.value: [2024],
                Table.GDP.Column.QUARTER.value: [4],
                Table.GDP.Column.GDP_TRUE_GROWTH_ACC.value: [7.09],
            }
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value, table_name=Table.GDP.name, df=df
        )

        self._logger.log_info(f"Start manually input data.")

    def _process_macroeconomics_gdp(self) -> None:
        self._logger.log_info("Start processing macroeconomics GDP data.")

        self._process_macroeconomics_gdp_vietstock()
        self._process_macroeconomics_gdp_worldometer()
        self._process_macroeconomics_gdp_custom()  # NOTE: This is for manually input data

        self._logger.log_info("Finish processing macroeconomics GDP data.")

    def _process_macroeconomics_cpi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.CPI,
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
        df = df.drop(df.columns[:2], axis=1)
        df = df.transpose()
        df.columns = [
            Table.CPI.Column.CPI.value,
            Table.CPI.Column.FNB_SERVICES.value,
            Table.CPI.Column.STAPLE_FOOD.value,
            Table.CPI.Column.FOOD.value,
            Table.CPI.Column.FAFH.value,
            Table.CPI.Column.DRINK_AND_TOBACO.value,
            Table.CPI.Column.WEARING.value,
            Table.CPI.Column.HOUSING_AND_BUILDING_MATERIALS.value,
            Table.CPI.Column.HOUSEHOLD_APPLIANCES_AND_EQUIPMENT.value,
            Table.CPI.Column.MEDICINES_AND_MEDICAL_SERVICES.value,
            Table.CPI.Column.TRAFFIC.value,
            Table.CPI.Column.POST_AND_TELECOMMUNICATIONS.value,
            Table.CPI.Column.EDUCATION.value,
            Table.CPI.Column.CULTURE_ENTERTAINMENT_AND_TOURISM.value,
            Table.CPI.Column.OTHER_SUPPLIES_AND_SERVICES.value,
        ]
        df[Table.CPI.Column.MONTH.value] = (
            df.index.to_series().str.extract(r"Tháng (\d+)/\d+")[0].astype("Int64")
        )
        df[Table.CPI.Column.YEAR.value] = (
            df.index.to_series().str.extract(r"Tháng \d+/(\d+)")[0].astype("Int64")
        )
        df = df[
            [Table.CPI.Column.YEAR.value, Table.CPI.Column.MONTH.value]
            + [
                col
                for col in df.columns
                if col
                not in [Table.CPI.Column.YEAR.value, Table.CPI.Column.MONTH.value]
            ]
        ]
        df = df.reset_index(drop=True)

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value, table_name=Table.CPI.name, df=df
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_cpi(self) -> None:
        self._logger.log_info("Start processing macroeconomics CPI data.")

        self._process_macroeconomics_cpi_vietstock()

        self._logger.log_info("Finish processing macroeconomics CPI data.")

    def _process_data(self) -> None:
        self._logger.log_info("Start processing data.")

        # Macroeconomics
        self._process_macroeconomics_gdp()
        self._process_macroeconomics_cpi()

        # Stock market

        # Enterprise

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
