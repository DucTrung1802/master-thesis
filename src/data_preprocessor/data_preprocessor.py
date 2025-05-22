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
from utils.utils import (
    convert_numpy_datatype_to_postgres_datatype,
    get_newest_file_path,
)

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
        self, schema_name: Schema, table_name: Table, df: pd.DataFrame
    ) -> None:
        self._logger.log_info(f'Saving dataframe to table "{schema_name}.{table_name}"')

        for row in df.iterrows():
            self._database_driver.insert(
                schema_name=schema_name.value,
                table_name=table_name.value,
                records=[
                    Record(
                        data_model_list=[
                            DataModel(
                                column_name=df.columns[index],
                                data_type=convert_numpy_datatype_to_postgres_datatype(
                                    df.dtypes.iloc[index]
                                ),
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
            table_name=Table.GDP.value,
            columns=[
                Column(name="year", data_type=DataType.INT(), nullable=False),
                Column(name="quarter", data_type=DataType.INT(), nullable=False),
                Column(name="agriculture_share", data_type=DataType.DECIMAL(), nullable=True),
                Column(name="industry_share", data_type=DataType.DECIMAL(), nullable=True),
                Column(name="service_share", data_type=DataType.DECIMAL(), nullable=True),
                Column(name="gdp_true_growth_acc", data_type=DataType.DECIMAL(), nullable=True),
                Column(name="agriculture_true_growth_acc", data_type=DataType.DECIMAL(), nullable=True),
                Column(name="industry_true_growth_acc", data_type=DataType.DECIMAL(), nullable=True),
                Column(name="service_true_growth_acc", data_type=DataType.DECIMAL(), nullable=True),
            ],
            primary_keys=["year", "quarter"],
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
            "agriculture_share",
            "industry_share",
            "service_share",
            "gdp_true_growth_acc",
            "agriculture_true_growth_acc",
            "industry_true_growth_acc",
            "service_true_growth_acc",
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

        df[["year", "quarter"]] = df["period"].apply(extract_year_quarter)

        df = df[
            ["year", "quarter"]
            + [col for col in df.columns if col not in {"year", "quarter"}]
        ]

        df.drop(columns="period", inplace=True)

        df[df.columns] = df[df.columns].apply(pd.to_numeric, errors="coerce")

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS, table_name=Table.GDP, df=df
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

        df = pd.read_csv(file_path)
        df = df.iloc[:, [0, 3]]
        df.columns = [
            "year",
            "gdp_true_growth_acc",
        ]
        df = df[df["year"] >= SCRAPER_START_DATE.year]
        df.insert(1, "quarter", 4)
        df["gdp_true_growth_acc"] = (
            df["gdp_true_growth_acc"].str.rstrip("%").astype(float)
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS, table_name=Table.GDP, df=df
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_gdp_custom(self) -> None:
        # NOTE: At current date (23/05/2025), Worldometer does not have data for 2024. Have to input manually
        self._logger.log_info(f"Start manually input data.")

        df = pd.DataFrame(
            {"year": [2024], "quarter": [4], "gdp_true_growth_acc": [7.09]}
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS, table_name=Table.GDP, df=df
        )

        self._logger.log_info(f"Start manually input data.")

    def _process_macroeconomics_gdp(self) -> None:
        self._logger.log_info("Start processing macroeconomics GDP data.")

        self._process_macroeconomics_gdp_vietstock()
        self._process_macroeconomics_gdp_worldometer()
        self._process_macroeconomics_gdp_custom()  # NOTE: This is for manually input data

        self._logger.log_info("Finish processing macroeconomics GDP data.")

    def _process_data(self) -> None:
        self._logger.log_info("Start processing data.")

        # Macroeconomics
        self._process_macroeconomics_gdp()

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
