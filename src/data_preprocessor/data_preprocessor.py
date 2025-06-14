from dotenv import load_dotenv
import os
import pandas as pd
import re
from glob import glob

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

        success_count = 0
        for row in df.iterrows():
            result = self._database_driver.insert(
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

            if result == DatabaseExecutionStatus.SUCCESS:
                success_count += 1

        self._logger.log_info(
            f"Saved {success_count}/{len(df)} records into table '{schema_name}.{table_name}'"
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
            primary_keys=Table.CPI.primary_key,
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

        # IPI
        # fmt: off
        self._database_driver.create_table(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.IPI.name,
            columns = [
                Column(name=Table.IPI.Column.YEAR.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.IPI.Column.MONTH.value, data_type=DataType.INT(), nullable=False),
                Column(name=Table.IPI.Column.TOTAL.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.IPI.Column.EXTRACTIVE.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.IPI.Column.PROCESSING_AND_MANUFACTURING_INDUSTRY.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.IPI.Column.ELECTRICITY_GENERATION_AND_DISTRIBUTION.value, data_type=DataType.DECIMAL(), nullable=True),
                Column(name=Table.IPI.Column.WATER_SUPPLY_AND_WASTE_MANAGEMENT.value, data_type=DataType.DECIMAL(), nullable=True),
            ],
            primary_keys=Table.IPI.primary_key,
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

    def _process_macroeconomics_exchange_rate_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.EXCHANGE_RATE,
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
            df=df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_interest_rate_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.INTEREST_RATE,
            GdpSource.VIETSTOCK,
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
            df=result_df,
        )

        self._logger.log_info(f'Finish processing data in folder "{folder_path}".')

    def _process_macroeconomics_export_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.EXPORT,
            GdpSource.VIETSTOCK,
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
            df=export_df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_import_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IMPORT,
            GdpSource.VIETSTOCK,
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
            df=import_df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_ipi_vietstock(self) -> None:
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IPI,
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

        # Drop "Đơn vị tính" only if it exists
        if "Đơn vị tính" in df.columns:
            df = df.drop(columns=["Đơn vị tính"])

        # Transpose and rename
        df = df.set_index("Chỉ tiêu").T

        rename_map = {
            "Toàn ngành công nghiệp": Table.IPI.Column.TOTAL.value,
            "Khai khoáng": Table.IPI.Column.EXTRACTIVE.value,
            "Công nghiệp chế biến, chế tạo": Table.IPI.Column.PROCESSING_AND_MANUFACTURING_INDUSTRY.value,
            "Sản xuất và Phân phối điện": Table.IPI.Column.ELECTRICITY_GENERATION_AND_DISTRIBUTION.value,
            "Cung cấp nước, hoạt động quản lý và xử lý rác thải, nước thải": Table.IPI.Column.WATER_SUPPLY_AND_WASTE_MANAGEMENT.value,
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
            table_name=Table.IPI.name,
            df=df,
        )

        self._logger.log_info(f'Finish processing data in "{file_path}".')

    def _process_macroeconomics_exchange_rate(self) -> None:
        self._logger.log_info("Start processing macroeconomics EXCHANGE_RATE data.")

        self._process_macroeconomics_exchange_rate_vietstock()

        self._logger.log_info("Finish processing macroeconomics EXCHANGE_RATE data.")

    def _process_macroeconomics_interest_rate(self) -> None:
        self._logger.log_info("Start processing macroeconomics INTEREST_RATE data.")

        self._process_macroeconomics_interest_rate_vietstock()

        self._logger.log_info("Finish processing macroeconomics INTEREST_RATE data.")

    def _process_macroeconomics_export(self) -> None:
        self._logger.log_info("Start processing macroeconomics EXPORT data.")

        self._process_macroeconomics_export_vietstock()

        self._logger.log_info("Finish processing macroeconomics EXPORT data.")

    def _process_macroeconomics_import(self) -> None:
        self._logger.log_info("Start processing macroeconomics IMPORT data.")

        self._process_macroeconomics_import_vietstock()

        self._logger.log_info("Finish processing macroeconomics IMPORT data.")

    def _process_macroeconomics_import_ipi(self) -> None:
        self._logger.log_info("Start processing macroeconomics IPI data.")

        self._process_macroeconomics_ipi_vietstock()

        self._logger.log_info("Finish processing macroeconomics IPI data.")

    def _process_data(self) -> None:
        self._logger.log_info("Start processing data.")

        # Macroeconomics
        # self._process_macroeconomics_gdp()
        # self._process_macroeconomics_cpi()
        # self._process_macroeconomics_exchange_rate()
        # self._process_macroeconomics_interest_rate()
        # self._process_macroeconomics_export()
        # self._process_macroeconomics_import()
        self._process_macroeconomics_import_ipi()

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
