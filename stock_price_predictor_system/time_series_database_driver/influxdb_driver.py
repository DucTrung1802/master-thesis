from datetime import timezone
from influxdb_client import InfluxDBClient, WriteApi, QueryApi, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from typing import List

from .time_series_database_driver import TimeSeriesDatabaseDriver
from .model import *
from .enums import *
from ..logger.logger import Logger


class InfluxdbDriver(TimeSeriesDatabaseDriver):

    def __init__(self, _logger: Logger):
        self._logger = _logger

        self._client = None

    # region Public methods

    def open_connection(self, _authentication: InfluxdbAuthentication) -> bool:
        self._authentication = _authentication

        self._client: InfluxDBClient = None

        self._writer: WriteApi = None
        self._reader: QueryApi = None

        try:
            print("\nStart to connect to InfluxDB.")
            self._logger.log_info("Start to connect to InfluxDB.")

            self._client = InfluxDBClient(
                url=self._authentication.url,
                token=self._authentication.token,
                org=self._authentication.org,
            )

            self._writer = self._client.write_api(write_options=SYNCHRONOUS)
            self._reader = self._client.query_api()

            # Execute a query for checking the connection
            query = f"""from(bucket: "root")
  |> range(start: -1s)"""

            self._reader.query(query)

            print("\nConnected to InfluxDB.")
            self._logger.log_info("Connected to InfluxDB.")
            return True

        except Exception as e:
            print(f"\nFailed to connect to InfluxDB. Error: {e}")
            self._logger.log_error(f"Failed to connect to InfluxDB. Error: {e}")
            return False

    def close_connection(self) -> bool:
        self._client.close()

        print("\nClosed connection to InfluxDB.")
        self._logger.log_info(
            f"Closed connection to InfluxDB.",
        )

    def check_bucket_exist(self, bucket_name: str) -> bool:
        query = f"""from(bucket: "{bucket_name}")
  |> range(start: -1s)"""

        try:
            self._reader.query(query)
            return True

        except Exception as e:
            print(f'\nBucket "{bucket_name}" does not exist.')
            self._logger.log_error(f'Bucket "{bucket_name}" does not exist.')
            return False

    def write(self, write_component: WriteComponent) -> bool:
        if not self.check_bucket_exist(write_component.bucket):
            print(
                f"\nCannot write to bucket {write_component.bucket} since it does not exist."
            )
            self._logger.log_error(
                f"Cannot write to bucket {write_component.bucket} since it does not exist."
            )
            return False

        if isinstance(write_component.point_component_list, PointComponent):
            write_component.point_component_list = [
                write_component.point_component_list
            ]

        if (
            isinstance(write_component.point_component_list, List)
            and len(write_component.point_component_list) <= 0
        ):
            print("\nCannot write since Point list is empty.")
            self._logger.log_warning("Cannot write since Point list is empty.")
            return False

        records = [
            Point.from_dict(
                {
                    "measurement": point_component.measurement,
                    "tags": point_component.tags,
                    "fields": point_component.fields,
                    "time": point_component.time,
                }
            )
            for point_component in write_component.point_component_list
        ]

        try:
            self._writer.write(bucket=write_component.bucket, record=records)

            print(
                f'\nSuccessfully wrote {len(records)} points to bucket: "{write_component.bucket}".'
            )
            self._logger.log_info(
                f'Successfully wrote {len(records)} points to bucket: "{write_component.bucket}".'
            )
            return True

        except Exception as e:
            print(f"\nCannot write query. Error: {e}")
            self._logger.log_error(f"Cannot write query. Error: {e}")
            return False

    def read(self, read_component: ReadComponent) -> List:
        start_time = None
        end_time = None

        if not self.check_bucket_exist(read_component.bucket):
            return False

        if not read_component.start_time:
            print(f'\n"start_time" is invalid.')
            self._logger.log_error(f'"start_time" is invalid.')
            return []

        if isinstance(read_component.start_time, TimeInterval):
            start_time = read_component.start_time.format_time()

        elif isinstance(read_component.start_time, datetime):
            # Convert start_time and end_time to UTC
            start_time = read_component.start_time.astimezone(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            end_time = read_component.end_time.astimezone(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        else:
            print('\nUnsupport type of "start_time".')
            self._logger.log_error('Unsupport type of "start_time".')
            return False

        query = f"""from(bucket: "{read_component.bucket}")
  |> range(start: {start_time}{f", stop: {end_time}" if end_time else ""})
  |> filter(fn: (r) => r["_measurement"] == "{read_component.measurement}")"""

        try:
            table_list = self._reader.query(query=query)

            number_of_record = 0
            records = []
            if len(table_list):
                records = table_list[0].records
                number_of_record = len(records)

            print(
                f'\nSuccessfully read {number_of_record} records from bucket: "{read_component.bucket}".'
            )
            self._logger.log_info(
                f'Successfully read {number_of_record} records from bucket: "{read_component.bucket}".'
            )

            return records

        except Exception as e:
            print(f"\nCannot read query. Error: {e}")
            self._logger.log_error(f"Cannot read query. Error: {e}")
            return False
