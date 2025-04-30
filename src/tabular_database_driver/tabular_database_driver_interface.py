from abc import ABC, abstractmethod
from typing import List

from utils.enums import DatabaseExecutionStatus
from models.tabular_database_driver_models.base_tabular_database_connection_model import (
    BaseTabularDatabaseModel,
)
from models.tabular_database_driver_models.tabular_database_driver_models import *


class TabularDatabaseDriverInterface(ABC):
    @abstractmethod
    def connect(
        self, connection_model: BaseTabularDatabaseModel
    ) -> DatabaseExecutionStatus:
        """Establish a connection to the database."""
        pass

    @abstractmethod
    def disconnect(self) -> DatabaseExecutionStatus:
        """Close the connection to the database."""
        pass

    @abstractmethod
    def create_database(self, database_name: str) -> DatabaseExecutionStatus:
        """Create a new database."""
        pass

    @abstractmethod
    def drop_database(self, database_name: str) -> DatabaseExecutionStatus:
        """Drop an existing database."""
        pass

    @abstractmethod
    def change_database(self, database_name_to_select: str) -> DatabaseExecutionStatus:
        """Change the current database."""
        pass

    @abstractmethod
    def create_schema(self, schema_name: str) -> DatabaseExecutionStatus:
        """Create a new schema for current database."""
        pass

    @abstractmethod
    def drop_schema(self, schema_name: str) -> DatabaseExecutionStatus:
        """Drop an existing schema."""
        pass

    @abstractmethod
    def create_table(
        self,
        schema_name: str,
        table_name: str,
        columns: List[Column],
        primary_key: str,
        foreign_keys: List[ForeignKey] = None,
    ) -> DatabaseExecutionStatus:
        """Create a new table in the current database."""
        pass

    @abstractmethod
    def drop_table(self, schema_name: str, table_name: str) -> DatabaseExecutionStatus:
        """Drop an existing table."""
        pass

    @abstractmethod
    def insert(self, schema_name: str, table_name: str, records: List[Record]):
        """Insert records into a table."""
        pass

    # @abstractmethod
    # def fetch_results(self) -> List:
    #     """Fetch results from the last executed query."""
    #     pass
