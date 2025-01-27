from abc import ABC, abstractmethod
from typing import List


class RelationalDatabaseDriver(ABC):

    @abstractmethod
    def open_connection(self) -> bool:
        pass

    @abstractmethod
    def close_connection(self) -> bool:
        pass

    @abstractmethod
    def check_database_exist(self) -> bool:
        pass

    @abstractmethod
    def check_table_exist(self) -> bool:
        pass

    @abstractmethod
    def create_database(self) -> bool:
        pass

    @abstractmethod
    def create_table(self) -> bool:
        pass

    @abstractmethod
    def truncate_table(self) -> bool:
        pass

    @abstractmethod
    def drop_table(self) -> bool:
        pass

    @abstractmethod
    def select(self) -> List:
        pass

    @abstractmethod
    def insert(self) -> bool:
        pass

    @abstractmethod
    def update(self) -> bool:
        pass

    @abstractmethod
    def delete(self) -> bool:
        pass

    @abstractmethod
    def begin_transaction(self) -> bool:
        pass

    @abstractmethod
    def commit_transaction(self) -> bool:
        pass

    @abstractmethod
    def rollback_transaction(self) -> bool:
        pass
