from abc import ABC, abstractmethod
from typing import List


class TimeSeriesDatabaseDriver(ABC):

    @abstractmethod
    def open_connection(self) -> bool:
        pass

    @abstractmethod
    def close_connection(self) -> bool:
        pass

    @abstractmethod
    def check_bucket_exist(self) -> bool:
        pass

    @abstractmethod
    def write(self) -> bool:
        pass

    @abstractmethod
    def read(self) -> List:
        pass
