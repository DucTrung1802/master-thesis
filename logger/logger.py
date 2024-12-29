from enum import Enum
import logging


class LogType(Enum):
    CRITICAL = logging.CRITICAL
    FATAL = logging.FATAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG
    NOTSET = logging.NOTSET


class Logger:

    def __init__(self, file_name: str, level: LogType):
        logging.basicConfig(
            filename=f"{file_name}.log",
            level=level.value,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        self._logger = logging.getLogger()

    def log_debug(self, class_name: str, method_name: str, messsage: str):
        self._logger.debug(f"{class_name} - {method_name} - {messsage}")

    def log_info(self, class_name: str, method_name: str, messsage: str):
        self._logger.info(f"{class_name} - {method_name} - {messsage}")

    def log_warning(self, class_name: str, method_name: str, messsage: str):
        self._logger.warning(f"{class_name} - {method_name} - {messsage}")

    def log_error(self, class_name: str, method_name: str, messsage: str):
        self._logger.error(f"{class_name} - {method_name} - {messsage}")
