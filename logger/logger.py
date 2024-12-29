from enum import Enum
import logging
from helper.models import Context


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

    def log_debug(self, context: Context, messsage: str):
        self._logger.debug(f"{context.className} - {context.methodName} - {messsage}")

    def log_info(self, context: Context, messsage: str):
        self._logger.info(f"{context.className} - {context.methodName} - {messsage}")

    def log_warning(self, context: Context, messsage: str):
        self._logger.warning(f"{context.className} - {context.methodName} - {messsage}")

    def log_error(self, context: Context, messsage: str):
        self._logger.error(f"{context.className} - {context.methodName} - {messsage}")
