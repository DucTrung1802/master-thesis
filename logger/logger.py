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
            format="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s",
        )

    def log_debug(self, messsage: str, module_name: str = None):
        self._logger = logging.getLogger(module_name)
        self._logger.debug(messsage)

    def log_info(self, messsage: str, module_name: str = None):
        self._logger = logging.getLogger(module_name)
        self._logger.info(messsage)

    def log_warning(self, messsage: str, module_name: str = None):
        self._logger = logging.getLogger(module_name)
        self._logger.warning(messsage)

    def log_error(self, messsage: str, module_name: str = None):
        self._logger = logging.getLogger(module_name)
        self._logger.error(messsage)
