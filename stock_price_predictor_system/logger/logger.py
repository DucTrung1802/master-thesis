from enum import Enum
import inspect
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

    def __init__(self, file_name: str, level: LogType = LogType.INFO):
        logging.basicConfig(
            filename=f"{file_name}.log",
            level=level.value,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            encoding="utf-8",
        )
        self._logger = logging.getLogger()

    def _get_caller_context(self):
        """
        Helper method to retrieve the caller's class name and method name.
        Returns:
            tuple: (class_name, method_name)
        """
        caller_frame = inspect.stack()[2]  # [2] gets the frame of the caller
        caller_class = caller_frame.frame.f_locals.get("self", None)
        class_name = caller_class.__class__.__name__ if caller_class else "Unknown"
        method_name = caller_frame.function
        return class_name, method_name

    def log_debug(self, message: str):
        class_name, method_name = self._get_caller_context()
        self._logger.debug(f"{class_name} - {method_name}() - {message}")

    def log_info(self, message: str):
        class_name, method_name = self._get_caller_context()
        self._logger.info(f"{class_name} - {method_name}() - {message}")

    def log_warning(self, message: str):
        class_name, method_name = self._get_caller_context()
        self._logger.warning(f"{class_name} - {method_name}() - {message}")

    def log_error(self, message: str):
        class_name, method_name = self._get_caller_context()
        self._logger.error(f"{class_name} - {method_name}() - {message}")
