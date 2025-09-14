from dataclasses import dataclass

from logger.logger import Logger


@dataclass
class BaseTabularDatabaseDto:
    """
    Base class for tabular database models.
    This class serves as a template for creating specific database models.
    """

    logger: Logger
    host: str
    user: str
    password: list

    def __post_init__(self):
        if not self.host:
            self.logger.log_error("Host cannot be empty.")
            raise ValueError("Host cannot be empty.")
        if not self.user:
            self.logger.log_error("User cannot be empty.")
            raise ValueError("User cannot be empty.")
        if not self.password:
            self.logger.log_error("Password cannot be empty.")
            raise ValueError("Password cannot be empty.")
