from enum import Enum


class DatabaseExecutionStatus(Enum):
    """
    Enum for representing the status of a database query.
    """

    SUCCESS = "success"
    ALREADY_EXISTS = "already_exists"

    ERROR = "error"
