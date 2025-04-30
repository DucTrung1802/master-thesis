from dataclasses import dataclass

from models.tabular_database_driver_models.base_tabular_database_connection_model import (
    BaseTabularDatabaseModel,
)


@dataclass
class PostgreSQLConnectionModel(BaseTabularDatabaseModel):
    port: int = 5432
    database: str = "postgres"

    def __post_init__(self):
        super().__post_init__()

        if not isinstance(self.port, int) or self.port not in range(1024, 65536):
            self.logger.log_error(
                f"Invalid port: {self.port}. Port must be an integer between 1024 and 65535."
            )
            raise ValueError(
                f"Invalid port: {self.port}. Port must be an integer between 1024 and 65535."
            )

        self.port = str(self.port)
