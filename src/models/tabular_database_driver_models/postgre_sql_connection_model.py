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

        try:
            port_int = int(self.port)
            if not (1024 <= port_int <= 65535):
                raise ValueError
        except ValueError:
            error_msg = f"Invalid port: {self.port}. Port must be an integer between 1024 and 65535."
            self.logger.log_error(error_msg)
            raise ValueError(error_msg)

        self.port = str(port_int)
