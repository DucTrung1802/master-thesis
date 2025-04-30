from models.tabular_database_driver_models.tabular_database_driver_models import (
    DataType,
)


def format_value(value, data_type: DataType):
    """Format value based on its data type for SQL query."""
    match data_type:
        case DataType.VARCHAR:
            return f"'{str(value).replace("'", "''") if value else value}'"
        case DataType.DATE:
            return f"DATE '{value}'"
        case DataType.TIME:
            return f"TIME '{value}'"
        case DataType.TIMESTAMP:
            return f"TIMESTAMP '{value}'"
        case _:
            return str(value)
