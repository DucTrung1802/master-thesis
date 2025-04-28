import pytest
from unittest import mock
from tabular_database_driver.postgre_sql_driver import (
    PostgreSQLDriver,
)
from utils.enums import DatabaseExecutionStatus


@pytest.fixture
def mock_logger():
    return mock.Mock()


@pytest.fixture
def driver(mock_logger):
    return PostgreSQLDriver(logger=mock_logger)


@pytest.fixture
def mock_connection_and_cursor():
    mock_conn = mock.Mock()
    mock_cursor = mock.Mock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


def test_connect_success(driver, mock_connection_and_cursor, monkeypatch):
    mock_conn, mock_cursor = mock_connection_and_cursor
    monkeypatch.setattr("psycopg2.connect", lambda **kwargs: mock_conn)

    from models.tabular_database_driver_models.postgre_sql_connection_model import (
        PostgreSQLConnectionModel,
    )

    fake_logger = mock.Mock()
    connection_model = PostgreSQLConnectionModel(
        logger=fake_logger,
        host="localhost",
        user="user",
        password="pass",
        port=5432,
        database="testdb",
    )

    status = driver.connect(connection_model)
    assert driver.connection == mock_conn
    assert driver.cursor == mock_cursor
    driver.logger.log_info.assert_called_once()


def test_connect_failure(driver, monkeypatch):
    monkeypatch.setattr(
        "psycopg2.connect", mock.Mock(side_effect=Exception("Connect fail"))
    )

    from models.tabular_database_driver_models.postgre_sql_connection_model import (
        PostgreSQLConnectionModel,
    )

    fake_logger = mock.Mock()
    connection_model = PostgreSQLConnectionModel(
        logger=fake_logger,
        host="localhost",
        user="user",
        password="pass",
        port=5432,
        database="testdb",
    )

    with pytest.raises(ValueError):
        driver.connect(connection_model)
    driver.logger.log_error.assert_called_once()


def test_disconnect(driver, mock_connection_and_cursor):
    mock_conn, mock_cursor = mock_connection_and_cursor
    driver.connection = mock_conn
    driver.cursor = mock_cursor

    driver.disconnect()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()
    driver.logger.log_info.assert_called_once()


def test_create_database_success(driver):
    driver.cursor = mock.Mock()
    result = driver.create_database("newdb")
    driver.cursor.execute.assert_called_once()
    driver.logger.log_info.assert_called_once()
    assert result == DatabaseExecutionStatus.SUCCESS


def test_create_database_already_exists(driver):
    driver.cursor = mock.Mock()
    driver.cursor.execute.side_effect = Exception("already exists")
    result = driver.create_database("existingdb")
    driver.logger.log_warning.assert_called_once()
    assert result == DatabaseExecutionStatus.ALREADY_EXISTS


def test_create_database_error(driver):
    driver.cursor = mock.Mock()
    driver.cursor.execute.side_effect = Exception("random error")
    result = driver.create_database("faildb")
    driver.logger.log_error.assert_called_once()
    assert result == DatabaseExecutionStatus.ERROR


def test_drop_database_success(driver):
    driver.cursor = mock.Mock()
    result = driver.drop_database("somedb")
    driver.cursor.execute.assert_called_once()
    driver.logger.log_info.assert_called_once()
    assert result == DatabaseExecutionStatus.SUCCESS


def test_drop_database_does_not_exist(driver):
    driver.cursor = mock.Mock()
    driver.cursor.execute.side_effect = Exception("does not exist")
    result = driver.drop_database("missingdb")
    driver.logger.log_warning.assert_called_once()
    assert result == DatabaseExecutionStatus.DOES_NOT_EXIST


def test_drop_database_error(driver):
    driver.cursor = mock.Mock()
    driver.cursor.execute.side_effect = Exception("random error")
    result = driver.drop_database("error_db")
    driver.logger.log_error.assert_called_once()
    assert result == DatabaseExecutionStatus.ERROR


def test_create_schema_success(driver):
    driver.cursor = mock.Mock()
    result = driver.create_schema("new_schema")
    driver.cursor.execute.assert_called_once()
    driver.logger.log_info.assert_called_once()
    assert result == DatabaseExecutionStatus.SUCCESS


def test_create_schema_already_exists(driver):
    driver.cursor = mock.Mock()
    driver.cursor.execute.side_effect = Exception("already exists")
    result = driver.create_schema("existing_schema")
    driver.logger.log_warning.assert_called_once()
    assert result == DatabaseExecutionStatus.ALREADY_EXISTS


def test_create_schema_error(driver):
    driver.cursor = mock.Mock()
    driver.cursor.execute.side_effect = Exception("other error")
    result = driver.create_schema("fail_schema")
    driver.logger.log_error.assert_called_once()
    assert result == DatabaseExecutionStatus.ERROR


def test_drop_schema_success(driver):
    driver.cursor = mock.Mock()
    result = driver.drop_schema("some_schema")
    driver.cursor.execute.assert_called_once()
    driver.logger.log_info.assert_called_once()
    assert result == DatabaseExecutionStatus.SUCCESS


def test_drop_schema_does_not_exist(driver):
    driver.cursor = mock.Mock()
    driver.cursor.execute.side_effect = Exception("does not exist")
    result = driver.drop_schema("missing_schema")
    driver.logger.log_warning.assert_called_once()
    assert result == DatabaseExecutionStatus.DOES_NOT_EXIST


def test_drop_schema_other_object_depend(driver):
    driver.cursor = mock.Mock()
    driver.cursor.execute.side_effect = Exception("other objects")
    result = driver.drop_schema("dependent_schema")
    driver.logger.log_warning.assert_called_once()
    assert result == DatabaseExecutionStatus.OTHER_OBJECT_DEPEND


def test_drop_schema_error(driver):
    driver.cursor = mock.Mock()
    driver.cursor.execute.side_effect = Exception("other error")
    result = driver.drop_schema("fail_schema")
    driver.logger.log_error.assert_called_once()
    assert result == DatabaseExecutionStatus.ERROR


def test_create_table_success(driver):
    driver.cursor = mock.Mock()
    from models.tabular_database_driver_models.tabular_database_driver_models import (
        Column,
        ForeignKey,
    )

    columns = [Column(name="id", data_type="INT", nullable=False)]
    foreign_keys = [
        ForeignKey(column_name="id", ref_table="other_table", ref_column="id")
    ]

    result = driver.create_table("schema", "table", columns, "id", foreign_keys)
    driver.cursor.execute.assert_called_once()
    driver.logger.log_info.assert_called_once()
    assert result == DatabaseExecutionStatus.SUCCESS


def test_create_table_already_exists(driver):
    driver.cursor = mock.Mock()
    driver.cursor.execute.side_effect = Exception("already exists")
    from models.tabular_database_driver_models.tabular_database_driver_models import (
        Column,
    )

    columns = [Column(name="id", data_type="INT", nullable=False)]

    result = driver.create_table("schema", "table", columns, "id")
    driver.logger.log_warning.assert_called_once()
    assert result == DatabaseExecutionStatus.ALREADY_EXISTS


def test_create_table_error(driver):
    driver.cursor = mock.Mock()
    driver.cursor.execute.side_effect = Exception("some error")
    from models.tabular_database_driver_models.tabular_database_driver_models import (
        Column,
    )

    columns = [Column(name="id", data_type="INT", nullable=False)]

    result = driver.create_table("schema", "table", columns, "id")
    driver.logger.log_error.assert_called_once()
    assert result == DatabaseExecutionStatus.ERROR
