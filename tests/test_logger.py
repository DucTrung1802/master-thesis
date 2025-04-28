import os
import pytest
import logging
from unittest import mock
from logger.logger import (
    Logger,
    LogType,
)


@pytest.fixture
def logger(tmp_path):
    log_file = tmp_path / "test_log"
    return Logger(str(log_file), level=LogType.DEBUG)


def test_log_file_created(tmp_path):
    log_file = tmp_path / "mylog"
    Logger(str(log_file))
    assert os.path.exists(f"{log_file}.log")


def test_directory_created(tmp_path):
    nested_dir = tmp_path / "nested" / "deeper"
    log_file = nested_dir / "mylog"

    Logger(str(log_file))
    assert nested_dir.exists()
    assert os.path.exists(f"{log_file}.log")


def test_log_info(logger, caplog):
    with caplog.at_level(logging.INFO):
        logger.log_info("This is an info message")
    assert "This is an info message" in caplog.text
    assert "log_info()" in caplog.text


def test_log_debug(logger, caplog):
    with caplog.at_level(logging.DEBUG):
        logger.log_debug("This is a debug message")
    assert "This is a debug message" in caplog.text
    assert "log_debug()" in caplog.text


def test_log_warning(logger, caplog):
    with caplog.at_level(logging.WARNING):
        logger.log_warning("This is a warning message")
    assert "This is a warning message" in caplog.text
    assert "log_warning()" in caplog.text


def test_log_error(logger, caplog):
    with caplog.at_level(logging.ERROR):
        logger.log_error("This is an error message")
    assert "This is an error message" in caplog.text
    assert "log_error()" in caplog.text


def test_get_caller_context_returns_correct_values(logger):
    with mock.patch.object(
        logger, "_get_caller_context", wraps=logger._get_caller_context
    ) as mocked_context:
        logger.log_info("Testing context")
        mocked_context.assert_called_once()


def test_create_log_directory_failure(tmp_path, capsys):
    # Simulate os.makedirs throwing an exception
    bad_path = tmp_path / "bad_dir" / "log"
    with mock.patch("os.makedirs", side_effect=OSError("Cannot create directory")):
        Logger(str(bad_path))
        captured = capsys.readouterr()
        assert "Error creating directory: Cannot create directory" in captured.out


def test_create_log_file_failure(tmp_path, capsys):
    # Simulate open() throwing an exception
    log_file = tmp_path / "testlog"

    with mock.patch("builtins.open", side_effect=OSError("Cannot create file")):
        Logger(str(log_file))
        captured = capsys.readouterr()
        assert "Error creating log file: Cannot create file" in captured.out
