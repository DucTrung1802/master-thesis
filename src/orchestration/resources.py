# src\orchestration\resources.py
"""Shared resources: the repo `Logger`, and a connected `DataPreprocessor`.

Both exist so an asset body stays two or three lines. The important design choice
is in `preprocessor()`: it reproduces the connect / `CREATE DATABASE` / `CREATE
SCHEMA` preamble of `DataPreprocessor.ingest_*_data` but NOT their
`try/except/finally` — see the docstring there for why that matters.
"""

from contextlib import contextmanager
from typing import Iterator

from dagster import ConfigurableResource

from orchestration._bootstrap import bootstrap

bootstrap()

import os

from dotenv import load_dotenv

from orchestration.preprocessor import DataPreprocessor
from dtos.tabular_database_driver_dtos.postgre_sql_connection_dto import (
    PostgreSQLConnectionDto,
)
from logger.logger import LogType, Logger
from utils.constants import LOG_FILE_BASE, DATABASE_MAIN_V2
from utils.switch_handler import SwitchHandler

load_dotenv()


class RepoLogger(ConfigurableResource):
    """The repo's own `Logger`, unchanged.

    Kept as-is deliberately for now: `Logger` is threaded through the constructor of
    every scraper, `DataPreprocessor` and `ThreadManager`, so swapping it for
    Dagster's `context.log` is a wide refactor and a separate decision. The cost of
    keeping it is that `logs/app.log` is a single global file written by
    `logging.basicConfig` — fine under Dagster's in-process executor, interleaved
    under the multiprocess one. That is why `definitions.py` pins in-process.
    """

    log_file_base: str = LOG_FILE_BASE

    def build(self) -> Logger:
        return Logger(file_name=self.log_file_base, level=LogType.INFO)


class SwitchConfig(ConfigurableResource):
    """The existing `SwitchHandler`.

    ⚠️ Under Dagster this is NO LONGER the run plan — asset selection is. It is still
    needed because `DataPreprocessor.__init__` requires one, and because a few
    scrapers read genuine PARAMETERS out of it (TradingView's enabled
    country/sector leaves). Assets therefore call the per-tab methods
    (`scrape_all_index_price`) directly rather than the switch-gated `scrape()`,
    so a materialisation runs regardless of what the JSON says.
    """

    config_path: str = "src/switch_config.json"

    def build(self, logger: Logger) -> SwitchHandler:
        return SwitchHandler(logger=logger, config_path=self.config_path)

    def build_unblocked(self, logger: Logger, *prefixes: str) -> SwitchHandler:
        """A handler with the given RUN-PLAN prefixes forced true, leaving everything
        below them exactly as the JSON has it.

        ⚠️ NEEDED BECAUSE THE TWO ROLES ARE ENTANGLED IN ONE TREE. `is_enabled` requires
        EVERY ancestor to be true, and the committed config has `"web_scraper": false`
        plus `".../links": false` — so a TradingView asset would enumerate zero
        countries/sectors and scrape nothing, silently, no matter what the user selected
        in Dagster. Those ancestors mean "run this stage", which is now Dagster's
        decision; the leaves below (which countries, which sectors) are PARAMETERS and
        must keep coming from the JSON.

        Only the TradingView assets need this — every other scraper is driven by calling
        its per-tab method directly, which consults no switch at all. Phase 5 of the
        migration deletes the run-plan keys and this method with them.
        """
        handler = SwitchHandler(logger=logger, config_path=self.config_path)
        for prefix in prefixes:
            handler._switches[prefix] = True
        return handler


class PreprocessorResource(ConfigurableResource):
    """Yields a `DataPreprocessor` whose driver is connected, and disconnects after.

    ⚠️ NO `except` CLAUSE, and that is the point. `ingest_bronze_data` and its two
    siblings wrap every ingest in `try: ... except Exception as e: log_error(...)`,
    so a failed ingest returns normally and the caller cannot tell it failed — the
    only trace is one line in `app.log`. Under Dagster that would mark a run GREEN on
    a failure. Here the exception propagates and the asset fails, which is the whole
    reason for the migration.
    """

    logger: RepoLogger
    switches: SwitchConfig

    @contextmanager
    def session(self, schema: str) -> Iterator[DataPreprocessor]:
        # ⚠️ RE-ASSERT `src` ON sys.path, AT RUN TIME. Importing this module ran
        # `bootstrap()` already — but Dagster loads a code location inside a context
        # manager that RESTORES sys.path afterwards, so by the time a step actually
        # executes, the entry bootstrap added is gone. Modules imported during the load
        # survive in `sys.modules` and hide the problem; a module imported LAZILY at run
        # time does not. `_build_transform_func_map` imports `ta.ta_functions` on the
        # first `_helper_transform` call, which is why `gold/stocks_financials_bank_fa`
        # died with `ModuleNotFoundError: No module named 'ta'` in the step subprocess
        # while every earlier asset passed. `bootstrap()` is idempotent and costs
        # nothing, so it belongs at the entry to every session.
        bootstrap()

        logger = self.logger.build()
        preprocessor = DataPreprocessor(
            logger=logger, switch_handler=self.switches.build(logger)
        )
        driver = preprocessor._database_driver

        # Same preamble as ingest_bronze_data: connect to `postgres`, then create the
        # real database and schema if they are absent (both are IF NOT EXISTS).
        driver.connect(
            PostgreSQLConnectionDto(
                logger=logger,
                host=os.getenv("POSTGRES_HOST"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                port=os.getenv("POSTGRES_PORT"),
                database="postgres",
            )
        )
        driver.create_database(DATABASE_MAIN_V2)
        driver.create_schema(schema)

        try:
            yield preprocessor
        finally:
            driver.disconnect()
