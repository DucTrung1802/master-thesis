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

from orchestration import enabled
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
    """A `SwitchHandler`, now built from `config.json` rather than a file of its own.

    ⚠️ **`src/switch_config.json` WAS DELETED ON 2026-08-06** and `enabled.py` raises if
    it reappears. Its only live content was TradingView's parameter tree — which
    countries, sectors, brokers and categories each asset class enumerates — and that is
    the `parameters` section of [config.json](config.json) now. The other 19 keys were
    run-plan switches, dead since Phase 5 made asset selection the run plan.

    ⚠️ Under Dagster this resource is NOT the run plan. It survives for two reasons and
    they are unequal: TradingView's adders genuinely read parameters out of it, and
    `DataPreprocessor.__init__` plus six scrapers merely REQUIRE one in their
    constructor without ever calling it. The second group gets `build()`, which is an
    empty handler and honestly so.
    """

    def build(self, logger: Logger) -> SwitchHandler:
        """An EMPTY handler, for the callers that take one and never consult it.

        CafeF, CafeF-index, Simplize, the PDF/news/financials scrapers and
        `DataPreprocessor` all accept a `SwitchHandler` and none of them calls
        `is_enabled` on a path that matters — the assets drive their per-tab methods
        directly. Handing them an empty handler rather than a loaded one makes that
        visible instead of implying a dependency none of them has.
        """
        return SwitchHandler(logger=logger)

    def build_trading_view(self, logger: Logger, phase: str) -> SwitchHandler:
        """The handler the TradingView adders read: `parameters` + this phase forced on.

        Replaces `build_unblocked` for TradingView. The parameter leaves come from
        `config.json`; the three run-plan ancestors (`web_scraper`,
        `web_scraper/trading_view`, `web_scraper/trading_view/<phase>`) are forced true
        because they mean "run this stage", which is Dagster's decision now and not a
        JSON file's. See `enabled.trading_view_run_plan_switches` for why the ASSET
        CLASS prefix is pointedly not forced with them.
        """
        return SwitchHandler(
            logger=logger, switches=enabled.trading_view_run_plan_switches(phase)
        )

    def build_unblocked(self, logger: Logger, *prefixes: str) -> SwitchHandler:
        """An empty handler with `prefixes` forced true. GICS only.

        `GicsScraper.scrape` gates itself on `web_scraper/gics/structure`, so the asset
        has to force that path on — the switch means "run this stage" and Dagster has
        already decided that by selecting the asset. This is the last caller: with the
        TradingView assets moved to `build_trading_view`, nothing else needs it.

        ⚠️ It is force-ONLY now, with no file underneath, so it cannot resurrect the
        `IndexError` described in `enabled.trading_view_run_plan_switches` — forcing a
        prefix true can only make it a leaf if something reads leaves below it, and GICS
        reads none.
        """
        return SwitchHandler(logger=logger, switches={p: True for p in prefixes})


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
