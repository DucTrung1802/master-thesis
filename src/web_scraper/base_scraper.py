# src\web_scraper\base_scraper.py

from abc import ABC, abstractmethod
from typing import Dict, Optional, Type

from logger.logger import Logger
from utils.switch_handler import SwitchHandler
from utils.constants import (
    THREAD_MANAGER_POWER,
    SCRAPER_RETRY_ATTEMPTS,
    SCRAPER_RETRY_DELAY,
    SCRAPER_MAX_WORKERS,
)
from thread_manager.thread_manager import ThreadManager


class BaseScraper(ABC):
    """Strategy interface shared by every data-source scraper.

    Concrete sources (TradingView, CafeF, ...) subclass this, set SOURCE_NAME,
    implement scrape(), and register themselves with @register_scraper. Shared
    infrastructure — logger, switch handler, thread manager, retry policy — lives
    here, so each source only implements its own fetching logic.

    Adding a new source is closed-for-modification / open-for-extension: create a
    subclass, implement scrape(), decorate with @register_scraper. No existing
    code (main.py, the orchestrator, other sources) needs to change.
    """

    SOURCE_NAME: str = "base"

    def __init__(
        self,
        logger: Logger,
        switch_handler: Optional[SwitchHandler] = None,
        power: int = THREAD_MANAGER_POWER,
        retry_attempts: int = SCRAPER_RETRY_ATTEMPTS,
        retry_delay: float = SCRAPER_RETRY_DELAY,
        max_workers: int = SCRAPER_MAX_WORKERS,
    ):
        self._logger = logger
        self._switch_handler = switch_handler
        self._thread_manager = ThreadManager(
            logger=logger, power=power, max_workers=max_workers
        )
        self._retry_attempts = retry_attempts
        self._retry_delay = retry_delay

    @abstractmethod
    def scrape(self) -> None:
        """Run this source's scraping (typically driven by the switch config)."""
        raise NotImplementedError


# ── Registry / factory ────────────────────────────────────────────────────────
# Lets callers build a scraper by source name and lets new sources self-register.
SCRAPER_REGISTRY: Dict[str, Type[BaseScraper]] = {}


def register_scraper(cls: Type[BaseScraper]) -> Type[BaseScraper]:
    """Class decorator: register a scraper under its SOURCE_NAME."""
    SCRAPER_REGISTRY[cls.SOURCE_NAME] = cls
    return cls


def build_scraper(source_name: str, *args, **kwargs) -> BaseScraper:
    """Instantiate a registered scraper by source name."""
    if source_name not in SCRAPER_REGISTRY:
        raise ValueError(
            f"Unknown scraper source '{source_name}'. "
            f"Registered: {sorted(SCRAPER_REGISTRY)}"
        )
    return SCRAPER_REGISTRY[source_name](*args, **kwargs)
