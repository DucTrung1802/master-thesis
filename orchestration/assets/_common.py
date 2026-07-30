# orchestration\assets\_common.py
"""Helpers shared by the landing (scrape) assets.

The important one is `landed()`. Every scrape asset's contract is "data is on disk
afterwards", and that is checked rather than assumed — the scrapers still swallow
their own failures the way the preprocessor used to before Phase 0 (`GicsScraper.scrape`
does `if not self._download_structure(...): log_error(...); return`), so a scraper can
return normally having written nothing. Counting what landed turns that back into a
failed asset without having to edit every scraper first.
"""

from pathlib import Path
from typing import Iterable

from dagster import MetadataValue

from orchestration._bootstrap import bootstrap

bootstrap()

from utils.exceptions import MissingSourceDataError


def _count_rows(paths: Iterable[Path]) -> int:
    total = 0
    for p in paths:
        try:
            with p.open("r", encoding="utf-8-sig", errors="replace") as f:
                total += max(sum(1 for _ in f) - 1, 0)  # minus the header
        except OSError:
            continue
    return total


def landed(
    folder: str | Path,
    pattern: str = "*.csv",
    require: bool = True,
    count_rows: bool = True,
) -> dict:
    """Measure what a scraper left on disk, and FAIL the asset if it left nothing.

    `require=False` is for the handful of steps that legitimately produce nothing —
    TradingView's crypto data step is not implemented and its options step adds no
    tasks, so an empty folder there is the documented behaviour, not a failure.
    """
    path = Path(folder)
    files = sorted(path.rglob(pattern)) if path.exists() else []

    if require and not files:
        raise MissingSourceDataError(
            f"scraper wrote nothing to {path} (expected {pattern}). The scraper "
            f"returned without raising — check logs/app.log for its own error line."
        )

    meta = {"files": len(files), "folder": MetadataValue.path(str(path))}
    if count_rows:
        meta["rows"] = _count_rows(files)
    if files:
        meta["latest_file"] = MetadataValue.text(files[-1].name)
    return meta


def ticker_key(exchange: str, symbol: str) -> str:
    """Partition key for a per-ticker asset. `HOSE_VCB` — the same shape the scrapers
    already use for their output filenames, so a partition maps to a file by eye."""
    return f"{exchange}_{symbol}"


def split_ticker_key(key: str) -> tuple[str, str]:
    exchange, _, symbol = key.partition("_")
    return exchange, symbol
