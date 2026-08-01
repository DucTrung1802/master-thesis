# src\utils\exceptions.py
"""Pipeline exceptions — the failure signal the ETL used to swallow.

⚠️ WHY THESE EXIST. Until Phase 0 of the Dagster migration, a failing stage logged
one ERROR line and **returned normally**: `if df is None: log_error(...); return` in
every ingest, and `except Exception: return pd.DataFrame()` in `driver.select`. A
caller therefore could not tell a successful ingest from a failed one, and neither
could an orchestrator — the only trace was a line in `logs/app.log` that nothing
read. `src/orchestration/` needs the opposite: a stage that did not do its work must
raise, so the asset goes red.

The classes are narrow on purpose. A caller that wants to tolerate a specific
failure catches the specific type; the three entry points in `DataPreprocessor` catch
`PipelineError` so one bad table does not abort a whole layer (see their docstrings).
"""


class PipelineError(Exception):
    """Base for every failure the pipeline raises deliberately."""


class MissingSourceDataError(PipelineError):
    """The input a stage reads is absent or empty.

    Raised where an ingest used to `log_error(...); return` — a missing `raw_data/`
    folder, a folder holding no readable CSV, a reference file that is not there.

    ⚠️ This is NOT always a bug: bronze has one leaf per source and each reads its own
    folder, so running an ingest whose scraper has never run is a legitimate way to
    reach this. It is still a FAILURE of that stage — it produced no table — and the
    caller decides whether to tolerate it. What it must never be is a silent success.
    """


class DatabaseQueryError(PipelineError):
    """A query failed.

    Chiefly `PostgreSQLDriver.select`, which used to return an EMPTY DataFrame on any
    error. That is the most dangerous swallow in the repo: a typo'd column, a missing
    table or a dropped connection all became "0 rows", which every caller downstream
    treats as legitimate data — an ingest then writes nothing and reports success.
    """
