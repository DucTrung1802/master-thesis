# src\orchestration\_bootstrap.py
"""Make the flat `src/` modules importable, and pin the process's working directory.

Two things every Dagster process here needs, and neither is Dagster's job:

1. **`src/` on `sys.path`.** The repo's modules import each other flat
   (`from web_scraper.cafef_scraper import CafeFScraper`), which works today only
   because `python src/main.py` puts `src/` at `sys.path[0]`. Dagster imports a
   module instead, so nothing puts `src/` on the path. `pytest.ini` solves the same
   problem with `pythonpath = src`; this is that, for Dagster.

   ⚠️ **This package now LIVES in `src/`, so the path entry it adds is also what makes
   `orchestration` itself importable.** That is a chicken-and-egg for the entry point:
   `definitions.py` cannot `from orchestration._bootstrap import bootstrap` until `src/`
   is already on the path, which is why that one file repeats the two-line insert
   inline before its first import. Every OTHER module here is reached through the
   package and needs nothing.

2. **cwd = repo root.** `Logger` writes a relative `logs/app.log` and the
   `*_RAW_DATA_DIR` constants are relative too, so both resolve against the working
   directory. Dagster's daemon/webserver do not guarantee one, and a wrong cwd fails
   QUIETLY — a scraper writes its CSVs under some other directory and the asset still
   goes green. Setting it once here is cheaper than making every consumer absolute.

   ⚠️ One item left this list on 2026-08-06: `SwitchHandler` used to default to the
   relative `src/switch_config.json`, and a wrong cwd made it unreadable, which
   returned `{}` — every switch false, every TradingView adder queueing nothing. That
   file is gone and `config.json` is resolved from `__file__`, so the configuration
   half of this hazard is closed; the output half above is not.

Import this before ANY repo module. `orchestration/__init__.py` does that, so
importing anything under `orchestration` is enough.
"""

import os
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent  # …/src (this package sits inside it)
REPO_ROOT = SRC.parent


def bootstrap() -> Path:
    """Idempotent: safe to call from every module and every spawned subprocess."""
    src = str(SRC)
    if src not in sys.path:
        # Ahead of anything else, so `logger`/`utils` resolve to the repo's and not
        # to a same-named site-package.
        sys.path.insert(0, src)

    if Path(os.getcwd()).resolve() != REPO_ROOT:
        os.chdir(REPO_ROOT)

    return REPO_ROOT
