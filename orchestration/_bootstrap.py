# orchestration\_bootstrap.py
"""Make the flat `src/` modules importable, and pin the process's working directory.

Two things every Dagster process here needs, and neither is Dagster's job:

1. **`src/` on `sys.path`.** The repo's modules import each other flat
   (`from web_scraper.cafef_scraper import CafeFScraper`), which works today only
   because `python src/main.py` puts `src/` at `sys.path[0]`. Dagster imports a
   module instead, so nothing puts `src/` on the path. `pytest.ini` solves the same
   problem with `pythonpath = src`; this is that, for Dagster.

2. **cwd = repo root.** `SwitchHandler` defaults to the RELATIVE path
   `src/switch_config.json`, `Logger` writes a relative `logs/app.log`, and the
   `*_RAW_DATA_DIR` constants are relative too — so every one of them resolves
   against the working directory. Dagster's daemon/webserver do not guarantee one,
   and a wrong cwd fails quietly (an unreadable switch config returns `{}`, i.e.
   every switch false). Setting it once here is cheaper than making every consumer
   absolute.

Import this before ANY repo module. `orchestration/__init__.py` does that, so
importing anything under `orchestration` is enough.
"""

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"


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
