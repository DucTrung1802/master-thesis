# src\utils\inputs.py
"""Declare an input as REQUIRED or OPTIONAL, out loud.

⚠️ THE FAILURE MODE THIS EXISTS TO KILL. The repo is full of

    if os.path.exists(path):
        ...read it...

and from the outside those two cases are indistinguishable:

  * *"this input is optional; without it I lose a named, minor thing"* — fine, but the
    loss should be visible in the log rather than inferred later from blank columns;
  * *"this input is required; without it I silently produce nothing"* — a bug wearing
    the costume of a guard clause.

The second is the dangerous one, and it hides best under expensive work. `schema_of`
read a missing chart of accounts as an EMPTY chart, so the parser mapped nothing and
rejected all 65 filings of a ticker — after ~2.4 h of OCR — for figures it had read
correctly. Nothing raised, nothing warned, and the log looked like a hard parsing
problem rather than a missing file.

So: say which one it is, at the read site.

    require_file(SCHEMA_PATH, what="the chart of accounts for 'bank'",
                 why="every line is matched against it; absent, nothing maps",
                 fix="cafef_schema.save('bank', SCHEMA_DIR)")

    optional_file(INDUSTRY_CSV, logger, what="Simplize industry",
                  degrades="sector/industry_group columns are left blank")

`require_*` raises `MissingSourceDataError` with a message that says what is missing,
what breaks, and how to fix it. `optional_file` returns a bool and logs a WARNING that
names the degradation.
"""

import os
from typing import Optional

from utils.exceptions import MissingSourceDataError


def _message(kind: str, path: str, what: str, why: str, fix: Optional[str]) -> str:
    parts = [f"missing {kind}: {what} — expected at {path!r}."]
    if why:
        parts.append(f"Consequence: {why}.")
    if fix:
        parts.append(f"Fix: {fix}")
    return " ".join(parts)


def require_file(
    path: str, *, what: str, why: str = "", fix: Optional[str] = None
) -> str:
    """The stage cannot do its job without this file. Raises if it is absent.

    Returns the path, so it composes:  `open(require_file(p, what=...))`.
    """
    if not os.path.isfile(path):
        raise MissingSourceDataError(_message("file", path, what, why, fix))
    return path


def require_dir(
    path: str, *, what: str, why: str = "", fix: Optional[str] = None, non_empty: bool = True
) -> str:
    """As `require_file`, for a directory. `non_empty` also rejects an EMPTY directory —
    an empty archive folder is the same failure as an absent one, and it is the shape a
    half-finished download leaves behind."""
    if not os.path.isdir(path):
        raise MissingSourceDataError(_message("directory", path, what, why, fix))
    if non_empty and not os.listdir(path):
        raise MissingSourceDataError(
            _message("contents of directory", path, what, why, fix)
        )
    return path


def optional_file(path: str, logger=None, *, what: str, degrades: str) -> bool:
    """This file genuinely may be absent — but say so, and say what is lost.

    ⚠️ `degrades` is not decoration: it is the difference between a warning someone can
    act on and one they scroll past. Name the columns/features that come out empty, not
    "some data will be missing".
    """
    if os.path.isfile(path):
        return True
    if logger:
        logger.log_warning(
            f"optional input absent: {what} ({path!r}) — {degrades}. "
            f"Proceeding without it."
        )
    return False
