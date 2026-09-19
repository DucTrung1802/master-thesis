"""The FOOTPRINT of a selection run — what makes two runs the same experiment, hashed.

⚠️ **A SELECTION AT `d = 10` COSTS ~101 MINUTES PER POOL** (measured 2026-09-19: one
9-phase cycle is 551 s and a run is the real pass plus `null_draws` of them), so reusing
one is worth real time — and reusing the WRONG one is worth a wrong result. This module
is the key that decides which.

⚠️ **WHAT THE REUSE KEY WAS UNTIL TODAY, AND WHAT IT COULD NOT SEE.** `event_chain.chain`
matched an existing run on `(schema, target, lookback_d, horizon_h)` plus the pool's name,
read out of `metadata.json`. Everything below was invisible to it:

| input | consequence of it being outside the key |
|---|---|
| `holdout_start` | it is READ into the frame and never filtered on — a different holdout reused silently |
| `null.draws` | a 10-draw bar answers a request for 20 (§5 rule 1: every selection needs its own null) |
| `device` | a sampled XGBoost draws from another RNG stream on CUDA — `gpu.py` §1. Two experiments, one key |
| the pool's own DATA | a re-scrape adds rows and the stale selection is reused. §5 rule 10's shape, one layer up |
| the bytes of `selector.py` / `windows.py` / `gpu.py` | the ranker changes and the key does not move |

⚠️ **THE CODE BLOCK IS THE ONE THAT CANNOT BE BACKFILLED.** A run archived before this
module records `git_commit` like `1593f663+dirty`, and `+dirty` cannot be resolved back to
bytes — `FPR-1` exactly, where a gap plan could not tell which parser had run. Such a run
gets `code: "unknown"` and is **never eligible for silent reuse**: it can be read, quoted
and compared, but not skipped over. §5 rule 2 — absent is absent, not assumed equal.

⚠️ **THE FOOTPRINT IS COMPUTED FROM THE ASSEMBLED `metadata.json` AND NOT FROM THE LIVE
OBJECTS.** That is deliberate and it is what makes the archive backfillable: the same
function reads a run written five minutes ago and one written in August. A second
implementation reading the live selector would drift from this one, which is `MTH-1`'s
lesson about two writers of one fact.
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Dict, List, Optional

# ⚠️ **THE MODULES WHOSE BYTES DECIDE A RANKING, and only those.** Adding a file here
# invalidates every future footprint against the archive, so it takes a reason: this list
# is the ranking path (`selector` ranks, `windows` builds the design, `gpu` computes the
# Spearman and the permutation importance, `selector_methods` is the ensemble, `run`
# orchestrates, `evaluation`/`cross_sectional` compute the null and the IC). ⚠️ `report.py`
# is ABSENT on purpose — it writes the artefacts and cannot move a number, and including
# it would invalidate every selection whenever a heading changed.
CODE_FILES = (
    "selector.py",
    "selector_methods.py",
    "windows.py",
    "gpu.py",
    "run.py",
    "evaluation.py",
    "cross_sectional.py",
)

UNKNOWN = "unknown"

# The `input` fields that identify the DATA a selection read. ⚠️ `columns_by_table` is in
# here because a pool that gained a channel is a different offer even at the same row
# count, and `last_date` because a re-scrape is the common way this moves.
DATA_KEYS = ("schema", "tables", "panel_rows", "panel_columns",
             "first_date", "last_date", "sessions", "tickers")

# The `setup` fields that identify the PROCEDURE. ⚠️ A superset of `contract.SETUP_KEYS`,
# which answers a different question: that tuple decides whether two runs may be UNIONED
# into one `__final__` table, this one decides whether one may be SKIPPED because the
# other already ran. Skipping is the stricter test — `holdout_start`, `device` and
# `target` do not stop a union and absolutely stop a reuse.
SETUP_KEYS = ("target", "horizon_h", "lookback_d", "normalize", "window_stats",
              "purge_gap_rows", "holdout_start", "channels", "device", "methods",
              "max_features", "corr_threshold", "n_splits", "min_train", "random_state",
              "subsample", "colsample_bytree", "permutation_repeats", "selector_class",
              "panel_col", "feature_normalize", "design_dtype", "env_fingerprint")


def _short(payload: object) -> str:
    """12 hex of sha256 over a canonical JSON rendering — stable across dict orders."""
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:12]


def code_digest() -> str:
    """12 hex over the BYTES of the modules that decide a ranking.

    ⚠️ **BYTES, NOT A COMMIT.** A commit hash says nothing on a dirty tree, which is the
    state this repo is in for most of a working day; `web_scraper`'s `parser_digest()`
    learned the same thing (`FPR-1`) and this is the selection's copy of it.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    parts = {}
    for name in CODE_FILES:
        path = os.path.join(here, name)
        if not os.path.exists(path):
            # A module that is GONE changes the ranking path as surely as an edited one.
            parts[name] = "absent"
            continue
        with open(path, "rb") as handle:
            parts[name] = hashlib.sha256(handle.read()).hexdigest()[:16]
    return _short(parts)


def blocks(metadata: Dict) -> Dict[str, object]:
    """The four identity blocks of a run, from its `metadata.json`.

    ⚠️ Every value is taken from the FILE, so a run that did not record a field reads
    `None` for it rather than today's value. That is the difference between a footprint
    and a guess.
    """
    data = {k: (metadata.get("input") or {}).get(k) for k in DATA_KEYS}
    # sorted, because a column order is not an input
    by_table = (metadata.get("input") or {}).get("columns_by_table") or {}
    data["columns_by_table"] = {t: sorted(map(str, cols)) for t, cols in by_table.items()}
    if isinstance(data.get("tables"), list):
        data["tables"] = sorted(map(str, data["tables"]))

    setup = {k: (metadata.get("setup") or {}).get(k) for k in SETUP_KEYS}
    # `device` lives in two places and the setup's copy is the PREFERENCE, not what ran.
    setup["device_used"] = ((metadata.get("execution") or {}) or {}).get("device")

    null = metadata.get("null") or {}
    return {
        "data": data,
        "setup": setup,
        # ⚠️ 0 draws is not "no opinion": it is a run that recorded NO BAR, and reusing it
        # for a request that wants 10 would hand the caller `evidence=no_null` silently.
        "null_draws": None if null is None else null.get("draws"),
    }


def of_metadata(metadata: Dict, code: Optional[str] = None) -> Dict[str, object]:
    """`{footprint, parts, code, reusable}` for one run.

    `code` is the digest to record. At WRITE time the caller passes today's
    `code_digest()`; on a BACKFILL it is whatever the file already stored, and `None`
    becomes `"unknown"` — which is what makes an archived run quotable but not skippable.
    """
    part = blocks(metadata)
    parts = {
        "data": _short(part["data"]),
        "setup": _short(part["setup"]),
        "null_draws": str(part["null_draws"]),
        "code": code or UNKNOWN,
    }
    return {
        "footprint": _short(parts),
        "parts": parts,
        "code": parts["code"],
        # ⚠️ The one flag a caller may act on. An unknown code means the ranking path that
        # produced this run cannot be named, so "the same inputs" cannot be asserted.
        "reusable": parts["code"] != UNKNOWN,
        "schema_version": 1,
    }


def differences(left: Dict, right: Dict) -> List[str]:
    """Which identity blocks differ — so a MISS says why instead of just re-running.

    ⚠️ A cache that only ever says "miss" teaches nobody anything; the whole value of a
    footprint on a 101-minute job is being told it was the `null_draws`, not the data.
    """
    a = (left or {}).get("parts") or {}
    b = (right or {}).get("parts") or {}
    return [k for k in ("data", "setup", "null_draws", "code")
            if a.get(k, UNKNOWN) != b.get(k, UNKNOWN)]


def read(run_dir: str) -> Optional[Dict]:
    """A run folder's stored footprint, computing one for an archived run that has none."""
    path = os.path.join(run_dir, "metadata.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as handle:
        metadata = json.load(handle)
    stored = metadata.get("footprint")
    if isinstance(stored, dict) and stored.get("footprint"):
        return stored
    # ⚠️ An archived run gets a footprint over the blocks it DID record and `code:
    # unknown` — readable, comparable, never silently reused.
    return of_metadata(metadata, code=None)
