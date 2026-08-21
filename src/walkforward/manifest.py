# src\walkforward\manifest.py
"""WFO-1 — one track directory, one experiment, and a REFUSAL when they disagree.

`run.DEFAULT_OUT` is a single fixed path and every artefact under it — `folds.csv`,
`per_fold.csv`, `predictions_oos.csv` — is written by BASENAME, with no term for the
table, the target or the horizon. So `RUNBOOK.md` §3's documented command, run at a
second horizon, overwrites the first horizon's entire OOS track: silently, with no
fingerprint check and no message. That is `WFO-1`, and it was caught once by reading
`DEFAULT_OUT` before pressing enter, which is not a control.

⚠️ **THE GUARD IS THE FIX, NOT A RENAME.** Deriving the leaf from the experiment
(`results/walkforward/<ticker>__<table>/`) was the other candidate and it was rejected:
five tracks already exist on disk under hand-chosen names that three registers cite by
path, and moving them to buy a guarantee a refusal gives anyway trades a real citation
for a hypothetical collision.

**What identifies an experiment** is everything that changes the TENSORS a fold is built
from — the table (which carries the target, the lookback and the horizon) plus the fold
geometry and the dataset knobs. Two sweeps that agree on all of it are the same
experiment and re-running one is legitimate; anything else is refused.

⚠️ **A LEGACY DIRECTORY IS STILL PROTECTED, AND THAT IS THE POINT.** The five tracks that
predate this module carry no manifest — but `folds.csv` records each fold's run name,
which carries the table, so the table is recoverable and compared. It is the only field
that is: the knobs are not in `folds.csv` and are not inferred (§5 rule 2 — an absent
measurement is absent, never implied). So a legacy track is guarded against the horizon
collision that actually happened, and not against a knob-only one.
"""

from __future__ import annotations

import json
import os
from typing import Dict, List, Optional

MANIFEST = "manifest.json"

#: The fields that make two sweeps the same experiment. `table` carries the target, the
#: lookback and the horizon; the rest are what `run.main` can vary without renaming it.
IDENTITY_KEYS = (
    "ticker", "table", "first_test", "step_months", "val_months",
    "scale_target", "rank_min_width",
)


def horizon_of(table: str) -> int:
    """`rank_20day__final__d20_h20` -> 20, from the ONE parser that owns the name.

    ⚠️ Imported from `train_test_creator`, never re-derived here. CLAUDE.md §3b: `d` and
    `h` come from the source TABLE NAME and never from a parameter, and `TGT-1` is the
    issue that got a second horizon-string constructor deleted.
    """
    from train_test_creator import FINAL_TABLE

    match = FINAL_TABLE.match(table or "")
    if not match:
        raise ValueError(
            f"cannot read a horizon out of {table!r} — expected "
            f"<target>__final__d<d>_h<h>[__<scope>]"
        )
    return int(match.group("horizon"))


def identity(**fields) -> Dict:
    """The subset of `run.main`'s arguments that decides which experiment this is."""
    missing = [key for key in IDENTITY_KEYS if key not in fields]
    if missing:
        raise ValueError(f"identity() is missing {missing}")
    return {key: fields[key] for key in IDENTITY_KEYS}


def describe(ident: Dict) -> str:
    return "  ".join(f"{key}={ident.get(key)!r}" for key in IDENTITY_KEYS)


def read(directory: str) -> Optional[Dict]:
    path = os.path.join(directory, MANIFEST)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def write(directory: str, ident: Dict, **extra) -> str:
    """Write the manifest. `extra` is provenance and is NOT part of the identity."""
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, MANIFEST)
    payload = dict(ident)
    payload["horizon"] = horizon_of(ident["table"])
    payload.update(extra)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    return path


def _table_from_run_name(run: str) -> Optional[str]:
    """`lstm__all__rank_20day__final__d20_h20__oos2017__20260819-023033` -> the table.

    The run name is `<model>__<ticker>__<table>__<fold tag>__<stamp>` and the table's own
    segments (`<target>__final__d<d>_h<h>[__<scope>]`) sit in the middle, so the two ends
    come off by position. Returns None rather than guessing when the shape does not hold.
    """
    parts = str(run).split("__")
    if len(parts) < 5:
        return None
    return "__".join(parts[2:-2]) or None


def table_from_folds(directory: str) -> List[str]:
    """Every distinct table named by a `folds.csv` at or one level below `directory`.

    ⚠️ One level, not a full walk: an arm sweep writes `<out>/<label>/folds.csv`, so the
    parent that `--out` names holds no `folds.csv` of its own and would otherwise look
    empty. Deeper nesting (`results/walkforward/prf8/gbt/`) belongs to a directory that
    was itself claimed with its own `--out`, and reaching into it would refuse sweeps
    that are correctly separated.
    """
    import pandas as pd

    candidates = [os.path.join(directory, "folds.csv")]
    if os.path.isdir(directory):
        for entry in sorted(os.listdir(directory)):
            candidates.append(os.path.join(directory, entry, "folds.csv"))

    tables: List[str] = []
    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            frame = pd.read_csv(path)
        except Exception:
            continue
        if "run" not in frame.columns:
            continue
        for run in frame["run"].astype(str):
            table = _table_from_run_name(run)
            if table and table not in tables:
                tables.append(table)
    return tables


def claim(directory: str, ident: Dict, *, force: bool = False) -> Dict:
    """Refuse `directory` unless it is free, or already holds THIS experiment.

    Raises `SystemExit` with the two identities printed side by side — the message is the
    control the reader did not have, so it names the offending field and the flag that
    fixes it rather than saying "mismatch".
    """
    existing = read(directory)
    if existing is not None:
        differing = [key for key in IDENTITY_KEYS
                     if existing.get(key) != ident.get(key)]
        if differing and not force:
            raise SystemExit(
                f"\n⚠️  {directory}\n"
                f"    already holds a DIFFERENT walk-forward experiment, and every "
                f"artefact here is written by basename — continuing would overwrite it "
                f"(WFO-1).\n\n"
                f"    on disk : {describe(existing)}\n"
                f"    asked   : {describe(ident)}\n"
                f"    differs : {', '.join(differing)}\n\n"
                f"    Pass --out <a different directory> for this experiment, or "
                f"--force-out to overwrite the one above on purpose.\n"
            )
        return existing

    seen = table_from_folds(directory)
    other = [table for table in seen if table != ident["table"]]
    if other and not force:
        raise SystemExit(
            f"\n⚠️  {directory}\n"
            f"    holds a walk-forward track for {other!r} and this sweep is for "
            f"{ident['table']!r}. Every artefact is written by basename, so continuing "
            f"would overwrite it (WFO-1).\n\n"
            f"    ⚠️  That track predates the manifest, so only its TABLE could be "
            f"compared — the fold geometry and the dataset knobs are not recorded in "
            f"`folds.csv` and are not inferred.\n\n"
            f"    Pass --out <a different directory>, or --force-out to overwrite it "
            f"on purpose.\n"
        )
    return {}


def horizon_for(directory: str, requested: Optional[int] = None) -> int:
    """The horizon a SCORING tool must use on this track, and a refusal on disagreement.

    ⚠️ **THIS IS THE HALF OF `WFO-1` THAT MISSTATES A NUMBER RATHER THAN DESTROYING ONE.**
    `evaluate --horizon` and `compare --horizon` both defaulted to 20, and they set BOTH
    the interval the periods are cut at AND the `return_{h}day` column that is scored —
    so an h=10 track scored without the flag silently scored the wrong label against the
    right predictions, and nothing in the output said so.

    Derived from the track itself (the manifest, else `folds.csv`); an explicit
    `--horizon` that disagrees is an error, never a silent override.
    """
    ident = read(directory)
    tables = [ident["table"]] if ident else table_from_folds(directory)
    if not tables:
        if requested is None:
            raise SystemExit(
                f"\n⚠️  cannot tell what horizon {directory} was built at — it carries "
                f"no {MANIFEST} and no readable folds.csv. Pass --horizon.\n"
            )
        return requested
    if len(set(tables)) > 1:
        raise SystemExit(
            f"\n⚠️  {directory} names more than one table ({sorted(set(tables))}), so it "
            f"is not one track. Score each one from its own directory.\n"
        )

    derived = horizon_of(tables[0])
    if requested is not None and requested != derived:
        raise SystemExit(
            f"\n⚠️  --horizon {requested} against a track built on {tables[0]!r}, which "
            f"is h={derived}.\n"
            f"    The horizon sets the holding interval AND the return_{{h}}day column "
            f"scored, so this would score the wrong label against the right "
            f"predictions.\n"
            f"    Drop --horizon (it is derived from the track) or point --out at the "
            f"h={requested} track.\n"
        )
    return derived
