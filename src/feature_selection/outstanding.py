# src\feature_selection\outstanding.py
"""One archived run → `outstanding.csv`, the channels that run actually chose.

`feature_importance.csv` is one row per CHANNEL for every candidate a run scored —
918 of them on `pool__ta`. What the next stage needs is far smaller: the channels the
run kept, with duplicates collapsed, mapped back to the pool table they came from so
they can be read and joined on `date`.

    python -m feature_selection.outstanding            # write one per run folder
    python -m feature_selection.outstanding --dry-run  # print, touch nothing

    reports/feature_selection/<run>/outstanding.csv    ⭐ one per run, the deliverable

⚠️ **One file per run, deliberately — there is no combined file.** The runs are not
one experiment. §8 of CONTEXT.md is a list of ways two runs look comparable and are
not, and a single merged shortlist is exactly the artefact that gets quoted against a
configuration it was never computed for. Merging is the NEXT module's job, done
knowingly, against the `run_id`, `target`, `lookback_d`, `grain` and `evidence` each
file carries for that purpose.

## The three filters, in order

**1. The cut — how many channels this run supports** ([selection_cut.py](selection_cut.py)).
⚠️ **This used to be the run's own `kept` column, which meant `max_features=12`** —
one number applied to a 27-channel pool and to a 1,458-channel one alike, and §9i and
§13c both measured all channels beating the pruned 12 in every fold. It is now
measured per run: a channel is a candidate if its cross-method agreement beats a
shuffled-methods null (BH at `fdr_q`) **or** it sits before the knee of some single
method's own score curve, and the survivors of an UNCAPPED |ρ| ≥ 0.9 prune are the
list. **10-236 channels per run, median 40, 952 rows over 20 runs** — against 12
everywhere before, on pools ranging from 27 channels to 1,458.

**2. Ties on the ensemble score, broken by `permutation`.** The ensemble is a mean
RANK over six methods, so exact ties are common — `volume_negotiated` and
`foreign_own` both score 8.667 in the `basic` run. CONTEXT §4: `permutation` is the
only ranker measured OUT OF SAMPLE and is the one to believe when the methods
disagree, so it breaks the tie; |ρ| against the target breaks a tie in that. The
loser is dropped and named in `beat_in_tie`, never silently.

**3. Nothing is re-ranked.** The ensemble order is the run's own; the cut decides
where to stop reading it, not what it says.

## ⚠️ What a row does NOT mean

**No run in the archive is a clean pass.** Nineteen of the twenty computed no null at
all, and the twentieth — `bank` — sat on its null's mean (z = +0.11, CONTEXT §13).
CONTEXT §6b measured a positive out-of-sample IC on SHUFFLED LABELS, so a high rank in
a run with `"null": null` is not evidence — it is an internally consistent description
of noise, and §11c says so about a ranking of this exact shape. `evidence` carries that
verdict on every row; `no_null` is the honest majority value.

⚠️ **The two columns answer different questions and neither substitutes for the
other.** `evidence` is the RUN's verdict against shuffled LABELS — does this pool
predict this target at all. `kept_by` is the CHANNEL's verdict against shuffled
METHODS — does this channel stand out within the run. A row can read
`kept_by=consensus` and `evidence=no_null`, which means the six rankers agree about a
channel in a run that was never shown to beat noise. Twelve of the 952 rows are
`consensus`; all 952 sit in runs with no label null.

⚠️ **And the archive does not agree with itself.** Nineteen runs contain the same 27
`pool__basic` channels; the most repeatedly kept one survives 9 of those 19 selections
(CONTEXT §14a). Treat a single run's list as that run's list.
"""

import json
import os
import sys
from typing import Dict, List, Optional, Sequence

import pandas as pd

from feature_selection import contract, selection_cut
from feature_selection.contract import UNKNOWN_SOURCE
from feature_selection.report import DEFAULT_REPORT_ROOT
from utils import runtime

# ⚠️ One definition, in `contract.py`, imported by both sides of the handoff. The name
# `OUTSTANDING_FILENAME` is unchanged because `pipeline.stages` and
# `train_test_creator.dataset` import it from here.
OUTSTANDING_FILENAME = contract.SHORTLIST_FILENAME
CORRELATION_FILENAME = "channel_correlation.csv"

# ⚠️ **THE FALLBACK ONLY.** The authority is `metadata.json`'s
# `input.columns_by_table`, recorded by `UnifiedSchemaReader.join` at read time; this
# name-based guess runs only for run folders archived before that existed (the 19
# country runs of 2026-08-12/13). It cannot be extended to cover the pools built since
# — `pool__forex_<exchange>`, `pool__funds`, `pool__bonds`, `pool__stock_market`,
# `pool__basic_bank` are all `{exchange}__{ticker}__{measure}` and share one shape —
# and `contract.py` has the measured table of what it returns for each.
#
# A channel carries no note of where it came from, but the pools it DOES cover are
# disjoint (verified across the archive: basic ∩ fa = basic ∩ ta = fa ∩ ta = ∅) and
# the economy columns are self-identifying by prefix. So the source table is derivable
# from the archive alone, with no database connection — which matters because this has
# to run on a checkout.
POOL_BASIC_CHANNELS = frozenset({
    "avg_vol_per_buy_order", "avg_vol_per_sell_order", "buy_order_vol", "close_adjust",
    "close_raw", "foreign_buy_value", "foreign_buy_volume", "foreign_net_value",
    "foreign_net_volume", "foreign_own", "foreign_room_left", "foreign_sell_value",
    "foreign_sell_volume", "high", "low", "n_buy_orders", "n_sell_orders", "open",
    "prop_buy_val", "prop_buy_vol", "prop_sell_val", "prop_sell_vol", "sell_order_vol",
    "value_matched", "value_negotiated", "volume_matched", "volume_negotiated",
})

# The order the deliverable is written in: what to fetch, then how much to trust it.
COLUMNS = [
    "outstanding_rank", "channel", "schema", "source_table", "grain",
    "ensemble", "permutation", "spearman_vs_target", "best_stat",
    "kept_by", "consensus_p", "tie_group_size", "beat_in_tie",
    "absorbed_as_redundant", "n_candidates",
    "run_id", "target", "horizon_h", "lookback_d", "evidence",
    # ⚠️ `metadata` = the pool was READ from `input.columns_by_table`; `heuristic` =
    # it was guessed from the channel name because the run predates that map. A
    # consumer reading `heuristic` is reading an inference, and on any pool built
    # since 2026-08-10 that inference is `unknown` (see `source_table`).
    "source_table_from",
    # ⚠️ How much of the sample the channel actually EXISTS for (issue COV-1).
    # `prop_buy_vol` was shortlisted at 0.20 coverage — empty until 2023 — and the
    # ranking that chose it was computed on the fifth of history where it exists.
    # `train_test_creator` then drops it as untrainable, which is a workaround at the
    # wrong end: by then the channel has already displaced another from the list.
    # This does NOT filter — the archive cannot see where a downstream train/test cut
    # will fall, so a coverage of 0.20 is a WARNING, not a verdict. It is carried so
    # the fetch list states the risk instead of a later stage discovering it.
    "coverage", "coverage_flag",
    # ⚠️ The parameters the CUT actually ran with, stamped into the deliverable.
    # Without them the shortlist does not say what produced it, and a consumer is
    # left reading `setup.max_features` from `metadata.json` — which describes the
    # SELECTOR's old cap and has not determined this file since `selection_cut`
    # replaced it. `final_features.SETUP_KEYS` reads these two instead.
    "cut_fdr_q", "cut_corr_threshold",
]

# Below this share of non-null rows a channel is flagged. 0.95 is deliberately
# generous: it catches `prop_*` (0.20) and the late-starting macro series without
# flagging the ordinary ragged head of a price channel.
COVERAGE_FLOOR = 0.95
COVERAGE_FILENAME = "coverage.csv"


def source_table(
    channel: str,
    tables: List[str],
    columns_by_table: Optional[Dict[str, Sequence[str]]] = None,
) -> str:
    """Which `pool__*` a channel came from — read from the run's map, or guessed.

    ⚠️ **`columns_by_table` is the answer and the rest is an inference.** It comes
    from `UnifiedSchemaReader.join`, which is the only code that saw both the pool and
    the column. When it is present nothing is guessed: a channel that is not in it is
    `unknown`, because a channel the join did not produce is not a channel of this
    panel at all.

    ⚠️ The fallback below falls back to `unknown` rather than guessing further. A
    wrong table sends the next module to read a column that is not there, which fails
    loudly — but a SILENT wrong guess on a column name that exists in two pools would
    not. It happens anyway, in one direction this cannot see: with `pool__ta` in the
    run, ANY unrecognised channel is attributed to it, which is how a forex channel
    came out labelled `pool__ta` (`contract.py`). That is the reason the map exists.
    """
    if columns_by_table:
        for table, columns in columns_by_table.items():
            if channel in columns:
                return table
        return UNKNOWN_SOURCE

    if channel in POOL_BASIC_CHANNELS:
        return "pool__basic"
    for table in tables:
        if table.startswith("pool__economy_"):
            country = table[len("pool__economy_"):]
            if channel.startswith(f"{country}__economy__"):
                return table
    for table in ("pool__ta", "pool__fa"):
        if table in tables:
            return table
    return UNKNOWN_SOURCE


def _evidence(meta: Dict) -> str:
    """The null verdict of one run — what a row in this file is actually worth."""
    null = meta.get("null")
    if not null:
        return "no_null"
    return "cleared_p95_not_a_pass" if null.get("clears_bar") else "failed_null"


def _grain(meta: Dict) -> str:
    """`date` or `date+ticker` — whether this run's channels are one row per session.

    ⚠️ The next module joins on `date`. A cross-sectional run is keyed `(date, ticker)`
    — 20 banks on one Tuesday are 20 rows — so its file cannot be concatenated into a
    date-indexed table without collapsing the dimension the run was ABOUT. The column
    says so on every row instead of the run being quietly omitted.
    """
    return "date+ticker" if (meta["input"].get("tickers") or 1) > 1 else "date"


def break_ties(kept: pd.DataFrame) -> pd.DataFrame:
    """One row per distinct `ensemble` score — the strongest of each tied group.

    Adds `tie_group_size` and `beat_in_tie` so the drop is auditable: a shortlist that
    silently loses a channel is the artefact CONTEXT §10 exists to prevent.
    """
    ordered = kept.assign(_abs_rho=kept["spearman_vs_target"].abs()).sort_values(
        ["ensemble", "permutation", "_abs_rho"], ascending=[True, False, False]
    )
    rows = []
    for _, group in ordered.groupby("ensemble", sort=False):
        winner = group.iloc[0].copy()
        winner["tie_group_size"] = len(group)
        winner["beat_in_tie"] = "; ".join(group["channel"].iloc[1:])
        rows.append(winner)
    out = pd.DataFrame(rows).drop(columns=["_abs_rho"])
    return out.sort_values("ensemble").reset_index(drop=True)


def _correlation_loader(folder: str):
    """Read only the candidate channels' submatrix out of the archived N×N.

    ⚠️ `channel_correlation.csv` is **40 MB** on `basic+economy_usa` and 16.5 MB on
    `ta` (§10b) — it is the full pairwise matrix the prune used, and `p` runs to the
    thousands. `usecols` turns that into the few hundred columns the candidate set
    needs. Returns `None` when the file is absent, which skips the prune rather than
    failing: an older run folder is still readable, just with a looser count.
    """
    path = os.path.join(folder, CORRELATION_FILENAME)

    def load(channels: List[str]) -> Optional[pd.DataFrame]:
        if not os.path.exists(path):
            return None
        frame = pd.read_csv(path, index_col=0, usecols=["channel", *channels])
        return frame.loc[channels]

    return load


def _coverage_loader(folder: str) -> Dict[str, float]:
    """`channel -> non-null share`, from the run's own `coverage.csv`.

    Returns `{}` when the file is absent, which leaves `coverage` NaN and the flag
    empty rather than failing — an older run folder stays readable, and a missing
    measurement is recorded as missing instead of as "fine" (the same rule §10 applies
    to an absent null).
    """
    path = os.path.join(folder, COVERAGE_FILENAME)
    if not os.path.exists(path):
        return {}
    frame = pd.read_csv(path)
    return dict(zip(frame["channel"], frame["coverage"].astype(float)))


def build_one(folder: str, **cut_kwargs) -> pd.DataFrame:
    """One run folder → its `outstanding` table. Does not write.

    `cut_kwargs` go to `selection_cut.suitable` — `fdr_q`, `corr_threshold`, `seed`.
    """
    meta = json.load(open(os.path.join(folder, "metadata.json"), encoding="utf-8"))
    full = pd.read_csv(os.path.join(folder, "feature_importance.csv"))
    tables = meta["input"]["tables"]
    # ⚠️ Absent on every run archived before 2026-08-15, present on every run since.
    # The two are different KINDS of answer, so which one was used is recorded on
    # every row rather than left to be inferred from the run's date.
    recorded = meta["input"].get("columns_by_table")

    out = break_ties(
        selection_cut.suitable(
            full, corr_loader=_correlation_loader(folder), **cut_kwargs
        )
    )
    out["schema"] = meta["input"]["schema"]
    out["source_table"] = [source_table(c, tables, recorded) for c in out["channel"]]
    out["source_table_from"] = "metadata" if recorded else "heuristic"
    out["grain"] = _grain(meta)
    out["best_stat"] = out.get("best_stat__permutation")
    out["run_id"] = meta["run_id"]
    out["target"] = meta["target"]["name"]
    out["horizon_h"] = meta["setup"]["horizon_h"]
    out["lookback_d"] = meta["setup"]["lookback_d"]
    out["evidence"] = _evidence(meta)
    cov = _coverage_loader(folder)
    out["coverage"] = [cov.get(c, float("nan")) for c in out["channel"]]
    out["coverage_flag"] = [
        "" if pd.isna(v) else ("PARTIAL" if v < COVERAGE_FLOOR else "")
        for v in out["coverage"]
    ]
    out["cut_fdr_q"] = cut_kwargs.get("fdr_q", selection_cut.DEFAULT_FDR_Q)
    out["cut_corr_threshold"] = cut_kwargs.get(
        "corr_threshold", selection_cut.DEFAULT_CORR_THRESHOLD
    )
    out["outstanding_rank"] = range(1, len(out) + 1)
    return out[COLUMNS]


def main(
    root: str = DEFAULT_REPORT_ROOT,
    write: bool = True,
    strict: bool = True,
    **cut_kwargs,
) -> Dict[str, pd.DataFrame]:
    """Write one `outstanding.csv` into every run folder under `root`.

    ⚠️ **A shortlist `final_features` cannot read is NOT written** (2026-08-15). Every
    file is checked against `contract.validate_shortlist` first — the columns the next
    stage reads, the SQL-identifier rule it interpolates them under, and above all
    that no channel carries `source_table='unknown'`. A file that fails is skipped,
    named, and re-raised at the END, after every other run has been written: the same
    rule as CLAUDE.md §5 rule 20, one level up. Writing it instead would move the
    failure to `python -m final_features`, hours and two stages later, where the run
    that caused it is no longer the thing being run.

    `strict=False` reports the problems and writes nothing bad, but does not raise —
    for a caller that wants the good files and will read `problems` itself.
    """
    built: Dict[str, pd.DataFrame] = {}
    problems: List[str] = []
    for name in contract.run_folders(root):
        folder = os.path.join(root, name)
        table = build_one(folder, **cut_kwargs)
        found = contract.validate_shortlist(
            table, setup=contract.read_setup(root, name), label=name
        )
        if found:
            problems.extend(found)
            for problem in found:
                print(f"WARNING: {problem}")
            print(f"WARNING: {name}/{OUTSTANDING_FILENAME} was NOT written.")
            continue
        if write:
            table.to_csv(os.path.join(folder, OUTSTANDING_FILENAME), index=False)
        built[name] = table

    if problems and strict:
        raise ValueError(
            f"{len(problems)} problem(s) in the feature_selection -> final_features "
            f"handoff; {len(built)} good run(s) were written first:\n  "
            + "\n  ".join(problems)
        )
    return built


if __name__ == "__main__":
    write = "--dry-run" not in sys.argv
    with runtime.RunTimer("feature_selection.outstanding"):
        results = main(write=write)
        for run, table in results.items():
            unknown = int((table["source_table"] == UNKNOWN_SOURCE).sum())
            consensus = int(table["kept_by"].str.contains("consensus").sum())
            source = table["source_table_from"].iloc[0]
            print(
                f"{len(table):>4} channels  {consensus:>2} consensus  "
                f"{table['evidence'].iloc[0]:<24}{source:<11}"
                f"{'  ⚠️ %d unknown' % unknown if unknown else '':<16}{run}"
            )
        print(
            f"\n{len(results)} runs, {sum(len(t) for t in results.values())} rows "
            f"{'written' if write else 'built (--dry-run: nothing written)'}"
        )
        # ⚠️ A run folder with no shortlist is INVISIBLE to `final_features`, and was
        # invisible here too until this printed it: `main` only ever reported the
        # files it wrote. Two finished runs sat in this state on 2026-08-15.
        missing = contract.missing_shortlists(root=DEFAULT_REPORT_ROOT)
        if missing:
            print(
                f"WARNING: {len(missing)} run folder(s) still carry no "
                f"{OUTSTANDING_FILENAME} and final_features cannot see them: "
                f"{missing}"
            )
