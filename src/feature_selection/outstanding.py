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
from typing import Dict, List, Optional

import pandas as pd

from feature_selection import selection_cut
from feature_selection.report import DEFAULT_REPORT_ROOT

OUTSTANDING_FILENAME = "outstanding.csv"
CORRELATION_FILENAME = "channel_correlation.csv"

# A channel carries no note of where it came from, but the pools are DISJOINT (verified
# across the archive: basic ∩ fa = basic ∩ ta = fa ∩ ta = ∅) and the economy columns are
# self-identifying by prefix. So the source table is derivable from the archive alone,
# with no database connection — which matters because this has to run on a checkout.
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


def source_table(channel: str, tables: List[str]) -> str:
    """Which `pool__*` a channel came from, from the channel name and the run's tables.

    ⚠️ Falls back to `unknown` rather than guessing. A wrong table here sends the next
    module to read a column that is not there, which fails loudly — but a SILENT wrong
    guess on a column name that exists in two pools would not, so it is not attempted.
    """
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
    return "unknown"


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

    out = break_ties(
        selection_cut.suitable(
            full, corr_loader=_correlation_loader(folder), **cut_kwargs
        )
    )
    out["schema"] = meta["input"]["schema"]
    out["source_table"] = [source_table(c, tables) for c in out["channel"]]
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
    root: str = DEFAULT_REPORT_ROOT, write: bool = True, **cut_kwargs
) -> Dict[str, pd.DataFrame]:
    """Write one `outstanding.csv` into every run folder under `root`."""
    built: Dict[str, pd.DataFrame] = {}
    for name in sorted(os.listdir(root)):
        folder = os.path.join(root, name)
        if not os.path.exists(os.path.join(folder, "metadata.json")):
            continue
        table = build_one(folder, **cut_kwargs)
        if write:
            table.to_csv(os.path.join(folder, OUTSTANDING_FILENAME), index=False)
        built[name] = table
    return built


if __name__ == "__main__":
    results = main(write="--dry-run" not in sys.argv)
    for run, table in results.items():
        unknown = int((table["source_table"] == "unknown").sum())
        consensus = int(table["kept_by"].str.contains("consensus").sum())
        print(
            f"{len(table):>4} channels  {consensus:>2} consensus  "
            f"{table['evidence'].iloc[0]:<22}"
            f"{'  ⚠️ %d unknown' % unknown if unknown else '':<16}{run}"
        )
    print(f"\n{len(results)} runs, {sum(len(t) for t in results.values())} rows written")
