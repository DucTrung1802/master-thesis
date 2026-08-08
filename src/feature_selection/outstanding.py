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

## The two filters, in order

**1. `kept` — the run's own selection.** The ensemble ranked every channel, the
correlation prune dropped anything at |ρ| ≥ 0.9 against a better-ranked twin, and
`max_features` capped what survived. `kept=True` IS "the strongest features"; nothing
is re-ranked here.

**2. Ties on the ensemble score, broken by `permutation`.** The ensemble is a mean
RANK over six methods, so exact ties are common — `volume_negotiated` and
`foreign_own` both score 8.667 in the `basic` run. CONTEXT §4: `permutation` is the
only ranker measured OUT OF SAMPLE and is the one to believe when the methods
disagree, so it breaks the tie; |ρ| against the target breaks a tie in that. The
loser is dropped and named in `beat_in_tie`, never silently.

## ⚠️ What a row does NOT mean

**No run in the archive is a clean pass.** 18 of 22 computed no null at all; of the
four that did, `pool__fa` scored BELOW its null's mean (z = −0.25), `bank` sat on it
(z = +0.11), and `pool__ta` cleared its p95 bar but one shuffled draw of twenty still
beat it (CONTEXT §12). CONTEXT §6b measured a positive out-of-sample IC on SHUFFLED
LABELS, so a high rank in a run with `"null": null` is not evidence — it is an
internally consistent description of noise, and §11c says so about a ranking of this
exact shape. `evidence` carries that verdict on every row; `no_null` is the honest
majority value.

⚠️ **And the archive does not agree with itself.** Nineteen runs contain the same 27
`pool__basic` channels; the most repeatedly kept one survives 9 of those 19 selections
(CONTEXT §14a). Treat a single run's list as that run's list.
"""

import json
import os
import sys
from typing import Dict, List

import pandas as pd

from feature_selection.report import DEFAULT_REPORT_ROOT

OUTSTANDING_FILENAME = "outstanding.csv"

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
    "tie_group_size", "beat_in_tie", "absorbed_as_redundant",
    "run_id", "target", "horizon_h", "lookback_d", "evidence",
]


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
    return out.sort_values("rank").reset_index(drop=True)


def _absorbed(full: pd.DataFrame) -> Dict[str, str]:
    """channel → the redundant channels the |ρ| ≥ 0.9 prune folded into it."""
    dropped = full[full["dropped_for"].notna() & (full["dropped_for"] != "")]
    return {
        winner: "; ".join(sorted(group["channel"]))
        for winner, group in dropped.groupby("dropped_for")
    }


def build_one(folder: str) -> pd.DataFrame:
    """One run folder → its `outstanding` table. Does not write."""
    meta = json.load(open(os.path.join(folder, "metadata.json"), encoding="utf-8"))
    full = pd.read_csv(os.path.join(folder, "feature_importance.csv"))
    tables = meta["input"]["tables"]

    out = break_ties(full[full["kept"]].copy())
    out["schema"] = meta["input"]["schema"]
    out["source_table"] = [source_table(c, tables) for c in out["channel"]]
    out["grain"] = _grain(meta)
    out["best_stat"] = out.get("best_stat__permutation")
    out["absorbed_as_redundant"] = out["channel"].map(_absorbed(full)).fillna("")
    out["run_id"] = meta["run_id"]
    out["target"] = meta["target"]["name"]
    out["horizon_h"] = meta["setup"]["horizon_h"]
    out["lookback_d"] = meta["setup"]["lookback_d"]
    out["evidence"] = _evidence(meta)
    out["outstanding_rank"] = range(1, len(out) + 1)
    return out[COLUMNS]


def main(root: str = DEFAULT_REPORT_ROOT, write: bool = True) -> Dict[str, pd.DataFrame]:
    """Write one `outstanding.csv` into every run folder under `root`."""
    built: Dict[str, pd.DataFrame] = {}
    for name in sorted(os.listdir(root)):
        folder = os.path.join(root, name)
        if not os.path.exists(os.path.join(folder, "metadata.json")):
            continue
        table = build_one(folder)
        if write:
            table.to_csv(os.path.join(folder, OUTSTANDING_FILENAME), index=False)
        built[name] = table
    return built


if __name__ == "__main__":
    results = main(write="--dry-run" not in sys.argv)
    for run, table in results.items():
        unknown = int((table["source_table"] == "unknown").sum())
        print(
            f"{len(table):>3} channels  {table['evidence'].iloc[0]:<22}"
            f"{'  ⚠️ %d unknown' % unknown if unknown else '':<16}{run}"
        )
    print(f"\n{len(results)} runs, {sum(len(t) for t in results.values())} rows written")
