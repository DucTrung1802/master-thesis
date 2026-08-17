# src\feature_selection\studies\two_layer_null.py
"""⚠️ THE NULL THE LAYER-2 RUN DID NOT PAY FOR — TODO P0-1.

`feature_selection.run --null-draws N` shuffles the label and re-runs **the selection it
was given**. On a layer-2 run that is layer 2 ONLY, and the 208 channels it selects from
were already chosen using the same label at layer 1. So the pre-selection sits inside
every draw, the bar is computed for a procedure that is not the one that produced the
number, and six pools that each failed their own null can union into a shortlist that
"clears" at 2.4x the best individual IC. That is what happened on 2026-08-17:

    layer 2 observed  ic_mean +0.1369   p95 bar +0.0428   z +4.48   p 0.0909  "CLEARS"

This module computes the honest bar: **each draw shuffles the label once and then re-runs
BOTH layers on it** — six layer-1 selections, the union of their survivors, and one
layer-2 selection over that union. What comes out is the distribution of layer-2 ICs
obtainable from a label with the same autocorrelation and no relation to the features.

    python -m feature_selection.studies.two_layer_null --draws 10

⚠️ **The shuffle is applied ONCE per draw and shared by all seven inner selections.** A
per-selection reshuffle would break the very thing being measured: layer 2's candidates
have to be chosen using the *same* fake label that layer 2 is then scored against, or the
draw is not reproducing the procedure.

⚠️ **`block_shuffle` at `d + h = 25`**, the same block `evaluation.null_distribution`
uses, so this bar and the run's own bar differ ONLY in what they re-run.

⚠️ **Panels are read ONCE and cached.** Only the target column is re-shuffled per draw.
Re-reading would add ~40 min to a 2 h job and could not change a number.

⚠️ **This is not cheap and it is not optional.** ~12.5 min per draw measured
(layer 1 no-null ~11 min for six pools, layer 2 ~1.5 min), so 10 draws is ~2 h. The
alternative is to keep quoting a bar that prices in none of layer 1.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Dict, List

import numpy as np
import pandas as pd

from feature_selection import evaluation
from feature_selection.run import ALL_TARGETS, IDENTITY, TARGETS_TABLE
from feature_selection.selector import FeatureSelector
from feature_selection.unified_reader import UnifiedSchemaReader
from utils import runtime

# The six layer-1 pools of the 2026-08-17 sweep, in the order they were run.
LAYER1_POOLS = (
    "pool__market_breadth",
    "pool__news_daily",
    "pool__bonds",
    "pool__stock_market",
    "pool__fa",
    "pool__ta",
)

# ⚠️ The setup of the run being judged. A null computed under a different setup is a bar
# for a different experiment (§5 rule 1), so these are not defaults to be tuned — they are
# a transcription of `metadata.json`'s `setup` block from the observed run.
SETUP = dict(
    lookback=20,
    horizon=5,
    n_splits=5,
    min_train=500,
    random_state=18,
    corr_threshold=0.9,
    normalize="none",
)
OBSERVED_IC = 0.1369
BAR_PERCENTILE = 95


def _write_progress(path: str, payload: Dict) -> None:
    """Rewrite the progress file whole. ⚠️ Written, not appended: a truncated append is
    unparseable, and a reader that cannot parse the file learns nothing."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _panels(ticker: str, target: str) -> Dict[str, pd.DataFrame]:
    """`{pool: pool__basic ⋈ pool ⋈ pool__targets}`, read once."""
    out: Dict[str, pd.DataFrame] = {}
    with UnifiedSchemaReader(ticker) as reader:
        for pool in LAYER1_POOLS:
            frame = reader.join(["pool__basic", pool, TARGETS_TABLE])
            out[pool] = frame
            print(f"  read {pool:24} {frame.shape[0]:,} x {frame.shape[1]}", flush=True)
    return out


def _select(panel: pd.DataFrame, target: str, device: str) -> List[str]:
    """One selection, returning the kept channels. `stability=False` — a draw is not
    diagnosed, only scored, and stability is the single most expensive diagnostic."""
    exclude = IDENTITY + [c for c in ALL_TARGETS if c != target]
    selector = FeatureSelector(
        panel=panel, target=target, exclude=exclude, device=device, **SETUP
    )
    return list(selector.run(stability=False).kept)


def _layer2_ic(panel: pd.DataFrame, channels: List[str], target: str, device: str) -> float:
    exclude = IDENTITY + [c for c in ALL_TARGETS if c != target]
    keys = [c for c in ("date", "exchange", "ticker") if c in panel.columns]
    frame = panel[keys + [c for c in channels if c in panel.columns] + [target]].copy()
    selector = FeatureSelector(
        panel=frame, target=target, exclude=exclude, device=device, **SETUP
    )
    result = selector.run(stability=False)
    return float(evaluation.ic_summary(result.validation, SETUP["horizon"])["ic_mean"])


def main(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", default="VCB")
    parser.add_argument("--target", default="return_5day")
    parser.add_argument("--draws", type=int, default=10)
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--seed", type=int, default=18)
    parser.add_argument("--out", default="reports/two_layer_null.json")
    args = parser.parse_args(argv)

    with runtime.RunTimer(
        f"two_layer_null  {args.ticker} / {args.target}  draws={args.draws}",
        device=args.device,
    ):
        print("reading panels once...", flush=True)
        panels = _panels(args.ticker, args.target)
        # The union panel every layer-2 selection is cut from. Built from the widest
        # read so any layer-1 survivor can be found in it.
        union = None
        for pool, frame in panels.items():
            union = frame if union is None else union.merge(
                frame, on=[c for c in ("date", "exchange", "ticker") if c in frame.columns],
                how="inner", suffixes=("", f"__dup_{pool}"),
            )
        union = union.loc[:, ~union.columns.str.contains("__dup_")]
        # ⚠️ Cut every panel to the union's keys. Without this the shuffled label cannot
        # be shared: the panels differ by 31 rows because `pool__ta` stops 2026-06-26
        # (`STA-1`), so its INNER join is 4,235 against the others' 4,266. The quantity
        # being nulled is a LAYER-2 IC, so matching layer 2's row set is the faithful
        # choice — at the cost of five layer-1 selections seeing 31 fewer rows than they
        # did in the observed run.
        keys = [c for c in ("date", "exchange", "ticker") if c in union.columns]
        spine = union[keys].copy()
        panels = {
            pool: spine.merge(frame, on=keys, how="inner").reset_index(drop=True)
            for pool, frame in panels.items()
        }
        union = union.reset_index(drop=True)
        widths = {pool: len(frame) for pool, frame in panels.items()}
        assert set(widths.values()) == {len(union)}, f"row counts disagree: {widths}"
        print(f"  union panel {union.shape[0]:,} x {union.shape[1]}\n", flush=True)

        progress_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..",
                         "reports", "two_layer_null.progress.json")
        )
        print(f"progress -> {progress_path}", flush=True)
        rng = np.random.default_rng(args.seed)
        block = SETUP["lookback"] + SETUP["horizon"]
        draws: List[float] = []
        started = time.perf_counter()

        for i in range(args.draws):
            shuffled = evaluation.block_shuffle(union[args.target], block, rng)
            kept: List[str] = []
            for pool, frame in panels.items():
                fake = frame.copy()
                # Safe by POSITION only because every panel was cut to the union's keys
                # in the union's own order above, and that is asserted there.
                fake[args.target] = np.asarray(shuffled)
                kept.extend(_select(fake, args.target, args.device))
            kept = sorted(set(kept))

            fake_union = union.copy()
            fake_union[args.target] = np.asarray(shuffled)
            ic = _layer2_ic(fake_union, kept, args.target, args.device)
            draws.append(ic)

            done = i + 1
            elapsed = time.perf_counter() - started
            # ⚠️ **RULE 20, AND IT BIT ON THIS VERY SCRIPT.** The first smoke run produced
            # an EMPTY output file for 14 minutes while the process sat at 860 s of CPU —
            # `print(flush=True)` flushes Python's buffer, but a redirected stdout is
            # re-buffered downstream, which is exactly how a 4-hour null run was lost
            # once before. A 2-hour job that cannot be watched is a job that gets killed
            # on suspicion, so each draw is written to disk the moment it finishes and
            # the file is rewritten whole rather than appended.
            _write_progress(
                progress_path,
                {
                    "draw": done,
                    "n_draws": args.draws,
                    "elapsed_s": round(elapsed, 1),
                    "eta_s": round(elapsed / done * (args.draws - done), 1),
                    "draws_so_far": [round(x, 6) for x in draws],
                    "observed_ic": OBSERVED_IC,
                    "beaten_so_far": int(sum(1 for x in draws if x >= OBSERVED_IC)),
                },
            )
            print(
                f"  draw {done:>3}/{args.draws} {done / args.draws:>4.0%}  "
                f"layer-1 union {len(kept):>4} channels -> layer-2 ic {ic:+.4f}"
                f"   [{elapsed / 60:.1f} min elapsed, "
                f"~{elapsed / done * (args.draws - done) / 60:.1f} min left]",
                flush=True,
            )

        array = np.array(draws, dtype=float)
        bar = float(np.percentile(array, BAR_PERCENTILE))
        beat = int((array >= OBSERVED_IC).sum())
        summary = {
            "observed_ic": OBSERVED_IC,
            "n_draws": int(len(array)),
            "null_mean": float(array.mean()),
            "null_sd": float(array.std(ddof=1)) if len(array) > 1 else float("nan"),
            "null_p95_BAR": bar,
            "null_max": float(array.max()),
            "draws_at_or_above_observed": beat,
            "p_value": (beat + 1) / (len(array) + 1),
            "clears_bar": bool(OBSERVED_IC > bar),
            "draws": [round(x, 6) for x in draws],
            "what_this_prices_in": (
                "BOTH selection layers, re-run inside every draw — unlike the run's own "
                "null, which re-runs layer 2 only and inherits layer 1's pre-selection."
            ),
        }
        print(
            f"\nobserved {OBSERVED_IC:+.4f} | null mean {summary['null_mean']:+.4f} "
            f"| p95 bar {bar:+.4f} | null MAX {summary['null_max']:+.4f} "
            f"| p {summary['p_value']:.4f} | "
            f"{'CLEARS' if summary['clears_bar'] else 'FAILS'}"
        )
        if summary["null_max"] >= OBSERVED_IC:
            print(
                f"WARNING: a draw reached {summary['null_max']:+.4f}, at or above the "
                f"observed - quote the max beside the bar (rule 3)."
            )
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", args.out))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2)
        print(f"written {path}")


if __name__ == "__main__":
    main()
