# src\feature_selection\evaluation.py
"""The bar a selection has to clear, and the arithmetic that sets it.

A feature-selection run always produces a positive out-of-sample IC. Picking the
best 12 of 27 channels by their fit to the labels earns one from noise alone, and
the amount it earns grows with the number of candidates and configurations tried.
So `IC > 0` is not a result. This module computes what *is*.

## The three things here

| | answers |
|---|---|
| `null_distribution` | what does this exact pipeline score on labels that cannot be predicted? |
| `effective_sample` | how many INDEPENDENT observations are behind an IC? |
| `ic_summary` | mean, trend, hit rate and an `n_eff`-aware error bar, per fold |

## ⚠️ The null must be BLOCK-shuffled, and the selection must be inside it

Two ways to get this wrong, both of which manufacture significance:

1. **Shuffling row by row.** A plain permutation destroys the label's own
   autocorrelation — consecutive `return_5day` values share 4 of their 5 days — so
   the null comes out far tighter than reality and the real result clears a bar
   that was never there. `block_shuffle` permutes contiguous blocks of `d + h`
   rows instead, which keeps the overlap structure intact.
2. **Holding the feature set fixed and only permuting the validation.** Selection
   is the step that inflates, so it has to be re-run on every draw. `factory` takes
   a panel and returns a `SelectionResult` for exactly that reason.

⚠️ **The null is not centred on zero, and that is the whole point.** Measured on
`pool__basic` at `d=20, h=5`: mean **+0.0167**, p95 **+0.0556**. Anything below the
p95 is inside what shuffled labels produce.

⚠️ **Re-run it whenever the candidate pool changes.** A wider pool has more to
overfit, so the bar rises with it. A null computed for 27 channels says nothing
about a run over `pool__ta`'s 900.
"""

import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence

import numpy as np
import pandas as pd
from scipy import stats


def block_shuffle(y: pd.Series, block: int, rng: np.random.Generator) -> pd.Series:
    """Permute `y` in contiguous blocks of `block` rows.

    The label's dependence structure survives; its relationship to the features
    does not. That is exactly the null wanted: "these features, this pipeline, a
    target with the same statistical shape and no connection to them".
    """
    if block < 1:
        raise ValueError(f"block must be >= 1, got {block}")
    values = y.to_numpy()
    n_blocks = int(np.ceil(len(values) / block))
    padded = np.concatenate(
        [values, np.full(n_blocks * block - len(values), np.nan)]
    )
    blocks = padded.reshape(n_blocks, block)
    shuffled = blocks[rng.permutation(n_blocks)].ravel()[: len(values)]
    return pd.Series(shuffled, index=y.index, name=y.name)


def effective_sample(n_rows: int, horizon: int) -> float:
    """`n / h` — independent observations behind `n` overlapping ones.

    ⚠️ **Optimistic.** It prices in the label overlap (consecutive samples share
    `h-1` of `h` label days) but not the INPUT overlap (`d-1` of `d` days), which
    a windowed design adds on top. Treat it as an upper bound on independence and
    therefore a lower bound on the error bar.
    """
    return max(1.0, n_rows / max(1, horizon))


def ic_standard_error(n_rows: int, horizon: int) -> float:
    """`1/√(n_eff − 1)` — the standard error of a rank correlation."""
    return 1.0 / np.sqrt(max(effective_sample(n_rows, horizon) - 1.0, 1.0))


def ic_summary(
    validation: pd.DataFrame,
    horizon: int,
    feature_set: str = "selected",
) -> pd.Series:
    """Mean, trend, spread and hit rate of the per-fold out-of-sample IC.

    ⚠️ **`trend` is reported beside `mean` because the mean hides it.** A mean of
    +0.05 built from folds that decay monotonically to negative is not a signal
    that averages +0.05 — it is one that stopped working, and only the slope says
    so.
    """
    rows = validation[validation["feature_set"] == feature_set]
    if rows.empty:
        raise ValueError(
            f"no rows for feature_set={feature_set!r}; have "
            f"{sorted(validation['feature_set'].unique())}"
        )
    ic = rows["ic"].to_numpy(float)
    n_test = int(rows["n_test"].iloc[0])
    # ⚠️ `n_eff` comes from the SELECTOR when it supplied one. `n_test / h` is right
    # for one company and wrong for a cross-section, where `N` stocks on one date are
    # one observation of the market and not `N` of them — and this function cannot
    # tell the two panels apart from a row count. The fallback keeps every result
    # produced before the column existed readable.
    n_eff = (
        float(rows["n_eff_test"].iloc[0])
        if "n_eff_test" in rows.columns
        else effective_sample(n_test, horizon)
    )
    trend = (
        stats.linregress(np.arange(len(ic)), ic).slope if len(ic) > 1 else np.nan
    )
    return pd.Series(
        {
            "folds": len(ic),
            "ic_mean": float(ic.mean()),
            "ic_trend_per_fold": float(trend),
            "ic_min": float(ic.min()),
            "ic_max": float(ic.max()),
            "ic_fold_sd": float(ic.std(ddof=1)) if len(ic) > 1 else np.nan,
            "hit_rate": float(rows["hit_rate"].mean()),
            "n_test_rows": n_test,
            "n_eff_per_fold": round(n_eff, 1),
            "se_ic_per_fold": round(
                float(1.0 / np.sqrt(max(n_eff - 1.0, 1.0))), 4
            ),
        },
        name=feature_set,
    )


@dataclass
class NullResult:
    """What the pipeline scores on labels it cannot predict."""

    draws: np.ndarray
    observed: float
    block: int
    failures: int = 0
    label: str = ""

    @property
    def p_value(self) -> float:
        """`P[null ≥ observed]`, the empirical one-sided p-value.

        `(k + 1) / (n + 1)` — the standard add-one estimator. It is floored at
        `1/(n+1)` by construction rather than by a clamp: 20 draws cannot
        distinguish p = 0.05 from p = 0.001, and printing 0.000 would claim they
        can.

        ⚠️ **THIS WAS `max(k, 1) / (n + 1)` UNTIL 2026-08-10, WHICH IS THE SAME
        NUMBER FOR k = 0 AND k = 1** (issue **NUL-4**). Flooring the COUNT instead of
        adding one conflated "no shuffled draw beat the real data" with "one did", and
        was anti-conservative by exactly one draw at every k ≥ 1:

            k (draws >= observed)      was        correct
            0                       0.0476        0.0476   <- agree
            1                       0.0476        0.0952   <- reported as significant
            2                       0.0952        0.1429
            3                       0.1429        0.1905

        Found by the `basic+economy_japan` run of 2026-08-10, which reported
        `p = 0.0476` while `null_max = +0.0916` sat well above its own
        `observed = +0.0509` — a p at the `1/(n+1)` floor is a claim that NO draw
        beat the observed, and the max said one had. Four of the five archived nulls
        were affected; only japan's crossed the 0.05 line (0.0476 → 0.0952). The
        2026-08-10 `vcb__basic` run (z = +2.15) is NOT affected — its `null_max`
        of +0.0648 is below its observed +0.0783, so k really is 0.
        """
        n = len(self.draws)
        if n == 0:
            return np.nan
        return float((self.draws >= self.observed).sum() + 1) / (n + 1)

    @property
    def z(self) -> float:
        sd = self.draws.std(ddof=1) if len(self.draws) > 1 else np.nan
        return (self.observed - self.draws.mean()) / sd if sd else np.nan

    @property
    def bar(self) -> float:
        """The 95th percentile of the null — the number to beat, not zero."""
        return float(np.percentile(self.draws, 95)) if len(self.draws) else np.nan

    @property
    def clears(self) -> bool:
        return bool(len(self.draws)) and self.observed > self.bar

    def summary(self) -> pd.Series:
        return pd.Series(
            {
                "label": self.label,
                "observed_ic": round(self.observed, 4),
                "null_mean": round(float(self.draws.mean()), 4),
                "null_sd": round(float(self.draws.std(ddof=1)), 4),
                "null_p95_BAR": round(self.bar, 4),
                "null_max": round(float(self.draws.max()), 4),
                "p_value": round(self.p_value, 4),
                "z_vs_null": round(self.z, 2),
                "clears_bar": self.clears,
                "draws": len(self.draws),
                "failed_draws": self.failures,
            },
            name=self.label or "null",
        )


def null_distribution(
    panel: pd.DataFrame,
    target: str,
    factory: Callable[[pd.DataFrame], "object"],
    observed: float,
    horizon: int,
    lookback: int = 1,
    n_draws: int = 20,
    seed: int = 0,
    feature_set: str = "selected",
    label: str = "",
    progress: bool = True,
) -> NullResult:
    """Re-run the WHOLE pipeline on block-shuffled labels, `n_draws` times.

    Args:
        panel: the joined frame the real run used.
        target: the label column to shuffle.
        factory: `panel -> SelectionResult`. Must perform the SELECTION too — a
            null that holds the feature set fixed measures the wrong thing.
        observed: the real run's mean IC, for the p-value.
        horizon, lookback: set the block size, `lookback + horizon`, so a block is
            at least as long as one sample's whole footprint.
        n_draws: 20 gives a p-value resolution of ~0.05. Raise it to make a
            borderline result decidable; nothing else will.

    ⚠️ A draw that raises is COUNTED, not silently skipped — a null built only
    from the draws that happened to succeed is a biased null.
    """
    rng = np.random.default_rng(seed)
    block = lookback + horizon
    draws: List[float] = []
    failures = 0

    # ⚠️ **THE ONE PERCENTAGE HERE THAT PREDICTS TIME.** The draws are the SAME
    # procedure on the same panel, so `done/n_draws` is a real fraction of the
    # work and the remaining time is a real extrapolation — unlike a count of
    # unequal phases. Cost: two `perf_counter()` calls per draw, each of which
    # is a whole selection.
    #
    # ⚠️ The per-draw selection is SILENCED while this runs. Without it a 20-draw
    # null prints its inner phase progress 21 times and the draw counter — the
    # only line that says how far along the run actually is — is lost in it.
    from feature_selection import selector as _selector

    started = time.perf_counter()

    def _report(i: int, message: str) -> None:
        if not progress:
            return
        done = i + 1
        elapsed = time.perf_counter() - started
        remaining = elapsed / done * (n_draws - done)
        print(
            f"  draw {done:>3}/{n_draws} {done / n_draws:>4.0%}  {message}"
            f"   [{elapsed / 60:.1f} min elapsed, ~{remaining / 60:.1f} min left]",
            flush=True,
        )

    for i in range(n_draws):
        shuffled = panel.copy()
        shuffled[target] = block_shuffle(panel[target], block, rng)
        try:
            with _selector.silenced():
                result = factory(shuffled)
            rows = result.validation
            draws.append(
                float(rows[rows["feature_set"] == feature_set]["ic"].mean())
            )
        except Exception as error:  # noqa: BLE001 - reported, not swallowed
            failures += 1
            _report(i, f"FAILED — {error}")
            continue
        _report(i, f"null mean IC {draws[-1]:+.4f}")
    return NullResult(
        draws=np.array(draws), observed=observed, block=block,
        failures=failures, label=label,
    )
