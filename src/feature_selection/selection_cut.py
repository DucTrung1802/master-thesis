# src\feature_selection\selection_cut.py
"""How many channels a run supports — measured per run, instead of fixed at 12.

`max_features=12` was chosen for a 27-channel single-ticker pool and then carried
into every run in the archive, including one with 1,458 channels. CONTEXT §9i and
§13c both measured the consequence: at 780 names and in the bank sector, **all 27
channels beat the pruned 12 in every fold**, so the cap was discarding information
rather than selecting. A cap that is the same number on 27 channels and on 1,458 is
not a decision about the data.

This module replaces it with two tiers measured from the run's OWN
`feature_importance.csv`, plus the redundancy prune the selector already ran but
truncated. Nothing is re-fitted and no database is touched: the archived CSVs
determine every number here.

    from feature_selection.selection_cut import suitable
    kept = suitable(importance_df, corr_df)      # 10-236 channels, run by run

## The two tiers, and why one is not enough

**1. CONSENSUS — the ensemble beats a shuffled-methods null.** The `ensemble` column
is the mean of six per-method ranks. Shuffle each method's rank column INDEPENDENTLY
and that mean still exists, so "a good ensemble score" needs a bar in exactly the
sense §8 demands. The shuffle preserves each method's marginal rank distribution
exactly — ties from `method="min"` included, and a dead method's constant column is
a constant after shuffling too — and destroys only cross-method agreement, which is
the thing the ensemble claims to measure. Channels are kept at a Benjamini-Hochberg
FDR of `fdr_q`.

⚠️ **The independence assumption was MEASURED, not assumed.** Mean pairwise Spearman
between the six rank columns is **+0.071** across the 20 archived runs (range −0.063
to +0.193). The methods rank almost independently, so the null's variance is right to
within a few percent. That number is also a finding in its own right: the six rankers
agree barely more than chance, which is §14a's instability seen inside a single run
instead of across nineteen.

⚠️ **This tier is BRUTAL — 12 rows out of 952 across the whole archive**, 0-2 per
run, and on `vcb__basic` it keeps none. That is the correct answer to the question it
asks and the wrong basis for a fetch list, which is why it is not the only tier.

**2. SPECIALIST — top of some single method's own score curve.** The ensemble is a
mean rank, so it punishes a channel that one method is certain about and five have no
opinion on. `windows.aggregate_to_channels` already rejects that logic at the stat
level ("is there ANY view of this window that carries signal") and the same argument
applies across methods. For each LIVE method, its normalised scores are sorted
descending and cut at the **knee** — the point of greatest drop below the chord — and
the union over methods is the tier.

⚠️ **The knee works on SCORES and is meaningless on RANKS.** Measured: a knee on the
`ensemble` curve keeps 92 of 113 and 1,313 of 1,458, because a mean of ranks is
near-uniform by construction and has no breakpoint — the same flatness §10c.1 fixed
in the ranking chart. A per-method score curve is long-tailed (`xgb_gain` spans three
orders of magnitude) and has a real elbow.

## Then the prune, uncapped

The union is walked best-first and a channel is dropped when it correlates at
`|ρ| >= corr_threshold` with something already kept — `selector._prune`'s rule with
no `max_features` break. **The cap was also truncating `dropped_for`**: in the
archive every channel ranked below the break has `kept=False` and an empty
`dropped_for`, so "pruned as redundant" and "never examined" are the same value.
Re-pruning from the archived `channel_correlation.csv` is what makes them different.

## ⚠️ What a count from here does NOT mean

`consensus` carries an error rate; `specialist` carries none — it is a description of
where a score curve bends, not evidence that the channel predicts anything. The
run-level null (§10's `"null": null`) is still the only thing that says whether the
run as a whole beat shuffled LABELS, and 19 of the 20 archived runs never computed
one. `kept_by` is on every row so the two are never confused.
"""

from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

from feature_selection.selector import METHODS

# Pooled null values to draw. The bar is a quantile of `draws x p` numbers, so the
# draw count scales DOWN with the pool width — 274 draws on 1,458 channels is the
# same 400k-value pool as 3,000 draws on 27, and the wide runs are the slow ones.
NULL_POOL_TARGET = 400_000
MIN_NULL_DRAWS = 200

DEFAULT_FDR_Q = 0.10
DEFAULT_CORR_THRESHOLD = 0.9


def live_methods(importance: pd.DataFrame) -> List[str]:
    """The methods that separated anything — a constant column ranked nothing.

    Mirrors `selector.run`'s `dead_methods` rule so a method dead in the run is dead
    here too, rather than contributing a knee over a flat curve.
    """
    return [m for m in METHODS if m in importance and importance[m].nunique() > 1]


def method_ranks(importance: pd.DataFrame) -> pd.DataFrame:
    """Rebuild the per-method rank matrix the ensemble was averaged from.

    ⚠️ `feature_importance.csv` stores the MIN-MAX NORMALISED scores, not the ranks.
    A min-max is monotone, so `rank` of the stored score is the rank the selector
    computed — verified across all 20 archived runs, where the rebuilt mean rank
    reproduces the stored `ensemble` to 2.8e-14.
    """
    return importance[live_methods(importance) or list(METHODS)].rank(
        ascending=False, method="min"
    )


# ------------------------------------------------------------------- tier 1


def consensus_null(ranks: pd.DataFrame, seed: int = 0) -> np.ndarray:
    """Sorted pool of ensemble scores with cross-method agreement destroyed.

    Each column is permuted on its own, so every method keeps its exact marginal
    distribution of ranks and only the alignment between methods is broken.
    """
    values = ranks.to_numpy()
    p, m = values.shape
    draws = max(MIN_NULL_DRAWS, NULL_POOL_TARGET // max(p, 1))
    rng = np.random.default_rng(seed)
    pool = np.empty((draws, p))
    for b in range(draws):
        pool[b] = np.column_stack(
            [rng.permutation(values[:, j]) for j in range(m)]
        ).mean(axis=1)
    return np.sort(pool.ravel())


def _bh_mask(pvalues: np.ndarray, q: float) -> np.ndarray:
    """Benjamini-Hochberg step-up at FDR `q`."""
    order = np.argsort(pvalues)
    ordered = pvalues[order]
    n = len(ordered)
    passing = ordered <= q * np.arange(1, n + 1) / n
    mask = np.zeros(n, dtype=bool)
    if passing.any():
        mask[order[: int(np.max(np.nonzero(passing)[0])) + 1]] = True
    return mask


def consensus_tier(
    importance: pd.DataFrame, fdr_q: float = DEFAULT_FDR_Q, seed: int = 0
) -> Tuple[Set[str], pd.Series]:
    """Channels whose cross-method agreement beats the shuffled-methods null.

    Returns the kept set and the empirical p-value per channel. A LOW `ensemble` is
    good, so the test is one-sided on the left tail; the p-value carries the
    `(k+1)/(N+1)` floor for the same reason §9d quotes a z rather than a p at 20
    draws — a pool of N values cannot report a p below `1/(N+1)`.

    ⚠️ Benjamini-Hochberg assumes independent or positively-dependent tests. The
    p-values here share one null pool and are therefore dependent; `fdr_q` is a
    stated working level, not a guarantee.
    """
    ranks = method_ranks(importance)
    pool = consensus_null(ranks, seed=seed)
    observed = ranks.mean(axis=1).to_numpy()
    below = np.searchsorted(pool, observed, side="right")
    pvalues = (below + 1.0) / (len(pool) + 1.0)
    kept = set(importance.loc[_bh_mask(pvalues, fdr_q), "channel"])
    return kept, pd.Series(pvalues, index=importance["channel"], name="consensus_p")


# ------------------------------------------------------------------- tier 2


def knee(curve: Sequence[float]) -> int:
    """How many points sit before a descending curve's elbow (Kneedle, 1-based).

    Normalise both axes to [0, 1] and take the point furthest BELOW the chord from
    first to last. On a convex curve — a few large scores then a long flat tail —
    that is the bend. A concave or straight curve has no bend and returns 1, which
    is the honest answer: only the leader stands out.

    ⚠️ **The elbow point itself is EXCLUDED.** On `[1.0, .9, .8, .1, .08, …]` the
    maximum deviation lands on `0.1` — the foot of the cliff, already in the tail —
    so the count is 3, not 4. Floored at 1 so a curve whose deviation peaks at the
    first point still returns its leader rather than an empty tier.
    """
    y = np.asarray(curve, dtype=float)
    n = len(y)
    if n < 3:
        return n
    span = y[0] - y[-1]
    if span <= 0:
        return 1
    x = np.linspace(0.0, 1.0, n)
    return max(1, int(np.argmax((1.0 - x) - (y - y[-1]) / span)))


def specialist_tier(importance: pd.DataFrame) -> Tuple[Set[str], Dict[str, int]]:
    """Union over live methods of the channels before that method's own knee.

    Returns the set and each method's knee position, because the positions are the
    diagnostic: a method whose knee sits near `p` did not separate its pool and is
    admitting everything, which is visible only as a number.
    """
    kept: Set[str] = set()
    knees: Dict[str, int] = {}
    for method in live_methods(importance):
        ordered = importance[["channel", method]].sort_values(method, ascending=False)
        cut = knee(ordered[method].to_numpy())
        knees[method] = cut
        kept |= set(ordered["channel"].iloc[:cut])
    return kept, knees


# -------------------------------------------------------------------- prune


def prune(
    order: Sequence[str],
    corr: pd.DataFrame,
    threshold: float = DEFAULT_CORR_THRESHOLD,
) -> Tuple[List[str], Dict[str, str]]:
    """`selector._prune` with no `max_features` break — walk everything.

    Kept separate from the selector's copy on purpose: that one runs inside a fit and
    reads a live correlation matrix, this one runs against an archived CSV. They must
    agree on the RULE, which is why the loop is identical apart from the missing cap.

    ⚠️ **A pair is resolved by ENSEMBLE order, so the better specialist can lose.**
    In the `basic` run `prop_sell_val` scores 1.000 on `spearman` — the largest |ρ|
    in the pool — and is absorbed into `prop_sell_vol`, which `spearman` scores 0.771
    but the ensemble ranks half a place higher. At |ρ| ≥ 0.9 the two carry the same
    information, so the fetch list loses nothing; the SCORE reported against the pair
    is the weaker one. `absorbed_as_redundant` names the twin for exactly this reason.
    Ordering the prune by best-single-method rank instead would diverge from
    `selector._prune`, and the two must stay the same rule.
    """
    kept: List[str] = []
    dropped: Dict[str, str] = {}
    matrix = corr.to_numpy()
    position = {channel: i for i, channel in enumerate(corr.index)}
    for channel in order:
        i = position[channel]
        clash = next(
            (k for k in kept if abs(matrix[i, position[k]]) >= threshold), None
        )
        if clash is None:
            kept.append(channel)
        else:
            dropped[channel] = clash
    return kept, dropped


# --------------------------------------------------------------------- rule


def suitable(
    importance: pd.DataFrame,
    corr: Optional[pd.DataFrame] = None,
    *,
    corr_loader: Optional[Callable[[List[str]], Optional[pd.DataFrame]]] = None,
    fdr_q: float = DEFAULT_FDR_Q,
    corr_threshold: float = DEFAULT_CORR_THRESHOLD,
    seed: int = 0,
) -> pd.DataFrame:
    """One run's `feature_importance.csv` → the channels it actually supports.

    Args:
        importance: the archived `feature_importance.csv`, one row per channel.
        corr: the archived `channel_correlation.csv`. With neither this nor
            `corr_loader` the redundancy prune is skipped and `dropped_for` stays
            empty — the count is then the union of the two tiers, an OVER-count
            wherever the pool has twins.
        corr_loader: called with the candidate channels and returning just their
            submatrix. ⚠️ Preferred on a wide pool: the archived matrix is the full
            N×N and reaches **40 MB** on `basic+economy_usa` (§10b), of which a
            250-channel candidate set needs 3 %.
        fdr_q: Benjamini-Hochberg level for the consensus tier.
        corr_threshold: `|ρ|` at or above which the lower-ranked channel is redundant.

    Returns the kept rows in `ensemble` order, with five columns added: `kept_by`
    (`consensus`, `specialist` or both), `consensus_p`, `dropped_for` (recomputed,
    complete), `absorbed_as_redundant` and `n_candidates`.
    """
    frame = importance.sort_values("ensemble", kind="stable").reset_index(drop=True)

    consensus, pvalues = consensus_tier(frame, fdr_q=fdr_q, seed=seed)
    specialist, _ = specialist_tier(frame)
    candidates = consensus | specialist
    order = [c for c in frame["channel"] if c in candidates]

    if corr is None and corr_loader is not None:
        corr = corr_loader(order)
    if corr is not None:
        kept, dropped = prune(
            order, corr.loc[order, order], threshold=corr_threshold
        )
    else:
        kept, dropped = order, {}

    out = frame[frame["channel"].isin(kept)].copy()
    out["kept_by"] = [
        "+".join(
            filter(
                None,
                [
                    "consensus" if c in consensus else "",
                    "specialist" if c in specialist else "",
                ],
            )
        )
        for c in out["channel"]
    ]
    out["consensus_p"] = out["channel"].map(pvalues)
    out["dropped_for"] = out["channel"].map(dropped).fillna("")
    out["n_candidates"] = len(candidates)
    # What each survivor stands in for — the prune's victims, which the capped run
    # could not record past its break.
    absorbed: Dict[str, List[str]] = {}
    for loser, winner in dropped.items():
        absorbed.setdefault(winner, []).append(loser)
    out["absorbed_as_redundant"] = [
        "; ".join(sorted(absorbed.get(c, []))) for c in out["channel"]
    ]
    return out.reset_index(drop=True)


def cut_report(
    importance: pd.DataFrame,
    corr: Optional[pd.DataFrame] = None,
    *,
    corr_loader: Optional[Callable[[List[str]], Optional[pd.DataFrame]]] = None,
    fdr_q: float = DEFAULT_FDR_Q,
    corr_threshold: float = DEFAULT_CORR_THRESHOLD,
    seed: int = 0,
) -> Dict:
    """The counts behind one `suitable()` call — what each tier and the prune did.

    ⚠️ `specialist_share` near 1.0 means the tier excluded nothing. On a 27-channel
    pool six knees cover almost every channel, so the rule degenerates to "the whole
    non-redundant pool" — which is the honest answer at that width (§9h: a 27-channel
    single-ticker run cannot resolve anything) but must not be read as a selection.
    """
    frame = importance.sort_values("ensemble", kind="stable").reset_index(drop=True)
    consensus, _ = consensus_tier(frame, fdr_q=fdr_q, seed=seed)
    specialist, knees = specialist_tier(frame)
    kept = suitable(
        frame,
        corr,
        corr_loader=corr_loader,
        fdr_q=fdr_q,
        corr_threshold=corr_threshold,
        seed=seed,
    )
    return {
        "n_channels": len(frame),
        "live_methods": live_methods(frame),
        "n_consensus": len(consensus),
        "n_specialist": len(specialist),
        "specialist_share": round(len(specialist) / max(len(frame), 1), 3),
        "method_knees": knees,
        "n_candidates": len(consensus | specialist),
        "n_kept": len(kept),
        "n_pruned_redundant": len(consensus | specialist) - len(kept),
        "fdr_q": fdr_q,
        "corr_threshold": corr_threshold,
    }
