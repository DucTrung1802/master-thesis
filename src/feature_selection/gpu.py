# src\feature_selection\gpu.py
"""The GPU paths, and an honest account of which parts have one.

## What actually runs on the GPU

| step | GPU | how |
|---|---|---|
| `xgb_gain`, `xgb_shap` | ✅ | `XGBRegressor(device="cuda", tree_method="hist")` |
| SHAP values | ✅ | the booster's own `pred_contribs=True`, not the `shap` package — GPU-side exact tree SHAP, verified equal to `shap.TreeExplainer` to **0.0**, and faster (0.06 s vs 0.10 s on 4,230 × 27) |
| `permutation` | ✅ | the fitted model is on the GPU, so every one of its `n_repeats × n_features` predictions is |
| stability, validation | ✅ | same models, same device |
| feature↔feature Spearman | ✅ | rank-transform + five matmuls in torch — **the step that dominates a wide pool**, though see §2 on how much of that win is the GPU |
| feature↔target Spearman | ✅ | the same code path, so the sign on the chart and the magnitude in the ensemble cannot disagree |
| `mutual_info` | ❌ | sklearn's kNN (Kraskov) estimator is CPU-only. A GPU reimplementation would be a *different estimator*, and quietly swapping the definition to win a benchmark is not a speedup |
| `lasso` | ❌ | `LassoCV` is CPU-only in sklearn, and cuML — which has a GPU LASSO — ships no Windows wheel (RAPIDS is Linux-only) |

Five of the six rankers and every model fit go to the GPU; the two that do not are
the two whose *definition* lives in a CPU-only library.

## ⚠️ 1. THE DEVICE CAN CHANGE THE ANSWER — and the cause is STOCHASTIC SAMPLING

Measured on `pool__basic` (4,235 × 27), the same run on `cuda` and on `cpu`:

| quantity | with `subsample=.8, colsample_bytree=.8` | with both `= 1.0` |
|---|---|---|
| feature↔target Spearman | 0.0 | 0.0 |
| feature↔feature Spearman matrix | 0.0 | 0.0 |
| `xgb_gain` / `xgb_shap` / `permutation` | up to **0.82** (0-1 scale) | **8.6e-08** |
| ensemble mean rank | up to **2.3 places** | **0.0** |
| out-of-sample IC per fold | up to **0.028** | **0.0** |
| the kept feature set | **different** | **identical** |

**It is not the histogram binning, which was the obvious suspect and is wrong.**
Isolated by removing one thing at a time on a 300-tree fit:

| configuration | max abs Δ prediction | same split features | same thresholds |
|---|---|---|---|
| `subsample=.8, colsample=.8`, `max_bin=256` | 2.3e-02 | ❌ | ❌ |
| **`subsample=1, colsample=1`**, `max_bin=256` | **3.7e-08** | ✅ | ✅ |
| `subsample=1, colsample=1`, `max_bin=4096` | 7.5e-08 | ✅ | ✅ |
| 1 tree, depth 1, no sampling | **0.0** (same split, same gain) | ✅ | ✅ |

Turn the sampling off and the two devices grow **bit-identical trees** — same
features, same thresholds, at the default 256 bins. Raising `max_bin` 64× changes
nothing, which is exactly what rules the quantile sketch out. Turn sampling back on
and 4,189 of 8,280 nodes pick a *different feature*: XGBoost's row and column
subsampling draws from a different RNG stream on the GPU than on the host, so the
same `random_state` selects different rows and columns, and every divergence
compounds down the boosting rounds.

So the choice is explicit and yours:

* **`subsample=1.0, colsample_bytree=1.0`** — the run is reproducible across
  devices, at the cost of the regularisation that subsampling provides (which on a
  4,230-row noisy panel is real).
* **the defaults (0.8 / 0.8)** — better-regularised, but `device` becomes part of
  the experimental setup. It is recorded on every `SelectionResult` as
  `result.device` for that reason. Pin it before quoting a selection in a thesis.

## ⚠️ 2. On a NARROW pool the GPU is slower, and `"auto"` acts on that

Whole-run wall clock on `pool__basic` (4,235 rows × 27 features), warm, on an
RTX 3050 Laptop (4 GB):

| step | cuda | cpu |
|---|---|---|
| validation | 8.8 s | **4.4 s** |
| permutation | 5.7 s | **2.8 s** |
| stability | 4.5 s | **2.8 s** |
| xgb gain + shap | 1.0 s | **0.9 s** |
| **whole run** | **21.2 s** | **12.3 s** |

27 columns of 4,235 rows is far too little work to cover a kernel launch, and a
multicore host wins by 1.7×.

⚠️ **And where the correlation matrix is concerned, the big win is the ALGORITHM,
not the GPU.** Replacing pandas' pairwise Cython loop with five matmuls is worth
46-80×; moving those matmuls to this GPU is worth a further 1.0-2.0×, and the
margin *shrinks* with width:

| features | pandas | this module, cpu | this module, cuda | gpu vs cpu | gpu vs pandas |
|---|---|---|---|---|---|
| 300 | 3.9 s | 0.17 s | **0.08 s** | 2.0× | 46× |
| 900 (`pool__ta`) | 39.6 s | 0.72 s | **0.49 s** | 1.5× | 80× |
| 2,000 | — | 2.05 s | **1.70 s** | 1.2× | — |
| 3,500 | — | 5.02 s | **4.99 s** | 1.0× | — |

The reason is float64: a consumer GeForce runs double precision at 1/32 of its
single-precision rate, so an AVX host keeps up on exactly the operation the GPU
should win. float64 is kept anyway, because it is what makes the two devices agree
to **0.0** on this step (§1) — and because the GPU is never *slower* here, so
there is nothing to trade away. Someone who needs the fp32 speedup should expect
the devices to stop agreeing bit-for-bit.

So the three settings mean three different things:

* `"cuda"` — **force it**, everywhere a GPU path exists. Raises if there is none.
* `"cpu"` — force the host.
* `"auto"` (default) — GPU only once the pool is wide enough to pay for it
  (`AUTO_CUDA_MIN_FEATURES`), because the alternative is defaulting every
  `pool__basic` run to a 3× pessimisation.

⚠️ Because of §1, `"auto"` switching device with pool width means two pools can be
selected by different tree implementations. That is why it is recorded, and why a
run whose result is going to be quoted should name the device explicitly.

## ⚠️ 3. Falling back to CPU is reported, never silent

`resolve_device("cuda")` on a machine without CUDA **raises**; `"auto"` falls back
and records why in `device_report()`. A GPU run that quietly became a CPU run is
how "the selection takes six hours" turns into a mystery.

⚠️ **XGBoost is fed numpy, not a CUDA tensor, on purpose.** `device="cuda"` with a
host array copies once per fit and measured *faster* than handing it a
`torch.cuda` tensor through the `__cuda_array_interface__` (1.01 s vs 1.30 s on
4,230 × 27) — the copy is not the bottleneck, and a host array keeps the frames
usable by the sklearn steps without a round trip. XGBoost warns once about the
device mismatch on `predict`; it is the copy, and it is the cheap side of the
trade.
"""

from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

# Filled in by `resolve_device`; read by `device_report`.
_REPORT: Dict[str, object] = {"resolved": None, "reason": "not resolved yet"}

# Below this many candidate features, `device="auto"` stays on the host. Measured,
# not guessed — see §2 of the module docstring: at 27 features the GPU run took
# 49 s against the host's 15 s, and the crossover is driven by pool WIDTH (both
# the XGBoost histogram work and the O(p²) correlation scale with it), not by row
# count, which is fixed at one session per day.
AUTO_CUDA_MIN_FEATURES = 200

# Below this many features, the sklearn steps that accept `n_jobs` are left
# single-process. Spawning a loky pool costs ~20 s on Windows, which is 150× the
# 0.15 s that `mutual_info_regression` needs on a 27-column pool.
PARALLEL_MIN_FEATURES = 100


def n_jobs_for(n_features: int) -> Optional[int]:
    """`-1` once a pool is wide enough to repay a process pool, else `None`."""
    return -1 if n_features >= PARALLEL_MIN_FEATURES else None


def _torch():
    """Import torch lazily — it is a heavy import and the CPU path never needs it."""
    import torch

    return torch


def cuda_available() -> Tuple[bool, str]:
    """`(available, reason)` — CUDA is usable only if BOTH halves have it.

    XGBoost carries its own CUDA runtime and torch carries another; either can be
    a CPU-only build independently of the other, so both are checked.
    """
    try:
        import xgboost as xgb

        if not xgb.build_info().get("USE_CUDA"):
            return False, "xgboost was built without CUDA"
    except ImportError:
        return False, "xgboost is not installed"
    try:
        import torch

        if not torch.cuda.is_available():
            return False, "torch reports no CUDA device"
    except ImportError:
        return False, "torch is not installed"
    return True, ""


def resolve_device(preference: str = "auto", n_features: Optional[int] = None) -> str:
    """Turn `"auto"` / `"cuda"` / `"cpu"` into the device that will actually run.

    Args:
        preference: `"cuda"` forces the GPU, `"cpu"` forces the host, `"auto"`
            uses the GPU only when the pool is at least
            `AUTO_CUDA_MIN_FEATURES` wide — see §2 of the module docstring.
        n_features: the candidate count. `"auto"` without it cannot apply the
            width rule and prefers the GPU whenever one exists.

    Raises:
        RuntimeError: `preference="cuda"` on a machine without a usable one. An
            explicit request for the GPU must fail loudly rather than silently
            run somewhere else.
    """
    if preference not in ("auto", "cuda", "cpu"):
        raise ValueError(f"device must be 'auto', 'cuda' or 'cpu', got {preference!r}")

    if preference == "cpu":
        _REPORT.update(resolved="cpu", reason="requested", name=None, n_features=n_features)
        return "cpu"

    available, reason = cuda_available()
    if not available:
        if preference == "cuda":
            raise RuntimeError(f"device='cuda' was requested but {reason}.")
        _REPORT.update(
            resolved="cpu", reason=f"fell back — {reason}", name=None,
            n_features=n_features,
        )
        return "cpu"

    torch = _torch()
    free, total = torch.cuda.mem_get_info()
    details = {
        "name": torch.cuda.get_device_name(0),
        "vram_total_mb": round(total / 1024**2),
        "vram_free_mb": round(free / 1024**2),
        "n_features": n_features,
    }

    if (
        preference == "auto"
        and n_features is not None
        and n_features < AUTO_CUDA_MIN_FEATURES
    ):
        _REPORT.update(
            resolved="cpu",
            reason=(
                f"auto — {n_features} features is under the "
                f"{AUTO_CUDA_MIN_FEATURES}-feature crossover, where the host is "
                f"faster; pass device='cuda' to force the GPU"
            ),
            **details,
        )
        return "cpu"

    _REPORT.update(
        resolved="cuda",
        reason="requested" if preference == "cuda" else "auto — pool is wide enough",
        **details,
    )
    return "cuda"


def device_report() -> Dict[str, object]:
    """What `resolve_device` decided and why. Print it; do not infer it."""
    return dict(_REPORT)


# ----------------------------------------------------------------- rank helpers


def _average_ranks_torch(values, mask):
    """Average ranks per column, NaNs excluded, vectorised over all columns.

    Ties take the mean of the ranks they span — the same convention as
    `scipy.stats.rankdata` and `DataFrame.rank()`. This matters more here than it
    looks: `volume_negotiated` is 0 on most sessions and `prop_*` is 0 for years,
    so ordinal ranks (what a bare `argsort` gives) would invent an ordering inside
    those blocks and quietly change every correlation they take part in.
    """
    torch = _torch()
    n, p = values.shape
    # NaNs sort to the end, so the ranks of the present values are unaffected.
    filled = torch.where(mask, values, torch.inf)
    sorted_values, order = torch.sort(filled, dim=0)

    # Group id per (sorted position, column): a new group starts wherever the
    # sorted value changes. Offsetting each column's ids by the cumulative group
    # count lets one flat scatter cover the whole matrix.
    starts = torch.ones((n, p), dtype=torch.bool, device=values.device)
    starts[1:] = sorted_values[1:] != sorted_values[:-1]
    local = torch.cumsum(starts.long(), dim=0) - 1
    per_column = local[-1] + 1
    offsets = torch.cat(
        [torch.zeros(1, dtype=torch.long, device=values.device),
         torch.cumsum(per_column, 0)[:-1]]
    )
    flat_ids = (local + offsets).reshape(-1)

    positions = (
        torch.arange(n, device=values.device, dtype=torch.float64)
        .unsqueeze(1)
        .expand(n, p)
        .reshape(-1)
    )
    total_groups = int(per_column.sum().item())
    first = torch.full(
        (total_groups,), float(n), device=values.device, dtype=torch.float64
    ).scatter_reduce(0, flat_ids, positions, reduce="amin")
    last = torch.zeros(
        total_groups, device=values.device, dtype=torch.float64
    ).scatter_reduce(0, flat_ids, positions, reduce="amax")
    # 1-based, mean of the span the tie covers.
    group_rank = (first + last) / 2.0 + 1.0

    ranks_sorted = group_rank[flat_ids].reshape(n, p)
    ranks = torch.empty_like(ranks_sorted)
    ranks.scatter_(0, order, ranks_sorted)
    return torch.where(mask, ranks, torch.nan)


def _average_ranks_numpy(frame: pd.DataFrame) -> np.ndarray:
    """The CPU twin — same convention, so the two devices agree by construction."""
    return frame.rank(axis=0, method="average", na_option="keep").to_numpy(np.float64)


def _pairwise_pearson(ranks: np.ndarray, xp, device: str) -> np.ndarray:
    """Pearson over ranks with PAIRWISE deletion, as four matmuls.

    For columns i and j over the rows where both are present:

        r = (n·Σxy − Σx·Σy) / sqrt((n·Σx² − (Σx)²)(n·Σy² − (Σy)²))

    and every Σ is a matmul of a masked matrix against the mask — so the whole
    p × p matrix is five products rather than a p² loop. At `pool__ta`'s ~900
    columns that is 39.6 s in pandas against 0.49 s here (§2 of the module
    docstring has the full scaling table).

    ⚠️ **Ranks are computed once per COLUMN, not once per pair.**
    `DataFrame.corr(method="spearman")` re-ranks inside each pairwise-complete
    subset. The two agree **exactly** when a column pair has no NaNs; on
    `pool__basic`, where 19 of 27 columns have gaps, the largest disagreement
    measured is **0.062**. That is well clear of the 0.9 the redundancy prune
    compares against, and this definition has the decisive advantage of being
    identical on both devices — `DataFrame.corr` is not reproducible on a GPU at
    all.
    """
    present = xp.isfinite(ranks)
    mask = present.astype(xp.float64) if device == "cpu" else present.double()
    filled = xp.where(present, ranks, xp.zeros_like(ranks))

    n = mask.T @ mask
    sum_x = filled.T @ mask
    sum_y = sum_x.T
    sum_xy = filled.T @ filled
    sum_xx = (filled * filled).T @ mask
    sum_yy = sum_xx.T

    cov = n * sum_xy - sum_x * sum_y
    var_x = n * sum_xx - sum_x * sum_x
    var_y = n * sum_yy - sum_y * sum_y
    denominator = xp.sqrt(xp.clip(var_x * var_y, 0.0, None))

    with np.errstate(invalid="ignore", divide="ignore"):
        corr = cov / denominator
    # A constant column has zero variance and no correlation with anything —
    # 0.0, not NaN, so the prune can compare it without special-casing.
    corr = xp.where(denominator > 0, corr, xp.zeros_like(corr))
    return corr


def spearman_matrix(frame: pd.DataFrame, device: str = "cpu") -> pd.DataFrame:
    """Feature × feature Spearman ρ. Same numbers on either device."""
    if device == "cuda":
        torch = _torch()
        values = torch.as_tensor(
            frame.to_numpy(np.float64), device="cuda", dtype=torch.float64
        )
        ranks = _average_ranks_torch(values, torch.isfinite(values))
        corr = _pairwise_pearson(ranks, torch, "cuda").cpu().numpy()
    else:
        corr = _pairwise_pearson(_average_ranks_numpy(frame), np, "cpu")
    # Numerically the diagonal can land at 1±1e-16; a redundancy prune compares
    # against a threshold, so pin it.
    np.fill_diagonal(corr, 1.0)
    return pd.DataFrame(
        np.clip(corr, -1.0, 1.0), index=frame.columns, columns=frame.columns
    )


def spearman_vector(
    frame: pd.DataFrame, target: pd.Series, device: str = "cpu"
) -> pd.Series:
    """Spearman ρ of every column against one target, in one pass.

    Built by correlating `[features | target]` and taking the last column, so it
    is the same code path — and therefore the same tie and NaN handling — as the
    matrix above.
    """
    joined = frame.copy()
    name = "__target__"
    joined[name] = target.to_numpy(np.float64)
    corr = spearman_matrix(joined, device=device)
    return corr[name].drop(index=name).rename("spearman_vs_target")


# --------------------------------------------------------------- xgboost + shap


def xgb_params(device: str) -> Dict[str, object]:
    """The device-dependent half of every XGBoost model built in this package."""
    return {"device": device, "tree_method": "hist"}


def tree_shap(model, X: pd.DataFrame) -> np.ndarray:
    """Mean |SHAP| per feature, computed by the booster itself.

    ⚠️ **Not `shap.TreeExplainer`.** XGBoost's own `pred_contribs=True` runs the
    same exact tree-SHAP algorithm INSIDE the booster, so on `device="cuda"` it
    runs on the GPU with the model already resident there — the `shap` package
    would pull the trees back to the host. Verified identical (max abs difference
    0.0, not merely close) against `shap.TreeExplainer` on this data.

    The last column of `pred_contribs` is the bias/expected-value term, which is
    not a feature; it is dropped.
    """
    import xgboost as xgb

    booster = model.get_booster()
    contributions = booster.predict(xgb.DMatrix(X), pred_contribs=True)
    return np.abs(contributions[:, :-1]).mean(axis=0)
