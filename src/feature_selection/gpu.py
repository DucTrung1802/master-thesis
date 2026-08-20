# src\feature_selection\gpu.py
"""The GPU paths, and an honest account of which parts have one.

## What actually runs on the GPU

| step | GPU | how |
|---|---|---|
| `xgb_gain`, `xgb_shap` | ✅ | `XGBRegressor(device="cuda", tree_method="hist")` |
| SHAP values | ✅ | the booster's own `pred_contribs=True`, not the `shap` package — GPU-side exact tree SHAP, verified equal to `shap.TreeExplainer` to **0.0**, and faster (0.06 s vs 0.10 s on 4,230 × 27) |
| `permutation` | ✅ | `permutation_importance_batched` — GPU-resident, one column written per draw, predicts batched. **49× (cpu) / 63× (cuda)** against sklearn's |
| stability, validation | ✅ | same models, same device |
| feature↔feature Spearman | ✅ | rank-transform + five matmuls in torch — **the step that dominates a wide pool**, though see §2 on how much of that win is the GPU |
| feature↔target Spearman | ✅ | `spearman_vector`, a vector not a p × p matrix — **28× on cuda at 8,748 columns**, and exact against the old path |
| `lasso` | ✅ | `gpu_rankers.lasso_cv` — FISTA on the same alpha path and the same purged folds. Same selected alpha, `|coef|` rank correlation **1.00000** |
| `mutual_info` | ✅ | `gpu_rankers.mutual_info` — the SAME Kraskov estimator, agreeing with sklearn to **8.9e-16**. ⚠️ Slower than sklearn's KDTree on this hardware; see §5 |

⚠️ **`device="cuda"` MEANS EVERY RANKER, WITH NO WIDTH GATE AND NO SILENT HOST
DISPATCH** (2026-08-10, second pass). It did not mean that for most of that day: a
measured threshold kept `lasso` on coordinate descent below 2,000 design columns and
`mutual_info` on sklearn always, which was defensible per-step and produced a run
that reported `device=cuda` while spending **67 % of its wall clock on the host** —
`lasso` alone was 52.8 % of it. A flag that means "the device, sometimes" cannot
answer "did this run on the GPU?", so it now means the device. `AUTO_CUDA_MIN_FEATURES`
and `GPU_LASSO_MIN_COLUMNS` survive as the measured crossovers that `device="auto"`
and `device="cpu"` still trade on. §4 has what the conversion moved; **§5 has what it
costs**, because on this hardware the answer is not free.

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
trade. ⚠️ **`permutation_importance_batched` is the exception and does hand it a
resident tensor**, because there the copy happens once per PERMUTATION rather than
once per fit.

## ⚠️ 4. THE 2026-08-10 CONVERSION — what moved, and the two things that did not

Before this date **no run in the archive had ever used the GPU**: all 22 recorded
`device="cpu"`, because that was the default in `run.py` and in the Dagster asset.
Forcing `device="cuda"` was then measured on `basic+economy_vietnam` and made the
whole run **6.8× SLOWER** (154 s → 1,046 s). So "turn the GPU on" was never a
one-line change, and the conversion is three separate pieces of work:

| | before | after | why |
|---|---|---|---|
| `permutation` | 85 s cpu / **986 s cuda** | **1.9 s** | sklearn copied the whole DataFrame per draw and round-tripped to the host; this writes one column and batches the predicts |
| feature↔target Spearman | 18.2 s cpu / **31.9 s cuda** @ 8,748 cols | **1.1 s** | it was building a 584 MB p × p matrix to keep one column |
| `lasso` | **215 min** on `usa`, cpu-only | GPU above 2,000 cols | FISTA on the same objective and folds |

## ⚠️ 5. WHAT `device="cuda"` COSTS, NOW THAT IT MEANS EVERY RANKER

The first pass of the conversion left two width-gated dispatches to the host. They
were each defensible and together they produced a run reporting `device=cuda` that
spent **67.3 %** of its wall clock on the CPU. Removed. Measured on `japan` (205
channels, 1,230 design columns), one selection pass, before and after:

| step | width-gated | every ranker on cuda | |
|---|---|---|---|
| **`lasso`** | **155.4 s** cpu | **0.7 s** cuda | **231.9×** |
| `mutual_info` | 41.8 s cpu | 22.4 s cuda | 1.9× |
| everything else | 96.1 s cuda | 65.7 s cuda | 1.5× |
| **pass** | **294.4 s** | **89.7 s** | **3.3×** |
| **share on CPU** | **67.3 %** | **0.9 %** | |
| whole run, 1 + 20 null draws | 53.0 min | **28.9 min** | 1.8× |

⚠️ **`lasso` at 232× is the headline and it is a lesson about benchmarks.** §4's
synthetic i.i.d. table put FISTA at 1.2-3.1× and said so was an extrapolation to real
data. Real design columns are windowed macro series and are massively collinear;
coordinate descent degrades badly on them (**9.0 s synthetic at 2,000 columns vs
155.4 s real at 1,230**) and FISTA does not, because its cost is a fixed matmul per
iteration regardless of conditioning.

⚠️ **`mutual_info` is the honest counterweight: it is SLOWER, and it runs anyway.**
Its compute genuinely loses to a KDTree (§4's warm-pool table). The 1.9× above is a
COLD process, where sklearn pays ~35 s of `loky` spawn; within a 21-pass run the pool
warms and sklearn's amortised cost is lower — GPU `mutual_info` costs roughly
**+5 minutes per country run**, paid out of the ~10 minutes `lasso` saves. It is used
because `device` must mean the device; `device="cpu"` remains the fast path for it.

⚠️ **`window design` is the one step still on the host** — 0.8 s, **0.9 %**. It is the
strided reduction that BUILDS the design matrix, i.e. data preparation rather than a
ranker. GPU float64 reductions sum in a different order, so `nanmean`/`nanstd`/`slope`
would likely stop matching the host bit-for-bit, and that difference would enter the
matrix every ranker reads — including the Spearman steps that agree across devices to
exactly **0.0** today (§1). Not worth 0.9 %.

⚠️ **The conversion changed no ranker's DEFINITION.** `spearman` and `permutation`
were already ours and are unchanged; `lasso` is the same objective under a different
optimiser (see `gpu_rankers`, and note the `p > n` non-uniqueness caveat there);
`mutual_info` still runs sklearn's own code. What changed is where the arithmetic
happens — except in `permutation`, where the repeats are now independent draws
rather than sklearn's chained ones, which `test_gpu_permutation.py` justifies.
"""

from typing import Callable, Dict, Optional, Tuple

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

# ⚠️ Below this many DESIGN columns the GPU LASSO LOSES, so `gpu_rankers.lasso_cv`
# is not used under it. Measured at n=4,211 against `LassoCV`, on i.i.d. Gaussian
# columns and with the selected alpha and `|coef|` ranking IDENTICAL at every width:
#
#     p        sklearn      gpu_rankers/cuda     ratio
#     700         0.6 s              ~5 s        0.1x   <- sklearn wins
#     2,000       9.0 s               7.4 s      1.2x   <- crossover
#     5,000      42.0 s              21.2 s      2.0x
#
# Coordinate descent with warm starts is very hard to beat while the active set is
# small; FISTA pays a full 4,211 x p matmul per iteration whatever the sparsity, and
# only wins once that matmul is what a GPU is for. The design matrix is
# `channels x 6`, so this threshold is crossed at ~333 channels — `basic+economy_usa`
# (8,748 columns) is well past it and every other country pool is well under.
GPU_LASSO_MIN_COLUMNS = 2000


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


# ⚠️ **THE RANK STEP NEEDS ~10× ITS INPUT IN VRAM, AND THAT IS WHAT KILLED THE FIRST
# UNIVERSE-PANEL RUN** (Kaggle T4, 2026-08-17). Counted off `_average_ranks_torch`'s own
# body, every one an `n × p` allocation live at the same time: `values`, `filled`,
# `sorted_values`, `order` (int64), `positions`, `local` (int64), `flat_ids` (int64),
# `ranks_sorted`, `ranks` — nine, plus two boolean masks. At 1.247 M rows × ~536 float64
# columns the input alone is 4.98 GiB, so the working set is ~50 GiB against a T4's
# 14.56, and it OOMed inside `torch.sort` with 10.70 GiB already in use.
#
# ⚠️ **The multiple is a COUNT of the tensors above, not a fitted fudge factor** — if
# that body gains an `n × p` temporary, raise it. It is deliberately not measured by
# probing `torch.cuda.memory_allocated`: a budget that depends on when the allocator
# last released is a budget that changes between two runs of the same code.
RANK_WORKING_MULTIPLE = 10

# How much of the FREE VRAM one block may claim. Not all of it: the caller's own frame,
# the target ranks and the correlation temporaries live alongside, and an allocator that
# has just freed a block does not always hand the same address back (fragmentation).
RANK_BUDGET_FRACTION = 0.35

# The floor, for a card whose free memory cannot be read. 512 MiB of working set is the
# 4 GB RTX 3050 with room to spare, and every archived run is far below one block anyway.
RANK_FALLBACK_BUDGET = 512 * 1024**2


def rank_block_columns(rows: int, columns: int, device: str) -> int:
    """How many columns may be ranked at once. `columns` (i.e. one block) unless VRAM says no.

    ⚠️ **A NARROW OR SHORT PANEL TAKES EXACTLY ONE BLOCK, so every archived run's code
    path is unchanged** — VCB's 4,266 rows put the whole design in one block on any card,
    and a single block calls the same dense body that has always run. Chunking only
    engages where the dense form could not have run at all.
    """
    if device != "cuda" or columns <= 1:
        return columns
    per_column = max(1, rows * 8 * RANK_WORKING_MULTIPLE)
    try:
        free = _torch().cuda.mem_get_info()[0]
        budget = int(free * RANK_BUDGET_FRACTION)
    except Exception:  # noqa: BLE001 — an unreadable card is not a failed ranking
        budget = RANK_FALLBACK_BUDGET
    return int(max(1, min(columns, budget // per_column)))


def _average_ranks_torch(values, mask, block_columns: Optional[int] = None):
    """Average ranks per column, NaNs excluded, in blocks of `block_columns`.

    ⚠️ **BLOCKING IS EXACT, NOT AN APPROXIMATION.** A rank is computed within its own
    column and reads nothing from any other, so a block boundary cannot move a number —
    which is the whole reason this is the answer here and `float32` is not (`float32`
    changed a measured `ic_mean` by 52 %, TODO P0-3). `test_gpu_spearman.py` asserts
    blocked and dense agree at **0.0**, not merely closely.
    """
    if block_columns is not None and 0 < block_columns < values.shape[1]:
        torch = _torch()
        out = torch.empty_like(values)
        for start in range(0, values.shape[1], block_columns):
            stop = min(start + block_columns, values.shape[1])
            out[:, start:stop] = _average_ranks_dense_torch(
                values[:, start:stop].contiguous(), mask[:, start:stop].contiguous()
            )
        return out
    return _average_ranks_dense_torch(values, mask)


def _average_ranks_dense_torch(values, mask):
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
        # ⚠️ The RANKING is blocked; `_pairwise_pearson` is not, because it needs every
        # column against every other and a blocked form would be a block-pair loop, not
        # a slice. That is fine at the width this function is called on — the channel
        # correlation matrix, ~100 channels, not the ~536-column design — and it is why
        # `spearman_vector` (which IS called on the design) got the full treatment.
        ranks = _average_ranks_torch(
            values,
            torch.isfinite(values),
            rank_block_columns(*frame.shape, "cuda"),
        )
        del values
        corr = _pairwise_pearson(ranks, torch, "cuda").cpu().numpy()
    else:
        corr = _pairwise_pearson(_average_ranks_numpy(frame), np, "cpu")
    # Numerically the diagonal can land at 1±1e-16; a redundancy prune compares
    # against a threshold, so pin it.
    np.fill_diagonal(corr, 1.0)
    return pd.DataFrame(
        np.clip(corr, -1.0, 1.0), index=frame.columns, columns=frame.columns
    )


def _pearson_against_last(ranks: np.ndarray, xp, device: str):
    """The LAST column of `_pairwise_pearson`, without forming the other p².

    Same five sums and the same pairwise deletion — every one specialised to
    `j = last`, so each `p × p` product collapses to a `p`-vector. Algebraically a
    strict subset of `_pairwise_pearson`, which is why the two agree to **0.0**
    rather than merely closely (asserted in `test_gpu_spearman.py`).
    """
    features, tgt = ranks[:, :-1], ranks[:, -1:]
    present = xp.isfinite(features)
    tgt_present = xp.isfinite(tgt)

    mask = present.astype(xp.float64) if device == "cpu" else present.double()
    tmask = tgt_present.astype(xp.float64) if device == "cpu" else tgt_present.double()
    x = xp.where(present, features, xp.zeros_like(features))
    y = xp.where(tgt_present, tgt, xp.zeros_like(tgt))

    # Every term is masked by BOTH columns, which is the pairwise deletion.
    both = mask * tmask                      # n x p
    n = both.sum(0)
    sum_x = (x * tmask).sum(0)
    sum_y = (y * mask).sum(0)
    sum_xy = (x * y).sum(0)
    sum_xx = (x * x * tmask).sum(0)
    sum_yy = (y * y * mask).sum(0)

    cov = n * sum_xy - sum_x * sum_y
    var_x = n * sum_xx - sum_x * sum_x
    var_y = n * sum_yy - sum_y * sum_y
    denominator = xp.sqrt(xp.clip(var_x * var_y, 0.0, None))
    with np.errstate(invalid="ignore", divide="ignore"):
        corr = cov / denominator
    return xp.where(denominator > 0, corr, xp.zeros_like(corr))


def spearman_vector(
    frame: pd.DataFrame, target: pd.Series, device: str = "cpu"
) -> pd.Series:
    """Spearman ρ of every column against one target, in one pass.

    Ranks are computed by the same helpers as `spearman_matrix`, so the tie and NaN
    conventions are shared by construction and the two functions cannot drift.

    ⚠️ **THIS USED TO CALL `spearman_matrix` AND THROW AWAY ALL BUT ONE COLUMN**, and
    on a wide DESIGN matrix that is not a tidy shortcut but the dominant cost of the
    step. `selector.run` calls this on the design matrix, which is `channels × 6`
    stats: for `basic+economy_usa` that is **8,748 columns**, so the discarded matrix
    was 8,748² float64 = **584 MB**, with ~10 live temporaries inside
    `_pairwise_pearson` on a card that has 3.3 GB free. Measured on this machine,
    4,211 rows, 15 % NaN, a constant column and a 1,000-row tie block:

    | width | cpu before | cpu after | cuda before | cuda after |
    |---|---|---|---|---|
    | 300 | 0.14 s | 0.14 s | 0.57 s | **0.02 s** — 23× |
    | 2,000 | 1.48 s | 1.07 s | 1.49 s | **0.16 s** — 9× |
    | **8,748** (`usa`) | 18.18 s | 5.19 s | **31.93 s** | **1.13 s** — 28× |

    ⚠️ **Note the `before` column for cuda: the GPU was SLOWER than the host at every
    width**, because a p² matmul in float64 is the one thing a consumer GeForce is bad
    at (§2). The vector form is what makes this step a GPU win at all. It also takes
    the peak working set from ~5.8 GB of p² temporaries to the 295 MB of the ranks,
    which is what kept a 4 GB card one bad allocation from an OOM at a width the
    country sweep reaches routinely — so this is a correctness fix and not only a
    speed one. **`max abs diff` against the old path is 0.0 at every width above, on
    both devices.**
    """
    if device == "cuda":
        corr = _spearman_vector_cuda(frame, target)
    else:
        joined = frame.copy()
        joined["__target__"] = target.to_numpy(np.float64)
        corr = _pearson_against_last(_average_ranks_numpy(joined), np, "cpu")

    return pd.Series(
        np.clip(corr, -1.0, 1.0), index=frame.columns, name="spearman_vs_target"
    )


def _spearman_vector_cuda(frame: pd.DataFrame, target: pd.Series) -> np.ndarray:
    """The cuda path, one COLUMN BLOCK at a time — host to device, ρ, discard.

    ⚠️ **THIS IS WHY THE FIRST UNIVERSE-PANEL RUN DIED, AND CHUNKING THE RANK HELPER
    ALONE WOULD NOT HAVE SAVED IT.** The old body moved the whole design to the device in
    one `as_tensor` (4.98 GiB at 1.247 M × 536), ranked it (~10× that, see
    `RANK_WORKING_MULTIPLE`), and then handed the full `n × p` rank matrix to
    `_pearson_against_last`, which builds `present`, `mask`, `x`, `both` and one product
    temporary — five more `n × p` allocations. Every stage is `O(n × p)`, so every stage
    had to become `O(n × block)`, not just the loudest one. Measured on a Kaggle T4:
    OOM at `torch.sort` asking 4.98 GiB with 10.70 GiB of 14.56 already in use.

    ⚠️ **EXACT, and it has to be**: each ρ is computed from its own column and the target
    alone — `_pearson_against_last` specialises every sum to `j = last` — so a block
    boundary changes no number. `float32` would also have fitted and is forbidden for a
    quotable run (TODO P0-3: 52 % relative change in `ic_mean`).

    ⚠️ **THE TARGET IS RANKED ONCE**, outside the loop. Re-ranking it per block would be
    identical arithmetic and pure waste — and, less obviously, it is the one column whose
    ranks must not be blocked, because the pairwise deletion in `_pearson_against_last`
    reads it against every feature.
    """
    torch = _torch()
    rows, columns = frame.shape
    block = rank_block_columns(rows, columns, "cuda")

    y = torch.as_tensor(
        np.ascontiguousarray(target.to_numpy(np.float64)).reshape(-1, 1),
        device="cuda", dtype=torch.float64,
    )
    y_ranks = _average_ranks_torch(y, torch.isfinite(y))
    del y

    parts = []
    for start in range(0, columns, block):
        stop = min(start + block, columns)
        values = torch.as_tensor(
            np.ascontiguousarray(frame.iloc[:, start:stop].to_numpy(np.float64)),
            device="cuda", dtype=torch.float64,
        )
        ranks = _average_ranks_torch(values, torch.isfinite(values))
        del values
        stacked = torch.cat([ranks, y_ranks], dim=1)
        del ranks
        parts.append(_pearson_against_last(stacked, torch, "cuda").cpu().numpy())
        del stacked
        # ⚠️ Between blocks, not inside: the allocator caches freed blocks and a run of
        # differently-shaped allocations fragments it. Once per block is ~1 ms against a
        # sort of millions of rows; once per tensor would be a real cost.
        if block < columns:
            torch.cuda.empty_cache()

    return np.concatenate(parts) if len(parts) > 1 else parts[0]


# --------------------------------------------------------------- xgboost + shap


def xgb_params(device: str) -> Dict[str, object]:
    """The device-dependent half of every XGBoost model built in this package."""
    return {"device": device, "tree_method": "hist"}


def _as_device_matrix(frame: pd.DataFrame, device: str):
    """`frame` as the array type this device predicts from, float32, contiguous."""
    values = np.ascontiguousarray(frame.to_numpy(np.float32))
    if device != "cuda":
        return values
    torch = _torch()
    return torch.as_tensor(values, device="cuda")


def permutation_batch_size(n_rows: int, n_cols: int, device: str) -> int:
    """How many permuted copies fit in one predict, from the FREE memory now.

    ⚠️ Sized against `mem_get_info` rather than a constant, because this card has
    4 GB total and whatever else is resident (the fitted booster, the design matrix,
    the rank buffers) is not knowable from here. A quarter of free VRAM, floored at
    one copy so the function degrades to the unbatched loop instead of raising.
    """
    per_copy = n_rows * n_cols * 4  # float32
    if device == "cuda":
        torch = _torch()
        free, _total = torch.cuda.mem_get_info()
        budget = int(free * 0.25)
    else:
        budget = 512 * 1024**2
    return int(max(1, min(64, budget // max(per_copy, 1))))


def permutation_importance_batched(
    model,
    X: pd.DataFrame,
    score: Callable[[np.ndarray], float],
    *,
    n_repeats: int = 10,
    random_state: int = 0,
    device: str = "cpu",
) -> np.ndarray:
    """Permutation importance that never leaves the device and never copies `X`.

    Returns `importances_mean`, one value per column of `X`, on the same definition
    `sklearn.inspection.permutation_importance` uses: for each column and repeat,
    permute that column and take `baseline - permuted_score`, then average over
    repeats. `score` takes a prediction vector and returns the number the selection
    is judged on — the caller closes it over the right `y`, so a cross-sectional run
    keeps its per-date IC.

    ## ⚠️ Why this replaced sklearn's, which was ALREADY the GPU's biggest loss

    Measured on `basic+economy_vietnam` (113 channels → 678 design columns), one
    whole selection:

    | step | cpu | cuda (sklearn loop) |
    |---|---|---|
    | permutation | 85.3 s | **986.3 s** |
    | whole run | 154 s | **1,046 s** |

    Forcing the GPU made the run **6.8× slower**, and permutation was all of it.
    Two causes, and the GPU is not really either:

    1. **sklearn copies the whole `X` per permutation.** `_calculate_permutation_scores`
       holds an `X.copy()` and rewrites one column of a **pandas DataFrame** — an
       `O(n·p)` copy plus pandas indexing overhead to change `O(n)` values. That cost
       scales with WIDTH, which is why the archived `usa` run (8,747 design columns)
       spent **204 minutes** here. This function writes one column and restores it.
    2. **One host→device round trip per permutation.** With `device="cuda"` sklearn
       hands XGBoost a host array 6,780 times. Measured on this card: `predict` on a
       host array is 14.2 ms and `inplace_predict` on a resident tensor is 11.9 ms —
       so the trip is real but small. **Batching is the larger win**: stacking copies
       into one `inplace_predict` took a 40-feature case from 9.6 to 2.2 ms per
       permutation, 8× against sklearn, and the margin grows with `p`.

    ⚠️ **ONE IMPLEMENTATION FOR BOTH DEVICES, on purpose.** `xp` is numpy or torch and
    nothing else differs, so `cpu` and `cuda` cannot drift on this step the way they
    legitimately do on the tree methods (§1). It also means the host gets the same
    algorithmic win, which is most of it.

    ⚠️ **ONE DELIBERATE DIFFERENCE FROM SKLEARN.** sklearn re-shuffles the ALREADY
    PERMUTED column on each repeat, so its draws are a chained composition; this
    permutes the ORIGINAL column every time, so the repeats are independent draws.
    Both are uniform over permutations and have the same expectation — the mean over
    repeats estimates the same quantity — but chaining creates a sequential dependency
    that cannot be batched. The independent draws are also the better estimator. Agreement
    with sklearn is therefore statistical, not bit-exact, and `test_gpu_permutation.py`
    asserts it against the Monte-Carlo spread rather than against a tolerance.
    """
    columns = list(X.columns)
    n_rows, n_cols = X.shape
    booster = model.get_booster()
    if device == "cuda":
        booster.set_param({"device": "cuda"})

    xp = _torch() if device == "cuda" else np
    base = _as_device_matrix(X, device)

    def predict(matrix) -> np.ndarray:
        # ⚠️ `inplace_predict` RETURNS WHAT IT WAS GIVEN, ON THAT DEVICE. Handed a
        # `torch.cuda` tensor it returns a **cupy** array, which refuses implicit
        # `np.asarray` conversion ("Please use .get()") — so the three array types
        # are handled by name rather than hoped through a single cast.
        out = booster.inplace_predict(matrix)
        if hasattr(out, "get"):          # cupy
            out = out.get()
        elif hasattr(out, "cpu"):        # torch
            out = out.cpu().numpy()
        return np.asarray(out, dtype=np.float64)

    baseline = score(predict(base))

    batch = permutation_batch_size(n_rows, n_cols, device)
    # The work list is every (column, repeat) pair; batching across BOTH means a
    # narrow pool with few repeats still fills a batch.
    jobs = [(j, r) for j in range(n_cols) for r in range(n_repeats)]

    if device == "cuda":
        torch = xp
        stacked = base.repeat(batch, 1).contiguous()
        generator = torch.Generator(device="cuda")
        generator.manual_seed(int(random_state))
    else:
        stacked = np.tile(base, (batch, 1))
        generator = np.random.default_rng(random_state)

    drops = np.zeros((n_cols, n_repeats), dtype=np.float64)
    for start in range(0, len(jobs), batch):
        chunk = jobs[start : start + batch]
        for slot, (j, _r) in enumerate(chunk):
            lo = slot * n_rows
            if device == "cuda":
                order = torch.randperm(n_rows, device="cuda", generator=generator)
            else:
                order = generator.permutation(n_rows)
            stacked[lo : lo + n_rows, j] = base[order, j]

        predictions = predict(stacked[: len(chunk) * n_rows])

        for slot, (j, r) in enumerate(chunk):
            lo = slot * n_rows
            drops[j, r] = baseline - score(predictions[lo : lo + n_rows])
            # ⚠️ RESTORE BEFORE THE NEXT BATCH. The slot is reused by a different
            # column, and a left-over permutation there would silently score every
            # later job against a corrupted matrix — the failure would look like
            # noise, not like a bug.
            stacked[lo : lo + n_rows, j] = base[:, j]

    return drops.mean(axis=1)


#: Target bytes for one `pred_contribs` block. The allocation is
#: `rows x (cols + 1) x 4`, so this caps the block rather than the whole design.
SHAP_BLOCK_BYTES = 512 * 1024 * 1024


def shap_row_chunk(n_columns: int, block_bytes: int = SHAP_BLOCK_BYTES) -> int:
    """Rows per `pred_contribs` call so one block stays near `block_bytes`."""
    per_row = max(1, (n_columns + 1) * 4)
    return int(max(10_000, min(1_000_000, block_bytes // per_row)))


def tree_shap(model, X: pd.DataFrame, row_chunk: Optional[int] = None) -> np.ndarray:
    """Mean |SHAP| per feature, computed by the booster itself, in ROW BLOCKS.

    ⚠️ **Not `shap.TreeExplainer`.** XGBoost's own `pred_contribs=True` runs the
    same exact tree-SHAP algorithm INSIDE the booster, so on `device="cuda"` it
    runs on the GPU with the model already resident there — the `shap` package
    would pull the trees back to the host. Verified identical (max abs difference
    0.0, not merely close) against `shap.TreeExplainer` on this data.

    The last column of `pred_contribs` is the bias/expected-value term, which is
    not a feature; it is dropped.

    ⚠️ **THE BLOCKING IS `VRM-1`'s FIX AND IT IS EXACT, NOT AN APPROXIMATION.**
    `pred_contribs=True` materialises `(n_rows, n_columns + 1)` floats in ONE
    allocation, linear in the width — which is what killed a 140-channel panel
    selection on a 14.6 GiB T4 (*free 3.00 GB, requested 3.15 GB*) while host RAM
    peaked at 24.5 GB and survived. A SHAP contribution is per-row independent and
    the statistic wanted here is a column MEAN, so summing `|contributions|` over
    row blocks and dividing by `n` at the end returns the same number the single
    allocation would have. Accumulation is float64 so the block count cannot move
    the result.

    ⚠️ `selector._tick` cannot see this allocation — it reports torch's VRAM while
    XGBoost allocates through its own CUDA allocator, so the ceiling was invisible
    until a run died at it.
    """
    import xgboost as xgb

    booster = model.get_booster()
    n_rows, n_columns = X.shape
    if n_rows == 0:
        return np.zeros(n_columns, dtype=float)
    chunk = row_chunk or shap_row_chunk(n_columns)

    total = np.zeros(n_columns, dtype=np.float64)
    for lo in range(0, n_rows, chunk):
        block = X.iloc[lo : lo + chunk]
        contributions = booster.predict(xgb.DMatrix(block), pred_contribs=True)
        total += np.abs(contributions[:, :-1]).sum(axis=0, dtype=np.float64)
    return total / n_rows
