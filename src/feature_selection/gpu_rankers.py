# src\feature_selection\gpu_rankers.py
"""The two rankers whose sklearn implementations have no GPU path, reimplemented.

`gpu.py` is device plumbing plus primitives that were always ours (the rank
correlations, the batched permutation loop). **This module is different in kind: it
reimplements two estimators that sklearn defines**, so every function here owes a
verification against the thing it replaces, and each has one in
`test_gpu_rankers.py`.

| ranker | sklearn | here | agreement |
|---|---|---|---|
| `lasso` | `LassoCV` (coordinate descent) | FISTA on the same alpha path and the same purged folds | **same objective, different optimiser** — coefficients agree to ~1e-6, selected alpha identical on every case tested |
| `mutual_info` | `mutual_info_regression` (Kraskov kNN) | the SAME Kraskov estimator, distances on the GPU | **same estimator** — agrees to ~1e-12 when the added noise is seeded identically |

## ⚠️ Why this was worth doing, and what it cost

The archived `basic+economy_usa` run (1,458 channels → 8,747 design columns) spent
**215 minutes in `lasso`** out of 428 total. `LassoCV` is CPU-only in sklearn and
cuML — which has a GPU LASSO — ships no Windows wheel, so the choice was to
reimplement or to keep the floor.

⚠️ **THE OPTIMISER IS DIFFERENT AND THAT IS THE HONEST CAVEAT.** Coordinate descent
and FISTA minimise the *same* convex objective

    (1 / 2n) · ||y − Xw||²  +  alpha · ||w||₁

so they converge to the same minimiser wherever it is unique. With `p > n` — which
is every wide pool here — the LASSO minimiser need **not** be unique, and two
optimisers can legitimately return different coefficient vectors with identical
objective values. The tests therefore assert on the OBJECTIVE and on the selected
alpha, and check the coefficients as a ranking rather than element-wise. What this
ranker contributes to the ensemble is `|coef|` as a rank, and that is what is
verified.

## ⚠️ float32 vs float64

The matmuls run in **float64**. A consumer GeForce runs double precision at 1/32 of
its single-precision rate, so this is deliberately giving up most of the card's
throughput — and it is still worth it, because at 8,747 columns the step went from
215 minutes to seconds either way, and float32 changed which coefficients landed
exactly on zero. Precision that decides a feature's inclusion is not a knob to trade
for speed we do not need.
"""

from typing import List, Optional, Sequence, Tuple

import numpy as np

from feature_selection import gpu


def _xp(device: str):
    """The array module for this device, and a `to`/`from` pair for host arrays."""
    if device == "cuda":
        torch = gpu._torch()

        def to_device(array):
            return torch.as_tensor(
                np.ascontiguousarray(array, dtype=np.float64),
                device="cuda",
                dtype=torch.float64,
            )

        def to_host(tensor):
            return tensor.detach().cpu().numpy()

        return torch, to_device, to_host

    return np, (lambda a: np.ascontiguousarray(a, dtype=np.float64)), (lambda a: a)


# ═══════════════════════════════════════════════════════════════════ LASSO


def _lipschitz(X, xp, device: str, iterations: int = 60) -> float:
    """`||X||₂² / n` by power iteration — the FISTA step size.

    ⚠️ Power iteration, not `svdvals`. The exact spectral norm of a 4,211 × 8,747
    matrix is a full SVD; the gradient step only needs an UPPER bound to be stable,
    and power iteration converges to it from below, so the result is inflated by
    1.01 to stay on the safe side. An underestimate makes FISTA diverge, which is
    the one failure mode that would be silent in a ranking.
    """
    n, p = X.shape
    # ⚠️ **SEEDED ON BOTH DEVICES.** The start vector was unseeded on CUDA until
    # 2026-08-10, and the consequence was not "a slightly different norm": the
    # Lipschitz constant is the FISTA STEP SIZE, so a different start vector gives
    # different iterates, different coefficients and — at the CV stage — potentially a
    # different selected alpha. `lasso` is a scored ranker, so that is a
    # non-reproducible run, and `test_gpu_determinism.py::test_lasso_is_bit_identical`
    # caught it. The CPU path was seeded from the start; only CUDA was not.
    if device == "cuda":
        generator = xp.Generator(device="cuda")
        generator.manual_seed(0)
        v = xp.randn(p, dtype=xp.float64, device="cuda", generator=generator)
    else:
        v = np.random.default_rng(0).standard_normal(p)
    v = v / (xp.linalg.norm(v) + 1e-30)
    value = 0.0
    for _ in range(iterations):
        w = X.T @ (X @ v)
        norm = xp.linalg.norm(w)
        if float(norm) <= 0.0:
            return 1.0
        v = w / norm
        value = float(norm)
    return 1.01 * value / n


def _soft_threshold(w, amount, xp):
    """The L1 proximal operator: shrink toward zero, clamp the crossing at zero."""
    return xp.sign(w) * xp.clip(xp.abs(w) - amount, 0.0, None)


def _fista_path(
    X, y, alphas: Sequence[float], xp, device: str, max_iter: int, tol: float
) -> Tuple[List, bool]:
    """Coefficients for every alpha, warm-started down the path.

    Returns `(coefs, converged)`. The path runs from the LARGEST alpha down, so each
    solve starts from the previous (sparser) solution — the standard homotopy, and
    the reason 100 alphas cost far less than 100 independent fits.
    """
    n, p = X.shape
    step = _lipschitz(X, xp, device)
    if device == "cuda":
        w = xp.zeros(p, dtype=xp.float64, device="cuda")
    else:
        w = np.zeros(p, dtype=np.float64)

    coefs, converged = [], True
    for alpha in alphas:
        z = w.copy() if device != "cuda" else w.clone()
        t = 1.0
        hit_limit = True
        for _ in range(max_iter):
            gradient = X.T @ (X @ z - y) / n
            w_next = _soft_threshold(z - gradient / step, alpha / step, xp)
            t_next = 0.5 * (1.0 + np.sqrt(1.0 + 4.0 * t * t))
            z = w_next + ((t - 1.0) / t_next) * (w_next - w)
            shift = float(xp.max(xp.abs(w_next - w)))
            w, t = w_next, t_next
            if shift < tol:
                hit_limit = False
                break
        converged = converged and not hit_limit
        coefs.append(w.copy() if device != "cuda" else w.clone())
    return coefs, converged


def lasso_cv(
    X: np.ndarray,
    y: np.ndarray,
    folds: Sequence[Tuple[np.ndarray, np.ndarray]],
    *,
    n_alphas: int = 100,
    eps: float = 1e-3,
    max_iter: int = 5000,
    tol: float = 1e-4,
    device: str = "cpu",
) -> Tuple[np.ndarray, float, bool]:
    """`LassoCV` on the given folds: `(coef, alpha, converged)`.

    Mirrors sklearn's procedure step for step, which is what makes the comparison in
    `test_gpu_rankers.py` meaningful rather than a coincidence:

    1. `alpha_max = max|Xᵀ(y − ȳ)| / n` over the FULL data, then `n_alphas`
       log-spaced values down to `eps · alpha_max`;
    2. for each fold, fit the whole path on train and score MSE on test;
    3. pick the alpha with the lowest MEAN test MSE across folds;
    4. refit on the full data at that alpha.

    ⚠️ **The folds are the caller's PURGED walk-forward splits, never `KFold`.**
    `LassoCV(cv=5)` defaults to plain K-fold, which with a 5-day forward label puts
    day `t+1` in train and day `t` in validation — the penalty would then be tuned
    against a leaked score. This signature takes `folds` positionally and has no
    default for exactly that reason.

    ⚠️ **`fit_intercept=True` is emulated by CENTERING PER FOLD**, as sklearn does.
    `X` arrives standardised over the whole development sample, so a fold's train
    slice is not itself mean-zero, and centering on the full sample instead would
    leak the fold's own mean into its fit.

    ⚠️ **`tol=1e-4` AND `n_alphas=100` ARE BOTH LOAD-BEARING, in opposite directions.**
    Measured at `n=4,211, p=700` against `LassoCV`:

    | tol | n_alphas | seconds | selected alpha | `|coef|` rank corr | nonzeros |
    |---|---|---|---|---|---|
    | 1e-7 | 100 | 88.9 | **exact** | **1.00000** | 11 = sklearn |
    | 1e-5 | 100 | 37.7 | **exact** | **1.00000** | 11 |
    | **1e-4** | **100** | **5.5** | **exact** | **1.00000** | **11** |
    | 1e-4 | 30 | 2.4 | **0.974× — WRONG** | 0.95811 | 12 |

    Tightening the tolerance past 1e-4 buys nothing but time: the selected alpha and
    the ranking are already identical to sklearn's. **Shortening the alpha path is
    the false economy** — 30 alphas is twice as fast again and lands on a different
    penalty, which changes the coefficient vector this ranker contributes. Cheap in
    seconds, wrong in answer.
    """
    xp, to_device, to_host = _xp(device)
    Xd, yd = to_device(X), to_device(y)
    n = Xd.shape[0]

    y_centered = yd - yd.mean()
    alpha_max = float(xp.max(xp.abs(Xd.T @ y_centered))) / n
    if alpha_max <= 0.0:
        return np.zeros(X.shape[1]), 0.0, True
    alphas = np.logspace(np.log10(alpha_max), np.log10(alpha_max * eps), n_alphas)

    def rows(index: np.ndarray):
        """Row selector this device can index with."""
        if device != "cuda":
            return index
        return gpu._torch().as_tensor(
            np.ascontiguousarray(index, dtype=np.int64), device="cuda"
        )

    mse = np.zeros((len(folds), n_alphas))
    converged = True
    for f, (train_idx, test_idx) in enumerate(folds):
        tr, te = rows(train_idx), rows(test_idx)
        Xtr, ytr = Xd[tr], yd[tr]
        Xte, yte = Xd[te], yd[te]
        x_mean, y_mean = Xtr.mean(0), ytr.mean()
        coefs, ok = _fista_path(
            Xtr - x_mean, ytr - y_mean, alphas, xp, device, max_iter, tol
        )
        converged = converged and ok
        for a, coef in enumerate(coefs):
            residual = yte - ((Xte - x_mean) @ coef + y_mean)
            mse[f, a] = float(xp.mean(residual * residual))

    best = int(np.argmin(mse.mean(axis=0)))
    x_mean, y_mean = Xd.mean(0), yd.mean()
    coefs, ok = _fista_path(
        Xd - x_mean, yd - y_mean, alphas[: best + 1], xp, device, max_iter, tol
    )
    return to_host(coefs[-1]), float(alphas[best]), bool(converged and ok)


# ═══════════════════════════════════════════════════════ MUTUAL INFORMATION


def _digamma(values, xp, device: str):
    """ψ over an ARRAY on this device. Scalars go through scipy on the host."""
    if device == "cuda":
        return xp.digamma(values)
    from scipy.special import digamma

    return digamma(values)


def mutual_info(
    X: np.ndarray,
    y: np.ndarray,
    *,
    n_neighbors: int = 3,
    random_state: int = 0,
    device: str = "cpu",
    column_batch: Optional[int] = None,
) -> np.ndarray:
    """Kraskov mutual information of every column against `y`, on the GPU.

    ⚠️ **THE SAME ESTIMATOR AS sklearn, NOT A HISTOGRAM SUBSTITUTE.** `gpu.py` used
    to record that a GPU `mutual_info` "would be a *different* estimator, and quietly
    swapping the definition to win a benchmark is not a speedup". That objection is
    about substituting a binned estimator; it does not apply to running Kraskov's own
    kNN formula on a GPU, which is what this is. Method 1 of Kraskov et al. (2004),
    as `sklearn.feature_selection._mutual_info._compute_mi_cc` implements it:

        I(x, y) = ψ(k) + ψ(n) − ⟨ψ(nₓ+1) + ψ(n_y+1)⟩

    where the neighbourhood radius is the Chebyshev distance to the k-th neighbour in
    the JOINT space, and `nₓ` / `n_y` count strictly-inside neighbours per marginal.

    ⚠️ **THE PREPROCESSING IS PART OF THE ESTIMATOR, NOT A DETAIL.** sklearn divides
    every continuous column and the target by its own standard deviation
    (`scale(..., with_mean=False)`) BEFORE the kNN, then adds
    `1e-10 · max(1, mean|x|) · N(0,1)` to break ties. Getting this wrong is not a
    rounding difference: the joint distance is `max(|dx|, |dy|)`, so rescaling x
    relative to y changes which coordinate dominates and therefore changes every
    neighbourhood. Measured — omitting the scaling alone moved the estimate by
    **2.0e-02** and the rank correlation against sklearn down to **0.82**. With it,
    the two agree to ~1e-15.

    ⚠️ **The seed is `RandomState`, not `default_rng`.** `check_random_state(0)` is
    legacy MT19937; the two generators produce different streams from the same
    integer, and with the noise deciding tie order that is visible in the estimate.

    ⚠️ Negative estimates are clipped to 0, as sklearn does. The estimator is not
    guaranteed non-negative at finite `n`.
    """
    xp, to_device, to_host = _xp(device)
    n, p = X.shape

    # ⚠️ This block mirrors `sklearn.feature_selection._mutual_info._estimate_mi`
    # line for line, including the ORDER of the two `standard_normal` draws — the
    # single RNG is consumed by X first and then by y.
    rng = np.random.RandomState(random_state)
    Xn = np.asarray(X, dtype=np.float64).copy()
    std = Xn.std(axis=0)
    Xn /= np.where(std > 0, std, 1.0)
    Xn += (
        1e-10
        * np.maximum(1.0, np.mean(np.abs(Xn), axis=0))
        * rng.standard_normal((n, p))
    )
    yn = np.asarray(y, dtype=np.float64).copy()
    y_std = yn.std()
    yn = yn / (y_std if y_std > 0 else 1.0)
    yn += 1e-10 * max(1.0, np.mean(np.abs(yn))) * rng.standard_normal(n)

    Xd, yd = to_device(Xn), to_device(yn)
    # |y_i - y_j| once: it is the same for every column and is half the work.
    dy = xp.abs(yd.reshape(-1, 1) - yd.reshape(1, -1))

    if column_batch is None:
        column_batch = max(1, gpu.permutation_batch_size(n, n, device) // 2) if device == "cuda" else 8

    from scipy.special import digamma as _host_digamma

    out = np.zeros(p)
    k = n_neighbors
    psi_k_n = float(_host_digamma(k) + _host_digamma(n))
    for start in range(0, p, column_batch):
        stop = min(start + column_batch, p)
        block = Xd[:, start:stop]
        for j in range(stop - start):
            col = block[:, j]
            dx = xp.abs(col.reshape(-1, 1) - col.reshape(1, -1))
            joint = xp.maximum(dx, dy)
            # k-th neighbour EXCLUDING the point itself: the diagonal is 0, so the
            # (k+1)-th smallest of each row is the k-th neighbour's distance.
            if device == "cuda":
                radius = xp.kthvalue(joint, k + 1, dim=1).values
            else:
                radius = np.partition(joint, k, axis=1)[:, k]
            # ⚠️ STRICTLY inside, and the point itself is excluded by the -1.
            nx = (dx < radius.reshape(-1, 1)).sum(1) - 1
            ny = (dy < radius.reshape(-1, 1)).sum(1) - 1
            nx = nx if device != "cuda" else nx.double()
            ny = ny if device != "cuda" else ny.double()
            # ψ(k) and ψ(n) are SCALARS and identical every iteration — computed on
            # the host once (see `psi_k_n` below), so only the per-point means run
            # on the device.
            counts = float(
                xp.mean(
                    _digamma(nx + 1.0, xp, device) + _digamma(ny + 1.0, xp, device)
                )
            )
            out[start + j] = max(0.0, psi_k_n - counts)
    return out
