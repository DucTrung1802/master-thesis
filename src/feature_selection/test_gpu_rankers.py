# src\feature_selection\test_gpu_rankers.py
"""The two reimplemented rankers must answer what sklearn answers.

`gpu_rankers` replaces two estimators that sklearn DEFINES, so unlike the rest of
this package these functions owe a comparison against the original rather than an
internal consistency check. That is what is here.

    python -m pytest feature_selection/test_gpu_rankers.py -q

⚠️ **The two are verified to DIFFERENT standards, and the difference is the point.**
`mutual_info` is the same estimator and is asserted to ~1e-12. `lasso` is the same
OBJECTIVE under a different optimiser, so it is asserted on the selected alpha, the
objective value and the ranking — never element-wise on coefficients, because with
`p > n` the LASSO minimiser need not be unique and two optimisers may legitimately
disagree on which of several equally-optimal vectors they return.
"""

import numpy as np
import pytest
from scipy.stats import spearmanr
from sklearn.feature_selection import mutual_info_regression
from sklearn.linear_model import LassoCV
from sklearn.preprocessing import StandardScaler

from feature_selection import gpu, gpu_rankers as gr
from feature_selection.selector import PurgedWalkForward

CUDA, _REASON = gpu.cuda_available()
needs_cuda = pytest.mark.skipif(not CUDA, reason=f"no usable CUDA: {_REASON}")

DEVICES = ["cpu"] + (["cuda"] if CUDA else [])


def _regression(n=800, p=120, k=8, seed=0):
    rng = np.random.default_rng(seed)
    X = rng.standard_normal((n, p))
    w = np.zeros(p)
    w[:k] = rng.standard_normal(k) * 2
    y = X @ w + rng.standard_normal(n) * 2
    return StandardScaler().fit_transform(X), y


def _objective(coef, alpha, X, y):
    """`(1/2n)||y - Xw||^2 + alpha*|w|_1`, centred as `fit_intercept=True` implies."""
    residual = (y - y.mean()) - (X - X.mean(0)) @ coef
    return float(residual @ residual / (2 * len(y)) + alpha * np.abs(coef).sum())


# ════════════════════════════════════════════════════════════════════ LASSO


@pytest.mark.parametrize("device", DEVICES)
def test_lasso_picks_the_same_alpha(device):
    """⚠️ The CV choice is the part that must not drift.

    The coefficients are a consequence of the penalty; if the two implementations
    choose different penalties they are answering different questions, and no
    tolerance on the coefficients would tell you so.
    """
    X, y = _regression()
    folds = PurgedWalkForward(5, 5, 300, 20).split(len(y))
    sk = LassoCV(cv=folds, random_state=42, max_iter=5000).fit(X, y)
    _coef, alpha, _ok = gr.lasso_cv(X, y, folds, device=device)
    assert alpha == pytest.approx(sk.alpha_, rel=1e-9)


@pytest.mark.parametrize("device", DEVICES)
def test_lasso_reaches_the_same_objective(device):
    """Same convex problem, so the OPTIMUM is the same number even where the
    argmin need not be unique."""
    X, y = _regression()
    folds = PurgedWalkForward(5, 5, 300, 20).split(len(y))
    sk = LassoCV(cv=folds, random_state=42, max_iter=5000).fit(X, y)
    coef, alpha, _ok = gr.lasso_cv(X, y, folds, device=device)
    assert _objective(coef, alpha, X, y) == pytest.approx(
        _objective(sk.coef_, sk.alpha_, X, y), rel=1e-6
    )


@pytest.mark.parametrize("device", DEVICES)
def test_lasso_ranks_features_the_same(device):
    """What the ensemble actually consumes is `|coef|` AS A RANK."""
    X, y = _regression()
    folds = PurgedWalkForward(5, 5, 300, 20).split(len(y))
    sk = LassoCV(cv=folds, random_state=42, max_iter=5000).fit(X, y)
    coef, _alpha, _ok = gr.lasso_cv(X, y, folds, device=device)
    assert spearmanr(np.abs(sk.coef_), np.abs(coef)).statistic > 0.999
    top = lambda v: set(np.argsort(-np.abs(v))[:8])
    assert top(sk.coef_) == top(coef)


@needs_cuda
def test_lasso_agrees_across_devices():
    X, y = _regression()
    folds = PurgedWalkForward(5, 5, 300, 20).split(len(y))
    a, alpha_a, _ = gr.lasso_cv(X, y, folds, device="cpu")
    b, alpha_b, _ = gr.lasso_cv(X, y, folds, device="cuda")
    assert alpha_a == pytest.approx(alpha_b, rel=1e-12)
    assert np.abs(a - b) .max() < 1e-5


def test_lasso_shortening_the_alpha_path_is_detectable():
    """⚠️ THE FALSE ECONOMY, pinned as a test so nobody 'optimises' it back in.

    30 alphas is twice as fast and lands on a DIFFERENT penalty. This asserts the
    documented failure rather than the fix, so that if a future change makes the
    short path safe, this test fails and the docstring gets revisited.
    """
    X, y = _regression(n=1200, p=300)
    folds = PurgedWalkForward(5, 5, 400, 20).split(len(y))
    _c, full, _ = gr.lasso_cv(X, y, folds, device="cpu", n_alphas=100)
    _c, short, _ = gr.lasso_cv(X, y, folds, device="cpu", n_alphas=30)
    assert full != pytest.approx(short, rel=1e-6)


def test_lasso_all_zero_target_is_handled():
    """`alpha_max = 0` must return zeros, not divide by it."""
    X, _ = _regression(n=300, p=20)
    y = np.zeros(300)
    folds = PurgedWalkForward(3, 5, 100, 20).split(300)
    coef, alpha, ok = gr.lasso_cv(X, y, folds, device="cpu")
    assert np.all(coef == 0.0) and alpha == 0.0 and ok


# ══════════════════════════════════════════════════════ MUTUAL INFORMATION


@pytest.mark.parametrize("device", DEVICES)
def test_mutual_info_matches_sklearn(device):
    """⚠️ THE SAME ESTIMATOR, so this is a 1e-12 assertion and not a rank check.

    Includes the three shapes that break a naive Kraskov port: a long tie block, a
    constant column, and a column on a wildly different scale (which is what makes
    sklearn's per-column `scale(with_mean=False)` load-bearing — the joint distance
    is `max(|dx|, |dy|)`, so relative scale decides every neighbourhood).
    """
    rng = np.random.default_rng(0)
    n, p = 900, 40
    X = rng.standard_normal((n, p))
    y = np.sin(X[:, 0] * 2) + X[:, 1] ** 2 + 0.5 * X[:, 2] + rng.standard_normal(n) * 0.5
    X[: n // 2, 5] = 0.0
    X[:, 6] = 0.0
    X[:, 7] = X[:, 0] * 1e6
    expected = mutual_info_regression(X, y, n_neighbors=3, random_state=0)
    actual = gr.mutual_info(X, y, n_neighbors=3, random_state=0, device=device)
    assert np.abs(expected - actual).max() < 1e-12


@pytest.mark.parametrize("device", DEVICES)
def test_mutual_info_is_non_negative(device):
    """Kraskov is not guaranteed non-negative at finite n; sklearn clips and so
    does this."""
    rng = np.random.default_rng(5)
    X = rng.standard_normal((400, 15))
    y = rng.standard_normal(400)
    assert (gr.mutual_info(X, y, random_state=0, device=device) >= 0.0).all()


@needs_cuda
def test_mutual_info_agrees_across_devices():
    rng = np.random.default_rng(2)
    X = rng.standard_normal((600, 25))
    y = X[:, 0] ** 2 + rng.standard_normal(600)
    cpu = gr.mutual_info(X, y, random_state=0, device="cpu")
    cuda = gr.mutual_info(X, y, random_state=0, device="cuda")
    assert np.abs(cpu - cuda).max() < 1e-12
