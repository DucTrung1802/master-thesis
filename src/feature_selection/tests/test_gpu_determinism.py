# src\feature_selection\test_gpu_determinism.py
"""A fixed seed must give bit-identical numbers on a fixed device.

This is the property the whole archive depends on and the one nothing checked. It is
**not** the same claim as §1 of `gpu.py`, which says cpu and cuda give DIFFERENT
answers — that is about two devices, this is about one device twice. Both are true and
the tests below assert both, because it is easy to read the first as implying the GPU
is simply non-deterministic, and it is not.

Every function here is a component that could plausibly reorder a reduction between
runs: the batched permutation loop (a CUDA RNG plus a batched predict), the rank
transform (`scatter_reduce`, which is documented as non-deterministic for some reduce
modes), and the FISTA path (thousands of chained cuBLAS matmuls).

    python -m pytest feature_selection/test_gpu_determinism.py -q

⚠️ **The whole-selection version of this is not in the suite because it takes ~70 s
per run.** It was measured out of band instead, twice from a COLD interpreter, and
`feature_selection/CONTEXT.md` §16f records the result: identical sha256 over the raw
float64 bytes of `scores`, `ranks`, `target_corr`, `corr`, `stability` and
`validation`, plus an identical kept list.
"""

import numpy as np
import pandas as pd
import pytest
import xgboost as xgb
from scipy.stats import spearmanr

from feature_selection import gpu, gpu_rankers as gr
from feature_selection.selector import PurgedWalkForward

CUDA, _REASON = gpu.cuda_available()
needs_cuda = pytest.mark.skipif(not CUDA, reason=f"no usable CUDA: {_REASON}")
DEVICES = ["cpu"] + (["cuda"] if CUDA else [])

SEED = 18


def _frame(n=500, p=40, seed=0):
    rng = np.random.default_rng(seed)
    values = rng.standard_normal((n, p))
    values[rng.random(values.shape) < 0.1] = np.nan
    return pd.DataFrame(values, columns=[f"c{i}" for i in range(p)])


@pytest.mark.parametrize("device", DEVICES)
def test_spearman_vector_is_bit_identical(device):
    frame = _frame()
    target = pd.Series(np.random.default_rng(1).standard_normal(len(frame)))
    a = gpu.spearman_vector(frame, target, device=device).to_numpy()
    b = gpu.spearman_vector(frame, target, device=device).to_numpy()
    assert a.tobytes() == b.tobytes()


@pytest.mark.parametrize("device", DEVICES)
def test_spearman_matrix_is_bit_identical(device):
    frame = _frame()
    a = gpu.spearman_matrix(frame, device=device).to_numpy()
    b = gpu.spearman_matrix(frame, device=device).to_numpy()
    assert a.tobytes() == b.tobytes()


@pytest.mark.parametrize("device", DEVICES)
def test_lasso_is_bit_identical(device):
    rng = np.random.default_rng(0)
    n, p = 600, 80
    X = rng.standard_normal((n, p))
    w = np.zeros(p)
    w[:6] = rng.standard_normal(6) * 2
    y = X @ w + rng.standard_normal(n)
    folds = PurgedWalkForward(3, 5, 200, 20).split(n)
    a, alpha_a, _ = gr.lasso_cv(X, y, folds, device=device)
    b, alpha_b, _ = gr.lasso_cv(X, y, folds, device=device)
    assert alpha_a == alpha_b
    assert a.tobytes() == b.tobytes()


@pytest.mark.parametrize("device", DEVICES)
def test_permutation_is_bit_identical_at_a_fixed_seed(device):
    """⚠️ The one with a random number generator IN the loop.

    On CUDA the permutations come from a `torch.Generator(device="cuda")` seeded per
    call; on the host from a `default_rng`. Either would silently become
    irreproducible if the seeding moved outside the function.
    """
    rng = np.random.default_rng(0)
    p = 40
    X_train = rng.standard_normal((1200, p))
    w = np.zeros(p)
    w[:5] = rng.standard_normal(5) * 3
    y_train = X_train @ w + rng.standard_normal(1200)
    X_test = pd.DataFrame(
        rng.standard_normal((300, p)), columns=[f"f{i}" for i in range(p)]
    )
    y_test = X_test.to_numpy() @ w + rng.standard_normal(300)
    model = xgb.XGBRegressor(
        device=device, tree_method="hist", n_estimators=150, max_depth=3,
        random_state=SEED, verbosity=0,
    ).fit(X_train, y_train)

    def score(prediction):
        ic = spearmanr(prediction, y_test).statistic
        return 0.0 if np.isnan(ic) else float(ic)

    a = gpu.permutation_importance_batched(
        model, X_test, score, n_repeats=6, random_state=SEED, device=device
    )
    b = gpu.permutation_importance_batched(
        model, X_test, score, n_repeats=6, random_state=SEED, device=device
    )
    assert a.tobytes() == b.tobytes()


@pytest.mark.parametrize("device", DEVICES)
def test_permutation_responds_to_the_seed(device):
    """⚠️ THE TEST THAT STOPS THE ONE ABOVE BEING VACUOUS.

    A function that ignored `random_state` entirely — or that permuted nothing —
    would pass every bit-identity assertion here. It must also CHANGE when the seed
    does.
    """
    rng = np.random.default_rng(0)
    p = 40
    X_train = rng.standard_normal((1200, p))
    y_train = rng.standard_normal(1200)
    X_test = pd.DataFrame(
        rng.standard_normal((300, p)), columns=[f"f{i}" for i in range(p)]
    )
    y_test = rng.standard_normal(300)
    model = xgb.XGBRegressor(
        device=device, tree_method="hist", n_estimators=150, max_depth=3,
        random_state=SEED, verbosity=0,
    ).fit(X_train, y_train)

    def score(prediction):
        ic = spearmanr(prediction, y_test).statistic
        return 0.0 if np.isnan(ic) else float(ic)

    a = gpu.permutation_importance_batched(
        model, X_test, score, n_repeats=6, random_state=SEED, device=device
    )
    b = gpu.permutation_importance_batched(
        model, X_test, score, n_repeats=6, random_state=SEED + 1, device=device
    )
    assert a.tobytes() != b.tobytes()


@needs_cuda
def test_the_two_devices_still_disagree_on_the_trees():
    """⚠️ §1 of `gpu.py`, pinned — determinism per device is NOT cross-device equality.

    XGBoost's row/column subsampling draws from a different RNG stream on the GPU, so
    the same `random_state` selects different rows and columns. If this test ever
    starts failing, the caveat in §16e has stopped being true and every "pin the
    device" instruction in this repo should be revisited.
    """
    rng = np.random.default_rng(0)
    X = rng.standard_normal((1500, 30))
    y = X[:, 0] * 2 + rng.standard_normal(1500)
    predictions = []
    for device in ("cpu", "cuda"):
        model = xgb.XGBRegressor(
            device=device, tree_method="hist", n_estimators=300, max_depth=4,
            subsample=0.8, colsample_bytree=0.8, random_state=SEED, verbosity=0,
        ).fit(X, y)
        predictions.append(model.predict(X))
    assert not np.array_equal(predictions[0], predictions[1])
