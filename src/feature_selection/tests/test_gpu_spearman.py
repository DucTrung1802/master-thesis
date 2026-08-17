# src\feature_selection\test_gpu_spearman.py
"""The Spearman helpers must agree with pandas, with each other, and across devices.

No database and no fit — synthetic frames shaped like the real pools, which means
NaN gaps, constant columns and long tie blocks, because those are the three things
that make a hand-written rank correlation disagree with `DataFrame.corr`.

    python -m pytest feature_selection/test_gpu_spearman.py -q

⚠️ **The CUDA tests SKIP rather than fail without a GPU**, and that is the only
honest option on a repo that is developed on one machine — but a skip is not a pass,
so `test_vector_matches_matrix_cuda` is the one to check by hand after any change to
`_average_ranks_torch` or `_pearson_against_last`.
"""

import numpy as np
import pandas as pd
import pytest

from feature_selection import gpu

CUDA, _REASON = gpu.cuda_available()
needs_cuda = pytest.mark.skipif(not CUDA, reason=f"no usable CUDA: {_REASON}")


def _awkward_frame(rows: int = 400, cols: int = 40, seed: int = 0) -> pd.DataFrame:
    """A frame carrying every shape that breaks a naive rank correlation."""
    rng = np.random.default_rng(seed)
    values = rng.standard_normal((rows, cols))
    values[rng.random(values.shape) < 0.15] = np.nan   # scattered gaps
    values[:, 0] = 0.0                                 # a constant column
    values[: rows // 2, 1] = 0.0                       # a long tie block
    values[:, 2] = np.nan                              # an all-NaN column
    return pd.DataFrame(values, columns=[f"c{i}" for i in range(cols)])


def _target(rows: int = 400, seed: int = 1) -> pd.Series:
    return pd.Series(np.random.default_rng(seed).standard_normal(rows), name="y")


def _matrix_last_column(frame: pd.DataFrame, target: pd.Series, device: str):
    """What `spearman_vector` used to do: the full p x p, then one column."""
    joined = frame.copy()
    joined["__t__"] = target.to_numpy(np.float64)
    return gpu.spearman_matrix(joined, device=device)["__t__"].drop(index="__t__")


# ─────────────────────────────────────────────────────── the vector rewrite

def test_vector_matches_matrix_cpu():
    """⚠️ THE REWRITE'S WHOLE CLAIM: 0.0, not 'close'.

    `_pearson_against_last` is algebraically the last column of `_pairwise_pearson`
    with every p x p product collapsed to a p-vector, so anything above exact
    equality means the specialisation dropped a term.
    """
    frame, target = _awkward_frame(), _target()
    old = _matrix_last_column(frame, target, "cpu").to_numpy()
    new = gpu.spearman_vector(frame, target, device="cpu").to_numpy()
    assert np.nanmax(np.abs(old - new)) == 0.0


@needs_cuda
def test_vector_matches_matrix_cuda():
    frame, target = _awkward_frame(), _target()
    old = _matrix_last_column(frame, target, "cuda").to_numpy()
    new = gpu.spearman_vector(frame, target, device="cuda").to_numpy()
    assert np.nanmax(np.abs(old - new)) == 0.0


@needs_cuda
def test_vector_agrees_across_devices():
    """§1 of the module docstring exempts this step from the device caveat.

    The tree methods legitimately differ between devices (subsampling RNG), but the
    Spearman steps must not — they are what makes the sign on the chart and the
    magnitude in the ensemble the same number.
    """
    frame, target = _awkward_frame(), _target()
    cpu = gpu.spearman_vector(frame, target, device="cpu").to_numpy()
    cuda = gpu.spearman_vector(frame, target, device="cuda").to_numpy()
    assert np.nanmax(np.abs(cpu - cuda)) == 0.0


def test_vector_keeps_column_order_and_name():
    """The result indexes the FEATURES only — the target must not survive into it."""
    frame, target = _awkward_frame(), _target()
    out = gpu.spearman_vector(frame, target, device="cpu")
    assert list(out.index) == list(frame.columns)
    assert out.name == "spearman_vs_target"
    assert "__target__" not in out.index


# ──────────────────────────────────────────────── blocking (TODO P1-4, MEM-1)


def test_one_block_is_the_default_for_anything_that_used_to_fit():
    """⚠️ **THE PROPERTY THAT PROTECTS EVERY ARCHIVED RUN**: a panel that could be
    ranked densely still is, so the code path does not move under runs whose numbers
    are already quoted. VCB is 4,266 rows — three orders below the budget."""
    assert gpu.rank_block_columns(4_266, 600, "cuda") == 600
    assert gpu.rank_block_columns(400, 40, "cpu") == 40           # cpu never blocks
    assert gpu.rank_block_columns(4_266, 1, "cuda") == 1


def test_the_universe_panel_is_blocked_and_the_budget_is_the_reason():
    """1.247 M rows x 536 float64 columns is the shape that OOMed a T4 (MEM-1).

    The exact block count depends on free VRAM, so this asserts the DIRECTION — it
    must not be one block — and the arithmetic the choice rests on.
    """
    block = gpu.rank_block_columns(1_247_098, 536, "cuda")
    assert 1 <= block < 536, block
    per_column = 1_247_098 * 8 * gpu.RANK_WORKING_MULTIPLE
    assert per_column > 90 * 1024**2  # ~100 MB of working set per column, at this height


@needs_cuda
def test_blocked_ranks_equal_dense_ranks():
    """⚠️ 0.0, not 'close'. A rank is computed within its own column and reads nothing
    from any other, so a block boundary that changed a number would mean the blocking
    had introduced cross-column state. This is what makes chunking the honest answer to
    an OOM where `float32` is not (P0-3: a 52 % relative change in `ic_mean`)."""
    import torch

    frame = _awkward_frame(rows=500, cols=37)
    values = torch.as_tensor(
        frame.to_numpy(np.float64), device="cuda", dtype=torch.float64
    )
    mask = torch.isfinite(values)
    dense = gpu._average_ranks_torch(values, mask).cpu().numpy()
    for block in (1, 2, 5, 36, 37, 100):
        blocked = gpu._average_ranks_torch(values, mask, block).cpu().numpy()
        assert np.array_equal(
            np.nan_to_num(dense, nan=-1.0), np.nan_to_num(blocked, nan=-1.0)
        ), block


@needs_cuda
def test_blocked_spearman_vector_equals_the_unblocked_one(monkeypatch):
    """The whole cuda path, forced into blocks of one column at a time.

    ⚠️ The block size is forced rather than waited for: this machine's card is 4 GiB
    and the frame that would block naturally is 1.25 M rows, which does not fit on it
    at all — the shape that needs this cannot be tested on the hardware that has it.
    """
    frame, target = _awkward_frame(rows=500, cols=37), _target(rows=500)
    whole = gpu.spearman_vector(frame, target, device="cuda").to_numpy()
    for block in (1, 3, 16):
        monkeypatch.setattr(gpu, "rank_block_columns", lambda *a, _b=block, **k: _b)
        blocked = gpu.spearman_vector(frame, target, device="cuda").to_numpy()
        assert np.nanmax(np.abs(whole - blocked)) == 0.0, block


# ─────────────────────────────────────────────────────── agreement with pandas

def test_matches_pandas_when_there_are_no_gaps():
    """With no NaNs the definition is identical to `DataFrame.corr('spearman')`.

    ⚠️ WITH gaps it is deliberately NOT identical — this module ranks once per
    column, pandas re-ranks inside each pairwise-complete subset. The largest
    measured disagreement on `pool__basic` is 0.062, well clear of the 0.9 the
    redundancy prune compares against (see `_pairwise_pearson`).
    """
    rng = np.random.default_rng(3)
    frame = pd.DataFrame(rng.standard_normal((300, 12)))
    target = pd.Series(rng.standard_normal(300))
    joined = frame.copy()
    joined["__t__"] = target.to_numpy()
    expected = joined.corr(method="spearman")["__t__"].drop(index="__t__").to_numpy()
    actual = gpu.spearman_vector(frame, target, device="cpu").to_numpy()
    assert np.allclose(expected, actual, atol=1e-12)


def test_constant_column_is_zero_not_nan():
    """A zero-variance column correlates with nothing — 0.0, so the prune can
    compare it without a special case."""
    frame, target = _awkward_frame(), _target()
    out = gpu.spearman_vector(frame, target, device="cpu")
    assert out["c0"] == 0.0          # constant
    assert out["c2"] == 0.0          # all-NaN
    assert np.isfinite(out.to_numpy()).all()


def test_ties_take_the_mean_rank():
    """Ordinal ranks would invent an ordering inside a tie block and change every
    correlation it takes part in — the `prop_*` columns are 0 for years."""
    frame = pd.DataFrame({"x": [1.0, 1.0, 1.0, 2.0, 3.0]})
    target = pd.Series([5.0, 4.0, 3.0, 2.0, 1.0])
    expected = frame.assign(y=target).corr(method="spearman").loc["x", "y"]
    actual = gpu.spearman_vector(frame, target, device="cpu")["x"]
    assert abs(expected - actual) < 1e-12
