

def test_xgbrf_is_a_forest_on_the_card_and_refuses_what_it_cannot_do():
    """⚠️ `xgbrf` replaces ExtraTrees where a CUDA path is wanted — sklearn has none.

    Measured on the basket panel (419,923 x 151): `et` 1,103.8 s against `xgbrf` 55.0 s at
    the same 200 trees, a 20x cut. It is a DIFFERENT estimator, so this pins the shape of
    the swap, never an equality of their outputs.
    """
    import numpy as np
    import pytest

    from model.forest.model import KINDS, ForestWindow

    assert "xgbrf" in KINDS
    rng = np.random.default_rng(0)
    X = rng.normal(size=(400, 1, 6)).astype(np.float32)
    y = (X[:, 0, 0] + rng.normal(scale=0.4, size=400) > 0).astype(int)

    m = ForestWindow(6, 1, kind="xgbrf", n_estimators=20, max_depth=4, min_child_weight=5.0,
                     max_features=0.8, device="cpu")
    m.set_task("classification")
    m.fit(X, y)
    logit = m.predict_logit(X)
    assert logit.shape == (400,) and np.isfinite(logit).all()
    assert m.n_params > 0                      # decision nodes, the capacity measure
    assert m.provenance() == {"kind": "xgbrf", "device": "cpu", "backend": "xgboost"}

    # XGBoost has no unlimited tree, so the depth cap sklearn made optional is required
    with pytest.raises(ValueError, match="max_depth"):
        ForestWindow(6, 1, kind="xgbrf", n_estimators=10)
    # and an sklearn kind must not silently ignore a CUDA request
    with pytest.raises(ValueError, match="no CUDA path"):
        ForestWindow(6, 1, kind="et", device="cuda")
