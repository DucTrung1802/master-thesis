# src\feature_selection\tests\__init__.py
"""The package's tests — moved here from the top level 2026-08-16.

⚠️ **This file is load-bearing, not decoration.** With it, pytest resolves each
test as `feature_selection.tests.test_*` and puts `src` on `sys.path` — the same
import mode these files had when they sat beside `selector.py`. Delete it and
pytest switches to inserting THIS directory on the path and importing them as
top-level `test_*`, which works today only because no other package in the repo
has a test file of the same name (checked: `model/common/test_features.py`,
`result_evaluator/test_metrics.py`, `train_test_creator/test_dataset.py` — no
collisions). Keeping the package mode means a future collision cannot silently
shadow one of these.

Nothing else moved with them: every test still imports `from feature_selection
import ...`, which resolves through `pytest.ini`'s `pythonpath = src`.
"""
