# src\walkforward\test_arms.py
"""PRF-8 — what the multi-arm sweep must not get wrong, pinned.

An arm sweep exists to make "same data, different model" true by construction. Each test
below names a way that claim quietly stops holding: an arm resolved to the wrong package,
two arms sharing an output directory, or two tracks compared that were never over the same
rows.
"""

import os

import numpy as np
import pandas as pd
import pytest

from walkforward.compare import load_arms, paired
from walkforward.run import Arm, namespace_lock, parse_arms, resolve_model


def test_the_package_is_the_architecture():
    module = resolve_model("lstm")
    for name in ("CONFIG_DIR", "load_config", "train"):
        assert hasattr(module, name)
    assert module.MODEL_TYPE == "LSTM"
    assert resolve_model("gbt").MODEL_TYPE == "GBT"


def test_an_unknown_package_raises_and_says_what_an_arm_is():
    """⚠️ `lstm_small` is a CONFIG, not a package — the most likely wrong guess, so the
    message has to name the right form rather than only report the import failure."""
    with pytest.raises(ValueError, match="ARCHITECTURE"):
        resolve_model("lstm_small")


def test_an_arm_needs_the_package_colon_config_form():
    with pytest.raises(ValueError, match="package.*config"):
        parse_arms(["--arm", "lstm_small__all__rank_20day__final__d20_h20.yaml"])


def test_the_two_prf8_arms_parse_and_carry_the_right_sizes():
    """The configs are the experiment: two knobs move on the LSTM and nothing else."""
    arms = parse_arms([
        "--arm", "lstm:lstm_small__all__rank_20day__final__d20_h20.yaml",
        "--arm", "gbt:gbt__all__rank_20day__final__d20_h20.yaml",
    ])
    assert [a.label for a in arms] == ["lstm_small", "gbt"]
    small, gbt = arms
    assert small.config["model"] == {"type": "LSTM", "hidden_size": 16,
                                     "num_layers": 1, "dropout": 0.2}
    assert gbt.config["model"]["type"] == "GBT"
    # both must read the SAME dataset, or the comparison is between two experiments
    assert small.config["dataset"] == gbt.config["dataset"]
    assert small.config["n_features"] == gbt.config["n_features"] == 13
    assert small.config["lookback"] == gbt.config["lookback"] == 20


def test_the_small_lstm_moves_only_capacity_against_the_big_one():
    """⚠️ The point of PRF-8 is that ONE thing differs. A drifted optimiser schedule or a
    different seed would make a tie unreadable, so the equality is asserted here rather
    than trusted to a careful copy-paste."""
    big = parse_arms(["--arm", "lstm:lstm__all__rank_20day__final__d20_h20.yaml"])[0]
    small = parse_arms(
        ["--arm", "lstm:lstm_small__all__rank_20day__final__d20_h20.yaml"])[0]
    assert big.config["train"] == small.config["train"]
    assert big.config["seed"] == small.config["seed"]
    assert big.config["dataset"] == small.config["dataset"]
    assert big.config["task"] == small.config["task"]
    moved = {k for k in big.config["model"]
             if big.config["model"][k] != small.config["model"][k]}
    assert moved == {"hidden_size", "num_layers"}


def test_two_arms_that_would_share_an_output_directory_raise():
    """The label is `run_name` up to the first `__`; two arms sharing one would overwrite
    each other's `predictions_oos.csv` and the second would silently win."""
    with pytest.raises(ValueError, match="label"):
        parse_arms([
            "--arm", "lstm:lstm__all__rank_20day__final__d20_h20.yaml",
            "--arm", "lstm:lstm__all__rank_20day__final__d20_h20.yaml",
        ])


def _track(tmp_path, name, tickers, seed):
    directory = tmp_path / name
    directory.mkdir()
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2017-01-02", periods=40)
    rows = [{"date": d, "ticker": t, "y_true": rng.normal(),
             "y_pred": rng.normal(), "fold": "oos2017"}
            for d in dates for t in tickers]
    pd.DataFrame(rows).to_csv(directory / "predictions_oos.csv", index=False)
    return str(directory)


def test_comparing_tracks_over_different_rows_raises(tmp_path):
    """⚠️ Two tracks from different universes still yield two Sharpes that LOOK
    comparable. Nothing downstream would notice, so the index equality is the guard."""
    a = _track(tmp_path, "a", ["AAA", "BBB", "CCC"], 1)
    b = _track(tmp_path, "b", ["AAA", "BBB"], 2)
    with pytest.raises(AssertionError, match="two different experiments"):
        load_arms([("a", a), ("b", b)])


def test_comparing_tracks_over_the_same_rows_loads(tmp_path):
    a = _track(tmp_path, "a", ["AAA", "BBB"], 1)
    b = _track(tmp_path, "b", ["AAA", "BBB"], 2)
    loaded = load_arms([("a", a), ("b", b)])
    assert set(loaded) == {"a", "b"}
    assert len(loaded["a"]) == len(loaded["b"])


def test_the_paired_test_removes_a_common_factor():
    """The whole reason `compare` pairs: two arms that differ by a small constant on top
    of a large shared market series are far apart on a paired test and indistinguishable
    on an unpaired one."""
    rng = np.random.default_rng(0)
    market = pd.Series(rng.normal(0, 0.08, 120),
                       index=pd.bdate_range("2017-01-02", periods=120))
    a = market + 0.004
    b = market.copy()

    stat = paired(a, b, horizon=20)
    assert stat["corr"] > 0.99
    assert stat["t"] > 10          # paired: the constant is obvious

    # unpaired, the same difference against the raw dispersion of either series
    unpaired = (a.mean() - b.mean()) / np.sqrt(a.var(ddof=1) / len(a)
                                               + b.var(ddof=1) / len(b))
    assert abs(unpaired) < 1.0     # and invisible


def test_a_second_sweep_is_refused_while_one_holds_the_namespace():
    """MEASURED 2026-08-19: two sweeps over one table rebuild and delete each other's
    fold tensors. The loud half was a FileNotFoundError; the silent half was a model
    reading tensors another process was mid-write on."""
    with namespace_lock("first"):
        with pytest.raises(RuntimeError, match="holds the fold-dataset namespace"):
            with namespace_lock("second"):
                pass


def test_the_lock_is_released_even_when_the_sweep_raises():
    """A sweep that dies must not block every later one — the failure mode that makes
    people delete lock files by hand and then forget the guard exists."""
    with pytest.raises(ValueError):
        with namespace_lock("boom"):
            raise ValueError("boom")
    with namespace_lock("after"):
        pass


def test_a_lock_held_by_a_dead_process_is_taken_over(tmp_path, monkeypatch, capsys):
    import json as _json
    from walkforward import run as R

    path = tmp_path / ".walkforward.lock"
    path.write_text(_json.dumps({"pid": 999_999_999, "started": "then", "out": "x"}),
                    encoding="utf-8")
    monkeypatch.setattr(R, "_lock_path", lambda: str(path))
    monkeypatch.setattr(R, "_pid_alive", lambda pid: False)
    with R.namespace_lock("mine"):
        assert _json.loads(path.read_text(encoding="utf-8"))["pid"] == os.getpid()
    assert "stale lock" in capsys.readouterr().out
