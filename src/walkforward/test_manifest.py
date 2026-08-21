# src\walkforward\test_manifest.py
"""`WFO-1` — pin the refusal, because the only control before this was reading
`DEFAULT_OUT` before pressing enter.

⚠️ The first two tests reproduce the exact command pair that nearly destroyed `PRF-1`:
`RUNBOOK.md` §3's documented h=20 line, then the same line at h=10 with `--out` omitted.
The third pins the half that misstates a number instead of destroying one.
"""

from __future__ import annotations

import json
import os

import pandas as pd
import pytest

from walkforward import manifest as M

H20 = "rank_20day__final__d20_h20"
H10 = "rank_10day__final__d20_h10"

BASE = dict(ticker="all", first_test="2017-01-01", step_months=12, val_months=12,
            scale_target=True, rank_min_width=5)


def _ident(table, **over):
    return M.identity(table=table, **{**BASE, **over})


def _legacy_track(directory, table, model="lstm"):
    """A track as it existed BEFORE the manifest — `folds.csv` and nothing else."""
    os.makedirs(directory, exist_ok=True)
    pd.DataFrame([
        {"fold": tag, "train": 1, "val": 1, "test": 1, "n_features": 13,
         "run": f"{model}__all__{table}__{tag}__20260819-023033"}
        for tag in ("oos2017", "oos2018")
    ]).to_csv(os.path.join(directory, "folds.csv"), index=False)


# ── the collision that actually happened ──────────────────────────────────────

def test_a_second_horizon_into_the_same_directory_is_refused(tmp_path):
    out = str(tmp_path / "walkforward")
    M.claim(out, _ident(H20))
    M.write(out, _ident(H20))

    with pytest.raises(SystemExit) as raised:
        M.claim(out, _ident(H10))
    assert "WFO-1" in str(raised.value)
    assert "table" in str(raised.value)


def test_a_LEGACY_track_with_no_manifest_is_protected_too(tmp_path):
    """The five tracks on disk predate the manifest — the guard must still cover them."""
    out = str(tmp_path / "walkforward")
    _legacy_track(out, H20)
    assert M.read(out) is None

    with pytest.raises(SystemExit) as raised:
        M.claim(out, _ident(H10))
    assert H20 in str(raised.value)


def test_an_ARM_sweep_protects_its_parent_through_the_leaves(tmp_path):
    """`--out` names the parent; only the arm leaves hold a `folds.csv`."""
    out = str(tmp_path / "arch")
    for arm in ("lstm", "gbt"):
        _legacy_track(os.path.join(out, arm), H10, model=arm)

    with pytest.raises(SystemExit):
        M.claim(out, _ident(H20))
    M.claim(out, _ident(H10))          # the matching experiment is allowed through


def test_re_running_the_SAME_experiment_is_allowed(tmp_path):
    out = str(tmp_path / "walkforward")
    M.write(out, _ident(H20))
    M.claim(out, _ident(H20))          # must not raise — a redo is legitimate


def test_a_knob_only_difference_is_refused_when_recorded(tmp_path):
    """A settings sweep changes the tensors without changing the table."""
    out = str(tmp_path / "settings")
    M.write(out, _ident(H10))
    with pytest.raises(SystemExit) as raised:
        M.claim(out, _ident(H10, val_months=24))
    assert "val_months" in str(raised.value)


def test_a_knob_only_difference_is_NOT_caught_on_a_legacy_track(tmp_path):
    """⚠️ Stated, not silently hoped for: `folds.csv` records no knobs, and §5 rule 2
    says an absent measurement is absent rather than inferred. So a legacy directory is
    guarded against the horizon collision that happened and not against this one."""
    out = str(tmp_path / "settings")
    _legacy_track(out, H10)
    M.claim(out, _ident(H10, val_months=24))     # passes — and that is the known limit


def test_force_out_overrides_the_refusal(tmp_path):
    out = str(tmp_path / "walkforward")
    M.write(out, _ident(H20))
    M.claim(out, _ident(H10), force=True)


# ── the scoring half: the horizon is derived, never defaulted ─────────────────

def test_the_horizon_is_derived_from_the_track(tmp_path):
    out = str(tmp_path / "h10")
    _legacy_track(out, H10)
    assert M.horizon_for(out) == 10          # not the old `--horizon` default of 20


def test_an_explicit_horizon_that_disagrees_raises(tmp_path):
    out = str(tmp_path / "h10")
    _legacy_track(out, H10)
    with pytest.raises(SystemExit) as raised:
        M.horizon_for(out, requested=20)
    assert "wrong label" in str(raised.value)


def test_an_explicit_horizon_that_agrees_is_fine(tmp_path):
    out = str(tmp_path / "h10")
    _legacy_track(out, H10)
    assert M.horizon_for(out, requested=10) == 10


def test_a_scoped_table_still_yields_its_horizon():
    """`rank_10day__final__d20_h10__wide10` is a real table on disk."""
    assert M.horizon_of("rank_10day__final__d20_h10__wide10") == 10


def test_a_table_that_does_not_parse_raises_rather_than_guessing():
    with pytest.raises(ValueError):
        M.horizon_of("pool__basic")


def test_the_manifest_records_the_horizon_it_derived(tmp_path):
    out = str(tmp_path / "h20")
    path = M.write(out, _ident(H20), arm="lstm")
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)
    assert payload["horizon"] == 20
    assert payload["arm"] == "lstm"
    # ⚠️ provenance must NOT leak into the identity, or two arms of one sweep would read
    # as two experiments and `compare` could never be run on them.
    assert "arm" not in M.IDENTITY_KEYS


def test_two_horizons_cannot_be_compared_arm_for_arm(tmp_path):
    """`compare` pairs arms inside ONE sweep; two horizons is `walkforward.pair`."""
    from walkforward import compare

    a, b = str(tmp_path / "h20"), str(tmp_path / "h10")
    _legacy_track(a, H20)
    _legacy_track(b, H10)
    with pytest.raises(SystemExit) as raised:
        compare._shared_horizon([("h20", a), ("h10", b)])
    assert "walkforward.pair" in str(raised.value)
