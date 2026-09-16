"""`event_chain.trial` — the log of COMPLETE runs. No database."""

import json
import os

import pandas as pd

from event_chain import trial


def _body(trial_id: str, kind: str = "trial", models=None) -> dict:
    split = {"first_label_date": "2009-07-27", "last_label_date": "2021-05-18",
             "samples": 100, "positives": 10, "base_rate": 0.1}
    return {
        "schema_version": 2,
        "trial": {"id": trial_id, "kind": kind, "started_at": "2026-09-17 01:00:00"},
        "universe": {"ticker": "VCB", "schema": "unified_schema_vcb"},
        "event": {"column": "up_5pct_5day", "definition": "close[t+5] >= 1.05 x close[t]"},
        "environment": {"runtime": {"gpu": {"name": "RTX"}}},
        "split": {"train_ratio": 0.7, "val_ratio": 0.15, "holdout_start": "2021-06-22",
                  "purge_gap_rows": 24, "splits": {"train": split, "val": split, "test": split}},
        "selection": {"null_draws_configured": 10},
        "final_table": {"table": "up_5pct_5day__final__d20_h5", "keep_failed_pools": False,
                        "pools_in_table": ["pool__basic", "pool__event_features"]},
        "dataset": {"n_features": 30, "lookback": 20},
        "best": {"run_name": "gbt_d2__vcb__x"},
        "models": models if models is not None else [
            {"run_id": "gbt_d2__vcb__x__20260917-010000", "run_name": "gbt_d2__vcb__x",
             "model_type": "GBT", "device": "cpu", "created_at": "2026-09-17T01:00:00",
             "config": {"model": {"type": "GBT", "max_depth": 2}, "seed": 42},
             "timing": {"run_seconds": 4.2, "fit_seconds": 1.1},
             "event_metrics": {"val_auc": 0.7, "test_auc": 0.55, "test_auc_bar": 0.66}},
            {"run_id": "lstm_h16__vcb__x__20260917-010010", "run_name": "lstm_h16__vcb__x",
             "model_type": "LSTM", "device": "cuda", "created_at": "2026-09-17T01:00:10",
             "env": {"cuda_device": "NVIDIA GeForce RTX 3050 Laptop GPU"},
             "config": {"model": {"type": "LSTM", "hidden_size": 16}, "train": {"lr": 0.001}},
             "event_metrics": {"val_auc": 0.6, "test_auc": 0.7, "test_auc_bar": 0.66}},
            {"run_id": "", "run_name": "blend_top3__a+b", "model_type": "BLEND",
             "event_metrics": {"val_auc": 0.72}},
        ],
    }


def _write(tmp_path, body):
    folder = tmp_path / "trials" / body["trial"]["id"]
    folder.mkdir(parents=True)
    (folder / "trial.json").write_text(json.dumps(body), encoding="utf-8")


def _redirect(monkeypatch, tmp_path):
    monkeypatch.setattr(trial, "TRIALS_DIR", str(tmp_path / "trials"))
    monkeypatch.setattr(trial, "TRIALS_LOG", str(tmp_path / "trials.csv"))


def test_the_log_holds_one_row_per_model_run_of_a_complete_trial(monkeypatch, tmp_path):
    _redirect(monkeypatch, tmp_path)
    _write(tmp_path, _body("20260917-010000__vcb__t"))
    _write(tmp_path, _body("20260916-230000__probe", kind="probe"))    # not a complete trial
    _write(tmp_path, _body("20260916-231000__crash", models=[]))       # nothing was scored
    log = trial.rebuild_log()
    assert list(log.columns) == trial.LOG_COLUMNS
    assert list(log["model_variant"]) == ["gbt_d2", "lstm_h16"]        # the blend is not a run
    assert pd.read_csv(tmp_path / "trials.csv").shape == (2, len(trial.LOG_COLUMNS))


def test_a_row_names_its_data_features_target_split_model_time_hardware_and_result(monkeypatch, tmp_path):
    _redirect(monkeypatch, tmp_path)
    _write(tmp_path, _body("20260917-010000__vcb__t"))
    gbt, lstm = trial.rebuild_log().to_dict(orient="records")
    assert gbt["data_table"] == "unified_schema_vcb.up_5pct_5day__final__d20_h5"
    assert gbt["feature_pools"] == "basic+event_features" and gbt["n_features"] == 30
    assert gbt["target"] == "up_5pct_5day" and gbt["split_ratio"] == "train 70% / val 15% / test 15%"
    assert json.loads(gbt["hyperparameters"]) == {"model": {"max_depth": 2}}
    assert gbt["run_seconds"] == 4.2 and gbt["device"] == "cpu" and gbt["gpu"] == "not used"
    assert gbt["chosen_on_val"] is True and gbt["test_beats_null"] is False
    assert lstm["gpu"] == "NVIDIA GeForce RTX 3050 Laptop GPU" and lstm["test_beats_null"] is True
    assert json.loads(lstm["hyperparameters"])["train"] == {"lr": 0.001}


def test_deleting_a_trial_folder_removes_its_rows(monkeypatch, tmp_path):
    _redirect(monkeypatch, tmp_path)
    _write(tmp_path, _body("20260917-010000__vcb__t"))
    assert len(trial.rebuild_log()) == 2
    import shutil

    shutil.rmtree(tmp_path / "trials" / "20260917-010000__vcb__t")
    assert trial.rebuild_log().empty


def test_the_code_digest_ignores_line_endings(monkeypatch, tmp_path):
    for name, body in (("a", b"x = 1\r\ny = 2\r\n"), ("b", b"x = 1\ny = 2\n")):
        root = tmp_path / name
        (root / "src").mkdir(parents=True)
        (root / "src" / "m.py").write_bytes(body)
        monkeypatch.setattr(trial.C, "REPO_ROOT", str(root))
        monkeypatch.setattr(trial, "CODE_FILES", ("src/m.py",))
        if name == "a":
            first = trial.code_digest()["digest"]
    assert trial.code_digest()["digest"] == first
