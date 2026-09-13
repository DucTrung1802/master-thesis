"""Two fleet lanes are two PROCESSES sharing one checkout — pin what they must not share.

⚠️ **MEASURED ON THE `--mode fragmented` FLEET, 2026-09-13, AND IT COST 36 OF 96 DOCUMENTS.**
Five lanes ran at once. Two defects, one per file here:

`BLD-1` — `runner.build` opened with `shutil.rmtree(BUILD_DIR)` against a directory that was
the SAME for every job, then wrote `kernel-metadata.json` into it. `JobConfig.payload_dir` had
already been made per-job with the docstring *"two jobs must not overwrite each other"*; the
build directory beside it had not, which is `SPB-2`'s shape — a rule taught to one of the two
places that must agree. Lane `ductrung180200#1` pushed SHB's kernel carrying
`lyductrung/mt-cafef-filings-pow-...`, **another account's dataset**, and Kaggle answered
`rejected dataset source(s) ... and pushed the kernel anyway`.
⚠️ **THE REJECTION IS THE LUCKY HALF.** Had the source been readable, the kernel would have run
to completion against the WRONG TICKER'S FILINGS and returned a well-formed parse — and a run
folder records the rows, not which dataset was mounted, so nothing downstream could tell.

`SES-1` — `_fetch_status` treated every `denied` as a verdict. Right after a push it is
Kaggle's read-after-write lag: lane `lyductrung#2` logged `pushed version 2` for POW and the
next status call answered `denied`. The lane raised and moved on, leaving a GPU session nobody
watched, and the account's next five tickers came back `Maximum batch GPU session count of 2
reached`. **The session cap is per ACCOUNT, so a lane that loses track of its own kernel spends
the other lane's slot too.**
"""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from kgpu import config as kcfg
from kgpu import runner


# ── `BLD-1` — the staged build is per JOB ────────────────────────────────────

def _job(name: str, dataset: str) -> kcfg.JobConfig:
    return kcfg.JobConfig(name=name, id=f"owner/{name}", title=name,
                          notebook="src/kaggle_gpu/RUN__pdf_ocr.ipynb",
                          dataset_sources=[dataset])


def test_two_jobs_stage_into_two_directories():
    a, b = _job("pdf-ocr-shb", "me/filings-shb"), _job("pdf-ocr-pow", "you/filings-pow")
    assert a.build_dir != b.build_dir
    assert a.built_notebook != b.built_notebook
    # and the notebook keeps its own name inside each, because Kaggle's `code_file` names it
    assert a.built_notebook.name == b.built_notebook.name == "RUN__pdf_ocr.ipynb"


def test_one_lane_s_metadata_survives_another_lane_building(tmp_path, monkeypatch):
    """The failure as it happened: B builds while A is mid-push, and A's metadata is B's.

    ⚠️ Pinned on `kernel_metadata()` + the directory contract rather than on a real
    `build()`, which patches a notebook and reads git. The defect was never in the JSON —
    it was in WHERE the JSON lands.
    """
    monkeypatch.setattr(kcfg, "BUILD_DIR", tmp_path / ".build")
    a, b = _job("pdf-ocr-shb", "me/filings-shb"), _job("pdf-ocr-pow", "you/filings-pow")
    for job in (a, b):
        job.build_dir.mkdir(parents=True)
        (job.build_dir / "kernel-metadata.json").write_text(
            json.dumps(job.kernel_metadata()), encoding="utf-8")

    staged = json.loads((a.build_dir / "kernel-metadata.json").read_text(encoding="utf-8"))
    assert staged["dataset_sources"] == ["me/filings-shb"]
    assert staged["id"] == "owner/pdf-ocr-shb"


# ── `SES-1` — a `denied` right after a push is lag, not an answer ────────────

class _DeniedThenReady:
    """Kaggle's read-after-write: `denied` for the first `n` calls, then the status."""

    def __init__(self, denials: int):
        self.calls = 0
        self.denials = denials

    def kernels_status(self, _id):
        self.calls += 1
        if self.calls <= self.denials:
            raise ValueError("403 - Permission 'kernels.get' was denied")
        return SimpleNamespace(status="RUNNING", failure_message=None)


def test_a_denied_answer_still_raises_when_no_grace_was_asked_for():
    """The old behaviour, unchanged — this is the half that was right."""
    with pytest.raises(RuntimeError, match="has no kernel"):
        runner._fetch_status(_DeniedThenReady(99), _job("pdf-ocr-x", "me/d"))


def test_a_denied_answer_is_retried_inside_the_grace_window(monkeypatch):
    monkeypatch.setattr(runner.time, "sleep", lambda _s: None)
    api = _DeniedThenReady(2)
    response = runner._fetch_status(api, _job("pdf-ocr-x", "me/d"),
                                    denied_grace_minutes=5.0)
    assert runner._status_name(response.status) == "RUNNING"
    assert api.calls == 3


def test_the_grace_is_BOUNDED_so_a_truly_missing_kernel_still_raises(monkeypatch):
    """⚠️ A grace that never gives up is a lane that hangs instead of one that leaks."""
    monkeypatch.setattr(runner.time, "sleep", lambda _s: None)
    clock = iter([0.0] + [i * 60.0 for i in range(1, 40)])
    monkeypatch.setattr(runner.time, "perf_counter", lambda: next(clock))
    with pytest.raises(RuntimeError, match="has no kernel"):
        runner._fetch_status(_DeniedThenReady(99), _job("pdf-ocr-x", "me/d"),
                             denied_grace_minutes=2.0)


def test_the_raise_says_a_session_may_be_running(monkeypatch):
    """⚠️ The COST of this failure is the session, not the missing read — say so."""
    with pytest.raises(RuntimeError) as excinfo:
        runner._fetch_status(_DeniedThenReady(99), _job("pdf-ocr-x", "me/d"))
    assert "SESSION MAY BE RUNNING UNWATCHED" in str(excinfo.value)
    assert "SES-1" in str(excinfo.value)
