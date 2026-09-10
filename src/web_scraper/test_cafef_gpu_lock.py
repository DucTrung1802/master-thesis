"""`gpu_lock` and the recycled pid — the stale lock that could never clear.

⚠️ **MEASURED 2026-09-10, AND IT HAD ALREADY BLOCKED EVERY OCR RUN ON THIS MACHINE FOR HOURS.**
A SHB batch died at 00:01:12 leaving `{"pid": 13720, "label": "HOSE_SHB", "started":
"2026-09-10T00:01:12"}` in `reports/pdf_ocr/.pdf_ocr.lock`. By 00:39:37 Windows had handed pid
13720 to a `conhost`, so `psutil.pid_exists(13720)` answered True, `gpu_lock` honoured the lock,
and the next run raised *"another PDF-OCR run (pid 13720, HOSE_SHB) holds the OCR device"* about
a process that had been dead for forty minutes. The docstring's promise — *"advisory and
self-healing: a lock whose pid is dead is taken over, or a killed run would block every later
one"* — was exactly the branch that could not be reached.

⚠️ **THE PROOF NEEDED NO NEW FIELD.** A process cannot write a lock before it starts, so a
holder whose `create_time` is later than the lock's own `started` is a different process wearing
a recycled pid.
"""
import contextlib
import json
import os
from datetime import datetime, timedelta

import pytest

from web_scraper import pdf_ocr_job as job

psutil = pytest.importorskip("psutil")


def _stamp(**delta):
    return (datetime.now() + timedelta(**delta)).isoformat(timespec="seconds")


def test_a_live_process_that_wrote_the_lock_still_holds_it():
    assert job._pid_alive(os.getpid(), _stamp()) is True


def test_a_recycled_pid_does_not_hold_a_lock_it_could_not_have_written():
    """⚠️ THE WHOLE DEFECT: this process is alive and its pid matches, and it started LONG
    after the lock was written, so it is not the writer."""
    assert job._pid_alive(os.getpid(), _stamp(hours=-3)) is False


def test_a_pid_that_does_not_exist_is_not_alive():
    assert job._pid_alive(999_999, _stamp()) is False


def test_a_lock_with_no_stamp_is_still_honoured():
    """A lock written before this check existed carries no proof either way, and `gpu_lock`
    refuses rather than clobbers — the failure it guards is two runs on one 4 GiB card."""
    assert job._pid_alive(os.getpid(), None) is True


def test_an_unreadable_stamp_is_honoured_rather_than_clobbered():
    assert job._pid_alive(os.getpid(), "not-a-date") is True


def test_gpu_lock_takes_over_a_lock_whose_pid_was_recycled(tmp_path, capsys):
    """End to end: the stale lock is taken over and SAID, not silently replaced."""
    root = tmp_path / "pdf_ocr"
    root.mkdir()
    (root / ".pdf_ocr.lock").write_text(json.dumps({
        "pid": os.getpid(), "label": "HOSE_STALE", "started": _stamp(hours=-3),
    }), encoding="utf-8")
    with job.gpu_lock("HOSE_TST", out_root=root):
        held = json.loads((root / ".pdf_ocr.lock").read_text(encoding="utf-8"))
        assert held["label"] == "HOSE_TST"
    out = capsys.readouterr().out
    assert "taking over a stale OCR lock" in out
    assert "HOSE_STALE" in out


def test_gpu_lock_still_refuses_a_lock_its_writer_could_still_hold(tmp_path):
    """The guard that matters is untouched: a lock this very process wrote a moment ago is a
    lock a second parse in the same process may NOT take — there is no legitimate re-entry."""
    root = tmp_path / "pdf_ocr"
    root.mkdir()
    (root / ".pdf_ocr.lock").write_text(json.dumps({
        "pid": os.getpid(), "label": "HOSE_LIVE", "started": _stamp(),
    }), encoding="utf-8")
    with pytest.raises(RuntimeError, match="holds the OCR device"):
        with job.gpu_lock("HOSE_TST", out_root=root):
            pass


def test_the_bypass_still_works(tmp_path, monkeypatch):
    monkeypatch.setenv("CAFEF_OCR_NO_LOCK", "1")
    root = tmp_path / "pdf_ocr"
    root.mkdir()
    (root / ".pdf_ocr.lock").write_text(json.dumps({
        "pid": os.getpid(), "label": "HOSE_LIVE", "started": _stamp(),
    }), encoding="utf-8")
    with contextlib.suppress(RuntimeError):
        with job.gpu_lock("HOSE_TST", out_root=root):
            return
    raise AssertionError("CAFEF_OCR_NO_LOCK did not bypass the lock")
