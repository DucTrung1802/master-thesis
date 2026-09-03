"""The batch driver's decisions, pinned without a PDF, a network or an OCR engine.

⚠️ Every test here is about a decision that has already gone wrong once in this repo, and the
comments say which. None of them touches the cascade: `run_batch` spawns `pdf_ocr_job`, and
that module has its own suite.
"""
import json
import subprocess

import pytest

from web_scraper import pdf_ocr_batch as batch


# ── the VRAM floor ────────────────────────────────────────────────────────────
def test_free_vram_is_none_when_nvidia_smi_cannot_answer(monkeypatch):
    """⚠️ `None` means CANNOT TELL and must never be manufactured into a number.

    A CPU-only box and a Kaggle worker both have no `nvidia-smi`; inventing a 0 there would
    make `wait_for_vram` block for its whole timeout on every document, and inventing a large
    number would be §5 rule 2 — a measurement nobody took.
    """
    def boom(*_a, **_kw):
        raise FileNotFoundError("nvidia-smi")

    monkeypatch.setattr(subprocess, "run", boom)
    assert batch.free_vram_mb() is None


def test_an_unmeasurable_card_does_not_block_the_run(monkeypatch):
    monkeypatch.setattr(batch, "free_vram_mb", lambda: None)
    assert batch.wait_for_vram(floor_mb=99_999, timeout=0) is None


def test_a_short_card_is_reported_and_the_document_still_starts(monkeypatch):
    """⚠️ IT MUST NOT RAISE, and that is a decision rather than laziness.

    A document that starts short of memory may have layers raise — and `pdf_ocr_merge` refuses
    such a document whole (`VCR-1`), so nothing wrong reaches disk. Ending a 70-document batch
    because a browser tab held the card would cost far more than the one document at risk. What
    it MUST do is say so, so the shortfall is in the log beside the artefact that records it.
    """
    monkeypatch.setattr(batch, "free_vram_mb", lambda: 300)
    said = []
    free = batch.wait_for_vram(floor_mb=2600, timeout=0, log=said.append)
    assert free == 300
    assert any("300" in line for line in said), said


# ── the run folder a child produced ───────────────────────────────────────────
def _folder(root, name):
    path = root / name
    (path / "documents").mkdir(parents=True)
    return path


def test_a_folder_older_than_the_child_is_not_claimed_by_it(tmp_path):
    """⚠️ A ticker accumulates ONE FOLDER PER DOCUMENT in a batch, so "newest by name" is the
    PREVIOUS document's folder whenever a child died before creating one — and the batch would
    then merge a folder it did not produce, crediting this run with another's parse.
    """
    old = _folder(tmp_path, "20260101-000000__hose_ctg__pdf_ocr")
    import os
    os.utime(old, (1_000_000, 1_000_000))
    assert batch._newest_folder(tmp_path, "HOSE", "CTG", since=2_000_000) is None
    assert batch._newest_folder(tmp_path, "HOSE", "CTG", since=0) == old


def test_engine_errors_are_counted_across_a_folders_documents(tmp_path):
    folder = _folder(tmp_path, "20260101-000000__hose_ctg__pdf_ocr")
    (folder / "documents" / "a.json").write_text(
        json.dumps({"engine_errors": [["onnx@200", "CUDA failure 2: out of memory"]]}),
        encoding="utf-8")
    (folder / "documents" / "b.json").write_text(json.dumps({}), encoding="utf-8")
    assert batch._engine_errors(folder) == 1


# ── the merge order ───────────────────────────────────────────────────────────
def _run_folder(tmp_path, name, symbol, periods):
    folder = _folder(tmp_path, name)
    (folder / "metadata.json").write_text(json.dumps({
        "inputs": {"exchange": "HOSE", "symbol": symbol},
        "results": [{"period": p, "report": "income_statement"} for p in periods],
    }), encoding="utf-8")
    return folder


def test_the_merge_visits_one_period_per_call_oldest_first(tmp_path, monkeypatch):
    """⚠️ THE ORDER IS THE WHOLE POINT (`SPN-1`).

    `merge_run` plans a folder against disk and writes afterwards, so the `months` span a Q3
    records reaches Q4's planner only in the NEXT call. A batch that re-parsed a span operand
    and the Q4 it unblocks gets both ONLY if the two are separate calls in calendar order —
    and a batch folder is named by timestamp, which is not calendar order.
    """
    late = _run_folder(tmp_path, "20260102-000000__hose_ctg__pdf_ocr", "CTG", ["Q3-2019"])
    early = _run_folder(tmp_path, "20260101-000000__hose_ctg__pdf_ocr", "CTG", ["Q4-2019"])

    seen = []

    class _Report:
        decisions: list = []
        to_write: list = []
        backup = None

        def lines(self):
            return ["header"]

    def fake_merge_run(folder, **kw):
        seen.append((folder.name, tuple(kw["periods"])))
        return _Report()

    # ⚠️ PATCH THE MODULE'S FUNCTIONS, NOT `sys.modules`. `merge_batch` does
    # `from web_scraper import pdf_ocr_merge`, which reads the ATTRIBUTE on the package once
    # the real module has been imported by any earlier test — so substituting the entry in
    # `sys.modules` passes when this file runs alone and is ignored in the full suite. Both
    # of these tests failed exactly that way before the fix.
    from web_scraper import pdf_ocr_merge as real
    monkeypatch.setattr(real, "merge_run", fake_merge_run)
    monkeypatch.setattr(real, "record_merge", lambda *_a, **_kw: None)

    batch.merge_batch([late, early], apply=False, log=lambda _s: None)
    assert [p for _f, (p,) in seen] == ["Q3-2019", "Q4-2019"]


def test_one_backup_per_ticker_not_one_per_period(tmp_path, monkeypatch):
    """⚠️ Seventy timestamped copies of three CSVs answer *"what did this change?"* worse than
    one. `merge_run` only takes a backup when it is going to write, so asking for one until the
    first write happens yields exactly one — `pdf_ocr_job._upsert_period` takes the same line.
    """
    folder = _run_folder(tmp_path, "20260101-000000__hose_ctg__pdf_ocr", "CTG",
                         ["Q1-2019", "Q2-2019"])
    asked = []

    class _Report:
        decisions: list = []
        to_write: list = []
        backup = None

        def lines(self):
            return ["header"]

    def fake_merge_run(_folder, **kw):
        asked.append(kw["backup"])
        report = _Report()
        report.backup = tmp_path / "backup" if kw["backup"] else None
        return report

    # ⚠️ PATCH THE MODULE'S FUNCTIONS, NOT `sys.modules`. `merge_batch` does
    # `from web_scraper import pdf_ocr_merge`, which reads the ATTRIBUTE on the package once
    # the real module has been imported by any earlier test — so substituting the entry in
    # `sys.modules` passes when this file runs alone and is ignored in the full suite. Both
    # of these tests failed exactly that way before the fix.
    from web_scraper import pdf_ocr_merge as real
    monkeypatch.setattr(real, "merge_run", fake_merge_run)
    monkeypatch.setattr(real, "record_merge", lambda *_a, **_kw: None)

    batch.merge_batch([folder], apply=True, log=lambda _s: None)
    assert asked == [True, False]


def test_a_folder_without_metadata_is_skipped_and_said(tmp_path):
    """An interrupted child leaves a folder with documents and no `metadata.json`. Merging it
    would be merging a run that never finished; ignoring it SILENTLY is how a batch reports a
    coverage it does not have."""
    _folder(tmp_path, "20260101-000000__hose_ctg__pdf_ocr")
    said = []
    tally = batch.merge_batch([tmp_path / "20260101-000000__hose_ctg__pdf_ocr"],
                              apply=False, log=said.append)
    assert tally["passes"] == 0
    assert any("no metadata.json" in line for line in said), said


# ── the plan ──────────────────────────────────────────────────────────────────
def test_the_ticker_key_is_exchange_and_symbol():
    plan = batch.TickerPlan(exchange="HOSE", symbol="CTG", template="bank",
                            template_how="given")
    assert plan.key == "HOSE_CTG"


@pytest.mark.parametrize("floor", [0, 1])
def test_the_floor_is_a_free_memory_test_not_a_budget(monkeypatch, floor):
    """The constant is compared against FREE memory, so a card with a big total and no room
    fails it — which is the case that actually happened (`GPU-1`: 4096 MiB total, ~900 free)."""
    monkeypatch.setattr(batch, "free_vram_mb", lambda: 1000)
    assert batch.wait_for_vram(floor_mb=floor, timeout=0) == 1000


# ── the progress hook ─────────────────────────────────────────────────────────
def test_the_bar_moves_one_document_at_a_time_through_the_stage(tmp_path, monkeypatch):
    """⚠️ WITHOUT THIS THE BAR STANDS STILL THROUGH THE LONGEST THING THE CALLER DOES.
    `run_batch` is one stage of the control notebook's plan and it is ~86 % of it; a stage
    that reports only at its end is indistinguishable from a hung one for hours.

    ⚠️ The FLOOR of a document is claimed before it is read and its ceiling only after — a bar
    that credits work before it happens is the one thing a progress readout must not do.
    """
    from utils import progress

    lines = []
    reporter = progress.Stages([("before", "before", 50.0), ("parse", "OCR", 50.0)],
                               label="HOSE_CTG 2q", emit=lines.append)
    reporter.begin("parse")

    monkeypatch.setattr(batch, "wait_for_vram", lambda *_a, **_kw: None)
    monkeypatch.setattr(subprocess, "call", lambda *_a, **_kw: 0)
    seen = []
    monkeypatch.setattr(batch, "_newest_folder",
                        lambda *_a, **_kw: seen.append(reporter.fraction) or tmp_path)
    monkeypatch.setattr(batch, "_engine_errors", lambda _f: 0)

    plan = batch.TickerPlan(exchange="HOSE", symbol="CTG", template="bank",
                            template_how="override", quarters=["2014-Q3", "2014-Q4"],
                            operands=[], settled={}, filed=2, complete=False)
    batch.run_batch([plan], out_root=tmp_path, progress=reporter)

    # the FLOOR of each document as it started: 0/2 and 1/2 of the second half of the plan
    assert seen == [pytest.approx(0.5), pytest.approx(0.75)]
    assert reporter.fraction == pytest.approx(1.0)
    # ⚠️ and the driver's own lines came out in the ONE shape, not through a bare print
    assert lines and all(l.count("%") >= 1 for l in lines)
    assert any("HOSE_CTG 2q" in l and "2014-Q3" in l for l in lines)


def test_progress_is_optional_so_the_cli_prints_what_it_always_printed(tmp_path,
                                                                      monkeypatch, capsys):
    """⚠️ A formatting change that reaches a command nobody asked to change is a change
    nobody consented to — the same contract `kgpu.runner._stage` keeps for `reporter=None`."""
    monkeypatch.setattr(batch, "wait_for_vram", lambda *_a, **_kw: None)
    monkeypatch.setattr(subprocess, "call", lambda *_a, **_kw: 0)
    monkeypatch.setattr(batch, "_newest_folder", lambda *_a, **_kw: tmp_path)
    monkeypatch.setattr(batch, "_engine_errors", lambda _f: 0)
    plan = batch.TickerPlan(exchange="HOSE", symbol="CTG", template="bank",
                            template_how="override", quarters=["2014-Q4"],
                            operands=[], settled={}, filed=1, complete=False)
    batch.run_batch([plan], out_root=tmp_path)
    out = capsys.readouterr().out
    assert out.lstrip().startswith("──") and "%" not in out.splitlines()[0]
