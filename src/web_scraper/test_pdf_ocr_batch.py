"""The batch driver's decisions, pinned without a PDF, a network or an OCR engine.

⚠️ Every test here is about a decision that has already gone wrong once in this repo, and the
comments say which. None of them touches the cascade: `run_batch` spawns `pdf_ocr_job`, and
that module has its own suite.
"""
import datetime as dt
import json
import subprocess
from pathlib import Path

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


# ── the immediate write: `merge_each` ─────────────────────────────────────────
def _document(folder, symbol, period, accepted, **extra):
    (folder / "documents" / f"HOSE_{symbol}__{period}.json").write_text(
        json.dumps({"period": period, "exchange": "HOSE", "symbol": symbol,
                    "template": "bank", "accepted": accepted, **extra}),
        encoding="utf-8")


def test_a_quarter_is_complete_only_with_all_three_statements(tmp_path):
    """⚠️ THE GATE IS THE FILING, NOT THE STATEMENT. Two of three is a quarter whose CSVs
    would move apart — a balance sheet on disk with no cash flow beside it — so it is HELD for
    the sweep, where a person reads which statement is missing and why."""
    folder = _folder(tmp_path, "20260101-000000__hose_ctg__pdf_ocr")
    _document(folder, "CTG", "Q1-2019", {r: {"layer": "onnx@200", "items": 30}
                                         for r in ("balance_sheet", "income_statement",
                                                   "cash_flow")})
    _document(folder, "CTG", "Q2-2019", {"balance_sheet": {"layer": "onnx@200", "items": 30}})

    assert batch.complete_periods(folder) == ["Q1-2019"]
    held = batch.held_periods(folder)
    assert list(held) == ["Q2-2019"]
    assert "cash_flow" in held["Q2-2019"] and "income_statement" in held["Q2-2019"]


def test_a_raised_layer_makes_a_full_quarter_incomplete(tmp_path):
    """⚠️ `VCR-1`: an exception measures the MACHINE, so whatever won the cascade won BY
    DEFAULT — and the `accepted` block of such a document is indistinguishable from a good
    one, a real layer and a real item count. `plan_merge` refuses it whole too; refusing it
    here is what puts the reason in the batch log beside the document."""
    folder = _folder(tmp_path, "20260101-000000__hose_ctg__pdf_ocr")
    _document(folder, "CTG", "Q1-2019",
              {r: {"layer": "tesseract@200", "items": 30}
               for r in ("balance_sheet", "income_statement", "cash_flow")},
              engine_errors=[["onnx@200", "CUDA failure 2: out of memory"]])

    assert batch.complete_periods(folder) == []
    assert "RAISED" in batch.held_periods(folder)["Q1-2019"]


def test_an_interrupted_child_leaves_nothing_to_merge_and_does_not_raise(tmp_path):
    """A subprocess killed before it wrote its `documents/*.json` leaves a folder with nothing
    in it. The batch's job is to go on to the next filing — `wait_for_vram` takes the same
    line — and the sweep reports the folder at the end."""
    folder = _folder(tmp_path, "20260101-000000__hose_ctg__pdf_ocr")
    assert batch.complete_periods(folder) == []
    assert batch.held_periods(folder) == {}


def test_the_write_happens_between_documents_and_never_forces(tmp_path, monkeypatch):
    """⚠️ THE TWO PROPERTIES THE FLAG EXISTS FOR, PINNED TOGETHER.

    (1) The merge of document N runs BEFORE document N+1 is opened — that is what makes an
        interrupted run keep what it read (`BND-1`), and it is `SPN-1`'s ordering for free,
        since `TickerPlan.quarters` is sorted and `YYYY-QQ` sorts chronologically.
    (2) `force_differs` is NEVER passed. Four measured builds downgraded a quarter through an
        automatic per-quarter write; what separates this from them is that every refusal
        stays on, so a figure disagreeing with a `pdf` row on disk is still refused.
    """
    calls = []

    class _Report:
        decisions: list = []
        to_write: list = []
        backup = None

        def lines(self):
            return ["header"]

    def fake_merge_run(folder, **kw):
        calls.append(("merge", kw["periods"][0], kw))
        return _Report()

    from web_scraper import pdf_ocr_merge as real
    monkeypatch.setattr(real, "merge_run", fake_merge_run)
    monkeypatch.setattr(real, "record_merge", lambda *_a, **_kw: None)
    monkeypatch.setattr(batch, "wait_for_vram", lambda *_a, **_kw: None)
    monkeypatch.setattr(batch, "_engine_errors", lambda _f: 0)

    three = {r: {"layer": "onnx@200", "items": 30}
             for r in ("balance_sheet", "income_statement", "cash_flow")}
    # ⚠️ ONE FOLDER PER DOCUMENT, because that is what a batch produces: `run_batch` spawns a
    # child per quarter and `_newest_folder` claims the folder THAT child made. A fixture
    # sharing one folder would have the second merge re-visit the first quarter and pin a
    # behaviour the real driver never has.
    made = {}

    def fake_call(cmd, **_kw):
        quarter = cmd[cmd.index("--quarters") + 1]
        period = f"Q{quarter[-1]}-{quarter[:4]}"
        calls.append(("parse", quarter, None))
        made[quarter] = _folder(tmp_path, f"2026010{len(made) + 1}-000000__hose_ctg__pdf_ocr")
        _document(made[quarter], "CTG", period, three)
        return 0

    monkeypatch.setattr(subprocess, "call", fake_call)
    monkeypatch.setattr(batch, "_newest_folder",
                        lambda *_a, **_kw: list(made.values())[-1])

    plan = batch.TickerPlan(exchange="HOSE", symbol="CTG", template="bank",
                            template_how="given", quarters=["2019-Q3", "2019-Q4"],
                            operands=["2019-Q3"], filed=2, complete=0)
    batch.run_batch([plan], out_root=tmp_path, merge_each=True, log=lambda _s: None)

    # ⚠️ The Q3 SPAN OPERAND is written before Q4 is even parsed — which is the dependency the
    # two-pass sweep exists to reproduce (`SPN-1`).
    assert [(what, period) for what, period, _kw in calls] == [
        ("parse", "2019-Q3"), ("merge", "Q3-2019"),
        ("parse", "2019-Q4"), ("merge", "Q4-2019")]
    assert all(not kw.get("force_differs") for _w, _p, kw in calls if kw)
    # ⚠️ ONE BACKUP PER TICKER: asked for until a merge actually takes one. This fake never
    # returns a backup path, so both calls still ask — `test_one_backup_per_ticker_not_one_per_period`
    # is where the stopping is pinned.
    assert [kw["backup"] for _w, _p, kw in calls if kw] == [True, True]


def test_a_complete_quarter_is_written_even_with_no_magnitude_band(tmp_path, monkeypatch):
    """⚠️ **BY REQUEST, 2026-09-06 — AND IT LIFTS A REAL GUARD.**

    Refusing a statement whose `sane` band was empty is `BND-1`'s loop rather than a guard: no
    `pdf` row on disk means no band, no band means every statement refused, and every statement
    refused means there is still no `pdf` row and no CSV. So a quarter whose filing produced
    ALL THREE statements is written whether or not `sane` had anything to judge it by, and
    `run_batch` has no `force_empty_band` argument left to decide otherwise.

    ⚠️ What replaces the guard is the GATE (all three, nothing raised) and the RECORD
    (`Decision.band == 0`, printed beside the WRITE and carried into the run folder).
    """
    seen = []

    class _Report:
        decisions: list = []
        to_write: list = []
        backup = None

        def lines(self):
            return ["header"]

    from web_scraper import pdf_ocr_merge as real
    monkeypatch.setattr(real, "merge_run",
                        lambda folder, **kw: seen.append(kw) or _Report())
    monkeypatch.setattr(real, "record_merge", lambda *_a, **_kw: None)
    monkeypatch.setattr(batch, "wait_for_vram", lambda *_a, **_kw: None)
    monkeypatch.setattr(batch, "_engine_errors", lambda _f: 0)

    folder = _folder(tmp_path, "20260101-000000__hose_ctg__pdf_ocr")
    _document(folder, "CTG", "Q1-2019", {r: {"layer": "onnx@200", "items": 30}
                                         for r in ("balance_sheet", "income_statement",
                                                   "cash_flow")})
    monkeypatch.setattr(subprocess, "call", lambda *_a, **_kw: 0)
    monkeypatch.setattr(batch, "_newest_folder", lambda *_a, **_kw: folder)

    plan = batch.TickerPlan(exchange="HOSE", symbol="CTG", template="bank",
                            template_how="given", quarters=["2019-Q1"], filed=1, complete=0)
    batch.run_batch([plan], out_root=tmp_path, merge_each=True, log=lambda _s: None)

    assert [kw["force_empty_band"] for kw in seen] == [True]
    # ⚠️ AND STILL UNFORCED WHERE IT MATTERS: two runs disagreeing about a figure is not
    # settled by preferring the newer one, and the lift above does not touch that.
    assert all(not kw.get("force_differs") for kw in seen)


def test_the_sweep_lifts_the_band_for_a_complete_quarter_and_not_for_a_partial_one(
        tmp_path, monkeypatch):
    """⚠️ **THE TWO WRITERS MUST NOT DISAGREE ABOUT THE SAME QUARTER.** `merge_batch` applies
    the same rule as the immediate path, so a quarter that produced all three statements lands
    whichever one reaches it — and a filing that produced two of three stays the operator's
    call, which is what `force_empty_band` still governs."""
    folder = _folder(tmp_path, "20260101-000000__hose_ctg__pdf_ocr")
    three = {r: {"layer": "onnx@200", "items": 30}
             for r in ("balance_sheet", "income_statement", "cash_flow")}
    _document(folder, "CTG", "Q1-2019", three)
    _document(folder, "CTG", "Q2-2019", {"balance_sheet": {"layer": "onnx@200", "items": 30}})
    (folder / "metadata.json").write_text(json.dumps({
        "inputs": {"exchange": "HOSE", "symbol": "CTG"},
        "results": [{"period": p, "report": "balance_sheet"}
                    for p in ("Q1-2019", "Q2-2019")]}), encoding="utf-8")

    seen = {}

    class _Report:
        decisions: list = []
        to_write: list = []
        backup = None

        def lines(self):
            return ["header"]

    from web_scraper import pdf_ocr_merge as real
    monkeypatch.setattr(real, "merge_run", lambda _f, **kw: seen.update(
        {kw["periods"][0]: kw["force_empty_band"]}) or _Report())
    monkeypatch.setattr(real, "record_merge", lambda *_a, **_kw: None)

    batch.merge_batch([folder], apply=False, log=lambda _s: None)
    assert seen == {"Q1-2019": True, "Q2-2019": False}


def test_merge_each_is_off_by_default(tmp_path, monkeypatch):
    """⚠️ THE DEFAULT IS STILL "THIS DRIVER WRITES NOTHING". `merge_batch` is the deliberate
    act; a caller that has not asked for the immediate write must not get one."""
    monkeypatch.setattr(batch, "wait_for_vram", lambda *_a, **_kw: None)
    monkeypatch.setattr(subprocess, "call", lambda *_a, **_kw: 0)
    monkeypatch.setattr(batch, "_engine_errors", lambda _f: 0)
    monkeypatch.setattr(batch, "_newest_folder", lambda *_a, **_kw: tmp_path)

    def boom(*_a, **_kw):
        raise AssertionError("run_batch merged with merge_each unset")

    from web_scraper import pdf_ocr_merge as real
    monkeypatch.setattr(real, "merge_run", boom)

    plan = batch.TickerPlan(exchange="HOSE", symbol="CTG", template="bank",
                            template_how="given", quarters=["2019-Q3"], filed=1, complete=0)
    assert batch.run_batch([plan], out_root=tmp_path, log=lambda _s: None) == [tmp_path]


def test_the_run_ends_with_the_sweep_so_a_held_quarter_is_not_left_for_a_human(
        tmp_path, monkeypatch):
    """⚠️ **`merge_each` HOLDS A FILING THAT PRODUCED TWO STATEMENTS OF THREE, AND THE SWEEP
    THAT PICKS IT UP USED TO BE A NOTEBOOK CELL SOMEBODY HAD TO RUN.**

    Measured on HOSE_MBB, 2026-09-08: a 158-minute T4 round trip accepted 176 of 186 cells
    and wrote 0, because the process holding the notebook died after the pull and §9 never
    ran. `BND-1`'s third face — the parse is durable in the run folder, the CSV was never
    opened, and a green run says nothing about which. So the driver runs the sweep itself
    before it returns.
    """
    calls = []

    class _Report:
        decisions: list = []
        to_write: list = []
        backup = None

        def lines(self):
            return ["header"]

    def fake_merge_run(folder, **kw):
        calls.append(kw["periods"][0])
        return _Report()

    from web_scraper import pdf_ocr_merge as real
    monkeypatch.setattr(real, "merge_run", fake_merge_run)
    monkeypatch.setattr(real, "record_merge", lambda *_a, **_kw: None)
    monkeypatch.setattr(batch, "wait_for_vram", lambda *_a, **_kw: None)
    monkeypatch.setattr(batch, "_engine_errors", lambda _f: 0)

    # ⚠️ TWO STATEMENTS OF THREE — the shape `merge_each` refuses to write, so nothing is
    # merged between documents and the only write that can happen is the closing sweep.
    two = {r: {"layer": "onnx@200", "items": 30}
           for r in ("balance_sheet", "income_statement")}
    made = {}

    def fake_call(cmd, **_kw):
        quarter = cmd[cmd.index("--quarters") + 1]
        folder = _run_folder(tmp_path, f"2026010{len(made) + 1}-000000__hose_ctg__pdf_ocr",
                             "CTG", [f"Q{quarter[-1]}-{quarter[:4]}"])
        _document(folder, "CTG", f"Q{quarter[-1]}-{quarter[:4]}", two)
        made[quarter] = folder
        return 0

    monkeypatch.setattr(subprocess, "call", fake_call)
    monkeypatch.setattr(batch, "_newest_folder", lambda *_a, **_kw: list(made.values())[-1])

    plan = batch.TickerPlan(exchange="HOSE", symbol="CTG", template="bank",
                            template_how="given", quarters=["2019-Q3", "2019-Q4"],
                            filed=2, complete=0)
    batch.run_batch([plan], out_root=tmp_path, merge_each=True, log=lambda _s: None)

    # ⚠️ ONE PASS PER PERIOD, OLDEST FIRST — the sweep is `merge_batch` and keeps its order.
    assert calls == ["Q3-2019", "Q4-2019"]


def test_the_closing_sweep_respects_the_write_nothing_defaults(tmp_path, monkeypatch):
    """⚠️ The sweep must not become a back door around `merge_apply=False` or around the
    driver's own default of writing nothing — both of which exist so a caller who has not
    asked for a write does not get one."""
    monkeypatch.setattr(batch, "wait_for_vram", lambda *_a, **_kw: None)
    monkeypatch.setattr(batch, "_engine_errors", lambda _f: 0)

    def boom(*_a, **_kw):
        raise AssertionError("the sweep wrote with merge_apply=False")

    from web_scraper import pdf_ocr_merge as real
    monkeypatch.setattr(real, "merge_run", boom)

    made = {}

    def fake_call(cmd, **_kw):
        quarter = cmd[cmd.index("--quarters") + 1]
        made[quarter] = _run_folder(tmp_path, "20260101-000000__hose_ctg__pdf_ocr", "CTG",
                                    [f"Q{quarter[-1]}-{quarter[:4]}"])
        return 0

    monkeypatch.setattr(subprocess, "call", fake_call)
    monkeypatch.setattr(batch, "_newest_folder", lambda *_a, **_kw: list(made.values())[-1])

    plan = batch.TickerPlan(exchange="HOSE", symbol="CTG", template="bank",
                            template_how="given", quarters=["2019-Q3"], filed=1, complete=0)
    batch.run_batch([plan], out_root=tmp_path, merge_each=True, merge_apply=False,
                    log=lambda _s: None)


def test_force_differs_reaches_the_merge_only_when_a_caller_asks(tmp_path, monkeypatch):
    """⚠️ **IT IS NOT WIRED TO A RUN'S `OVERWRITE`, AND IT WAS UNTIL 2026-09-08.**

    `kgpu.runner.merge_statements` read the job's `OVERWRITE` and passed it as
    `force_differs`, so a re-parse asked for by quarter also lifted the refusal that protects
    a good `pdf` row already on disk. Those are two questions: which quarters to PARSE, and
    whether to replace a row. The second is `REPAIR` / `kgpu merge --overwrite`.
    """
    folder = _run_folder(tmp_path, "20260101-000000__hose_ctg__pdf_ocr", "CTG", ["Q1-2019"])
    seen = []

    class _Report:
        decisions: list = []
        to_write: list = []
        backup = None

        def lines(self):
            return ["header"]

    def fake_merge_run(_folder, **kw):
        seen.append(kw.get("force_differs"))
        return _Report()

    from web_scraper import pdf_ocr_merge as real
    monkeypatch.setattr(real, "merge_run", fake_merge_run)
    monkeypatch.setattr(real, "record_merge", lambda *_a, **_kw: None)

    batch.merge_batch([folder], apply=False, log=lambda _s: None)
    batch.merge_batch([folder], apply=False, force_differs=True, log=lambda _s: None)
    assert seen == [False, True]


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


# ── `CWD-2` — one root, one directory ─────────────────────────────────────────

def _quiet(monkeypatch, tmp_path, seen):
    """Everything `run_batch` touches outside the two facts under test."""
    monkeypatch.setattr(batch, "wait_for_vram",
                        lambda *_a, **_kw: seen.setdefault("waits", []).append(1))
    monkeypatch.setattr(subprocess, "call",
                        lambda cmd, **_kw: seen.update(cmd=list(cmd)) or 0)
    monkeypatch.setattr(batch, "_newest_folder",
                        lambda root, *_a, **_kw: seen.update(looked_in=root) or tmp_path)
    monkeypatch.setattr(batch, "_engine_errors", lambda _f: 0)
    return batch.TickerPlan(exchange="HOSE", symbol="MSN", template="corp",
                            quarters=["2021-Q4"], template_how="override",
                            operands=[], settled={}, filed=1, complete=False)


def test_a_relative_out_root_reaches_the_child_as_an_ABSOLUTE_path(tmp_path, monkeypatch):
    """⚠️ **THE CHILD DOES NOT SHARE THIS PROCESS'S WORKING DIRECTORY** (`CWD-2`).

    Every document is a subprocess launched with `cwd=<repo>/src`, so a relative `--out` names
    `<repo>/src/<root>` there and `<cwd>/<root>` here. Measured on MSN 2026-09-11: ten
    documents and ~2 h of GPU wrote all ten run folders into `src/reports/pdf_ocr_msn/`, the
    parent globbed an empty `reports/pdf_ocr_msn/`, and every one reported "exit 0 and NO run
    folder" — nothing raised, nothing was lost, and nothing was merged.
    """
    monkeypatch.chdir(tmp_path)
    seen = {}
    plan = _quiet(monkeypatch, tmp_path, seen)

    # `fill_gaps` reads the PDF index from a RELATIVE data root, which this `chdir` moves
    # out from under it; the fact under test is the path handed to the child.
    batch.run_batch([plan], out_root="reports/pdf_ocr_msn", fill_gaps=False)

    handed = seen["cmd"][seen["cmd"].index("--out") + 1]
    assert Path(handed).is_absolute()
    assert Path(handed) == (tmp_path / "reports" / "pdf_ocr_msn").resolve()


def test_the_parent_looks_in_the_same_directory_it_told_the_child_to_write(tmp_path,
                                                                          monkeypatch):
    """The other half: a root that means two places is only visible when both are named."""
    monkeypatch.chdir(tmp_path)
    seen = {}
    plan = _quiet(monkeypatch, tmp_path, seen)

    # `fill_gaps` reads the PDF index from a RELATIVE data root, which this `chdir` moves
    # out from under it; the fact under test is the path handed to the child.
    batch.run_batch([plan], out_root="reports/pdf_ocr_msn", fill_gaps=False)

    assert Path(seen["cmd"][seen["cmd"].index("--out") + 1]) == seen["looked_in"]


# ── `VRW-1` — a CPU cascade does not queue behind the card ────────────────────

def test_a_tesseract_only_cascade_does_NOT_wait_for_vram(tmp_path, monkeypatch):
    """⚠️ **20 CORES SPENT 120 s PER DOCUMENT WAITING FOR A CARD THEY NEVER TOUCH** (`VRW-1`).

    Measured 2026-09-11: three tesseract-only sweeps running beside one GPU job took the
    full `VRAM_WAIT_SECONDS` timeout on 20 of their first 158 documents — 40 minutes of
    nothing, and the gate could not have protected them from anything.
    """
    seen = {}
    plan = _quiet(monkeypatch, tmp_path, seen)

    batch.run_batch([plan], layers=["tesseract@200", "tesseract@400+relax"],
                    out_root=tmp_path)

    assert seen.get("waits") is None


def test_a_cascade_WITH_onnx_still_waits(tmp_path, monkeypatch):
    """The gate is worth keeping where it was measured: a document that starts short of VRAM
    is a document whose layers raise, and `VCR-1` then refuses it whole."""
    seen = {}
    plan = _quiet(monkeypatch, tmp_path, seen)

    batch.run_batch([plan], layers=["tesseract@200", "onnx@200"], out_root=tmp_path)

    assert seen.get("waits") == [1]


def test_NO_named_layers_means_the_whole_cascade_and_it_waits(tmp_path, monkeypatch):
    """`layers=None` is the shipped 117, of which 115 are onnx."""
    seen = {}
    plan = _quiet(monkeypatch, tmp_path, seen)

    batch.run_batch([plan], out_root=tmp_path)

    assert seen.get("waits") == [1]


def test_an_UNKNOWN_layer_name_is_treated_as_gpu_work(tmp_path, monkeypatch):
    """⚠️ §5 rule 2 at the gate: "not in the shipped cascade" is not evidence of "runs on the
    CPU", and the safe direction is to wait."""
    seen = {}
    plan = _quiet(monkeypatch, tmp_path, seen)

    batch.run_batch([plan], layers=["tesseract@200", "easyocr@200"], out_root=tmp_path)

    assert seen.get("waits") == [1]


# ── `GRD-2` — the grid reaches the corpus, not this ticker's stale index ──────

class _Task:
    def __init__(self, period):
        self.period = period


class _Builder:
    def __init__(self, seen):
        self.seen = seen

    def fill_period_grid(self, _ex, _sym, _tpl, periods, **_kw):
        self.seen["periods"] = list(periods)
        return {}


def _index(root, **by_symbol):
    """A `raw_data/cafef/pdfs/index/` directory holding one CSV per ticker."""
    index = root / "pdfs" / "index"
    index.mkdir(parents=True)
    for symbol, periods in by_symbol.items():
        lines = ["symbol,exchange,year,quarter,period,name"]
        lines += [f"{symbol},HOSE,{p.split('-')[1]},{p[1]},{p},f.pdf" for p in periods]
        (index / f"HOSE_{symbol}.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return index


def _grid_periods(monkeypatch, filed, ceiling, **kwargs):
    """Run `fill_grid` over `filed` and return the period list it handed the writer."""
    seen = {}
    monkeypatch.setattr(batch.job, "plan", lambda *_a, **_kw: [_Task(p) for p in filed])
    monkeypatch.setattr(batch, "corpus_ceiling", lambda **_kw: ceiling)
    plan = batch.TickerPlan(exchange="HOSE", symbol="BSR", template="corp",
                            template_how="override", quarters=[], operands=[], settled={},
                            filed=len(filed), complete=False)
    batch.fill_grid(plan, builder=_Builder(seen), apply=False, **kwargs)
    return seen.get("periods")


@pytest.fixture()
def fresh_ceiling():
    """`corpus_ceiling` memoises on `PDFS_DIR`; a test that changes it must clear that."""
    batch._CEILING_CACHE.clear()
    yield
    batch._CEILING_CACHE.clear()


def test_a_quarter_that_has_not_ENDED_is_never_the_ceiling(tmp_path, monkeypatch,
                                                           fresh_ceiling):
    """⚠️ **ONE MISLABELLED INDEX ROW WOULD OTHERWISE REACH ALL 784 TICKERS** (`GRD-2`).

    Measured 2026-09-11: one index file carried `Q3-2026`, a quarter that does not close until
    2026-09-30. A ceiling taken from the raw maximum would have written a `missing` row for a
    quarter nobody can have filed, in every CSV in the corpus.
    """
    _index(tmp_path, AAA=["Q1-2026", "Q2-2026"], BBB=["Q3-2026"])
    monkeypatch.setattr(batch.fin, "PDFS_DIR", str(tmp_path / "pdfs"))

    assert batch.corpus_ceiling(today=dt.date(2026, 9, 11)) == "Q2-2026"


def test_the_ceiling_is_the_newest_ENDED_quarter_ANY_index_carries(tmp_path, monkeypatch,
                                                                   fresh_ceiling):
    """The other guard: a quarter that has ended but that nobody has filed is not a ceiling
    either, because the ~30-day filing window may still be open."""
    _index(tmp_path, AAA=["Q4-2019"], BBB=["Q1-2026"], CCC=["Q4-2025"])
    monkeypatch.setattr(batch.fin, "PDFS_DIR", str(tmp_path / "pdfs"))

    assert batch.corpus_ceiling(today=dt.date(2026, 9, 11)) == "Q1-2026"


def test_an_index_that_cannot_be_READ_names_no_ceiling(tmp_path, monkeypatch, fresh_ceiling):
    """⚠️ §5 rule 2 at the ceiling: no answer is `None`, and `None` extends nothing."""
    monkeypatch.setattr(batch.fin, "PDFS_DIR", str(tmp_path / "nothing-here"))

    assert batch.corpus_ceiling(today=dt.date(2026, 9, 11)) is None


def test_a_damaged_index_file_is_skipped_rather_than_raising(tmp_path, monkeypatch,
                                                             fresh_ceiling):
    """784 files, and one of them being unreadable may not end a batch."""
    index = _index(tmp_path, AAA=["Q1-2026"])
    (index / "HOSE_BAD.csv").write_text("\n".join(["period", "FY-2009", "x"]) + "\n",
                                        encoding="utf-8")
    monkeypatch.setattr(batch.fin, "PDFS_DIR", str(tmp_path / "pdfs"))

    assert batch.corpus_ceiling(today=dt.date(2026, 9, 11)) == "Q1-2026"


def test_the_grid_EXTENDS_to_the_ceiling_when_this_tickers_index_is_behind(monkeypatch):
    """⚠️ **A STALE SCRAPE AND A COMPANY THAT STOPPED FILING ARE THE SAME SENTENCE ON DISK**
    (`GRD-2`). BSR's index stops at Q4-2020 and the corpus reaches Q2-2026; the grid now says
    so instead of ending quietly in 2020."""
    got = _grid_periods(monkeypatch, ["Q3-2020", "Q4-2020"], "Q2-2026")

    assert got[-1] == "Q2-2026"
    assert got[:2] == ["Q3-2020", "Q4-2020"]


def test_a_grid_ALREADY_past_the_ceiling_is_left_alone(monkeypatch):
    """The ceiling raises a floor; it is never a truncation."""
    got = _grid_periods(monkeypatch, ["Q1-2026", "Q2-2026"], "Q1-2026")

    assert got == ["Q1-2026", "Q2-2026"]


def test_to_ceiling_False_keeps_the_first_filed_to_last_filed_grid(monkeypatch):
    """The behaviour every CSV on disk was built under, still reachable."""
    got = _grid_periods(monkeypatch, ["Q3-2020", "Q4-2020"], "Q2-2026", to_ceiling=False)

    assert got == ["Q3-2020", "Q4-2020"]


def test_NO_ceiling_extends_nothing(monkeypatch):
    """An unreadable index must not shorten OR lengthen a grid."""
    got = _grid_periods(monkeypatch, ["Q3-2020", "Q4-2020"], None)

    assert got == ["Q3-2020", "Q4-2020"]
