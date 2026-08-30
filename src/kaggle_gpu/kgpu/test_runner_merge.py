"""Pure tests for the pull's statement merge — no Kaggle, no network, no OCR engine.

⚠️ **THE DEFECT THESE PIN IS SILENCE, NOT A WRONG ANSWER.** `pull` offers the upsert only the
run folders it COPIED this time, and `merge_statements` opened with `if not folders: return`.
So a folder already in the report root — a re-pull, or a second push of the same job — took
the statement CSVs out of the round trip entirely, and the whole trace was one line about the
run FOLDER. Measured 2026-08-30: an 8h40m HOSE_CTG round trip finished green, wrote a complete
run folder and created no CSV, and nothing in the output said which of the two had happened.
"""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from kgpu import runner


def _cfg(**kw):
    """The handful of `JobConfig` fields the merge path reads, and nothing else."""
    return SimpleNamespace(**{
        "name": "pdf-ocr-tst-all",
        "results_into": "reports/pdf_ocr",
        "merge_statements": True,
        "merge_force_empty_band": False,
        "parameters": {},
        "data": None,
        **kw,
    })


def _run_folder(root: Path, name: str) -> Path:
    """A directory that `merge_results` will recognise: it holds a `metadata.json`."""
    folder = root / name
    (folder / "documents").mkdir(parents=True)
    (folder / "metadata.json").write_text(json.dumps({"run_id": name}), encoding="utf-8")
    return folder


@pytest.fixture()
def roots(tmp_path, monkeypatch):
    """`results/` (the scratch mirror a pull downloads into) and the repo's report root."""
    monkeypatch.setattr(runner, "RESULTS_DIR", tmp_path / "results")
    monkeypatch.setattr(runner, "REPO_ROOT", tmp_path / "repo")
    (tmp_path / "results" / "reports" / "pdf_ocr").mkdir(parents=True)
    return SimpleNamespace(downloaded=tmp_path / "results" / "reports" / "pdf_ocr",
                           repo=tmp_path / "repo" / "reports" / "pdf_ocr")


# ──────────────────────────────────────────────────────────────────────────────
# merge_results reports BOTH halves
# ──────────────────────────────────────────────────────────────────────────────


def test_a_new_folder_is_copied_and_reported_as_copied(roots):
    _run_folder(roots.downloaded, "20260830-000000__hose_tst__pdf_ocr")

    copied, present = runner.merge_results(_cfg())

    assert [p.name for p in copied] == ["20260830-000000__hose_tst__pdf_ocr"]
    assert present == []
    assert (roots.repo / "20260830-000000__hose_tst__pdf_ocr" / "metadata.json").is_file()


def test_a_folder_already_in_the_repo_is_reported_rather_than_dropped(roots):
    """⚠️ THE LIST THAT DID NOT EXIST. Without it the caller cannot tell "there was nothing
    to merge" from "the thing you are waiting for was already here and so was skipped"."""
    name = "20260830-000000__hose_tst__pdf_ocr"
    _run_folder(roots.downloaded, name)
    _run_folder(roots.repo, name)

    copied, present = runner.merge_results(_cfg())

    assert copied == []
    assert [p.name for p in present] == [name]


def test_a_job_that_merges_nowhere_returns_two_empty_lists(roots):
    copied, present = runner.merge_results(_cfg(results_into=""))
    assert (copied, present) == ([], [])


# ──────────────────────────────────────────────────────────────────────────────
# and merge_statements says which case it is
# ──────────────────────────────────────────────────────────────────────────────


def test_nothing_copied_but_something_present_names_the_command_that_finishes_it(capsys):
    written = runner.merge_statements(
        _cfg(), [], already_present=[Path("reports/pdf_ocr/20260830-000000__x__pdf_ocr")])

    out = capsys.readouterr().out
    assert written == 0
    assert "NOTHING WAS MERGED" in out
    assert "were already in" in out
    # ⚠️ The command, spelled out. A warning that does not say what to do next is a warning
    # that gets read once.
    assert "python -m kgpu merge pdf-ocr-tst-all" in out


def test_nothing_came_home_at_all_says_so_rather_than_returning_in_silence(capsys):
    written = runner.merge_statements(_cfg(), [], already_present=[])

    out = capsys.readouterr().out
    assert written == 0
    assert "no run folder came home" in out


def test_a_merge_that_wrote_nothing_points_at_the_bootstrap_knob(capsys):
    """⚠️ `BND-1`: on a ticker with no CSV yet `sane` has no band, so every statement is
    refused and the loop closes on itself. The end of the run is where that has to be said —
    the `skip` lines above it look identical to a genuinely unreadable filing."""
    runner._say_what_landed(0, apply=True)

    out = capsys.readouterr().out
    assert "0 statements were written" in out
    assert "FORCE_EMPTY_BAND" in out
    assert "BND-1" in out


def test_a_merge_that_wrote_something_says_how_many(capsys):
    runner._say_what_landed(126, apply=True)
    assert "126 statement(s) written" in capsys.readouterr().out


def test_a_dry_run_never_claims_a_write(capsys):
    runner._say_what_landed(0, apply=False)
    out = capsys.readouterr().out
    assert "DRY RUN" in out
    assert "were written" not in out
