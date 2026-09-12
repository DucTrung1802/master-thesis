"""`ALT-1`'s retry was dead on every Kaggle run, and nothing said so.

⚠️ **THE `continue` THAT KILLED IT SITS BEFORE ITS OWN LOG LINE.** `_alternate_retry` finds the
other filings of a period through `os.path.exists`, and a documents payload shipped ONE filing
per quarter (`FinancialsBuilder.documents()`), so on a worker every alternate was skipped in
silence — no line, no warning, no field in the run folder. A green run that did not do the
thing (§5 rule 10).

⚠️ **MEASURED OVER VN30, 2026-09-12: 127 of the 536 open cells have an alternate on disk that
was never retried**, and the split says which machine parsed what — the tickers where the retry
DID fire are the locally-parsed ones (VNM 18 → 2, SHB 14 → 4, MSN 3 → 0, VPB 2 → 0) while every
Kaggle-parsed ticker reads 100 % never-tried (BVH 27/27, PLX 24/24, POW 13/13, VIB 12/12).

⚠️ **AND THE RETRY IS WORTH ASKING**: TCB Q2-2019's closing cash is printed under the company's
round stamp and no OCR configuration can read it, while the REVIEWED consolidated filing of the
same quarter reads the whole tail cleanly at layer 1.

No PDF, no network, no OCR engine.
"""
import os

import pytest

from web_scraper import cafef_financials as fin
from web_scraper import pdf_ocr_job as job


class _Logger:
    def __init__(self):
        self.lines = []
        self.warnings = []

    def line(self, text):
        self.lines.append(text)

    def log_warning(self, text):
        self.warnings.append(text)


def _task(**over):
    base = dict(exchange="HOSE", symbol="TCB", period="Q2-2019", template="bank",
                path="chosen.pdf", file="chosen.pdf", consolidated="True",
                assurance="audited", cumulative=False,
                index_row={"period": "Q2-2019", "year": "2019", "quarter": "2",
                           "path": "files/HOSE_TCB/chosen.pdf", "consolidated": "True"})
    base.update(over)
    return job.DocumentTask(**base)


@pytest.fixture()
def one_alternate(monkeypatch):
    """The index offers exactly one alternate of the same period and entity."""
    alt = {"period": "Q2-2019", "year": "2019", "quarter": "2", "assurance": "reviewed",
           "file": "reviewed.pdf", "path": "files/HOSE_TCB/reviewed.pdf",
           "consolidated": "True", "annual": "False", "half_year": "False"}
    monkeypatch.setattr(fin.FinancialsBuilder, "alternates",
                        lambda self, e, s, chosen: [alt], raising=False)
    return alt


def test_an_alternate_that_is_not_on_disk_is_now_SAID(one_alternate, monkeypatch, tmp_path):
    """⚠️ THE DEFECT ITSELF. The skip was a bare `continue` above the log line, so a worker
    whose payload held one filing per quarter ran no retry and reported nothing at all — and
    `_alternate_retry` returning `{}` is indistinguishable from "there was no alternate".
    """
    monkeypatch.setattr(fin, "PDFS_DIR", str(tmp_path))
    logger = _Logger()

    origin = job._alternate_retry(fin.FinancialsBuilder(logger=None), _task(),
                                  accepted={"balance_sheet": {}}, band={}, open_ref=None,
                                  logger=logger)

    assert origin == {}
    assert any("in the PDF INDEX and NOT on disk" in w for w in logger.warnings)
    assert any("with_alternates" in w for w in logger.warnings)


def test_nothing_is_said_when_the_document_was_fully_parsed_anyway(one_alternate, monkeypatch,
                                                                   tmp_path):
    """⚠️ A warning on a quarter that came out complete is noise, and noise is how a real one
    stops being read. The retry breaks before it looks at an alternate once all three
    statements are in — the condition here is the same one.
    """
    monkeypatch.setattr(fin, "PDFS_DIR", str(tmp_path))
    logger = _Logger()

    job._alternate_retry(fin.FinancialsBuilder(logger=None), _task(),
                         accepted={r: {} for r in fin.REPORTS}, band={}, open_ref=None,
                         logger=logger)

    assert logger.warnings == []


def test_a_task_with_no_index_row_asks_nothing_and_says_nothing(monkeypatch, tmp_path):
    monkeypatch.setattr(fin, "PDFS_DIR", str(tmp_path))
    logger = _Logger()

    assert job._alternate_retry(fin.FinancialsBuilder(logger=None), _task(index_row=None),
                                accepted={}, band={}, open_ref=None, logger=logger) == {}
    assert logger.warnings == []


def test_an_alternate_ON_disk_is_retried_and_not_warned_about(one_alternate, monkeypatch,
                                                              tmp_path):
    """The other side of the same branch: with the file present the retry runs, so the warning
    must not fire — it reports a question that could NOT be asked, never one that was."""
    monkeypatch.setattr(fin, "PDFS_DIR", str(tmp_path))
    target = tmp_path / "files" / "HOSE_TCB"
    target.mkdir(parents=True)
    (target / "reviewed.pdf").write_bytes(b"%PDF-1.4\n")
    monkeypatch.setattr(fin.FinancialsBuilder, "_parse_cascaded",
                        lambda self, *a, **k: ({"cash_flow": {"layer": "onnx@200",
                                                              "items": 20}}, {}),
                        raising=False)
    monkeypatch.setattr(fin.FinancialsBuilder, "_period_end",
                        lambda self, period: "2019-06-30", raising=False)
    accepted = {"balance_sheet": {}, "income_statement": {}}
    logger = _Logger()

    origin = job._alternate_retry(fin.FinancialsBuilder(logger=None), _task(),
                                  accepted=accepted, band={}, open_ref=None, logger=logger)

    assert "cash_flow" in accepted and origin["cash_flow"]["file"] == "reviewed.pdf"
    assert logger.warnings == []
    assert any("retrying on the reviewed filing" in l for l in logger.lines)
