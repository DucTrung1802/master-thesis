"""`pdf_ocr_job._alternate_retry` — the other filings of the same period, pinned without a PDF.

⚠️ **A QUARTER CAN HAVE MORE THAN ONE FILING AND THIS MODULE READ ONLY ONE.** `documents()`
returns the single best document per period, so a statement every layer refused was recorded
`missing` while another CONSOLIDATED filing of the same quarter sat unread on disk. `build()`
has had this retry since 2026-08-25; `pdf_ocr_job` was documented as not having it *"because it
needs state a one-document run does not have"* — and that is `_decumulate`'s reason, not this
one's. The retry needs the PDF index and the band and `open_ref` `run_document` already holds.

⚠️ **MEASURED ON TCB's Q2-2019.** Its cash flow was refused by all 67 layers because the closing
balance in the AUDITED consolidated filing is printed under the company's round stamp —
47.141.880 read as 171414880 / 17141880 / 19111880 / 17141.880 at 200 / 300+500 / 400+pad6 / 600
dpi, never right, because the ink is over the digits. The REVIEWED consolidated filing of the
same quarter is a different scan and reads `…tiền cuối kỳ 47.141.880 50.050.197` at `onnx@200`.
*"No OCR configuration can read this figure"* was true, and was taken for *"this quarter cannot
be parsed"*, which is a claim about a different thing.
"""
import pytest

from web_scraper import cafef_financials as fin
from web_scraper import pdf_ocr_job as job
from web_scraper.cafef_pdf_parser import BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT, REPORTS


CHOSEN = {"period": "Q2-2019", "year": "2019", "quarter": "2", "consolidated": "True",
          "assurance": "audited", "half_year": "False",
          "path": "files/HOSE_TCB/Q2-2019_audited.pdf", "file": "Q2-2019_audited.pdf"}
ALT = {**CHOSEN, "assurance": "reviewed", "half_year": "True",
       "path": "files/HOSE_TCB/Q2-2019_reviewed.pdf", "file": "Q2-2019_reviewed.pdf"}
# ⚠️ ALT IS A HALF-YEAR FILING AND `ALT_Q` IS THE QUARTERLY ONE, AND THE DIFFERENCE IS THE
# WHOLE SUBJECT OF THE TWO `_cumulative` TESTS BELOW. `documents()` ranks assurance first, so the
# CHOSEN Q2 is typically the reviewed half-year and the alternate the unaudited quarterly — a
# standalone three-month figure, which is what the CSV column holds. Both directions occur, and
# `run_document` must label `months` off the filing the figures CAME from either way.
ALT_Q = {**ALT, "half_year": "False", "assurance": "unaudited",
         "path": "files/HOSE_TCB/Q2-2019_quarterly.pdf", "file": "Q2-2019_quarterly.pdf"}


def _task(**kw):
    base = dict(exchange="HOSE", symbol="TCB", period="Q2-2019", template="bank",
                path="/dev/null", file=CHOSEN["file"], consolidated="True",
                assurance="audited", cumulative=False, index_row=dict(CHOSEN))
    return job.DocumentTask(**{**base, **kw})


class _Builder:
    """A `FinancialsBuilder` stand-in: it hands back canned alternates and canned parses.

    ⚠️ The class is stubbed, never `sys.modules` — `pdf_ocr_batch` reads the package ATTRIBUTE
    once any earlier test has imported the real module, so a stub in `sys.modules` passes alone
    and is ignored in the full suite (CLAUDE.md §6-2-sexquinquagies).
    """

    def __init__(self, alts, parses, raises=()):
        self._alts, self._parses, self._raises = alts, parses, set(raises)
        self.seen = []

    def alternates(self, exchange, symbol, chosen):
        assert (exchange, symbol) == ("HOSE", "TCB")
        assert chosen is not None
        return list(self._alts)

    @staticmethod
    def _period_end(period):
        return None

    def _parse_cascaded(self, path, period_end, template, band, open_ref):
        self.seen.append(path)
        if path in self._raises:
            raise RuntimeError("the alternate is a damaged scan")
        return dict(self._parses.get(path, {})), {}


def _accepted(*reports):
    return {r: (f"row-{r}", f"stmt-{r}", "onnx@200") for r in reports}


@pytest.fixture
def existing(monkeypatch, tmp_path):
    """`os.path.exists` must say yes for the canned alternates and nothing else."""
    monkeypatch.setattr(fin, "PDFS_DIR", str(tmp_path))
    for row in (CHOSEN, ALT, ALT_Q):
        p = tmp_path / row["path"].replace("/", "\\" if "\\" in str(tmp_path) else "/")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"%PDF-1.4\n")
    return tmp_path


def _alt_path(root, row):
    import os
    return os.path.join(str(root), row["path"].replace("/", os.sep))


# ── the recovery ──────────────────────────────────────────────────────────────

def test_an_absent_statement_is_recovered_from_the_other_filing(existing):
    b = _Builder([ALT], {_alt_path(existing, ALT): _accepted(CASH_FLOW)})
    accepted = _accepted(BALANCE_SHEET, INCOME_STATEMENT)
    log = job.CollectingLogger(echo=False)

    origin = job._alternate_retry(b, _task(), accepted, {}, None, log)

    assert set(accepted) == set(REPORTS)
    # ⚠️ The origin carries `_cumulative` since 2026-09-12 — the ALTERNATE's own span flag,
    # which `run_document` needs to label `months` after the shape refusal was removed. It reads
    # `True` here because ALT carries `half_year == "True"` while the TASK is quarterly: the
    # term is a property of the filing the figures came from, never of the chosen document.
    assert origin[CASH_FLOW] == {**ALT, "_cumulative": True}


def test_the_origin_is_per_statement_so_a_row_cannot_name_the_wrong_filing(existing):
    """⚠️ §6-2-terdecies: a row that names the filing `documents()` chose while holding figures
    read from another asserts a document it did not come from."""
    b = _Builder([ALT], {_alt_path(existing, ALT): _accepted(CASH_FLOW)})
    accepted = _accepted(BALANCE_SHEET, INCOME_STATEMENT)

    origin = job._alternate_retry(b, _task(), accepted, {}, None,
                                  job.CollectingLogger(echo=False))

    assert origin[CASH_FLOW]["file"] == "Q2-2019_reviewed.pdf"
    assert BALANCE_SHEET not in origin and INCOME_STATEMENT not in origin


def test_nothing_is_opened_when_every_statement_was_already_accepted(existing):
    """It costs nothing on the success path — the same property `build()`'s retry has."""
    b = _Builder([ALT], {_alt_path(existing, ALT): _accepted(*REPORTS)})
    accepted = _accepted(*REPORTS)

    assert job._alternate_retry(b, _task(), accepted, {}, None,
                                job.CollectingLogger(echo=False)) == {}
    assert b.seen == []


def test_a_task_without_an_index_row_retries_nothing(existing):
    """A caller that built a `DocumentTask` by hand gets today's behaviour and no surprise."""
    b = _Builder([ALT], {_alt_path(existing, ALT): _accepted(CASH_FLOW)})
    accepted = _accepted(BALANCE_SHEET)

    assert job._alternate_retry(b, _task(index_row={}), accepted, {}, None,
                                job.CollectingLogger(echo=False)) == {}
    assert b.seen == []


def test_an_alternate_that_raises_does_not_end_the_run(existing):
    """One bad scan is not a run — the same rule `run_document` applies to the chosen filing."""
    b = _Builder([ALT], {}, raises=[_alt_path(existing, ALT)])
    accepted = _accepted(BALANCE_SHEET, INCOME_STATEMENT)
    log = job.CollectingLogger(echo=False)

    assert job._alternate_retry(b, _task(), accepted, {}, None, log) == {}
    assert any("raised" in line for line in log.lines)


def test_a_missing_file_is_skipped_rather_than_opened(existing):
    gone = {**ALT, "path": "files/HOSE_TCB/not_on_disk.pdf", "file": "not_on_disk.pdf"}
    b = _Builder([gone], {})
    accepted = _accepted(BALANCE_SHEET)

    assert job._alternate_retry(b, _task(), accepted, {}, None,
                                job.CollectingLogger(echo=False)) == {}
    assert b.seen == []


# ── the guards, and all three are `build()`'s rather than a second set ─────────

def test_an_already_accepted_statement_is_never_replaced(existing):
    """The chosen document's reading wins: the retry FILLS gaps and does not re-decide."""
    b = _Builder([ALT], {_alt_path(existing, ALT): _accepted(*REPORTS)})
    accepted = _accepted(BALANCE_SHEET)
    keep = accepted[BALANCE_SHEET]

    origin = job._alternate_retry(b, _task(), accepted, {}, None,
                                  job.CollectingLogger(echo=False))

    assert accepted[BALANCE_SHEET] is keep
    assert BALANCE_SHEET not in origin


def test_a_differing_cumulative_shape_is_CARRIED_now_and_no_longer_refused(existing):
    """⚠️ **THIS TEST PINNED THE OPPOSITE BEHAVIOUR UNTIL 2026-09-12, AND ITS REASON WAS
    RIGHT WHILE ITS REMEDY WAS THE WRONG END OF IT.** It read: *"`half_year` is a property of
    the DOCUMENT, and `_decumulate` subtracts Q1..Q(q-1) from a year-to-date P&L — so a
    quarterly alternate's income statement taken under a half-year chosen document's flag would
    subtract quarters from a figure that never contained them."* Every word of that is true of
    the code that carried it: `run_document` labelled `months` from `task.cumulative`, i.e.
    from the CHOSEN document, for figures read out of a different one.

    **Fixing the term removes the hazard; refusing the statement removed the cell.** And the
    mismatch is usually the right way round — `documents()` ranks assurance-first, so a Q2 is
    typically the REVIEWED half-year while the alternate is the unaudited quarterly, **a
    standalone three-month figure, which is exactly what the CSV column holds.** Measured on
    POW Q2-2019: its income statement parsed cleanly from that alternate and was thrown away.
    """
    # the POW direction: a CUMULATIVE chosen filing, a standalone quarterly alternate
    b = _Builder([ALT_Q], {_alt_path(existing, ALT_Q): _accepted(INCOME_STATEMENT, CASH_FLOW)})
    accepted = _accepted(BALANCE_SHEET)
    log = job.CollectingLogger(echo=False)

    origin = job._alternate_retry(b, _task(cumulative=True), accepted, {}, None, log)

    assert INCOME_STATEMENT in accepted and CASH_FLOW in accepted
    # the ALTERNATE's own flag, not the task's — this is the term `run_document` reads
    assert origin[INCOME_STATEMENT]["_cumulative"] is False
    assert not any("cumulative shape differs" in line for line in log.lines)

    # ⚠️ AND THE OTHER DIRECTION, WHICH IS THE ONE THAT USED TO BE SAFE BY ACCIDENT: a
    # quarterly chosen filing and a HALF-YEAR alternate. The statement is carried now, so the
    # flag has to survive the trip — labelling this `months = 3` would put six months of
    # revenue in a three-month column, silently.
    b2 = _Builder([ALT], {_alt_path(existing, ALT): _accepted(INCOME_STATEMENT)})
    accepted2 = _accepted(BALANCE_SHEET, CASH_FLOW)

    origin2 = job._alternate_retry(b2, _task(cumulative=False), accepted2, {}, None,
                                   job.CollectingLogger(echo=False))

    assert origin2[INCOME_STATEMENT]["_cumulative"] is True


def test_run_document_labels_months_from_the_filing_the_figures_CAME_from():
    """⚠️ **THE HALF THAT MAKES THE LINE ABOVE SAFE, AND WITHOUT IT THE OLD REFUSAL WAS
    CORRECT.** `statement_months` decides the span from a cumulative flag and the statement's
    own quarter column; the flag must be the ALTERNATE's, or a standalone quarterly recovered
    under a reviewed half-year is labelled `months = 6` and then de-cumulated against priors it
    never contained. `statement.quarter_column` needs no such care — `statement` IS the
    alternate's own parse.
    """
    import inspect

    src = inspect.getsource(job.run_document)
    assert 'src["_cumulative"]' in src
    assert "cumulative=bool(task.cumulative)," not in src


def test_the_entity_guard_is_alternates_own_and_is_not_re_implemented_here():
    """⚠️ **THE ENTITY IS FIXED, NOT PREFERRED**, and the rule lives in ONE place. `alternates`
    returns only filings whose `consolidated` equals the chosen one's, so a fallback can never
    quietly change which company a row describes and `allow_parent` stays the only route to a
    standalone filing. A second copy of that test here would be a second thing to keep true —
    `NST-1`'s lesson — so this asserts that the source has no `consolidated` comparison of its
    own instead."""
    import inspect

    src = inspect.getsource(job._alternate_retry)
    assert "consolidated" not in src.split('"""')[2]      # the code, not the docstring
    assert "builder.alternates(" in src


def test_the_band_and_open_ref_reach_the_alternates_cascade(existing):
    """The retry judges what it finds with the SAME band `run_document` was given — nothing is
    accepted here that would not have been accepted from the chosen document."""
    seen = {}

    class _Recording(_Builder):
        def _parse_cascaded(self, path, period_end, template, band, open_ref):
            seen["band"], seen["open_ref"] = band, open_ref
            return super()._parse_cascaded(path, period_end, template, band, open_ref)

    band = {CASH_FLOW: [1, 2, 3]}
    b = _Recording([ALT], {_alt_path(existing, ALT): _accepted(CASH_FLOW)})
    job._alternate_retry(b, _task(), _accepted(BALANCE_SHEET), band, 42,
                         job.CollectingLogger(echo=False))

    assert seen == {"band": band, "open_ref": 42}
