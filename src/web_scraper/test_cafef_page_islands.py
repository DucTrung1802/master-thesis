"""`_drop_islands` — the gap is measured along the statement's own run, not in a ±1 window.

The defect: a form code has to survive OCR to anchor a statement, and on an old scan usually
only ONE of its pages keeps one. TCB's Q3-2013 balance sheet runs pages 2-4 — all three
classified `balance_sheet` by title — and only page 4 kept "B02a/TCTD-HN" (page 2's reads
"B020/TCID-HN", page 3's "B022/TCTD-HN"). The pruner measured page 2's distance from that
single anchor, found 2 > 1, and discarded it as an island — **while keeping page 3, which sits
between them**, which is not what a gap looks like.

What went with page 2 was the FIRST page of the statement and the one place in the whole
filing that prints `Đơn vị tính: triệu đồng` (the income statement prints no unit line at all
and the cash flow's pages do not repeat it). Every figure of all three statements was then
read as đồng — a uniform 10^6 error that reconciles perfectly against itself, and that only
`sane` could refuse. The quarter had been `missing` since the ticker was first parsed.

The fix walks outward from the form-coded pages through pages the SAME report already owns,
and keeps the ±1 tolerance that admits a continuation page whose own header OCR destroyed.

⚠️ VCB's Q2-2023 is the case this pruner exists for and it must still be pruned: its
balance-sheet title matched pages 6-7, two pages clear of the real statement on 9-11, and
page 8 belongs to no report — so the walk stops and the islands stay islands.
"""
import pytest

from web_scraper.cafef_pdf_parser import (BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT,
                                          PdfParser)


def _pages(spec):
    """{index: page} from `[(kind, from_form), ...]`, indexed from 0 like `scan`."""
    return {i: {"kind": kind, "from_form": ff, "text": "", "words": [], "width": 595.0}
            for i, (kind, ff) in enumerate(spec)}


def _kinds(pages):
    return [pages[i]["kind"] for i in sorted(pages)]


def test_the_statements_own_contiguous_run_is_kept_however_far_the_form_code_is():
    """TCB Q3-2013: pages 2-4 are one run and only the last kept its form code."""
    pages = _pages([
        (None, False),                       # page 1 — the cover
        (BALANCE_SHEET, False),              # page 2 — the unit line lives here
        (BALANCE_SHEET, False),              # page 3
        (BALANCE_SHEET, True),               # page 4 — the only surviving form code
        (INCOME_STATEMENT, False),
    ])
    PdfParser._drop_islands(pages)

    assert _kinds(pages) == [None, BALANCE_SHEET, BALANCE_SHEET, BALANCE_SHEET,
                             INCOME_STATEMENT]


def test_a_page_two_clear_of_the_run_is_still_an_island():
    """VCB Q2-2023: pages 6-7 match the title, the statement is on 9-11, page 8 is neither."""
    pages = _pages([
        (BALANCE_SHEET, False),              # 6 — the auditor's report wearing the words
        (BALANCE_SHEET, False),              # 7
        (None, False),                       # 8 — the gap, and it is what makes them islands
        (BALANCE_SHEET, True),               # 9
        (BALANCE_SHEET, False),              # 10
        (BALANCE_SHEET, False),              # 11
    ])
    PdfParser._drop_islands(pages)

    assert _kinds(pages) == [None, None, None,
                             BALANCE_SHEET, BALANCE_SHEET, BALANCE_SHEET]


def test_the_one_page_tolerance_either_side_is_unchanged():
    """A continuation page whose header OCR destroyed still joins its statement — that is what
    the ±1 has always been for, and the walk must not narrow it."""
    pages = _pages([
        (BALANCE_SHEET, False),              # the untitled continuation before the anchor
        (BALANCE_SHEET, True),
        (BALANCE_SHEET, False),              # …and after it
    ])
    PdfParser._drop_islands(pages)

    assert _kinds(pages) == [BALANCE_SHEET] * 3


def test_a_report_with_no_form_coded_page_is_left_alone():
    """No anchor means nothing to measure a distance from, and inventing one would prune a
    statement that is merely badly scanned. `_drop_after_notes` is the flag for that case."""
    pages = _pages([(CASH_FLOW, False), (None, False), (CASH_FLOW, False)])
    PdfParser._drop_islands(pages)

    assert _kinds(pages) == [CASH_FLOW, None, CASH_FLOW]


def test_the_walk_does_not_cross_into_another_report():
    """Contiguity is per REPORT: an income-statement page between two balance-sheet pages does
    not join them into one run, so a balance-sheet page beyond it is still an island."""
    pages = _pages([
        (BALANCE_SHEET, False),              # island
        (INCOME_STATEMENT, False),           # not the balance sheet's
        (INCOME_STATEMENT, False),
        (BALANCE_SHEET, True),               # the real statement
    ])
    PdfParser._drop_islands(pages)

    assert _kinds(pages)[0] is None
    assert _kinds(pages)[3] == BALANCE_SHEET


# ──────────────────────────────────────────────────────────────────────────────
# The unit block, which the dropped page also decides: a layer that multiplies every
# figure of a statement by a million may not be the layer that skips its arithmetic.
# ──────────────────────────────────────────────────────────────────────────────


def test_a_unit_layer_demands_the_cash_identity():
    """⚠️ MEASURED ON TCB Q3-2013. At 200 dpi its cash flow reads both balances correctly
    (22,621,969 and 25,611,174 mn) and the NET MOVEMENT as **205** where the page prints
    2,989,205 — the detector box starts inside the figure. `reconcile` proves the closing
    balance and `sane` probes that same closing balance, so BOTH GATES PASS and the layer would
    have ended the cascade with a wrong cell, while the identical document at 300 dpi reads the
    line correctly. The identity catches it in one line."""
    import inspect

    from web_scraper.cafef_financials import FinancialsBuilder

    source = inspect.getsource(FinancialsBuilder._parse_cascaded)
    assert "layer.relax_totals\n" in source or "layer.relax_totals" in source
    assert "or layer.unit_from_document" in source


def test_the_unit_block_offers_both_resolutions():
    """A statement that needs the document's unit may also need the resolution — TCB Q3-2013's
    income statement returns 7 split figures at 200 dpi and none at 300 — and until 2026-08-29
    the unit block was 200 dpi only, so such a statement had nowhere to land."""
    from web_scraper.cafef_financials import FinancialsBuilder

    unit_layers = [l for l in FinancialsBuilder.LAYERS if l.unit_from_document]
    assert {l.dpi for l in unit_layers} == {200, 300}
    # …and the label repair still runs before the bare unit fix, at each resolution
    for dpi in (200, 300):
        names = [l.name for l in unit_layers if l.dpi == dpi]
        assert names.index(f"onnx@{dpi}+unit+tail") < names.index(f"onnx@{dpi}+unit")
