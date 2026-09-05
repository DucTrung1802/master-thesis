"""`CBS-1` — the balance sheet reads the closing cash balance a SECOND time.

⚠️ **THIS IS `CFV-1`'s ANSWER, AND THE HOLE IT CLOSES IS THAT A STRICT-LAYER CASH FLOW WAS
CHECKED BY NOTHING.** `_cash_flow_identity` rides with `relax_totals`, so on a strict layer
`reconcile` demands only that a closing balance EXISTS. FPT's Q3-2025 reached disk on
2026-09-05 with one the company seal had broken, and what caught it was an arithmetic screen
run over the CSV afterwards — not a gate.

VAS ties B03's code 70 to B01's code 110, so the same printed quantity is read twice, on two
pages, by two OCR passes, and the balance sheet's reading is corroborated a third time by its
own two components summing to it.

Measured over the accepted `corp` documents in `reports/pdf_ocr/` before it shipped:
**193 agree, 15 differ (9 distinct quarters), 74 abstain because a figure is absent** — and
every one of the nine is a genuine defect, several of them `pdf` on disk at the time
(FPT Q3-2023 reads **795** for a printed 7,153,625,069,795; VIC Q1-2025 reads
491,938,000,000 for 32,491,938,000,000).

⚠️ **`bank` IS ABSENT BECAUSE IT IS MEASURED NOT TO HOLD.** A bank balance sheet's cash line
is notes and coin; its cash flow closes on a far wider aggregate. Across the five parsed bank
tickers the two agree on **0 of 226** quarters, at ratios of 3.5x to 18x.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer, ocr_key, parse_key
from web_scraper.cafef_pdf_parser import CASH_FLOW, Row, Statement

CORP_TOTAL, CORP_PARTS = FinancialsBuilder.BS_CASH_LINE["corp"]
CLOSE = "hdtc_tien_va_tuong_duong_tien_cuoi_ky_70_50_60_61"


@pytest.fixture(scope="module")
def b():
    return FinancialsBuilder(logger=None)


def bs_row(total, parts=None):
    out = {CORP_TOTAL: total}
    if parts is not None:
        out.update(dict(zip(CORP_PARTS, parts)))
    return out


def cash_statement():
    """A statement long enough to clear `MIN_ROWS`, carrying nothing this gate reads.

    The closing balance is handed in through `mapped` instead, so these tests measure the
    gate and never the row matcher.
    """
    rows = [Row(label=f"x{i}", key=f"x{i}", number="", values=[i + 1, i + 1])
            for i in range(FinancialsBuilder.MIN_ROWS)]
    return Statement(report=CASH_FLOW, pages=[1], unit=1, n_columns=2, rows=rows)


# ── the reference ────────────────────────────────────────────────────────────────────────

def test_bank_has_no_entry_and_therefore_no_reference(b):
    """⚠️ Measured, not argued: 0 of 226 bank quarters agree, at ratios of 3.5x to 18x."""
    assert "bank" not in FinancialsBuilder.BS_CASH_LINE
    assert b.balance_sheet_cash("bank", bs_row(5, (2, 3))) == (None, False)


def test_securities_and_insurance_are_absent_on_op_identitys_grounds(b):
    """Neither chart has ever met a filing, so an entry would be a guess (§5 rule 2)."""
    for t in ("securities", "insurance"):
        assert t not in FinancialsBuilder.BS_CASH_LINE
        assert b.balance_sheet_cash(t, bs_row(5, (2, 3))) == (None, False)


def test_the_components_decide_whether_the_reference_may_be_written(b):
    """The gate takes the figure as it stands; the repair needs the third reading."""
    assert b.balance_sheet_cash("corp", bs_row(100, (40, 60))) == (100, True)
    assert b.balance_sheet_cash("corp", bs_row(100, (40, 59))) == (100, False)
    assert b.balance_sheet_cash("corp", bs_row(100, (40, None))) == (100, False)
    assert b.balance_sheet_cash("corp", bs_row(100)) == (100, False)


def test_no_balance_sheet_means_no_reference(b):
    assert b.balance_sheet_cash("corp", None) == (None, False)
    assert b.balance_sheet_cash("corp", {}) == (None, False)


# ── the gate ─────────────────────────────────────────────────────────────────────────────

def test_a_disagreeing_closing_balance_is_refused(b):
    """FPT Q1-2025's own numbers: the seal-damaged reading against the printed figure."""
    why = b.reconcile(cash_statement(), {CLOSE: 6_755_015_214_252},
                      bs_cash=6_755_645_214_252)
    assert why and "disagrees with the balance sheet" in why


def test_the_filings_own_rounding_still_passes(b):
    """⚠️ Three FPT quarters on disk differ by exactly 1 dong — Q3-2013, Q1-2017, Q1-2020.

    An EXACT test would refuse all three, which is why the comparison is `_equal` and not the
    `OP_IDENTITY_TOL` bar `P49` uses: the two figures are printed on different pages and each
    is rounded on its own.
    """
    assert b.reconcile(cash_statement(), {CLOSE: 2_292_011_717_348},
                       bs_cash=2_292_011_717_347) is None


def test_no_reference_leaves_the_statement_judged_exactly_as_before(b):
    """§5 rule 2: a check that cannot run is absent, never a pass — and never a refusal."""
    assert b.reconcile(cash_statement(), {CLOSE: 6_755_015_214_252}, bs_cash=None) is None


# ── the repair ───────────────────────────────────────────────────────────────────────────

def test_the_repair_replaces_a_damaged_reading(b):
    row = {CLOSE: 6_755_015_214_252}
    b._cash_close_from_balance_sheet(row, 6_755_645_214_252)
    assert row[CLOSE] == 6_755_645_214_252


def test_the_repair_never_invents_a_column_the_statement_did_not_produce(b):
    """A cash flow that yielded no closing balance has not been READ.

    Giving it one from another page would turn `no closing cash balance` — a refusal that is
    usually right — into an acceptance nothing on its own page supports.
    """
    for row in ({}, {CLOSE: None}):
        before = dict(row)
        b._cash_close_from_balance_sheet(row, 6_755_645_214_252)
        assert row == before


def test_the_repair_does_nothing_without_a_reference(b):
    row = {CLOSE: 1}
    b._cash_close_from_balance_sheet(row, None)
    assert row == {CLOSE: 1}


def test_the_cross_check_is_re_run_after_the_cascade():
    """⚠️ **WITHOUT THE END-OF-CASCADE PASS, 20 % OF THIS GATE IS INERT.**

    Inside the layer loop the reference exists only where the balance sheet is ALREADY
    accepted, and `REPORTS` order guarantees that only within ONE layer — a cash flow accepted
    at layer 1 is judged before a balance sheet accepted at layer 2. Measured over the accepted
    `corp` documents in `reports/pdf_ocr/`: **59 of 288**. VIC's Q3-2014 is one, and it
    reproduced clean in a regression run before this was added: closing 5,220,100,054,978
    against a balance-sheet cash line of 5,209,108,954,978 that the same run read and that its
    own two components confirm.

    This asserts the pass EXISTS in `_parse_cascaded` rather than driving a whole document; the
    comparison it makes is the same `_equal` the tests above pin.
    """
    import inspect

    src = inspect.getsource(FinancialsBuilder._parse_cascaded)
    body = src[src.index("self.absent_rows = {}") - 2500:src.index("self.absent_rows = {}")]
    assert "balance_sheet_cash" in body, (
        "the cross-check must be re-run after the layer loop, or a cash flow accepted before "
        "its own balance sheet is never compared with it")
    assert "del accepted[CASH_FLOW]" in body


# ── the cascade ──────────────────────────────────────────────────────────────────────────

def test_every_cashbs_layer_is_late_and_forces_the_identity():
    """⚠️ The safety argument is the POSITION, and `verify_cash` is what proves the figure.

    Only a cash flow that has defeated every reading of its own page may reach these, and the
    substituted figure must then satisfy `opening + movement + fx` EXACTLY — which is why
    `_parse_cascaded` forces `verify_cash` on for the flag rather than leaving it to
    `relax_totals`.
    """
    layers = FinancialsBuilder.LAYERS
    block = [i for i, l in enumerate(layers, 1) if l.cash_close_from_bs]
    strict = [i for i, l in enumerate(layers, 1) if l.is_strict]
    assert block, "the block exists"
    assert min(block) > max(strict), "no layer reading the page as printed may run after it"


def test_the_flag_is_a_widening_and_is_counted_as_one():
    """A `ParseLayer` carrying it may not be reported strict — the omission `is_strict`'s own
    docstring predicts, and which had already happened once to `notes_tail`/`notes_head`."""
    assert not ParseLayer("x", "onnx", 200, cash_close_from_bs=True).is_strict


def test_the_flag_is_not_a_parse_key_and_costs_no_ocr_pass():
    """It changes the MAPPING, so two layers differing only in it share one parse — and the
    cascade's OCR-pass count, which is the progress denominator, must not move."""
    a = ParseLayer("a", "onnx", 200)
    c = ParseLayer("c", "onnx", 200, cash_close_from_bs=True)
    assert parse_key(a) == parse_key(c)
    assert ocr_key(a) == ocr_key(c)
