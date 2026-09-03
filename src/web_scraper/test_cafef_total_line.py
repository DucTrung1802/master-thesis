"""`EQW-1` — a row printed "TỔNG <account>" is that account's own total line.

⚠️ **THE DEFECT IS NOT A TIE-BREAK, AND THAT MATTERS FOR WHERE THE FIX GOES.** `table_rows`
glues a section header onto the sub-line beneath it when the header prints no figure of its
own, so a bank balance sheet arrives with BOTH

    VỐN CHỦ SỞ HỮU Vốn        78,626   <- header VIII + sub-line "Vốn của TCTD"
    TỔNG VỐN CHỦ SỞ HỮU      179,501   <- the line the account actually names

Both CONTAIN the account `von_chu_so_huu`, both take the flat 0.95, and both reach an edge —
so `_contains_at_an_edge` cannot separate them either. ⚠️ `_align` then cannot pick the right
one at ANY score: it is a monotonic alignment maximising the TOTAL, the chart lists the
subtotal BEFORE its own sub-lines, and the filing prints the total line AFTER them — so
matching the total row would push every sub-line out of the alignment and always lose on sum.
The fix therefore has to reach `_anchor`, which re-matches the subtotals without regard to
position, and it does so by scoring the total row ABOVE the containment floor.

Measured 2026-09-03 by replaying every archived statement's stored `row_dump` (a mapping
change cannot alter what the OCR read): **166 of 16,920 mappings move, 23 statements, 0
columns lost, 0 gained** — 22 TCB balance sheets and BID's Q2-2020 income statement, and
every one of them is a REPAIR adjudicated by the filing's own arithmetic.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder


@pytest.fixture(scope="module")
def b():
    return FinancialsBuilder(logger=None)


def test_the_total_line_outscores_a_merged_header(b):
    """The whole defect, in the two rows that carry it."""
    account = "von_chu_so_huu"
    merged = b._label_score(account, "von_chu_so_huu_von")      # header + sub-line
    total = b._label_score(account, "tong_von_chu_so_huu")      # the account's own total
    assert merged == pytest.approx(0.95), "the merged row still takes the containment floor"
    assert total > merged, "the total line must outrank the merged header"
    assert total == pytest.approx(b.TOTAL_LINE_SCORE)


def test_tong_cong_is_the_same_word(b):
    """A filing writes "TỔNG" and "TỔNG CỘNG" for one thing."""
    assert b._label_score("von_chu_so_huu", "tong_cong_von_chu_so_huu") == \
        pytest.approx(b.TOTAL_LINE_SCORE)


def test_an_exact_account_still_wins_its_own_row(b):
    """⚠️ The ONE pair of accounts in the twelve charts where X and tongX are both accounts.

    `b_no_phai_tra_va_von_chu_so_huu` (the "B." section header) is offered the grand-total row
    at TOTAL_LINE_SCORE, and `tong_no_phai_tra_va_von_chu_so_huu` matches it EXACTLY. Scoring
    the total line below 1.0 is what keeps the grand total with the account that names it —
    `NST-1` is the same collision from the other side.
    """
    row = "tong_no_phai_tra_va_von_chu_so_huu"
    assert b._label_score("no_phai_tra_va_von_chu_so_huu", row) == \
        pytest.approx(b.TOTAL_LINE_SCORE)
    assert b._label_score(row, row) == pytest.approx(1.0)
    assert b._label_score(row, row) > b.TOTAL_LINE_SCORE


def test_it_is_an_equality_never_a_containment(b):
    """⚠️ A merged row cannot reach the rule — that is what bounds it.

    "TỔNG VỐN CHỦ SỞ HỮU Vốn điều lệ" is a total line MERGED with the next item, and it must
    keep the ordinary containment score rather than the total-line one; otherwise the rule
    would re-create the very defect it removes, one row further down.
    """
    s = b._label_score("von_chu_so_huu", "tong_von_chu_so_huu_von_dieu_le")
    assert s == pytest.approx(0.95), "a merged total row is not the account's total line"


def test_a_short_account_is_not_offered_the_rule(b):
    """MIN_CONTAINS guards this the way it guards containment: a short name is inside too
    many others to prove anything."""
    assert len("tien_mat") < b.MIN_CONTAINS
    assert b._label_score("tien_mat", "tong_tien_mat") < b.TOTAL_LINE_SCORE


def test_exactly_one_chart_pair_can_collide():
    """⚠️ THE BLAST RADIUS IS A FACT ABOUT THE CHARTS, NOT AN ARGUMENT — so it is asserted.

    Measured over all 12 charts of accounts: exactly ONE (X, tongX) pair are both accounts of
    one chart. A second one appearing is not necessarily wrong, but it is a new situation this
    rule was never measured against, and it should be looked at rather than discovered later.
    """
    b = FinancialsBuilder(logger=None)
    pairs = []
    for tpl in ("bank", "corp", "securities", "insurance"):
        for rep in ("balance_sheet", "income_statement", "cash_flow"):
            schema = b.schema_of(tpl, rep)
            bare = {acct.replace("_", ""): col for col, acct in schema}
            for col, acct in schema:
                twin = bare.get("tong" + acct.replace("_", ""))
                if twin and twin != col:
                    pairs.append((tpl, rep, col, twin))
    assert pairs == [("bank", "balance_sheet",
                      "b_no_phai_tra_va_von_chu_so_huu",
                      "tong_no_phai_tra_va_von_chu_so_huu")], pairs
