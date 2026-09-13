"""`OPB-2` — an operand the issuer NEVER FILED is permanent; one that lost to the parser is work.

`_quarter_priors` refuses a cumulative Q2/Q4 whose Q1..Q(q-1) operands are not `pdf` rows, and it
printed **one sentence for both kinds**: `Q1-2008 is 'absent' on disk` is what a lost operand says
and what an operand that does not exist says. `TRC-1`'s shape, inside the merge — a permanent
answer and a winnable one filed under one message, and the permanent one is the kind nobody
re-opens while the winnable one is the kind that should be re-asked every time the parser changes.

⚠️ **MEASURED OVER VN30, 2026-09-13**: of the **89** blocked dependent income statements,
**16 (18 %) name an operand that is not a task at all** — SSB **8**, every Q1 from 2008 to 2015,
plus BID 3, VPB 2 and one each for POW, SAB and TPB — so those 16 cells are `missing` and
`missing` is the correct answer (§5 rule 24). The other **73 are parser work**, PLX 19 and SHB 10.

⚠️ **`filed` CHANGES THE WORDING AND NEVER THE VERDICT.** A function whose ANSWER depended on how
much the caller happened to know would be worse than the conflation it fixes.
"""
from web_scraper import pdf_ocr_merge


class _Builder:
    """Just enough of `FinancialsBuilder` for `_quarter_priors`: the rows already on disk."""

    def __init__(self, rows):
        self._rows = rows

    def _existing(self, _exchange, _symbol, _template, _report):
        return self._rows


def _why(rows, period, filed=None):
    priors, why = pdf_ocr_merge._quarter_priors(
        _Builder(rows), "HOSE", "SSB", "bank", period, {}, filed=filed)
    assert priors is None, priors
    return why


def test_an_operand_that_is_not_a_filing_says_so_and_says_it_is_permanent():
    """SSB Q4-2008's shape: the issuer filed no Q1-2008, so nothing can ever de-cumulate it."""
    why = _why({}, "Q4-2008", filed={"2008-Q4"})

    assert "NEVER FILED" in why
    assert "`missing` is the correct answer" in why
    assert "OPB-2" in why


def test_an_operand_that_was_filed_and_lost_keeps_the_ordinary_wording():
    """PLX Q4-2018's shape: Q1-2018 IS a filing, it simply has no `pdf` row yet — work."""
    why = _why({}, "Q4-2018", filed={"2018-Q1", "2018-Q4"})

    assert "NEVER FILED" not in why
    assert "is `absent` on disk" in why


def test_without_filed_the_wording_is_exactly_what_it_was():
    """⚠️ The parameter is optional, and a caller that does not pass it must see no change —
    every existing run folder's recorded reason has to stay comparable."""
    assert _why({}, "Q4-2008") == "Q1-2008 is `absent` on disk"


def test_a_missing_row_is_distinguished_from_an_absent_one_either_way():
    """`missing` is a written blank and `absent` is no row at all; both refuse, and the message
    keeps saying which — that distinction predates this change and must survive it."""
    rows = {"Q1-2018": {"source": "missing"}}
    assert "is `missing` on disk" in _why(rows, "Q4-2018", filed={"2018-Q1", "2018-Q4"})
    assert "is `absent` on disk" in _why({}, "Q4-2018", filed={"2018-Q1", "2018-Q4"})


def test_the_period_forms_are_translated_and_not_compared_raw():
    """⚠️ The merge speaks `Q1-2008` and every planner speaks `2008-Q1`. Comparing them raw
    would report EVERY operand as never filed, which is the failure that looks like a fix."""
    assert pdf_ocr_merge._as_quarter("Q1-2008") == "2008-Q1"
    assert "NEVER FILED" not in _why({}, "Q4-2008", filed={"2008-Q1"})
