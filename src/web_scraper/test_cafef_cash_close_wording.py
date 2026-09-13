r"""`CCB-2` — the schema omits `các khoản` and the filings print it, so the closing balance missed.

VAS form B03-DN prints *"Tiền và các khoản tương đương tiền cuối kỳ"*. The corp schema column is
`hdtc_tien_va_tuong_duong_tien_cuoi_ky_70_50_60_61`, which the scorer bares to
`tienvatuongduongtiencuoiky` — **no `cackhoan`** — so the canonical printed wording scored below
the bar and `reconcile` answered `no closing cash balance`.

⚠️ **MEASURED BY REPLAY, NOT BY GPU** (2026-09-13): over every refused cash flow in
`reports/pdf_ocr/`, mapping the stored rows under `cash_wording=True` left **88 statements with no
closing column**, and of the rows that carried a FIGURE and mapped nowhere the top spelling was
the canonical wording itself (6 rows), then `…tuong_duong_cuoi_ky` (4, OCR dropped one `tiền`).
With these aliases the same replay reads **55**, and the unmapped-with-a-figure rows fall from 33
across 15 spellings to 24 across 10.

⚠️ **AND THE BANK CHART SAYS IT A THIRD WAY** — `…tien_tai_thoi_diem_cuoi_ky` — so a filing
printing the plain `cuối kỳ` missed there too. Both bare forms are keys.

⚠️ **THIS IS THE ALIAS ROUTE AND THE `CASH_CLOSE` NEEDLE IS STILL NOT IT.**
`replay_cash_close.py` reverted a needle change for exactly this reason: a needle satisfies
`reconcile`'s GATE and fills NO COLUMN, writing a `pdf` row whose closing-cash cell is blank —
which is `CCB-1`, 100 rows of it. An entry here puts the printed figure IN the column.
"""
from web_scraper.cafef_financials import FinancialsBuilder


def _builder():
    return FinancialsBuilder(logger=None)


def test_the_canonical_vas_wording_is_an_alias_of_the_corp_column():
    """The spelling 6 measured rows print, against the column that bares without `các khoản`."""
    alts = _builder().cash_wording_aliases("tienvatuongduongtiencuoiky")

    assert "tienvacackhoantuongduongtiencuoiky" in alts
    assert "tienvacackhoantuongduongcuoiky" in alts, "OCR drops one `tiền` — 4 measured rows"


def test_the_bank_chart_form_is_a_key_of_its_own():
    """⚠️ A chart that spells the column differently needs its own KEY, not a second alias —
    the lookup is exact on the account, so an alias on the corp key reaches no bank statement."""
    builder = _builder()
    bank = "tienvacackhoantuongduongtientaithoidiemcuoiky"

    assert builder.cash_wording_aliases(bank), "the bank closing column has no aliases"
    assert "tienvatuongduongtiencuoiky" in builder.cash_wording_aliases(bank)


def test_every_aliased_key_also_carries_the_period_gate():
    """⚠️ **THE UNSAFE HALF OF THIS CHANGE IS AN ALIAS WITHOUT THE GATE.** The opening and
    closing lines are the same words apart from one, so a key offering aliases and no period
    test can hand the closing slot the OPENING figure — a well-formed wrong reading, which is
    the kind nothing downstream can tell from a correct one.
    """
    for key in FinancialsBuilder.CASH_WORDING:
        assert key in FinancialsBuilder.CASH_WORDING_PERIOD, key
        want, other = FinancialsBuilder.CASH_WORDING_PERIOD[key]
        assert {want, other} == {"cuoi", "dau"}, (key, want, other)


def test_the_opening_and_closing_alias_sets_are_disjoint():
    """No spelling may answer for both balances — that is the mirror `CASH_WORDING_PERIOD`
    exists to stop, checked here at the table rather than only at the score."""
    builder = _builder()
    closing = set(builder.cash_wording_aliases("tienvatuongduongtiencuoiky"))
    opening = set(builder.cash_wording_aliases("tienvatuongduongtiendauky"))

    assert closing and opening
    assert not (closing & opening), closing & opening
    assert all("cuoi" in a for a in closing), closing
    assert all("dau" in a for a in opening), opening


def test_an_unrelated_account_is_offered_nothing():
    """⚠️ `NST-1` — the table must not widen into accounts it was not measured on."""
    builder = _builder()
    for account in ("luuchuyentienthuantrongky", "loinhuantruocthue",
                    "khauhaotscdvabdsdt"):
        assert builder.cash_wording_aliases(account) == (), account
