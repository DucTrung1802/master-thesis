"""`JVW-2` — a trailing NOTE REFERENCE was scored as part of the account name.

⚠️ **`JVW-1` ON A DIFFERENT FAMILY OF SPELLINGS, AND FOUND BY REPLAY RATHER THAN BY READING A
FILING.** `.claude/tools/replay_operating_profit.py` asks, of every income statement whose
EVERY deepest-layer reason is `operating profit does not close`, whether the residual equals a
figure the reading PRODUCED and did not map. Measured over VN30's run folders, 2026-09-13:
**19 of 24 do, and 17 of those rows are one account** — the share of profit or loss from
associates and joint ventures, VAS line 24, `OP_IDENTITY`'s optional corp term.

⚠️ **TWO INDEPENDENT DEFECTS SIT ON THOSE CELLS AND EITHER ALONE FIXES NOTHING**, which is
HPG's lesson of 2026-09-09 (six defects, 170 -> 192):

  * the filing prints the NOTE NUMBER beside the account name — "…công ty liên kết **V.4(c)**",
    "…trong liên doanh **VI.6b**" — and `_prefix_trims` trimmed only LEADING words, so the
    footnote was scored as part of the name;
  * and the wording itself is a genuine gap: every spelling says **"chia"** where the chart
    says *phần lãi (lỗ) trong*.

Measured on the 13 distinct keys: **0.600-0.785 today, every one under the 0.80 bar**; the trim
alone leaves all 13 short, the aliases alone leave 9 short, and together all 13 clear it at
**0.849-0.902**.

⚠️ **17 IS NOT A CELL COUNT** (§5 rule 21) — each statement must still close the identity and
pass `sane` — and **15 of the 17 are VNM**, so this is one issuer's chart of accounts.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder
from web_scraper.cafef_pdf_parser import INCOME_STATEMENT


ACCOUNT = "phan_lai_lo_trong_cong_ty_lien_doanh_lien_ket"

# The keys VN30's run folders actually produced for this line, verbatim — OCR noise included
# (`loi_nhuan_oo_…` is "Lợi nhuận/(lỗ)" with the brackets misread, and it must still score).
MEASURED_KEYS = (
    "loi_nhuan_chia_se_tu_lien_doanh_lien_ket",
    "phan_lai_lo_trong_lien_doanh",
    "loi_nhuan_lo_duoc_chia_tu_cong_ty_lien_ket",
    "phan_lai_lo_trong_lien_doanh_vi_6b",
    "loi_nhuan_duoc_chia_tu_cong_ty_lien_ket_v_4_c",
    "lo_loi_nhuan_duoc_chia_tu_cong_ty_lien_ket_v_4",
    "lo_chia_tu_cong_ty_lien_ket_v_4_c",
    "loi_nhuan_oo_duoc_chia_tu_cong_ty_lien_ket_v_4",
    "lai_chia_tu_cong_ty_lien_ket_lien_doanh_v_4_c",
    "lai_lo_chia_tu_cong_ty_lien_ket_lien_doanh_v_4",
    "loi_nhuan_duoc_chia_tu_cong_ty_lien_ket_6_c",
    "loi_nhuan_duoc_chia_tu_cong_ty_lien_ket_v_5_c",
    "lo_lai_chia_tu_cong_ty_lien_ket_lien_doanh_v_4",
)


@pytest.fixture
def b():
    return FinancialsBuilder(logger=None)


def _best(b, key):
    """The score the mapper would actually reach — every variant, every alias, the max."""
    names = [ACCOUNT] + list(b.account_aliases(INCOME_STATEMENT, ACCOUNT))
    return max(b._label_score(n, v) for n in names for v in b._prefix_trims(key))


def test_every_measured_spelling_of_the_associates_line_now_clears_the_bar(b):
    """The measurement, asserted key by key so a regression names the spelling it broke."""
    short = {k: round(_best(b, k), 3) for k in MEASURED_KEYS
             if _best(b, k) < b.SCHEMA_MATCH}

    assert not short, f"under SCHEMA_MATCH={b.SCHEMA_MATCH}: {short}"


def test_a_trailing_note_reference_is_offered_as_its_own_candidate(b):
    """⚠️ **OFFERED, NEVER SUBSTITUTED — and one measured score is why.**
    `phan_lai_lo_trong_lien_doanh_vi_6b` scores **0.785 raw and 0.767 trimmed**, because the
    footnote's characters happened to match the chart's. A trim that REPLACED the key would
    lower that cell's score; `_prefix_trims` yields both and the caller takes the max.
    """
    variants = list(b._prefix_trims("loi_nhuan_duoc_chia_tu_cong_ty_lien_ket_v_4_c"))

    assert "loi_nhuan_duoc_chia_tu_cong_ty_lien_ket_v_4_c" in variants   # the key itself
    assert "loi_nhuan_duoc_chia_tu_cong_ty_lien_ket" in variants         # and the trim
    # the raw key is still the one a scorer may prefer
    assert (b._label_score(ACCOUNT, "phan_lai_lo_trong_lien_doanh_vi_6b")
            > b._label_score(ACCOUNT, "phan_lai_lo_trong_lien_doanh"))


def test_the_trim_stops_at_two_words_and_never_eats_a_real_one(b):
    """⚠️ **THE FLOOR IS THE SECOND GUARD, AND THIS TEST IS WHY IT IS THREE AND NOT TWO.**
    `NOTE_REF_RE` matches single letters and small numbers, which are also the shape of a
    fragment — and `_label_score`'s CONTAINMENT shortcut can reach a short account from a
    two-word one, so the trim would INVENT a match rather than reveal one. At a floor of two
    `tien_v_1` trimmed to `tien_v`; at three it is left alone. Three costs nothing on the
    measured population: every key this was built for trims to SIX words.
    """
    # ⚠️ assert on the SET, not on the last yield — `_prefix_trims` also drops LEADING words
    # (BID's FY-2016 carry), so `v_1` is a legitimate variant of this key and the thing to
    # test is that the TRAILING branch contributed nothing.
    assert "tien_v" not in set(b._prefix_trims("tien_v_1"))
    # a real word that LOOKS like nothing is not a note reference
    assert list(b._prefix_trims("chi_phi_ban_hang"))[-1] != "chi_phi_ban"
    # and a genuine footnote tail of several tokens goes in one pass
    assert "loi_nhuan_duoc_chia_tu_cong_ty_lien_ket" in \
        list(b._prefix_trims("loi_nhuan_duoc_chia_tu_cong_ty_lien_ket_v_4_c"))


def test_NST_1_the_aliases_are_scoped_to_the_income_statement_because_they_HAVE_to_be(b):
    """⚠️ **THE REPORT KEY IS LOAD-BEARING HERE AND NOT TIDINESS.** Across all twelve charts
    `laichiatucongtylienketliendoanh` scores **0.820** against `corp/balance_sheet`'s
    `dautuvaocongtylienketliendoanh` and `lailochiatucongtylienketliendoanh` **0.794** — over
    and near the bar. Both rivals are BALANCE-SHEET accounts, which an income-statement row is
    never scored against, so the scoping removes the competition instead of tolerating it.
    """
    assert b.account_aliases(INCOME_STATEMENT, ACCOUNT)
    # the same account on the BALANCE SHEET gets only the unscoped entries
    scoped = set(b.account_aliases(INCOME_STATEMENT, ACCOUNT))
    other = set(b.account_aliases("balance_sheet", ACCOUNT))
    assert "laichiatucongtylienketliendoanh" in scoped
    assert "laichiatucongtylienketliendoanh" not in other


def test_the_existing_JVW_1_aliases_still_reach_MSNs_spellings(b):
    """⚠️ A fix for one family must not displace the other. `JVW-1`'s four MSN spellings."""
    for key in ("lai_tu_cac_cong_ty_lien_ket", "phan_lai_tu_cac_cong_ty_lien_ket",
                "loi_nhuan_tu_cac_cong_ty_lien_ket",
                "phan_lai_tu_cac_cong_ty_lien_ket_13_c"):
        assert _best(b, key) >= b.SCHEMA_MATCH, key
