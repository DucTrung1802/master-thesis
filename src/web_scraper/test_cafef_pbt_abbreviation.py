r"""`PBT-2` — the filing may abbreviate `lợi nhuận` to `LN`, and no needle knew that spelling.

`reconcile` refuses an income statement outright when it cannot find a profit-before-tax figure
(`no profit before tax`), and that reason is the **largest single block over de-cumulation
ROOTS** — measured 2026-09-13 over VN30: **19 of the 73 roots, carrying 47 cells between them**
once each root's dependents are counted.

⚠️ **AND 12 OF THE 19 HOLD THE ROW AND CANNOT MAP IT.** PLX prints
`Tổng LN kế toán trước thuế (50=30+40)` on every Q1 income statement from 2016 on — the row is
read with all three period figures — and its slug is `tong_ln_ke_toan_truoc_thue_50_30_40`,
which `C_PBT` had no spelling for. 9 of the 12 are exactly that slug.

⚠️ **`JVW-2`'s TRAILING TRIM ALREADY DID THE OTHER HALF**: the line code `50_30_40` is three
note-shaped tokens, so `_prefix_trims` yields `tong_ln_ke_toan_truoc_thue` on its own. This is
HPG's two-defects-on-one-cell again with one of them already fixed, which is why an alias alone
is enough here and was not enough for `JVW-2`.

⚠️ **THE REPLAY THAT FOUND IT FIRST ANSWERED `0 of 19 PRESENT`, AND THAT WAS ITS OWN REGEX** —
`truoc\s*thue` cannot match a slug, which separates words with an UNDERSCORE. The same shape as
`replay_operating_profit`'s `0 of 24`: a confident null from the tool, not from the data.
"""
from web_scraper.cafef_financials import FinancialsBuilder


def _builder():
    return FinancialsBuilder(logger=None)


def test_the_abbreviated_slug_resolves_to_a_C_PBT_key_after_the_note_trim():
    """PLX's measured slug, end to end: the trim strips the line code, the alias matches."""
    builder = _builder()
    key = "tong_ln_ke_toan_truoc_thue_50_30_40"
    candidates = list(builder._prefix_trims(key)) + [key]

    assert "tong_ln_ke_toan_truoc_thue" in candidates, candidates
    assert [c for c in candidates if c in FinancialsBuilder.C_PBT] == [
        "tong_ln_ke_toan_truoc_thue"]


def test_the_spelled_out_forms_still_resolve():
    """⚠️ An alias added for one ticker must not be the only one that works — every chart's
    own spelling is still in `C_PBT`, bank through insurance."""
    for key in ("xi_tong_loi_nhuan_truoc_thue", "tong_loi_nhuan_truoc_thue",
                "tong_loi_nhuan_ke_toan_truoc_thue",
                "15_tong_loi_nhuan_ke_toan_truoc_thue",
                "ix_tong_loi_nhuan_ke_toan_truoc_thue",
                "25_tong_loi_nhuan_ke_toan_truoc_thue"):
        assert key in FinancialsBuilder.C_PBT, key


def test_the_text_needle_carries_the_abbreviation_too():
    """`reconcile` falls back to searching the OCR TEXT when the rows did not map, so the two
    tables have to learn the spelling together — a needle in one and not the other is how a
    fix works on the mapped path and silently fails on the fallback."""
    assert "tong ln ke toan truoc thue" in FinancialsBuilder.PBT
    assert "tong loi nhuan ke toan truoc thue" in FinancialsBuilder.PBT


def test_the_alias_is_specific_enough_not_to_answer_for_another_account():
    """⚠️ `NST-1` — a loose key invents a match. `tong_ln_ke_toan_truoc_thue` must not be
    produced by the trims of an account that is NOT profit before tax, and the two nearest
    neighbours on PLX's own statement are the test: both mention `LN` and neither is PBT."""
    builder = _builder()
    for key in ("ln_thuan_tu_hoat_dong_kd_30_20_21_22_2",
                "ln_sau_thue_tndn_60_50_51_52",
                "ln_gop_ve_ban_hang_va_cung_cap_dich_vu"):
        candidates = list(builder._prefix_trims(key)) + [key]
        assert not [c for c in candidates if c in FinancialsBuilder.C_PBT], (key, candidates)
