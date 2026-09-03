"""`SGN-1` — the de-cumulation crossed a SIGN CONVENTION and returned a number that is neither.

The defect. A deduction's stored sign is a property of the SCAN: the filing prints an expense
in brackets or it does not, and the bracket survives OCR for the whole statement at once.
`reconcile` therefore tries both conventions and accepts either, which is right for a
statement judged ALONE — and `OP_IDENTITY_TOL`'s comment records the measurement behind it.

`_decumulate` is the one place that judges FOUR STATEMENTS TOGETHER. `Q4 = FY - (Q1+Q2+Q3)`
reads its operands out of four separate documents, and when the annual brackets its expenses
while the quarterlies do not, the subtraction crosses that boundary. What comes out has its
HEADLINE FIGURES EXACT and its expense columns wrong, which is why it survived so long:

    CTG Q4-2014, written 2026-09-02 and reverted the same day — PBT 1,822,348,747,831,
    PAT 1,451,441,019,652 and net interest 4,351,526,220,542 each summing to the audited
    FY-2014 total exactly, and 6 of 16 columns wrong. `2_chi_phi_lai` came out
    -17,396,025,395,742 where the FY implies -5,843,164,604,258.

⚠️ **IT IS ALREADY ON DISK.** Measured 2026-09-03 across the 384 `pdf` income-statement rows:
**24 are mixed de-cumulations** — 19 CTG, 5 VIC, every one a Q2 or Q4 — identified by the
closed form that re-signing the priors makes every one of the row's own identities close,
which is not something OCR damage does.

The fix. Each operand's convention is MEASURED from its own printed subtotals
(`deduction_sign`) and the priors are re-signed to the year-to-date's before subtracting. The
bit is never defaulted: both conventions are common and real — 252 negative against 71
positive on disk, with ACB/BID/VCB ~100% negative and **CTG 35 of 36 POSITIVE** — so a corpus
majority would be wrong for a whole ticker at a time.

Where a convention cannot be measured the DEDUCTION COLUMNS ARE DROPPED, never guessed (§5
rule 2). That is not so much a coverage cost of the fix as a quarantine: almost every blind
operand is one of the 24 corrupted rows, and repairing it restores the columns — measured on
CTG 2014, where the Q4 goes from 10 columns with 6 dropped to **16 columns with 0 dropped**
the moment Q2-2014 is re-derived.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder as F


@pytest.fixture(scope="module")
def b():
    return F(logger=None)


def _bank(**over):
    """A bank income statement whose deductions are stored NEGATIVE, and which closes."""
    v = {
        "1_thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu": 10_000,
        "2_chi_phi_lai_va_cac_chi_phi_tuong_tu": -6_000,
        "i_thu_nhap_lai_thuan": 4_000,
        "3_thu_nhap_tu_hoat_dong_dich_vu": 900,
        "4_chi_phi_hoat_dong_dich_vu": -400,
        "ii_lai_lo_thuan_tu_hoat_dong_dich_vu": 500,
        "viii_chi_phi_hoat_dong": -1_500,
        "x_chi_phi_du_phong_rui_ro_tin_dung": -700,
        "ix_loi_nhuan_thuan_tu_hoat_dong_kinh_doanh_truoc_chi_phi_du_phong_rui_ro_tin_dung":
            3_000,
        "xi_tong_loi_nhuan_truoc_thue": 2_300,
        "xii_chi_phi_thue_tndn": -460,
        "xiii_loi_nhuan_sau_thue": 1_840,
    }
    v.update(over)
    return v


def _flip(values, b, template="bank"):
    """The same statement in the OTHER convention — deductions positive."""
    return {c: (-v if c in b.deduction_columns(template) and v is not None else v)
            for c, v in values.items()}


# ─────────────────────────────────────────────────────────────────────────────
# the probe

def test_a_statement_that_brackets_its_expenses_reads_as_negative(b):
    assert b.deduction_sign(_bank(), "bank") == +1


def test_the_same_statement_unbracketed_reads_as_positive(b):
    assert b.deduction_sign(_flip(_bank(), b), "bank") == -1


def test_a_statement_with_no_complete_identity_abstains(b):
    """§5 rule 2 — an unmeasurable convention is absent, never inferred."""
    thin = {"xi_tong_loi_nhuan_truoc_thue": 2_300, "viii_chi_phi_hoat_dong": -1_500}
    assert b.deduction_sign(thin, "bank") is None


def test_a_zero_deduction_casts_no_vote(b):
    """Both conventions agree on zero, so a statement whose only mapped deduction is 0 has
    demonstrated nothing and must not be credited with a convention."""
    v = {"1_thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu": 10_000,
         "2_chi_phi_lai_va_cac_chi_phi_tuong_tu": 0,
         "i_thu_nhap_lai_thuan": 10_000}
    assert b.deduction_sign(v, "bank") is None


def test_contradictory_identities_abstain(b):
    """One identity voting each way means a row is misread, not that a convention is known."""
    v = {"1_thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu": 10_000,
         "2_chi_phi_lai_va_cac_chi_phi_tuong_tu": -6_000,
         "i_thu_nhap_lai_thuan": 4_000,                 # votes NEGATIVE
         "3_thu_nhap_tu_hoat_dong_dich_vu": 900,
         "4_chi_phi_hoat_dong_dich_vu": 400,
         "ii_lai_lo_thuan_tu_hoat_dong_dich_vu": 500}   # votes POSITIVE
    assert b.deduction_sign(v, "bank") is None


def test_the_tolerance_is_four_dong_not_a_relative_one(b):
    """`EQUAL_REL` on a trillion is millions wide; the errors this must not swallow are
    200,000 (BSR Q3-2019) and 23 (BID FY-2016 read at 300 dpi)."""
    within = {"1_thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu": 10_000,
              "2_chi_phi_lai_va_cac_chi_phi_tuong_tu": -6_000,
              "i_thu_nhap_lai_thuan": 4_004}
    assert b.deduction_sign(within, "bank") == +1
    beyond = dict(within, i_thu_nhap_lai_thuan=4_005)
    assert b.deduction_sign(beyond, "bank") is None


# ─────────────────────────────────────────────────────────────────────────────
# the deduction set — DERIVED from the chart, never listed

def test_the_deduction_set_is_derived_from_the_chart_of_accounts(b):
    """`ANCHORS` was a hand-written literal duplicating the role tuples and missed 5 of 7
    roles on every non-bank chart (`TPL-1`). This one cannot fall behind a chart."""
    assert b.deduction_columns("bank") == frozenset({
        "2_chi_phi_lai_va_cac_chi_phi_tuong_tu",
        "4_chi_phi_hoat_dong_dich_vu",
        "6_chi_phi_hoat_dong_khac",
        "viii_chi_phi_hoat_dong",
        "chi_phi_hoat_dong_khac",
        "x_chi_phi_du_phong_rui_ro_tin_dung",
        "7_chi_phi_thue_tndn_hien_hanh",
        "8_chi_phi_thue_tndn_hoan_lai",
        "xii_chi_phi_thue_tndn",
    })


def test_every_chart_yields_a_deduction_set_including_the_two_never_parsed(b):
    """`securities` and `insurance` get no IDENTITIES — but the deduction set is structural
    and comes out of the chart either way."""
    for template, n in (("bank", 9), ("corp", 8), ("securities", 20), ("insurance", 10)):
        assert len(b.deduction_columns(template)) == n, template


def test_the_two_unparsed_charts_have_no_sign_identities(b):
    """`OP_IDENTITY`'s own grounds: an identity written from a chart alone is a guess that
    refuses real statements. No identities means the convention abstains, which drops
    deduction columns — the safe direction, and the one §5 rule 2 asks for."""
    for template in ("securities", "insurance"):
        assert b.sign_identities(template) == ()
        assert b.deduction_sign(_bank(), template) is None


def test_op_identity_is_folded_in_rather_than_retyped(b):
    """The `NST-1` lesson about `ANCHORS`: a second hand-written copy of a table disagrees
    with the first the day one of them moves."""
    subtotals = {sub for sub, _, _, _ in b.sign_identities("bank")}
    assert "xi_tong_loi_nhuan_truoc_thue" in subtotals          # from OP_IDENTITY
    assert "i_thu_nhap_lai_thuan" in subtotals                  # from SIGN_IDENTITIES
    assert "xi_tong_loi_nhuan_truoc_thue" not in {
        sub for sub, _, _ in F.SIGN_IDENTITIES["bank"]}


def test_op_identitys_optional_terms_stay_optional(b):
    """⚠️ The first draft folded them onto the ADDED side, which makes them REQUIRED — an
    identity that needs a line the filing may not print abstains instead of voting."""
    entry = next(e for e in b.sign_identities("corp")
                 if e[0] == "11_loi_nhuan_thuan_tu_hoat_dong_kinh_doanh")
    _, added, deducted, optional = entry
    assert "6_lai_lo_cua_hoat_dong_ban_thanh_ly_bat_dong_san_dau_tu" in optional
    assert "phan_lai_lo_trong_cong_ty_lien_doanh_lien_ket" in optional
    assert not set(optional) & set(added)


# ─────────────────────────────────────────────────────────────────────────────
# the subtraction

def test_matching_conventions_subtract_exactly_as_before(b):
    """The change must be inert where it has nothing to correct — 113 of the 232 replayable
    cumulative statements in the archive are in this case."""
    ytd = {k: v * 2 for k, v in _bank().items()}
    out, flipped, dropped = b._subtract_priors(ytd, {"Q1": _bank()}, "bank")
    assert flipped == [] and dropped == {}
    assert out["2_chi_phi_lai_va_cac_chi_phi_tuong_tu"] == -6_000
    assert out["xiii_loi_nhuan_sau_thue"] == 1_840


def test_a_prior_in_the_other_convention_is_re_signed(b):
    """THE DEFECT. Without this the expense comes out at -12,000 - (+6,000) = **-18,000**
    where the quarter is -6,000 — three times the right figure, in a three-month cell, while
    PAT stays exact."""
    ytd = {k: v * 2 for k, v in _bank().items()}
    out, flipped, dropped = b._subtract_priors(ytd, {"Q1": _flip(_bank(), b)}, "bank")
    assert flipped == ["Q1"] and dropped == {}
    assert out["2_chi_phi_lai_va_cac_chi_phi_tuong_tu"] == -6_000
    assert out["viii_chi_phi_hoat_dong"] == -1_500
    assert out["xiii_loi_nhuan_sau_thue"] == 1_840          # unaffected either way


def test_the_headline_figures_are_right_under_the_defect_which_is_why_it_survived(b):
    """CTG Q4-2014 had PBT, PAT and net interest exact and 6 of 16 columns wrong. A check on
    the headline alone passes a mixed de-cumulation."""
    ytd = {k: v * 2 for k, v in _bank().items()}
    naive, _, _ = b._subtract_priors(ytd, {"Q1": _flip(_bank(), b)}, None)   # None == old code
    assert naive["xiii_loi_nhuan_sau_thue"] == 1_840                # exact
    assert naive["xi_tong_loi_nhuan_truoc_thue"] == 2_300           # exact
    assert naive["2_chi_phi_lai_va_cac_chi_phi_tuong_tu"] == -18_000    # …and wrong
    assert b._subtract_priors(ytd, {"Q1": _flip(_bank(), b)}, "bank")[0][
        "2_chi_phi_lai_va_cac_chi_phi_tuong_tu"] == -6_000   # what it should be


def test_an_unmeasurable_prior_drops_the_deduction_columns_and_keeps_the_rest(b):
    """Not defaulted to the corpus majority: both conventions are common, and a default would
    be wrong for a whole ticker at a time."""
    ytd = {k: v * 2 for k, v in _bank().items()}
    blind = {"2_chi_phi_lai_va_cac_chi_phi_tuong_tu": -6_000,
             "viii_chi_phi_hoat_dong": -1_500,
             "xiii_loi_nhuan_sau_thue": 1_840}
    assert b.deduction_sign(blind, "bank") is None
    out, flipped, dropped = b._subtract_priors(ytd, {"Q1": blind}, "bank")
    assert flipped == []
    assert set(dropped) == {"2_chi_phi_lai_va_cac_chi_phi_tuong_tu", "viii_chi_phi_hoat_dong"}
    assert all("could not be measured" in w for w in dropped.values())
    assert "2_chi_phi_lai_va_cac_chi_phi_tuong_tu" not in out
    assert out["xiii_loi_nhuan_sau_thue"] == 1_840            # a non-deduction column stays


def test_a_zero_contribution_from_a_blind_prior_does_not_taint(b):
    """Both conventions agree on zero, so there is nothing to be wrong about."""
    ytd = {k: v * 2 for k, v in _bank().items()}
    blind = {"2_chi_phi_lai_va_cac_chi_phi_tuong_tu": 0,
             "viii_chi_phi_hoat_dong": -1_500,
             "xiii_loi_nhuan_sau_thue": 1_840}
    assert b.deduction_sign(blind, "bank") is None
    out, _, dropped = b._subtract_priors(ytd, {"Q1": blind}, "bank")
    assert "2_chi_phi_lai_va_cac_chi_phi_tuong_tu" in out
    assert list(dropped) == ["viii_chi_phi_hoat_dong"]
    assert "could not be measured" in dropped["viii_chi_phi_hoat_dong"]


def test_an_unmeasurable_year_to_date_taints_everything_it_could_align_to(b):
    """Nothing can be aligned TO a statement that cannot say which convention it is in."""
    ytd = {"2_chi_phi_lai_va_cac_chi_phi_tuong_tu": -12_000,
           "xiii_loi_nhuan_sau_thue": 3_680}
    assert b.deduction_sign(ytd, "bank") is None
    out, flipped, dropped = b._subtract_priors(ytd, {"Q1": _bank()}, "bank")
    assert flipped == [] and list(dropped) == ["2_chi_phi_lai_va_cac_chi_phi_tuong_tu"]
    assert out["xiii_loi_nhuan_sau_thue"] == 1_840


def test_a_column_a_prior_does_not_carry_is_still_dropped_not_zeroed(b):
    """Rule 1 is unchanged: a line the prior filing printed and this parse missed would
    otherwise have its whole year-to-date value written into a three-month cell."""
    ytd = {k: v * 2 for k, v in _bank().items()}
    prior = {k: v for k, v in _bank().items() if k != "3_thu_nhap_tu_hoat_dong_dich_vu"}
    out, _, _ = b._subtract_priors(ytd, {"Q1": prior}, "bank")
    assert "3_thu_nhap_tu_hoat_dong_dich_vu" not in out


def test_template_none_reproduces_the_pre_sgn1_arithmetic_exactly(b):
    """A caller that has not said which chart it is in must not silently lose columns it used
    to get — the alignment is skipped rather than abstaining into a drop."""
    ytd = {k: v * 2 for k, v in _bank().items()}
    out, flipped, dropped = b._subtract_priors(ytd, {"Q1": _flip(_bank(), b)}, None)
    assert flipped == [] and dropped == {}
    assert len(out) == len(_bank())


def test_re_signing_only_changes_signs_and_only_of_deduction_columns(b):
    aligned, moved = b.align_deductions(_flip(_bank(), b), "bank", +1)
    assert moved and aligned == _bank()


def test_aligning_to_a_convention_already_held_is_a_no_op(b):
    aligned, moved = b.align_deductions(_bank(), "bank", +1)
    assert not moved and aligned == _bank()


def test_a_column_the_ytds_own_subtotals_contradict_is_dropped(b):
    """Rule 4, and the measured case is CTG's FY-2014. `II = 3 + 4` closes under NEITHER
    convention there because `4_chi_phi_hoat_dong_dich_vu` lost a leading digit — read
    -36,683 mn where II implies -936,683 mn — and the de-cumulation produced a service
    expense of **+587.8 bn, positive**, which no gate would have caught: `P49` tests only
    `XI = IX + X`, and that identity closes."""
    ytd = {k: v * 2 for k, v in _bank().items()}
    ytd["4_chi_phi_hoat_dong_dich_vu"] = -80          # II = 1,800 + (-80) closes under neither
    out, _, dropped = b._subtract_priors(ytd, {"Q1": _bank()}, "bank")
    assert set(dropped) == {"3_thu_nhap_tu_hoat_dong_dich_vu",
                            "4_chi_phi_hoat_dong_dich_vu",
                            "ii_lai_lo_thuan_tu_hoat_dong_dich_vu"}
    # ⚠️ the two drop reasons are told apart — one is recoverable by repairing a prior, the
    # other is a misread figure in the SOURCE, and one word for both is the defect shape this
    # module keeps hitting.
    assert all("contradict" in w for w in dropped.values())
    assert out["xiii_loi_nhuan_sau_thue"] == 1_840   # the rest of the statement is untouched
    assert out["2_chi_phi_lai_va_cac_chi_phi_tuong_tu"] == -6_000


def test_every_term_of_a_broken_identity_goes_not_the_one_that_looks_wrong(b):
    """The identity is one equation in three unknowns, so it cannot say WHICH term is
    misread. Picking would be a guess and §5 rule 2 does not allow one."""
    ytd = {k: v * 2 for k, v in _bank().items()}
    ytd["ii_lai_lo_thuan_tu_hoat_dong_dich_vu"] = 500      # the SUBTOTAL is the wrong one here
    _, _, dropped = b._subtract_priors(ytd, {"Q1": _bank()}, "bank")
    assert set(dropped) == {"3_thu_nhap_tu_hoat_dong_dich_vu",
                            "4_chi_phi_hoat_dong_dich_vu",
                            "ii_lai_lo_thuan_tu_hoat_dong_dich_vu"}


def test_an_identity_that_cannot_be_evaluated_is_not_a_contradiction(b):
    """An unmapped term and a zero deduction are absences, and an absence is not a
    contradiction — the same rule `deduction_sign` casts no vote under."""
    ytd = {k: v * 2 for k, v in _bank().items()}
    del ytd["3_thu_nhap_tu_hoat_dong_dich_vu"]
    assert b._contradicted_columns(ytd, "bank") == set()
    zeroed = {k: v * 2 for k, v in _bank().items()}
    zeroed["4_chi_phi_hoat_dong_dich_vu"] = 0
    zeroed["ii_lai_lo_thuan_tu_hoat_dong_dich_vu"] = 12_345      # wrong, but 4 is zero
    assert "ii_lai_lo_thuan_tu_hoat_dong_dich_vu" not in b._contradicted_columns(zeroed, "bank")


def test_rule_4_does_not_fire_on_a_sound_statement(b):
    """It must be inert on the 156 of 174 archived cumulative statements that are sound."""
    assert b._contradicted_columns({k: v * 2 for k, v in _bank().items()}, "bank") == set()
    assert b._contradicted_columns(_flip(_bank(), b), "bank") == set()


def test_the_merge_and_the_builder_share_one_subtraction(b):
    """⚠️ They were two copies written apart, and stayed identical only for as long as nobody
    changed either. `SGN-1` is the change that would have landed in one of them."""
    from web_scraper import pdf_ocr_merge
    ytd = {k: v * 2 for k, v in _bank().items()}
    priors = {"Q1": _flip(_bank(), b)}
    assert (pdf_ocr_merge._decumulate(b, "bank", ytd, priors)
            == b._subtract_priors(ytd, priors, "bank"))


class _Log:
    """`FinancialsBuilder` logs through `log_info`/`log_warning`, not a stdlib logger."""

    def __init__(self):
        self.lines = []

    def log_info(self, m):
        self.lines.append(("INFO", m.strip()))

    def log_warning(self, m):
        self.lines.append(("WARN", m.strip()))

    def log_error(self, m):
        self.lines.append(("ERROR", m.strip()))


def test_the_builders_own_decumulate_path_aligns_and_says_so():
    """`_subtract_priors` is reached two ways and both must carry the template through —
    `build()` passes it from its own resolution, the merge from the run folder. A test on the
    shared function alone would not have caught a call site that forgot it."""
    from web_scraper.cafef_financials import INCOME_STATEMENT
    log = _Log()
    b = F(logger=log)
    prior = _flip(_bank(), b)                       # Q1 in the OTHER convention
    data = {INCOME_STATEMENT: {"Q1-2020": prior,
                               "Q2-2020": {k: v * 2 for k, v in _bank().items()}}}
    meta = {INCOME_STATEMENT: {"Q1-2020": {}, "Q2-2020": {}}}
    b._decumulate(data, {"Q2-2020": True}, meta, {"Q1-2020", "Q2-2020"}, "bank")
    q2 = data[INCOME_STATEMENT]["Q2-2020"]
    assert q2["2_chi_phi_lai_va_cac_chi_phi_tuong_tu"] == -6_000
    assert q2["xiii_loi_nhuan_sau_thue"] == 1_840
    assert meta[INCOME_STATEMENT]["Q2-2020"]["months"] == 3
    assert any("re-signed" in m and "SGN-1" in m for _, m in log.lines)


def test_the_builder_warns_naming_the_reason_when_a_column_is_dropped():
    """A drop is a figure the filing prints and the row will not carry, so it is a WARNING
    that names which columns and why — not a silent absence."""
    from web_scraper.cafef_financials import INCOME_STATEMENT
    log = _Log()
    b = F(logger=log)
    ytd = {k: v * 2 for k, v in _bank().items()}
    ytd["4_chi_phi_hoat_dong_dich_vu"] = -80         # II closes under neither convention
    data = {INCOME_STATEMENT: {"Q1-2020": _bank(), "Q2-2020": ytd}}
    b._decumulate(data, {"Q2-2020": True}, {INCOME_STATEMENT: {}}, {"Q1-2020", "Q2-2020"},
                  "bank")
    warns = [m for lvl, m in log.lines if lvl == "WARN"]
    assert warns and "DROPPED" in warns[0] and "contradict" in warns[0]
    assert "4_chi_phi_hoat_dong_dich_vu" in warns[0]


# ─────────────────────────────────────────────────────────────────────────────
# measured and REJECTED — recorded so neither is re-attempted

def test_the_result_validated_recovery_was_measured_and_not_shipped():
    """⚠️ A blind operand could in principle be resolved by trying BOTH conventions and
    keeping the one whose RESULT closes its own subtotals — the same closed form that
    convicted the 24 rows on disk. Measured 2026-09-03 over the 28 quarters in the archive
    whose columns the drop rule costs: it resolves **1**, leaves 0 ambiguous, and for **27**
    no assignment closes, because the blind operand is a genuinely broken row rather than one
    in the other convention. One quarter does not buy an 8-way search and a second way for a
    figure to be chosen; repairing the corrupted prior recovers all of them instead.

    This records the disproven reasoning and asserts nothing about the code."""


def test_the_candidate_bank_identity_ix_was_measured_and_not_added():
    """⚠️ `IX = Tổng thu nhập hoạt động + VIII` is a real identity the bank chart offers, and
    `tong_thu_nhap_hoat_dong` is mapped on 33 of the bank `pdf` rows. Measured 2026-09-03 it
    rescues **0** rows from abstaining and turns **0** contradictory — every row carrying it
    already votes through another identity. The file's own rule is that a probe recovering
    nothing is pure cost."""
