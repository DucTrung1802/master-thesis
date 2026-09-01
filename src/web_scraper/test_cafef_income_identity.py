"""The income statement's arithmetic gate — `_operating_profit_identity` (TODO `P49`).

The defect: `reconcile` gave every statement one arithmetic test except this one. The balance
sheet is checked on `assets == liabilities + equity` and the cash flow on
`opening + movement + fx == closing`; the income statement was checked on whether a PBT line
EXISTS — and on nothing about whether it is the right number. That is why every `SLD-1`-shaped
defect has landed there, four on record and each found by hand:

    BSR Q3-2019 (`LNB-1`)   PBT written 48,726,111,955 for a printed 624,185,898,676
    TCB Q4-2013 (`PAR-1`)   six cells positive where the filing brackets them
    ACB Q1-2024 (`PAR-1`)   `6_chi_phi_hoat_dong_khac` +907 for a printed -109,907
    BID Q3-2011 (`QUO-1`)   two cells taken from the PRIOR-PERIOD column

⚠️ **THE MEASURED CASE IS THE ONE PINNED BELOW.** BSR Q3-2019 is ACCEPTED at `onnx@300` with
`10_chi_phi_quan_ly_doanh_nghiep` read 200,000 đồng too high, so the cascade stops there and
the exact reading at `onnx@400` is never reached. Two things follow, and both are tests here:
the gate must run on EVERY layer (not only the relaxed ones, as the cash-flow identity does),
and its tolerance cannot be `_equal` — `EQUAL_REL = 1e-5` on 599 bn is ±5,996,952, three
orders of magnitude wider than the error.

⚠️ **THE RISK THIS TRADES AGAINST IS A FALSE REFUSAL**, which turns a `pdf` row into
`missing`. That is the safe direction (§5 rule 2 — absent beats wrong) and it is not free, so
it was measured before it shipped: replayed over the RAW mapped values of the 85 accepted
income statements in `reports/pdf_ocr/` — before `_decumulate`, which is the population the
gate actually sees — **41 answer it, 39 abstain for an unmapped term, 5 fail**. One of the
five is BSR Q3-2019; the other four are CTG quarters written with no magnitude band at all
(`BND-1`), whose residuals are 1.2 bn, 466.7 bn and a round 8,000,000,000,000.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder as F

BANK_PBT = "xi_tong_loi_nhuan_truoc_thue"
BANK_OP = "ix_loi_nhuan_thuan_tu_hoat_dong_kinh_doanh_truoc_chi_phi_du_phong_rui_ro_tin_dung"
BANK_PROV = "x_chi_phi_du_phong_rui_ro_tin_dung"

CORP_OP = "11_loi_nhuan_thuan_tu_hoat_dong_kinh_doanh"
CORP_GROSS = "5_loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu"
CORP_FIN_INC = "7_doanh_thu_hoat_dong_tai_chinh"
CORP_FIN_EXP = "8_chi_phi_tai_chinh"
CORP_SELL = "9_chi_phi_ban_hang"
CORP_ADMIN = "10_chi_phi_quan_ly_doanh_nghiep"
CORP_JV = "phan_lai_lo_trong_cong_ty_lien_doanh_lien_ket"


@pytest.fixture
def b():
    return F.__new__(F)


# ---------------------------------------------------------------- the real cases


def bsr_q3_2019(admin):
    """BSR Q3-2019, verbatim from disk except for the one cell the DPI moves.

    The five components and the printed operating profit are what
    `is_HOSE_BSR.csv` holds; `admin` is `10_chi_phi_quan_ly_doanh_nghiep`, which reads
    89,916,450,279 at `onnx@300` and 89,916,650,279 at `onnx@400`. ⚠️ BSR stores its
    deductions POSITIVE — the filing brackets them and the recogniser drops the bracket —
    which is the whole reason both sign conventions are tried.
    """
    return {
        CORP_GROSS: 741_250_764_806,
        CORP_FIN_INC: 164_818_827_274,
        CORP_FIN_EXP: 44_761_421_794,
        CORP_SELL: 171_696_283_924,
        CORP_ADMIN: admin,
        CORP_OP: 599_695_236_083,
    }


def test_the_measured_case_is_refused(b):
    """`onnx@300` is out by 200,000 — the error the whole item exists for."""
    why = b._operating_profit_identity(bsr_q3_2019(89_916_450_279))
    assert why is not None
    assert "operating profit does not close" in why


def test_the_better_reading_passes(b):
    """…and `onnx@400`, which the cascade could not previously reach, closes exactly."""
    assert b._operating_profit_identity(bsr_q3_2019(89_916_650_279)) is None


def test_the_tolerance_is_not_equal_rel(b):
    """⚠️ THE ERROR IS THREE ORDERS OF MAGNITUDE INSIDE `_equal`, so the gate cannot use it.

    Pinned as an inequality rather than as a constant so that widening `OP_IDENTITY_TOL`
    toward `_equal` fails here rather than silently re-opening the defect.
    """
    mapped = bsr_q3_2019(89_916_450_279)
    assert F._equal(599_695_436_083, mapped[CORP_OP]), "the premise: _equal would accept it"
    assert F.OP_IDENTITY_TOL < 200_000


def test_acb_q1_2024_bank_identity_closes(b):
    """ACB Q1-2024, verbatim: 5,404,530 - 512,217 = 4,892,313 (triệu, scaled to đồng)."""
    assert b._operating_profit_identity({
        BANK_OP: 5_404_530_000_000,
        BANK_PROV: -512_217_000_000,        # the bracket survived — stored negative
        BANK_PBT: 4_892_313_000_000,
    }) is None


def test_a_bank_provision_stored_positive_still_closes(b):
    """⚠️ THE SIGN OF A DEDUCTION IS A PROPERTY OF THE SCAN, NOT OF THE ARITHMETIC.

    The same ACB quarter with the bracket lost — which is exactly what `PAR-1`/`QUO-1` do,
    and what CTG Q1-2019 does on disk. Refusing it would have been a false refusal in 12 of
    the 41 answerable statements measured.
    """
    assert b._operating_profit_identity({
        BANK_OP: 5_404_530_000_000,
        BANK_PROV: 512_217_000_000,
        BANK_PBT: 4_892_313_000_000,
    }) is None


def test_one_sign_bit_does_not_launder_a_wrong_digit(b):
    """⚠️ THE POINT OF ONE BIT PER STATEMENT RATHER THAN ONE PER LINE.

    A digit error shifts BOTH branches by the same amount, so trying two conventions buys a
    scan its bracket back and buys a wrong figure nothing.
    """
    why = b._operating_profit_identity({
        BANK_OP: 5_404_530_000_000,
        BANK_PROV: -512_217_000_000,
        BANK_PBT: 4_892_313_000_009,        # nine đồng out, past OP_IDENTITY_TOL
    })
    assert why is not None


# ------------------------------------------------------------------- abstention


def test_it_abstains_when_a_required_term_is_absent(b):
    """⚠️ A CHECK THAT CANNOT RUN IS NOT A CHECK THAT FAILED — §5 rule 2.

    Asking only for the terms that turned up would let a statement pass by having LOST the
    line that would have failed it, so the requirement is the chart's own set.
    """
    mapped = bsr_q3_2019(89_916_450_279)
    del mapped[CORP_ADMIN]
    assert b._operating_profit_identity(mapped) is None


def test_it_abstains_on_a_chart_with_no_entry(b):
    """`securities` and `insurance` have never met a filing, so nothing is written for them."""
    assert b._operating_profit_identity({
        "ix_tong_loi_nhuan_ke_toan_truoc_thue": 1_000,      # securities PBT
        "tong_cong_tai_san": 9_999,
    }) is None


def test_it_abstains_on_an_unmapped_statement(b):
    """`reconcile` falls back to searching the OCR text when nothing mapped; there is no
    canonical column to read then, and the gate must not invent one."""
    assert b._operating_profit_identity({}) is None


def test_an_optional_term_is_added_when_present_and_never_required(b):
    """⚠️ THE OPTIONAL LINES ARE THE HAZARD THE ABSTAIN RULE EXISTS FOR.

    A filing that prints the joint-venture share and a parse that missed it would fail an
    identity that is actually sound — so they are added where mapped and never demanded.
    """
    mapped = bsr_q3_2019(89_916_650_279)
    mapped[CORP_OP] += 12_345_000
    assert b._operating_profit_identity(mapped) is not None
    mapped[CORP_JV] = 12_345_000
    assert b._operating_profit_identity(mapped) is None


# ------------------------------------------------------------- wiring, not values


def test_the_identity_runs_on_every_layer(b):
    """⚠️ UNLIKE `_cash_flow_identity`, WHICH IS CONFINED TO THE RELAXED LAYERS.

    BSR Q3-2019 is accepted at `onnx@300` — a strict layer — so a gate that only ran on the
    relaxed ones would never see it. `reconcile` takes no flag for this one, and that is the
    property being pinned: it is reachable with every optional argument at its default.
    """
    import web_scraper.cafef_financials as fin
    from web_scraper.cafef_pdf_parser import Row, Statement

    st = Statement(report=fin.INCOME_STATEMENT, pages=[1], unit=1, n_columns=2,
                       rows=[Row(number="", key=f"k{i}", label=f"l{i}", values=[i])
                             for i in range(F.MIN_ROWS)])
    mapped = bsr_q3_2019(89_916_450_279)
    mapped["15_tong_loi_nhuan_ke_toan_truoc_thue"] = 624_185_898_676  # so the PBT check
    why = b.reconcile(st, mapped)                                     # is not what fires
    assert why is not None and "operating profit does not close" in why


def test_the_new_roles_are_not_in_anchors():
    """⚠️ `_anchor`'s BLAST RADIUS IS NOT THIS ITEM'S TO SPEND.

    `ANCHORS` drives the position-independent re-match, so admitting five more accounts to it
    would change which row every statement claims. These are identity roles and nothing else.
    """
    roles = set()
    for col, (plus, minus, optional) in F.OP_IDENTITY.items():
        roles |= {col, *plus, *minus, *optional}
    assert roles & set(F.ANCHORS) == {BANK_PBT}, "only C_PBT was an anchor already"
