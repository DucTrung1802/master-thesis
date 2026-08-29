# src\web_scraper\cafef_financials.py

# ===== Standard Library =====
import csv
import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

# ===== Local / Custom Modules =====
from web_scraper.cafef_pdf_parser import (
    BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT, REPORTS, PdfParser, Statement,
)
from utils.constants import CAFEF_FINANCIALS_TICKERS, CAFEF_RAW_DATA_DIR
from utils.exceptions import MissingSourceDataError
from utils.inputs import optional_file, require_dir, require_file


@dataclass(frozen=True)
class ParseLayer:
    """One way to read a filing: an OCR method + its settings, plus optional matching relaxations.

    A statement is attempted layer by layer (see `FinancialsBuilder.LAYERS`) until one reconciles;
    a filing that defeats the fast default is retried at higher resolution, on a different engine,
    and finally with relaxed total-label recovery, before the CafeF-tab fallback. Adding a new way
    to parse a stubborn filing is adding a `ParseLayer` to the list — nothing else changes.

    Fields:
      * `engine` / `dpi` — the OCR configuration handed to `PdfParser` (onnx or tesseract, and the
        render resolution). Higher DPI separates lines a low-res scan merges.
      * `relax_totals` — recover a statement's SUBTOTAL lines from label VARIANTS the strict
        fuzzy match rejects. Two of them:
          - the balance sheet's grand totals, for a filing that prints "TỔNG CỘNG TÀI SẢN" where
            the schema expects "TỔNG TÀI SẢN", or that merges the grand-total label into the line
            above it (ACB Q1-2019);
          - the cash flow's opening and closing balances, which the filing DATES ("TẠI NGÀY 31
            THÁNG 3") where the schema names the period ("tại thời điểm cuối kỳ") — see
            CASH_TAIL. Because a mis-taken closing balance would be caught by nothing else, a
            cash flow recovered this way must additionally agree with the components printed
            beneath it (`_closing_breakdown`).
        Off for the strict layers so every other quarter is untouched; the relaxed layers run
        ONLY after the strict ones fail, and the reconcile + magnitude gates still guard whatever
        they recover.
      * `relax_components` — widen what counts as a CASH-EQUIVALENT in the closing balance's
        printed breakdown (`CASH_COMPONENT_RELAXED`). The narrow set knows cash, deposits,
        securities and gold; a bank also parks cash in TREASURY BILLS, and a filing that lists
        `tín phiếu` as its fifth component has that line silently dropped from the sum. The check
        then reports a balance that disagrees with its own components and refuses a statement
        whose every figure is right — six ACB quarters (see CASH_COMPONENT_RELAXED).

        Scoped as a LAYER rather than widened globally on purpose: adding a marker to the narrow
        set would re-judge all 65 quarters at once, and a component wrongly swept in makes the
        sum OVERSHOOT, turning a passing quarter into a rejected one. Here only a statement that
        has already failed every existing layer is ever judged this way.
      * `realign_rows` — re-pair labels with figures when the scan puts every numeric box a
        CONSTANT distance above the text box of its own printed line. Past `Y_TOL` the two never
        group, and `table_rows` then hands each figure to the label line ABOVE it, sliding the
        whole statement by one row with every digit read correctly — BID's Q1-2021 balance sheet
        reported total assets of 10,770,158 (the cash line) against a printed 1,558,887,407.
        The offset is MEASURED per statement, by maximising the number of lines that hold both a
        label and a figure, and is left at zero unless it beats the unshifted page by half again
        (`PdfParser._value_row_offset`). Off for every other layer: this re-reads a page that
        already parses, so it must never judge a statement that reconciles today.
      * `tail_continuation` — admit a statement's sparse FINAL page. `_fill_continuations`
        absorbs an unidentifiable page into the statement running through it only if the page
        holds `MIN_TABLE_WORDS` figures, and a statement's LAST page is legitimately below that:
        it prints the closing rows and then the signature block. For the cash flow that is the
        page carrying the CLOSING BALANCE, so losing it fails the statement outright — BID's
        Q1-2012 consolidated cash flow puts codes 53/54/55 on page 7 with **13 figures against a
        threshold of 15**, every digit correct, and the quarter was refused for "no closing cash
        balance". The threshold is NOT lowered — it is what keeps a narrative page out, and a
        tail page is mostly signature; instead the page is admitted on POSITIVE evidence, by
        carrying the statement's own closing line (`PdfParser.TAIL`), and the run ends there.
      * `label_wrap` — reassemble a label that WRAPPED AROUND its own value line. `table_rows`
        builds a label from the lines ABOVE the figures, which is wrong when the figures sit
        BETWEEN its two halves, and BID's Q1-2012 cash flow does that twice. Two repairs, both
        needed: a line holding only the filing's item code (`53`) no longer counts as a gap that
        clears the pending label, and a value line with no label OF ITS OWN takes the label-only
        line just beneath it — widening a branch that previously required the carry to be empty
        too. ⚠️ **The second is the one that matters for correctness**: without it the closing
        balance is keyed on the opening line's wording, the ordered walk hands BOTH cash figures
        to the wrong accounts, and `reconcile` and `sane` BOTH PASS — a wrong figure written as
        `pdf`, which is the one failure mode this parser must not have. Rides with
        `tail_continuation`: the same filing needs both, and neither is any use alone.
      * `cash_extra_terms` — let the cash-flow identity count the reconciling lines a filing
        prints BETWEEN its two balances that the chart of accounts has no column for. The
        identity is `closing = opening + movement + fx`, and a bank that ABSORBS another bank
        gains cash that is none of those three: BID prints such a line in three separate years
        (MHB 1,477,340 in 2015 and 3,004,011 in 2016, LienVietPostBank 1,540,994 in 2017). Its
        FY-2016 consolidated cash flow reads every figure correctly at `crop_pad=6` —
        55,806,145 + 6,711,633 + 3,004,011 = 65,521,789, exact to the đồng — and was refused
        for `fx not mapped`, because the fourth term has nowhere to go.

        ⚠️ **THE TERM IS COUNTED, NEVER WRITTEN, AND THAT IS THE WHOLE DESIGN.** Writing it to
        `hdtc_vi_dieu_chinh_anh_huong_cua_thay_doi_ty_gia` would put merger cash in the FX
        column, and the identity would then CONFIRM the wrong account because the arithmetic is
        right — measured 2026-08-27, CLAUDE.md §6-2-vicies. So this admits the figure to the
        CHECK and leaves the column empty, which is what §5 rule 2 asks of a number nothing can
        attribute.

        ⚠️ **THE MATCHING GUARD IN `_recover_totals` IS NO LONGER TIED TO THIS FLAG — `P39`,
        2026-08-27.** It shipped that way and the wiring was the defect: the positional FX
        claim was refused on the THREE layers carrying `cash_extra_terms` and allowed on the
        other forty-four, `onnx@200+relax` (layer 5) among them. By the time it was read off
        `cf_HOSE_BID.csv` it had already written merger cash into the FX column twice —
        Q4-2015 `1,477,340` and Q2-2017 `1,540,994`, each confirmed by the identity to the
        đồng. A row must now name itself FX to fill that column **on every layer**, and this
        flag decides only whether the span is COUNTED, never whether the guard applies.

        ⚠️ Only the CURRENT-period cell counts, never `_first_value`'s fall-through: BID's 2016
        column leaves the MHB line blank and prints 1,477,340 beside it in the 2015 comparative,
        and taking that would break an identity that closes exactly without it. A term OCR could
        not read is skipped, the identity then misses, and the statement is refused — which is
        the correct outcome and not a loss.
      * `crop_pad` — how far outside a detected box to crop before RECOGNISING it (onnx only,
        in points; `None` = the engine default of 2). The detector sometimes starts its box
        INSIDE a number and the leading digit is simply not in the crop, so the recogniser
        cannot read what it was never shown: ACB's Q3-2023 reads 93.261.018 as 261.018 at every
        DPI and correctly at 6. Raising the DPI cannot help — the missing pixels are missing at
        any resolution — which is why this is its own knob rather than another DPI step.
    """
    name: str
    engine: str
    dpi: int
    relax_totals: bool = False
    relax_components: bool = False
    relax_split_tail: bool = False
    join_digits: bool = False
    title_over_form: bool = False
    loose_form_code: bool = False
    realign_rows: bool = False
    notes_boundary: bool = False
    relax_merged_seam: bool = False
    tail_continuation: bool = False
    label_wrap: bool = False
    unit_from_document: bool = False
    annual_tail: bool = False
    cash_extra_terms: bool = False
    crop_pad: Optional[float] = None

# ⚠️ **THE EARLIEST QUARTER ANY FILING MAY CONTRIBUTE — a DECISION, taken 2026-08-24.**
# Filings before Q1-2008 are blocked at the INPUT, so no pre-2008 document is ever opened and
# no pre-2008 row is ever written. Three measured reasons, none of them "old data is bad":
#
#   1. **A pre-2008 Q4 income statement cannot be de-cumulated and never could.** CafeF holds
#      an ANNUAL report for VCB 2006 and 2007 and no quarterly filing at all, so `_decumulate`
#      has no Q1..Q3 to subtract and correctly drops the row every single time. The work is
#      guaranteed waste, and it was measured being wasted on 2026-08-24.
#   2. **The cheapest thing in the cascade is a document never opened.** VCB's Q4-2006 annual
#      cost **29.7 minutes** — its cash flow is absent, and `_parse_cascaded` only breaks when
#      all three statements are accepted, so the two that WERE read at layer 1 paid for the
#      whole 21-layer escalation. That single blocked document is 65 % of that run's wall clock.
#   3. **The price panel starts later anyway.** Nothing downstream joins a 2006 fundamental.
#
# ⚠️ It is a FLOOR ON THE PERIOD, not on the file date: a 2009 annual report covering FY-2008
# is kept, because the period it contributes is Q4-2008. ⚠️ And it DELETES: VCB carries 3 `pdf`
# rows before Q1-2008 (Q4-2006 BS, Q4-2007 BS, Q4-2007 CF) which an authoritative run will drop
# with the rest of that grid. That is the intended trade and it is stated rather than hidden.
FINANCIALS_PERIOD_MIN = "Q1-2008"


def _period_key(period: str) -> Tuple[int, int]:
    """`"Q3-2014"` -> `(2014, 3)`, the order every period comparison here uses."""
    q, y = period.split("-")
    return (int(y), int(q[1]))


PDFS_DIR = os.path.join(CAFEF_RAW_DATA_DIR, "pdfs")
FIN_DIR = os.path.join(CAFEF_RAW_DATA_DIR, "financials")

# financials/
# ├── schema/<template>_<report>.csv          the 4 charts of accounts x 3 statements
# ├── statements/<template>/<report>/<EXCHANGE>_<SYMBOL>.csv
# └── templates.csv                           ticker -> template + cash-flow method
#
# The TEMPLATE is a folder, not a column, because the four charts of accounts share no line
# items: a directory is then schema-homogeneous — every file under `statements/bank/` has the
# same 90 columns and they mean the same thing. Mixing them would give a table whose columns
# depend on which row you are reading.
SCHEMA_DIR = os.path.join(FIN_DIR, "schema")
STATEMENTS_DIR = os.path.join(FIN_DIR, "statements")
TEMPLATES_INDEX = os.path.join(FIN_DIR, "templates.csv")

# The report's short code, prefixed onto the file name. Inside the tree the directory already
# says which statement a file holds, so the prefix is redundant there — but a file LEAVES its
# directory constantly (opened in an editor tab, attached to a mail, copied beside two others
# for a diff) and three tabs all reading `HOSE_ACB.csv` say nothing about which statement each
# one is. The directory stays the authority; this only makes the name self-describing.
REPORT_PREFIX = {BALANCE_SHEET: "bs", INCOME_STATEMENT: "is", CASH_FLOW: "cf"}


def statement_path(template: str, report: str, exchange: str, symbol: str) -> str:
    """The single place a statement CSV's path is formed — the writer and every reader share
    it, so the naming can change here without hunting down the readers that hardcoded it.

    Reads `STATEMENTS_DIR` at CALL time, not import time, because the experiment harnesses
    re-point that module global to an absolute path (they run from their own directory, where
    the relative default resolves to nothing).
    """
    return os.path.join(STATEMENTS_DIR, template, report,
                        f"{REPORT_PREFIX[report]}_{exchange}_{symbol}.csv")

DATA_COLS = ["symbol", "exchange", "template", "period", "year", "quarter",
             # WHICH PARSE LAYER READ THIS STATEMENT ("onnx@200", "tesseract@200",
             # "onnx@300+relax"). The cascade already knows it — `_parse_cascaded` records the
             # winning layer per statement — and it was being thrown away at the CSV boundary,
             # which is exactly where it is worth having: a quarter carried by a RELAXED layer
             # passed a different set of gates than one read at onnx@200, and the layer mix is
             # how the cost of a re-run is predicted (a tesseract quarter is minutes, an
             # onnx@200 one is seconds). Blank for a `cafef` or `missing` row — no layer parsed
             # it. Not to be confused with `cash_flow_method` below, which is the COMPANY's
             # accounting choice (direct/indirect), nothing to do with OCR.
             "method",
             "source",
             "publish_date", "assurance",
             # ⚠️ WHICH ENTITY THIS ROW DESCRIBES — "True" for a CONSOLIDATED filing
             # (hợp nhất, parent + subsidiaries), "False" for the STANDALONE one (công ty
             # mẹ / riêng lẻ). It became a column the day `documents()` gained
             # `allow_parent`: before that every row was consolidated by construction, so
             # the fact was implicit and safe to omit. It is neither once both can appear
             # in one file — two entities in one column, with nothing saying which is
             # which, is the same defect as sourcing a figure from a web tab. ⚠️ **Read
             # it before comparing two quarters of the same ticker.**
             "consolidated",
             "cash_flow_method", "unit", "n_columns",
             "document",
             # Share capital read from the filing's "Vốn cổ phần" note — a per-DOCUMENT fact
             # (one filing, one note), so all three statements of a quarter carry the same
             # numbers, like publish_date. Blank when the quarter came from CafeF's tabs (they
             # have no share field) or the note could not be read.
             "shares_authorized", "shares_issued", "shares_outstanding"]
INDEX_COLS = ["exchange", "symbol", "template", "cash_flow_method", "sector",
              "industry_group_code", "industry_group_slug"]


def _blank(v):
    """A missing share count is an empty cell, never 0 — 0 shares is a real, wrong figure."""
    return "" if v is None else v


class FinancialsBuilder:
    """Build quarterly financial statements for a ticker from its LOCAL PDF archive.

        raw_data/cafef/pdfs/     (in)   downloaded by CafeFPdfScraper
        raw_data/cafef/financials/<report>/<EXCHANGE>_<SYMBOL>.csv   (out)

    One row per quarter, one column per line item, keyed by the label the filing prints —
    the filing has no item codes of its own (CafeF's 300/411/800 are CafeF's numbering, not
    the document's), so the printed label is the only identifier that actually exists.

    Nothing is written unless it reconciles. A parsed statement must balance against its own
    printed subtotals and be of a sane magnitude beside its neighbours; a quarter that fails
    is left blank. A wrong figure is worse than a missing one — and there are three ways to
    be wrong that no single check catches:

      * UNITS      — most filings are in Triệu VNĐ, VCB's 2009 ones in plain đồng. Read the
                     wrong one and every figure is out by 10^6 while reconciling perfectly.
      * CUMULATIVE — the semi-annual report prints ONLY the Jan-Jun column, so its income
                     statement is not the standalone quarter (VCB Q2-2024 prints PBT 20,835bn
                     where the quarter is 10,116bn). The cumulative figures balance against
                     each other, and 2x sits well inside any magnitude band.
      * OCR        — a misread digit. Reconciliation catches it only when the line takes part
                     in an identity.
    """

    # Which document to read for a quarter. Consolidated only — the parent-company report
    # covers a different entity. Prefer the reviewed/audited version when a quarter was filed
    # twice, since the later document restates the earlier.
    ASSURANCE_RANK = {"audited": 0, "reviewed": 1, "unaudited": 2}

    # Subtotals used to reconcile, by the words the filing prints. Matched with spaces and
    # underscores stripped, so OCR losing a space cannot defeat them.
    TOTAL_ASSETS = ("tong tai san", "tong cong tai san")
    TOTAL_RESOURCES = ("tong no phai tra va von chu so huu", "tong cong nguon von")
    TOTAL_LIABILITIES = ("tong no phai tra",)
    TOTAL_EQUITY = ("von chu so huu", "tong von chu so huu")
    PBT = ("tong loi nhuan truoc thue", "loi nhuan truoc thue",
           "tong loi nhuan ke toan truoc thue")
    NET_CF = ("luu chuyen tien thuan trong ky", "luu chuyen tien thuan trong nam")
    CASH_CLOSE = ("tien va cac khoan tuong duong tien tai thoi diem cuoi",
                  "tien va tuong duong tien cuoi ky")
    # ⚠️ THE OPENING BALANCE IS THE ONE THING THE CLOSING NEEDLE MUST NEVER ANSWER,
    # and on a fuzzy sliding window it very nearly does: the two lines are the same words with
    # one different, and the opening is printed FIRST, so first-hit-wins hands it over.
    # Measured on VIC Q1-2026 at 0.90 against a 0.85 threshold, which is TPL-1's predicted
    # failure arriving. Passed to `Statement.find` as a disqualifier, so a row saying "dau ky"
    # is skipped whatever it scores. Both the period and the annual wordings, because an
    # annual report says "dau nam" - see ANNUAL_WORDING, which makes the same distinction on
    # the scoring side.
    CASH_OPEN_WORDS = ("dau ky", "dau nam", "dau quy")

    # The same subtotals, as CANONICAL columns (schema/<template>_<report>.csv). Looked up here
    # a line cannot be lost to OCR damage — which is what most rejections were.
    #
    # ⚠️ **ONE TUPLE PER ROLE, COVERING ALL FOUR CHARTS OF ACCOUNTS — not one per template.**
    # These were BANK column names only until 2026-08-28, and `reconcile`, `_probe` and
    # `_cash_flow_identity` take no `template` argument to pick a set with, so on a non-bank
    # filing every one of them fell through to `Statement.find`'s fuzzy TEXT search. That is
    # not a graceful degradation: measured on VIC Q1-2026 (`corp`), the closing-cash needle
    # matched the OPENING balance row and `reconcile` passed on 72,226,561 where the filing
    # closes at 54,750,360 — a wrong figure, not a refusal (`TPL-1`, `CRP-1`).
    #
    # A UNION rather than a per-template table because the lookup is
    # `next(c for c in C_X if c in mapped)` and a chart of accounts answers at most ONE name
    # per role — verified for all 4 templates x 3 reports in
    # `test_cafef_financials_anchors.py`, which is what makes the union equivalent to the
    # table and lets it ship without changing four signatures. The bank entries stay FIRST so
    # a reader can see which came from where, and `test_bank_anchor_resolution_is_unchanged`
    # pins that the bank charts resolve exactly what they resolved before.
    #
    # ⚠️ Two roles are genuinely ABSENT from a chart and that is a fact about the filing,
    # never a gap to be filled by the nearest thing: `securities` prints no "lưu chuyển tiền
    # thuần trong kỳ" (so `_cash_flow_identity` sums the section subtotals instead, which is
    # what that fallback is for), and `insurance` prints NO CLOSING CASH LINE AT ALL — it ends
    # at HDTC_39 "đầu kỳ" and HDTC_40 (FX). That one needs a schema repair, not a tuple entry.
    C_ASSETS = ("tong_tai_san",                      # bank
                "tong_cong_tai_san")                 # corp / securities / insurance
    C_RESOURCES = ("tong_no_phai_tra_va_von_chu_so_huu",        # bank
                   "tong_cong_nguon_von",                       # corp / insurance
                   "tong_cong_no_phai_tra_va_von_chu_so_huu")   # securities
    C_LIABILITIES = ("tong_no_phai_tra", "no_phai_tra",   # bank
                     "c_no_phai_tra")                     # corp / securities / insurance
    C_EQUITY = ("viii_von_chu_so_huu", "von_chu_so_huu",  # bank
                "d_von_chu_so_huu")                       # corp / securities / insurance
    # ⚠️ The non-bank charts keep the filing's own line NUMBER on this column ("15." for corp,
    # "IX." for securities, "25." for insurance), so the unprefixed name matches none of them.
    C_PBT = ("xi_tong_loi_nhuan_truoc_thue", "tong_loi_nhuan_truoc_thue",   # bank
             "tong_loi_nhuan_ke_toan_truoc_thue",
             "15_tong_loi_nhuan_ke_toan_truoc_thue",                        # corp
             "ix_tong_loi_nhuan_ke_toan_truoc_thue",                        # securities
             "25_tong_loi_nhuan_ke_toan_truoc_thue")                        # insurance
    C_NET_CF = ("hdtc_iv_luu_chuyen_tien_thuan_trong_ky",            # bank
                "luu_chuyen_tien_thuan_trong_ky",
                "hdtc_luu_chuyen_tien_thuan_trong_ky_50_20_30_40")   # corp / insurance
    C_CASH_CLOSE = ("hdtc_vii_tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_cuoi_ky",  # bank
                    "tien_va_tuong_duong_tien_cuoi_ky",
                    "hdtc_vi_tien_va_cac_khoan_tuong_duong_tien_cuoi_ky",       # securities
                    "hdtc_tien_va_tuong_duong_tien_cuoi_ky_70_50_60_61")        # corp

    MIN_ROWS = 12          # a statement with fewer parsed rows than this is not a statement

    # The cash flow's two balance lines are printed with the ACTUAL DATE where the chart of
    # accounts says "đầu kỳ" / "cuối kỳ": ACB prints "TIỀN VÀ CÁC KHOẢN TƯƠNG ĐƯƠNG TIỀN TẠI
    # NGÀY 1 THÁNG 1" for the opening balance and "… TẠI NGÀY 31 THÁNG 3" for the closing one.
    # That is a wording difference in the FILING, not OCR damage, so it does not improve with
    # resolution: both score ~0.72 against their account and neither reaches SCHEMA_MATCH, which
    # loses the closing balance — the only subtotal a cash flow can be reconciled on.
    #
    # The two share this whole prefix and differ only in the date, so the prefix alone cannot
    # say which is which. It does not have to: the schema lists đầu kỳ before cuối kỳ and the
    # filing prints them in that order, so the ordered walk in `map_to_schema` assigns the first
    # to the opening and the second to the closing on its own. `_anchor`, which has no order to
    # lean on, separates them by length instead — "…taingay31thang3" spans 0.95 of the closing
    # account against "…taingay"'s 0.77.
    CASH_TAIL = "tienvacackhoantuongduongtientai"

    # The closing balance is followed by its own components ("Tiền và các khoản tương đương tiền
    # gồm có: — Tiền mặt…, — Tiền gửi thanh toán tại NHNN, — Tiền gửi tại các TCTD"), which is a
    # SECOND statement of the same figure and the only independent check a cash flow carries.
    # "gomco" is the "gồm có:" header itself, which carries no figure of its own but which OCR
    # merges with the FIRST component below it — ACB's Q1-2024 reads the whole block as
    # "tháng 3 tiền và các khoản tương đương tiền gồm có" while holding the tiền-mặt value
    # 6,470,319. Without it that first component is skipped and the sum falls short by exactly
    # that amount.
    CASH_COMPONENT = ("tienmat", "tiengui", "chungkhoan", "vangbac", "gomco")

    # ⚠️ A BANK'S FIFTH CASH EQUIVALENT IS A TREASURY BILL, and the narrow set above cannot see
    # it. ACB lists "Tín phiếu Chính phủ đủ điều kiện chiết khấu với NHNN" (and later just "Tín
    # phiếu Ngân hàng Nhà nước") beneath its closing balance, and dropping that line from the sum
    # is not a small error — it is the whole reason six quarters were refused, each of which adds
    # up EXACTLY once the line is counted:
    #
    #   Q4-2010  36,663,890 + 1,646,997 = 38,310,887      Q3-2018  24,329,090 + 2,484,412 = 26,813,502
    #   Q1-2012  46,762,473 + 5,252,163 = 52,014,636      Q3-2019  30,543,805 +   516,752 = 31,060,557
    #   Q1-2013  11,292,341 + 4,440,122 = 15,732,463      Q1-2020  38,337,214 + 1,272,095 = 39,609,309
    #
    # Reached only through a `relax_components` layer, never by default. The direction of the
    # error is what makes that matter: a marker that is too narrow makes the sum fall SHORT and
    # refuses a sound statement (recoverable — a later layer can still take it), while one that
    # is too wide makes it OVERSHOOT and can refuse a quarter that passes today. Widening the
    # default set would put every quarter at that second risk to fix six.
    CASH_COMPONENT_RELAXED = CASH_COMPONENT + ("tinphieu",)

    # How alike two labels must be to be the SAME line item across quarters.
    #
    # The filing prints no item codes, so a line is identified by its label — and OCR renders
    # the same printed label slightly differently from one scan to the next ("kế toán" ->
    # "kếtoán", a dropped diacritic, a swallowed space). Keyed literally, every quarter mints
    # fresh columns and the panel does not line up in time: VCB's balance sheet produced 123
    # columns across 3 quarters, of which only 35 appeared in all three.
    #
    # So a parsed label is matched against the columns already seen and reuses the closest
    # one. The threshold is deliberately high: it must absorb OCR noise without merging two
    # genuinely different lines, and "chi phí hoạt động" vs "chi phí hoạt động khác" differ by
    # more than this.
    SIMILARITY = 0.92

    # ⚠️ **HOW MANY MAPPED LINES A STATEMENT NEEDS BEFORE IT MAY BECOME A REFERENCE for
    # `sane`.** Not a gate on ACCEPTING a statement — a thin one that reconciles is still
    # written — but on letting its probe into `history`, which is a different job: `history`
    # is the population every LATER quarter is judged against, so one bad entry rejects
    # everything after it.
    #
    # ⚠️ Measured 2026-08-24, and it cost a run: ACB's Q3-2009 standalone filing produced an
    # income statement with **2 mapped items**, it reconciled, its probe became the ONLY entry
    # in `history[income_statement]` — and `sane`'s median-based +/-20x band is therefore that
    # single figure, so **Q1-2010's income statement, which had read cleanly at `onnx@200` for
    # as long as the file has existed, was rejected as a magnitude outlier.** The guard that
    # exists to catch bad data had been poisoned by bad data.
    MIN_ITEMS_FOR_HISTORY = 8

    # The PARSE LAYERS tried per filing, in order, until each statement reconciles. A statement
    # that fails one layer (over-included pages, a misread digit, a note column pulled into the
    # value zone, a total-label variant) is retried by the next before the CafeF-tab fallback.
    # Ordered cheapest-first and strict-first: onnx@200 is the fast default that handles most
    # quarters and skips OCR entirely on clean-text filings; higher DPI and Tesseract run ONLY for
    # a filing that still has an unreconciled statement (ACB Q2-2010's balance sheet fails onnx@200
    # but reconciles at onnx@300); the two RELAXED layers run last of all, recovering grand-total
    # columns from label variants the strict fuzzy match rejects (ACB Q1-2019 prints "TỔNG CỘNG
    # TÀI SẢN" and merges the grand-total label into the line above), so no other quarter is
    # touched. Extend the pipeline by adding a ParseLayer here.
    LAYERS = [
        ParseLayer("onnx@200", "onnx", 200),
        ParseLayer("onnx@300", "onnx", 300),
        ParseLayer("onnx@400", "onnx", 400),
        ParseLayer("tesseract@200", "tesseract", 200),
        ParseLayer("onnx@200+relax", "onnx", 200, relax_totals=True),
        ParseLayer("onnx@300+relax", "onnx", 300, relax_totals=True),
        # LAST RESORT — A DIFFERENT RECOGNISER, not more pixels. onnx's VietOCR prepends a
        # phantom digit to some figures on some scans and does it at EVERY resolution: ACB's
        # Q1-2023 cash flow reads its closing balance as 196.922.247 where the filing prints
        # 96.922.247, identically at 200, 300, 400, 500 and 600 dpi, so raising the DPI is not a
        # remedy (the 11-character output occupies exactly the 52.5pt box a 10-character number
        # occupies elsewhere on the page — the recogniser, not the detector). Tesseract reads the
        # same crop correctly. It is terminal and relaxed, so only a statement that defeated every
        # onnx layer reaches it, and what it recovers still faces reconcile + `sane` + the cash
        # breakdown and identity gates.
        ParseLayer("tesseract@400+relax", "tesseract", 400, relax_totals=True),
        # THE BREAKDOWN'S COMPONENT SET, WIDENED — added last, after every existing layer, so a
        # statement that reconciles today can never reach them (the design rule here: a change
        # that recovers six quarters and quietly breaks a sixtieth is a net loss). These recover
        # the filings whose closing balance is right and whose breakdown lists a TREASURY BILL
        # the narrow set skips; see CASH_COMPONENT_RELAXED. The OCR configs are the two cheapest,
        # because for these quarters the pixels were never the problem — every engine and DPI
        # already reads the figures identically, and the first is a cache hit on onnx@200's
        # existing parse, so this costs a re-map and no second OCR pass.
        ParseLayer("onnx@200+components", "onnx", 200, relax_components=True),
        ParseLayer("onnx@300+components", "onnx", 300, relax_components=True),
        # …and with the total-label relaxations too, for a quarter that needs both.
        ParseLayer("onnx@200+relax+components", "onnx", 200,
                   relax_totals=True, relax_components=True),
        # A WIDER CROP, LAST OF ALL — for the filings whose figures are wrong rather than
        # unmatched. The detector box starts inside the number and the leading digit is never
        # shown to the recogniser: ACB's Q3-2023 reads its 93.261.018 deposit line as 261.018,
        # identically at 200/300/400 dpi and on tesseract, so no existing layer can reach it. At
        # pad 6 it reads correctly and the breakdown then closes to the đồng (6,552,560 +
        # 12,405,261 + 93,261,018 + 499,617 = 112,718,456). Re-renders the pages, so it is the
        # most expensive thing here and runs only when everything cheaper has failed.
        ParseLayer("onnx@200+pad6+components", "onnx", 200,
                   relax_components=True, crop_pad=6.0),
        ParseLayer("onnx@200+pad6+relax+components", "onnx", 200,
                   relax_totals=True, relax_components=True, crop_pad=6.0),
        # THE BALANCE LINE'S LABEL WRAPPED AND TOOK ITS FIGURE WITH IT (`relax_split_tail`).
        # At 300 dpi — and only there — ACB's Q3-2017 also reads its `tiền mặt` component as
        # 4,080,492 rather than 492, and 4,080,492 + 7,411,264 + 3,553,094 = 15,044,850 is
        # exactly the orphaned figure, so the recovery is confirmed by the breakdown rather than
        # asserted. Last of all, after every cheaper layer.
        ParseLayer("onnx@300+split", "onnx", 300,
                   relax_totals=True, relax_split_tail=True),
        ParseLayer("onnx@300+split+components", "onnx", 300,
                   relax_totals=True, relax_split_tail=True, relax_components=True),
        # A THOUSANDS SEPARATOR THE RECOGNISER READ AS A SPACE (`join_digits`). ACB's Q2-2012
        # returns '3 396.864' for a printed 3.396.864, and the run splitter — built for the
        # opposite case, one box holding two period figures — keeps only 396.864, leaving the
        # breakdown short by exactly 3,000,000 at every dpi and crop padding. Joined, the five
        # components come to 8,789,172 + 3,396,864 + 36,517,230 + 200,000 + 2,787,891 =
        # 51,691,157, the closing balance the filing prints.
        ParseLayer("onnx@200+join+components", "onnx", 200,
                   relax_components=True, join_digits=True),
        ParseLayer("onnx@200+join+relax+components", "onnx", 200,
                   relax_totals=True, relax_components=True, join_digits=True),
        # THE FILING MIS-STAMPED ITS OWN FORM CODE (`title_over_form`). VCB's Q2-2014 interim
        # report prints "Mẫu B04a/TCTD-HN" on both its income statement and its cash flow, so the
        # income statement is classified as a cash flow and never reaches the row builder — a
        # page-CLASSIFICATION failure, which is why no engine, dpi or crop setting touches it.
        # A verbatim title match overrules the code here, and nowhere else.
        ParseLayer("onnx@200+title", "onnx", 200, title_over_form=True),
        ParseLayer("onnx@200+title+relax", "onnx", 200,
                   relax_totals=True, title_over_form=True),
        # OCR APPENDED A STRAY DIGIT TO THE FORM CODE (`loose_form_code`), which costs the page
        # its anchor and stops `_drop_islands` pruning — VCB's Q1-2009 income statement came out
        # as five pages, four of them notes that merely echo "hoạt động kinh doanh" in a note
        # heading. Last of all, and the pruning it enables is what the gates then judge.
        ParseLayer("onnx@200+loose", "onnx", 200, loose_form_code=True),
        ParseLayer("onnx@200+loose+relax", "onnx", 200,
                   relax_totals=True, loose_form_code=True),
        # ── THE COMBINATIONS THE CASCADE NEVER HAD, added 2026-08-24 ──────────────────
        # Every classification knob above appears ALONE and at 200 dpi only: there was no
        # `title_over_form` + `loose_form_code` layer, and no `loose_form_code` above 200.
        # Probed against the three statements that defeat every existing layer (VCB Q1-2009
        # and Q2-2009 balance sheets), **`onnx@400+loose` gets strictly further than anything
        # else** — Q1-2009 moves from "no total assets" to "assets != liabilities + equity",
        # i.e. the grand total is finally read; 200 and 300 dpi cannot find it at all.
        #
        # ⚠️ **NONE OF THE THREE ACCEPTS A STATEMENT TODAY, AND THEY ARE HERE ANYWAY —
        # deliberately, and this is the one place that argument is made.** The file's rule is
        # that a layer recovering zero quarters is pure cost, which is why these sit LAST, past
        # every cheaper layer: only a statement that has already defeated all 21 predecessors
        # can reach them, so no passing quarter changes and the cost falls solely on documents
        # that were paying the full cascade regardless. What buys them their place is that the
        # failure they leave behind is a DIFFERENT one — an arithmetic gap, not a missing
        # total — which is the failure the schema mapping can act on.
        #
        # ⚠️ **AND THEY ARE NOT A FIX FOR THOSE TWO QUARTERS.** Their gap is `A - (L+E)` at a
        # stable **4.3-4.5 % of assets** across 200/300/400 dpi and every crop padding, so the
        # digits are being read correctly and no OCR setting can close it. That is a schema
        # mapping problem in the 2009-era consolidated VAS bank presentation. CLAUDE.md
        # §6-2-decies.
        ParseLayer("onnx@400+loose", "onnx", 400, loose_form_code=True),
        ParseLayer("onnx@400+loose+relax", "onnx", 400,
                   relax_totals=True, loose_form_code=True),
        ParseLayer("onnx@300+loose", "onnx", 300, loose_form_code=True),
        ParseLayer("onnx@300+loose+relax", "onnx", 300,
                   relax_totals=True, loose_form_code=True),
        ParseLayer("onnx@200+title+loose", "onnx", 200,
                   title_over_form=True, loose_form_code=True),
        ParseLayer("onnx@200+title+loose+relax", "onnx", 200,
                   relax_totals=True, title_over_form=True, loose_form_code=True),
        # ── THE FIGURES AND THEIR LABELS NEVER SHARED A LINE (`realign_rows`) ─────────
        # LAST OF ALL, and it is the one knob here that re-reads a page rather than a figure.
        # On some scans the detector puts every numeric box a constant ~7pt above the text box
        # of the same printed line — past `Y_TOL`, so the two never group, and each figure is
        # then handed to the label line ABOVE it. The statement slides by exactly one row while
        # every digit is read correctly, which is why no engine, DPI or crop setting touches it:
        # BID's Q1-2021 balance sheet reported total assets of 10,770,158 — its own cash line —
        # against the 1,558,887,407 printed on page 2, and was refused 26 times for
        # "assets != liabilities + equity".
        #
        # ⚠️ **A WRONG OFFSET WOULD WRITE A WHOLE STATEMENT OF WRONG NUMBERS, which is worse
        # than `missing`**, so the offset is measured rather than assumed and two things bound
        # it: it is chosen by maximising CO-LOCATION (lines holding both a label and a figure),
        # a criterion that never looks at what the figures ARE and so cannot be pulled toward a
        # total that happens to reconcile; and it is discarded unless it beats the unshifted
        # page by half again. On BID Q1-2021 the offset is 7pt and co-location goes 55 -> 174;
        # on ACB Q1-2021 and BID Q2-2021, both of which parse correctly today, the best shift
        # scores 1.09x and 1.03x and is refused. What it recovers still faces reconcile, `sane`
        # and the cash gates like anything else.
        ParseLayer("onnx@200+realign", "onnx", 200, realign_rows=True),
        ParseLayer("onnx@200+realign+relax", "onnx", 200,
                   realign_rows=True, relax_totals=True),
        ParseLayer("onnx@300+realign", "onnx", 300, realign_rows=True),
        ParseLayer("onnx@300+realign+relax", "onnx", 300,
                   realign_rows=True, relax_totals=True),
        ParseLayer("onnx@200+realign+relax+components", "onnx", 200,
                   realign_rows=True, relax_totals=True, relax_components=True),
        # ── THE FORM CODES ALL DIED AND THE NOTES WERE SWEPT IN (`notes_boundary`) ────
        # `_drop_islands` prunes a stray title page by its distance from a FORM-CODED one, so a
        # filing whose every code is unreadable cannot be pruned at all. BID's Q3-2025 reads
        # `from_form = False` on all 37 pages: pages 12-13 and 18-34 are notes whose headers
        # score against the balance-sheet title, `_fill_continuations` swept every numbered
        # table after them into the statement, and the result was **22 pages / 316 rows** whose
        # grand-total anchors came from a NOTE table — 115,110 against a real 3,071,970,196.
        # ⚠️ `reconcile` PASSES on that (assets and resources are the same garbage); `sane` is
        # the only gate that refused it, which is why the quarter read `missing` rather than
        # wrong. With the boundary applied the statement is **3 pages / 73 rows** and both
        # totals are the printed ones.
        #
        # `relax_merged_seam` rides with it because the same filing needs both: the balance
        # sheet merges "B. NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU" with "I. Các khoản nợ Chính phủ và
        # NHNN", and without the split the EQUITY anchor answers with the government-debt
        # figure — a wrong number that reconciles. Ordered narrow-first so a filing needing only
        # the page fix is never re-judged on its labels.
        # ⚠️ SEAM FIRST, AND THE ORDER IS LOAD-BEARING — measured 2026-08-26. `+notes` alone
        # ACCEPTS BID's Q3-2025 balance sheet, because both grand totals are then correct and
        # that is all `reconcile` and `sane` look at — while the EQUITY anchor still holds the
        # merged row's government-debt figure. A layer that passes the gates ends the cascade,
        # so the narrower layer must not run first: it would write a wrong number and stop.
        ParseLayer("onnx@200+notes+seam", "onnx", 200,
                   notes_boundary=True, relax_merged_seam=True),
        ParseLayer("onnx@200+notes+seam+relax", "onnx", 200,
                   notes_boundary=True, relax_merged_seam=True, relax_totals=True),
        ParseLayer("onnx@300+notes+seam", "onnx", 300,
                   notes_boundary=True, relax_merged_seam=True),
        ParseLayer("onnx@200+notes+seam+realign", "onnx", 200,
                   notes_boundary=True, relax_merged_seam=True, realign_rows=True),
        # …and the page fix WITHOUT the seam guess, last of all, for a filing the split hurts.
        ParseLayer("onnx@200+notes", "onnx", 200, notes_boundary=True),
        # ── THE STATEMENT'S LAST PAGE WAS TOO SPARSE TO BE ABSORBED (`tail_continuation`) ──
        # `_fill_continuations` requires `MIN_TABLE_WORDS = 15` figures before it will hand an
        # unidentifiable page to the statement running through it — the rule that keeps a
        # signature page out. A statement's FINAL page fails it for the same reason it exists:
        # a few closing rows and then the signatures. BID's Q1-2012 consolidated cash flow runs
        # pages 5-7 and page 7 holds codes 53/54/55 — opening 48,919,272,456,242, closing
        # 43,180,157,643,381, every digit read correctly at `onnx@200` — in **13 numeric words**.
        # The page was dropped and the quarter recorded `missing` for "no closing cash balance".
        #
        # ⚠️ THE THRESHOLD IS NOT LOWERED. A tail page is admitted on POSITIVE evidence — it must
        # carry the statement's own closing line (`PdfParser.TAIL`) — so a narrative or note page
        # can never qualify however many stray figures it holds, and the run ENDS at the page it
        # admits. What it recovers still faces reconcile, the cash-flow identity and `sane`.
        ParseLayer("onnx@200+tail", "onnx", 200,
                   tail_continuation=True, label_wrap=True),
        ParseLayer("onnx@200+tail+relax", "onnx", 200,
                   tail_continuation=True, label_wrap=True, relax_totals=True),
        ParseLayer("onnx@300+tail", "onnx", 300,
                   tail_continuation=True, label_wrap=True),
        ParseLayer("onnx@200+tail+relax+components", "onnx", 200,
                   tail_continuation=True, label_wrap=True,
                   relax_totals=True, relax_components=True),
        # ── THE STATEMENT NAMED NO UNIT AND THE FILING DID (`unit_from_document`) ─────
        # `unit_of` cannot tell "printed in đồng" from "did not say" and returns x1 for both.
        # BID's Q1-2026 cash flow prints "Triệu VNĐ" on NEITHER of its two pages while the
        # balance sheet of the same filing does, so every figure was read as đồng — a uniform
        # 10^6 error that reconciles perfectly against itself. `sane` is the only gate that
        # sees it (`magnitude 5.45e+08 vs typical 1.19e+14`), and it is why that quarter was
        # refused rather than written wrong. ⚠️ Last in the cascade because it multiplies every
        # figure of the statement it touches by a million: it may only judge one already refused.
        # ⚠️ **`+tail` FIRST, AND THE ORDER IS LOAD-BEARING — measured 2026-08-27.** With the
        # unit corrected but the labels still torn, `onnx@200+unit` passes `reconcile` AND
        # `sane` while writing the OPENING balance into the closing slot (530,277,690 mn where
        # the filing prints 544,528,992 mn) — both gates see one plausible cash balance and
        # cannot tell which line it came from. A layer that passes ends the cascade, so the
        # half-right layer must never run first. `PGB-1` recorded this exact trap for
        # `+notes` vs `+notes+seam`; this is the second instance, so it is the rule and not
        # the anecdote: **when two new layers differ by a label repair, the repair goes first.**
        # ⚠️ **AND A UNIT LAYER MUST NOT SKIP THE ARITHMETIC — measured on TCB Q3-2013.** At
        # 200 dpi that filing's cash flow reads its two balances correctly (22,621,969 and
        # 25,611,174 mn) and its NET MOVEMENT as **205** where the page prints 2,989,205: the
        # detector box starts inside the figure, so the leading digits were never shown to the
        # recogniser. `reconcile` cannot see it — `_closing_breakdown` proves the closing
        # balance, which is right — and `sane` cannot either, since the probe is that same
        # closing balance. Both gates passed, and the layer would have ENDED the cascade with a
        # wrong cell while the identical document at 300 dpi reads the line correctly.
        # `_cash_flow_identity` catches it in one line (22,621,969 + 205 != 25,611,174), which
        # is why `run_cash_identity` rides with `unit_from_document` as well as with
        # `relax_totals`: **a layer that multiplies every figure of a statement by a million
        # may not also be the layer that skips its arithmetic.** §6-2-tervicies drew the same
        # conclusion for `annual_tail` — *"only the `+relax` variant ships"* — and this is the
        # same rule stated as a property of the layer rather than as a choice of which to ship.
        ParseLayer("onnx@200+unit+tail", "onnx", 200,
                   unit_from_document=True, tail_continuation=True, label_wrap=True),
        ParseLayer("onnx@200+unit+tail+relax", "onnx", 200,
                   unit_from_document=True, tail_continuation=True, label_wrap=True,
                   relax_totals=True),
        # …and the unit fix WITHOUT the label repair, last of all, for a filing it hurts.
        ParseLayer("onnx@200+unit", "onnx", 200, unit_from_document=True),
        # ⚠️ **THE SAME BLOCK AT 300 DPI, BECAUSE THE STATEMENT THAT NEEDS THE UNIT MAY ALSO
        # NEED THE RESOLUTION, AND UNTIL NOW NO LAYER OFFERED BOTH.** TCB Q3-2013 needs exactly
        # that pair twice over: its income statement returns **7 figures split across two boxes**
        # at 200 dpi (`SPL-1`, refused as fragmented) and none at 300, and its cash flow is the
        # misread above. Both are sound at 300 with the document's unit, and neither had any
        # layer to land on — the unit block was 200-only.
        ParseLayer("onnx@300+unit+tail", "onnx", 300,
                   unit_from_document=True, tail_continuation=True, label_wrap=True),
        ParseLayer("onnx@300+unit", "onnx", 300, unit_from_document=True),
        # ── SOMEBODY ELSE'S WORDS ARE STUCK TO THE FRONT OF THE LABEL (`annual_tail`) ──
        # `table_rows` keys a row on `carry + label`, so a previous item whose label wrapped
        # onto its own line — too far below its figures for the forward branch to reclaim —
        # is still pending when the NEXT item's figures arrive. BID's FY-2016 cash flow keys
        # its closing balance `ty_con_khi_hop_nhat_tien_vi_cac_khoan_tuong_duong_tien_cuoi_nam`
        # while the line itself reads "Tiền vì các khoản tương đương tiền cuối năm" and carries
        # the right figures (65,521,789 / 55,806,145). `_split_merged` cannot cut it (no section
        # numeral at the seam) and containment cannot either (OCR "tiền vì" for "tiền và",
        # and the filing says "cuối năm" where the schema says "cuối kỳ").
        #
        # ⚠️ **AND `table_rows` CANNOT FIX IT EITHER, WHICH IS WHY THIS SITS IN THE SCORER.**
        # On that page a wrapped continuation is 11.8pt below its own line and the next ordinary
        # row is 15.1pt below — widening the forward branch would be tuning on a 3pt margin and
        # would swallow real labels. Scoring the suffixes is bounded instead: a trimmed
        # candidate still has to clear `SCHEMA_MATCH` and still has to pass the ordered walk.
        # ⚠️ **ONLY THE `+relax` VARIANT SHIPS, AND THAT IS THE WHOLE POINT — measured.** A
        # STRICT layer does not run `_cash_flow_identity` (`verify_cash` rides with
        # `relax_totals`), so `onnx@200+annual` ACCEPTS BID's FY-2016 with its opening and
        # closing correct and its NET CASH FLOW misread: the cell reads "6.711.6.3", which
        # `parse_num` strips to **671,163** against a true **6,711,633** — a 10x error on one
        # line, in a statement whose two balances are right. The relaxed variant refuses it.
        # A layer that can recover a label must not also be a layer that skips the arithmetic.
        ParseLayer("onnx@200+annual+relax", "onnx", 200, annual_tail=True, relax_totals=True),
        # ── THE STATEMENT HAS A FOURTH TERM AND THE CHART HAS NO COLUMN FOR IT ────────────
        # (`cash_extra_terms`, and `crop_pad=6` is what makes it reachable at all)
        #
        # BID's FY-2016 consolidated cash flow prints FIVE lines where the chart of accounts has
        # four: IV movement, V opening, "…từ việc nhận sáp nhập MHB", "…nhận từ các công ty con
        # khi hợp nhất", VIII closing. The identity `opening + movement + fx == closing` cannot
        # close without the fourth term and there is nowhere to put it, so the quarter was
        # refused for `fx not mapped` while every figure on the page was right.
        #
        # ⚠️ **AND EVERY FIGURE IS ONLY RIGHT AT `crop_pad=6` — measured 2026-08-27, twice, on
        # the page itself.** The detector box ends INSIDE the movement figure and the recogniser
        # is never shown the last digit: 6.711.633 reads as `6.711.6.3` at onnx@200, `6.711.610`
        # at 300 and `6.711.63)` at 400, all of which `parse_num` turns into a plausible wrong
        # number. This is `crop_pad`'s own documented defect (ACB Q3-2023) at the OTHER END of
        # the box, and raising the DPI cannot help — the pixels are outside the crop at every
        # resolution. At pad 6 the whole tail reads correctly and repeatably, and the identity
        # closes to the đồng: 55,806,145 + 6,711,633 + 3,004,011 = 65,521,789.
        #
        # ⚠️ THE LABEL REPAIR GOES FIRST (§6-2-unvicies, the rule `PGB-1` and `Q1-2026` both
        # measured): an annual report words its balances "cuối năm" where the schema says "cuối
        # kỳ", so without `annual_tail` the closing balance is recovered only by POSITION, and a
        # layer that passes the gates ends the cascade. ⚠️ The OCR pass is shared with
        # `onnx@200+pad6+components` above — `crop_pad` is part of the parse cache key and
        # `annual_tail` / `cash_extra_terms` are not, because they re-MAP an existing parse —
        # so the first of these three costs no OCR at all.
        ParseLayer("onnx@200+pad6+annual+extra", "onnx", 200, crop_pad=6.0,
                   annual_tail=True, relax_totals=True, cash_extra_terms=True),
        ParseLayer("onnx@200+pad6+extra", "onnx", 200, crop_pad=6.0,
                   relax_totals=True, cash_extra_terms=True),
        ParseLayer("onnx@300+pad6+annual+extra", "onnx", 300, crop_pad=6.0,
                   annual_tail=True, relax_totals=True, cash_extra_terms=True),
        # ── ⚠️ THREE DEFAULT-CROP `+extra` LAYERS WERE ADDED HERE ON 2026-08-27 AND
        # REMOVED THE SAME DAY, ON THE MEASUREMENT THAT WAS SUPPOSED TO JUSTIFY THEM.
        #
        # The argument was that the span REPLACES the positional FX guess (`P39`), so it must
        # be reachable wherever the guess was — and all three layers above carry `crop_pad=6`,
        # a padding BID's FY-2016 needed for an unrelated reason. The two quarters the guess
        # actually wrote read correctly at the DEFAULT crop, so leaving them nothing to reach
        # looked like a coverage loss bought for no correctness.
        #
        # ⚠️ **BOTH WERE RESCUED BY THE PAD-6 LAYERS ABOVE AND NEITHER NEW LAYER FIRED.**
        # Measured through the real cascade, with the history a full run would have had:
        #     Q4-2015 -> ACCEPTED [onnx@300+pad6+annual+extra], 27 items, 11.0 min
        #     Q2-2017 -> ACCEPTED [onnx@200+pad6+annual+extra], 19 items, 19.7 min
        # each with `open`, `IV` and `close` identical to what is on disk and `fx` empty.
        # The file's own rule is that a layer recovering zero quarters is pure cost, and being
        # well-argued is not evidence — six layers added 2026-08-24 are kept on that argument
        # and have returned nothing since. These had a measurement available and it went
        # against them, so they are gone rather than kept. **Do not re-add them without a
        # quarter they demonstrably recover.**
    ]

    def __init__(self, logger=None):
        self._logger = logger
        self._parser = PdfParser(logger=logger)          # env-default engine (kept for callers)
        self._parsers: Dict[str, PdfParser] = {self._parser.engine: self._parser}
        self._schema_cache: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
        # ⚠️ **PROGRESS HOOK — `on_layer(index, total, layer, cached)`, called once per layer of
        # `_parse_cascaded` before it runs.** It reports a POSITION in the cascade, never a
        # fraction of the work: the layers are wildly unequal (one runs a fresh OCR pass over
        # every page, the next re-maps a cached parse in milliseconds), and `cached` says which
        # kind this one is. `None` = no reporting, which is every caller but `pdf_ocr_job`.
        self.on_layer = None
        # ⚠️ **THE PAGE HOOK IS OWNED BY THE BUILDER, NOT SET ON THE PARSERS FROM OUTSIDE.**
        # `_parser_for` builds a parser LAZILY, per engine, on the first layer that needs it —
        # so a caller that reached into `_parsers` and set `on_page` on what was there reached
        # the DEFAULT parser and not the onnx one that does the work. Measured 2026-08-28: page
        # progress emitted zero lines for exactly that reason. Owning it here means every
        # parser the builder creates gets it, whenever it is created.
        self.on_page = None

    def _parser_for(self, engine: str) -> PdfParser:
        """A reusable parser per engine — built once, its DPI re-pointed per config, so the onnx
        models and Tesseract setup are not reloaded on every retry."""
        if engine not in self._parsers:
            self._parsers[engine] = PdfParser(logger=self._logger, engine=engine)
        parser = self._parsers[engine]
        # Re-pointed on every call rather than at construction: both are set on the BUILDER
        # after it exists, and a parser cached from an earlier document would otherwise keep
        # whichever logger was in force when it was first built.
        parser._logger = self._logger
        parser.on_page = self.on_page
        return parser

    def _parse_cascaded(self, path: str, period_end,
                        template: str, history: Dict[str, List[int]],
                        open_ref: Optional[int] = None):
        """Parse every statement of one filing, escalating OCR config until each reconciles.

        -> (accepted, doc_facts) where `accepted[report] = (row, statement, "engine@dpi")` holds
        only statements that passed BOTH the reconcile gate and the magnitude (`sane`) gate, and
        `doc_facts` carries the per-document publish_date + share counts (read from the first
        config that produced any statement). A report absent from `accepted` is left to the
        CafeF-tab fallback in `build`.
        """
        accepted: Dict[str, tuple] = {}
        # {report: [(layer name, why it was refused)]} — kept so an absent statement can say
        # WHY. Populated even for reports that later succeed; only the absent ones are printed.
        refused: Dict[str, List[Tuple[str, str]]] = {}
        # ⚠️ **A LAYER THAT RAISES IS NOT A LAYER THAT REFUSED, AND THE DIFFERENCE DECIDES
        # WHETHER A RESULT MAY BE BELIEVED.** A refusal is a measurement of the document; an
        # exception is a broken tool, and the cascade's answer then comes from whichever layer
        # the tool did not break on. Measured 2026-08-29: `vocr.vn`'s TLS certificate expired,
        # `Cfg.load_config_from_name` fetches `base.yml` from it on EVERY predictor
        # construction, so all three `onnx@*` layers raised and `tesseract@200` won a filing
        # that has read `onnx@200` since it was first parsed — with different figures, both
        # gates passing. The warning was there and nothing downstream could see it.
        self.layer_errors: List[Tuple[str, str]] = []
        facts = {"publish_date": "", "shares": {"shares_authorized": None,
                                                "shares_issued": None,
                                                "shares_outstanding": None}}
        # OCR is the expensive step, so the parse of each (engine, dpi) is cached within this
        # filing — the relaxed layers reuse a strict layer's already-OCR'd statements and only
        # re-map them, no second OCR pass.
        parsed: Dict[Tuple[str, int], Dict[str, Statement]] = {}
        for layer_index, layer in enumerate(self.LAYERS, start=1):
            if len(accepted) == len(REPORTS):
                break
            parser = self._parser_for(layer.engine)
            if not parser.ocr_ready and layer.engine != "onnx":
                continue                                  # engine unavailable on this machine
            # The crop padding is part of the KEY, not just a setting: two layers that share an
            # engine and DPI but crop differently produce different text, and keying on
            # (engine, dpi) alone would hand the wider-crop layer the narrow crop's cached parse
            # — the one that already failed.
            # `relax_merged_seam`, `annual_tail` and `cash_extra_terms` are deliberately ABSENT:
            # each changes the MAPPING or the GATE, not the parse, so two layers differing only
            # in them share one OCR pass. That is what makes `onnx@200+pad6+annual+extra` free —
            # `onnx@200+pad6+components` has already rendered those pages.
            key = (layer.engine, layer.dpi, layer.crop_pad, layer.join_digits,
                   layer.title_over_form, layer.loose_form_code, layer.realign_rows,
                   layer.notes_boundary, layer.tail_continuation, layer.label_wrap, layer.unit_from_document)
            if self.on_layer is not None:
                # `cached` is the whole cost story: a layer whose parse key is already in
                # `parsed` re-maps in milliseconds, while a new key re-OCRs every page.
                self.on_layer(layer_index, len(self.LAYERS), layer, key in parsed)
            if key not in parsed:
                parser.set_dpi(layer.dpi)
                parser.set_crop_pad(layer.crop_pad)
                parser.set_join_split(layer.join_digits)
                parser.set_title_over_form(layer.title_over_form)
                parser.set_loose_form_code(layer.loose_form_code)
                parser.set_realign_rows(layer.realign_rows)
                parser.set_notes_boundary(layer.notes_boundary)
                parser.set_tail_continuation(layer.tail_continuation)
                parser.set_label_wrap(layer.label_wrap)
                parser.set_unit_from_document(layer.unit_from_document)
                try:
                    parsed[key] = parser.parse(path, period_end)
                except Exception as e:
                    self._warn(f"    {layer.name}: parse failed — {type(e).__name__}: {e}")
                    self.layer_errors.append((layer.name, f"{type(e).__name__}: {e}"))
                    parsed[key] = {}
            statements = parsed[key]

            if not facts["publish_date"]:
                facts["publish_date"] = next(
                    (s.publish_date for s in statements.values() if s.publish_date), "")
                st_any = next(iter(statements.values()), None)
                if st_any:
                    facts["shares"] = {
                        "shares_authorized": st_any.shares_authorized,
                        "shares_issued": st_any.shares_issued,
                        "shares_outstanding": st_any.shares_outstanding,
                    }

            for report in REPORTS:
                if report in accepted:
                    continue
                st = statements.get(report)
                if st is None:
                    refused.setdefault(report, []).append(
                        (layer.name, "no such statement on any page of this filing"))
                    continue
                row = self.map_to_schema(st, template, relax_totals=layer.relax_totals,
                                         relax_split_tail=layer.relax_split_tail,
                                         relax_merged_seam=layer.relax_merged_seam,
                                         annual_tail=layer.annual_tail)
                # ⚠️ THE SHORT-CIRCUIT IS LOAD-BEARING AND IS PRESERVED EXACTLY: `sane` runs
                # only when `reconcile` passed, as it always has. What is new is that the
                # refusal is KEPT rather than discarded — see the report below the loop.
                # ⚠️ `unit_from_document` demands the cash identity for the same reason
                # `relax_totals` does — see the unit block in LAYERS.
                why = self.reconcile(st, row,
                                     verify_cash=(layer.relax_totals
                                                  or layer.unit_from_document),
                                     open_ref=open_ref,
                                     relax_components=layer.relax_components,
                                     cash_extra_terms=layer.cash_extra_terms)
                if why is not None:
                    why = f"reconcile: {why}"
                else:
                    bad = self.sane(st, history[report], row)
                    why = f"sane: {bad}" if bad is not None else None
                if why is None:
                    accepted[report] = (row, st, layer.name)
                else:
                    refused.setdefault(report, []).append((layer.name, why))

        # ⚠️ WHY A STATEMENT IS ABSENT, NOT MERELY THAT IT IS. Until 2026-08-25 the only trace
        # a refused statement left was the word `absent` in the period line — which is exactly
        # what a filing with no such page prints, and exactly what a statement rejected by the
        # magnitude guard prints. `SAN-1` was found by diffing against a backup rather than by
        # reading a log, and diagnosing ONE refused BID cash flow afterwards took four probe
        # runs to recover a reason the parser already knew and threw away.
        #
        # Distinct reasons only, each attributed to the FIRST layer that gave it: 21 layers
        # usually fail the same two or three ways, and printing all 21 buries the one that
        # matters. This is pure reporting — no gate, no threshold and no ordering changes.
        for report in REPORTS:
            if report in accepted or report not in refused:
                continue
            first_seen: Dict[str, str] = {}
            for name, why in refused[report]:
                first_seen.setdefault(why, name)
            self._warn(f"    {report} absent after {len(refused[report])} layer(s):")
            for why, name in first_seen.items():
                self._warn(f"      [{name}] {why}")
        return accepted, facts

    # ──────────────────────────────────────────────────────────────────────
    # Which template does a ticker file on?
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def build_templates_index(symbols: List[Tuple[str, str]],
                             logger=None) -> List[dict]:
        """Resolve every ticker to its accounting template -> financials/templates.csv.

        This is the map that says WHERE a ticker's statements live, and it is built by
        FINGERPRINTING each ticker (`cafef_schema.detect_template`) — reading the chart of
        accounts CafeF actually renders for it — never by classifying it from GICS. The two
        disagree: HVA sits in the securities industry group and files on the CORPORATE
        template. The GICS columns are carried alongside so a disagreement is visible, but the
        fingerprint is the one the parser obeys.
        """
        from web_scraper.cafef_schema import cash_flow_method, detect_template

        # ⚠️ OPTIONAL, AND THE WARNING IS THE POINT. The template itself is fingerprinted
        # over the network, so this file's absence does not stop anything — it silently
        # BLANKS the sector / industry_group columns, which are the only reason a
        # GICS-vs-fingerprint disagreement is visible in the data at all (HVA sits in the
        # securities group and files corporate). A blank column reads like "no
        # disagreement", so the degradation has to be said out loud.
        gics = {}
        path = os.path.join(CAFEF_RAW_DATA_DIR, "..", "simplize", "industry.csv")
        if optional_file(
            path,
            logger,
            what="the Simplize industry map",
            degrades=(
                "templates.csv keeps its fingerprinted template but its sector / "
                "industry_group_code / industry_group_slug columns are left BLANK, so a "
                "GICS-vs-fingerprint disagreement becomes invisible"
            ),
        ):
            with open(path, encoding="utf-8-sig") as f:
                gics = {r["ticker"]: r for r in csv.DictReader(f)}

        rows = []
        for exchange, symbol in symbols:
            template = detect_template(symbol)
            method = cash_flow_method(symbol)
            g = gics.get(symbol, {})
            rows.append({
                "exchange": exchange, "symbol": symbol,
                "template": template or "", "cash_flow_method": method or "",
                "sector": g.get("economic_sector_name", ""),
                "industry_group_code": g.get("industry_group_code", ""),
                "industry_group_slug": g.get("industry_group_slug", ""),
            })
            if logger:
                logger.log_info(f"cafef financials: {symbol} -> {template} / {method}")

        rows.sort(key=lambda r: (r["template"], r["symbol"]))
        os.makedirs(FIN_DIR, exist_ok=True)
        tmp = TEMPLATES_INDEX + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=INDEX_COLS, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        os.replace(tmp, TEMPLATES_INDEX)
        return rows

    @classmethod
    def build_all(cls, logger=None, switch_handler=None,
                  symbols: Optional[List[Tuple[str, str]]] = None,
                  use_api: bool = False,
                  skip_existing: bool = True) -> Dict[str, Dict[str, int]]:
        """Switch-driven batch entry point (`web_scraper/cafef/financials`) — main.py's
        way in, mirroring the scrapers' `scrape()` even though nothing here is scraped:
        this reads the LOCAL PDF archive `CafeFPdfScraper` already downloaded.

        ⚠️ THE TEMPLATE INDEX IS REBUILT FROM THE WHOLE LIST, ONCE, BEFORE ANY PARSING.
        `build_templates_index` writes templates.csv from exactly the symbols handed to
        it, so calling it per ticker inside the loop would leave a one-row file naming
        only the last ticker parsed — which is how VCB lost its row when ACB was parsed
        on its own. Building the index for every ticker up front makes that failure
        impossible regardless of which subset actually parses.

        Cost is per ticker and large (~2.4 h each — the note-page OCR that reads the
        share counts roughly doubled it), so the universe is the explicit
        CAFEF_FINANCIALS_TICKERS, never the ~777-code listing. One ticker failing does
        not abort the rest; the returned dict maps `EXCHANGE:TICKER` to that ticker's
        per-report row counts (`{}` where it raised).
        """
        if switch_handler and not switch_handler.is_enabled(
            "web_scraper", "cafef", "financials"
        ):
            return {}

        universe = list(symbols if symbols is not None else CAFEF_FINANCIALS_TICKERS)
        if not universe:
            if logger:
                logger.log_warning(
                    "cafef financials: no tickers configured (CAFEF_FINANCIALS_TICKERS)."
                )
            return {}

        cls.build_templates_index(universe, logger=logger)

        builder = cls(logger=logger)
        results: Dict[str, Dict[str, int]] = {}
        failed: List[str] = []

        for exchange, symbol in universe:
            key = f"{exchange}:{symbol}"
            try:
                results[key] = builder.build(
                    exchange, symbol, use_api=use_api, skip_existing=skip_existing
                )
                if logger:
                    logger.log_info(f"cafef financials DONE {key}: {results[key]}")
            except Exception as e:
                # Per-ticker isolation is kept DELIBERATELY: a ticker costs ~2.4 h, so
                # letting one failure discard the nine that already parsed would be far
                # worse than the failure itself.
                results[key] = {}
                failed.append(key)
                if logger:
                    logger.log_error(
                        f"cafef financials FAILED {key}: {type(e).__name__}: {e}"
                    )

        # ⚠️ The summary is the point. `results[key] = {}` for a failure is
        # indistinguishable from a ticker that legitimately produced no rows, and a
        # single FAILED line among hours of OCR logging goes unread. Orchestration does
        # NOT come through here — a Dagster ticker partition calls `build()` directly,
        # so the exception propagates and that partition goes red on its own.
        if logger:
            if failed:
                logger.log_error(
                    f"cafef financials: {len(failed)} of {len(universe)} ticker(s) "
                    f"FAILED: {failed}"
                )
            else:
                logger.log_info(
                    f"cafef financials: all {len(universe)} ticker(s) OK."
                )
        return results

    def template_of(self, symbol: str) -> Optional[str]:
        """From templates.csv when it is there, else fingerprint the ticker."""
        if os.path.exists(TEMPLATES_INDEX):
            with open(TEMPLATES_INDEX, encoding="utf-8-sig") as f:
                for r in csv.DictReader(f):
                    if r["symbol"] == symbol and r["template"]:
                        return r["template"]
        from web_scraper.cafef_schema import detect_template
        return detect_template(symbol)

    def _log(self, msg: str) -> None:
        if self._logger:
            self._logger.log_info(msg)
        else:
            print(msg)

    def _warn(self, msg: str) -> None:
        if self._logger:
            self._logger.log_warning(msg)
        else:
            print(msg)

    # ──────────────────────────────────────────────────────────────────────
    # Choosing the documents
    # ──────────────────────────────────────────────────────────────────────

    def documents(self, exchange: str, symbol: str,
                  allow_parent: bool = False,
                  period_min: Optional[str] = FINANCIALS_PERIOD_MIN) -> List[dict]:
        """The one filing to read per quarter, oldest first — consolidated by preference.

        ⚠️ **`allow_parent` FALLS BACK TO THE STANDALONE FILING FOR A PERIOD THAT HAS NO
        CONSOLIDATED ONE, AND IT IS NOT A LOOSENING — measured 2026-08-24.** With the
        consolidated-only rule this method opens **13,912 of the 55,998 documents on disk
        (24.8 %)**, and **273 of 784 tickers yield NOTHING AT ALL**: a company with no
        subsidiaries files no `hợp nhất` statement, so its standalone report is not a lesser
        version of a consolidated one — it is the only statement that exists, and it IS the
        company. `HNX_ADC` files 51 documents, every one of them standalone. With the
        fallback the method opens **26,280 documents (x1.89)** and the tickers that yield
        nothing drop to **22**.

        It also reaches ACB's early years: ACB filed no consolidated statement before 2010,
        so its 2008-09 quarters were unreachable and had been silently filled from CafeF's
        web tabs instead (`ISSUES.md` FIN-1). ACB goes 65 -> 73 documents, VCB 72 -> 75.

        ⚠️ **CONSOLIDATED STILL WINS WHENEVER BOTH EXIST** — the preference is the first key
        of the sort, ahead of assurance, so no quarter that reads consolidated today can
        change entity. ⚠️ **AND THE CHOICE IS RECORDED**: every row carries a `consolidated`
        column, because two entities in one column with nothing saying which is which is the
        same defect as sourcing a figure from a web tab.

        Q4 is taken from the AUDITED ANNUAL report (CafeF files it under quarter 5) whenever
        one exists. It is the same period — the balance sheet at 31 December IS the Q4 balance
        sheet, and the cash flow is cumulative to year end either way — but audited rather than
        unaudited, and it is the better-produced document.

        The income statement is the exception and must be de-cumulated: an annual report's P&L
        is the whole year, so the Q4 quarter is FY − (Q1+Q2+Q3). The row is tagged `annual` so
        `_decumulate` knows to do that.
        """
        index = os.path.join(PDFS_DIR, "index", f"{exchange}_{symbol}.csv")
        if not os.path.exists(index):
            raise FileNotFoundError(
                f"no PDF index at {index} — run CafeFPdfScraper for {symbol} first")
        with open(index, encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        if not allow_parent:
            rows = [r for r in rows if r["consolidated"] == "True"]

        # ⚠️ CONSOLIDATED FIRST, ASSURANCE SECOND. Ordering the tuple the other way would let
        # an AUDITED standalone report displace an UNAUDITED consolidated one — a change of
        # entity bought for a change of assurance, which is not a trade this method may make.
        def _pref(r: dict) -> tuple:
            return (0 if r["consolidated"] == "True" else 1,
                    self.ASSURANCE_RANK.get(r["assurance"], 9))

        best: Dict[str, dict] = {}
        annual: Dict[str, dict] = {}
        for r in rows:
            q = str(r["quarter"])
            if not q.isdigit():
                continue
            q = int(q)
            if q == 5:                        # the audited annual -> stands in for Q4
                p = f"Q4-{r['year']}"
                if p not in annual or _pref(r) < _pref(annual[p]):
                    annual[p] = {**r, "period": p, "quarter": "4", "annual": "True"}
                continue
            if q not in (1, 2, 3, 4):
                continue
            p = r["period"]
            if p not in best or _pref(r) < _pref(best[p]):
                best[p] = {**r, "annual": "False"}

        # The annual report wins Q4 — but ⚠️ **NEVER ACROSS ENTITIES**. This was a bare
        # `best.update(annual)` and was safe only while both dicts were consolidated-only:
        # with `allow_parent` a STANDALONE annual would silently displace a CONSOLIDATED
        # Q4 quarterly, changing which entity that row describes to buy a better-produced
        # document. Measured before the guard: **86 of 13,912 consolidated periods moved**.
        # Compare the entity rank only — within one entity the annual still wins on the
        # original grounds (same period, audited, better typeset).
        for p_, a in annual.items():
            if p_ not in best or _pref(a)[0] <= _pref(best[p_])[0]:
                best[p_] = a
        # ⚠️ THE FLOOR IS APPLIED TO THE PERIOD THE DOCUMENT CONTRIBUTES, AFTER the annual
        # has been folded onto its Q4 — so a 2009-dated FY-2008 report is judged as Q4-2008
        # and kept, while a 2007 annual is dropped as Q4-2007. Filtering the raw index rows
        # instead would have used the FILING year and thrown away the report that carries the
        # first quarter we want. See FINANCIALS_PERIOD_MIN for why the floor exists.
        out = best.values()
        if period_min:
            floor = _period_key(period_min)
            out = [r for r in out if _period_key(r["period"]) >= floor]
        return sorted(out, key=lambda r: (int(r["year"]), int(r["quarter"])))

    def alternates(self, exchange: str, symbol: str, chosen: dict) -> List[dict]:
        """Other filings of the SAME period and the SAME ENTITY as `chosen`, best first.

        ⚠️ **A PERIOD CAN HAVE MORE THAN ONE FILING AND ONLY ONE WAS EVER TRIED.**
        `documents` returns the single best document per quarter, ranked entity-then-assurance,
        and until 2026-08-25 a quarter whose every layer refused that document was simply
        recorded `missing` — while a second consolidated filing of the same quarter sat unread
        on disk. Measured on BID: of its 13 genuinely failed statements, **4 have an unread
        CONSOLIDATED alternate** (Q4-2015, Q4-2016, Q2-2017, Q4-2017), each the unaudited
        quarterly beside the audited annual `documents` preferred.

        ⚠️ **THE ENTITY IS FIXED, NOT PREFERRED.** Only filings whose `consolidated` equals
        `chosen`'s are returned, so a fallback can never quietly change which company a row
        describes — the same rule `documents` applies to the annual-report merge, and the one
        `allow_parent` exists to make explicit. A standalone report is reachable only by
        setting that flag, never by a quarter happening to fail.

        Assurance may drop, and that is the whole point: an unaudited filing of the quarter is
        a WORSE-PRODUCED document of the SAME period and entity, which is a trade worth making
        when the alternative is `missing`. It is still gated by reconcile + `sane` like any
        other parse — nothing is accepted here that would not have been accepted first time.
        """
        index = os.path.join(PDFS_DIR, "index", f"{exchange}_{symbol}.csv")
        if not os.path.exists(index):
            return []
        with open(index, encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))

        period, entity = chosen["period"], chosen.get("consolidated", "True")
        year, quarter = chosen["year"], str(chosen["quarter"])

        def serves(r: dict) -> bool:
            q = str(r["quarter"])
            if not q.isdigit():
                return False
            # quarter 5 is the annual, and it stands in for that year's Q4 — the same
            # widening `documents` applies, so an alternate for Q4 may be either shape.
            p = f"Q4-{r['year']}" if int(q) == 5 else r["period"]
            return p == period

        out = [r for r in rows
               if serves(r)
               and r.get("consolidated", "True") == entity
               and r["path"] != chosen["path"]]
        out.sort(key=lambda r: self.ASSURANCE_RANK.get(r["assurance"], 9))
        return [{**r, "period": period, "year": year, "quarter": quarter,
                 "annual": "True" if str(r["quarter"]) == "5" else "False"}
                for r in out]

    # ──────────────────────────────────────────────────────────────────────
    # Gates
    # ──────────────────────────────────────────────────────────────────────

    # ──────────────────────────────────────────────────────────────────────
    # Mapping a parsed statement onto the canonical chart of accounts
    # ──────────────────────────────────────────────────────────────────────

    # How close an OCR'd line must be to a schema line to BE it.
    #
    # 0.80, not lower: a shorter name is a subsequence of a longer one far more often than it
    # looks. "TỔNG VỐN CHỦ SỞ HỮU" scores 0.75 against "TỔNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU" —
    # every character of the first appears, in order, inside the second. At 0.72 total equity
    # captured the grand total's column, the real grand total then had nowhere to go, and 48 of
    # 69 balance sheets were rejected for "assets != liabilities + equity".
    SCHEMA_MATCH = 0.80
    MIN_CONTAINS = 10        # too short a name is contained in too many others to prove much
    # Containment ("one name sits inside the other") runs in BOTH directions, and MIN_CONTAINS
    # guards only the schema account — which is the wrong string to check for `key in account`.
    # There the contained side is the parsed row, and OCR produces two very different kinds of
    # short row:
    #   * a WORD FRAGMENT, when OCR splits a label across two lines and the tail becomes a row of
    #     its own. ACB's Q1-2023 income statement breaks "Chi phí thuế thu nhập doanh nghiệp /
    #     hoãn lại" exactly there, and the fragment "hoanlai" — carrying the 87,884 — identifies
    #     line 8 among its neighbours as surely as the full label would.
    #   * the statement's own NUMBERING (a, b, c, i, ii … viii), when the letter is split off its
    #     text. "a" is contained in almost every account name there is, and it handed ACB's
    #     Q1-2023 balance sheet the 1,709,488 sitting beside a stray "a" row as TỔNG NỢ PHẢI TRẢ.
    # Length separates them cleanly: every roman numeral and section letter is at most 4 chars
    # ("viii"), while a fragment that is a real word runs longer. So the contained side must be a
    # word, not a numeral — that is what this floor asserts, and it is deliberately NOT
    # MIN_CONTAINS, which answers a different question about the account side.
    MIN_CONTAINS_FRAGMENT = 5

    # The cash-flow section prefixes, and the method tags the union adds. Only these may be
    # stripped from the front of a column — a blanket "drop the first word" also eats the
    # first word of `tong_tai_san`, leaving `tai_san`, which then fuzzy-matches any asset line
    # and hands TOTAL ASSETS the value of some line halfway up the statement.
    COL_PREFIXES = ("hdkd_indirect_", "hdkd_direct_", "hdkd_", "hddt_", "hdtc_")
    # An index: roman, digit or single letter, e.g. `vii_1_a_`.
    INDEX_RE = re.compile(r"^(?:[ivxlc]+_|\d+_|[a-z]_)+")

    def schema_of(self, template: str, report: str) -> List[Tuple[str, str]]:
        """[(canonical column, its account name)] in statement order, from schema/.

        ⚠️ REQUIRED, AND IT RAISES. This used to be guarded by `if os.path.exists(path)`
        and returned an EMPTY list when the file was absent — an empty chart of accounts,
        against which nothing matches, so `map_to_schema` mapped no line, `reconcile`
        found no subtotal, and every statement of every quarter was rejected. After ~2.4 h
        of OCR, and reported as a parsing failure rather than a missing file.

        The 12 schema CSVs have NO PRODUCER in the pipeline — `cafef_schema.save()` writes
        them but nothing calls it — so they are a git-tracked repo input, and "absent"
        means someone deleted or moved one, never that a run has not reached them yet.
        """
        key = (template, report)
        if key in self._schema_cache:
            return self._schema_cache[key]

        path = require_file(
            os.path.join(SCHEMA_DIR, f"{template}_{report}.csv"),
            what=f"the {template}/{report} chart of accounts",
            why=(
                "every parsed line is matched against it, so without it NOTHING maps "
                "and every statement is rejected as unreconcilable"
            ),
            fix=f"cafef_schema.save({template!r}, SCHEMA_DIR) — or restore it from git",
        )
        items: List[Tuple[str, str]] = []
        with open(path, encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                col = r["column"]
                rest = col
                for p in self.COL_PREFIXES:
                    if rest.startswith(p):
                        rest = rest[len(p):]
                        break
                account = self.INDEX_RE.sub("", rest)
                items.append((col, account or rest))
        self._schema_cache[key] = items
        return items

    def preflight(self, exchange: str, symbol: str) -> str:
        """Validate every input `build()` needs, BEFORE spending hours on OCR. Returns
        the resolved template.

        ⚠️ THE POINT IS THE ORDERING. Each of these was already fatal; each was
        discovered late, and one of them not as itself — a missing chart of accounts
        surfaced as "all 65 filings failed to reconcile". A parse is ~2.4 h per ticker,
        so an input check that costs milliseconds belongs in front of it, not inside it.

        Called at the top of `build()`, so main.py, a notebook and an orchestrator all
        get it — this must never live only in the Dagster asset.
        """
        template = self.template_of(symbol)
        if not template:
            raise MissingSourceDataError(
                f"no accounting template for {symbol}: it is absent from "
                f"{TEMPLATES_INDEX!r} and the fingerprint call returned nothing. "
                f"Fix: FinancialsBuilder.build_templates_index([({exchange!r}, "
                f"{symbol!r})]) — note it REWRITES the file, so pass the whole list."
            )

        for report in REPORTS:
            self.schema_of(template, report)  # raises, with the actionable message

        require_file(
            os.path.join(PDFS_DIR, "index", f"{exchange}_{symbol}.csv"),
            what=f"the {exchange}:{symbol} PDF index",
            why="it says which filing covers which quarter; `documents()` cannot choose",
            fix=f"CafeFPdfScraper().scrape_pdfs({exchange!r}, {symbol!r})",
        )
        require_dir(
            os.path.join(PDFS_DIR, "files", f"{exchange}_{symbol}"),
            what=f"the {exchange}:{symbol} filing archive",
            why="the statements are parsed from those PDFs; there is nothing to read",
            fix=f"CafeFPdfScraper().scrape_pdfs({exchange!r}, {symbol!r})",
        )

        self._log(f"cafef financials preflight OK: {exchange}:{symbol} -> {template}")
        return template

    # The filings ABBREVIATE where the chart of accounts spells out, and the two then share
    # almost no characters: "vay các TCTD khác" against "vay các tổ chức tín dụng khác" scores
    # ~0.70 and the line is simply lost. That is how ACB's Q1-2022 mis-filled
    # `ii_tien_gui_va_vay_cac_tctd_khac` — neither of its two children could reach its own
    # account, so one of them won the PARENT's slot on containment instead. Expanded on both
    # sides before scoring, each child matches its own line exactly and the parent is left alone.
    ABBREV = {
        "tctd": "tochuctindung",
        "nhnn": "nganhangnhanuoc",
        "tscd": "taisancodinh",
        "tndn": "thunhapdoanhnghiep",
        "bdsdt": "batdongsandautu",
        # ⚠️ **"TỔNG CỘNG" AND "TỔNG" ARE ONE WORD IN A STATEMENT HEADING, and one inserted
        # syllable was enough to lose a grand total and to hand EQUITY the wrong figure.**
        # TCB files its 2013 balance sheet as "TỔNG CỘNG TÀI SẢN CÓ" against the chart of
        # accounts' "TỔNG TÀI SẢN": no containment (the syllable is in the middle) and
        # SequenceMatcher gives **0.769**, under SCHEMA_MATCH, so total assets simply did not
        # map. Worse, its "TỔNG CỘNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU" scored **0.929** for its
        # own anchor while merely CONTAINING "vốn chủ sở hữu" — a flat 0.95 by containment —
        # so the EQUITY anchor took the grand total (165,878,786 mn against a real 13,857,834)
        # and the grand total was left absent. Both gates passed: `reconcile` falls through to
        # `Statement.find`, which reads the right rows out of the OCR text.
        # Normalised on BOTH sides, that row scores **1.000** against its own account and the
        # collision disappears — the anchors settle it themselves, with no new threshold.
        # ⚠️ Verified to introduce **0 new account collisions** across all 12 charts (31 before,
        # 31 after — the ordered walk already disambiguates those by position).
        "tongcong": "tong",
    }

    @classmethod
    def _expand(cls, s: str) -> str:
        for short, full in cls.ABBREV.items():
            if short in s:
                s = s.replace(short, full)
        return s

    # How many leading words `annual_tail` may drop from a row's label. A carry that leaked in
    # is one wrapped line — a handful of words — never half a statement, and an unbounded search
    # would let any row match any account by discarding enough of itself.
    MAX_TAIL_TRIM = 6

    # How an ANNUAL report words the two cash balances, against how the chart of accounts does.
    # Keyed schema-side so only these two lines are ever rewritten; everything else scores as
    # before. ⚠️ The OPENING already matched by luck — "tại đầu năm" keeps the "tai" the schema
    # also has — and it is listed anyway so the pair is treated the same way.
    ANNUAL_WORDING = {
        "taithoidiemcuoiky": "cuoinam",
        "taithoidiemdauky": "daunam",
    }

    def _prefix_trims(self, key: str):
        """The key, then the key with 1..`MAX_TAIL_TRIM` leading words dropped.

        ⚠️ **A ROW'S LABEL CAN CARRY A PREFIX THAT BELONGS TO THE LINE ABOVE IT, and neither
        `_split_merged` nor containment can remove it.** `table_rows` builds a label as
        `carry + label`, so when the previous item's label wrapped onto its own line and was
        too far below its figures to be reclaimed, that wrapped half is still pending when the
        NEXT item's figures arrive. BID's FY-2016 cash flow prints
        "…nhận từ [công] / ty con khi hợp nhất" and then, 18pt lower, its closing balance —
        which comes out keyed `ty_con_khi_hop_nhat_tien_vi_cac_khoan_tuong_duong_tien_cuoi_nam`
        while the line ITSELF reads "Tiền vì các khoản tương đương tiền cuối năm" and is
        perfectly good.

        ⚠️ `_split_merged` cannot cut it — there is no section numeral at the seam — and
        containment cannot either, because OCR wrote "tiền vì" for "tiền và" and the filing says
        "cuối năm" where the schema says "cuối kỳ". Distance cannot separate the two cases on
        this page: the wrapped half sits **11.8pt** below its own line while the next ordinary
        row is **15.1pt** below, so widening `table_rows`' forward branch would be tuning on a
        3pt margin and would swallow real labels.

        Scoring the suffixes instead costs nothing structural: a trimmed candidate must still
        clear `SCHEMA_MATCH` and still pass the ordered walk, so this cannot invent a match —
        it can only stop a good line being hidden behind somebody else's words.
        """
        yield key
        parts = key.split("_")
        for i in range(1, min(self.MAX_TAIL_TRIM, max(0, len(parts) - 2)) + 1):
            yield "_".join(parts[i:])

    def _label_score(self, account: str, key: str, relax: bool = False,
                     annual_tail: bool = False,
                     edge_containment: bool = False) -> float:
        """How alike a schema account and a parsed row label are, both separator-stripped.

        One measure shared by the ordered walk and `_anchor`, so a line is scored the same way
        whichever finds it. Above the raw ratio sit two shortcuts:

          * CONTAINMENT — one name sits inside the other. Evidence only when BOTH are
            substantial: see MIN_CONTAINS (the account) and MIN_CONTAINS_FRAGMENT (the row,
            which OCR may have cut down to a word).
          * The CASH-FLOW TAIL, under `relax` only — the filing dates these lines where the
            schema names the period (see CASH_TAIL). The prefix identifies the pair; order and
            length tell them apart.

        The cash-tail shortcut is gated because it is not additive. It makes two rows that
        previously matched nothing match strongly, and where a quarter ALREADY had a closing
        balance that changes which row wins: on a sample of 16 accepted quarters it moved
        Q4-2021's closing from 82,601,567 to the 46,022,071 in the comparative column. Confined
        to the relaxed layers it can only be reached by a statement the strict ones could not
        read at all, so no quarter that already parses is touched.
        """
        from difflib import SequenceMatcher

        if annual_tail:
            # ⚠️ AN ANNUAL REPORT DATES ITS CASH BALANCES BY THE **YEAR** WHERE THE CHART OF
            # ACCOUNTS NAMES THE **PERIOD** — "cuối năm" against "tại thời điểm cuối kỳ". That
            # is a wording difference in the FILING, like `CASH_TAIL`'s, so it does not improve
            # with resolution: BID's FY-2016 closing scores **0.79** against its own account and
            # is lost. Both halves are needed and neither suffices alone — measured on that
            # filing, the trimmed label scores 0.765 against the schema wording and **0.944**
            # against the annual one.
            bare = account.replace("_", "")
            accounts = {account}
            want = None
            for schema_words, annual_words in self.ANNUAL_WORDING.items():
                if schema_words in bare:
                    accounts.add(bare.replace(schema_words, annual_words))
                    want = annual_words[:4]          # "cuoi" / "daun" -> the period word
            # ⚠️ **THE PERIOD WORD IS A HARD DISCRIMINATOR HERE, NOT A SCORE — measured.**
            # Rewriting the account to the annual wording drops "tại thời điểm", which is text
            # the two balances SHARE, and that raises the relative weight of everything else
            # until the OPENING row scores **0.804** against the CLOSING account — over the bar.
            # The walk then handed BID's FY-2016 closing slot the opening figure, and only
            # `sane`'s equality gate caught it. A row that says "đầu" cannot be the closing
            # balance whatever it scores, so it is refused outright rather than out-ranked.
            if want:
                other = "daun" if want == "cuoi" else "cuoi"
                bare_key = key.replace("_", "")
                if other in bare_key and want not in bare_key:
                    return 0.0
            return max(self._label_score(a, k, relax,
                                         edge_containment=edge_containment)
                       for a in accounts for k in self._prefix_trims(key))

        account, key = self._expand(account), self._expand(key)
        r = SequenceMatcher(None, account, key).ratio()
        if (len(account) >= self.MIN_CONTAINS
                and min(len(account), len(key)) >= self.MIN_CONTAINS_FRAGMENT
                and (account in key or key in account)
                # ⚠️ ANCHORS ONLY. An account buried inside a merged row is a MENTION and
                # not that row's line item — see `_contains_at_an_edge`. The ORDERED WALK
                # keeps the flat score, because position already keeps it honest: gating
                # the walk too was MEASURED at 23 of 228 statements changed, several of
                # them sound cells lost, against 3 when it is confined to the anchors.
                and (not edge_containment
                     or self._contains_at_an_edge(account, key))):
            r = max(r, 0.95)
        if relax and account.startswith(self.CASH_TAIL) and key.startswith(self.CASH_TAIL):
            r = max(r, 0.95)
        return r

    @staticmethod
    def _contains_at_an_edge(account: str, key: str) -> bool:
        """Is the account the BEGINNING or the END of the row's label, rather than buried in it?

        ⚠️ **CONTAINMENT AWARDS A FLAT 0.95 TO ANY LINE THAT MERELY MENTIONS AN ACCOUNT, AND A
        MERGED ROW IS `HEADER + LINE`.** `table_rows` glues a section header onto the item
        beneath it when the header prints no figure of its own, so a balance sheet's
        "B. NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU" arrives as the prefix of "II. Tiền gửi và vay các
        TCTD khác" — and the EQUITY anchor, whose account text is eleven characters, scored that
        row 0.95 and took **24,686,177 mn of interbank deposits as TCB's Q3-2013 equity**, while
        `_claim` evicted the deposits line the ordered walk had placed correctly. Both gates
        passed: the grand totals were sound, so `reconcile` and `sane` had nothing to see.
        `PGB-1` is the same defect one filing over, and its seam split cannot reach this one —
        the seam is the item's roman numeral, which THIS scan did not read (at 200 dpi it did,
        and the row split correctly, which is how the two readings were told apart).

        The discriminator is structural, not a threshold: **in `header + line` the header is a
        PREFIX and the line is a SUFFIX**, so an account that is neither is a mention inside
        somebody else's item. Measured over the 281 anchor containment hits in the corpus, every
        harmful one sits strictly inside (head 11-44, tail 22-48) and every sound one reaches an
        edge — VCB's "TỔNG TÀI SẢN CÓ" (prefix, tail 2), ACB's Q1-2022 "Dự phòng rủi ro khác |
        TỔNG NỢ PHẢI TRẢ" (suffix, the case `_claim` exists to pin), and the equity line OCR
        merged as "VỐN CHỦ SỞ HỮU | Vốn và các quỹ VIII" (prefix).
        """
        return key in account or key.startswith(account) or key.endswith(account)

    def map_to_schema(self, st: Statement, template: str,
                     relax_totals: bool = False,
                     relax_split_tail: bool = False,
                     relax_merged_seam: bool = False,
                     annual_tail: bool = False) -> Dict[str, int]:
        """Parsed rows -> canonical columns.

        This is what makes the output a PANEL rather than a pile. Keyed on the OCR text, the
        same printed line becomes a different column every quarter — VCB's balance sheet threw
        332 columns against a 90-column chart of accounts, so nothing lined up in time. Keyed
        on the schema, a line is the same column in every quarter of every ticker on that
        template.

        Matching walks the schema and the parsed rows together, in statement order, and is
        fuzzy because OCR damages the names ("TỔNG NỢ PHẢI TRẢ" -> `tong_nuphai_tra`). Order is
        what keeps a fuzzy match honest: the schema's own sequence stops "chi phí hoạt động"
        from being answered by "chi phí hoạt động khác" further down.

        `relax_totals` (set only by the relaxed ParseLayers, after the strict ones fail) recovers
        the balance-sheet grand-total columns from label variants the fuzzy match rejects — see
        `_recover_totals`.
        """
        from web_scraper.cafef_pdf_parser import PdfParser

        schema = self.schema_of(template, st.report)
        if not schema:
            return {}

        # The current-period figure is the first POPULATED column, not literally index 0: OCR
        # can over-segment the columns (a left-hand note-reference number clustering as its own
        # spurious column) and push the real value into index 1 — ACB's Q2-2010 grand total
        # parsed as [None, 176999825…, None, …]. A strict values[0] read it as empty, dropped
        # it, and the balance sheet was rejected for "no total assets".
        rows = []
        for i, row in enumerate(st.rows):
            val = st._first_value(row.values)
            if val is None:
                continue
            key = self._split_merged(row.key, row.label,
                                     relax_merged_seam)
            if annual_tail and key == row.key and row.label:
                # ⚠️ `slug` CAPS A KEY AT 60 CHARACTERS AND THAT IS WHERE THE SUFFIX LIVES.
                # With somebody else's words on the front, "…tiền cuối năm" is cut to "…tien_cuo"
                # and no amount of trimming can bring it back. Re-slug the full label, exactly
                # as `_split_merged` already does when hunting for a seam.
                key = PdfParser.slug(row.label, maxlen=self.SEAM_SLUG_LEN)
            key = key.replace("_", "")
            if key:
                rows.append((i, val, key))

        accounts = [a.replace("_", "") for _, a in schema]
        out: Dict[str, int] = {}
        src: Dict[str, int] = {}                # column -> the parsed row that filled it
        for j, ri in self._align([k for _, _, k in rows], accounts, relax_totals,
                                 annual_tail).items():
            self._claim(out, src, schema[j][0], rows[ri][0], rows[ri][1])

        self._anchor(out, schema, st, relax_totals, src, relax_merged_seam, annual_tail)
        self._split_fx_from_balance(out, src, st, schema)
        if relax_totals:
            self._recover_totals(out, st, src, relax_split_tail)
        return out

    def _split_fx_from_balance(self, out: Dict[str, int], src: Dict[str, int],
                               st: Statement, schema: List[Tuple[str, str]]) -> None:
        """The FX line printed BLANK, so its label rode onto the closing balance's figures.

        ⚠️ **MEASURED ON VIC Q1-2026 (`corp`), 2026-08-28, and it wrote a wrong figure that
        BOTH GATES PASSED.** The filing prints

            Ảnh hưởng của thay đổi tỷ giá hối đoái quy đổi ngoại tệ        (no figure)
            Tiền và tương đương tiền cuối kỳ (70 = 50+60+61)      54,750,360   32,491,938

        A line with no figure of its own becomes `carry` in `table_rows`, so the next line's
        figures arrive under BOTH labels joined — and `slug` caps a key at 60 characters,
        which is exactly where "…tương đương tiền cuối kỳ" was cut off. The row therefore
        reads as a pure FX line, the ordered walk maps it to the FX column, and the statement
        comes out with **54,750,360 in the foreign-exchange adjustment and no closing balance
        at all** — a 54.75 tn FX effect on 72 tn of cash. `reconcile` never noticed (its
        closing-balance lookup fell through to the fuzzy text search and answered with the
        OPENING row), and `sane` had no band for a ticker with nothing on disk.

        ⚠️ **IT IS FIXED IN THE DEFAULT PATH AND THAT IS DELIBERATE.** Every comparable
        recovery in this file is a `ParseLayer` flag sitting late in the cascade, and that
        would be useless here: VIC's cash flow is ACCEPTED at `onnx@200`, layer 1 of 47, so a
        layer that fixes it is never reached. `PGB-1` and CLAUDE.md §6-2-unvicies each record
        the same trap from the other side — a half-right layer that passes the gates ends the
        cascade — and the lesson generalises: when the gates cannot see the defect, the repair
        cannot be an escalation.

        Four preconditions, and every one of them narrows it to this shape:

          * the statement is a CASH FLOW, and this chart of accounts has both an FX column and
            a closing-balance column (`insurance` has no closing line at all, so it is skipped
            rather than guessed at);
          * the closing column is EMPTY — a statement that already found its closing balance is
            never touched;
          * the row holding FX begins with the FX account's own wording, so this is the merge
            being described and not some other line that reached the FX column;
          * and what FOLLOWS that wording matches the closing account. The tail is re-slugged
            from the FULL label at `SEAM_SLUG_LEN`, because the 60-character cap is what
            destroyed the evidence in the first place — the same reason `annual_tail` re-slugs.

        ⚠️ **THE FIGURE MOVES; IT IS NOT COPIED.** The FX cell is left EMPTY, because the
        filing printed no FX figure — writing one would be a number nothing can attribute
        (§5 rule 2), and `_cash_flow_identity`'s `fx = 0` substitution already handles a filing
        that made no adjustment, but only when the arithmetic then closes to the đồng.
        """
        if st.report != CASH_FLOW:
            return
        cols = {c for c, _ in schema}
        fx_col = next((c for c in self.C_CASH_FX if c in cols), None)
        close_col = next((c for c in self.C_CASH_CLOSE if c in cols), None)
        if fx_col is None or close_col is None:
            return
        if close_col in out or fx_col not in out or fx_col not in src:
            return
        ri = src[fx_col]
        if ri is None or ri >= len(st.rows):
            return
        accounts = dict(schema)
        head = accounts[fx_col].replace("_", "")
        full = PdfParser.slug(st.rows[ri].label,
                              maxlen=self.SEAM_SLUG_LEN).replace("_", "")
        if len(full) <= len(head):
            return
        if self._label_score(head, full[:len(head)]) < Statement.NAME_MATCH:
            return
        tail = full[len(head):]
        if not tail or self._label_score(accounts[close_col].replace("_", ""),
                                         tail) < Statement.NAME_MATCH:
            return
        value = out.pop(fx_col)
        src.pop(fx_col, None)
        self._claim(out, src, close_col, ri, value)
        self._warn(f"    cash flow: the FX line printed no figure and its label rode onto the "
                   f"closing balance — {value:,} moved from `{fx_col}` to `{close_col}`, "
                   f"FX left empty")

    # A row OCR built by merging a SECTION HEADER with the numbered line beneath it. The filing
    # numbers its lines ("09.", "15."), so a two-digit group sitting mid-label is the seam:
    # what precedes it is the header, what follows is the line that actually owns the figure.
    # ACB's Q1-2022 reads "Những thay đổi về tài sản hoạt động" and "09. (Tăng)/giảm các khoản
    # tiền gửi…" as one row carrying 2,671,012; the header wins on containment, so a title ends
    # up holding a figure it cannot have while line 09 comes out empty. Splitting at the seam
    # puts it back. The length floors keep this off ordinary labels — a header runs well over 12
    # characters, and a "…_v_1" note reference is a single digit at the END, not two in the
    # middle.
    # The seam is a line MARKER: the two-digit number the cash flow prints ("09.", "15.") or the
    # roman numeral the balance sheet prints ("XII."). ACB's Q1-2022 merges "b. Hao mòn bất động
    # sản đầu tư" — a dash — with "XII. Tài sản có khác" and its 7,710,713, so the amortisation
    # line ends up holding another section's total. Single-letter numerals are deliberately
    # excluded: "…_v_1" and "…_v_6" are note references sitting at the end of ordinary labels,
    # and splitting on those would shred them.
    MERGED_SEAM_RE = re.compile(
        r"^(.{12,}?)_(?:\d{2}|xviii|xvii|xvi|xiv|xiii|xii|xix|xi|xv|viii|vii|vi|iv|ix|ii)_(.{5,})$")

    # ⚠️ THE SAME SEAM, PLUS A SINGLE-LETTER NUMERAL — reached only from `relax_merged_seam`.
    # Section I is the one a bank balance sheet merges most often, because "B. NỢ PHẢI TRẢ VÀ
    # VỐN CHỦ SỞ HỮU" and "I. Các khoản nợ Chính phủ và NHNN" are printed adjacent: BID's
    # Q3-2025 reads them as one row carrying 215,823,611, and "vốn chủ sở hữu" then sits inside
    # that label, so the row answers the EQUITY anchor with the government-debt figure — 7.0 %
    # of assets against BID's usual 5.2-5.4 %.
    #
    # The strict pattern excludes single letters deliberately, because "…_v_1" and "…_v_6" are
    # note references at the END of ordinary labels. That exclusion is broader than it needs to
    # be: `(.{5,})$` already requires five characters after the marker, which no such reference
    # has. It stays behind a layer regardless — a seam is a guess about what OCR merged, and a
    # wrong guess moves a figure to the wrong account rather than refusing it.
    MERGED_SEAM_RE_LOOSE = re.compile(
        r"^(.{12,}?)_(?:\d{2}|xviii|xvii|xvi|xiv|xiii|xii|xix|xi|xv|viii|vii|vi|iv|ix|ii|i)_(.{5,})$")

    # How far to re-slug a label when hunting for the seam. `PdfParser.slug` caps a row key at
    # 60 characters, which is ample for a real line and far too short for a merged one — and a
    # merged row is long BY DEFINITION, so the marker that reveals the seam is exactly what the
    # cap throws away. ACB's Q1-2022 balance sheet reads "…B NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU / Các
    # khoản nợ Chính phủ và Ngân hàng Nhà nước / II Tiền gửi và vay các TCTD khác V.8" as one row
    # carrying 44,323,457: the key stops at "…cac_khoan_no_chinh_phu", so the "II" that owns the
    # figure is gone and nothing can place it.
    SEAM_SLUG_LEN = 400

    def _split_merged(self, key: str, label: str = "",
                      relax_merged_seam: bool = False) -> str:
        """The part of a merged row's label that owns its figure — the text after the last
        section marker OCR swept in, or the key unchanged when there is no seam.

        The FULL label is searched first and the (capped) key second, but a seam found in
        neither leaves the key exactly as it was. That is what keeps this off ordinary rows: a
        long label with no marker in it — ACB's `iv_cac_cong_cu_tai_chinh_phai_sinh…v_3_von_tai_tro…`
        is one — comes back byte-identical to before.
        """
        from web_scraper.cafef_pdf_parser import PdfParser

        candidates = [key]
        if label:
            candidates.insert(0, PdfParser.slug(label, maxlen=self.SEAM_SLUG_LEN))
        seam = (self.MERGED_SEAM_RE_LOOSE if relax_merged_seam
                else self.MERGED_SEAM_RE)
        for cand in candidates:
            m = seam.match(cand)
            if m:
                return m.group(2)
        return key

    @staticmethod
    def _claim(out: Dict[str, int], src: Dict[str, int], col: str,
               row_i: Optional[int], val: int) -> None:
        """Write a value and record WHICH parsed row it came from, evicting any other column
        that same row had filled.

        One printed line is one line item, and until this was enforced across the whole mapping
        a single row could answer two accounts at once. ACB's Q1-2022 prints "Dự phòng rủi ro
        khác" and "TỔNG NỢ PHẢI TRẢ" on what OCR reads as one row: the ordered pass gave its
        480,433,095 to `vii_3_du_phong_rui_ro_khac`, then `_anchor` gave the same figure to
        `tong_no_phai_tra`. The second is right; the first is a provision line holding total
        liabilities, and nothing downstream could tell, because both were merely "mapped".
        """
        if row_i is not None:
            for c, ri in list(src.items()):
                if ri == row_i and c != col:
                    out.pop(c, None)
                    src.pop(c, None)
        out[col] = val
        src[col] = row_i

    # Tie-break only: a hair of cost per schema line stepped over, so that among alignments of
    # equal score the COMPACT one wins. Far too small (0.001 against a 0.80 match floor) to buy
    # a wrong match with a shorter path.
    SCHEMA_GAP = 0.001

    def _align(self, keys: List[str], accounts: List[str],
               relax: bool, annual_tail: bool = False) -> Dict[int, int]:
        """Best monotonic alignment of parsed rows onto schema lines -> {schema index: row index}.

        The ordered walk this replaces was greedy: each row took the best account still open
        ahead of it and the cursor never went back, so whichever row asked FIRST won — and
        accounting names NEST, so the row that asks first is very often the wrong one. ACB's
        Q1-2022 balance sheet prints the parent "Tiền gửi VÀ cho vay các TCTD khác" (54,337,806)
        above its two children; the parent's name contains "cho vay các TCTD khác", so it scored
        the containment 0.95 against `iii_2` and took it, the real `iii_2` (2,960,720) arrived to
        find the cursor past it and settled for `iii_3`, and a provision line ended up holding a
        loan balance. The same shape cost the income statement its credit-provision line, where
        the wrapped fragment "ro_tin_dung" outbid the exact match one row below it.

        Scoring the whole grid and maximising the TOTAL fixes it with no new threshold, because
        the right answer is worth more: parent->iii_, child->iii_1, child->iii_2 scores
        0.70 + 1.00 + 1.00 against the greedy 0.95 + 0.95. Order is preserved by construction —
        this is sequence alignment, and the schema's own sequence is still what keeps a fuzzy
        match honest. Short damaged fragments keep working, since one only has to beat whatever
        else competes for that line: "hoanlai" still answers line 8, "chiu_rui_ro" still answers
        `hdkd_19`.
        """
        n, m = len(keys), len(accounts)
        if not n or not m:
            return {}
        f = [[0.0] * (m + 1) for _ in range(n + 1)]
        bt = [[0] * (m + 1) for _ in range(n + 1)]     # 0 skip row, 1 skip account, 2 match
        for i in range(1, n + 1):
            fi, fp, bi = f[i], f[i - 1], bt[i]
            key = keys[i - 1]
            for j in range(1, m + 1):
                best, b = fp[j], 0
                cand = fi[j - 1] - self.SCHEMA_GAP
                if cand > best:
                    best, b = cand, 1
                s = self._label_score(accounts[j - 1], key, relax, annual_tail)
                if s >= self.SCHEMA_MATCH:
                    cand = fp[j - 1] + s
                    if cand > best:
                        best, b = cand, 2
                fi[j], bi[j] = best, b

        pairs: Dict[int, int] = {}
        i, j = n, m
        while i > 0 and j > 0:
            b = bt[i][j]
            if b == 2:
                pairs[j - 1] = i - 1
                i -= 1
                j -= 1
            elif b == 1:
                j -= 1
            else:
                i -= 1
        return pairs

    # The grand-total lines, and the printed-label variants that identify each even when the
    # strict fuzzy match rejects them. A row's key (separators stripped) that CONTAINS one of
    # these substrings IS that total — "tong cong tai san" for total assets (the schema expects
    # "tong tai san"; the extra "cong"/"cộng" = "sum" drops the fuzzy ratio below threshold), and
    # "tong no phai tra va" for the grand total of resources (the "…va…" = "and equity" is what
    # distinguishes it from total LIABILITIES alone, and survives even when OCR merges the label
    # into the retained-earnings line above it, as in ACB Q1-2019).
    TOTAL_ALIASES = {
        "tong_tai_san": ("tongtaisan", "tongcongtaisan"),
        "tong_no_phai_tra_va_von_chu_so_huu": ("tongnophaitrava", "tongcongnguonvon"),
    }

    # The cash flow's two dated balance lines, in the order the filing prints them.
    CASH_BALANCES = ("hdtc_v_tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_dau_ky",
                     "hdtc_vii_tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_cuoi_ky")

    def _is_cash_tail(self, key: str) -> bool:
        """Is this row one of the cash flow's two DATED balance lines?

        Matched by CONTAINMENT, not by prefix. The section numeral is printed hard against the
        label and OCR keeps a fragment of it: ACB's Q1-2023 opening balance comes back as
        `t_tien_va_cac_khoan_tuong_duong_tien_tai_ngay_1_thang_1`, and that one stray leading
        character was enough for a `startswith` test to miss the line entirely — leaving the
        statement with no opening balance, unverifiable, and refused, although every figure on
        the page had been read correctly.

        Containment stays exact where it matters: the components printed underneath begin
        "Tiền và các khoản tương đương tiền GỒM CÓ", which does not contain "…tiền TẠI…", so
        the header of the breakdown is still not mistaken for a balance line.
        """
        return self.CASH_TAIL in key.replace("_", "")

    # The cash phrase WITHOUT the trailing "tại" that dates it. Deliberately not CASH_TAIL and
    # deliberately not used in its place: it also matches the breakdown header printed under the
    # closing balance ("Tiền và các khoản tương đương tiền GỒM CÓ"), which carries a component
    # figure and would be taken for the closing line itself.
    CASH_PHRASE = "tienvacackhoantuongduongtien"

    def _cash_balance_rows(self, st: Statement) -> List[Tuple[int, object]]:
        """The opening and closing balance rows when OCR has destroyed the DATE on both.

        `_is_cash_tail` recognises the balance lines by the date the filing prints on them. A
        second mangling defeats that: OCR merges the TRAILING words of the previous row onto the
        front of the next, so VCB's FY-2011 annual reads its two balance lines as
        `tien_va_cac_khoan_tuong_duong_tien_v` and
        `tai_thoi_diem_dau_nam_vii_tien_va_cac_khoan_tuong_duong_tien` — the date has moved off
        one line and onto the other, and neither contains "…tiền TẠI…". Both figures are read
        perfectly (96,678,346 and 124,705,018); only the labels are wrong.

        So this falls back to the undated cash phrase, and it runs ONLY when the dated scan found
        fewer than two rows — because the phrase alone is not specific enough to lead: it also
        matches the "gồm có" breakdown header, which is why ACB's statements, where the dated
        scan succeeds, never reach this. The breakdown header is excluded here as well, and
        whatever this returns still has to satisfy `_cash_flow_identity`, so a wrong pairing is
        rejected rather than written.
        """
        rows = [(i, r) for i, r in enumerate(st.rows)
                if self.CASH_PHRASE in r.key.replace("_", "")
                and "gomco" not in r.key.replace("_", "")
                and st._first_value(r.values) is not None]
        return rows if len(rows) >= 2 else []

    # A DATE ORPHANED ON ITS OWN ROW. When the balance line's label wraps, OCR can break it at
    # the date and put the figure on the continuation: ACB's Q3-2017 reads
    # `…tuong_duong_tien_tai_ngay` with an EMPTY current-period cell and `thang_9` on the next
    # row holding 15,044,850. The continuation is a bare date fragment — no account is named
    # "tháng 9" — which is what makes it safe to recognise and splice back.
    SPLIT_TAIL_KEY = re.compile(r"^(thang|ngay|nam)(_\d+)*$")

    def _split_tail_value(self, st: Statement, i: int) -> Optional[int]:
        """The figure belonging to a balance line whose label wrapped, or None.

        Only ever consulted when the balance row's OWN current-period cell is empty — the case
        where `_first_value` would otherwise fall through and return the COMPARATIVE column, i.e.
        the prior year's closing balance, which is indistinguishable from a right answer.
        """
        if i + 1 >= len(st.rows):
            return None
        nxt = st.rows[i + 1]
        if not self.SPLIT_TAIL_KEY.match(nxt.key):
            return None
        return nxt.values[0] if nxt.values else None

    def _recover_totals(self, out: Dict[str, int], st: Statement,
                        src: Optional[Dict[str, int]] = None,
                        split_tail: bool = False) -> None:
        """Fill a statement's subtotal columns from label variants (relaxed layers only).

        Runs after the ordered walk and `_anchor` have done their best and the statement STILL
        did not reconcile — so it can only add these few columns, and whatever it adds is
        re-checked by `reconcile` + `sane` before the row is accepted. A filing whose subtotals
        are genuinely unreadable is unaffected (nothing matches) and still falls through to the
        CafeF tabs. The LAST matching row wins: the grand total is printed at the foot of its
        section, below any partial subtotal that shares the substring.

        For the CASH FLOW the same "last one wins" settles its opening and closing balances,
        which are the same words with a different date (see CASH_TAIL). Position is the only
        sound discriminator: they tie on score, and length is actively misleading because OCR
        decides it — ACB's Q1-2026 opening reads 44 characters against a 44-character account,
        a perfect 1.00 ratio, while the real closing line scores 0.955 and lost to it. The
        filing prints opening first and closing last, so first and last is what they are.
        """
        if src is None:
            src = {}
        if st.report == CASH_FLOW:
            # Matched on the LABEL alone, with no requirement that the row carry a figure. A
            # balance line whose current-period cell is a dash — or whose figure OCR pushed onto
            # the next row when the label wrapped — still marks its POSITION, and position is
            # what the opening/closing pairing and the IV guess below are built on. Filtering it
            # out for having no value cost ACB's Q1-2022 its IV: row 33 holds the opening's label
            # and only the comparative figure, so dropping it left one dated row, no pairing, and
            # the -8,595,083 sitting immediately above went unclaimed.
            dated = [(i, r) for i, r in enumerate(st.rows) if self._is_cash_tail(r.key)]
            if len(dated) < 2:
                dated = self._cash_balance_rows(st)
            if len(dated) >= 2:
                # THE OPENING BALANCE IS TAKEN STRICTLY FROM THE CURRENT-PERIOD COLUMN — never
                # by falling through to the next populated one, the way every other figure here
                # is read. It is the one field on the statement where the comparative column is
                # INDISTINGUISHABLE from a correct answer: an opening balance simply IS a prior
                # period's closing, so a fall-through returns a number of exactly the right kind,
                # and the identity it feeds then "verifies" against it. ACB's Q1-2022 loses this
                # one cell to OCR and `_first_value` hands back 46,022,071 — Q1-2021's opening —
                # in place of the 82,601,567 the filing prints, so a statement whose closing
                # (74,021,373), IV and FX are all correct fails to close and is thrown away.
                # Left unmapped instead, `_cash_flow_identity` substitutes the previous year's
                # closing and the statement verifies exactly.
                opening = dated[0][1]
                # The closing is the LAST dated row that actually carries a figure, and never
                # the opening's own row: the pairing is positional, but a row the filing left
                # blank cannot BE the closing balance.
                closing = next((r for i, r in reversed(dated)
                                if i != dated[0][0]
                                and st._first_value(r.values) is not None), None)
                v = opening.values[0] if opening.values else None
                if v is not None:
                    self._claim(out, src, self.CASH_BALANCES[0], dated[0][0], v)
                else:
                    # It must be REMOVED, not merely left unset. The ordered walk in
                    # `map_to_schema` has already matched this row through the CASH_TAIL
                    # shortcut and written `_first_value` into the column — the very
                    # fall-through this is here to undo — so declining to overwrite leaves the
                    # comparative figure sitting there and nothing downstream can tell.
                    out.pop(self.CASH_BALANCES[0], None)
                if split_tail:
                    # THE CLOSING BALANCE MAY BE ON THE NEXT ROW. Re-pick it before the normal
                    # choice: the row whose label carries the date can be the one with an EMPTY
                    # current-period cell (its figure went to the continuation), and the test
                    # above — "the last dated row that carries a figure" — then selects it anyway
                    # on the strength of its COMPARATIVE column. ACB's Q3-2017 takes 13,316,705,
                    # which is Q3-2016's closing balance, while its own 15,044,850 sits one row
                    # below on `thang_9` and is exactly what its components sum to.
                    for ci, r in reversed(dated):
                        if ci == dated[0][0]:
                            continue
                        if r.values and r.values[0] is not None:
                            break
                        v = self._split_tail_value(st, ci)
                        if v is not None:
                            self._claim(out, src, self.CASH_BALANCES[1], ci, v)
                            closing = None          # claimed here; do not overwrite below
                            break
                if closing is not None:
                    ci = next(i for i, r in dated if r is closing)
                    self._claim(out, src, self.CASH_BALANCES[1], ci,
                                st._first_value(closing.values))
                # IV, the net movement, is printed immediately above the opening balance. OCR
                # merges its label into the financing-section header above it ("LƯU CHUYỂN TIỀN
                # TỪ HOẠT ĐỘNG TÀI CHÍNH 02 Tiền thu từ phát hành…"), so it never matches its
                # account by name even though its FIGURE is read correctly — ACB's Q1-2024 has
                # -9,499,874 sitting right there. Taking the row above the opening balance is a
                # guess, but a self-checking one: `_cash_flow_identity` immediately tests
                # opening + IV + fx == closing, so a wrong guess is rejected, not written.
                first_i = dated[0][0]
                iv = self.C_NET_CF[0]
                if first_i > 0 and iv not in out:
                    v = st._first_value(st.rows[first_i - 1].values)
                    if v is not None:
                        self._claim(out, src, iv, first_i - 1, v)

                # ⚠️ VI, THE FX ADJUSTMENT, IS PRINTED BETWEEN THE TWO BALANCES — and its
                # absence, not the balances', is what refuses these statements. Measured on BID
                # 2026-08-25: probing all 13 of its genuinely failed statements through the full
                # 26-layer cascade, **every one of the eight completed so far ends at
                # `[onnx@200+relax] cash flow unverifiable — fx not mapped`**. The relaxed layer
                # recovers the opening and closing balances by position and then cannot prove
                # them, because `_cash_flow_identity` needs all four terms and the FX line's
                # label ("VI. Điều chỉnh ảnh hưởng của thay đổi tỷ giá") does not reach
                # SCHEMA_MATCH on these scans.
                #
                # The chart of accounts prints HDTC_48 opening, HDTC_49 FX, HDTC_50 closing —
                # adjacent, in that order — so when the recovered pair sits exactly two rows
                # apart the row between them is the FX line. This is the SAME guess the IV
                # recovery above makes, with the same safety: `_cash_flow_identity` tests
                # opening + IV + fx == closing to the đồng immediately afterwards, so a wrong
                # row is REJECTED, not written. It cannot loosen a gate — it can only let a gate
                # that was skipping for want of a term actually run.
                #
                # ⚠️ Two rows apart EXACTLY, never "somewhere between": a wider search would
                # start choosing among flow lines, and a plausible wrong FX that happens to make
                # the identity close is the one failure this must not manufacture.
                #
                # ⚠️ **AND THAT SAFETY ARGUMENT IS FALSE WHERE THE FOURTH TERM IS NOT FX —
                # measured 2026-08-27 on BID's FY-2015, CLAUDE.md §6-2-vicies.** The row between
                # the balances there is the MHB MERGER line, i.e. the genuine fourth term with
                # the wrong NAME, so the identity closes *because the arithmetic is right and
                # the account is wrong*, and it cannot reject what it confirms.
                #
                # ⚠️ **THE ROW MUST NAME ITSELF FX, ON EVERY LAYER — 2026-08-27, `P39`.** The
                # guard shipped the same day gated on `cash_extra_terms`, i.e. it was live on
                # the THREE layers carrying that flag and absent on the other forty-four,
                # `onnx@200+relax` among them — **layer 5 of 47**. Read off `cf_HOSE_BID.csv`
                # afterwards, the unguarded claim had already written merger cash into this
                # column TWICE, from two different documents, and the identity confirmed both
                # to the đồng:
                #
                #   Q4-2015  50,202,708 + 4,288,806 + **1,477,340** = 55,968,854   (MHB, FY-2015
                #            audited annual, `onnx@200+relax`)
                #   Q2-2017  65,521,789 + 2,648,425 + **1,540,994** = 69,711,208   (LienVietPost-
                #            Bank, Q2-2017 reviewed quarterly, `onnx@200+relax`)
                #
                # Nothing is lost by refusing them: the row is still COUNTED by
                # `_extra_cash_terms` on a `cash_extra_terms` layer, where it stands in for the
                # unattributable fourth term and is written to no column at all (§5 rule 2). A
                # statement that reaches no such layer is `missing`, which is the correct answer
                # for a figure this parser cannot attribute — §5 rule 24.
                #
                # ⚠️ `extra_terms` WAS this branch's only reader and the parameter is gone with
                # it. A knob that decides whether a guard applies is a knob that turns a guard
                # off, and the measurement above is what that cost.
                fx = self.C_CASH_FX[0]
                close_i = src.get(self.CASH_BALANCES[1])
                if (fx not in out and close_i is not None
                        and close_i - first_i == 2):
                    cand = st.rows[first_i + 1]
                    v = st._first_value(cand.values)
                    named_fx = self._label_score(
                        fx.replace("_", ""),
                        self._split_merged(cand.key, cand.label).replace("_", ""),
                    ) >= self.SCHEMA_MATCH
                    if v is not None and named_fx:
                        self._claim(out, src, fx, first_i + 1, v)
            return
        if st.report != BALANCE_SHEET:
            return
        for col, aliases in self.TOTAL_ALIASES.items():
            for ri, row in enumerate(st.rows):
                k = row.key.replace("_", "")
                if any(a in k for a in aliases):
                    v = st._first_value(row.values)
                    if v is not None:
                        self._claim(out, src, col, ri, v)
        return

    # The lines reconciliation stands on. They are unambiguous — no other line in a statement
    # is called "TỔNG TÀI SẢN" — so they are re-matched GLOBALLY, ignoring position.
    # ⚠️ **DERIVED FROM THE ROLE TUPLES, NOT RE-LISTED — and the duplication is exactly how
    # this went wrong.** Until 2026-08-28 this was a hand-written literal of NINE bank column
    # names, so `_anchor`'s `if c in self.ANCHORS` filter selected 2 of 7 roles on the `corp`
    # chart and 2 on `insurance`: the position-independent re-match, which exists because "the
    # ordered walk drifts", simply did not run for total liabilities, equity, PBT, the net cash
    # movement or the closing balance on any non-bank filing. Deriving it means a column added
    # to a role can no longer be missed here.
    #
    # ⚠️ **THE SEVEN ROLES ARE THE ROLES IT HAS ALWAYS HAD.** `C_CASH_OPEN`, `C_CASH_FX` and
    # `C_FLOW_SECTIONS` are deliberately NOT anchored: adding them would change which row wins
    # on a BANK filing, and `_anchor`'s own docstring records that the two dated balance lines
    # are separated only by a position tie-break. Widening that competition is a separate
    # change with its own regression, not a side effect of this one.
    ANCHORS = (C_ASSETS + C_RESOURCES + C_LIABILITIES + C_EQUITY
               + C_PBT + C_NET_CF + C_CASH_CLOSE)
    ANCHOR_MATCH = 0.86      # stricter than the ordered pass: this one has no order to lean on
    # A heavily OCR-damaged anchor label can fall just under ANCHOR_MATCH while still being the
    # right line — ACB's Q4-2014 grand total reads "tong_ng_pha_tra_va_von_chu_sd_hoij" (nợ->ng,
    # phải->pha, sở->sd, hữu->hoij), scoring 0.808 against "tong no phai tra va von chu so huu".
    # It is accepted ONLY when the damaged label is nearly the FULL LENGTH of the target — which
    # is exactly what separates it from the documented false match: "tong von chu so huu" (total
    # EQUITY) scores 0.73 at 58% of the length, and plain equity 0.60 at 42%. A short label
    # scoring high against a long one is a coincidence; a full-length one is the line itself.
    ANCHOR_MATCH_LONG = 0.80      # relaxed floor, gated on the length ratio below
    ANCHOR_LEN_RATIO = 0.85       # damaged label must be >= this fraction of the target length

    def _anchor(self, out: Dict[str, int], schema: List[Tuple[str, str]],
                st: Statement, relax: bool = False,
                src: Optional[Dict[str, int]] = None,
                relax_merged_seam: bool = False,
                annual_tail: bool = False) -> None:
        """Re-match the subtotals without regard to position.

        The ordered walk drifts. Once it has advanced past a column, a row that belongs there
        lands on the next-best thing instead — VCB's Q2-2014 and Q2-2023 gave the GRAND TOTAL
        column the value of TỔNG NỢ PHẢI TRẢ, and `assets - resources` came out exactly equal to
        equity. These few lines are unambiguous (nothing else in a statement is called "TỔNG
        TÀI SẢN"), so they are searched for over the whole statement.

        The anchors COMPETE for rows rather than each taking its own best independently. One
        printed line is one line item, so it can answer only one anchor — and resolved
        separately they collide, because the anchor names NEST: "TỔNG NỢ PHẢI TRẢ" is a literal
        prefix of "TỔNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU", and "VỐN CHỦ SỞ HỮU" sits inside both.
        Each therefore scores the full containment 0.95 against the other's row, and ACB's
        Q1-2023 gave total liabilities the GRAND TOTAL's 611,223,523 — a figure that then looks
        entirely plausible and balances, while the real 548,693,358 sat one row above it.
        Scoring every (anchor, row) pair and assigning best-first, each row spent once, settles
        them against each other: the grand-total row matches its own anchor exactly (1.00) and is
        taken there, so total liabilities falls to the row that genuinely carries it.
        """
        from difflib import SequenceMatcher

        accounts = {c: a.replace("_", "") for c, a in schema if c in self.ANCHORS}
        cands = []
        for col, a in accounts.items():
            for ri, row in enumerate(st.rows):
                # first populated column = current period (see map_to_schema / _first_value):
                # an over-segmented grand-total row has its figure in index 1, not 0.
                val = st._first_value(row.values)
                if val is None:
                    continue
                # ⚠️ THE SPLIT KEY, NOT THE RAW ONE — but only when asked. The ordered walk
                # in `map_to_schema` has always scored `_split_merged(...)`; this did not, so a
                # row OCR built by merging a section HEADER with the line beneath it could still
                # answer an anchor with the wrong figure here. BID's Q3-2025 merges "B. NỢ PHẢI
                # TRẢ VÀ VỐN CHỦ SỞ HỮU" with "I. Các khoản nợ Chính phủ và NHNN", so "vốn chủ
                # sở hữu" sits inside the label and the EQUITY anchor took 215,823,611 — the
                # government-debt line. Split, that row answers `i_cac_khoan_no_chinh_phu_va_nhnn`
                # and equity is left ABSENT, which is the correct answer for a figure the
                # statement never printed on its own line (CLAUDE.md §5 rule 2).
                k = self._split_merged(row.key, row.label,
                                       relax_merged_seam).replace("_", "")
                r = self._label_score(a, k, relax, annual_tail,
                                      edge_containment=True)
                # Length ratio: how much of the target the OCR'd label actually spans (min/max
                # so a too-long label is penalised too). It also ranks the ties containment
                # creates, since 0.95 is awarded flat to the line itself AND to anything that
                # merely MENTIONS it — a short label scoring high against a long one is a
                # coincidence, a full-length one is the line itself.
                ln = min(len(a), len(k)) / max(len(a), len(k))
                if r >= self.ANCHOR_MATCH or (r >= self.ANCHOR_MATCH_LONG
                                              and ln >= self.ANCHOR_LEN_RATIO):
                    cands.append((r, ln, col, ri, val))

        # Ranked by score, then by how much of the account the label spans, then by POSITION —
        # later wins. The last is what separates the cash flow's two dated balance lines, which
        # are the same words with a different date and therefore tie exactly on both other keys
        # (0.95, 42/44): the closing balance is the one printed BELOW the opening, the same
        # reasoning `_recover_totals` uses for a grand total sitting at the foot of its section.
        # Without it ACB's Q1-2025 closing balance took the opening's 139,824,608.
        cands.sort(key=lambda c: (c[0], c[1], c[3]), reverse=True)
        # ⚠️ **AN ANCHOR MAY NOT STEAL A ROW THAT FITS ANOTHER ACCOUNT BETTER** — measured on
        # VIC Q1-2026 (`corp`) the day the non-bank anchors were added. Equity's account text
        # is "vốn chủ sở hữu", ELEVEN characters, so containment awards a flat 0.95 to every
        # line that merely mentions it, and the length-ratio tie-break then prefers the SHORT
        # impostor: "Quỹ khác thuộc vốn chủ sở hữu" (117,845 mn, ratio 0.48) beat the real
        # equity row (153,703,820 mn, ratio 0.32, its label polluted by the page header OCR
        # merged onto it). `_claim` then evicted `i_9_quy_khac_thuoc_von_chu_so_huu`, so ONE
        # anchor turned two correct cells into two wrong ones.
        #
        # The discriminator is not a threshold: the impostor is a line the chart of accounts
        # ALREADY HAS, and the ordered walk had placed it there EXACTLY (1.00 against 0.95).
        # A row whose own account fits it better than this anchor does is not this anchor's
        # line, whatever the containment score says.
        #
        # ⚠️ **STRICTLY better, and that is what preserves `_claim`'s documented case.** ACB's
        # Q1-2022 reads "Dự phòng rủi ro khác" and "TỔNG NỢ PHẢI TRẢ" as ONE row: both
        # accounts score 0.95 by containment, so the anchor still wins the tie and still takes
        # the row — which is the behaviour that case exists to pin.
        held_account = {c: a.replace("_", "") for c, a in schema}
        taken_col, taken_row = set(), set()
        for r, ln, col, ri, val in cands:
            if col in taken_col or ri in taken_row:
                continue
            if src:
                other = next((c for c, i in src.items() if i == ri and c != col), None)
                if other is not None and other in held_account:
                    k = self._split_merged(st.rows[ri].key, st.rows[ri].label,
                                           relax_merged_seam).replace("_", "")
                    if self._label_score(held_account[other], k, relax, annual_tail,
                                         edge_containment=True) > r:
                        continue
            # through _claim, so winning an anchor also RELEASES whatever else this row had
            # been given by the alignment pass — one printed line, one line item
            self._claim(out, src if src is not None else {}, col, ri, val)
            taken_col.add(col)
            taken_row.add(ri)

    SCHEMA_WINDOW = 25       # how far ahead in the chart of accounts a line may be found

    def _canonical(self, key: str, known: List[str]) -> str:
        """The column this label belongs to: an existing one if close enough, else itself.

        Matching is on the label with separators stripped, so a lost space or underscore is
        not a difference. A suffixed duplicate column ("..._2") is never a match target — it
        exists to separate two lines that really are distinct.
        """
        from difflib import SequenceMatcher

        flat = key.replace("_", "")
        best, score = None, 0.0
        for col in known:
            if re.match(r".*__\d+$", col):
                continue
            r = SequenceMatcher(None, flat, col.replace("_", "")).ratio()
            if r > score:
                best, score = col, r
        return best if score >= self.SIMILARITY else key

    def reconcile(self, st: Statement,
                  mapped: Optional[Dict[str, int]] = None,
                  verify_cash: bool = False,
                  open_ref: Optional[int] = None,
                  relax_components: bool = False,
                  cash_extra_terms: bool = False) -> Optional[str]:
        """None if the statement balances against its OWN printed subtotals, else why not.

        The subtotals are taken from the CANONICAL columns when the rows have been mapped —
        `mapped` — and only fall back to searching the OCR text when they have not. Searching
        the text is what most rejections actually were: the row was parsed, its figure correct,
        and the lookup simply could not recognise the name OCR had mangled.
        """
        if len(st.rows) < self.MIN_ROWS:
            return f"only {len(st.rows)} rows parsed"
        # A FRAGMENTED READING IS REFUSED BEFORE ANY OF ITS FIGURES ARE BELIEVED. The detector
        # splits one printed figure into two boxes on some scans, and both halves are plausible
        # numbers — VIC Q3-2014's balance sheet at onnx@200 reads `i_1_tien` as 158.154 against
        # a printed 945.186.158.154, 60 figures in all, while both grand totals survive whole
        # and every gate below passes. Nothing downstream can see that, so the only place to
        # stop it is here, and the only useful answer is to escalate: the same document at
        # onnx@300 splits NOTHING. `PdfParser.split_figures` carries the measurement that fixes
        # the gap, and it counts 0 on every bank filing checked.
        if st.split_figures:
            return (f"{st.split_figures} figure(s) split across two boxes — "
                    f"this reading is fragmented")

        def get(canonical: Tuple[str, ...], *text: str,
                reject: Tuple[str, ...] = ()) -> Optional[int]:
            if mapped:
                for c in canonical:
                    if c in mapped:
                        return mapped[c]
            return st.find(*text, reject=reject)

        if st.report == BALANCE_SHEET:
            assets = get(self.C_ASSETS, *self.TOTAL_ASSETS)
            resources = get(self.C_RESOURCES, *self.TOTAL_RESOURCES)
            if assets is None:
                return "no total assets"
            if resources is not None and not self._equal(assets, resources):
                return "assets != liabilities + equity"
            if resources is None:
                liab = get(self.C_LIABILITIES, *self.TOTAL_LIABILITIES)
                eq = get(self.C_EQUITY, *self.TOTAL_EQUITY)
                if liab is None or eq is None:
                    return "no total to balance against"
                if abs(assets - (liab + eq)) > assets * 0.02:
                    return "assets != liabilities + equity"

        if st.report == INCOME_STATEMENT:
            if get(self.C_PBT, *self.PBT) is None:
                return "no profit before tax"

        if st.report == CASH_FLOW:
            close = get(self.C_CASH_CLOSE, *self.CASH_CLOSE,
                         reject=self.CASH_OPEN_WORDS)
            # THE CLOSING BALANCE IS REQUIRED, not "either this or IV". Satisfying the gate with
            # IV alone is what wrote five quarters as `pdf` with an empty closing-balance column
            # — ACB Q3-2012, Q1-2015, Q1-2019 and VCB Q4-2011, Q2-2019 — each with the figure
            # sitting on the page, read correctly, under a label the strict match does not
            # recognise. That is the worst of both outcomes: the grid claims a parsed row and the
            # one column the statement is probed on is blank, so it reads as neither a gap nor a
            # value. Requiring it changes nothing where it already maps, and escalates the rest
            # to the relaxed layers, which recognise those labels and whose recovery
            # `_cash_flow_identity` then has to verify before anything is written.
            if close is None:
                return "no closing cash balance"
            # And CHECK it on every layer, not only the relaxed ones. Requiring the closing
            # balance without checking it merely trades one failure for a worse one: ACB's
            # Q3-2012 and Q1-2019 stop failing for an absent figure and start passing with the
            # COMPARATIVE column's — 54,560,217 and 22,356,020, each its own prior-year quarter,
            # each internally consistent and contradicted by nothing else on the page. The
            # breakdown printed beneath the closing balance states that figure a second time and
            # is the only thing that tells them apart. It fails open when the filing prints no
            # breakdown, so a statement that cannot answer it is judged exactly as before.
            bad = self._closing_breakdown(st, close, relax_components)
            if bad:
                return bad
            # The IDENTITY, by contrast, stays on the relaxed layers only. It tests the whole
            # statement at once and so cannot say WHICH term is wrong — and on a strict layer the
            # wrong one is usually not the closing balance. ACB's FY-2013 reads its closing
            # 9,762,451 and its opening 16,668,138 correctly at nearly every layer (the opening
            # matches Q4-2012's closing exactly), but IV maps to -6,905,687, which is precisely
            # closing - opening: a figure that already absorbs the FX line. Adding the mapped fx
            # of -445,111 on top double-counts it, the identity misses by exactly that, and a
            # sound quarter is thrown away. On a relaxed layer the mapping was recovered by label
            # variant and has to be proved, so there it still runs (fix #12).
            if verify_cash:
                bad = self._cash_flow_identity(
                    mapped or {}, open_ref,
                    st=st if cash_extra_terms else None)
                if bad:
                    return bad
        return None

    # The statement's own arithmetic: closing = opening + what moved + the FX adjustment. The
    # movement is IV when it was mapped, else the section subtotals it is the sum of.
    #
    # ⚠️ **ALL FOUR CHARTS, for the reason C_ASSETS gives.** These three were bank-only until
    # 2026-08-28, which is why the identity could not run on a non-bank filing AT ALL: VIC
    # Q1-2026 mapped its opening, its movement and (wrongly) its FX and still reported
    # "opening, movement, fx, closing not mapped", because not one of the three names it was
    # looking for exists in the `corp` chart. A check that cannot run is not a check that
    # passed — §5 rule 2 — and it left `_recover_totals`' positional FX guess as the only
    # thing standing between a merged label and a wrong FX column.
    C_CASH_OPEN = ("hdtc_v_tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_dau_ky",  # bank
                   "hdtc_v_tien_va_cac_khoan_tuong_duong_tien_dau_ky",   # securities
                   "hdtc_tien_va_tuong_duong_tien_dau_ky_60",            # corp
                   "hdtc_tien_va_tuong_duong_tien_dau_ky")               # insurance
    C_CASH_FX = ("hdtc_vi_dieu_chinh_anh_huong_cua_thay_doi_ty_gia",     # bank
                 # corp / securities / insurance all word it identically. ⚠️ `securities`
                 # carries a SECOND one (`…_ngoai_te_2`) and it is deliberately left out: the
                 # unsuffixed line is the one printed in the tail, and admitting both would
                 # make `next(...)` depend on tuple order rather than on the filing.
                 "hdtc_anh_huong_cua_thay_doi_ty_gia_hoi_doai_quy_doi_ngoai_te")
    # ⚠️ Unlike the others this one is a SUM, so several matches per chart is the correct
    # answer and the at-most-one invariant deliberately does not apply to it.
    C_FLOW_SECTIONS = ("hdkd_i_luu_chuyen_tien_thuan_tu_hoat_dong_kinh_doanh",   # bank
                       "hddt_ii_luu_chuyen_tien_thuan_tu_hd_dau_tu",
                       "hdtc_iii_luu_chuyen_tien_thuan_tu_hd_tai_chinh",
                       "hdkd_luu_chuyen_tien_thuan_tu_hoat_dong_kinh_doanh",     # corp
                       "hddt_luu_chuyen_tien_thuan_tu_hoat_dong_dau_tu",
                       "hdtc_luu_chuyen_tien_thuan_tu_hoat_dong_tai_chinh",      # corp / sec / ins
                       "hdkd_luu_chuyen_tien_thuan_su_dung_vao_hoat_dong_kinh_doanh",  # securities
                       "hddt_luu_chuyen_tien_thuan_tu_su_dung_vao_hoat_dong_dau_tu",
                       "hdkd_indirect_luu_chuyen_tien_thuan_tu_hoat_dong_kinh_doanh",  # insurance
                       "hdkd_direct_luu_chuyen_tien_thuan_tu_hdkd",
                       "hddt_luu_chuyen_tien_thuan_su_dung_vao_hoat_dong_dau_tu")

    def _cash_balance_span(self, st: Statement) -> Optional[Tuple[int, int]]:
        """(row index of the opening balance, row index of the closing balance), or None.

        The SAME pairing `_recover_totals` makes — dated rows first, the undated cash phrase as
        the fallback, opening first and closing the last one carrying a figure — factored out so
        the identity below can ask what the filing printed BETWEEN them without re-deriving it a
        second, subtly different way.
        """
        dated = [(i, r) for i, r in enumerate(st.rows) if self._is_cash_tail(r.key)]
        if len(dated) < 2:
            dated = self._cash_balance_rows(st)
        if len(dated) < 2:
            return None
        first_i = dated[0][0]
        close_i = next((i for i, r in reversed(dated)
                        if i != first_i and st._first_value(r.values) is not None), None)
        return None if close_i is None or close_i <= first_i else (first_i, close_i)

    def _extra_cash_terms(self, st: Statement) -> int:
        """What the filing printed BETWEEN its two cash balances, in the CURRENT period.

        ⚠️ **A BANK THAT ABSORBS ANOTHER BANK GAINS CASH THAT IS NEITHER A FLOW NOR AN FX
        EFFECT, AND THE CHART OF ACCOUNTS HAS NO COLUMN FOR IT.** BID prints such a line in
        three separate years — MHB 1,477,340 (2015) and 3,004,011 (2016), LienVietPostBank
        1,540,994 (2017) — and its FY-2016 consolidated cash flow prints TWO of them at once,
        one for each column. Every figure is read correctly at `crop_pad=6` and the statement
        was still refused for `fx not mapped`, because `_cash_flow_identity` needs a fourth term
        and had nowhere to take one from.

        ⚠️ **THE CURRENT-PERIOD CELL ONLY — never `_first_value`.** BID's 2016 column leaves the
        MHB line blank and the 2015 comparative beside it reads 1,477,340; falling through to it
        would add a prior-year figure to this year's identity and break a sum that closes
        exactly without it (55,806,145 + 6,711,633 + 3,004,011 = 65,521,789). A line whose own
        cell OCR could not read contributes nothing, the identity then misses, and the statement
        is refused — which is the right answer, not a loss.

        ⚠️ **POSITION IS THE WHOLE DEFINITION AND THAT IS DELIBERATE.** Matching these lines by
        label would mean guessing which words name a reconciling item, and the filings word them
        differently every time ("từ việc nhận sáp nhập MHB", "nhận từ … các công ty con khi hợp
        nhất"). Between the opening balance and the closing one a cash-flow statement prints
        nothing else — the FX adjustment and whatever else reconciles the two — so the span
        needs no vocabulary. Whatever it returns is immediately tested by the identity to the
        đồng, so a span that swept in a wrong row is rejected rather than written.
        """
        span = self._cash_balance_span(st)
        if span is None:
            return 0
        first_i, close_i = span
        total = 0
        for row in st.rows[first_i + 1:close_i]:
            v = row.values[0] if row.values else None
            if v is not None:
                total += v
        return total

    def _cash_flow_identity(self, mapped: Dict[str, int],
                            open_ref: Optional[int] = None,
                            st: Optional[Statement] = None) -> Optional[str]:
        """Closing must equal opening + movement + FX — the statement's own arithmetic.

        `_closing_breakdown` proves ONE figure, and that turned out not to be enough: ACB's
        Q1-2024 recovered a closing balance of 126,501,216 that agrees with its components to the
        đồng while the eleven other figures around it were read from the COMPARATIVE column
        (its FX line came out as Q1-2023's -43,527 against the -70,648 the filing prints). The
        row reconciled and was written, and was wrong.

        This ties the closing balance back to the opening one through the flows, so the interior
        of the statement has to hold together too, and it is what separates that from the sound
        recoveries: Q1-2026 gives 163,213,792 - 21,438,814 - 336,492 - 89,055 = 141,349,431
        exactly.

        Applied only when the opening, the FX line and at least one flow subtotal all mapped —
        a statement that did not yield them cannot answer this and is judged as before, and this
        runs on the relaxed layers alone, so a quarter the strict layers already read is never
        subjected to it.

        `st` is passed ONLY by a `cash_extra_terms` layer, and only then is the fourth-term
        retry below reachable. Every other caller keeps the identity it has always had.
        """
        close = next((mapped[c] for c in self.C_CASH_CLOSE if c in mapped), None)
        open_ = next((mapped[c] for c in self.C_CASH_OPEN if c in mapped), None)
        fx = next((mapped[c] for c in self.C_CASH_FX if c in mapped), None)
        net = next((mapped[c] for c in self.C_NET_CF if c in mapped), None)
        if net is None:
            sections = [mapped[c] for c in self.C_FLOW_SECTIONS if c in mapped]
            net = sum(sections) if sections else None

        # A relaxed recovery must be VERIFIABLE, not merely un-contradicted. Letting a statement
        # through because it failed to yield the lines that would test it is how ACB's Q1-2023
        # was written: its closing balance is right and agrees with its components, while
        # `hdkd_13` reads 96 for a printed (438.096), `hdkd_20` holds the line below it, and one
        # investing line took the comparative column. The breakdown proves one figure; only this
        # ties the whole statement together, so if it cannot run, the CafeF tabs are the better
        # source — they are keyed by item code and cannot mis-assign a line at all.
        # An opening balance OCR could not read is not a dead end. "Đầu kỳ" is 1 January, so it is
        # the CLOSING BALANCE OF THE PREVIOUS YEAR'S Q4 — a figure this run has already accepted
        # and verified. Substituting it costs nothing in rigour because the identity must then
        # close EXACTLY: ACB's Q1-2022 gives 82,601,567 - 8,595,083 + 14,889 = 74,021,373, the
        # closing its own printed components independently confirm. A statement whose interior
        # came from the comparative column cannot pass this — its IV and FX belong to a different
        # period, so the sum misses — which is what keeps Q1-2024's failure mode caught.
        if open_ is None and open_ref is not None and None not in (close, net, fx):
            if open_ref + net + fx == close:
                open_ = open_ref
        # A filing that made no FX adjustment prints no such line, and demanding one would refuse
        # a statement that verifies perfectly without it: VCB's FY-2011 closes
        # 96,678,346 + 28,026,672 = 124,705,018 with nothing left over, and its Q2-2019 likewise.
        # The absent line may stand in as zero ONLY when the identity then holds to the đồng —
        # no tolerance at all, where the check below allows `_equal`. An FX line that exists and
        # was missed would have to be exactly zero to slip through, which is the same as it not
        # being there.
        if fx is None and None not in (close, open_, net) and open_ + net == close:
            fx = 0
        # ⚠️ THE FILING MAY PRINT A FOURTH TERM THE CHART OF ACCOUNTS HAS NO COLUMN FOR, and
        # then the identity is unanswerable rather than false. `_extra_cash_terms` reads what
        # the statement printed BETWEEN its two balances — the FX line if it is there, plus
        # whatever else reconciles them — and stands in for `fx`, which it already contains.
        # Tried BEFORE the verdict, so it rescues both shapes of failure: "fx not mapped", where
        # nothing between the balances matched the FX account (BID's FY-2016, which prints TWO
        # merger lines so the positional guess never fires), and "does not close", where the FX
        # column was filled from a row that is not FX. ⚠️ It is also what makes the guard in
        # `_recover_totals` free: BID's FY-2015 has ONE row between its balances and the guess
        # DOES fire there, so refusing to call merger cash "FX" would leave a sound statement
        # unverifiable — unless this counts it, which it does (50,199,476 + 4,129,579 +
        # 1,477,340 = 55,806,145). The account is left empty; the arithmetic still holds.
        #
        # ⚠️ **EXACTLY, TO THE ĐỒNG — no `_equal` tolerance**, the same bar the `fx = 0`
        # substitution above is held to and for the same reason: this is a span of rows, not a
        # named line, so the only thing separating a sound recovery from a wrong one is that
        # four independently-read figures agree to the last unit. BID's FY-2016 gives
        # 55,806,145 + 6,711,633 + 3,004,011 = 65,521,789, and the total is corroborated outside
        # this filing by the opening balance Q2/Q3/Q4-2017 each print.
        #
        # ⚠️ **AND THE TERM IS NOT WRITTEN ANYWHERE.** It is admitted to the CHECK and to
        # nothing else: putting merger cash in the FX column would be a wrong figure that the
        # identity itself then confirms (CLAUDE.md §6-2-vicies). A number nothing can attribute
        # is left absent — §5 rule 2.
        if st is not None and None not in (close, net):
            extra = self._extra_cash_terms(st)
            base = open_ if open_ is not None else open_ref
            if base is not None and base + net + extra == close:
                return None
        if close is None or open_ is None or fx is None or net is None:
            missing = [n for n, v in (("opening", open_), ("movement", net), ("fx", fx),
                                      ("closing", close)) if v is None]
            return f"cash flow unverifiable — {', '.join(missing)} not mapped"
        if not self._equal(open_ + net + fx, close):
            return (f"cash flow does not close: opening {open_:.6g} + movement {net:.6g} "
                    f"+ fx {fx:.6g} != closing {close:.6g}")
        return None

    def _closing_breakdown(self, st: Statement, close: int,
                           relax_components: bool = False) -> Optional[str]:
        """Check the closing cash balance against the components printed beneath it.

        Alone among the three statements the cash flow has no internal identity to test — the
        old check only asked whether a subtotal was PRESENT, which is not reconciliation at all,
        and a closing balance mis-read by a whole digit passed it. ACB's Q1-2023 reads
        196,922,247 where the filing prints 96,922,247 (the leading 1 is a recognition error at
        every DPI; Q1-2024's comparative column prints the true figure). It is 6.7x the running
        median, so the magnitude band waves it through too.

        But the statement states the figure TWICE: right under the closing balance it prints
        "Tiền và các khoản tương đương tiền gồm có" and lists the components, which must add up
        to it. That is a genuine reconciliation, and it is what tells a misread digit from a
        sound read: Q1-2023's components come to 106,922,247 against a claimed 196,922,247,
        while Q3-2022's come to 72,894,066 against exactly that.

        Only ever applied when the breakdown was actually parsed (two or more components), so a
        filing that does not print one, or whose components OCR lost, is judged as before rather
        than rejected for a check it cannot answer.
        """
        # the closing balance is the LAST of the dated cash-tail lines (the opening one precedes
        # it); its components are the rows that follow, until the list ends
        last = None
        for i, row in enumerate(st.rows):
            if self._is_cash_tail(row.key):
                last = i
        if last is None:
            return None

        # A component is recognised by CONTAINING its marker, not by starting with it: the
        # "gồm có" header is printed on its own line and OCR merges it into the first component,
        # so that row reads "tiền và các khoản tương đương tiền gồm có tiền mặt vàng bạc đá quý"
        # — the marker is there, just not at the front. Matching on the prefix found nothing at
        # all and the check silently never ran. The breakdown is the last block on the page, so
        # the scan runs to the end and simply skips anything that is not a component.
        markers = (self.CASH_COMPONENT_RELAXED if relax_components
                   else self.CASH_COMPONENT)
        parts = []
        for row in st.rows[last + 1:]:
            k = row.key.replace("_", "")
            if not any(m in k for m in markers):
                continue
            # STRICTLY the current period here, not `_first_value`. A component the bank did not
            # hold this quarter is printed "-", and falling through to the next populated column
            # adds the PRIOR year's figure to a current-year sum: ACB's Q1-2025 holds no
            # "Chứng khoán đầu tư" but reported 1,003,259 of it in Q1-2024, which turned a
            # breakdown that agrees exactly into one that overshoots by that amount.
            v = row.values[0] if row.values else None
            if v is None:
                continue
            # "GỒM CÓ" IS A HEADER, AND ON ITS OWN LINE IT RESTATES THE TOTAL. ACB's Q3-2010
            # prints "Tiền và các khoản tương đương tiền gồm có" carrying 28,792,816 — the very
            # figure the four component lines beneath it add up to. Counted as a component it
            # doubles the breakdown EXACTLY (57,585,632 against a claimed 28,792,816) and the
            # check rejects a statement whose every figure is right, as a misread digit. Five
            # quarters were lost this way (Q3-2010, Q4-2010, Q1-2012, Q2-2012, Q1-2013).
            #
            # The marker stays in CASH_COMPONENT for the OTHER mangling it was added for, where
            # OCR MERGES the header into the first component ("…tương đương tiền gồm có tiền mặt
            # vàng bạc đá quý") and the row genuinely does hold a component's value. That row
            # does not equal the closing balance, so it is untouched.
            #
            # Skipping only the exact-total case cannot hide a real discrepancy: a component that
            # genuinely equalled the whole balance would leave fewer than two parts, and the
            # check then falls open and judges the statement exactly as before.
            if "gomco" in k and self._equal(v, close):
                continue
            parts.append(v)
        if len(parts) < 2:
            return None                     # no breakdown to check against

        total = sum(parts)
        if not self._equal(total, close):
            return (f"closing cash {abs(close):.6g} != its own components "
                    f"{abs(total):.6g} (misread digit?)")
        return None

    # Two figures that should be identical, allowing for OCR. VCB's Q1-2020 balance sheet reads
    # total assets 1,144,270,267 and total resources 1,144,270,262 (in millions) — the same
    # figure with ONE digit misread, 4.4e-6 apart. An absolute tolerance of 2 rejected it.
    #
    # 1e-5 is a millionth of a percent, and still nowhere near a real discrepancy: the errors
    # that matter are whole wrong rows (Q2-2014 was out by exactly the equity figure, ~9%) or a
    # digit inserted into the total (Q2-2018 reads 10x). Those are 3-6 orders of magnitude
    # larger and are still caught.
    EQUAL_REL = 1e-5

    @classmethod
    def _equal(cls, a: int, b: int) -> bool:
        return abs(a - b) <= max(2, abs(a) * cls.EQUAL_REL)

    @staticmethod
    def _period_end(period: str):
        """The last day of the quarter — the floor a signing date must clear."""
        from datetime import date

        q, y = int(period[1]), int(period.split("-")[1])
        return {1: date(y, 3, 31), 2: date(y, 6, 30),
                3: date(y, 9, 30), 4: date(y, 12, 31)}[q]

    def _probe(self, report: str, mapped: Dict[str, int],
               st: Statement) -> Optional[int]:
        """The one figure a statement is size-checked on."""
        canonical, text = {
            BALANCE_SHEET: (self.C_ASSETS, self.TOTAL_ASSETS),
            INCOME_STATEMENT: (self.C_PBT, self.PBT),
            CASH_FLOW: (self.C_CASH_CLOSE, self.CASH_CLOSE),
        }[report]
        for c in canonical:
            if c in mapped:
                return mapped[c]
        # ⚠️ the same disqualifier `reconcile` applies, and for the same reason: this
        # is the figure `sane` bands a cash flow on, so answering it with the OPENING balance
        # poisons the magnitude history for every quarter that follows it (SAN-1).
        return st.find(*text, reject=self.CASH_OPEN_WORDS if report == CASH_FLOW else ())

    def sane(self, st: Statement, history: List[int],
             mapped: Optional[Dict[str, int]] = None) -> Optional[str]:
        """Magnitude guard against the quarters already accepted.

        Reconciliation is ratio-based: it cannot see a UNITS error or a CUMULATIVE column,
        because both balance perfectly and are still wrong. This is the only thing that does.
        """
        got = self._probe(st.report, mapped or {}, st)
        if got is None or not history:
            return None

        # THE COMPARATIVE COLUMN READ AS THE CURRENT ONE. Every statement prints the prior
        # period beside the current one, and when the column clustering slips by one the whole
        # statement is taken from the comparative — internally consistent, correctly signed, of
        # exactly the right magnitude. Reconciliation cannot see it (the prior period balances
        # just as well as this one) and neither can the band below.
        #
        # The tell is that the figure is one we have ALREADY ACCEPTED: ACB's Q4-2022 annual
        # filing read total assets 527,769,944 — to the đồng, the Q4-2021 figure printed in the
        # column beside it. Two quarters agreeing on a 9-to-12-digit total to the last unit is
        # not something a going concern does; it means the wrong column was read.
        #
        # This was latent until the anchors were made to compete: before that, a mis-clustered
        # ACB Q4-2022 also produced a garbage grand total, so reconcile rejected it for the
        # wrong reason and the cascade escalated to a layer that read it correctly. Now that the
        # totals agree with each other, the statement is coherent and only this catches it.
        if got and got in set(history):
            return (f"probe {abs(got):.3g} exactly equals an already-accepted quarter "
                    f"(comparative column read as the current one?)")

        ref = sorted(abs(v) for v in history)
        median = ref[len(ref) // 2]
        if median and abs(got) and not (median / 20 <= abs(got) <= median * 20):
            return (f"magnitude {abs(got):.3g} vs typical {median:.3g} "
                    f"(units? cumulative column? OCR misread?)")
        return None

    # ──────────────────────────────────────────────────────────────────────
    # Build
    # ──────────────────────────────────────────────────────────────────────

    def _existing(self, exchange: str, symbol: str, template: str,
                  report: str) -> Dict[str, dict]:
        """{period: row} already on disk for this statement, or {} — the base a partial run
        merges into, and where a Q1's `open_ref` comes from when this run did not parse the
        previous Q4."""
        path = statement_path(template, report, exchange, symbol)
        if not os.path.exists(path):
            return {}
        try:
            with open(path, encoding="utf-8-sig") as f:
                return {r["period"]: r for r in csv.DictReader(f) if r.get("period")}
        except Exception as e:
            self._warn(f"  could not read existing {path}: {e}")
            return {}

    def _skippable_years(self, exchange: str, symbol: str, template: str,
                         docs: List[dict]) -> set:
        """Years in which EVERY attempted quarter already reads `source == 'pdf'` in all
        three statements — i.e. nothing left for a re-parse to win.

        ⚠️ THE UNIT IS A YEAR, NOT A QUARTER, AND THAT IS A CORRECTNESS REQUIREMENT.
        `_decumulate` turns a cumulative income statement into a standalone quarter as
        `YTD − (Q1..Q(q-1))`, and it takes those priors from THIS RUN'S `data` — a
        quarter whose priors are absent is DROPPED, not guessed. So skipping Q1..Q3 of
        2014 because they are already `pdf`, while Q4-2014 still needs parsing, would
        delete the very Q4 the run exists to fix, and it would do so every time. Keeping
        a year whole means the priors are always present. It also matches the documented
        behaviour that "fixing a Q1/Q2 pays for its Q4 as well".

        Cross-YEAR dependencies are already safe: a Q1's `open_ref` is read back from the
        file on disk when this run did not parse the previous Q4.
        """
        existing = {r: self._existing(exchange, symbol, template, r) for r in REPORTS}
        by_year: Dict[int, List[str]] = {}
        for d in docs:
            by_year.setdefault(int(d["period"].split("-")[1]), []).append(d["period"])

        return {
            year
            for year, periods_ in by_year.items()
            if all(
                existing[report].get(period, {}).get("source") == "pdf"
                for period in periods_
                for report in REPORTS
            )
        }

    def build(self, exchange: str, symbol: str,
              periods: Optional[List[str]] = None,
              use_api: bool = False,
              merge: Optional[bool] = None,
              allow_parent: bool = False,
              period_min: Optional[str] = FINANCIALS_PERIOD_MIN,
              skip_existing: bool = True) -> Dict[str, int]:
        """Parse the archive into the three statement CSVs.

        `periods` restricts which quarters are PARSED; `merge` decides what happens to the ones
        that are not.

        ⚠️ **A SUBSET RUN MUST MERGE, or it destroys the quarters it did not parse.** The grid is
        rebuilt from what this run holds in memory, so `build(periods=["Q4-2010"])` used to write
        a file in which every other quarter had lost its `pdf` row and been re-filled from the
        CafeF tabs — a 4-hour full run thrown away by a 6-minute one. Merging upserts instead:
        only the quarters this run actually produced are rewritten, the rest are left exactly as
        they were, and the file stays in quarter order. Defaults to on whenever `periods` is
        given, off for a full run (which legitimately owns the whole grid).

        Two things are weaker in a subset run and are compensated where possible: `sane` has no
        neighbouring quarters to judge magnitude against (it fails open), and `open_ref` is read
        back from the file on disk rather than from this run's own Q4.

        ⚠️ **AND `sane` CAN FAIL CLOSED IN A SUBSET RUN, WHICH THIS DOCSTRING USED TO DENY —
        measured 2026-08-24.** It judges magnitude against the quarters accumulated in THIS
        run, so a subset spanning odd period types gives it a misleading neighbourhood rather
        than an empty one: VCB's Q2-2009 cash flow reconciles cleanly on its own (`reconcile`
        and `sane` both `None` at `onnx@200`, with and without `open_ref`) and a five-quarter
        run holding three Q4 annuals plus two 2009 quarters rejected it. **Use `periods` to
        PROBE, never to PRODUCE** — an authoritative grid needs `skip_existing=False` and no
        `periods`. CLAUDE.md §6-2-decies.

        `skip_existing=True` (the default, matching every other scraper here) drops any YEAR
        whose quarters already read `source == 'pdf'` in all three statements — there is
        nothing a re-parse could win there, and at ~2.4 h per ticker that is the difference
        between a re-run costing minutes and costing hours. ⚠️ THE UNIT IS A YEAR because
        de-cumulation needs a quarter's priors in the same run; see `_skippable_years`.

        ⚠️ **`skip_existing=False` IS THE AUTHORITATIVE RUN, and it is not merely slower.**
        Skipping makes every run a subset run, which switches `sane` — the magnitude guard
        that compares a figure against its neighbouring quarters — to failing open. That
        guard is what caught ACB's Q1-2024 carrying Q1-2023's PBT. Use the default for
        "fill the gaps"; use `skip_existing=False` when the parser itself has changed.
        """
        if merge is None:
            merge = bool(periods)

        # ⚠️ INPUTS FIRST, OCR SECOND. `preflight` costs milliseconds and checks every
        # file this run needs — the template, the three charts of accounts, the PDF index
        # and the archive itself. Without it a missing chart of accounts was discovered
        # only after ~2.4 h, and not as itself: `schema_of` returned an empty list, so it
        # surfaced as "all 65 filings failed to reconcile". See `preflight`.
        template = self.preflight(exchange, symbol)

        docs = self.documents(exchange, symbol, allow_parent=allow_parent,
                              period_min=period_min)
        if periods:
            docs = [d for d in docs if d["period"] in set(periods)]

        if skip_existing:
            done = self._skippable_years(exchange, symbol, template, docs)
            if done:
                before = len(docs)
                docs = [d for d in docs if int(d["period"].split("-")[1]) not in done]
                # ⚠️ A PARTIAL RUN MUST MERGE. `_write` rebuilds the grid from what this
                # run holds, so without merging the skipped years — the COMPLETE ones —
                # would lose their `pdf` rows and be re-filled from CafeF's tabs. That is
                # the documented way a 6-minute run destroys a 4-hour one.
                merge = True
                self._log(
                    f"cafef financials: {symbol}: skipping {len(done)} complete "
                    f"year(s) {sorted(done)} — {before - len(docs)} of {before} quarters "
                    f"already read `pdf` in all 3 statements (skip_existing=True; pass "
                    f"False for an authoritative full re-parse)"
                )

        if not docs:
            # Nothing to do. Return early rather than rewriting the file with an empty
            # `data` — a merging write would be a no-op, but a non-merging one would not,
            # and "it happened to be harmless" is not a guarantee worth relying on.
            counts = {
                report: sum(
                    1
                    for row in self._existing(exchange, symbol, template, report).values()
                    if row.get("source") == "pdf"
                )
                for report in REPORTS
            }
            self._log(f"cafef financials: {symbol}: nothing to parse, {counts} on disk")
            return counts

        self._log(f"cafef financials: {symbol} ({template}): "
                  f"{len(docs)} consolidated quarters to parse")

        # report -> period -> {column: value}; and the column order as first seen
        data: Dict[str, Dict[str, dict]] = {r: {} for r in REPORTS}
        items: Dict[str, List[str]] = {r: [] for r in REPORTS}
        meta: Dict[str, Dict[str, dict]] = {r: {} for r in REPORTS}
        # ⚠️ **PER REPORT *AND* PER ENTITY.** `sane` judges a statement's magnitude against
        # the quarters already accepted, and a STANDALONE company is not the same company as
        # the CONSOLIDATED group — its profit and its balance sheet are legitimately smaller.
        # Pooling them makes the band meaningless in both directions. This mattered the moment
        # `allow_parent` existed, because a ticker can now file standalone for its early years
        # and consolidated later — which is exactly ACB (standalone 2008-09, consolidated 2010+).
        history: Dict[str, Dict[str, List[int]]] = {
            r: {"True": [], "False": []} for r in REPORTS}
        half_year: Dict[str, bool] = {}
        # The publish date belongs to the QUARTER'S DOCUMENT, not to one statement: one filing
        # produced all three, so they were all published on the same day. Keeping it per
        # statement lost it whenever a statement was rejected and its row came from CafeF's
        # tabs instead — VCB's Q1-2009 balance sheet had no date even though the very document
        # it failed to parse prints "Hà Nội, ngày 27 tháng 04 năm 2009" on page 4.
        published: Dict[str, str] = {}
        assurance: Dict[str, str] = {}
        # Share capital is also a per-DOCUMENT fact — read from the filing's capital note, not
        # from any statement — so it is kept per quarter, not per statement, exactly like the
        # publish date. {period: {shares_authorized, shares_issued, shares_outstanding}}.
        shares: Dict[str, Dict[str, Optional[int]]] = {}

        for di, d in enumerate(docs):
            period = d["period"]
            # both a semi-annual and an annual report print a CUMULATIVE income statement
            half_year[period] = (d["half_year"] == "True"
                                 or d.get("annual") == "True")
            path = os.path.join(PDFS_DIR, d["path"].replace("/", os.sep))
            if not os.path.exists(path):
                self._warn(f"  {period}: file missing on disk — {d['path']}")
                continue
            # Escalate OCR config per statement until each reconciles (see _parse_cascaded).
            # "Đầu kỳ" is 1 January, so every quarter of a year opens on the SAME figure: the
            # closing balance of the previous year's Q4, already accepted and verified earlier in
            # this run. Handed to `_cash_flow_identity`, it rescues a statement whose opening cell
            # OCR lost — and only when the identity then closes exactly.
            prev_key = f"Q4-{int(period.split('-')[1]) - 1}"
            prev_q4 = data[CASH_FLOW].get(prev_key, {})
            open_ref = next((prev_q4[c] for c in self.C_CASH_CLOSE if c in prev_q4), None)
            if open_ref is None and merge:
                # A SUBSET RUN HAS NO PREVIOUS Q4 IN MEMORY — read it back off disk, where the
                # full run that produced it already accepted and verified it. Without this a
                # re-parsed Q1 is judged more harshly than the same quarter in a full run, and
                # would fail for an opening balance the file on disk can supply.
                on_disk = self._existing(exchange, symbol, template, CASH_FLOW).get(prev_key, {})
                if on_disk.get("source") == "pdf":
                    for c in self.C_CASH_CLOSE:
                        if on_disk.get(c):
                            open_ref = int(on_disk[c])
                            break
            entity = d.get("consolidated", "True")
            accepted, facts = self._parse_cascaded(
                path, self._period_end(period), template,
                {r: history[r][entity] for r in REPORTS}, open_ref)

            # WHICH FILING EACH STATEMENT CAME FROM. Normally `d` for all three, but a
            # statement recovered below comes from a different document and its provenance
            # must say so — CLAUDE.md §6-2-terdecies is what happens when it does not.
            origin = {r: d for r in REPORTS}

            # ⚠️ A REFUSED QUARTER MAY HAVE A SECOND FILING NOBODY READ. `documents` returns
            # one document per period; until 2026-08-25 a quarter whose every layer refused it
            # was recorded `missing` while another CONSOLIDATED filing of the same quarter sat
            # on disk. Measured on BID: 4 of its 13 failed statements have exactly that.
            # Costs nothing on the success path — it runs only where something is still absent.
            if len(accepted) < len(REPORTS):
                for alt in self.alternates(exchange, symbol, d):
                    alt_path = os.path.join(PDFS_DIR, alt["path"].replace("/", os.sep))
                    if not os.path.exists(alt_path):
                        continue
                    self._log(f"  {period}: {len(REPORTS) - len(accepted)} statement(s) absent"
                              f" — retrying on the {alt['assurance']} filing of the same"
                              f" period ({alt['file']})")
                    more, alt_facts = self._parse_cascaded(
                        alt_path, self._period_end(period), template,
                        {r: history[r][entity] for r in REPORTS}, open_ref)
                    # ⚠️ THE CUMULATIVE SHAPE MUST MATCH FOR THE INCOME STATEMENT. `half_year`
                    # is a property of the DOCUMENT, and `_decumulate` subtracts Q1..Q(q-1)
                    # from a cumulative P&L. An annual chosen document and a quarterly
                    # alternate disagree about that, so taking the alternate's IS under the
                    # chosen document's flag would subtract the earlier quarters from a figure
                    # that never contained them. The balance sheet is a point in time and the
                    # cash flow is cumulative either way, so only the P&L is at risk.
                    alt_cumulative = (alt["half_year"] == "True"
                                      or alt.get("annual") == "True")
                    for report, got in more.items():
                        if report in accepted:
                            continue
                        if (report == INCOME_STATEMENT
                                and alt_cumulative != half_year[period]):
                            self._warn(f"    {period}: {report} from the alternate REFUSED —"
                                       f" cumulative shape differs from the chosen filing")
                            continue
                        accepted[report] = got
                        origin[report] = alt
                        self._log(f"    {period}: {report} recovered from"
                                  f" {alt['assurance']} {alt['file']}")
                    if not facts["publish_date"] and alt_facts["publish_date"]:
                        facts = alt_facts
                    if len(accepted) == len(REPORTS):
                        break

            # the document's own date, kept whether or not any of its statements reconcile
            assurance[period] = d["assurance"]
            published[period] = facts["publish_date"] or d.get("file_date", "")
            shares[period] = facts["shares"]

            notes = []
            for report in REPORTS:
                if report not in accepted:
                    notes.append(f"{report}=absent")          # -> CafeF-tab fallback below
                    continue
                row, st, cfg = accepted[report]

                for col in row:
                    if col not in items[report]:
                        items[report].append(col)

                data[report][period] = row
                # ⚠️ `origin[report]`, NOT `d` — a statement recovered from an alternate filing
                # must carry THAT filing's assurance and file name, or the row asserts a
                # document it did not come from. The entity is identical by construction
                # (`alternates` fixes it), so only these two can differ.
                src = origin[report]
                meta[report][period] = {
                    "assurance": src["assurance"],
                    # which ENTITY this statement describes; see DATA_COLS."consolidated"
                    "consolidated": src.get("consolidated", ""),
                    "unit": st.unit,
                    "n_columns": st.n_columns, "document": src["file"],
                    # read from THIS filing: a company chooses the method and may switch it
                    "cash_flow_method": st.cash_flow_method or "",
                    "ocr_config": cfg,          # which cascade config produced this statement
                }
                # ⚠️ THE INDEX SAYS THE FILING IS CUMULATIVE; THE STATEMENT SAYS WHETHER IT
                # ACTUALLY IS. A semi-annual or annual report is de-cumulated because it is
                # assumed to print only the year-to-date column — but VCB's Q2-2014 prints
                # "Quý II" BESIDE "Lũy kế từ đầu năm", so column 0 is already the standalone
                # quarter and subtracting Q1 removes it twice (PBT −154,988 for a bank that
                # earned 1,345,661). The document itself is the authority, not the index.
                if report == INCOME_STATEMENT and st.quarter_column:
                    half_year[period] = False
                # ⚠️ A THIN STATEMENT IS WRITTEN BUT NEVER BECOMES A REFERENCE — see
                # MIN_ITEMS_FOR_HISTORY. Accepting it and trusting it are separate decisions.
                v = self._probe(report, row, st)
                if v is not None and len(row) >= self.MIN_ITEMS_FOR_HISTORY:
                    history[report][entity].append(v)
                elif v is not None:
                    self._warn(
                        f"    {period} {report}: only {len(row)} mapped items — accepted, "
                        f"but withheld from the magnitude history (needs "
                        f"{self.MIN_ITEMS_FOR_HISTORY})")
                notes.append(f"{report}={len(row)} items [{cfg}]")
            self._log(f"  {period:<8} {'; '.join(notes)}")

            # Snapshot to disk after each quarter so progress is visible and survives an
            # interrupted run — the atomic temp+rename write makes each snapshot safe to read.
            # This is a PROGRESS snapshot only: income statements are still cumulative here
            # (they are de-cumulated below), and the CafeF-tab fallback has not run yet, so
            # quarters no PDF could produce are absent until the final write. `attempted` counts
            # every document seen so far so a failed quarter shows as `missing`, not vanished.
            #
            # ⚠️ NOT IN MERGE MODE. A snapshot is a PROGRESS VIEW — income statements in it are
            # still cumulative — and a merging write puts it into the authoritative file, where
            # `_decumulate` can no longer take it back: it drops the cumulative row from `data`,
            # the final write then sees the quarter as "not produced" and leaves the file alone,
            # and what it leaves is the snapshot. ACB's Q4-2010 income statement came out holding
            # the FULL-YEAR PBT 3,102,248 (Q1..Q3 1,422,302 + the true Q4 1,679,946) — the exact
            # cumulative-in-a-quarterly-row error the parser exists to prevent. A subset run is
            # short and needs no progress view; it writes once, at the end, after de-cumulation.
            if not merge:
                attempted_so_far = [(int(dd["year"]), int(dd["quarter"]))
                                    for dd in docs[:di + 1]]
                attempted_so_far += [(int(p.split("-")[1]), int(p[1]))
                                     for r in REPORTS for p in data[r]]
                self._write(exchange, symbol, data, items, meta, attempted_so_far, template,
                            published, assurance, shares, merge=merge)

        self._decumulate(data, half_year)

        # Whatever the filings could not give us, take from CafeF's own tabs. Keyed by item
        # CODE, so it lands on the canonical column exactly — for a quarter whose scan is too
        # degraded to read, this is the better source, not the lesser one.
        if use_api:
            api = self.from_api(symbol, template)
            for report in REPORTS:
                filled = 0
                for period, row in api[report].items():
                    if period in data[report] or not row:
                        continue
                    data[report][period] = row
                    meta[report][period] = {"source": "cafef"}
                    for col in row:
                        if col not in items[report]:
                            items[report].append(col)
                    filled += 1
                if filled:
                    self._log(f"cafef financials: {symbol} {report}: "
                              f"{filled} quarters filled from the CafeF tabs")

        # every quarter we ATTEMPTED, so one we failed to read is reported as missing rather
        # than vanishing from the grid
        attempted = [(int(d["year"]), int(d["quarter"])) for d in docs]
        attempted += [(int(p.split("-")[1]), int(p[1]))
                      for r in REPORTS for p in data[r]]
        return self._write(exchange, symbol, data, items, meta, attempted, template,
                           published, assurance, shares, merge=merge)

    # ──────────────────────────────────────────────────────────────────────
    # The fallback: CafeF's own tabs
    # ──────────────────────────────────────────────────────────────────────

    # CafeF's "not reported" sentinel is -1, not 0 and not null. Written through it becomes a
    # literal -1 dong in a column of billions, and no reconciliation catches it because an
    # unreported line takes part in no subtotal.
    NOT_REPORTED = "-1"

    def from_api(self, symbol: str, template: str) -> Dict[str, Dict[str, dict]]:
        """{report: {period: {canonical column: value}}} from the three tabs of
        cafef.vn/du-lieu/<exchange>/<sym>-tai-chinh.chn.

        ⚠️ **FORBIDDEN AS A SOURCE SINCE 2026-08-24 — `use_api` DEFAULTS TO FALSE AND MUST STAY
        THERE.** CLAUDE.md §5 rule 24: a financial statement value comes from the filing PDF and
        from nothing else. **A quarter no readable PDF can produce is `missing`, and `missing`
        is the correct answer.** The method is kept, not deleted, because the reconciliation
        cross-checks below still read these tabs to CHALLENGE a parsed figure — comparing against
        a transcription is legitimate; sourcing from one is not.

        ⚠️ *This docstring used to open: "This is not a lesser source — for the quarters OCR
        cannot read it is a BETTER one. The tabs are keyed by the same item CODES the schema was
        built from, so a value lands on its canonical column exactly: no OCR, no fuzzy matching,
        no chance of a line being mistaken for its neighbour."* That argument is **right about
        the mechanism** and is kept for exactly that reason — it is overruled on a different
        ground: **a transcription is somebody else's parse of the document**, and once it is in
        the table nothing downstream can tell it from the filing. The evidence for the overrule
        is already in this file: CafeF's Q4 figures disagree with its own annual tab for VCB
        2011-13/2015/2020, eight of its values are confirmed WRONG against the filings, and
        `NOT_REPORTED` below is a literal `-1` that lands as −1 dong in a column of billions and
        takes part in no subtotal, so no reconciliation catches it. `ISSUES.md` `FIN-1`.
        """
        from web_scraper.cafef_schema import TABS, _get

        # Canonical column for each of CafeF's codes, from the chart of accounts.
        # ⚠️ REQUIRED — this used to `continue` past a missing schema file, and the loop
        # below then skips that report entirely (`if not codes: continue`). The fallback
        # is what fills a quarter whose scan is unreadable, so losing it silently turns a
        # recoverable quarter into a permanent `source='missing'` row. `require_file`
        # rather than a bare open, so the message names the fix.
        by_code: Dict[str, Dict[str, str]] = {}
        for report in REPORTS:
            path = require_file(
                os.path.join(SCHEMA_DIR, f"{template}_{report}.csv"),
                what=f"the {template}/{report} chart of accounts",
                why=(
                    "it maps CafeF's item codes onto canonical columns; without it the "
                    "CafeF-tab fallback silently skips this report and unreadable "
                    "quarters become permanent gaps"
                ),
                fix=f"cafef_schema.save({template!r}, SCHEMA_DIR) — or restore it from git",
            )
            with open(path, encoding="utf-8-sig") as f:
                by_code[report] = {r["cafef_code"]: r["column"]
                                   for r in csv.DictReader(f) if r["cafef_code"]}

        out: Dict[str, Dict[str, dict]] = {r: {} for r in REPORTS}
        for report, (url, sections, shape) in TABS.items():
            codes = by_code.get(report, {})
            if not codes:
                continue
            for sec in sections:
                try:
                    head = _get(f"{url}?symbol={symbol}&pageIndex=1&pageSize=1"
                                f"&reportType={sec}&TypeTime=QUY").get("value") or {}
                    n = head.get("count") or 0
                    if not n:
                        continue
                    v = _get(f"{url}?symbol={symbol}&pageIndex=1&pageSize={n}"
                             f"&reportType={sec}&TypeTime=QUY").get("value") or {}
                except Exception as e:
                    # ⚠️ A DEAD SECTION VOIDS THE WHOLE REPORT, it does not merely skip itself.
                    # The sections are HALVES OF ONE STATEMENT — "NV" is every liability and
                    # equity line of the balance sheet, "HDTC" every financing line of the cash
                    # flow — so filling a quarter from the survivors writes a row that reads as
                    # complete and silently has no `tong_no_phai_tra`. Continuing here is what
                    # put 54-of-107-column balance sheets into ACB's 2008-09 quarters on the
                    # 2026-07-29 run. A gap is recoverable; a plausible-looking half-row is not.
                    self._warn(f"cafef financials: {symbol} {report}/{sec} tab failed: {e} — "
                               f"dropping the whole {report} fallback rather than writing "
                               f"rows missing this section")
                    out[report] = {}
                    break
                blocks = (v["data"][0]["data"] if shape == "nested" and v.get("data")
                          else v.get("data") or [])
                for blk in blocks:
                    period = blk.get("time")
                    if not period:
                        continue
                    row = out[report].setdefault(period, {})
                    for cell in blk.get("data", []):
                        col = codes.get(str(cell.get("code")))
                        val = cell.get("value")
                        if not col or val in (None, "") or str(val) == self.NOT_REPORTED:
                            continue
                        try:
                            row[col] = int(val)
                        except (TypeError, ValueError):
                            continue
        return out

    def _decumulate(self, data: Dict[str, Dict[str, dict]],
                    half_year: Dict[str, bool]) -> None:
        """Turn a year-to-date income statement into the standalone quarter.

        A semi-annual filing prints only the cumulative Jan-Jun column, so its figures are
        Jan-Jun, not Apr-Jun. Nothing else catches this: the cumulative numbers reconcile
        perfectly against each other and 2x is well inside any magnitude band. The quarter is
        recovered as YTD minus the quarters already accepted for that year; a line whose
        prior quarters are not all available is DROPPED rather than guessed.

        Cash flow is NOT adjusted — it is cumulative by nature.
        """
        rows = data[INCOME_STATEMENT]
        for period in sorted(rows):
            q, year = int(period[1]), int(period.split("-")[1])
            # a half-year filing prints Jan-Jun; an ANNUAL one prints the whole year. Both are
            # cumulative, and the quarter is what is left when the earlier quarters come off.
            if q == 1 or not half_year.get(period):
                continue
            prior = [rows.get(f"Q{i}-{year}") for i in range(1, q)]
            if any(p is None for p in prior):
                self._warn(f"  {period}: cumulative, but Q1..Q{q-1} not all parsed — "
                           f"dropping (would otherwise be a 6-month figure in a "
                           f"quarterly row)")
                del rows[period]
                continue
            out = {}
            for col, ytd in rows[period].items():
                vals = [p.get(col) for p in prior]
                if any(v is None for v in vals):
                    continue
                out[col] = ytd - sum(vals)
            rows[period] = out
            self._log(f"  {period:<8} income_statement de-cumulated "
                      f"(YTD - Q1..Q{q-1}), {len(out)} items")

    def _write(self, exchange: str, symbol: str,
               data: Dict[str, Dict[str, dict]], items: Dict[str, List[str]],
               meta: Dict[str, Dict[str, dict]],
               attempted: List[Tuple[int, int]], template: str,
               published: Dict[str, str], assurance: Dict[str, str],
               shares: Dict[str, Dict[str, Optional[int]]],
               merge: bool = False) -> Dict[str, int]:
        """One CSV per report, under the ticker's own TEMPLATE — so every file in a directory
        has the same columns and they mean the same thing.

        The quarter grid spans every quarter ATTEMPTED, not merely the ones that parsed. A
        grid built from the parsed periods hides its own failures: VIC's balance sheet read 6
        quarters out of 71 and reported "6/10", because the 61 it could not read fell outside
        the min..max of the 6 it could. A quarter we failed on is a blank `source=missing` row
        — never skipped, never zero-filled.

        Columns are the CANONICAL ones, in the chart of accounts' own order, so the same column
        means the same line in every quarter and across every ticker on this template."""
        counts = {}
        for report in REPORTS:
            rows = data[report]
            if not rows and not attempted:
                counts[report] = 0
                continue
            ys = sorted(set(attempted) | {(int(p.split("-")[1]), int(p[1])) for p in rows})
            (y0, q0), (y1, q1) = ys[0], ys[-1]

            out = []
            y, q = y0, q0
            while (y, q) <= (y1, q1):
                period = f"Q{q}-{y}"
                m = meta[report].get(period, {})
                # ⚠️ **A PARSE THAT DID NOT SURVIVE LEAVES ITS METADATA BEHIND.**
                # `_decumulate` drops a cumulative Q4 income statement from `rows` when
                # this run lacks its Q1..Q3 priors, but it does not clear `meta` — so the
                # row reports `source='missing'` while still carrying the document, the OCR
                # layer and the ENTITY of a parse that was thrown away. Measured 2026-08-24:
                # ACB Q4-2009 IS came out `missing` with `consolidated='false'`, and VCB
                # Q4-2008 IS `missing` with `consolidated='true'` — a fact asserted about a
                # quarter nothing was written for, which is precisely what the blank exists
                # to avoid. Provenance is taken ONLY from a period that produced a row.
                # ⚠️ `publish_date` and `assurance` are deliberately NOT in this set: they
                # are facts about the DOCUMENT and are kept whether or not any statement of
                # it reconciled (see where they are assigned, above).
                produced = period in rows
                prov = m if produced else {}
                row = {"symbol": symbol, "exchange": exchange, "template": template,
                       "period": period, "year": y, "quarter": q,
                       # the cascade layer that produced this statement; empty unless
                       # `source == "pdf"`, since nothing else came off a filing
                       "method": prov.get("ocr_config", ""),
                       # `pdf` = read off the filing; `cafef` = taken from CafeF's tabs
                       # because the filing could not be read
                       "source": (m.get("source", "pdf") if produced else "missing"),
                       # From the QUARTER, not the statement: one filing produced all three, so
                       # they share a publish date — even a row that had to come from CafeF's
                       # tabs because its statement would not parse. Join on THIS, not the
                       # period end, or the fundamentals leak months of look-ahead: VCB's
                       # Q4-2024 figures were not public until 2025-03-28.
                       "publish_date": published.get(period, ""),
                       "assurance": m.get("assurance") or assurance.get(period, ""),
                       # blank for a `missing` row: no filing was read, so no entity was
                       # chosen. Never defaulted to "True" — that would assert a fact about
                       # a quarter nothing was parsed for.
                       "consolidated": prov.get("consolidated", ""),
                       "cash_flow_method": prov.get("cash_flow_method", ""),
                       "unit": prov.get("unit", ""),
                       "n_columns": prov.get("n_columns", ""),
                       "document": prov.get("document", ""),
                       # from the QUARTER's filing, shared across its three statements — blank
                       # for a cafef/missing quarter (the tabs have no share field)
                       "shares_authorized": _blank(shares.get(period, {}).get(
                           "shares_authorized")),
                       "shares_issued": _blank(shares.get(period, {}).get("shares_issued")),
                       "shares_outstanding": _blank(shares.get(period, {}).get(
                           "shares_outstanding"))}
                row.update(rows.get(period, {}))
                out.append(row)
                y, q = (y + 1, 1) if q == 4 else (y, q + 1)

            path = statement_path(template, report, exchange, symbol)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            # every column the chart of accounts defines, in ITS order — a line the filings
            # never reported is an empty column, not an absent one, so the shape of the table
            # is the same for every ticker on this template
            schema_cols = [c for c, _ in self.schema_of(template, report)]
            extra = [c for c in items[report] if c not in schema_cols]

            prev_head: List[str] = []
            if merge:
                # UPSERT: only the quarters this run PRODUCED are rewritten. A quarter it did not
                # parse keeps whatever the file already holds — which is the whole point, since
                # the rest of the grid is the output of a run that may have cost hours. A quarter
                # it attempted and FAILED is likewise left alone rather than being overwritten
                # with a blank `missing` row: failing to re-read a statement is not evidence that
                # the statement is unreadable, and the row on disk may be a good one.
                path_rows = self._existing(exchange, symbol, template, report)
                if path_rows:
                    with open(path, encoding="utf-8-sig") as f:
                        prev_head = csv.DictReader(f).fieldnames or []
                produced = set(rows)
                merged = dict(path_rows)
                for row in out:
                    if row["period"] in produced or row["period"] not in merged:
                        merged[row["period"]] = row
                out = sorted(merged.values(),
                             key=lambda r: (int(r["year"]), int(r["quarter"])))
                extra += [c for c in prev_head
                          if c not in schema_cols and c not in extra and c not in DATA_COLS]
            head = DATA_COLS + schema_cols + extra
            tmp = path + ".tmp"
            with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=head, extrasaction="ignore")
                w.writeheader()
                w.writerows(out)
            os.replace(tmp, path)

            n = sum(1 for r in out if r["source"] == "pdf")
            counts[report] = n
            self._log(f"cafef financials: {symbol} {report}: {len(out)} quarters "
                      f"({out[0]['period']}..{out[-1]['period']}), {n} parsed, "
                      f"{len(out) - n} missing, {len(items[report])} line items -> {path}")
        return counts
