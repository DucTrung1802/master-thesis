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
    crop_pad: Optional[float] = None

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
             "publish_date", "assurance", "cash_flow_method", "unit", "n_columns",
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

    # The same subtotals, as CANONICAL columns (schema/<template>_<report>.csv). Looked up here
    # a line cannot be lost to OCR damage — which is what most rejections were.
    C_ASSETS = ("tong_tai_san", "tong_cong_tai_san")
    C_RESOURCES = ("tong_no_phai_tra_va_von_chu_so_huu", "tong_cong_nguon_von")
    C_LIABILITIES = ("tong_no_phai_tra", "no_phai_tra")
    C_EQUITY = ("viii_von_chu_so_huu", "von_chu_so_huu", "d_von_chu_so_huu")
    C_PBT = ("xi_tong_loi_nhuan_truoc_thue", "tong_loi_nhuan_truoc_thue",
             "tong_loi_nhuan_ke_toan_truoc_thue")
    C_NET_CF = ("hdtc_iv_luu_chuyen_tien_thuan_trong_ky", "luu_chuyen_tien_thuan_trong_ky")
    C_CASH_CLOSE = ("hdtc_vii_tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_cuoi_ky",
                    "tien_va_tuong_duong_tien_cuoi_ky")

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
    ]

    def __init__(self, logger=None):
        self._logger = logger
        self._parser = PdfParser(logger=logger)          # env-default engine (kept for callers)
        self._parsers: Dict[str, PdfParser] = {self._parser.engine: self._parser}
        self._schema_cache: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}

    def _parser_for(self, engine: str) -> PdfParser:
        """A reusable parser per engine — built once, its DPI re-pointed per config, so the onnx
        models and Tesseract setup are not reloaded on every retry."""
        if engine not in self._parsers:
            self._parsers[engine] = PdfParser(logger=self._logger, engine=engine)
        return self._parsers[engine]

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
        facts = {"publish_date": "", "shares": {"shares_authorized": None,
                                                "shares_issued": None,
                                                "shares_outstanding": None}}
        # OCR is the expensive step, so the parse of each (engine, dpi) is cached within this
        # filing — the relaxed layers reuse a strict layer's already-OCR'd statements and only
        # re-map them, no second OCR pass.
        parsed: Dict[Tuple[str, int], Dict[str, Statement]] = {}
        for layer in self.LAYERS:
            if len(accepted) == len(REPORTS):
                break
            parser = self._parser_for(layer.engine)
            if not parser.ocr_ready and layer.engine != "onnx":
                continue                                  # engine unavailable on this machine
            # The crop padding is part of the KEY, not just a setting: two layers that share an
            # engine and DPI but crop differently produce different text, and keying on
            # (engine, dpi) alone would hand the wider-crop layer the narrow crop's cached parse
            # — the one that already failed.
            key = (layer.engine, layer.dpi, layer.crop_pad, layer.join_digits,
                   layer.title_over_form, layer.loose_form_code)
            if key not in parsed:
                parser.set_dpi(layer.dpi)
                parser.set_crop_pad(layer.crop_pad)
                parser.set_join_split(layer.join_digits)
                parser.set_title_over_form(layer.title_over_form)
                parser.set_loose_form_code(layer.loose_form_code)
                try:
                    parsed[key] = parser.parse(path, period_end)
                except Exception as e:
                    self._warn(f"    {layer.name}: parse failed — {type(e).__name__}: {e}")
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
                    continue
                row = self.map_to_schema(st, template, relax_totals=layer.relax_totals,
                                         relax_split_tail=layer.relax_split_tail)
                if (self.reconcile(st, row, verify_cash=layer.relax_totals,
                                   open_ref=open_ref,
                                   relax_components=layer.relax_components) is None
                        and self.sane(st, history[report], row) is None):
                    accepted[report] = (row, st, layer.name)
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

        gics = {}
        path = os.path.join(CAFEF_RAW_DATA_DIR, "..", "simplize", "industry.csv")
        if os.path.exists(path):
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
                  use_api: bool = True) -> Dict[str, Dict[str, int]]:
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
        for exchange, symbol in universe:
            key = f"{exchange}:{symbol}"
            try:
                results[key] = builder.build(exchange, symbol, use_api=use_api)
                if logger:
                    logger.log_info(f"cafef financials DONE {key}: {results[key]}")
            except Exception as e:
                results[key] = {}
                if logger:
                    logger.log_error(f"cafef financials FAILED {key}: {e}")
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

    def documents(self, exchange: str, symbol: str) -> List[dict]:
        """The one consolidated filing to read per quarter, oldest first.

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
            rows = [r for r in csv.DictReader(f) if r["consolidated"] == "True"]

        best: Dict[str, dict] = {}
        annual: Dict[str, dict] = {}
        for r in rows:
            q = str(r["quarter"])
            if not q.isdigit():
                continue
            q = int(q)
            if q == 5:                        # the audited annual -> stands in for Q4
                p = f"Q4-{r['year']}"
                rank = self.ASSURANCE_RANK.get(r["assurance"], 9)
                if p not in annual or rank < self.ASSURANCE_RANK.get(
                        annual[p]["assurance"], 9):
                    annual[p] = {**r, "period": p, "quarter": "4", "annual": "True"}
                continue
            if q not in (1, 2, 3, 4):
                continue
            p = r["period"]
            rank = self.ASSURANCE_RANK.get(r["assurance"], 9)
            if p not in best or rank < self.ASSURANCE_RANK.get(best[p]["assurance"], 9):
                best[p] = {**r, "annual": "False"}

        best.update(annual)                   # the annual report wins Q4
        return sorted(best.values(),
                      key=lambda r: (int(r["year"]), int(r["quarter"])))

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
        """[(canonical column, its account name)] in statement order, from schema/."""
        key = (template, report)
        if key in self._schema_cache:
            return self._schema_cache[key]

        path = os.path.join(SCHEMA_DIR, f"{template}_{report}.csv")
        items: List[Tuple[str, str]] = []
        if os.path.exists(path):
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
    }

    @classmethod
    def _expand(cls, s: str) -> str:
        for short, full in cls.ABBREV.items():
            if short in s:
                s = s.replace(short, full)
        return s

    def _label_score(self, account: str, key: str, relax: bool = False) -> float:
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

        account, key = self._expand(account), self._expand(key)
        r = SequenceMatcher(None, account, key).ratio()
        if (len(account) >= self.MIN_CONTAINS
                and min(len(account), len(key)) >= self.MIN_CONTAINS_FRAGMENT
                and (account in key or key in account)):
            r = max(r, 0.95)
        if relax and account.startswith(self.CASH_TAIL) and key.startswith(self.CASH_TAIL):
            r = max(r, 0.95)
        return r

    def map_to_schema(self, st: Statement, template: str,
                     relax_totals: bool = False,
                     relax_split_tail: bool = False) -> Dict[str, int]:
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
            key = self._split_merged(row.key, row.label).replace("_", "")
            if key:
                rows.append((i, val, key))

        accounts = [a.replace("_", "") for _, a in schema]
        out: Dict[str, int] = {}
        src: Dict[str, int] = {}                # column -> the parsed row that filled it
        for j, ri in self._align([k for _, _, k in rows], accounts, relax_totals).items():
            self._claim(out, src, schema[j][0], rows[ri][0], rows[ri][1])

        self._anchor(out, schema, st, relax_totals, src)
        if relax_totals:
            self._recover_totals(out, st, src, relax_split_tail)
        return out

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

    # How far to re-slug a label when hunting for the seam. `PdfParser.slug` caps a row key at
    # 60 characters, which is ample for a real line and far too short for a merged one — and a
    # merged row is long BY DEFINITION, so the marker that reveals the seam is exactly what the
    # cap throws away. ACB's Q1-2022 balance sheet reads "…B NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU / Các
    # khoản nợ Chính phủ và Ngân hàng Nhà nước / II Tiền gửi và vay các TCTD khác V.8" as one row
    # carrying 44,323,457: the key stops at "…cac_khoan_no_chinh_phu", so the "II" that owns the
    # figure is gone and nothing can place it.
    SEAM_SLUG_LEN = 400

    def _split_merged(self, key: str, label: str = "") -> str:
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
        for cand in candidates:
            m = self.MERGED_SEAM_RE.match(cand)
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
               relax: bool) -> Dict[int, int]:
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
                s = self._label_score(accounts[j - 1], key, relax)
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
    ANCHORS = ("tong_tai_san", "tong_cong_tai_san", "tong_no_phai_tra",
               "tong_no_phai_tra_va_von_chu_so_huu", "tong_cong_nguon_von",
               "viii_von_chu_so_huu", "xi_tong_loi_nhuan_truoc_thue",
               "hdtc_iv_luu_chuyen_tien_thuan_trong_ky",
               "hdtc_vii_tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_cuoi_ky")
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
                src: Optional[Dict[str, int]] = None) -> None:
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
                k = row.key.replace("_", "")
                r = self._label_score(a, k, relax)
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
        taken_col, taken_row = set(), set()
        for r, ln, col, ri, val in cands:
            if col in taken_col or ri in taken_row:
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
                  relax_components: bool = False) -> Optional[str]:
        """None if the statement balances against its OWN printed subtotals, else why not.

        The subtotals are taken from the CANONICAL columns when the rows have been mapped —
        `mapped` — and only fall back to searching the OCR text when they have not. Searching
        the text is what most rejections actually were: the row was parsed, its figure correct,
        and the lookup simply could not recognise the name OCR had mangled.
        """
        if len(st.rows) < self.MIN_ROWS:
            return f"only {len(st.rows)} rows parsed"

        def get(canonical: Tuple[str, ...], *text: str) -> Optional[int]:
            if mapped:
                for c in canonical:
                    if c in mapped:
                        return mapped[c]
            return st.find(*text)

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
            close = get(self.C_CASH_CLOSE, *self.CASH_CLOSE)
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
                bad = self._cash_flow_identity(mapped or {}, open_ref)
                if bad:
                    return bad
        return None

    # The statement's own arithmetic: closing = opening + what moved + the FX adjustment. The
    # movement is IV when it was mapped, else the section subtotals it is the sum of.
    C_CASH_OPEN = ("hdtc_v_tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_dau_ky",)
    C_CASH_FX = ("hdtc_vi_dieu_chinh_anh_huong_cua_thay_doi_ty_gia",)
    C_FLOW_SECTIONS = ("hdkd_i_luu_chuyen_tien_thuan_tu_hoat_dong_kinh_doanh",
                       "hddt_ii_luu_chuyen_tien_thuan_tu_hd_dau_tu",
                       "hdtc_iii_luu_chuyen_tien_thuan_tu_hd_tai_chinh")

    def _cash_flow_identity(self, mapped: Dict[str, int],
                            open_ref: Optional[int] = None) -> Optional[str]:
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
        return st.find(*text)

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

    def build(self, exchange: str, symbol: str,
              periods: Optional[List[str]] = None,
              use_api: bool = True,
              merge: Optional[bool] = None) -> Dict[str, int]:
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
        """
        if merge is None:
            merge = bool(periods)
        docs = self.documents(exchange, symbol)
        if periods:
            docs = [d for d in docs if d["period"] in set(periods)]
        # the chart of accounts the parsed rows are mapped onto — fingerprinted, not guessed
        template = self.template_of(symbol) or "unknown"
        self._log(f"cafef financials: {symbol} ({template}): "
                  f"{len(docs)} consolidated quarters to parse")

        # report -> period -> {column: value}; and the column order as first seen
        data: Dict[str, Dict[str, dict]] = {r: {} for r in REPORTS}
        items: Dict[str, List[str]] = {r: [] for r in REPORTS}
        meta: Dict[str, Dict[str, dict]] = {r: {} for r in REPORTS}
        history: Dict[str, List[int]] = {r: [] for r in REPORTS}
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
            accepted, facts = self._parse_cascaded(
                path, self._period_end(period), template, history, open_ref)

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
                meta[report][period] = {
                    "assurance": d["assurance"], "unit": st.unit,
                    "n_columns": st.n_columns, "document": d["file"],
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
                v = self._probe(report, row, st)
                if v is not None:
                    history[report].append(v)
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

        This is not a lesser source — for the quarters OCR cannot read it is a BETTER one. The
        tabs are keyed by the same item CODES the schema was built from, so a value lands on
        its canonical column exactly: no OCR, no fuzzy matching, no chance of a line being
        mistaken for its neighbour. What it is not is the filing itself — CafeF transcribes,
        and it has gaps (it omits Q2-2024 market-wide) and it rounds — so the PDF is still
        read first and this fills only what the PDF could not.
        """
        from web_scraper.cafef_schema import TABS, _get

        # canonical column for each of CafeF's codes, from the schema we already built
        by_code: Dict[str, Dict[str, str]] = {}
        for report in REPORTS:
            path = os.path.join(SCHEMA_DIR, f"{template}_{report}.csv")
            if not os.path.exists(path):
                continue
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
                row = {"symbol": symbol, "exchange": exchange, "template": template,
                       "period": period, "year": y, "quarter": q,
                       # the cascade layer that produced this statement; empty unless
                       # `source == "pdf"`, since nothing else came off a filing
                       "method": m.get("ocr_config", ""),
                       # `pdf` = read off the filing; `cafef` = taken from CafeF's tabs
                       # because the filing could not be read
                       "source": (m.get("source", "pdf") if period in rows else "missing"),
                       # From the QUARTER, not the statement: one filing produced all three, so
                       # they share a publish date — even a row that had to come from CafeF's
                       # tabs because its statement would not parse. Join on THIS, not the
                       # period end, or the fundamentals leak months of look-ahead: VCB's
                       # Q4-2024 figures were not public until 2025-03-28.
                       "publish_date": published.get(period, ""),
                       "assurance": m.get("assurance") or assurance.get(period, ""),
                       "cash_flow_method": m.get("cash_flow_method", ""),
                       "unit": m.get("unit", ""),
                       "n_columns": m.get("n_columns", ""),
                       "document": m.get("document", ""),
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
