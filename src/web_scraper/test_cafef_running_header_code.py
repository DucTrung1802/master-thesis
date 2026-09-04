"""`RHF-1` — the notes RUNNING HEADER carries a second form code onto a statement page.

`_page_kind` opens with a TABLE-OF-CONTENTS guard: a filing's contents page lists every
statement WITH its form code, so a page carrying two or more distinct codes is talking ABOUT
the statements rather than being one. That guard counted every code on the page.

⚠️ MEASURED ON FPT Q3-2011 page 5, which IS the cash flow: its header prints

    THUYẾT MINH BÁO CÁO TÀI CHÍNH HỢP NHẤT     MẪU SỐ B 09-DN/HN     <- the notes RUNNING HEADER
    BÁO CÁO LƯU CHUYỂN TIỀN TỆ HỢP NHẤT        MẪU SỐ B 03-DN/HN     <- the statement's own

Two codes, so `_page_kind` returned None, `_fill_continuations` handed the page to the
statement running above it, and the cash flow was reported `no such statement on any page of
this filing` on all 76 layers while every figure on the page was readable. `title_over_form`
could not help: the guard returns before the form-code branch is reached.

⚠️ A CONTENTS PAGE IS STILL GUARDED, BECAUSE IT LISTS SEVERAL STATEMENTS — that is what a
contents page is. Measured over the 7,389 text-layer pages of the eight parsed tickers: **8
pages carry more than one distinct code**, 7 of them name two or three STATEMENTS (all still
refused) and exactly ONE names a single statement — a `cong_ty_me` FY-2024 balance sheet
carrying {B02, B05}, i.e. this same defect on another ticker.
"""
import pytest

from web_scraper.cafef_pdf_parser import (BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT,
                                          PdfParser)


def _parser(**flags):
    p = PdfParser.__new__(PdfParser)
    p.loose_form_code = False
    p.title_over_form = False
    p.column_header_blind = False
    for k, v in flags.items():
        setattr(p, k, v)
    return p


# FPT Q3-2011 page 5's header, as OCR returns it.
CASH_FLOW_UNDER_NOTES_HEADER = (
    "CÔNG TY CÓ PHẦN FPT\n"
    "Tòa nhà FPT Cầu Giấy, Phố Duy Tân\n"
    "Báo cáo tài chính hợp nhất\n"
    "Phường Dịch Vọng Hậu, Quận Cầu Giấy\n"
    "Cho kỳ hoạt động từ ngày 01 tháng 01 năm 2011\n"
    "Hà Nội, CHXHCN Việt Nam\n"
    "đến ngày 30 tháng 09 năm 2011\n"
    "THUYẾT MINH BÁO CÁO TÀI CHÍNH HỢP NHÁT\n"
    "MÀU SỐ B 09-DN/HN\n"
    "Các thuyết minh này là một bộ phận hợp thành và cần được đọc đồng thời với báo cáo\n"
    "BÁO CÁO LƯU CHUYẾN TIỀN TỆ HỢP NHÁT\n"
    "Cho kỳ hoạt động từ ngày 01 tháng 01 năm 2011 đến ngày 30 tháng 09 năm 2011\n"
    "MĂU SỐ B 03-DN/HN\n"
    "Đơn vị: VND\n")

# A real contents page: three statements, each with its code, and every title verbatim.
CONTENTS = (
    "CÔNG TY CỔ PHẦN X\nNỘI DUNG\n"
    "Bảng cân đối kế toán hợp nhất (Mẫu B01-DN/HN)\n"
    "Báo cáo kết quả hoạt động kinh doanh hợp nhất (Mẫu B02-DN/HN)\n"
    "Báo cáo lưu chuyển tiền tệ hợp nhất (Mẫu B03-DN/HN)\n"
    "Thuyết minh báo cáo tài chính hợp nhất (Mẫu B09-DN/HN)\n")


def test_the_statement_wins_its_own_page_back():
    kind, from_form = _parser()._page_kind(CASH_FLOW_UNDER_NOTES_HEADER)
    assert (kind, from_form) == (CASH_FLOW, True)


def test_a_contents_page_is_still_refused():
    """⚠️ THE GUARD THIS RULE NARROWS, AND IT MUST STILL FIRE. Found via experiment_8 on ACB's
    FY-2013 filing: a contents page was classified as the first statement it named, anchored
    the run pages early, and fed its own page numbers into the period-column clustering."""
    assert _parser()._page_kind(CONTENTS) == (None, False)


def test_two_codes_and_NO_statement_among_them_is_still_refused():
    """Several codes, none of them a statement — nothing has been learned about the page."""
    text = "THUYẾT MINH ... MẪU SỐ B 09-DN/HN\nvà MẪU SỐ B 05/TCTD\n"
    assert _parser()._page_kind(text) == (None, False)


def test_the_TITLE_must_agree_before_the_lone_statement_code_is_trusted():
    """⚠️ WHAT KEEPS A STRAY CODE FROM CLAIMING A PAGE. The one statement code is trusted only
    when the header also carries that statement's own title VERBATIM — `title_over_form`'s own
    standard (score 1.0), applied here to ADMIT a page rather than to re-label one."""
    no_title = (
        "THUYẾT MINH BÁO CÁO TÀI CHÍNH HỢP NHẤT\nMẪU SỐ B 09-DN/HN\n"
        "Số dư đầu kỳ theo MẪU SỐ B 03-DN/HN\n")
    assert _parser()._page_kind(no_title) == (None, False)


def test_a_page_with_ONE_code_is_unaffected():
    """The ordinary case, and the vast majority: nothing about it moves."""
    p = _parser()
    assert p._page_kind("BẢNG CÂN ĐỐI KẾ TOÁN\nMẪU SỐ B 01-DN/HN\n") == (BALANCE_SHEET, True)
    assert p._page_kind("BÁO CÁO KẾT QUẢ HOẠT ĐỘNG KINH DOANH\nMẫu B02-DN/HN\n") \
        == (INCOME_STATEMENT, True)


def test_the_statement_code_is_the_one_READ_not_the_first_one_printed():
    """⚠️ THE HALF THAT IS EASY TO MISS. The notes code comes FIRST in the running header, so
    taking `form_re.search(text)`'s first match would have named the page `notes` even after
    the guard let it through — a different wrong answer for the same reason."""
    kind, _ = _parser()._page_kind(CASH_FLOW_UNDER_NOTES_HEADER)
    assert kind == CASH_FLOW, "the notes code is printed first and must not win"


def test_title_over_form_can_still_overrule_the_code_it_admits():
    """The two rules compose: the guard admits the page on its single statement code, and
    `title_over_form` may then re-label it if a DIFFERENT statement's title is verbatim. Here
    they agree, so the answer is the same with the flag on."""
    assert _parser(title_over_form=True)._page_kind(CASH_FLOW_UNDER_NOTES_HEADER)[0] == CASH_FLOW
