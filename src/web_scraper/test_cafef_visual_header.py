"""`VHD-1` — a text layer that emits the statement title after the figures."""
from web_scraper.cafef_pdf_parser import PdfParser


def _w(x, y, text):
    return (x, y, x + 8.0 * len(text), y + 9.0, text, 0, 0, 0)


# GAS Q2-2022 page 4 as the text layer orders it: figures first, the title last.
WORDS = [_w(40, 64, "TẬP ĐOÀN DẦU KHÍ QUỐC GIA VIỆT NAM"), _w(40, 144, "Mã số"), _w(300, 144, "30/06/2022"),
         _w(40, 170, "A. TÀI SẢN NGẮN HẠN"), _w(300, 170, "37.469.263.584.216"),
         _w(40, 190, "I. Tiền và các khoản tương đương tiền"), _w(300, 190, "9.054.658.184.090"),
         _w(160, 102, "BẢNG CÂN ĐỐI KẾ TOÁN HỢP NHẤT"), _w(170, 117, "Tại ngày 30 tháng 06 năm 2022")]
TEXT_ORDER = "\n".join(w[4] for w in WORDS if w[4] != "BẢNG CÂN ĐỐI KẾ TOÁN HỢP NHẤT"
                        and "Tại ngày" not in w[4]) + "\n" + "\n".join(["x"] * 20) + \
    "\nBẢNG CÂN ĐỐI KẾ TOÁN HỢP NHẤT\nTại ngày 30 tháng 06 năm 2022"


def test_the_fixture_reproduces_the_defect():
    assert PdfParser()._page_kind(TEXT_ORDER)[0] is None


def test_a_title_the_text_layer_emits_last_is_read_from_the_top_of_the_page():
    kind, _form = PdfParser()._page_kind_visual(TEXT_ORDER, WORDS)
    assert kind == "balance_sheet"


def test_reading_order_is_top_to_bottom_then_left_to_right():
    lines = PdfParser._visual_text(WORDS).splitlines()
    assert lines[0].startswith("TẬP ĐOÀN") and lines[1] == "BẢNG CÂN ĐỐI KẾ TOÁN HỢP NHẤT"
    assert lines[3] == "Mã số 30/06/2022"


def test_a_page_the_text_order_classified_keeps_its_verdict():
    notes = "THUYẾT MINH BÁO CÁO TÀI CHÍNH HỢP NHẤT\nCác thuyết minh này là một bộ phận hợp thành"
    p = PdfParser()
    assert p._page_kind_visual(notes, WORDS) == p._page_kind(notes)


def test_a_page_whose_reading_order_names_no_statement_stays_unclassified():
    words = [_w(40, 64, "Chữ ký"), _w(40, 90, "Nguyễn Văn A")]
    assert PdfParser()._page_kind_visual("Nguyễn Văn A\nChữ ký", words) == (None, False)
