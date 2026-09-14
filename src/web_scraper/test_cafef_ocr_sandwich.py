"""`ocr_sandwich` — a layer that OCRs a scan whose text layer is somebody else's OCR (`SDW-1`).

⚠️ **TPB Q1-2020 IS 41 FULL-PAGE IMAGES UNDER INVISIBLE TEXT**, and that text reads its profit before
tax as `tong_iqi_nhun_trirac_thu` — so every layer of the cascade read the transcription and none
showed the page to the OCR. These pin the detector on a REAL PDF built here (an image covering the
page, text in render mode 3), that the flag is off by default, that it sends only such a page to the
engine, and that it keys both caches it must without re-reading any other page.
"""
import fitz

from web_scraper import cafef_financials as fin
from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer
from web_scraper.cafef_pdf_parser import PdfParser

TEXT = "Tong loi nhuan truoc thue 1.009.455 852.866 " * 20


def _pdf(tmp_path, *, image=True, invisible=True, name="f.pdf"):
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    if image:
        pix = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, 60, 85), False)
        pix.set_rect(pix.irect, (255, 255, 255))
        page.insert_image(page.rect, pixmap=pix)
    page.insert_textbox(fitz.Rect(40, 40, 555, 800), TEXT, fontsize=9,
                        render_mode=3 if invisible else 0)
    path = str(tmp_path / name)
    doc.save(path)
    return path


class _Engine:
    crop_pad = None
    red_channel = False

    def __init__(self):
        self.calls = 0

    def read_page(self, page):
        self.calls += 1
        return "OCR", [(40.0, 120.0, 90.0, 131.0, "OCR", 0, 0, 0)]


def _parser(flag):
    p = PdfParser()
    p.engine, p.ocr_ready, p._onnx = "onnx", True, _Engine()
    # The fixture's base-14 font carries no Vietnamese diacritics, which `_native_garbled` would
    # rightly call substitution mojibake; that verdict is not what these tests are about.
    p._native_garbled = lambda native: False
    p.set_ocr_sandwich(flag)
    return p


def test_a_scan_under_invisible_text_is_a_sandwich(tmp_path):
    path = _pdf(tmp_path)
    with fitz.open(path) as doc:
        assert PdfParser()._is_sandwich(doc[0])
    assert PdfParser().has_sandwich_page(path)


def test_a_digital_page_is_not(tmp_path):
    """Visible text over the same image is a printed page, not a transcription."""
    path = _pdf(tmp_path, invisible=False)
    with fitz.open(path) as doc:
        assert not PdfParser()._is_sandwich(doc[0])
    assert not PdfParser().has_sandwich_page(path)


def test_invisible_text_without_a_page_sized_image_is_not(tmp_path):
    path = _pdf(tmp_path, image=False)
    with fitz.open(path) as doc:
        assert not PdfParser()._is_sandwich(doc[0])


def test_the_default_reads_the_transcription_and_the_flag_reads_the_pixels(tmp_path):
    path = _pdf(tmp_path)
    with fitz.open(path) as doc:
        page = doc[0]
        native = page.get_text()
        off = _parser(False)
        off._use_document(path)
        _t, words, _s = off._read_page(page, native)
        assert "OCR" not in [w[4] for w in words]           # the embedded text, as today
        on = _parser(True)
        on._use_document(path)
        _t, words, _s = on._read_page(page, native)
        assert [w[4] for w in words] == ["OCR"]


def test_the_flag_changes_nothing_on_a_digital_page(tmp_path):
    path = _pdf(tmp_path, invisible=False)
    with fitz.open(path) as doc:
        page = doc[0]
        on = _parser(True)
        on._use_document(path)
        _t, words, _s = on._read_page(page, page.get_text())
        assert "OCR" not in [w[4] for w in words]
        assert on._onnx.calls == 0


def test_the_page_cache_separates_the_two_readings_of_a_sandwich_page(tmp_path):
    """⚠️ A page read from its text layer must never be served to a layer that OCRs it."""
    path = _pdf(tmp_path)
    with fitz.open(path) as doc:
        page = doc[0]
        p = _parser(False)
        p._use_document(path)
        first = p._ocr_page(page, page.get_text())
        p.set_ocr_sandwich(True)
        second = p._ocr_page(page, page.get_text())
        assert [w[4] for w in second[1]] == ["OCR"] and first != second


def test_the_flag_is_off_by_default():
    assert PdfParser().ocr_sandwich is False
    assert PdfParser.ocr_sandwich is False
    assert ParseLayer("x", "onnx", 200).ocr_sandwich is False


def test_the_flag_is_a_parse_key_and_an_ocr_key():
    plain = ParseLayer("plain", "onnx", 200)
    sandwich = ParseLayer("sandwich", "onnx", 200, ocr_sandwich=True)
    assert fin.parse_key(plain) != fin.parse_key(sandwich)
    assert fin.ocr_key(plain) != fin.ocr_key(sandwich)       # it decides whether pixels are read


def test_a_sandwich_layer_is_a_widening_and_apply_layer_carries_it():
    assert not ParseLayer("x", "onnx", 200, ocr_sandwich=True).is_strict
    p = PdfParser()
    FinancialsBuilder.apply_layer(p, ParseLayer("x", "onnx", 200, ocr_sandwich=True))
    assert p.ocr_sandwich is True
    FinancialsBuilder.apply_layer(p, ParseLayer("y", "onnx", 200))
    assert p.ocr_sandwich is False


def test_the_sandwich_layers_are_last():
    names = [l.name for l in FinancialsBuilder.LAYERS if l.engine == "onnx"]
    first = next(i for i, l in enumerate(FinancialsBuilder.LAYERS)
                 if l.engine == "onnx" and l.ocr_sandwich)
    assert all(l.ocr_sandwich for l in FinancialsBuilder.LAYERS[first:] if l.engine == "onnx")
    assert "onnx@200+sandwich" in names


# -- `SDW-2`: a signature stamp's visible text does not make a scan a printed page -----------
def _signed_pdf(tmp_path):
    """A full-page image under invisible text, plus a visible stamp line at the foot of the page."""
    path = _pdf(tmp_path, name="signed.pdf")
    doc = fitz.open(path)
    doc[0].insert_textbox(fitz.Rect(4, 816, 300, 842), "Ký bởi: NGÂN HÀNG TMCP", fontsize=6, render_mode=0)
    out = str(tmp_path / "signed2.pdf")
    doc.save(out)
    return out


def test_visible_text_inside_the_signature_widget_is_left_out(tmp_path):
    """SHB Q3-2022 page 1: 2,144 invisible characters and a 103-character stamp inside its widget."""
    path = _signed_pdf(tmp_path)
    p = PdfParser()
    p._signature_rects = lambda page: [fitz.Rect(0, 810, 320, 842)]
    with fitz.open(path) as doc:
        assert p._is_sandwich(doc[0])


def test_the_same_visible_text_outside_any_widget_still_makes_it_a_printed_page(tmp_path):
    path = _signed_pdf(tmp_path)
    p = PdfParser()
    p._signature_rects = lambda page: []
    with fitz.open(path) as doc:
        assert not p._is_sandwich(doc[0])
