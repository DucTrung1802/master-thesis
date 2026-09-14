"""`native_despite_garbled` — a layer that reads the text layer `_native_garbled` distrusts.

Pinned without a PDF and without an OCR engine. ⚠️ **THE DEFAULT PATH IS RIGHT MORE OFTEN THAN IT IS
WRONG** — SHB's substituted layer is shredded past use — so the flag is a LAYER, and these tests pin
that it is off by default, keys both caches it must, and still OCRs an image page.
"""
from web_scraper import cafef_financials as fin
from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer
from web_scraper.cafef_pdf_parser import PdfParser

# TPB Q3-2016's own balance-sheet text layer as `get_text()` returns it (2026-09-13): every accent
# substituted away, so the ratio test calls it mojibake — while its figures are the PDF's own.
TPB_TEXT = "\n".join([
    "Ngan hang TMCP Tien Phong", "BAO CAD TAI CHINH Quy III nam 2016", "BANG CAN DOI ICE TOAN",
    "Tai ngay 30 thong 09 nam 2016", "Chi den Thuyet minh 30/09/2016 Trieu VND 31/12/2015 Trieu VND",
    "A. TAI SAN 89.507.678 76.220.834", "I Tien mat, yang bac, da quy 5 684.695 621.500",
    "II Tien giii tai NHNN 6 912.888 1.227.426",
    "III Tien, yang giri tai cac TCTD khfic va cho vay cac TCTD khac 7 18.577.694 20.290.118",
    "1 Tien, yang gni tai cac TCTD khac 12.510.850 17.809.208",
    "2 Cho vay cac to clarc tin dung khac 6.066.844 2.480.910"])
NATIVE_WORDS = [(40.0, 120.0, 90.0, 131.0, "Tien", 0, 0, 0),
                (400.0, 120.0, 452.0, 131.0, "684.695", 0, 0, 1)]


class _Rect:
    width = 595.0
    height = 842.0


class _Page:
    number = 0
    rotation = 0
    rect = _Rect()

    def get_text(self, kind="text", **_):
        return list(NATIVE_WORDS) if kind == "words" else TPB_TEXT

    def widgets(self):
        return []


class _Engine:
    """Stands in for the onnx reader: whatever it returns is unmistakably the OCR path."""
    crop_pad = None
    red_channel = False

    def read_page(self, page):
        return "OCR", [(40.0, 120.0, 90.0, 131.0, "OCR", 0, 0, 0)]


def _parser(flag: bool) -> PdfParser:
    p = PdfParser()
    p.engine, p.ocr_ready, p._onnx = "onnx", True, _Engine()
    p.set_native_despite_garbled(flag)
    return p


def test_the_fixture_is_a_text_layer_the_default_path_throws_away():
    """The fixture must reproduce the defect, or nothing below proves anything."""
    p = _parser(False)
    assert p._native_garbled(TPB_TEXT)
    _text, words, _split = p._read_page(_Page(), TPB_TEXT)
    assert [w[4] for w in words] == ["OCR"]


def test_the_flag_reads_the_text_layer_instead():
    _text, words, split = _parser(True)._read_page(_Page(), TPB_TEXT)
    assert [w[4] for w in words] == ["Tien", "684.695"]
    assert split is False                    # the native path never takes the split repair


def test_an_image_page_is_still_ocrd_with_the_flag_on():
    """The flag overrides the MOJIBAKE verdict only — a page with too little text is an image."""
    _text, words, _split = _parser(True)._read_page(_Page(), "12 3")
    assert [w[4] for w in words] == ["OCR"]


def test_the_flag_is_off_by_default():
    assert PdfParser().native_despite_garbled is False
    assert PdfParser.native_despite_garbled is False
    assert ParseLayer("x", "onnx", 200).native_despite_garbled is False


def test_the_flag_keys_the_page_cache():
    """⚠️ A page read as OCR must never be served to a layer that reads it natively — the trap
    `reseat_words` fell into one cache up."""
    assert _parser(True)._ocr_config() != _parser(False)._ocr_config()


def test_the_flag_is_a_parse_key_and_not_an_ocr_key():
    plain = ParseLayer("plain", "onnx", 200)
    native = ParseLayer("native", "onnx", 200, native_despite_garbled=True)
    assert fin.parse_key(plain) != fin.parse_key(native)
    assert fin.ocr_key(plain) == fin.ocr_key(native)       # it reads no pixel; it declines to


def test_a_native_layer_is_a_widening():
    assert not ParseLayer("x", "onnx", 200, native_despite_garbled=True).is_strict


def test_apply_layer_carries_the_flag():
    p = PdfParser()
    FinancialsBuilder.apply_layer(p, ParseLayer("x", "onnx", 200, native_despite_garbled=True))
    assert p.native_despite_garbled is True
    FinancialsBuilder.apply_layer(p, ParseLayer("y", "onnx", 200))
    assert p.native_despite_garbled is False


def test_the_cascade_offers_a_native_reading_only_after_every_strict_one():
    """⚠️ A native layer reads words every strict layer refused to believe, so it must never run
    before one — `is_strict` counts the flag, and the block sits past the last strict layer."""
    layers = FinancialsBuilder.LAYERS
    native = [i for i, l in enumerate(layers) if l.native_despite_garbled]
    strict = [i for i, l in enumerate(layers) if l.is_strict]
    assert native, "no native layer in the cascade - this test measures nothing"
    assert min(native) > max(strict)
    assert any(layers[i].notes_boundary and layers[i].relax_merged_seam for i in native)


# -- `ENC-1`: two flavours a fold-first test cannot see ---------------------------------------
# SHB Q3-2016 page 3 as `get_text()` returns it: a Japanese OCR layer over a Vietnamese scan.
SHB_CJK = "ヽ ヽ ０ ヽ ヽ ミ ヽ ミ ュ ミ ヽ ミ ヽ ％ ミ ヽ ミ ヽ ミ ヽ ミ ¨ ヽ ら ヽ ヽ ヽ ヽ ヽ ミ ヽ ミ し 、 一 卜 ゛ Ｎ ． 一 卜 り （ い 一 ， 一 一 し （ ヽ い い い い 一 " * 20
# SAB FY-2008's balance sheet: VNI-encoded Vietnamese read as Latin-1.
SAB_VNI = ("Toång Coâng ty Coå phaàn Bia – Röôïu – Nöôùc Giaûi Khaùt Saøi Goøn vaø caùc coâng ty con "
           "Baûng caân ñoái keá toaùn hôïp nhaát taïi ngaøy 31 thaùng 12 naêm 2008 ") * 6
GENUINE = ("Tổng Công ty Cổ phần Bia – Rượu – Nước Giải Khát Sài Gòn và các công ty con "
           "Bảng cân đối kế toán hợp nhất tại ngày 31 tháng 12 năm 2008 ") * 6


def test_a_foreign_script_text_layer_is_garbled():
    """It folds to almost nothing, which the length test used to read as `too little to judge`."""
    assert PdfParser()._native_garbled(SHB_CJK) is True


def test_a_legacy_encoded_text_layer_is_garbled():
    assert PdfParser()._native_garbled(SAB_VNI) is True


def test_the_same_words_in_unicode_are_not():
    assert PdfParser()._native_garbled(GENUINE) is False
