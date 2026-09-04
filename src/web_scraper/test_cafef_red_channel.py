"""The COMPANY SEAL stamped across a printed figure — pinned without a PDF or an engine.

A Vietnamese filing is signed with a round RED seal, and on some scans it lands on the
grand-total line. The composite image the recogniser is normally shown has the ink and the
digits on top of each other; the RED CHANNEL alone renders the seal as near-white and leaves
every black stroke, because red ink is (R high, G low, B low) against black print's (low, low,
low).

⚠️ THE SIGNATURE OF INK OVER A FIGURE IS THAT EVERY DPI IS WRONG AND WRONG DIFFERENTLY. FPT
Q1-2016's `TỔNG CỘNG NGUỒN VỐN` is printed 24.695.453.363.505 and reads 24.693.152.363.505 at
200 dpi, the same at 300, 2.469.355.661.505 at 400 and 24.605.453.361.505 at 500 — four
answers, none of them the printed one, while `A + B` closes on the assets side to the đồng. A
resolution problem converges as the resolution rises; this does not, because the pixels the
recogniser needs are not missing, they are covered.

⚠️ BLANKING THE SATURATED PIXELS WAS TRIED FIRST AND IS WORSE, and that measurement is pinned
below so it is not re-made. Where the seal crosses a digit the pixel is BOTH saturated and part
of the stroke, so setting saturated pixels to white deletes what it overlapped — the same
figure came back as `24.6??.453.363.5?5`, a fresh wrong answer rather than a refusal. Taking
one channel keeps every black stroke and drops only ink that is not black.
"""
import numpy as np

from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer, ocr_key, parse_key
from web_scraper.cafef_pdf_parser import PdfParser


def _rgb(pixels):
    return np.array(pixels, dtype=np.uint8)


def _red_channel(img):
    """What `OnnxOcr.read_page` does under the flag, on the array it has already built."""
    return np.repeat(img[:, :, :1], 3, axis=2)


def _blank_saturated(img, sat=40):
    """The rejected alternative, kept so the measurement below can be made against it."""
    mx = img.max(axis=2).astype(np.int16)
    mn = img.min(axis=2).astype(np.int16)
    out = img.copy()
    out[(mx - mn) >= sat] = 255
    return out


# One row of pixels: white page, a black digit stroke, the same stroke with red ink over it,
# and bare red ink. The two ink colours are the ones a scanned seal and scanned print actually
# return — a seal is red enough to saturate and print is neutral.
WHITE, BLACK = (255, 255, 255), (35, 35, 35)
INK_OVER_PRINT, INK = (120, 30, 30), (205, 60, 60)


def test_the_red_channel_keeps_every_black_stroke_and_drops_the_seal():
    """The claim the flag rests on, as arithmetic: after the channel copy a stroke is still
    dark whether or not the seal crossed it, and bare ink is light."""
    img = _rgb([[WHITE, BLACK, INK_OVER_PRINT, INK]])
    out = _red_channel(img)
    page, stroke, crossed, ink = out[0, :, 0]
    assert page == 255
    assert stroke == 35                      # untouched print stays print
    assert crossed == 120                    # a stroke UNDER the seal is still much darker
    assert ink == 205                        # …and the seal itself is near-white
    assert int(ink) - int(crossed) > 60, "the seal must separate from the stroke it crosses"
    # …and every channel is the same, because the detector and the recogniser want 3 channels.
    assert (out[:, :, 0] == out[:, :, 1]).all() and (out[:, :, 1] == out[:, :, 2]).all()


def test_blanking_the_saturated_pixels_deletes_the_stroke_it_overlapped():
    """⚠️ THE REJECTED ALTERNATIVE, PINNED. It is the obvious repair and it is the one that
    turns a refusal into a wrong figure: the pixel where the seal crosses a digit is saturated,
    so blanking it removes the digit. FPT Q1-2016 read `24.6??.453.363.5?5` this way."""
    img = _rgb([[WHITE, BLACK, INK_OVER_PRINT, INK]])
    out = _blank_saturated(img)
    assert (out[0, 2] == 255).all(), "the crossed stroke is erased — this is the defect"
    assert (out[0, 1] == BLACK).all(), "…while the stroke the seal missed survives"
    # so the two differ exactly where it matters, and only the channel copy keeps the digit
    assert _red_channel(img)[0, 2, 0] < 200


def test_a_greyscale_scan_cannot_move():
    """⚠️ THE SAFETY ARGUMENT, AND IT IS A CONSTRUCTION RATHER THAN A MEASUREMENT: on a page
    with no coloured ink R == G == B, so the channel copy is the identity. A filing whose scan
    is grey is bit-identical with the flag on and off, which is why these layers can sit in the
    cascade at all."""
    grey = _rgb([[(0, 0, 0), (128, 128, 128), (255, 255, 255)],
                 [(17, 17, 17), (200, 200, 200), (9, 9, 9)]])
    assert (_red_channel(grey) == grey).all()


def test_the_flag_is_off_by_default_and_reaches_the_engine():
    assert ParseLayer("x", "onnx", 200).red_channel is False

    class _Onnx:
        crop_pad = 2.0
        red_channel = False

    p = PdfParser.__new__(PdfParser)
    p._onnx = _Onnx()
    p.engine = "onnx"
    p.set_red_channel(True)
    assert p._onnx.red_channel is True
    p.set_red_channel(False)
    assert p._onnx.red_channel is False


def test_set_red_channel_is_a_no_op_without_the_onnx_engine():
    """Tesseract rasterises its own page, so the flag has nowhere to go — and must not raise."""
    p = PdfParser.__new__(PdfParser)
    p._onnx = None
    p.engine = "tesseract"
    p.set_red_channel(True)              # no exception, nothing set


def test_it_is_part_of_BOTH_keys_because_it_changes_the_pixels():
    """⚠️ `ocr_key` is the one that matters and the one that is easy to forget: served a page
    another layer read in colour, a `+red` layer would 'run' without reading a pixel and report
    the reading that had already failed. `reseat_words` cost a whole run to that exact
    omission."""
    a = FinancialsBuilder.LAYERS[0]
    b = next(l for l in FinancialsBuilder.LAYERS
             if l.red_channel and l.dpi == a.dpi and l.crop_pad == a.crop_pad)
    assert ocr_key(a) != ocr_key(b)
    assert parse_key(a) != parse_key(b)


def test_the_parser_cache_key_separates_the_two_readings():
    """The page cache is keyed on `_ocr_config`, so the same page read in colour and in the red
    channel must be two entries — the same argument one level down from `ocr_key`."""

    class _Onnx:
        crop_pad = 2.0
        red_channel = False

    p = PdfParser.__new__(PdfParser)
    p._onnx = _Onnx()
    p.engine = "onnx"
    p.dpi = 200
    plain = p._ocr_config()
    p.set_red_channel(True)
    assert p._ocr_config() != plain


def test_the_BARE_red_layers_are_STRICT_and_close_the_strict_zone():
    """⚠️ NOT A WIDENING, AND `is_strict` IS WHERE THAT IS DECIDED. Like `dpi` and `crop_pad`
    this changes what the recogniser is SHOWN, not what the matcher will believe — so a layer
    carrying it AND NOTHING ELSE obeys the rule they do: no layer reading the box as printed
    may run after one that widens what is believed. That puts the bare pair at the END of the
    strict zone, and every widening block's own `block[0] > max(strict)` guard moves with it.
    """
    layers = FinancialsBuilder.LAYERS
    bare = [i for i, l in enumerate(layers) if l.red_channel and l.is_strict]
    assert bare, "no strict +red layer in the cascade"
    strict = [i for i, l in enumerate(layers) if l.is_strict]
    assert max(bare) == max(strict), "the bare +red block is the tail of the strict zone"
    assert all(not layers[i].is_strict for i in range(max(bare) + 1, len(layers)))


def test_the_flag_may_also_ride_with_a_widening_one_and_then_it_is_widening():
    """⚠️ TWO BLOCKS, AND THAT IS WHAT A REUSABLE FLAG LOOKS LIKE — `join_lost_separator` went
    the same way when `+deskew` needed it. FPT Q1-2016 needs the seal removed AND the lost
    thousands separators joined: strict `+red` still reads 5 fragments and `+joinlost` alone
    reads the sealed total as 24.693.152.363.505 against a printed 24.695.453.363.505, so the
    two repairs have to meet. A layer carrying both is a WIDENING layer, because
    `join_lost_separator` is one, and it must therefore sit after every strict read.
    """
    layers = FinancialsBuilder.LAYERS
    both = [i for i, l in enumerate(layers) if l.red_channel and l.join_lost_separator]
    assert both, "no +joinlost+red layer in the cascade"
    assert all(not layers[i].is_strict for i in both)
    strict = [i for i, l in enumerate(layers) if l.is_strict]
    assert min(both) > max(strict)


def test_it_costs_ONE_new_OCR_pass_PER_DPI_however_many_layers_carry_it():
    """⚠️ THE COST, STATED, AND IT IS PER RENDER AND NOT PER LAYER. `ocr_key` is
    `(engine, dpi, crop_pad, red_channel)`, so `onnx@200+red` and `onnx@200+joinlost+red` are
    ONE trip through the recogniser — which is what makes the second block free. Two DPIs is
    two passes over a filing that has already defeated forty-five, and that is the whole bill."""
    reds = [l for l in FinancialsBuilder.LAYERS if l.red_channel]
    assert len({ocr_key(l) for l in reds}) == 2,         "one pass per dpi: 200 and 300, however many layers carry the flag"
    plain = {ocr_key(l) for l in FinancialsBuilder.LAYERS if not l.red_channel}
    assert not (plain & {ocr_key(l) for l in reds}),         "a +red layer sharing a pass with a colour one would read the pixels it was added to avoid"
