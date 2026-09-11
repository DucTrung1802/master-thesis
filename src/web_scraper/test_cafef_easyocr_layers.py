"""The third engine's place in the cascade — `EOC-1`. No PDF, no GPU, no OCR engine.

⚠️ **`easyocr` WAS IMPLEMENTED FROM THE BEGINNING AND NO `ParseLayer` HAD EVER USED IT.**
`PdfParser._ocr_page_easyocr` has existed the whole time; the module header explained the
omission by saying the engine "fragments boxes differently". ⚠️ **That note was written while the
easyocr branch of `_read_page` SKIPPED `_merge_split_figures`** — the repair the onnx branch gets
and the Tesseract branch was given on 2026-09-08 (`TSM-1`). The easyocr branch got it on
2026-09-11 (`TSM-2`), and the note was the symptom of a missing repair rather than a verdict on
an engine.

⚠️ **MEASURED BEFORE SHIPPING, ON CELLS THAT ARE ACTUALLY OPEN** (2026-09-11). Eight documents
drawn from the 206 open cells whose page WAS found and whose reading was refused: **2 cells won**
— STB Q1-2016's cash flow and STB Q3-2015's balance sheet, both refused by all 115 ONNX layers.
Both wins are on SCANS, and that is the whole shape of the result: `_read_page` uses a page's
native text layer when it has one, so on a text PDF no OCR engine runs and these layers cannot
matter. **0 of 4 text documents, 2 of 4 scans.**

What this file pins is not the hit rate — that is a measurement, and it lives in
`.claude/findings/data-state.md`. It pins the two decisions that keep the cascade honest:
**where the block sits**, and **that `engine == "onnx"` still selects a cascade without
it**. ⚠️ The first of those was got WRONG on the first attempt and the suite caught it —
see `test_the_block_sits_beside_TESSERACT_and_not_at_the_end`.
"""
from web_scraper import cafef_financials as fin

EASYOCR = ("easyocr@200", "easyocr@300", "easyocr@400",
           "easyocr@300+relax", "easyocr@400+relax")


def _layers():
    return fin.FinancialsBuilder.LAYERS


def test_the_block_sits_beside_TESSERACT_and_not_at_the_end():
    """⚠️ **APPENDING THEM TO THE END WAS THE FIRST ATTEMPT AND IT BROKE 20 TESTS, CORRECTLY.**

    The cost argument says put a 12-minute engine last, so that is what was written: the five
    at positions 117-121, reachable only by a document that had lost everything. **The cascade
    escalates CREDULITY, and that ordering inverted it.** `ParseLayer.is_strict`'s contract is
    that *no layer reading the box AS PRINTED may run after a widening one*, so a strict
    `easyocr@200` at position 118 would have let a RELAXED onnx reading win a statement an
    EXACT easyocr reading could have taken — and `_parse_cascaded` stops at the first pass, so
    the looser reading is the one that would have been written.

    ⚠️ **THE REPO ALREADY HAD THE ANSWER IN `tesseract@200` AT POSITION 3.** A second engine is
    interleaved by credulity, not appended. The compensation for meeting them early is that a
    document easyocr CAN read stops there instead of running 110 more layers.
    """
    names = [layer.name for layer in _layers()]
    first = names.index("easyocr@200")

    assert names[first - 1] == "tesseract@400+relax"
    assert tuple(names[first:first + 5]) == EASYOCR


def test_the_STRICT_easyocr_layers_come_before_its_relaxed_ones():
    """The block is itself a ladder, and it climbs in the one direction the cascade climbs."""
    names = [layer.name for layer in _layers()]
    by_name = {layer.name: layer for layer in _layers()}
    strict = [n for n in EASYOCR if by_name[n].is_strict]
    wide = [n for n in EASYOCR if not by_name[n].is_strict]

    assert strict and wide
    assert max(names.index(n) for n in strict) < min(names.index(n) for n in wide)


def test_ONNX_ONLY_still_selects_a_cascade_WITHOUT_them():
    """⚠️ `TSS-1`: the control notebook sets `ONNX_ONLY = True` so a Kaggle worker and this
    machine read one document the same way, and the merge does not see the difference as
    `DIFFERS`. The filter is `engine == "onnx"` and must keep excluding every new engine."""
    kept = [layer.name for layer in _layers() if layer.engine == "onnx"]

    assert len(kept) == 115
    assert not [name for name in kept if "easyocr" in name or "tesseract" in name]


def test_every_easyocr_layer_mirrors_a_shape_the_onnx_head_already_runs():
    """The five are the ONNX head's own shapes on a different engine — `@{200,300,400}` with
    no flags, plus two `+relax`. A verdict that differs is then a difference in the ENGINE and
    not in the configuration, which is what made the probe's answer readable at all."""
    by_name = {layer.name: layer for layer in _layers()}
    for name, dpi, relax in (("easyocr@200", 200, False), ("easyocr@300", 300, False),
                             ("easyocr@400", 400, False),
                             ("easyocr@300+relax", 300, True),
                             ("easyocr@400+relax", 400, True)):
        layer = by_name[name]
        assert (layer.engine, layer.dpi, layer.relax_totals) == ("easyocr", dpi, relax)


def test_no_easyocr_layer_carries_a_REPAIR_FLAG():
    """⚠️ **A NEW ENGINE AND A NEW REPAIR IN ONE LAYER IS A MEASUREMENT THAT CANNOT BE READ.**
    If one of these won a cell, the question "was it the engine or the flag?" would have no
    answer on disk. The five carry `relax_totals` and nothing else, so a win is the engine's."""
    for layer in _layers():
        if layer.engine != "easyocr":
            continue
        for flag in ("join_digits", "join_lost_separator", "merged_tail", "truncated_total",
                     "reseat_words", "deskew_rows", "notes_tail", "condensed_form"):
            assert getattr(layer, flag, False) is False, f"{layer.name} carries {flag}"


def test_the_cascade_has_no_duplicate_layer_NAME():
    """A name is how `run_batch(layers=[...])` addresses a layer across a process boundary, so
    two layers sharing one is a run that cannot say what it did."""
    names = [layer.name for layer in _layers()]

    assert len(set(names)) == len(names)
