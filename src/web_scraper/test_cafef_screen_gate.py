"""`SCG-1` — the release screens asked inside the cascade, so a flagged reading does not end it."""
import web_scraper.cafef_financials as fin

FLAGGED = {"tong_cong_tai_san": 100, "tong_cong_nguon_von": 100, "a_tai_san_ngan_han": 100}
CLEAN = {"tong_cong_tai_san": 100, "tong_cong_nguon_von": 100, "a_tai_san_ngan_han": 60}


class _Statement:
    publish_date = "2012-08-14"
    unit = 1
    shares_authorized = shares_issued = shares_outstanding = None
    report = "balance_sheet"
    n_columns = 2
    pages: list = []
    rows: list = []

    def find(self, *a, **k):
        return None

    def _first_value(self, values):
        return None


class _Parser:
    ocr_ready = True
    on_page = None
    _logger = None

    def parse(self, path, period_end, want_shares=True):
        return {"balance_sheet": _Statement()}

    def has_sandwich_page(self, path):
        return False

    def is_one_page(self, path):
        return False

    def __getattr__(self, name):
        return lambda *a, **k: None


def _builder(rows):
    b = fin.FinancialsBuilder.__new__(fin.FinancialsBuilder)
    b._logger = None
    b.on_layer = b.on_page = None
    b._parsers = {}
    b._parser_for = lambda engine: _Parser()
    calls = []

    def _map(*a, **k):
        calls.append(1)
        return dict(rows[min(len(calls), len(rows)) - 1])

    b.map_to_schema = _map
    b.reconcile = lambda *a, **k: None
    b.sane = lambda *a, **k: None
    b._warn = lambda *a, **k: None
    return b


def test_a_flagged_reading_does_not_end_the_cascade_when_a_later_layer_reads_clean():
    """VPB Q2-2012: a line item holding the grand total, accepted at the first layer that reconciled."""
    b = _builder([FLAGGED, CLEAN])
    accepted, _facts = b._parse_cascaded("x.pdf", None, "bank", {r: [] for r in fin.REPORTS})
    row, _st, layer = accepted["balance_sheet"]
    assert row["a_tai_san_ngan_han"] == 60
    assert layer != fin.FinancialsBuilder.LAYERS[0].name
    assert any(why.startswith("screens:") for _l, why in b.refusals["balance_sheet"])


def test_a_statement_no_layer_reads_clean_keeps_its_first_reading():
    b = _builder([FLAGGED])
    accepted, _facts = b._parse_cascaded("x.pdf", None, "bank", {r: [] for r in fin.REPORTS})
    row, _st, layer = accepted["balance_sheet"]
    assert row == FLAGGED and layer == fin.FinancialsBuilder.LAYERS[0].name


def test_a_clean_first_reading_is_accepted_at_once():
    b = _builder([CLEAN, FLAGGED])
    accepted, _facts = b._parse_cascaded("x.pdf", None, "bank", {r: [] for r in fin.REPORTS})
    row, _st, layer = accepted["balance_sheet"]
    assert row == CLEAN and layer == fin.FinancialsBuilder.LAYERS[0].name


# -- `SCG-2`: the in-cascade screens see the statement's rows -----------------------------------
def test_the_cascade_screens_read_a_loans_line_off_the_statements_rows():
    """BVH Q1-2013: A + B + the banking subsidiary's loans is the total; only the rows hold the loans."""
    from web_scraper.cafef_pdf_parser import BALANCE_SHEET, Row, Statement
    builder = fin.FinancialsBuilder(logger=None)
    rows = [Row(label="Cho vay và ứng trước cho khách hàng", key="cho_vay_va_ung_truoc_cho_khach_hang",
                number=None, values=[7_516_886_653_078, 7_042_879_686_335])]
    st = Statement(report=BALANCE_SHEET, pages=[1], unit=1, n_columns=2, rows=rows)
    row = {"a_tai_san_ngan_han": 18_996_260_888_045, "b_tai_san_dai_han": 21_338_277_357_893,
           "tong_cong_tai_san": 47_851_424_899_016, "tong_cong_nguon_von": 47_851_424_899_016}
    assert builder._screen_flags(BALANCE_SHEET, row, st) == []
    st.rows = []
    assert builder._screen_flags(BALANCE_SHEET, row, st) != []
