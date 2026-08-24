"""Pure tests for `FinancialsBuilder.documents` — no network, no OCR, no database.

They pin the two properties that make `allow_parent` safe to turn on, because both are
invariants a future edit can break silently:

1. **Consolidated always wins where both filings exist.** The fallback may only ADD periods,
   never change which ENTITY an existing period describes.
2. **The annual report wins Q4 only within one entity.** The merge that implements "the
   audited annual stands in for Q4" was a bare `dict.update`, which was safe only while both
   sides were consolidated-only; with `allow_parent` it silently moved **86 of 13,912**
   consolidated periods onto a standalone annual before the guard was added.
"""
import csv
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web_scraper import cafef_financials as cf
from web_scraper.cafef_financials import FinancialsBuilder

COLS = ["symbol", "exchange", "year", "quarter", "period", "name", "consolidated",
        "assurance", "half_year", "file_date", "bytes", "file", "path", "url"]


def _row(year, quarter, consolidated, assurance="unaudited", tag=""):
    period = f"FY-{year}" if quarter == 5 else f"Q{quarter}-{year}"
    name = f"{period}-{consolidated}-{assurance}{tag}"
    return {"symbol": "TST", "exchange": "HOSE", "year": str(year), "quarter": str(quarter),
            "period": period, "name": name, "consolidated": consolidated,
            "assurance": assurance, "half_year": "False", "file_date": "", "bytes": "1",
            "file": name + ".pdf", "path": f"files/HOSE_TST/{name}.pdf", "url": ""}


class DocumentSelectionTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        os.makedirs(os.path.join(self._tmp.name, "index"))
        self._prev = cf.PDFS_DIR
        cf.PDFS_DIR = self._tmp.name
        self.addCleanup(lambda: setattr(cf, "PDFS_DIR", self._prev))
        self.b = FinancialsBuilder()

    def _write(self, rows):
        path = os.path.join(self._tmp.name, "index", "HOSE_TST.csv")
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=COLS)
            w.writeheader()
            w.writerows(rows)

    def _pick(self, allow_parent):
        return {d["period"]: d for d in self.b.documents("HOSE", "TST",
                                                         allow_parent=allow_parent)}

    # ── 1. the fallback only ADDS ────────────────────────────────────────────
    def test_consolidated_wins_when_both_exist(self):
        self._write([_row(2015, 1, "True"), _row(2015, 1, "False")])
        self.assertEqual(self._pick(True)["Q1-2015"]["consolidated"], "True")

    def test_consolidated_wins_even_when_the_parent_is_better_assured(self):
        """Entity outranks assurance — a change of entity is never a trade for a change
        of assurance."""
        self._write([_row(2015, 2, "True", "unaudited"),
                     _row(2015, 2, "False", "audited")])
        self.assertEqual(self._pick(True)["Q2-2015"]["consolidated"], "True")

    def test_parent_is_used_only_where_no_consolidated_exists(self):
        self._write([_row(2015, 1, "True"), _row(2015, 2, "False")])
        self.assertNotIn("Q2-2015", self._pick(False))
        picked = self._pick(True)
        self.assertEqual(picked["Q1-2015"]["consolidated"], "True")
        self.assertEqual(picked["Q2-2015"]["consolidated"], "False")

    def test_fallback_never_changes_an_existing_period(self):
        self._write([_row(2015, q, "True") for q in (1, 2, 3)]
                    + [_row(2015, q, "False") for q in (1, 2, 3, 4)])
        before, after = self._pick(False), self._pick(True)
        for period, doc in before.items():
            self.assertEqual(after[period]["file"], doc["file"], period)
        self.assertEqual(set(after) - set(before), {"Q4-2015"})

    def test_a_single_entity_company_yields_nothing_without_the_fallback(self):
        """273 of 784 real tickers are in exactly this position."""
        self._write([_row(2015, q, "False") for q in (1, 2, 3, 4)])
        self.assertEqual(self._pick(False), {})
        self.assertEqual(len(self._pick(True)), 4)

    # ── 2. the annual stands in for Q4, but never across entities ────────────
    def test_annual_replaces_q4_within_one_entity(self):
        self._write([_row(2015, 4, "True", "unaudited"),
                     _row(2015, 5, "True", "audited")])
        picked = self._pick(True)["Q4-2015"]
        self.assertEqual(picked["assurance"], "audited")
        self.assertEqual(picked["annual"], "True")

    def test_a_parent_annual_does_not_displace_a_consolidated_q4(self):
        """The regression the guard exists for: measured at 86 of 13,912 periods."""
        self._write([_row(2015, 4, "True", "unaudited"),
                     _row(2015, 5, "False", "audited")])
        picked = self._pick(True)["Q4-2015"]
        self.assertEqual(picked["consolidated"], "True")
        self.assertEqual(picked["annual"], "False")

    def test_a_parent_annual_is_used_when_there_is_no_consolidated_q4(self):
        self._write([_row(2015, 5, "False", "audited")])
        picked = self._pick(True)["Q4-2015"]
        self.assertEqual(picked["consolidated"], "False")
        self.assertEqual(picked["annual"], "True")

    # ── 3. the row records which entity it came from ─────────────────────────
    def test_consolidated_is_a_data_column(self):
        """Two entities in one column with nothing saying which is which is the same
        defect as sourcing a figure from a web tab."""
        self.assertIn("consolidated", cf.DATA_COLS)

    # ── 4. the Q1-2008 input floor ───────────────────────────────────────────
    def test_the_floor_defaults_to_q1_2008(self):
        self.assertEqual(cf.FINANCIALS_PERIOD_MIN, "Q1-2008")

    def test_periods_below_the_floor_are_dropped(self):
        self._write([_row(2007, 2, "True"), _row(2008, 1, "True")])
        self.assertEqual(sorted(self._pick(True)), ["Q1-2008"])

    def test_the_floor_is_on_the_period_not_the_filing_year(self):
        """A 2009-dated FY-2008 annual contributes Q4-2008 and must be KEPT — filtering the
        raw index rows by year would have thrown away the report carrying the first quarter
        the floor exists to include."""
        self._write([_row(2008, 5, "True", "audited")])
        self.assertEqual(sorted(self._pick(True)), ["Q4-2008"])

    def test_a_pre_floor_annual_is_dropped(self):
        self._write([_row(2007, 5, "True", "audited")])
        self.assertEqual(self._pick(True), {})

    def test_the_floor_can_be_lifted(self):
        self._write([_row(2007, 2, "True"), _row(2008, 1, "True")])
        picked = self.b.documents("HOSE", "TST", allow_parent=True, period_min=None)
        # chronological, which is what the caller relies on for de-cumulation priors
        self.assertEqual([d["period"] for d in picked], ["Q2-2007", "Q1-2008"])

    def test_a_mid_year_floor_keeps_only_later_quarters(self):
        self._write([_row(2008, q, "True") for q in (1, 2, 3, 4)])
        picked = self.b.documents("HOSE", "TST", period_min="Q3-2008")
        self.assertEqual(sorted(d["period"] for d in picked), ["Q3-2008", "Q4-2008"])

    def test_undated_quarters_are_ignored(self):
        self._write([_row(2015, 1, "True"), {**_row(2015, 1, "True"), "quarter": "x"}])
        self.assertEqual(len(self._pick(True)), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)


class MagnitudeHistoryTest(unittest.TestCase):
    """`sane`'s reference population — the guard that a run poisoned on 2026-08-24.

    ACB's Q3-2009 STANDALONE filing produced an income statement with 2 mapped items. It
    reconciled, so its probe became the only entry in `history[income_statement]`, and
    `sane`'s median +/-20x band was therefore that single figure — which rejected Q1-2010's
    income statement, a quarter that had read cleanly at `onnx@200` for as long as the file
    had existed. Two independent defects, one symptom, and both are pinned here.
    """

    def setUp(self):
        self.b = FinancialsBuilder()

    def test_an_empty_history_never_rejects(self):
        st = _statement(cf.INCOME_STATEMENT)
        self.assertIsNone(self.b.sane(st, [], {self.b.C_PBT[0]: 1_000}))

    def test_a_single_bad_reference_rejects_a_good_quarter(self):
        """The mechanism itself — kept as a test so the fix cannot be quietly undone."""
        st = _statement(cf.INCOME_STATEMENT)
        verdict = self.b.sane(st, [1_000], {self.b.C_PBT[0]: 1_000_000_000})
        self.assertIsNotNone(verdict)
        self.assertIn("magnitude", verdict)

    def test_the_minimum_is_named_and_above_one(self):
        """A 2-item statement must not be able to define the band."""
        self.assertGreater(FinancialsBuilder.MIN_ITEMS_FOR_HISTORY, 2)

    def test_history_is_keyed_by_report_and_entity(self):
        """A standalone company is not the consolidated group; pooling their magnitudes makes
        the band meaningless in both directions."""
        import inspect
        src = inspect.getsource(FinancialsBuilder.build)
        self.assertIn('{"True": [], "False": []}', src)
        self.assertIn("history[report][entity]", src)

    def test_a_probe_equal_to_an_accepted_quarter_is_refused(self):
        """The comparative column read as the current one — unchanged by the fix."""
        st = _statement(cf.INCOME_STATEMENT)
        verdict = self.b.sane(st, [527_769_944], {self.b.C_PBT[0]: 527_769_944})
        self.assertIsNotNone(verdict)
        self.assertIn("exactly equals", verdict)


def _statement(report):
    from web_scraper.cafef_pdf_parser import Statement
    return Statement(report=report, pages=[1], unit=1, n_columns=2, rows=[])
