"""`HLM-1` — the fleet census a CONTIGUITY goal needs: the quarters BETWEEN two solid ones.

Pinned on a stub `job` rather than on disk, because what is being tested is the RULE — a gap at
either end is not a hole (`_holes` says so for the count; this says so for the plan) — and the
corpus moves every run.
"""
import pytest

from kgpu import fleet

SOLID = ["balance_sheet", "income_statement", "cash_flow"]


class _Task:
    def __init__(self, period):
        self.period = period


class _Job:
    """Just enough of `pdf_ocr_job` for the census: a filing chain and what each quarter holds."""

    REPORTS = SOLID

    def __init__(self, parsed):
        self._parsed = parsed

    def resolve_template(self, _builder, _symbol):
        return ("corp", "resolved")

    def plan(self, _builder, _exchange, _symbol, **_kw):
        return [_Task(p) for p in self._parsed]

    def parsed_reports(self, _builder, task):
        return self._parsed[task.period]

    @staticmethod
    def as_quarter(period):
        return f"{period[3:]}-{period[:2]}"


class _Builder:
    def __init__(self, *a, **k):
        pass


@pytest.fixture()
def stub(monkeypatch):
    def _install(parsed):
        import sys
        import types
        job = _Job(parsed)
        fin = types.SimpleNamespace(FinancialsBuilder=_Builder)
        import importlib
        pkg = importlib.import_module("web_scraper")
        monkeypatch.setitem(sys.modules, "web_scraper.pdf_ocr_job", job)
        monkeypatch.setitem(sys.modules, "web_scraper.cafef_financials", fin)
        # ⚠️ `from web_scraper import pdf_ocr_job` reads the PACKAGE ATTRIBUTE once the submodule
        # has been imported by anything else in the session, so the `sys.modules` key alone
        # stubs nothing in a full-suite run — it passed alone and failed beside its neighbours.
        monkeypatch.setattr(pkg, "pdf_ocr_job", job, raising=False)
        monkeypatch.setattr(pkg, "cafef_financials", fin, raising=False)
        monkeypatch.setattr(fleet, "anchor", lambda: None)
        return job
    return _install


def test_a_gap_at_either_end_is_not_a_hole(stub):
    stub({"Q1-2020": [], "Q2-2020": SOLID, "Q3-2020": ["balance_sheet"], "Q4-2020": SOLID,
          "Q1-2021": []})
    census = fleet.hole_quarters([("AAA", "HOSE")])
    assert census["AAA"]["quarters"] == ["2020-Q3"]
    assert census["AAA"]["cells"] == 2


def test_a_ticker_whose_gaps_are_all_at_the_edges_is_not_on_the_plan(stub):
    stub({"Q1-2020": [], "Q2-2020": SOLID, "Q3-2020": SOLID, "Q4-2020": []})
    assert fleet.hole_quarters([("AAA", "HOSE")]) == {}
    assert fleet.hole_plans([("AAA", "HOSE")]) == []


def test_the_plans_are_richest_first_and_carry_the_resolved_template(stub):
    stub({"Q1-2020": SOLID, "Q2-2020": ["cash_flow"], "Q3-2020": [], "Q4-2020": SOLID})
    plans = fleet.hole_plans([("AAA", "HOSE")])
    assert len(plans) == 1
    assert plans[0].symbol == "AAA" and plans[0].template == "corp"
    assert plans[0].quarters == ["2020-Q2", "2020-Q3"]


def test_the_mode_is_offered_by_the_cli_and_the_lane_runners():
    import inspect
    src = inspect.getsource(fleet)
    assert src.count('choices=("open", "alternates", "fragmented", "holes")') == 2
    assert src.count('else hole_plans(names, reports_root=root, log=say) if mode == "holes"') == 2
    assert 'elif mode == "holes":' in src

