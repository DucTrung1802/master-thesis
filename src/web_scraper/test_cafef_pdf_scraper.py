# src\web_scraper\test_cafef_pdf_scraper.py
"""The two PURE decisions in `CafeFPdfScraper`: is a link a PDF, and is a document in
the year window. Both are classmethods over plain dicts — no network, no disk, no
database — so they can be pinned the way `preprocessor/test_filters.py` pins a screen.

    python -m pytest src/web_scraper/test_cafef_pdf_scraper.py -q

Every case below is a shape MEASURED on the live endpoint on 2026-08-23, over all 784
listed codes / 84,076 documents — not an invented one.
"""

import pytest

from web_scraper.cafef_pdf_scraper import CafeFPdfScraper as S


def doc(year, quarter=1, link="https://x/y.pdf"):
    return {"Year": year, "Quarter": quarter, "Link": link}


# ── _is_pdf_link ──────────────────────────────────────────────────────────────

def test_plain_pdf_link_is_a_pdf():
    assert S._is_pdf_link("https://cafefnew.mediacdn.vn/a/ACB_03CN_BCTC.pdf")


def test_cache_buster_does_not_stop_it_being_a_pdf():
    # ⚠️ The whole defect: 1,408 of 84,076 documents carry `?v=…`, VCB's own Q2-2026
    # filing among them, and `link.endswith(".pdf")` skipped every one silently.
    assert S._is_pdf_link(
        "https://cafefnew.mediacdn.vn/a/VCB_...Q2202026_31072026105556.pdf"
        "?v=1785470157744"
    )


@pytest.mark.parametrize("link", [
    "https://x/y.rar", "https://x/y.xls", "https://x/y.pdf.xls", "", None,
])
def test_non_pdf_links_are_rejected(link):
    assert not S._is_pdf_link(link)


def test_uppercase_extension_still_counts():
    assert S._is_pdf_link("https://x/Y.PDF")


# ── _in_year_window ───────────────────────────────────────────────────────────

def test_no_filter_keeps_everything():
    docs = [doc(2015), doc(2021), doc(0)]
    kept, undated = S._in_year_window(docs)
    assert len(kept) == 3 and undated == 1


def test_year_max_is_inclusive():
    kept, _ = S._in_year_window([doc(2019), doc(2020), doc(2021)], year_max=2020)
    assert [d["Year"] for d in kept] == [2019, 2020]


def test_year_min_is_inclusive():
    kept, _ = S._in_year_window([doc(2020), doc(2021), doc(2022)], year_min=2021)
    assert [d["Year"] for d in kept] == [2021, 2022]


def test_the_two_phases_partition_the_corpus_exactly():
    """⚠️ The property that matters for TODO `P2`: phase 1 + phase 2 = everything, with
    nothing counted twice and — the part that is easy to get wrong — nothing LOST."""
    docs = [doc(y) for y in (2002, 2015, 2020, 2021, 2026)] + [doc(0), doc(202)]
    p1, _ = S._in_year_window(docs, year_max=2020)
    p2, _ = S._in_year_window(docs, year_min=2021)
    assert len(p1) + len(p2) == len(docs)
    assert not ({id(d) for d in p1} & {id(d) for d in p2})


@pytest.mark.parametrize("bad", [0, 202, 203, None, "", "n/a"])
def test_an_undated_document_lands_in_the_year_max_phase(bad):
    # CafeF really files 8 documents at Year 0, one at 202 and one at 203.
    kept, undated = S._in_year_window([doc(bad)], year_max=2020)
    assert len(kept) == 1 and undated == 1
    assert S._in_year_window([doc(bad)], year_min=2021)[0] == []


def test_an_undated_document_never_matches_an_exact_year_set():
    assert S._in_year_window([doc(0)], years={2020})[0] == []


def test_exact_set_and_window_compose():
    docs = [doc(2018), doc(2019), doc(2020)]
    kept, _ = S._in_year_window(docs, years={2018, 2020}, year_max=2019)
    assert [d["Year"] for d in kept] == [2018]


def test_year_is_read_from_a_string_too():
    kept, _ = S._in_year_window([doc("2019"), doc("2021")], year_max=2020)
    assert [d["Year"] for d in kept] == ["2019"]
