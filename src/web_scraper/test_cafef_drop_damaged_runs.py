"""`SPL-2` — a numeric run no grouping can read is DROPPED, not apportioned into fake figures.

`_split_number_runs`' last branch cuts a multi-part numeric run into one box per part. When the
separators were merely read as spaces that is the repair (`join_lost` joins them back), but when
the run is DAMAGED — a group of the wrong length, so no join is well-formed — apportioning it
emits two or more plausible numbers that are not figures, `split_figures` counts the adjacencies
between them, and `reconcile` refuses the WHOLE STATEMENT for one box.

⚠️ **MEASURED ON HPG's Q1-2012 CASH FLOW, 2026-09-13**: page 5 holds 31 lost-separator runs in
the value zone, **30 of which `join_lost` joins into a well-formed figure and ONE of which it
cannot** — `'- 9 22 566 554'`, whose group `'22'` is two digits, so `'9.22.566.554'` fails
`MERGE_JOIN_RE`. Apportioned, it becomes `'9'`, `'22'`, `'566'`, `'554'`; the gate counts two
adjacencies and the cash flow is refused at **every one of the 115 layers**, minimum count 2.

⚠️ **AND IT CANNOT BE REPAIRED WITHOUT INVENTING A DIGIT** — regrouping gives `922.566.554`
while the print may equally say `9.922.566.554`, three orders apart. §5 rule 24 forbids that as
a source, so the honest choices are to refuse the statement or to drop the box.
"""
from web_scraper.cafef_pdf_parser import PdfParser


def _box(text, x0=380.0, y=100.0, w=57.0, idx=0):
    return (x0, y, x0 + w, y + 9.0, text, 0, 0, idx)


def _texts(words):
    return [str(w[4]) for w in words]


def test_damaged_run_is_dropped_when_the_flag_is_on():
    """The run `join_lost` declined leaves NO box behind, so nothing can land on a column."""
    out = PdfParser._split_number_runs([_box("- 9 22 566 554")],
                                       join_lost=True, drop_damaged=True)
    assert out == [], _texts(out)


def test_damaged_run_is_apportioned_when_the_flag_is_off():
    """Today's behaviour is untouched — the flag is the whole change, and it is off by default."""
    out = PdfParser._split_number_runs([_box("- 9 22 566 554")], join_lost=True)
    assert _texts(out) == ["-", "9", "22", "566", "554"], _texts(out)
    # ⚠️ and THIS is what the gate counts: three adjacent plausible numbers where one figure
    # was printed.
    assert PdfParser._split_number_runs([_box("- 9 22 566 554")]) != []


def test_a_repairable_run_is_still_joined_and_not_dropped():
    """`drop_damaged` must not reach a run `join_lost` CAN read — 30 of HPG's 31 are these."""
    out = PdfParser._split_number_runs([_box("162 110 847 306")],
                                       join_lost=True, drop_damaged=True)
    assert _texts(out) == ["162.110.847.306"], _texts(out)


def test_a_detached_sign_is_not_a_damaged_run():
    """`'- 123'` strips to ONE group, so there is nothing to join and nothing to drop.

    The sign is dropped and read from the box, exactly as without the flag — a box with a
    single numeric group must survive, or every negative figure on a scan disappears.
    """
    out = PdfParser._split_number_runs([_box("- 123")], join_lost=True, drop_damaged=True)
    assert _texts(out) == ["-", "123"], _texts(out)


def test_two_period_figures_boxed_together_are_untouched_by_the_flag():
    """ACB Q1-2025's shape joins well-formed, so `join_lost` claims it first, flag or not.

    ⚠️ That join is WRONG on this box — it is two period figures — and it is accepted as a
    risk confined to the late layers where `sane` judges the magnitude (`_split_number_runs`
    says so in as many words). What this pins is that `drop_damaged` does not change it: the
    flag's whole population is the runs `join_lost` DECLINED.
    """
    box = _box("135.272.610 126.501.216")
    with_flag = PdfParser._split_number_runs([box], join_lost=True, drop_damaged=True)
    without = PdfParser._split_number_runs([box], join_lost=True)
    assert _texts(with_flag) == _texts(without), (_texts(with_flag), _texts(without))


def test_the_predicate_is_join_losts_own_so_a_repairable_run_survives_the_flag_alone():
    """⚠️ **THE FLAG IS SAFE WITHOUT `join_lost` BECAUSE IT ASKS THE SAME QUESTION** — and this
    test asserted the opposite first, which is why it is here.

    The drop predicate is `MERGE_JOIN_RE` over the same regrouped parts `join_lost` joins, so a
    run that CAN be read is never dropped whatever the other flag says: with `join_lost` off it
    is apportioned exactly as today, and only a run no grouping can form into a figure is
    dropped. That symmetry is what makes the pair a convention of the cascade rather than a
    dependency of the code.
    """
    repairable = PdfParser._split_number_runs([_box("162 110 847 306")], drop_damaged=True)
    assert _texts(repairable) == ["162", "110", "847", "306"], _texts(repairable)
    damaged = PdfParser._split_number_runs([_box("- 9 22 566 554")], drop_damaged=True)
    assert damaged == [], _texts(damaged)
