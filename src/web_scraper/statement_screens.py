"""The arithmetic screens - what stands in for `sane` when `sane` could not run.

⚠️ **THIS IS `P47`(b), AND IT IS HERE BECAUSE THE SCRIPT WAS WRITTEN AD HOC FOUR TIMES.**
TCB (2026-08-29), CTG (2026-08-30), FPT (2026-09-04) and FPT again (2026-09-04) each had a
whole-ticker run merged with `force_empty_band=True`, which lifts the magnitude guard - and on
three of those four the screens stood between wrong figures and a CSV: 9 of TCB's 169 accepted
cells, 43 of CTG's 201, 5 of FPT's 128. They cost seconds, read no PDF, open no network and
need no OCR, and a rule that is retyped each time is a rule that drifts.

⚠️ **THEY ARE NOT `sane` AND DO NOT REPLACE IT.** `sane` compares a candidate against the
magnitudes a run has ALREADY ACCEPTED, per report and per entity; these are identities the
filing itself asserts, plus one continuity test. A statement can pass every one of them and
still be the wrong column of the right page - `PYR-1` is exactly that, and needs a check
across quarters that only `P43`/`P59` carry. Use them to decide what NOT to merge, never as
evidence a figure is right.

⚠️ **AND THE `unit` SCREEN IS DELIBERATELY ABSENT.** Taking the MINORITY `unit` of a report
as the suspect convicted 8 TCB statements correctly and then flagged 32 CTG statements that
were all CORRECT - those interim filings genuinely print dong. `accepted.values` are ALREADY
scaled, so the declared unit is a fact about the FILING and never evidence about the figure.
What convicts is the MAGNITUDE, which is what `continuity` measures.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from web_scraper.cafef_financials import FinancialsBuilder

# ⚠️ A RELATIVE tolerance, and looser than `_equal`'s 1e-5 on purpose. These identities are
# read off DE-CUMULATED and OCR'd figures and a filing rounds its own subtotals; the errors
# this exists to catch are 5 % to 6 orders of magnitude, never 1e-4. A tight bound here would
# flag sound statements and the register would stop being read.
REL_TOL = 5e-3
# ⚠️ Measured, not chosen: across the seven parsed tickers no genuine quarter-on-quarter
# move in total assets exceeds 1.7x - CTG's 193.6 tn -> 2,924.2 tn spans 17 years without one.
# A jump past this is a mis-read magnitude or a comparative column, both of which are wrong.
MAX_STEP = 1.7

# ⚠️ **THE SECTION SUBTOTALS OF THE `corp` BALANCE SHEET - `{label: (part, part, total)}`.**
# The VAS form's own arithmetic, and the only identity a corp balance sheet has that is not
# true by construction. `bank` carries none of these columns, so the check simply does not fire
# there and this needs no template argument.
# `label -> (parts, optional, total)`. ⚠️ **THE OPTIONAL TERMS ARE THE SAME ONES `reconcile`
# TRIES BOTH WAYS (`CDF-3`), AND LEAVING THEM OUT MAKES THE SCREEN FLAG EVERY SOUND PRE-2015
# CORPORATE BALANCE SHEET.** Under Decision 15/2006 the NGUỒN VỐN side has FOUR top-level lines
# — `Nguồn kinh phí và quỹ khác` and `Lợi ích của cổ đông thiểu số` sit OUTSIDE `D. VỐN CHỦ SỞ
# HỮU`, where Circular 200 folds them in — so `C + D` falls short by exactly their sum.
# Measured on FPT Q1-2009: gap 624,624,135,544 against 9,622,225,302 + 615,001,910,241.
# ⚠️ Tried BOTH ways for the same reason `reconcile` does: on a modern form they are already
# inside `D` and adding them would double-count. A screen that cries wolf on a whole era of
# filings is a screen nobody reads.
SECTION_SUMS = {
    "assets A+B": (("a_tai_san_ngan_han", "b_tai_san_dai_han"), (), "tong_cong_tai_san"),
    "sources C+D": (("c_no_phai_tra", "d_von_chu_so_huu"),
                    ("ii_nguon_kinh_phi_va_quy_khac_430",
                     "i_11_loi_ich_co_dong_khong_kiem_soat"),
                    "tong_cong_nguon_von"),
}


def _q(period: str) -> Tuple[int, int]:
    q, y = period.split("-")
    return int(y), int(q[1])


def _first(values: Dict[str, int], columns: Sequence[str]) -> Optional[int]:
    for c in columns:
        v = values.get(c)
        if v is not None:
            return v
    return None


def _close(a: Optional[int], b: Optional[int], rel: float = REL_TOL) -> bool:
    return a is not None and b is not None \
        and abs(a - b) <= rel * max(abs(a), abs(b), 1)


def screen_document(doc: dict, builder: FinancialsBuilder) -> Dict[str, List[str]]:
    """`{report: [why it is suspect]}` for ONE document JSON of a run folder.

    Only statements the run ACCEPTED are screened - an absent one has no figures to judge.
    """
    out: Dict[str, List[str]] = {}
    for report, acc in (doc.get("accepted") or {}).items():
        values = acc.get("values") or {}
        why: List[str] = []
        if report == "balance_sheet":
            a = _first(values, builder.C_ASSETS)
            r = _first(values, builder.C_RESOURCES)
            liab = _first(values, builder.C_LIABILITIES)
            eq = _first(values, builder.C_EQUITY)
            # ⚠️ On `corp` this is the TRIVIAL identity and passes by construction on any
            # page that reads both totals (`CRP-1`); it is kept because on `bank` it is not.
            if a is not None and r is not None and not _close(a, r):
                why.append("assets {:,} != resources {:,}".format(a, r))
            if a is not None and liab is not None and eq is not None \
                    and not _close(a, liab + eq):
                why.append("assets {:,} != liabilities + equity {:,} (gap {:,})"
                           .format(a, liab + eq, a - liab - eq))
            # ⚠️ **THE SECTION SUMS ARE THE ONLY REAL CHECK A `corp` BALANCE SHEET HAS, and
            # leaving them out is what let FPT Q3-2022 reach disk.** `C_LIABILITIES` does not
            # map on that chart (`CRP-1`), so `A != L + E` cannot run and `assets ==
            # resources` is true on any page that reads both totals. The defect that convicted
            # that quarter was **A + B = 35,467,952,822,566 against a printed
            # 55,127,101,516,155** - `b_tai_san_dai_han` reading 198,477,998,944 for a company
            # holding 55 tn - and the screen as first written could not see it. Both sides are
            # checked, because the sources side fails the same way.
            for label, (parts, optional, total) in SECTION_SUMS.items():
                got = [values.get(c) for c in parts]
                whole = values.get(total)
                if any(v is None for v in got) or whole is None:
                    continue
                extra = sum(v for v in (values.get(c) for c in optional) if v is not None)
                if _close(sum(got), whole) or (extra and _close(sum(got) + extra, whole)):
                    continue
                why.append("{} {:,} + {:,} = {:,} != {:,} (gap {:,})"
                           .format(label, got[0], got[1], sum(got), whole,
                                   sum(got) - whole))
        elif report == "cash_flow":
            close = _first(values, builder.C_CASH_CLOSE)
            opening = _first(values, builder.C_CASH_OPEN)
            net = _first(values, builder.C_NET_CF)
            fx = _first(values, builder.C_CASH_FX) or 0
            for name, v in (("closing", close), ("opening", opening)):
                if v is not None and v < 0:
                    why.append("NEGATIVE {} cash {:,}".format(name, v))
            if close is not None and opening is not None and net is not None \
                    and not _close(opening + net + fx, close):
                why.append("opening + net + fx {:,} != closing {:,}"
                           .format(opening + net + fx, close))
        if why:
            out[report] = why
    return out


def screen_run(folders: Iterable[os.PathLike | str],
               builder: Optional[FinancialsBuilder] = None
               ) -> Dict[Tuple[str, str], List[str]]:
    """`{(period, report): [why]}` over a batch's run folders - the whole screen.

    ⚠️ **CONTINUITY IS MEASURED ACROSS THE BATCH AND NOT WITHIN A DOCUMENT**, which is why
    this cannot be `screen_document` in a loop: a magnitude that is wrong by 10^6 reconciles
    perfectly against itself and is only visible beside its neighbours.
    """
    builder = builder or FinancialsBuilder(logger=None)
    flagged: Dict[Tuple[str, str], List[str]] = {}
    assets: Dict[str, int] = {}
    for folder in folders:
        docs = sorted(Path(folder).glob("documents/*.json"))
        for path in docs:
            doc = json.loads(path.read_text(encoding="utf-8"))
            period = doc.get("period") or ""
            for report, why in screen_document(doc, builder).items():
                flagged.setdefault((period, report), []).extend(why)
            bs = (doc.get("accepted") or {}).get("balance_sheet") or {}
            total = _first(bs.get("values") or {}, builder.C_ASSETS)
            if total:
                assets[period] = total
    order = sorted(assets, key=_q)
    for before, after in zip(order, order[1:]):
        # ⚠️ **PER QUARTER, NOT PER PAIR — two periods a YEAR apart legitimately move
        # further.** A batch parses the quarters that were OUTSTANDING, so consecutive HERE is
        # not consecutive on the calendar: FPT's run held Q2-2009 and then Q2-2010, four
        # quarters later, and the honest 1.79x between them was flagged while every neighbour
        # of the pair (x1.05, x1.07) confirmed both figures. Comparing the implied per-quarter
        # rate keeps the bound meaningful at any gap and still catches the errors this exists
        # for: a 10^3 magnitude slip reads 5.6x per quarter even spread over a year.
        gap = max(1, (_q(after)[0] - _q(before)[0]) * 4 + _q(after)[1] - _q(before)[1])
        ratio = assets[after] / assets[before]
        rate = ratio ** (1.0 / gap)
        if rate > MAX_STEP or rate < 1 / MAX_STEP:
            flagged.setdefault((after, "balance_sheet"), []).append(
                "total assets {:,} -> {:,} ({:.2f}x over {} quarter(s) from {}, "
                "{:.2f}x per quarter)"
                .format(assets[before], assets[after], ratio, gap, before, rate))
    return flagged


def report(flagged: Dict[Tuple[str, str], List[str]],
           log=print) -> None:
    """One line per suspect statement, in period order."""
    for (period, rep), why in sorted(flagged.items(), key=lambda kv: (_q(kv[0][0]), kv[0][1])):
        log("  {:8s} {:18s} {}".format(period, rep, " ; ".join(why)))
    log("  {} statement(s) flagged".format(len(flagged)))
