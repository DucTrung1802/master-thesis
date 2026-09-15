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

import csv
import json
import re
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
# ⚠️ **AND `A != L + E` NEEDS THE SAME TERMS, CHART BY CHART** (2026-09-13). The identity below summed
# only `C_LIABILITIES` and `C_EQUITY`, so a Decision-15 sheet printing `II. Nguồn kinh phí và quỹ khác`
# and `C. LỢI ÍCH CỦA CỔ ĐÔNG THIỂU SỐ` beside `D. VỐN CHỦ SỞ HỮU` was held with both grand totals equal
# to the đồng: BVH Q1-2009 `assets 28,114,653,581,831 != liabilities + equity 26,839,644,337,063`, the
# gap exactly 35,760,473,126 + 1,239,248,771,642, and Q4-2008 exactly 36,500,034,959 + 489,359,121,900.
# Tried in every subset (`_section_candidates`) for the reason `SECTION_SUMS` gives below: a
# Circular-200 sheet folds them into equity, and adding them there would double-count.
RESOURCE_EXTRAS = (
    "ii_nguon_kinh_phi_va_quy_khac_430", "i_11_loi_ich_co_dong_khong_kiem_soat",   # corp
    "ii_nguon_kinh_phi_va_quy_khac", "c_loi_ich_co_dong_thieu_so",                 # insurance, securities
    "ix_loi_ich_cua_co_dong_thieu_so",                                             # bank
)

SECTION_SUMS = {
    # ⚠️ `LNS-2`: the assets side's optional lines are `reconcile`'s own (`LNS-1`, `GWL-1`).
    "assets A+B": (("a_tai_san_ngan_han", "b_tai_san_dai_han"),
                   ("cho_vay_va_ung_truoc_cho_khach_hang", "loi_the_thuong_mai_269"), "tong_cong_tai_san"),
    # ⚠️ The optional terms were the CORP chart's names only, so an insurance or securities sheet
    # printing the same two lines under its own names (BVH, above `RESOURCE_EXTRAS`) failed here too.
    "sources C+D": (("c_no_phai_tra", "d_von_chu_so_huu"), RESOURCE_EXTRAS, "tong_cong_nguon_von"),
}


# ⚠️ **`EQS-1` — A SECTION TOTAL SMALLER THAN ONE OF ITS OWN SUB-SECTIONS** (2026-09-14). BVH's
# insurance chart prints `D. VỐN CHỦ SỞ HỮU` over `I. Vốn chủ sở hữu`, and the reading can hand the
# section's column the next line down: Q3-2010 wrote `d_von_chu_so_huu` 6,267,090,790,000 beside
# `i_von_chu_so_huu` 10,524,257,835,935, Q3-2012 6,804,714,340,000 beside 11,770,043,689,217 — the
# charter capital in the total's place, under grand totals that reconcile. `(whole, part)` pairs;
# over the 1,737 `pdf` balance sheets on disk the check fires on exactly those two.
SECTION_PARTS = (("d_von_chu_so_huu", "i_von_chu_so_huu"),)


# ⚠️ **`HLI-1` — A READING CONVICTED BY INSPECTION, WHICH NO ARITHMETIC ON THE PAGE CAN CONVICT**
# (2026-09-14). A run folder is immutable and the release writes whatever it holds that passes the
# screens (`GTT-3`), so a reading known to be wrong and invisible to every identity would be
# re-written by the next sweep after a roll-back. BVH Q2-2011's balance sheet put the charter
# capital 6,804,714,340,000 in `d_von_chu_so_huu` with `c_no_phai_tra` unmapped, so neither
# `A = L + E` nor `EQS-1` can run on it. One row per reading, naming the run folder, with its reason.
HELD_READINGS_PATH = (Path(__file__).resolve().parents[2] / "raw_data" / "cafef" / "financials"
                      / "held_readings.csv")


def held_readings(path: Optional[os.PathLike | str] = None) -> Dict[Tuple[str, str, str], str]:
    """`{(run folder, period, report): reason}` from the held-readings register — `HLI-1`."""
    path = Path(path) if path is not None else HELD_READINGS_PATH
    if not path.exists():
        return {}
    with open(path, encoding="utf-8", newline="") as f:
        return {(r["folder"], r["period"], r["report"]): r["reason"] for r in csv.DictReader(f)}


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


# `CXT-1`: how many times the rounding bound a closing cash balance must be before a positional
# span may close the identity — below it the bound, not the figures, decides.
SPAN_MIN_RATIO = 10_000


def _span_closes(acc: dict, opening: int, net: int, close: int) -> bool:
    """Whether the reading's own rows BETWEEN its two cash balances close the identity — `CXT-1`.

    ⚠️ **`reconcile` ACCEPTS A CASH FLOW ON `opening + movement + <every line printed between the
    two balances>`, AND THIS SCREEN ASKED ONLY `opening + movement + fx`.** The fourth term is
    counted and never written (`cash_extra_terms`, `_extra_cash_terms`), so the accepted `values`
    cannot show it, and a sound statement that needed it was held by the release as failing an
    identity. Measured 2026-09-13 on VJC, whose consolidated cash flow prints `Chênh lệch quy đổi
    ngoại tệ các hoạt động ở nước ngoài` beside the FX line: **5 of 6 held cash flows had a gap
    EXACTLY equal to that line** — Q4-2015 526,748,546,327 + 391,119,906,214 - 4,474,463,459
    + 10,118,928,613 = 923,512,917,695 to the đồng — and the sixth (Q1-2021) closes on the span
    because its FX line was read and not mapped.

    ⚠️ **THE SAME POSITIONAL DEFINITION AS `_extra_cash_terms`, AND THE SAME TIGHT BOUND.** The
    span is read off the stored `row_dump` (current-period cell only, already scaled), anchored on
    the rows whose first figure IS the accepted opening and closing balance, and it must close to
    the filing's own rounding — `unit` per figure summed, `OP_IDENTITY_TOL`'s precedent — and not
    to this module's `REL_TOL` or even `_equal`'s 1e-5, because a span can sweep in any row and a
    loose match over a free sum is arithmetic, not evidence. A reading with no row dump, or whose
    balances cannot be found in it, is judged exactly as before.

    ⚠️ **AND THE BOUND MUST BE NEGLIGIBLE BESIDE THE FIGURES, OR SINGLE-DIGIT CODES CLOSE IT BY
    ACCIDENT.** The first version (on `_equal`, whose floor is 2) released PLX Q1-2013, a reading
    of `Mã số` codes — opening `4`, movement `3`, closing `8` — because a few small integers
    between two small integers sum to anything within two. `SPAN_MIN_RATIO`: the closing balance
    must be that many times the tolerance, which no cash balance of a listed company fails.
    """
    firsts = []
    for row in acc.get("row_dump") or []:
        cells = row[3] if len(row) > 3 and isinstance(row[3], list) else []
        first = cells[0] if cells else None
        firsts.append(int(first) if isinstance(first, (int, float)) else None)
    open_i = next((i for i, v in enumerate(firsts) if v == opening), None)
    if open_i is None:
        return False
    close_i = next((i for i in range(len(firsts) - 1, open_i, -1) if firsts[i] == close), None)
    if close_i is None:
        return False
    span = [v for v in firsts[open_i + 1:close_i] if v is not None]
    tolerance = int(acc.get("unit") or 1) * (len(span) + 3)
    if abs(close) < SPAN_MIN_RATIO * tolerance:
        return False
    return abs(opening + net + sum(span) - close) <= tolerance


# ⚠️ **`CFS-1` — A CASH FLOW WHOSE THREE SECTIONS DO NOT ADD UP TO THE NET PRINTED BENEATH THEM**
# (2026-09-14). Every chart prints `50 = 20 + 30 + 40` (the bank's `IV = I + II + III`) and neither
# `reconcile` nor this module ever asked it: the cash identity runs `opening + net + fx = closing`, which
# a reading passes with any section misread. Over the 554 VN30 `pdf` cash flows on disk that map all
# four lines, **68 fail** — HPG Q1-2015's operating flow reads +500,012,110,163 where the net needs
# -500,012,110,163 (the gap is exactly twice it), FPT Q2-2016 is 500,000,000,000 off, GAS Q3-2019
# 900,000,000,000, and BCM Q1-2018 was written from a GPU run with the sections 365,966,829,013 short.
# Abstains unless exactly one line per section and the net are mapped; the filing's rounding is a
# `unit` per figure.
FLOW_SECTION_PREFIXES = ("hdkd_", "hddt_", "hdtc_")
FLOW_UNIT_SLACK = 3


def flow_sections_gap(values: Dict[str, int], builder: FinancialsBuilder,
                      unit: int = 1) -> Optional[Tuple[int, int]]:
    """`(operating + investing + financing, net)` when all four are mapped and disagree — `CFS-1`."""
    net = _first(values, builder.C_NET_CF)
    if net is None:
        return None
    sections = []
    for prefix in FLOW_SECTION_PREFIXES:
        found = [values[c] for c in builder.C_FLOW_SECTIONS
                 if c.startswith(prefix) and values.get(c) is not None]
        if len(found) != 1:
            return None
        sections.append(found[0])
    total = sum(sections)
    if _close(total, net) or abs(total - net) <= FLOW_UNIT_SLACK * max(1, int(unit or 1)):
        return None
    return total, net


# ⚠️ **`ISR-1` — FIGURES NO FILING OF A LISTED COMPANY CAN PRINT** (2026-09-14). The largest balance
# sheet in the corpus is BID's ~2.7e15 đồng, so a line of 1e16 or more is a glued or mis-scaled
# reading and never a figure: FPT's four 2025 income statements carried `12_thu_nhap_khac`
# -2,993,843,310,849,432,064 on disk. Per-share lines are exempt only because they are not money.
ABSURD_MAGNITUDE = 10 ** 16
# Gross income lines — a printed one is never negative, and a standalone quarter subtracted out
# of a year-to-date one is not either. `5_thu_nhap_tu_hoat_dong_khac` is judged on a PRINTED
# statement only: a de-cumulated quarter of "other income" can go negative when the year-to-date
# figure is restated, and one sound cell held is the wrong trade for a line that rare.
GROSS_INCOME = ("1_doanh_thu_ban_hang_va_cung_cap_dich_vu",
                "3_doanh_thu_thuan_ve_ban_hang_va_cung_cap_dich_vu",
                "1_thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu",
                "3_thu_nhap_tu_hoat_dong_dich_vu")
GROSS_INCOME_PRINTED = GROSS_INCOME + ("5_thu_nhap_tu_hoat_dong_khac",)
# `(whole, part)`: revenue before deductions, net revenue, gross profit — each at least the next.
REVENUE_ORDER = (("1_doanh_thu_ban_hang_va_cung_cap_dich_vu",
                  "3_doanh_thu_thuan_ve_ban_hang_va_cung_cap_dich_vu"),
                 ("3_doanh_thu_thuan_ve_ban_hang_va_cung_cap_dich_vu",
                  "5_loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu"))


def absurd_figures(values: Dict[str, int]) -> List[str]:
    """The columns holding a figure of `ABSURD_MAGNITUDE` or more — `ISR-1`."""
    return sorted(k for k, x in values.items()
                  if isinstance(x, (int, float)) and abs(x) >= ABSURD_MAGNITUDE and "co_phieu" not in k)


# ⚠️ **`TNY-1` — ROW CODES AND FIGURE TAILS WRITTEN AS LINE ITEMS** (screen added 2026-09-15). A statement
# is printed in đồng or triệu đồng and stored in đồng, so a line item under 1,000 on a statement whose figures
# reach 1e9 is a form row code, a note reference or the last group of a figure whose leading groups were lost —
# never an amount. SSI Q3-2014's cash flow was accepted at `onnx@200` and written with its operating, investing
# and financing nets reading `20`, `30`, `40` — the `Mã số` column — because the two cash balances and the net
# movement closed on their own; over the VN30 `pdf` rows on disk 44 carry three or more such lines (GVR Q4-2023's
# balance sheet 32, VNM Q1-2016's 26, VIC Q4-2009's income statement revenue `271`). Per-share lines are not money.
TINY_FIGURE = 1_000
TINY_MIN_COUNT = 3
TINY_SCALE_MIN = 10 ** 9


def tiny_figures(values: Dict[str, int], min_count: int = TINY_MIN_COUNT) -> List[str]:
    """The line items under `TINY_FIGURE` đồng when there are `min_count` of them on a statement whose
    figures reach `TINY_SCALE_MIN` — `TNY-1`. Three or more convict the READING (the screen holds it);
    the merge drops even one, because a single such cell is still not an amount (MBB Q3-2014's closing
    cash `725`, VIC Q2-2011's profit after tax `142`, PLX Q1-2015's inventory `581`)."""
    money = {k: x for k, x in values.items()
             if isinstance(x, (int, float)) and x and "co_phieu" not in k}
    if not money or max(abs(x) for x in money.values()) < TINY_SCALE_MIN:
        return []
    tiny = sorted(k for k, x in money.items() if abs(x) < TINY_FIGURE)
    return tiny if len(tiny) >= min_count else []


# ⚠️ **`PBC-1` — PROFIT BEFORE TAX MAPPED INTO ANOTHER LINE** (2026-09-15). POW Q3-2019 was accepted at
# `onnx@200+tail` with `16_chi_phi_thue_tndn_hien_hanh` = 874,740,417,770 — exactly operating profit
# 868,882,824,378 plus other profit 5,857,593,392 — and no profit-before-tax or after-tax column: `reconcile`
# found the PBT row by its text, the map gave its figure to the tax line, and the reading was written. A corp
# chart's PBT is `11 + 14` by the form itself, so any OTHER column holding that sum to the unit is misplaced.
OPERATING_PROFIT = "11_loi_nhuan_thuan_tu_hoat_dong_kinh_doanh"
OTHER_PROFIT = "14_loi_nhuan_khac"
PROFIT_BEFORE_TAX = "15_tong_loi_nhuan_ke_toan_truoc_thue"
PBT_CARRIER_MIN = 10 ** 9


def pbt_carriers(values: Dict[str, int], unit: int = 1) -> List[str]:
    """Columns other than profit before tax holding operating profit + other profit — `PBC-1`."""
    op, other = values.get(OPERATING_PROFIT), values.get(OTHER_PROFIT)
    if op is None or other is None or abs(op + other) < PBT_CARRIER_MIN:
        return []
    tol = 3 * max(1, int(unit or 1))
    return sorted(c for c, x in values.items()
                  if c not in (PROFIT_BEFORE_TAX, OPERATING_PROFIT) and isinstance(x, (int, float))
                  and abs(x - (op + other)) <= tol)


def income_statement_screens(values: Dict[str, int], builder: FinancialsBuilder,
                             unit: int = 1, derived: bool = False) -> List[str]:
    """Why an income statement's figures cannot all be right — `ISR-1` / `DCS-1`.

    ⚠️ **A STORED READING IS RE-JUDGED BY WHAT THE PARSER HAS LEARNED SINCE, OR THE RELEASE WRITES
    IT** — `GTT-3`'s lesson on the third statement. Run folders are immutable, and a reading taken
    before `BIS-1` was never asked whether its net lines close: MBB Q3-2017's 2026-09-08 reading,
    one row up, holds `i_thu_nhap_lai_thuan` -2,262,098 m (the interest EXPENSE) and service income
    2,834,971 m (the net interest income), and GVR Q1-2023's holds net revenue 167,499 đồng beside a
    gross profit of 1,005,877,717,111 — held by nothing, and marked writable by the census.

    ⚠️ **AND A DE-CUMULATED QUARTER IS JUDGED TOO (`derived`), BECAUSE ITS ERRORS ARE THE PRIORS'.**
    `Q4 = FY - (Q1+Q2+Q3)` is linear, so a quarter whose net lines fail or whose gross income is
    negative proves an operand on disk is wrong even when the year-to-date reading is sound: MBB
    Q4-2017 came out at service income -1,182,763 m from a sound FY-2017 minus that Q3 reading.
    Its rounding is up to one `unit` per document, which the caller folds into `unit`.

    Only inequalities no correct statement breaks and the `BIS-1` identity; each abstains when a
    term is unmapped, so a sparse reading is judged on what it has.
    """
    why: List[str] = []
    lines = GROSS_INCOME if derived else GROSS_INCOME_PRINTED
    negative = [c for c in lines if values.get(c) is not None and values[c] < 0]
    if negative:
        why.append("NEGATIVE gross income: " + ", ".join(
            "{} {:,}".format(c, values[c]) for c in negative))
    pairs = REVENUE_ORDER + tuple((income, net) for net, (income, _) in builder.BANK_NET_LINES.items())
    for whole, part in pairs:
        big, small = values.get(whole), values.get(part)
        if (big is not None and small is not None and big > 0 and small > 0
                and big < small and not _close(big, small)):
            why.append("{} {:,} is below {} {:,}, which is a part of it".format(whole, big, part, small))
    net_lines = builder._bank_net_lines(values, unit)
    if net_lines:
        why.append(net_lines)
    carriers = pbt_carriers(values, unit)
    if carriers:
        why.append("profit before tax ({:,}) sits in {}".format(
            values[OPERATING_PROFIT] + values[OTHER_PROFIT], ", ".join(carriers)))
    absurd = absurd_figures(values)
    if absurd:
        why.append("a figure no listed company prints: " + ", ".join(
            "{} {:,}".format(c, values[c]) for c in absurd))
    return why


def suspect_columns(values: Dict[str, int], builder: FinancialsBuilder, unit: int = 1) -> set:
    """Every column a failing `income_statement_screens` check names — `DCS-2`.

    ⚠️ **A WRONG PRIOR CORRUPTS ITS DEPENDENT COLUMN BY COLUMN, AND THE QUARTER'S OWN SCREENS CANNOT
    SEE IT.** MBB Q1-2017 on disk reads interest income 1,455,144 m beside a net interest income of
    2,406,612 m, so `Q4-2017 = FY - (Q1+Q2+Q3)` wrote interest income 8,519,667 m for a quarter whose
    neighbours print 5.1-5.6 tn — and every check on the quarter passed, because the expense line
    that could have contradicted it had already been dropped. `_subtract_priors` rule 4's answer,
    one statement over: a broken identity cannot say which of its terms is misread, so all go.
    """
    out = set()
    out.update(c for c in GROSS_INCOME_PRINTED if values.get(c) is not None and values[c] < 0)
    pairs = REVENUE_ORDER + tuple((income, net) for net, (income, _) in builder.BANK_NET_LINES.items())
    for whole, part in pairs:
        big, small = values.get(whole), values.get(part)
        if (big is not None and small is not None and big > 0 and small > 0
                and big < small and not _close(big, small)):
            out.update((whole, part))
    tol = builder.TOTALS_TOL * max(1, int(unit or 1))
    for net, (income, expense) in builder.BANK_NET_LINES.items():
        if any(values.get(c) is None for c in (net, income, expense)):
            continue
        as_stored = values[income] + values[expense]
        as_expense = values[income] - abs(values[expense])
        if min(abs(as_stored - values[net]), abs(as_expense - values[net])) > tol:
            out.update((net, income, expense))
    out.update(absurd_figures(values))
    return out


# ⚠️ **`DED-1` — THE REVENUE-DEDUCTIONS LINE HOLDING REVENUE ITSELF** (2026-09-14). VRE, VHM and POW print
# no `Các khoản giảm trừ doanh thu` figure, and the reading hands that account the revenue line under it:
# VRE Q3-2021 on disk reads gross revenue AND deductions 787,355,000,000, while its gross profit
# (130,349,000,000) plus cost of sales (657,006,000,000) is that same figure — net revenue, so the
# deductions are nil. Over the 775 corp `pdf` income statements on disk, 15 carry deductions equal to
# gross revenue and one (POW Q1-2020) equal to net revenue, every figure over a billion. An equality to
# the đồng is the evidence; a smaller deduction, or the item codes VIC 2009-2012 print (`271`), is not
# judged here.
REVENUE_GROSS = "1_doanh_thu_ban_hang_va_cung_cap_dich_vu"
REVENUE_DEDUCTIONS = "2_cac_khoan_giam_tru_doanh_thu"
REVENUE_NET = "3_doanh_thu_thuan_ve_ban_hang_va_cung_cap_dich_vu"
DEDUCTION_MIN = 10 ** 9


COST_OF_SALES = "4_gia_von_hang_ban"
GROSS_PROFIT = "5_loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu"


def deduction_fix(values: Dict[str, int], unit: int = 1) -> Tuple[Dict[str, str], List[str]]:
    """`({deductions: net revenue}, [])` when the statement's own `5 = 3 - 4` proves the deductions
    figure IS net revenue and net revenue is empty; `({}, [deductions])` when it equals gross or net
    revenue to the đồng and no identity can say more — `DED-1`. The figure moves or goes; nothing is
    computed."""
    d = values.get(REVENUE_DEDUCTIONS)
    if d is None or abs(d) < DEDUCTION_MIN:
        return {}, []
    gp, cogs = values.get(GROSS_PROFIT), values.get(COST_OF_SALES)
    if (values.get(REVENUE_NET) is None and gp is not None and cogs is not None
            and abs(abs(d) - (gp + abs(cogs))) <= 3 * max(1, int(unit or 1))):
        return {REVENUE_DEDUCTIONS: REVENUE_NET}, []
    if any(values.get(c) is not None and abs(values[c]) == abs(d) for c in (REVENUE_GROSS, REVENUE_NET)):
        return {}, [REVENUE_DEDUCTIONS]
    return {}, []


def deduction_carriers(values: Dict[str, int]) -> List[str]:
    """The deductions column when `deduction_fix` would move or drop it — `DED-1`."""
    moves, drops = deduction_fix(values)
    return sorted(set(moves) | set(drops))


def _dump_first(acc: dict, needles: Sequence[str]) -> Optional[int]:
    """The current-period figure of the first STORED row whose key contains one of `needles` — `LNS-2`.

    ⚠️ **`reconcile` FINDS AN UNMAPPED SECTION LINE BY ITS TEXT, AND THE RELEASE SCREEN COULD NOT.** BVH
    Q2-2013's balance sheet was accepted because A + B + the banking subsidiary's loans, printed outside
    both sections and mapped to no column, close its total to the đồng (18,476,896,092,843 +
    22,610,961,839,993 + 7,831,864,053,459); the screen summed only the mapped `values`, so it held a sheet
    that is right. And it passed one that is wrong: BVH Q3-2013 carries the share premium
    3,184,332,381,197 as `d_von_chu_so_huu` with `c_no_phai_tra` unmapped, so `C + D` abstained. The
    stored row dump holds both lines, so the screen asks it, by containment of the same text `reconcile`
    uses — stricter than `Statement.find`'s fuzzy match, because a screen that finds the wrong row
    convicts a right sheet.
    """
    slugs = [n.replace(" ", "_") for n in needles]
    if not slugs:
        return None
    for row in acc.get("row_dump") or []:
        if len(row) < 4 or not isinstance(row[3], list):
            continue
        if any(slug in str(row[1] or "") for slug in slugs):
            first = next((x for x in row[3] if isinstance(x, (int, float))), None)
            return None if first is None else int(first)
    return None


# ⚠️ **`EQS-3` — THE EQUITY SECTION TAKEN FROM A LATER `VỐN CHỦ SỞ HỮU` LINE, WITH ITS SUB-SECTION UNMAPPED**
# (2026-09-14). `EQS-1` needs `i_von_chu_so_huu` mapped, and BVH's forms print three lines worded `vốn chủ sở
# hữu` — the section, its first sub-section and the charter capital — so a reading can put the capital or the
# share premium in `d_von_chu_so_huu` and map neither of the others: BVH Q3-2013 carries 3,184,332,381,197
# where the first such line prints 11,835,131,000,702, and a GPU run wrote Q2-2011 and Q3-2012 with the
# capital 6,804,714,340,000 there. Over the 1,749 stored balance sheets the section below its first `vốn chủ
# sở hữu` line and equal to a later one occurs 34 times; below it by more than `EQUITY_LINE_SHORT` only on
# BVH's capital and premium readings, GVR's 40 tn charter capital, FPT Q4-2008 and MCH's item codes — the
# rest are the section read as its sub-section, a gap of 0.1-2 % that is the funds line.
EQUITY_LINE_SHORT = 0.10


def equity_section_from_a_later_line(values: Dict[str, int], acc: dict) -> Optional[str]:
    """Why `d_von_chu_so_huu` is a later equity line and not the section — `EQS-3` — or None."""
    d = values.get("d_von_chu_so_huu")
    if not d or d <= 0:
        return None
    lines = []
    for row in acc.get("row_dump") or []:
        key = str(row[1] or "") if len(row) > 1 else ""
        if (len(row) < 4 or not isinstance(row[3], list) or "von_chu_so_huu" not in key
                or "no_phai_tra" in key or "tong" in key):
            continue
        first = next((x for x in row[3] if isinstance(x, (int, float))), None)
        if first is not None:
            lines.append(int(first))
    if len(lines) < 2 or d not in lines[1:] or d >= (1 - EQUITY_LINE_SHORT) * lines[0]:
        return None
    return ("d_von_chu_so_huu {:,} is a later equity line, below the section's own {:,} (`EQS-3`)"
            .format(d, lines[0]))


# ⚠️ **`LES-1` — A BANK SHEET'S TWO SUBTOTALS MISSED ITS OWN GRAND TOTAL BY ONE MISREAD DIGIT** (2026-09-15).
# The bank chart prints `TỔNG NỢ PHẢI TRẢ` + `VIII. VỐN CHỦ SỞ HỮU` (+ `IX. LỢI ÍCH CỦA CỔ ĐÔNG THIỂU SỐ`) =
# `TỔNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU` to the unit, and no term sits outside the three. `reconcile` closes the
# two grand totals and the release screen asks `A = L + E` within `REL_TOL` (0.5 %), which lets a single
# misread digit through: over the 1,656 VN30 `pdf` balance sheets on disk, 42 bank rows missed the agreeing
# totals by more than 3 units — MBB Q3-2017 `tong_no_phai_tra` 253,479,200 m for a printed 263,479,200 m
# (exactly 10 tn), HDB Q2-2025 by exactly 100 bn, VPB Q2-2026 by 1 bn, VCB Q1-2018 equity 131.4 tn for 56.1 tn.
# Nothing on the sheet says WHICH subtotal is wrong, so both go and nothing is computed in their place.
# ⚠️ And the minority line is not always MAPPED: HDB Q4-2015 prints it as `Lợi ích cổ đông không kiểm soát`
# (449,264 m) and SHB Q2-2017's reading spells it `lot_ich_cua_co_dong_khong_kiem_soat` (2,384 m), each
# exactly the gap, so an unmapped row of the stored dump closes the identity as the mapped column does.
BANK_SUBTOTALS = ("tong_no_phai_tra", "viii_von_chu_so_huu")
BANK_MINORITY = "ix_loi_ich_cua_co_dong_thieu_so"
BANK_MINORITY_KEY = re.compile(r"lo[ijt]?_?ich.*(thieu_so|khong_kiem_soat)")


def unclosed_bank_subtotals(values: Dict[str, int], unit: int = 1,
                            row_dump: Optional[Sequence] = None) -> List[str]:
    """The two bank-chart subtotals when they miss the agreeing grand totals by more than 3 units — `LES-1`."""
    liab, eq = (values.get(c) for c in BANK_SUBTOTALS)
    a, t = values.get("tong_tai_san"), values.get("tong_no_phai_tra_va_von_chu_so_huu")
    total = t if t is not None else a
    if total is None or total <= 0:
        return []
    # ⚠️ ONE SUBTOTAL ALONE has no identity, and SHB Q1-2011's reading carried `tong_no_phai_tra`
    # 8,012,998,261 under totals of 55,815,813,286,430 with no equity line. A bank funds itself with
    # liabilities: over the VN30 bank sheets on disk liabilities are 80-97 % of the total, so a lone
    # subtotal outside half of it on its own side is the reading's and never the bank's.
    if liab is not None and eq is None:
        return [BANK_SUBTOTALS[0]] if not 0.5 <= liab / total <= 1.0 else []
    if eq is not None and liab is None:
        return [BANK_SUBTOTALS[1]] if not 0.0 < eq / total <= 0.5 else []
    if liab is None or eq is None:
        return []
    tol = 3 * max(1, int(unit or 1))
    if a is not None and t is not None and abs(a - t) > tol:
        return []                     # the totals disagree: a different defect, judged elsewhere
    minorities = {0, values.get(BANK_MINORITY) or 0}
    for r in row_dump or ():
        figures = [v for v in (r[3] if len(r) > 3 else []) if v is not None]
        if figures and BANK_MINORITY_KEY.search(str(r[1])):
            minorities.add(figures[0])
    if any(abs(liab + eq + m - total) <= tol for m in minorities):
        return []
    return list(BANK_SUBTOTALS)


def grand_total_carriers(values: Dict[str, int], builder: FinancialsBuilder) -> List[str]:
    """The balance-sheet columns, other than the two grand totals and the chart's own section-header
    total, that hold the figure total assets holds — `GTT-3`'s test, shared with the merge (`GTT-4`)."""
    a = _first(values, builder.C_ASSETS)
    if not a:
        return []
    exempt = set(builder.C_ASSETS) | set(builder.C_RESOURCES) | set(builder.TWIN_TOTAL_HEADERS)
    return sorted(k for k, x in values.items() if x == a and k not in exempt)


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
            # ⚠️ **`GTT-3` — A STORED READING THE RELEASE WOULD RE-WRITE AFTER THE PARSER LEARNED TO
            # REFUSE IT** (2026-09-14). `GTT-2`'s lock lives in `_twin_totals`, which runs at PARSE
            # time, while a run folder is immutable: the release sweep after the next GPU run re-wrote
            # SHB Q1-2011 and SSI Q1-2016 from `GTT-1`'s stored readings, the grand total still sitting
            # in a line item. So the same test is applied here, on the stored values: no column but the
            # two totals and the chart's own section-header total may equal total assets.
            if a:
                carriers = grand_total_carriers(values, builder)
                if carriers:
                    why.append("a line item holds the grand total {:,}: {}".format(a, ", ".join(carriers)))
            later = equity_section_from_a_later_line(values, acc)
            if later:
                why.append(later)
            for whole_col, part_col in SECTION_PARTS:
                whole, part = values.get(whole_col), values.get(part_col)
                if (whole is not None and part is not None and whole > 0 and part > 0
                        and whole < part and not _close(whole, part)):
                    why.append("{} {:,} is below {} {:,}, a part of it (`EQS-1`)"
                               .format(whole_col, whole, part_col, part))
            # ⚠️ On `corp` this is the TRIVIAL identity and passes by construction on any
            # page that reads both totals (`CRP-1`); it is kept because on `bank` it is not.
            if a is not None and r is not None and not _close(a, r):
                why.append("assets {:,} != resources {:,}".format(a, r))
            if a is not None and liab is not None and eq is not None and not any(
                    _close(a, c) for c in FinancialsBuilder._section_candidates(
                        liab + eq, [values.get(x) for x in RESOURCE_EXTRAS])):
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
                # ⚠️ `LNS-2`: a part or an optional line the mapping missed is read off the row dump by the
                # text `reconcile` itself falls back to.
                # ⚠️ ONLY THE OPTIONAL LINES: a part found by text convicted 129 of 1,749 stored sheets the
                # gate had accepted (`tai_san_ngan_han` is contained in `tai_san_ngan_han_khac`), and an
                # extra candidate can only ever release.
                got = [values.get(c) for c in parts]
                whole = values.get(total)
                if any(v is None for v in got) or whole is None:
                    continue
                extras = [values.get(c) if values.get(c) is not None
                          else _dump_first(acc, builder.SECTION_EXTRA_TEXT.get(c, ())) for c in optional]
                # ⚠️ EVERY SUBSET, not all-or-none — `_section_candidates` is `reconcile`'s own
                # rule, and FPT Q1-2016 is why: its two optional lines sit in different places.
                if any(_close(c, whole) for c in FinancialsBuilder._section_candidates(sum(got), extras)):
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
            if (close is not None and opening is not None and net is not None
                    and not _close(opening + net + fx, close)
                    and not _span_closes(acc, opening, net, close)):
                why.append("opening + net + fx {:,} != closing {:,}"
                           .format(opening + net + fx, close))
            flows = flow_sections_gap(values, builder, int(acc.get("unit") or 1))
            if flows is not None:
                why.append("operating + investing + financing {:,} != net {:,} (gap {:,}, `CFS-1`)"
                           .format(flows[0], flows[1], flows[0] - flows[1]))
        if report == "income_statement":
            why += income_statement_screens(values, builder, unit=int(acc.get("unit") or 1))
        else:
            absurd = absurd_figures(values)
            if absurd:
                why.append("a figure no listed company prints: " + ", ".join(
                    "{} {:,}".format(c, values[c]) for c in absurd))
        tiny = tiny_figures(values)
        if tiny:
            why.append("{} line item(s) under 1,000 đồng on a statement whose figures reach 1e9 — a row code "
                       "or a figure's tail read as the figure (`TNY-1`): {}".format(
                           len(tiny), ", ".join("{} {:,}".format(c, values[c]) for c in tiny[:6])))
        if why:
            out[report] = why
    return out


def screen_run(folders: Iterable[os.PathLike | str],
               builder: Optional[FinancialsBuilder] = None,
               held_path: Optional[os.PathLike | str] = None,
               disk_assets: Optional[Dict[str, int]] = None
               ) -> Dict[Tuple[str, str], List[str]]:
    """`{(period, report): [why]}` over a batch's run folders - the whole screen.

    ⚠️ **CONTINUITY IS MEASURED ACROSS THE BATCH AND NOT WITHIN A DOCUMENT**, which is why
    this cannot be `screen_document` in a loop: a magnitude that is wrong by 10^6 reconciles
    perfectly against itself and is only visible beside its neighbours.
    """
    builder = builder or FinancialsBuilder(logger=None)
    flagged: Dict[Tuple[str, str], List[str]] = {}
    assets: Dict[str, int] = {}
    held = held_readings(held_path)
    for folder in folders:
        docs = sorted(Path(folder).glob("documents/*.json"))
        for path in docs:
            doc = json.loads(path.read_text(encoding="utf-8"))
            period = doc.get("period") or ""
            for report, why in screen_document(doc, builder).items():
                flagged.setdefault((period, report), []).extend(why)
            for report in (doc.get("accepted") or {}):
                reason = held.get((Path(folder).name, period, report))
                if reason:
                    flagged.setdefault((period, report), []).append(
                        f"held by inspection (`HLI-1`): {reason}")
            bs = (doc.get("accepted") or {}).get("balance_sheet") or {}
            total = _first(bs.get("values") or {}, builder.C_ASSETS)
            if total:
                assets[period] = total
    # ⚠️ **`MES-2` — A RUN OF ONE DOCUMENT HAS NO NEIGHBOURS, SO THE DISK LENDS THEM** (2026-09-14). The
    # run's own merge screens one folder, and a parent-only BVH Q3-2023 (18,278,478,405,182) was written
    # beside a consolidated 220,767,878,358,031 the release had held. Disk periods only ever serve as a
    # neighbour: a figure already on disk is never flagged here.
    in_run = set(assets)
    for period, total in (disk_assets or {}).items():
        if total:
            assets.setdefault(period, total)
    order = sorted(assets, key=_q)

    def _breaks(a: str, b: str) -> bool:
        gap = max(1, (_q(b)[0] - _q(a)[0]) * 4 + _q(b)[1] - _q(a)[1])
        rate = (assets[b] / assets[a]) ** (1.0 / gap)
        return rate > MAX_STEP or rate < 1 / MAX_STEP

    # ⚠️ **ONE WRONG FIGURE CONVICTED ITS RIGHT NEIGHBOUR** (`NBR-1`, 2026-09-13 — `BND-2`'s lesson,
    # made mechanical). Flagging the LATER period of every broken step held TPB Q1-2010 (13.47 tn,
    # sound) because Q4-2009 reads 10,728,532,331 — a thousandth of the bank — and Q1-2011 (25.57 tn)
    # because the Q4-2010 reading beside it is 20.9 bn; BVH Q4-2025 was held on Q2-2025's 18.8 tn. So
    # the OUTLIER is found first — a period breaking with BOTH neighbours, or at an end breaking with
    # its one neighbour while that neighbour agrees with the next — and every other period is judged
    # against the nearest period that is NOT an outlier. A step nobody can resolve is still flagged.
    n = len(order)
    outliers = set()
    for i, period in enumerate(order):
        if 0 < i < n - 1:
            if _breaks(order[i - 1], period) and _breaks(period, order[i + 1]):
                outliers.add(period)
        elif i == 0 and n >= 3:
            if _breaks(period, order[1]) and not _breaks(order[1], order[2]):
                outliers.add(period)
        elif i == n - 1 and n >= 3:
            if _breaks(order[i - 1], period) and not _breaks(order[i - 2], order[i - 1]):
                outliers.add(period)
    judged = []
    for i, after in enumerate(order):
        if i == 0:
            if after in outliers:
                judged.append((order[1], after))
            continue
        if after in outliers:
            judged.append((order[i - 1], after))
            continue
        before = next((order[j] for j in range(i - 1, -1, -1) if order[j] not in outliers), None)
        if before is not None:
            judged.append((before, after))
    for before, after in judged:
        if after not in in_run:
            continue
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
