"""Replay the CLOSING-CASH anchor over every cash flow on disk — no OCR, no network.

⚠️ **THIS TOOL EXISTS BECAUSE IT TALKED ME OUT OF THE CHANGE IT WAS BUILT TO JUSTIFY, and that
is the measurement worth keeping.** Written 2026-09-10 to size `RAT-1`'s biggest single block:
of the 67 cash flows refused `no closing cash balance` with a stored row dump, **28 print
`Tiền tồn cuối năm`** and five more print a `Tiền và các khoản tương đương tiền cuối kỳ/năm`
variant that `FinancialsBuilder.CASH_CLOSE`'s two needles do not reach. The figure is on the
page, read correctly, under a label the match does not know.

The obvious fix — add the spellings to `CASH_CLOSE` — passes every check this tool makes:
**0 of the 35 accepted statements that actually consult a needle move** (the other 1,066 are
answered by the canonical `C_CASH_CLOSE` column and never reach `Statement.find`), and **24 of
the refused ones come back with the filing's own `opening + net + fx == closing` confirming the
figure**. It was written, and then REVERTED, because `test_cafef_cash_wording.py` says what the
checks cannot: ⚠️ **`CASH_CLOSE` satisfies `reconcile`'s GATE and populates NO COLUMN.** A
needle that lets the statement through leaves the canonical closing-cash cell BLANK, which is
the exact outcome `reconcile`'s own comment records for five ACB/VCB quarters — *"the grid
claims a parsed row and the one column the statement is probed on is blank, so it reads as
neither a gap nor a value"*. That is worse than the refusal it replaces.

⚠️ **THE RIGHT MECHANISM ALREADY EXISTS AND IS `GCW-1`'s `cash_wording`** — a SCHEMA ALIAS, so
the figure lands in the column, behind a widening layer because renaming an account is a
widening. What this tool then measured is that GAS's 2026-09-07 run **predates it**: that run's
cascade was 100 layers and ended at `cashbs`, while `cashword` shipped later the same day. So
those 28 quarters are not an unfixed defect at all — they are a re-run, and `RAT-1`'s table
counts them under "older cascade".

**Read it as: a needle is not an alias, and a gate satisfied is not a column filled.**

  * **side A — the regression.** For every ACCEPTED cash flow carrying a `row_dump`, resolve the
    anchor under the shipped needles and under `CANDIDATE` and require the two to be IDENTICAL.
    ⚠️ **SCOPED TO THE STATEMENTS THAT ACTUALLY CONSULT A NEEDLE**, because `reconcile`'s `get()`
    reads the canonical column first: a version that ignored that short-circuit reported **445
    "moved" anchors, every one `None -> a figure` on a statement whose mapped column had already
    answered** — a wall of false alarms in a code path the run never takes, which is how a
    safety check stops being read.
  * **side B — the recovery.** For every REFUSED cash flow with an `absent_rows` dump, ask
    whether `CANDIDATE` finds a figure and then whether the filing's OWN identity confirms it.
    ⚠️ **A needle that finds a number is worth nothing; one that finds the number the
    statement's own arithmetic already predicts is evidence.** The 11 it finds and cannot
    confirm are the useful half: GAS Q1-2012 reads `877,860` where the identity says
    `10,805,383,877,860` — a truncated read the anchor would have handed straight to a gate.

    python .claude/tools/replay_cash_close.py            # the two sides, summarised
    python .claude/tools/replay_cash_close.py --list     # every recovered quarter, named
"""
import glob
import json
import os
import re
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.abspath("src"))

from web_scraper.cafef_financials import FinancialsBuilder as FB  # noqa: E402
from web_scraper.cafef_pdf_parser import Row, Statement           # noqa: E402

# ⚠️ **THE CANDIDATE SET, AND EVERY ENTRY IS A WHOLE PRINTED LINE.** `find` matches a needle
# as a substring OR through a fuzzy window of the needle's own length, so a SHORTER needle is
# a LOOSER one — truncating `tien va tuong duong tien cuoi ky` to `... cuoi` to cover both the
# `ky` and `nam` spellings would shrink the window to where the opening line scores inside it
# (measured at 0.90 on VIC Q1-2026, which is why `reject` exists). Each spelling is therefore
# written out in full.
CANDIDATE = FB.CASH_CLOSE + (
    "tien ton cuoi ky",
    "tien ton cuoi nam",
    "tien va cac khoan tuong duong tien cuoi ky",
    "tien va cac khoan tuong duong tien cuoi nam",
    "tien va tuong duong tien cuoi nam",
)

NET = ("luu chuyen tien thuan trong ky", "luu chuyen tien thuan trong nam")
OPEN = ("tien va cac khoan tuong duong tien dau ky",
        "tien va tuong duong tien dau ky", "tien ton dau ky", "tien ton dau nam",
        "tien va cac khoan tuong duong tien dau nam")
FX = ("anh huong cua thay doi ty gia", "anh huong cua thay doi ty gia hoi doai")


def statement_of(rows) -> Statement:
    """A `Statement` carrying just enough for `find` — the rows, in their printed order."""
    return Statement(report="cash_flow", pages=[], unit=1, n_columns=1, rows=[
        Row(label=r[0], key=r[1], number=r[2], values=r[3]) for r in rows])


def dump_rows(a) -> list:
    """`(label, key, number, values)` from either dump shape, in printed order.

    ⚠️ `row_dump` FIRST, and not a test on `"rows"` — an ACCEPTED block carries `rows` as an
    integer COUNT beside its `row_dump`, so keying on that name reads a length as a list."""
    if a.get("row_dump") is not None:                          # accepted: [number,key,label,vals]
        return [(r[2], r[1], r[0], r[3]) for r in a["row_dump"]]
    return [(r.get("label", ""), r["key"], r.get("number", ""), r["values"])
            for r in a["rows"]]                                # absent_rows


def anchor(st: Statement, needles) -> int:
    return st.find(*needles, reject=FB.CASH_OPEN_WORDS)


def identity(st: Statement, close):
    """`(closes, opening, net, fx)` — the filing's own arithmetic, or None where a term is
    absent. ⚠️ A term this cannot find is `None` and the row is reported UNVERIFIED, never
    assumed zero (§5 rule 2)."""
    op = st.find(*OPEN)
    net = st.find(*NET)
    fx = st.find(*FX)
    if op is None or net is None or close is None:
        return None, op, net, fx
    return (op + net + (fx or 0)) == close, op, net, fx


def main() -> int:
    show = "--list" in sys.argv
    latest = {}
    for f in glob.glob("reports/pdf_ocr/*__pdf_ocr/documents/*.json"):
        run = f.split(os.sep)[-3][:15]
        key = os.path.basename(f)
        if key not in latest or run > latest[key][0]:
            latest[key] = (run, f)

    same = moved = mapped_answered = 0
    moved_rows = []
    refused = found = closes = missed = 0
    wins, unverified = [], []
    for _run, f in sorted(latest.values()):
        try:
            d = json.load(open(f, encoding="utf-8"))
        except Exception:                                     # noqa: BLE001
            continue
        sym, per = d.get("symbol"), d.get("period", "")
        acc = (d.get("accepted") or {}).get("cash_flow")
        if acc and acc.get("row_dump"):
            # `reconcile`'s own order: the canonical column, then the needles. A statement the
            # chart of accounts answers never reaches `Statement.find`, so no needle can move it.
            vals = acc.get("values") or {}
            if next((c for c in FB.C_CASH_CLOSE if vals.get(c) is not None), None):
                mapped_answered += 1
            else:
                st = statement_of(dump_rows(acc))
                a, b = anchor(st, FB.CASH_CLOSE), anchor(st, CANDIDATE)
                if a == b:
                    same += 1
                else:
                    moved += 1
                    moved_rows.append((sym, per, a, b))
        why = (d.get("absent_reasons") or {}).get("cash_flow") or []
        ar = (d.get("absent_rows") or {}).get("cash_flow")
        if ar and any("no closing cash balance" in w for _l, w in why):
            refused += 1
            st = statement_of(dump_rows(ar))
            if anchor(st, FB.CASH_CLOSE) is not None:
                continue                                       # a different blocker won it
            close = anchor(st, CANDIDATE)
            if close is None:
                missed += 1
                continue
            found += 1
            ok, op, net, fx = identity(st, close)
            if ok:
                closes += 1
                wins.append((sym, per, close))
            else:
                unverified.append((sym, per, close, ok, op, net, fx))

    print("side A — the regression, over every ACCEPTED cash flow with a row dump")
    print(f"   answered by C_CASH_CLOSE, no needle read   : {mapped_answered}")
    print(f"   anchors UNCHANGED by the candidate needles : {same}")
    print(f"   anchors that MOVED                         : {moved}"
          + ("   ⚠️ THE CHANGE IS NOT SAFE AS WRITTEN" if moved else "   ✅"))
    for sym, per, a, b in moved_rows[:20]:
        print(f"      {sym} {per}   {a} -> {b}")

    print("\nside B — the recovery, over every cash flow refused `no closing cash balance`")
    print(f"   refused with a stored dump                 : {refused}")
    print(f"   the candidate finds a closing balance      : {found}")
    print(f"   ...and the filing's OWN identity confirms it: {closes}"
          f"   (opening + net + fx == closing)")
    print(f"   the candidate finds nothing either         : {missed}   "
          f"(a different defect — the line was never read)")
    print(f"   found but NOT confirmed by the identity    : {len(unverified)}   "
          f"⚠️ these are NOT wins; `reconcile`'s own gates still judge them")
    if show:
        print("\n   confirmed:")
        for sym, per, c in wins:
            print(f"      {sym:5} {per:8} closing {c:>20,}")
        print("\n   found but unverified (a term the dump cannot supply, or a real mismatch):")
        for sym, per, c, ok, op, net, fx in unverified[:40]:
            print(f"      {sym:5} {per:8} closing {c:>18,}  open={op}  net={net}  fx={fx}")
    return 1 if moved else 0


if __name__ == "__main__":
    raise SystemExit(main())
