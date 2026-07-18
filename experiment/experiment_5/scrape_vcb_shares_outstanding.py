"""
Reconstruct VCB's point-in-time SHARES OUTSTANDING (KLCP), 2009 -> present.
Self-contained: no login, stdlib only.

Why this method (and why the old one was wrong)
-----------------------------------------------
The obvious approach — anchor on today's share count and walk CafeF's corporate-action
log (`LichSuKien.ashx`) backwards, undoing each stock dividend / bonus / placement — is
**broken**, because that log is INCOMPLETE: for VCB it omits three 2010-2012 capital
increases. Cross-checked against VCB's own filed balance sheets (experiment_7), the old
reconstruction overstated the mid-2011 share count by 31.8% (2,317,388,397 vs the filed
1,758,754,000) and put the pre-2010 base at 1.74bn when the filings say 1.21bn.

So the authoritative source here is the company's own **filed charter capital**
("Vốn điều lệ", balance-sheet code 411), read straight from the quarterly financial
statements — the same CafeF BCTC API experiment_7 uses:

    shares_outstanding = charter_capital / 10,000        (10,000 VND par, the VN standard)

That is complete and filing-backed. The corporate-action log is still fetched, but only to
**date and label** each step (an ex-date is more precise than "sometime in this quarter").

Dating rule (no look-ahead):
  * if a corporate action plausibly explains a step (right ratio, ex-date in the window
    since the previous quarter-end) -> use the action's exact EX-DATE.
  * otherwise (the 2010-2012 events CafeF never logged) -> fall back to the QUARTER-END on
    which the new charter capital first appears. Conservative: the increase is applied no
    earlier than the first date it is provably true.

Output
------
    vcb_shares_outstanding.csv   effective_date, period, shares_outstanding, charter_capital,
                                 prev_shares, delta_shares, pct_change, event_type, event_text,
                                 dating
    vcb_corporate_actions.csv    the raw CafeF action log (ex_date, type, ratio, text)
"""

from __future__ import annotations

import csv
import json
import re
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).parent
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
SYMBOL = "VCB"
PAR_VALUE = 10_000          # VND per share — the Vietnamese standard
CHARTER_CODE = "411"        # balance-sheet line "Vốn điều lệ"

BCTC = ("https://apiweb.cafef.vn/api/v2/BCTC/GetReportCDKT"
        "?symbol={sym}&pageIndex=1&pageSize={n}&reportType=NV&TypeTime=QUY")
EVENTS = ("https://cafef.vn/du-lieu/Ajax/PageNew/LichSuKien.ashx"
          "?Symbol={sym}&PageIndex=1&PageSize=500")


def _get(url: str) -> dict:
    req = urllib.request.Request(url, headers={
        "User-Agent": UA, "Referer": f"https://cafef.vn/du-lieu/hose/{SYMBOL.lower()}-tai-chinh.chn"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


# ---------------------------------------------------------------- charter capital
def charter_series() -> list[dict]:
    """-> [{period, year, quarter, charter, shares}] oldest first, from the filings."""
    n = _get(BCTC.format(sym=SYMBOL, n=1))["value"]["count"]
    v = _get(BCTC.format(sym=SYMBOL, n=n))["value"]
    blocks = v["data"][0]["data"] if v.get("data") else []

    out = []
    for b in blocks:
        cell = next((c for c in b["data"] if c["code"] == CHARTER_CODE), None)
        charter = (cell or {}).get("value") or 0
        if not charter:                       # quarter not filed / all-zero -> skip
            continue
        out.append({"period": b["time"], "year": b["year"], "quarter": b["quater"],
                    "charter": int(charter), "shares": round(int(charter) / PAR_VALUE)})
    out.sort(key=lambda r: (r["year"], r["quarter"]))
    return out


def quarter_end(year: int, q: int) -> date:
    return {1: date(year, 3, 31), 2: date(year, 6, 30),
            3: date(year, 9, 30), 4: date(year, 12, 31)}[q]


# ---------------------------------------------------------------- corporate actions
def _epoch(s: str) -> date | None:
    m = re.search(r"/Date\((\d+)\)/", s or "")
    return datetime.fromtimestamp(int(m.group(1)) / 1000, tz=timezone.utc).date() if m else None


def _classify(text: str) -> tuple[str, float]:
    """-> (event_type, multiplicative factor) — factor 1.0 if it doesn't change the count."""
    low = text.lower()
    m = re.search(r"t[ỷy]\s*l[ệe]\s*(\d+)\s*:\s*(\d+)", low)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        if "cổ tức bằng cổ phiếu" in low:
            return "stock_dividend", 1 + b / a
        if "thưởng bằng cổ phiếu" in low:
            return "bonus_issue", 1 + b / a
        if "bán ưu đãi" in low:
            return "rights_issue", 1 + b / a
    if "phát hành riêng lẻ" in low or "phát hành thêm" in low:
        return "private_placement", 0.0            # additive, ratio unknown
    if "cổ tức bằng tiền" in low:
        return "cash_dividend", 1.0
    return "other", 1.0


def corporate_actions() -> list[dict]:
    rows = []
    for e in _get(EVENTS.format(sym=SYMBOL))["Data"]:
        d = _epoch(e.get("Time", ""))
        for text in e.get("Text", []):
            etype, factor = _classify(text)
            rows.append({"ex_date": d, "event_type": etype, "factor": round(factor, 6),
                         "raw_text": text})
    rows.sort(key=lambda r: r["ex_date"] or date.min)
    return rows


# ---------------------------------------------------------------- build the series
def build(series: list[dict], actions: list[dict]) -> list[dict]:
    changers = [a for a in actions if a["event_type"] not in ("cash_dividend", "other")]
    used: set[int] = set()
    steps, prev = [], None

    for row in series:
        sh, qe = row["shares"], quarter_end(row["year"], row["quarter"])
        if prev is None:
            steps.append({**row, "effective_date": qe, "prev_shares": "", "delta_shares": "",
                          "percentage_change": "", "event_type": "baseline", "dating": "first_filing",
                          "event_text": "earliest filed charter capital"})
            prev = row
            continue

        if abs(sh - prev["shares"]) <= 1000:            # unchanged (allow rounding noise)
            prev = row
            continue

        ratio = sh / prev["shares"]
        # An action explains this step if its factor matches the observed jump. Its ex-date
        # can sit a little BEFORE the previous quarter-end — charter capital only registers
        # once the shares are actually issued, which lags the ex-date (e.g. the 2010-12-13
        # rights issue first shows up in the Q1-2011 balance sheet). So search back ~15
        # months from this quarter-end and take the latest match not already consumed.
        cands = [
            (i, a) for i, a in enumerate(changers)
            if i not in used and a["ex_date"] and (qe - timedelta(days=450)) <= a["ex_date"] <= qe
            and ((a["factor"] and abs(a["factor"] - ratio) < 0.02)
                 or a["event_type"] == "private_placement")
        ]
        match = None
        if cands:
            i, match = max(cands, key=lambda c: c[1]["ex_date"])
            used.add(i)
        steps.append({
            **row,
            "effective_date": match["ex_date"] if match else qe,
            "prev_shares": prev["shares"],
            "delta_shares": sh - prev["shares"],
            "percentage_change": f"{(ratio - 1) * 100:.1f}%",
            "event_type": match["event_type"] if match else "unlogged_capital_increase",
            "event_text": match["raw_text"] if match else
                          "not in CafeF's action log — dated to the quarter-end it first appears",
            "dating": "ex_date" if match else "quarter_end",
        })
        prev = row
    return steps


def apply_milestones(steps: list[dict]) -> int:
    """Optional exact counts from vcb_shares_milestones.csv (period -> shares).

    Charter capital is filed in millions of VND, so `charter / 10,000` is only accurate to
    ~±50 shares. Where an exact figure is known (e.g. CafeF's current KLCP), pin it here and
    it wins.
    """
    p = HERE / "vcb_shares_milestones.csv"
    if not p.exists():
        return 0
    pins: dict[str, int] = {}
    with p.open(encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            per = (r.get("period") or "").strip()
            val = re.sub(r"[.,\s]", "", r.get("shares_outstanding") or "")
            if per and val.isdigit():
                pins[per] = int(val)
    n = 0
    for s in steps:
        if s["period"] in pins:
            s["shares"] = pins[s["period"]]
            s["dating"] += "+pinned"
            n += 1
    return n


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    series = charter_series()
    actions = corporate_actions()
    steps = build(series, actions)
    n_pin = apply_milestones(steps)

    with (HERE / "vcb_corporate_actions.csv").open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["ex_date", "event_type", "factor", "raw_text"])
        w.writeheader()
        w.writerows(actions)

    cols = ["effective_date", "period", "shares_outstanding", "charter_capital", "prev_shares",
            "delta_shares", "percentage_change", "event_type", "event_text", "dating"]
    with (HERE / "vcb_shares_outstanding.csv").open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for s in steps:
            w.writerow([s["effective_date"], s["period"], s["shares"], s["charter"],
                        s["prev_shares"], s["delta_shares"], s["percentage_change"],
                        s["event_type"], s["event_text"], s["dating"]])

    print(f"{SYMBOL} shares outstanding — from FILED charter capital "
          f"({len(series)} quarters, {series[0]['period']} .. {series[-1]['period']})\n")
    print(f"{'effective':<12}{'period':<9}{'shares':>16}{'change':>9}  event")
    print("-" * 86)
    for s in steps:
        print(f"{str(s['effective_date']):<12}{s['period']:<9}{s['shares']:>16,}"
              f"{str(s['pct_change']):>9}  {s['event_type']}"
              + ("" if s["dating"].startswith("ex_date") else "  [dated to quarter-end]"))
    print(f"\nwrote vcb_shares_outstanding.csv ({len(steps)} steps), vcb_corporate_actions.csv")


if __name__ == "__main__":
    main()
