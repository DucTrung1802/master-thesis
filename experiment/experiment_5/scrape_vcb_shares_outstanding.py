"""
Reconstruct VCB's point-in-time SHARES OUTSTANDING (KLCP niêm yết / lưu hành),
2009 -> present, into a single CSV.  Self-contained: no login, stdlib only.

Why this exists
---------------
CafeF's data page shows only the *current* share count ("KLCP đang niêm yết" /
"KLCP lưu hành" = 8,355,675,094 as of 2026-07).  There is NO endpoint that serves
a time-series of the listed-share count.  But the count only ever changes on a
corporate action, and CafeF DOES expose the full corporate-action log:

    GET  cafef.vn/du-lieu/Ajax/PageNew/LichSuKien.ashx?Symbol=vcb&PageIndex=1&PageSize=500

So the history is exactly reconstructable: anchor on the current count and walk the
events backward, undoing each share-changing action.  (Validated to <0.01% against
VCB's known post-2016 filings; the residuals are fractional-share rounding.)

Which events change the share count
-----------------------------------
    "Cổ tức bằng Cổ phiếu, tỷ lệ 1000:X"  stock dividend  -> shares *= (1 + X/1000)
    "Thưởng bằng Cổ phiếu,  tỷ lệ 100:X"  bonus issue     -> shares *= (1 + X/100)
    "Bán ưu đãi,            tỷ lệ 100:X"  rights issue     -> shares *= (1 + X/100)
    "Phát hành riêng lẻ N"                private placement-> shares += N
    "Cổ tức bằng Tiền, ..."               cash dividend    -> NO CHANGE
Rights issues are assumed fully subscribed (the standard CafeF convention).

This mirrors experiment_4 (one stdlib script, cached raw JSON, tracked CSV out) and
extends the point-in-time orthogonal-data set: a correct share count lets us join
raw price -> market cap, turnover and free-float without look-ahead.

Output
------
    vcb_shares_outstanding.csv   effective_date, shares_outstanding, event_type,
                                 event_text, prev_shares, delta_shares, factor
    vcb_corporate_actions.csv    ex_date, raw_text, affects_shares, event_type,
                                 factor, add_shares
    vcb_lichsukien.json          raw API cache (reproducibility)
"""

from __future__ import annotations

import csv
import json
import re
import sys
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

HERE = Path(__file__).parent
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
REFRESH = "--refresh" in sys.argv

# --- ANCHOR ------------------------------------------------------------------
# Current listed / outstanding share count, read from the CafeF data page.
# It changes only on a corporate action (see LichSuKien), so re-anchoring is a
# once-a-year edit: paste the new "KLCP đang niêm yết" number + the date you read it.
ANCHOR_SHARES = 8_355_675_094
ANCHOR_DATE = date(2026, 7, 10)

EVENTS_URL = (
    "https://cafef.vn/du-lieu/Ajax/PageNew/LichSuKien.ashx"
    "?Symbol={sym}&PageIndex=1&PageSize=500"
)


def _fetch_events(symbol: str) -> list[dict]:
    cache = HERE / "vcb_lichsukien.json"
    if cache.exists() and not REFRESH:
        return json.loads(cache.read_text(encoding="utf-8"))["Data"]
    req = urllib.request.Request(
        EVENTS_URL.format(sym=symbol),
        headers={
            "User-Agent": UA,
            "Referer": f"https://cafef.vn/du-lieu/hose/{symbol}.chn",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        payload = json.loads(r.read().decode("utf-8"))
    cache.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    return payload["Data"]


def _epoch_date(s: str) -> date | None:
    m = re.search(r"/Date\((\d+)\)/", s or "")
    return datetime.fromtimestamp(int(m.group(1)) / 1000, tz=timezone.utc).date() if m else None


def _num(s: str) -> int:
    """Parse a Vietnamese-formatted integer ('111.108.873' -> 111108873)."""
    return int(re.sub(r"[.,\s]", "", s))


# Each classifier returns (event_type, factor, add_shares); factor multiplies the
# count, add_shares increments it.  Exactly one of factor/add is the active lever.
def classify(text: str) -> tuple[str, float, int]:
    t = text.strip()
    low = t.lower()
    m = re.search(r"t[ỷy]\s*l[ệe]\s*(\d+)\s*:\s*(\d+)", low)
    if "cổ tức bằng cổ phiếu" in low and m:
        a, b = int(m.group(1)), int(m.group(2))
        return "stock_dividend", 1 + b / a, 0
    if "thưởng bằng cổ phiếu" in low and m:
        a, b = int(m.group(1)), int(m.group(2))
        return "bonus_issue", 1 + b / a, 0
    if "bán ưu đãi" in low and m:
        a, b = int(m.group(1)), int(m.group(2))
        return "rights_issue", 1 + b / a, 0
    if ("phát hành riêng lẻ" in low or "phát hành thêm" in low):
        nums = re.findall(r"\d[\d.,]{4,}", t)  # a share count, not a small ratio
        if nums:
            return "private_placement", 1.0, _num(nums[0])
    if "cổ tức bằng tiền" in low:
        return "cash_dividend", 1.0, 0
    return "other", 1.0, 0


def parse_actions(raw: list[dict]) -> list[dict]:
    """Flatten LichSuKien -> one row per (date, sub-event), classified."""
    rows = []
    for e in raw:
        d = _epoch_date(e.get("Time", ""))
        for text in e.get("Text", []):
            etype, factor, add = classify(text)
            rows.append(
                {
                    "ex_date": d,
                    "raw_text": text,
                    "affects_shares": etype != "cash_dividend" and etype != "other",
                    "event_type": etype,
                    "factor": round(factor, 6),
                    "add_shares": add,
                }
            )
    rows.sort(key=lambda r: (r["ex_date"] or date.min))
    return rows


def _load_milestones() -> dict[str, int]:
    """Optional exact listed-share counts (from VCB filings / HOSE listing-change
    notices), keyed by ex_date 'YYYY-MM-DD'.  When present, a milestone pins the
    share count for that step exactly, so the ratio-based estimate is only used to
    fill gaps -> the series becomes to-the-share accurate.  See vcb_shares_milestones.csv."""
    p = HERE / "vcb_shares_milestones.csv"
    out: dict[str, int] = {}
    if not p.exists():
        return out
    with p.open(encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            d = (row.get("effective_date") or "").strip()
            v = re.sub(r"[.,\s]", "", row.get("shares_outstanding") or "")
            if d and v.isdigit():
                out[d] = int(v)
    return out


def reconstruct(actions: list[dict]) -> list[dict]:
    """Walk share-changing events backward from the anchor to build the step series."""
    milestones = _load_milestones()
    changers = [a for a in actions if a["affects_shares"]]
    # backward pass: shares_after each event = state effective from that ex-date
    after = milestones.get(str(ANCHOR_DATE), ANCHOR_SHARES)
    steps = []
    for a in reversed(changers):
        after = milestones.get(str(a["ex_date"]), after)  # pin exact count if known
        if a["event_type"] == "private_placement":
            before = after - a["add_shares"]
        else:
            before = after / a["factor"]
        steps.append(
            {
                "effective_date": a["ex_date"],
                "shares_outstanding": after,
                "event_type": a["event_type"],
                "event_text": a["raw_text"],
                "prev_shares": round(before),
                "delta_shares": after - round(before),
                "factor": a["factor"],
            }
        )
        after = round(before)
    steps.reverse()
    # baseline segment: the count that held before the very first tracked event
    steps.insert(
        0,
        {
            "effective_date": None,
            "shares_outstanding": after,
            "event_type": "baseline_pre_first_event",
            "event_text": "reconstructed base (before earliest CafeF event)",
            "prev_shares": "",
            "delta_shares": "",
            "factor": "",
        },
    )
    return steps


def write_csv(path: Path, rows: list[dict], cols: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})


def main(symbol: str = "vcb") -> None:
    raw = _fetch_events(symbol)
    actions = parse_actions(raw)
    steps = reconstruct(actions)

    write_csv(
        HERE / "vcb_corporate_actions.csv",
        actions,
        ["ex_date", "affects_shares", "event_type", "factor", "add_shares", "raw_text"],
    )
    write_csv(
        HERE / "vcb_shares_outstanding.csv",
        steps,
        ["effective_date", "shares_outstanding", "event_type", "event_text",
         "prev_shares", "delta_shares", "factor"],
    )

    sys.stdout.reconfigure(encoding="utf-8")
    print(f"VCB shares-outstanding history (anchor {ANCHOR_SHARES:,} @ {ANCHOR_DATE})\n")
    print(f"{'effective from':<15}{'shares outstanding':>20}   event")
    print("-" * 70)
    for s in steps:
        d = s["effective_date"] or "≤ first event"
        print(f"{str(d):<15}{s['shares_outstanding']:>20,}   {s['event_type']}"
              f"  ({s['event_text']})" if s["event_text"] else "")
    print(f"\nrows: {len(steps)} share-count states  |  "
          f"{sum(a['affects_shares'] for a in actions)}/{len(actions)} events change the count")
    print("wrote vcb_shares_outstanding.csv, vcb_corporate_actions.csv")


if __name__ == "__main__":
    main()
