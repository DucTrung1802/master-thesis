"""⚠️ `JVW-2`'s VALIDATION, AND IT NEEDS NO GPU — replays the stored rows through the map.

The fix is two halves (`_prefix_trims`' trailing note-reference trim, and six
`ACCOUNT_WORDING` aliases) and the scores say all 13 spellings now clear `SCHEMA_MATCH`.
⚠️ **A SCORE OVER THE BAR IS NOT A MAPPED COLUMN AND A MAPPED COLUMN IS NOT A PASSED
STATEMENT** (§5 rule 21, and `GLU-1`'s VIC Q1-2009 is the lesson: all six canonical keys
appeared and the statement stayed refused). So this asks the two further questions off the
artefacts:

  1. does `map_to_schema` now put a figure in `phan_lai_lo_trong_cong_ty_lien_doanh_lien_ket`?
  2. does `_operating_profit_identity` then CLOSE?

⚠️ **IT REPLAYS `absent_rows`, WHICH IS `RSN-1`'s EARLIEST READING** — the rows stored are the
FIRST layer's, not the deepest — so a cell this says yes to is a cell the cascade should reach
at or before that layer, and a cell it says no to may still be won by a deeper one. Read the
count as a population, never as a cell count.
"""
import collections
import glob
import json
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.abspath("src"))

from web_scraper import cafef_financials as fin
from web_scraper.cafef_pdf_parser import INCOME_STATEMENT, Row, Statement

B = fin.FinancialsBuilder(logger=None)
ACC = "phan_lai_lo_trong_cong_ty_lien_doanh_lien_ket"

latest = {}
for f in glob.glob("reports/pdf_ocr/*__pdf_ocr/documents/*.json"):
    run = f.split(os.sep)[-3][:15]
    key = os.path.basename(f)
    if key not in latest or run > latest[key][0]:
        latest[key] = (run, f)

out = collections.Counter()
named = collections.defaultdict(list)
for run, f in sorted(latest.values()):
    try:
        d = json.load(open(f, encoding="utf-8"))
    except Exception:                                      # noqa: BLE001
        continue
    deep = (d.get("absent_deepest") or {}).get("income_statement") or []
    reasons = [r[1] for r in deep if isinstance(r, list) and len(r) > 1]
    if not reasons or not all("operating profit does not close" in r for r in reasons):
        continue
    stored = (d.get("absent_rows") or {}).get("income_statement") or {}
    rows = [r for r in (stored.get("rows") or []) if isinstance(r, dict)]
    if not rows:
        out["no stored rows to replay"] += 1
        continue
    cell = os.path.basename(f)[:-5]
    template = "bank" if cell.split("_")[1] in ("ACB", "BID", "CTG", "MBB", "SHB", "STB",
                                                "TCB", "TPB", "VCB", "VIB", "VPB") else "corp"
    built = [Row(label=r.get("label") or "", key=r.get("key") or "",
                 number=r.get("number"), values=list(r.get("values") or []))
             for r in rows]
    st = Statement(report=INCOME_STATEMENT, pages=list(stored.get("pages") or []),
                   unit=1, n_columns=max((len(r.values) for r in built), default=1),
                   rows=built)
    try:
        # ⚠️ **`ACCOUNT_WORDING` IS CONSULTED ONLY UNDER `equity_wording`, WHICH IS A CASCADE
        # REPAIR FLAG AND NOT THE DEFAULT PATH** — `_label_score`'s alias branch sits inside
        # `if equity_wording:`. So `JVW-1`'s aliases and `JVW-2`'s have ALWAYS taken effect
        # only at the layers that set it, and a replay on the default path measures the
        # statement as if the aliases did not exist. Both are asked here, because the
        # difference between them IS the finding.
        mapped = B.map_to_schema(st, template)
        mapped_eq = B.map_to_schema(st, template, equity_wording=True)
    except Exception as exc:                               # noqa: BLE001
        out[f"map_to_schema raised: {type(exc).__name__}"] += 1
        continue
    for label, m in (("default path", mapped), ("equity_wording layer", mapped_eq)):
        got = m.get(ACC)
        why = B._operating_profit_identity(m)
        if got is None:
            out[f"{label}: the associates line does NOT map"] += 1
            named[f"{label} / unmapped"].append(cell)
        elif why is None:
            out[f"{label}: ✅ maps AND the identity CLOSES"] += 1
            named[f"{label} / closes"].append(cell)
        else:
            out[f"{label}: maps, identity still open"] += 1
            named[f"{label} / still open"].append(cell)

print("replaying `absent_rows` (the EARLIEST reading, `RSN-1`) through today's map:\n")
for k, v in out.most_common():
    print(f"  {v:4d}  {k}")
for k, cells in named.items():
    print(f"\n{k}: {', '.join(sorted(cells))}")
