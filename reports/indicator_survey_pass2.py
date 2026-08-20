"""Second pass: the two things that decide whether a signal is TRADABLE.

  1. t+1 ENTRY. The channels are built from day t's closing order counts, so they
     cannot be traded before t+1. `return_{h}day` at date t spans t -> t+h, i.e. it
     includes the t->t+1 move nobody can capture. The tradable target is
     `return_{h}day` measured at t+1. backtest/CONTEXT.md 8b measured this lag
     destroying ~75% of the order-flow edge; this checks it channel by channel.
  2. REGIME. 8g found the hand screen's whole edge sits pre-2022. Split 2018-2021
     against 2022-2026 and read both.
"""
import os, sys, math, warnings
warnings.filterwarnings("ignore")
sys.stdout.reconfigure(errors="replace")
import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, os.path.abspath("src"))
from feature_selection.unified_reader import UnifiedSchemaReader

CHANNELS = [
    "drv_order_vol_imb", "drv_log_order_size_ratio", "drv_order_vol_imb_5",
    "drv_dist_from_high_252", "drv_close_pos_252", "drv_dist_from_high_63",
    "drv_realized_vol_63", "drv_rogers_satchell_21", "drv_parkinson_5",
    "drv_clv", "drv_order_count_imb_5", "drv_foreign_flow_ratio_21",
]
HORIZONS = [5, 10, 20]

with UnifiedSchemaReader("all") as reader:
    basic = reader.read("pool__basic", columns=["date", "ticker", "value_matched"] + CHANNELS)
    tgt = reader.read("pool__targets",
                      columns=["date", "ticker"] + [f"return_{h}day" for h in HORIZONS])
for f in (basic, tgt):
    f["date"] = pd.to_datetime(f["date"])
    f["ticker"] = f["ticker"].astype(str).str.upper()
df = basic.merge(tgt, on=["date", "ticker"], how="inner", validate="one_to_one")

# ⚠️ t+1 ENTRY: shift the forward return BACK one row per ticker, so the value sitting
# beside the signal at t is the return actually earned from the close of t+1.
df = df.sort_values(["ticker", "date"])
for h in HORIZONS:
    df[f"lag1_{h}"] = df.groupby("ticker")[f"return_{h}day"].shift(-1)

df["liq"] = df.groupby("date")["value_matched"].rank(pct=True)
liq = df[df["liq"] >= 0.80].copy()
for h in HORIZONS:
    for col in (f"return_{h}day", f"lag1_{h}"):
        liq.loc[liq[col].abs() > 1.0, col] = np.nan

REGIMES = {
    "2018-2021": ("2018-01-01", "2021-12-31"),
    "2022-2026": ("2022-01-01", "2026-12-31"),
}

def xs_ic(frame, ch, y, h):
    daily = frame.groupby("date").apply(
        lambda g: stats.spearmanr(g[ch], g[y]).statistic if g[ch].nunique() > 4 else np.nan
    ).dropna()
    if len(daily) <= h:
        return np.nan, np.nan
    ic = float(daily.mean())
    t = ic / (daily.std(ddof=1) / math.sqrt(len(daily) / h))
    return ic, float(t)

rows = []
for h in HORIZONS:
    same, lag = f"return_{h}day", f"lag1_{h}"
    for ch in CHANNELS:
        base = liq.dropna(subset=[ch])
        s_ic, s_t = xs_ic(base.dropna(subset=[same]), ch, same, h)
        l_ic, l_t = xs_ic(base.dropna(subset=[lag]), ch, lag, h)
        rec = {"h": h, "channel": ch,
               "ic_same": s_ic, "t_same": s_t, "ic_t1": l_ic, "t_t1": l_t,
               "kept_pct": 100 * l_ic / s_ic if s_ic and abs(s_ic) > 1e-9 else np.nan}
        for name, (lo, hi) in REGIMES.items():
            w = base[(base["date"] >= lo) & (base["date"] <= hi)].dropna(subset=[lag])
            r_ic, r_t = xs_ic(w, ch, lag, h)
            rec[f"ic_{name}"] = r_ic
            rec[f"t_{name}"] = r_t
        rows.append(rec)
    print(f"  h={h} done", flush=True)

out = pd.DataFrame(rows)
path = os.path.join(os.environ.get("TMP", "."), "survey_tradable.csv")
out.to_csv(path, index=False)
print("wrote", path, len(out), "rows", flush=True)
