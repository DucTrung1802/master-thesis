"""Survey: which channels predict forward return at h=5/10/20, cross-sectionally
and per-ticker, on the liquid tier of unified_schema_all.

⚠️ Both ICs are reported because they answer DIFFERENT questions:
   xs_ic  = within-date Spearman  -> can I rank a GROUP today?
   ts_ic  = per-ticker Spearman   -> does this predict ONE stock's own future?
n_eff is n_dates/h for the panel and n_rows/h for a series (CLAUDE.md rule 7).
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
    "drv_order_vol_imb", "drv_order_vol_imb_5", "drv_order_vol_imb_21",
    "drv_order_count_imb", "drv_order_count_imb_5", "drv_log_order_size_ratio",
    "avg_vol_per_sell_order", "drv_order_fill_ratio",
    "drv_clv", "drv_close_vs_vwap", "drv_gap_open_pct", "drv_upper_shadow",
    "drv_dist_from_high_63", "drv_dist_from_high_252", "drv_dist_from_low_21",
    "drv_close_z_21", "drv_close_pos_252", "drv_volume_pos_63",
    "drv_downside_vol_21", "drv_realized_vol_63", "drv_ret_skew_63",
    "drv_ret_kurt_63", "drv_vol_ratio_10_63", "drv_rogers_satchell_21",
    "drv_parkinson_5", "drv_amihud_63",
    "drv_foreign_flow_ratio_21", "drv_foreign_own_chg_21", "drv_foreign_participation",
]
HORIZONS = [5, 10, 20]

with UnifiedSchemaReader("all") as reader:
    basic = reader.read("pool__basic",
                        columns=["date", "ticker", "value_matched"] + CHANNELS)
    tgt = reader.read("pool__targets",
                      columns=["date", "ticker"] + [f"return_{h}day" for h in HORIZONS])

for f in (basic, tgt):
    f["date"] = pd.to_datetime(f["date"])
    f["ticker"] = f["ticker"].astype(str).str.upper()

df = basic.merge(tgt, on=["date", "ticker"], how="inner", validate="one_to_one")
print(f"joined {len(df):,} rows x {df['ticker'].nunique()} tickers, "
      f"{df['date'].min().date()} -> {df['date'].max().date()}")

# ⚠️ POINT-IN-TIME liquidity tier: ntile on THAT date's turnover, never the whole sample.
df["liq"] = df.groupby("date")["value_matched"].rank(pct=True)
liq = df[df["liq"] >= 0.80].copy()
print(f"top liquidity quintile: {len(liq):,} rows, "
      f"{liq.groupby('date').size().median():.0f} names/date median")

# ⚠️ Extreme forward returns are screened, not winsorised (backtest CONTEXT 8).
for h in HORIZONS:
    liq.loc[liq[f"return_{h}day"].abs() > 1.0, f"return_{h}day"] = np.nan

rows = []
for h in HORIZONS:
    y = f"return_{h}day"
    sub = liq.dropna(subset=[y])
    n_dates = sub["date"].nunique()
    n_eff_xs = n_dates / h
    for ch in CHANNELS:
        s = sub.dropna(subset=[ch])
        if len(s) < 5000:
            continue
        # --- cross-sectional: Spearman within each date, then the mean
        daily = s.groupby("date").apply(
            lambda g: stats.spearmanr(g[ch], g[y]).statistic if g[ch].nunique() > 4 else np.nan
        ).dropna()
        xs_ic = float(daily.mean())
        xs_t = float(xs_ic / (daily.std(ddof=1) / math.sqrt(len(daily) / h))) if len(daily) > h else np.nan

        # --- per-ticker: Spearman within each ticker's own series, then the mean
        per = s.groupby("ticker").apply(
            lambda g: stats.spearmanr(g[ch], g[y]).statistic if len(g) > 200 and g[ch].nunique() > 4 else np.nan
        ).dropna()
        ts_ic = float(per.mean())
        ts_t = float(ts_ic / (per.std(ddof=1) / math.sqrt(len(per)))) if len(per) > 2 else np.nan

        # --- top-decile lift, the tradable form
        s = s.copy()
        s["d"] = s.groupby("date")[ch].transform(lambda v: v.rank(pct=True))
        top = s[s["d"] >= 0.90][y]
        bot = s[s["d"] <= 0.10][y]
        rows.append({
            "h": h, "channel": ch, "coverage": len(s) / len(sub),
            "xs_ic": xs_ic, "xs_t": xs_t, "n_eff_xs": n_eff_xs,
            "ts_ic": ts_ic, "ts_t": ts_t, "n_tickers": len(per),
            "top_dec_ret": float(top.mean()), "bot_dec_ret": float(bot.mean()),
            "spread": float(top.mean() - bot.mean()),
        })
    print(f"  h={h} done")

out = pd.DataFrame(rows)
path = os.path.join(os.environ.get("TMP", "."), "survey_channels.csv")
out.to_csv(path, index=False)
print("wrote", path, len(out), "rows")
