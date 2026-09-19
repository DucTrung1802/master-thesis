"""Which AUC selects better models, and how much of this basket's edge is TIMING?

Read-only: the trial log's 15 rows for the 2026-09-18 basket trial, plus the chosen row's
per-session files. No refit, no GPU.
"""
import os
import sys

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

sys.stdout.reconfigure(encoding="utf-8")
os.chdir(r"D:\GIT\master-thesis")
D = "reports/event_chain/liquid__upopen_5pct_5day__final__d1_h6__bsk"

# ---------------------------------------------------------------- 1. does the choice metric matter?
log = pd.read_csv("reports/event_chain/trials.csv")
t = log[log.trial_id.str.startswith("20260918-1722")].copy()
print(f"models scored: {len(t)}")

outcomes = ["test_daily_auc", "test_hit_at_k", "test_basket_return", "test_sharpe_50bps"]
for label, sub in (("all 15 models", t),
                   ("the 13 that rank at all (drop baseline_prior, dir_c001)",
                    t[t.val_daily_auc > 0.55])):
    print(f"\n-- {label} (n={len(sub)}) — Spearman(val metric, test outcome)")
    print(f"{'outcome':<22}{'val_daily_auc':>16}{'val_auc (pooled)':>20}{'val_hit':>10}")
    for col in outcomes:
        row = [spearmanr(sub[key], sub[col]).statistic
               for key in ("val_daily_auc", "val_auc", "val_hit_at_k")]
        print(f"{col:<22}{row[0]:>16.3f}{row[1]:>20.3f}{row[2]:>10.3f}")

print("\n-- what each rule would have PICKED, and what that pick earned on test")
for key in ("val_daily_auc", "val_auc", "val_hit_at_k"):
    pick = t.sort_values(key, ascending=False).iloc[0]
    print(f"  max {key:<14} -> {pick.model_variant:<22} test daily {pick.test_daily_auc:.4f} "
          f"hit {pick.test_hit_at_k:.3f} ret {pick.test_basket_return:+.4f} "
          f"sharpe {pick.test_sharpe_50bps:+.2f}")
best = t.sort_values("test_basket_return", ascending=False).iloc[0]
print(f"  (the best test row, unknowable in advance: {best.model_variant} "
      f"ret {best.test_basket_return:+.4f} sharpe {best.test_sharpe_50bps:+.2f})")

# ------------------------------------------------- 2. how much of the decision is CROSS-session?
ses = pd.read_csv(os.path.join(D, "sessions_test_rolling.csv"), parse_dates=["date"])
bsk = pd.read_csv(os.path.join(D, "baskets_test_rolling.csv"), parse_dates=["date"])
top = bsk.groupby("date").agg(p_max=("y_prob", "max"), p_mean=("y_prob", "mean"))
ses = ses.set_index("date").join(top)

print("\n-- TIMING: does the session's own score level predict the session's base rate?")
for col in ("p_max", "p_mean"):
    r = spearmanr(ses[col], ses["base"])
    print(f"  Spearman({col}, session base rate) = {r.statistic:+.3f}  (p {r.pvalue:.1e}, "
          f"n={len(ses)} sessions, ~{len(ses)/5:.0f} independent)")

# a within-session shuffle cannot move p_mean at all, so the null for TIMING is a
# shuffle of the SESSIONS' base rates against the scores
rng = np.random.default_rng(0)
obs = spearmanr(ses["p_mean"], ses["base"]).statistic
null = np.array([spearmanr(ses["p_mean"], rng.permutation(ses["base"].to_numpy())).statistic
                 for _ in range(200)])
print(f"  null (200 session-permutations): p95 {np.quantile(null, 0.95):+.3f}, "
      f"z {(obs - null.mean()) / null.std(ddof=1):+.2f}")

print("\n-- what the CUT actually buys, split into the two skills")
for tau in (0.0, 0.25, 0.36):
    act = ses[ses["p_max"] >= tau] if tau else ses
    print(f"  cut {tau:.2f}: {len(act):>3} of {len(ses)} sessions traded, "
          f"base on traded {act['base'].mean():.4f} vs {ses['base'].mean():.4f} overall "
          f"(timing = {act['base'].mean() - ses['base'].mean():+.4f}), "
          f"hit {act['hit'].mean():.4f} (selection = {act['hit'].mean() - act['base'].mean():+.4f})")
