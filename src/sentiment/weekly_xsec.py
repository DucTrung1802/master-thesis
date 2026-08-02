"""Cross-sectional weekly model + costed walk-forward over `gold.news_weekly_panel`.

This is guidance.md §8 step 6: **run the evaluation harness on `if_news`/`n_docs` alone,
before any NLP work.** It answers two questions the sentiment block cannot be judged
without:

1. **Does the publication effect exist on VN?** Paper 57 finds covered stocks beat
   uncovered ones by 2.24%/week in US small caps *regardless of tone*. If that is here,
   `if_news` earns its place before a single article is scored.
2. **What must a sentiment feature beat?** Everything downstream is an A/B against the
   controls-only model measured here.

## Design, and the paper behind each choice

| choice | why |
|---|---|
| **cross-sectional relative return**, not absolute | experiment_3.3; paper 57 is the only cross-sectional long–short study in the folder |
| **quantile labels 25/50/25** | paper 53 — the base rate is then KNOWN by construction, not hidden |
| **threshold must clear costs** | paper 56 — a label whose move cannot pay the round trip is not a trade |
| **momentum controls** | paper 57's news strategy correlates **0.80** with momentum; without `mom_12w` a news result is indistinguishable from a momentum result |
| **purged + embargoed walk-forward** | the target peeks `h` weeks ahead, so training must stop `h` weeks before the test window opens |
| **liquidity universe from TRAILING turnover** | paper 56's "tyranny of the index" — today's VN100 membership applied to 2015 is survivorship bias. A trailing-median screen is point-in-time by construction |
| **MCC + Brier + base rate, per fold** | paper 51; paper 49 shows what a single pooled number hides (0.90 → 0.56 as the test set grows) |
| **costed backtest is the deciding metric** | experiment_3; paper 44 raised F1 +18% while LOWERING Sharpe |
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import brier_score_loss, f1_score, matthews_corrcoef

# ── configuration ────────────────────────────────────────────────────────────────────

#: Round-trip transaction cost. VN brokerage ~0.15-0.35% a side plus the 0.1% sell tax.
#: ⚠️ Paper 56's rule: the label threshold must EXCEED this, or the "up" class contains
#: moves that cannot pay for themselves.
ROUND_TRIP_COST = 0.005

#: Liquidity screen — top N by trailing median weekly turnover, recomputed every week.
UNIVERSE_SIZE = 100
UNIVERSE_LOOKBACK_WEEKS = 12

#: Feature blocks. The ablation IS the finding (same shape as `price_predictor.evaluate`).
NEWS_FEATURES = [
    "if_news", "n_docs", "n_days", "n_editorial", "n_disclosure", "n_docs_named",
    "n_earnings", "n_insider_txn", "n_dividend", "n_personnel", "n_capital",
    "n_uncategorized", "if_earnings_week", "relevance_max",
]
CONTROL_FEATURES = [
    "ret_w", "mom_1w", "mom_4w", "mom_12w", "mom_26w", "log_value_w", "sessions",
]

LABEL_NAMES = {0: "avoid", 1: "neutral", 2: "long"}


# ── labelling ────────────────────────────────────────────────────────────────────────


def build_labels(
    panel: pd.DataFrame,
    horizon: int,
    universe_size: int = UNIVERSE_SIZE,
    lookback: int = UNIVERSE_LOOKBACK_WEEKS,
    cost: float = ROUND_TRIP_COST,
) -> pd.DataFrame:
    """Add the liquidity universe, the forward relative return and the 3-class label.

    `rel_h` = the stock's `h`-week forward return **minus the universe's equal-weight
    return over the same window**. Absolute return would make every stock "positive" in a
    rising market — the base-rate pathology visible in paper 49 (recall 1.00) and paper 58
    (accuracy rising monotonically with horizon).
    """
    df = panel.copy()
    df["week_start"] = pd.to_datetime(df["week_start"])
    for col in ("close_last", "value_w"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    weeks = np.sort(df["week_start"].unique())
    widx = pd.Series(np.arange(len(weeks)), index=weeks)
    df["widx"] = df["week_start"].map(widx).astype(int)

    # ── liquidity universe, from TRAILING turnover only ──────────────────────────────
    df = df.sort_values(["exchange", "ticker", "widx"], kind="mergesort")
    df["turnover_med"] = (
        df.groupby(["exchange", "ticker"], sort=False)["value_w"]
        .transform(lambda s: s.rolling(lookback, min_periods=max(4, lookback // 3)).median())
    )
    df["liq_rank"] = df.groupby("widx")["turnover_med"].rank(ascending=False, method="first")
    df["in_universe"] = (df["liq_rank"] <= universe_size) & df["turnover_med"].notna()

    # ── forward return, by an explicit index join (gaps must NOT silently misalign) ──
    fwd = df[["exchange", "ticker", "widx", "close_last"]].copy()
    fwd["widx"] -= horizon
    fwd = fwd.rename(columns={"close_last": "close_fwd"})
    df = df.merge(fwd, on=["exchange", "ticker", "widx"], how="left")
    df["fwd_ret"] = df["close_fwd"] / df["close_last"] - 1.0

    # ── cross-sectional excess, within the universe of that week ─────────────────────
    uni = df[df["in_universe"] & df["fwd_ret"].notna()]
    bench = uni.groupby("widx")["fwd_ret"].mean().rename("bench_ret")
    df = df.merge(bench, on="widx", how="left")
    df["rel"] = df["fwd_ret"] - df["bench_ret"]

    # ── quantile labels, base rate 25/50/25 BY CONSTRUCTION (paper 53) ───────────────
    def _label(group: pd.Series) -> pd.Series:
        lo, hi = group.quantile(0.25), group.quantile(0.75)
        return pd.Series(np.where(group > hi, 2, np.where(group < lo, 0, 1)), index=group.index)

    mask = df["in_universe"] & df["rel"].notna()
    df["label"] = np.nan
    df.loc[mask, "label"] = df.loc[mask].groupby("widx")["rel"].transform(_label)

    # ⚠️ Paper 56: state whether the class boundary clears the round trip. A week whose
    # 75th percentile sits under cost has no tradeable long class.
    q = df[mask].groupby("widx")["rel"].quantile(0.75).rename("q75")
    df = df.merge(q, on="widx", how="left")
    df["tradeable_week"] = df["q75"] > cost
    return df


# ── splitting ────────────────────────────────────────────────────────────────────────


def purged_folds(
    widx: Sequence[int], horizon: int, n_folds: int = 6, min_train: int = 104
) -> Iterator[Tuple[int, int, int]]:
    """Yield `(train_end, test_start, test_end)` week indices, expanding window.

    ⚠️ `train_end = test_start − horizon − 1` is the purge: the label at week `w` reads
    prices through `w + horizon`, so training on `w` when the test opens at `w + horizon`
    would train on the test window's own returns. The embargo is that same gap.
    """
    lo, hi = int(min(widx)), int(max(widx))
    first_test = lo + min_train + horizon
    if first_test >= hi:
        return
    edges = np.linspace(first_test, hi, n_folds + 1).astype(int)
    for i in range(n_folds):
        test_start, test_end = int(edges[i]), int(edges[i + 1])
        if test_end <= test_start:
            continue
        yield test_start - horizon - 1, test_start, test_end


# ── evaluation ───────────────────────────────────────────────────────────────────────


@dataclass
class FoldMetrics:
    fold: int
    n_train: int
    n_test: int
    accuracy: float
    majority: float
    macro_f1: float
    mcc: float
    brier: float


@dataclass
class Result:
    name: str
    horizon: int
    folds: List[FoldMetrics] = field(default_factory=list)
    portfolio: Dict[str, float] = field(default_factory=dict)

    def mean(self, attr: str) -> float:
        vals = [getattr(f, attr) for f in self.folds]
        return float(np.mean(vals)) if vals else float("nan")


def _make_model() -> HistGradientBoostingClassifier:
    # HistGB, not exact GB: `sentiment/CONTEXT.md` §5 — exact GB is 1-2 orders slower and
    # this is 6 folds × 5 horizons × 3 feature sets.
    return HistGradientBoostingClassifier(
        max_iter=200, learning_rate=0.06, max_depth=4,
        l2_regularization=1.0, random_state=0,
    )


def evaluate(
    labelled: pd.DataFrame,
    features: List[str],
    name: str,
    horizon: int,
    n_folds: int = 6,
    cost: float = ROUND_TRIP_COST,
) -> Result:
    """Purged walk-forward. Returns per-fold classification metrics + a costed backtest."""
    df = labelled[labelled["in_universe"] & labelled["label"].notna()].copy()
    df = df.dropna(subset=["rel"])
    res = Result(name=name, horizon=horizon)

    picks: List[pd.DataFrame] = []
    for i, (train_end, test_start, test_end) in enumerate(
        purged_folds(df["widx"], horizon, n_folds)
    ):
        tr = df[df["widx"] <= train_end]
        te = df[(df["widx"] >= test_start) & (df["widx"] < test_end)]
        if len(tr) < 500 or len(te) < 100:
            continue

        X_tr = tr[features].to_numpy(dtype=float)
        X_te = te[features].to_numpy(dtype=float)
        y_tr = tr["label"].to_numpy(dtype=int)
        y_te = te["label"].to_numpy(dtype=int)

        model = _make_model().fit(X_tr, y_tr)
        proba = model.predict_proba(X_te)
        pred = model.classes_[proba.argmax(axis=1)]

        long_col = list(model.classes_).index(2) if 2 in model.classes_ else None
        p_long = proba[:, long_col] if long_col is not None else np.zeros(len(te))

        res.folds.append(
            FoldMetrics(
                fold=i,
                n_train=len(tr),
                n_test=len(te),
                accuracy=float((pred == y_te).mean()),
                majority=float(pd.Series(y_te).value_counts(normalize=True).max()),
                macro_f1=float(f1_score(y_te, pred, average="macro", zero_division=0)),
                mcc=float(matthews_corrcoef(y_te, pred)),
                brier=float(brier_score_loss((y_te == 2).astype(int), p_long)),
            )
        )
        out = te[["widx", "ticker", "rel", "fwd_ret"]].copy()
        out["p_long"] = p_long
        picks.append(out)

    if picks:
        res.portfolio = _backtest(pd.concat(picks), horizon, cost)
    return res


def _backtest(picks: pd.DataFrame, horizon: int, cost: float) -> Dict[str, float]:
    """Long-only top-quartile by `p_long`, rebalanced every `horizon` weeks, costed.

    Long-only on purpose: single-stock shorting is effectively unavailable on HOSE
    (experiment_3.2), which is also why papers 51/55/57 converge on exclusion rather than
    reversal. Held against the equal-weight universe over the same weeks.
    """
    rows = []
    for widx, g in picks.groupby("widx"):
        if len(g) < 8:
            continue
        cut = g["p_long"].quantile(0.75)
        sel = g[g["p_long"] >= cut]
        rows.append(
            {
                "widx": widx,
                "strategy": float(sel["fwd_ret"].mean()) - cost,
                "benchmark": float(g["fwd_ret"].mean()),
                "n_held": len(sel),
            }
        )
    if not rows:
        return {}
    bt = pd.DataFrame(rows).sort_values("widx")
    # Non-overlapping rebalances only: a weekly-formed h-week position would otherwise be
    # counted h times over.
    bt = bt.iloc[::horizon]

    per_year = 52.0 / horizon
    out: Dict[str, float] = {}
    for col in ("strategy", "benchmark"):
        r = bt[col].to_numpy(dtype=float)
        curve = np.cumprod(1 + r)
        years = len(r) / per_year
        out[f"{col}_cagr"] = float(curve[-1] ** (1 / years) - 1) if years > 0 else float("nan")
        out[f"{col}_sharpe"] = float(r.mean() / r.std() * np.sqrt(per_year)) if r.std() > 0 else float("nan")
        out[f"{col}_maxdd"] = float((curve / np.maximum.accumulate(curve) - 1).min())
    out["excess_cagr"] = out["strategy_cagr"] - out["benchmark_cagr"]
    out["periods"] = float(len(bt))
    out["avg_held"] = float(bt["n_held"].mean())
    return out


def format_report(results: List[Result], cost: float = ROUND_TRIP_COST) -> str:
    """One table per horizon. ⚠️ Every accuracy sits next to its majority-class rate —
    without that column the number is meaningless (papers 47, 49, 50, 53)."""
    lines: List[str] = []
    by_h: Dict[int, List[Result]] = {}
    for r in results:
        by_h.setdefault(r.horizon, []).append(r)

    for horizon in sorted(by_h):
        lines.append(f"\n{'=' * 96}\nHORIZON = {horizon} week(s)   round-trip cost = {cost:.2%}\n{'=' * 96}")
        lines.append(
            f"{'features':<22}{'folds':>6}{'acc':>8}{'major':>8}{'macroF1':>9}"
            f"{'MCC':>8}{'Brier':>8}{'CAGR':>9}{'bench':>9}{'Sharpe':>8}{'maxDD':>9}"
        )
        lines.append("-" * 96)
        for r in by_h[horizon]:
            p = r.portfolio
            lines.append(
                f"{r.name:<22}{len(r.folds):>6}"
                f"{r.mean('accuracy'):>8.3f}{r.mean('majority'):>8.3f}"
                f"{r.mean('macro_f1'):>9.3f}{r.mean('mcc'):>8.3f}{r.mean('brier'):>8.3f}"
                f"{p.get('strategy_cagr', float('nan')):>9.2%}"
                f"{p.get('benchmark_cagr', float('nan')):>9.2%}"
                f"{p.get('strategy_sharpe', float('nan')):>8.2f}"
                f"{p.get('strategy_maxdd', float('nan')):>9.2%}"
            )
        lines.append("\n  per-fold MCC (a single pooled number hides what paper 49 measured):")
        for r in by_h[horizon]:
            per_fold = "  ".join(f"{f.mcc:+.3f}" for f in r.folds)
            lines.append(f"    {r.name:<22}{per_fold}")
    return "\n".join(lines)
