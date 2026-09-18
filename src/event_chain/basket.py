"""The BASKET report and picker — WHICH names to buy at the close of session N, at most X.

    python -m event_chain --setup basket --apply           # the chain; its report stage is `write`
    python -m event_chain.basket --pick 2026-08-14         # the basket of one session
    python -m event_chain.basket --pick 2026-08-21 --top-k 5 --model ensemble_geo2
    python -m event_chain.basket --pick 2026-08-21 --min-prob 0      # always X names, no cut
    python -m event_chain.basket --min-prob-study                    # re-choose the cut, no refit

The label is the event chain's (`utils.event_target`, `up_5pct_5day`: the close h sessions out
is at least g % above the close of N). A basket setup (`config.SETUPS["basket"]`) fits the same
estimators on a PANEL universe, and this module turns their scores into a decision: on every
session, the `top_k` highest scores among the names that could be BOUGHT that day are the
basket, held h sessions and sold.

| metric | reads | why |
|---|---|---|
| `hit` | per session, the share of the basket that realised the event; averaged over sessions | the question as asked: "which names will be +g % in h sessions?" |
| `base`, `lift` | the same share over EVERY buyable name of the session, and `hit / base` | a basket of random names hits at `base`; that is what `hit` has to beat |
| `hit_bar`, `hit_null_max`, `hit_z`, `hit_p` | `hit` under `BASKET_NULL_DRAWS` baskets drawn at random from each session's buyable names | the WITHIN-DATE shuffle null of the headline chain: it keeps every session's base rate and breaks only the ranking |
| `all_hit` | the share of sessions on which EVERY name of the basket hit | the strictest reading of the question |
| `bret`, `uret` | the basket's mean h-session return, and the equal-weight universe's | a hit rate can rise by picking volatile names that also fall further; the return is the check |
| `bret_ge_g` | the share of sessions whose basket, equal-weighted, returned at least g % | the other reading: the PORTFOLIO is +g % |
| `daily_auc` | the ROC-AUC within each session, averaged | the whole ranking, not only its top |
| `auc`, `auc_bar`, `auc_z` · `daily_auc_bar`, `daily_auc_z` | the POOLED ROC-AUC over every name-session, and both AUCs under `BASKET_NULL_DRAWS` shuffles of the label WITHIN each session | ⚠️ the within-session shuffle keeps every session's base rate, so the pooled null is the AUC of TIMING alone (above 0.5 when the score knows which sessions are eventful) and `auc − auc_bar` is the cross-sectional part; the daily null sits at 0.5 |
| `sharpe_<bps>`, `cagr_<bps>`, `maxdd_<bps>` | one basket every h-th session (non-overlapping), net of a round trip | how the decision trades |

⚠️ **A BASKET HOLDS AT MOST X NAMES, AND MAY HOLD NONE** (`config.BASKET_MIN_PROB_*`): the top X
names whose P(event) reaches `min_prob`, the cut chosen on VAL. `min_prob_study` measures it with
metrics of its own, because a basket of variable size has no fixed denominator:

| metric | reads | why |
|---|---|---|
| `precision` (`_bar`, `_z`) | names that rose ≥ g % over names bought, pooled over active sessions; the null draws the SAME number of names at random from each active session | the question per name, against a basket that has the model's timing and no selection |
| `base_active`, `p_mean` | the buyable base rate on the active sessions, and the mean P(event) of the names bought | how much of `precision` is TIMING (the base rate rises when the cut trades); calibration beside it |
| `active_share`, `size` | the share of sessions with a basket, and its mean size | a cut that trades 1 % of sessions is a handful of baskets (`n_eff` = active / h) |
| `basket_ge_g` (`_bar`, `_z`) | the share of active sessions whose equal-weight basket returned ≥ g % | the question per basket: "is the POSITION +g %?" |
| `loss_rate`, `bret`, `uret_active` | the share of active baskets that lost, their mean return, the universe's on the same sessions | a hit rate rewards volatile names; the return is the check |
| `ev` | the mean net return per SESSION, a cash session returning 0 | what the cut is chosen on: it prices staying out |
| `sharpe`, `sharpe_min`, `cagr`, `maxdd` | the non-overlapping track at the last cost, cash when idle, averaged over the h start offsets (`sharpe_min` the worst offset) | one offset is one sample of a sparse track |

⚠️ **A NAME AT ITS CEILING ON N IS BUYABLE** (`config.BASKET_EXCLUDE_CEILING = False`, the user's
decision 2026-09-17 — an order at the ceiling can fill while sellers remain). Every row still
carries `at_ceiling_n`, and `True` restores the `PRF-0` entry screen
(`backtest.portfolio.mark_ceiling`), under which such a name is never in a basket.

⚠️ **THE MODEL IS CHOSEN ON VAL** by `config.BASKET_CHOOSE_ON` — `val_hit` for the first basket
trial, the pooled ROC-AUC (`val_auc`) since the second — with the other two as tie-breaks, and its
test row is quoted beside every other row. `auc` is the POOLED ROC-AUC over every name-session;
`daily_auc` ranks names within a session, which is what cutting a basket uses.

⚠️ **A PANEL IS PURGED IN SESSIONS, NOT ROWS.** A refit before session S uses rows dated at
least `d + h - 1` trading sessions before S — the dataset's own purge, on the market calendar.
`report.rolling_test` cuts blocks of SAMPLES, which on a panel is a fraction of one session.

⚠️ **`--pick` REFITS, IT DOES NOT REUSE A RUN.** The chosen configuration is refitted on every
labelled row dated at least `d + h - 1` sessions before N — the labels known by then — and N's
rows are read from the final table, imputed with the dataset's train medians and scaled with
its scaler, so a session whose label does not exist yet can still be scored.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from event_chain import config as C
from event_chain import report as R

ELIGIBLE_EXCLUDED = R.NOT_ELIGIBLE


# ------------------------------------------------------------------ data
def market_frame(chain, entry: Optional[str] = None) -> pd.DataFrame:
    """`date, ticker, exchange, ret, at_ceiling` over the universe's whole calendar.

    `ret` is what a basket bought on N earns, under `config.BASKET_ENTRY`: the label's own
    close-to-close return, or `open_adjust[N+1] -> close_adjust[N+h]` — the same h sessions of
    holding, bought the MORNING AFTER the signal (`ret_close` is kept beside it either way).
    """
    from backtest import portfolio as P
    from feature_selection.unified_reader import UnifiedSchemaReader

    h = chain.horizon
    entry = entry or C.BASKET_ENTRY
    if entry not in ("close", "next_open"):
        raise ValueError(f"BASKET_ENTRY {entry!r} is neither 'close' nor 'next_open'")
    with UnifiedSchemaReader(chain.ticker) as reader:
        targets = reader.read("pool__targets", columns=["date", "ticker", f"return_{h}day"])
        basic = reader.read("pool__basic", columns=["date", "exchange", "ticker", "close_adjust",
                                                   "open", "close_raw"])
    for frame in (targets, basic):
        frame["date"] = pd.to_datetime(frame["date"])
        frame["ticker"] = frame["ticker"].astype(str).str.upper()
    basic = basic.sort_values(["ticker", "date"]).reset_index(drop=True)
    basic["day_ret"] = basic.groupby("ticker")["close_adjust"].pct_change()
    basic["at_ceiling"] = P.mark_ceiling(basic)
    # `open`/`close_raw` are the RAW scale and `close_adjust` the adjusted one, so the open is
    # carried over by the row's own adjustment factor rather than compared across it.
    for column in ("open", "close_raw", "close_adjust"):
        basic[column] = pd.to_numeric(basic[column], errors="coerce")
    basic["open_adjust"] = basic["open"] * basic["close_adjust"] / basic["close_raw"]
    by = basic.groupby("ticker", sort=False)
    # ⚠️ h sessions AFTER the entry, which is one session after the signal — the
    # `open` rule of `utils.event_target`, so the money and the label price one trade.
    basic["ret_next_open"] = by["close_adjust"].shift(-(h + 1)) / by["open_adjust"].shift(-1) - 1
    keep = ["date", "ticker", "exchange", "at_ceiling", "close_adjust", "open_adjust", "ret_next_open"]
    out = basic[keep].merge(targets.rename(columns={f"return_{h}day": "ret_close"}),
                            on=["date", "ticker"], how="left", validate="one_to_one")
    out["ret"] = out["ret_close"] if entry == "close" else out["ret_next_open"]
    out["entry"] = entry
    return out.sort_values(["date", "ticker"]).reset_index(drop=True)


def calendar(market: pd.DataFrame) -> pd.DatetimeIndex:
    """The trading sessions of the universe — the unit every purge here is counted in."""
    return pd.DatetimeIndex(np.sort(market["date"].unique()))


def attach(pred: pd.DataFrame, market: pd.DataFrame,
           exclude_ceiling: bool = C.BASKET_EXCLUDE_CEILING) -> pd.DataFrame:
    """Predictions + `ret`, `at_ceiling`, `exchange` on `(date, ticker)`.

    `at_ceiling` is the ENTRY SCREEN every function below applies; `at_ceiling_n` is the fact.
    With `exclude_ceiling=False` the screen is off and the fact is kept for the report.
    """
    pred = pred.copy()
    pred["date"] = pd.to_datetime(pred["date"])
    pred["ticker"] = pred["ticker"].astype(str).str.upper()
    columns = ["date", "ticker", "exchange", "at_ceiling", "ret"] + \
              [c for c in ("ret_close", "ret_next_open") if c in market]
    out = pred.merge(market[columns], on=["date", "ticker"], how="left", validate="one_to_one")
    if out["at_ceiling"].isna().any():
        missing = int(out["at_ceiling"].isna().sum())
        raise ValueError(f"{missing} scored rows have no pool__basic row — the panel and the "
                         f"universe disagree, and a row that cannot say it was buyable is not traded")
    out["at_ceiling_n"] = out["at_ceiling"].astype(bool)
    out["at_ceiling"] = out["at_ceiling_n"] if exclude_ceiling else False
    return out


# --------------------------------------------------------------- metrics
def _sorted_codes(dates: pd.Series):
    codes, uniques = pd.factorize(pd.to_datetime(dates), sort=True)
    return codes.astype(np.int64), pd.DatetimeIndex(uniques)


def _first_k(codes_sorted: np.ndarray, counts: np.ndarray, k: int) -> np.ndarray:
    starts = np.concatenate([[0], np.cumsum(counts)[:-1]])
    return (np.arange(len(codes_sorted)) - starts[codes_sorted]) < k


def _daily_auc(codes: np.ndarray, y: np.ndarray, p: np.ndarray, n_dates: int) -> np.ndarray:
    """ROC-AUC within each session (NaN where a session holds one class)."""
    ranks = pd.Series(p).groupby(codes).rank(method="average").to_numpy()
    pos = np.bincount(codes, weights=y, minlength=n_dates)
    tot = np.bincount(codes, minlength=n_dates).astype(float)
    neg = tot - pos
    rank_pos = np.bincount(codes, weights=ranks * y, minlength=n_dates)
    with np.errstate(invalid="ignore", divide="ignore"):
        auc = (rank_pos - pos * (pos + 1) / 2.0) / (pos * neg)
    auc[(pos == 0) | (neg == 0)] = np.nan
    return auc


def sessions(frame: pd.DataFrame, k: int, score: str = "y_prob") -> pd.DataFrame:
    """One row per session: its buyable width, base rate and the basket's outcome.

    `frame` holds `date, ticker, y_true, <score>, ret, at_ceiling`. The basket is the `k`
    highest scores among the session's BUYABLE names, ties broken by ticker.
    """
    buy = frame[~frame["at_ceiling"]].sort_values(["date", "ticker"]).reset_index(drop=True)
    codes, dates = _sorted_codes(buy["date"])
    n_dates = len(dates)
    y = (buy["y_true"].to_numpy(dtype=float) >= 0.5).astype(float)
    p = buy[score].to_numpy(dtype=float)
    r = buy["ret"].to_numpy(dtype=float)
    tie = pd.factorize(buy["ticker"], sort=True)[0]
    counts = np.bincount(codes, minlength=n_dates)
    order = np.lexsort((tie, -p, codes))
    cs = codes[order]
    pick = _first_k(cs, counts, k)
    picked = np.minimum(counts, k).astype(float)
    hits = np.bincount(cs[pick], weights=y[order][pick], minlength=n_dates)
    rsum = np.bincount(cs[pick], weights=np.nan_to_num(r[order][pick]), minlength=n_dates)
    return pd.DataFrame({
        "date": dates,
        "width": counts,
        "picked": picked,
        "base": np.bincount(codes, weights=y, minlength=n_dates) / counts,
        "hit": hits / picked,
        "all_hit": (hits == picked).astype(float),
        "bret": rsum / picked,
        "uret": np.bincount(codes, weights=np.nan_to_num(r), minlength=n_dates) / counts,
        "daily_auc": _daily_auc(codes, y, p, n_dates),
    })


def baskets(frame: pd.DataFrame, k: int, score: str = "y_prob") -> pd.DataFrame:
    """Every session's basket, one row per pick: `date, rank, ticker, score, y_true, ret`."""
    buy = frame[~frame["at_ceiling"]].sort_values(["date", score, "ticker"],
                                                  ascending=[True, False, True])
    buy = buy.assign(rank=buy.groupby("date").cumcount() + 1)
    out = buy[buy["rank"] <= k]
    keep = (["date", "rank", "ticker"] + [c for c in ("exchange", "at_ceiling_n") if c in out]
            + [score, "y_true", "ret"])
    return out[keep].reset_index(drop=True)


def trading(per_date: pd.DataFrame, horizon: int, costs: Sequence[float] = C.BASKET_COSTS) -> Dict[str, float]:
    """One basket every `horizon`-th session, held `horizon` sessions, net of a round trip."""
    periods = per_date.iloc[::horizon]
    out: Dict[str, float] = {"periods": int(len(periods))}
    if len(periods) < 2:
        return out
    per_year = 252.0 / horizon
    uret = periods["uret"].to_numpy(dtype=float)
    out["universe_sharpe"] = float(uret.mean() / (uret.std(ddof=1) or np.nan) * np.sqrt(per_year))
    out["universe_cagr"] = float(np.prod(1 + uret) ** (per_year / len(uret)) - 1)
    for cost in costs:
        net = periods["bret"].to_numpy(dtype=float) - cost
        curve = np.cumprod(1 + net)
        tag = f"{int(round(cost * 1e4))}"
        out[f"sharpe_{tag}"] = float(net.mean() / (net.std(ddof=1) or np.nan) * np.sqrt(per_year))
        out[f"cagr_{tag}"] = float(curve[-1] ** (per_year / len(net)) - 1)
        out[f"maxdd_{tag}"] = float((curve / np.maximum.accumulate(curve) - 1).min())
    out["se_sharpe"] = float(np.sqrt((1 + 0.5 * out[f"sharpe_{int(round(costs[-1] * 1e4))}"] ** 2 / per_year)
                                     / len(periods)) * np.sqrt(per_year))
    return out


def null_hits(frame: pd.DataFrame, k: int, draws: int, seed: int = 0) -> np.ndarray:
    """`hit` of `draws` baskets drawn uniformly from each session's buyable names."""
    buy = frame[~frame["at_ceiling"]].sort_values(["date", "ticker"]).reset_index(drop=True)
    codes, dates = _sorted_codes(buy["date"])
    n_dates = len(dates)
    y = (buy["y_true"].to_numpy(dtype=float) >= 0.5).astype(float)
    counts = np.bincount(codes, minlength=n_dates)
    picked = np.minimum(counts, k).astype(float)
    rng = np.random.default_rng(seed)
    out = np.empty(draws)
    for i in range(draws):
        order = np.lexsort((rng.random(len(y)), codes))
        cs = codes[order]
        pick = _first_k(cs, counts, k)
        out[i] = float(np.mean(np.bincount(cs[pick], weights=y[order][pick], minlength=n_dates) / picked))
    return out


def auc_nulls(frame: pd.DataFrame, draws: int, seed: int = 0) -> Dict[str, float]:
    """Pooled and within-session AUC, observed and under a WITHIN-SESSION label shuffle.

    The score's ranks are computed once; a draw only permutes the labels inside each session,
    so a draw costs two `bincount`s and no sort of the scores.
    """
    from scipy.stats import rankdata

    buy = frame[~frame["at_ceiling"]].sort_values(["date", "ticker"]).reset_index(drop=True)
    codes, dates = _sorted_codes(buy["date"])
    n_dates = len(dates)
    y = (buy["y_true"].to_numpy(dtype=float) >= 0.5).astype(float)
    p = buy["y_prob"].to_numpy(dtype=float)
    pos, n = y.sum(), len(y)
    if pos == 0 or pos == n:
        return {}
    ranks_all = rankdata(p)
    ranks_day = pd.Series(p).groupby(codes).rank(method="average").to_numpy()
    tot = np.bincount(codes, minlength=n_dates).astype(float)

    def pooled(lab):
        return float((ranks_all[lab > 0].sum() - pos * (pos + 1) / 2) / (pos * (n - pos)))

    def daily(lab):
        pd_ = np.bincount(codes, weights=lab, minlength=n_dates)
        nd = tot - pd_
        rp = np.bincount(codes, weights=ranks_day * lab, minlength=n_dates)
        ok = (pd_ > 0) & (nd > 0)
        return float(np.mean((rp[ok] - pd_[ok] * (pd_[ok] + 1) / 2) / (pd_[ok] * nd[ok])))

    obs_p, obs_d = pooled(y), daily(y)
    rng = np.random.default_rng(seed)
    null_p, null_d = np.empty(draws), np.empty(draws)
    for i in range(draws):
        lab = y[np.lexsort((rng.random(n), codes))]
        null_p[i], null_d[i] = pooled(lab), daily(lab)

    def z(obs, null):
        sd = float(null.std(ddof=1))
        return float((obs - null.mean()) / sd) if sd > 1e-12 else np.nan

    return {"auc_null_mean": float(null_p.mean()), "auc_bar": float(np.quantile(null_p, 0.95)),
            "auc_null_max": float(null_p.max()), "auc_z": z(obs_p, null_p),
            "daily_auc_bar": float(np.quantile(null_d, 0.95)),
            "daily_auc_null_max": float(null_d.max()), "daily_auc_z": z(obs_d, null_d)}


def basket_metrics(frame: pd.DataFrame, k: int, horizon: int, gain_pct: float,
                   draws: int = C.BASKET_NULL_DRAWS, seed: int = 0) -> Dict[str, float]:
    from sklearn.metrics import roc_auc_score

    per = sessions(frame, k)
    y = (frame["y_true"].to_numpy(dtype=float) >= 0.5)
    out: Dict[str, float] = {
        "k": int(k), "sessions": int(len(per)), "n_eff": len(per) / max(1, horizon),
        "rows": int(len(frame)), "buyable_share": float((~frame["at_ceiling"]).mean()),
        "ceiling_share": float(frame["at_ceiling_n"].mean()) if "at_ceiling_n" in frame else np.nan,
        "width": float(per["width"].mean()),
        "hit": float(per["hit"].mean()), "base": float(per["base"].mean()),
        "all_hit": float(per["all_hit"].mean()),
        "bret": float(per["bret"].mean()), "uret": float(per["uret"].mean()),
        "bret_ge_g": float((per["bret"] >= gain_pct / 100.0).mean()),
        # a session holding one class has no AUC; a split where every session does has none
        "daily_auc": float(np.nanmean(per["daily_auc"])) if per["daily_auc"].notna().any() else np.nan,
        "auc": float(roc_auc_score(y, frame["y_prob"])) if 0 < y.sum() < len(y) else np.nan,
    }
    out["lift"] = out["hit"] / out["base"] if out["base"] else np.nan
    if draws:
        null = null_hits(frame, k, draws, seed)
        sd = float(null.std(ddof=1))
        out.update(hit_null_mean=float(null.mean()), hit_bar=float(np.quantile(null, 0.95)),
                   hit_null_max=float(null.max()), null_draws=int(draws),
                   # a null with no spread (every session narrower than k) has no z
                   hit_z=float((out["hit"] - null.mean()) / sd) if sd > 1e-12 else np.nan,
                   hit_p=float((1 + (null >= out["hit"]).sum()) / (draws + 1)))
        out.update(auc_nulls(frame, draws, seed))
    out.update(trading(per, horizon))
    return out


# ------------------------------------------------------ variable-size basket
def capped(top: pd.DataFrame, per: pd.DataFrame, tau: float, score: str = "y_prob") -> pd.DataFrame:
    """Each session's basket cut at `tau`: the rows of `top` (its top k) scoring at least `tau`.

    `per` is `sessions(...)` of the full frame (buyable width, base rate, universe return). A
    session none of whose top k reaches `tau` holds no basket — cash — and `n` is 0.
    """
    sel = top[top[score] >= tau]
    g = sel.assign(hit=(sel["y_true"].astype(float) >= 0.5).astype(float)).groupby("date")
    cut = pd.DataFrame({"n": g.size(), "hits": g["hit"].sum(), "bret": g["ret"].mean(),
                        "p_mean": g[score].mean()})
    out = per.set_index("date")[["width", "base", "uret"]].join(cut, how="left")
    out["n"] = out["n"].fillna(0).astype(int)
    return out


def capped_metrics(cut: pd.DataFrame, tau: float, horizon: int, gain_pct: float,
                   cost: float = C.BASKET_COSTS[-1]) -> Dict[str, float]:
    active = cut[cut["n"] > 0]
    out: Dict[str, float] = {"tau": float(tau), "sessions": int(len(cut)), "active": int(len(active)),
                             "active_share": len(active) / max(1, len(cut)),
                             "size": float(active["n"].mean()) if len(active) else np.nan}
    if len(active) < 2:
        return out
    net = np.where(cut["n"] > 0, cut["bret"].fillna(0.0) - cost, 0.0)
    out.update(
        precision=float(active["hits"].sum() / active["n"].sum()),
        p_mean=float((active["p_mean"] * active["n"]).sum() / active["n"].sum()),
        base_active=float(active["base"].mean()), base_all=float(cut["base"].mean()),
        basket_ge_g=float((active["bret"] >= gain_pct / 100.0).mean()),
        loss_rate=float((active["bret"] < 0).mean()),
        bret=float(active["bret"].mean()), uret_active=float(active["uret"].mean()),
        ev=float(net.mean()), n_eff=len(active) / max(1, horizon))
    per_year = 252.0 / horizon
    sharpe, cagr, maxdd = [], [], []
    for offset in range(horizon):   # every start of the non-overlapping track
        r = net[offset::horizon]
        sd = r.std(ddof=1)
        sharpe.append(r.mean() / sd * np.sqrt(per_year) if sd > 0 else np.nan)
        curve = np.cumprod(1 + r)
        cagr.append(curve[-1] ** (per_year / len(r)) - 1)
        maxdd.append((curve / np.maximum.accumulate(curve) - 1).min())
    finite = [v for v in sharpe if np.isfinite(v)]
    out.update(sharpe=float(np.mean(finite)) if finite else np.nan,
               sharpe_min=float(np.min(finite)) if finite else np.nan,
               cagr=float(np.mean(cagr)), maxdd=float(np.mean(maxdd)))
    return out


def capped_null(frame: pd.DataFrame, cut: pd.DataFrame, gain_pct: float, draws: int,
                seed: int = 0) -> Dict[str, np.ndarray]:
    """`precision`, `bret`, `basket_ge_g` of baskets holding the SAME number of names per
    session as `cut`, drawn at random from that session's buyable names."""
    buy = frame[~frame["at_ceiling"]].sort_values(["date", "ticker"]).reset_index(drop=True)
    codes, dates = _sorted_codes(buy["date"])
    counts = np.bincount(codes, minlength=len(dates))
    n_t = np.minimum(cut["n"].reindex(dates).fillna(0).to_numpy().astype(int), counts)
    active = n_t > 0
    y = (buy["y_true"].to_numpy(dtype=float) >= 0.5).astype(float)
    r = np.nan_to_num(buy["ret"].to_numpy(dtype=float))
    starts = np.concatenate([[0], np.cumsum(counts)[:-1]])
    rng = np.random.default_rng(seed)
    out = {key: np.empty(draws) for key in ("precision", "bret", "basket_ge_g")}
    for i in range(draws):
        order = np.lexsort((rng.random(len(y)), codes))
        cs = codes[order]
        pick = (np.arange(len(cs)) - starts[cs]) < n_t[cs]
        hits = np.bincount(cs[pick], weights=y[order][pick], minlength=len(dates))
        rets = np.bincount(cs[pick], weights=r[order][pick], minlength=len(dates))[active] / n_t[active]
        out["precision"][i] = hits[active].sum() / n_t[active].sum()
        out["bret"][i] = rets.mean()
        out["basket_ge_g"][i] = (rets >= gain_pct / 100.0).mean()
    return out


def choose_min_prob(table: pd.DataFrame) -> float:
    """`config.BASKET_MIN_PROB_ON` over a VAL grid, among cuts trading at least
    `BASKET_MIN_ACTIVE` sessions; the first (lowest) cut wins a tie. 0 = no cut."""
    ok = table[(table["tau"] > 0) & (table["active"] >= C.BASKET_MIN_ACTIVE)]
    if ok.empty or C.BASKET_MIN_PROB_ON not in ok or ok[C.BASKET_MIN_PROB_ON].isna().all():
        return 0.0
    return float(ok.loc[ok[C.BASKET_MIN_PROB_ON].idxmax(), "tau"])


def _with_ceiling_fact(top: pd.DataFrame, market: pd.DataFrame) -> pd.DataFrame:
    if "at_ceiling_n" in top:
        return top
    fact = market[["date", "ticker", "at_ceiling"]].rename(columns={"at_ceiling": "at_ceiling_n"})
    return top.merge(fact, on=["date", "ticker"], how="left")


def min_prob_study(chain, val_frame: pd.DataFrame, test_frame: pd.DataFrame,
                   tops: Dict[str, pd.DataFrame], k: int, market: pd.DataFrame,
                   draws: int = C.BASKET_NULL_DRAWS) -> tuple:
    """`(table, min_prob, summary, lines)`: the cut chosen on VAL, then read on each test basket.

    `tops` holds each test basket's top-k rows (`baskets(...)`, or its saved CSV); the null and
    the universe come from `test_frame`, whose rows and labels every test basket shares.
    """
    h, g = chain.horizon, chain.event.gain_pct
    grid = (0.0,) + tuple(C.BASKET_MIN_PROB_GRID)
    val_per, val_top = sessions(val_frame, k), baskets(val_frame, k)
    rows = [{"split": "val", **capped_metrics(capped(val_top, val_per, t), t, h, g)} for t in grid]
    tau = choose_min_prob(pd.DataFrame(rows))
    val_row = next(r for r in rows if r["tau"] == tau)
    test_per = sessions(test_frame, k)
    summary: Dict[str, Dict] = {"min_prob": tau, "rule": C.BASKET_MIN_PROB_ON, "val": val_row}
    for label, top in tops.items():
        top = _with_ceiling_fact(top[top["rank"] <= k], market)
        rows += [{"split": f"test_{label}", **capped_metrics(capped(top, test_per, t), t, h, g)} for t in grid]
        for cut_at in dict.fromkeys((0.0, tau)):
            cut = capped(top, test_per, cut_at)
            m = capped_metrics(cut, cut_at, h, g)
            if draws and m["active"] >= 2:
                for key, arr in capped_null(test_frame, cut, g, draws).items():
                    sd = float(arr.std(ddof=1))
                    m.update({f"{key}_bar": float(np.quantile(arr, 0.95)), f"{key}_null_max": float(arr.max()),
                              f"{key}_z": float((m[key] - arr.mean()) / sd) if sd > 1e-12 else np.nan})
            free = capped(top[~top["at_ceiling_n"].fillna(False).astype(bool)], test_per, cut_at)
            m.update({f"no_ceiling_{a}": b for a, b in capped_metrics(free, cut_at, h, g).items()
                      if a in ("active", "precision", "bret", "sharpe")})
            m["ceiling_share"] = float(top.loc[top["y_prob"] >= cut_at, "at_ceiling_n"].fillna(False).astype(float).mean())
            active = cut[cut["n"] > 0]
            m["years"] = [{"year": int(y), "active": int(len(a)),
                           "precision": float(a["hits"].sum() / a["n"].sum()),
                           "bret": float(a["bret"].mean()),
                           "basket_ge_g": float((a["bret"] >= g / 100.0).mean())}
                          for y, a in active.groupby(active.index.year)]
            summary[label if cut_at == tau else f"{label}_uncut"] = m
    table = pd.DataFrame(rows)
    return table, tau, summary, _min_prob_lines(chain, table, tau, summary, k)


def _min_prob_lines(chain, table, tau, summary, k) -> List[str]:
    g, h = chain.event.gain_pct, chain.horizon
    v = summary["val"]
    lines = [
        f"## At most {k} names — a name under P(event) {tau:.2f} is not bought",
        "",
        f"The cut is chosen on VAL (`BASKET_MIN_PROB_ON = \"{C.BASKET_MIN_PROB_ON}\"`: the mean net return per "
        f"session at {C.BASKET_COSTS[-1] * 1e4:.0f} bps, a cash session returning 0, among cuts trading at least "
        f"{C.BASKET_MIN_ACTIVE} val sessions): **{tau:.2f}** — on val it traded {_f(v.get('active_share'))} of "
        f"sessions, {_f(v.get('size'), 2)} names each, precision {_f(v.get('precision'))} (base on those sessions "
        f"{_f(v.get('base_active'))}), EV {_f(v.get('ev'), 4, pct=True)} per session. A session none of whose top "
        f"{k} reaches the cut holds cash. Null: the same number of names per active session, drawn at random "
        f"({C.BASKET_NULL_DRAWS} draws). Sharpe/CAGR: one basket every {h} sessions, cash when idle, "
        f"mean over the {h} start offsets (the worst in brackets).",
        "",
        f"| test basket | cut | active (share) | names | **precision** (null p95 · z) | base on active | p_mean | "
        f"basket ≥ {g:g} % (null p95) | loss rate | basket ret (null p95) | universe ret | EV/session | "
        f"Sharpe@{C.BASKET_COSTS[-1] * 1e4:.0f} (worst) | CAGR | max DD | without ceiling names: precision · ret · Sharpe |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for key, m in summary.items():
        if not isinstance(m, dict) or key in ("val",):
            continue
        label = key.replace("_uncut", "")
        lines.append(
            f"| {label} | {m['tau']:.2f} | {m['active']} ({_f(m.get('active_share'))}) | {_f(m.get('size'), 2)} | "
            f"**{_f(m.get('precision'))}** ({_f(m.get('precision_bar'))} · {_f(m.get('precision_z'), 2)}) | "
            f"{_f(m.get('base_active'))} | {_f(m.get('p_mean'))} | {_f(m.get('basket_ge_g'))} "
            f"({_f(m.get('basket_ge_g_bar'))}) | {_f(m.get('loss_rate'))} | {_f(m.get('bret'), 4, pct=True)} "
            f"({_f(m.get('bret_bar'), 4, pct=True)}) | {_f(m.get('uret_active'), 4, pct=True)} | "
            f"{_f(m.get('ev'), 4, pct=True)} | {_f(m.get('sharpe'), 2)} ({_f(m.get('sharpe_min'), 2)}) | "
            f"{_f(m.get('cagr'), 3, pct=True)} | {_f(m.get('maxdd'), 3, pct=True)} | "
            f"{_f(m.get('no_ceiling_precision'))} · {_f(m.get('no_ceiling_bret'), 4, pct=True)} · "
            f"{_f(m.get('no_ceiling_sharpe'), 2)} |")
    lines += ["", "Per year, at the chosen cut:", ""]
    for key, m in summary.items():
        if isinstance(m, dict) and key not in ("val",) and not key.endswith("_uncut"):
            lines.append(f"- {key}: " + "; ".join(
                f"{y['year']} {y['active']} sessions, precision {y['precision']:.3f}, basket "
                f"{y['bret'] * 100:+.2f} %, ≥ {g:g} % on {y['basket_ge_g']:.2f}" for y in m["years"]))
    show = sorted({0.0, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, round(tau, 2)})
    lines += ["", "The grid (⚠️ DESCRIPTIVE on test: only the val-chosen cut is the result) — "
              "active share · precision · basket ret · Sharpe:", "",
              "| cut | " + " | ".join(table["split"].unique()) + " |",
              "|---|" + "---|" * table["split"].nunique()]
    for t in show:
        cells = []
        for split in table["split"].unique():
            r = table[(table["split"] == split) & (table["tau"].round(2) == t)]
            r = r.iloc[0] if len(r) else {}
            cells.append(f"{_f(r.get('active_share'), 2)} · {_f(r.get('precision'))} · "
                         f"{_f(r.get('bret'), 3, pct=True)} · {_f(r.get('sharpe'), 2)}")
        lines.append(f"| {t:.2f}{' ⬅' if t == round(tau, 2) else ''} | " + " | ".join(cells) + " |")
    lines += ["", "`min_prob.csv` holds the whole grid; `min_prob.json` the cut `--pick` applies.", ""]
    return lines


def _save_min_prob(output_dir: str, best, table: pd.DataFrame, tau: float, summary: Dict, k: int) -> None:
    import json

    table.to_csv(os.path.join(output_dir, "min_prob.csv"), index=False, float_format="%.6g")
    body = {"model": str(best["run_name"]), "top_k": int(k), **summary}
    with open(os.path.join(output_dir, "min_prob.json"), "w", encoding="utf-8") as fh:
        json.dump(body, fh, indent=2, default=float)


def _chosen_predictions(chain, best, runs_dir: Optional[str]) -> Dict[str, pd.DataFrame]:
    """The chosen row's frozen val/test predictions, re-assembled from its runs."""
    runs = R.model_runs(chain, R._runs_dir(runs_dir)).set_index("run_name")
    names = _members(best)
    missing = [n for n in dict.fromkeys(names) if n not in runs.index]
    if missing:
        raise ValueError(f"{best['run_name']}: no run for {missing} on the current dataset hash")
    out = {}
    for split in ("val", "test"):
        frames = [_predictions(runs.loc[n, "dir"], split) for n in names]
        out[split] = _geo(frames) if len(frames) > 1 else frames[0]
    return out


def study(chain, k: Optional[int] = None, runs_dir: Optional[str] = None) -> Dict:
    """Re-choose the cut from a written report's artefacts — no refit, no leaderboard."""
    k = int(k or chain.top_k)
    board = latest_board(chain)
    best = best_on_val(board)
    market = market_frame(chain)
    pred = _chosen_predictions(chain, best, runs_dir)
    val_frame, test_frame = attach(pred["val"], market), attach(pred["test"], market)
    tops = {"frozen": baskets(test_frame, k)}
    for label in ("refit", "rolling"):
        path = os.path.join(chain.output_dir, f"baskets_test_{label}.csv")
        if not os.path.exists(path):
            continue
        if C.BASKET_EXCLUDE_CEILING:
            # the saved baskets were cut from a ranking that still held the ceiling names;
            # dropping them here would shorten a basket instead of promoting the next name
            print(f"  {label}: skipped — its saved baskets predate the ceiling screen")
            continue
        top = pd.read_csv(path, parse_dates=["date"])
        if top.groupby("date").size().max() < k:
            raise ValueError(f"{path} holds fewer than {k} names per session")
        # ⚠️ the outcome columns are re-taken from the market: a saved basket carries the
        # return of the pricing that wrote it (`config.BASKET_ENTRY`)
        top = top.drop(columns=[c for c in ("ret", "y_true", "at_ceiling_n") if c in top]).merge(
            test_frame[["date", "ticker", "y_true", "ret", "at_ceiling_n"]], on=["date", "ticker"],
            how="left", validate="one_to_one")
        tops[label] = top
    table, tau, summary, lines = min_prob_study(chain, val_frame, test_frame, tops, k, market)
    _save_min_prob(chain.output_dir, best, table, tau, summary, k)
    path = os.path.join(chain.output_dir, "min_prob.md")
    head = [f"# The cut — `{str(best['run_name']).split('__')[0]}` on {chain.ticker}, top {k}", "",
            f"Entry `{C.BASKET_ENTRY}` "
            + ("(bought at the close of N, the label's own price)" if C.BASKET_ENTRY == "close"
               else f"(bought at the OPEN of N+1, sold at the close of N+{chain.horizon})")
            + ("; names at their ceiling on N are not bought." if C.BASKET_EXCLUDE_CEILING
               else "; names at their ceiling on N ARE bought."), ""]
    head += ["",
            "Re-chosen from the report's saved predictions and baskets (`--min-prob-study`); "
            "`report.md` carries the same section when the chain writes it.", ""]
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(head + lines) + "\n")
    print("\n".join(lines) + f"\nwritten: {path}")
    return {"min_prob": tau, "summary": summary, "table": table}


def _load_min_prob(chain, row) -> Optional[float]:
    import json

    path = os.path.join(chain.output_dir, "min_prob.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        body = json.load(fh)
    return float(body["min_prob"]) if body.get("model") == str(row["run_name"]) else None


# ------------------------------------------------------------ predictions
def _predictions(run_dir: str, split: str) -> pd.DataFrame:
    frame = pd.read_csv(os.path.join(run_dir, "results", f"predictions_{split}.csv"))
    if "ticker" not in frame.columns:
        raise ValueError(f"{run_dir} predictions carry no ticker — not a PANEL run (PNL-1)")
    return frame


def _geo(frames: List[pd.DataFrame]) -> pd.DataFrame:
    base = frames[0][["date", "ticker", "y_true"]].copy()
    for other in frames[1:]:
        same = (other["date"].astype(str).to_numpy() == base["date"].astype(str).to_numpy()).all() and \
               (other["ticker"].astype(str).to_numpy() == base["ticker"].astype(str).to_numpy()).all()
        if not same:
            raise ValueError("ensemble members scored different rows — their predictions are not aligned")
    base["y_prob"] = R._geo_mean([f["y_prob"].to_numpy(dtype=float) for f in frames])
    return base


# ----------------------------------------------------------------- board
def _row(chain, name: str, model_type: str, preds: Dict[str, pd.DataFrame], market, k) -> Dict:
    row = {"run_name": name, "model_type": model_type}
    for split, pred in preds.items():
        m = basket_metrics(attach(pred, market), k, chain.horizon, chain.event.gain_pct,
                           draws=C.BASKET_NULL_DRAWS if split == "test" else 0)
        row.update({f"{split}_{key}": v for key, v in m.items()})
    return row


def _selection_key(board: pd.DataFrame) -> pd.DataFrame:
    """The choice rule: `config.BASKET_CHOOSE_ON` first, the other two as tie-breaks."""
    keys = {"val_auc": ["val_auc", "val_daily_auc", "val_hit"],
            "val_daily_auc": ["val_daily_auc", "val_auc", "val_hit"],
            "val_hit": ["val_hit", "val_daily_auc", "val_auc"]}[C.BASKET_CHOOSE_ON]
    return board.sort_values(keys, ascending=[False] * 3, na_position="last").reset_index(drop=True)


def leaderboard(chain, market: pd.DataFrame, k: int, runs_dir: Optional[str] = None):
    """`(board, predictions)` — one row per run and fixed ensemble, sorted by the choice rule."""
    runs_dir = R._runs_dir(runs_dir)
    rows, preds = [], {}
    for _, run in R.model_runs(chain, runs_dir).iterrows():
        pair = {s: _predictions(run["dir"], s) for s in ("val", "test")}
        preds[run["run_name"]] = pair
        row = _row(chain, run["run_name"], run["model_type"], pair, market, k)
        row.update(run_id=run["run_id"], n_params=run.get("n_params"))
        rows.append(row)
        print(f"  scored {run['run_name'].split('__')[0]:<34} val hit {row['val_hit']:.3f} "
              f"test hit {row['test_hit']:.3f} (base {row['test_base']:.3f})")
    board = pd.DataFrame(rows)
    if board.empty:
        return board, preds
    prefix = board["run_name"].str.split("__").str[0]
    for name, members in (chain.ensembles or {}).items():
        hit = [board.loc[prefix == m, "run_name"] for m in members]
        if any(h.empty for h in hit):
            print(f"ensemble {name}: a member has no run on this dataset — skipped")
            continue
        names = [h.iloc[0] for h in hit]
        pair = {s: _geo([preds[n][s] for n in names]) for s in ("val", "test")}
        full = f"{name}__{chain.ticker.lower()}__{chain.table}"
        preds[full] = pair
        row = _row(chain, full, "ENSEMBLE", pair, market, k)
        row.update(run_id="", ensemble_members=",".join(names),
                   ensemble_run_ids=",".join(board.loc[board["run_name"].isin(names), "run_id"]),
                   n_params=float(pd.to_numeric(board.loc[board["run_name"].isin(names), "n_params"],
                                                errors="coerce").sum()))
        rows.append(row)
    # `val_auc`/`test_auc` are the POOLED ROC-AUC — the columns `trial` reads by their
    # single-ticker names; the basket's own ranking is `daily_auc`.
    return _selection_key(pd.DataFrame(rows)), preds


def best_on_val(board: pd.DataFrame) -> Optional[pd.Series]:
    eligible = board[~board["model_type"].isin(ELIGIBLE_EXCLUDED)]
    return None if eligible.empty else eligible.iloc[0]


# -------------------------------------------------------------- refitting
class Stacked:
    """The dataset's three splits in one float32 block, with dates, tickers and sessions."""

    def __init__(self, dataset, market_dates: pd.DatetimeIndex):
        splits = ("train", "val", "test")
        self.X = np.concatenate([getattr(dataset, f"X_{s}") for s in splits]).astype(np.float32, copy=False)
        self.y = np.concatenate([getattr(dataset, f"y_{s}") for s in splits]).astype(int)
        self.dates = np.concatenate([getattr(dataset, f"dates_{s}") for s in splits])
        self.tickers = np.concatenate([np.load(os.path.join(dataset.dir, f"tickers_{s}.npy"))
                                       for s in splits])
        self.split = np.concatenate([[s] * len(getattr(dataset, f"y_{s}")) for s in splits])
        self.aux = {c: np.concatenate([a[s] for s in splits]).astype(float)
                    for c, a in (getattr(dataset, "aux", {}) or {}).items()}
        self.session = market_dates.searchsorted(pd.to_datetime(self.dates))
        # ⚠️ The per-split tensors are released: the estimators read the dataset's metadata,
        # aux and scaler, never its X, and a 700k-row panel does not fit in memory twice.
        shape = dataset.X_train.shape
        for s in splits:
            setattr(dataset, f"X_{s}", np.empty((0,) + shape[1:], dtype=np.float32))

    def fit_score(self, spec, dataset, fit: np.ndarray, score: np.ndarray) -> np.ndarray:
        return R._fit_score(*spec, dataset, self.X, self.y, self.dates, self.aux, fit, score)

    def frame(self, mask: np.ndarray, prob: np.ndarray) -> pd.DataFrame:
        return pd.DataFrame({"date": self.dates[mask].astype(str), "ticker": self.tickers[mask],
                             "y_true": self.y[mask].astype(float), "y_prob": prob})


def _specs(chain, board, runs_dir, dataset, names) -> Dict[str, tuple]:
    out = {}
    for name in names:
        spec = R._estimator(chain, board, name, runs_dir, dataset)
        if spec is not None:
            out[name] = spec
    return out


def _ensemble_rows(chain, board, probs, stacked, mask, market, k, prefix) -> List[Dict]:
    rows = []
    for _, ens in board[board["model_type"] == "ENSEMBLE"].iterrows():
        members = str(ens["ensemble_members"]).split(",")
        if all(m in probs for m in members):
            p = R._geo_mean([probs[m] for m in members])
            m = basket_metrics(attach(stacked.frame(mask, p), market), k, chain.horizon,
                               chain.event.gain_pct)
            probs[ens["run_name"]] = p
            rows.append({"run_name": ens["run_name"], **{f"{prefix}_{a}": b for a, b in m.items()}})
    return rows


def refit_test(chain, board, stacked, dataset, market, k, runs_dir) -> tuple:
    """Every refittable run refitted ONCE on train+val and scored on test."""
    fit, test = stacked.split != "test", stacked.split == "test"
    gap = int(dataset.lookback + chain.purge_horizon - 1)
    first = stacked.session[test].min()
    fit &= stacked.session < first - gap
    probs, rows = {}, []
    for name, spec in _specs(chain, board, runs_dir, dataset, board["run_name"]).items():
        print(f"  refit on train+val: {name.split('__')[0]}")
        p = stacked.fit_score(spec, dataset, fit, test)
        probs[name] = p
        m = basket_metrics(attach(stacked.frame(test, p), market), k, chain.horizon, chain.event.gain_pct)
        rows.append({"run_name": name, **{f"refit_{a}": b for a, b in m.items()}})
    rows += _ensemble_rows(chain, board, probs, stacked, test, market, k, "refit")
    return pd.DataFrame(rows), probs


def _members(row) -> List[str]:
    return (str(row["ensemble_members"]).split(",") if row["model_type"] == "ENSEMBLE"
            else [str(row["run_name"])])


def rolling_test(chain, board, stacked, dataset, market, k, runs_dir, best,
                 every: int = C.BASKET_REFIT_SESSIONS) -> tuple:
    """TEST scored in blocks of `every` SESSIONS, refitted before each on all rows dated at
    least `d + h - 1` sessions earlier (train, val and the earlier test blocks).

    ⚠️ **ONLY THE VAL-CHOSEN ROW** (an ensemble: its members): a panel refit fits ~600k rows,
    and the rolling number that matters is the one of the model that would be used.
    """
    test = stacked.split == "test"
    gap = int(dataset.lookback + chain.purge_horizon - 1)
    sessions_ = np.unique(stacked.session[test])
    starts = sessions_[::every]
    types = dict(zip(board["run_name"], board["model_type"]))
    names = [n for n in _members(best) if not str(types.get(n)).startswith(C.BASKET_NO_ROLLING)]
    probs, rows = {}, []
    for name, spec in _specs(chain, board, runs_dir, dataset, names).items():
        p = np.full(int(test.sum()), np.nan)
        where = np.flatnonzero(test)
        for i, s0 in enumerate(starts):
            s1 = starts[i + 1] if i + 1 < len(starts) else np.inf
            block = test & (stacked.session >= s0) & (stacked.session < s1)
            fit = stacked.session < s0 - gap
            print(f"  rolling refit {i + 1}/{len(starts)}: {name.split('__')[0]} "
                  f"fit {int(fit.sum()):,} rows, score {int(block.sum()):,}")
            p[np.searchsorted(where, np.flatnonzero(block))] = stacked.fit_score(spec, dataset, fit, block)
        probs[name] = p
        m = basket_metrics(attach(stacked.frame(test, p), market), k, chain.horizon, chain.event.gain_pct)
        rows.append({"run_name": name, "rolling_refits": len(starts),
                     **{f"rolling_{a}": b for a, b in m.items()}})
    rows += _ensemble_rows(chain, board, probs, stacked, test, market, k, "rolling")
    return pd.DataFrame(rows), probs


def walk_forward(chain, board, best, stacked, dataset, market, k, runs_dir) -> pd.DataFrame:
    """Yearly expanding refits of the chosen row (an ensemble: each member) over the
    post-holdout years, each fold purged `d + h - 1` sessions."""
    holdout = pd.Timestamp(chain.holdout_start())
    gap = int(dataset.lookback + chain.purge_horizon - 1)
    names = _members(best)
    specs = _specs(chain, board, runs_dir, dataset, names)
    if set(specs) != set(names):
        return pd.DataFrame()
    dates = pd.to_datetime(stacked.dates)
    rows = []
    for year in sorted(set(dates[dates >= holdout].year)):
        score = np.asarray((dates >= max(holdout, pd.Timestamp(f"{year}-01-01")))
                           & (dates < pd.Timestamp(f"{year + 1}-01-01")))
        if score.sum() == 0:
            continue
        fit = stacked.session < stacked.session[score].min() - gap
        print(f"  walk-forward {year}: fit {int(fit.sum()):,} rows, score {int(score.sum()):,}")
        # a member listed twice is WEIGHTED twice (`config.BASKET_ENSEMBLES`), fitted once
        fitted = {n: stacked.fit_score(specs[n], dataset, fit, score) for n in dict.fromkeys(names)}
        probs = [fitted[n] for n in names]
        p = R._geo_mean(probs) if len(probs) > 1 else probs[0]
        m = basket_metrics(attach(stacked.frame(score, p), market), k, chain.horizon,
                           chain.event.gain_pct, draws=C.BASKET_NULL_DRAWS)
        rows.append({"model": str(best["run_name"]).split("__")[0], "year": year,
                     "train_n": int(fit.sum()), **m})
    return pd.DataFrame(rows)


# ------------------------------------------------------------------ write
def _f(v, digits=3, pct=False):
    if v is None or (isinstance(v, float) and not np.isfinite(v)):
        return "—"
    if isinstance(v, (int, np.integer)):
        return f"{int(v):,}"
    return f"{float(v) * 100:+.{digits - 2 if digits > 2 else 1}f} %" if pct else f"{float(v):.{digits}f}"


def write(chain, walkforward: bool = True, runs_dir: Optional[str] = None,
          output_dir: Optional[str] = None, selection_before: Optional[str] = None,
          k: Optional[int] = None) -> Dict:
    """Score every run as a basket, refit, write the report; returns what `trial.record` logs."""
    from model.common.data import load_dataset

    k = int(k or chain.top_k)
    runs_dir = R._runs_dir(runs_dir)
    output_dir = output_dir or chain.output_dir
    os.makedirs(output_dir, exist_ok=True)
    dataset = load_dataset(chain.creator().name)
    meta = dataset.meta or {}
    market = market_frame(chain)
    board, preds = leaderboard(chain, market, k, runs_dir)
    runs = chain.runs(before=selection_before)
    if board.empty:
        raise ValueError(f"no panel run scored on {dataset.name} ({dataset.hash})")
    best = best_on_val(board)
    split_counts = {s: len(getattr(dataset, f"y_{s}")) for s in ("train", "val", "test")}
    base_rates = {s: float(np.mean(getattr(dataset, f"y_{s}"))) for s in split_counts}
    stacked = Stacked(dataset, calendar(market))
    refit, refit_probs = refit_test(chain, board, stacked, dataset, market, k, runs_dir)
    if not refit.empty:
        board = board.merge(refit, on="run_name", how="left")
    rolling, rolling_probs = (rolling_test(chain, board, stacked, dataset, market, k, runs_dir, best)
                              if walkforward and best is not None else (pd.DataFrame(), {}))
    if not rolling.empty:
        board = board.merge(rolling, on="run_name", how="left")
    wf = (walk_forward(chain, board, best, stacked, dataset, market, k, runs_dir)
          if walkforward and best is not None else pd.DataFrame())
    board = _selection_key(board)
    board.to_csv(os.path.join(output_dir, "leaderboard.csv"), index=False)
    if not runs.empty:
        runs.drop(columns=["dir"]).to_csv(os.path.join(output_dir, "selection.csv"), index=False)
    if not wf.empty:
        wf.to_csv(os.path.join(output_dir, "walkforward.csv"), index=False)

    # THE ANSWER, per session: the chosen row's baskets on test, as fitted and as refitted.
    test_mask = stacked.split == "test"
    picks = {"frozen": attach(preds[best["run_name"]]["test"], market)}
    for label, probs in (("refit", refit_probs), ("rolling", rolling_probs)):
        if best["run_name"] in probs:
            picks[label] = attach(stacked.frame(test_mask, probs[best["run_name"]]), market)
    for label, frame in picks.items():
        baskets(frame, k).to_csv(os.path.join(output_dir, f"baskets_test_{label}.csv"), index=False,
                                 float_format="%.6g")
        sessions(frame, k).to_csv(os.path.join(output_dir, f"sessions_test_{label}.csv"), index=False,
                                  float_format="%.6g")
    # AT MOST k: the cut chosen on the chosen row's val scores, read on every test basket
    cut_table, min_prob, cut_summary, cut_lines = min_prob_study(
        chain, attach(preds[best["run_name"]]["val"], market), picks["frozen"],
        {label: baskets(frame, k) for label, frame in picks.items()}, k, market)
    _save_min_prob(output_dir, best, cut_table, min_prob, cut_summary, k)

    split = meta.get("split", {})
    g = chain.event.gain_pct
    lines = [
        f"# Basket report — {chain.ticker}: the top {k} names per session for `{chain.event.column}`",
        "",
        f"**Question:** at the close of session N, which names — at most **{k}** — will close "
        f"session N+{chain.horizon} at least **{g:g} %** higher? **Event:** {chain.event.describe()}. "
        f"**Table** `{chain.schema}.{chain.table}` → dataset `{dataset.name}` (hash `{dataset.hash}`), "
        f"{dataset.n_features} channels, purge {split.get('purge_gap_rows')} sessions. "
        + ("A name at its exchange ceiling on N is never bought (`PRF-0`)." if C.BASKET_EXCLUDE_CEILING
           else "A name at its exchange ceiling on N IS bought (`BASKET_EXCLUDE_CEILING = False`)."),
        "",
        "| split | label dates | rows | base rate |",
        "|---|---|---|---|",
    ]
    for s in ("train", "val", "test"):
        rng = split.get("date_ranges", {}).get(s, ["?", "?"])
        lines.append(f"| {s} | {rng[0]} → {rng[1]} | {split_counts[s]:,} | {base_rates[s]:.3f} |")
    lines += ["", f"Feature selection read only rows before **{chain.holdout_start()}** (the val start).", ""]
    if not runs.empty:
        lines += ["## Selection, one pool per run (within-date IC; null = shuffled re-selections)", "",
                  "| pool | channels | kept | CV IC | null p95 | null max | z | clears |",
                  "|---|---|---|---|---|---|---|---|"]
        for _, r in runs.sort_values("z", ascending=False, na_position="last").iterrows():
            lines.append(f"| {r['tables']} | {_f(r['channels'])} | {_f(r['kept'])} | {_f(r['ic'], 4)} | "
                         f"{_f(r['null_bar'], 4)} | {_f(r['null_max'], 4)} | {_f(r['z'], 2)} | "
                         f"{'yes' if r['clears'] else 'no'} |")
        lines.append("")
    lines += [
        f"## Leaderboard — chosen on `{C.BASKET_CHOOSE_ON}` (the other VAL metrics break a tie), test read once",
        "",
        f"`hit` = share of the basket that rose ≥ {g:g} %; `base` = the same share over all buyable "
        f"names; null = {C.BASKET_NULL_DRAWS} random baskets per session. Refit = train+val once; "
        f"rolling = refitted every {C.BASKET_REFIT_SESSIONS} test sessions.",
        "",
        "| model | val AUC | val daily AUC | val hit | **test AUC** | test daily AUC | refit AUC · daily | "
        "rolling AUC · daily | **test hit** | test base | null p95 / max · z | lift | "
        "all-hit | basket ret | universe ret | Sharpe@50 (CAGR) | refit hit (lift) | "
        "rolling hit (lift) | rolling Sharpe@50 (CAGR) |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for _, r in board.iterrows():
        lines.append(
            f"| `{str(r['run_name']).split('__')[0]}` | {_f(r.get('val_auc'))} | {_f(r.get('val_daily_auc'))} | "
            f"{_f(r.get('val_hit'))} | **{_f(r.get('test_auc'))}** | {_f(r.get('test_daily_auc'))} | "
            f"{_f(r.get('refit_auc'))} · {_f(r.get('refit_daily_auc'))} | "
            f"{_f(r.get('rolling_auc'))} · {_f(r.get('rolling_daily_auc'))} | "
            f"**{_f(r.get('test_hit'))}** | {_f(r.get('test_base'))} | {_f(r.get('test_hit_bar'))} / "
            f"{_f(r.get('test_hit_null_max'))} · {_f(r.get('test_hit_z'), 2)} | {_f(r.get('test_lift'), 2)} | "
            f"{_f(r.get('test_all_hit'))} | {_f(r.get('test_bret'), 4, pct=True)} | "
            f"{_f(r.get('test_uret'), 4, pct=True)} | "
            f"{_f(r.get('test_sharpe_50'), 2)} ({_f(r.get('test_cagr_50'), 3, pct=True)}) | "
            f"{_f(r.get('refit_hit'))} ({_f(r.get('refit_lift'), 2)}) | "
            f"{_f(r.get('rolling_hit'))} ({_f(r.get('rolling_lift'), 2)}) | "
            f"{_f(r.get('rolling_sharpe_50'), 2)} ({_f(r.get('rolling_cagr_50'), 3, pct=True)}) |"
        )
    b = board[board["run_name"] == best["run_name"]].iloc[0]
    lines += [
        "",
        f"**Chosen on val (`{C.BASKET_CHOOSE_ON}`):** `{str(b['run_name']).split('__')[0]}` — val AUC "
        f"{_f(b['val_auc'])}, within-session {_f(b['val_daily_auc'])}, hit@{k} {_f(b['val_hit'])}. "
        f"**Test AUC {_f(b['test_auc'])}** (within-session shuffle null p95 {_f(b.get('test_auc_bar'))}, "
        f"z {_f(b.get('test_auc_z'), 2)}; within-session AUC {_f(b['test_daily_auc'])}, null p95 "
        f"{_f(b.get('test_daily_auc_bar'))}, z {_f(b.get('test_daily_auc_z'), 2)}; refitted on train+val "
        f"{_f(b.get('refit_auc'))} · {_f(b.get('refit_daily_auc'))}; refitted every "
        f"{C.BASKET_REFIT_SESSIONS} sessions {_f(b.get('rolling_auc'))} · {_f(b.get('rolling_daily_auc'))}). "
        f"**On test, {_f(b['test_hit'])} of its basket names rose ≥ {g:g} %** against a buyable base rate "
        f"of {_f(b['test_base'])} (lift {_f(b['test_lift'], 2)}×) and a random-basket null p95 of "
        f"{_f(b['test_hit_bar'])} (max {_f(b['test_hit_null_max'])}, z {_f(b['test_hit_z'], 2)}); every "
        f"name hit on {_f(b['test_all_hit'])} of sessions; the basket's mean {chain.horizon}-session "
        f"return was {_f(b['test_bret'], 4, pct=True)} against the universe's {_f(b['test_uret'], 4, pct=True)}, "
        f"and the equal-weight basket itself was ≥ {g:g} % on {_f(b['test_bret_ge_g'])} of sessions. "
        f"Traded every {chain.horizon} sessions at 50 bps a round trip: Sharpe {_f(b.get('test_sharpe_50'), 2)}, "
        f"CAGR {_f(b.get('test_cagr_50'), 3, pct=True)}, max drawdown {_f(b.get('test_maxdd_50'), 3, pct=True)} "
        f"(universe Sharpe {_f(b.get('test_universe_sharpe'), 2)}, CAGR {_f(b.get('test_universe_cagr'), 3, pct=True)})."
        + (f" Refitted on train+val: hit {_f(b.get('refit_hit'))}, Sharpe@50 {_f(b.get('refit_sharpe_50'), 2)}."
           if pd.notna(b.get("refit_hit", np.nan)) else "")
        + (f" Refitted every {C.BASKET_REFIT_SESSIONS} sessions: hit {_f(b.get('rolling_hit'))} "
           f"(null p95 {_f(b.get('rolling_hit_bar'))}), Sharpe@50 {_f(b.get('rolling_sharpe_50'), 2)}, "
           f"CAGR {_f(b.get('rolling_cagr_50'), 3, pct=True)}." if pd.notna(b.get("rolling_hit", np.nan)) else ""),
        "",
        "The test baskets themselves — date, rank, ticker, score, outcome — are "
        "`baskets_test_{frozen,refit,rolling}.csv`; `python -m event_chain.basket --pick <date>` "
        "answers one session.",
        "",
    ]
    lines += cut_lines
    if not wf.empty:
        lines += [f"## Walk-forward — yearly expanding refits of `{str(b['run_name']).split('__')[0]}`", "",
                  "| year | train rows | sessions | hit | base | lift | null p95 · z | basket ret | "
                  "universe ret | daily AUC | Sharpe@50 |",
                  "|---|---|---|---|---|---|---|---|---|---|---|"]
        for _, r in wf.iterrows():
            lines.append(f"| {r['year']} | {int(r['train_n']):,} | {int(r['sessions'])} | {_f(r['hit'])} | "
                         f"{_f(r['base'])} | {_f(r['lift'], 2)} | {_f(r['hit_bar'])} · {_f(r['hit_z'], 2)} | "
                         f"{_f(r['bret'], 4, pct=True)} | {_f(r['uret'], 4, pct=True)} | "
                         f"{_f(r['daily_auc'])} | {_f(r.get('sharpe_50'), 2)} |")
        lines.append("")
    lines += [
        "## Read before quoting",
        "",
        f"- `n_eff` ≈ sessions / h: the test split's {int(b['test_sessions'])} sessions are "
        f"~{_f(b['test_n_eff'], 0)} independent baskets, and the trading track {int(b.get('test_periods', 0))} periods.",
        "- The null prices ONE run against random baskets; the grid is several runs and the kinds were "
        "chosen on VCB (NUL-1). Quote the val-chosen row.",
        "- The universe is survivors-only and its membership is not point-in-time (CLAUDE.md §2c): that "
        "protects the z, not the return.",
        "- A hit rate rewards volatile names; read the basket return beside it.",
    ]
    path = os.path.join(output_dir, "report.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return {"text": "\n".join(lines) + f"\n\nwritten: {path}", "path": path, "output_dir": output_dir,
            "board": board, "selection": runs, "walkforward": wf, "refit": refit, "rolling": rolling,
            "basket": {"top_k": k, "best": str(best["run_name"]), "min_prob": min_prob,
                       "min_prob_summary": cut_summary}}


# ------------------------------------------------------------------- pick
def _live_rows(chain, dataset, date: pd.Timestamp) -> pd.DataFrame:
    """Session `date`'s rows of the final table, imputed and scaled as the dataset was."""
    import joblib

    from feature_selection.unified_reader import UnifiedSchemaReader

    features = dataset.meta["features"]
    columns = list(features["feature_columns"])
    scaled = list(features.get("scaled_columns") or [])
    path = os.path.join(dataset.dir, "feature_medians.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"{path} is missing — the dataset predates saved medians; rebuild it with "
            f"`python -m event_chain --setup {chain.setup} --apply --stages dataset` (its hash does not move)")
    medians = pd.read_csv(path, index_col="channel")["median"]
    with UnifiedSchemaReader(chain.ticker) as reader:
        with reader.driver._cursor_ctx() as cur:
            cols = ", ".join(f'"{c}"' for c in ["date", "exchange", "ticker"] + columns)
            cur.execute(f"SELECT {cols} FROM {chain.schema}.{chain.table} WHERE date = %s", (date.date(),))
            rows = cur.fetchall()
    frame = pd.DataFrame(rows, columns=["date", "exchange", "ticker"] + columns)
    if frame.empty:
        raise ValueError(f"{chain.schema}.{chain.table} holds no row dated {date.date()}")
    values = frame[columns].apply(pd.to_numeric, errors="coerce").astype(float).fillna(medians[columns])
    scaler = dataset.feature_scaler or joblib.load(os.path.join(dataset.dir, "feature_scaler.pkl"))
    if scaled:
        values[scaled] = scaler.transform(values[scaled].to_numpy())
    frame = frame[["date", "exchange", "ticker"]].assign(date=pd.to_datetime(frame["date"]),
                                                          ticker=frame["ticker"].astype(str).str.upper())
    frame["X"] = list(values.to_numpy(dtype=np.float32))
    return frame


def latest_board(chain) -> pd.DataFrame:
    path = os.path.join(chain.output_dir, "leaderboard.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} — run `python -m event_chain --setup {chain.setup} --apply` first")
    return pd.read_csv(path)


def pick(chain, date: str, k: Optional[int] = None, model: Optional[str] = None,
         runs_dir: Optional[str] = None, write_csv: bool = True,
         min_prob: Optional[float] = None) -> pd.DataFrame:
    """The basket of session `date`: the chosen configuration refitted on the labels known
    by then, N's buyable names scored, the top `k` whose P(event) reaches `min_prob` returned
    (with outcomes when known) — possibly none. `min_prob=None` reads the val-chosen cut
    (`min_prob.json`) when it belongs to this row, else applies no cut."""
    from model.common.data import load_dataset

    k = int(k or chain.top_k)
    runs_dir = R._runs_dir(runs_dir)
    board = latest_board(chain)
    if model:
        hit = board[board["run_name"].str.split("__").str[0] == model]
        if hit.empty:
            raise ValueError(f"no row {model!r} on {chain.output_dir}/leaderboard.csv")
        row = hit.iloc[0]
    else:
        row = best_on_val(board)
    if min_prob is None:
        min_prob = _load_min_prob(chain, row)
        if min_prob is None:
            print("no val-chosen cut for this row (min_prob.json) — the basket is the plain top k")
    min_prob = float(min_prob or 0.0)
    dataset = load_dataset(chain.creator().name)
    market = market_frame(chain)
    cal = calendar(market)
    wanted = pd.Timestamp(date)
    at = int(cal.searchsorted(wanted, side="right")) - 1
    if at < 0:
        raise ValueError(f"{date} is before the universe's first session {cal[0].date()}")
    session = cal[at]
    if session != wanted:
        print(f"{wanted.date()} is not a session — using the last session before it, {session.date()}")
    live = _live_rows(chain, dataset, session)
    stacked = Stacked(dataset, cal)
    gap = int(dataset.lookback + chain.purge_horizon - 1)
    fit = stacked.session < at - gap
    if not fit.any():
        raise ValueError(f"no labelled row is {gap} sessions before {session.date()}")
    names = _members(row)
    specs = _specs(chain, board, runs_dir, dataset, names)
    if set(specs) != set(names):
        raise ValueError(f"{row['run_name']} is not refittable here (members {names})")
    X_live = np.stack(live["X"].to_numpy())[:, None, :]
    X_all = np.concatenate([stacked.X, X_live])
    y_all = np.concatenate([stacked.y, np.zeros(len(live), dtype=int)])
    dates_all = np.concatenate([stacked.dates, np.array([str(session.date())] * len(live), dtype=stacked.dates.dtype)])
    aux_all = {c: np.concatenate([a, np.full(len(live), np.nan)]) for c, a in stacked.aux.items()}
    fit_all = np.concatenate([fit, np.zeros(len(live), dtype=bool)])
    score_all = np.concatenate([np.zeros(len(stacked.y), dtype=bool), np.ones(len(live), dtype=bool)])
    fitted = {}
    for n in dict.fromkeys(names):
        print(f"fitting {n.split('__')[0]} on {int(fit.sum()):,} labelled rows dated <= "
              f"{cal[max(0, at - gap - 1)].date()}")
        fitted[n] = R._fit_score(*specs[n], dataset, X_all, y_all, dates_all, aux_all, fit_all, score_all)
    probs = [fitted[n] for n in names]   # a member listed twice is weighted twice
    live["p_event"] = R._geo_mean(probs) if len(probs) > 1 else probs[0]
    live = live.drop(columns=["X", "exchange"]).merge(
        market[["date", "ticker", "exchange", "at_ceiling", "ret", "close_adjust"]],
        on=["date", "ticker"], how="left")
    live["at_ceiling_n"] = live["at_ceiling"].fillna(False).astype(bool)
    live["at_ceiling"] = live["at_ceiling_n"] if C.BASKET_EXCLUDE_CEILING else False
    known = live["ret"].notna().any()
    live["hit"] = np.where(live["ret"].notna(), (live["ret"] >= chain.event.gain_pct / 100.0 - 1e-12).astype(float), np.nan)
    buyable = live[~live["at_ceiling"]].sort_values(["p_event", "ticker"], ascending=[False, True])
    top = buyable.head(k).reset_index(drop=True)
    top.insert(0, "rank", np.arange(1, len(top) + 1))
    top = top[["rank", "ticker", "exchange", "p_event", "close_adjust", "at_ceiling_n", "ret", "hit"]]
    out = top[top["p_event"] >= min_prob].reset_index(drop=True)
    entry_note = (f"buy at the close of N, sell at the close of N+{chain.horizon}"
                  if C.BASKET_ENTRY == "close" else
                  f"buy at the OPEN of the next session, sell at the close of N+{chain.horizon + 1}")
    print(f"\n{'=' * 78}\nBASKET for {session.date()} — {entry_note}; model "
          f"`{str(row['run_name']).split('__')[0]}` (val hit@{int(row.get('val_k', k))} "
          f"{row.get('val_hit', float('nan')):.3f}, test {row.get('test_hit', float('nan')):.3f})")
    print(f"universe {len(live)} names on the session, {int(live['at_ceiling_n'].sum())} at the ceiling "
          f"({'excluded' if C.BASKET_EXCLUDE_CEILING else 'buyable'}); mean P(event) {live['p_event'].mean():.3f}")
    print("entry " + ("`close`: the report prices this basket at TODAY'S CLOSE — a fill nobody reading "
                      "the close can get" if C.BASKET_ENTRY == "close" else
                      f"`next_open`: the label's own prices — the OPEN of the next session to the close of "
                      f"N+{chain.horizon + 1}, {chain.horizon} sessions of holding"))
    print(f"cut: P(event) >= {min_prob:.2f} — {len(out)} of the top {k} clear it"
          + ("; NO NAME CLEARS IT: hold cash this session" if out.empty else ""))
    with pd.option_context("display.width", 200):
        print(top.assign(buy=top["p_event"] >= min_prob).to_string(
            index=False, formatters={"p_event": "{:.3f}".format, "ret": "{:+.2%}".format}))
    if known and not out.empty:
        rate = float(buyable["hit"].mean())
        print(f"\nrealised: {int(out['hit'].sum())} of {len(out)} rose >= {chain.event.gain_pct:g} % "
              f"(buyable base rate that session {rate:.3f}); basket return {out['ret'].mean():+.2%}, "
              f"universe {buyable['ret'].mean():+.2%}")
    elif known:
        print(f"\nrealised: no basket (cash); the top {k} returned {top['ret'].mean():+.2%}, "
              f"universe {buyable['ret'].mean():+.2%}")
    else:
        print(f"\noutcome unknown: session N+{chain.horizon} has not closed in the data yet")
    if write_csv:
        folder = os.path.join(chain.output_dir, "picks")
        os.makedirs(folder, exist_ok=True)
        cut_tag = f"__p{min_prob:.2f}" if min_prob > 0 else ""
        path = os.path.join(folder, f"{session.date()}__top{k}{cut_tag}__{str(row['run_name']).split('__')[0]}.csv")
        out.to_csv(path, index=False, float_format="%.6g")
        print(f"written: {path}")
    return out


def main(argv: Optional[Sequence[str]] = None) -> None:
    from event_chain.chain import EventChain, _utf8

    _utf8()
    parser = argparse.ArgumentParser(prog="python -m event_chain.basket")
    parser.add_argument("--setup", default="basket", choices=[s for s, v in C.SETUPS.items() if "top_k" in v])
    parser.add_argument("--ticker", default=None, help="the panel universe (default: the setup's)")
    parser.add_argument("--pick", default=None, help="YYYY-MM-DD: the basket of that session")
    parser.add_argument("--top-k", type=int, default=None, help=f"names per basket (default {C.BASKET_SIZE})")
    parser.add_argument("--model", default=None, help="a leaderboard row (default: the val-chosen one)")
    parser.add_argument("--entry", choices=["close", "next_open"], default=None,
                        help="when the basket is bought (default: config.BASKET_ENTRY) — `next_open` "
                             "prices it at the open of N+1, which an evening scrape can trade")
    parser.add_argument("--min-prob", type=float, default=None,
                        help="the P(event) cut (default: the val-chosen one; 0 = always top-k)")
    parser.add_argument("--min-prob-study", action="store_true",
                        help="re-choose the cut on val from the written report's artefacts (no refit)")
    parser.add_argument("--report", action="store_true",
                        help="re-score the report without logging a trial (debugging)")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.entry:
        C.BASKET_ENTRY = args.entry
    chain = EventChain.from_setup(args.setup, ticker=args.ticker, top_k=args.top_k)
    if args.pick:
        pick(chain, args.pick, k=args.top_k, model=args.model, min_prob=args.min_prob)
    elif args.min_prob_study:
        study(chain, k=args.top_k)
    elif args.report:
        print(write(chain, k=args.top_k)["text"])
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
