"""Is finishing the filing OCR worth it FOR THE BASKET QUESTION? — measured, not argued.

    python .claude/tools/survey_ocr_value.py            # ~2 min, read-only, no GPU

The basket chain (`event_chain --setup basket`) asks which names rise >= 5 % in 5 sessions.
The OCR corpus (`raw_data/cafef/financials/statements/`) is the only source of company
fundamentals this repo may use (CLAUDE.md §5 rule 24). This tool answers three questions on
the rows where both exist — the VN30 panel (`unified_schema_vn30`, `up_5pct_5day`):

| question | measurement | null |
|---|---|---|
| **reach** — how much of the basket universe would OCR data touch? | LIQUID names with any parsed statement; name-sessions with a point-in-time profit figure | — |
| **dates** — is the event more likely right after a filing is published? | within-session difference in the event rate, names that published in the last 5 sessions vs the rest | the publisher flag shuffled within each session |
| **content** — do the parsed numbers rank the event within a session? | daily Spearman IC of year-on-year profit / revenue growth, and of the same growth among fresh publishers | the feature shuffled within each session |
| **increment** — does a model gain from them? | daily AUC and top-5 hit of an event logit on event features, with and without fundamentals, fitted before the chain's holdout and scored after it, paired by session | a session-block bootstrap of the paired difference |

⚠️ **POINT-IN-TIME BY `publish_date`** (the filing's own signature date, read from the PDF), +1
session; a quarter with none is available 45 calendar days after its end. Only `months == 3`
rows are quarterly values; a cumulative row is not a quarter.
⚠️ **The panel is VN30 because it is where OCR coverage is dense (34 parsed tickers, 29 of them
VN30).** 30 names is below the ~100-name width at which a rank signal was measured to survive
(CLAUDE.md §2) — so a null result here bounds the effect, it does not rule it out for LIQUID.
"""

from __future__ import annotations

import glob
import os
import sys

import numpy as np
import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(REPO, "src"))
STATEMENTS = os.path.join(REPO, "raw_data", "cafef", "financials", "statements")
HOLDOUT = pd.Timestamp("2021-05-04")   # the basket chain's val start (selection holdout)
DRAWS = 200
LABEL = "up_5pct_5day"

PROFIT = {"corp": "15_tong_loi_nhuan_ke_toan_truoc_thue", "bank": "xi_tong_loi_nhuan_truoc_thue",
          "insurance": "25_tong_loi_nhuan_ke_toan_truoc_thue",
          "securities": "ix_tong_loi_nhuan_ke_toan_truoc_thue"}
REVENUE = {"corp": "3_doanh_thu_thuan_ve_ban_hang_va_cung_cap_dich_vu", "bank": "tong_thu_nhap_hoat_dong",
           "insurance": "5_doanh_thu_thuan_tu_hdkd_bh", "securities": "cong_doanh_thu_hoat_dong"}


def statements() -> pd.DataFrame:
    frames = []
    for template in PROFIT:
        for path in glob.glob(os.path.join(STATEMENTS, template, "income_statement", "*.csv")):
            d = pd.read_csv(path, low_memory=False, encoding="utf-8-sig")
            d = d.assign(profit=pd.to_numeric(d.get(PROFIT[template]), errors="coerce"),
                         revenue=pd.to_numeric(d.get(REVENUE[template]), errors="coerce"))
            frames.append(d[["symbol", "year", "quarter", "source", "publish_date", "months",
                             "profit", "revenue"]])
    s = pd.concat(frames, ignore_index=True)
    s["ticker"] = s["symbol"].str.upper()
    quarterly = (s["source"] == "pdf") & (s["months"] == 3)
    s.loc[~quarterly, ["profit", "revenue"]] = np.nan
    end = pd.PeriodIndex.from_fields(year=s["year"], quarter=s["quarter"], freq="Q").end_time.normalize()
    pub = pd.to_datetime(s["publish_date"], errors="coerce")
    s["published"] = pub
    s["available"] = pub.fillna(pd.Series(end + pd.Timedelta(days=45), index=s.index))
    s = s.sort_values(["ticker", "year", "quarter"]).reset_index(drop=True)
    g = s.groupby("ticker")
    for col in ("profit", "revenue"):
        prior = g[col].shift(4)
        same = (g["year"].shift(4) == s["year"] - 1) & (g["quarter"].shift(4) == s["quarter"])
        growth = (s[col] - prior) / prior.abs()
        s[f"{col}_yoy"] = growth.where(same & (prior.abs() > 0)).clip(-3, 3)
    return s


def panel() -> pd.DataFrame:
    from feature_selection.unified_reader import UnifiedSchemaReader

    with UnifiedSchemaReader("VN30") as reader:
        y = reader.read("pool__targets", columns=["date", "ticker", LABEL, "return_5day"])
        evt = reader.read("pool__event_features")
    keep = [c for c in evt.columns if c.startswith(("evt_", "har_", "px_", "sec_", "flow_"))]
    p = y.merge(evt[["date", "ticker"] + keep], on=["date", "ticker"], how="inner")
    p["date"] = pd.to_datetime(p["date"])
    p["ticker"] = p["ticker"].str.upper()
    return p.dropna(subset=[LABEL]).sort_values(["date", "ticker"]).reset_index(drop=True), keep


def attach(p: pd.DataFrame, s: pd.DataFrame) -> pd.DataFrame:
    """Each (date, ticker) gets the newest quarter AVAILABLE by the prior session."""
    sessions = pd.DatetimeIndex(np.sort(p["date"].unique()))
    s = s.copy()
    # +1 session: a filing signed on day D is traded on from the session after D at the earliest
    pos = sessions.searchsorted(s["available"], side="right")
    s = s[pos < len(sessions)].copy()
    s["from"] = sessions[pos[pos < len(sessions)]]
    left = p.sort_values("date")
    right = s.sort_values("from")[["ticker", "from", "published", "profit", "revenue",
                                   "profit_yoy", "revenue_yoy"]]
    out = pd.merge_asof(left, right, left_on="date", right_on="from", by="ticker", direction="backward")
    pub_pos = sessions.searchsorted(out["published"])
    date_pos = sessions.searchsorted(out["date"])
    out["sessions_since_pub"] = np.where(out["published"].notna(), date_pos - pub_pos, np.nan)
    out["fresh"] = (out["sessions_since_pub"] >= 1) & (out["sessions_since_pub"] <= 5)
    return out.sort_values(["date", "ticker"]).reset_index(drop=True)


def _codes(dates):
    return pd.factorize(dates, sort=True)[0]


def within_date_contrast(df, flag, y, draws=DRAWS, seed=0, step=1):
    """Mean over sessions (holding both groups) of rate(flag) - rate(not flag), and its null.

    ⚠️ `step=5` keeps every 5th session only: the label spans 5 sessions and a publication
    flag persists 5, so consecutive sessions are not independent and a within-session
    shuffle over ALL of them draws a null that is too narrow by ~sqrt(5).
    """
    rng = np.random.default_rng(seed)
    codes = _codes(df["date"])
    if step > 1:
        df = df[codes % step == 0]
        codes = _codes(df["date"])
    f = df[flag].to_numpy(bool)
    v = df[y].to_numpy(float)

    def stat(fl):
        n1 = np.bincount(codes, weights=fl)
        n0 = np.bincount(codes, weights=~fl)
        s1 = np.bincount(codes, weights=v * fl)
        s0 = np.bincount(codes, weights=v * ~fl)
        ok = (n1 > 0) & (n0 > 0)
        return float(np.mean(s1[ok] / n1[ok] - s0[ok] / n0[ok])), int(ok.sum()), float(n1[ok].sum())

    obs, n_dates, n_flag = stat(f)
    null = []
    order = np.argsort(codes, kind="stable")
    for _ in range(draws):
        perm = order[np.lexsort((rng.random(len(codes)), codes[order]))]
        g = np.empty_like(f)
        g[order] = f[perm]
        null.append(stat(g)[0])
    null = np.asarray(null)
    return {"diff": obs, "sessions": n_dates, "flagged_rows": n_flag,
            "null_p95": float(np.quantile(null, 0.95)), "null_p05": float(np.quantile(null, 0.05)),
            "z": float((obs - null.mean()) / null.std(ddof=1))}


def _corr_by_code(codes, x, y, n):
    """Pearson correlation of x and y within each code, vectorised."""
    c = np.bincount(codes, minlength=n).astype(float)
    sx, sy = np.bincount(codes, x, n), np.bincount(codes, y, n)
    sxx, syy, sxy = np.bincount(codes, x * x, n), np.bincount(codes, y * y, n), np.bincount(codes, x * y, n)
    with np.errstate(invalid="ignore", divide="ignore"):
        cov = sxy / c - sx * sy / c ** 2
        vx, vy = sxx / c - (sx / c) ** 2, syy / c - (sy / c) ** 2
        r = cov / np.sqrt(vx * vy)
    r[(vx <= 1e-12) | (vy <= 1e-12)] = np.nan
    return r


def daily_ic(df, feature, y, min_width=8, draws=DRAWS, seed=0):
    """Mean over sessions of the within-session Spearman IC, and its within-session shuffle null."""
    rng = np.random.default_rng(seed)
    d = df.dropna(subset=[feature, y])
    width = d.groupby("date")[feature].transform("size")
    d = d[width >= min_width].sort_values("date")
    if d.empty:
        return {"ic": np.nan, "sessions": 0}
    codes = _codes(d["date"])
    n = int(codes.max()) + 1
    fx = d.groupby("date")[feature].rank().to_numpy(float)
    fy = d.groupby("date")[y].rank().to_numpy(float)
    per = _corr_by_code(codes, fx, fy, n)
    null = []
    for _ in range(draws):
        order = np.lexsort((rng.random(len(codes)), codes))
        null.append(np.nanmean(_corr_by_code(codes, fx[order], fy, n)))
    null = np.asarray(null)
    ok = np.isfinite(per)
    n_eff = ok.sum() / 5.0
    return {"ic": float(np.nanmean(per)), "sessions": int(ok.sum()),
            "t_neff": float(np.nanmean(per) / np.nanstd(per, ddof=1) * np.sqrt(n_eff)),
            "null_p95": float(np.quantile(null, 0.95)), "null_max": float(null.max()),
            "z": float((np.nanmean(per) - null.mean()) / null.std(ddof=1)),
            "width": float(np.bincount(codes)[ok].mean())}


def ticker_permutation_ic(df, feature, y, min_width=8, draws=DRAWS, seed=0):
    """Daily IC against a null that KEEPS each series' persistence: every draw hands each
    ticker's whole `feature` history to another ticker. A quarterly figure is constant for
    ~60 sessions, so a within-session shuffle breaks exactly the autocorrelation that makes
    its daily ICs dependent, and its null is far too narrow."""
    rng = np.random.default_rng(seed)
    X = df.pivot(index="date", columns="ticker", values=feature)
    Y = df.pivot(index="date", columns="ticker", values=y).reindex_like(X)

    def mean_ic(Xm):
        valid = Xm.notna() & Y.notna()
        rx = Xm.where(valid).rank(axis=1).to_numpy(float)
        ry = Y.where(valid).rank(axis=1).to_numpy(float)
        v = valid.to_numpy()
        n = v.sum(axis=1).astype(float)
        mx = np.nansum(rx, axis=1) / n
        my = np.nansum(ry, axis=1) / n
        cov = np.nansum((rx - mx[:, None]) * (ry - my[:, None]), axis=1)
        sx = np.sqrt(np.nansum((rx - mx[:, None]) ** 2, axis=1))
        sy = np.sqrt(np.nansum((ry - my[:, None]) ** 2, axis=1))
        with np.errstate(invalid="ignore", divide="ignore"):
            r = cov / (sx * sy)
        r[(n < min_width) | (sx == 0) | (sy == 0)] = np.nan
        return r

    per = mean_ic(X)
    cols = X.columns.to_numpy()
    null = []
    for _ in range(draws):
        Xp = X.copy()
        Xp.columns = rng.permutation(cols)
        null.append(np.nanmean(mean_ic(Xp.reindex(columns=cols))))
    null = np.asarray(null)
    obs = float(np.nanmean(per))
    return {"ic": obs, "sessions": int(np.isfinite(per).sum()),
            "ticker_null_p95": float(np.quantile(null, 0.95)),
            "ticker_null_p05": float(np.quantile(null, 0.05)),
            "ticker_null_max_abs": float(np.abs(null).max()),
            "ticker_null_z": float((obs - null.mean()) / null.std(ddof=1))}


def increment(df, base_cols, extra_cols, seed=0):
    """Event logit on `base_cols` vs `base_cols + extra_cols`: fit before HOLDOUT, score after."""
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score
    from sklearn.preprocessing import StandardScaler

    d = df.copy()
    for c in extra_cols:
        d[c + "_missing"] = d[c].isna().astype(float)
    extra = list(extra_cols) + [c + "_missing" for c in extra_cols]
    train, test = d["date"] < HOLDOUT - pd.Timedelta(days=10), d["date"] >= HOLDOUT
    out = {}
    for name, cols in (("event", list(base_cols)), ("event+fund", list(base_cols) + extra)):
        X = d[cols].astype(float)
        X = X.fillna(X[train].median())
        sc = StandardScaler().fit(X[train])
        Z = np.clip(sc.transform(X), -5, 5)
        m = LogisticRegression(C=0.03, max_iter=5000).fit(Z[train], d.loc[train, LABEL].astype(int))
        out[name] = m.decision_function(Z[test])
    t = d.loc[test, ["date", "ticker", LABEL, "return_5day"]].reset_index(drop=True)
    rows = []
    for name, p in out.items():
        t["p"] = p
        g = t.sort_values(["date", "p"], ascending=[True, False]).groupby("date")
        top = g.head(5).groupby("date")[LABEL].mean()
        auc = t.groupby("date")[[LABEL, "p"]].apply(
            lambda x: np.nan if x[LABEL].nunique() < 2 else roc_auc_score(x[LABEL], x["p"]))
        rows.append(pd.DataFrame({f"hit5_{name}": top, f"auc_{name}": auc}))
    per = pd.concat(rows, axis=1)
    per["base"] = t.groupby("date")[LABEL].mean()
    rng = np.random.default_rng(seed)
    blocks = np.arange(len(per)) // 5
    members = [np.flatnonzero(blocks == k) for k in np.unique(blocks)]
    res = {"sessions": len(per), "base": float(per["base"].mean())}
    for metric in ("hit5", "auc"):
        a, b = per[f"{metric}_event"], per[f"{metric}_event+fund"]
        diff = (b - a).to_numpy()
        boot = []
        for _ in range(2000):
            idx = np.concatenate([members[k] for k in rng.integers(0, len(members), len(members))])
            boot.append(np.nanmean(diff[idx]))
        res.update({f"{metric}_event": float(a.mean()), f"{metric}_event_fund": float(b.mean()),
                    f"{metric}_diff": float(np.nanmean(diff)),
                    f"{metric}_diff_ci": [float(np.quantile(boot, 0.025)), float(np.quantile(boot, 0.975))]})
    return res


def reach(s: pd.DataFrame) -> dict:
    from feature_selection.unified_reader import UnifiedSchemaReader

    with UnifiedSchemaReader("LIQUID") as reader:
        liquid = reader.read("pool__targets", columns=["date", "ticker"])
    liquid["date"] = pd.to_datetime(liquid["date"])
    liquid["ticker"] = liquid["ticker"].str.upper()
    parsed = set(s.loc[s["profit"].notna(), "ticker"])
    names = set(liquid["ticker"])
    cov = pd.read_csv(os.path.join(REPO, "src", "kaggle_gpu", "pdf_ocr_coverage.csv"))
    cov["ticker"] = cov["ticker"].str.upper()
    in_liquid = cov[cov["ticker"].isin(names)]
    attached = attach(liquid.assign(**{LABEL: 0.0, "return_5day": 0.0}), s)
    return {
        "liquid_names": len(names), "liquid_name_sessions": int(len(liquid)),
        "names_with_a_parsed_profit": len(names & parsed),
        "name_sessions_with_a_point_in_time_profit": float(attached["profit"].notna().mean()),
        "liquid_filings_listed": int(in_liquid["total"].sum()),
        "liquid_filings_ocred": int(in_liquid["ocred"].sum()),
        "liquid_names_with_any_ocr": int((in_liquid["ocred"] > 0).sum()),
    }


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    s = statements()
    p, evt_cols = panel()
    df = attach(p, s)
    covered = df[df["ticker"].isin(set(s.loc[s["profit"].notna(), "ticker"]))].copy()
    print(f"VN30 panel {len(p):,} rows, {p['ticker'].nunique()} names; covered by OCR: "
          f"{covered['ticker'].nunique()} names, {covered['profit'].notna().mean():.1%} of their rows "
          f"hold a point-in-time quarterly profit")
    out = {"reach": reach(s)}
    print("\n# reach\n", pd.Series(out["reach"]).to_string())

    out["dates"] = {
        "event_rate_fresh_minus_rest": within_date_contrast(covered, "fresh", LABEL),
        "event_rate_fresh_minus_rest_every5": within_date_contrast(covered, "fresh", LABEL, step=5),
        "return_5d_fresh_minus_rest": within_date_contrast(covered, "fresh", "return_5day"),
        "return_5d_fresh_minus_rest_every5": within_date_contrast(covered, "fresh", "return_5day", step=5),
        "base_rate": float(covered[LABEL].mean()),
    }
    print("\n# dates (published 1-5 sessions ago vs not, same session)\n", pd.DataFrame(out["dates"]).T.to_string()
          if False else out["dates"])

    ref = "har_rv_5" if "har_rv_5" in df.columns else next(c for c in evt_cols if c.startswith("har_"))
    content = {}
    for feature in ("profit_yoy", "revenue_yoy", "sessions_since_pub", ref, "evt_thr_z_20"):
        if feature in covered.columns:
            content[feature] = daily_ic(covered, feature, LABEL)
    fresh = covered[covered["fresh"]]
    content["profit_yoy|fresh"] = daily_ic(fresh, "profit_yoy", LABEL, min_width=3)
    content["profit_yoy->return_5day"] = daily_ic(covered, "profit_yoy", "return_5day")
    for feature in ("profit_yoy", "revenue_yoy", "sessions_since_pub", ref, "evt_thr_z_20"):
        content[feature].update(ticker_permutation_ic(covered, feature, LABEL))
    out["content"] = content
    with pd.option_context("display.width", 250):
        print("\n# content (daily Spearman IC vs the event, VN30 covered names)\n",
              pd.DataFrame(content).T.round(4).to_string())

    base = [c for c in evt_cols if c.startswith(("evt_", "har_", "px_"))]
    extra = ["profit_yoy", "revenue_yoy", "sessions_since_pub"]
    covered["sessions_since_pub"] = covered["sessions_since_pub"].clip(upper=80)
    out["increment"] = increment(covered, base, extra)
    print("\n# increment (event logit, fit < holdout, scored >= holdout, paired by session)\n",
          out["increment"])


if __name__ == "__main__":
    main()
