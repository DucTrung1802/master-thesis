"""
Deep-learning shoot-out on the POOLED VN100 panel for the "next 5 trading days
up >= 5%" signal -- the data-rich setting where DL has a real shot vs gradient
boosting (cf. dl_model_comparison_vcb.py, where DL lost on VCB alone).

Universe: the 100 VN100 constituents (95 present in gold_schema.stocks; missing
DSE, KOS, NAB, SIP, VPI are recent listings). ~279k stock-days.

Pipeline (leakage-guarded):
  * label per ticker:  y[t] = 1 if close[t+5]/close[t]-1 >= 0.05
  * global DATE cutoff -> train = older dates, test = newer dates (no look-ahead)
  * features standardized PER TICKER using that ticker's pre-cutoff stats
  * top-K features chosen by univariate AUC on the pooled TRAIN rows only
  * W-day windows built per ticker (never crossing ticker boundaries)
Models: GBM-full baseline, MLP, LSTM, GRU, CNN1D, Transformer, DL ensemble.
Output: dl_vn100_pooled_results.csv + console.
"""

import os
import warnings
import numpy as np
import pandas as pd
import psycopg2
import torch
import torch.nn as nn
from dotenv import load_dotenv
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.ensemble import HistGradientBoostingClassifier

warnings.filterwarnings("ignore")
torch.manual_seed(0)
np.random.seed(0)

HORIZON, GAIN = 5, 0.05
WINDOW, TOPK, WARMUP = 20, 64, 120
EPOCHS, PATIENCE, BATCH = 40, 8, 512
CUTOFF_Q = 0.80
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_CSV = os.path.join(HERE, "dl_vn100_pooled_results.csv")
ID_COLS = {"exchange", "ticker", "date", "target"}

VN100 = ["ACB","ANV","BCM","BID","BMP","BSI","BVH","BWE","CII","CMG","CTD","CTG",
"CTR","CTS","DBC","DCM","DGC","DGW","DIG","DPM","DSE","DXG","DXS","EIB","EVF","FPT",
"FRT","FTS","GAS","GEE","GEX","GMD","GVR","HAG","HCM","HDB","HDC","HDG","HHV","HPG",
"HSG","HT1","IMP","KBC","KDC","KDH","KOS","LPB","MBB","MSB","MSN","MWG","NAB","NKG",
"NLG","NT2","OCB","PAN","PC1","PDR","PHR","PLX","PNJ","POW","PPC","PTB","PVD","PVT",
"REE","SAB","SBT","SCS","SHB","SIP","SJS","SSB","SSI","STB","SZC","TCB","TCH","TLG",
"TPB","VCB","VCG","VCI","VGC","VHC","VHM","VIB","VIC","VIX","VJC","VND","VNM","VPB",
"VPI","VRE","VSC","VTP"]


def load_panel():
    load_dotenv(os.path.join(HERE, "..", "..", "..", ".env"))
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")
    df = pd.read_sql(
        "SELECT * FROM gold_schema.stocks "
        "WHERE exchange='HOSE' AND ticker = ANY(%s) ORDER BY ticker, date",
        conn, params=(VN100,))
    conn.close()
    df["date"] = pd.to_datetime(df["date"])
    return df


def build(df):
    feat = [c for c in df.columns if c not in ID_COLS]
    for c in feat:
        if df[c].dtype == bool:
            df[c] = df[c].astype(float)
    feat = [c for c in feat if np.issubdtype(df[c].dtype, np.number)]

    cutoff = df["date"].quantile(CUTOFF_Q)
    std_frames, labels, dates, tickers = [], [], [], []

    for tk, g in df.groupby("ticker", sort=False):
        g = g.sort_values("date")
        if len(g) < WARMUP + 60:
            continue
        close = g["close"].astype(float).values
        y = np.full(len(close), np.nan)
        y[:-HORIZON] = (close[HORIZON:] / close[:-HORIZON] - 1.0 >= GAIN).astype(float)

        X = g[feat].astype(float).ffill().fillna(0.0)
        pre = g["date"].values < np.datetime64(cutoff)
        ref = X[pre] if pre.sum() >= 50 else X
        mu, sd = ref.mean(), ref.std().replace(0, 1)
        Xs = ((X - mu) / sd).astype(np.float32)

        sl = slice(WARMUP, len(close) - HORIZON)          # drop warm-up + unlabeled tail
        std_frames.append(Xs.iloc[sl])
        labels.append(y[sl])
        dates.append(g["date"].values[sl])
        tickers.append(np.full(sl.stop - sl.start, tk))

    X = pd.concat(std_frames, ignore_index=True)
    y = np.concatenate(labels)
    d = pd.to_datetime(np.concatenate(dates))
    tk = np.concatenate(tickers)
    return X, y, d, tk, cutoff, feat


def select_topk(X, y, train_mask):
    ytr = y[train_mask].astype(int)
    scores = []
    for c in X.columns:
        col = X[c].values[train_mask]
        ok = ~np.isnan(col)
        if ok.sum() < 1000 or len(np.unique(col[ok])) < 5:
            continue
        scores.append((c, abs(roc_auc_score(ytr[ok], col[ok]) - 0.5)))
    return [c for c, _ in sorted(scores, key=lambda z: -z[1])[:TOPK]]


def make_windows(X, tk):
    """Per-ticker W-day windows; row i ends at row i (windows never cross ticker)."""
    arr = X.values.astype(np.float32)
    n, k = arr.shape
    seq = np.zeros((n, WINDOW, k), dtype=np.float32)
    start = 0
    for t in pd.unique(tk):
        idx = np.where(tk == t)[0]
        a, b = idx[0], idx[-1] + 1
        block = arr[a:b]
        for j in range(len(block)):
            s = max(0, j - WINDOW + 1)
            w = block[s:j + 1]
            seq[a + j, WINDOW - len(w):] = w
    return seq


# ── models (same as the VCB shoot-out) ────────────────────────────────────────
class MLP(nn.Module):
    def __init__(s, k):
        super().__init__()
        s.net = nn.Sequential(nn.Linear(k, 128), nn.ReLU(), nn.Dropout(0.4),
                              nn.Linear(128, 64), nn.ReLU(), nn.Dropout(0.3), nn.Linear(64, 1))
    def forward(s, x): return s.net(x[:, -1, :]).squeeze(-1)


class RNN(nn.Module):
    def __init__(s, k, kind="lstm"):
        super().__init__()
        s.rnn = (nn.LSTM if kind == "lstm" else nn.GRU)(k, 64, batch_first=True)
        s.head = nn.Sequential(nn.Dropout(0.3), nn.Linear(64, 1))
    def forward(s, x):
        out, _ = s.rnn(x); return s.head(out[:, -1, :]).squeeze(-1)


class CNN1D(nn.Module):
    def __init__(s, k):
        super().__init__()
        s.conv = nn.Sequential(nn.Conv1d(k, 64, 3, padding=1), nn.ReLU(),
                               nn.Conv1d(64, 64, 3, padding=1), nn.ReLU(), nn.AdaptiveMaxPool1d(1))
        s.head = nn.Sequential(nn.Dropout(0.3), nn.Linear(64, 1))
    def forward(s, x):
        return s.head(s.conv(x.transpose(1, 2)).squeeze(-1)).squeeze(-1)


class TransformerNet(nn.Module):
    def __init__(s, k, d=64):
        super().__init__()
        s.emb = nn.Linear(k, d)
        s.pos = nn.Parameter(torch.randn(1, WINDOW, d) * 0.02)
        layer = nn.TransformerEncoderLayer(d, 4, d * 2, 0.3, batch_first=True)
        s.enc = nn.TransformerEncoder(layer, 2)
        s.head = nn.Sequential(nn.LayerNorm(d), nn.Linear(d, 1))
    def forward(s, x):
        return s.head(s.enc(s.emb(x) + s.pos).mean(1)).squeeze(-1)


def train_eval(model, Xtr, ytr, Xva, yva, Xte, pos_weight):
    model = model.to(DEVICE)
    opt = torch.optim.Adam(model.parameters(), lr=1e-3, weight_decay=1e-4)
    loss_fn = nn.BCEWithLogitsLoss(pos_weight=torch.tensor(pos_weight, device=DEVICE))
    Xtr_t = torch.tensor(Xtr); ytr_t = torch.tensor(ytr)
    Xva_t = torch.tensor(Xva, device=DEVICE)
    best, best_state, wait = -1, None, 0
    for ep in range(EPOCHS):
        model.train()
        perm = torch.randperm(len(Xtr_t))
        for i in range(0, len(perm), BATCH):
            idx = perm[i:i + BATCH]
            opt.zero_grad()
            loss = loss_fn(model(Xtr_t[idx].to(DEVICE)), ytr_t[idx].to(DEVICE))
            loss.backward(); opt.step()
        model.eval()
        with torch.no_grad():
            vp = torch.sigmoid(model(Xva_t)).cpu().numpy()
        auc = roc_auc_score(yva, vp) if len(np.unique(yva)) > 1 else 0.5
        if auc > best:
            best, best_state, wait = auc, {k: v.cpu().clone() for k, v in model.state_dict().items()}, 0
        else:
            wait += 1
            if wait >= PATIENCE: break
    model.load_state_dict(best_state); model.eval()
    out = []
    with torch.no_grad():
        for i in range(0, len(Xte), 4096):
            out.append(torch.sigmoid(model(torch.tensor(Xte[i:i+4096], device=DEVICE))).cpu().numpy())
    return np.concatenate(out)


def metrics(name, y_true, p):
    base = y_true.mean(); k = max(1, len(p) // 10)
    prec = y_true[np.argsort(p)[-k:]].mean()
    return {"model": name, "test_auc": round(roc_auc_score(y_true, p), 3),
            "test_pr_auc": round(average_precision_score(y_true, p), 3),
            "top_decile_prec": round(float(prec), 3),
            "lift": round(float(prec) / max(base, 1e-9), 2)}


def main():
    print(f"device: {DEVICE}")
    df = load_panel()
    X, y, d, tk, cutoff, feat = build(df)
    del df

    lab = ~np.isnan(y)
    test_mask = (d.values >= np.datetime64(cutoff)) & lab
    val_cut = pd.Series(d[d.values < np.datetime64(cutoff)]).quantile(0.9)
    train_mask = (d.values < np.datetime64(val_cut)) & lab
    val_mask = (d.values >= np.datetime64(val_cut)) & (d.values < np.datetime64(cutoff)) & lab
    print(f"pooled rows {lab.sum():,} | train {train_mask.sum():,} / "
          f"val {val_mask.sum():,} / test {test_mask.sum():,} "
          f"| cutoff {pd.Timestamp(cutoff).date()} | test base {y[test_mask].mean():.1%}")

    topk = select_topk(X, y, train_mask)
    print(f"top-{TOPK} signals (univariate, train): {', '.join(topk[:12])} ...")

    # GBM baseline on FULL point-in-time features
    Xf = X.values.astype(np.float32)
    gbm = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05,
        max_leaf_nodes=31, l2_regularization=1.0, random_state=0)
    gbm.fit(Xf[train_mask | val_mask], y[train_mask | val_mask].astype(int))
    yte = y[test_mask].astype(int)
    rows = [metrics("GBM-full (baseline)", yte, gbm.predict_proba(Xf[test_mask])[:, 1])]
    print(f"  GBM-full     test_auc={rows[0]['test_auc']}")
    del Xf

    seq = make_windows(X[topk], tk)
    Xtr, ytr = seq[train_mask], y[train_mask].astype(np.float32)
    Xva, yva = seq[val_mask], y[val_mask].astype(int)
    Xte = seq[test_mask]
    pos_weight = (ytr == 0).sum() / max((ytr == 1).sum(), 1)

    dl_probs = []
    for name, build_m in {"MLP": lambda: MLP(TOPK), "LSTM": lambda: RNN(TOPK, "lstm"),
                          "GRU": lambda: RNN(TOPK, "gru"), "CNN1D": lambda: CNN1D(TOPK),
                          "Transformer": lambda: TransformerNet(TOPK)}.items():
        torch.manual_seed(0)
        p = train_eval(build_m(), Xtr, ytr, Xva, yva, Xte, pos_weight)
        dl_probs.append(p); rows.append(metrics(name, yte, p))
        print(f"  {name:12s} test_auc={rows[-1]['test_auc']}")

    rows.append(metrics("Ensemble (DL mean)", yte, np.mean(dl_probs, axis=0)))
    res = pd.DataFrame(rows).sort_values("test_auc", ascending=False).reset_index(drop=True)
    res.to_csv(OUT_CSV, index=False)
    print(f"\nSaved -> {os.path.basename(OUT_CSV)}\n")
    print(res.to_string(index=False))


if __name__ == "__main__":
    main()
