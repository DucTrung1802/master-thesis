"""
Experiment 2 - VCB lookback sweep.

Same task (one sample = a W-day x ~1053-feature matrix -> scalar label
"next 5 days up >= 5%"), swept over lookback W in {1,2,3,5,8,10,12,15,18,20}.
W=1 is the point-in-time case (the experiment_1 ~0.77 baseline).

For every W we train each model family and record test ROC-AUC:
    GBM        HistGradientBoosting on the flattened window (W*K)
    MLP        dense net on the flattened window (W*K)
    LSTM/GRU   recurrent over (W, K)
    CNN1D      temporal conv over (W, K)
    Transformer encoder over (W, K)
    Ensemble   mean prob of the four sequence nets

Leakage-guarded: chronological 70/10/20 split, train-only standardization.
Output: vcb_lookback_auc.csv (pivot: rows=lookback, cols=model) + a long-form
vcb_lookback_detail.csv with all metrics.
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
torch.manual_seed(0); np.random.seed(0)

TICKER = "vcb"
HORIZON, GAIN = 5, 0.05
WINDOWS = [1, 2, 3, 5, 8, 10, 12, 15, 18, 20]
WARMUP = 120
EPOCHS, PATIENCE, BATCH = 50, 8, 128
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
HERE = os.path.dirname(os.path.abspath(__file__))
ID_COLS = {"exchange", "ticker", "date", "target"}


def prepare():
    load_dotenv(os.path.join(HERE, "..", "..", ".env"))
    conn = psycopg2.connect(host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")
    df = pd.read_sql(f"SELECT * FROM unified_schema.unified_{TICKER} ORDER BY date", conn)
    conn.close()
    close = df["close"].astype(float).values
    y = np.full(len(close), np.nan)
    y[:-HORIZON] = (close[HORIZON:] / close[:-HORIZON] - 1.0 >= GAIN).astype(float)
    feat = [c for c in df.columns if c not in ID_COLS]
    X = df[feat].copy()
    for c in feat:
        if X[c].dtype == bool:
            X[c] = X[c].astype(float)
    X = X.select_dtypes(include=[np.number]).astype(float).ffill().fillna(0.0)
    valid = np.arange(WARMUP, len(close) - HORIZON)
    X, y = X.iloc[valid].reset_index(drop=True), y[valid]
    n = len(X)
    tr_end, va_end = int(n * 0.70), int(n * 0.80)
    mu, sd = X.iloc[:tr_end].mean(), X.iloc[:tr_end].std().replace(0, 1)
    return ((X - mu) / sd).astype(np.float32).values, y.astype(np.float32), tr_end, va_end


def windows(arr, W):
    n, k = arr.shape
    seq = np.zeros((n, W, k), dtype=np.float32)
    for i in range(n):
        s = max(0, i - W + 1)
        w = arr[s:i + 1]
        seq[i, W - len(w):] = w
    return seq


class MLP(nn.Module):
    def __init__(s, d):
        super().__init__()
        s.net = nn.Sequential(nn.Linear(d, 256), nn.ReLU(), nn.Dropout(0.5),
                              nn.Linear(256, 64), nn.ReLU(), nn.Dropout(0.3), nn.Linear(64, 1))
    def forward(s, x): return s.net(x.flatten(1)).squeeze(-1)


class RNN(nn.Module):
    def __init__(s, k, kind="lstm"):
        super().__init__()
        s.rnn = (nn.LSTM if kind == "lstm" else nn.GRU)(k, 64, batch_first=True)
        s.head = nn.Sequential(nn.Dropout(0.3), nn.Linear(64, 1))
    def forward(s, x):
        o, _ = s.rnn(x); return s.head(o[:, -1, :]).squeeze(-1)


class CNN1D(nn.Module):
    def __init__(s, k):
        super().__init__()
        s.conv = nn.Sequential(nn.Conv1d(k, 64, 3, padding=1), nn.ReLU(),
                               nn.Conv1d(64, 64, 3, padding=1), nn.ReLU(), nn.AdaptiveMaxPool1d(1))
        s.head = nn.Sequential(nn.Dropout(0.3), nn.Linear(64, 1))
    def forward(s, x): return s.head(s.conv(x.transpose(1, 2)).squeeze(-1)).squeeze(-1)


class TransformerNet(nn.Module):
    def __init__(s, k, W, d=64):
        super().__init__()
        s.emb = nn.Linear(k, d)
        s.pos = nn.Parameter(torch.randn(1, W, d) * 0.02)
        layer = nn.TransformerEncoderLayer(d, 4, d * 2, 0.3, batch_first=True)
        s.enc = nn.TransformerEncoder(layer, 2)
        s.head = nn.Sequential(nn.LayerNorm(d), nn.Linear(d, 1))
    def forward(s, x): return s.head(s.enc(s.emb(x) + s.pos).mean(1)).squeeze(-1)


def train_eval(model, Xtr, ytr, Xva, yva, Xte, pw):
    model = model.to(DEVICE)
    opt = torch.optim.Adam(model.parameters(), lr=1e-3, weight_decay=1e-4)
    loss_fn = nn.BCEWithLogitsLoss(pos_weight=torch.tensor(pw, device=DEVICE))
    Xtr_t, ytr_t = torch.tensor(Xtr), torch.tensor(ytr)
    Xva_t = torch.tensor(Xva, device=DEVICE)
    best, state, wait = -1, None, 0
    for ep in range(EPOCHS):
        model.train()
        perm = torch.randperm(len(Xtr_t))
        for i in range(0, len(perm), BATCH):
            idx = perm[i:i + BATCH]
            opt.zero_grad()
            loss_fn(model(Xtr_t[idx].to(DEVICE)), ytr_t[idx].to(DEVICE)).backward()
            opt.step()
        model.eval()
        with torch.no_grad():
            vp = torch.sigmoid(model(Xva_t)).cpu().numpy()
        auc = roc_auc_score(yva, vp) if len(np.unique(yva)) > 1 else 0.5
        if auc > best:
            best, state, wait = auc, {k: v.cpu().clone() for k, v in model.state_dict().items()}, 0
        else:
            wait += 1
            if wait >= PATIENCE: break
    model.load_state_dict(state); model.eval()
    out = []
    with torch.no_grad():
        for i in range(0, len(Xte), 2048):
            out.append(torch.sigmoid(model(torch.tensor(Xte[i:i+2048], device=DEVICE))).cpu().numpy())
    return np.concatenate(out)


def main():
    print(f"=== {TICKER.upper()} lookback sweep === device: {DEVICE}")
    arr, y, tr_end, va_end = prepare()
    K = arr.shape[1]
    yte = y[va_end:].astype(int)
    pw = (y[:tr_end] == 0).sum() / max((y[:tr_end] == 1).sum(), 1)
    print(f"K={K} features | train {tr_end} / val {va_end-tr_end} / test {len(y)-va_end} "
          f"| test base {yte.mean():.1%}\n")

    def auc(p): return round(roc_auc_score(yte, p), 3)
    detail, pivot = [], []

    for W in WINDOWS:
        seq = windows(arr, W)
        flat = seq.reshape(len(seq), -1)
        tr, va = slice(0, tr_end), slice(tr_end, va_end)
        row = {"lookback": W}

        gbm = HistGradientBoostingClassifier(max_iter=200, learning_rate=0.05,
            max_leaf_nodes=31, l2_regularization=1.0, random_state=0)
        gbm.fit(flat[:va_end], y[:va_end].astype(int))
        row["GBM"] = auc(gbm.predict_proba(flat[va_end:])[:, 1])

        torch.manual_seed(0)
        row["MLP"] = auc(train_eval(MLP(W*K), flat[tr], y[tr], flat[va], y[va].astype(int), flat[va_end:], pw))

        dl = []
        for name, build in {"LSTM": lambda: RNN(K, "lstm"), "GRU": lambda: RNN(K, "gru"),
                            "CNN1D": lambda: CNN1D(K), "Transformer": lambda: TransformerNet(K, W)}.items():
            torch.manual_seed(0)
            p = train_eval(build(), seq[tr], y[tr], seq[va], y[va].astype(int), seq[va_end:], pw)
            dl.append(p); row[name] = auc(p)
        row["Ensemble"] = auc(np.mean(dl, axis=0))
        pivot.append(row)
        print(f"  W={W:<2d}  " + "  ".join(f"{m}={row[m]}" for m in
              ["GBM","MLP","LSTM","GRU","CNN1D","Transformer","Ensemble"]))

    pv = pd.DataFrame(pivot).set_index("lookback")
    pv.to_csv(os.path.join(HERE, "vcb_lookback_auc.csv"))
    print("\n=== VCB test ROC-AUC by lookback (rows) x model (cols) ===")
    print(pv.to_string())
    best_w = pv.max(axis=1)
    print("\nbest model AUC per lookback:")
    print("  " + "  ".join(f"W{w}={pv.loc[w].max():.3f}({pv.loc[w].idxmax()})" for w in WINDOWS))


if __name__ == "__main__":
    main()
