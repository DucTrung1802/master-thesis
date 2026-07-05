"""
Experiment 2 - VCB, windowed input.

One sample = a 20-day x 1053-feature MATRIX (lookback window of unified_vcb), the
target = a single scalar label y in {0,1} (next 5 trading days up >= 5%).

End-to-end flow over every model family from experiment_1:
    GBM         HistGradientBoosting on the FLATTENED window (20*1053 = 21060)
    MLP         dense net on the FLATTENED window (21060)
    LSTM/GRU    recurrent over the (20, 1053) sequence
    CNN1D       temporal conv over the (20, 1053) sequence
    Transformer encoder over the (20, 1053) sequence
    Ensemble    mean probability of the four sequence nets
A reference row (GBM on the last day only, (1053,)) shows the experiment_1 ~0.77.

Leakage-guarded: chronological 70/10/20 split; per-feature standardization with
TRAIN statistics only; warm-up rows dropped.
Output: vcb_seq20x1053_results.csv + console.
"""

import os
import sys
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

HORIZON, GAIN = 5, 0.05
WINDOW, WARMUP = 20, 120
EPOCHS, PATIENCE, BATCH = 60, 10, 128
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
HERE = os.path.dirname(os.path.abspath(__file__))
ID_COLS = {"exchange", "ticker", "date", "target"}


def prepare(ticker):
    load_dotenv(os.path.join(HERE, "..", "..", ".env"))
    conn = psycopg2.connect(host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")
    df = pd.read_sql(f"SELECT * FROM unified_schema.unified_{ticker} ORDER BY date", conn)
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
    Xs = ((X - mu) / sd).astype(np.float32)
    return Xs, y.astype(np.float32), tr_end, va_end


def make_windows(X):
    arr = X.values.astype(np.float32)
    n, k = arr.shape
    seq = np.zeros((n, WINDOW, k), dtype=np.float32)
    for i in range(n):
        s = max(0, i - WINDOW + 1)
        w = arr[s:i + 1]
        seq[i, WINDOW - len(w):] = w
    return seq                                   # (n, 20, 1053)


# ── models ────────────────────────────────────────────────────────────────────
class MLP(nn.Module):                            # consumes flattened (20*1053,)
    def __init__(s, d):
        super().__init__()
        s.net = nn.Sequential(nn.Linear(d, 256), nn.ReLU(), nn.Dropout(0.5),
                              nn.Linear(256, 64), nn.ReLU(), nn.Dropout(0.3), nn.Linear(64, 1))
    def forward(s, x): return s.net(x.flatten(1)).squeeze(-1)


class RNN(nn.Module):                            # consumes (20, 1053)
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
    def __init__(s, k, d=64):
        super().__init__()
        s.emb = nn.Linear(k, d)
        s.pos = nn.Parameter(torch.randn(1, WINDOW, d) * 0.02)
        layer = nn.TransformerEncoderLayer(d, 4, d * 2, 0.3, batch_first=True)
        s.enc = nn.TransformerEncoder(layer, 2)
        s.head = nn.Sequential(nn.LayerNorm(d), nn.Linear(d, 1))
    def forward(s, x): return s.head(s.enc(s.emb(x) + s.pos).mean(1)).squeeze(-1)


def train_eval(model, Xtr, ytr, Xva, yva, Xte, pos_weight):
    model = model.to(DEVICE)
    opt = torch.optim.Adam(model.parameters(), lr=1e-3, weight_decay=1e-4)
    loss_fn = nn.BCEWithLogitsLoss(pos_weight=torch.tensor(pos_weight, device=DEVICE))
    Xtr_t, ytr_t = torch.tensor(Xtr), torch.tensor(ytr)
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
        for i in range(0, len(Xte), 2048):
            out.append(torch.sigmoid(model(torch.tensor(Xte[i:i+2048], device=DEVICE))).cpu().numpy())
    return np.concatenate(out)


def metrics(name, shape, y_true, p):
    base = y_true.mean(); k = max(1, len(p) // 10)
    prec = y_true[np.argsort(p)[-k:]].mean()
    return {"model": name, "input_per_sample": shape,
            "test_auc": round(roc_auc_score(y_true, p), 3),
            "test_pr_auc": round(average_precision_score(y_true, p), 3),
            "top_decile_prec": round(float(prec), 3),
            "lift": round(float(prec) / max(base, 1e-9), 2)}


def main(ticker):
    out_csv = os.path.join(HERE, f"{ticker}_seq20x1053_results.csv")
    print(f"=== {ticker.upper()} ===  device: {DEVICE}")
    Xs, y, tr_end, va_end = prepare(ticker)
    K = Xs.shape[1]
    seq = make_windows(Xs)                         # (n, 20, K)
    flat = seq.reshape(len(seq), -1)               # (n, 20*K)
    yte = y[va_end:].astype(int)
    print(f"sample input = ({WINDOW}, {K}) matrix; flattened = {WINDOW*K}; "
          f"train {tr_end} / val {va_end-tr_end} / test {len(seq)-va_end} "
          f"| test base {yte.mean():.1%}\n")

    rows = []

    # reference: experiment_1 point-in-time GBM (last day only, (1053,))
    last = seq[:, -1, :]
    gbm_ref = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05,
        max_leaf_nodes=31, l2_regularization=1.0, random_state=0)
    gbm_ref.fit(last[:va_end], y[:va_end].astype(int))
    rows.append(metrics("GBM (last-day, ref)", f"({K},)", yte, gbm_ref.predict_proba(last[va_end:])[:, 1]))
    print(f"  GBM last-day (ref)  auc={rows[-1]['test_auc']}")

    # GBM on the flattened 20-day window
    gbm = HistGradientBoostingClassifier(max_iter=150, learning_rate=0.05,
        max_leaf_nodes=31, l2_regularization=1.0, random_state=0)
    gbm.fit(flat[:va_end], y[:va_end].astype(int))
    rows.append(metrics("GBM (flatten 20d)", f"({WINDOW*K},)", yte, gbm.predict_proba(flat[va_end:])[:, 1]))
    print(f"  GBM flatten         auc={rows[-1]['test_auc']}")

    pos_weight = (y[:tr_end] == 0).sum() / max((y[:tr_end] == 1).sum(), 1)
    tr, va = slice(0, tr_end), slice(tr_end, va_end)

    # MLP on flattened window
    torch.manual_seed(0)
    p = train_eval(MLP(WINDOW*K), flat[tr], y[tr], flat[va], y[va].astype(int), flat[va_end:], pos_weight)
    rows.append(metrics("MLP (flatten 20d)", f"({WINDOW*K},)", yte, p)); print(f"  MLP                 auc={rows[-1]['test_auc']}")

    # sequence models on (20, K)
    dl = []
    for name, build in {"LSTM": lambda: RNN(K, "lstm"), "GRU": lambda: RNN(K, "gru"),
                        "CNN1D": lambda: CNN1D(K), "Transformer": lambda: TransformerNet(K)}.items():
        torch.manual_seed(0)
        p = train_eval(build(), seq[tr], y[tr], seq[va], y[va].astype(int), seq[va_end:], pos_weight)
        dl.append(p); rows.append(metrics(name, f"({WINDOW}, {K})", yte, p))
        print(f"  {name:12s}        auc={rows[-1]['test_auc']}")

    rows.append(metrics("Ensemble (seq mean)", f"({WINDOW}, {K})", yte, np.mean(dl, axis=0)))

    res = pd.DataFrame(rows).sort_values("test_auc", ascending=False).reset_index(drop=True)
    res.insert(0, "ticker", ticker.upper())
    res.to_csv(out_csv, index=False)
    print(f"\nSaved -> {os.path.basename(out_csv)}\n")
    print(res.to_string(index=False))


if __name__ == "__main__":
    for tk in (sys.argv[1:] or ["vcb"]):
        main(tk.lower())
