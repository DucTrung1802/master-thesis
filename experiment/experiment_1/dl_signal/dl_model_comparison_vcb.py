"""
Deep-learning shoot-out for the VCB "next 5 trading days up >= 5%" signal.

Compares several architectures against the gradient-boosting baseline, all on the
SAME chronological test split (most-recent 20% of days) so AUCs are comparable:

    GBM-full   HistGradientBoosting on all point-in-time features  (the baseline)
    MLP        feed-forward net on the last day's features
    LSTM       recurrent over a W-day window
    GRU        recurrent over a W-day window
    CNN1D      temporal 1-D conv over a W-day window
    Transformer encoder over a W-day window
    Ensemble   mean probability of the four sequence/DL nets

    label y[t] = 1 if close[t+5]/close[t]-1 >= 0.05
    sequence X[t] = top-K features over days [t-W+1 .. t], known at close of t.

To keep the sequence models tractable and denoised, features are reduced to the
top-K by univariate AUC computed on the TRAIN split only (no look-ahead). Features
are standardized with train statistics; class imbalance handled with pos_weight;
early stopping on validation AUC.

Output: dl_model_comparison_vcb.csv + console.
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
WINDOW = 20          # trading-day lookback for sequence models
TOPK = 64            # features kept (selected on train only)
WARMUP = 120         # drop leading rows (indicator warm-up)
EPOCHS, PATIENCE, BATCH = 100, 14, 128
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_CSV = os.path.join(HERE, "dl_model_comparison_vcb.csv")
ID_COLS = {"exchange", "ticker", "date", "target"}


# ── data ────────────────────────────────────────────────────────────────────
def load():
    load_dotenv(os.path.join(HERE, "..", "..", "..", ".env"))
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")
    df = pd.read_sql("SELECT * FROM unified_schema.unified_vcb ORDER BY date", conn)
    conn.close()
    return df


def prepare():
    df = load()
    close = df["close"].astype(float).values
    y = np.full(len(close), np.nan)
    y[:-HORIZON] = (close[HORIZON:] / close[:-HORIZON] - 1.0 >= GAIN).astype(float)

    feat = [c for c in df.columns if c not in ID_COLS]
    X = df[feat].copy()
    for c in feat:
        if X[c].dtype == bool:
            X[c] = X[c].astype(float)
    X = X.select_dtypes(include=[np.number]).astype(float)
    X = X.ffill().fillna(0.0)

    # drop warm-up region and unlabeled tail
    valid = np.arange(WARMUP, len(close) - HORIZON)
    X, y = X.iloc[valid].reset_index(drop=True), y[valid]

    n = len(X)
    tr_end, va_end = int(n * 0.70), int(n * 0.80)   # 70 / 10 / 20 chrono split

    # standardize on train only
    mu, sd = X.iloc[:tr_end].mean(), X.iloc[:tr_end].std().replace(0, 1)
    Xs = (X - mu) / sd

    # feature selection: top-K univariate AUC on TRAIN rows only
    ytr = y[:tr_end].astype(int)
    scores = []
    for c in Xs.columns:
        col = Xs[c].iloc[:tr_end].values
        if len(np.unique(col)) < 3:
            continue
        scores.append((c, abs(roc_auc_score(ytr, col) - 0.5)))
    topk = [c for c, _ in sorted(scores, key=lambda z: -z[1])[:TOPK]]
    return Xs[topk], Xs, y, tr_end, va_end


def make_windows(Xtopk, y):
    """Sequence tensors [N, WINDOW, K] aligned so row i ends at original row i."""
    arr = Xtopk.values.astype(np.float32)
    n, k = arr.shape
    seq = np.zeros((n, WINDOW, k), dtype=np.float32)
    for i in range(n):
        s = max(0, i - WINDOW + 1)
        w = arr[s:i + 1]
        seq[i, WINDOW - len(w):] = w
    return seq, y.astype(np.float32)


# ── models ──────────────────────────────────────────────────────────────────
class MLP(nn.Module):
    def __init__(self, k):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(k, 128), nn.ReLU(), nn.Dropout(0.4),
            nn.Linear(128, 64), nn.ReLU(), nn.Dropout(0.3), nn.Linear(64, 1))

    def forward(self, x):              # x: [B, W, K] -> use last day
        return self.net(x[:, -1, :]).squeeze(-1)


class RNN(nn.Module):
    def __init__(self, k, kind="lstm"):
        super().__init__()
        cell = nn.LSTM if kind == "lstm" else nn.GRU
        self.rnn = cell(k, 64, num_layers=1, batch_first=True)
        self.head = nn.Sequential(nn.Dropout(0.3), nn.Linear(64, 1))

    def forward(self, x):
        out, _ = self.rnn(x)
        return self.head(out[:, -1, :]).squeeze(-1)


class CNN1D(nn.Module):
    def __init__(self, k):
        super().__init__()
        self.conv = nn.Sequential(
            nn.Conv1d(k, 64, 3, padding=1), nn.ReLU(),
            nn.Conv1d(64, 64, 3, padding=1), nn.ReLU(), nn.AdaptiveMaxPool1d(1))
        self.head = nn.Sequential(nn.Dropout(0.3), nn.Linear(64, 1))

    def forward(self, x):              # x: [B, W, K] -> [B, K, W]
        z = self.conv(x.transpose(1, 2)).squeeze(-1)
        return self.head(z).squeeze(-1)


class TransformerNet(nn.Module):
    def __init__(self, k, d=64):
        super().__init__()
        self.emb = nn.Linear(k, d)
        self.pos = nn.Parameter(torch.randn(1, WINDOW, d) * 0.02)
        layer = nn.TransformerEncoderLayer(d, 4, d * 2, 0.3, batch_first=True)
        self.enc = nn.TransformerEncoder(layer, 2)
        self.head = nn.Sequential(nn.LayerNorm(d), nn.Linear(d, 1))

    def forward(self, x):
        z = self.enc(self.emb(x) + self.pos)
        return self.head(z.mean(1)).squeeze(-1)


def train_eval(model, tr, va, te, pos_weight):
    model = model.to(DEVICE)
    opt = torch.optim.Adam(model.parameters(), lr=1e-3, weight_decay=1e-4)
    loss_fn = nn.BCEWithLogitsLoss(pos_weight=torch.tensor(pos_weight, device=DEVICE))
    Xtr, ytr = tr
    best_auc, best_state, wait = -1, None, 0
    Xtr_t = torch.tensor(Xtr, device=DEVICE)
    ytr_t = torch.tensor(ytr, device=DEVICE)
    Xva_t = torch.tensor(va[0], device=DEVICE)

    for ep in range(EPOCHS):
        model.train()
        perm = torch.randperm(len(Xtr_t), device=DEVICE)
        for i in range(0, len(perm), BATCH):
            idx = perm[i:i + BATCH]
            opt.zero_grad()
            loss = loss_fn(model(Xtr_t[idx]), ytr_t[idx])
            loss.backward()
            opt.step()
        model.eval()
        with torch.no_grad():
            va_p = torch.sigmoid(model(Xva_t)).cpu().numpy()
        auc = roc_auc_score(va[1], va_p) if len(np.unique(va[1])) > 1 else 0.5
        if auc > best_auc:
            best_auc, best_state, wait = auc, {k: v.cpu().clone() for k, v in model.state_dict().items()}, 0
        else:
            wait += 1
            if wait >= PATIENCE:
                break
    model.load_state_dict(best_state)
    model.eval()
    with torch.no_grad():
        te_p = torch.sigmoid(model(torch.tensor(te[0], device=DEVICE))).cpu().numpy()
    return te_p


def metrics(name, y_true, p):
    base = y_true.mean()
    k = max(1, len(p) // 10)
    prec_top = y_true[np.argsort(p)[-k:]].mean()
    return {"model": name,
            "test_auc": round(roc_auc_score(y_true, p), 3),
            "test_pr_auc": round(average_precision_score(y_true, p), 3),
            "top_decile_prec": round(float(prec_top), 3),
            "lift": round(float(prec_top) / max(base, 1e-9), 2)}


def main():
    print(f"device: {DEVICE}")
    Xtopk, Xfull, y, tr_end, va_end = prepare()
    seq, yf = make_windows(Xtopk, y)

    s_tr, s_va, s_te = slice(0, tr_end), slice(tr_end, va_end), slice(va_end, None)
    yte = yf[s_te].astype(int)
    pos_weight = (yf[s_tr] == 0).sum() / max((yf[s_tr] == 1).sum(), 1)
    print(f"train {tr_end} / val {va_end - tr_end} / test {len(yf) - va_end}  "
          f"| test base rate {yte.mean():.1%} | K={TOPK} W={WINDOW} "
          f"| pos_weight {pos_weight:.1f}\n")

    rows, dl_probs = [], []

    # gradient-boosting baseline on full point-in-time features (same split)
    Xf = Xfull.values.astype(np.float32)
    gbm = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05,
                                         max_leaf_nodes=31, l2_regularization=1.0,
                                         random_state=0)
    gbm.fit(Xf[:va_end], yf[:va_end].astype(int))
    rows.append(metrics("GBM-full (baseline)", yte, gbm.predict_proba(Xf[s_te])[:, 1]))

    builders = {
        "MLP": lambda: MLP(TOPK),
        "LSTM": lambda: RNN(TOPK, "lstm"),
        "GRU": lambda: RNN(TOPK, "gru"),
        "CNN1D": lambda: CNN1D(TOPK),
        "Transformer": lambda: TransformerNet(TOPK),
    }
    tr = (seq[s_tr], yf[s_tr])
    va = (seq[s_va], yf[s_va].astype(int))
    te = (seq[s_te], yte)
    for name, build in builders.items():
        torch.manual_seed(0)
        p = train_eval(build(), tr, va, te, pos_weight)
        dl_probs.append(p)
        rows.append(metrics(name, yte, p))
        print(f"  trained {name:12s} test_auc={rows[-1]['test_auc']}")

    rows.append(metrics("Ensemble (DL mean)", yte, np.mean(dl_probs, axis=0)))

    res = pd.DataFrame(rows).sort_values("test_auc", ascending=False).reset_index(drop=True)
    res.to_csv(OUT_CSV, index=False)
    print(f"\nSaved -> {os.path.basename(OUT_CSV)}\n")
    print(res.to_string(index=False))


if __name__ == "__main__":
    main()
