"""Text → price-reaction sentiment model.

Trains a classifier on the **news text** (PhoBERT embeddings of headline + content lead)
to predict the 5-level **price-reaction** label from `price_reaction_labels` — i.e. it
learns "what kind of text precedes an up-move / down-move", with the label grounded in the
market's reaction instead of a general-domain sentiment model.

⚠️ The label peeks H days ahead, so evaluation is a **purged, embargoed, walk-forward**
split by `event_date` (via `sentiment_features.purged_walkforward_folds`): train only on
events whose forward window closed before each cut, drop an H-day embargo, test strictly
after. Reported out-of-sample, per model:
  • **macro-F1** — mean per-class F1 (equal class weight; ignores the neutral majority),
  • **QWK** (quadratic weighted kappa) — ordinal agreement; "off by one level" ≪ "off by
    four", the right metric for an ordered scale,
both against a majority-class baseline.

Two models: Logistic Regression (baseline) and Gradient Boosting (main). Embeddings are
computed once and reused across folds/models.
"""

from __future__ import annotations

import os

# The model weights are cached locally; force offline so `from_pretrained` never makes a
# network HEAD request (which stalls with long retries on a flaky/blocked connection).
os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

from dataclasses import dataclass, field
from typing import Callable, Dict, List

import numpy as np
import pandas as pd

from sentiment.price_reaction_labels import LEVELS
from sentiment.sentiment_features import purged_walkforward_folds

# PhoBERT encoder for text features (frozen, mean-pooled) — the base LM, not the
# fine-tuned sentiment head (we are supplying our own price-grounded labels).
ENCODER_NAME = "vinai/phobert-base"
DEFAULT_MAX_LENGTH = 256
DEFAULT_CONTENT_CHARS = 600  # headline + this many chars of body → the encoder

DEFAULT_CUTS = [
    "2019-12-31",
    "2020-12-31",
    "2021-12-31",
    "2022-12-31",
    "2023-12-31",
    "2024-12-31",
    "2025-06-30",
]
MIN_TRAIN = 200
MIN_TEST = 30


@dataclass
class FoldMetrics:
    cut: str
    n_train: int
    n_test: int
    macro_f1: float
    qwk: float
    macro_f1_baseline: float
    qwk_baseline: float


@dataclass
class ModelResult:
    model_name: str
    folds: List[FoldMetrics] = field(default_factory=list)

    def _avg(self, a: str) -> float:
        v = [getattr(f, a) for f in self.folds]
        return float(np.mean(v)) if v else float("nan")

    @property
    def n_folds(self) -> int:
        return len(self.folds)

    @property
    def avg_macro_f1(self) -> float:
        return self._avg("macro_f1")

    @property
    def avg_qwk(self) -> float:
        return self._avg("qwk")

    @property
    def avg_macro_f1_baseline(self) -> float:
        return self._avg("macro_f1_baseline")

    @property
    def avg_qwk_baseline(self) -> float:
        return self._avg("qwk_baseline")


def build_text(
    headline: pd.Series, content: pd.Series, content_chars: int = DEFAULT_CONTENT_CHARS
) -> List[str]:
    """headline + a lead slice of content → the string fed to the encoder."""
    h = headline.fillna("").astype(str).str.strip()
    c = content.fillna("").astype(str).str.strip().str.slice(0, content_chars)
    return (h + ". " + c).str.slice(0, content_chars + 120).tolist()


def embed_texts(
    texts: List[str], batch_size: int = 64, max_length: int = DEFAULT_MAX_LENGTH
) -> np.ndarray:
    """Mean-pooled PhoBERT last-hidden-state embeddings (frozen). CUDA if available."""
    import torch
    from transformers import AutoModel, AutoTokenizer

    tok = AutoTokenizer.from_pretrained(ENCODER_NAME)
    model = AutoModel.from_pretrained(ENCODER_NAME)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device).eval()

    vecs = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        enc = tok(
            batch,
            truncation=True,
            max_length=max_length,
            padding=True,
            return_tensors="pt",
        ).to(device)
        with torch.no_grad():
            out = model(**enc).last_hidden_state
            mask = enc["attention_mask"].unsqueeze(-1)
            pooled = (out * mask).sum(1) / mask.sum(1).clamp(min=1)
        vecs.append(pooled.cpu().numpy())
    return np.vstack(vecs)


def _make_models() -> Dict[str, tuple[Callable, bool]]:
    # HistGradientBoosting is 1-2 orders of magnitude faster than the exact
    # GradientBoosting on this 768-dim dense embedding (histogram binning), which
    # matters across 7 folds × 2 models; results are equivalent for our purposes.
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression

    return {
        "logistic": (
            lambda: LogisticRegression(
                max_iter=3000, class_weight="balanced", C=1.0
            ),
            True,
        ),
        "gradient_boosting": (
            lambda: HistGradientBoostingClassifier(
                max_iter=300,
                max_depth=3,
                learning_rate=0.05,
                class_weight="balanced",
                random_state=0,
            ),
            False,
        ),
    }


def _score_fold(
    X: np.ndarray,
    y: np.ndarray,
    factory: Callable,
    needs_scaling: bool,
    tr: np.ndarray,
    te: np.ndarray,
    cut: pd.Timestamp,
) -> FoldMetrics | None:
    if tr.sum() < MIN_TRAIN or te.sum() < MIN_TEST or len(np.unique(y[tr])) < 2:
        return None
    from collections import Counter

    from sklearn.metrics import cohen_kappa_score, f1_score
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    Xtr, ytr, Xte, yte = X[tr], y[tr], X[te], y[te]
    est = factory()
    clf = make_pipeline(StandardScaler(), est) if needs_scaling else est
    clf.fit(Xtr, ytr)
    pred = clf.predict(Xte)

    majority = Counter(ytr).most_common(1)[0][0]
    base = np.full(len(yte), majority)
    labels = list(range(len(LEVELS)))
    return FoldMetrics(
        cut=str(cut.date()),
        n_train=int(tr.sum()),
        n_test=int(te.sum()),
        macro_f1=float(f1_score(yte, pred, average="macro", labels=labels, zero_division=0)),
        qwk=float(cohen_kappa_score(yte, pred, weights="quadratic", labels=labels)),
        macro_f1_baseline=float(
            f1_score(yte, base, average="macro", labels=labels, zero_division=0)
        ),
        qwk_baseline=float(
            cohen_kappa_score(yte, base, weights="quadratic", labels=labels)
        ),
    )


def evaluate(
    ev: pd.DataFrame,
    X: np.ndarray,
    cut_dates: List[str] | None = None,
    horizon: int = 5,
) -> List[ModelResult]:
    """Purged walk-forward over `event_date`; `ev` must align row-for-row with `X` and
    carry `event_date` + `price_level_id`."""
    cut_dates = cut_dates or DEFAULT_CUTS
    y = ev["price_level_id"].astype(int).to_numpy()
    panel = ev.rename(columns={"event_date": "date"})  # folds split on `date`

    results: List[ModelResult] = []
    for name, (factory, needs_scaling) in _make_models().items():
        mr = ModelResult(model_name=name)
        for cut, tr_mask, te_mask in purged_walkforward_folds(
            panel, cut_dates, horizon=horizon
        ):
            fold = _score_fold(X, y, factory, needs_scaling, tr_mask, te_mask, cut)
            if fold is not None:
                mr.folds.append(fold)
        results.append(mr)
    return results


def format_report(results: List[ModelResult], target_desc: str) -> str:
    lines = [
        f"Text -> price-reaction sentiment ({target_desc}, purged walk-forward, "
        "out-of-sample)\n"
        "  macro-F1: equal-weight per-class F1; QWK: ordinal agreement (1=perfect, "
        "0=chance). Both vs majority baseline.\n"
    ]
    header = (
        f"{'model':<20} {'folds':>5} {'macroF1':>8} {'F1_base':>8} "
        f"{'QWK':>7} {'QWK_base':>9}"
    )
    lines.append(header)
    lines.append("-" * len(header))
    for mr in results:
        lines.append(
            f"{mr.model_name:<20} {mr.n_folds:>5} {mr.avg_macro_f1:>8.3f} "
            f"{mr.avg_macro_f1_baseline:>8.3f} {mr.avg_qwk:>7.3f} "
            f"{mr.avg_qwk_baseline:>9.3f}"
        )
    return "\n".join(lines)
