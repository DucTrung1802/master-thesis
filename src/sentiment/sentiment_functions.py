"""Vietnamese news sentiment scoring — pure text → score logic.

Mirrors `src/ta/ta_functions.py`: this module holds the model wrapper and the
scoring functions with **no database concern**. `data_preprocessor` imports it,
reads `bronze.cafef_news`, calls `score_texts`, and writes the silver table.

The model is a PhoBERT-based 3-class Vietnamese sentiment classifier
(`mr4/phobert-base-vi-sentiment-analysis`), loaded once and reused. It runs on
CUDA when available (verified ~500 texts/s on an RTX 3050 → the full CafeF-news
table scores in ~12 s), else CPU. Labels are normalised to English
`negative` / `neutral` / `positive`; a signed `sentiment_score` in [-1, 1] is
derived as `p(positive) - p(negative)` for an ordered, model-agnostic feature.

⚠️ The CafeF news text is **Vietnamese**, so an English sentiment model would be
wrong here — this model is chosen for that reason (see `sentiment/CONTEXT.md`).
It is general-domain (not finance-fine-tuned), so treat the score as a directional
signal, not a calibrated financial-tone measure.
"""

from __future__ import annotations

import os

# Weights are cached locally; force offline so `from_pretrained` never blocks on a
# network HEAD request (stalls with long retries on a flaky/blocked connection).
os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import pandas as pd

# The model card's id2label, mapped to canonical English keys. The order of the
# Vietnamese labels is fixed by the checkpoint (0=Tiêu cực, 1=Tích cực, 2=Trung
# tính); we translate rather than assume an index so a re-labelled checkpoint
# surfaces loudly instead of silently swapping polarity.
_VI_TO_EN = {
    "Tiêu cực": "negative",
    "Tích cực": "positive",
    "Trung tính": "neutral",
}
CANONICAL_LABELS = ("negative", "neutral", "positive")

# Model identity — bumped when the checkpoint or scoring changes, stored on every
# row as `model_version` so a re-score is traceable and a mixed table is detectable.
MODEL_NAME = "mr4/phobert-base-vi-sentiment-analysis"
MODEL_VERSION = f"{MODEL_NAME}@v1"

# Inference knobs (validated in the scratchpad benchmark).
DEFAULT_BATCH_SIZE = 32
DEFAULT_MAX_LENGTH = 256  # PhoBERT's positional cap; long editorials are truncated.


@dataclass
class SentimentResult:
    """Per-text scoring output. `score` is signed p(pos) − p(neg) in [-1, 1]."""

    label: str
    score: float
    prob_negative: float
    prob_neutral: float
    prob_positive: float


class VietnameseSentimentModel:
    """Lazy singleton-ish wrapper around the HF model + tokenizer.

    Construction is cheap; the weights load on first `score_texts` (or an explicit
    `load()`), so importing this module never pulls ~500 MB of model into memory.
    Keep ONE instance and reuse it — the model is stateless across calls."""

    def __init__(
        self,
        model_name: str = MODEL_NAME,
        device: Optional[str] = None,
        max_length: int = DEFAULT_MAX_LENGTH,
    ) -> None:
        self.model_name = model_name
        self.max_length = max_length
        self._device = device
        self._tokenizer = None
        self._model = None
        # label id → canonical english, resolved from the loaded config.
        self._id2en: dict[int, str] = {}

    # ── loading ─────────────────────────────────────────────────────────────
    def load(self) -> "VietnameseSentimentModel":
        if self._model is not None:
            return self
        # Import torch/transformers lazily so a caller that never scores (e.g. a
        # switch-gated-off run) doesn't pay the import cost.
        import torch
        from transformers import (
            AutoModelForSequenceClassification,
            AutoTokenizer,
        )

        if self._device is None:
            self._device = "cuda" if torch.cuda.is_available() else "cpu"

        self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self._model = (
            AutoModelForSequenceClassification.from_pretrained(self.model_name)
            .to(self._device)
            .eval()
        )

        # Resolve id → english label from the checkpoint's own id2label, translating
        # the Vietnamese names. Fail loudly if a label is unrecognised (a swapped or
        # different checkpoint) rather than mislabelling every row.
        id2label = self._model.config.id2label
        for idx, vi in id2label.items():
            en = _VI_TO_EN.get(vi.strip())
            if en is None:
                raise ValueError(
                    f"Unexpected sentiment label {vi!r} from {self.model_name}; "
                    f"known: {list(_VI_TO_EN)}. Refusing to score with an unknown "
                    f"label mapping."
                )
            self._id2en[int(idx)] = en
        return self

    @property
    def device(self) -> str:
        return self._device or "cpu"

    # ── scoring ─────────────────────────────────────────────────────────────
    def score_texts(
        self, texts: List[str], batch_size: int = DEFAULT_BATCH_SIZE
    ) -> List[SentimentResult]:
        """Score a list of raw strings → one `SentimentResult` each, order-preserved.

        Empty / whitespace-only texts are scored as `neutral` with score 0 and are
        never sent to the model (the tokenizer would otherwise emit an all-pad row).
        Batches are softmaxed on-device; `max_length` truncation applies."""
        self.load()
        import torch
        import torch.nn.functional as F

        results: List[Optional[SentimentResult]] = [None] * len(texts)

        # Split into the rows worth scoring vs the blank ones (scored trivially).
        live_idx = [i for i, t in enumerate(texts) if isinstance(t, str) and t.strip()]
        for i in range(len(texts)):
            if i not in set(live_idx):
                results[i] = SentimentResult("neutral", 0.0, 0.0, 1.0, 0.0)

        # Index positions in CANONICAL_LABELS order for the three canonical probs.
        neg_i = {v: k for k, v in self._id2en.items()}["negative"]
        neu_i = {v: k for k, v in self._id2en.items()}["neutral"]
        pos_i = {v: k for k, v in self._id2en.items()}["positive"]

        for start in range(0, len(live_idx), batch_size):
            batch_positions = live_idx[start : start + batch_size]
            batch_texts = [texts[i] for i in batch_positions]
            enc = self._tokenizer(
                batch_texts,
                truncation=True,
                max_length=self.max_length,
                padding=True,
                return_tensors="pt",
            ).to(self._device)
            with torch.no_grad():
                probs = F.softmax(self._model(**enc).logits, dim=-1).cpu().numpy()

            for pos, row in zip(batch_positions, probs):
                p_neg = float(row[neg_i])
                p_neu = float(row[neu_i])
                p_pos = float(row[pos_i])
                top = int(row.argmax())
                results[pos] = SentimentResult(
                    label=self._id2en[top],
                    score=p_pos - p_neg,  # signed polarity in [-1, 1]
                    prob_negative=p_neg,
                    prob_neutral=p_neu,
                    prob_positive=p_pos,
                )

        return results  # type: ignore[return-value]


def build_scored_text(
    headline: Optional[str],
    content: Optional[str],
    news_type: Optional[str],
    content_chars: int = 512,
) -> str:
    """Assemble the string handed to the model from a news row.

    The **headline carries most of the signal** and is always included first.
    For `editorial` rows a lead slice of the body is appended for context (the
    tokenizer truncates to `max_length` anyway, so this just gives the model more
    than the headline when it's an article). `disclosure` rows are short filing
    stubs whose headline already says it all, so their body is skipped to avoid
    diluting the signal with procedural boilerplate."""
    head = (headline or "").strip()
    if news_type == "editorial" and content:
        return f"{head}. {content.strip()[:content_chars]}".strip()
    return head


def score_news_frame(
    df: pd.DataFrame,
    model: Optional[VietnameseSentimentModel] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    headline_col: str = "headline",
    content_col: str = "content",
    type_col: str = "type",
) -> pd.DataFrame:
    """Score a news DataFrame → the same index + the sentiment columns.

    Returns a frame with `sentiment_label`, `sentiment_score`, `prob_negative`,
    `prob_neutral`, `prob_positive`, `model_version` (one row per input row, aligned
    by position). Pure: it does not touch the DB and adds no keys — the caller
    concatenates these onto its keyed frame."""
    model = model or VietnameseSentimentModel()
    texts = [
        build_scored_text(
            df[headline_col].iloc[i] if headline_col in df else None,
            df[content_col].iloc[i] if content_col in df else None,
            df[type_col].iloc[i] if type_col in df else None,
        )
        for i in range(len(df))
    ]
    scored = model.score_texts(texts, batch_size=batch_size)
    out = pd.DataFrame(
        {
            "sentiment_label": [s.label for s in scored],
            "sentiment_score": [s.score for s in scored],
            "prob_negative": [s.prob_negative for s in scored],
            "prob_neutral": [s.prob_neutral for s in scored],
            "prob_positive": [s.prob_positive for s in scored],
            "model_version": MODEL_VERSION,
        },
        index=df.index,
    )
    return out
