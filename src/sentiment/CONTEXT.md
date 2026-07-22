# Context — `src/sentiment` (news sentiment + does it predict price?)

> Handoff notes for a new session. This module scores Vietnamese company news for
> sentiment and tests, rigorously, whether that sentiment predicts short-horizon price
> movement. **The headline finding is a well-evidenced NEGATIVE result** (see §6): news
> text does not forecast 5-day price/direction/jumps for the tickers we have. The code is
> a clean, point-in-time-correct experimental harness; verify anything before acting on it.

## 0. TL;DR

- **Model 1 — text → sentiment** works as a *language* task (QWK 0.61 on 5 levels) but is
  **general-domain and finance-miscalibrated** (it scores a VCB dividend approval −0.97).
- **Model 2 — sentiment → price** does **not** work, in every framing we tried:
  - predict `close[N+5]` (regression) → **0/7 folds beat random-walk** `close[N+5]=close[N]`;
  - predict up/down direction → **~0.49 accuracy** (below a coin flip);
  - predict P(≥5% jump in 5d) → **ROC-AUC ≈ 0.5**, top-decile lift **0.74×** (worse than base rate);
  - redefine sentiment BY the price reaction (5-level, exchange-aware bands) and predict it
    from text → **QWK ≈ 0.00** (text carries no forward-price signal).
- This is consistent with the thesis memory (`project-vcb-forecasting-conclusion`,
  `project-cross-sectional-strategy`): single-stock short-horizon moves are ~unpredictable.
- ⚠️ **Only 3 tickers have news** (VCB/FPT/PNJ) — too few for a real market/sentiment
  relationship; the negative result is about *this* data, and would need breadth (VN30/100)
  and a finance-tuned label to be a general claim.

## 1. Where it sits

- **Input:** `bronze.cafef_news` (5,599 rows, headline+content+type+category+timestamp,
  Vietnamese) and `silver.stocks_basic` (daily `close_adjust`, volume, foreign flow).
- **Model 1 output (materialised):** `silver.cafef_news_sentiment` — one row per news
  `row_id`, written by `data_preprocessor._ingest_silver_cafef_news_sentiment` (gated by the
  `data_preprocessor/data_quality_silver/news_sentiment` switch leaf). See
  `data_preprocessor/CONTEXT.md` for the ETL; the scoring itself is this module.
- **Model 2:** experimental only — prototypes below read those tables and print
  out-of-sample metrics. **Nothing from Model 2 is written to the DB** (it found no signal
  worth persisting).
- **Design precedent:** mirrors `src/ta` — pure functions here, orchestration/DB in
  `data_preprocessor`. Model-loading files are DB-agnostic and import torch/transformers
  lazily.

## 2. Model 1 — Vietnamese news sentiment (the scorer)

`sentiment_functions.py` — a PhoBERT-based 3-class Vietnamese sentiment classifier
(`mr4/phobert-base-vi-sentiment-analysis`), loaded once, GPU when available (~500 texts/s
on an RTX 3050 → the whole news table scores in ~12 s).

- **`VietnameseSentimentModel`** — lazy singleton wrapper; VN labels (Tiêu cực / Tích cực /
  Trung tính) are translated to `negative/neutral/positive`, failing loudly on an unknown
  label. `score_texts` → per-text `SentimentResult(label, score, prob_neg/neu/pos)` where
  **`score = p(pos) − p(neg) ∈ [−1, 1]`** (signed polarity), batched + softmaxed on-device.
- **`build_scored_text`** — headline always; for `editorial` rows a lead slice of the body
  is appended (disclosures are short filing stubs whose headline says it all).
- **`score_news_frame`** — DB-agnostic: news DataFrame → the sentiment columns aligned by
  index. This is what the silver ingest calls.
- Output columns on `silver.cafef_news_sentiment`: `sentiment_label`, signed
  `sentiment_score`, `prob_{negative,neutral,positive}`, `model_version`.

⚠️ **Finance-miscalibration (important, recurring caveat).** The model is general-domain,
so procedural/finance language is mislabelled: dividend approvals, bond issuances and share
placements read strongly NEGATIVE (e.g. "VCB: …chi trả cổ tức 2025…" → −0.97), and routine
"an toàn tài chính" filings read positive. On long editorials it skews even more negative
(scoring full content pushed ~51% of articles to VERY_NEGATIVE). So the score is a
*general Vietnamese tone* signal, **not** a calibrated financial-sentiment measure.

### 1-bis. A 5-level text-sentiment experiment (scratchpad, not shipped)
Bootstrapping 5 levels from the signed score (bins at ±0.2/±0.6) and training a frozen-
PhoBERT-embedding + logistic head reproduced them at **QWK 0.613 / macro-F1 0.49** — a
solid *text* result, because the label is derived from the text. Content-vs-headline sweep:
content-only lifted QWK to 0.75 but skewed labels negative; **headline+content** is the
balanced choice. This only proves the text is *self-consistent*, not that it predicts price.

## 3. Model 2 — does sentiment predict price? (the experiments)

All Model-2 code shares one **point-in-time discipline** (`sentiment_features.py`):

- **`build_event_panel`** — per-`(ticker, date)` panel: trailing-week **sentiment features**
  (`sent_mean_week`, `sent_sum_week`, `news_week`, `very_pos_week`, `very_neg_week`) +
  **price/TA/foreign** features (returns, SMA gaps, RSI, vol ratio, foreign-net ratio, order
  imbalance) + targets `close_fwd = close_adjust[t+H]`, `up_fwd`, and
  `jump_fwd = 1{fwd_ret ≥ threshold}`. Only rows with news in the trailing week are kept.
- **`purged_walkforward_folds`** — ⚠️ the whole point: the targets peek `H` days ahead, so
  training uses only events whose forward window **closed before** each cut, an **`H`-day
  embargo** is dropped around the cut, and test is strictly after it. Cuts slide across
  2019-12 … 2025-06 and metrics are averaged. **Never a random split.**

### 3a. Predict `close[N+5]` and direction — `price_predictor.py` / `run_prototype.py`
Two heads (GradientBoosting regressor for the close, classifier for direction), each judged
against its honest baseline: **random-walk `close[t+H]=close[t]`** for price, **majority /
0.5** for direction. `evaluate` sweeps feature sets (sentiment-only / price-only / all) —
the **ablation is the finding**.

### 3b. Predict P(≥5% jump in 5 days) — `jump_predictor.py` / `run_jump_prototype.py`
Your requested target: probability the close rises ≥ threshold within `horizon` (default 5d,
5%). **Base rate 11.3%** (1,040/9,234), so it's evaluated by **ROC-AUC / PR-AUC / Brier +
top-decile lift** vs base rate (NOT accuracy — an "always-no" model scores 88.7%).
Logistic + GradientBoosting, both **calibrated** (`CalibratedClassifierCV`, sigmoid),
natural class balance kept. **Sentiment-only features by request.**

### 3c. Redefine sentiment BY price reaction — `price_reaction_labels.py` /
`text_reaction_model.py` / `run_reaction_prototype.py`
The pivot: stop trusting the language model's label; **define sentiment as what the price
did.** For each news mapped to its trading day, the `H`-day forward return is bucketed into
**5 exchange-aware levels**:

| Level | Forward return |
|---|---|
| VERY_NEGATIVE | `< −limit` |
| NEGATIVE | `−limit … −neutral` |
| NEUTRAL | `|r| ≤ neutral` (±2%) |
| POSITIVE | `+neutral … +limit` |
| VERY_POSITIVE | `> +limit` |

⚠️ **`limit` is the exchange daily price band (biên độ)** — HOSE ±7%, HNX ±10%, UPCoM ±15%
(`EXCHANGE_DAILY_LIMIT`) — a principled "more than one full limit day of net move" cut for
the extreme classes. Default H=5. Then `text_reaction_model.py` trains a text model (PhoBERT
frozen mean-pooled embeddings of headline+content) to **predict that price-defined label**,
scored by **macro-F1 + QWK** on the purged walk-forward. Models: Logistic +
**HistGradientBoosting** (1–2 orders faster than exact GB on the 768-dim embedding).

## 4. Running the prototypes

```
cd src
python -m sentiment.run_prototype              # close[N+5] + direction, feature ablation
python -m sentiment.run_jump_prototype         # P(>=5% in 5d), AUC/PR/Brier/lift
python -m sentiment.run_reaction_prototype     # price-defined 5-level, text->label, F1/QWK
   # flags: --threshold 0.05 --horizon 5 --neutral 0.02 --rescore
```

Each: loads tables → (Model 1 scores, stored or `--rescore`) → panel/labels → walk-forward →
prints the report + a one-line VERDICT. Read-only on the DB.

## 5. Gotchas

- **⚠️ `HF_HUB_OFFLINE=1` is REQUIRED and now baked in.** `from_pretrained` does a network
  HEAD request to huggingface.co on every load; on a blocked/flaky connection it retries with
  backoff and **hangs for many minutes** (this bit us — a run sat "running" ~15 min with empty
  output). `sentiment_functions.py` and `text_reaction_model.py` set `HF_HUB_OFFLINE=1` /
  `TRANSFORMERS_OFFLINE=1` at import (weights are cached locally); keep it that way.
- **Stale GPU processes.** The 4 GB RTX 3050 fills up fast; leftover python processes from
  earlier runs fragment VRAM and slow everything. Check `nvidia-smi` if a run crawls.
- **`stocks_basic` is ~2.4M rows** — the runners push the ticker filter **server-side**
  (`Condition` + `SqlOperator.IN`); never fetch it whole and filter in pandas (that alone
  stalled a run past the 5-min timeout).
- **Exact `GradientBoosting` is slow** across 7 folds × 2 models on dense 768-dim embeddings.
  `text_reaction_model` uses `HistGradientBoostingClassifier`; the price/jump predictors still
  use exact GB (fewer/narrower features, tolerable) — swap to Hist if you widen them.
- **Metric choice is load-bearing.** With an 11% jump base rate and a 48% neutral majority,
  **accuracy is meaningless** — use ROC-AUC/PR-AUC/Brier/lift for the jump, macro-F1/QWK for
  the levels, and **always report the baseline** (random-walk / majority / base-rate). The
  negative results are only trustworthy *because* they're measured against those.
- **`driver.select` returns `numeric` as `Decimal`→pandas `object`**; the feature builders
  `pd.to_numeric(...)` everything, so this is handled — but keep it in mind if you add columns.

## 6. Results (as run, 3 tickers, 7-fold purged walk-forward)

| Experiment | Target | Metric | Model | Baseline | Verdict |
|---|---|---|---|---|---|
| 3a | `close[N+5]` | MAE | 2,680 (all feats) | 1,957 (random-walk) | **0/7 beat RW** |
| 3a | direction | accuracy | ~0.49 | ~0.49 majority | no edge |
| 3b | P(≥5% / 5d) | ROC-AUC | 0.506 (logistic) | 0.5 | no signal; lift@10% **0.74×** |
| 3c | 5-level price reaction | QWK | 0.00–0.04 | 0.00 | text can't predict reaction |
| 1-bis | 5-level text sentiment | QWK | 0.613 | 0.00 | learnable (label is FROM text) |

**Interpretation.** Sentiment is learnable from text **only when the label is derived from
the text** (1-bis). The moment the target is *price* — level, direction, jump, or a
price-defined sentiment label — the signal vanishes. The news headlines/content we have do
not forecast 5-day price for VCB/FPT/PNJ.

### 6a. Incremental ablation — does sentiment add anything ON TOP of price/TA?

The sharpest test (scratchpad probe, same 7-fold purged walk-forward, HistGB): hold the
target fixed and compare **sentiment/text-only vs price/TA-only vs both**. If sentiment
carried *any* independent signal, `price+sentiment` should beat `price-only`.

| Target | features | metric | value | vs baseline |
|---|---|---|---:|---|
| direction (5d) | sentiment-only | ROC-AUC | 0.482 | below 0.5 |
| direction (5d) | **price/TA-only** | ROC-AUC | **0.543** | slight signal |
| direction (5d) | price + sentiment | ROC-AUC | 0.534 | **worse than price alone** |
| 5-level reaction | text-emb-only | QWK | −0.012 | ≈ 0 |
| 5-level reaction | **price/TA-only** | QWK | **0.175** | real, weak signal |
| 5-level reaction | text + price | QWK | 0.045 | **collapses vs price alone** |

**Two conclusions, both important:**

1. **Adding sentiment makes the model WORSE, not better** — direction 0.543 → 0.534, and
   5-level QWK **0.175 → 0.045** (the 768-dim text embedding swamps the 14 price features:
   textbook noise-feature degradation). So sentiment is not merely useless on its own — it
   **actively dilutes** the signal price/TA already has. It carries **zero incremental
   information** for 5-day price.
2. **The only (faint) predictability in this data is TECHNICAL, not sentiment** — price/TA
   -only is consistently above baseline across the 7 folds (direction AUC 0.543, 5-level QWK
   0.175). Weak, but real and repeatable. This points back at the thesis's price/technical
   and cross-sectional threads, not news.

So the full verdict is stronger than §6 alone: news sentiment does not predict 5-day price
for VCB/FPT/PNJ **on its own or as an add-on**, and what little signal exists comes from
price technicals.

## 7. If continuing — the honest next moves

1. ~~Text + price/TA together (does sentiment add on top of price?)~~ **DONE — see §6a:**
   it does not; adding sentiment makes it worse, and the only faint signal is price/TA.
2. **Cross-sectional relative return**, not absolute price — the thesis's documented tradeable
   target (`project-cross-sectional-strategy`); needs many more tickers than 3.
3. **More tickers** — the news scraper covers only 3; breadth is the biggest lever.
4. **Finance-tuned labels** — an LLM/rubric (local, per the constraint) to fix the
   general-domain miscalibration in Model 1, so at least the *sentiment* is trustworthy even
   if it still doesn't predict price.
5. **Lower threshold / longer horizon** for the jump (3% or 10d gives more positives) — but
   expect the same ceiling.
