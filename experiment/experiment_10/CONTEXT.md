# Experiment 10 — Literature: sentiment / text as an external factor

Unlike experiments 1–9, this folder holds **papers, not code**. It is the reading
file for the one orthogonal-data lever the thesis has *not* built: **text**
(news, social media) as a predictor, which experiment_3's conclusion — *"the
binding constraint is DATA, not model/target"* — puts on the table.

One entry per paper below: what it does, what one training sample looks like,
what it actually establishes, and **whether to cite it, follow it, or both**.

## Naming convention

**Every PDF is prefixed with its paper number** — `9. …pdf`, `28. …pdf`, `43. …pdf`.
The number is the paper's identity everywhere: the `#` column below, the section
heading (`# Paper 28 — …`), the file index, and how the paper is referred to in
conversation. **"analyze paper 28" means the file whose filename starts with `28.`**
— resolve by prefix, not by title. Numbers are assigned by the user and are not
sequential; gaps are expected.

Workflow: one paper per request → analysis (sample I/O · model · cite? · follow?) →
this file updated → committed and pushed. PDFs themselves stay untracked
(`.gitignore: *.pdf`); **CONTEXT.md is the only artefact in git.**

| # | Paper | Year | Verdict |
|---|---|---|---|
| 9 | Khan et al. — *Stock market prediction using ML classifiers and social media, news* | 2020 | **Cite, do not follow** |
| 28 | Vargas, de Lima, Evsukoff — *Deep learning for stock market prediction from financial news articles* | 2017 | **Cite; borrow the architecture, not the setup** |
| 43 | Usmani & Shamsi — *LSTM based stock prediction using weighted and categorized financial news* | 2023 | **Cite; borrow the NEWS HIERARCHY — the one idea to build on** |
| 44 | Huang et al. — *ML-GAT: a multilevel graph attention model for stock prediction* | 2022 | **Cite for its NEGATIVE results; do not follow** |
| 45 | Xiao & Ihnaini — *Stock trend prediction using sentiment analysis* | 2023 | **Cite the RECIPE (tool validation + market-hours window); do not follow the design** |
| 46 | Wu, Liu, Zou, Weng — *S_I_LSTM: stock price prediction based on multiple data sources and sentiment analysis* | 2021 | **⚠️ PROVABLE LABEL LEAK — cite only the CJK pipeline + forum-heavy corpus** |
| 47 | Koukaras, Nousi, Tjortjis — *Stock market prediction using microblogging sentiment analysis and ML* | 2022 | **⚠️ SELF-REFUTING — cite as the evaluation cautionary tale; do not follow** |
| 48 | Gu et al. — *Predicting stock prices with FinBERT-LSTM: integrating news sentiment analysis* | 2024 | **Cite the 3-D SOFTMAX sentiment + the corpus scale; ⚠️ worse than persistence** |
| 49 | Maqbool et al. — *Stock prediction by integrating sentiment scores of financial news and MLP-Regressor* | 2023 | **⭐ Cite the 10/30/100-day DECAY TABLE — the folder's best test-size evidence** |
| 50 | Khedr, Salama, Yaseen — *Predicting stock market behavior using data mining technique and news sentiment analysis* | 2017 | **⚠️ FEATURE = LABEL. Cite only its Kappa 0.078 — sentiment alone has no skill** |
| 51 | Fazlija & Harder — *Using financial news sentiment for stock price direction prediction* | 2022 | **⭐⭐ THE METHODOLOGICAL TEMPLATE — follow the PROTOCOL, not the method** |
| 52 | Nemes & Kiss — *Prediction of stock values changes using sentiment analysis of stock news headlines* | 2021 | **⚠️ NO PREDICTION MODEL AT ALL — cite only as "tool choice decides the answer"** |
| 53 | Li, Wu, Wang — *Incorporating stock prices and news sentiments for stock market prediction: a case of Hong Kong* | 2020 | **⭐ Cite the QUANTILE LABELS + LMFinance win; ⚠️ every result is below its own base rate** |
| 54 | Joshi, Bharathi, Rao — *Stock trend prediction using news sentiment analysis* | 2016 | **⚠️ CIRCULAR LABELLING — cite only as the anti-pattern; no stock prediction in it** |
| 55 | Bharathi & Geetha — *Sentiment analysis for effective stock market prediction* | 2017 | **⭐ Cite the VETO framing (sentiment abstains, never generates); ⚠️ target undefined** |
| 56 | Li, Xie, Chen, Wang, Deng — *News impact on stock price return via sentiment analysis* | 2014 | **⭐⭐ THE THRESHOLD PROOF — accuracy is gameable by labelling; th must exceed costs** |
| 64 | *(duplicate of 56 — byte-identical file)* | — | see paper 56 |

**They form a progression, and the progression is the point.** All five add news or
social text to price features to predict short-horizon direction, and they differ in
*how the text enters the model*:

| | paper 9 (2020) | paper 28 (2017) | paper 43 (2023) | paper 44 (2022) | paper 45 (2023) |
|---|---|---|---|---|---|
| text enters as | **one scalar** beside OHLCV | a **sequence + own encoder** | **three weighted streams** (market/sector/stock) | **BERT `[CLS]`** summed into a graph node | **one scalar**, but signed, engagement-weighted, market-hours aligned |
| price features | yes | yes | yes | yes | **none** ⚠️ |
| stock relations | — | — | implicit (sector) | **explicit graph** (Wikidata) | — |
| sentiment tool validated? | no | n/a | no (3 lexicons compared post-hoc) | no | **⭐ YES — on labelled benchmarks first** |
| aggregation window | calendar day ⚠️ | calendar day ⚠️ | calendar day ⚠️ | calendar day ⚠️ | **market hours, tested** ⭐ |
| split | random 70/30 ⚠️ | chronological | time-series CV, val ahead | chronological | **10-fold CV** ⚠️ |
| test size | 150 | **187** ⚠️ | 41 stocks, per-stock | 316 d × 423/286 stocks | **~25/fold** ⚠️ |
| repeats | 1 | 1 | 1 | **10, means reported** | 1 |
| baseline reported | — | **none** ⚠️ | **none** ⚠️ | **none** ⚠️ | **none** ⚠️ |
| accuracy | 80.5% (leaked) | 62.0% | **~0.50–0.55** | ~0.51/0.57 (**3-class**) | ~0.60 (leaked) |

**Paper 46 sits outside this table** — it is the only **regression** paper (target =
the closing price level, scored by MAE/MSE/RMSE) and the only one with a *provable*
label leak, so its numbers are not comparable to any column above.

Read together: **the more careful the evaluation gets, the closer accuracy falls to
chance.** Papers 43 and 44 are the most methodologically careful and both land near
coin-flip; papers 9 and 45 are the loosest and report the highest numbers. That
ordering is itself the folder's main finding. **Not one of the six reports a
majority-class or naive baseline.**

**The design splits cleanly across papers.** Nobody has all the pieces:
**45 = the aggregation recipe** · **43 = the hierarchy** · **28 = the encoder shape**
· **44 = the warning** · **46 = the non-English pipeline stage**. Paper 9 contributes
nothing structural.

---

# Paper 9 — Khan, Ghazanfar, Azam, Karami, Alyoubi, Alfakeeh (2020)

*Stock market prediction using machine learning classifiers and social media, news.*
**J. Ambient Intelligence and Humanized Computing**, Springer.
DOI 10.1007/s12652-020-01839-w · file: `9. Stock market prediction using machine learning classifiers and social media, news.pdf` (24 pp).

> **→ Verdict: reference it in related work and in the evaluation-design
> justification; do NOT replicate the method.** Its value to this thesis is as
> (a) the clearest published worked example of the naive *append-sentiment-to-OHLCV*
> pipeline, and (b) a named, citable specimen of the leakage pattern that makes
> daily-direction accuracy look like 80%.

### Setup

| | |
|---|---|
| Universe | 11 series: KSE, LSE, NYSE (exchanges) + HPQ, IBM, MSFT, ORCL, RHT, TWTR, MSI, NOK |
| Period | 2016-07-01 → 2018-06-30 (2 yrs, ~500 trading days) |
| Price data | Yahoo Finance (`Adjusted Close` **discarded**, §3.2.2) |
| Social text | Twitter via GetOldTweets-python, **cashtag** query (`$HPQ`); TWTR 380k … **KSE 34 tweets** |
| News text | Business Insider **headlines only** via JSOUP; MSFT 3,316 … **KSE/NYSE 0** |
| Models | 12 classifiers (GNB, MNB, SVM, LR, MLP, KNN, CART, LDA, AB, GBM, RF, ET), scikit-learn |
| Split | random **70/30** (350 train / 150 test) + 10-fold CV for tuning |
| Reported on | **HPQ only**, despite 11 series collected |

### The sentiment pipeline (§3.2–3.4, §4.3)

1. **Preprocess** (identical both streams): tokenize → strip HTML / `@author` / cashtags →
   drop URLs → stopwords → stemming → de-duplicate tweets.
2. **Spam filter** (tweets only, §4.3): MNB trained on **380** labelled spam/ham tweets,
   81.74% test accuracy. Spam share: HPQ 14.97%, MSFT 7.54%, RHT 0.49%, LSE 14.0%, KSE 0%.
3. **Score** each document with **Stanford CoreNLP RNTN** (Socher et al. 2013, Sentiment
   Treebank, 215,154 phrases). Ordinal 0–4: `0` very negative · `1` negative · **`2` neutral**
   · `3` positive · `4` very positive. Chosen over lexicon/BoW because summing word polarities
   "ignores word order" and loses negation/conjunction scope — the one argument in the paper
   that transfers cleanly.
4. **Aggregate to one number per calendar date** by **summing** the document scores →
   `Social Sentiment` / `News Sentiment`.

### One training sample (news arm; social arm identical bar column 8)

`X` = 8 features, one row per trading day per stock:

| # | Feature | Type | Origin |
|---|---|---|---|
| 1 | Date | — | Yahoo row key (**used as a feature** — Fig. 3 sweeps K=4…8 over these 8) |
| 2–6 | Open, High, Low, Close, Volume | numeric | Yahoo Finance |
| 7 | Trend | categorical | Eq. 2 — sign(Close_d − Open_d) ∈ {Pos, Neu, Neg} |
| 8 | **News / Social Sentiment** | numeric | **sum** of RNTN scores over that date's documents |

`y` = `Future Trend_n` ∈ {Positive, Neutral, Negative}, Eq. 3, comparing today's close to the
close **n days later**. **n is fixed per model, not per sample** — they train 10 separate
models, n = 1…10; that is what "Day 1 … Day 10" means on every accuracy chart.

```
X = [7/1/2016, 12.55, 12.76, 12.51, 12.73, 11865149, "Positive", 6]
              open   high   low   close   volume    trend    sentiment
y = "Negative"        # for the n-day-horizon model being trained
```

Preprocessing: `StandardScaler` inside a `Pipeline` fitted per CV fold (§3.6.1 — the one
leakage control they get right), optional `SelectKBest(chi2)` or `PCA` 8 → 6 (§4.2).

### Headline results (HPQ, day 9, best classifier RF)

| setup | accuracy |
|---|---|
| price only | 75.16% |
| **+ social sentiment** | **80.53%** (+5.37) |
| price only (news-arm baseline) | 69.79% |
| **+ news sentiment** | **75.16%** (+5.37) |
| social + news combined | 79.86% (ET, day 10) — best single model **drops** |
| RF + majority-vote ensemble (RF/ET/GBM) | 83.22% |

Secondary claims: RF is the most consistent classifier; SelectKBest > PCA; spam reduction
helps 7 of 12 classifiers and **hurts** KNN/GBM/ET; MLP gains up to +7.3% at 3 hidden layers;
volatility (σ, β, closing-price fluctuation) flags RHT and NYSE as hardest to predict;
IBM/TWTR/NYSE most social-media-influenced, MSFT/TWTR/LSE most news-influenced — the
"influence" finding derived purely by **eyeballing spike counts** in Figs. 15–16, no test.

### ⚠️ Why the method is not followable

1. **The daily sum measures VOLUME, not polarity.** Neutral = 2, not 0, so a day with 50
   neutral tweets scores 100 and a day with 20 very-positive tweets scores 80. Their own
   Table 2 shows it — HPQ's column runs 0, 33, 41, 20, 7, 26, 50… tracking tweet count. The
   paper states "if the overall sentiment count on a date is higher, then sentiment positivity
   is higher", which only holds at constant daily volume.
2. **Random 70/30 split on time-ordered data**, with 10-fold CV over the same ordered set.
3. **The n=10 label overlaps across 10 consecutive rows** — rows *d* and *d+1* share 9 of 10
   days of price path. Random splitting puts near-duplicates on both sides. This is the most
   likely source of the 75–80% figures.
4. **`Date` is a model feature** on a monotonically trending 2-year stock — a tree can split
   on it and recover much of the label directly.
5. **No lag on sentiment.** Headlines from date *d*, including post-close ones, sit in the
   same row as date *d*'s OHLCV and predict forward from *d*.
6. **The Neutral class is never predicted.** Tables 4–5 show 0.00 / NA precision, recall and
   F for neutral across essentially all 12 classifiers — it requires two closes to be exactly
   equal. So "3-class accuracy" is binary accuracy on an imbalanced set.
7. **The +5.37 pp gain is byte-identical in both arms.** Two independent feature sets giving
   the same delta to two decimals; the authors notice and explain it away as "the same
   sentiment technique". More consistent with a reporting artifact.
8. **`Trend` is a deterministic function of `Open` and `Close`**, both already in X.
9. **chi2 requires non-negative inputs; StandardScaler emits negatives.** The paper describes
   both without stating the order — as written, that combination raises in scikit-learn.
10. **Eq. 3 sign convention is wrong as printed** (`P_tc − P_nc > 0` → "Positive" labels a
    price *fall* as positive), contradicting the surrounding prose.
11. **Data too thin for the claims:** ~500 samples; KSE has 34 tweets and 0 news; LSE/NYSE
    have 0 news — those series cannot support the stated analysis at all.

### How to use it in the thesis

**Cite — three places, increasing value:**
1. Generic "text is an external factor that moves prices" support. One sentence.
2. **The pipeline exemplar** — the explicit *scrape → clean → spam-filter → RNTN → daily
   aggregate → append to OHLCV → classify* chain. Most papers hand-wave the middle; this one
   spells it out. Cite it, then state what is done differently.
3. **The methodological counter-example** — the highest-value use. Experiment_3 already
   established that per-stock absolute direction is not the tradable target and that costed,
   look-ahead-free evaluation kills most apparent edges. This paper is a compact published
   specimen of the opposite: random split on overlapping labels, date-as-feature, accuracy on
   a class that is never predicted. A named example justifies the stricter evaluation design
   far better than asserting "many papers leak".

**Do not follow — four thesis-specific reasons:**
- **Wrong target.** Per-stock 3-class absolute direction; the working target here is `rel5`,
  market-relative ~1-week return (experiment_3.3).
- **Tooling does not transfer.** RNTN is English, trained on movie-review phrases, with no
  Vietnamese path and no financial-domain calibration. VN text needs a PhoBERT-class model
  fine-tuned on VN financial sentiment — and what labelled VN financial data exists must be
  checked before committing.
- **The input data does not exist yet.** Bronze holds prices, foreign flow and financials
  (Simplize / CafeF / TradingView). The nearest thing to a text corpus is **experiment_6's
  VCB news feed** — 1,629 headlines *with content*, one ticker. A VN news/forum corpus plus
  annotation plus a VN sentiment model is its own chapter.
- **The core aggregation is broken**, per ⚠️ 1.

**Salvageable, without following the paper:**
- Spam / low-quality filtering as an explicit **pre-sentiment** stage (matters more on VN
  forums than on Twitter).
- Scaler inside the CV pipeline (§3.6.1).
- Compositional sentiment over lexicon summing — the argument, not the RNTN.

**If sentiment is built, invert the two decisions that matter:**
- **Signed and normalised aggregation** — map 0–4 → −2…+2, take the **mean**, and carry
  document count as a *separate* feature so volume cannot masquerade as polarity.
- **Make the sentiment feature cross-sectional**, matching the `rel5` target: rank or
  z-score each stock's daily sentiment against the VN30/VN100 cross-section that day, so a
  market-wide mood shift cancels instead of becoming spurious signal. Then lag it — news from
  date *d* predicts from *d+1*.

**Cost/benefit:** roughly one paragraph in related work and one in the evaluation-design
justification. **Zero lines of code.**

---

# Paper 28 — Vargas, de Lima, Evsukoff (2017)

*Deep learning for stock market prediction from financial news articles.*
**IEEE** (978-1-5090-4253-1/17), COPPE / Universidade Federal do Rio de Janeiro.
file: `28. Deep learning for stock market prediction from financial news articles.pdf` (6 pp).
This is the "Vargas et al." that paper 9 lists in its related work.

> **→ Verdict: cite it, and borrow the ARCHITECTURE — a text encoder and a
> technical encoder fused late — but not the target, the temporal claims, or the
> evaluation.** More useful than paper 9: chronological splits, a precisely
> specified model, and three clean ablation axes. Undone by a **187-sample test
> set** and **no majority-class baseline**.

### Setup

| | |
|---|---|
| Target series | **S&P 500 index** (Yahoo Finance) — index level, not single stocks |
| Text | **106,494 Reuters articles**, 2006-10-20 → 2013-11-21 (the Ding et al. [29] dataset) |
| Filter | keep only titles referring to the ~100 index constituents → **17,171 documents survive** |
| Text used | **titles only** — [29] found titles more useful than article contents |
| Split | **chronological**: train 2006-10-02→2012-06-18 · dev →2013-02-21 · test 2013-02-22→2013-11-21 |
| Instances | **1,419 / 168 / 187** (≈10–12 news per day) |
| Framework | TensorFlow; SGD, momentum 0.9, lr 0.1 |

### One training sample

**One sample = one trading day.** Days with no released news are **dropped**.
Two parallel inputs:

| branch | shape | contents |
|---|---|---|
| **Text** | `L × 300` | `L` = news **titles** that day (varies). Each title → one 300-d **sentence embedding** = the **mean** of its word2vec word vectors |
| **Technical** | `7 × 5` | 7 technical indicators (from [3]) over a delay window `n = 5`, chronological |

`y` = binary one-hot — `[1,0]` if close(t+1) > close(t), `[0,1]` if it falls.

```
X_text = [[…300 floats…],    # title 1 of day t
          […300 floats…], …] # L rows, L varies per day
X_tech = 7 × 5 indicator matrix ending at day t
y      = [1,0]               # index rises tomorrow
```

Word2vec: 300-d CBOW **initialised from Google News pretrained vectors** (100 bn
words); out-of-vocabulary words initialised randomly.

### Model — SI-RCNN (Fig. 1)

Dual-branch, late fusion:

```
titles → sentence-embed → 1-D CNN → temporal max-pool → LSTM(128) ┐
                                                                   ├→ concat → FC + softmax → up/down
7 indicators × 5 days ──────────────────────────────→ LSTM(50)    ┘
```

- **CNN:** filter widths `[3×300] [4×300] [5×300]`, **64 filters each** (→192 maps),
  stride 1, padded, ReLU, **dropout 0.5**, max-pool window 2 → `[((L−2)/2)+1 × 192]`.
- **Recurrent:** conv output read as a sequence of `L−R+1` steps by a **128-unit
  LSTM**; the technical branch has its own **50-unit LSTM**.
- Naming grid `{W|S}{I?}-{CNN|RCNN}`: W = word embedding, S = sentence embedding,
  I = technical indicators included. Three clean ablation axes.

### Results — S&P 500 direction, test accuracy (Table II)

| model | test | | baseline | test |
|---|---|---|---|---|
| W-CNN | 57.22 | | BW-SVM [36] | 56.38 |
| S-CNN | 60.96 | | E-NN [29] | 58.83 |
| W-RCNN | 60.22 | | WB-CNN [30] | 60.57 |
| S-RCNN | 61.49 | | **EB-CNN [30]** | **64.21** |
| WI-RCNN | 61.29 | | | |
| **SI-RCNN (proposed)** | **62.03** | | | |

Claims: sentence embedding > word embedding (all 3 pairs); RCNN > CNN; technical
indicators help; **day-before news beats week/month aggregation**. The proposed
model **loses to EB-CNN's event embedding**, which the authors state plainly.

### ⚠️ Why the numbers cannot carry weight

1. **187 test samples.** SE on 62% ≈ **3.6 pp** → 95% interval ≈ **±7 pp**. Every
   ranking in the left column (S-RCNN 61.49 / WI-RCNN 61.29 / SI-RCNN 62.03) is
   noise. Single run, no seed variance.
2. **No majority-class baseline anywhere.** The test window (2013-02→2013-11) is a
   strong S&P bull run; "always predict up" plausibly scores mid-to-high 50s.
   **Until that number is computed, 62.03% cannot be called skill.** This is the
   decisive missing figure in a results section made entirely of accuracy.
3. **Days without news are dropped** — breaks series continuity, and the dropped
   days are not random.
4. **Accuracy only** — no precision/recall, no trading simulation, **no costs**
   (listed as future work).

### What it does right — and paper 9 does not

Chronological train/dev/test with genuine separation; an architecture specified
precisely enough to reimplement; a systematic ablation grid (W vs S, CNN vs RCNN,
±I). The *consistency* of S-* > W-* across three independent pairs is the paper's
most robust finding, even though each individual gap sits inside the noise band.

### How to use it in the thesis

**Cite — three uses, all higher-value than paper 9's:**
1. **The architectural template.** Text enters as a *sequence with its own encoder*,
   fused late with the technical branch — the direct answer to paper 9 collapsing it
   to one scalar. The 9-vs-28 contrast is a ready-made methods paragraph.
2. **The encoding choice** — sentence-level representation addresses word sparsity in
   short-title corpora (S-* > W-* on all three pairs).
3. **News has a short temporal effect** — day-before-only beats past-week and
   past-month aggregations. Their cleanest result (same architecture, ablated), and
   directly actionable for lagging/windowing a VN news feature.
   Via [29], **titles beat article bodies** — bears on experiment_6, which scraped both.

**Borrow:**
- **Two-branch late fusion.** The technical branch here is already built and
  saturated (GBM/MLP over 1,053 features, AUC 0.62–0.77). A parallel text encoder
  concatenated before the head is the correct integration and leaves the existing
  feature pipeline undisturbed.
- **Sentence-level embedding** for headlines — for VN that means a PhoBERT-class
  sentence encoder, not mean-pooled word2vec (2013-era, no usable VN equivalent).
- **Chronological train/dev/test** — already house standard; cite as precedent.

**Do not follow:**
- **The target.** Index-level binary next-day direction; the thesis target is
  **`rel5`** (cross-sectional market-relative 5-day return), settled in experiment_3.3.
- **The temporal architecture.** Experiments 1.6, 1.7, 2.1 and 2.3 already show
  sequence models do not beat point-in-time GBM on this data and that short lookbacks
  win — the LSTM-on-technicals branch is the part with the most evidence against it.
- **The evaluation.** 187 samples, accuracy only, no baseline, no costs. Experiment_3
  established costed walk-forward as the deciding test.

### ⚠️ The blocking practical issue — news DENSITY

SI-RCNN needs `L` titles **per day per target**, averaging **10–12**. Experiment_6's
VCB feed carries **1,629 headlines over 18.5 years ≈ 0.35 per trading day** — about
**30× too sparse**, and VCB is the best-covered ticker in the repo. Per-ticker, this
architecture is **not feasible on VN data as currently scraped**. Two routes, both
real work:

- **Pool market-wide news** (CafeF / VietStock market feed, not the per-ticker event
  stream) and treat it as a **common factor** — which suits the cross-sectional
  target: a market-wide text signal is exactly what should be differenced out per
  stock, or used to condition the cross-section.
- **Broaden the per-ticker scrape** beyond CafeF's disclosure feed to raise density.

Either way **the corpus must exist before the model question is askable.** That is
the sequencing, and it is the same conclusion paper 9 reaches from the other side.

---

# Paper 43 — Usmani, Shamsi (2023)

*LSTM based stock prediction using weighted and categorized financial news.*
**PLOS ONE** 18(3): e0282234 · DOI 10.1371/journal.pone.0282234 · Systems Research
Laboratory, FAST-NUCES Karachi. 27 pp, **open access**, **data public** on Mendeley
(`10.17632/mc4s7zvx9c.1`).
file: `43. LSTM based stock prediction using weighted and categorized financial news.pdf`.

> **→ Verdict: cite it, and borrow the NEWS HIERARCHY — market / sector / stock as
> three separately weighted streams. That single idea is the most transferable
> thing in this folder**, because it is already a cross-sectional decomposition and
> because it is the standard fix for a sparse per-ticker corpus. Take nothing else:
> the accuracies are ~0.50–0.55, i.e. coin-flip.

### Setup

| | |
|---|---|
| Market | **Pakistan Stock Exchange (PSX)**, prices Jan 2006 → Aug 2018 |
| Universe | **41 stocks / 6 sectors** (Oil & Gas, Textile, Technology & Communication, Power Gen & Distribution, Refinery, Commercial Banks) |
| Text | headlines from *The News* archive 2006–2018, categorised by the authors' own semi-automatic scheme [11] |
| Lexicons | **HIV4** (Harvard IV, general), **LM** (Loughran–McDonald, financial), **Vader** (social) |
| Time steps | **n ∈ {3, 7, 10}** — ablated |
| Split | 70% train; the remaining 30% halved into test + early-stopping. **Time-series split CV, 3 folds, validation always ahead of training** |
| Training | RMSProp, lr 0.001 with decay, batch 32, max 500 epochs, early stop patience 10; grid search over neurons {20,50,100,200}, dropout {0.2,0.35,0.5} |

### One training sample

**One sample = one (stock, day) pair.** Four parallel sequences over `[t−n, t−1]`:

| track | shape | contents |
|---|---|---|
| **δ** price | `n × 12` | close, volume + **10 technical indicators** — MA10/20/30, MACD (DIFF), MACD (DEA), MACD, RSI6/12/24, MFI (Table 3, from Li et al. [8]) |
| **θ1** market | `n × d` | daily lexicon sentiment of **PSX/market-wide** headlines |
| **θ2** sector | `n × d` | daily lexicon sentiment of that stock's **sector** headlines |
| **θ3** stock | `n × d` | daily lexicon sentiment of **the stock's own** headlines |

`y` = binary (Eq. 1): **`1` if `Close_t > Close_{t−1}`, else `0`** — next-day direction.
Inputs stop at `t−1`, so the alignment is clean; no leakage of the paper-9 kind.

⚠️ **`d` — the sentiment-vector width — is never stated.** It is the width of three
of the four input tracks. Genuine reproducibility gap.

### Model — WCN-LSTM (Fig. 2)

Four independent towers, hierarchical fusion:

```
δ  price+TA → LSTM → Drop → LSTM → Drop ──────────────────────┐
θ1 market   → LSTM → Drop → LSTM → Drop ──×α ┐                │
θ2 sector   → LSTM → Drop → LSTM → Drop ──×β ├→ concat → Dense┤→ concat → Dense(sigmoid)
θ3 stock    → LSTM → Drop → LSTM → Drop ──×γ ┘                ┘
```

subject to **α + β + γ = 1** (Eq. 3). Baseline = Li et al. [8]: same data, **one
uncategorised** sentiment stream concatenated by date into a single LSTM stack (Fig. 3).

⚠️ α/β/γ are called "learned weights" but Table 5 shows them **grid-searched** over
`{0.2, 0.3, 0.4, 0.5, 0.6}` under the sum-to-1 constraint — tuned hyper-parameters,
not gradient-learned. Mild overclaim.

**Their selected values are the most useful number in the paper:** α (market) lands
at **0.4–0.6 in all nine scenarios**; β (sector) and γ (stock) at 0.2–0.4. Market-level
news dominates — and market news is also by far the most abundant category (Table 2).

### Results

| | HIV4 | LM | Vader |
|---|---|---|---|
| **accuracy** WCN > LSTM, t=3 | 32/41 | 28/41 | 22/41 |
| t=7 | 28/41 | 23/41 | 23/41 |
| t=10 | 24/41 | 22/41 | 26/41 |
| **F1** WCN > LSTM, t=3 | 15/41 | 21/41 | 24/41 |
| t=10 | 17/41 | 18/41 | 18/41 |

Wilcoxon signed-rank on paired per-stock accuracy: WCN-LSTM significantly better in
**7 of 9** scenarios (Table 6). **HIV4 (general) beats LM (financial-domain)** —
flagged by the authors as unexpected, with BERT-based lexicon adaptation proposed as
future work. Time steps 3 and 7 beat 10 (Table 8).

### ⚠️ Three problems

1. **The accuracies are ~0.50–0.55.** Sector-average accuracy in Figs 10–12 runs
   **0.48–0.55**. On binary next-day direction that is coin-flip, and — as in paper
   28 — **no majority-class baseline appears anywhere**. The paper shows model A
   differs from model B; it does not show either has skill.
2. **Accuracy rises while F1 falls.** WCN-LSTM wins most accuracy cells and *loses*
   most F1 cells. On an imbalanced binary target that means it is predicting the
   majority class more often — buying accuracy with recall. Both numbers are
   reported; the contradiction is never interpreted. Most important unremarked
   finding in the paper, and it undercuts the headline claim.
3. **The Wilcoxon test does not test what matters.** Paired over 41 stocks, it asks
   "is WCN consistently above LSTM?" A consistent +2–3 pp gap around 50% is easily
   significant at n=41 and still economically meaningless. Significance *between two
   near-chance models* is not evidence of signal.

Also: stock-level news is desperately thin — Table 2 lists **BPL 0, HTL 0, SNGP 1,
PAKD 1, PKGP 1, KAPCO 2, SYS 2** headlines over 12 years, yet γ = 0.2–0.4 weights
those near-empty sequences. Refinery has 112 sector headlines over 12 years for 4
stocks. No trading simulation, no costs.

### What it does better than papers 9 and 28

Time-series split CV with validation ahead of training; **statistical testing across
stocks** (rare in this literature); an honest three-lexicon head-to-head that reports
the *domain-specific* lexicon losing; a time-step ablation; **public data**. It is
the most careful of the three — which is exactly why its near-chance accuracy is the
most informative result in the folder.

### How to use it in the thesis — the hierarchy is the deliverable

**The citable contribution is the three-level decomposition (market / sector / stock)
entering as separately weighted streams.** Not the LSTM, not the lexicons, not the
accuracy.

**1. The hierarchy IS a cross-sectional decomposition.** The thesis target is `rel5`
— market-*relative* 5-day return. Their three streams map onto it directly:

| their stream | effect on `rel5` |
|---|---|
| **market** news (θ1) | hits every stock alike → **cancels in the cross-section** |
| **sector** news (θ2) | the sector tilt — a real cross-sectional signal |
| **stock** news (θ3) | idiosyncratic — the purest `rel5` signal |

They need all three because they predict *absolute* direction. **The thesis needs the
decomposition for the opposite reason** — to separate what cancels from what does
not. Their finding that **α (market) dominates** is, for a cross-sectional target, a
statement that most of their signal is precisely the component to difference away.
That is a citable, non-obvious argument for the design here.

**2. It answers paper 28's density blocker.** Paper 28 needs ~10 titles/day/target;
experiment_6 delivers **0.35**. The hierarchy is the standard fix — sparse per-ticker
news is *backed by* abundant market- and sector-level streams, so a ticker with
near-zero own-news still gets a populated input. Table 2 is the proof case: stocks
with 0–2 headlines still train.

**3. The sector layer already exists here.** The Simplize industry tree (GICS-based,
10 groups / 50 sub-groups) gives ticker→sector for VN — that is θ2's grouping,
already scraped. Market level = a CafeF/VietStock market feed. Only θ3 needs the
per-ticker scrape experiment_6 prototyped.

**Do not take:**
- **Lexicon sentiment.** HIV4 / LM / Vader are English. "General beats domain-specific"
  is a claim about Pakistani English-language newspaper prose; for VN the question is
  *which fine-tuned transformer*, not which lexicon.
- **The four-tower LSTM.** Experiments 1.6, 1.7, 2.1, 2.3 show sequence models do not
  beat point-in-time GBM on this data. Keep the **streams**, feed them to the existing
  model class.
- **Target and evaluation.** Next-day absolute direction; accuracy/F1 only; no costs,
  no baseline.
- **α+β+γ=1 by grid search.** If streams are weighted, let the model learn it
  (attention, or plain learned scalars) — or hand all three to a GBM and read the
  importances.

⚠️ **Honest caveat:** paper 43 gets ~0.52 on 12 years of real news across 41 stocks
with a careful protocol. Adopt the hierarchy because it makes a sparse VN corpus
*usable*, **not** because it promises accuracy.

---

# Paper 44 — Huang, Li, Liu, Yang, Yu (2022)

*ML-GAT: A Multilevel Graph Attention Model for Stock Prediction.*
**IEEE Access** vol. 10, pp. 86408–86422 · DOI 10.1109/ACCESS.2022.3199008 ·
Zhejiang Univ. of Science & Technology / Zhejiang Yuexiu Univ. / UCSI Malaysia.
15 pp, open access (CC BY-NC-ND).
file: `44. ML-GATA_Multilevel_Graph_Attention_Model_for_Stock_Prediction.pdf`
(*filename says "ML-GATA"; the paper is ML-GAT*).

> **→ Verdict: cite it for its NEGATIVE results — accuracy up / Sharpe down, and
> relation choice mattering more than architecture — and do not follow the method.**
> The relational module rests on Wikidata corporate entities, which do not exist at
> usable coverage for VN listed firms, and the headline gain is contaminated by
> selecting relation types on the test set.

### Setup

| | |
|---|---|
| Universe | **S&P 500 → 423 stocks** + **CSI 300 → 286 stocks** (stocks with no Wikidata relations dropped) |
| Price | Yahoo Finance (S&P) / CSMAR (CSI) |
| Text | ~**150,000** news texts tied to target stocks |
| Relations | **Wikidata** — 9 first-order + 62 second-order relation types (Tables 7–8), meta-path ≤ 2 hops, heterogeneous → homogeneous company graph |
| Split | **chronological** — train 2013-02-08→2017-05-23 (1,080 d) · val →2018-03-27 (213 d) · test →2019-08-29 (**316 d**) |
| Training | Adam, lr 5e-4, weight decay 5e-5, batch 32, dropout 0.5, leaky ReLU, 100 epochs, **10 independent repetitions** |

### One training sample

**One sample = one (stock, day) node in a graph spanning the whole index.** Three
encoders feed one node representation:

| source | encoder | output |
|---|---|---|
| **price** | LSTM over the **price change rate** `R_i^t = (P^t − P^{t−1})/P^{t−1}`, **sequence length 50** | last hidden state `e_i ∈ R^128` |
| **news** | **BERT**, `T` texts for that stock, final-layer `[CLS]`, max 128 tokens, batch 32 | `f_i ∈ R^V` |
| **relations** | Wikidata graph → meta-path → **ML-GAT** two-level attention | `e_i^r` |

Fused by **plain addition**: `rep_i = e_i^r + e_i + f_i` (Eq. 12), then
`ŷ_i = softmax(W_n·rep_i + b_n)` (Eq. 13), cross-entropy loss (Eq. 14).

`y` = **3 classes** (Eq. 15): `up` if `R ≥ r_up`, `down` if `R ≤ r_down`, else `neutral`.

### Model — the actual contribution

Two stacked attention levels over the relation graph:

```
level 1  "state attention"    → attend over NEIGHBOURS within one relation type r_m → Z_i^{r_m}  (Eq. 8–9)
level 2  "relation attention" → attend ACROSS relation types                        → e_i^r      (Eq. 10–11)
```

*Which peers matter inside a relation*, then *which relations matter at all*. This is
a **learned generalisation of paper 43's hand-specified market/sector/stock weights**
— arbitrary relation types, gradient-learned instead of grid-searched.

### Results — mean of 10 runs (Tables 5–6)

| | MLP | CNN | LSTM | GCN | TGC | **ML-GAT** |
|---|---|---|---|---|---|---|
| F1 S&P / CSI | .357/.379 | .394/.383 | .400/.427 | .415/.432 | .441/.451 | **.510/.569** |
| Acc S&P / CSI | .302/.359 | .328/.392 | .443/.403 | .442/.414 | .463/.444 | **.509/.570** |
| Sharpe S&P / CSI | .42/.48 | .43/.49 | .79/.75 | .52/.59 | .85/.95 | **1.89/1.90** |

### ⚠️ Five problems, two of them serious

1. **⚠️ Relation types were selected ON THE TEST SET.** Tables 3–4 rank every relation
   type by **test-set F1** (best 0.458, worst 0.267), then §IV-E: *"we use the 10 best
   relations obtained in our experiments to create relational graphs."* Headline
   numbers are reported on the same test set used to choose the graph. Since relation
   choice alone swings F1 by **19 pp**, this plausibly accounts for the entire margin
   over TGC.
2. **⚠️ The prediction offset is never stated.** The LSTM consumes `R_i^t` up to and
   including `t`, and the label is the class of *a* price change rate — the paper
   never says `R^t` or `R^{t+1}`. With the trading rule ("if upside probability is
   highest, the stock is bought **at the closing price of the day**"), the decision
   uses the day-*t* close and executes at the day-*t* close. As written it cannot be
   shown look-ahead-free.
3. **The label thresholds are broken as printed.** `r_up = 0.6`, `r_down = −0.6` on a
   *rate* means **±60% in a day** — under which nearly every label is `neutral` and a
   trivial classifier scores ~99%, contradicting the reported 0.30–0.57. The real
   threshold must be 0.6% or a scaled quantity; the paper never says. Same units
   ambiguity hits "average daily return 0.1193" — 11.9% *per day* as a fraction,
   irreconcilable with Fig. 4's 1000 → ~2000 over 316 days.
4. **No majority-class baseline** — fourth paper in a row. On 3 classes with a
   dominant `neutral`, 0.51 may sit *below* "always predict neutral".
5. **Trading sim has no costs**, goes **all-in on a single stock**, and Sharpe is
   never stated as daily or annualised (per-run values span −2.08 to +6.2).

### What it does right

Chronological split with explicit dates; **10 independent repetitions with means
reported** — the only paper in this folder to do so; profitability metrics alongside
accuracy; two real ablations (news component, relation types).

### ⭐ The most valuable result is a NEGATIVE one

**Fig. 3 — adding the BERT news module raises F1 (+18.08%), accuracy (+11.54%) and
average daily return (+2.3%), but LOWERS the Sharpe ratio.** The authors call it
regrettable and blame news sparsity.

That is **experiment_3's conclusion reached independently**, by another team, on
another market: *classification gains do not translate into risk-adjusted return.* A
published paper whose own ablation shows accuracy improving while Sharpe degrades is
a far stronger citation for the evaluation chapter than any in-house assertion.

Second citable negative: **GCN — which aggregates all relations equally — scores
below plain LSTM on accuracy** (.442 vs .443 S&P). Naive relational averaging does
not help; only weighted aggregation does. And relation *choice* moves F1 from 0.267
to 0.458 — **the edges matter more than the architecture**.

### How to use it in the thesis

**Cite — three uses:**
1. **The accuracy-vs-Sharpe divergence** (Fig. 3). Independent corroboration of
   experiment_3. This alone justifies the citation.
2. **Relation choice dominates architecture** (Tables 3–4) and **unweighted graph
   aggregation underperforms no graph at all** (GCN < LSTM). The two things to know
   before building any cross-sectional graph.
3. **Multilevel attention** — within-relation then across-relation — as the learned
   generalisation of paper 43's fixed market/sector/stock weights.

**Do not follow:**
- **⚠️ Wikidata corporate relations do not exist for VN at usable coverage.** The
  whole relational module rests on them; VN listed firms have thin-to-absent entries.
  Their own drop from 500 → **423** S&P constituents shows the coverage cliff even
  for US large-caps.
- **The headline gain is contaminated** by test-set relation selection — there is no
  reliable effect size to aim at.
- **The offset ambiguity** makes a look-ahead-safe reimplementation impossible from
  the paper alone.
- Experiments 1.6, 1.7, 2.1–2.3 already show DL losing to point-in-time GBM on this
  panel; a GNN over 30–100 tickers with thin edges is not where that flips.

**What IS worth taking, cheaply:** experiment 1.8 already found **cross-sectional
volatility rank** among the few useful added features. A per-date cross-sectional
rank is a degenerate, zero-cost version of what a GNN computes by message-passing
over a fully-connected panel. If more relational structure is ever wanted, the
affordable VN edge sets are **sector membership** (Simplize tree, already scraped)
and **ownership / subsidiary links** (recoverable from statement notes via the
experiment_7/8/9 pipeline) — **not Wikidata**.

---

# Paper 45 — Xiao, Ihnaini (2023)

*Stock trend prediction using sentiment analysis.*
**PeerJ Computer Science** 9:e1293 · DOI 10.7717/peerj-cs.1293 · Dept. of Computer
Science, Wenzhou Kean University, China. 18 pp, open access CC-BY, **raw data in
supplemental files**.
file: `45. Stock trend prediction using sentiment analysis.pdf`.

> **→ Verdict: cite it for the AGGREGATION RECIPE — validate the sentiment scorer on
> labelled data first, then aggregate over a MARKET-HOURS window with holiday decay
> and engagement weighting. Do not follow the experimental design**, which is the
> weakest in the folder (10-fold CV on a time series, one year, four stocks, no price
> features, no baseline).

### Setup

| | |
|---|---|
| Universe | **4 stocks** — AAPL, MSFT (Technology), AMZN, NFLX (Service) |
| Period | **2021-01-04 → 2021-12-31 — ONE YEAR**, ~250 days/stock |
| Tweets | **260,000**, harvested by cashtag via **Twint** (not the official API) |
| News | **6,000** headlines from 8 outlets — CNBC, Forbes, The Street, Reuters, The Motley Fool, Business Insider, WSJ, Bloomberg |
| Models | KNN, Tree, SVM, RF, **NB**, LR — **10-fold CV** |

### ⭐ The genuinely good part — sentiment tools VALIDATED before use

Three scorers benchmarked on **labelled** data first: 1,300 hand-labelled tweets, and
the **Financial PhraseBank** (4,845 annotated sentences, Malo et al. 2014):

| | on tweets | on news |
|---|---|---|
| **VADER** | **68%** | 54% |
| Loughran-McDonald | 56% | — |
| **FinBERT** | 53% | **86%** |

A clean domain split — **VADER for laypeople's messages, FinBERT for professional
prose** — and they select accordingly. **No other paper in this folder validates its
sentiment tool before trusting it.** This also *explains* paper 43's odd result that
general HIV4 beat domain-specific LM: LM is built from 10-K filings and was being
applied to newspaper headlines — the same domain mismatch, in the other direction.

### One training sample

**One sample = one trading day for one stock**, and the input is
**sentiment only — NO price or technical features at all.**

| feature | construction |
|---|---|
| `CN` | natural-hours combined: `α·TN_modified + (1−α)·NN_modified`, **α = 0.25** |
| `CO` | opening-hours combined: `α·TO_modified + (1−α)·NO_modified`, **α = 0.25** |

Built bottom-up — **this chain is the deliverable**:

1. **per tweet** — `T_weighted = VADER compound × (retweets + 1)` (Eq. 3) — engagement weighting
2. **per headline** — `N_weighted = (pos − neg)/(pos + neg)` from **FinBERT** (Eq. 4) — signed and normalised
3. **per day** — summed over one of **two time divisions** (Eqs 5–6):
   - **TD1 "natural hours"** `00:00ₜ → 00:00ₜ₊₁`
   - **TD2 "opening hours"** `09:30ₜ → 09:30ₜ₊₁`
4. **holiday / weekend** — exponential decay into the next trading day (Eqs 7–8):
   `TO_modified = e⁻ⁿTOₜ₋ₙ + … + e⁻¹TOₜ₋₁ + TOₜ`

`y` — **two separate binary targets**:
- **Goal I** (Eq. 1): `1 if Openₜ ≤ Openₜ₊₁` — open-to-open
- **Goal II** (Eq. 2): `1 if Closeₜ ≤ Openₜ₊₁` — **the overnight gap**

### Results (accuracy)

| stock | Goal I best | Goal II best |
|---|---|---|
| AAPL | NB 0.604 (TD1) | NB 0.609 (TD2) |
| AMZN | NB **0.624** (TD1) | LR/NB 0.621 (TD2) |
| MSFT | NB 0.600 (TD1) | KNN 0.590 (TD2) |
| NFLX | LR 0.588 (TD1) | NB 0.594 (TD2) |

Naïve Bayes wins 6 of 8. Claimed pattern: natural hours (`CN`) better for Goal I,
opening hours (`CO`) better for Goal II.

### ⚠️ Five problems

1. **⚠️ 10-fold CV on a time series.** No chronological split anywhere — paper 9's
   central sin repeated. With ~250 rows per stock each fold tests on **~25 samples**,
   so every accuracy carries an SE near **10 pp**.
2. **One year, four stocks.** 2021 only; no out-of-period validation.
3. **⚠️ No majority-class baseline — and they come within one sentence of noticing.**
   The conclusion reads: *"the dataset might be highly skewed, with one class being
   much more prevalent than the others. NB is known to perform well in such
   scenarios."* They diagnose imbalance as **the reason their best model wins** and
   still never state the base rate. All four stocks rose through 2021, so "up" is the
   majority class and 0.60 may sit at or below the trivial rate.
4. **α = 0.25 chosen because "result is best"** — tuned on the reported data.
5. **No price features, so no ablation is possible.** Framed as a strength ("only
   sentiment features"), it means nothing shows sentiment beats price alone. The
   62.4% is then compared to Kabbani & Usta's 63.6% — different data, stocks, period.

Minor: the abstract claims opening-hours "outperformed" natural-hours, but their own
tables split it by goal (`CN` wins Goal I, `CO` wins Goal II). And Goal II — the
overnight gap — is not tradable the way the framing implies.

### How to use it in the thesis

**Cite — two techniques, both directly transferable:**

1. **⭐ Validate the sentiment scorer on labelled data before using it.** The most
   actionable item in the folder. For VN: benchmark any PhoBERT-class model on a
   labelled Vietnamese financial-text set *before* trusting it, and expect the
   prose-vs-laypeople split to reappear between forum posts (F319/F247-style) and
   CafeF / VietStock articles. Paper 45 supplies both the template and the reason.
2. **Treat the aggregation window as a design decision.** This is the fix for the
   alignment flaw in papers 9, 28, 43 and 44 — all of which sum by calendar day, which
   drops post-close news into the same row as that day's close. A **market-hours
   window** does not. VN analogue: a **09:00 → 09:00 ICT** division. Their holiday
   exponential decay matters *more* here than in the US — **Tết closes the VN market
   for up to ~9 days**, exactly the case the decay term exists for.

Also worth lifting: **engagement weighting** (`compound × (retweets+1)`), which maps
onto views/replies on VN forums; and the **signed normalised** news score
`(pos−neg)/(pos+neg)` — the correct version of what paper 9 got wrong by summing a
0–4 scale whose neutral is 2.

**Do not follow:** 10-fold CV on time series, one year, four stocks, no chronological
split, no baseline, no price features, no costs. Its ~0.60 sits high *because* the
evaluation is loose.

**The framing worth writing down:** paper 45 fixes exactly the two things paper 9 got
wrong — calendar-day summation and unvalidated tooling — while repeating paper 9's
central methodological sin. **Take its recipe, not its results.**

---

# Paper 46 — Wu, Liu, Zou, Weng (2021)

*S_I_LSTM: stock price prediction based on multiple data sources and sentiment analysis.*
**Connection Science** (Taylor & Francis) · DOI 10.1080/09540091.2021.1940101 ·
Hunan University, Changsha + Providence University, Taichung. 20 pp.
file: `46. Stock price prediction based on multiple data sources and sentiment analysis.pdf`.

> **→ Verdict: ⚠️ DISQUALIFIED as a method — the label is algebraically recoverable
> from three of its own input features, provable from the paper's own Table 2.** Cite
> it for exactly three things: the **CJK/non-English pipeline stage**, the
> **forum-dominant corpus composition** that matches a VN build, and the **metric
> contradiction** it shares with papers 43 and 44.

### Setup

| | |
|---|---|
| Universe | **5 Shanghai A-shares** — CITIC Securities (600030) + **four of the Big-4 banks**: BOC, ICBC, CCB, ABC ⚠️ essentially one factor |
| Period | 2017-07-01 → 2020-04-30, **3,377 stock-days** |
| Text | **2,351 news articles + 33,500 forum posts** from EastMoney.com — headlines only, per Vargas et al. |
| Split | chronological — train → 2019-12-31, **test 2020-01-01 → 2020-04-30 (~80 days)** ⚠️ the COVID crash |
| Task | **REGRESSION** — predict the closing price; MAE / MSE / RMSE |

### One training sample

**One sample = one (stock, day).** The only regression paper in the folder.

| # | feature | note |
|---|---|---|
| 1–4 | Open, High, Low, **Close** | Close is the **label**; Open/High/Low of the **same day** are inputs |
| 5 | Volume | |
| 6 | **Sentiment index** | Eq. 7 — `(M_tpos − M_tneg)/(M_tpos + M_tneg)` over that day's news + posts |
| 7–9 | **%K, %R, RSI** | TA-Lib, window 7 |

`y` = `Close_t`, the same-day closing price.

**Sentiment module:** Jieba word segmentation + HIT stopword list → Word2Vec
skip-gram → 3-branch 1-D CNN (128 filters each, global max-pool, concatenate, dense,
dropout) → positive/negative. CNN training labels come from `R_at = O_{a+t} − C_a`.
**Price module:** LSTM(32) over a 10-step window → attention layer → dense.

### ⚠️⚠️ The label is algebraically recoverable from the inputs

Not an inference — it follows from the paper's own Eq. 1 and Table 2.

`%K = 100·(C − L)/(H − L)` uses the **same day's** close. Rearranged:

```
C = L + (%K / 100) × (H − L)
```

`L`, `H` and `%K` are all input features. Checking their own Table 2, row 2019/12/31
— Open 25.59, High 25.72, Low 25.00, Close 25.30, %K 41.66:

```
25.00 + 0.4166 × (25.72 − 25.00) = 25.00 + 0.300 = 25.30    ✓ EXACT
```

`%R = 100·(H − C)/(H − L)` = `100 × 0.42/0.72` = **58.33** ✓ — also exact, also a
same-day function of the close.

**The model receives three numbers from which the target is computable in closed
form.** Whatever the reported errors measure, it is not out-of-sample forecasting.
And structurally it was never a forecast: `High` and `Low` for day *t* are only known
once day *t* has ended, by which point the close is known too. **Same-day
interpolation presented as prediction.**

### ⚠️ Four further problems

1. **MAE in price units across stocks spanning 3.4 → 25.7 CNY.** The headline
   `MAE = 2.386835` is ~9% of price for CITIC and ~68% for BOC — not comparable across
   the five. Table 6 then compares it to **other papers on other stocks in other
   periods** (GAN 3.041; S_EMDAM_LSTM 2.396). Currency-unit MAE cannot be compared
   across datasets at all.
2. **⚠️ The metric contradiction — third instance in this folder** (Table 5):

   | source | MAE | MSE | RMSE |
   |---|---|---|---|
   | transaction only | 2.469 | 7.461 | 2.732 |
   | **technical only** | 2.459 | **6.865** | **2.620** |
   | sentiment only | **2.507** ⚠️ worst | 7.705 | 2.776 |
   | **multi-source** | **2.387** | 7.272 | 2.697 |

   Multi-source wins on MAE and **loses on MSE and RMSE** to technicals-only. Squared
   error punishes large misses, so adding sentiment shaved typical errors while making
   the bad ones worse. Noted by the authors, never resolved. **Sentiment-only is the
   worst of the four single sources** — they say so plainly.
3. **The "sentiment index" is a distilled return forecast, not an opinion measure.**
   The CNN's training labels come from `R_at = O_{a+t} − C_a` — *future* price
   movement. Text is labelled by what the price subsequently did, the CNN learns to
   predict returns from text, and its output becomes a feature for predicting returns.
   Circular, and the label construction reaches forward in time.
4. **One regime, one factor, 400 test rows** — the COVID crash, 80 days, all five
   stocks falling together, four of them the same Big-4 bank exposure.

Minor: Eq. 7's stated range "between −0.5 and +0.5" is wrong; `(a−b)/(a+b)` spans
[−1, +1].

### How to use it in the thesis

**Cite — three narrow uses:**

1. **⭐ The non-English pipeline precedent.** The only paper here handling a language
   that requires **word segmentation** before anything else — Jieba plus a
   language-specific stopword list, then embeddings. **Vietnamese has the same
   requirement** (*"học sinh"* is one word written as two syllables), so the VN
   analogue is **underthesea / VnCoreNLP segmentation** ahead of any encoder. Papers
   9, 28, 43 and 45 all assume space-delimited English and skip the stage entirely.
   Cite 46 as the precedent that the pipeline gains a mandatory step.
2. **The corpus composition matches what a VN build would produce** — **33,500 forum
   posts to 2,351 news articles, 14:1**. That is the VN situation exactly, where
   F319/F247-style forum volume dwarfs formal financial journalism. Every other paper
   here is news-dominant or Twitter-only. The citation to use when arguing for a
   forum-weighted VN corpus.
3. **The metric contradiction**, alongside 43 (accuracy↑/F1↓) and 44
   (accuracy↑/Sharpe↓). Three independent papers, three different metric pairs, the
   same shape: **the headline metric improves while a second metric degrades, and
   none of them resolves it.** A strong evidential base for insisting on costed
   walk-forward on `rel5` over any single error number.

**Do not follow — this one is disqualifying, not merely weak.** The other five have
design flaws; this has a **provable leak**, demonstrable from its own worked example.
Nothing downstream of it can be trusted, and the framing was never a forecasting task.
Two further reasons it would not transfer even if repaired: the target is a **price
level** where experiment_3 settled on `rel5`, and the universe is five names of which
four are the same bank factor — the opposite of a cross-section.

**⚠️ Negative to carry forward:** sentiment-only is the *worst* single source here,
and adding sentiment to technicals made squared error worse. With paper 44's Sharpe
regression that is now **two independent papers where the text feature degrades the
more risk-sensitive metric**.

---

# Paper 47 — Koukaras, Nousi, Tjortjis (2022)

*Stock Market Prediction Using Microblogging Sentiment Analysis and Machine Learning.*
**Telecom** (MDPI) 3, 358–378 · DOI 10.3390/telecom3020019 · School of Science and
Technology, International Hellenic University, Thessaloniki. 21 pp, open access CC-BY.
file: `47. Stock Market Prediction Using Microblogging Sentiment Analysis and Machine Learning.pdf`.

> **→ Verdict: ⚠️ the paper refutes itself. Its own Table 10 and Figure 9 show
> validation ≈ 52% while the abstract advertises 76.3%.** Cite it as the folder's
> cautionary tale on headline-vs-honest numbers, and for the one base-rate check the
> literature actually permits. Take nothing methodological.

### Setup

| | |
|---|---|
| Universe | **Microsoft only** |
| Period | **16 Jul → 31 Oct 2020 — 77 trading days** |
| Text | **90,000 tweets** (Twitter API, `#Microsoft`/`#MSFT`) + **7,440 StockTwits** (`$MSFT`) |
| Sentiment | **TextBlob** and **VADER** — both general-purpose, **neither validated** ⚠️ |
| Design | 2×2 (Twitter/StockTwits × TextBlob/VADER) × 7 classifiers (KNN, SVM, LR, NB, DT, RF, MLP) |
| Split | 80/20; test = **10–31 Oct 2020 ≈ 15 samples** ⚠️ |
| Metrics | F-score and AUC only |

### One training sample

**One sample = one trading day of MSFT.**

`X` = `daily mean sentiment score, Low, High, Volume, Adj Close`
`y` = `StockChange = (Close − Open)/Open` (Eq. 17), binarised to ±1.

### ⚠️ Same-day leak — the paper-46 pattern repeated

The target is the **same day's** open-to-close return, while `High_t`, `Low_t`,
`Volume_t` and `Adj Close_t` are inputs. From their own Table 3, Adj Close is a fixed
multiple of Close across this window:

```
208.36 / 211.60 = 0.98468        205.55 / 208.75 = 0.98468     ✓ identical
```

`Close` is therefore exactly recoverable from `Adj Close`, giving the model
`(Close − Low)/(High − Low)` — the same %K quantity that leaked paper 46. This is not
forecasting; it is classifying a session that has already ended.

### ⚠️⚠️ Table 10 and Figure 9 contradict the abstract

| model | avg **training** score | avg **validation** score |
|---|---|---|
| **SVM** | 72.0% | **54.83%** |
| NB | 66.9% | 51.58% |
| LR | 63.03% | 53.13% |
| RF | 62.85% | 52.0% |
| DT | 60.93% | 52.3% |
| KNN | 59.13% | 51.5% |
| MLP | 66.4% | 51.5% |

Figure 9's learning curve agrees — validation sits at **0.40–0.55** for every model at
every training size while training runs 0.6–0.9. **The honest number is ~52%.** The
abstract advertises **76.3%**, which is one cell out of `4 datasets × 7 models ×
2 metrics = 56 reported numbers`, computed on **~15 test samples**.

**⭐ And here the base rate is finally computable.** Table 5 publishes the class
distribution — Twitter+VADER: **53 buy / 46 sell = 53.5% majority class**. Best
average validation across all seven models: **54.83%**. **The model performs at the
base rate.** No other paper in this folder supplies enough to run this check; 47 does,
and it fails.

### ⚠️ Three more

1. **9.6%–27.3% of the data deleted as "outliers"** (Table 4) — floored at the 10th
   and capped at the 90th percentile of *sentiment*, i.e. **precisely the days with
   the strongest sentiment signal are removed**.
2. **Missing sentiment replaced by the mean; missing prices linearly interpolated**
   across non-trading days, then predicted from.
3. **The target definition is written backwards** (§3.2.4): *"if the stock change is
   larger than zero, the stock movement is positive **and the stock price falls**"* —
   incoherent as printed.

### What it does honestly

Publishes the learning curve and average validation scores that undercut its own
headline; publishes the class distribution; and an explicit limitations section naming
sarcasm, bot/spam accounts and class imbalance. That candour is what makes it citable.

### How to use it in the thesis

**Cite — two narrow uses:**
1. **⭐ The self-refuting example.** A published paper whose own Table 10 and Figure 9
   show ~52% validation against a 76.3% abstract. A stronger illustration of
   headline-vs-honest-number divergence than any in-house assertion, and it is on the
   record in the paper itself.
2. **The base-rate check** — the only paper here publishing its class distribution,
   and the model lands on the majority-class rate.

**Do not follow.** Same-day leak, ~15 test samples, 56 reported numbers with no
multiple-comparison discipline, unvalidated general-purpose sentiment tools, 20% of
the sentiment distribution deleted, mean/linear imputation, no price-only baseline.

⚠️ Their **StockTwits-vs-Twitter** comparison (general Twitter beat finance-specialist
StockTwits) maps onto the VN question of general social vs F319/F247-style forums —
but at n≈15 it is noise and must **not** be cited as a finding.

---

# Paper 48 — Gu, Zhong, Li, Wei, Dong, Wang, Yan (2024)

*Predicting Stock Prices with FinBERT-LSTM: Integrating News Sentiment Analysis.*
**ICCBDC 2024** (ACM), Oxford, UK · DOI 10.1145/3694860.3694870 · Johns Hopkins
(Carey) / NYU Courant / SMU Cox / UC Berkeley / Northeastern. 6 pp, CC-BY.
file: `48. Predicting Stock Prices with FinBERT-LSTM Integrating NewsSentiment Analysis.pdf`.

> **→ Verdict: cite the 3-DIMENSIONAL SOFTMAX sentiment representation and the corpus
> scale. ⚠️ Do not follow the evaluation — MAPE 4.5% on next-day close is roughly
> 2–3× WORSE than the persistence baseline they never compute**, and their "Accuracy"
> is `1 − MAPE`, i.e. error wearing a different name.

### Setup

| | |
|---|---|
| Universe | **NASDAQ-100** |
| Text | **843,062 Benzinga articles**, 2009-02-15 → 2020-06-12 — **by far the largest corpus in this folder** |
| Prices | Yahoo Finance |
| Split | "hierarchical segmentation", 85% per stock to train+val, then 85/15 → **609,113 / 107,490 / 126,459** |
| Training | 3 stacked LSTM(50) → Dense(1), MSE, Adam, 100 epochs, Tesla P100 |

### One training sample

**One sample = one (stock, day)** with an **8-session lookback**:

| shape | contents |
|---|---|
| `8 × 4` | per timestep: **3 FinBERT softmax probabilities** (neutral / positive / negative) + **1 normalised closing price** |

`y` = **next day's closing price** (regression, MinMax-normalised to (0,1)).
**No volume, no OHLC, no technical indicators** — 4 features total.

Baselines: the same LSTM on price only (8 days), and a DNN (256/128/64, LeakyReLU
α=0.01, price only).

### ⭐ The one genuinely good idea — keep the whole distribution

They feed **all three FinBERT softmax probabilities as separate features** instead of
collapsing them into a single polarity scalar. That preserves the **neutral mass**,
and a day that is 90% neutral is a fundamentally different information state from a
day with balanced positive/negative — which `(pos−neg)/(pos+neg)` **cannot
distinguish**. Papers 9, 45 and 46 all discard it. Cost: two extra columns.

### ⚠️⚠️ The model is worse than doing nothing, and nobody checks

| approach | test loss | MAE | MAPE | "Accuracy" |
|---|---|---|---|---|
| **FinBERT-LSTM** | 0.00083 | 173.67 | **0.045** | 0.955 |
| LSTM | 0.00092 | 183.36 | 0.072 | 0.928 |
| DNN | 21.77 | 489.32 | 0.22 | 0.78 |

**MAPE 4.5% on a next-day close.** A NASDAQ-100 stock's mean absolute *daily* return
over 2009–2020 runs roughly 1.5–2%. So the persistence baseline `Ĉ_{t+1} = C_t` —
predicting no change at all — sits around **2–3× better than the proposed model**.
It is never computed. For price-level regression, persistence is the mandatory
benchmark, and without it the whole table is uninterpretable.

**"Accuracy = 1 − MAPE" (Eq. 3) is not accuracy.** It is error rebranded into a number
that invites comparison against classification accuracy. 0.955 means MAPE 4.5%.

### ⚠️ Three more

1. **Pooled dollar MAE across a 100-stock universe.** MAE 173.67 with MAPE 0.045
   implies an average price near **$3,860** — no such NASDAQ-100 average exists, so the
   figure is dominated by a few high-priced names. Same defect as paper 46.
2. **MAE and MAPE move by very different factors** — LSTM → FinBERT-LSTM improves MAE
   5.3% but MAPE 37%. A stable error distribution would move both together; the gap
   means the gain concentrates in low-priced stocks. **Fourth instance of the folder's
   metric-divergence pattern.**
3. **The DNN baseline never converged** — test loss 21.77 vs 0.00083 on the same
   normalised (0,1) target, a 26,000× gap. A broken third place flatters the ranking.

Also: the split is "hierarchical segmentation… to ensure equitable representation",
**never stated as chronological** — a reproducibility gap on the most important design
choice; and sentiment is aggregated by **calendar date**, the same alignment flaw as
9, 28, 43, 44 and 46.

### How to use it in the thesis

**Cite — two uses:**
1. **⭐ The 3-D softmax sentiment representation.** Keep `P(neutral), P(positive),
   P(negative)` as three features rather than one polarity scalar. Cheap, directly
   transferable to a VN transformer scorer, and strictly more informative than the
   scalar in papers 9/45/46. **Add to the assembled design.**
2. **The scale reference point.** 843k articles over 11 years is what "enough news"
   looks like — set against paper 28's ~10 titles/day/target requirement and
   experiment_6's **0.35/day**. Useful for sizing the corpus argument.

**Do not follow:** price-level regression scored by MAPE with no persistence baseline,
"accuracy" that is rebranded error, pooled dollar MAE, an unconverged baseline, four
input features, and an unstated split policy.

---

# Paper 49 — Maqbool, Aggarwal, Kaur, Mittal, Ganaie (2023)

*Stock Prediction by Integrating Sentiment Scores of Financial News and MLP-Regressor:
A Machine Learning Approach.*
**Procedia Computer Science** 218 (2023) 1067–1078 · ICMLDE · Elsevier ·
DOI 10.1016/j.procs.2023.01.086 · UIET, Panjab University, Chandigarh. 12 pp,
open access CC BY-NC-ND.
file: `49. Stock Prediction by Integrating Sentiment Scores of Financial News and MLP-Regressor A Machine Learning Approach.pdf`.

> **→ Verdict: ⭐ cite the 10/30/100-day accuracy DECAY TABLE — the single cleanest
> demonstration in this folder that small test sets manufacture skill.** Same model,
> same data, same features; accuracy falls **0.90 → 0.70 → 0.56** as the test set
> grows. Take nothing else.

### Setup

| | |
|---|---|
| Universe | **4 Indian stocks, 4 sectors** — Reliance, Tata Motors, Tata Steel, HDFC |
| Period | **Jan 2010 → Jan 2020 — ten years** (Reliance to May 2020) |
| Text | Indian financial news; headline + short description **concatenated per date** |
| Sentiment | **VADER** (pos/neg/neu/compound), **TextBlob** (subjectivity/polarity), **FLAIR** (score_sum) — all general-purpose, **none validated** ⚠️; all 8 combinations tested |
| Model | **MLP-Regressor** (scikit-learn), Google Colab defaults |
| Metrics | MAPE for price; accuracy/precision/recall/F1 for derived trend |

### One training sample

**One sample = one (stock, day).** Per Table 4: `Close, label, subjectivity, polarity,
compound, negative, neutral, positive, score_sum` — previous close plus up to 9
sentiment scores plus a **company-relevance flag** (`label` = 1 if the news mentions
that company's keywords). `y` = **closing price**; `trend` (Eq. 1) and `future trend`
(Eq. 2, n = 5) are derived from the prediction and scored.

⚠️ **An unresolved contradiction that blocks reproduction.** §4.3 states *"The other
two features include Trend and Future Trend"* — but `future trend` is defined from
**`C_{d+n}` with n = 5**, five days ahead. As an input that is a five-day look-ahead
leak. **Table 4's dataset view contains no such columns.** The paper never reconciles
the two, so it cannot be reimplemented under either reading.

### ⭐ The accidental experiment — the folder's best result

Every configuration is reported at **three test horizons: 10, 30 and 100 days.** Same
model, same data, same features — **only the test-set size changes** (HDFC, Table 7):

| features | 10 days | 30 days | **100 days** |
|---|---|---|---|
| FLAIR | **0.90** | 0.70 | **0.56** |
| VADER + TextBlob | **0.90** | 0.70 | **0.62** |
| FLAIR + TextBlob | 0.90 | 0.73 | 0.62 |
| VADER | 0.80 | 0.66 | **0.53** |
| TextBlob | 0.70 | 0.63 | **0.56** |
| VADER+TextBlob+FLAIR | 0.60 | 0.66 | **0.52** |

**Accuracy decays monotonically toward chance as the test set grows.** The abstract
headlines **0.90** — which is 9 correct out of **10 samples**. At 100 days everything
lands in **0.52–0.62**.

This is **stronger evidence than paper 47's train-vs-validation gap**, because
test-set size is the *only* variable that moves. Every other paper in this folder
reports a single small-n number; 49 accidentally shows what happens when you don't.

### ⚠️ Three more

1. **Recall = 1.00 in many cells** — VADER, TextBlob and FLAIR all reach recall 1.0 on
   future-trend at 10 days. Table 5 clubs `0/1` together as "upward or static", so
   recall 1.0 with accuracy 0.8 means **the model predicts "up" for nearly everything**
   — and Indian equities 2010–2020 were a bull market. The majority-class pathology,
   visible directly in the recall column. **Ninth paper in a row with no
   majority-class baseline.**
2. **MAPE 1.48–2.32 — at or worse than persistence, again.** Indian large caps move
   ~1.5% on an average day, so `Ĉ_{t+1} = C_t` scores ≈1.5% MAPE. Best here is FLAIR
   at **1.48**; VADER 1.77; TextBlob 2.32. Per company: HDFC 1.61, Tata Steel 2.55,
   Reliance 2.57, **Tata Motors 4.71**. Same omission as paper 48.
3. **The news is not company-specific.** *"All financial news headlines were added
   without segregating news of different sectors or companies."* Every stock receives
   the same market-wide text — **paper 43's market stream alone**, with no sector or
   stock stream, which is very likely why the signal is so weak. Notably, adding the
   company-relevance `label` **reduces MAPE by 0.17** — a small independent echo of
   43's hierarchy result.

Minor: Table 2 lists Reliance as "Telecom"; it is an energy/petrochemical conglomerate.

### How to use it in the thesis

**Cite — one strong use plus one weak one:**
1. **⭐ The 10/30/100-day decay table.** The folder's best evidence for test-set-size
   effects, produced by the authors' own systematic sweep rather than by critique.
   Use it verbatim in the evaluation chapter: *published headline 0.90 on 10 samples,
   0.56 on 100, same model throughout.* Combined with **47**'s 52% validation and
   **48**'s sub-persistence MAPE, the case for costed walk-forward over headline
   metrics is now made **entirely from the literature's own numbers**.
2. The `label` relevance-flag result weakly corroborates paper 43's hierarchy —
   filtering market-wide news down to company-relevant news measurably helps.

**Do not follow:** MLP on price-level regression, general-purpose unvalidated scorers,
market-wide news applied undifferentiated across four stocks, no baseline, and the
§4.3-vs-Table 4 contradiction over a five-day-forward feature.

---

# Paper 50 — Khedr, Salama, Yaseen (2017)

*Predicting Stock Market Behavior using Data Mining Technique and News Sentiment Analysis.*
**I.J. Intelligent Systems and Applications** 2017, 7, 22–30 · MECS Press ·
DOI 10.5815/ijisa.2017.07.03 · Future University in Egypt / Helwan University /
King Abdulaziz University. 9 pp.
file: `50. Predicting Stock Market Behavior using Data Mining Technique and News Sentiment Analysis.pdf`.

> **→ Verdict: ⚠️ the feature IS the label — the most blatant leak in the folder,
> stated in the paper's own consecutive sentences.** Cite it for exactly one number:
> **Kappa = 0.078 for sentiment alone**, the base-rate-corrected statistic every other
> paper omits, which says the sentiment signal has no skill at all.

### Setup

| | |
|---|---|
| Universe | **3 NASDAQ companies** — §III.A says Yahoo, **Microsoft**, Facebook; §IV says Yahoo, **Google**, Facebook ⚠️ contradictory |
| Text | ~**3 news articles/day** from nasdaq.com, Reuters, WSJ, MarketWatch, Zacks, Yahoo/Google Finance |
| Numeric | OHLC |
| Period | **⚠️ NEVER STATED** — no date range, no sample counts, no split ratio |

### One training sample

**One sample = one (stock, day).**
`X` = `news sentiment (pos/neg)` + **discretized Open, High, Low, Close**
`y` = `raise` / `fall`

**Sentiment stage:** tokenize → lowercase standardisation → stopwords → **Porter
stemming** → abbreviation expansion → filter ≤2-char tokens → **bigrams → TF-IDF →
Naïve Bayes**. **Prediction stage:** join by date → **KNN** → raise/fall.

### ⚠️⚠️ The feature is the label

§III.B.2, verbatim:

> *"all four attributes values will be labelled with 'positive', 'negative' and
> 'equal' based on the comparative with the previous day closing price. The second
> step, computes a label for each data instance, the trend 'raise' or 'fall' is
> calculated based on the difference between the closing price of the current day and
> the previous day."*

The discretized **Close** attribute is `positive ⟺ Close_d > Close_{d−1}`.
The label is `raise ⟺ Close_d > Close_{d−1}`. **Identical comparison — one input
column is the target, copied.** Paper 46 required algebra to expose; this is stated
outright in two consecutive sentences.

Table 3 shows precisely what that buys:

| features | accuracy | **Kappa** |
|---|---|---|
| sentiment only | 59.18% | **0.078** |
| sentiment **+ numeric** | **89.80%** | 0.79 |

The 30-point jump is the leaked column being added.

### ⭐ Kappa 0.078 — the number the whole folder has been missing

Cohen's kappa is exactly the **base-rate-corrected** statistic. **0.078 means no
agreement beyond chance.** The 59.18% "accuracy" for sentiment alone is the majority
class and nothing else.

Nine papers had failed to report any baseline. **50 accidentally publishes the
corrected statistic anyway — and it reads ~0.** Paper 47 let the base rate be
*computed*; paper 50 *prints* the base-rate-corrected result outright.

### Also worth noting

Their conclusion describes **three news categories — market news, company news, and
financial reports published by financial experts** — which is paper 43's
market/sector/stock hierarchy **six years earlier**. Useful for showing the hierarchy
idea has independent lineage rather than originating with 43.

### How to use it in the thesis

**Cite — two uses:**
1. **⭐ Kappa = 0.078.** The folder's only *published* base-rate-corrected measure of a
   sentiment feature's standalone worth, and it is essentially zero. Pairs directly
   with **47**'s computable base rate and **49**'s decay table.
2. **The clearest teachable leak.** Feature and label are the same comparison, in the
   authors' own words — far easier to present than 46's algebraic recovery.

**Do not follow:** no date range, no sample counts, no split description, a
self-contradictory universe, 2017-era NB/TF-IDF sentiment whose ground-truth polarity
labels are never explained, and the target copied into the feature matrix.

---

# Paper 51 — Fazlija, Harder (2022) ⭐⭐ THE METHODOLOGICAL TEMPLATE

*Using Financial News Sentiment for Stock Price Direction Prediction.*
**Mathematics** (MDPI) 2022, 10, 2156 · DOI 10.3390/math10132156 · School of Management
and Law, ZHAW Zurich University of Applied Sciences. 20 pp, open access CC-BY.
file: `51. Using Financial News Sentiment for Stock Price Direction Prediction.pdf`.

> **→ Verdict: ⭐⭐ the only paper in this folder whose EVALUATION PROTOCOL should be
> followed.** Its target (index direction) is still wrong for this thesis, but it is
> the first study here with a **naive benchmark**, **base-rate-corrected metrics**,
> **explicit leak prevention**, and **walk-forward out-of-sample testing**. It also
> settles a question the rest of the folder gets wrong: **content beats titles.**
> ⚠️ And note what the honest protocol yields: **MCC 0.069, before transaction costs.**

### Setup

| | |
|---|---|
| Target | **S&P 500 total-return index**, next-day direction, binary (zero-returns oversampled with the previous day's value) |
| Text | **Bloomberg** 447,279 articles (2007-01-03 → 2013-11-26) + **Reuters** 8,556,310 items (2007-01-01 → 2016-08-16); Reuters ships no content, so **345,776 articles were scraped from their URLs** |
| Normalisation | capped at **exactly 58 items/day** (the smallest daily count), randomly sampled ⚠️ discards information |
| Scorer | **FinBERT** (BERT_BASE uncased, 12 encoders) **fine-tuned on the Financial PhraseBank** — 4,846 sentences at >50% annotator agreement; max_seq_len 48, lr 2e-5, 4 epochs, batch 32 |
| Model | Random forest (GridSearchCV, k=8, in-sample only) + a no-ML "simple strategy" |

### One training sample

**One sample = one trading day.**
`X` = **58 sentiment scores** for that day, each `P(positive) − P(negative)`, in three
variants — **title**, **content**, or **title+content** — optionally plus 1-day-lagged
moving averages of the index.
`y` = **next day's** index direction.

For multi-sentence content, the sentence-level scores are averaged into one document
score.

### ⭐ Six things no other paper in this folder does

1. **⭐ A RANDOM-WALK BENCHMARK** (Eq. 8), simulated **1,000 times** and averaged.
   **The first naive baseline in eleven papers.**
2. **⭐ MCC and Binary Brier Score** reported alongside accuracy — base-rate-corrected
   and calibration measures, not just the headline metric.
3. **⭐ Explicit same-day leak prevention**, stated outright: *"This also prevents
   news, respectively sentiment scores, from being used for the prediction of the
   return direction of the same day."* Friday/Saturday/Sunday sentiment is averaged
   into Monday; missing trading days are forward-filled.
4. **⭐ Walk-forward EXPANDING-WINDOW out-of-sample, 6 folds** (Table 4): fit
   2007–2010 → test 2011; fit 2007–2011 → test 2012; … → test 2016.
5. **⭐ The sentiment model is validated before use** — FinBERT scores **accuracy
   0.836 / weighted F1 0.837** on held-out PhraseBank data (Table 6, Fig. 7).
6. **⭐ k-fold CV is confined to hyperparameter tuning inside the in-sample block**,
   with the time-series objection acknowledged in the text and the final test kept
   genuinely out-of-sample.

### Results — Table 11

| | Accuracy | Brier | **MCC** |
|---|---|---|---|
| **RFC Content** | **0.561** | **0.439** | **0.069** |
| RFC Title & Content | 0.554 | 0.446 | 0.039 |
| RFC Title, Content & MA | 0.552 | 0.448 | 0.045 |
| RFC Title | 0.535 | 0.465 | **−0.023** |
| Content Sentiment (no ML) | 0.518 | 0.482 | 0.026 |
| Title Sentiment (no ML) | 0.206 | 0.794 | 0.007 |
| **Random Walk Model** | 0.499 | 0.501 | −0.011 |

**Best MCC = 0.069.** MCC is the prediction-vs-truth correlation corrected for base
rate; 0 is random. The authors say so themselves: *"one would not expect overly high
accuracy or MCC."* Beating the random walk by ~6 pp of accuracy is real but marginal.

Trading (Tables 7–10, OOS 2011-01-01 → 2016-08-16):

| | Return p.a. | Return/Risk | **Max Drawdown** |
|---|---|---|---|
| S&P 500 TR | 12.045% | 0.797 | **18.641%** |
| RFC Content SC | **16.261%** | **1.078** | **28.187%** ⚠️ |
| Content SC (no ML) | 15.208% | 1.008 | 20.598% |
| RFC Title SC | 7.673% | 0.508 | 25.518% |
| Title SC (no ML) | −0.934% | −0.062 | 32.089% |

### ⭐ The substantive finding: CONTENT BEATS TITLES, decisively

Content wins on every metric — MCC **0.069 vs −0.023**, return **16.26% vs 7.67%**.
Title-only sentiment is worse than useless: 0.206 accuracy means it predicts "down"
almost always while the index rises, and the no-ML title strategy **loses money**.

**This directly contradicts papers 28 and 46**, both of which use headlines only on
the authority of Ding et al. **→ For experiment_6, which scraped headline AND content
for 1,629 VCB items, this says: use the `content` column, not the headline.**

### ⚠️ Where it still falls short

1. **⚠️ No transaction costs** — their own first-listed limitation. The strategy
   rebalances **daily** and takes **100% short** positions on negative signals. At
   realistic costs the ~4 pp excess over the index very likely vanishes.
2. **⚠️ They trace the entire outperformance to ONE episode.** Quoted: *"an
   outperformance is generated at the end of 2011, which explains the outperformance
   of this strategy since, in the following years, all price downturns are taken along
   and are not correctly reflected by the content sentiment scores."* After 2011 the
   strategy is simply long the market, drawdowns included.
3. **Max drawdown is worse than the index** — 28.19% vs 18.64% for the best model.
   Higher return/risk bought with a much deeper hole.
4. **Recall-optimised RF models were excluded from the paper** because they degenerated
   into predicting "up" for everything. Honest — and it shows how close the exercise
   sits to trivial.
5. Index-level, not cross-sectional. The OOS window 2011–2016 is a strong bull market.
6. The **58-items/day cap with random sampling** throws away most of the corpus and
   injects sampling noise.

### How to use it in the thesis

**⭐⭐ This is the answer to "can I follow any of them?" — follow the PROTOCOL, not
the method.** Adopt, wholesale:

- **MCC + Brier alongside accuracy.** MCC would have exposed 47, 49 and 50 instantly.
- **A naive benchmark, always** — random walk for direction, persistence for price
  level, majority class for classification.
- **An explicit written statement of how same-day leakage is prevented.**
- **Walk-forward expanding window**, with hyperparameter tuning confined to the
  in-sample block.
- **Validate the sentiment scorer before using it** (with **45**).
- **Report max drawdown, not just return.**

Add the one thing 51 omits and experiment_3 already enforces: **transaction costs.**

**Also cite:** the **content-over-titles** result, which overturns the headline-only
convention of 28 and 46 and tells experiment_6 which field to use.

**⚠️ And cite what the honest protocol produced: MCC 0.069, before costs, on 8.5
million news articles.** The best-designed study in the folder finds a real but
marginal effect that its own authors attribute to a single month in 2011. **That is
the most credible estimate available of the ceiling on this whole line of work.**

---

# Paper 52 — Nemes, Kiss (2021)

*Prediction of stock values changes using sentiment analysis of stock news headlines.*
**Journal of Information and Telecommunication** 5:3, 375–394 · Taylor & Francis ·
DOI 10.1080/24751839.2021.1874252 · Dept. of Information Systems, ELTE Eötvös Loránd
University, Budapest / J. Selye University, Komárno. 20 pp, open access CC-BY.
**Widely read — 22,562 views, 44 citing articles.**
file: `52. Prediction of stock values changes using sentiment analysis of stock news headlines.pdf`.

> **→ Verdict: ⚠️ despite the title, this paper contains NO PREDICTION MODEL.** No
> target variable, no train/test split for forecasting, no error metric, **no lag** —
> it computes **same-day correlation matrices** over ~13 trading days. Cite it for one
> inverted lesson: **with a small sample and an unvalidated scorer, the choice of
> sentiment tool determines the conclusion.**

### Setup

| | |
|---|---|
| Universe | **AMD, AMZN, FB, GOOG** |
| Period | **2020-10-27 → 2020-11-14 — about THREE WEEKS, ~13 trading days** ⚠️ |
| Text | headlines scraped from **finviz.com** (scraper code published, Listing 1) |
| Prices | Yahoo Finance |
| Scorers | **TextBlob**, **NLTK-VADER**, a **custom RNN**, and **BERT** as "benchmark" |

### There is no training sample

Nothing supervised is constructed. The analysis is
`corr(Compound, {open, high, low, adjclose, volume})` **on the same day** —
descriptive of the period, not predictive of anything.

### ⭐ The one useful result — and not the one they claim

Four tools, **identical headlines over identical days**, produce correlation matrices
that **disagree in SIGN** (Figs 14–16):

| pair | TextBlob | VADER | **RNN** |
|---|---|---|---|
| Compound ↔ adjclose | +0.74 | +0.68 | **−0.87** |
| Compound ↔ high | +0.96 | +0.89 | **−0.35** |
| Compound ↔ volume | −0.77 | −0.63 | **+0.92** |

The authors read this as a finding about model quality — *"the correlation matrix of
RNN is completely different and surprising"* — and treat the volume correlation as a
discovery.

It is neither. **A correlation over ~13 observations carries a standard error near
0.3**, so no value here is distinguishable from zero and sign flips are exactly what
noise produces at that sample size. But the demonstration is citable **inverted**:
*when the sample is small and the scorer unvalidated, the tool chosen decides the
answer.* The strongest available argument for paper 45's validate-first discipline —
skip validation and any conclusion is available on request.

### ⚠️ Four more

1. **The RNN is trained on the IMDB movie-review dataset** and applied to financial
   headlines. Paper 45 measured general-domain scorers at ~54% on financial prose
   against FinBERT's 86%; movie reviews sit further away still.
2. **"BERT" is plain BERT, not FinBERT**, with no stated sentiment fine-tuning — and it
   outputs **50.5% positive / 49.5% negative**, a coin flip. Using it as the *benchmark*
   the other three are judged against compounds the error.
3. **⚠️ Minimising the neutral class is treated as a GOAL** — *"A key factor is to
   minimize neutral values"* — and the RNN is praised for eliminating it (0% neutral),
   even though TextBlob reads **75.25%** of these headlines as genuinely neutral.
   Forcing those into positive/negative adds noise, not information. **Directly
   contradicts paper 48**, whose one good idea is that the neutral mass is informative.
4. **~13 trading days**, four stocks, one three-week window in late 2020.

**Small credit:** the scraper is published (`finviz.com/quote.ashx?t=<ticker>`,
BeautifulSoup, `id='news-table'`), and the authors are candid in places — calling
their own result *"thought-provoking"* and its detail *"questionable"*.

### How to use it in the thesis

**Cite — two negative lessons:**
1. **⭐ Tool choice determines the conclusion** at small n with an unvalidated scorer:
   four tools, one dataset, sign-flipped correlations. Pair with **45** (validate
   first) and **49** (small test sets manufacture skill) — three different mechanisms,
   one message.
2. **The neutral-minimisation anti-pattern**, against **48**. An explicit design
   decision to reject in the assembled pipeline.

**Do not follow:** no prediction model, no target, no lag, no split, ~13 days, a
movie-review-trained RNN, and an unfine-tuned BERT serving as ground truth.

---

# Paper 53 — Li, Wu, Wang (2020)

*Incorporating stock prices and news sentiments for stock market prediction: A case of
Hong Kong.*
**Information Processing & Management** (Elsevier) · DOI 10.1016/j.ipm.2020.102212 ·
College of Computer and Information, Hohai University, Nanjing. 19 pp.
file: `53. Incorporating stock prices and news sentiments for stock market prediction - A case of Hong Kong.pdf`.

> **→ Verdict: a well-executed study whose own labelling scheme lets you compute the
> baseline — and ⚠️ NOTHING IN IT CLEARS THAT BASELINE.** Labels are set by 25%/75%
> return quantiles, so "always predict horizontal" scores ~50%; the best sector-average
> accuracy in the paper is 0.496. Cite it for the **quantile-label design**, the
> **LMFinance-beats-general-dictionaries** result, and **per-window Z-scoring**.

### Setup

| | |
|---|---|
| Market | **Hong Kong Exchange (HKEx)**, FINET news |
| Period | **Jan 2003 → Mar 2008** |
| Universe | **12 stocks, 3 per sector** — Properties (0001/0012/0016), Utilities (0002/0003/0006), Finance (**0005 HSBC**/0011/2388), Commerce (0013/0762/0941) |
| News density | **0.53 → 7.06** articles/stock/day (Table 2) |
| Dictionaries | **SenticNet 5**, **SentiWordNet 3.0**, **Vader**, **Loughran–McDonald Financial 2018 (LMFinance)** |
| Model | 2-layer LSTM + dropout → dense softmax (3 classes); RMSProp, batch 32, early stop |
| Baselines | SVM and **EasyMKL** — *both ML models; no naive benchmark* ⚠️ |
| Split | **chronological**: train Mar 2003–Mar 2007 (80%) · early-stop Mar–Sep 2007 (10%) · **test Sep 2007–Mar 2008** (10%) |

### One training sample

**One sample = one rolling window of τ trading days for one stock**, step 1 day. Each
day contributes `Q_i = [P_i, T_i, S_i]`:

| block | contents |
|---|---|
| `P` | Close, Volume |
| `T` | **10 technical indicators** — MA10/20/30, MACD (DIFF), MACD (DEA), MACD, RSI6/12/24, MFI — **identical to paper 43's set** |
| `S` | daily news sentiment vector: per-word dictionary vectors summed per article (Eq. 6), then **averaged across that day's articles** |

`y` = **3 classes from the close-to-close return of the day AFTER the window** (Eq. 21):

```
fall (0)        r < p25
horizontal (1)  p25 ≤ r ≤ p75     ← 50% of the sample BY CONSTRUCTION
rise (2)        r > p75
```

The label comes from the period *after* the window closes — **clean, no same-day leak**.

### ⭐ Four things it does right

1. **Chronological split, stated and justified**: *"we neither shuffle the input samples
   nor use a bidirectional LSTM layer… because stock data are strictly time-ordered,
   which means that we cannot use future data to predict stock histories."*
2. **Time-series split cross-validation** (Fig. 4) — 3-fold expanding, validation always
   after training. Same discipline as paper 43.
3. **⭐ Per-window Z-score normalisation** (Eq. 7) — mean and std computed *within each
   rolling window*, so no global-statistics leak. Cheap and worth adopting.
4. **⭐ Quantile-defined labels**, which make the base rate explicit instead of hidden.

### ⚠️⚠️ And point 4 sinks the paper

**25/50/25 by construction means "always predict horizontal" scores ~50%** — not 33%.
Test-set LSTM accuracy, averaged over the 12 stocks (Tables 6–9):

| dictionary | mean LSTM test accuracy |
|---|---|
| **LMFinance** | **0.487** |
| SenticNet | 0.365 |
| Vader | 0.362 |
| SentiWordNet | 0.354 |

Sector averages (Table 10) peak at **0.496** (LSTM) and 0.502 (MKL). The single best
cell anywhere in the paper is 0.617.

**Nothing beats the trivial classifier.** The crash skews the test window's class mix
somewhat, but nowhere near enough to lower the bar to the 33% random rate — and the
comparison is only ever against SVM and MKL, never against predicting the middle class.

### ⚠️ Two more

1. **The "up to 120% improvement from sentiment" is measured against a broken
   baseline.** Table 11 / Fig. 7: for 0016.HK the price-only model scores **~0.25 on a
   three-class problem** — *below random*. Improving a model that loses to coin-flipping
   is not evidence that sentiment helps.
2. **⚠️ The test window is the onset of the GFC** — Sep 2007 → Mar 2008, HSI ~31,000 →
   ~21,000. The model trains on a four-year bull market and is tested on a crash. That
   explains the collapse from ~0.82 validation (Table 5) to ~0.49 test, and it means
   the numbers largely measure regime-shift failure. **The paper never mentions it.**

### ⭐ The one solid, transferable finding

**The domain-specific dictionary wins, decisively and systematically.** LMFinance beats
SenticNet, SentiWordNet and Vader on essentially every stock, model and metric
(Figs. 5–6), with Δ_news gains "over 80%" against 10–40% for the general dictionaries.

**This settles the 43-vs-45 disagreement.** Paper 43 found general HIV4 beating
domain-specific LM; paper 45 found FinBERT (86%) crushing VADER (54%) on financial
prose. Paper 53 is the most systematic test of the three — **12 stocks × 3 models × 4
dictionaries** — and it lands with 45. **Two against one: use a finance-domain scorer.**
For VN that means a finance-tuned model, not off-the-shelf general sentiment.

### How to use it in the thesis

**Cite — three uses:**
1. **⭐⭐ The quantile-labelled 3-class design.** Classes set by the 25th/75th percentiles
   give a **known base rate** and clean extremes. It maps directly onto the thesis
   target: **rank by `rel5` per date → top quartile long, bottom quartile short/avoid,
   middle 50% ignored** — which is what experiment_3's top-6/bottom-6 construction
   already does. A citable precedent for that design.
2. **⭐ LMFinance > general dictionaries**, settling domain-specificity (with **45**,
   against **43**).
3. **⭐ Per-window Z-score normalisation** as leak-free feature scaling.

**Do not follow:** every reported accuracy sits at or below the ~50% baseline the
paper's own labelling guarantees; the headline improvement is measured against a
sub-random price-only model; and the test window is an unacknowledged regime break.

---

# Paper 54 — Joshi, Bharathi H. N., Rao (2016)

*Stock Trend Prediction Using News Sentiment Analysis.*
Dept. of Computer Engineering, **K. J. Somaiya College of Engineering, Mumbai**.
11 pp. Student paper / preprint; no journal identified.
file: `54. Stock trend prediction using news sentiment analysis.pdf`.

> **→ Verdict: ⚠️ the paper contains NO STOCK PREDICTION.** Every reported number
> measures how accurately a classifier reproduces a **lexicon's own output on the same
> documents that produced it**. Cite it for exactly one thing — as the folder's
> cleanest example of **circular labelling**, which is the precise trap awaiting any
> VN sentiment corpus built without human annotation.

### Setup

| | |
|---|---|
| Universe | **Apple Inc (AAPL) only** |
| Period | **1 Feb 2013 → 2 Apr 2016** |
| Text | "major key events news articles" from news.google.com, reuters.com, finance.yahoo.com — **92 documents total** ⚠️ |
| Sentiment | bag-of-words dictionary — general polarity words **+ Loughran–McDonald finance words**; **2,360 positive / 7,383 negative**; own finance-specific stopword list |
| Score | `count(pos matches) − count(neg matches)`; **≥ 0 → positive**, else negative |
| Models | RF, NB, SVM over **TF-IDF**, via Weka |

### ⚠️⚠️ There is no prediction of stock movement

From their own Fig. 1:

```
Phase 1:  news text → dictionary counting rule → (News, Polarity score)
Phase 2:  TF-IDF of the SAME news text → RF / NB / SVM → predict that polarity label
Phase 3:  plot sentiment, plot price, "observe the relationship"
```

**All results come from Phase 2.** Prices appear nowhere in the evaluation. Phase 3 is
Fig. 4 — two stacked time series, **no correlation, no lag, no metric**. The abstract
confirms the task: *"in comparison with news random labelling with 50% of accuracy"* —
a random baseline for **polarity classification**, not for price direction.

### The circularity IS the result

```
label    = f(text)   where f = count(pos) − count(neg), thresholded at 0
features = TF-IDF(text)
```

The classifier's only job is to **rediscover `f`** from a bag-of-words view of the same
text. That is close to a solved problem, which is exactly why accuracies land at
80–96%. **A self-labelling loop — the number says nothing about markets.**

*(Distinct from paper 46's circularity, where labels came from future returns. Here
label and features are two functions of the same document.)*

### ⚠️ The sample sizes

| test option | actual counts |
|---|---|
| k-fold CV | **92 documents total** — RF 80/92, NB 76/92, SVM 75/92 |
| 70% split | **28 test samples** — SVM 27/28 = "96.42%" |
| 80% split | **18 test samples** — SVM 17/18 = "94.44%" |
| "new data" | **20 articles** — SVM 18/20 = the headline **"90%"** |

**92 articles for AAPL over three years** — ~30/year for one of the most heavily
covered stocks in the world. Set against paper 48's **843,062**.

Minor: with 2,360 positive vs 7,383 negative dictionary entries and ties assigned to
positive (*"if the score of the document is 0, then we label it as positive"*), the
labelling carries an unexamined bias.

### ⭐ The one thing worth taking — as a warning

**This is the cleanest circular-labelling example in the folder, and it is precisely
the temptation awaiting a VN corpus build.**

There is no Vietnamese Financial PhraseBank. The cheap path is to auto-label VN
financial text with a lexicon and train a model on it. **54 shows what that yields**:
a 90% headline measuring only how well a model re-derives the rule that made its
labels.

**→ Labels must come from HUMAN ANNOTATION** — as in **45** and **51**, both of which
use the human-annotated Financial PhraseBank (16 annotators, 4,846 sentences) — **or
from an independent source. Never from a lexicon applied to the same text being
trained on.**

### How to use it in the thesis

**Cite — one use:** the circular-labelling anti-pattern, paired with **46**'s
label-from-future-returns circularity. Two distinct ways to build a sentiment model
that measures itself.

**Do not follow:** no target variable, no price alignment, no lag, no trading, 92
documents, a 20-article "test set", and a title describing work the paper does not
contain. **The weakest entry in this folder.**

---

# Paper 55 — Bharathi, Geetha (2017)

*Sentiment Analysis for Effective Stock Market Prediction.*
**International Journal of Intelligent Engineering & Systems** (INASS) Vol. 10, No. 3,
pp. 146–154 · DOI 10.22266/ijies2017.0630.16 · Dept. of Computer Science and
Engineering, B.S. Abdur Rahman University, Chennai. 9 pp.
file: `55. Sentiment Analysis for Effective Stock Market Prediction.pdf`.

> **→ Verdict: ⭐ cite it for ONE idea the authors do not appear to realise they had —
> sentiment used as a VETO that only withdraws a signal, never generates one.**
> Independently corroborated by paper 51's divest-don't-short result. ⚠️ Everything
> else fails: the target variable is never defined, the experiment is one month, and
> the paper calls a Jordanian series "Sensex" throughout.

### Setup

| | |
|---|---|
| Target | **ARBK (Arab Bank), Amman Stock Exchange** — though Fig. 6's 6,600–7,200 range is the **ASE general index**, not ARBK's ~25–30 JOD share price ⚠️ |
| Text | RSS feed, `investing.einnews.com/news/ase-stock` |
| Prices | `marketstoday.net/markets/jordan` |
| Period | prices "2005 to 2007", but the analysis is **April 2006 only** (Fig. 6 covers 1–18 April) ⚠️ |
| Scorer | POS tagger (**noun + adverb only**, "chosen for the best result") → **SentiWordNet** → Sentence Sentiment Score |

### One sample — and there is no learned model

**One sample = one trading day.** Two signals combined by a **hand-written 4-row lookup
table** (Table 1). Nothing is fitted; no parameters, no training, no split.

| signal | construction |
|---|---|
| **MA rule** | `MA5 > MA10 > MA15` → positive · `MA5 < MA10 < MA15` → negative · otherwise neutral |
| **Sentiment** | SSS = sum of SentiWordNet word scores per sentence → positive / negative / neutral |

| sentiment | MA | → output |
|---|---|---|
| Positive | Positive | **Positive** |
| Positive | Negative | Neutral |
| Negative | Positive | Neutral |
| Negative | Negative | **Negative** |

### ⭐ The one idea worth taking — sentiment as a VETO, not a generator

Read that truth table closely. **Sentiment can never flip a call from positive to
negative or back.** It can only downgrade an MA signal to Neutral when the two
disagree. The sentiment layer is a pure **abstention mechanism** — it withdraws
signals, it never creates them.

**No other paper in this folder does this**; in all fourteen others sentiment generates
directional calls. And it is the right shape for a weak signal:

- **⭐ Paper 51 found the same thing independently.** Its "SC2" variants — which
  *divest* rather than short on a negative prediction — beat the shorting variants on
  every metric (RFC Title: return **9.92% vs 7.67%**, drawdown **18.67% vs 25.52%**).
  **Two papers, two markets: a weak signal should REDUCE exposure, not reverse it.**
- **It fits experiment_3's binding constraint.** If costs are what kill the strategy, a
  layer that only *removes* trades is directly testable in a costed backtest and can
  only reduce turnover.

**→ For this thesis:** the GBM ranks `rel5` as it already does; sentiment only drops
names where the text disagrees. A materially cheaper claim to defend than "sentiment
predicts returns", and it is falsifiable with the walk-forward machinery already built.

### ⚠️ Five failures

1. **⚠️ The target variable is never defined.** Table 2 reports "Correctly Classified
   391/499 = 78.75%", but the paper never states what counts as correct, how ground
   truth was established, the class distribution, or any split. And since the sentiment
   layer only converts directional calls into *neutral* ones, the whole 64.32% → 78.75%
   improvement hinges on how neutral predictions are scored — never explained.
2. **⚠️ "Sensex" is used throughout for a Jordanian series.** §3.1 and Eq. 1 define the
   **BSE Sensex** (India, 30 stocks); reference [15] is `bseindia.com`. The data is ASE.
   Eq. 1 is therefore irrelevant to anything computed, and it is left unclear whether
   the target is the stock or the index.
3. **One month of data.** "499 total instances" from a single security over ~20 April
   2006 trading days is never reconciled.
4. **The baselines come from a different paper on different data** — ID3 (46.69%) and
   C4.5 (47.49%) are quoted from Al-radaideh et al. [16], not re-run. Both fall *below*
   the 3-line MA heuristic (64.32%), which suggests they were misapplied there too.
5. **"Precision %" is accuracy.** The Table 2 row is labelled Precision but computed as
   correct/total. Eqs. 4–5 define precision and recall properly; neither is ever
   computed per class.

Also: **SentiWordNet is general-purpose** — paper 53 showed LMFinance beating it
decisively across 12 stocks × 3 models — the **noun+adverb** POS restriction was
selected on outcome (and is odd, since adjectives carry most English sentiment), and
**no lag is stated anywhere**.

### How to use it in the thesis

**Cite — one use:** the **veto / abstention framing**, paired with **51**'s
divest-don't-short result. Two independent studies converging on *a weak signal should
reduce exposure, not reverse it* is a defensible basis for designing the VN sentiment
feature as a filter over the existing GBM ranking rather than as a predictor.

**Do not follow:** undefined ground truth, one month, a target that may be a stock or
an index, borrowed baselines, general-purpose scorer, no lag, and a paper-wide
terminology error.

---

# Paper 56 — Li, Xie, Chen, Wang, Deng (2014) ⭐⭐ THE THRESHOLD PROOF

*News impact on stock price return via sentiment analysis.*
**Knowledge-Based Systems** 69 (2014) 14–23 · Elsevier · DOI 10.1016/j.knosys.2014.04.022 ·
City University of Hong Kong / Hong Kong Baptist University / Shanghai Jiaotong. 10 pp.
files: `56. News impact on stock price return via sentiment analysis.pdf`
**= `64. …` (byte-identical, same MD5 `2b78fb86…`) — one paper, two numbers.**
*Same first author as paper 53, and its direct predecessor; 53 cites this.*

> **→ Verdict: ⭐⭐ cite it for the single best methodological result in this folder —
> an ANALYTICAL PROOF that classification accuracy can be driven to 1.00 by widening
> the neutral band, with no change to the model** — plus the rule that **the label
> threshold must exceed round-trip transaction costs**. ⚠️ Do not follow the target:
> intraday news labelled with the *same day's* open-to-close return is contemporaneous
> impact, not forecasting.

### Setup

| | |
|---|---|
| Market | **HKEx**, Hang Seng Index constituents; news from **FINET** |
| Universe | **22 stocks** across Commerce 10 / Finance 5 / Properties 4 / Utilities 3 |
| ⭐ Universe hygiene | stocks added to the HSI after 2003-01-01 are **removed** — *"during the first few months the newly added constituents do not appear rational and they are usually mispriced"* (**tyranny of the index**); 50 → 22 |
| Period | **Jan 2003 → Mar 2008** |
| Split | **chronological** — train 2003–2005 · validation 2006 · **independent test Jan 2007 – Mar 2008** |
| Model | **SVM (RBF)**, grid search over 90 (γ, C) pairs |

### One training sample

**One sample = one (stock, trading day).**

`X` = **sentiment vector** `S = N ⊙ D` (Eq. 3) — term-frequency vector `N` of that day's
news inner-multiplied with dictionary `D`: **Harvard IV-4 (182 dimensions)** or
**Loughran–McDonald (6 dimensions)**. Multiple items for one stock-day are concatenated
into one document first.

`y` = **3 classes from the Open-to-Close return** `R = (Close − Open)/Open` (Eq. 4):

```
positive   R ≥ th
neutral    −th < R < th
negative   R ≤ −th
```

Open-to-Close is chosen over Close-to-Close for three stated reasons: seasonality,
the non-trading-day gap, and **HK's T+0 mechanism** — *"Information has been reflected
in the Close price by the intra-day trading."*

### ⭐⭐ The threshold analysis (§4.2, Eqs. 14–17, Figs. 4–5)

No other paper here attempts this. Assume Gaussian returns and draw predictions
randomly from the prior; then `acc = P²₋₁ + P²₀ + P²₊₁`, and by symmetry
(`P₋₁ = P₊₁ = P`, `P₀ = 1 − 2P`):

```
acc = 6P² − 4P + 1                    (Eq. 17)
```

A U-curve with **minimum 1/3 at P = 1/3**, rising toward **1.00** as `th` widens and
almost everything becomes neutral. In their own words:

> *"the accuracy can be increased only by manipulating the labeling method without any
> change of the learning model."*

Fig. 4 confirms it empirically on four stocks — accuracy climbs to ~1.0 with `th`.

**⭐ And they then choose `th` honestly**, under two constraints: it must exceed the
**HK transaction cost of 0.003 (30 bps)**, and it must not sit in the inflating region.
They select **th = 0.005 (50 bps), deliberately near the MINIMUM of the trivial-accuracy
curve** — the threshold least flattering to their own results.

**This is the analytical counterpart to paper 49's empirical 0.90 → 0.70 → 0.56 decay.
Together: the theory and the demonstration of the same failure.**

### ⭐ Results — and the polarity ratio collapses

Index-level accuracy (Table 15):

| approach | validation | independent test |
|---|---|---|
| **LMD** (6-dim, financial) | **0.5976** | **0.5527** |
| HVD (182-dim, general) | 0.5892 | 0.5460 |
| SN (SenticNet) | 0.5876 | 0.5445 |
| BoW | 0.5858 | 0.5391 |
| **PO** — polarity ratio `(pos−neg)/(pos+neg)` | **0.2230** | **0.2789** |
| **APO** — adversarial polarity | 0.2172 | 0.2665 |

**Conclusion #2, verbatim: *"Simply focusing on positive and negative dimensions could
not bring useful predictions."*** A direct empirical refutation of the single-polarity
scalar used by **9, 45, 46 and 55** — and corroboration of **48**'s keep-the-full-softmax
insight from the opposite direction. **The dictionary's multi-dimensional structure is
what carries the signal; collapsing it to one ratio destroys it.**

**⭐ Explicit CV rejection**, footnote 15: *"It would be better not use cross-validation
for time-series-like problem, which might bring look-into-the-future issue if the lagged
correlation is high."* Only **51** is this explicit.

### ⚠️ Two problems

1. **⚠️ This is a contemporaneous IMPACT study, not a forecast.** Their own Fig. 3
   worked example uses news timestamped **2008-03-17 11:19 — mid-session** — to label
   *that same day's* open-to-close return. Their third justification for the
   open-to-close target explicitly invokes intraday information reflection. That
   establishes **contemporaneity, not predictability**. The title says "impact"; the
   conclusions say "prediction". The former is what was measured.
2. **The sentiment-beats-BoW claim is a coin flip.** Pairwise wins across 22 stocks
   (Tables 11–12): HVD beats BoW **12–10** in validation but **loses 8–14** in the
   independent test; LMD goes 14–8 then 12–10. Index-level, LMD leads BoW by **1.4 pp**.
   Real but marginal, and unstable between sets.

Also: no trading simulation despite cost-awareness at the labelling stage, and no
majority-class baseline stated (though Eq. 17 supplies the random-prior one implicitly).

### How to use it in the thesis

**⭐⭐ Adopt two things directly:**
1. **The threshold discipline.** Before reporting any classification metric, state the
   **trivial-guessing accuracy at the chosen threshold**, and pick the threshold near
   the *minimum* of that curve rather than where it flatters the result.
2. **Label thresholds must exceed round-trip transaction costs.** This ties labelling to
   tradability at the source — exactly experiment_3's finding. For `rel5`: do not label
   a move "up" unless it clears costs. A concrete, cheap design constraint with a
   citation behind it.

**Also cite:**
- **The polarity-ratio refutation** (PO/APO at 0.22–0.28 vs 0.55 for full dictionaries),
  with **48**, against **9/45/46/55**.
- **The tyranny-of-the-index correction** — directly applicable to VN30/VN100 constituent
  churn, which is a live bias source in the panel here.

**Do not follow:** the same-day open-to-close target with intraday news.

---

## Combined reading — where the sixteen papers leave the thesis

1. **All six predict the wrong target for this thesis** — per-stock or index absolute
   short-horizon direction (45 the overnight gap, 46 the price level itself). None
   touches cross-sectional relative return.
2. **On integration, the ordering is 43 ≈ 44 > 28 > 45 > 9 > 46.** Sentiment-as-scalar
   (9) conflates polarity with document volume by construction;
   text-as-sequence-with-encoder (28) fixes that; **text-as-hierarchy** (43) separates
   the component that cancels in a cross-section from the one that does not; **44
   learns that weighting** instead of grid-searching it, over arbitrary relation types.
   **45 is a scalar again — but the RIGHT scalar**, signed, engagement-weighted and
   market-hours aligned. **46 is disqualified** by its leak. **Take 45's recipe, 43's
   streams, 28's encoder shape, 44's two-level weighting, and 46's segmentation stage;
   take nothing structural from 9.**
3. **⚠️ NOT ONE of the seven reports a majority-class or naive baseline.** 9 leaks
   through a random split over overlapping labels; 28 omits the base rate on 187 test
   samples; 43 sits at ~0.52 without stating it; 44 reports 0.51 on a **3-class**
   target whose dominant class is never quantified; **45 explicitly diagnoses class
   imbalance as the reason its best model wins and still does not report the rate**;
   46 reports currency-unit MAE with no persistence (`Ĉ_t = C_{t−1}`) benchmark.
   **⭐ Only 47 publishes enough to compute one — and its model lands exactly on the
   base rate (53.5% majority vs 54.83% best validation).** **48 and 49 both omit the
   persistence benchmark on price-level regressions and both land at or WORSE than
   it** (MAPE 4.5% and 1.48–2.32% against ~1.5–2% daily moves). All nine reinforce
   that the protocol already in use here (chronological, purged, costed) is the
   differentiator.
4. **⚠️⚠️ REPORTED SKILL IS A FUNCTION OF TEST-SET SIZE, AND THE LITERATURE PROVES IT
   ITSELF.** Three independent demonstrations, none of them a critique from outside:
   - **49 — the decisive one.** Same model, same data, same features, three test
     horizons: **0.90 (10 days) → 0.70 (30) → 0.56 (100)**. Test-set size is the only
     variable that moves.
   - **47** — own Table 10 and learning curve: **~52% validation** against a **76.3%**
     abstract, on ~15 test samples.
   - **48 / 49** — price regressions reporting MAPE at or **worse than predicting no
     change**, with the persistence benchmark never computed.
   - **⭐ 50 — the direct measurement.** It publishes **Kappa = 0.078 for sentiment
     alone**, the base-rate-corrected statistic. Not "weak" — *no agreement beyond
     chance*. Its 59.18% sentiment-only accuracy is pure majority class.
   - **⭐⭐ 51 — the properly-run experiment.** The only paper with a random-walk
     benchmark, MCC/Brier, explicit leak prevention and walk-forward OOS. Its verdict:
     **MCC 0.069, before transaction costs**, on 8.5M articles — and the authors trace
     the entire outperformance to one month in 2011. **This is the ceiling estimate.**
   - **⭐ 53 — the computable one.** Labels are set by 25%/75% return quantiles, so
     "always predict horizontal" scores **~50% by construction**. Best sector-average
     accuracy in the paper: **0.496**. Its own design supplies the bar and nothing
     clears it.
   - **⭐⭐ 56 — the analytical proof.** `acc = 6P² − 4P + 1` for random guessing from
     the prior, so **widening the neutral band drives accuracy to 1.00 with no model
     change at all**. The authors say so outright and then deliberately pick the
     threshold at the *minimum* of that curve. **With 49's empirical decay, this is
     theory plus demonstration for the same failure.**

   Across the folder, accuracy tracks looseness inversely: 80.5% and ~0.60 from the
   papers using random/k-fold CV on time series (9, 45); ~0.51–0.52 from the two most
   careful (43, 44); 46's flattering MAE from a target its own inputs determine
   exactly. Read with experiment_3 — where a genuine AUC-0.77 signal still failed to
   beat Buy&Hold after costs — **the expected value of a VN news feature should be set
   low, and the evaluation chapter can be argued entirely from these papers' own
   numbers.**
5. **⚠️ Five of the six aggregate text by CALENDAR DAY**, which drops post-close news
   into the same row as that day's close. Only 45 treats the window as a design
   decision and tests a market-hours division. Any implementation here must align to
   VN market hours (**09:00 → 09:00 ICT**) and lag to `d+1`.
6. **⭐ Only 45 validates its sentiment scorer on labelled data before using it** —
   and finds an 86%-vs-53% domain split between professional prose and laypeople's
   messages. That single step is the cheapest, highest-value thing to copy, and it
   retroactively explains 43's puzzling "general lexicon beats domain-specific" result.
7. **⭐ THE METRIC CONTRADICTION APPEARS IN THREE INDEPENDENT PAPERS**, with three
   different metric pairs, and none of them resolves it:
   - **44** — news raises F1 +18.1%, accuracy +11.5%, daily return +2.3%, and **lowers
     the Sharpe ratio**
   - **43** — WCN-LSTM wins most accuracy cells and **loses most F1 cells**
   - **46** — multi-source wins MAE and **loses MSE and RMSE**; sentiment-only is the
     **worst** of its four single sources

   Three teams, three markets, three metric pairs, one shape: *the headline metric
   improves while a risk- or error-sensitive second metric degrades.* This is
   experiment_3's conclusion — a better classifier is not a better portfolio — arrived
   at independently three times. **Cite the triple rather than asserting the claim.**
8. **The binding constraint is the CORPUS, not the architecture.** Paper 28 quantifies
   what a working text model needs (~10 titles/day/target); experiment_6 delivers
   **0.35**. Paper 43 supplies the fix — back sparse per-ticker news with abundant
   market- and sector-level streams. Paper 44 confirms the same sparsity problem from
   the other end (its Sharpe regression is blamed on thin news). The sector layer
   already exists (Simplize industry tree); the market layer needs a market-wide feed;
   θ3 needs experiment_6 generalised beyond VCB.
9. **On relational structure:** paper 44 shows the **edges matter more than the
   architecture** (relation choice swings F1 0.267 → 0.458) and that **unweighted
   aggregation is worse than no graph** (GCN < LSTM). For VN, Wikidata is not a viable
   edge source; sector membership and ownership links are. And experiment 1.8's
   cross-sectional rank features are already a zero-cost degenerate GNN.
10. **If text becomes a thesis chapter, the shape is now fully specified** — and no
    single paper has it, so it is assembled across five:
    - **segmentation** — VN word segmentation (underthesea / VnCoreNLP) + a VN
      stopword list **before any encoder**; a mandatory stage the English-only papers
      do not have *(46)*
    - **⚠️ annotation** — labels for the VN scorer must come from **human annotation**
      (the Financial PhraseBank pattern: 16 annotators, 4,846 sentences) or an
      independent source. **Never auto-label with a lexicon and train on the same
      text** — that measures only the lexicon *(54; and 46's label-from-future-returns
      is the other flavour of the same mistake)*
    - **scorer** — a **finance-domain** VN transformer sentence encoder, **validated on
      labelled VN financial text first** *(45)*, separately for prose vs forum
      registers. Domain-specific beats general — **53** (12 stocks × 3 models ×
      4 dictionaries) and **45** both say so; only **43** dissents, and it was applying
      a 10-K-derived lexicon to newspaper headlines
    - **per-document score** — keep the **full multi-dimensional** sentiment
      representation, **never a single polarity ratio**. **56** measures the cost
      directly: full dictionaries score 0.55 at index level, `(pos−neg)/(pos+neg)`
      scores **0.22–0.28**. Keep the 3-way softmax `P(neu)/P(pos)/P(neg)` as separate
      features *(48)*; engagement-weighted *(45)*
    - **aggregation** — mean not sum, over a **09:00→09:00 ICT** market-hours window,
      with exponential decay across Tết and weekends, document count kept as its own
      feature *(45)*
    - **streams** — market / sector / stock, weighted *(43)*, learned rather than
      grid-searched *(44)*
    - **cross-section** — ranked per date against the VN30/VN100 panel, so the market
      stream cancels and the idiosyncratic stream survives *(43 + this thesis)*
    - **labels** — **quantile-defined**: top quartile of `rel5` long, bottom quartile
      short/avoid, middle 50% ignored. Makes the base rate **known by construction**
      and matches experiment_3's top-6/bottom-6 *(53)*. **⭐⭐ And the threshold must
      EXCEED round-trip transaction costs**, with the trivial-guessing accuracy at that
      threshold stated alongside every metric — `acc = 6P² − 4P + 1`, so a wider neutral
      band buys accuracy for free *(56)*
    - **scaling** — **per-window Z-score** (local mean/std inside each rolling window),
      never global statistics *(53)*
    - **lag** — news from `d` predicts from `d+1`
    - **model** — the existing GBM, not a sequence net *(experiments 1.6–2.3)*
    - **text field** — the **content**, not the headline; title-only sentiment is
      worse than useless *(51, overturning 28 and 46)*
    - **judgement** — costed walk-forward on `rel5`, **never accuracy** *(experiment_3, 44)*
    - **⭐ role** — sentiment as a **VETO over the existing GBM ranking, not a
      predictor**: it may drop a name where the text disagrees, never flip a call.
      **55** and **51** converge on this independently — a weak signal should *reduce*
      exposure, not reverse it (51's divest-only SC2 beat its shorting SC on every
      metric). It is also the only framing that is cheap to defend and directly
      testable in the costed walk-forward already built
    - **⭐⭐ protocol** — **51's, wholesale**: a naive benchmark always (random walk /
      persistence / majority class), **MCC + Brier** beside accuracy, a written
      statement of how same-day leakage is prevented, walk-forward expanding window
      with tuning confined to the in-sample block, and **max drawdown reported** —
      plus the transaction costs 51 omits and experiment_3 already enforces

---

## File index

- `9. Stock market prediction using machine learning classifiers and social media, news.pdf`
  — Khan et al. 2020, JAIHC (Springer). 24 pp. *Sentiment scalar appended to OHLCV;
  cite as pipeline exemplar + leakage counter-example.*
- `28. Deep learning for stock market prediction from financial news articles.pdf`
  — Vargas, de Lima, Evsukoff 2017, IEEE. 6 pp. *SI-RCNN dual-branch text+technical;
  cite and borrow the architecture.*
- `43. LSTM based stock prediction using weighted and categorized financial news.pdf`
  — Usmani & Shamsi 2023, PLOS ONE (open access, data on Mendeley). 27 pp.
  *WCN-LSTM market/sector/stock weighted news hierarchy; **cite and borrow the
  hierarchy** — the one idea in this folder to build on.*
- `44. ML-GATA_Multilevel_Graph_Attention_Model_for_Stock_Prediction.pdf`
  — Huang et al. 2022, IEEE Access (open access). 15 pp. *ML-GAT two-level graph
  attention over Wikidata corporate relations; **cite the negative results** —
  accuracy up / Sharpe down, and edges mattering more than architecture.*
- `45. Stock trend prediction using sentiment analysis.pdf`
  — Xiao & Ihnaini 2023, PeerJ Comput. Sci. (open access, data supplied). 18 pp.
  *VADER-on-tweets + FinBERT-on-news, market-hours aggregation window, holiday decay;
  **cite the recipe** — the only paper here that validates its scorer first.*
- `46. Stock price prediction based on multiple data sources and sentiment analysis.pdf`
  — Wu, Liu, Zou & Weng 2021, Connection Science (Taylor & Francis). 20 pp.
  *S_I_LSTM, Jieba→Word2Vec→CNN sentiment + Att-LSTM price regression on 5 Shanghai
  A-shares. **⚠️ Provable label leak** (`C = L + %K/100 × (H − L)`); cite only the
  CJK segmentation stage, the 14:1 forum-to-news corpus, and the MAE/MSE contradiction.*
- `47. Stock Market Prediction Using Microblogging Sentiment Analysis and Machine Learning.pdf`
  — Koukaras, Nousi & Tjortjis 2022, Telecom (MDPI, open access). 21 pp. *MSFT only,
  77 days, Twitter+StockTwits × TextBlob+VADER × 7 classifiers. **⚠️ Self-refuting** —
  Table 10 shows 52% validation against a 76.3% abstract; cite as the evaluation
  cautionary tale and for the one computable base rate in the folder.*
- `48. Predicting Stock Prices with FinBERT-LSTM Integrating NewsSentiment Analysis.pdf`
  — Gu, Zhong, Li, Wei, Dong, Wang & Yan 2024, ICCBDC (ACM). 6 pp. *843k Benzinga
  articles → FinBERT 3-way softmax + 8-day price → stacked LSTM → next close.
  **Cite the 3-D sentiment representation and the corpus scale**; ⚠️ MAPE 4.5% is
  worse than persistence, and "Accuracy" is `1 − MAPE`.*
- `49. Stock Prediction by Integrating Sentiment Scores of Financial News and MLP-Regressor A Machine Learning Approach.pdf`
  — Maqbool, Aggarwal, Kaur, Mittal & Ganaie 2023, Procedia Comput. Sci. (Elsevier,
  open access). 12 pp. *VADER+TextBlob+FLAIR → MLP-Regressor on 4 Indian stocks,
  10 years. **⭐ Cite the 10/30/100-day decay table** (0.90 → 0.70 → 0.56) — the
  folder's best evidence that small test sets manufacture skill.*
- `50. Predicting Stock Market Behavior using Data Mining Technique and News Sentiment Analysis.pdf`
  — Khedr, Salama & Yaseen 2017, IJISA (MECS Press). 9 pp. *NB/TF-IDF sentiment +
  discretized OHLC → KNN on 3 NASDAQ names. **⚠️ Feature = label** (discretized close
  vs previous close IS the raise/fall target); **cite only Kappa 0.078** — sentiment
  alone has no skill beyond chance.*
- `51. Using Financial News Sentiment for Stock Price Direction Prediction.pdf`
  — Fazlija & Harder 2022, Mathematics (MDPI, open access). 20 pp. *FinBERT fine-tuned
  on the Financial PhraseBank → 58 daily scores → RF → S&P 500 direction.
  **⭐⭐ THE METHODOLOGICAL TEMPLATE** — random-walk benchmark, MCC/Brier, explicit
  leak prevention, walk-forward OOS; **content beats titles**; ⚠️ MCC 0.069, no costs.*
- `52. Prediction of stock values changes using sentiment analysis of stock news headlines.pdf`
  — Nemes & Kiss 2021, J. Information and Telecommunication (T&F, open access). 20 pp.
  *TextBlob/VADER/RNN/BERT over ~13 days of finviz headlines on 4 tech names.
  **⚠️ No prediction model — same-day correlations only**; cite only that the four
  tools disagree in SIGN on identical data.*
- `53. Incorporating stock prices and news sentiments for stock market prediction - A case of Hong Kong.pdf`
  — Li, Wu & Wang 2020, Information Processing & Management (Elsevier). 19 pp.
  *2-layer LSTM over price + 10 TA + 4 sentiment dictionaries, 12 HKEx stocks.
  **⭐ Cite the quantile-label design, LMFinance beating general dictionaries, and
  per-window Z-scoring**; ⚠️ every accuracy sits at or below the ~50% base rate its own
  25/50/25 labelling guarantees, and the test window is the GFC onset.*
- `54. Stock trend prediction using news sentiment analysis.pdf`
  — Joshi, Bharathi H. N. & Rao 2016, K. J. Somaiya College of Engineering, Mumbai.
  11 pp. *Dictionary labels → TF-IDF → RF/NB/SVM on **92 AAPL articles**.
  **⚠️ Circular labelling — no stock prediction in the paper at all**; cite only as the
  anti-pattern for auto-labelled sentiment corpora.*
- `55. Sentiment Analysis for Effective Stock Market Prediction.pdf`
  — Bharathi & Geetha 2017, IJIES (INASS). 9 pp. *SentiWordNet SSS + MA5/10/15 rule
  combined by a 4-row lookup table, ARBK on the Amman exchange, April 2006.
  **⭐ Cite the veto framing** — sentiment only withdraws a signal, never generates one
  (with 51's divest-don't-short); ⚠️ target never defined, "Sensex" used for a
  Jordanian series.*
- `56. News impact on stock price return via sentiment analysis.pdf`
  **= `64. …` (byte-identical duplicate, same MD5)** — Li, Xie, Chen, Wang & Deng 2014,
  Knowledge-Based Systems (Elsevier). 10 pp. *HVD/LMD sentiment vectors → SVM →
  open-to-close direction, 22 HSI stocks. **⭐⭐ Cite the threshold proof**
  (`acc = 6P² − 4P + 1`; th must exceed transaction costs), the polarity-ratio
  refutation, and the tyranny-of-the-index correction; ⚠️ intraday news vs same-day
  return is impact, not forecasting.*
