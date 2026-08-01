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

**They form a progression, and the progression is the point.** All four add news text
to price features to predict short-horizon direction, and they differ in *how the
text enters the model*:

| | paper 9 (2020) | paper 28 (2017) | paper 43 (2023) | paper 44 (2022) |
|---|---|---|---|---|
| text enters as | **one scalar** beside OHLCV | a **sequence + own encoder** | **three weighted streams** (market/sector/stock) | **BERT `[CLS]`** summed into a graph node |
| stock relations | — | — | implicit (sector grouping) | **explicit graph** (Wikidata, 2-level attention) |
| split | random 70/30 ⚠️ | chronological | time-series CV, val ahead of train | chronological |
| test size | 150 | **187** ⚠️ | 41 stocks, per-stock | 316 d × 423/286 stocks |
| repeats | 1 | 1 | 1 | **10, means reported** |
| baseline reported | — | **none** ⚠️ | **none** ⚠️ | **none** ⚠️ |
| accuracy | 80.5% (leaked) | 62.0% | **~0.50–0.55** | ~0.51/0.57 (**3-class**) |

Read together: **the more careful the evaluation gets, the closer accuracy falls to
chance.** Papers 43 and 44 are the most methodologically careful and both land near
coin-flip — which makes their weak results the most informative numbers in the
folder, not the least. **Not one of the four reports a majority-class baseline.**

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

## Combined reading — where the four papers leave the thesis

1. **All four predict the wrong target for this thesis** — per-stock or index
   absolute short-horizon direction. None touches cross-sectional relative return.
2. **On integration, the ordering is 43 ≈ 44 > 28 > 9.** Sentiment-as-scalar (9)
   conflates polarity with document volume by construction; text-as-sequence-with-encoder
   (28) fixes that; **text-as-hierarchy** (43) separates the component that cancels in
   a cross-section from the one that does not; **44 learns that weighting** instead of
   grid-searching it, over arbitrary relation types. **Take 43's streams, 28's encoder
   shape, and 44's two-level weighting idea; take nothing structural from 9.**
3. **⚠️ NOT ONE of the four reports a majority-class baseline.** 9 leaks through a
   random split over overlapping labels; 28 omits the base rate on 187 test samples;
   43 sits at ~0.52 without stating it; 44 reports 0.51 on a **3-class** target whose
   dominant class is never quantified. All four reinforce that the protocol already in
   use here (chronological, purged, costed) is the differentiator.
4. **⚠️ Accuracy falls toward chance as evaluation quality rises: 80.5% → 62.0% →
   ~0.52 → ~0.51.** That ordering across the four papers is itself the finding. Read
   with experiment_3 — where a genuine AUC-0.77 signal still failed to beat Buy&Hold
   after costs — the expected value of a VN news feature should be set **low**.
5. **⭐ Paper 44 supplies the sharpest evidence for that, from its own ablation:**
   adding news raises F1 +18.1%, accuracy +11.5% and daily return +2.3% while
   **lowering the Sharpe ratio**. An independent team, another market, the same
   lesson experiment_3 learned — *classification gains need not become risk-adjusted
   return*. Cite this rather than asserting it.
6. **The binding constraint is the CORPUS, not the architecture.** Paper 28 quantifies
   what a working text model needs (~10 titles/day/target); experiment_6 delivers
   **0.35**. Paper 43 supplies the fix — back sparse per-ticker news with abundant
   market- and sector-level streams. Paper 44 confirms the same sparsity problem from
   the other end (its Sharpe regression is blamed on thin news). The sector layer
   already exists (Simplize industry tree); the market layer needs a market-wide feed;
   θ3 needs experiment_6 generalised beyond VCB.
7. **On relational structure:** paper 44 shows the **edges matter more than the
   architecture** (relation choice swings F1 0.267 → 0.458) and that **unweighted
   aggregation is worse than no graph** (GCN < LSTM). For VN, Wikidata is not a viable
   edge source; sector membership and ownership links are. And experiment 1.8's
   cross-sectional rank features are already a zero-cost degenerate GNN.
8. **If text becomes a thesis chapter, the shape is settled:** three streams
   (market / sector / stock) × a transformer sentence encoder for VN × lagged to
   `d+1` × ranked cross-sectionally against the VN30/VN100 panel × fed to the
   existing GBM, and judged by costed walk-forward — **not** by accuracy.

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
