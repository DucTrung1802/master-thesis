# Experiment 10 — Literature: sentiment / text as an external factor

Unlike experiments 1–9, this folder holds **papers, not code**. It is the reading
file for the one orthogonal-data lever the thesis has *not* built: **text**
(news, social media) as a predictor, which experiment_3's conclusion — *"the
binding constraint is DATA, not model/target"* — puts on the table.

One entry per paper below: what it does, what one training sample looks like,
what it actually establishes, and **whether to cite it, follow it, or both**.

| # | Paper | Year | Verdict |
|---|---|---|---|
| 9 | Khan et al. — *Stock market prediction using ML classifiers and social media, news* | 2020 | **Cite, do not follow** |

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

## File index

- `9. Stock market prediction using machine learning classifiers and social media, news.pdf`
  — Khan et al. 2020, JAIHC (Springer). 24 pp.
