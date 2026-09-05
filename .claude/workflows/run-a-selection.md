# Workflow — run a feature selection

> **Goal:** find out which channels carry signal, with a null that makes the answer readable.
> Stages 2 and 4 of the chain. **Cost: ~1 min for a narrow pool with no null, up to 6 h for a wide
> pool with 20 draws.**
>
> ⚠️ **This is the single most expensive decision point in the repo, and the expense is not the
> compute.** A selection run that clears a bar nobody can interpret produces a number that gets
> quoted for months. Every step below exists to stop that.

---

## 1. Decide the ROOT first — before the pools, before the draws

**A run that measures the SELECTION is not a run that feeds the CHAIN**, and only `--root`
separates them.

| root | holds |
|---|---|
| `reports/feature_selection/` | runs that FEED the chain |
| `reports/feature_selection_probes/` | runs that MEASURE the selection — a window probe, a representation probe, a pilot |

⚠️ **`--scope` fixes neither** — it suffixes both groups identically. A probe left in the chain's
root fails one of two ways: with the same setup keys it is **SILENTLY UNIONED** into a chain
table; with a different one it **raises and nothing plans at all**, including unrelated chains.

## 2. Decide the draw count — **10 to FAIL, 20 to PASS**

Decided on measurement, not taste:

| | 10 draws | 20 draws |
|---|---|---|
| the p95 BAR | already stable — two independent seeds gave +0.0573 and +0.0565 | — |
| **`SE(sd)`** — the denominator of `z` | **0.0083** | **0.0051** |

⚠️ **`p` is not the criterion.** Until a draw beats the observed it is pinned at the `1/(n+1)`
floor either way — 0.0909 at 10 draws, 0.0476 at 20 — which says *"no draw beat it"* and never
*"p is small"*. **`z` is the statistic.**

The rule follows from the asymmetry: **when the observed lands below or near the null's mean, 10
draws settle it. When it lands far above, the whole claim is *how far*, and that is `z`.**

⚠️ **More draws can move `z` DOWN.** One run went `z = +2.13` at 10 draws to **+1.83** at 20 — the
observed is deterministic and unchanged; only the null moved, its `sd` growing 19 %.

## 3. Check the pools are on ONE calendar — **C1**, then `pipeline`'s `data` row

The joins are INNER. A sibling pool two months behind truncates the run to its calendar and the
report says nothing about it.

## 4. Prune a wide pool first — **W7**

`python -m feature_selection.prune` is **LABEL-FREE by construction, and that is the point**:
ranking candidates by their correlation with the target would build look-ahead into the candidate
set before any null could see it.

⚠️ **`pool__ta` reduced 711 → 145 this way** — 208 of its 922 columns are booleans (thresholded
copies of numeric channels already present), 133 are pairwise MA-vs-MA combinatorics, and 7 are
measured duplicates of `pool__basic` channels.

## 5. Choose the machine

| | this laptop | Kaggle T4 |
|---|---|---|
| VRAM | **4.0 GiB** | 14.6 GiB |
| RAM free | ~7 GB | ~29 GB |
| known-good width | narrow pools | **162 channels** (233 died in the ranker phase, four attempts) |

⚠️ **A Kaggle run is a different PROCEDURE, not the same run on a faster card.** Its image ships
**xgboost 3.2.0 / sklearn 1.6.1** against `mt_env`'s **2.1.1 / 1.7.2**, and XGBoost subsamples
from a different RNG stream per device. `contract.SETUP_KEYS` carries `env_fingerprint`, so the
two cannot be unioned by accident — **expect a group collision instead, and pass `--scope`.**

**On Kaggle: K1 → K2 → K3 → K4.** ⚠️ **`kgpu rehearse` every time a shipped module changed.** It
is the only thing that catches an import the worker cannot satisfy — a new top-level import once
died at cell 0, **1m 29s into a 233-channel run**, quota already spent.

⚠️ **The queue is the floor, not the compute** — 5.2 min of an 8m 15s round trip. Batch one large
run rather than several small ones, and iterate in `rehearse`.

## 6. Run it — **C2** (or **C4** for layer 2)

## 7. Read the report — four columns, not one

| read | because |
|---|---|
| `ic_mean` | the observed |
| the p95 bar | the threshold |
| ⚠️ **the null MAX** | **when it exceeds the observed, `clears_bar` is the wrong summary** — quote the max beside it. One pool cleared its p95 while a shuffled draw still beat the real data |
| ⚠️ **the null MEAN** | it is often **not zero**. One selection earned +0.0291 on a shuffled label, so its excess over chance was +0.078, not the +0.1075 headline — quoting the raw IC overstated it by 37 % |
| `ic_trend_per_fold` | a mean built from folds decaying to negative is not a signal — and a **rising** trend on a ragged pool measures **data arrival**, not strengthening skill |

⚠️ **Then check `n_dead_train`.** An all-NaN train slice is imputed to the constant `0.0` and then
RANKED — there is no median to take, so `_impute` invents one in a unit the channel never had.
**197 of 357 channels in one fold, and 44 of the 66 SELECTED.** `validation.csv` carries
`n_dead_train`/`n_dead_test` per fold; on a single-stock pool it does **not**, and the count has to
be taken externally (`P21`).

⚠️ **Exclude `prop_*` at a short time scale.** Proprietary flow starts 2023-01-03 — coverage
~0.20 — so it is all-NaN in the training slice of most folds, imputed to 0.0, and ranked anyway.
Order stats (from 2010) and foreign flow (from 2012) survive; the prop block does not.

## 8. Price the search, not just the features

⚠️ **`NUL-1`: the null prices the FEATURE search inside one run, and nothing else.** Not the
universe, not the horizon, not the target, not the fact that this is the fifth thread tried.

Measured directly once: five tickers were searched, and `P(z > 1.83)` once is 0.0336 while
**`P(at least one of five) = 0.157`.** The survivor's survival was about as surprising as nothing
at all. **Say how many things you tried, beside the `z`.**

---

## Done when

- [ ] the run folder is under the right `--root`, and holds an `outstanding.csv` — ⚠️ **a run
      folder without one is skipped by `final_features` WITHOUT A WORD**
- [ ] you can state `ic_mean`, the bar, the null **max** and the null **mean**, and say whether
      rule 3 fires
- [ ] you have written down how many experiments were tried to get here
- [ ] `evidence=` in the table `COMMENT` says what was actually measured — `no_null` is an
      **unknown**, never a pass

## Traps

⚠️ **A notebook run and a CLI run were not the same artefact until 2026-08-16.** The notebook
recorded no `columns_by_table`, no `execution` block and **no `outstanding.csv`** — and two runs
sat in exactly that state while `final_features` planned 19 runs and reported no error.

⚠️ **Never append an override cell below the notebook's parameter cell.** That cell ends with
`EXCLUDE = IDENTITY + [c for c in ALL_TARGETS if c != TARGET]`; an override after it leaves
`EXCLUDE` excluding the OLD target, handing the run its own label as a candidate feature. **It
does not raise — it reports an IC near 1.**

⚠️ **The cost model has no term for the target and it is worth 13.7×.** The same panel took
2,016 s on a price level and 146 s on a return. Treat `budget_minutes` raises as advisory.
