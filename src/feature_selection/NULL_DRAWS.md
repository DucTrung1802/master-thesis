# What `--null-draws 20` is, and why the run is worthless without it

> Written 2026-08-15 against [evaluation.py](evaluation.py), [run.py](run.py),
> [cross_sectional.py](cross_sectional.py) and [outstanding.py](outstanding.py).
> Depth behind one line of [CONTEXT.md](CONTEXT.md) §6b and CLAUDE.md §5 rule 1.
>
> Every number quoted here was measured in this repo. None is illustrative.

---

## 1. The one-sentence answer

A **null draw** is one complete re-run of the entire selection pipeline — all six
rankers, the ensemble, the correlation prune, the purged walk-forward — on a copy of
the panel where **the label has been shuffled so it cannot possibly be predicted**.
`--null-draws 20` does that twenty times and keeps the twenty resulting ICs.

Those twenty numbers are **what this exact procedure scores when there is nothing to
find**. The real run's IC is only a result if it beats them.

```
python -m feature_selection.run --pools pool__basic --null-draws 20
                                                      └── 1 real run + 20 fake ones
```

---

## 2. Why zero is not the bar

The instinct is: IC > 0 means the features carry signal. **That is false here, and it
was measured.**

`run.py` picks the best channels *by how well they fit the labels*. On random labels it
still picks the best-fitting channels — there are always some — and then reports how
well they fit. The selection step **manufactures a positive IC out of noise.**

Measured on `pool__basic`, VCB, `d=20, h=5` ([CONTEXT.md](CONTEXT.md) §6b):

| | |
|---|---|
| mean IC over 20 **shuffled-label** runs | **+0.0167** |
| its p95 | **+0.0556** |
| the **real** run's IC | **+0.0559** |

⚠️ **The null is centred on +0.017, not on zero.** Picking 12 of 27 channels by their
fit to the labels earns +0.017 from noise alone. So `IC > 0` says nothing;
`IC > ~0.017` is where a claim can even begin, and the actual bar is the p95, **+0.0556**
— which the observed +0.0559 essentially ties.

Without the null, that run reports "+0.056 out-of-sample IC, 12 channels selected" and
looks like a finding. With the null it is **z = +1.56, p = 0.050, does not clear** — and
one shuffled draw scored **+0.0606, higher than the real data**.

---

## 3. What one draw actually does

[evaluation.py:270-288](evaluation.py#L270-L288), per draw:

1. Copy the joined panel.
2. Replace the target column with `block_shuffle(y, block = lookback + horizon)`.
3. Re-run **the whole selection** via `factory` — this is a fresh `FeatureSelector`,
   fresh rankers, fresh prune, fresh purged CV.
4. Record the mean out-of-sample IC of the *selected* feature set.

Two design choices in there are the difference between a real bar and a fake one, and
both are documented as ways to manufacture significance:

**⚠️ The shuffle is by BLOCK, not by row.** Consecutive `return_5day` values share 4 of
their 5 days. A row-wise permutation destroys that autocorrelation, the null comes out
far too tight, and the real result clears a bar that was never there.
`block_shuffle` permutes contiguous blocks of `d + h` rows so the label keeps its own
statistical shape and loses only its connection to the features.

**⚠️ The SELECTION is re-run inside every draw.** Holding the feature set fixed and only
re-scoring measures the wrong thing entirely — selection is the step that inflates. This
is why `factory` takes a panel and returns a whole `SelectionResult` rather than a
score.

**⚠️ On a cross-sectional target the shuffle must be panel-aware.** `evaluation.block_shuffle`
permutes rows, which on an N × T panel tears each date's cross-section apart and destroys
the structure the target is computed *within*. `cross_sectional.cross_sectional_null`'s
`date_block` mode pivots the label to `date × ticker` and permutes blocks of **dates**
instead ([run.py:298-309](run.py#L298-L309)). `run.py` picks the right one from the
`cs_` prefix on the target name; you do not pass a flag.

---

## 4. What the twenty numbers become

[`NullResult`](evaluation.py#L135) turns the draws into five quantities. All five go into
`metadata.json` and the run README.

| quantity | is | how to read it |
|---|---|---|
| `null_mean` | what the pipeline earns from pure noise | the floor before anything has been said |
| `null_p95_BAR` | 95th percentile of the draws | **the number to beat, in place of zero** |
| `null_max` | the single best shuffled draw | ⚠️ if this exceeds the observed, noise beat you at least once |
| `z_vs_null` | `(observed − null_mean) / null_sd` | the number to quote at 20 draws |
| `p_value` | `(k + 1) / (n + 1)` | floored at 1/21 = 0.0476 by construction |

⚠️ **`clears_bar` is the wrong summary whenever `null_max ≥ observed`** — quote the max
beside it (CLAUDE.md §5 rule 3). `run.py` prints an explicit WARNING in that case.
`pool__ta` cleared its p95 at z = +2.52 and *one of twenty shuffled draws still scored
higher than the real data*. That is not a pass.

⚠️ **`p_value` is `(k+1)/(n+1)`, not `max(k,1)/(n+1)`** — the latter was the code until
2026-08-10 (issue **NUL-4**) and reported k=0 and k=1 as the same number, i.e. "no
shuffled draw beat the real data" and "one did" printed identically. Found because the
`basic+economy_japan` run reported p = 0.0476 while its own `null_max` sat above its
observed IC.

---

## 5. Why *twenty*, and not 5 or 200

**20 draws buys a p-value resolution of ~0.05 and a usable z.** The p-value is floored at
`1/(n+1)`, so 20 draws **cannot distinguish p = 0.05 from p = 0.001** — which is why
every conclusion in [CONTEXT.md](CONTEXT.md) is stated as a **z**, not a p.

- **Fewer than ~10** and the sd is too poorly estimated for the z to mean anything.
- **More than 20** only helps a result that is genuinely borderline. Nothing in this
  repo has been borderline: `study_3` re-ran the whole grid at 10 draws and the bars
  differ in the third decimal with **the verdict identical in every cell** — the results
  are not close enough to the bar for the draw count to matter.

**And the cost is exactly 20× the run.** Each draw is a full selection, so the null
dominates wall-clock. Measured:

| run | channels | draws | cost |
|---|---|---|---|
| `pool__basic`, `return_5day`, Kaggle T4 (2026-08-15) | 15 | 20 | **3.7 min end to end** |
| `pool__forex`, `return_5day` | 357 | 20 | **41 min** |
| `pool__forex`, `close_adjust_5day` (price LEVEL) | 357 | 0 | 2,016 s for the run alone |
| `basic+economy_usa` | 1,458 | 20 | **~68 CPU-hours** (issue **EVD-1**) |

⚠️ **Target choice moves the null's cost by 13.7×.** `lasso` dominates the bill and
zeroes every coefficient on a return target, converging at once — the same 357-channel
panel takes 2,016 s on `close_adjust_5day` and 146 s on `return_5day`
([CONTEXT.md](CONTEXT.md) §15c-target). **On a return target a 20-draw null is
affordable even on a wide pool**, so `--null-draws 0` there is a choice, not a budget.

---

## 6. What happens downstream if you skip it

`--null-draws 0` is legal and does not fail. It writes `"null": null` into
`metadata.json`, and [outstanding.py:142](outstanding.py#L142) turns that into
`evidence=no_null` on **every row of the shortlist**. From there the string travels
verbatim:

```
outstanding.csv  →  final_features (table COMMENT)  →  train_test_creator
                 →  (dataset metadata.json)         →  model run lineage
```

Every model trained off that shortlist carries "no bar was computed" in its own
provenance. That is deliberate: **an absent null is recorded as absent, never omitted
and never implied to be a pass.** The three values `evidence` can take are

| value | means |
|---|---|
| `no_null` | **unknown** — nobody measured whether this beats noise |
| `failed_null` | **measured**, and it did not clear |
| `cleared_p95_not_a_pass` | measured, cleared the p95 — and the name says the rest |

⚠️ This is why `--null-draws` defaults to **20** in `run.py` where `RUN_NULL` defaults to
`False` in the notebook: a scripted run has no human at the keyboard to read the warning.

⚠️ **`evidence` and `kept_by` answer different questions and neither substitutes for the
other.** `evidence` is the RUN's verdict against shuffled **labels** — does this pool
predict this target at all. `kept_by=consensus` is a CHANNEL's verdict against shuffled
**methods** ([selection_cut.py](selection_cut.py)) — does this channel stand out *within*
the run. A row can read `kept_by=consensus, evidence=no_null`: the six rankers agree
about a channel in a run that was never shown to beat noise.

---

## 7. ⚠️ Re-run the null whenever anything about the run changes

A bar computed for one configuration says nothing about another (CLAUDE.md §5 rule 1).
The measurement that established this:

| representation, same data, same folds | null p95 bar |
|---|---|
| `none` (raw levels) | +0.053 |
| `zscore` | **+0.076** |

**+43% on the bar with no change to the data at all** — standardised windows simply give
the selector more room to overfit. A pool's width moves it too: +0.0556 at 27 channels →
+0.0740 at 162 → +0.0754 at 918 (it saturates, but it moves).

The corollary is that a null is **not** transferable between: pools, targets,
representations, `d`/`h`, or devices. And note the reverse case is also worth measuring —
cs-ranked vs RAW features on VN100 gave bars of +0.0117 and +0.0115, agreeing to the third
decimal, because a per-date target caps what the pipeline can earn from noise regardless
of what the features look like. **That was run rather than assumed**, which is the rule
working.

---

## 8. What the null has actually bought this project

It is the reason §2 of CLAUDE.md exists. Without nulls, this repo would report a working
single-stock predictor:

| run | reports without a null | reports with one |
|---|---|---|
| VCB `pool__basic`, `d=20 h=5` | "+0.056 IC, 12 channels" | z = +1.56, ❌, one draw beat it |
| VCB `pool__ta`, 918 channels | "+0.112 IC" | z = +2.52, cleared p95, ⚠️ null max **+0.1189 > observed** |
| VCB `pool__fa`, 162 channels | "+0.016 IC" | z = **−0.25** — below its null's *mean* |
| the 5-config grid (§6c) | five positive-looking configs | **not one clears its own null** |
| VN100 cross-section | "+0.029 IC" | z = **+6.09** ✅ — *and here the bar is what makes it credible* |

⚠️ **At VN100 the signal did not get bigger — the BAR got smaller.** The observed IC
*fell*, +0.056 → +0.029; the null's mean collapsed from +0.017 to +0.004. The one
positive result in this thesis is legible **only** because the null was computed on both
sides of the comparison.

And the null is what keeps the honest failures honest. The 2026-08-15 Kaggle run:

```
observed +0.0494 | null mean -0.0004 | p95 bar +0.0510 | null MAX +0.0714
                 | z +1.60 | p 0.1429 | FAILS
WARNING: a shuffled draw reached +0.0714, at or above the observed +0.0494
```

Twenty draws, 3.7 minutes, and a result that would otherwise have read as a 5% IC.

---

## 9. In practice

```powershell
# the default, and what to use
python -m feature_selection.run --pools pool__basic --null-draws 20

# deliberately no bar - records evidence=no_null all the way to the model lineage
python -m feature_selection.run --pools pool__ta --null-draws 0

# a borderline result is the ONLY reason to pay for more
python -m feature_selection.run --pools pool__basic --null-draws 100
```

- The null seed is `NULL_SEED = 7`, **fixed and separate from `--random-state`** — the
  bar must not move when the selector's seed does, or the bar and the number it judges
  stop being comparable ([run.py:105](run.py#L105)).
- ⚠️ **A failed null does not discard the observed run.** `run.py` catches it, prints a
  warning, records `evidence=no_null` and still writes the report — this was measured
  twice on 2026-08-10, each time costing a completed selection to an exception in a
  summary f-string ([run.py:310-320](run.py#L310-L320)).
- ⚠️ **A draw that raises is COUNTED, not skipped.** `failed_draws` is on the summary; a
  null built only from the draws that happened to succeed is a biased null.
- Every draw's IC is written to `null_draws.csv` in the run folder. Read it — the twenty
  raw numbers say more than the p-value does.
