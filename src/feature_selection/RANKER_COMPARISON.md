# The six rankers, compared — advantage, cost, necessity, efficiency

> Written 2026-08-16 against [selector.py](selector.py), [ranker_eval.py](ranker_eval.py)
> and the 21 run folders in `reports/feature_selection/`. Depth behind
> [CONTEXT.md](CONTEXT.md) §4 and §19, and behind the one-line entry in CLAUDE.md §4.
>
> **Every number here was measured in this repo.** None is illustrative. Reproduce the
> cost half in 0.1 s with `python -m feature_selection.ranker_eval --cost-only`, and the
> advantage half in ~16 minutes with `--apply`.

---

## 1. The one-sentence answer

**Of six rankers, one is load-bearing, two are nearly free and mildly useful, and three
were costing 97 % of the ranking phase to contribute nothing measurable** — so the default
ensemble went from six to three on 2026-08-16, and the same selection now runs **9.2×
faster** on identical data (ranking phase 376.3 s → 10.2 s, §6).

⚠️ Of that 97 %, `lasso` alone is **95.6** and `mutual_info` **1.8**; `xgb_gain`'s 0.2 is
**not saved** by dropping it, because it shares one fit with `xgb_shap`. That removal buys
a correct weighting, not seconds.

```
before   spearman  mutual_info  xgb_gain  xgb_shap  lasso  permutation     411.8 s
after    spearman                         xgb_shap         permutation      44.8 s
```

---

## 2. Why this had never been measured

§4 of [CONTEXT.md](CONTEXT.md) has listed the six since 2026-08-03 with a column headed
**"sees"**: `spearman` sees monotone association, `mutual_info` sees any dependence,
`xgb_shap` sees interactions, and so on. That is a coherent argument, and it is an
argument about **inductive bias** — about what each method is *capable* of noticing.

It is not the same claim as *this method picks channels that generalise*, and the second
claim had never been tested. The ensemble was six members because six kinds of blindness
seemed better than one, which is a reasonable prior and was never anything more.

⚠️ **The cost of not testing it was not subtle.** `lasso` was **87.2 % of the average
archived run's wall clock** for four months.

---

## 3. How the comparison was done

### 3a. The design

One selection is run per target — the expensive part, once — and then **every candidate
selector's own top-k is scored out of sample on the same purged walk-forward folds, with
the same model**, using the selector's own `_splits` / `_impute` / `_xgb` / `_ic`. "Same
folds, same model" is therefore an identity, not a promise.

| | |
|---|---|
| panel | `unified_schema_vcb.pool__basic ⋈ pool__targets`, **84 channels**, 4,242 windowed rows |
| setup | `d=20, h=5`, `normalize=none`, 5 purged folds, `min_train=500`, `device=cuda`, seed 18 |
| targets | `return_5day` **and** `return_rel_5day` |
| widths | k = 10 **and** k = 20 |
| cells | 2 targets × 2 widths = **4 per selector** |
| control | **40 random draws** of k channels from the same list, per cell |
| selectors | 6 alone + 6 leave-one-out + the full ensemble + the proposed default = 14 |

### 3b. ⚠️ The null is RANDOM-k, not shuffled labels — and that is deliberate

Whether this pool predicts anything is a **different question and it is already settled**:
CLAUDE.md §2 says it does not, and §6b of [CONTEXT.md](CONTEXT.md) measured a positive
out-of-sample IC on shuffled labels. Re-testing that here would answer nothing new.

The question here is narrower — **does ranker M choose better channels than chance?** —
and the correct control for it is chance. So a selector at the 100th percentile beats
*chance at picking channels*, on a pool that does not clear its own label null.

> ⚠️ **Nothing in this document is evidence of predictive skill.** It is evidence about
> which ranker to spend compute on.

### 3c. What each column answers

| column | question | how |
|---|---|---|
| `advantage_pct` | does it beat chance? | its top-k's mean IC, as a percentile of the 40 random draws |
| `advantage_min_cell` | does it beat chance *reliably*? | its worst of the four cells |
| `blend_without` | — | the ensemble's percentile with that member removed |
| `necessity_delta` | does the blend need it? | full ensemble − `blend_without`; **> 0 means the blend is worse without it** |
| `pct_of_run` | what does it cost the operator? | mean over 21 archived runs' own `timings_seconds` |
| `pct_of_ranking` | what is on the table if you drop it? | the same, over the timed rankers only |
| `edge_per_pct` | efficiency | `(advantage − 50) / pct_of_ranking`, clipped at 0 |

---

## 4. The scorecard

Full ensemble of six = **80.0**; chance = **50**. Cost is the **level-target** regime
(20 of the 21 archived runs); §5 has the return-target regime, which is very different.

> ⚠️ **This table is a paste of `scorecard.csv`**, not a transcription — reproduced by
> `python -m feature_selection.ranker_eval --apply` at
> `reports/ranker_evaluation/20260816-104411/`. An earlier draft computed `edge_per_pct`
> by hand from the *rounded* cost column (119.0 instead of 110.65), which is how a table
> stops matching the code that generated it.

| ranker | in default | advantage | worst cell | blend without it | necessity Δ | % of run | % of ranking | edge per % | verdict |
|---|---|---|---|---|---|---|---|---|---|
| **`permutation`** | ✅ | **85.0** | **75.0** | **56.2** | **+23.8** | 1.97 | 2.18 | 16.04 | **keep — irreplaceable** |
| **`xgb_shap`** | ✅ | 73.8 | 42.5 | 83.1 | −3.1 | 0.20 | 0.21 | **110.65** | keep — near-free |
| **`spearman`** | ✅ | 67.5 | 35.0 | 87.5 | −7.5 | **0.00** | **0.00** | *undefined* | keep — literally free |
| `xgb_gain` | ❌ | **46.2** | 25.0 | 80.0 | 0.0 | 0.20 | 0.21 | 0.00 | **remove — duplicate** |
| `mutual_info` | ❌ | **42.5** | 7.5 | 85.0 | −5.0 | 1.71 | 1.80 | 0.00 | **remove — worst and dearest** |
| `lasso` | ❌ | *withdrawn* | *withdrawn* | 80.0 | 0.0 | **91.30** | **95.59** | *n/a* | **remove — 95.6 % of ranking, 0 effect** |

### 4a. Reading the `necessity_delta` column — this is the one that decided it

**Five of the six members can be dropped with the blend staying level or improving.** Only
one carries weight:

```
                       blend    necessity_delta
                     without   (full - without)
ensemble -spearman      87.5              -7.5   blend is BETTER without it
ensemble -mutual_info   85.0              -5.0   blend is BETTER without it
ensemble -xgb_shap      83.1              -3.1   blend is BETTER without it
ensemble -xgb_gain      80.0               0.0   unchanged
ensemble -lasso         80.0               0.0   unchanged - identical, not approximately
ENSEMBLE (6)            80.0                 -
ensemble -permutation   56.2             +23.8   blend is WORSE without it  <-- the only one
```

⚠️ **`permutation` carries all of it**, and it is the only member measured **out of
sample**. That is not a coincidence: the other five are fitted on the whole labelled
sample and answer *what does this data support*, not *what generalises*.

⚠️ **`permutation` alone (85.0) beats the ensemble of six (80.0).** Five in-sample voters
outvoting one out-of-sample voter made the blend worse than that voter by itself.

### 4b. ⚠️ `lasso`'s advantage is WITHDRAWN, not low — and why that matters

`lasso` collapses in **all four cells** — `zero share: lasso 1.00` on both targets — but
it collapses in **two different ways**, and telling them apart took a second correction:

| target | what LassoCV returns | `nunique()` | looks degenerate? |
|---|---|---|---|
| `return_5day` | byte-identical zeros | **1** | yes — every channel tied |
| `return_rel_5day` | values all ≤ **1e-12**, differing in their last bits | **84** | **no** |

On the first, `rank(method="min")` gives every channel the same rank and `sort_values()`
returns them in **pool column order** — its "top-10" is *the first ten columns of the
pool*. Scored anyway it produced the **92.5th percentile in one cell and the 2.5th in
another**; both are facts about column order.

⚠️ **The second is the more dangerous, because it does not look broken.** Ranking values
that are all below 1e-12 is sorting floating-point noise, and it produced a perfectly
respectable-looking **81.25th percentile**. The first version of the withdrawal rule tested
`nunique() <= 1`, caught the first case and missed this one entirely.

`ranker_eval` now withdraws a method whose raw scores are constant **or** all ≤ `ZERO_TOL`
(1e-12), and `test_ranker_eval.py` pins both halves.

> ⚠️ **So `lasso` is removed on COST and INERTNESS, never on skill.** Nothing here measured
> whether a cross-validated LASSO ranks well. On a level target it does rank — and charges
> 95.6 % of the ranking phase to do it.

**Inertness is exact, not approximate**: because a constant column adds a constant to a
mean, and a constant does not change an order, `ensemble -lasso` is **bit-identical** to
`ENSEMBLE (6)` in all four cells — same IC, same fold sd, same trend. It has been an
ensemble member that cannot vote.

### 4c. ⚠️ `xgb_gain` is not a second opinion, it is the same opinion

`xgb_gain` and `xgb_shap` are computed **from one fit of one booster**. Across the 21
archived runs they correlate at **ρ = 0.864**, against 0.15–0.50 for every other pair:

| | spearman | mutual_info | xgb_gain | xgb_shap | lasso | permutation |
|---|---|---|---|---|---|---|
| spearman | 1.000 | 0.289 | 0.198 | 0.150 | −0.068 | −0.160 |
| mutual_info | 0.289 | 1.000 | 0.480 | 0.499 | 0.049 | 0.306 |
| xgb_gain | 0.198 | 0.480 | 1.000 | **0.864** | 0.111 | 0.225 |
| xgb_shap | 0.150 | 0.499 | **0.864** | 1.000 | 0.156 | 0.333 |
| lasso | −0.068 | 0.049 | 0.111 | 0.156 | 1.000 | 0.135 |
| permutation | −0.160 | 0.306 | 0.225 | 0.333 | 0.135 | 1.000 |

So the blend gave **one model 2 of 6 votes**. §4 of [CONTEXT.md](CONTEXT.md) already said
gain "splits credit arbitrarily among correlated features" where SHAP does so "less
arbitrarily" — the module's own documentation said which of the two was worse, and both
were kept anyway.

⚠️ **Dropping `xgb_gain` saves no time** — the fit is shared. This one is about the
*weighting*, not the wall clock.

### 4d. `spearman` is the weakest survivor, and it stays

67.5 mean with a worst cell of 35.0 is the least stable of the three kept. It stays for two
reasons, neither of which is its score:

1. ⚠️ **It is free.** `target_corr` is computed by `run()` regardless, because the signed
   correlation goes in every report — a ranking without a sign cannot be read as a
   strategy. Its **marginal** cost is exactly 0.0 %, which is why `edge_per_pct` is
   *undefined* rather than infinite.
2. **It is the only model-free member left.** Without it the ensemble is one XGBoost fit
   looked at twice — once in sample (`xgb_shap`) and once out (`permutation`).

---

## 5. ⚠️ Cost is target-dependent, and by a factor of 9

The mean shares above are the level-target regime. The two regimes barely resemble each
other:

| ranker | % of ranking, **level** target (20 runs) | % of ranking, **return** target |
|---|---|---|
| `lasso` | **95.6** | 11.3 |
| `permutation` | 2.2 | **38.0** |
| `mutual_info` | 1.8 | **36.5** |
| `xgb_gain` + `xgb_shap` | 0.4 | 14.2 |
| `spearman` | 0.0 | 0.0 |

⚠️ **This is the whole of the 13.7× target-cost gap recorded in CLAUDE.md §15c-target.**
That note said the cost model "has no term for the target"; the truth is that **the target
was never the term — `lasso` was**. A price LEVEL keeps the coordinate descent working; a
RETURN collapses it to zero coefficients at once, so the same 357-channel panel took
2,016 s on one and 146 s on the other.

⚠️ **And once `lasso` is gone, `mutual_info` is the expensive one** — 36.5 % of the
ranking phase on a return target, and 9.0–9.3 s of ~19.4 s on the two probe runs here. §16
of [CONTEXT.md](CONTEXT.md) had already measured that its GPU path is **4–8× SLOWER** than
sklearn's KDTree, and kept it because `device` must mean the device. So the worst-ranking
member was also, after lasso, the dearest.

---

## 6. The A/B — same data, same seed, one flag apart

`python -m feature_selection.run --pools pool__basic --target close_adjust_5day
--null-draws 0`, once with `--methods all` and once with the new default. VCB, 99
channels, `d=20 h=5`, cuda:

| | ranking phase | whole run | `lasso` share | channels kept |
|---|---|---|---|---|
| six rankers | **376.3 s** | **411.8 s** | **355.8 s = 87.4 %** | 57 |
| three (default) | **10.2 s** | **44.8 s** | — | 57 |
| | **36.9× faster** | **9.2× faster** | | |

⚠️ **87.4 % on this run against 87.2 % as the archive mean — the same number twice, from
two independent measurements.** Both kept 57 channels; their shortlists share 16 (Jaccard
0.70) and their `ic_mean` differ by 0.011. The selection is **not** the same — it cannot
be, the blend changed — but it is the same size and mostly the same channels, at a ninth
of the cost.

---

## 7. What was tested and REJECTED — mRMR

Every one of the six scores a channel **in isolation**; redundancy is handled afterwards by
a hard |ρ| ≥ 0.9 prune. **mRMR** prices it at ranking time and would have needed **no new
computation** — relevance is the `spearman` column, redundancy is the channel correlation
matrix the prune already builds. It was the one genuinely new *kind* of member available
for free.

| variant | ret k10 | ret k20 | rel k10 | rel k20 |
|---|---|---|---|---|
| `+mrmr(shap)` | 50.0 | 52.5 | **100** | **100** |
| `+mrmr(ensemble)` | 85.0 | 52.5 | 95.0 | 97.5 |
| `+mrmr(spearman)` | 27.5 | 52.5 | 35.0 | 97.5 |
| `ensemble+mrmr` | 77.5 | 85.0 | 87.5 | 47.5 |

⚠️ **It did not replicate, and on one target it looked like the best thing in the table.**
`+mrmr(shap)` is 100/100 on `return_rel_5day` and 50.0/52.5 on `return_5day`. Adding it on
the strength of the first target alone would have shipped noise as a feature — which is
what the second target is for, and is the same lesson §6b teaches about a positive IC.

**Not added.** Recorded as a measured negative so the next person does not re-derive it.

---

## 8. ⚠️ What this comparison does NOT establish

1. **Not that any of them works.** The control is random-k on a pool that fails its own
   label null. See §3b.
2. **Not a ranking of the survivors.** 21 selectors × 2 k × 2 targets = **84 tests**, so
   ~4.2 false 95th-percentile passes are expected and 3–5 were seen. The IC span across
   selectors is **5–6 × the fold SE**, so the *ends* of the table are separable and
   **adjacent rows are not**. `shap+perm` (91.2) topping the table is not evidence that it
   beats the chosen default (90.6).
3. **One pool, one ticker, one window.** 84 channels of `pool__basic`, VCB, `d=20 h=5`. A
   1,458-channel macro pool may behave differently — and `mutual_info` is exactly the
   member whose case ("catches dependence a rank correlation misses") would be strongest
   there.
4. **Two of the three removals are STRUCTURAL, not statistical**, which is why they are
   acted on at all: `lasso`'s constant column and `xgb_gain`'s ρ = 0.864 from the same fit
   are facts about the code, not 5-fold estimates. `mutual_info` is the one removal resting
   on the statistics, and it rests on being **below chance in the mean and worst in the
   minimum on both targets** while costing the most of what remained.
5. **The removals are DEFAULTS, not verdicts.** Nothing was deleted.

---

## 9. Reproducing it

```powershell
# the cost half — archived timings only, no GPU, no database, 0.1 s
python -m feature_selection.ranker_eval --cost-only

# the plan, touching nothing
python -m feature_selection.ranker_eval

# the full measurement, ~8 minutes per target on an RTX 3050
python -m feature_selection.ranker_eval --apply --targets return_5day,return_rel_5day

# reproduce a pre-2026-08-16 selection exactly
python -m feature_selection.run --pools pool__basic --methods all
```

Output goes to **`reports/ranker_evaluation/<stamp>/`** — `advantage.csv`,
`runtime_shares.csv`, `cost.csv`, `scorecard.csv`, `evaluation.json`.

⚠️ **NOT into `reports/feature_selection/`, and the folder holds `evaluation.json` rather
than a `metadata.json`.** `contract.run_folders` calls any directory containing a
`metadata.json` a selection run, so writing there would make this evaluation appear in
`final_features.plan_from_reports` as a run whose channels belong in a `__final__` table.

⚠️ **Nothing here writes to the database**, and that is the package boundary, not an
oversight — `feature_selection` produces result objects and figures; `final_features` is
the only stage that writes tables (CLAUDE.md §8).

---

## 10. ⚠️ Two errors this document exists because of

Both were published on 2026-08-16 and corrected the same day. They are recorded here rather
than quietly fixed, because both are reasons the measurement now lives in
[ranker_eval.py](ranker_eval.py) with tests instead of in a scratchpad script.

1. **The `mean` column averaged its own `min`.** `min` was written into the frame before
   `mean(axis=1)` ran, so every published mean was an average over the four measured cells
   *and its own minimum* — biased low by 2–12 points on every row. The ordering and all
   four conclusions survived; the numbers did not. Pinned by
   `test_mean_is_over_the_cells_and_never_includes_its_own_min`.
2. **A degenerate ranking was scored as if it were a ranking.** `lasso` was reported "at
   chance (52.0)" when the quantity was not a measurement at all (§4b). Pinned by
   `test_a_constant_score_column_is_withdrawn_not_scored`.
3. **The fix for (2) was itself too narrow.** Testing `nunique() <= 1` caught the target
   where LassoCV returns identical zeros and missed the one where it returns values all
   ≤ 1e-12 that differ in their last bits — which scored the 81.25th percentile for
   sorting noise. Found only because the shipped module was re-run to check that this
   document reproduces from it, which is the argument for doing that at all. Pinned by
   `test_all_but_zero_scores_are_withdrawn_too_not_only_identical_ones`.

⚠️ **A number that decides which code ships is not a number to compute in a scratchpad.**
