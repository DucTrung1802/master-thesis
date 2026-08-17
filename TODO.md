# TODO — what is proposed but not done

> Opened 2026-08-17. Everything here was **proposed with a measured cost** during the
> 2026-08-16/17 session, and none of it has been run.
>
> Relationship to the other registers: [ISSUES.md](ISSUES.md) is what is **broken**,
> [RUNBOOK.md](RUNBOOK.md) is how to **run** things, and this is what is **next**. An
> item that turns out to be a defect graduates to ISSUES.md with a code; an item that
> gets done leaves a measurement behind and is deleted from here, not ticked.
>
> ⚠️ Costs below are measured on this machine (RTX 3050 4 GB, 15.6 GB RAM), three
> rankers, GPU. They are not the stale fitted models in `feature_selection/CONTEXT.md`
> §15c — see **D1**, which is about exactly that.

---

## A. Settle the layer-2 "clear" — the one open scientific question

The 2026-08-17 layer-2 run on `return_5day` reports `ic_mean +0.1369` against a p95 bar
of `+0.0428`, `z = +4.48`, and is recorded as **`cleared_p95_not_a_pass`**. Four
measured reasons not to believe it (full detail in commit `8c0dbb97`):

1. `p = 0.0909` **is the floor** — with 10 draws the minimum is `1/11`.
2. ⚠️ **The null does not price in layer 1.** The 208 candidates were chosen using this
   same label; the null shuffles the label and re-runs **layer 2 only**.
3. Fold ICs `+0.125 / −0.017 / +0.142 / +0.127 / +0.306`, trend `+0.0507` — rule 23's
   data-arrival signature on a pool whose news channels are all-NULL before 2013.
4. 9 of 66 channels are constant across the train slice.

| # | task | settles | cost |
|---|---|---|---|
| **A1** | **Two-layer null** — re-run layer 1 AND layer 2 inside each shuffled draw | objection 2, decisively | **~2 h** (layer 1 with no null ≈ 11 min for all six + layer 2 ≈ 1.5 min → ~12.5 min × 10 draws) |
| **A2** | **Holdout** — score the 66 channels on a date range neither layer ever saw, with a shuffled-label control beside it (§5 rule 4) | objections 2 and 3, more cheaply | ~20 min |
| **A3** | **Train the LSTM** on `vcb__return_5day__final__d20_h5__tr70_val15_test15__std` (2,921 / 611 / 636 × 20 × 57) | whether anything survives downstream | ~5 min |

**Do A1 or A2 before A3.** A model trained on a shortlist whose bar is in question
inherits the question; the earlier bar is the cheaper one to fix.

⚠️ **Prediction, recorded now so it cannot be revised later**: A1 will not clear. Six
pools that each failed their own null producing a union at 2.4× the best individual IC
is the signature of selection fitting noise across the union, not of a signal that only
appears in combination.

---

## B. Ship what is documented but not coded

| # | task | why it matters | cost |
|---|---|---|---|
| **B1** | **Withdraw `hit_rate` on a single-signed target in `feature_selection`** — `selector.py:1006` and `:1102` still compute a bare `np.mean(np.sign(pred) == np.sign(y))` | CLAUDE.md §5 rule 21 has claimed since 2026-08-14 that this is `NaN` → `—`. It is not. Every archived selection README on a level target prints **`hit_rate +1.0000`**. Shipped in `result_evaluator` on 2026-08-16; the selection stage never got it | ~1 h + a test |
| **B2** | **Verify `float32` reproduces `float64`** — run a small cross-sectional selection at both dtypes and compare kept sets and `ic_mean` | ⚠️ `--design-dtype float32` shipped 2026-08-16 **UNVERIFIED**. The agreement probe was killed mid-read and produced no output, so "the precision loss is nominal because every ranker is rank-based or XGBoost" is an argument, not a measurement. It is `MEM-1`'s only mitigation | ~30 min |

---

## C. The three issues opened 2026-08-16

Full text in [ISSUES.md](ISSUES.md). They block every wide-universe run and are listed
here in fix order, cheapest first.

| # | issue | fix | cost |
|---|---|---|---|
| **C1** | **`PNL-2`** — selection picks series-vs-panel from the target's NAME | Derive `cross` from the panel's own `ticker` count, as resolved `PNL-1` already made the SCORER do. Reader choice stays name-based (only `cs_rank_*` needs deriving), selector choice becomes data-based. No chicken-and-egg: the read happens before `build()` | **half a day** — cheapest in the register |
| **C2** | **`CSP-1`** — a cross-sectional run can read exactly one pool | Give `read_universe_panel` the `UnifiedSchemaReader.join()` the single-ticker path already uses. ⚠️ Makes `MEM-1` worse by the width joined — `pool__ta` at 922 channels is ~10× the design | medium |
| **C3** | **`MEM-1`** — the windowed design is built dense and whole (4.03 GB per million rows, measured) | Stop materialising it: window per fold or per ticker-chunk, never holding the blocks and the `pd.concat` result at once | structural |

⚠️ **C1 partly dissolves C2 for free.** Once grain comes from the data, the `else`
branch reads via `reader.join(pools)` — so `--ticker ALL --pools pool__basic,pool__X
--target return_5day` becomes a real cross-sectional multi-pool run. And `daily_ic` is
Spearman per date, so ranking `return_5day` within a date is the same metric as
`cs_rank_5day`; only the ranker *fit* differs.

---

## D. The cost model is wrong and it is blocking a bigger thing

| # | task | cost |
|---|---|---|
| **D1** | **Re-fit both cost models and collapse them into one function** | ~2 h |

Two models exist and disagree; both were fitted with `lasso` in the ensemble, which was
dropped 2026-08-16:

| model | predicted the 644-channel / 10-draw run at | actual |
|---|---|---|
| Dagster guard `1.1 × (ch/113)² × (1+draws)` | **393 min** | **29.7 min** |
| `CONTEXT` §15c `0.364 × ch^0.77` | ~53 min/pass | ~3 min/pass |

Needed: one shared function; a **draw coefficient** (draws skip `stability` and the
holdout, so `(1 + draws)` is wrong); and a **raggedness term** — an exponent of ~0.83
fits the well-behaved runs while the 1,406-channel `usa` run sits **6× off** it, likely
rule 23's all-NaN train slices rather than width.

⚠️ **The premise of the `budget_minutes` guard is falsified.** CLAUDE.md says `usa` is
"7.2 h with no null"; it ran **35 min 12 s** on 2026-08-16. Rewrite the raise message
with it.

**The payoff:** at today's measured rates a 20-draw null on each of the 19 country pools
is **~2–3 hours**, not the ~1,000 CPU-hours `EVD-1` is scoped at. Fixing D1 is what
makes EVD-1 closable rather than perpetual.

---

## E. The wide-universe measurement nobody has

| # | task | cost |
|---|---|---|
| **E1** | `cs_rank_5day` on the **top ~300 by turnover** — `read_universe_panel` already takes a `tickers` list and filters in SQL, so this needs a CLI flag, not a new schema. ~1.3 M rows fits today with none of C1–C3 fixed | ~1 h |

It would put a real number against §2b's `ALL` row, which currently reads
**"never ran — ⚠️ unverified"** at IC +0.109. ⚠️ And today's measurement says the
liquidity tier is the variable worth testing: the cross-sectional 5-day reversal runs
`t = −18.60` over all names, `−10.43` at top 300, and **`−1.96` at top 100** — it lives
in the illiquid tail, exactly where costs are worst.

---

## F. Hygiene found in passing — small, and each one distorts a number

| # | task | why |
|---|---|---|
| **F1** | Exclude **`mkt_n_names`** before ranking, or drop it from `pool__market_breadth` | It rises 380 → 771 across the sample because tickers listed and because silver holds no delisted name. A tree splitting on it is reading the calendar — `close_adjust`'s trap in a new column |
| **F2** | Confirm `validation.csv` emits **`n_dead_train` / `n_dead_test`** per fold, and read them for `pool__news_daily` | Rule 23. The news channels are entirely NULL before 2013, so fold 1's train slice is imputed to a constant `0.0` and then ranked. The 2026-08-17 layer-2 `validation.csv` showed no such column |
| **F3** | Decide what to do about **`STA-1` costing the chain 31 sessions** | `pool__ta` stops 2026-06-26, and the INNER join drops the whole chain from 4,266 to **4,235 rows** — the `return_5day` table and dataset both end 2026-06-25 instead of 2026-08-07 |
| **F4** | Investigate the XGBoost warning in every run: *"Falling back to prediction using DMatrix due to mismatched devices — XGBoost is running on cuda:0, while the input data is on cpu"* | It appeared in all six layer-1 runs. If the design is being copied host→device per prediction, the GPU conversion is leaving speed on the table |

---

## Closed without action

- **`execution.finished_at = None`** in every run's `metadata.json` — investigated
  2026-08-16 and it is **working as designed** ([runtime.py:329](src/utils/runtime.py#L329)):
  `summary()` is called mid-run because `write_report` is what writes `metadata.json`,
  and waiting for `stop()` would record a runtime of zero. `finished_at` is `None`
  "rather than a guess" — §5 rule 2 at the clock. `runtime_seconds` is the measurement;
  the folder mtime is the true end time if ever needed.
