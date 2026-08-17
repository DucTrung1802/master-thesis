# TODO — the one backlog

> Consolidated 2026-08-17 from this file and the retired `src/orchestration/todo.md`
> (28 numbered items, deleted in the same commit — `git show HEAD~1:src/orchestration/todo.md`
> brings it back). **Everything here carries a measured cost or a measured symptom.**
>
> ### The four root registers — one job each, no overlap
>
> | file | answers | when you touch it |
> |---|---|---|
> | **[CLAUDE.md](CLAUDE.md)** | *what is this, and what has it PROVED?* | auto-loaded every session; the map and the verdict |
> | **[RUNBOOK.md](RUNBOOK.md)** | *how do I RUN it?* | commands, stage order, the flags that destroy things |
> | **[ISSUES.md](ISSUES.md)** | *what is BROKEN?* | permanent codes; a code is never renumbered or reused |
> | **[TODO.md](TODO.md)** | *what is NEXT?* | priority-ordered, every item with a measured cost |
>
> Movement between them is one-way and worth knowing: a TODO item that turns out to be a
> defect **graduates to ISSUES.md with a code**; an ISSUES entry that gets fixed keeps its
> row and is struck through; a TODO item that gets done leaves its measurement in CLAUDE.md
> or a `CONTEXT.md` and is **deleted, not ticked**. Anything a future session must not
> rediscover belongs in CLAUDE.md, not here.
>
> ⚠️ Costs are measured on this machine (RTX 3050 4 GB, 15.6 GB RAM), three rankers, GPU
> — not the stale fitted models in `feature_selection/CONTEXT.md` §15c, which **P1-1** is
> about.

**Priority rule used below:** a thing that makes a number you ALREADY HAVE wrong outranks
a thing that would give you a new number; a thing that unblocks hours of other work
outranks a thing that is only itself; structural code comes last because it only pays off
for runs that are currently blocked anyway.

---

## P0 — a number you already have is wrong or unreadable until this is done

### P0-1 · Settle the layer-2 "clear" ⏱ ~2 h (or ~20 min)

The 2026-08-17 layer-2 run on `return_5day` reports `ic_mean +0.1369` against a p95 bar
of `+0.0428`, `z = +4.48`, recorded as **`cleared_p95_not_a_pass`**. A `__final__` table
and a dataset are **already built on it**. Four measured reasons not to believe it:

1. `p = 0.0909` **is the floor** — with 10 draws the minimum is `1/11`.
2. ⚠️ **The null does not price in layer 1.** The 208 candidates were chosen using this
   same label; the null shuffles the label and re-runs **layer 2 only**. This is how six
   pools that each failed produce a union at 2.4× the best individual IC.
3. Fold ICs `+0.125 / −0.017 / +0.142 / +0.127 / +0.306`, trend `+0.0507` — rule 23's
   data-arrival signature on a pool whose news channels are all-NULL before 2013.
4. 9 of 66 channels are constant across the train slice.

- **Two-layer null** — re-run layer 1 AND layer 2 inside each shuffled draw. ~12.5 min ×
  10 draws ≈ **2 h**. Decisive on objection 2.
- **or Holdout** — score the 66 channels on a range neither layer saw, with a
  shuffled-label control beside it (§5 rule 4). **~20 min**, weaker but cheap.

⚠️ **Prediction, recorded now so it cannot be revised afterwards: this will not clear.**

### P0-2 · Ship rule 21's `hit_rate` withdrawal in `feature_selection` ⏱ ~1 h + test

`selector.py:1006` and `:1102` still compute a bare `np.mean(np.sign(pred) == np.sign(y))`.
CLAUDE.md §5 rule 21 has claimed since 2026-08-14 that this is withdrawn to `NaN`. It is
not — **every archived selection README on a level target prints `hit_rate +1.0000`**.
Shipped in `result_evaluator` on 2026-08-16; the selection stage never got it.

### P0-3 · Verify `float32` reproduces `float64` ⏱ ~30 min

⚠️ `--design-dtype float32` shipped 2026-08-16 **UNVERIFIED** — the agreement probe was
killed mid-read and produced no output. It is `MEM-1`'s only mitigation, and the claim
"the precision loss is nominal because every ranker is rank-based or XGBoost" is an
argument, not a measurement. Run a small cross-sectional selection at both dtypes;
compare kept sets and `ic_mean`.

### P0-4 · Drop or exclude `mkt_n_names` ⏱ ~5 min

It rises 380 → 771 across the sample because tickers listed and because silver holds no
delisted name. A tree splitting on it is **reading the calendar** — `close_adjust`'s trap
wearing a new name. It is in `pool__market_breadth` today.

---

## P1 — unblocks hours of other work

### P1-1 · Re-fit the cost model into ONE function ⏱ ~2 h

Two models exist, disagree, and were both fitted with `lasso` — dropped 2026-08-16:

| model | predicted the 644-ch / 10-draw run at | actual |
|---|---|---|
| Dagster guard `1.1 × (ch/113)² × (1+draws)` | **393 min** | **29.7 min** |
| `CONTEXT` §15c `0.364 × ch^0.77` | ~53 min/pass | ~3 min/pass |

Needs a **draw coefficient** (draws skip `stability` and the holdout, so `(1 + draws)` is
wrong) and a **raggedness term** — exponent ~0.83 fits the well-behaved runs while the
1,406-channel `usa` run sits **6× off**, likely rule 23's all-NaN slices rather than width.

⚠️ **The guard's premise is falsified**: CLAUDE.md says `usa` is "7.2 h with no null"; it
ran **35 min 12 s**. Rewrite the raise message with the measured number.

**Payoff:** a 20-draw null on each of the 19 country pools becomes **~2-3 hours**, not the
~1,000 CPU-hours `EVD-1` is scoped at. This is what makes EVD-1 closable.

### P1-2 · Fix `PNL-2` ⏱ half a day

Cheapest fix in the issue register. Derive `cross` from the panel's own `ticker` count, as
resolved `PNL-1` already made the SCORER do. No chicken-and-egg: the read happens before
`build()`.

⚠️ **It partly dissolves `CSP-1` for free** — once grain comes from the data, the `else`
branch reads via `reader.join(pools)`, so `--ticker ALL --pools pool__basic,pool__X
--target return_5day` becomes a real cross-sectional multi-pool run. `daily_ic` is
Spearman per date, so ranking `return_5day` within a date is the same metric as
`cs_rank_5day`; only the ranker *fit* differs.

---

## P2 — new measurements worth having

### P2-1 · ⚠️ Run the chain at a 4-WEEK horizon ⏱ ~2 h

**The highest-value untried experiment in the repo, and it was buried in a todo file.**
CLAUDE.md §2a-bis: the `controls` block loses to its benchmark at `rel5`/`rel10` and beats
it by **30.39% vs 18.07% CAGR at 4 weeks (Sharpe 1.10)**, positive in 30 of 30 folds. Four
independent threads have failed at `h=5`; **nothing has ever been run end to end at
`h≈20`.** Needs `pool__targets` to carry a 20-session horizon (`UNIFIED_TARGET_HORIZONS`).

### P2-2 · `cs_rank_5day` on the top ~300 by turnover ⏱ ~1 h

Fits today with **none of C fixed** — `read_universe_panel` already takes a `tickers` list
and filters in SQL, so this is a CLI flag, not a new schema. ~1.3 M rows. Puts a number
against §2b's `ALL` row, which reads **"never ran — ⚠️ unverified"** at IC +0.109.
⚠️ Today's measurement says liquidity is the variable: the 5-day cross-sectional reversal
runs `t = −18.60` over all names, `−10.43` at top 300, **`−1.96` at top 100**.

### P2-3 · Train the LSTM on the `return_5day` dataset ⏱ ~5 min

`vcb__return_5day__final__d20_h5__tr70_val15_test15__std`, 2,921 / 611 / 636 × 20 × 57.
**Do P0-1 first** — a model trained on a shortlist whose bar is in question inherits the
question.

---

## P3 — structural code, only pays off for runs currently blocked

| item | what | note |
|---|---|---|
| **P3-1** | `CSP-1` — give `read_universe_panel` the `UnifiedSchemaReader.join()` the single-ticker path uses | ⚠️ makes `MEM-1` worse by the width joined; `pool__ta` at 922 channels is ~10× the design |
| **P3-2** | `MEM-1` — stop materialising the whole design; window per fold or per ticker-chunk, never holding the blocks and the `pd.concat` result at once | 4.03 GB per million rows, measured |
| **P3-3** | `_ingest_gold_stocks` still carries a legacy column (`close`/`volume` check: 1 of 2 present) | ⚠️ Fixing it redefines `gold.stocks`' OHLC as adjusted and commits to a ~2.4 M × ~900 col rebuild. Its own decision, not a side effect. Related to `STA-1` |

---

## P4 — hygiene, each item distorts one number or hides one failure

| item | what | status |
|---|---|---|
| **P4-1** | **`STA-1` costs the chain its last 31 sessions** — `pool__ta` stops 2026-06-26, and the INNER join drops the whole chain 4,266 → **4,235 rows**. The `return_5day` table and dataset end 2026-06-25 | measured 2026-08-17 |
| **P4-2** | Confirm `validation.csv` emits **`n_dead_train` / `n_dead_test`** and read them for `pool__news_daily` — rule 23, its channels are entirely NULL before 2013 | the 2026-08-17 layer-2 `validation.csv` showed no such column |
| **P4-3** | **262 rows in `bronze.cafef_price` have `high < low`** (e.g. ACB 2018-07-31: high 35,800 low 36,500). CafeF's defect, surfaces in gold as a negative `range_hl`. Needs a bronze data-quality screen, not a gold patch | ⚠️ **re-verified 2026-08-17, still 262.** Probably deserves an ISSUES.md code |
| **P4-4** | XGBoost warns in **every** run: *"Falling back to prediction using DMatrix due to mismatched devices — running on cuda:0, input data on cpu"* | if the design is copied host→device per prediction, the GPU conversion is leaving speed on the table |
| **P4-5** | `landed()` cannot answer "did THIS run produce anything" — it rglobs a folder where the previous run's dated files still sit. 140 header-only CSVs went green (2026-07-31) | this is §5 rule 10's mechanism; fix is to compare against the run's own outputs |
| **P4-6** | `logs/app.log` has many writers now — the executor is multiprocess and every step appends, so records interleave | fix is per-process filenames in `Logger`, **not** going back to sequential |
| **P4-7** | `raw/trading_view` partitions `crypto` and `options` are permanently red — both `true` in config, folders never existed, `landed(require=True)` fails them | choose `require=False` or accept two red partitions |
| **P4-8** | Decide the fate of `raw/trading_view_collected_links` — nothing reads it | it is a leaf, not a hub |
| **P4-9** | ⚠️ If ever backfilling TradingView, use a **single-run backfill**. `tag_concurrency_limits` is per-RUN, so 9 partitions the default way is 9 runs × 8 browsers = **72 Chrome** | `.dagster/dagster.yaml` is empty |
| **P4-10** | Four heavy assets have never been observed running end to end through Dagster: `trading_view_links`/`data`, the 5 CafeF stock tabs + news, `cafef_pdfs` (100 partitions), `cafef_financials` (~2.4 h each) | *"built is not run"* |

---

## Closed — recorded so they are not reopened

| what | why closed |
|---|---|
| **News sentiment scorer** (old items 7-16: annotation, LLM labelling, PhoBERT fine-tune, LIME gate, full panel) | ⛔ **Decided against 2026-08-03 and confirmed 2026-08-17.** 7 paired tests, every \|t\| < 1.3; adding news costs 2-8 pp CAGR for ΔMCC ±0.003. The one reason to continue — coverage — was tested on the top-30 most-covered tickers and did not survive. The event-count half is now `pool__news_daily` and it measured `z = +0.53` at layer 1 |
| **Silver leaf assets** (old item 17: bonds, forex, funds, indices, gics) | ✅ all five exist |
| **Gold leaf assets** (old item 18: bonds, forex, funds) | ✅ all three exist |
| **`switch_config.json` cleanup** (old items 22, 23) | ✅ moot — the file is gone (§5a); a leftover copy now RAISES |
| **`execution.finished_at = None`** in every `metadata.json` | ✅ **working as designed** ([runtime.py:329](src/utils/runtime.py#L329)) — `summary()` is called mid-run because `write_report` writes the file, and waiting for `stop()` would record a runtime of zero. `None` "rather than a guess" is §5 rule 2 at the clock. I called it a bug on 2026-08-16 and was wrong |
