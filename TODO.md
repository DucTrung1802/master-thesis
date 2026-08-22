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
> — not the stale fitted models in `feature_selection/CONTEXT.md` §15c, which **`P19`** is
> about. **A cost marked `est.` is an estimate**, and it says which measured run it is
> anchored to.
>
> ### ⚠️ ONE CODE SCHEME, SINCE 2026-08-21: `P3` is the highest priority, then `P4`, `P3` …
>
> | you see | it is |
> |---|---|
> | **`P7`** — bare, no hyphen | a **LIVE** item, position 7 in the one list |
> | **`P1-9`**, `PRF-8`, `M-3`, `SSK-1` — **hyphenated** | a **RETIRED** code — see the crosswalk |
>
> **A hyphen means retired.** Five schemes were in use before this and an item's code told
> you which list somebody had been writing at the time, not what to do next. ⚠️ **Retired
> codes were NOT rewritten anywhere else** — they are cited ~150 times across CLAUDE.md,
> RUNBOOK.md, ISSUES.md, four `CONTEXT.md` files, source comments and dozens of immutable
> archived run READMEs. **The crosswalk is the bridge.**

**Priority rule used below:** a thing that makes a number you ALREADY HAVE wrong outranks
a thing that would give you a new number; a thing that unblocks hours of other work
outranks a thing that is only itself; structural code comes last because it only pays off
for runs that are currently blocked anyway.

⚠️ **THAT RULE NOW ORDERS ONE LIST INSTEAD OF SORTING ITEMS INTO BANDS.** The five bands
(`P2`…`P4`) and the two parallel tracks (`PROFIT`/`PRF-*`, the model program `M-*`) are
retired as ORGANISING DEVICES — they are still below as history, because their reasoning
is the evidence, but every open item they held is in the one list. ⚠️ **The `PROFIT`
section was ordered by a DIFFERENT rule** — by what would change a decision about trading
rather than by what makes an existing number wrong — and merging it means that rule no
longer competes with this one. Its *"what is already measured, so nobody re-measures it"*
table survives and is still the best thing in it.

⚠️ **RE-ORDERED 2026-08-19, and TWO THINGS ABOUT THE SHAPE OF THE FILE CHANGED WITH IT.**

1. **Every `✅ DONE` block moved to [§ Archive](#archive--done-kept-because-the-reasoning-is-the-evidence) at the bottom.** The convention
   says a done item is *deleted*, not ticked, once its measurement lives in `CLAUDE.md` or
   a `CONTEXT.md` — and all of them now do (`backtest/CONTEXT.md` §8h, `walkforward/CONTEXT.md`,
   CLAUDE.md §2b-bis, §6-0). They are demoted rather than deleted **only** because the
   reasoning around the numbers — the two recorded predictions that turned out wrong, the
   three failed T4 attempts — is itself the evidence, and deleting it is a separate call.
   **Nothing above the Archive is finished work.** ⚠️ **20 of this file's 32 blocks moved**
   — all six P0s, four of the eight P1s, six of the eight P2s, and three of the ten PRFs
   closed on 2026-08-19 alone.
2. **A BLOCKED item no longer outranks a cheap unblocked one.** The rule below already
   said so ("a thing that unblocks hours of other work outranks a thing that is only
   itself"), but `PRF-9` — 90 → 800 channels, the largest upside left — sat first while
   being blocked by `P1-2` *and* by `MEM-1`. It is now third, behind the two items that
   can be run today, and `P1-2` was promoted to the head of P3 because it is what unblocks
   it.

---

## ⚠️ START HERE — ONE CODE, ONE ORDER. `P1` is the highest priority.

> **Renumbered 2026-08-21.** This file used to carry **five** code schemes at once —
> `P0-*`/`P1-*`/`P2-*`/`P3-*`/`P4-*` (priority BANDS with sub-numbers), `PRF-*` (the profit
> track), `SSK-*` (the single-stock track) and `M-*` (the model program). An item's code
> told you which list somebody had been writing at the time and nothing about what to do
> next. **There is now one list**: `P1` … `P31`, strictly in priority order, `P1` first.
>
> ### ⚠️ THE ONE RULE THAT KEEPS AN OLD MESSAGE READABLE
>
> | you see | it is | where it lives |
> |---|---|---|
> | **`P7`** — bare, no hyphen | a **LIVE** item, position 7 in the list below | this file, §the list |
> | **`P1-9`**, `P4-12`, `PRF-8`, `SSK-1` — **hyphenated** | a **RETIRED** code | the crosswalk below, or the Archive |
>
> **A hyphen means the code is retired.** The two cannot be confused by a reader or by
> `grep`, which is the whole reason the new codes have no hyphen — `P1` and `P1-1` are
> different strings and always were.
>
> ⚠️ **THE RETIRED CODES ARE NOT REWRITTEN AND THAT IS DELIBERATE.** They are cited about
> **150 times** outside this file — 65 in CLAUDE.md, 37 in `walkforward/CONTEXT.md`, 26 in
> RUNBOOK.md, 17 in ISSUES.md, plus source comments (`backtest/portfolio.py` says *"PRF-0"*
> to explain a line) and **dozens of archived run READMEs, which are immutable artefacts**.
> Renaming them would break an evidence trail to buy tidiness. **The crosswalk below is the
> bridge**; nothing else moved.
>
> ⚠️ **A DONE ITEM KEEPS ITS OLD CODE FOREVER.** Everything under `## Archive` and every
> `✅ DONE` block is historical evidence CLAUDE.md and the `CONTEXT.md` files cite **by
> code** — §6-0-b names `P4-12` as the place a wrong recorded prediction is kept. Only
> **open** items were renumbered.
>
> ⚠️ **THE FIVE OLD BANDS ARE RETIRED AS BANDS**, renamed `Retired band A … E` further
> down so that a bare `P1` can never mean *"the `P1` tier"* again. Their priority RULE
> survives and still orders the list: *a thing that makes a number you already have wrong
> outranks a thing that would give you a new number; a thing that unblocks hours of other
> work outranks a thing that is only itself; structural code comes last because it only
> pays off for runs that are currently blocked anyway.*

✅ **TWO ITEMS WERE COMPLETED ON 2026-08-21 AND THE LIST WAS RENUMBERED BEHIND THEM** —
the old `P1` (re-score the h=20 arm sweep through the fixed `compare`) and the old `P2`
(the seed floor). Their measurements live in `walkforward/CONTEXT.md` §8a-bis / §8a-ter /
**§15**, CLAUDE.md §6-0-ter / §6-0-ter-2 and RUNBOOK §8 rules 10-13; per this file's own
rule they are **deleted here, not ticked**. ⚠️ **Everything below shifted up by two** —
today's `P3` was yesterday's `P1`.

⚠️ **AND THE LIST WAS RE-PRIORITISED AGAIN THE SAME EVENING (pass c).** Documenting the
chain's OUTPUT (`pipeline.md`) measured something that changes what to do first:
**after 2026-06-11 only 7 of 150 names carry data**, so the chain **cannot emit a pick
list at all**. `FRZ-1` moved **12 → 1** and a new `P2` (the live-scoring module, which
does not exist) was inserted. ✅ The published +74 % is unaffected — `long_only_top_k`
skipped those three stub dates, giving exactly the 236 periods on the artefact.

**Three sentences from them are worth carrying forward, because they change how the tables
in this file and in `walkforward/CONTEXT.md` are read:**

1. ⚠️ **An arm gap below `|d_sharpe| ≈ 0.09` is a RESEED, not a result.** Measured on five
   `gbt` seeds over the identical h=10 folds. At h=10 that leaves only `cnn` (4.5×) and
   `gbt` (4.1×) above the floor; `transformer`, `tcn` and `bilstm` are **at or inside** it.
2. ⚠️ **A per-fold Sharpe cell is 4.4× noisier still** — mean per-fold range **0.593**
   against **0.134** pooled. **Never compare two arms in one fold.** ✅ The DECAY across
   folds survives (slope −0.308 ± 0.027 over five seeds).
3. ✅ **The h=20 architecture ties hold on BOTH estimands**, so `PRF-8`'s headline needed no
   caveat after all — ⚠️ **but `gbt`'s h=10 advantage CHANGES SIGN there** (`d_sharpe`
   +0.360 at h=10, **−0.016** at h=20), which is the strongest argument yet against
   promoting it.

---

## ⚠️ MEASURED 2026-08-22 — THE TRADABILITY GATE, AND WHAT IT DOES TO THIS LIST

**Nothing was renumbered.** This is evidence attached to items that already exist, recorded
here because it changes their ORDER OF IMPORTANCE without changing their positions. Full
measurement in `pipeline.md` §9 and §10; the operating rules are RUNBOOK §8 rules 14-16.

**What was asked:** cap the book at 5 names and re-run. **What was found:** `--top-k` already
IS that cap (no code needed), all ten stages were `up to date`, and re-scoring the published
h=10 track at k=5 gives CAGR@30 **+181.3 %** against k=20's +74.0 %, clearing a 200-draw null
at **z = +15.01**. Then three things turned that from a result into a warning:

| measured | number |
|---|---|
| median matched turnover of a PICKED row at k=5 | **0.03 bn VND/day** (universe median 2.22) |
| picks under 0.1 bn/day at k=5 / k=20 | **61.4 %** / 38.6 % |
| daily IC across h=1 … h=30 | **FLAT**, +0.1403 … +0.1328 — it should peak at its own label |
| rows with a forward 1-day return of EXACTLY zero, ADV60 < 0.1 bn | **51.2 %** |
| CAGR h=10 k=5 under ADV60 ≥ 1 bn / ≥ 5 bn | **+36.5 %** / **+19.9 %** (from +181.3 %) |
| `z` at k=5 vs k=20, gated, all 8 cells | **k=20 is higher in every one** |

✅ All eight gated cells still clear their own null (null MAX below observed), so **the signal
survives the gate — the LEVELS do not.**

### What this does to the existing items

| item | what changed |
|---|---|
| **`P12`** (execution realism — ADV cap, sell-side floor, ATC) | ⚠️ **It now has a measured price tag: roughly 145 pp of CAGR at k=5, and it is the difference between a headline and a strategy.** It was ranked on argument; it is now ranked on a number. Nothing else on this list moves a number this far |
| **`P5`** (portfolio construction — `k`, weighting, laddering) | ⚠️ **The `k` half is answered and the answer is NO.** Lowering `k` raises CAGR only by concentrating into unbuyable names; on a gated basket k=5 and k=20 earn within a point of each other while k=20 carries the stronger `z`. What is left of `P5` is **weighting and the ladder**, not `k` |
| **`P16`** (survivorship / point-in-time listing) | unchanged in rank, but the stale-price finding is adjacent: a name that stops trading is not delisted in this data, it just returns 0 forever — and 0 ranks well in a falling cross-section |
| **`P21`** (`cs_rank_5day`) | ⚠️ **Do not read the h=5 numbers above as `P21` being done.** They are the h=10 MODEL traded every 5 sessions, not a chain selected and trained on `cs_rank_5day`. Confirmed on disk: `unified_schema_all` holds only `rank_10day` / `rank_20day` finals, and the only `cs_*` selection runs are `cs_rank_20day` ×2 and `cs_rank_10day` ×1 |
| **`P2`** (live scoring) | ✅ **Cheaper than believed.** The 2026-06-11 cliff is in the LABEL, not the price — all 150 names carry a close through **2026-06-25**, so a live ranker has ~10 more usable sessions than `pipeline.md` used to claim. Ranking a book and scoring a book fail on different dates |
| **`plan.md`** (the `screen_schema` plan, `main_v4`) | ⚠️ **These numbers are the first measurement of what that screen is worth**, and they cut both ways: it removes most of the reported return, and what remains still clears a null |

### ⚠️ A PREDICTION, RECORDED BEFORE THE SCREENED CHAIN RUNS

Per this file's convention (five recorded, four wrong — which is why the rest are worth
anything). When `plan.md`'s screen is built and the chain re-selected and retrained on the
screened basket, I predict:

1. **Sharpe@30 lands near 1.0-1.4 at h=10, not near 2.5**, and CAGR near **+25 … +45 %/yr** —
   i.e. close to the post-hoc gated figures above, because re-selecting on a cleaner basket
   recovers only the part of the edge that was not stale-price ranking.
2. **Daily IC falls to ~0.06-0.09** from 0.1412.
3. **The horizon ladder flattens** — h=1 will no longer "earn" an order of magnitude more
   than h=10, because that gap is a staleness artefact.
4. ⚠️ **The screened chain will still clear its within-date null.**

If (1) comes back near +2.5 Sharpe on a genuinely liquid basket, this whole block is wrong and
the post-hoc gate was the flawed instrument — say so here rather than editing it out.

---

## THE LIST — P3 … P31, highest priority first

### P3 … P12 · the live program

⚠️ **EVERY COST IN THIS BLOCK THAT IS MARKED `est.` IS AN ESTIMATE ANCHORED TO A MEASURED
RUN**, and marked so it cannot be read back as a measurement. The anchors are `walkforward`
§12 (**~20 min** per `gbt` track), §11 (**2h 48m** for 7 arms × 10 folds; **3m 23s** to
re-score them paired), §13 (**44m 12s** for a 162-channel selection with no null) and §9
(**33m 26s** + **8m 59s** for one LSTM track with a 200-draw null).

| # | item | ⏱ | local? | was | why it is here |
|---|---|---|---|---|---|
| **P1** | ⚠️ **`FRZ-1` — RE-SCRAPE THE 143 FROZEN TICKERS**, then put the per-ticker freshness split in `pipeline.status_data` | ~1 h + the scrape | ✅ | *promoted from 12* | ⚠️ **IT BLOCKS THE CHAIN'S OUTPUT, NOT JUST A NUMBER — measured 2026-08-21.** After **2026-06-11 only 7 of 150 names carry data**, so the last three rebalance dates hold 7 names and `long_only_top_k` skips them silently (`len(day) < k`). ✅ The published +74 % is clean (239 − 3 = **236** periods, matching the artefact) — ⚠️ **but no pick list can be emitted until this is fixed**, and it also blocks `P6`, `P7` and any re-run. `MAX(date)` says 2026-08-07 and hides it completely. `pipeline.md` §6.1 |
| **P2** | ⚠️ **THE LIVE-SCORING MODULE — it does not exist** | ~½ day *est.* | ✅ | — | Every stage writes predictions for a **dataset's test split**. Nothing loads a trained fold, windows the last 20 sessions for all 150 names on today's date and emits a ranking — so the chain cannot answer *"which ticker, on which date"* for any date not already in a split. ⚠️ **Blocked behind `P1`**: a 7-name cross-section is not a cross-section. `pipeline.md` §6.2 |
| **P3** | ⭐ **rank the FEATURES within date, as the selection did** (`FNM-1`) | ~1 h + 20 min *est.* | ✅ | `M-1` | **The largest single untried lever**, and three independent measurements already on disk point at it. **↓ detail block** |
| **P4** | **ensemble the seven arms** — predictions already on disk | ~30 min *est.* | ✅ CPU | `M-3` | Free variance reduction at ρ 0.91-0.94; no GPU, no training. **↓ detail block** |
| **P5** | **portfolio construction** — `k`, weighting, a laddered book | ~4 h *est.* | ✅ CPU | `M-4` | The only place with a **−55 % drawdown** to spend and no retraining to pay for it. ⚠️ Run it WITH `P12`. **↓ detail block** |
| **P6** | ⚠️ **take the 30-name VN30 result DOWNSTREAM** | ~2 h | ✅ | — | `t = +3.77` is a **SELECTION bar**, and §5d + `P2-3`: a cleared selection bar has **never once** survived to a model here. Run `final_features → train_test_creator → model → result_evaluator` on 2-3 names that cleared individually (VJC z +3.67, SSB +2.60, PLX +2.55). ⚠️ **A negative closes the thread cleanly** — that is the value |
| **P7** | **a wider cross-section at h=10 that is NOT VN30** | ~1 h | ✅ | — | §6-1-quater cannot separate **N** from the **date window** from the **universe rule** — all three moved together. Top-150 by `liquidity_before` restricted to 2017→ isolates the window; top-30 by the same rule isolates the RULE. Reuses `ProvidedPanel`, no Kaggle quota |
| **P8** | **train the estimand** — a ranking loss instead of MSE | ~½ day *est.* | ✅ | `M-5` | The only untried change aimed at **what is actually scored**. **↓ detail block** |
| **P9** | **BUILD cross-sectional channels** — selection is nearly exhausted, construction is not | ~1 day + T4 *est.* | ⚠️ **quota** | `M-6` | The only real feature lever left, and the **only item on this page that spends Kaggle quota**. **↓ detail block** |
| **P10** | **sweep `lookback`** — the one dataset knob never swept | ~4 h *est.* | ✅ | `M-7` | `walkforward` §12c named it and nothing has moved since. **↓ detail block** |
| **P11** | **date-only pools as a REGIME OVERLAY**, not as ranking channels | ~1 day *est.* | ✅ CPU | `M-8` | Risk control, **not** a fix for the decay — §9b already located that in the features. **↓ detail block** |
| **P12** | **execution realism** — ADV cap, sell-side floor days, the ATC auction | ~1 day | ✅ CPU | `PRF-4` | Moves the LEVELS, and after the architecture and width tests the levels are the only thing still moving. **↓ detail block** |

### P13 … P31 · the backlog

| # | item | ⏱ | was | note |
|---|---|---|---|---|
| **P13** | Emit **`n_dead_train` / `n_dead_test`** from `validation.csv` | ~2 h | `P4-2` | Rule 23 explains **both** apparent clearances in §6-1 and the column still does not exist, so every ragged-pool run needs a hand computation to read at all. ⚠️ Confirmed still open 2026-08-19: the file carries `n_train`/`n_test`/`ic`/`r2`/`hit_rate` and nothing about dead channels |
| **P14** | **re-run each LEGACY walk-forward track once** | ~0 | — | ⚠️ **A limit CREATED on 2026-08-21, not discovered.** The `WFO-1` guard covers the five pre-existing tracks on their **TABLE only** — `folds.csv` records no dataset knobs and §5 rule 2 forbids inferring them. One re-run writes the manifest and closes it. `walkforward/CONTEXT.md` §14b |
| **P15** | ⚠️ **`STA-1`** — decide whether `gold.stocks_ta` is rebuilt | see `STA-1` | `P4-1` + *item 7* | **BLOCKING, not untidy, and it costs two different things.** (a) It disagrees with `silver.stocks_basic` for all 30 VN30 names in BOTH directions (ACB 4,882 vs 4,383; BID 3,096 vs 3,121), so the single-stock track can **never see a technical indicator**. (b) `pool__ta` stops 2026-06-26 and the INNER join drops the whole `return_5day` chain 4,266 → **4,235 rows**. Rebuild = 13 renamed columns + 289 k moved rows — its own decision, not a side effect |
| **P16** | **survivorship** — a point-in-time listing/delisting table | ~2 days | `PRF-5` | `z = +18.6` is protected by the null; **+74 %/yr is not**. A DATA problem, not a code one. **↓ detail block** |
| **P17** | **the rolling-vs-expanding training window** | ~1 day | `PRF-3` | Mostly answered by `PRF-2` — the post-2022 break is in the FEATURES, not the market. Only the training-window half is left, untested at h=10 and h=20. **↓ detail block** |
| **P18** | ⚠️ **new information** — intraday/tick, point-in-time listing, dated fundamentals | months | `PRF-6` | ⚠️ **The main lever, not the last resort**, and it sits at 19 only because nothing here is actionable this week. **↓ detail block** |
| **P19** | **re-fit the cost model into ONE function** | ~2 h | `P1-1` | Two models exist, disagree, and were both fitted with `lasso` — dropped 2026-08-16. The guard over-predicts by 4-13×. **↓ detail block** |
| **P20** | **the streaming design** — cut the host-side peak so a top-300 panel fits | days | `P1-4b` + `P3-2` + `VRM-1`'s host half | Three codes for one piece of work: stop materialising the whole design; window per fold or per ticker-chunk. 4.03 GB per million rows, measured. **↓ detail block** |
| **P21** | **`cs_rank_5day` on the top ~300 by turnover** | ~1 h | `P2-2` | Puts a number against §2b's `ALL` row, which still reads *"never ran — unverified"*. ⚠️ Blocked behind `P20` at that width. **↓ detail block** |
| **P22** | **`CSP-1`** — give `read_universe_panel` the `UnifiedSchemaReader.join()` the single-ticker path uses | days | `P3-1` | ⚠️ Makes `MEM-1` worse by the width joined; `pool__ta` at 922 channels is ~10× the design |
| **P23** | `_ingest_gold_stocks` still carries a legacy column (`close`/`volume` check: 1 of 2 present) | — | `P3-3` | ⚠️ Fixing it redefines `gold.stocks`' OHLC as adjusted and commits to a ~2.4 M × ~900 col rebuild. Its own decision. Related to `P15` |
| **P24** | **262 rows in `bronze.cafef_price` have `high < low`** (ACB 2018-07-31: high 35,800, low 36,500) | — | `P4-3` | CafeF's defect; surfaces in gold as a negative `range_hl`. Needs a bronze data-quality screen, not a gold patch. ⚠️ Re-verified 2026-08-17, still 262. Probably deserves an ISSUES.md code |
| **P25** | XGBoost warns in **every** run: *"Falling back to prediction using DMatrix due to mismatched devices"* | — | `P4-4` | If the design is copied host→device per prediction, the GPU conversion is leaving speed on the table |
| **P26** | `landed()` cannot answer *"did THIS run produce anything"* | — | `P4-5` | It rglobs a folder where the previous run's dated files still sit; 140 header-only CSVs went green (2026-07-31). §5 rule 10's mechanism; the fix is to compare against the run's own outputs |
| **P27** | `logs/app.log` has many writers now, so records interleave | — | `P4-6` | The executor is multiprocess and every step appends. Fix is per-process filenames in `Logger`, **not** going back to sequential |
| **P28** | `raw/trading_view` partitions `crypto` and `options` are permanently red | — | `P4-7` | Both `true` in config, folders never existed, `landed(require=True)` fails them. Choose `require=False` or accept two red partitions |
| **P29** | Decide the fate of `raw/trading_view_collected_links` — nothing reads it | — | `P4-8` | It is a leaf, not a hub |
| **P30** | ⚠️ If ever backfilling TradingView, use a **single-run backfill** | — | `P4-9` | `tag_concurrency_limits` is per-RUN, so 9 partitions the default way is 9 runs × 8 browsers = **72 Chrome**. `.dagster/dagster.yaml` is empty |
| **P31** | Four heavy assets have never been observed running end to end through Dagster | — | `P4-10` | `trading_view_links`/`data`, the 5 CafeF stock tabs + news, `cafef_pdfs` (100 partitions), `cafef_financials` (~2.4 h each). *"Built is not run"* |

⚠️ **EVERYTHING EXCEPT `P9` IS LOCAL OR CPU** — nothing else touches the 30 GPU-h/week
Kaggle quota.

⚠️ **What is deliberately NOT on this list**, on measured evidence: **another architecture**
(224× of capacity was tried at h=10, 101× at h=20), **another slice of `pool__ta`** (tied at
both horizons), and **another dataset setting** (six tracks, every `|t|` < 1.4). `P12`,
`P16` and `P18` make the number **honest**, not bigger, and that is why they rank where
they do rather than higher.

⚠️ **A PREDICTION IS RECORDED FOR `P4` … `P11`, BEFORE EACH RUNS.** This file already holds
five predictions made that way and **four were wrong** (`PRF-1`, `P4-12`, and both of the
arm sweep's) — which is the only reason the numbers that followed are worth anything. Score
them; do not quietly edit them.

---

## THE CROSSWALK — every retired OPEN code, and where it went

⚠️ **Read this before following any reference in CLAUDE.md, RUNBOOK.md, ISSUES.md or a
`CONTEXT.md`.** Those files were not rewritten (see the rule at the top), so they still say
`PRF-4` where this file now says `P12`.

⚠️ **THE LIST HAS BEEN RENUMBERED THREE TIMES ON ITS FIRST DAY.** Per the convention chosen
for this file, **the whole list shifts rather than leaving gaps** — so the number always
equals the rank. The cost of that choice is the table below, and there will be a row like it
after every completion and every re-prioritisation.

⚠️ **THE ROWS BELOW ARE FROZEN HISTORY AND MUST NOT BE RENUMBERED WITH THE LIST.** They were
once, by a regex that could not tell a live pointer from a record of the past, and the table
then described a renumbering that had never happened. **A crosswalk that renumbers itself is
worthless.**

| pass | what moved |
|---|---|
| **2026-08-21 (a)** | five schemes → one list, `P1` … `P32` |
| **2026-08-21 (b)** | old `P1` and old `P2` **DONE** → deleted; **old `P3` … `P32` shifted to `P1` … `P30`** (subtract 2) |
| **2026-08-21 (c)** | `FRZ-1` **promoted 12 → 1** (it blocks the OUTPUT, not a number) and a new `P2` inserted (live scoring). **old `P1`…`P11` → `P3`…`P13`; old `P13`…`P30` → `P14`…`P31`** |

⚠️ **SO A BARE `P<n>` IS ONLY STABLE BETWEEN RENUMBERINGS.** Read any older reference against
the pass it was written in — *execution realism* has been `PRF-4`, then `P12`, then `P10`,
and is `P12` again after pass (c). **The HYPHENATED codes never move and are the safer thing
to cite in another file.**

| retired code | now | note |
|---|---|---|
| `M-1` | **`P3`** | the model program, written and renumbered the same day |
| `M-2` | — | ✅ **DONE 2026-08-21** as the old `P2`, the seed floor. `walkforward/CONTEXT.md` §15 |
| `M-3` | **`P4`** | |
| `M-4` | **`P5`** | |
| `M-5` | **`P8`** | |
| `M-6` | **`P9`** | |
| `M-7` | **`P10`** | |
| `M-8` | **`P11`** | |
| `PRF-3` | **`P17`** | only the training-window half is still open |
| `PRF-4` | **`P12`** | |
| `PRF-5` | **`P16`** | |
| `PRF-6` | **`P18`** | |
| `P1-1` | **`P19`** | |
| `P1-4b` | **`P20`** | merged with `P3-2` and `VRM-1`'s host half — one piece of work, three codes |
| `P3-2` | **`P20`** | ⚠️ merged, not moved |
| `P2-2` | **`P21`** | |
| `P3-1` | **`P22`** | |
| `P3-3` | **`P23`** | |
| `P4-1` | **`P15`** | ⚠️ merged into the `STA-1` decision it was a consequence of |
| `P4-2` | **`P13`** | |
| `P4-3` … `P4-10` | **`P24`** … **`P31`**, in order | |
| `SSK-1` | — | ⚠️ **not renumbered: it is a MEASURED RESULT, not a task.** Its numbers are CLAUDE.md §6-1 / §6-1-bis; its open follow-ups became `P6`, `P7`, `P13`, `P1`, `P15` |
| `PRF-0`, `PRF-1`, `PRF-2`, `PRF-7`, `PRF-8`, `PRF-9` | — | **DONE.** Keep their codes; cited by name from CLAUDE.md and `walkforward/CONTEXT.md` |
| `P0-1` … `P0-6`, `P1-2` … `P1-9`, `P2-1`, `P2-3`, `P2-4`, `P4-11`, `P4-12` | — | **DONE.** Keep their codes; in the Archive |
| `P0-7` | — | done and **deleted** per this file's rule (documentation staleness, no reusable reasoning) |

⚠️ **`ISSUES.md` CODES WERE NOT TOUCHED AND NEVER WILL BE.** `STA-1`, `FRZ-1`, `FNM-1`,
`WFO-1`, `NUL-1`, `CSP-1`, `MEM-1`, `VRM-1`, `DRF-1` … are ISSUE codes — permanent by that
file's own rule, never renumbered, never reused. A TODO item may *point at* one (`P15` at
`STA-1`, `P1` at `FRZ-1`, `P3` at `FNM-1`) and that is the relationship: **the issue is
what is broken, the `P<n>` is what somebody is going to do about it.**

---

### P3 · ⚠️ FEED THE MODEL THE REPRESENTATION THE SELECTION USED ⏱ ~1 h + 20 min *est.*  ·  *(was `M-1`)*

**Three independent measurements already on disk point at this, and the third is the one
that makes it first rather than fifth.**

1. **`FNM-1`.** The cross-sectional selection ranked every channel **within each date**
   (`cross_sectional.py` §3: it removes *"the level that acted as a date proxy AND the size
   that acts as a permanent stock label"*). `train_test_creator.build` fits **one global
   `StandardScaler`** on the train slice. The h=10 dataset's own `metadata.json` reads
   **19 scaled columns, 0 bounded** — read from disk 2026-08-21.
2. **The 19 channels include raw LEVELS.** `close_adjust` (a VND price) and
   `drv_vwap_raw` (a VND VWAP) are two of them, and `n_sell_orders` is a raw count. That
   dataset's own `drift.csv`: **`close_adjust` puts 5.48 % of the test set beyond 5
   train-sigmas at a test mean z of +1.098**, and `drv_order_vol_imb_21` puts **7.65 %**
   there. That is `DRF-1` biting the chain that produces every headline in §6-0. ⚠️ §6-2
   warns about exactly this channel in exactly this role — *"in a cross-sectional rank
   problem a price LEVEL is the size proxy `cross_sectional.py` §3 exists to remove"* —
   and it is warning about `pool__ta`'s `close` while `close_adjust` sits in the chain's
   own shortlist.
3. ⚠️ **`step6` IS WHY THIS RANKS FIRST.** `walkforward` §12 retrained **twice as often**
   (20 folds against 10) for `t = −0.09` and ρ = 0.989, so the ~45 % Sharpe decay across
   the sweep — present at **both** horizons — is **not staleness of the FIT**. A
   non-stationary feature REPRESENTATION is the candidate left standing, and a within-date
   rank is stationary by construction.

**The code exists and needs no train/test fitting.**
`cross_sectional.cross_sectional_normalize` ranks per date, before the window, using
nothing from the future — §3 makes that argument for the selector and it transfers
unchanged.

```powershell
cd src
# 1. wire --feature-normalize into train_test_creator (rank per date, BEFORE _window)
# 2. one gbt track, its own --out, paired against the baseline that already exists
python -m walkforward --ticker all --table rank_10day__final__d20_h10 `
    --first-test 2017-01-01 --model gbt --config gbt__all__rank_10day__final__d20_h10.yaml `
    --feature-normalize cs_rank --out ../results/fnorm_h10/csrank
python -m walkforward.compare --top-k 20 --horizon 10 --universe all --draws 2000 `
    baseline=../results/settings_h10/baseline csrank=../results/fnorm_h10/csrank
```

⚠️ **Two things will break it if they are not handled:**

- **A ranked column is ALREADY bounded** in `[−0.5, +0.5]`. `dataset._classify` splits
  columns into `scaled` and `bounded`; a ranked channel must land in `bounded` or it is
  standardised a second time and the whole point is lost.
- **It is a new EXPERIMENT, not a new setting.** A `cs_rank`-feature track and a `std`
  track are two different panels; give it its own `--out` and its own dataset-name
  segment, or `compare` pairs two things that are not comparable. §12b is the case where
  the guard caught exactly this, and the refusal *was* the measurement.

**Prediction, recorded before the run:** ⚠️ **it helps — ΔSharpe +0.2 to +0.6 paired — and
the gain concentrates in the LAST folds**, where the level channels are furthest outside
the training distribution. **If it ties, that is the more interesting answer**: it says the
trees were already reading these channels relatively and `FNM-1` is a documentation defect
rather than a modelling one. **If it LOSES**, the level channels carry real information
about the era and the decay is something else again.

---

### P4 · ENSEMBLE THE SEVEN ARMS — THE PREDICTIONS ARE ALREADY ON DISK ⏱ ~30 min *est.*  ·  *(was `M-3`)*

Seven arms exist at h=10, pairwise **ρ 0.91-0.94**, and **all seven clear the within-date
null with the null MAX below the observed**. Their inductive biases genuinely differ —
§11a: `cnn` pools the sequence away, `tcn`/`transformer` keep a per-timestep view of the
whole window, `gbt` sees only 78 window statistics. **Averaging correlated, individually
unbiased predictors is the standard variance reduction, and it has never been tried here.**

⚠️ **RANK-AVERAGE WITHIN EACH DATE, NEVER SCORE-AVERAGE.** The arms' outputs are on
different scales — `gbt` predicts an unscaled rank while the nets predict a standardised
one and `engine._write_predictions` inverse-transforms — and the estimand is the ORDER
anyway (§6-0-c(1): R² **+0.0003**).

Everything needed is on disk: `results/walkforward_h10_arch/<arm>/predictions_oos.csv`,
seven of them over the identical 10 folds and the identical panel. No GPU, no training.

⚠️ **PRICE THE SEARCH.** An ensemble chosen after seeing seven arms is an **eighth arm**;
read its `p_sharpe` against Bonferroni **0.05/7 = 0.0071**, exactly as §11c reads `gbt`'s
0.044 against 0.05/6.

**Prediction:** ⚠️ **it ties `gbt` and beats the `lstm` reference by +0.2 to +0.4**, and
the effect shows up more in **`se_sharpe` and the fold-to-fold spread** than in the pooled
level — because it is a variance reduction, not a new signal. **A tie with the single best
arm is the expected outcome at ρ 0.93 and is not a failure.**

---

### P5 · PORTFOLIO CONSTRUCTION — NO RETRAINING, AND A −55 % DRAWDOWN TO SPEND ⏱ ~4 h *est.*  ·  *(was `M-4`)*

`backtest.long_only_top_k` is **equal-weight, one book, one rebalance grid** —
`weight = 1.0 / len(picks)`, and `rebalance_dates` takes every `h`-th date from a fixed
origin. Three knobs have never been varied on a walk-forward track:

| knob | state | why it should move |
|---|---|---|
| **`k`** | scanned **once**, at h=20, on the SINGLE split (Sharpe 1.53 at k=10 → 0.81 at k=75). Never on a walk-forward, never at h=10 | the level `k=20` was chosen on is not the level being quoted |
| **weighting** | equal-weight only | **inverse-volatility** is the standard answer to `PRF-4`'s **−55 to −58 %** max drawdown, and it costs no signal — it re-weights an order it does not change |
| **the rebalance grid** | ONE book, every `h`-th date from a fixed origin | **`h` laddered sub-books**, each entering on a different day, removes the timing luck of the origin and smooths turnover across the month. ⚠️ It is also the honest version of the horizon comparison `P2-4` could only pair on the calendar |

Cost is CPU, and the predictions are on disk for both horizons and all seven arms.

⚠️ **EACH KNOB IS A SEARCH.** Report the whole surface, never the argmax, and read any
winner against the number of settings tried — `NUL-1` one level up, the same shape §6-1
point 3 records for the five-ticker search.
⚠️ **Run it WITH `PRF-4`'s rows**, not before them: an ADV cap and a sell-side floor screen
move the same numbers, and measuring them separately means measuring each against a panel
the other has not touched.

**Prediction:** ⚠️ **laddering is worth +0.1 to +0.3 Sharpe and cuts the drawdown by about
a third; inverse-vol cuts the drawdown and costs ~0.1 Sharpe; `k` is flat between 15 and
25** and the k=20 already in use is not a knife-edge — §6-0-bis measured a monotone decay,
not a peak.

---

### P8 · TRAIN THE ESTIMAND — A RANKING LOSS INSTEAD OF MSE ⏱ ~½ day *est.*  ·  *(was `M-5`)*

`engine.CRITERIA[REGRESSION]` is **`nn.MSELoss`** and `model.gbt` uses XGBoost's default
squared error. **Every verdict this repo quotes is the ORDER**: test R² **+0.0003**,
`mase` **0.9937**, and §6-0-c(1) says in as many words that the magnitudes carry nothing.
Optimising squared error on a `[−0.5, +0.5]` per-date rank spends the model's capacity on
a quantity nobody reads.

Two versions, cheapest first:

1. **`gbt` with `objective="rank:pairwise"` and `qid` = the date.** LambdaMART with each
   date as one query group is the textbook match to a per-date rank target, and `gbt` is
   the fastest and best-measured arm. ⚠️ **It is not a config line**: `window_statistics`
   discards the date, so `model/gbt/model.py` has to carry a `qid` through and the design
   has to be sorted by it.
2. **A differentiable per-date IC loss for the nets** — soft-rank, or a per-date Pearson
   of standardised predictions against standardised labels. A day, and it needs the date
   inside the batch, which the current loader does not provide.

**Prediction:** ⚠️ **(1) is a tie or a small win, ±0.2** — §11a's reading is that these
arms already extract the same thing from the 19 channels, and a loss change is a much
smaller intervention than the 224× capacity span that produced 0.76 Sharpe of spread.
⚠️ **I would rather be wrong here than not know**: it is the only untried change aimed at
the estimand itself, and a tie is a *stronger* version of §11a rather than a wasted day.

---

### P9 · ⚠️ SELECTION IS NEARLY EXHAUSTED; CONSTRUCTION IS WIDE OPEN ⏱ ~1 day + T4 *est.*  ·  *(was `M-6`)*

**State this precisely, because "try more features" is not actionable in this database.**

| the wall | measured |
|---|---|
| `CSP-1` | a cross-sectional selection reads **exactly ONE pool** — `read_universe_panel` is hand-written SQL, and `run_selection` raises on any other `--pools` value for a `cs_` target |
| `PRF-9` / §13 | a **date-only** column has a **constant within-date rank** and cannot rank a cross-section at all — and **71 of 76 gold tables are date-only** |
| `backtest` §10d | **`pool__fa` holds 2 tickers** on `unified_schema_all` (VCB and ACB), so fundamentals do not exist at panel grain |

So of the 23 pools on `unified_schema_all`, the ones that **can** rank are `pool__basic`
(done, every horizon) and `pool__ta` (done twice — ties twice). **The selection lever is
close to spent. The CONSTRUCTION lever has barely been touched**, and the hit rate says
where to point it: `pool__basic` carries only **5 cross-sectional `drv_cs_*` channels**
and **2 of the 5 are in the 19-channel shortlist** — a **40 % hit rate against 21 %** for
the pool as a whole (19 of 90).

| family to build | why | anchored on |
|---|---|---|
| **sector-relative** — each channel minus its GICS-industry within-date median | the GICS tree is already attached in silver, and `drv_cs_ret_vs_industry` is one of the 19 | §3a, §6-2 |
| ⚠️ **the surviving order-flow PAIR, as an interaction** — `drv_log_order_size_ratio` × `drv_order_count_imb_5` | `backtest` §10c: the **only two channels that survive BOTH the t+1 execution lag and the 2022 break**, and they are deliberately opposed in sign (institutional vs retail tape). ⚠️ **Neither is in the 19** | `backtest` §10c |
| **residual momentum / idiosyncratic vol** — return and vol after removing a market beta | it removes what the rank target removes anyway, leaving what is name-specific | §2b |
| **within-date z-score twins** of the level channels | `P3`'s argument at CHANNEL grain instead of dataset grain — and it survives even if `P3` is rejected | `drift.csv` |

⚠️ **This is the ONLY item that spends Kaggle quota** (~45 min of T4 for the selection with
no null; a 20-draw null on ~110 channels is hours). ⚠️ And it is a `pool__basic` rebuild on
`unified_schema_all` — **11m 08s and 2,388,975 rows**, measured — so it is not free
locally either.

**Prediction:** ⚠️ **the order-flow interaction shortlists; the sector-relative family
mostly does not** — a within-date rank already removes most of what a sector median
removes, so the two are close to the same operation.

---

### P10 · SWEEP `lookback` — THE ONE DATASET KNOB NEVER SWEPT ⏱ ~4 h *est.*  ·  *(was `M-7`)*

`walkforward` §12c point 4 names it and nothing has moved since: **`d` comes from the
source TABLE NAME**, `engine._verify` asserts it, so it is not a flag — every value needs
its own selection run. **`d = 20` is the only value the cross-sectional chain has ever
used**, at either horizon.

⚠️ **§11a is a direct hint that the window is longer than the information in it**: a tree
seeing **78 window statistics** beats an LSTM seeing **260 numbers** on the identical
folds. If the sequence inside the window carries nothing, the window's LENGTH is the
untested half of that sentence.

⚠️ **`d` also moves the PURGE**, which is `d + h − 1` (§5 rule 6). At h=10 that is 29 rows
at `d=20` and 14 at `d=5` — so a shorter window buys training rows as well as changing the
representation, and the two effects must be reported separately or the result cannot be
read.

Three values (`d = 5, 10, 40`) ≈ 45 min selection + 20 min sweep each.

**Prediction:** ⚠️ **`d=10` ties `d=20`; `d=5` loses slightly; `d=40` loses.** If `d=40`
WINS, §11a's *"the sequence inside the lookback is worth nothing"* has to be restated as
*"the sequence THIS LONG is worth nothing"*, which is a different and weaker claim.

---

### P11 · DATE-ONLY POOLS AS A REGIME OVERLAY, NOT AS RANKING CHANNELS ⏱ ~1 day *est.*  ·  *(was `M-8`)*

The structural fact that kills the date-only pools for RANKING does not kill them for
TIMING. `pool__market_breadth` (8 channels compressing all 781 names to one row per
session), `pool__stock_market` (6 indices × 27 measures, including VNINDEX order and
foreign flow) and `pool__bonds` are all constant within a date — **zero within-date rank
IC by construction** — and every one is a candidate for scaling the **book**, not the
ranking.

**The target is specific**: **2022, the only bad fold at either horizon** (−0.07 at h=20,
+0.37 at h=10, in a year the equal-weight universe itself ran −0.94) and `PRF-4`'s **−55 to
−58 %** max drawdown.

⚠️ **READ `PRF-2` / §9b FIRST: the post-2022 break is in the FEATURES, not the market.** So
an overlay is a **risk control and not a fix for the decay**, and it has to be scored as
one — drawdown and `se_sharpe`, not the pooled level alone.
⚠️ **It is a second search over the same ten folds.** Fit the overlay's rule on pre-2017
data and freeze it, or price the search; anything else re-learns 2022 from 2022.

**Prediction:** ⚠️ **it cuts the drawdown materially and moves Sharpe by less than its own
error bar.** A regime filter that also raises the Sharpe on this data would be surprising
and should be treated as a search artefact until it survives a frozen pre-2017 rule.

---

## ⚠️ What was next on 2026-08-21 (morning) — the six-item program, SUPERSEDED for ORDERING by the block above

> ⚠️ **Its ORDER is superseded; its measurements are not.** *"What the five
> measured items actually settled"*, the leaderboard read from disk, and items
> **4-11** of *"What is left, in order"* are all still current — the M-items above
> are what to run *for the MODEL*, these are what to run *for the REPO*.

**Six of six done.** Item 6 — the one that touches the chain every number rests on — closed
the same day, together with `P1-8` and `P1-9`. Their measurements live in CLAUDE.md
§6-0-ter-2, `walkforward/CONTEXT.md` §11c and §14, and `pipeline/CONTEXT.md` §1c-1d; per
this file's own rule they are **deleted here, not ticked**.

| # | asked for | verdict |
|---|---|---|
| 1 | bring the new features in | ✅ **162 channels ran** (90 `pool__basic` + 72 pruned `pool__ta`), 35 % wider than the previous best, after **four memory walls** |
| 2 | feature selection + final features, carefully | ✅ **the widened chain TIES the narrow one**, paired `t` = **+0.46** over 236 periods |
| 3 | many train/test settings | ✅ **six tracks, every `\|t\|` < 1.4** — the settings are worth nothing |
| 4 | LSTM / CNN / LSTM-CNN / bidirectional | ✅ **7 arms × 10 folds**; architecture matters **only downward** |
| 5 | other models | ✅ TCN + Transformer written and run; more is available on request |
| 6 | **one complete flow** | ✅ **DONE** — `python -m pipeline` now covers the cross-sectional chain end to end: **10 stages, 5.8 s, every row `up to date`**, and `RUNBOOK` §3a's two "do not use pipeline here" blocks are gone rather than reworded |

### What the five measured items actually settled

- **Architecture matters at h=10, but only DOWNWARD** (§6-0-ter-2) — ⚠️ **and `P1-9`
  narrowed this the same day.** Those `t`s test MEAN RETURN. On the SHARPE difference only
  `cnn` loses (p = 0.001); `bilstm` and `cnnlstm` earn less at lower volatility and **tie**
  (p = 0.61 / 0.30). `gbt` GAINS +0.36 Sharpe at a nominal p = 0.044 that does **not**
  survive the six arms tried (Bonferroni 0.0083). ⚠️ `PRF-8`'s h=20 sweep has **not** been
  re-scored, so its ties remain mean-return results only.
- **The dataset and split settings are worth nothing** (`walkforward` §12). ⚠️ **`step6` is
  the one to read**: retraining **twice as often** gives `t` = −0.09, so the ~45 % Sharpe
  decay across the sweep is **not staleness**.
- **`pool__ta` changes the SHORTLIST and not the MONEY**, now measured at two horizons, two
  candidate widths and two architectures (`walkforward` §13).
- **The h=10 walk-forward holds** — z = +18.58, IC positive 10/10 folds — and **`PRF-7` at
  h=10 came back clean** (51 of 61 channels survive a pre-2017 selection).
- **The indicator survey** (`backtest` §10): 29 channels × 3 horizons × 2 grains, and only
  **three** survive the execution lag AND the 2022 break. ⚠️ Its most reusable finding is
  that `drv_dist_from_high_252` **flips sign between grains** — a relative signal, not an
  absolute timing rule.

### ⚠️ Three predictions I recorded before measuring, and three were wrong

Kept verbatim where they were written, because the register's value is that it does not
quietly edit them: *"they all tie"* (three arms lost), *"`best_epoch` 0-2 for every arm"*
(43 of 70 — the convolutional arms train 6-20 epochs, so **"best epoch is 1" is an LSTM and
GBT property, not a property of the problem**), and *"row-blocking `window_design` will cut
the peak"* (it moved 0.1 GB; the panel REASSEMBLY was the allocation).

### ⚠️ THE LEADERBOARD AS IT STANDS — re-read FROM DISK 2026-08-21, not quoted from a register

Both tracks reproduce CLAUDE.md §6-0 to every digit via
`walkforward.evaluate --draws 0` (RUNBOOK §7b, ~2 min each, no GPU). Top-20, 30 bps,
buyable only:

| track | IC | `ic_t` | `sharpe@30` | `cagr@30` | market | periods | `se_sharpe` |
|---|---|---|---|---|---|---|---|
| **h=10 · `gbt`** | 0.1460 | 16.36 | **+2.891** 🥇 | +69.8 % | +13.9 % | 236 | 0.142 |
| h=10 · `transformer` | 0.1433 | 16.60 | +2.622 | +72.9 % | +13.9 % | 236 | 0.131 |
| h=10 · `tcn` | 0.1426 | 16.43 | +2.622 | +73.4 % | +13.9 % | 236 | 0.131 |
| **h=10 · `lstm`** | 0.1412 | 16.05 | +2.531 | **+74.0 %** 🥇 | +13.9 % | 236 | 0.128 |
| h=10 · `bilstm` | 0.1419 | 16.12 | +2.474 | +65.9 % | +13.9 % | 236 | 0.125 |
| h=10 · `cnnlstm` | 0.1308 | 13.73 | +2.367 | +64.3 % | +13.9 % | 236 | 0.121 |
| h=10 · `cnn` | 0.1171 | 12.42 | +2.133 | +56.8 % | +13.9 % | 236 | 0.113 |
| **h=20 · `lstm`** *(the chain)* | 0.1097 | 6.90 | +1.991 | +47.5 % | +14.6 % | 118 | 0.155 |

⚠️ **THE HIGHEST-SHARPE ARM IS NOT THE HIGHEST-CAGR ARM**, and it is `P1-9` visible in the
LEVELS rather than only in the paired test: `gbt` earns **4.2 pp/yr less** than `lstm` while
scoring **0.36 more** Sharpe. **"The best model" is not well-formed without an estimand** —
RUNBOOK §7c.

### ~~What is left, in order~~ — ⚠️ SUPERSEDED, its eleven items are now `P3` … `P18`

The table that stood here listed eleven items by POSITION (*"item 1"*, *"item 2"*) with no
codes, which is half of why this file needed renumbering at all. **They were not dropped —
every one is in the list at the top**, and nothing about them changed except that they now
have a name:

| was | now | |
|---|---|---|
| item 1 | **`P3`** | re-score the h=20 arm sweep through the fixed `compare` |
| item 2 | **`P6`** | take the 30-name VN30 result downstream |
| item 3 | **`P7`** | a wider cross-section at h=10 that is NOT VN30 |
| item 4 | **`P13`** | emit `n_dead_train` / `n_dead_test` *(was `P4-2`)* |
| item 5 | **`P1`** | `FRZ-1` in `pipeline.status_data` |
| item 6 | **`P14`** | re-run each legacy walk-forward track once |
| item 7 | **`P15`** | the `STA-1` decision *(merged with `P4-1`, its other consequence)* |
| item 8 | **`P12`** | execution realism *(was `PRF-4`)* |
| item 9 | **`P16`** | survivorship *(was `PRF-5`)* |
| item 10 | **`P17`** | the rolling-vs-expanding half *(was `PRF-3`)* |
| item 11 | **`P18`** | new information *(was `PRF-6`)* |

⚠️ **The *"lower still"* line that followed it is gone too**, and its contents are
`P19`…`P31`. A backlog named only by what it is *below* cannot be prioritised.

⚠️ **A NEW LIMIT WAS CREATED, NOT DISCOVERED, AND IT IS WRITTEN DOWN RATHER THAN HOPED
OVER**: the `WFO-1` guard protects the five pre-2026-08-21 tracks on their **TABLE only**,
because `folds.csv` records no dataset knobs and §5 rule 2 forbids inferring them. Running
any legacy track once writes its manifest and closes the gap (`P14`).
`walkforward/CONTEXT.md` §14b.

⚠️ **What is NOT worth doing, on this session's evidence**: a bigger model (224× of capacity
tied or lost), more of this data (`pool__ta` tied twice), and more dataset settings (six
tied). What is left is honest EXECUTION and NEW INFORMATION — now **`P12`**, **`P16`**,
**`P18`**.

---

## ✅ DONE 2026-08-21 — the 7-arm architecture sweep at h=10, and BOTH predictions were wrong

**Result: architecture matters at h=10, but only DOWNWARD.** No arm beats the LSTM
significantly; **three lose to it significantly**. Numbers in CLAUDE.md §6-0-ter-2 and
`walkforward/CONTEXT.md` §11 — do not re-derive them here. 2h 48m sweep + 22m scoring,
70/70 arm-folds, 0 errors.

| | verdict |
|---|---|
| `gbt` (1,398 nodes) | Sharpe@30 **+2.891**, IC **+0.1460** — leads both, and is the SMALLEST arm |
| `transformer` / `tcn` | +2.622 each — tie the LSTM (`t` −0.33 / −0.20) |
| `bilstm` (313 k) | +2.474, **`t` = −2.09** ❌ loses, while being the LARGEST arm |
| `cnnlstm` / `cnn` | +2.367 / +2.133, **`t` = −2.15 / −3.37** ❌ |

⚠️ **Three things this changed elsewhere in this file**, all applied: `PRF-8`'s "ruled out"
row is now "ruled out AT h=20"; the START HERE claim that "try a bigger model is closed" is
qualified; and **`P1-9` is opened** because the `|t|` both results were read off tests MEAN
RETURN, not Sharpe.

**The prediction as written at 00:20, kept verbatim below, and how it scored:**

| # | prediction | outcome |
|---|---|---|
| 1 | *"They all tie, paired \|t\| < 1.5"* | ❌ **WRONG** — three arms at 2.09-3.37 |
| 2 | *"`best_epoch` 0-2 for every arm in nearly every fold"* | ❌ **PARTLY WRONG** — 43 of 70; `cnn` averages 7.7 (max 20), `tcn` 5.7 |
| 3 | *"if anything wins, `gbt` or `tcn`"* | ✅ right in direction |
| 4 | *"`transformer` winning would surprise me"* | placed 2nd-equal, did not win |

⚠️ **Prediction 2 matters beyond bookkeeping**: *"best epoch is 1"* has been quoted four
times in this repo as evidence capacity is worthless. It is an **LSTM and GBT** property —
the convolutional arms genuinely train 6-20 epochs. Attach it to an architecture from now on.

---

### The prediction as recorded, before the run

> Written 2026-08-21 00:20, with 24 of 70 arm-folds done and no result visible. Recorded
> here because this file already holds three predictions made the same way and **two of
> them were wrong** (`PRF-1`, `P0-1`, `P4-12`) — which is the only reason the eventual
> numbers are worth anything.

`walkforward --out ../results/walkforward_h10_arch`, 10 expanding folds, seven arms, all
trained on ONE build of each fold so `walkforward.compare` can pair them:

| arm | parameters |
|---|---|
| `bilstm` | 313,153 |
| `lstm` | 208,769 |
| `transformer` | 68,417 |
| `cnnlstm` | 30,369 |
| `tcn` | 18,113 |
| `cnn` | 5,185 |
| `gbt` | 1,398 decision nodes |

**Capacity spans 224×**, against `PRF-8`'s 101× — the widest architecture test this repo
has run, and the first at h=10.

### The prediction, stated so it can be scored

1. ⚠️ **They all tie, paired |t| < 1.5, at every cost level.** `PRF-8` found this at h=20
   over a 101× span; §5c found it over eleven architectures inside ONE error bar. The
   horizon is the only thing that moved, and `PRF-2` showed the horizon moves the RESULT
   without touching the architecture question.
2. **`best_epoch` will be 0-2 for every arm in nearly every fold.** Already visible in the
   log: train loss 0.976 → 0.613 while val goes 0.972 → 1.315, best still epoch 1. If a
   sequence model needed depth over this window, some arm would want more epochs.
3. ⚠️ **If anything wins, I expect `gbt` or `tcn`, not the big recurrent nets** — the
   small models won or tied at h=20, and 19 channels over a 20-day window is not a regime
   where 313k parameters have anything to spend themselves on.
4. **The one thing that would surprise me is `transformer` winning.** Self-attention's
   argument is a long path length; at d=20 an LSTM's path is already short, so a win there
   would mean the window's INTERNAL structure carries something recurrence misses — and
   that would be the first evidence in this repo that architecture is worth anything.

**What the result cannot establish either way**: nothing here was tuned per arm, and a tie
under one schedule is not an optimum (`PRF-8` §8d). `NUL-1` applies to all seven — no null
prices the feature selection that chose the 19 channels every arm reads.

---

## ⚠️ What was next on 2026-08-20 — superseded by the block above

### ✅ CLOSED 2026-08-20 — the h=10 WALK-FORWARD, and the horizon is still NOT promoted

The one run that settles *"is h=10 one lucky split?"*. **33m 26s** sweep + **8m 59s**
scoring on the local RTX 3050, no Kaggle quota. Numbers in **CLAUDE.md §6-0-bis-3** and
**`walkforward/CONTEXT.md` §9**; do not re-derive them here.

| | h=10 (NEW) | h=20 (`PRF-1`) |
|---|---|---|
| Sharpe@30, pooled | **+2.531** | +1.991 |
| periods | **236** | 118 |
| IC / `ic_t` | **+0.1412** / **+16.05** | +0.1097 / +6.90 |
| null z @20/30/50 | **+18.42 / +18.58 / +18.86** | +12.18 / +12.28 / +12.46 |
| folds with IC > 0 | **10 of 10** | 9 of 10 |

⚠️ **AND IT IS STILL NOT A REASON TO MOVE THE CHAIN TO h=10.** Three measured reasons:

1. ✅ **THEY WERE PAIRED THE SAME DAY (`P2-4`), AND THE ANSWER SPLIT IN HALF.**
   `walkforward.pair` pairs on the CALENDAR (both hold a book on all 2,360 shared sessions,
   ρ = 0.723) because `compare` cannot pair 236 periods against 118. At 30 bps the **mean
   return gap is +17.0 pp/yr, p = 0.0004, CI [+8.6, +25.7]** ✅ — and the **Sharpe gap is
   +0.44 with a CI of [−0.079, +1.041]** ❌. **h=10 is higher-return and higher-VOLATILITY**;
   the risk-adjusted advantage is not established. ⚠️ Nor is equality — the CI reaches
   +1.04, so this is underpowered, not settled.
2. ✅ **`PRF-7` now bounds h=10 too — and it came back clean.** 9m 46s on a T4:
   **51 of 61** kept channels survive a pre-2017 selection (**+5.48 sd** above chance,
   Jaccard 0.750), **`drv_order_vol_imb` is #1 in both**, and 10 of the 12 shortlist misses
   sit in the early kept set. ⚠️ **It bounds the optimism rather than removing it**, so the
   LEVELS above are still levels-with-a-bias. CLAUDE.md §6-0-bis-3, `walkforward` §9e.
3. ⚠️ **The decay is the SAME at both horizons and the slope alone says otherwise** —
   −0.219/fold against −0.100, but **−45.8 %** against **−43.6 %** proportionally. The
   absolute slope is steeper only because the level is higher. I read this wrong once
   before writing it down.

**Opened by the same run**: **`WFO-1`** (`walkforward` overwrites the previous sweep — one
omitted `--out` from destroying `PRF-1`), now **`P1-8`**. **Also fixed**: both registers
pointed stage 9's artefact at repo-root `results/`, where it has never been — `backtest`
writes into `<run_dir>/results/`, which is gitignored (`RPR-1`).

---

## ⚠️ What was next on 2026-08-19 (evening)

> ⚠️ **HISTORY. Its two *"what is left / next"* tables are superseded by the one list at
> the top**, and its codes are the retired ones — use the crosswalk. Kept because the
> REASONING around `SSK-1` is the evidence, not because the ordering still holds.

> ⚠️ **TWO OF THE THREE OBVIOUS LEVERS ARE NOW CLOSED BY MEASUREMENT, AND THAT IS THE MOST
> IMPORTANT THING ON THIS PAGE.** `PRF-8`: a model **101× smaller** ties the 205 k LSTM
> (paired |t| < 1). `PRF-9`: **30 more candidate channels** tie (paired t = −0.29, and the
> wide model is if anything *worse* on money while *better* on IC). **The 13 original
> channels are the result.** ⚠️ **QUALIFIED 2026-08-21** — the h=10 arm sweep found three of
> six alternatives LOSING significantly to the LSTM (`cnn` −3.37, `cnnlstm` −2.15, `bilstm`
> −2.09), so the closed door is "a BIGGER model", not "the architecture does not matter".
> So "try a bigger model" and "add more of this data" are both
> spent — what is left is honest EXECUTION and NEW INFORMATION.

**Closed today** — `PRF-0`, `PRF-1`, `PRF-2`, `PRF-7`, `PRF-8`, `PRF-9`, `P0-7`, `P1-2`
(`PNL-2`), `P1-6` (`FNM-1`), `P4-12`, `PRB-1`. **Opened**: `VRM-1`.

### ⚠️ SSK-1 · THE SINGLE-STOCK h=10 TRACK — measured 2026-08-19, and it FAILED

The thesis' stated top goal is an absolute `return`/price forecast for ONE HOSE/HNX/UPCOM
name at 5-10 sessions. `h=5` was ruled out before spending anything, on two numbers already
in CLAUDE.md (§2a-bis: at h=5 even pure momentum loses its benchmark; §6-0-bis: at h=5 fees
alone are 17.6 %/yr against a 9.75 %/yr benchmark return). `h=10` had **never been run once**
— 0 of 32 archived selection runs used `return_10day`. It has now, on five names, and it does
not clear: pooled `t = +1.45`, rule 3 fires on 4 of 5. **Full numbers and the four things it
measured are CLAUDE.md §6-1** — do not re-derive them here.

**What was built to get there, and is now reusable:**

| | |
|---|---|
| 5 new unified partitions | `HPG SSI FPT VIC STB` in `assets/unified.py::UNIFIED_PARTITIONS` **and** `config.json` `partitions.unified` (that block is LISTED, so absent = OFF). No `UNIFIED_MEMBER_FILTERS` entry needed — an unlisted key falls through to `ticker = %s` |
| re-scrape | 4 CafeF tabs x 5 tickers, `skip_existing=False`, **19m29s**, 0 ERROR, all five to **2026-08-19** |
| pools | `pool__basic` + `pool__targets`, ~19s per partition, 96 cols x ~4,393 rows |

### ⚠️ SSK-1 UPDATE 2026-08-20 — 30 VN30 names, and the POOLED answer flipped

Numbers in **CLAUDE.md §6-1-bis / §6-1-ter / §6-1-quater**; do not re-derive them here.
One-line each:

| | |
|---|---|
| single-stock `return_10day`, **30 VN30 names** | pooled excess **+0.0611**, dependence-adjusted **t = +3.77, p = 0.002** — ⚠️ but 8 of 30 clear individually, 5 are negative, rule 3 fires on 22 of 30 |
| widening a SINGLE STOCK (90 → 470 ch) | HPG **z +0.11 → −0.40** ❌ — the date-only macro blocks, offered to a return target for the first time, take it below its null's mean |
| widening the CROSS-SECTION (100 → 245 ch) | VN30 **z +0.10 → +1.62** — opposite sign, same week, same data |
| the width ladder at h=10 | VN30 **z = +0.10** against `PRF-2`'s top-150 **+13.78**; the `1/√N` noise floor reproduces at 2.15 vs 2.24 predicted |
| `pool__ta` reduced | **711 → 145** label-free; `SKW-1` now has numbers (`val_matched_bn` is an EXACT copy of `value_matched`) |

**What this changes about what to do next:**

| # | item | ⏱ | why |
|---|---|---|---|
| 1 | ⚠️ **Take the 30-name result DOWNSTREAM before believing it** | ~2 h | `t = +3.77` is a SELECTION bar. §5d and `P2-3`: a cleared selection bar has never once survived to a model in this repo. `final_features → train_test_creator → model → result_evaluator` on 2-3 of the 8 names that cleared individually (VJC z +3.67, SSB +2.60, PLX +2.55) is the honest next step, and a negative there closes the thread cleanly |
| 2 | **A wider cross-section at h=10 that is NOT VN30** | ~1 h | §6-1-quater cannot separate N from the date window from the universe rule. Top-150 by `liquidity_before` restricted to **2017→** isolates the window; top-30 by the same rule isolates the RULE. Both reuse `xs_vn30.py`'s `ProvidedPanel` path, which needs no Kaggle quota |
| 3 | **`P4-2` — emit `n_dead_train`/`n_dead_test`** | ~2 h | still open, still costing a hand computation to read any ragged-pool run |
| 4 | ⚠️ **`STA-1` is now BLOCKING, not just untidy** | see `STA-1` | `pool__ta` cannot build on ANY one-company schema — `gold.stocks_ta` disagrees with `silver.stocks_basic` for all 30 VN30 names in BOTH directions (ACB 4,882 vs 4,383; BID 3,096 vs 3,121). Until it is rebuilt, the single-stock track can never see a technical indicator |
| 5 | `FRZ-1` in `pipeline.status_data` | ~1 h | unchanged |

### What is next on this track, in order

| # | item | ⏱ | why |
|---|---|---|---|
| 1 | **offer the date-only blocks** — `pool__economy_*` (19) and `pool__forex_*` (47) for these five names | ~1-2 h build + selection | ⚠️ **The one structurally NEW thing available.** `PRF-9` proved date-only channels cannot rank a cross-section (constant within-date rank); for a SINGLE stock they are perfectly valid. **71 of 76 gold tables are date-only** and none has ever been run on a return target — the 19 archived economy runs were all `close_adjust` with **0 null draws**, and `pool__forex_*` has never been selected on at all. This is the honest remaining lever for the stated goal |
| 2 | **exclude `prop_*` and re-run** | ~30 min | 5 channels with coverage 0.20 (first value **2023-01-03**) were shortlisted by 4 of 5 tickers while being constants in folds 1-4's training slices. Cheap, and it removes a rule-23 confound from every number above |
| 3 | **`P4-2` — emit `n_dead_train` / `n_dead_test`** | ~2 h | ⚠️ **CONFIRMED STILL OPEN 2026-08-19.** `validation.csv` carries `n_train`/`n_test`/`ic`/`r2`/`hit_rate` and nothing about dead channels, so rule 23's confound had to be computed externally to read this run at all. Until it ships, every ragged-pool run needs a hand check |
| 4 | **`FRZ-1`** — put the per-ticker freshness split in `pipeline.status_data` | ~1 h | 755 of 781 tickers were 37 sessions stale and `MAX(date)` said 2026-08-07. One query, beside `pools_behind` |
| 5 | ⚠️ **decide whether `gold.stocks_ta` gets rebuilt** | see `STA-1` | The re-scrape made `STA-1` bite these five names for the first time: `pool__basic` is now 37 sessions past `pool__ta`, so `basic + ta` INNER-joins the fresh rows away. Before, non-bank tickers ended on the SAME day in both and it cost nothing |

### What is left, in order

| # | item | ⏱ | where | output |
|---|---|---|---|---|
| ~~1~~ | ~~**the h=10 WALK-FORWARD**~~ | ~~**~20 min** *est.*~~ | ✅ **DONE 2026-08-20** | **42 min actual**, not 20. See the block at the top of this file. ⚠️ **Its last clause was wrong and is worth keeping visible**: *"`walkforward.compare` pairs the two tracks"* — it **cannot**, and the reason is structural (different holding intervals ⇒ no period-wise correspondence). Pairing two horizons is an OPEN problem, now **P2-4** |
| 2 | **`PRF-4`** — execution realism | ~1 day | **CPU** | ADV/size cap, floor days on the SELL side, the ATC auction. ⚠️ These move the LEVELS, and after PRF-8/PRF-9 the levels are the only thing still moving |
| 3 | **`PRF-5`** — survivorship | ~2 days | **data** | `z` is protected, `+47.5 %/yr` is not. A point-in-time listing table |
| 4 | **`PRF-3`** — ⚠️ **mostly ANSWERED by `PRF-2`**; what is left is the training-window test | ~1 day | local | PRF-2 showed the post-2022 break is in the FEATURES, not the market (19 selected channels +2.44 vs 3 hand-picked −0.26, same window and horizon). The rolling-vs-expanding half is still untested at h=10/h=20 |
| 5 | **`PRF-6`** — new information | months | — | ⚠️ **Now the main lever, not the last resort.** Intraday/tick (the 5-day signal decays inside ONE session), point-in-time listing, fundamentals with filing dates |
| 6 | **`P1-1`** cost model · **`P1-4b`**/`P3-2` streaming · **`VRM-1`** | ~2 h / days | CPU | `VRM-1` is what capped `PRF-9` at 30 of 405 channels. Fixing it (chunk the SHAP predict as `P1-4` chunked the ranker, or drop `xgb_shap` — §19 found only `permutation` load-bearing) is what would let the rest of `pool__ta` be tried |

⚠️ **Kaggle quota used this week: ~8.5 of 30 GPU-h.** Items 1-4 are all local or CPU.

---

## PROFIT — the track that ends in money, added 2026-08-19

> ⚠️ **ITS ORDERING TABLE IS SUPERSEDED BY THE ONE LIST AT THE TOP (2026-08-21).** Four of
> its six rows are DONE (`PRF-0`, `PRF-1`, `PRF-2`, `PRF-7`, `PRF-8`, `PRF-9`) and keep
> their codes; the four still open were renumbered — **`PRF-3` → `P17`**, **`PRF-4` →
> `P12`**, **`PRF-5` → `P16`**, **`PRF-6` → `P18`**. ⚠️ **What survives here and is not
> repeated at the top is the *"what is already measured, so nobody re-measures it"*
> table below** — that is the section's real value and it is still current.

> ⚠️ **This section is ordered by what would CHANGE A DECISION, not by effort.** Everything
> in it exists because stage 9 (`src/backtest/`) made the question askable for the first
> time: `result_evaluator` says *does it rank?*, and only a costed backtest says *does it
> pay?*. Read `backtest/CONTEXT.md` §3 (the cost identity), §4 (the h=20 model result) and
> §8f-8g (the +5%/5d screen under real market rules) before starting any of it.
>
> **What is already measured, so nobody re-measures it:**
>
> | | verdict |
> |---|---|
> | **h=20 model, WALK-FORWARD, 10 folds, top-20, 30 bps** | **Sharpe +1.991** (118 periods, se 0.155), CAGR +47.5 % vs market +0.737/+14.6 %; z = **+12.3**; IC positive **9/10 folds**, beats market **10/10** ✅ |
> | h=20 model, single split, top-15, 50 bps | Sharpe +1.484 test / +1.737 val, z = +4.29/+6.10 ✅ |
> | h=20 model, ceiling names excluded | **+1.551** test — the band does NOT bite it (PRF-0) |
> | h=10 hand screen, k=20, 30 bps | Sharpe **+0.652**, z = +4.72, beats market 0.404 ✅ |
> | h=5 hand screen, 30 bps | **ties** the market; loses at 50 bps ❌ |
> | h=3 | worst of all — turnover dominates ❌ |
> | **2022-2026** | ⚠️ **the two disagree and the horizon is why.** The h=5/h=10 HAND screens are flat-to-negative there (`backtest` §8g); the h=20 MODEL is clearly positive in 2023/24/25 (+2.64/+0.90/+1.39) with 2022 the only bad fold, bad for everyone |
> | **the selection look-ahead** | ✅ **BOUNDED 2026-08-19 (PRF-7)** — re-running the identical selection on dates < 2017 keeps **51 of 61** channels (5.8 sd above chance) and the same top two. The channel set is **not period-fitted**, so the levels roughly stand. ⚠️ It bounds the bias, it does not remove it |
> | **the ARCHITECTURE** | ✅ **RULED OUT AT h=20 2026-08-19 (PRF-8)** — a **2,033-parameter** LSTM (101× smaller) scores Sharpe **+1.997** and a 1,400-node GBT **+1.975** against the 205 k model's +1.991. Paired `\|t\| < 1` at every cost level. ⚠️ **NOT ruled out at h=10 (2026-08-21)**: over 224× of capacity, `gbt` leads at **+2.891** and three arms LOSE significantly — `cnn` −3.37, `cnnlstm` −2.15, `bilstm` −2.09. Architecture matters DOWNWARD. ⚠️ And that `\|t\|` tests MEAN RETURN, not Sharpe (**P1-9**) |
| the biggest open threat now | ⚠️ **execution realism (`P12`, was PRF-4) and survivorship (`P16`, was PRF-5)** — both hit the CAGR, neither touches the `z`. `+47.5 %/yr` is the number to distrust; `z = +12.3` is not |
>
> **Order below, and why it changed 2026-08-19** — PRF-0/1/7 closed in one day and their
> successors are not in the order the section was written in:
>
> | # | item | ⏱ | why here |
> |---|---|---|---|
> | 1 | **PRF-2** | ~1 h GPU + 1 h | the only horizon that is measured-to-work and unmeasured-by-a-model, and it **separates PRF-3's two hypotheses** (horizon vs feature set) by moving one variable. Also the first model-vs-hand-baseline number at any horizon |
> | 2 | **PRF-9** | days | the largest upside left (90 → 800 candidate channels) and the only one that is **BLOCKED** — needs `P1-2` shipped and `pool__ta` pruned past `MEM-1` first. ⚠️ **PRF-8 promoted it in substance**: with the architecture ruled out, FEATURES are the only lever left that is not new data |
> | 3 | **`P17`** *(was PRF-3)* | ~1 day | half-answered by PRF-1 already; **run PRF-2 first**, it is cheaper and may make this unnecessary |
> | 4 | **`P12`** *(was PRF-4)* | ~1 day | execution realism. Moves the LEVELS, not the `z` — and its cheapest row is already measured and just not shipped (`PRF-0`'s ceiling exclusion, ~1 h) |
> | 5 | **`P16`** *(was PRF-5)* | ~2 days | survivorship. Same shape: `z = +4.72` stands, `+47.5 %/yr` does not. It is a DATA problem, not a code one, which is why it is not higher |
> | 6 | **`P18`** *(was PRF-6)* | months | new information. The only lever §2d says is left, and the only one that is not a re-analysis of data already on disk |
>
> ⚠️ **`PRF-8` CLOSED 2026-08-19 and it changed what the rest of this list is for.** Three
> models spanning 205,441 parameters to 1,400 decision nodes tie on the identical folds, so
> "try a different model" is no longer an available answer to anything below. What is left
> is FEATURES (`PRF-9`), the HORIZON (`PRF-2`), honest EXECUTION (`PRF-4`/`PRF-5`) and new
> DATA (`PRF-6`).

### ✅ PRF-2 · DONE 2026-08-19 — the model beats three ranked columns by **2.7 Sharpe**, and it answers PRF-3

> **Why second (2026-08-19):** it is the cheapest way to move ONE variable. `PRF-3` lists
> two hypotheses for the post-2022 break — the horizon or the feature set — and this run
> holds the feature pipeline fixed and moves only the horizon, so **run it before PRF-3's
> training-window experiment**, which costs a day and may turn out unnecessary.

**The one horizon that is both measured-to-work and unmeasured-by-a-model.** A hand-built
3-channel rank gets Sharpe **+0.652 at 30 bps** there (z = +4.72, 211 periods, beats the
market's 0.404 at every cost level). Nobody has run the selection + LSTM chain at `h=10`.

The question it answers: **how much does a fitted model add over three ranked channels?**
That number is not known at any horizon — at h=20 there is a model result and no hand
baseline; at h=10 there is a hand baseline and no model.

```powershell
# selection: cs_rank_10day, top-150 by pre-2014 turnover, 20-draw null (Kaggle T4, ~6 h)
# then the local chain, which is minutes:
python -m final_features --apply
python -m train_test_creator --ticker all --table rank_10day__final__d20_h10 --save
python -m model.lstm --config configs/lstm__all__rank_10day__final__d20_h10.yaml
python -m result_evaluator --rescore ; python -m result_evaluator --rebuild-index
python -m backtest --run <run_id> --top-k 20 --draws 200
```

⚠️ **Cost drag halves against h=5 and doubles against h=20** — 8.8 %/yr at `τ=0.70`,
50 bps. That is the whole reason h=10 is worth a run and h=5 is not.
⚠️ **Add the hand screen as the baseline in the same backtest.** §5 rule 4's shape: a
model that does not beat three ranked columns has not earned its complexity.

**RESULT — 6 h 04 m selection on a T4 (20 draws) + ~25 min of local chain.**

| stage | number |
|---|---|
| selection | `ic_mean` **+0.1201**, p95 bar +0.0355, null max +0.0357 (below observed), **z = +13.78** — higher than h=20's +9.09, and `n_eff`/fold **76.6** against 38.1. 61 of 90 kept, 19 shortlisted, same channel at #1 (`drv_order_vol_imb`) |
| model | `lstm__all__rank_10day__final__d20_h10__20260819-163848`, 4m 31s. Test IC **+0.1393**, `ic_t` +8.19, **85.8 %** of days positive, `mase` **0.9874** ✅, R² +0.011 |
| backtest, top-20, 63 periods | **Sharpe +2.442 @30 bps**, CAGR +43.8 %, `se` 0.251, max_dd −7.2 %, **z = +8.99** ✅ |
| **vs the 3-channel hand rule, ONE panel** | hand **−0.263** @30 (z = −1.72 ❌). **ΔSharpe +2.71, paired `t` = +5.94** (ρ 0.74) |

⚠️ **THE HAND RULE SCORING −0.26 IS NOT A CONTRADICTION OF §8g.** Its +0.652 is over
2018-2026; this window is 2023-11 onward, inside the regime §8g itself measured at **+0.011**
(2022-2026). It is doing exactly what §8g said it does after 2022.

⚠️ **AND THAT ANSWERS `PRF-3` — the break is in the FEATURES, not the market.** Same window,
same universe, same `h=10`: 19 selected channels return +2.44, three hand-picked ones return
−0.26. Hypothesis (2) of PRF-3, not (1). The market did not stop being predictable after
2022; **those three columns stopped predicting it.**

⚠️ **h=10 BEATS h=20 WHILE PAYING DOUBLE THE FEES** (8.8 %/yr against 4.4 %) — Sharpe@30
+2.442 against +1.441 on the same universe, architecture and test window. ⚠️ **Do not
promote h=10 on this yet**: one split each, `se_sharpe` ~0.25, and h=20 has a 10-fold
walk-forward behind it while this has none. **The h=10 walk-forward is the run that settles
it**, and `src/walkforward/` already does it in one command.

⚠️ **A DEFECT WAS FOUND AND FIXED ON THE WAY — see `PRB-1` in ISSUES.md.** Two Kaggle probe
runs had been merged into the CHAIN's report root, where `final_features` groups them with
the real runs. `backtest/CONTEXT.md` §9.

### ✅ PRF-9 · DONE 2026-08-19 — `pool__ta` changes the SHORTLIST and not the MONEY

> ⚠️ **BLOCKED, and DEMOTED FROM #1 ON 2026-08-19 FOR THAT REASON.** This is the largest
> upside left in the repo — the 13 channels are the survivors of **90 candidates, not of
> 800** — and none of it can start today. Two hard blockers, in order: **`P1-2`** (half a
> day, without it a multi-pool cross-sectional run cannot be expressed at all) and
> **`MEM-1` / `P3-2`** (711 + 90 channels is ~8× a design that already peaked at 16.3 GB
> host RAM). The file's own rule — *a thing that unblocks hours of other work outranks a
> thing that is only itself* — puts `P1-2` ahead of this item, not inside it.

Surveyed 2026-08-19, because "we have much more data in gold, can it add features?" is
the obvious next question and the answer is structural rather than a matter of trying.

**⚠️ A CROSS-SECTIONAL RANK CANNOT USE A DATE-ONLY COLUMN.** `cs_rank_{h}day` ranks stocks
WITHIN a date. A column identical for every ticker on that date has a **constant within-date
rank**, so it carries no information about WHICH stock to buy. Measured, not argued:

| pool | sampled columns with cross-sectional variation |
|---|---|
| `pool__basic` | **12 of 12** ✅ |
| `pool__ta` | **12 of 12** ✅ |
| `pool__fa` | 8 of 12 (the misses are `year`, `quarter`) |
| **`pool__economy_vietnam`** | **0 of 12** ❌ |

**Of 76 gold tables, 71 are date-only** — all 19 `economy_*`, all 48 `forex_*`, `bonds`,
`funds`, `stock_market`, `market_breadth`. ⚠️ **That is ~4,500 channels that are
structurally incapable of ranking a cross-section**, and it explains the shortlist
composition without appealing to their being weak. They can only enter through
INTERACTIONS (macro × beta, macro × sector), and nothing in the pipeline builds one.

⚠️ **This is consistent with, but not the same as, the 2026-08-17 six-pool sweep.** That
tested `stock_market`/`bonds`/`news_daily`/`market_breadth` on a SINGLE-SERIES VCB target,
where a date-only column does vary — over time — and they failed anyway (z = +0.19…+0.53).
Two different arguments, one conclusion.

**Only five gold tables are PER-TICKER**, and this is the whole candidate list:

| source | shape | status |
|---|---|---|
| **`stocks_ta` → `pool__ta`** | **711 numeric channels**, 2,381,858 × 777 tickers | ✅ built on `all`, **0 name collisions** with `pool__basic`, coverage median **0.992** (7 of 40 sampled below 0.95). ⚠️ ends **2026-06-26** (`STA-1`) |
| `news_daily_panel` | 26 channels, 2.06 M rows, per-ticker | ⚠️ `pool__news_daily` exists **for VCB only** — must be built for `all`. Prior is weak: z = +0.53 at layer 1 |
| `stocks_financials_bank_fa` → `pool__fa` | 1,150 cols | ⚠️ **banks only** — ~20 of the 150 names, so ~87 % NULL on this universe |
| `stocks` | 41 cols | already the source of `pool__basic` |
| `news_weekly_panel` | 28 cols | weekly grain; the news thread is closed |

**So the prize is `pool__ta`: 711 channels against `pool__basic`'s 90, an 8× widening of
the only feature space that can rank.**

⚠️ **AND IT HAS NEVER BEEN OFFERED TO THE CROSS-SECTIONAL SELECTION — `CSP-1`.**
`cross_sectional.read_universe_panel` is ONE hand-written SQL statement reading
`pool__basic ⋈ pool__targets`. No `--pools` value, no notebook parameter and no config key
routes around it. **The 13 channels are the survivors of 90 candidates, not of 800.**

**Order of work, and each step is a real blocker for the next:**

1. **`P1-2` / `PNL-2` first** ⏱ half a day — derive grain from the panel's own ticker
   count. CLAUDE.md already states this "partly dissolves `CSP-1` for free": the `else`
   branch then reads via `reader.join(pools)`, making `--pools pool__basic,pool__ta` a real
   cross-sectional run.
2. ⚠️ **`MEM-1` becomes the wall, and P3-1 says so in advance.** 711 + 90 channels over
   624,448 rows is ~8× the design that already peaked at **16.3 GB host RAM** on 90
   channels (P1-4b). Straight-lining that is ~130 GB. **The blocked ranker (P1-4) fixed the
   VRAM half only.** So `pool__ta` must be PRUNED before it is selected over — a coverage
   screen plus a correlation prune, offline, is the cheap version.
3. **Then the selection.** At ~800 channels the fitted cost model gives ~5.4× the 90-channel
   run's 6 h 07 m ≈ **33 h at 20 draws**, above Kaggle's 30 GPU-h/week. **Run 10 draws**
   (§5's rule: 10 to fail, 20 to pass) ≈ 16 h, and pay for 20 only if it looks like passing.

⚠️ **`STA-1` is the tax on all of it**: `pool__ta` stops 2026-06-26, so an INNER join drops
the chain's last 31 sessions — the same 4,266 → 4,235 it already costs the VCB chain.

### ✅ SHIPPED 2026-08-19 — and BOTH memory walls are now MEASURED, not extrapolated

| shipped | |
|---|---|
| `feature_selection/prune.py` | a **LABEL-FREE** chooser: coverage ≥ 0.95, then \|Spearman\| redundancy at 0.90, then a deterministic budget cut. ⚠️ Ranking channels by correlation with the TARGET would build `PRF-7`'s look-ahead into the candidate set *before* the selection ran, where no null could price it — a test pins that adding a label column changes nothing. 8 tests |
| panel export takes extra pools | `data.panel.pools = {"pool__ta": [...]}`, joined server-side for the same reason the base panel is (`CSP-1`) |
| `pool__ta` measured | 711 numeric (+208 boolean flags, deliberately not offered) → **671** on coverage → **405** on redundancy @0.90 (284 @0.80) |

**The memory model, fitted on TWO measured points** (90 ch → 16.3 GB, 140 ch → 24.5 GB):
`peak_host_GB ≈ 1.54 + 0.164 × channels`. ⚠️ P1-4b's rule held — the single-point estimate
was close on the slope but blind to the **1.54 GB fixed cost**.

⚠️ **BUT HOST RAM IS NOT THE BINDING WALL, AND EVERY PREDICTION IN THIS ITEM SAID IT WAS.**
Attempt 1 at 140 channels: host peaked at **24.5 GB and SURVIVED**; the run died on **VRAM**
inside `XGBoosterPredictFromDMatrix` — *free 3.00 GB, requested 3.15 GB* on a 14.6 GiB T4.
The allocation is **`xgb_shap`'s SHAP contributions, `(n_rows, channels × 6 + 1)`**, so it
scales with exactly the thing PRF-9 wants to increase. This is `MEM-1` on the device side for
the **third** time (P1-4 fixed the ranker's half; XGBoost allocates outside torch's
accounting, which is why `_tick` reported 6.2 GB while ~11.6 GB was in use).

**Attempt 2, 120 channels (90 + 30) — COMPLETE, 32.6 min, and `pool__ta` DOES reach the
shortlist:**

| | |
|---|---|
| kept | **30 of 30** `pool__ta`, 60 of 98 `pool__basic` |
| shortlist | 22, of which **6 are `pool__ta`** — best at rank **#10** (`close_ema_50_200_direction`) |
| `ic_mean` | **+0.1285** against the 90-channel reference's +0.1075 |

⚠️ **DO NOT READ +0.1285 > +0.1075 AS "IT HELPS". THERE IS NO NULL.** §5 rule 1: the +9.09
bar was computed for a 90-channel configuration and says nothing about a 120-channel one — and
a wider pool mechanically has more room to fit, so the null moves UP with width. **This run is
descriptive.**
⚠️ **"30 of 30 kept" is largely an ARTEFACT of the prune**, not a quality signal: the offline
screen already removed redundancy among the `pool__ta` channels, so they arrive pre-thinned
while `pool__basic`'s 98 arrive raw and lose 38 to the selection's own correlation prune.
⚠️ The top **nine** shortlisted channels are all `pool__basic`.
⚠️ **`STA-1` cost 30 sessions, measured**: joining `pool__ta` takes the panel from 4,388 dates
ending 2026-08-07 to **4,358 ending 2026-06-26**.

### ✅ THE DOWNSTREAM TEST — DONE 2026-08-19, and the widening does NOT pay

The cheap route was taken (~35 min local, against ~8 GPU-h for a null that would not have
isolated `pool__ta` anyway). The 22-channel shortlist was built into
`rank_20day__final__d20_h20__wide`, trained with the architecture, schedule, seed, universe
and target **copied unchanged** from the narrow chain, and priced against it by
`backtest.head2head`.

⚠️ **PRICED ON THE INTERSECTION, WHICH IS THE WHOLE REASON THAT MODULE EXISTS.** `STA-1`
makes the wide chain's split land on 2023-11-03 → **2026-06-26** against the narrow one's
2023-11-15 → 2026-07-10, so reading the two `backtest_test.csv` files side by side would
compare two Sharpes over two different windows. 646 shared dates, 32 periods, top-15:

| | daily IC (shared rows) | Sharpe@30 | null z |
|---|---|---|---|
| **wide** — 22 ch, 6 from `pool__ta` | **+0.1053** (`ic_t` 3.97) | **+1.496** | +4.53 |
| **narrow** — 13 ch, `pool__basic` only | +0.0927 (`ic_t` 4.09) | **+1.623** | +5.42 |

**Paired** (ρ **0.90**): ΔSharpe **−0.126**, `t` = **−0.29** at 30 bps (−0.28/−0.31 at
20/50).

⚠️ **SO THE EXTRA CHANNELS MOVED THE SHORTLIST AND NOT THE MONEY.** The wide model ranks
*slightly better* (+0.1053 vs +0.0927 on identical rows) and earns *slightly less*, and both
gaps are inside the noise. That is a tie, and a tie is the answer: **offering `pool__ta`
bought nothing tradable.**

⚠️ **READ WITH `PRF-8` — TOGETHER THEY CLOSE TWO OF THE THREE OBVIOUS LEVERS.** A model
101× smaller ties (PRF-8); 30 more candidate channels tie (here). **The 13 original channels
are the result**, and what is left is not a better model or more of this data — it is
`PRF-4`/`PRF-5` (honest execution) and `PRF-6` (new information).

⚠️ **What this does NOT say.** Only **30 of the 405** pruned `pool__ta` channels were
offered, because `VRM-1` caps a run at ~120 channels — so this is *"these 30 did not pay"*,
never *"`pool__ta` is useless"*. One split, 32 periods, `se_sharpe` ~0.25. The wide selection
carries **no null**, so its +0.1285 was never evidence and is not treated as any here.

⚠️ **`--root` + `--scope` were BOTH needed and neither alone would do.** The wide run shares
setup keys with `PRF-7`'s probe, so a shared root would have UNIONED them (`PRB-1` again);
and without `--scope wide` the plan wants `rank_20day__final__d20_h20` — the name the
chain's own table already holds, which `--replace` would have destroyed.

⚠️ **THE "8× WIDENING" IS NOT AVAILABLE ON A T4 AND THAT IS NOW A MEASUREMENT.** 90 + 405
pruned channels is ~83 GB of host RAM and blows VRAM long before that. The reachable width is
~120 channels per run, i.e. **~30 of `pool__ta`'s 405**. Finishing PRF-9 therefore needs one
of:

| route | ⏱ | what it buys |
|---|---|---|
| **a 20-draw null at 120 ch** | ~7-8 h Kaggle | turns the +0.1285 into evidence or kills it. Does NOT isolate `pool__ta`'s contribution |
| **the downstream model test** | ~30 min local | build the 22-channel table → dataset → LSTM → backtest, and pair it against the h=20 model on ONE panel (`backtest.handscreen`'s shape). **Answers the actual question — does the widening pay?** ⚠️ needs the probe promoted to a chain root, or a `--root` run |
| slices (layer 1 + layer 2, §3c) | ~2 h + ~6 h | offers all 405 across ~13 chunks. ⚠️ §3c's own warning applies: a layer-1 union is arithmetic, not consensus |

---

⚠️ **`PRF-7` WAS THE PRECONDITION AND IT IS NOW DISCHARGED (2026-08-19)** — but only for
90 candidates. Widening the pool from 90 to 800 makes selection look-ahead WORSE, not
better: more channels is more opportunity for the selection to have fitted the test folds,
and PRF-7's measured overlap (51 of 61 kept, Jaccard 0.761) was measured **at the current
width**. ⚠️ **So the pre-2017 comparison has to be re-run at the new width**, not assumed
to carry over — it is one extra Kaggle job on the same job definition, and it is what
keeps a +1.991 defensible after the widening.

### P17 · ⚠️ The regime question — **PARTLY ANSWERED by `PRF-1`, and the answer flipped** ⏱ ~1 day  ·  *(was `PRF-3`)*

⚠️ **UPDATE 2026-08-19.** This item was written when three independent measurements all
found the edge dying after 2022. **PRF-1's walk-forward found the opposite at h=20**:
2023/2024/2025 score **+2.64 / +0.90 / +1.39** against markets of +1.57 / +0.35 / +0.94,
and 2022 is the only bad fold — a year the equal-weight universe itself ran Sharpe −0.94.

So the break is **not** universal. It is present in the h=5 and h=10 HAND screens and
absent in the h=20 MODEL. Two candidate reasons, and they are separable: the **horizon**
(consistent with §2a-bis and with everything else measured this week) or the **feature
set** (13 selected channels against 3 hand-picked ones). **PRF-2 separates them** — it runs
the real chain at h=10, holding the feature pipeline fixed and moving only the horizon.
Run PRF-2 before the training-window experiment below; it is cheaper and it may make it
unnecessary.

The original framing, still valid for the hand screens:


Three independent measurements now find the same break at the same place:

| study | pre | post |
|---|---|---|
| `model/CONTEXT.md` §11 (h=5, foreign flow, 28 folds) | net@20bps **+1.46** (2017-20) | **−0.51** (2022-26) |
| the h=5 hand screen, 2026-08-19 | Sharpe **+1.104** (2018-21) | **−0.099** (2022-26) |
| the h=10 hand screen, 2026-08-19 | **+1.671** (2018-21) | **+0.011** (2022-26) |

⚠️ §11 already tested **rolling vs expanding training at h=5 and it did not help** —
rolling *lowered* AUC (0.513 vs 0.520). So "stale training data" is not the explanation
there. **Untested at h=10 and h=20**, which is the gap.

Two hypotheses that make different predictions, which is what makes this worth running:

1. **The market changed** (more retail flow, more index products, tighter spreads) — then
   *no* feature set trained on 2018-21 works post-2022, and rolling retraining does not
   rescue it (§11's result, one horizon up).
2. **The FEATURES decayed** — order-flow imbalance from daily order COUNTS is a crowded,
   widely-visible signal by 2022. Then a *different* feature set still works, and the
   answer is §2d's ladder, not a longer training window.

**Distinguishing test**: train on 2022-2026 only and score 2022-2026 by walk-forward. If
even a model that has only ever seen the new regime cannot find an edge in it, that is
hypothesis 1 and the honest conclusion is that this data cannot be traded now.

### P12 · ⚠️ Execution realism — the remaining fictions ⏱ ~1 day  ·  *(was `PRF-4`)*

Each is a way the backtest is still kinder than the market. Ordered by expected damage:

| gap | why it matters | measured? |
|---|---|---|
| **ADV / size cap** | a 20-name book at real size moves a VN mid-cap. `pool__basic.value_matched` is on hand, so cap each position at a fraction of it and re-run | ❌ |
| **floor days on the SELL side** | the ceiling exclusion covers ENTRY only. A name at its floor on the exit date cannot be sold either, and a loser is exactly when that happens — so this is biased against the strategy in the direction that matters | ❌ |
| **the ATC auction** | signals built from full-day order counts settle only after close; but a partial-day version could be submitted into ATC. That recovers part of the ~19 pp/yr the t+1 lag costs at h=5 | ❌ |
| **the ceiling exclusion is a PROBE, not a default** | `PRF-0` measured it and the model survives (+1.484 → **+1.551** test), but `backtest.portfolio` still applies no exclusion, so the next run reproduces the untested number. Needs `exchange` on the panel, which `build_panel` does not carry. ⏱ ~1 h — **the cheapest row here, and the only one already measured** | ✅ measured, ❌ not shipped |
| ~~**`se_sharpe` on the h=20 cell**~~ | ✅ **CLOSED by PRF-1, 2026-08-19** — the walk-forward produced **118 periods** and `se_sharpe` **0.155**, against the single split's 32 and 0.256. Fixed the way it was predicted to be: more OOS periods, not a wider window | ✅ 2026-08-19 |
| **max drawdown −55 to −58 %** | at every `k` on the h=10 screen. Statistically tradable ≠ holdable; a vol target or a market-regime filter is the standard answer and neither is tested | ⚠️ known |

### P16 · Survivorship — the one bias that flatters a momentum screen ⏱ ~2 days  ·  *(was `PRF-5`)*

`silver.stocks_basic` holds **no delisted name** (§2c). A screen that buys recent winners
is the strategy most flattered by that, because the names that crashed out are absent.
⚠️ **The null is protected** (every shuffled draw picks from the same survivor basket) but
**the CAGR is not** — so `z = +4.72` stands while `+14.9 %/yr` does not.

Fix is data, not code: a point-in-time listing/delisting table. Related to §2d's
"point-in-time index membership" lever, and it makes PRF-1's fold series interpretable.

### P18 · New information — the only lever §2d says is left ⏱ months  ·  *(was `PRF-6`)*

Ranked by expected impact **on this specific problem**, which differs from §2d's original
single-stock ranking:

1. **Intraday / tick data.** ⚠️ The measured 5-day signal decays inside ONE SESSION —
   +24.4 % CAGR same-close against +5.6 % at t+1. Trading it intraday is not an
   improvement, it is the difference between a strategy and a curiosity. It also gives
   §2d's true #1, aggressor buy/sell imbalance, of which daily order COUNTS are a proxy.
2. **Point-in-time listing status** — **`P16`** *(was PRF-5)*.
3. **Fundamentals with filing dates** — `experiment_4` already recovered VCB's publish
   dates, so the method exists for one name and needs scaling.
4. ~~News / sentiment~~ — **closed**, see the Closed table. `pool__news_daily` measured
   z = +0.53 at layer 1.

---

## Retired band A *(was the `P2` tier)* — a number you already have is wrong or unreadable

⚠️ **RETIRED AS A BAND 2026-08-21 — it holds only DONE blocks now, and it was ALREADY
EMPTY of open items before that.** Its rule is the one that puts `P3` and `P4` at the
head of the list: *a thing that makes a number you already have wrong or unreadable
outranks a thing that would give you a new number.*

⚠️ **The band was empty as of 2026-08-20, and that was a claim, not an oversight.** `P0-1 … P0-6`
closed 2026-08-17/18; **`P0-7`** (CLAUDE.md §6-0 headlining the one-split chain) was fixed
and, being a documentation-staleness item whose reasoning is not evidence, was **deleted
per this file's own rule** rather than archived; **`P4-12`** (`mase` never computed on a
panel) closed 2026-08-19 and **moved to the Archive**, because CLAUDE.md §6-0-b cites that
block by name as the place a wrong recorded prediction is kept.

⚠️ **Nothing currently known makes a quoted number wrong.** The nearest candidates are
deliberately NOT here: `WFO-1` is a way to DESTROY a number rather than misstate one, and
`PRF-7`-at-h=10 measures how optimistic a level is rather than showing it to be wrong.
⚠️ **`P3` and `P4` are the closest thing to a band-A item on the page** — neither makes a
number *wrong*, both make one **unreadable as stated**, which is why they lead the list
ahead of every larger prize. **If you find something that is genuinely wrong, it outranks
`P3` and becomes the new `P3`** — renumber, and add a crosswalk row.



## Retired band B *(was the `P3` tier)* — unblocks hours of other work

⚠️ **RETIRED AS A BAND 2026-08-21.** Its two remaining open items were renumbered into the
one list at the top — `P1-1` → **`P19`** (the cost model) and `P1-4b` → **`P20`** (the
host-side peak, ⚠️ *merged* there with `P3-2` and `VRM-1`'s host half). **Their detail
blocks are still below, under their new codes.**

⚠️ **The DONE blocks in this band keep their old codes and are not going to move.**
`P1-8` (`WFO-1`) and `P1-9` (the Sharpe estimand) both shipped 2026-08-21 and are recorded
as DONE blocks rather than deleted, because each carries a REJECTED alternative and a
newly created limit that a future session must not rediscover. `P1-2` (`PNL-2`) shipped
2026-08-19; `P1-3`/`P1-4`/`P1-5`/`P1-6`/`P1-7` are done and live in the Archive; `P4-11`
was promoted in from band E and closed.


### ✅ P1-9 · DONE 2026-08-21 — and the Sharpe test disagrees about three of six arms

`compare.paired()` now returns BOTH estimands, each with its own interval, by reusing
`pair.block_bootstrap_diff` rather than writing a second one. Numbers in **CLAUDE.md
§6-0-ter-2** and **`walkforward/CONTEXT.md` §11c** — do not re-derive them here.

| what changed | |
|---|---|
| the column formerly `t_paired` | is **`t_ret`**, and it always was a MEAN-RETURN test |
| `d_sharpe` | carries `sh_ci_lo` / `sh_ci_hi` / `p_sharpe` from a PAIRED circular block bootstrap |
| `ac1` | printed per row — the lag-1 autocorrelation of the difference, which is what `--block` has to cover (it ran −0.09…+0.06, so `block=2` did no hidden work) |
| the verdict | **"three lose" was a mean-return claim; on Sharpe only `cnn` does.** `gbt` GAINS at a nominal p = 0.044 that does not survive six arms |

⚠️ **A defect in the ported code was found by the test that compares an arm with itself**:
`pair.summarise`'s two-sided p could return **2.0** on exact ties at zero. Clipped; it never
fired on `P2-4`'s published numbers because two strategies never tie exactly. **`BOO-1`**.

⚠️ **Left undone deliberately**: the h=20 `PRF-8` sweep is **not** re-scored, so §6-0-ter's
ties are still mean-return only. That is item 1 in START HERE and costs ~5 minutes.

### ✅ P1-8 · DONE 2026-08-21 — `WFO-1` closed by a REFUSAL, and `RPR-1`'s half with it

Numbers and the rejected alternative in **`walkforward/CONTEXT.md` §14**. One line each:

| | |
|---|---|
| the fix | `walkforward/manifest.py` — `run.main` CLAIMS the directory before a fold is built; the rename to `results/walkforward/<ticker>__<table>/` was **rejected** because five tracks are cited BY PATH in three registers |
| verified | the exact command that nearly destroyed `PRF-1` now exits in **< 1 s**, before any GPU |
| legacy tracks | protected via `folds.csv`'s run names — ⚠️ **on the TABLE only**; knobs are not recorded there and are not inferred |
| the scoring half | `evaluate` and `compare` DERIVE `--horizon` from the track and raise on disagreement; `compare` also refuses two horizons and points at `pair` |
| `RPR-1`'s half | `folds.csv` + `per_fold.csv` negated — **26 files, 41 KB**; `predictions_oos.csv` stays ignored at **323 MB** for 18. `git ls-files --others --exclude-standard results/` went 0 → 26 |

### ✅ P4-11 · DONE 2026-08-21 — the layer-2 detection is scoped to the chain being asked about

`_layer2_runs` scanned every run folder under the root and returned any whose
`outstanding.csv` named ANY `pool__shortlist__*`, with no term for schema, target or
horizon — while `status_selection_2` had already computed the right pool name and simply
did not pass it. **One argument.** Measured before and after:

| asked about | before | after |
|---|---|---|
| `return_5day__final__d20_h5` (VCB) | 2 runs | **1** — its own |
| `rank_20day__final__d20_h20` | 2 runs ❌ *(a vcb/return_5day/d20_h5 run)* | **0** |
| `rank_10day__final__d20_h10` | 2 runs ❌ | **0** |

⚠️ **The match is EXACT when a pool is named**, a prefix only when it is not — so the
unscoped call keeps its old meaning for a caller that genuinely wants "any layer 2", and
`…__d20_h20` cannot match `…__d20_h200`.

⚠️ **Why it mattered**: `RUNBOOK.md` §8 rule 1 makes `python -m pipeline` the gate on
quoting any number, and a gate that answers about the wrong experiment is worse than no
gate. It is also why RUNBOOK §3a had to warn readers off `pipeline` for the
cross-sectional chain in two separate blocks.

⚠️ **The sibling trap in the same table is NOT fixed and is working as designed**: the
`model` row keys on `--config`, not on `--table`, so without one it reports the DEFAULT
chain's run as up to date. Pass `--config`.

**The original entry, kept:**

### ~~P4-11~~ · `pipeline` CALLS ANOTHER EXPERIMENT'S RUN `up to date` — promoted from P4 2026-08-19 ⏱ ~2 h

⚠️ **`pipeline`'s `selection_2` ROW DESCRIBES A DIFFERENT EXPERIMENT AND CALLS IT
`up to date`.** Measured 2026-08-18: `python -m pipeline --ticker all --table
rank_20day__final__d20_h20` reports *"2 layer-2 run(s) over
`pool__shortlist__rank_20day__d20_h20`"* and then names
`2026-08-17_011642__vcb__shortlist__return_5day__d20_h5__return_5day` — a different
schema, a different target and a different pool. The layer-2 detection is not scoped to
the chain being asked about, **so a stage that has never run for this chain reads green.**

⚠️ **Related trap in the same table, working as designed but worth knowing:** the
**`model` row keys on `--config`, not on `--table`**, so without one it reports the
DEFAULT chain's run as up to date — pass `--config`, or `--apply` will skip the model
stage while saying everything is fine.

**Why it is P3 and not P4:** `RUNBOOK.md` §8 rule 1 makes this command the gate on
quoting any number (*"`python -m pipeline` must show `up to date` for every stage below
the one you are quoting"*), and RUNBOOK §3a already has to warn readers off `pipeline`
for the cross-sectional chain in two separate ⚠️ blocks (✅ **both removed 2026-08-21**). A gate that answers about the
wrong experiment is worse than no gate. ⚠️ It is also the reason the cross-sectional
chain is run command-by-command instead of through `--apply`.

### ✅ P1-6 · `FNM-1` DONE 2026-08-19 — the shortlist is representation-INVARIANT, 12 of 13

The selection scored the 13 channels under `feature_normalize=cs_rank` (each channel
ranked within its date before the window); `train_test_creator` feeds the model those
channels standardised **globally**. Same shape as `RNK-1`, one level over: the label was
the mismatch there, the FEATURES are the mismatch here.

⚠️ **It does not invalidate the model's own number** — that is measured on its own splits
under its own representation. It weakens the sentence *"built on a shortlist that cleared
z = +9.09"*, because §5 rule 1 says a bar computed for one configuration says nothing
about another.

⚠️ **THE COST ESTIMATE MOVED DOWN, 2026-08-19.** This item was written *"~1 h GPU"* before
panel mode existed; the run it actually needs is the `cross-sectional` Kaggle job with
`RUN_NULL=false`, because **the kept SET is the measurement here and not its bar**. `PRF-7`
measured that shape at **10m 34s** on 44 % of the dates, so the full sample is ~25 min plus
the ~5 min queue.

⚠️ **Which side moves is not obvious**, which is why nothing was changed: per-date ranking
in `train_test_creator` changes every panel dataset, and dropping it from the selection
throws away the argument in `cross_sectional.py` §3. **Re-run the selection with
`feature_normalize=none` and compare the kept set** — if the 13 survive, the question is
moot and the sentence is safe.

**RESULT — `cross-sectional-fnm` on a Kaggle T4, 22m 04s round trip. The 13 survive.**

Identical to the `cross-sectional` job in **every** recorded setup key except one —
`lookback_d`, `horizon_h`, `normalize`, `corr_threshold`, `n_splits`, `min_train`,
`random_state`, `selector_class`, `methods`, `design_dtype` and even `env_fingerprint`
(`b899d1bd4ec0`, the same Kaggle image) all match. Same payload, same 150 names, same
4,368 labelled dates, `n_eff_per_fold` 38.1 both sides.

| | `cs_rank` (the selection's) | `none` (the dataset's) |
|---|---|---|
| kept | 61 of 90 | **60 of 90** — overlap **53**, Jaccard **0.779**, **+5.90 sd** above chance (40.7 expected) |
| **REF's 13 shortlisted channels** | — | ⚠️ **12 of 13 are KEPT**; only `n_sell_orders` is not |
| shortlist | 13 | 24 (the cut is measured per run) — overlap 8, **+3.07 sd** |
| top of the shortlist | `drv_order_vol_imb` | **`drv_order_vol_imb`** — the same channel #1, and REF's top 5 are all re-shortlisted (#1 #3 #4 #2 #5) |
| `ic_mean` (selected) | +0.1075, sd 0.0342, trend **+0.0054** | **+0.1215**, sd 0.0307, trend −0.0018 |

⚠️ **THE SENTENCE IS SAFE, AND FOR A NARROWER REASON THAN "IT PASSED".** What this
establishes is that the CHANNEL SET does not depend on the representation — the shortlist
is not an artefact of ranking each feature within its date. It does **not** transfer the
BAR: `z = +9.09` was computed under `cs_rank`, this run carries **no null** (deliberately —
the kept set was the measurement, which is what took it from ~6 h to 22 min), and §5 rule 1
still says a bar computed for one configuration says nothing about another. **So: the
shortlist is representation-invariant; the +9.09 remains a `cs_rank` number.**

⚠️ **`none` SCORES HIGHER, NOT LOWER — +0.1215 against +0.1075**, on the same folds and
nearly the same channels. Two things follow and neither was expected: the `cs_rank` feature
normalisation is **not** what is doing the work, and the model's global standardisation is
not a handicap it has been carrying. ⚠️ Do not read the +0.1215 as a result — it has no
bar.

⚠️ **THE FOUR NEAR-MISSES ARE THE SAME TIE-BREAKING AS `PRF-7`, NOT A DISAGREEMENT.** Of
the five of REF's 13 that `none` did not shortlist, four are still KEPT and two have a
family twin ranked higher (`drv_parkinson_5` → `drv_parkinson_21`, `drv_dist_from_high_63`
→ `drv_dist_from_high_252` at #3). The correlation prune treats them as interchangeable and
the representation moves which representative wins, not which family does.

⚠️ **`close_adjust` slips from ensemble rank 38 to 45** and drops off the shortlist while
staying kept. That is the one channel with a mechanism for it: it is a price LEVEL, so
under `cs_rank` it becomes a clean within-date SIZE factor and under `none` it is a raw
level carrying era structure. `PRF-7` found it stable across the pre-2017 WINDOW; this
finds it the most representation-SENSITIVE of the 13.

### P19 · Re-fit the cost model into ONE function ⏱ ~2 h  ·  *(was `P1-1`)*

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

### P20 · Cut the host-side peak so the top-300 panel fits ⏱ days  ·  *(was `P1-4b`, merged with `P3-2` and `VRM-1`'s host half)*

⚠️ **THE FIRST EXTRAPOLATION HERE WAS WRONG AND THE SECOND MEASUREMENT KILLED IT.** It
read *"~1.5 GB of the smoke run's RSS is data over 48,521 rows, so the top-300 panel is
25.7× the rows → ~39 GB"*. The top-150 run then ended phase 4 at **11.0 GB on 624,448
rows**, and a straight line through both points predicts **20.6 GB** for top-300 — under
the box, not 10 GB over it. **One point does not fit a line**, and scaling a peak from a
tiny panel treats a large fixed cost as if it were per-row.

⚠️ **AND THE SECOND FIT IS NOT TO BE TRUSTED EITHER, FOR A DIFFERENT REASON: `rss` is
sampled BETWEEN phases.** The top-300 run died *inside* phase 4, so whatever killed it
was never printed. `selector._tick` now also reports `peak=` — the OS high-water mark
(`peak_wset` / `ru_maxrss`) — which is the number that decides whether a run survives.
✅ **AND THE 2026-08-18 NULL RUN REPORTED IT.** On top-150 the phases read `rss=11.2G`
but **`peak=16.3G`** — the high-water mark is **45 % above where the run settles**, and it
is reached inside `rank (the ensemble's methods)`, which is exactly where the top-300 run
died and exactly what an end-of-phase sample could never see. `window design` has the same
shape: 7.3 G settled, **10.8 G peak**. Doubling the rows puts top-300's peak at
**~28-30 GB against a ~29-30 GB box**, so that kill is now explained by a measurement.

⚠️ **Both earlier extrapolations were wrong, in opposite directions, and for the same
reason: each scaled a quantity that was not the binding one** (~39 GB from one tiny panel,
then ~20.6 GB from settled RSS). **top-300 needs the streaming design — which is THIS item, `P20`, the code `P3-2` was merged
into — not a trim.**

## Retired band C *(was the `P4` tier)* — new measurements worth having

⚠️ **RETIRED AS A BAND 2026-08-21.** Its one remaining open item was renumbered — `P2-2`
→ **`P21`**, detail block still below under the new code. `P2-1`, `P2-3` and `P2-4` are
DONE and keep theirs.

### ✅ P2-4 · DONE 2026-08-20 — paired on the CALENDAR, and the answer SPLIT

`src/walkforward/pair.py`, 48 s. Both strategies hold a book on all 2,360 shared sessions,
so their DAILY net-return series pair date by date (ρ = **0.723**) even though 236 and 118
periods cannot. At 30 bps, h=10 − h=20:

| estimand | Δ | Newey-West | bootstrap 95 % CI | |
|---|---|---|---|---|
| **mean return/yr** | **+17.0 pp** | t = +3.53, p = 0.0004 | [+8.6, +25.7] | ✅ significant |
| **Sharpe** | +0.44 | — | **[−0.079, +1.041]** | ❌ not established |

**h=10 is a higher-return, higher-VOLATILITY track.** The chain stays at h=20 — not because
h=10 lost, but because it has not won the test that matters. ⚠️ **A non-significant ΔSharpe
is not evidence of equality**: the CI reaches +1.04, so this is UNDERPOWERED, and the honest
next move is more OOS sessions, not a third test. Full numbers: `walkforward/CONTEXT.md` §10.

**Two defects the reconciliation caught**, both now recorded: the return matrix was pivoted
from the TRACK (2.21 % of cells missing → a 0 % return booked on days a held name was not
scored), and **`BKT-1`** — the backtest rebalances on a CALENDAR grid while `return_{h}day`
steps `h` ROWS of the ticker. `BKT-1` is measured at −0.015 Sharpe (h=20) and −0.038 (h=10),
i.e. every published figure is slightly CONSERVATIVE, and no conclusion moves.

⚠️ **And the first version of the module reported two tests that looked like they
disagreed** — Newey-West p = 0.0002 beside a bootstrap p = 0.067. They were testing the MEAN
and the SHARPE. **Two tests are only a cross-check when they test the same thing.**

**The original framing, kept because it is the reason the tool exists:**

### ~~P2-4~~ · Nothing can PAIR two horizons, and the old TODO assumed something could ⏱ ~1 day

⚠️ **Opened 2026-08-20.** The retired item above promised *"`walkforward.compare` pairs the
two tracks"*. It does not, and cannot: it pairs ARMS inside one sweep — arms that trade the
same dates out of the same panel, which is what makes ρ = 0.88 and a paired `t` meaningful
(`walkforward/CONTEXT.md` §8a). **h=10 and h=20 produce 236 and 118 periods over different
holding intervals**, so there is no period-wise correspondence to difference. The measured
gap — Sharpe@30 **+2.531 vs +1.991** — is two independent estimates at `se` ~0.13-0.16, not
a tested difference, and the repo has been burned by exactly that distinction (§5c: eleven
architectures, spread 0.227, one error bar).

**Three candidate designs, cheapest first:**

| | what | what it costs |
|---|---|---|
| **daily-return series** | Stop comparing PERIOD returns. Both strategies produce a daily P&L series over the same 2,383 dates; difference those and use Newey-West with a lag of `max(h)` to price the overlap. Pairs on the calendar, which the two DO share | ~half a day, no new runs |
| **block bootstrap on dates** | Resample date-blocks of ≥ 2h, rebuild both tracks inside each draw, difference the Sharpes. Makes no distributional assumption and handles the unequal period counts directly | ~1 day, CPU |
| **run h=20 at h=10's cadence** | Rebalance the h=20 signal every 10 sessions with a 20-day holding overlap. Then the periods DO correspond, but it is a third strategy, not h=20 | ~half a day + a design decision |

⚠️ **Until one of these exists, no register may state that h=10 beats h=20** — only that
each independently clears its own null, which both do decisively. Both `CLAUDE.md`
§6-0-bis-3 and `walkforward/CONTEXT.md` §9c are worded to that standard; keep them there.

✅ **The first design was the one built** (daily series + Newey-West), **and the block
bootstrap was added beside it rather than instead of it** — the two were meant to
cross-check, and making that work forced the estimand fix above. The third design (running
h=20 at h=10's cadence) is still unbuilt and is the one that would settle a tie.

⚠️ **It also blocks a cheaper question that looks unrelated**: `PRF-4`'s execution costs
(ADV cap, floor days on the sell side) are expected to hit **h=10 harder**, since it pays
double the fee drag — but "harder" is a difference between two tracks, which is this item.

### P21 · `cs_rank_5day` on the top ~300 by turnover ⏱ ~1 h  ·  *(was `P2-2`)*

`read_universe_panel` already takes a `tickers` list and filters in SQL, so this is a CLI
flag, not a new schema. ~1.3 M rows — ⚠️ **the same width as P2-1 v2, so assume the same
4 GiB VRAM ceiling and the same P1-3 dependency** until measured otherwise. Puts a number
against §2b's `ALL` row, which reads **"never ran — ⚠️ unverified"** at IC +0.109.
⚠️ Today's measurement says liquidity is the variable: the 5-day cross-sectional reversal
runs `t = −18.60` over all names, `−10.43` at top 300, **`−1.96` at top 100**.

## Retired band D *(was the `P3` tier)* — structural code, only pays off for runs currently blocked

⚠️ **EMPTY SINCE THE 2026-08-21 RENUMBERING, and empty is the correct state.** Its three
rows moved into the one list at the top with their text intact — `P3-1` → **`P22`**,
`P3-2` → **`P20`** (⚠️ *merged* with `P1-4b` and `VRM-1`'s host half, because the three
were one piece of work under three codes), `P3-3` → **`P23`**. The BAND's reasoning is what
survives and it still orders the list: **structural code comes last because it only pays
off for runs that are currently blocked anyway.**

---

## Retired band E *(was the `P4` tier)* — hygiene, each item distorts one number or hides one failure

⚠️ **EMPTY SINCE THE 2026-08-21 RENUMBERING.** Its ten rows moved into the one list at the
top with their text intact: `P4-1` → **`P15`** (⚠️ *merged* into the `STA-1` decision it was
a consequence of), `P4-2` → **`P13`**, `P4-3` … `P4-10` → **`P24`** … **`P31`** in order.
`P4-11` and `P4-12` are **DONE** and keep their codes — they are in the Archive, and
CLAUDE.md §6-0-b cites `P4-12` by name.

⚠️ **The note this table used to carry is still true and is worth keeping**: two rows left
this band upward on 2026-08-19 because neither was hygiene by this file's own test. **A
band was never a permanent property of an item** — which is the second half of why the
bands are retired and the list is flat.

---

## Closed — recorded so they are not reopened

| what | why closed |
|---|---|
| **News sentiment scorer** (old items 7-16: annotation, LLM labelling, PhoBERT fine-tune, LIME gate, full panel) | ⛔ **Decided against 2026-08-03 and confirmed 2026-08-17.** 7 paired tests, every \|t\| < 1.3; adding news costs 2-8 pp CAGR for ΔMCC ±0.003. The one reason to continue — coverage — was tested on the top-30 most-covered tickers and did not survive. The event-count half is now `pool__news_daily` and it measured `z = +0.53` at layer 1 |
| **Silver leaf assets** (old item 17: bonds, forex, funds, indices, gics) | ✅ all five exist |
| **Gold leaf assets** (old item 18: bonds, forex, funds) | ✅ all three exist |
| **`switch_config.json` cleanup** (old items 22, 23) | ✅ moot — the file is gone (§5a); a leftover copy now RAISES |
| **`execution.finished_at = None`** in every `metadata.json` | ✅ **working as designed** ([runtime.py:329](src/utils/runtime.py#L329)) — `summary()` is called mid-run because `write_report` writes the file, and waiting for `stop()` would record a runtime of zero. `None` "rather than a guess" is §5 rule 2 at the clock. I called it a bug on 2026-08-16 and was wrong |

---

## Archive — done, kept because the reasoning is the evidence

> ⚠️ **NOTHING BELOW THIS LINE IS WORK TO DO.** Moved here 2026-08-19, in the order they
> were closed. The convention at the top of this file says a done item is **deleted**, not
> ticked, once its measurement lives somewhere permanent — and every one of these does:
>
> | block | where the measurement now lives |
> |---|---|
> | `PRF-0` | `backtest/CONTEXT.md` §8h — the ceiling table, both splits |
> | `PRF-1` | **`walkforward/CONTEXT.md`** — the full 10-fold table |
> | `PRF-7` | `walkforward/CONTEXT.md` §6.1 + the run's own `README.md` |
> | `P0-1` … `P0-6`, `P1-3/4/5/7` | CLAUDE.md §3d-bis, §6-0, §6-0-bis; `ISSUES.md` (struck through) |
> | `P2-1 v2` + its three attempts + the 20-draw null | **CLAUDE.md §2b-bis** |
>
> They are demoted rather than deleted for one reason: **several of them record a
> prediction that was written down before the run and turned out wrong** (`P0-1`, `PRF-1`),
> and the three failed T4 attempts under `P2-1 v2` are the only account of why the design
> is top-150 and not top-300. That reasoning is not reproducible from a result table.
> ⚠️ **AND THE TABLE ABOVE IS NOT UNIFORMLY TRUE, WHICH IS THE SECOND REASON NOTHING WAS
> DELETED.** Checked 2026-08-19: several registers **cite this file rather than restate
> it** — CLAUDE.md §4 gives `P0-3`'s 52 % dtype figure and then writes *"(TODO P0-3)"*,
> §3 does the same for `P0-4`'s `mkt_n_names`. For those, deleting the block here would
> leave a live cross-reference pointing at nothing. **Before deleting any row above,
> `grep` the codes across `*.md` and move what is cited.**

### ✅ P4-12 · DONE 2026-08-19 — `mase` on a panel, and **my recorded prediction was wrong**

⚠️ **THE PREDICTION BELOW IS KEPT BECAUSE IT WAS WRONG.** It reads *"the honest expectation is that it will not beat the naive on MAGNITUDE either"*. Measured: the top-150 h=20 run scores `mase` **0.9937** — it DOES beat "predict no change", by 0.6 %. CLAUDE.md §6-0-b has the full table and cites this block by name.


⚠️ **BLOCK B (`mase`, `rmsse`, `skill_score`, `beats_naive`) IS NEVER COMPUTED ON A
PANEL.** `metrics.accuracy_vs_naive` is called from `evaluate` only; `evaluate_panel` runs
`panel_core_metrics` + `panel_null_metrics` + `regression_extras` and stops. So the
top-150 cross-sectional run — **the headline result of this whole repo** — carries
`test_mase = NaN` while both VCB runs carry a number.

**Why this is P2 and not hygiene:** `mase ≥ 1` is the column **P2-3 says is the line to
quote**, and it is the one that showed the `return_5day` model losing to "predict no
change" while its `ic` looked respectable. The cross-sectional model has never been asked
that question. ⚠️ Its `r2` is **+0.0003** and its RMSE is 0.29065 against a constant
predictor's 0.29070 — which is what a `mase` of ~1.0 looks like from the other side, so
the honest expectation is that it will not beat the naive on MAGNITUDE either, and the
result stands or falls on the RANK. **That is worth measuring rather than inferring.**

⚠️ **The fix is not a copy-paste.** The `lag_h` naive reads `y_true[i - h]` and assumes
rows are **consecutive samples in date order**, which is false on a panel where each date
holds N tickers — it would have to be per-ticker. Found 2026-08-18 while fixing `ICT-1`;
nothing was broken by that fix, the gap simply became visible once the panel row was read
column by column.


### ✅ PRF-0 · DONE 2026-08-19 — the price band does NOT bite the h=20 model

Opened because `backtest/CONTEXT.md` §8f measured the hand-built 5-day screen picking
names at their daily ceiling **2.14× more often than chance**, and excluding them took
that book from +19.3 % CAGR to +7.2 %. **The stage-9 run on the model applied no such
exclusion**, so the repo's headline number was suspect.

**It survives.** Measured on `lstm__all__rank_20day__final__d20_h20`, ceiling =
`day_ret ≥ 0.93 ×` the exchange band (HOSE 7 % / HNX 10 % / UPCOM 15 %):

| | universe at ceiling | model's top-15 | ratio | Sharpe as reported | **buyable only** |
|---|---|---|---|---|---|
| val | 3.76 % | 4.95 % | 1.32× | +1.7367 | **+1.7385** |
| test | 1.83 % | 2.46 % | 1.34× | +1.4845 | **+1.5512** |

⚠️ **The bias is real but small, and removing it IMPROVES the result** (test +1.484 →
+1.551). Two reasons, and both are the point: a 20-day model is not chasing one-day
spikes the way a 5-day momentum rank is (1.33× against 2.14×), and at k=15 of 150 a ~2 %
ceiling rate touches ~0.4 names per rebalance. **The band is a 5-day problem, not a
20-day one** — which is one more instance of the horizon being the variable.

**Left to do**: fold the exclusion into `backtest.portfolio` as an option so it is applied
by default rather than by a probe. ⏱ ~1 h. Needs `exchange` on the panel, which
`build_panel` does not currently carry.

### ✅ PRF-1 · DONE 2026-08-19 — 10 folds, and **my recorded prediction was half wrong**

**Every number in this section comes from ONE train/val/test split.** §11's regime finding
used **28 expanding folds**; the h=20 result uses one, and its test window happens to be a
+20.2 %/yr VNINDEX bull market.

The 2022-2026 rows are the reason this is P0-shaped rather than nice-to-have: at h=10 the
screen scores **+0.011 against a market −0.049**, a gap of 0.06 with an SE of difference
~0.13. **A single split cannot tell "the edge decayed" from "this split was lucky."**

**Do**: retrain every 6-12 months, expanding window, `d + h − 1` purge at each boundary,
score each fold's test block, then backtest the concatenated OOS predictions. The stages
all exist; what is missing is the loop and a run-folder convention for a fold set.

⚠️ **Predict before running** (the P0-1 discipline): if the edge is real and stationary the
per-fold Sharpe should be positive in most folds with no trend; if it is a pre-2022
artefact the fold series should decay. **My prediction: it decays, and the post-2022 folds
straddle zero.** Recorded so being wrong is informative.

**RESULT — `src/walkforward/`, 10 folds (test = calendar 2017…2026), ~35 min:**

| | |
|---|---|
| IC positive in | **9 of 10 folds** (only 2026, a 5-period stub, negative) |
| beats the equal-weight universe in | **10 of 10 folds** |
| pooled, 2,373 dates / **118 periods** | Sharpe **+1.991** @30 bps (`se` 0.155), CAGR +47.5 %, market +0.737 / +14.6 % |
| null, 200 within-date shuffles | z = **+12.18 / +12.28 / +12.46** at 20/30/50 bps, null MAX below observed at all three |

⚠️ **DECAY: RIGHT. "STRADDLE ZERO": WRONG.** Sharpe@30 slope **−0.100/fold**, first five
folds **+2.775** → last five **+1.564** (−44 %). But 2023/2024/2025 are **+2.64 / +0.90 /
+1.39**, all clearly positive and all above their market. **2022 is the only bad fold
(−0.07) and it is bad for everyone** — the universe itself ran Sharpe −0.94 that year.
The wrong half is left here rather than edited.

⚠️ **It contradicts the h=5/h=10 hand screens**, which were negative through 2022-2026
(`backtest/CONTEXT.md` §8g), and §11's regime wall. The variables that differ are the
HORIZON and 13 selected channels against 3 hand-picked ones.

⚠️ **NO MECHANICAL LEAK** — restricted to the single split's own test window the
walk-forward gives IC **+0.0849** against its **+0.0863**, agreeing to the third decimal.
The pooled 2.0 is higher than the split's 1.484 because 2017-2021 was a better period.

⚠️ **WHAT IT DOES NOT FIX: the SELECTION look-ahead.** The 13 channels were chosen on the
whole sample; only the MODEL's look-ahead is removed. Every fold's LEVEL stays optimistic
by an unmeasured amount, and the apples-to-apples check cannot rule it out because the
single split shares the same advantage. **That is now the single largest open threat to
every number in this repo, and it is PRF-7.**

⚠️ **9 of 10 folds stopped at EPOCH 1** (val loss 0.975-1.021, i.e. within ~2 % of a
standardised label's variance). With §5c's eleven architectures inside one error bar and
P2-3's identical observation on VCB: **capacity is not the binding constraint, and the
next model test should be SMALLER, not bigger.**

`src/walkforward/CONTEXT.md` has the full per-fold table.

### ✅ PRF-7 · DONE 2026-08-19 — the look-ahead is MILD, and the levels roughly stand

PRF-1 removed the model's look-ahead and left the selection's. The 13 channels behind
every result in `backtest/CONTEXT.md` §4 and the walk-forward above were chosen **using
the label over 2009-2026**, i.e. including every test fold.

**The honest version** re-runs `feature_selection` inside each fold on that fold's train
window only — ~6 GPU-h per fold on a T4, ~60 h for ten. Affordable on Kaggle's 30 GPU-h/week
across two weeks, not affordable locally.

**RESULT — `cross-sectional-early` on a Kaggle T4, 10m 34s.** Identical job in every
respect except the DATA WINDOW: dates < 2017-01-01, exactly what walk-forward fold 0 could
have seen (its train ends 2016-01-01, its val is 2016). Universe unchanged at 150 names,
same target/horizon/lookback/min_width/dtype/ensemble. Panel 273,367 × 104 over 1,995
dates against the full run's 624,448 over 4,368. `RUN_NULL=false` — the kept SET is the
measurement, not its bar.

| | full sample | pre-2017 | |
|---|---|---|---|
| kept | 61 of 90 | 57 of 90 | **overlap 51**, Jaccard **0.761**, **5.8 sd above chance** (38.6 expected) |
| shortlisted | 13 | 15 | **overlap 8**, chance 2.17 → **4.7 sd** |
| top 2 channels | `drv_order_vol_imb`, `drv_dist_from_high_252` | the same two, order swapped | ✅ |
| `ic_mean` | +0.1075 | **+0.0973** | −9.5 % on **44 %** of the data |
| Spearman of rank among the 8 shared | | | +0.571 |

⚠️ **AND ALL FIVE SHORTLIST "MISSES" HAVE A FAMILY TWIN IN THE EARLY KEPT SET** — which
makes 8-of-13 an understatement of the agreement:

| full shortlisted, early did not | twin present in early's kept set |
|---|---|
| `drv_parkinson_5` | `drv_garman_klass_5` — and the full run's own `outstanding.csv` records it as having ABSORBED that twin |
| `drv_rogers_satchell_5` | `drv_garman_klass_5` |
| `drv_order_vol_imb_21` | **itself** — kept, just not shortlisted |
| `drv_parkinson_21` | `drv_rogers_satchell_21` |
| `n_sell_orders` | `avg_vol_per_sell_order`, `drv_order_count_imb` |

**This is P0-3's phenomenon with a different cause**: the selection breaks ties among
channels the correlation prune considers interchangeable, and less data moves which
representative wins — not which FAMILY wins.

**Reading**: the shortlist is **not period-fitted**. A walk-forward that re-ran the
selection per fold would have picked substantially the same channels, so the levels in
`walkforward/CONTEXT.md` §3 roughly stand rather than being an artefact.

⚠️ **What it does NOT prove.** The early run is noisier by construction — `n_eff_per_fold`
**14.3 against 38.1**, `ic_fold_sd` 0.056 against 0.034, `ic_min` 0.012 against 0.060 — so
some of the disagreement is sample size and not window. And a stable channel SET does not
make the measured IC level unbiased; it bounds the problem rather than removing it. **The
~60 GPU-h per-fold version is no longer worth its cost**, which is what the cheap half was
for.

⚠️ `close_adjust` survives in BOTH (full #12, early #15), so the price-level-as-size-proxy
worry is not an artefact of the full-sample window either. It is a real, stable pick.

---

**The original framing, kept:**

**The cheap partial**: run the selection on 2009-2016 alone and compare the kept set with
the current 13. ⏱ ~6 GPU-h. If the overlap is high the look-ahead is mild and the levels
roughly stand; if it is low, every level in this repo is overstated and only the shapes
survive. **Do the cheap one first** — it is one run and it bounds the problem.

### ✅ PRF-8 · DONE 2026-08-19 — the architecture is worth nothing; a 2,033-parameter model ties it

> **Why it was first (2026-08-19):** the cheapest item in the section, no GPU queue, no
> export, and the only one that could change what the headline result *means* rather than
> how big it is. It cost **15m 03s** of compute and it did change the meaning.

⚠️ **Nine of ten walk-forward folds stopped at EPOCH 1** (the tenth at epoch 2), val loss
0.975-1.021 — within ~2 % of the variance of a standardised label. The LSTM takes what it
can in one pass and overfits from the second. `P2-3` recorded the same on VCB (*"best epoch
1 of 21"*), and §5c measured eleven architectures spanning 0 to 276 k parameters **inside
one error bar**, with a 25-parameter ridge among the best.

Three independent observations, one conclusion: **capacity is not the binding constraint.**
A 205 k-parameter LSTM that converges in one epoch is being paid for and not used.

**Do**: re-run the PRF-1 walk-forward with (a) the LSTM at `hidden_size=16, num_layers=1`
and (b) `model.gbt`, both on the identical folds, and compare the pooled Sharpe. The fold
machinery already exists, so this is a config change and one loop.

⚠️ **What would make it worth acting on**: if a model 100× smaller matches the pooled
+1.991, then the result is about the 13 CHANNELS and not about the architecture — which
also makes PRF-7's look-ahead the whole story rather than part of it. **A cheap model that
ties an expensive one is evidence about where the signal lives**, not just a saving.

**SHIPPED 2026-08-19 — the machinery, ahead of the result.**

| | |
|---|---|
| `walkforward --arm <package>:<config>` | repeatable. **All arms train on ONE build of each fold's tensors** — running the sweep twice would refit the scaler, the median and the coverage screen a second time, so "same data, different model" would rest on the builder being deterministic instead of on there being one dataset |
| `walkforward.compare` | scores N tracks identically and compares them **PAIRED** — see below |
| `lstm_small__all__rank_20day__final__d20_h20.yaml` | `hidden_size` 128→16, `num_layers` 2→1. **2,033 parameters against 205,441 — 101×.** A test asserts only those two keys differ from the big config |
| `gbt__all__rank_20day__final__d20_h20.yaml` | the selection's OWN estimator, hyper-parameters copied unchanged from the VCB config so this arm is not also a hyper-parameter search. **1,400 decision nodes**, and it sees 78 window statistics where the LSTM sees all 260 numbers |
| smoke, 1 fold (the largest train slice), both arms | **2m 14s** — so ten folds is ~25 min, not the ~2 h a per-arm sweep would cost |

⚠️ **THE COMPARISON HAD TO BE PAIRED AND THAT IS NOT A DETAIL.** Every arm trades the same
rebalance dates out of the same panel, so the market factor is common to all of them and
`se_sharpe ≈ 0.155` is the error bar on the WRONG quantity. An unpaired reading of two
Sharpes at that SE cannot resolve a 0.3 gap in either direction — it would call a real
difference noise and a spurious one signal. `compare` reports `t` on the difference series
`net_A − net_B`, plus the correlation that says how much the pairing bought.

⚠️ **PREDICTION, RECORDED BEFORE THE RUN FINISHED** (the P0-1 discipline; the last two
predictions in this file were wrong and half wrong, which is why they are still here):
**both cheap arms tie the 205 k LSTM — `|t_paired| < 2` at every cost level — and the
pooled Sharpe of all three lands inside ±0.3.** The grounds are three independent
measurements, not taste: §5c's eleven architectures inside one error bar, nine of ten folds
stopping at EPOCH 1, and the smoke fold's small LSTM converging at epoch 1 with val loss
0.9983 and then rising for fifteen straight epochs. ⚠️ **The way I expect to be wrong**: the
GBT is the arm with a real reason to differ — it compresses each 20-session window to six
statistics per channel, so if the SEQUENCE inside the lookback carries anything, the GBT
loses and the two LSTMs do not.

**RESULT — 10 folds × 2 arms, 15m 03s, and the prediction was RIGHT on both halves.**

| arm | capacity | IC | `ic_t` | Sharpe@30 | `z` (200 draws) |
|---|---|---|---|---|---|
| `lstm` (PRF-1) | 205,441 params | +0.1097 | 6.90 | **+1.991** | +12.28 ✅ |
| **`lstm_small`** | **2,033 params — 101×** | +0.1239 | 9.49 | **+1.997** | +12.43 ✅ |
| `gbt` | 1,400 decision nodes | +0.1249 | 8.90 | **+1.975** | +11.88 ✅ |

Paired against `lstm` (ρ **0.88** between the arms' period returns): `lstm_small`
`t = +0.87…+0.88`, `gbt` `t = +0.42…+0.47` at 20/30/50 bps. **Every |t| < 1**, every
ΔSharpe ~0.02 against `se_sharpe` 0.155.

⚠️ **THE RESULT LIVES IN THE 13 CHANNELS, NOT IN THE ARCHITECTURE.** That is the finding,
and it is not a saving. It also means **`PRF-7`'s selection look-ahead is close to the WHOLE
story** about where this Sharpe comes from, rather than part of it — the only other
candidate has just been ruled out.

⚠️ **AND THE SEQUENCE INSIDE THE LOOKBACK IS WORTH NOTHING** — the way I expected to be
wrong, and I was not. `model.gbt` sees **78 window statistics where the LSTM sees 260
numbers**, and it ties. Whatever the recurrent layers extract from the 20-session path over
and above last/mean/slope/sd/min/max does not reach either the IC or the Sharpe.

⚠️ **Do NOT read the table as "smaller is better"** even though `lstm_small` is nominally
ahead on IC and on days-positive: the portfolio difference is inside the paired error bar,
and the fold where the big model looks worst (2026, IC −0.0902 against +0.0016 / +0.0254)
is a 5-period stub with `se_sharpe` 1.04. **The defensible claim is "not worse", which is
exactly what PRF-8 was built to test.**

⚠️ **A CONCURRENCY TRAP WAS FOUND AND FIXED ON THE WAY**, and it is the reason the first
sweep was thrown away: two `walkforward` runs over one table rebuild and delete each other's
fold tensors, because the dataset directory is named from the DATA with no term for which
process built it. The loud half was a `FileNotFoundError`; the silent half was a model
reading tensors another process was mid-`np.save` on. `run.namespace_lock` now refuses the
second sweep. `walkforward/CONTEXT.md` §8c.

**Next**: this closes the architecture question and hands the baton to **`PRF-9`** — the
next model test is not a different model, it is 90 → 800 candidate CHANNELS.
`walkforward/CONTEXT.md` §8 has the full tables and §8d what it does not establish.

### ✅ P0-1 · DONE 2026-08-17 — the two-layer null CLEARS, and **my recorded prediction was wrong**

`feature_selection/studies/two_layer_null.py`, **20 draws across two seeds**, 3 h 39 m total (1h50m + 1h48m). Each draw shuffles the
label ONCE and re-runs **both** layers on it — six layer-1 selections, the union of their
survivors, one layer-2 selection.

| | layer-2 only *(the run's own null)* | **BOTH layers, 20 draws** *(honest)* |
|---|---|---|
| draws | 10 | **20** (seeds 18 + 19, pooled) |
| null mean | +0.0023 | +0.0156 |
| null sd | 0.0300 | 0.0314 |
| **p95 BAR** | **+0.0428** | **+0.0574** |
| null MAX | +0.0577 | +0.0676 |
| z | +4.48 | **+3.86** |
| p | 0.0909 | **0.0476** (the 1/21 floor) |
| observed | +0.1369 | +0.1369 |

⚠️ **The two seeds agree to the third decimal on the bar** — +0.0573 (seed 18) vs +0.0565
(seed 19) — which is the reassuring part, and more useful than the p-value. `SE(sd)` fell
from 0.0083 to 0.0051, so `z = +3.86` now rests on a stable dispersion estimate.

⚠️ **20 draws does not escape the floor, it moves it.** `p = 0.0476` is exactly `1/21`:
**0 of 20** draws reached the observed, so the p-value is again pinned at its minimum and
says "no draw beat it", not "p is small". Real resolution needs 50 draws (floor 0.0196,
~9 h) or 100 (0.0099, ~18 h). **Read `z`, which has no floor.**

**The criticism was right about the direction and wrong about the outcome.** Pricing in
layer 1 raises the bar **34%** — so the run's own null *was* too easy, exactly as argued.
But the observed IC is **2.4× the honest bar**, **0 of 10 draws** reach it, and the null
MAX (+0.0676) stays below it so rule 3 does not fire.

⚠️ **I wrote "Prediction, recorded now so it cannot be revised afterwards: this will not
clear." It cleared.** The prediction is left in the git history rather than quietly edited.

⚠️ **The bar is CONSERVATIVE, which makes clearing it stronger.** On a shuffled label
layer 1 keeps **~705-713 channels** against 208 on the real label — nothing dominates, so
the correlation prune removes less. Layer 2 in each draw therefore selects from a 3.4×
wider candidate set and has *more* room to overfit noise, pushing the null up.

**What this does NOT settle** — three of the four original objections are untouched:

1. `p = 0.0476` is **still the floor**, now at 1/21. Raising 10 → 20 draws bought a
   stable `sd` (SE 0.0083 → 0.0051) and a trustworthy `z = +3.86`; it did **not** buy
   p-value resolution, and 50-100 draws (9-18 h) is what would.
2. The fold trend `+0.125 / −0.017 / +0.142 / +0.127 / +0.306` is untested here, and rule
   23's data-arrival reading still fits a pool whose news channels are NULL before 2013.
3. 9 of 66 channels are constant across the train slice.

⚠️ **And a cleared SELECTION bar has never yet survived downstream in this repo** — §5d:
"The selection cleared its bar; the model did not clear its own." **P2-3** is now worth
running, and that is a change from this morning.

### ✅ P0-2 · DONE 2026-08-17 — rule 21 shipped, **and the report had to be fixed too**

`selector.py:1021` and `:1117` now call `evaluation.sign_hit_rate`, which returns **NaN
when every non-zero label shares a sign**. Implemented in `feature_selection` rather than
imported from `result_evaluator`, because the dependency runs the other way.

⚠️ **Shipping the metric was not enough, and this is the part that nearly slipped.** A
withdrawn `hit_rate` is NaN, and `report.py`'s formatter tested only `v is None`:
`abs(nan) < 10` is False, so it fell through to `f"{v:.1f}"` and the README printed a
bare **`nan`** — which reads as a defect rather than as the deliberate absence it is.
Both the summary table and the holdout table now test `v != v` and print `—`.

**Verified end to end, not just by unit test**, on a fresh level-target run
(`vcb__basic+market_breadth__close_adjust_5day`, 48.6 s):

```
README.md:47   | `hit_rate` | — | — |
validation.csv  hit_rate NaN in all 10 rows
```

5 unit tests besides, including the two edge cases that matter: an **all-negative** label
must withdraw too, and a single unchanged day (`0.0`) must **not** make a return series
single-signed — otherwise one flat session silently deletes the metric for a whole run.

### ✅ P0-3 · DONE 2026-08-17 — `float32` does **NOT** reproduce `float64`

Measured on two panels, both dtypes, **both on CPU** so XGBoost's per-device RNG could not
confound the dtype:

| panel | kept | shared | Jaccard | `ic_mean` float64 → float32 |
|---|---|---|---|---|
| `basic + market_breadth` | 64 vs 64 | 59 | 0.855 | +0.0275 → +0.0317 |
| `basic + stock_market` | 123 vs 123 | 115 | 0.878 | +0.0322 → **+0.0490** |

The second is a **52% relative change in the measured IC** — the same order as the effects
this package exists to detect. ⚠️ **The docstring shipped on 2026-08-16 claimed the
opposite** ("the precision loss is nominal…"); that claim is withdrawn and the measurement
is in its place.

⚠️ **What it swaps is not random, and that is the useful half.** Every differing channel
trades for its NEAR-TWIN — `foreign_sell_value`↔`foreign_sell_volume`,
`prop_sell_val`↔`prop_sell_vol`, `drv_parkinson_21`↔`drv_garman_klass_21`,
`drv_close_pos_63`↔`drv_close_z_63`, `…volume_negotiated`↔`…value_negotiated`. `float32`
is **breaking ties the correlation prune considers interchangeable**, not scrambling the
selection.

**Standing rule now:** never use `float32` for a run whose number will be quoted. It is
for the case where the alternative is not running at all (`MEM-1`'s universe panel), and
`contract.SETUP_KEYS` already carries `design_dtype` so the two can never be unioned.

### ✅ P0-4 · DONE 2026-08-17 — `mkt_n_names` blocked from the pool, kept in gold

`UNIFIED_MARKET_BREADTH_NOT_FEATURES` blocks it at the pool builder;
`pool__market_breadth` is now **4,266 × 10 (7 channels)**, and `gold.market_breadth` still
carries the column because a reader needs to know how wide each date's cross-section was.
A candidate FEATURE and a DIAGNOSTIC are different things.

⚠️ **It never bit**: on the 2026-08-17 `return_5day` chain **no `mkt_*` channel survived
layer 2 at all** — 4 of 208 reached the shortlist pool, 0 of 66 reached the final table.
This is a guard against the next run, not a repair of that one. ⚠️ The block-list raises
if it names a column the source does not have — a guard that silently matches nothing is
how an excluded column comes back after an upstream rename.

---

### ✅ P0-5 · DONE 2026-08-18 — `RNK-1`, the label is reconstituted at dataset build

**The model is trained on a label the selection never scored.** `final_features` stores
`return_{h}day` because a rank belongs to a run and not to a row (its §5), on the stated
understanding that *"the reader re-ranks"* — and no reader does. `train_test_creator`
builds `y` from the stored column and merely records `selected_for`.

⚠️ **§2b already measured the cost of exactly this swap, on the same panel and folds: the
IC drops 4× and the hit rate falls below a coin.** So this outranks any new number — it is
the priority rule's first clause, *a thing that makes a number you already have wrong*.

**Fix**: in `train_test_creator`, when the table's target is `derived`, recompute
`cross_sectional_rank` within each date over the rows the table holds, and record the
universe it ranked over in the dataset metadata. ⚠️ Do **not** reach for
`cross_sectional.cross_sectional_rank` and stop there — `min_width` is part of the label's
definition and must travel with it.

### ✅ P0-6 · DONE 2026-08-18 — `UNI-1`, the universe travels and cannot union

`RNK-1`'s sibling: RNK-1 is the wrong COLUMN, this is the wrong POPULATION. The run folder
already records `input.universe` (150 tickers); `final_features` never reads it and would
build over `unified_schema_all`'s **781**. Filter the build to it, and put it in the table
`COMMENT` so a dataset built later cannot silently widen it.

✅ **Both shipped, 151 tests passing across the three affected packages.** `RNK-1`: one
definition of the label, asserted equal to `cross_sectional_rank` at **atol = 0**; thin
dates dropped and counted; `metadata.json → target.column` now means what `y` IS.
`UNI-1`: the universe is a GROUP KEY, so two populations collide on the table name and
**raise** instead of unioning; `build_sql` emits `WHERE base.ticker IN (…)`; the COMMENT
carries eight names and a sha1. `final_features` had **no tests at all** and now has seven.

⚠️ **P1-5 is unblocked and has NOT been run** — that was the instruction.

### ⚠️ P1-5 · STAGES 5-8 ALL DONE 2026-08-18 — only **`FNM-1`** is left

**`final_features --apply`** built `unified_schema_all.rank_20day__final__d20_h20` in
**7.3 s**: 624,448 × 17, **150 tickers**, 2009-01-02 → 2026-08-07, 621,448 labelled. The
two VCB tables were reported `exists=True, fingerprint matches` and skipped, which is why
no `--scope` was needed. ⚠️ **First real exercise of `UNI-1`'s fix**: the plan carried all
150 names and the DDL emitted `WHERE base.ticker IN (…)` — 624,448 rows and not 2.39 M is
the proof it fired. The `COMMENT` carries the universe and its sha1 `301aeb491d`.

**`train_test_creator --ticker all --save`** built the dataset in **10.9 s**:

| split | windows | dates |
|---|---|---|
| train | 422,251 | 2009-02-05 → 2021-02-01 |
| val | 91,462 | 2021-04-05 → 2023-09-20 |
| test | 93,224 | 2023-11-15 → 2026-07-10 |

13 features kept, **0 dropped**; 3,000 unlabelled rows dropped — exactly `150 × 20`, the
h=20 tail of each ticker, which is the arithmetic check that the tail is per-ticker and
not global. **0 rows too thin to rank.**

⚠️ **AND THIS IS `RNK-1` PROVEN ON THE ARTEFACT RATHER THAN IN CODE.** The banner printed
*"y is 'cs_rank_20day', RE-RANKED within each date from 'return_20day'"* — the first time
that path ran through `read()`, which the unit tests could not reach — and the saved
tensors settle it: **excess kurtosis −1.199 (train) and −1.200 (test)**, the theoretical
value for a UNIFORM distribution, bounded at ±1.720 after standardisation. A 20-day
return is strongly leptokurtic. Before the fix `y` would have been that return.

⚠️ Read at training time: `evidence = cleared_p95_not_a_pass=1, no_null=1`, and **drift —
2 of 13 channels put >1 % of the test set beyond 5 train-sigmas** (0 put all of it there).

**Stage 7 ran**: `lstm__all__rank_20day__final__d20_h20__20260818-195738`, **4m 23s**,
LSTM 2×128 unchanged from the VCB and BANK runs so the difference is the DATA.

| | val | **test** |
|---|---|---|
| daily IC | +0.1282 | **+0.0863** |
| **t** (`n_eff = dates/h`) | **+4.15** | **+3.47** |
| days with IC > 0 | 78.1 % | **80.9 %** |
| dates · `n_eff` | 616 · 30.8 | 656 · 32.8 |
| `dir_auc` · `hit_rate` | 0.560 · 0.543 | 0.540 · 0.531 |
| R² | +0.0104 | **+0.0003** |
| RMSE vs constant predictor | 0.28912 / 0.29063 | **0.29065 / 0.29070** |

**This is the first model in the repo whose out-of-sample skill survives an honest error
bar.** ⚠️ And it RANKS without PRICING: R² ≈ 0 and RMSE is 0.017 % below a constant
predictor, so only the ORDER carries. ⚠️ `long_short = +0.0635` is a **rank** spread, not
money. ⚠️ The `t` above is computed by hand — the artefact's own `ic_t` reads 15.50 and is
wrong by √h (**P0-7 / `ICT-1`**).

**Stage 8 ran, after `ICT-1` was fixed so it could not index the overstated figure.**
`--rescore` (41.6 s) then `--rebuild-index` (42.7 s), no GPU. The run folder and
`index.csv` now both read `ic_t = +3.47` test / **+4.15** val — see CLAUDE.md §6-0.

⚠️ **`--rescore` DOES NOT REWRITE `index.csv`, and the register said it did.** It calls
`evaluate_run(write=True)` per folder, which rewrites `results/metrics.{csv,json}` and
`verdict.txt`; `index.csv` is written **only** by `rebuild_index`, a different branch of
`_main`. Measured 2026-08-18: after `--rescore` the folder read +3.47 while `index.csv`
still read 15.50. **Both flags, in that order, or the leaderboard keeps the old number.**

⚠️ **Block B is absent on a PANEL, so "read `mase` beside it" cannot be done here.**
`accuracy_vs_naive` is called from `metrics.evaluate` (the series path) only —
`evaluate_panel` runs `regression_extras` and never the naive comparison, so
`test_mase` is **NaN** for the top-150 run while the two VCB runs carry 21.36 and 1.068.
That is not a regression from this fix; it is a gap this fix made visible. New item
**P4-12**.

**Left:** ~~`FNM-1` (P1-6)~~ ✅ **measured 2026-08-19** — the shortlist does not depend on the representation, so neither side has to move.

**Why it is the question this repo has never answered.** Twice now a selection has cleared an honest
bar and the model below it has shown nothing (§5d, P2-3) — and `RNK-1` says that on a
cross-section the model was aimed at the wrong label both times, so those two data points
do not settle it. This is the first chance to ask it properly: a shortlist of 13 channels
behind **z = +9.09**, the strongest selection evidence in the repo.

```powershell
python -m final_features --apply                                    # ✅ DONE 7.3 s
python -m train_test_creator --ticker all --table rank_20day__final__d20_h20 --save  # ✅ DONE 10.9 s
python -m model.lstm --config configs/lstm__all__rank_20day__final__d20_h20.yaml     # ✅ DONE 4m 23s
python -m result_evaluator --rescore                                # ✅ DONE 41.6 s
python -m result_evaluator --rebuild-index                          # ✅ DONE 42.7 s — NOT optional
```
⚠️ **`--ticker all` is not optional** — the stage defaults to `chain.DEFAULT_TICKER`, which
is `vcb`, and would look for the table in the wrong schema.

⚠️ **NO `--scope`, and an earlier draft of this item was WRONG to say otherwise.**
`--scope` names EVERY table in the plan, not the one you meant: `--scope liquid150` was
measured on 2026-08-18 planning `close_adjust_5day__final__d20_h5__liquid150` and
`return_5day__final__d20_h5__liquid150` as well — two junk duplicates of VCB tables that
already exist. Plain `--apply` builds only what is missing: the plan reports the two VCB
tables as `exists=True` and skips them. A scope is for separating two runs that COLLIDE
on a name, and nothing collides here — `unified_schema_all.rank_20day__final__d20_h20` is
a name no other group wants.

⚠️ **On a panel, quote the daily-IC t-stat, never `ic_clears`** — `NUL-3`, the evaluator's
panel null is not label-neutral. ⚠️ And read `mase` beside it: P2-3's model cleared nothing
and lost to "predict no change" at `mase 1.068`, which is the line that mattered.

### ✅ P1-4 · DONE 2026-08-18 — the VRAM half is fixed; **the next wall is HOST RAM**

Shipped: `gpu.rank_block_columns` + a blocked `_average_ranks_torch` + a blocked
`_spearman_vector_cuda`. ⚠️ **Chunking the rank helper alone would not have been enough**
— the old path also handed a full `n × p` rank matrix to `_pearson_against_last`, which
builds five more `n × p` tensors. Every stage is `O(n × p)`, so every stage had to become
`O(n × block)`.

| verified three ways | |
|---|---|
| 4 new tests (11 in the file) | blocked == dense at **0.0**, blocks of 1/2/5/36/37/100 and the whole vector path at 1/3/16 |
| the 30-name smoke run, **through 2 blocks** | reproduces exactly: 60 kept, `ic_mean +0.0263`, trend +0.0297, shortlist 22 — and 0.6 s against 0.7 s, so blocking is free |
| `rank_block_columns(4_266, 600)` | **1 block** — every archived run keeps the dense path |

**On the T4 it worked**: phase 3 went from an OOM to **12.3 s**. Then the kernel **died in
phase 4 with no traceback** — `DeadKernelError: Kernel died`, which is a SIGKILL from the
cgroup, not a catchable CUDA error. So the binding constraint moved from VRAM to **host
RAM**, which is `MEM-1`'s original half (**P3-2**).

⚠️ **AND NOTHING IN THE RUN SAID HOW MUCH MEMORY IT WAS USING**, so the diagnosis was an
inference — which §5 rule 2 forbids leaving as one. `selector._tick` now prints
`rss=… vram=…` per phase (one `psutil` call per phase, nine per run). Measured on the
30-name smoke panel, 48,521 rows: RSS **0.8 → 2.3 GB**, peaking at `stability`.

### ✅ P1-3 · DONE 2026-08-17 — panel mode runs, and the FIRST rehearsal found the existing job broken

The worker side is `src/feature_selection/RUN__cross_sectional_panel.ipynb` + the
`cross-sectional` job. It re-implements no selection: it loads `panel.parquet` and calls
`feature_selection.run.run_selection` through a new `provided_panel` argument
(`run.ProvidedPanel`), which replaces **the read and nothing else**.

| measured, before any quota was spent | |
|---|---|
| `kgpu export cross-sectional` | **2m 04s** → 1,247,098 × 104, **477.4 MB**, 300 tickers, 4,388 dates |
| `kgpu rehearse cross-sectional` | **16.0 s**, both mount layouts, `n_eff = 218` |
| the notebook's OWN cells, end to end | ✅ 30 names / 48,521 rows, through the real bootstrap: **2m 11s**, 60 kept, shortlist 22, `source_table from metadata` |
| `feature_selection` tests | 113 passed |

⚠️ **THE REHEARSAL NEVER RUNS THE NOTEBOOK'S OWN CELLS** — it drives cell 0 and then
re-creates the panel path itself, so a defect in the notebook would have surfaced only
after the queue. Hence the third row: the built notebook was executed against a cut-down
payload with `KGPU_INPUT_DIR` / `KGPU_WORK_DIR` set — the same seam `rehearse` uses. ⚠️ A
cut-down panel is a **smoke test and never a measurement**; its `cs_rank` is still the
rank over the 300 exported names.

⚠️ **`KGP-1`, found in 3.6 s by the first rehearsal and fixed:** the payload never shipped
`src/utils` because `kgpu_bootstrap` **stubs** `utils`, and `report.py` gained
`from utils import runtime` on 2026-08-15 — **after this integration's only green round
trip**. The `feature-selection` job had been broken on the worker for two days with
nothing saying so. A stub is now installed only when the real module is **not importable**.

⚠️ **One more, unrelated to Kaggle and found by the test suite:** `ranker_eval.ALL_TARGETS`
never received the three `*_20day` labels `run.ALL_TARGETS` gained with the 4-week horizon
(`e87a3fa7`), so a scorecard run could have offered `return_20day` as a candidate FEATURE.
The two lists are one list now.

**What is left is the RUN, not the mode:** `kgpu data cross-sectional` (477 MB upload),
then `kgpu run cross-sectional`. That is P2-1 v2.

---

### ✅ P1-7 · DONE 2026-08-18 — stage 9 exists, and it answered the single-stock question

`src/backtest/`, 14 tests. Full measurement in **`backtest/CONTEXT.md`** and CLAUDE.md
§6-0-bis; the short version is that the PORTFOLIO clears a costed null (top-15 of 150,
20-session rebalance, 50 bps, z = **+4.29** test / **+6.10** val) and **VCB alone takes
zero trades in 33 periods** because the model never ranks it above the 0.826 percentile.

⚠️ **The cost identity is the finding that outlives the run**: at τ=0.70 and 50 bps the
annual fee drag is **17.6 % at h=5**, 8.8 % at h=10, 4.4 % at h=20, against a top-100
benchmark CAGR of 9.75 %. **h=5 pays more in fees than the market returns.**

**Left, in order:**

1. **`FNM-1` (P1-6)** — unchanged, and now the last thing between this chain and a claim.
2. **A WALK-FORWARD**, not one split. §11's regime finding used 28 expanding folds; stage
   9 has one, and §11 and this run disagree about 2022-26 (see §6-0-bis). One of them is
   measuring the horizon and one is measuring the window, and only a walk-forward at both
   horizons separates them. ⏱ model-stage work, not backtest work.
3. **`h=10` and `h=5` through the identical chain** — the controlled comparison. ⚠️ Read
   the drag table first: h=10 must beat h=20 by **4.4 pp/yr** just to break even on fees.
4. **Slippage / ADV cap** — a 15-name book at size moves a top-150 VN name.
   `pool__basic.value_matched` makes this buildable; stage 9 currently assumes fills.

### ~~P2-1~~ · RETIRED 2026-08-17 — the first version was a bad experiment (kept for the reasoning)

**What it said:** "run the chain at a 4-week horizon", where *the chain* is the VCB
single-stock chain. **That does not reproduce the evidence it cites.** CLAUDE.md §2a-bis
measured `controls` on **top-30 / top-100 CROSS-SECTIONS rebalanced weekly**, not on one
stock as a time series.

And it makes the binding constraint worse. `n_eff = n/h` on VCB:

| horizon | `n_eff` | purge gap |
|---|---|---|
| h=5 | 852 | 24 rows |
| h=10 | 426 | 29 rows |
| **h=20** | **213** | 39 rows |

`feature_selection` §6d priced the single-ticker study at ~850 independent observations
and said **1,500 were needed**. Running VCB at h=20 takes the constraint that is already
binding and tightens it **4×**, to test a result measured under a different design.

### ✅ P2-1 v2 · THE DESIGN — why width buys precision, and why 300 became 150

At h=20 the independent count is 213 **at either grain** — what differs is the QUALITY of
each observation:

| | one observation is | its sd |
|---|---|---|
| VCB, h=20 | one stock's ±1 sign | ~1.0 |
| top-300 cross-section, h=20 | an IC over ~300 names on one date | **~0.06** |

That is §2b's mechanism exactly: width buys **precision per observation**, `1/√N`, not
more observations. So h=20 kills the single-stock study and does **not** kill the
cross-section.

It combines the four things separate measurements have each pointed at:

- the **horizon** §2a-bis says works (4-13 weeks, not 5-10 sessions);
- the **grain** §2b says is the only one that ever cleared a null (≥100 names);
- the **liquidity tier** the 2026-08-17 reversal probe says is the real variable
  (t = −18.60 all names → −10.43 top 300 → **−1.96** top 100);
- and `cs_rank_20day` carries the `cs_` prefix, so it takes the correct path regardless of
  `PNL-2`.

⚠️ **The universe must be chosen from data available BEFORE the evaluation window**, or
"top 300 by turnover" is look-ahead: picking today's liquid names and applying them to
2010 is the same defect as a point-in-time index list, which §2c already records.

⚠️ **THE FOURTH CLAIM — "it fits in memory today" — IS FALSIFIED, and it was checked
wrong.** I checked RAM. **VRAM is the binding constraint**: the local pilot CUDA-OOMed in
`gpu.spearman_vector`, asking for **1.01 GiB on a 4.00 GiB card**. That is `MEM-1` on the
device side, and it is why this item now routes through Kaggle:

| | this machine | Kaggle T4 |
|---|---|---|
| VRAM | 4.0 GiB ❌ | **14.6 GiB** ✅ |
| RAM free | ~7 GB ❌ | ~29 GB ✅ |
| the design, 1.25 M × 104, float64 | ~12.8 GB | fits |

### ⚠️ FIRST ATTEMPT, 2026-08-17 — the T4 OOMed, and **the fifth claim was checked wrong too**

Uploaded (477 MB, dataset v1) and pushed with `RUN_NULL=false`. It reached the GPU and
died at **3m 28s**:

| phase, on the T4 | |
|---|---|
| payload mount + source unpack + reader swap | ✅ |
| `panel.parquet` loaded | 1,247,098 × 104, 1.57 GB, 300 tickers, **`n_eff = 218`**, density 0.947 |
| `prepare + coverage` | 6.4 s |
| **`window design`** | **189.3 s** |
| `spearman vs target` | ❌ **CUDA OOM: tried to allocate 4.98 GiB, 3.86 GiB free, 10.70 GiB already in use of 14.56** |

⚠️ **"The design is ~12.8 GB, so a 14.6 GiB T4 fits" priced the DESIGN and not the STEP.**
`gpu._average_ranks_torch` holds `values` + `filled` + a mask — 10.58 GiB at ~536 float64
columns × 1.247 M rows — and *then* `torch.sort` asks for its own output plus an int64
`order`, roughly another 10 GiB. **The step needs ~4× the design, so no card this side of
an A100 80 GB runs it as written.** That is the same error as the RAM-not-VRAM one
recorded above, one level down: I checked the wrong quantity again.

**The fix is exact, not approximate: rank in COLUMN BLOCKS.** Ranks are per-column
independent, so chunking `spearman_vector` changes no number — unlike `float32`, which
P0-3 measured at a **52% relative change in `ic_mean`** and which is forbidden here. It is
`MEM-1` on the device side and it is now the only thing between this repo and the
measurement §2a-bis has been pointing at since 2026-08-03. ⏱ ~2 h to chunk and re-verify
against the smoke run, then one more T4 round trip.

⚠️ **Panel mode is NOT what failed and must not be re-opened.** Everything `kgpu` adds ran
on the worker; what died is a ranker step at a width no single-ticker run has ever reached.

### ⚠️ SECOND ATTEMPT, 2026-08-18 — one wall further, and a DECISION was needed

With P1-4's chunking, the same job on the same payload got **past** the step that killed
it: `spearman vs target` **12.3 s** where it had OOMed. `window design` 198.1 s. Then
phase 4 of 9 — `rank (the ensemble's methods)` — took the kernel down with
`DeadKernelError` and no traceback: a host-RAM kill, ~39 GB wanted against ~29-30 GB.

**Two honest ways forward, and they are not equivalent:**

| | cost | what it costs the RESULT |
|---|---|---|
| **(a) top-150 by turnover**, everything else identical | one round trip, today | `n_eff` stays **218** — dates are unchanged. Daily-IC sd goes ~0.058 → ~0.082, so **z scales by ~0.71**. Still above §2b's ~100-name threshold |
| **(b) fix `P3-2` first**, then run top-300 | ~a day of streaming work | nothing — full power, `z` as designed |

⚠️ **Halving the DATES instead would cost exactly the same `z` (both scale it by √½), and
would cost `n_eff` as well** — 218 → 109. Prefer cutting names; they are the axis that
buys precision, not independence.

### ⚠️ THIRD ATTEMPT, 2026-08-18 — **IT RAN**, top-150, and the result has NO BAR

`2026-08-17_235146__all__basic__cs_rank_20day`, Tesla T4, **22m 53s**, 624,448 × 104,
150 tickers, 4,368 sessions, `n_eff = 218`, 90 channels → **61 kept**, shortlist 13.

| fold | 1 | 2 | 3 | 4 | 5 | mean |
|---|---|---|---|---|---|---|
| IC (selected) | +0.060 | +0.124 | **+0.153** | +0.104 | +0.097 | **+0.1075** |
| R² | −0.038 | +0.007 | +0.021 | +0.007 | +0.012 | — |

`ic_trend_per_fold` **+0.0054** (flat, not decaying — §5 rule 5), `hit_rate` 0.536,
`ic_fold_sd` 0.0342. **All five folds positive**, and R² is positive in four of five —
which §5c's eleven single-stock models never managed once.

⚠️ **AND `null: None`.** Rule 2: `evidence=no_null` is an **unknown, not a pass**, and
§2b's whole finding is that the observed IC barely moves while the noise floor collapses
— so the bar is the entire question and this run does not answer it. ⚠️ **Do not read
+0.1075 against §2b's bars** (VN100 +0.0117, LIQUID301 +0.0245): those were measured at
`h=5`, and at `h=20` each fold carries `n_eff = 38.1`, so this run's own null will be
**wider**. It must be its own.

**The null is now priced, which is what this run bought.** One pass 1,355 s, of which
`stability` 187 s is skipped by a draw → **~19.5 min per draw, ~6.5 h for 20**. Against
Kaggle's 12 h session cap and 27.6 h of weekly quota, that is **affordable in one
session** — the first time a 20-draw null on a real cross-section has been.

⚠️ **`permutation` is 726 s, 54 % of the run**, and §19 measured it as the one
load-bearing ensemble member, so it cannot be dropped to buy the null.

### ✅ THE 20-DRAW NULL — DONE 2026-08-18, **z = +9.09**, and my prediction was half right

`RUN_NULL=true, N_NULL=20` on the same payload. Each draw shuffles the label in **date
blocks** (`cross_sectional.shuffle_dates`, `mode="date_block"` — each stock keeps its own
returns, moved to a different fortnight) and re-runs the whole selection on it.

**RESULT — 6 h 07 m on a T4, 0 failed draws:** null mean **+0.0291**, sd **0.0086**, p95
bar **+0.0388**, null MAX **+0.0410** (below the observed, so rule 3 does not fire),
**z = +9.09**, p = 0.0476 (the 1/21 floor — read z). **It clears.**

⚠️ **THE PREDICTION WAS RIGHT ON THE BAR AND WRONG ON `z`, and the wrong half is left
here rather than edited.** Predicted bar +0.02 … +0.05 → actual **+0.0388** ✅. Predicted
z +2 … +6 → actual **+9.09** ❌ — I hedged the range upward "for safety" while my own
reasoning in the same paragraph implied a fold noise of ~0.013 and therefore a much
tighter null. **Padding a prediction is not conservatism; it is a worse prediction.** The
null sd came in at 0.0086.

⚠️ **And the number to quote is not +0.1075.** The null's mean is +0.0291, so the excess
over a shuffled label is **+0.078**. See CLAUDE.md §2b-bis for the four things this does
not settle — chiefly that there is **no holdout**, and that §2c records the VN100 result
clearing its bar and then failing exactly that test.

*(The original prediction, kept verbatim: "it WILL clear, with a p95 bar around
+0.02 … +0.05 and z between +2 and +6.")* The reasoning, so
that being wrong is informative:

- a daily IC over 150 names has sd ≈ `1/√150` ≈ **0.082**, and each fold averages ~760 of
  them, so even at 20× inflation for label overlap a fold's IC noise is ~0.013 — an order
  below the observed +0.1075;
- §2b's ladder is the precedent: LIQUID301 observed +0.0768 against a bar of +0.0245;
- the selection here keeps **61 of 90** channels, far less aggressive than the two-layer
  funnel P0-1 had to price in, so there is less selection for the null to absorb.

**What would falsify that reading, and it is not nothing:** rule 23's data-arrival
signature (this panel starts in 2009 and `drv_*` channels need 252 sessions of history),
and the fat-tailed nulls §10d records — `p = 1/21` is the floor either way, so **read `z`,
not `p`**.

⚠️ **A SEPARATE THING TO CHECK IN THAT RUN'S OUTPUT**: `ic_summary.se_ic_per_fold` reads
**0.1642** ≈ `1/√38.1`, which is the SINGLE-SERIES formula — it assumes a daily IC of sd
1.0, i.e. one stock's ±1 sign. On a 150-name cross-section the sd is ~0.082, so that
column appears to over-state the error bar by ~√N ≈ 12×. If so it is `PNL-1`'s family at
the summary instead of the scorer, and it is **conservative** (too wide), which is why it
has never manufactured a result — but it should not be quoted as this run's error bar.

### ✅ P2-3 · DONE 2026-08-17 — the selection cleared its bar; **the model did not**

`lstm__vcb__return_5day__final__d20_h5__20260817-205952`, 6.0 s, 228,225 parameters,
57 features × 20 × 2,921 training windows.

| | val | test |
|---|---|---|
| `ic` | +0.0611 | **+0.0858** |
| `ic_bar` (200 draws) | +0.1124 | **+0.1232** ❌ |
| `ic_p` | 0.184 | 0.109 |
| **`dir_auc`** | 0.5276 | **0.4974** — a coin |
| **`mase`** | 1.073 | **1.068** — LOSES to the naive |
| `skill_score` | −0.073 | −0.029 |
| `r2` | −0.081 | −0.031 |
| `calibration_slope` | 0.252 | 0.408 |
| best epoch | **1 of 21** | val loss rose from the first epoch on |

**This answers the question the run existed to ask.** The shortlist behind it cleared a
bar that priced in BOTH selection layers (P0-1, z = +3.86) — the strongest selection
evidence this repo has produced — and the model built on it shows **no skill on either
split**, loses to a zero-return naive on `mase`, and never beat its own first epoch.
CLAUDE.md §5d's sentence reproduced on better evidence: *"The selection cleared its bar;
the model did not clear its own."*

⚠️ **Two things ARE better than the `close_adjust_5day` chain, and both are about the
TARGET, not the model.** Both splits now agree in SIGN (+0.061 / +0.086) where the level
target gave −0.459 / +0.488 — one error bar straddling zero. And `dir_auc`, `hit_rate`
and `mase` are all **readable** here because a return is two-signed; on the level target
ROC AUC did not exist at all.

⚠️ **`mase > 1` on both splits is the line to quote.** A model trained on the best
shortlist this project has assembled does not beat "predict no change".
