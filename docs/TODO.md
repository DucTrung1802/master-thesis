# TODO — the one backlog

> **Registers:** [CLAUDE.md](../CLAUDE.md) *what is PROVED* · [RUNBOOK.md](RUNBOOK.md) *how to RUN* ·
> [ISSUES.md](ISSUES.md) *what is BROKEN* · **TODO.md** *what is NEXT*. Movement is one-way: a TODO
> item that turns out to be a defect graduates to ISSUES.md with a code; a done TODO item leaves its
> measurement in CLAUDE.md or a `CONTEXT.md` and is **deleted, not ticked**.
>
> ⚠️ Costs are measured on this machine (RTX 3050 4 GB, 15.6 GB RAM), three rankers, GPU. **A cost
> marked `est.` is an estimate** and says which measured run it is anchored to.

**Priority rule:** a thing that makes a number you ALREADY HAVE wrong outranks a thing that would give
you a new number; a thing that unblocks hours of other work outranks a thing that is only itself;
structural code comes last because it only pays off for runs that are currently blocked anyway.

---

## ⚠️ START HERE — NUMBERS ARE FROZEN. THE TOP ROW IS THE HIGHEST PRIORITY.

> **A `P<n>` means the same item forever; what to do next is the TOP ROW, not the smallest number.**
>
> | | |
> |---|---|
> | **identity** | the number. Assigned once, never reused, never renumbered — as `ISSUES.md` has always done |
> | **priority** | the ORDER OF THE ROWS. Re-order freely; that is all a re-prioritisation touches |
> | **status** | a `✅ DONE <date>` marker in the row, with the measurement and where it landed |
>
> ⚠️ **NO RANK COLUMN, DELIBERATELY** — a written-down rank is a second copy of what the row order
> already says, and a second copy can disagree with the first. ⚠️ **NO STRIKETHROUGH** — a closed item
> keeps its number, its row and its evidence in ordinary type, because the measurement it leaves
> behind is the point and it is cited BY CODE from CLAUDE.md and the `CONTEXT.md` files.
>
> ⚠️ **WHY FROZEN (2026-08-23).** A code used to carry both *which item* and *how urgent*, so it
> shifted every time anything finished — **three times in two days**. Each shift silently repointed
> every `P<n>` written before it, made resolving a code a two-step lookup, forced hand edits to live
> pointers in five other files, and destroyed information.
>
> ⚠️ **The list no longer starts at `P1` and will not stay monotonic** (`P1` closed 2026-08-23; a
> re-prioritised `P14` may sit above `P9`). **Read the order, cite the number.**
>
> | you see | it is |
> |---|---|
> | **`P7`** — bare, no hyphen | a **LIVE** item |
> | **`P1-9`**, `PRF-8`, `M-3`, `SSK-1` — **hyphenated** | a **RETIRED** code — see the crosswalks. Cited ~150× across CLAUDE.md, RUNBOOK.md, ISSUES.md, four `CONTEXT.md`s, source comments and immutable archived run READMEs, so they were **never rewritten**; the crosswalk is the bridge |
>
> ⚠️ **A `P<n>` written BEFORE 2026-08-23 still resolves to a different item.** Take the DATE of what
> you are reading, then the matching crosswalk below — the last two that will ever be needed.
> ⚠️ **Everything DATED in this file — archive blocks, recorded predictions, "what was next on
> <date>" sections — keeps the numbering it was written with**; those are records, not pointers.
>
> | group | items | what it is |
> |---|---|---|
> | **0 · PARSER** | `P54`, `P55`, `P43`, `P44`, `P45`, `P46`, `P48`, `P51`, `P52`, `P53` (`P39`/`P41`/`P42`/`P47`(b)/`P49`/`P50` done) | ⚠️ **ADDED 2026-08-27, ABOVE EVERYTHING** — a review of `cafef_pdf_parser.py` + `cafef_financials.py` against the parsed tickers on disk. Two classes are WRONG NUMBERS every gate passes; two are the cost `P38`/`P6` are budgeted on |
> | **A · DATA** | `P2` (`P1` done 2026-08-23) | per-ticker freshness shipped; what is left is the filing PDFs |
> | **B · OCR** | `P37`, `P38`, `P6`, `P5`, `P4` | ⚠️ `P37` sits ABOVE `P6`: the builder still defaults to CafeF's HTML tabs, so running the OCR program first would import transcribed rows at 784× scale (`FIN-1`). ⚠️ `P3` (the JSON gate) is CLOSED BY DECISION, not by measurement — the source is CafeF PDFs |
> | **C · OUTPUT** | `P7`-`P8` | ✅ unblocked 2026-08-23 — the cross-section is 771 names again, not 7 |
> | **D · MODEL & MONEY** | `P9`-`P17` | a better number, not more data |
> | **E · HONESTY** | `P18`-`P21` | makes an EXISTING number readable |
> | **F · BACKLOG** | `P22`-`P36` | |

**Three sentences worth carrying forward** (measured 2026-08-21, from the seed-floor and arm sweeps):

1. ⚠️ **An arm gap below `|d_sharpe| ≈ 0.09` is a RESEED, not a result** — five `gbt` seeds over the
   identical h=10 folds. Only `cnn` (4.5×) and `gbt` (4.1×) clear it; `transformer`, `tcn`, `bilstm`
   are at or inside it.
2. ⚠️ **A per-fold Sharpe cell is 4.4× noisier still** — per-fold range **0.593** vs **0.134** pooled.
   **Never compare two arms in one fold.** ✅ The DECAY survives (slope −0.308 ± 0.027 over five seeds).
3. ✅ **The h=20 architecture ties hold on BOTH estimands** — ⚠️ but `gbt`'s h=10 advantage CHANGES
   SIGN there (`d_sharpe` +0.360 at h=10, **−0.016** at h=20), the strongest argument against promoting it.

---

## ⚠️ THE DATA AUDIT — measured 2026-08-22, per ticker

**Asked:** *what has to be built so the pipeline has enough data for the whole universe?*
**Measured:** every ticker-keyed table in all three schemas, the raw folders on disk, the per-ticker
last date. It splits into **three things worth building, two blocked by physics, three that must NOT
be built.**

### ⚠️ THE CROSS-SECTION ENDED 2026-06-25 — ✅ FIXED 2026-08-23, kept as the before-picture

> ⚠️ **No longer true**: the cross-section now holds **771-783 names every session** through
> 2026-08-21 (CLAUDE.md §6-2-bis). Kept because the SHAPE of the failure — a scalar `MAX(date)`
> advancing six weeks while the universe stood still — is what `P4` exists to make queryable.

`silver.stocks_basic` reported `MAX(date) = 2026-08-19` from **five tickers**. Names per session:
**779** (06-24/25) → **627** (06-26, the cliff) → **28** (to 07-08) → **24** (to 08-07) → **5**.
**757 of 781 stale**, 599 stopping dead on 2026-06-26. §5 rule 10 at full scale: a 24-name
cross-section looks like a working pipeline to anything reading one number.

### The per-ticker audit — every table keyed by ticker

| layer | table | tickers | last date | verdict |
|---|---|---|---|---|
| bronze | `cafef_price` / `_order_stats` / `_foreign` | **781 / 781** | 2026-08-19 *(5 names)* | ✅ complete, ⚠️ stale |
| bronze | `cafef_prop_trading` | 431 | 2026-08-18 | ✅ **genuine** — optional LEFT JOIN, source starts 2023 |
| bronze | `cafef_news` | 777 | — | ✅ |
| bronze | `simplize_stocks` | 777 | 2026-06-26 | ✅ (not in the price spine) |
| bronze | `trading_view_stocks` | **571** | 2026-06-26 | ⚠️ 73 % — **not the price spine either** |
| bronze | `cafef_insider_shareholder_transactions` | **100** | — | ⚠️ the partition list IS 100 |
| bronze | `cafef_financials_bank_*` | **2** | — | ❌ **the hole** |
| silver | `stocks_basic` | **781** | 2026-08-19 *(5 names)* | ✅ complete, ⚠️ stale |
| silver | `cafef_news_sentiment` | **3** | — | ❌ and deliberately so |
| silver | `stocks_basic_financials_bank_fa` | **2** | 2026-06-25 | ❌ |
| gold | `stocks`, `news_daily_panel`, `news_weekly_panel` | 781 | **2026-07-08** | ⚠️ **30 sessions behind silver** |
| gold | `stocks_ta` | 777 | **2026-06-26** | ⚠️ **54 sessions behind** — `STA-1` |
| gold | `stocks_financials_bank_fa` | **2** | 2026-06-25 | ❌ **0.3 % of the universe** |

⚠️ **`silver.stocks_basic` IS CafeF ONLY** — `cafef_price` as the spine, LEFT JOINed to
`order_stats`/`foreign`/`prop_trading`, plus the GICS tree (verified in
`_ingest_silver_stocks_basic`). So TradingView at 571/781 and Simplize at 777 **block NOTHING** on
the price side.

### ⚠️ FUNDAMENTALS CANNOT REACH 781 TICKERS — three walls, all measured, none a code problem

| wall | measured | what it means |
|---|---|---|
| **disk** | ⚠️ re-measured 2026-08-23 **from CafeF**, not extrapolated: 784 codes list **84,076 documents ≈ 555 GiB**; `D:` extended to 636 GiB, 461 free | the whole corpus does not fit, but **≤2020 is 286 GiB and does**. Disk stopped being a reason to sample TICKERS and became a reason to phase YEARS (`P2`) |
| **time** | OCR statement parse ~**2.4 h/ticker** | 781 × 2.4 h ≈ **78 days** sequential on one 4 GB RTX 3050 |
| **schema** | ⚠️ re-measured 2026-08-25 (`TPL-1`): **all four charts of accounts exist** and the parser is template-generic; what is bank-only is the set of **reconcile anchors** | **761 of 781 names are not banks** (230 industrials, 117 materials, 93 consumer staples; only 20 are GICS 401010) |

⚠️ **THE SCHEMA WALL IS THE REAL ONE.** With infinite disk and time the current chain still reaches
20 of 781 names. ⚠️ **The escape route was closed BY DECISION on 2026-08-23, not by measurement**:
balance-sheet lines come from the CafeF PDFs, widened 2026-08-24 into CLAUDE.md §5 rule 24 (the
filing PDF is the ONLY permitted source; a quarter no readable PDF can produce is `missing`). The
JSON route is **UNTRIED, not disproven** — reconsiderable if the schema wall proves worse than `P5`
budgets.

### ⚠️ THREE THINGS THAT MUST **NOT** BE BUILT, on measured evidence

| gap | why NOT |
|---|---|
| `silver.cafef_news_sentiment` 3 → 781 | **§2a**: PhoBERT tone had no signal and made price/TA models **worse** (QWK 0.175 → 0.045). `pool__news_daily` (counts, no tone) then measured `z = +0.53`, mid-pack of six failing pools |
| `cafef_prop_trading` 431 → 781 | flow **starts 2023-01-03**, coverage 0.20. §6-1: shortlisted by 4 of 5 tickers, all-NaN in folds 1-4 train, imputed to `0.0` and ranked (rule 23). ⚠️ **EXCLUDE at this timescale, not extend** |
| `trading_view_stocks` 571 → 777 | not in the price spine. ~**10 h** of Selenium for a source nothing reads for prices. Re-scrape only when the UNIVERSE needs refreshing |

⚠️ **`plan.md` SCREEN IS BUILT** (2026-08-22): `filter_schema.universe__<screen>`, three screens
materialised (`PRICE10K` 480/781, `LIQUID` 206/781, `QUALITY` 200/781) with their `unified_schema_*`.
**What is built is the MECHANISM, not the measurement** — no selection and no model has run on a
screened basket, so the prediction below is still unscored.

---

## ⚠️ THE TRADABILITY GATE — measured 2026-08-22

Evidence attached to existing items; nothing renumbered. Full measurement in `pipeline.md` §9-§10,
operating rules RUNBOOK §8 rules 14-16.

**Asked:** cap the book at 5 names. **Found:** `--top-k` already IS that cap, all ten stages were
`up to date`, and the published h=10 track at k=5 gives CAGR@30 **+181.3 %** against k=20's +74.0 %,
clearing a 200-draw null at **z = +15.01**. Three things then turned that into a warning:

| measured | number |
|---|---|
| median matched turnover of a PICKED row at k=5 | **0.03 bn VND/day** (universe median 2.22) |
| picks under 0.1 bn/day at k=5 / k=20 | **61.4 %** / 38.6 % |
| daily IC across h=1 … h=30 | **FLAT**, +0.1403 … +0.1328 — it should peak at its own label |
| rows with a forward 1-day return of EXACTLY zero, ADV60 < 0.1 bn | **51.2 %** |
| CAGR h=10 k=5 under ADV60 >= 1 bn / >= 5 bn | **+36.5 %** / **+19.9 %** (from +181.3 %) |
| `z` at k=5 vs k=20, gated, all 8 cells | **k=20 is higher in every one** |

✅ All eight gated cells still clear their own null, so **the signal survives the gate — the LEVELS
do not.**

| item | what changed |
|---|---|
| **`P11`** (execution realism) | ⚠️ **a measured price tag: ~145 pp of CAGR at k=5**, the difference between a headline and a strategy. Ranked on argument before; on a number now |
| **`P10`** (portfolio construction) | ⚠️ **the `k` half is answered and the answer is NO** — lowering `k` raises CAGR only by concentrating into unbuyable names. What is left is **weighting and the ladder** |
| **`P18`** (survivorship) | unchanged in rank; adjacent — a name that stops trading is not delisted here, it returns 0 forever, and 0 ranks well in a falling cross-section |
| **`P25`** (`cs_rank_5day`) | ⚠️ the h=5 numbers above are the h=10 MODEL traded every 5 sessions, **not** a chain selected and trained on `cs_rank_5day` |
| **`P7`** (live scoring) | ✅ cheaper than believed — the 2026-06-11 cliff is in the LABEL, not the price; all 150 names carry a close through **2026-06-25** |

### ⚠️ A PREDICTION, RECORDED BEFORE THE SCREENED CHAIN RUNS

When the screen is built and the chain re-selected and retrained on the screened basket:
(1) **Sharpe@30 lands near 1.0-1.4 at h=10, not 2.5**, CAGR **+25 … +45 %/yr**; (2) **daily IC falls
to ~0.06-0.09** from 0.1412; (3) **the horizon ladder flattens** — the h=1 gap is a staleness
artefact; (4) ⚠️ **it will still clear its within-date null.** If (1) comes back near +2.5 on a
genuinely liquid basket, this block is wrong and the post-hoc gate was the flawed instrument — say
so here rather than editing it out.

---

## THE LIST — read TOP-DOWN; the number is a NAME, not a rank

### the live program

⚠️ **Every cost marked `est.` is an estimate anchored to a measured run**, so it cannot be read back
as a measurement. Anchors: `walkforward` §12 (~20 min per `gbt` track), §11 (2h 48m for 7 arms × 10
folds), §13 (44m 12s for a 162-channel selection, no null), §9 (33m 26s + 8m 59s for one LSTM track
with a 200-draw null).

| # | item | ⏱ | local? | why it is here |
|---|---|---|---|---|
| | **⬛ 0 · THE PDF PARSER ITSELF — ⚠️ ADDED 2026-08-27, ABOVE EVERYTHING.** Two of these are WRONG NUMBERS every gate passes; two were the cost `P38`/`P6` are budgeted on | | | |
| **P55** | ⚠️ **FIVE OF THE TEN ARE DONE — CTG's FOUR AND `PYR-1`'s; SIX REMAIN, AND THEY ARE TCB's PLUS ONE** | ~½ day | ✅ CPU | ✅ **DONE 2026-09-02 for CTG: Q1-2009 bs, Q2-2011 bs, Q2-2011 cf and Q1-2014 cf, four defects, and NOT ONE was an OCR failure** — `NOT-1` (the table's own column heading scoring **0.8125** against the NOTES title, so every CONTINUATION page reads as a note and the statement is truncated at the first of them), `SLH-1` (a `/` in the COLUMN GAP making the box parse as no number at all), `TAI-1` (the tail-page test knowing one spelling of the closing line and demanding more figures than that line carries) and `MTL-1` (a grand total merged onto the line above, where the SHORT `von_chu_so_huu` ended the row and took **the whole balance sheet as equity**). CTG is **208 of 210** cells, cash flow 70/70. ⚠️ **The CASE discriminator this item proposed for Q3-2010 was NOT what shipped** — the suffix search in `_anchor` reaches all three shapes and self-guards, where a caps rule would have cut `Tiền gửi tại NHNN II` to `nhnn_ii`. CLAUDE.md §6-2-septquinquagies. ⚠️ **WHAT IS LEFT**: TCB's five (Q4-2008, Q2-2012 `no total assets`; Q2-2019, Q3-2019, Q1-2021 `no closing cash balance`) and **CTG Q3-2010**, whose grand-total label wraps over THREE OCR lines with its two period figures interleaved between the halves — `onnx@300` does not join it, and the four figures to check any fix against are assets **321,339,286,721,871** = liabilities **303,973,587,730,206** + equity **17,174,049,474,868** + minority **191,649,516,797**, exactly. The original reading of the ten follows. **`no total assets` × 5** — CTG Q1-2009, Q3-2010, Q2-2011 and TCB Q4-2008, Q2-2012. **`no closing cash balance` × 5** — CTG Q2-2011, Q1-2014 and TCB Q2-2019, Q3-2019, Q1-2021. ⚠️ **THEY ARE NOT ONE DEFECT EACH.** The new `absent_rows` dump already splits the first cluster three ways on CTG alone: **Q3-2010 is a MERGED SEAM WITH NO NUMERAL** (`… nội bảng khác (t44) TỔNG CỘNG TÀI SẢN CÓ` carries 321,339,286,721,871; `MERGED_SEAM_RE` wants a roman numeral or a two-digit code and `t44` is neither, while the liabilities row has no marker at all — the only discriminator left is CASE, and `slug` lowercases before `_split_merged` sees it); **Q2-2011 has no total-assets row on any classified page** (pages [1,7,8,9], 64 rows) and **Q1-2009 classifies 17 rows from page 1 alone** — both page-CLASSIFICATION failures. ⚠️ Read the dumps FIRST and count how the ten split before writing any rule; a repair in `_split_merged` or `_page_kind` is DEFAULT-PATH, so it needs `NST-1`'s standard — re-map every archived statement under all six mapping-flag combinations, then re-parse five filings end to end. §6-2-sexquinquagies |
| **P56** | ⚠️ **`PYR-1` — THE ROOT CAUSE IS FIXED; THE CORPUS SCREEN IS NOT** | ~2 h | ✅ CPU | ✅ **DONE 2026-09-02 for the CAUSE, and it was never a mis-read**: CafeF filed the FY-2023 report under Q1-2024, and the proof is the index's own `file_date` — **2024-03-29, two days BEFORE the quarter ended**. `documents()` now ranks a filing that predates its own period LAST, behind entity and ahead of assurance; measured over the whole archive, **3 of 26,040 quarterly documents move** (CTG and ANV Q1-2024, and HSG Q3-2023 whose own filename reads `..._quy_2_nam_2023`). CTG's Q1-2024 income statement was repaired, adjudicated by arithmetic — the four 2024 quarters now sum to the **audited FY-2024 PBT of 31,763,925 mn, residual exactly 0** — and so were its balance sheet and cash flow, which carried **Q4-2023's figures to the đồng**. CLAUDE.md §6-2-septquinquagies. ⚠️ **WHAT IS LEFT IS THE SCREEN**: run the four-quarters-sum-to-the-audited-annual test over all seven parsed tickers, because a wrong-period statement can reach disk any way a document can be mislabelled, and `file_date` only catches the ones CafeF dated honestly. The original reading follows. CTG **Q1-2024** held FY-2023's four quarters summed, **to the đồng, difference exactly 0** — 24,989,525,000,000 for a bank whose quarterly PBT runs 6-7 tn. `pdf` at `onnx@200`, older than 2026-09-02, and invisible to both gates: `sane`'s ±20× band contains it and `reconcile` has no cross-quarter test. ⚠️ **It propagates**: `_decumulate` wrote Q4-2024 = FY − (Q1+Q2+Q3) as **−6,528 bn** before a four-quarter read caught it and it was reverted from the backup. **The screen is free and general** — the four quarters of a year must sum to the audited annual AND each must sit inside the ticker's own quarterly range; here they summed correctly while the SPLIT was nonsense, which is exactly the signature. ⚠️ `P48`'s screens check magnitude and unit and neither checks the split. Run it over all seven parsed tickers before any more de-cumulation is written. §6-2-sexquinquagies |
| **P57** ✅ | **DONE 2026-09-02 — VIC 27 → 70 quarters, 63 documents on a free T4** | ~6 h Kaggle *actual* | ❌ T4 | Three kernels, because Kaggle caps a run at 12 h: **21 documents each, 3 h 54 m / ~1 h / 1 h 10 m, 0 engine errors throughout**, onnx-only 53-layer cascade so the readings come from the same cascade the ticker's earlier rows did (`TSS-1`). ⚠️ **THE FIRST CHUNK COST 3.5× THE THIRD FOR THE SAME 21 DOCUMENTS, AND IT IS THE FILINGS RATHER THAN THE MACHINE**: chunk 1 is 2008-2015, where a statement that defeats the cascade pays all 53 layers; chunk 3 is 2021-2026, where almost everything accepts at `onnx@200`. Merged two-pass oldest-first and UNFORCED, diffed column by column against each pre-merge backup — **0 columns lost, 0 periods lost, no existing figure overwritten** — and screened: total assets continuous 6.0 → **1,088 nghìn tỷ**, no step above 1.7 ×/quarter. balance sheet 23 → **60** `pdf`, cash flow 25 → **58**, income statement 24 → **45**. ⚠️ **53 cells remain and 27 of them are income statements** — a cumulative Q2/Q4 cannot be de-cumulated while its own prior is `missing`, so that statement lags the other two BY DESIGN and unblocks in cascade as they land. ⚠️ `CRP-1` stands: nothing from a `corp` template may be quoted as a fundamental. §6-2-sexquinquagies |
| **P58** | ⚠️ **TCB Q3-2012 AND Q4-2008 PARSE AND WERE DELIBERATELY NOT WRITTEN** | ~2 h | ✅ CPU | Both are **STANDALONE** filings (they are why `ALLOW_PARENT` is needed at all), so `sane`'s band — keyed by ENTITY since `SAN-1` — is **EMPTY**, and writing them means `FORCE_EMPTY_BAND`, which lifts the one guard that would judge them. ⚠️ **And the one cross-filing figure available DISAGREES**: Q3-2013's consolidated filing prints Q3-2012 = 603,429 mn where this parent-only reading gives **768,830 mn**. Parent ≠ consolidated, so the gap may be real — which is the point: **it is unresolved, and an unguarded figure from a different entity is not a coverage win**. Q4-2008 is thinner still: **3 mapped line items** in the income statement, 6 in the cash flow, below `MIN_ITEMS_FOR_HISTORY`. Decide it with evidence (the FY-2012 audited annual's own comparative, or TCB's Q1..Q4 2012 summing to it), not by preferring the newer run. §6-2-sexquinquagies |
| **P59** | ⚠️ **`SGN-1`'s 23 SURVIVORS — the code fix cannot reach a row already written** | ~3 h | ✅ CPU | De-cumulation happens at WRITE time, so fixing it corrects every FUTURE subtraction and touches nothing on disk. Measured 2026-09-03 over the 385 `pdf` income-statement rows: **24 are mixed de-cumulations**, of which **1 was repaired** (CTG Q2-2014, because Q4-2014 needed it as an operand) and **23 remain** — CTG Q2-2011/12/13/15/16/17/19/21/22/23/25, Q4-2013/15/19/21/23/24/25 and VIC Q2-2017/19/20/23/24. ⚠️ **THE SCREEN AND THE REPAIR ARE BOTH FREE AND ALREADY WRITTEN DOWN**: a mixed row is convicted by the closed form that re-signing its priors makes every one of its own identities close (OCR damage never does that), and the repair is `D_right = D_wrong + 2 × Σ(flipped priors)` — no OCR, no network. ⚠️ **PREFER RE-MERGING FROM THE RUN FOLDER over the closed form** where the archive holds the cumulative filing: that path goes through the fixed `_subtract_priors` and is the same code any future write uses, where the closed form is a second implementation of the same arithmetic. ⚠️ **EACH ONE UNBLOCKS ITS OWN Q4** — a corrupted Q2 cannot state its convention, so the Q4 that subtracts it correctly drops its deduction columns; CTG Q4-2014 went from 10 columns with 6 dropped to 16 with 0 the moment Q2-2014 was repaired. ⚠️ **ADJUDICATE AGAINST THE FILING, never by preferring the newer run** — the Q3 filing's own 9M cumulative column is an independent third document and is what settled CTG 2014 (agreeing to 7 significant figures). Diff every column against a pre-merge backup. `SGN-1` · §6-2-undesexagies |
| **P49** ✅ | **DONE 2026-09-01 — the income statement has an arithmetic gate** | ~3 h *actual* | ✅ | **↓ detail block** |
| **P50** ✅ | **DONE 2026-09-02 — the corpus screen: 307 of 1,093 rows differ, and `LNB-1` is 96 % of it** | ~13 h GPU *actual* | ✅ | **↓ detail block** |
| **P51** | ⚠️ **BUCKET WORDS BY GEOMETRY, NOT EMISSION ORDER — tried 2026-09-01, MEASURED WORSE** | ~1 day | ✅ CPU | **↓ detail block** |
| **P46** | ⭐ ⚠️ **THE UNIT REPAIR IS UNREACHABLE WHERE IT IS NEEDED — 8 TCB statements written 10⁶ WRONG** | ~4 h | ✅ | `unit_from_document` is carried by three layers at **positions 41-43 of 47**, and a statement that reconciles at layer 1 ends the cascade — so the repair never runs where it is needed. ⚠️ **A uniform 10⁶ error reconciles against itself**, so `sane` is the only gate that sees it, and on a ticker with no history it is open (`BND-1`). Measured on TCB 2026-08-29: **8 statements read `unit=1` against a ticker norm of 1,000,000** — Q1-2014 PBT read 673,136 for a company that earned 673 tỷ. ✅ Five were repaired in 8m 58s with the cascade restricted to those layers, each at **exactly ×10⁶**. **The fix is not a new layer**: make `unit_of` return `None` for silence (it already distinguishes them internally) and consult the DOCUMENT's declared unit on the DEFAULT path when the statement is silent and the filing does not contradict itself — `declared_unit()`/`document_unit()` already exist and are wired to a flag. ⚠️ **MEASURE THE BLAST RADIUS FIRST**: re-map the stored `row_dump`s and require 0 changed cells on ACB/VCB/BID. `UNT-1` |
| **P47** | ⚠️ **BOOTSTRAPPING A NEW TICKER — the OCR job cannot, and nothing says so until afterwards.** ✅ **(b) DONE 2026-09-04**; (a) OPEN | ~1 h | ✅ CPU | `seed_history` reads DISK and re-seeds per document while `build()` accumulates within its run, so a ticker with no CSV parses with `sane` open, `pdf_ocr_merge` refuses every empty-band statement, nothing is written and the band stays empty. TCB paid **5h 21m** to learn it; 9 of 169 cells were wrong. **(a)** `plan()` should REFUSE — or warn ONCE, up front, before GPU is spent — when the ticker has no accepted quarter and no filter narrows the run, naming the Dagster path; today the warning is per document, after the cost. **(b)** Ship the two screens that convicted TCB's nine cells as CODE (a `unit` minority screen, a total-assets continuity screen over a finished run folder) — free, no OCR, and what `sane` would have done. ⚠️ **(b) is worth more than (a)** — it also runs on tickers that DO have history. ⚠️ **NEITHER IS DONE**; what shipped 2026-08-30 is `FORCE_EMPTY_BAND`, which opens the write half of the loop and makes (b) MORE urgent, not less. ⚠️ **THIRD INSTANCE 2026-09-04 on HOSE_FPT** — the same screens, written ad hoc for the third time (TCB, CTG, FPT), convicted **5 of 128** accepted statements before they could reach a CSV: two cash flows whose closing balance is a literal TAIL of the right figure (795 against 7,153,625,069,795) and two income statements carrying figures above 200 tn VND. ⚠️ **And the `unit` screen pointed at the right rows for the wrong reason again** — the two convicted income statements ARE the minority unit and the unit is CORRECT; what convicts is the MAGNITUDE, so (b) must ship the magnitude checks and not the unit one. ✅ **(b) SHIPPED 2026-09-04 as `web_scraper/statement_screens.py` (9 tests, no PDF/OCR/network) — `screen_document` for the identities a filing asserts about itself and `screen_run` for total-assets continuity, which needs the whole batch because a figure wrong by 10^6 reconciles perfectly against itself.** ⚠️ **The `unit` screen is deliberately NOT shipped**, on this item's own measurement: it convicted 8 TCB statements correctly and then flagged 32 CTG ones that were all right. `accepted.values` are ALREADY scaled, so the declared unit is a fact about the FILING and never evidence about the figure. ⚠️ **(a) is still open** — `plan()` still warns per document, after the cost. `BND-1` |
| **P43** | ⚠️ **A FREE INVARIANT NOBODY CHECKS — 10 BID cash flows ALREADY ON DISK** | ~3 h | ✅ CPU | **↓ detail block** |
| **P54** | ⚠️ **`NST-1`'s FIVE DISK ROWS, AND `NST-2`'s SLID ONE — the code reads four of them correctly now** | ~3 h | ✅ CPU | **↓ detail block** |
| **P52** | ⭐ ⚠️ **`EQW-1` — 20 VCB BALANCE SHEETS TAKE A SUB-LINE AS TOTAL EQUITY, and the rows on DISK are right** | ~4 h | ✅ CPU | **↓ detail block** |
| **P53** | ⚠️ **`MPD-1` — an index-only filing raises instead of being skipped; it ended a 67-document job in 30 s** | ~1 h | ✅ CPU | **↓ detail block** |
| **P54** | ⭐ ⚠️ **`SEC-1` — EIGHT VIC BALANCE SHEETS ON DISK DO NOT ADD UP, AND THE GATE THAT FINDS THEM ONLY SHIPPED TODAY** | ~4 h | ⚠️ GPU | The VAS section sums (`A + B = TỔNG CỘNG TÀI SẢN`) became part of `reconcile` on 2026-09-04. Replaying them over the rows already written convicts **8 VIC quarters**, every one `source='pdf'` and past every gate that existed when it was written: **Q1-2011 holds `A + B = 300`** — the item codes 100 and 200 read as figures, `MSO-1` — against a printed 26,146,849,247,419; **Q3-2022 holds `A + B = 1,017,000,000`** against 555,571,017,000,000; Q3-2017 is out by 204.5 tn; and Q2-2011, Q4-2013, Q2-2015, Q4-2016, Q3-2021 by 13 to 253 tn. ⚠️ **A code fix cannot reach a row already written** (`EQW-1`'s shape, and `P52` is the same job for VCB): each needs a re-parse and a scoped `force_differs`, **adjudicated by the filing's own printed total** and never by preferring the newer run. ⚠️ **Budget it from the SCREEN, not the ticker** — 8 documents, not VIC's 71. `SEC-1` |
| **P55** | ⚠️ **FPT: TWELVE INCOME STATEMENTS REFUSED ON MAGNITUDE, AND THEY BLOCK TWELVE MORE** | ~3 h | ⚠️ GPU | Every Q1 and Q3 from 2020 is refused by `sane` at ~1e6 against a typical 6.58e11 — Q3-2020 1.74e4, Q1-2024 2.53e6, Q3-2024 8.11e6, Q1-2026 2.80e6 — i.e. the probe reads as though the figures were printed in triệu đồng and never scaled. ⚠️ **Each one blocks a CUMULATIVE Q2/Q4 that then has no prior to subtract**, so twelve quarters are worth roughly twenty-four cells, which is most of FPT's remaining 38. ⚠️ **NOT a regression**: all twelve were `missing` before 2026-09-04 too; only the refusal reason changed. ⚠️ **AND THE OBVIOUS CAUSE IS RULED OUT** — `onnx@300+deskew` reads Q3-2024's net revenue correctly at 15,972,397,069,700 with `unit=1`, so the UNIT is right and something else answers the `C_PBT` anchor. **Probe which row `C_PBT` maps to at the winning layer before spending another cascade**; the answer is in the run folder's `row_dump` and costs no OCR. CLAUDE.md §6-2-quattuorsexagies |
| **P48** | ⚠️ **A BRACKET OCR DAMAGED READS AS POSITIVE — 6 wrong cells on disk in a ticker nobody was looking at** | ~3 h | ✅ CPU | `PAR-1` and `QUO-1` are one family: a parenthesised figure whose bracket the recogniser mangled is written **positive, or from the wrong column, and it reconciles**. Three variants measured — a thousands separator read as a SPACE inside the brackets, a box the parentheses SPAN, and the OPENING bracket read as a quote — each found by accident. ⚠️ **The corpus has never been screened.** TCB Q4-2013 holds six such cells today: `Dự phòng giảm giá chứng khoán kinh doanh` **+427** where the filing prints **(1.427)** and its own subtotal settles it (921.035 − 1.427 = 919.608); `Phát hành giấy tờ có giá` +548 for (4.807.548); `Mua sắm bất động sản đầu tư` +902 for (129.902); `Tiền chi đầu tư góp vốn` +800 for (35.800) — **the current code reads all six correctly, so this is a data repair, not a code one**. **(a)** a free DISK screen: a line named `dự phòng`/`chi phí`/`hao mòn`/`chi `/`mua sắm` with a POSITIVE value is a candidate, as is any component failing its own printed subtotal. **(b)** re-parse and repair what it convicts, adjudicated by **the filing's own subtotals**, never by "the newer run wins". ⚠️ Budget the re-parse from the screen, not the ticker count |
| **P41** ✅ | **DONE 2026-08-30 — the share-capital scan was 69-77 % of a parse, and invisible** | ~1 h *actual* | ✅ | **↓ detail block** |
| **P44** | ⚠️ **TWO HOLES IN THE GATES THAT LET `P43`'s ROWS THROUGH** | ~2 h | ✅ CPU | **(1)** `sane`'s comparative-column gate is `if got and got in set(history)` — **exact integer equality, so a UNIT MISMATCH is blind to it** (BID Q1-2013 reads a figure equal to a stored quarter only after scaling). **(2)** `_closing_breakdown` fails OPEN when it cannot print a breakdown, so a negative closing balance passes. Both are cheap, and both are what `P43`'s screen would otherwise have to catch downstream |
| **P42** ✅ | **DONE 2026-08-30 — 24 parse keys, 7 OCR configurations, pages read 24 times** | ~1 h *actual* | ✅ | ✅ `PdfParser._ocr_cache` memoises `(text, words)` per page under `(engine, dpi, crop_pad)`, scoped to ONE document. ⚠️ `_split_number_runs` STAYS OUTSIDE the cache — `join_split_digits` is per-LAYER, so freezing it in would silently disable the flag. ⚠️ The progress denominator moved with it (`fin.ocr_key`, not `parse_key`). **Measured: BID Q4-2016 64.6 → 9.5 min (6.8x), VIC Q1-2026 34.8 → 5.2, TCB Q3-2013 39.1 → 7.9, VCB Q1-2026 1.4 → 1.4 (the control).** ⚠️ Verified on `rows_sha`, not the mapped cells. ⚠️ **Do NOT divide `P38`/`P6` by 6.8** — the multiplier applies to the failing tail, not the 83 % that win at layer 1. CLAUDE.md §6-2-duoquadragies |
| **P45** | **PARSER HYGIENE — four small things, none of them a number** | ~3 h | ✅ CPU | **(a)** `if not parser.ocr_ready and layer.engine != "onnx"` — with onnxruntime absent the onnx layers still run and silently return an empty text layer; **(b)** dead parameters left behind by earlier fixes; **(c)** refusal reasons computed and discarded on paths that do not print them; **(d)** two helpers with one implementation apiece in two places |
| **P39** ✅ | **DONE 2026-08-27 — the FX guard was wired to a FLAG, and had already written two wrong cells** | 1h26m *actual* | ✅ | **↓ detail block** |
| | **⬛ A · SCRAPE — the data has to EXIST and be FRESH before anything else is worth running** | | | |
| **P1** ✅ | **DONE 2026-08-23 — per-ticker freshness shipped** (`pipeline.freshness`, three `health_schema` SQL functions, three new `status_data` columns) | ~35 min *actual* | ✅ | ⚠️ **It corrected two documented claims on its first run.** (1) The 13 post-re-scrape stragglers carry **SEVEN** distinct dates, not thirteen — `FRZ-1`'s own list disproved its prose, and that number is the diagnostic separating a delisting from a scrape failure; the conclusion survives, re-verified another way. (2) **28 of 30 single-name unified schemas are stale**, in four layers that are a fossil record of every scoped re-scrape — now `SCH-1`, rebuilt the same day at 21 s each. ⚠️ **It also created `DEP-1`, the most reusable thing here**: shipping the health objects as VIEWS blocked every `DROP TABLE` in the repo's builders, so **the monitor blocked every repair it recommended**; fixed with `plpgsql` functions, whose bodies carry no dependency. ⚠️ The alarm is a **SHARE**, not a count (an absolute floor of 5 fired immediately on five genuine delistings; the two measured regimes are 0.6 % and 77 %), and `sessions_behind` is counted against the **price spine's** calendar — a frozen table's own dates cannot contain the sessions it is missing. **22 tests, no database.** CLAUDE.md §6-2-quinquies |
| **P2** | **SCRAPE FILING PDFs — ✅ phase 1 (`year_max=2020`) DONE 2026-08-23; phase 2 PARKED behind the OCR** | 74 min *actual* + 267 GiB | ✅ | ✅ **50,345 of 50,382 expected documents landed for all 784 codes**, one Dagster run, 0 errors; the 37 absent are CafeF's dead links (404 on both hosts), verified per ticker against a pre-run count. ⚠️ Phase 2 is ~269 GiB against 197 free. **↓ detail block** |
| | **⬛ B · OCR — ⚠️ the source is FIXED (CafeF PDFs, 2026-08-23). The time wall is solvable; the SCHEMA wall decides how many names this reaches** | | | |
| **P40** ✅ | **DONE 2026-08-27 — BID is 171/171 from 2012, all three statements complete** | 35m 20s *actual* | ✅ | **↓ detail block** |
| **P38** | ⭐ **PARSE THE VN30 BASKET — 27 tickers left, ONE AT A TIME** ⚠️ started 2026-08-25; **VIC was started 2026-08-28 and stopped by hand at 27 of 72 quarters (12 h)** — see `P5` | ~63 h GPU *est.* | ✅ | **↓ detail block** |
| **P37** | ⚠️ **TURN OFF THE HTML FALLBACK, THEN RE-PARSE ACB AND VCB AUTHORITATIVELY** (`FIN-1`) | ~5 h GPU | ✅ | ⚠️ **This lands before `P6`**: the builder defaults `use_api=True`, so running the OCR program first imports transcribed rows at 784× scale. **↓ detail block** |
| **P6** | ⭐ **OCR THE ≤2020 CORPUS** (50,345 documents, 784 codes, on disk) | days of GPU | ⚠️ see below | ⚠️ Group 0 sits above it — `P41`/`P42` change what it COSTS and `P39`/`P43` what it would WRITE. ⚠️ **Running it today reaches 20 of 784 names** (`P5` decides 20 or 784). ⚠️ **A third wall, measured 2026-08-24**: `documents()` keeps `consolidated == "True"` and nothing else, so **273 of 784 tickers yield NOTHING** — a company with no subsidiaries files no `hợp nhất` report. ✅ `allow_parent` shipped off by default: **13,912 → 26,280 documents**, empty tickers 273 → **22**, and it DOUBLES the bill. ⚠️ The parse skips complete YEARS, not quarters (`_decumulate` needs this run's priors). ⚠️ `skip_existing=False` is the AUTHORITATIVE run — skipping makes it a subset run and flips `sane` open. ✅ **The Kaggle route exists and is measured**: `web_scraper.pdf_ocr_job` is the cascade with the WRITE removed and the machine a parameter; it writes a run folder, never a statement CSV, and `compare()` scores it cell-by-cell against disk (VCB Q1-2026: **98 of 98 identical**, same layer, two torch majors apart). ⚠️ **Do not budget on a speedup**: 1.55x on an easy filing and 1.24x on the hardest, and **both figures were withdrawn the same day** when two further local runs came in at 50.8 s / 50.3 s against 100.6 / 113.3 — a **2.25x** swing on one machine, the T4 sitting between the clusters. What a T4 buys is a SECOND machine running free in parallel, which is not a multiplier. `ORT-1` is why the first T4 run was a further 21 % slower |
| **P5** | ⚠️ **NON-BANK RECONCILE ANCHORS — ✅ the anchors are DONE 2026-08-28; what is left is the PAGE SCAN** (`TPL-1`, `CRP-1`) | ~1-2 days *est.* | ✅ | **↓ detail block** |
| **P4** | ⚠️ **PUSH THE OCR TO KAGGLE — a `kgpu` job for the statement parse** | ~2-3 days | ⚠️ **quota** | ~2.4 h/ticker locally → 781 tickers ≈ **78 days**; a T4 is 15 GiB and free 30 GPU-h/week. ⚠️ **`kgpu` had never shipped a non-table payload**, so three things must be measured before quota is spent: (a) the Kaggle **dataset size limit** against 100 GB of PDFs — if it binds, the job is per-ticker-batch; (b) whether the **onnx OCR stack** installs on Kaggle's image; (c) the **5.2-min queue floor**, which makes many small jobs the wrong shape — batch. ⚠️ `rehearse` runs the worker side locally and spends NO quota — do that first |
| | **⬛ C · THE CHAIN'S OUTPUT — unblocked the moment A lands, on a DIFFERENT resource from B** | | | |
| **P7** | ⚠️ **THE LIVE-SCORING MODULE — it does not exist** | ~½ day *est.* | ✅ | Every stage writes predictions for a **dataset's test split**. Nothing loads a trained fold, windows the last 20 sessions for all 150 names on today's date and emits a ranking, so the chain cannot answer *"which ticker, on which date"* for a date not already in a split. ✅ Unblocked 2026-08-23. `pipeline.md` §6.2 |
| **P8** | ⭐ **rank the FEATURES within date, as the selection did** (`FNM-1`) | ~1 h + 20 min *est.* | ✅ | the largest single untried MODELLING lever; three measurements on disk point at it. **↓ detail block** |
| | **⬛ D · MODEL AND MONEY — everything below is a better number, not more data** | | | |
| **P9** | **ensemble the seven arms** — predictions already on disk | ~30 min *est.* | ✅ CPU | free variance reduction at ρ 0.91-0.94; no GPU, no training. **↓ detail block** |
| **P10** | **portfolio construction** — weighting, a laddered book | ~4 h *est.* | ✅ CPU | the only place with a **−55 % drawdown** to spend and no retraining to pay for. ⚠️ Run WITH `P11`; the `k` half is already answered. **↓ detail block** |
| **P11** | **execution realism** — ADV cap, sell-side floor days, the ATC auction | ~1 day | ✅ CPU | ⚠️ **a measured price tag: ~145 pp of CAGR at k=5.** Moves the LEVELS, and the levels are what is left |
| **P12** | ⚠️ **take the 30-name VN30 result DOWNSTREAM** | ~2 h | ✅ | `t = +3.77` is a **SELECTION bar**, and per §5d + `P2-3` a cleared selection bar has **never once** survived to a model here. ⚠️ A negative closes the thread cleanly |
| **P13** | **a wider cross-section at h=10 that is NOT VN30** | ~1 h | ✅ | §6-1-quater cannot separate **N** from the **date window** from the **universe rule** — all three moved together. Reuses `ProvidedPanel`, no Kaggle quota |
| **P14** | **train the estimand** — a ranking loss instead of MSE | ~½ day *est.* | ✅ | the only untried change aimed at **what is actually scored**. **↓ detail block** |
| **P15** | **BUILD cross-sectional channels** — selection is nearly exhausted, construction is not | ~1 day + T4 *est.* | ⚠️ **quota** | the only real FEATURE lever left inside the data already held. **↓ detail block** |
| **P16** | **sweep `lookback`** — the one dataset knob never swept | ~4 h *est.* | ✅ | `walkforward` §12c named it; nothing has moved since. **↓ detail block** |
| **P17** | **date-only pools as a REGIME OVERLAY**, not as ranking channels | ~1 day *est.* | ✅ CPU | risk control, **not** a fix for the decay — §9b located that in the features. **↓ detail block** |

### the backlog

| # | item | ⏱ | note |
|---|---|---|---|
| | **⬛ E · HONESTY — each one makes an EXISTING number readable, not bigger** | | |
| **P18** | **survivorship** — a point-in-time listing/delisting table | ~2 days | `z = +18.6` is protected by the null; **+74 %/yr is not**. A DATA problem, not a code one. ⚠️ Adjacent to the stale-price finding |
| **P19** | ⚠️ **new information** — intraday/tick, point-in-time listing, dated fundamentals | months | ⚠️ **The main lever, not the last resort**, and `P2`-`P6` are its third item starting. **↓ detail block** |
| **P20** | **the rolling-vs-expanding training window** | ~1 day | mostly answered by `PRF-2` — the post-2022 break is in the FEATURES, not the market. Only the training-window half is left. **↓ detail block** |
| **P21** | emit **`n_dead_train` / `n_dead_test`** from `validation.csv` | ~2 h | rule 23 explains **both** apparent clearances in §6-1 and the column still does not exist, so every ragged-pool run needs a hand computation to read at all |
| | **⬛ F · BACKLOG** | | |
| **P22** | **re-run each LEGACY walk-forward track once** | ~0 | ⚠️ a limit CREATED 2026-08-21, not discovered: the `WFO-1` guard covers the five pre-existing tracks on their **TABLE only**. One re-run writes the manifest and closes it |
| **P23** | **re-fit the cost model into ONE function** | ~2 h | two models exist, disagree, and were both fitted with `lasso` — dropped 2026-08-16. The guard over-predicts by 4-13×. **↓ detail block** |
| **P24** | **the streaming design** — cut the host-side peak so a top-300 panel fits | days | stop materialising the whole design; window per fold or per ticker-chunk. 4.03 GB per million rows, measured. **↓ detail block** |
| **P25** | **`cs_rank_5day` on the top ~300 by turnover** | ~1 h | puts a number against §2b's `ALL` row, still reading *"never ran — unverified"*. ⚠️ Blocked behind `P24` at that width. **↓ detail block** |
| **P26** | **`CSP-1`** — give `read_universe_panel` the `UnifiedSchemaReader.join()` the single-ticker path uses | days | ⚠️ makes `MEM-1` worse by the width joined; `pool__ta` at 922 channels is ~10× the design |
| **P27** | `_ingest_gold_stocks` still carries a legacy column (`close`/`volume`: 1 of 2 present) | — | ⚠️ fixing it redefines `gold.stocks`' OHLC as adjusted and commits to a ~2.4 M × ~900 col rebuild |
| **P28** | **262 rows in `bronze.cafef_price` have `high < low`** (ACB 2018-07-31: high 35,800, low 36,500) | — | CafeF's defect; surfaces in gold as a negative `range_hl`. Needs a bronze data-quality screen. Re-verified 2026-08-17, still 262 |
| **P29** | `bronze.cafef_insider_shareholder_transactions` covers **100 of 781** — decide whether that partition list is the intended scope | ~1 h | it matches `raw/cafef_pdfs`' old 100-ticker list exactly, so probably deliberate and undocumented |
| **P30** | XGBoost warns in **every** run: *"Falling back to prediction using DMatrix due to mismatched devices"* | — | if the design is copied host→device per prediction, the GPU conversion is leaving speed on the table |
| **P31** | `landed()` cannot answer *"did THIS run produce anything"* | — | it rglobs a folder where the previous run's dated files still sit; 140 header-only CSVs went green. §5 rule 10's mechanism |
| **P32** | `logs/app.log` has many writers now, so records interleave | — | the executor is multiprocess and every step appends. Fix is per-process filenames in `Logger` |
| **P33** | `raw/trading_view` partitions `crypto` and `options` are permanently red | — | both `true` in config, folders never existed, `landed(require=True)` fails them |
| **P34** | decide the fate of `raw/trading_view_collected_links` — nothing reads it | — | it is a leaf, not a hub |
| **P35** | ⚠️ if ever backfilling TradingView, use a **single-run backfill** | — | `tag_concurrency_limits` is per-RUN, so 9 partitions the default way is 9 runs × 12 browsers = **108 Chrome** |
| **P36** | four heavy assets have never been observed running end to end through Dagster | — | `trading_view_links`/`data`, the 5 CafeF stock tabs + news, `cafef_pdfs`, `cafef_financials`. *"Built is not run"* |

⚠️ **THREE GAPS ARE DELIBERATE AND ARE NOT ON THIS LIST** (measured 2026-08-22) — the news-sentiment,
prop-trading and TradingView-stocks tables; see the "must NOT be built" table above.

⚠️ **EVERYTHING EXCEPT `P4`, `P6` AND `P15` IS LOCAL OR CPU** — nothing else touches the 30 GPU-h/week
Kaggle quota.

⚠️ **What is deliberately NOT on this list**, on measured evidence: **another architecture** (224× of
capacity tried at h=10, 101× at h=20), **another slice of `pool__ta`** (tied at both horizons), and
**another dataset setting** (six tracks, every `|t|` < 1.4). `P11`, `P16` and `P18` make the number
**honest**, not bigger, which is why they rank where they do.

⚠️ **A PREDICTION IS RECORDED FOR `P12` … `P20` BEFORE EACH RUNS.** This file holds five made that
way and **four were wrong** (`PRF-1`, `P4-12`, and both of the arm sweep's) — the only reason the
numbers that followed are worth anything. Score them; do not quietly edit them.

---

## THE CROSSWALKS — for anything written BEFORE 2026-08-23

⚠️ Resolving an old code takes **the DATE of the thing you are reading**, then the matching table
below; `grep` cannot tell the three schemes apart, which is the standing cost of bare codes and the
reason both bare → bare tables stay. ⚠️ **Dated blocks in this file were NOT renumbered and must not
be** — every "what was next on \<date\>", archive entry and recorded prediction keeps the numbering
it was written with, because renumbering a record of the past destroys information.

### 1 · written before 2026-08-23 → today: **`new = old − 3`**, uniformly, for `P4` … `P39`

Three items closed that day and the order was unchanged — only the labels moved. ⚠️ A mid-day
version of this table said `− 2`; if you saw it, subtract one more.

⚠️ **Old `P1`, `P2`, `P3` have no row on purpose** — they are the three that closed:
`P1` = the `FRZ-1` universe re-scrape (CLAUDE.md §6-2-bis), `P2` = the carry-up to
gold/filter/unified (§6-2-ter), `P3` = `STA-1`, rebuilding `gold.stocks_ta` (§6-2-quater).
⚠️ **Today's `P1` is the per-ticker freshness view** — a different item entirely, and `P1` meant
three different things in one day. That collision is exactly what a bare → bare renumber costs.

### 2 · written before 2026-08-22 evening → the 2026-08-23 scheme

| old | → | old | → | old | → | old | → |
|---|---|---|---|---|---|---|---|
| `P1` | `P1` | `P10` | `P19` | `P19` | `P26` | `P28` | `P36` |
| `P2` | `P10` | `P11` | `P20` | `P20` | `P27` | `P29` | `P37` |
| `P3` | `P11` | `P12` | `P14` ⚠️ up | `P21` | `P28` | `P30` | `P38` |
| `P4` | `P12` | `P13` | `P24` | `P22` | `P29` | `P31` | `P39` |
| `P5` | `P13` | `P14` | `P25` | `P23` | `P30` | `P32` | `P2` ⚠️ up |
| `P6` | `P15` | `P15` | `P3` ⚠️ up | `P24` | `P31` | `P33` | `P6` ⚠️ up |
| `P7` | `P16` | `P16` | `P21` | `P25` | `P33` | `P34` | `P8` ⚠️ up |
| `P8` | `P17` | `P17` | `P23` | `P26` | `P34` | `P35` | `P32` |
| `P9` | `P18` | `P18` | `P22` | `P27` | `P35` | `P36` | `P4` ⚠️ up |

⭐ NEW that pass: `P5` (scrape filing PDFs at scale), `P7` (the `kgpu` OCR job), `P9` (the mass OCR
run). ⚠️ **Then apply table 1** (`− 3`) to reach today's numbering.

### 3 · retired HYPHENATED codes → the 2026-08-21 one-list scheme

⚠️ **Read this before following any reference in CLAUDE.md, RUNBOOK.md, ISSUES.md or a
`CONTEXT.md`** — those files were never rewritten, so they still say `PRF-4` where this file says a
bare number. ⚠️ **These rows are frozen history**: they were renumbered once by a regex that could
not tell a live pointer from a record of the past, and the table then described a renumbering that
never happened. **A crosswalk that renumbers itself is worthless.** ⚠️ **The hyphenated codes never
move and are the safer thing to cite in another file.**

| retired | → (2026-08-21 scheme) | | retired | → |
|---|---|---|---|---|
| `M-1` | `P3` | | `P1-1` | `P19` |
| `M-3` … `M-8` | `P4`, `P5`, `P8`, `P9`, `P10`, `P11` | | `P1-4b` + `P3-2` | `P20` ⚠️ merged — one piece of work, three codes |
| `PRF-3` | `P17` (only the training-window half is open) | | `P2-2` | `P21` |
| `PRF-4` | `P12` | | `P3-1`, `P3-3` | `P22`, `P23` |
| `PRF-5` | `P16` | | `P4-1` | `P15` ⚠️ merged into the `STA-1` decision |
| `PRF-6` | `P18` | | `P4-2` | `P13` |
| `M-2` | — ✅ DONE 2026-08-21 (the seed floor), `walkforward` §15 | | `P4-3` … `P4-10` | `P24` … `P31`, in order |

| kept as-is | why |
|---|---|
| `SSK-1` | ⚠️ **a MEASURED RESULT, not a task** — its numbers are CLAUDE.md §6-1 / §6-1-bis |
| `PRF-0/1/2/7/8/9` | **DONE**; cited by name from CLAUDE.md and `walkforward/CONTEXT.md` |
| `P0-1`…`P0-6`, `P1-2`…`P1-9`, `P2-1`, `P2-3`, `P2-4`, `P4-11`, `P4-12` | **DONE**; in the Archive |
| `P0-7` | done and **deleted** per this file's rule — no reusable reasoning |

⚠️ **`ISSUES.md` CODES WERE NOT TOUCHED AND NEVER WILL BE.** `STA-1`, `FRZ-1`, `FNM-1`, `WFO-1`,
`NUL-1`, `CSP-1`, `MEM-1`, `VRM-1`, `DRF-1` … are permanent by that file's own rule. A TODO item may
*point at* one, and that is the relationship: **the issue is what is broken, the `P<n>` is what
somebody is going to do about it.**

---

### P49 · ✅ DONE 2026-09-01 — THE INCOME STATEMENT WAS GATED ON NOTHING ⏱ ~3 h *actual*

> ✅ **SHIPPED THE DAY IT WAS OPENED.** `OP_IDENTITY` is a table keyed by the operating-profit column
> — `{op: (added, deducted, optional)}` — so `reconcile` identifies the chart from the key it matched
> and needs no template argument. **`bank` (XI = IX + X) and `corp` (11 = 5 + 7 − 8 − 9 − 10) only**;
> `securities` and `insurance` have never met a filing and are absent on purpose (§5 rule 2).
> ⚠️ **Deductions are tried under BOTH sign conventions and either is accepted** — ONE bit for the
> whole statement, because a wrong DIGIT shifts both branches equally and a lost bracket does not;
> without it 12 of the 41 answerable statements would have been falsely refused. ⚠️ **The roles are
> deliberately NOT in `ANCHORS`** — that set drives `_anchor`'s re-match, and admitting five accounts
> would change which row every statement claims. **12 tests**, 493 pass in `src/web_scraper/`.

`reconcile` gives each statement one arithmetic test: the balance sheet `assets == liabilities +
equity`, the cash flow `opening + movement + fx == closing`. **The income statement got
`if get(C_PBT) is None` — that a PBT line EXISTS, and nothing about whether it is the right number.**

⚠️ **THAT IS WHY EVERY `SLD-1`-SHAPED DEFECT LANDS THERE.** Four on record, each found by hand:

| | what was written | what the filing prints |
|---|---|---|
| BSR Q3-2019 (`LNB-1`) | PBT **48,726,111,955** | **624,185,898,676** — 575 bn out |
| TCB Q4-2013 (`PAR-1`) | six cells positive where the filing brackets them | e.g. **+427** for **(1.427)** |
| ACB Q1-2024 (`PAR-1`) | `6_chi_phi_hoat_dong_khac` **+907** | **−109,907** |
| BID Q3-2011 (`QUO-1`) | two cells from the PRIOR-PERIOD column | the current quarter |

The measured case, on disk: BSR Q3-2019 at `onnx@300` reads `10 chi phi QLDN` 89,916,450,279 → 11
computes 599,695,436,083 against a printed 599,695,236,083 (**off by 200,000**); at `onnx@400` it is
EXACT. The cascade stops at 300 because that layer reconciles, so the better reading is never
reached and the error propagates into Q4-2019 through the de-cumulation.

Four things that made it delicate: **abstain, never guess** (run the identity only when every term
is mapped; a chart's optional lines are the hazard — a filing that prints one and a parse that
misses it would fail a sound identity); **the tolerance cannot be `_equal`** (`EQUAL_REL = 1e-5` on
599 bn is ±5,996,952 against an error of 200,000, so it is held to exact equality plus a few đồng of
the filing's own rounding); **the blast radius is a full re-parse**, not a `row_dump` replay, since
this changes which layer ACCEPTS; and ⚠️ **the risk is a FALSE REFUSAL** — the safe direction (§5
rule 2) but not free, so it was measured before shipping rather than after.

---

### P50 · ✅ DONE 2026-09-02 — 307 OF 1,093 ROWS DIFFER, AND `LNB-1` IS 96 % OF IT ⏱ ~13 h GPU *actual*

All seven parsed tickers re-parsed with `--overwrite`, **no merge**, onnx-only cascade (53 of 55
layers, or `TSS-1` swamps it). **371 documents, ~13 h, nothing written to `raw_data/`.**

| | REPRODUCED | DIFFERS | absent | ABSTAIN |
|---|---|---|---|---|
| BSR · VIC (corp) | 22 · 28 | 12 · 18 | 4 · 23 | 4 · 5 |
| BID · CTG · TCB · VCB · ACB (bank) | 85 · 107 · 92 · 110 · 125 | 66 · 58 · 50 · 61 · 42 | 6 · 13 · 13 · 5 · 4 | 28 · 25 · 24 · 33 · 30 |
| **total** | **569** | **307** | 68 | 149 |

**The answer, split by the DISK row's own engine** — which is the only thing that separates
`LNB-1` from `TSS-1`, since both produce a truncated figure:

| | disk read by onnx → `LNB-1` | disk read by tesseract → `TSS-1` |
|---|---|---|
| accounts recovered | **212** | 66 |
| truncated figures repaired | **192** | 8 |

⚠️ **`LNB-1` is 96 % of the damage** — and the opposite was predicted out loud when CTG and TCB
returned 53 and 65 truncations, on the reasoning that truncation is tesseract's signature.
⚠️ **`VAS-1` is `corp`-only as predicted, and now counted: 52 cells, 51 VIC + 1 CTG, 0 in bank.**
VIC Q1-2011's balance sheet holds **42** item-code cells where CLAUDE.md recorded four.

⚠️ **26 statements where the current code is WORSE than disk** — 20 VCB (one defect, `EQW-1`),
BID 3, CTG 2, TCB 1, ACB 0. Proven NOT to be `LNB-1`/`VAS-1`/`P49`: a re-parse under `7c3604c5`
reproduces the wrong figure.

⚠️ **AND THE SCREEN IS BLIND TO A CLASS ITS OWN CALIBRATION SET NAMED.** Of P50's three BSR 2019
residues it found **one** — (1) the 200,000, via `P49`. (2) sits in an ABSTAIN and is never
scored; (3) reads **REPRODUCED**, because the current code reproduces the error faithfully.
**`REPRODUCED` means the code agrees with disk, never that the figure is right**, so the 569
reproduced rows are 569 rows this screen says nothing about. `P43` and `P48`'s disk screens are
what covers them. CLAUDE.md §6-2-quaterquinquagies.

---

### P54 · ⚠️ `NST-1`/`NST-2` — FIVE WRONG DISK ROWS THE CODE NOW READS, AND ONE IT MUST NOT WRITE ⏱ ~3 h · *(opened 2026-09-02)*

A free disk screen — **total liabilities EXACTLY equal to total assets, grand-total column
blank** — convicts 5 of the 355 `pdf` balance sheets. `NST-1`'s fix changes what the CODE reads
for four of them, measured from their archived `row_dump`s with no GPU at all:

| | on disk | the current code |
|---|---|---|
| BID Q4-2013 | liab = assets = 548,386,083 mn | **516,093,515 mn** (0.9411, in line with its neighbours) |
| CTG Q3-2009 | liab = assets = 226,569,995 mn | **209,986,662 mn** (0.9268 against Q2's 0.9379) |
| BID Q1-2019 | liab = assets = 1,342,938,577 mn | **absent** — §5 rule 2, and better than wrong |
| BID Q3-2016 | liab = assets = 950,377,914 mn | **absent** |
| **CTG Q1-2011** | liab = assets = 395,843,604 mn | **unchanged** — see below |

⚠️ **THE ROWS ON DISK ARE UNTOUCHED.** Repairing one is a `force_differs` merge, and the repo's
rule is that the FILING adjudicates, never the newer run — the neighbour ratios above are
corroboration, not adjudication. Budget one re-parse per filing.

⚠️ **CTG Q1-2011 IS THE SAME DEFECT ONE WORDING FURTHER, and it is measured**: it prints
"TỔNG NỢ PHẢI TRẢ, VỐN CHỦ SỞ HỮU VÀ LỢI ÍCH CỦA CỔ ĐÔNG" — no abbreviation, but the same comma
for "VÀ" plus a long tail — which scores **0.774** against a bar of 0.86, and its genuine
liabilities row carries **no current-period figure at all** (`[None, 349,339,915,000,000]`). So
two things are wrong there and only one of them is this. The general repair is the CONNECTOR:
"A, B và C" and "A và B và C" name one total, and the information is destroyed by
`account.replace("_", "")` before any score is taken.

⚠️ **AND `NST-2` IS NOW A ROW ON DISK, NOT A ROW REFUSED.** VCB Q1-2009's balance sheet was
WRITTEN on 2026-09-02 by the owner's decision, with the slide measured and recommended against.
Its three grand totals are right; its ~40 line items are their neighbours'. **So this item is a
REPAIR, not a decision: either read the quarter correctly (the cascade reaches only
`onnx@400+loose`, and no later layer is tried once it accepts) or revert the row to `missing`.**
⚠️ Until then, nothing may quote a VCB Q1-2009 LINE ITEM; the three totals are sound. The proof itself is free and reusable: two consecutive interim filings print the SAME
prior-year comparative column, so their readings must agree line for line — 37 of 57 do not.

⚠️ **A half-empty-row screen is NOT the general detector.** On a 2-column balance sheet every
real line prints both columns, and Q1-2009 carries only one figure on 38.2 % of its rows against
a corpus median of 5.6 % — but the worst in the corpus is a WRITTEN statement at 46.9 %, so it
ranks and does not separate. Recorded so it is not re-tried as a gate.

### P52 · ⚠️ `EQW-1` — 20 VCB BALANCE SHEETS TAKE A SUB-LINE AS TOTAL EQUITY ⏱ ~4 h · *(opened 2026-09-02)*

Found by `P50`. On **every Q2 and Q4** VCB filing the ordered walk claims the merged
`VIII Vốn chủ sở hữu | Vốn của tổ chức tín dụng` before the `TỔNG VỐN CHỦ SỞ HỮU 22(a)` printed
below it — Q4-2024 reads **61,696,139 mn against a true 196,209,168**, which is `A − L` exactly,
with assets and liabilities reproducing to the đồng at the same layer.

⚠️ **THE ROWS ON DISK ARE RIGHT AND THE CODE CANNOT REPRODUCE THEM**, so this is a code repair
and NOT a data one — the opposite of `P48`. A re-parse under `7c3604c5` gives the same wrong
figure, so it is older than the 2026-09-01 fixes.

**The fix is `MEN-1`'s discriminator applied to the ordered walk**, which `MEN-1` measured as
changing 23 of 228 archived statements and therefore confined to `_anchor`. ⚠️ **So it cannot be
lifted wholesale**: the walk has POSITION to keep a fuzzy match honest and `_anchor` does not.
What is new is a bounded case — a row whose label CONTAINS the account as a prefix while a later
row NAMES it (`tổng` + the same account) — and the blast radius is measurable for free by
re-mapping the stored `row_dump`s of all 371 `P50` run folders. ⚠️ **Require 0 changed cells
outside the 26 regressions**, and adjudicate each by `A = L + E`.

---

### P53 · ⚠️ `MPD-1` — A FILING THE INDEX LISTS AND THE DISK LACKS KILLS THE RUN ⏱ ~1 h · *(opened 2026-09-02)*

`pdf_ocr_job.run` calls `os.path.getsize(task.path)` before opening each document, so ACB
**2009-Q3** — the one index-only quarter §6-2-sexquadragies measured — raised a bare
`FileNotFoundError` at document 3 of 69 and ended a 67-document job in ~30 s.

⚠️ **It fails at the END of a plan that already succeeded**, so a queued chain reports the ticker
done and moves on. Count it, skip it, and name it in the summary — the way a dead download link
already is (§6-2-septies). ⚠️ **`plan()` is the better place**: a document whose file is absent
is not a document the run can open, and the count belongs beside `documents :` before any GPU is
spent.

---

### P51 · ⚠️ LINE BUCKETING IS ORDER-DEPENDENT — the obvious fix is MEASURED WRONG ⏱ ~1 day

`table_rows` groups words into printed lines by walking `words_by_page[page]` **in the recogniser's
emission order**. `LNB-1` fixed half the consequence — the bucket is now the NEAREST within `Y_TOL`
rather than the first found — but **a bucket is still keyed by whichever word opened it, which is a
fact about emission order and not about the page**, so a word can join a bucket 3.1pt away because
the one 0.9pt away had not been opened yet.

⚠️ **THE OBVIOUS REPAIR WAS TRIED AND IS WORSE — do not re-make it blindly.** Sorting words by y
before bucketing is order-independent and gains a row on BSR Q3-2019 at `onnx@200`. **Reverted the
same hour**: a bucket keyed on its topmost word CHAINS, and the tolerance has no margin — rows on
that page are 11-13pt apart and wrapped label halves 4-8pt. At `onnx@400` it swept the deferred-tax
comparative into line 18 and wrote **1,648,126,921** as post-tax profit; at `onnx@300` it put the
prior-year column into the parent-company line. **Two wrong figures `reconcile` passes, for one row.**

A real fix needs: **a clustering with a stopping rule** (single-link chaining is the failure above,
so the criterion must bound a bucket's SPAN, not each word's distance from the key); ⚠️ **the page's
own line pitch as the evidence** — `_value_row_offset` already measures a per-statement geometry by
maximising CO-LOCATION without looking at the figures, and the same shape gives a median row pitch
that `Y_TOL` could be a fraction of instead of a constant 4.0pt; and **`P49`'s gate first**, because
every failure above is a wrong figure `reconcile` waves through.

⚠️ **THE SAME PATTERN SITS IN FOUR OTHER PLACES**, unmeasured: `_merge_split_figures`,
`split_figures`, `colocated` and the share-capital scan each take the first bucket within a
tolerance. None is known to be defective; none has been looked at.

---

### P41 · ✅ DONE 2026-08-30 — THE SHARE-CAPITAL NOTE SCAN ⏱ ~1 h *actual*

✅ **Profiled per phase rather than estimated, and it was bigger than the item said.** BID's FY-2016
annual: `scan` 14 pages / 37.6 s / 30.5 %, **`share_capital` 50 pages / 84.8 s / 68.8 %**, returning
nothing; VIC Q1-2026 (`corp`): `scan` 24.5 s / 22.9 %, **`share_capital` 58 pages / 81.9 s / 76.6 %**,
also nothing. ⚠️ **That is where the missing time was** — the scan calls `_ocr_page` directly rather
than through `scan`, so the page hook never saw it and 23 passes of ETA-inverted page rates summed
to 16 min against a 64.6 min run. ✅ **Fixed provably-identically**: `parse()` gained `want_shares`
and `_parse_cascaded` passes `not facts["publish_date"]` — the same condition the counts are READ
under two lines later. ⚠️ **Reduced, not closed**: a filing with no signing date on any page leaves
`facts` open and every layer still asks (TCB Q3-2013). Two further cuts were measured and
deliberately NOT taken because both change behaviour rather than cost — the bank-only anchor could
skip the other three templates outright (**0 of 91 `corp` rows on disk carry a share count against
201 of 753 `bank` rows**), and the walk still has no page budget. CLAUDE.md §6-2-duoquadragies.

---

### P43 · ⚠️ THE INVARIANT NOBODY CHECKS — 10 BID CASH FLOWS ALREADY ON DISK ⏱ ~3 h

**The invariant:** these filings print a cash flow CUMULATIVE from 1 January, so every quarter of a
year prints the SAME opening balance, and it equals the prior year's Q4 closing. §6-2-duovicies used
exactly this by hand to catch Q4-2016's 61,575,636 and called it *"reusable and cheap"*. **It is
still not code.**

| signature — over the three parsed tickers, 0 s, no OCR | ACB | BID | VCB |
|---|---|---|---|
| `closing` == prior Q4's `closing` — the 1-Jan opening in the CLOSING slot | 0 | **7** | 0 |
| `IV` == prior Q4's `closing` — the opening in the MOVEMENT slot | 0 | **2** | 0 |
| `opening` != prior Q4's `closing` | 0 | 6 | **17** |
| **negative closing cash balance** | 0 | **1** | 0 |

**The 7:** Q1-2013, Q3-2013, Q1-2014, Q1-2016, Q1-2017, Q3-2019, Q3-2020. ⚠️ **Every one accepted at
`onnx@200`** — a STRICT layer where `verify_cash` rides with `relax_totals` and is off, so
`_cash_flow_identity` never ran. ⚠️ Q1-2017 was already predicted by §6-2-quatervicies; **the other
six were not**, and they are why this is an item rather than a footnote. **The negative:** BID
Q3-2011, `closing = −23,457,326,032,339` — `reconcile` performs no sign test, `sane` compares
`abs()`, `_closing_breakdown` fails open, so all three gates pass it. ⚠️ **VCB's 17 are MIXED — a
SCREEN, not a verdict**: Q1-Q3 2023 open on 412,235,294 against a Q4-2022 closing of 412,135,294 (one
digit, an OCR error) while 2018's four quarters agree with each other and disagree with Q4-2017,
which is what a genuine restatement looks like.

**The work.** (1) A pure function over the three CSVs asserting the invariant per ticker-year and
reporting each violation beside its `method`; **it belongs at the end of `build` and must WARN, not
raise** — a restatement is legitimate. (2) A **sign test on the closing balance** in `reconcile`; a
balance below zero needs no threshold. (3) Re-parse the 10 flagged quarters, ⚠️ with a pre-run
backup and a diff that restores every non-target quarter the run moves — the history-provider
downgrade has reproduced **four** times, and the fourth lost **`publish_date` on four statements and
nothing else**, so a figures-only diff would have called that run clean.

⚠️ **TWO PIECES ARRIVED FROM `P39`, AND THEY ARE WHY THIS IS THE TOP ROW** — both are applications
of the invariant, not separate mechanisms.

4. ⚠️ **GUARD `alternates` AGAINST A RESTATEMENT — it needs the FORWARD reading, which is why `P39`
   could not do it.** When every layer refuses the chosen document, `alternates` retries another
   filing of the same period and entity. BID's **unaudited Q4-2016 closes ~62.6 tn where the audited
   annual prints 65,521,789**, both internally consistent, so `reconcile` and `sane` pass either —
   and that fallback is where the reverted 61,575,636 came from. The check `P39` proposed cannot
   separate them: the two filings share the SAME opening balance. What separates them is the opening
   the **2017** quarters print — three independent readings, all 65,521,789. **The guard is: an
   alternate-sourced row may not contradict what later quarters independently agree on.**
   ⚠️ **`FXM-1` cannot close until this ships**, and that issue now points here.
5. ⚠️ **FOUR BID ROWS CARRY A WRONG FX VALUE `P39`'s FIX DOES NOT REACH** — Q4-2011 `48,919,272` and
   Q2-2012 `40,110,402` are cash BALANCES sitting in the FX column, written at **strict** layers by
   `_align`/`_anchor` rather than by the positional guess `P39` removed. (Q4-2009 and Q4-2012 hold a
   literal `0`, a different question.) ⚠️ **`|fx| > 0.5 × |closing|` is a screen this item can run
   for nothing** and needs no tuning.

⚠️ **WHAT IT DOES NOT DO.** It cannot see a statement taken consistently from the comparative
column, and it cannot judge the income statement or the balance sheet — neither has a cross-quarter
identity of this kind. **One invariant, on one statement, and it found ten rows that 47 layers and
two gates did not.**

---

### P2 · ⚠️ THE PDF ARCHIVE, PHASED BY YEAR ⏱ scrape hours + 286 GiB

**Phase 1: every one of the 784 listed codes, `year_max=2020`. Then OCR that. Only then phase 2
(`year_min=2021`).** The ORDER is the item — running both phases first does not fit on this machine,
and OCR-ing 555 GiB before knowing whether the parser can read a non-bank filing (`P5`) is 78 days
spent to find out.

**Measured with no PDF downloaded**: `FileBCTC.ashx` lists a ticker's documents without serving one,
so the universe was counted for **784 small JSON calls — 123 s, 0 errors**; sizes come from the
15,217 PDFs already on disk whose byte counts the index CSVs record.

| | |
|---|---|
| universe | **784 codes · 84,076 documents** |
| size model | mean **7.02 MB/doc**, rising 2.75 MB (2008) → 9.32 MB (2025) |
| **whole corpus** | **≈ 555 GiB** |
| **phase 1 (`≤2020`)** | **50,382 docs ≈ 286 GiB** (~231 GiB not yet on disk) |
| phase 2 (`2021+`) | 33,694 docs ≈ 269 GiB |
| `D:` | extended 318 → 636 GiB; **461 GiB free** |
| per sàn | HOSE 333 codes / 44,547 docs / 292 GiB · HNX 206 / 21,034 / 141 GiB · UPCOM 245 / 18,495 / 122 GiB |

⚠️ **The flat and per-year size models agree to 1 %** (445 vs 450 GiB for the missing 676 tickers).
That agreement is the check; a single model would have been an assertion.

⚠️ **A PRIOR ESTIMATE OF 240 GiB WAS WRONG BY 2×, AND THE REASON IS WORTH KEEPING.** It extrapolated
from the 5 non-HOSE tickers in the existing sample, which average 52 MB — but those 5 are **partial
scrapes, not small companies** (`HNX_AMV` holds 9 files where CafeF lists 160; `UPCOM_CMT` 1 of 112).
Measured properly HNX averages 102 docs/ticker and UPCOM 75 against HOSE's 134. *"The sample is
small"* and *"the sample is a different thing"* are different failures, and only the second survives
more data.

**Shipped with it, 2026-08-23:** `year_min`/`year_max` on `scrape_pdfs`, `scrape_all_pdfs`,
`scrape()` and the Dagster asset, composing with the pre-existing `years`, with the window written
into the partition metadata (because `landed()` counts the folder, not the run — §5 rule 10);
⚠️ **a defect the counting found — `link.endswith(".pdf")` was silently skipping 1,408 of 84,076
documents (1.7 %)** because CafeF appends a cache-buster, VCB's own Q2-2026 filing among them, fixed
by testing `urlsplit(link).path`; ⚠️ **a latent one in the merge path the year filter made
reachable** — a scoped run merges un-inspected rows back from the index CSV where every cell is a
`str`, so `sum(bytes)` raised `int + str`, and the quiet half is worse (**`"False"` is truthy**, so
`consolidated` and `half_year` would have counted every carried row); and **21 tests**, no network,
pinning that **phase 1 and phase 2 partition the corpus exactly**.

⚠️ **AN UNDATED DOCUMENT LANDS IN PHASE 1 BY CONSTRUCTION** — CafeF files 10 of the 84,076 with a
`Year` that is not a year (eight `0`, one `202`, one `203`); `year_max` keeps them and `year_min`
does not, so nothing falls between the phases. Ten documents is not the point; a phase boundary that
leaks is.

⚠️ **This does NOT touch the schema wall** — re-diagnosed 2026-08-25 as `TPL-1`: all four charts of
accounts exist and what blocks a non-bank run is seven hardcoded reconcile anchors (`P5`). Phase 1
buys the INPUT for an OCR program whose parser has never been run against a corporate filing.
⚠️ Under §5 rule 24 the JSON route is not merely deprioritised, it is **forbidden as a source**.

---

### P40 · ✅ DONE 2026-08-27 — EVERY BID CASH FLOW FROM 2012 IS PARSED ⏱ 35m 20s *actual*

✅ Balance sheet **57/57**, income statement **57/57**, **cash flow 57/57** from Q1-2012; every row
of the ticker reads `pdf` or `missing` and nothing else. ⚠️ **The scope is a DENOMINATOR, not a
filter**: BID files 12 documents a year only from 2012 — before that an annual report and nothing
else, first quarterly Q3-2011 — so the older denominator was measuring the FILING CALENDAR as much
as the parser. **Keep the default floor**: 9 real parsed `pdf` cells sit before it, and a
`period_min` run deletes them from the CSVs while `BRZ-1` leaves them stranded in bronze.

⚠️ **THE LAST CELL WAS NOT AN FX PROBLEM AND NOT A DPI PROBLEM.** Two blockers: the movement
figure's LAST DIGIT sat outside the detector crop (`6.711.633` reads as `6.711.6.3` / `6.711.610` /
`6.711.63)` at 200/300/400 and correctly at `crop_pad=6`), and the filing prints a FOURTH term —
merger cash 3,004,011 — the chart of accounts has no column for. ⚠️ **This item's own previous text
said *"misread at every DPI"*, and that sentence kept the quarter closed for a day**: true of the
default crop, false at pad 6, never tried. New flag `cash_extra_terms` + three layers; the identity
closes to the đồng. CLAUDE.md §6-2-quatervicies.

---

### P39 · ✅ DONE 2026-08-27 — TWO CASH-FLOW RECOVERIES, MEASURED **WRONG** ⏱ 1h26m *actual*

> **Neither fix was ever run as written, and neither should be.**
>
> | | verdict |
> |---|---|
> | **Fix 1 · the positional FX guess** | ❌ **UNSAFE, and its own safety argument is false.** It claims the row between the balances as FX; on BID's FY-2015 that row is the MHB MERGER line, so the identity closes *because the arithmetic is right and the account is wrong* — **it cannot reject what it confirms** (§6-2-vicies) |
> | **Fix 2 · the alternate filing** | ❌ **NO GUARD FOR A RESTATEMENT.** BID's unaudited Q4-2016 closes ~**62.6 tn** where the audited annual prints **65,521,789**, both internally consistent, so `reconcile` and `sane` pass either. **This fallback is where the reverted 61,575,636 came from** (§6-2-quatervicies) |
>
> ✅ **The safe half shipped as `cash_extra_terms`** and does the opposite: it **sums** what the
> filing printed between the two balances, demands exact equality to the đồng, writes the term
> **nowhere**, and **refuses** the positional FX claim when the row's own label does not say FX. It
> recovered BID Q4-2016 (`P40`). ⚠️ **The guard was wired to that FLAG** — live on 3 of 47 layers,
> absent on `onnx@200+relax`, **layer 5** — and had already written merger cash into BID Q4-2015
> (1,477,340) and Q2-2017 (1,540,994), each confirmed by the identity to the đồng. Made
> unconditional; `extra_terms`/`cash_extra_terms` then went dead in `_recover_totals`/`map_to_schema`
> and were removed — ⚠️ *a knob that decides whether a guard applies is a knob that turns a guard
> off.* ✅ **Blast radius MEASURED**: all 32 rows the branch could have produced re-parsed at their
> own recorded layers — **30 unchanged, 2 dropped**, exactly the defect, 7.0 min. ⚠️ **The
> history-provider downgrade reproduced a FOURTH time**, losing `publish_date` on four statements —
> a figures-only diff would have called that run clean. ⚠️ **Two pieces moved to `P43`**, not
> dropped. `FXM-1` stays open and points there.
>
> ⚠️ **AND THE POPULATION THIS ITEM WAS SIZED ON IS GONE.** Its "8 of 13" came from each statement's
> LAST refusal reason, and **a cascade's final refusal names the hardest path tried, not the blocking
> defect** — six of the seven parsed at STRICT layers on 2026-08-27, where `verify_cash` is off.
> **The FX bottleneck was never 8 quarters; it was a reporting artefact.**

⚠️ **The cost measurements survive and are what `P6`/`P38` should budget on**: probing all 13 BID
failures through the full cascade took 3 h 56 m — **18.2 min per failed document against 4.2 for a
clean one (4.3×)**, the direct measurement of §6-2-decies' bimodal cost.

---

### P38 · ⭐ THE VN30 BASKET ⏱ ~103-190 h GPU *est.* · started 2026-08-25

| the job, counted from the PDF indexes on disk — no OCR, no network | |
|---|---|
| tickers | **30**, of which **3 done** (ACB, VCB, BID) → **27 left** |
| documents `documents()` opens, consolidated only | 1,646 total · **1,511 remaining** |
| with `allow_parent=true` | 1,737 total · **1,598 remaining** |
| **measured rate** | ⚠️ **NOT A CONSTANT — 1.63 / 3.13 / 7.15 min/doc** (VCB 70 docs / 114 min, ACB 69 / 216, BID 62 / 443). The driver is the share of quarters needing the full cascade: **4 % / 11 % / 36 %** → **min/doc ≈ 0.94 + 0.173 × %failing** over three points |
| **estimate** | **~190 h** at BID's rate · **~103 h** at the three-ticker mean · *(the ~63 h this row carried until 2026-08-25 assumed a flat 2.37 min/doc and is wrong)* |
| ⚠️ **for a NON-BANK** | unmeasured, and `TPL-1` implies the WORST case — two of the three non-bank templates cannot reconcile a cash flow at all, so the failure rate is near 100 % and the cascade at its ceiling. **`P5` is a COST item, not only a correctness one** |

⚠️ **RUN IT ONE TICKER AT A TIME.** `raw/cafef_financials` carries `op_tags={"resource": "gpu"}`,
capping it to ONE running step — onnxruntime-gpu on a 4 GB RTX 3050, and two partitions is VRAM
exhaustion. A partition range would not go faster; it would fail.

⚠️ **THE BINDING CONSTRAINT IS `P5`, NOT TIME — only 11 of the 27 can run today.** VN30 splits
**13 banks / 17 non-banks**: runnable now `BID CTG HDB MBB SHB SSB STB TCB TPB VIB VPB`; blocked
`BCM BVH FPT GAS GVR HPG MSN MWG PLX POW SAB SSI VHM VIC VJC VNM VRE`. ⚠️ **Two of the 17 are not
`corp` either — BVH is insurance, SSI securities** — and each needs its own template proven.
⚠️ **`TPB` is the `allow_parent` case in its purest form — 9 consolidated documents against 55 with
the fallback, 6.1×** — while **BID is the opposite pole, 62 either way**. The flag's value is a
property of the ISSUER and cannot be budgeted from an average.

**Kaggle** is open and must be MEASURED before quota is spent (`P4` names the three unknowns).
⚠️ The measured local profile argues the gain is smaller than it looks: the run holds **~1 CPU core
and 2.4 GiB VRAM at 31-47 % GPU utilisation**, so a T4 session (4 cores) fits ~4 partitions, not 6-8.

```powershell
dagster asset materialize -f src/orchestration/definitions.py `
  --select "raw/cafef_financials" --partition "HOSE_BID" --config full_parse.yaml
```
```yaml
ops:
  raw__cafef_financials:
    config:
      skip_existing: false     # authoritative: non-merging, restores `sane`'s full history
      allow_parent:  true      # TPB alone goes 9 -> 55 documents
```

⚠️ **BACK THE THREE CSVs UP FIRST** — an authoritative run writes NON-MERGING progress snapshots, so
an interrupted one leaves a TRUNCATED file (measured: ACB's three statements stood at 9 rows against
74 when a run was killed). ⚠️ **`--config <file>`, not `--config-json`** (RUNBOOK §3e-ter).
⚠️ **Diff every result against the backup cell by cell** — `SAN-1` was found that way and no other.

---

### P37 · ⚠️ THE PDF-ONLY REPAIR — the parse PLAN for ACB and VCB ⏱ ~5 h GPU

**1 · Default `use_api=False`, and expose no knob that turns it on** (`cafef_financials.py:485`,
`:1629`). CLAUDE.md §5 rule 24. ⚠️ **The `from_api` docstring argues the opposite in as many
words** — *"This is not a lesser source — for the quarters OCR cannot read it is a BETTER one"* — so
this is a DECISION to reverse, not a bug to patch. Rewrite the docstring rather than deleting the
method: its evidence (the Q4 weakness, the eight confirmed-wrong values, the literal `-1` sentinel)
is why the rule exists.

**2 · ⚠️ `use_api=False` ALONE DELETES NOTHING — the run must be AUTHORITATIVE.**
`skip_existing=True` forces `merge=True`, and a merging write only rewrites the quarters the run
produced, so **a failed retry leaves the existing `cafef` row exactly where it is.** Only
`skip_existing=False` writes non-merging and turns an unproduced quarter into `missing` — and it is
also the run that restores the `sane` magnitude guard, the one that caught ACB's Q1-2024 carrying
Q1-2023's PBT. ⚠️ One partition at a time (~2.4 h each).

**3 · ⚠️ WHAT THE RUN CAN AND CANNOT REACH — measured, not assumed.** `documents()` keeps
`consolidated == "True"` and nothing else, and **ACB filed no consolidated statement before 2010**:

| | ACB | VCB |
|---|---|---|
| documents `documents()` returns | **65**, covering 2010-2026 | **72**, covering 2006-2026 |
| years re-opened at `skip_existing=True` | **0 of 17** | **4 of 21** — 2006-2009 |
| HTML rows the parser can retry | **0 of 27** | **4 of 7** |

**The retryable rows — all VCB:** 2008Q4 IS, 2009Q1 BS, 2009Q2 BS, 2009Q2 CF (plus three `missing`
retried alongside). ⚠️ **THEY WERE RETRIED 2026-08-24 AND RECOVERED NOTHING — 45.5 min, 0 of 234
cells changed.** Five genuinely new parse configs were probed and **none accepts, so none is kept**:
the totals are stable across 200/300/400 dpi and every crop setting, and the gap is an accounting
one — `A − (L+E)` is **4.48 %** of assets in Q1-2009 and **4.33 %** in Q2-2009. **These documents do
not fail on OCR**; the lever is the schema mapping for the 2009-era consolidated VAS bank
presentation, which belongs beside `P5`.

⚠️ **AND `periods` MUST NOT BE USED TO PRODUCE.** VCB Q2-2009's cash flow reconciles cleanly on its
own and the run still rejected it: `sane` judges magnitude against the quarters accumulated **in that
run**. `build`'s docstring predicted `sane` would *fail open* in a subset run; measured, it can also
**fail closed**. Probe with `periods`; produce with `skip_existing=false` and no `periods`.

**Everything else becomes `missing`, correctly:** ACB's 27 (2008-2009 are parent-only filings the
parser will not open; CafeF lists no Q2-2026 document at all) and VCB's 2008Q3 (no filing exists).
⚠️ **COVERAGE WILL FALL AND THAT IS THE POINT** — ACB 0 → ~27 missing, VCB 18 → 18-21.
⚠️ **Reaching ACB's 27 is a separate DECISION, not a parser fix**: it means accepting a
PARENT-COMPANY statement, a change of which ENTITY the numbers describe. ⚠️ **Check ACB and VCB
Q2-2026 against a re-listed index first** — those 2021+ index rows come from the old scrape, and
VCB's own Q2-2026 filing is named as one of the 1,408 documents the `.pdf` cache-buster bug skipped.

✅ Verify with the one query that can see this at all:

```sql
SELECT ticker, source, COUNT(*) FROM bronze_schema.cafef_financial_reports
GROUP BY 1, 2 ORDER BY 1, 3 DESC;      -- expect only 'pdf' and 'missing'
```

---

### P5 · ⚠️ THE NON-BANK WALL IS SEVEN RECONCILE ANCHORS, NOT A MISSING TEMPLATE ⏱ ~1-2 days

⚠️ **THIS ITEM DESCRIBED THE WRONG WALL FOR AS LONG AS IT EXISTED** — *"a corporate template does
not exist in this repo"*, inferred from `statements/` holding one folder, `bank`. **That folder is
the parser's OUTPUT**, so it holds one family because one family has been RUN.

| what actually exists — verified on disk and in code | |
|---|---|
| charts of accounts | **12 files, 871 rows** in `financials/schema/` — 4 families × 3 statements (bank/corp/securities/insurance BS 91/141/133/96 · CF 50/45/80/45 · IS 27/25/83/55) |
| `cafef_schema.TEMPLATES` | all four, reference tickers `VCB`/`FPT`/`SSI`/`BVH` |
| `detect_template()` | **fingerprints the filing's own chart of accounts**, never GICS |
| `schema_of` / `map_to_schema` | take `template` as an argument; load any of the 12 |
| the bronze ingest | *"one wide table per (template, report) that has been parsed"* — the `_bank` suffix is a consequence, not a scope |

⚠️ **What is bank-shaped: seven hardcoded anchor tuples** (`C_ASSETS … C_CASH_CLOSE`), exact
dict-key lookups; on a miss `reconcile` and `_probe` fall through to `Statement.find`, a
substring-then-fuzzy search at `NAME_MATCH = 0.85`. Replaying that algorithm over each chart's own
clean labels — **the OPTIMISTIC case, before any OCR damage**:

| anchor | bank | corp | securities | insurance |
|---|---|---|---|---|
| `C_ASSETS`, `C_EQUITY` | ✅ | ✅ | ✅ | ✅ |
| `C_RESOURCES` | ✅ | ✅ | ⚠️ fuzzy 0.962 | ✅ |
| `C_LIABILITIES` | ✅ | ❌ both | ⚠️ **matches the GRAND TOTAL** | ❌ both |
| `C_PBT` | ✅ | ⚠️ text | ⚠️ text | ⚠️ text |
| `C_NET_CF` | ✅ | ⚠️ text | ❌ both | ⚠️ text |
| `C_CASH_CLOSE` | ✅ | ⚠️ **WRONG ROW** | ❌ both | ⚠️ **WRONG ROW** |

⚠️ **THE CASH FLOW DOES NOT MISS — IT LIES.** `CASH_CLOSE`'s needle fuzzy-matches the **OPENING**
balance at **0.885 (corp)** and **0.902 (insurance)**, and `find` scans in statement order where
`đầu kỳ` precedes `cuối kỳ` — first hit wins. So `reconcile` gates on, and `sane` probes, **the
opening balance labelled as the closing one**: a wrong figure, not a refusal, and nothing raises.
⚠️ **`bank` is protected only by an accident of ordering** (its own opening line scores 0.930 on the
same needle; its canonical column is present so the fallback is never reached). ⚠️ **`securities`
fails the opposite way and is therefore SAFE** — 0.789/0.831, both below threshold, so every
securities cash flow is refused. **A refusal is the correct failure mode.** ⚠️ **CafeF's insurance
cash-flow chart has NO closing-balance line at all**, so even with the anchor fixed there is nowhere
to store the figure — a schema repair, not a tuple edit. ⚠️ Two more bank-only sets, both silent:
`_cash_flow_identity`'s three columns are **0 of 3 present on every non-bank chart**, and
`TOTAL_ALIASES`' two recovery columns exist in **no** non-bank chart.

✅ **THE ANCHOR HALF SHIPPED 2026-08-28 AND WAS SMALLER THAN THIS ITEM ASSUMED.** The nine `C_*` role
tuples now cover all four charts and **`ANCHORS` is DERIVED from them** — the defect was
DUPLICATION, a hand-written literal beside the tuples, so `_anchor` re-matched **2 of 7 roles on
`corp`**. On VIC Q1-2026 the closing balance moved out of the FX column (54,750,360 mn), the identity
closes to the đồng, and `_probe` stopped answering with the opening balance. **15 of 15 bank
statements across 5 filings re-map identically**; 203 tests, 96 new. CLAUDE.md §6-2-untricies.

**What is left, and it is the bigger half:**

- **(a)** VIC's INCOME STATEMENT is still never found — pages 9-10 OCR to **25 and 5 words** against
  94-169 on their neighbours, so it is upstream of `_page_kind`, not a classification bug.
  ⚠️ **Budget this first — it is the only one that costs a whole statement.**
- **(e)** ⚠️ **THE COMPARATIVE COLUMN, measured 2026-08-29 on 27 quarters rather than one, and it
  costs the most cells.** The first authoritative VIC run (stopped by hand at 27 of 72 quarters,
  12 h) gives the first non-bank corpus: **BS 13 `pdf` / 14 `missing`, IS 21 / 6, CF 20 / 7**.
  ⚠️ **The balance sheet fails on SELF-PREPARED quarterlies and nowhere else — 12/13 on audited or
  reviewed filings against 1/14 on unaudited ones** — while the other two show no such split, so it
  is not scan quality. `sane` refused three quarters on the SAME probe and two more on another: the
  parser is taking the prior year-end column a quarterly balance sheet prints beside the current
  period. ⚠️ **`reconcile` cannot see it** (a comparative column is internally consistent) — `sane`'s
  equality gate is the only thing between this and 14 wrong balance sheets written as `pdf`.
- **(b)** `C_LIABILITIES` still misses on `corp` in the field — the row is there (1,024,990,928 mn,
  and it sums with equity to total assets exactly) but its account text is **9 characters, below
  `MIN_CONTAINS = 10`** — so a corp balance sheet reconciles on the TRIVIAL `assets == resources`.
- **(c)** `insurance` has no closing-cash line in its chart; **(d)** `securities` and `insurance`
  anchors are verified against their CHARTS and **have never met a filing**.

**The rest of the work, in order:** ⚠️ **fix `find`'s open/close confusion at the source, not by
threshold** — raising `NAME_MATCH` breaks the OCR tolerance the whole parser rests on; `_anchor`
already solves this by LENGTH, or use an explicit `đầu kỳ` exclusion. Decide the insurance cash flow.
Rebuild `templates.csv` (**one row** today, and `build_templates_index` refills it with a network
fingerprint call per ticker). **Run ONE `corp` discovery parse** before budgeting anything.
⚠️ **silver and gold are still bank-only** (`_ingest_silver_cafef_financials_bank`), so a non-bank
parse stops at bronze — **name that before `P38` is quoted as reaching 28 tickers**.

⚠️ **A resumed VIC run is NOT available** — `skip_existing: true` or `periods` makes it a subset run
and flips `sane` open (four measured downgrades), so the next attempt is the same 72-quarter run
from scratch and should not be launched until (a) and (e) are fixed. ⚠️ **Nothing on disk is wrong
today**: only bank tickers have been parsed. This is a wall in front of `P38`/`P6`, not a defect in
any published number.

---

### P8 · ⚠️ FEED THE MODEL THE REPRESENTATION THE SELECTION USED ⏱ ~1 h + 20 min *est.*

**Three measurements already on disk point here, and the third is what makes it first.**

1. **`FNM-1`.** The cross-sectional selection ranked every channel **within each date**
   (`cross_sectional.py` §3 removes *"the level that acted as a date proxy AND the size that acts as
   a permanent stock label"*), while `train_test_creator.build` fits **one global `StandardScaler`**
   on the train slice — the h=10 dataset's `metadata.json` reads **19 scaled columns, 0 bounded**.
2. **The 19 channels include raw LEVELS** — `close_adjust` (a VND price), `drv_vwap_raw`,
   `n_sell_orders` (a raw count). That dataset's `drift.csv`: **`close_adjust` puts 5.48 % of the
   test set beyond 5 train-sigmas** at a test mean z of +1.098, and `drv_order_vol_imb_21` **7.65 %**.
   That is `DRF-1` biting the chain behind every headline in §6-0 — and §6-2 warns about exactly this
   channel in exactly this role while looking at `pool__ta`'s `close`.
3. ⚠️ **`step6` IS WHY THIS RANKS FIRST.** `walkforward` §12 retrained **twice as often** for
   `t = −0.09`, ρ = 0.989, so the ~45 % Sharpe decay at **both** horizons is **not staleness of the
   FIT**. A non-stationary feature REPRESENTATION is the candidate left standing, and a within-date
   rank is stationary by construction.

**The code exists and needs no train/test fitting** — `cross_sectional.cross_sectional_normalize`
ranks per date, before the window, using nothing from the future.

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

⚠️ **Two things will break it:** a ranked column is **already bounded** in `[−0.5, +0.5]`, so
`dataset._classify` must land it in `bounded` or it is standardised twice and the point is lost; and
**it is a new EXPERIMENT, not a new setting** — give it its own `--out` and dataset-name segment or
`compare` pairs two things that are not comparable.

**Prediction:** ⚠️ **it helps — ΔSharpe +0.2 to +0.6 paired — and the gain concentrates in the LAST
folds.** **If it ties, that is the more interesting answer**: the trees were already reading these
channels relatively and `FNM-1` is a documentation defect. **If it LOSES**, the level channels carry
real information about the era and the decay is something else again.

---

### P9 · ENSEMBLE THE SEVEN ARMS — THE PREDICTIONS ARE ALREADY ON DISK ⏱ ~30 min *est.*

Seven arms exist at h=10, pairwise **ρ 0.91-0.94**, all seven clearing the within-date null with the
null MAX below the observed, and their inductive biases genuinely differ (§11a: `cnn` pools the
sequence away, `tcn`/`transformer` keep a per-timestep view, `gbt` sees only 78 window statistics).
**Averaging correlated, individually unbiased predictors is the standard variance reduction and has
never been tried here.** Everything needed is on disk —
`results/walkforward_h10_arch/<arm>/predictions_oos.csv`, seven over identical folds and panel. No
GPU, no training.

⚠️ **RANK-AVERAGE WITHIN EACH DATE, NEVER SCORE-AVERAGE** — the arms' outputs are on different
scales and the estimand is the ORDER anyway (R² **+0.0003**). ⚠️ **PRICE THE SEARCH**: an ensemble
chosen after seeing seven arms is an **eighth arm**, so read its `p_sharpe` against Bonferroni
**0.05/7 = 0.0071**.

**Prediction:** ⚠️ **it ties `gbt` and beats the `lstm` reference by +0.2 to +0.4**, showing up more
in `se_sharpe` and the fold-to-fold spread than in the pooled level — it is a variance reduction, not
a new signal. **A tie with the single best arm is expected at ρ 0.93 and is not a failure.**

---

### P10 · PORTFOLIO CONSTRUCTION — NO RETRAINING, AND A −55 % DRAWDOWN TO SPEND ⏱ ~4 h *est.*

`backtest.long_only_top_k` is **equal-weight, one book, one rebalance grid**. Three knobs have never
been varied on a walk-forward track:

| knob | state | why it should move |
|---|---|---|
| **`k`** | scanned **once**, at h=20, on the SINGLE split (Sharpe 1.53 at k=10 → 0.81 at k=75) | the level `k=20` was chosen on is not the level being quoted |
| **weighting** | equal-weight only | **inverse-volatility** is the standard answer to the **−55 to −58 %** max drawdown, and it costs no signal — it re-weights an order it does not change |
| **the rebalance grid** | ONE book, every `h`-th date from a fixed origin | **`h` laddered sub-books** remove the timing luck of the origin and smooth turnover across the month; also the honest version of the horizon comparison `P2-4` could only pair on the calendar |

Cost is CPU; the predictions are on disk for both horizons and all seven arms. ⚠️ **EACH KNOB IS A
SEARCH** — report the whole surface, never the argmax (`NUL-1` one level up). ⚠️ **Run it WITH
`P11`'s rows**, not before them: an ADV cap and a sell-side floor screen move the same numbers.

**Prediction:** ⚠️ **laddering is worth +0.1 to +0.3 Sharpe and cuts the drawdown by about a third;
inverse-vol cuts the drawdown and costs ~0.1 Sharpe; `k` is flat between 15 and 25** — §6-0-bis
measured a monotone decay, not a peak.

---

### P14 · TRAIN THE ESTIMAND — A RANKING LOSS INSTEAD OF MSE ⏱ ~½ day *est.*

`engine.CRITERIA[REGRESSION]` is **`nn.MSELoss`** and `model.gbt` uses XGBoost's default squared
error, while **every verdict this repo quotes is the ORDER** (test R² **+0.0003**, `mase` 0.9937).
Optimising squared error on a `[−0.5, +0.5]` per-date rank spends capacity on a quantity nobody
reads. Two versions, cheapest first: **`gbt` with `objective="rank:pairwise"` and `qid` = the date**
(LambdaMART with each date as one query group — ⚠️ **not a config line**: `window_statistics`
discards the date, so `model/gbt/model.py` must carry a `qid` and the design be sorted by it); and
**a differentiable per-date IC loss for the nets**, which needs the date inside the batch.

**Prediction:** ⚠️ **(1) is a tie or a small win, ±0.2** — §11a's reading is that these arms already
extract the same thing from the 19 channels. ⚠️ **I would rather be wrong here than not know**: it
is the only untried change aimed at the estimand itself, and a tie is a *stronger* version of §11a.

---

### P15 · ⚠️ SELECTION IS NEARLY EXHAUSTED; CONSTRUCTION IS WIDE OPEN ⏱ ~1 day + T4 *est.*

| the wall | measured |
|---|---|
| `CSP-1` | a cross-sectional selection reads **exactly ONE pool** — `read_universe_panel` is hand-written SQL and `run_selection` raises on any other `--pools` for a `cs_` target |
| `PRF-9` / §13 | a **date-only** column has a **constant within-date rank** and cannot rank a cross-section — and **71 of 76 gold tables are date-only** |
| `backtest` §10d | **`pool__fa` holds 2 tickers** on `unified_schema_all`, so fundamentals do not exist at panel grain |

So of the 23 pools, the ones that **can** rank are `pool__basic` (done, every horizon) and `pool__ta`
(done twice — ties twice). **The selection lever is close to spent; CONSTRUCTION has barely been
touched**, and the hit rate says where to point it: `pool__basic` carries only **5 cross-sectional
`drv_cs_*` channels and 2 of the 5 are in the 19-channel shortlist — a 40 % hit rate against 21 %**
for the pool as a whole.

| family to build | why |
|---|---|
| **sector-relative** — each channel minus its GICS-industry within-date median | the GICS tree is already attached in silver, and `drv_cs_ret_vs_industry` is one of the 19 |
| ⚠️ **the surviving order-flow PAIR as an interaction** — `drv_log_order_size_ratio` × `drv_order_count_imb_5` | `backtest` §10c: the **only two channels that survive BOTH the t+1 execution lag and the 2022 break**, deliberately opposed in sign (institutional vs retail tape). ⚠️ **Neither is in the 19** |
| **residual momentum / idiosyncratic vol** | removes what the rank target removes anyway, leaving what is name-specific |
| **within-date z-score twins** of the level channels | `P8`'s argument at CHANNEL grain, and it survives even if `P8` is rejected |

⚠️ **The ONLY item that spends Kaggle quota** (~45 min of T4 with no null; a 20-draw null on ~110
channels is hours), and it is a `pool__basic` rebuild on `unified_schema_all` — **11m 08s and
2,388,975 rows** — so not free locally either.

**Prediction:** ⚠️ **the order-flow interaction shortlists; the sector-relative family mostly does
not** — a within-date rank already removes most of what a sector median removes.

---

### P16 · SWEEP `lookback` — THE ONE DATASET KNOB NEVER SWEPT ⏱ ~4 h *est.*

**`d` comes from the source TABLE NAME** and `engine._verify` asserts it, so it is not a flag —
every value needs its own selection run, and **`d = 20` is the only value the cross-sectional chain
has ever used**, at either horizon. ⚠️ **§11a is a direct hint that the window is longer than the
information in it**: a tree seeing **78 window statistics** beats an LSTM seeing **260 numbers** on
identical folds. ⚠️ **`d` also moves the PURGE**, which is `d + h − 1` — at h=10 that is 29 rows at
`d=20` and 14 at `d=5`, so a shorter window buys training rows as well as changing the
representation, and the two effects must be reported separately. Three values (`d = 5, 10, 40`) ≈
45 min selection + 20 min sweep each.

**Prediction:** ⚠️ **`d=10` ties `d=20`; `d=5` loses slightly; `d=40` loses.** If `d=40` WINS,
§11a's *"the sequence inside the lookback is worth nothing"* must be restated as *"the sequence THIS
LONG is worth nothing"* — a different and weaker claim.

---

### P17 · DATE-ONLY POOLS AS A REGIME OVERLAY, NOT AS RANKING CHANNELS ⏱ ~1 day *est.*

The structural fact that kills the date-only pools for RANKING does not kill them for TIMING.
`pool__market_breadth`, `pool__stock_market` and `pool__bonds` are constant within a date — **zero
within-date rank IC by construction** — and every one is a candidate for scaling the **book**, not
the ranking. **The target is specific**: **2022, the only bad fold at either horizon** (−0.07 at
h=20, +0.37 at h=10, in a year the equal-weight universe itself ran −0.94) and the **−55 to −58 %**
max drawdown.

⚠️ **READ `PRF-2` / §9b FIRST: the post-2022 break is in the FEATURES, not the market.** So an
overlay is a **risk control and not a fix for the decay**, and must be scored as one — drawdown and
`se_sharpe`, not the pooled level. ⚠️ **It is a second search over the same ten folds**: fit the
rule on pre-2017 data and freeze it, or price the search; anything else re-learns 2022 from 2022.

**Prediction:** ⚠️ **it cuts the drawdown materially and moves Sharpe by less than its own error
bar.** A regime filter that also raised the Sharpe here would be surprising and should be treated as
a search artefact until it survives a frozen pre-2017 rule.

---

### P20 · ⚠️ The regime question — PARTLY ANSWERED by `PRF-1`, and the answer FLIPPED ⏱ ~1 day

⚠️ **UPDATE 2026-08-19.** Written when three independent measurements found the edge dying after
2022. **`PRF-1`'s walk-forward found the opposite at h=20**: 2023/2024/2025 score **+2.64 / +0.90 /
+1.39** against markets of +1.57 / +0.35 / +0.94, and **2022 is the only bad fold** — a year the
equal-weight universe itself ran Sharpe −0.94. So the break is **not** universal: present in the h=5
and h=10 HAND screens, absent in the h=20 MODEL. `PRF-2` separated the two candidates (horizon vs
feature set) and found the FEATURES.

The original framing, still valid for the hand screens — three measurements, same break, same place:

| study | pre | post |
|---|---|---|
| `model/CONTEXT.md` §11 (h=5, foreign flow, 28 folds) | net@20bps **+1.46** (2017-20) | **−0.51** (2022-26) |
| the h=5 hand screen | Sharpe **+1.104** (2018-21) | **−0.099** (2022-26) |
| the h=10 hand screen | **+1.671** (2018-21) | **+0.011** (2022-26) |

⚠️ §11 already tested **rolling vs expanding training at h=5 and it did not help** (rolling *lowered*
AUC, 0.513 vs 0.520), so "stale training data" is not the explanation there. **Untested at h=10 and
h=20**, which is the gap. **The distinguishing test**: train on 2022-2026 only and score 2022-2026
by walk-forward. If a model that has only ever seen the new regime still finds no edge, the honest
conclusion is that this data cannot be traded now.

### P11 · ⚠️ Execution realism — the remaining fictions ⏱ ~1 day

| gap | why it matters | measured? |
|---|---|---|
| **ADV / size cap** | a 20-name book at real size moves a VN mid-cap; `pool__basic.value_matched` is on hand, so cap each position at a fraction of it and re-run | ❌ |
| **floor days on the SELL side** | the ceiling exclusion covers ENTRY only. A name at its floor on the exit date cannot be sold either, and a loser is exactly when that happens — biased against the strategy in the direction that matters | ❌ |
| **the ATC auction** | signals from full-day order counts settle only after close, but a partial-day version could be submitted into ATC, recovering part of the ~19 pp/yr the t+1 lag costs at h=5 | ❌ |
| **the ceiling exclusion is a PROBE, not a default** | `PRF-0` measured it and the model survives (+1.484 → **+1.551**), but `backtest.portfolio` applies no exclusion, so the next run reproduces the untested number. Needs `exchange` on the panel. ⏱ ~1 h — **the cheapest row here** | ✅ measured, ❌ not shipped |
| **max drawdown −55 to −58 %** | at every `k` on the h=10 screen. Statistically tradable ≠ holdable; a vol target or regime filter is the standard answer and neither is tested | ⚠️ known |

*(`se_sharpe` on the h=20 cell closed with `PRF-1`: 118 periods and 0.155 against the single split's
32 and 0.256 — fixed the way it was predicted to be, by more OOS periods rather than a wider window.)*

### P18 · Survivorship — the one bias that flatters a momentum screen ⏱ ~2 days

`silver.stocks_basic` holds **no delisted name** (§2c), and a screen that buys recent winners is the
strategy most flattered by that. ⚠️ **The null is protected** (every shuffled draw picks from the
same survivor basket) but **the CAGR is not**. Fix is data, not code: a point-in-time
listing/delisting table — §2d's lever, and it makes `PRF-1`'s fold series interpretable.

### P19 · New information — the only lever §2d says is left ⏱ months

Ranked by expected impact **on this specific problem**, which differs from §2d's single-stock order:

1. **Intraday / tick data.** ⚠️ The measured 5-day signal decays inside ONE SESSION — **+24.4 % CAGR
   same-close against +5.6 % at t+1** — so trading it intraday is not an improvement, it is the
   difference between a strategy and a curiosity. It also gives §2d's true #1, aggressor buy/sell
   imbalance, of which daily order COUNTS are a proxy.
2. **Point-in-time listing status** — `P18`.
3. **Fundamentals with filing dates** — `experiment_4` recovered VCB's publish dates, so the method
   exists for one name and needs scaling.
4. News / sentiment — **closed**; `pool__news_daily` measured z = +0.53 at layer 1.

### P23 · Re-fit the cost model into ONE function ⏱ ~2 h

Two models exist, disagree, and were both fitted with `lasso` — dropped 2026-08-16:

| model | predicted the 644-ch / 10-draw run at | actual |
|---|---|---|
| Dagster guard `1.1 × (ch/113)² × (1+draws)` | **393 min** | **29.7 min** |
| `CONTEXT` §15c `0.364 × ch^0.77` | ~53 min/pass | ~3 min/pass |

Needs a **draw coefficient** (draws skip `stability` and the holdout, so `(1 + draws)` is wrong) and
a **raggedness term** — exponent ~0.83 fits the well-behaved runs while the 1,406-channel `usa` run
sits **6× off**, likely rule 23's all-NaN slices rather than width. ⚠️ **The guard's premise is
falsified**: CLAUDE.md says `usa` is "7.2 h with no null"; it ran **35 min 12 s**. **Payoff:** a
20-draw null on each of the 19 country pools becomes **~2-3 hours**, not the ~1,000 CPU-hours
`EVD-1` is scoped at — this is what makes `EVD-1` closable.

### P24 · Cut the host-side peak so the top-300 panel fits ⏱ days

⚠️ **TWO EXTRAPOLATIONS WERE WRONG, IN OPPOSITE DIRECTIONS, FOR THE SAME REASON: each scaled a
quantity that was not the binding one.** The first read *"~1.5 GB of the smoke run's RSS is data over
48,521 rows, so top-300 is 25.7× → ~39 GB"*; the top-150 run then ended phase 4 at **11.0 GB on
624,448 rows**, and a straight line through both points predicts **20.6 GB** — under the box. **One
point does not fit a line**, and scaling a peak from a tiny panel treats a large fixed cost as
per-row. ⚠️ **And the second fit is not to be trusted either: `rss` is sampled BETWEEN phases**, so
whatever killed the top-300 run inside phase 4 was never printed. `selector._tick` now reports
`peak=` (the OS high-water mark), and ✅ the 2026-08-18 null run showed top-150 settling at
`rss=11.2G` with **`peak=16.3G`** — **45 % above where it settles**, reached inside `rank`, exactly
where top-300 died. Doubling the rows puts top-300's peak at **~28-30 GB against a ~29-30 GB box**,
so that kill is now explained by a measurement. **top-300 needs the streaming design — this item —
not a trim.**

### P25 · `cs_rank_5day` on the top ~300 by turnover ⏱ ~1 h

`read_universe_panel` already takes a `tickers` list and filters in SQL, so this is a CLI flag, not a
new schema. ~1.3 M rows — ⚠️ **the same width as `P2-1 v2`, so assume the same 4 GiB VRAM ceiling**
until measured otherwise. Puts a number against §2b's `ALL` row, which reads *"never ran —
unverified"* at IC +0.109. ⚠️ Liquidity is the variable: the 5-day cross-sectional reversal runs
`t = −18.60` over all names, `−10.43` at top 300, **`−1.96` at top 100**.

---

## The retired bands — what survives of them

The five priority BANDS (`P0-*` … `P4-*`), the `PROFIT`/`PRF-*` track, the single-stock `SSK-*`
track and the model program `M-*` are retired as ORGANISING DEVICES; every open item they held is in
the one list above, and the crosswalks map the codes. **What survives is their RULE**, which still
orders the list: *a thing that makes a number you already have wrong or unreadable outranks a thing
that would give you a new number; structural code comes last because it only pays off for runs that
are currently blocked anyway.* ⚠️ **A band was never a permanent property of an item** — two rows
left the hygiene band upward on 2026-08-19 because neither was hygiene by this file's own test, and
that is the second half of why the list is flat.

⚠️ **Nothing currently known makes a quoted number wrong.** The nearest candidates are deliberately
not treated as such: `WFO-1` is a way to DESTROY a number rather than misstate one, and `PRF-7`-at-
h=10 measures how optimistic a level is rather than showing it to be wrong. **If you find something
genuinely wrong, it outranks everything and goes to the top row.**

---

## Closed — recorded so they are not reopened

| what | why closed |
|---|---|
| **News sentiment scorer** (annotation, LLM labelling, PhoBERT fine-tune, LIME gate, full panel) | ⛔ **Decided against 2026-08-03, confirmed 2026-08-17.** 7 paired tests, every \|t\| < 1.3; adding news costs 2-8 pp CAGR for ΔMCC ±0.003. The one reason to continue — coverage — was tested on the top-30 most-covered tickers and did not survive. The event-count half is `pool__news_daily` and it measured `z = +0.53` |
| **Silver / gold leaf assets** (bonds, forex, funds, indices, gics) | ✅ all exist |
| **`switch_config.json` cleanup** | ✅ moot — the file is gone (§5a); a leftover copy now RAISES |
| **`execution.finished_at = None`** in every `metadata.json` | ✅ **working as designed** ([runtime.py:329](../src/utils/runtime.py#L329)) — `summary()` is called mid-run because `write_report` writes the file, and waiting for `stop()` would record a runtime of zero. `None` "rather than a guess" is §5 rule 2 at the clock. I called it a bug on 2026-08-16 and was wrong |
| **`P3` · the JSON fundamentals gate** | ⚠️ **CLOSED 2026-08-23 BY DECISION, NOT BY MEASUREMENT, and archived UNMEASURED.** It would have priced `api.simplize.vn` / `vnstock` as a 1-day gate on the whole OCR program. The source is now fixed by CLAUDE.md §5 rule 24. ⚠️ **Nothing may cite it as evidence that a JSON source does not work** (§5 rule 2) — the route is UNTRIED, not disproven |

---

## Archive — done, kept because the reasoning is the evidence

> ⚠️ **NOTHING BELOW IS WORK TO DO.** This file's convention deletes a done item once its
> measurement lives somewhere permanent — and every one of these does. They survive as one-line
> records for two reasons: **several recorded a PREDICTION written down before the run that turned
> out wrong**, and other registers **cite this file rather than restate it** (CLAUDE.md §4 gives
> `P0-3`'s dtype figure and then writes *"(TODO P0-3)"*). ⚠️ **Before deleting any row, `grep` the
> code across `*.md` and move what is cited.**

| code | what it settled | where the measurement lives |
|---|---|---|
| **`PRF-1`** ✅ 08-19 | 10 expanding folds, 118 periods — **it is not one lucky split**. ⚠️ **My recorded prediction was HALF WRONG**: the level held, the decay did not (Sharpe@30 slope −0.100/fold) | `walkforward/CONTEXT.md`; CLAUDE.md §6-0-a |
| **`PRF-8`** ✅ 08-19 | three architectures over identical folds, **101× of capacity, every \|t\| < 1** — the architecture is worth nothing at h=20 | `walkforward/CONTEXT.md` §8; §6-0-ter |
| **7-arm sweep** ✅ 08-21 | 224× of capacity at h=10; ⚠️ **BOTH recorded predictions were wrong** — only `cnn` loses risk-adjusted, and `gbt`'s advantage does not survive six arms | `walkforward` §11; §6-0-ter-2 |
| **`PRF-2`** ✅ 08-19 | the model beats three hand-ranked columns by **2.7 Sharpe** at h=10 — **the post-2022 break is in the FEATURES, not the market** | §6-0-bis-2 |
| **`PRF-9`** ✅ 08-19 | `pool__ta` changes the SHORTLIST and not the MONEY (paired ΔSharpe −0.126, `t` = −0.29) | §6-0-quater |
| **`PRF-7`** ✅ 08-19 | the selection look-ahead is **MILD and bounded** — 51 of 61 channels survive a pre-2017 re-run | `walkforward` §6.1 |
| **`PRF-0`** ✅ 08-19 | the ceiling band does **not** bite the h=20 model; excluding it HELPS (+1.484 → +1.551) | `backtest/CONTEXT.md` §8h |
| **`P2-1 v2`** ✅ 08-18 | the top-150 `cs_rank_20day` design, **z = +9.09** — ⚠️ its **three failed T4 attempts** are the only account of why the design is top-150 and not top-300 | CLAUDE.md §2b-bis |
| **`P2-3`** ✅ 08-17 | the selection cleared its bar; **the model did not** — the line to quote about selection IC | §6-0-b |
| **`P2-4`** ✅ 08-20 | h=10 vs h=20 paired **on the CALENDAR** (they cannot be paired period-wise), and the answer SPLIT: mean return significant, ΔSharpe not | `walkforward` §10 |
| **`P4-12`** ✅ 08-19 | `mase` on a panel — the first thing here to beat "predict no change". ⚠️ **My recorded prediction was WRONG and is left in the register** | §6-0-b |
| **`P0-1`** ✅ 08-17 | the two-layer null CLEARS. ⚠️ **My recorded prediction was WRONG** | `feature_selection/CONTEXT.md` |
| **`P0-2`** ✅ 08-17 | rule 21 shipped (a metric that cannot fail is withdrawn) — **and the report had to be fixed too** | §5 rule 21 |
| **`P0-3`** ✅ 08-17 | **`float32` does NOT reproduce `float64`** — 52 % relative change in `ic_mean`, so `design_dtype` is a SETUP key | CLAUDE.md §4 |
| **`P0-4`** ✅ 08-17 | `mkt_n_names` is a calendar proxy — blocked from the pool, kept in gold | CLAUDE.md §3 |
| **`P0-5` / `P0-6`** ✅ 08-18 | `RNK-1` (the label is reconstituted at dataset build) and `UNI-1` (the universe travels and cannot union) | §3d-bis |
| **`P1-3` / `P1-4` / `P1-5` / `P1-7`** ✅ 08-17/18 | panel mode runs (and its first rehearsal found the existing job broken); the VRAM half fixed, **the next wall is HOST RAM**; stages 5-8; stage 9 exists and answered the single-stock question | §3d-bis, §6-0, §6-0-bis |
| **`P1-6` / `FNM-1`** ✅ 08-19 | the shortlist is representation-INVARIANT, **12 of 13** — ⚠️ but the BAR does not transfer | §6-0-c(4) |
| **`P1-8`** ✅ 08-21 | `WFO-1` closed by a **REFUSAL** rather than a fix, and `RPR-1`'s half with it — ⚠️ it CREATED a limit (`P22`) | `walkforward/CONTEXT.md` |
| **`P1-9`** ✅ 08-21 | `compare.paired()` reports **both estimands**; the Sharpe test disagrees with the mean-return one about three of six arms at h=10 | §6-0-ter-2 |
| **`P4-11`** ✅ 08-21 | layer-2 detection scoped to the chain being asked about — `pipeline` no longer calls another experiment's run `up to date` | `pipeline/CONTEXT.md` |
| **`SSK-1`** — *a MEASURED RESULT, not a task* | the single-stock h=10 track FAILED on 5 tickers (`t` = +1.45) and the POOLED answer FLIPPED on 30 VN30 names (`t` = +3.77, dependence-adjusted) — ⚠️ no individual name is convincing | §6-1, §6-1-bis |
| **`P0-7`** | done and **deleted** per this file's rule — documentation staleness, no reusable reasoning | — |
