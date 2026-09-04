# Running the filing OCR — the guide

> **Two notebooks, and you only ever open one.**
>
> | | |
> |---|---|
> | **[`src/kaggle_gpu/RUN__pdf_ocr_control.ipynb`](../RUN__pdf_ocr_control.ipynb)** | the one you open. **One parameter cell**, and `ENVIRONMENT` decides where the OCR happens: `"LOCAL"` parses on this machine, `"KAGGLE"` builds the job, ships the filings, starts the kernel, waits and pulls the run folder back |
> | `src/web_scraper/RUN__pdf_ocr.ipynb` | runs **on Kaggle**. `kgpu` patches its parameter cell and uploads it — you do not open or edit it |
>
> ⚠️ **A THIRD NOTEBOOK EXISTS AND PARSES NOTHING.**
> [`RUN__pdf_ocr_summary.ipynb`](../RUN__pdf_ocr_summary.ipynb) is read-only and answers the
> planning question instead — *which quarters are still missing, on every ticker at once*
> ([§8](#8-what-is-still-missing--across-every-ticker-at-once)).
>
> ⚠️ **IT LIVES UNDER `kaggle_gpu/` AND DRIVES BOTH MACHINES** (since 2026-08-29). The folder
> name is where its Kaggle half staged its payload from long before it had a LOCAL half; the
> guide beside it is this file, which is why it was not moved.
>
> Nothing is written to `kaggle_config.json`: [`pdf_ocr.py`](pdf_ocr.py) computes the job from
> your parameters and hands it to the **same** `config._validate` a file-borne job goes
> through, so every guard still fires.
>
> ⚠️ **ASKING FOR A TICKER BY NAME —** *"OCR ticker FPT LOCAL"*, *"OCR ticker TCB KAGGLE"* — is a
> request for a **prepared clone that waits**, not for a run:
> [§1a](#1a-asking-for-a-ticker-by-name--the-standing-request).

⚠️ **THE WORKER WRITES NO STATEMENT CSV, AND IT COULD NOT IF IT WANTED TO.** A Kaggle kernel
writes `/kaggle/working` and exits; your `raw_data/` is on this machine. Its output is a run
folder under `reports/pdf_ocr/` that **scores itself** against the CSVs already on disk.

Since 2026-08-29 the *pull* **upserts that result into the statement CSVs automatically** —
`MERGE_INTO_CSV = True` in the control notebook, `merge_statements=True` on the job, and
`kgpu merge <job>` writes unless you pass `--dry-run`. Every merge takes a backup first, prints
every changed cell, and refuses four things it cannot judge
([§6](#6-merging-a-recovered-quarter-into-raw_data)).

⚠️ **A `LOCAL` RUN UPSERTS EACH QUARTER AS IT FINISHES, AND THAT IS THE INTERRUPTION
GUARANTEE.** `FinancialsBuilder._write` renders to a `.tmp` and `os.replace`s it, and only the
quarters a merge PRODUCED are rewritten — so stopping a 12-hour run at hour 6 keeps every
quarter that finished and can lose at most the one in flight. **On Kaggle that guarantee is the
PULL's**: a kernel writes `/kaggle/working` and exits, so nothing reaches this disk until the
folder comes home.

⚠️ **THE REFUSALS ARE WHAT MAKES THAT SAFE, NOT THE EXTRA COMMAND.** This repo has measured
**four** builds in which a `periods` run silently DOWNGRADED a quarter it was given only for
history while the log said `RUN_SUCCESS` (CLAUDE.md §6-2-vicies, §6-2-unvicies,
§6-2-quatervicies, §6-2-quinvicies) — so an automatic write only earns its place because the
merge writes ONLY the quarters the run produced, backs the file up first, and refuses the three
cases below.

---

## 1. The short version

```powershell
.\mt_env\Scripts\Activate.ps1
jupyter lab src\kaggle_gpu\RUN__pdf_ocr_control.ipynb
```

Edit **1 · Parameters**, run everything. That is the whole procedure, and
`ENVIRONMENT` is the only thing that decides which machine does the OCR.
⚠️ **Run it TOP TO BOTTOM**: §2 drops this repo's modules from `sys.modules` so a long-lived
kernel cannot execute a previous commit's parser, and **§3 is where an EMPTY `QUARTERS`
resolves** — the plan in §4 reads what §3 produced.

```python
ENVIRONMENT = "LOCAL"        # "LOCAL" = parse here | "KAGGLE" = ship it to a T4
EXCHANGE    = "HOSE"         # HOSE | HNX | UPCOM
SYMBOL      = "VIC"
QUARTERS = ["2014-Q3"]       # A LIST. [] = every quarter this ticker files. YYYY-QQ.
ONLY_MISSING = False         # True narrows an EMPTY list to the gaps. ⚠️ The "OUTSTANDING"
                             #   sentinel this line used to carry was retired 2026-09-03 —
                             #   two types in one parameter cost three measured readings.
OVERWRITE = False            # False = fill the GAPS; True = re-parse and replace
MERGE_INTO_CSV = True        # upsert into raw_data/.../statements/
FORCE_EMPTY_BAND = True      # write even when `sane` had no band — the only way a
                             # NEW ticker is ever bootstrapped. See below.
PERIODS  = None              # optional; the repo-native form; intersects with QUARTERS
TEMPLATE = "corp"            # None = resolve it and record which route answered
```

### ⚠️ `OVERWRITE` — one word, and it decides at BOTH ends

| | `False` (the default) | `True` |
|---|---|---|
| a quarter already `pdf` in all three statements | **dropped before any OCR**, and before it is uploaded | re-parsed |
| a figure that DIFFERS from a good `pdf` row | refused by the merge | written |

⚠️ **A QUARTER IS "COMPLETE" ONLY WHEN ALL THREE STATEMENTS READ `pdf`.** One filing produces
all three, so a quarter missing its cash flow re-opens the document; the two statements that
come back with it are then judged on their own merits — identical is skipped, different is
refused. A `cafef` or `missing` row is not evidence a quarter is done (§5 rule 24).

⚠️ **The skip is per QUARTER here and per YEAR in `FinancialsBuilder.build()`**, because
`_decumulate` needs that run's own Q1..Q(q-1) and nothing in this path de-cumulates. Do not
carry the year rule across.

⚠️ **`OVERWRITE = True` IS NOT HOW YOU REPAIR ONE WRONG ROW — use `REPAIR`.** It lifts the
DIFFERS refusal for **every statement of every quarter in the run**, and a `pdf_ocr_job` run is
not the run that wrote those rows: its `sane` band is rebuilt from disk (`seed_history`) where
`build()` accumulates one as it goes, so **the two escalate differently and the seeded run can
win on an earlier, poorer layer.** Measured on ACB 2026-08-30 — Q2-2009's balance sheet is
**33 items at `onnx@200+relax`** on disk and **19 at `onnx@200`** in a seeded run, because the
thinner band lets layer 1 pass where the full run's band refused it. Repairing that filing's
income statement with the global knob would have overwritten the balance sheet in the same run,
saying only *"DIFFERS in 2 columns"*. The notebook's **11 · Repair one row** takes explicit
`(quarter, statement)` pairs and scopes the write through `merge_run`'s own `periods`/`reports`
filter, so nothing outside the list can move.

Prefer a terminal? The same thing, four verbs — see [§7](#7-the-cli-instead-of-the-notebook).

---

## 1a. Asking for a ticker by name — the standing request

⚠️ **"OCR ticker `<SYM>` LOCAL" and "OCR ticker `<SYM>` KAGGLE" ARE A REQUEST FOR A PREPARED
NOTEBOOK, NEVER FOR A RUNNING ONE.** The clone is
built, its parameters are resolved against what is on disk today, the plan is printed, and then it
**stops and waits for you to say run**. Nothing is parsed, nothing is uploaded, no CSV is opened.

⚠️ **THE WAIT IS THE POINT, AND IT IS THE ONE THING THAT MUST NOT BE OPTIMISED AWAY.** A whole-ticker
parse is 60-70 filings and hours of GPU — HOSE_FPT was **185 min over 71 filings** on a T4
(CLAUDE.md §6-2-duosexagies) — and the two parameters that decide whether the result may be WRITTEN
(`FORCE_EMPTY_BAND`, `OVERWRITE`) are judgement calls about *that ticker's* history, not defaults.
Both are printed by §2 and §3 before anything is spent, which is only useful if somebody reads them.

### What gets built

| | |
|---|---|
| **the file** | `src/kaggle_gpu/RUN__pdf_ocr_control_<sym>.ipynb` — the generic notebook, cloned, suffix lower-case (`RUN__pdf_ocr_control_fpt.ipynb`) |
| **what changes in it** | **cell 2 (§1 · Parameters) and nothing else.** Every other cell stays byte-identical to the generic notebook, and that is CHECKED rather than assumed — compare the two sources cell by cell, never a remembered hash: §9's md5 was quoted as `a00f9c83156b` on 2026-09-04 and is `eef90abff419` today, because §9 itself changed. A clone that drifts in a cell nobody edited is exactly what these files exist to avoid |
| **the outputs** | stripped. A clone carrying the parent's outputs reports another ticker's verdicts as its own |
| **what runs** | ⚠️ **nothing.** Not a cell, not `plan`, not `export` |

### The parameters, and how each is decided from disk

Resolved with **`pdf_ocr_batch.plan_batch`** and **`job.seed_history`** — the same calls §3 makes,
read-only, no OCR, no network beyond the template fingerprint. What they answer, in order:

| parameter | how it is chosen |
|---|---|
| `ENVIRONMENT` | the word you said — `LOCAL` or `KAGGLE`, nothing infers it |
| `EXCHANGE` / `SYMBOL` | the ticker, and the board it is listed on. ⚠️ Both must be registered — `CAFEF_FINANCIALS_TICKERS` **and** `config.json` — or a Dagster path is silently unaddressable (CLAUDE.md §6-2-untricies) |
| `QUARTERS` / `ONLY_MISSING` | `[]` + `ONLY_MISSING = True` when the ticker already has statement CSVs — parse the GAP, not the ticker. `[]` + `False` only when it has none |
| `TEMPLATE` | left `None` so it RESOLVES and records which route answered — but the resolved value is stated in the header, because `bank` and `corp` are different failure modes (`CRP-1`) |
| `OVERWRITE` / `SPAN_OPERANDS` | `False`/`False` when `plan_batch` reports **no span operands**, which is the safer pair: a quarter already `pdf` in all three is dropped before any OCR and a DIFFERS is refused. `True`/`True` only when there ARE operands — §2 refuses `SPAN_OPERANDS` without `OVERWRITE` |
| `FORCE_EMPTY_BAND` | ⚠️ **the judgement call, and it is quantified rather than guessed.** `seed_history` is asked, per open quarter and per report, whether `sane` would have a band at all; the header carries the count of cells that would be REFUSED with the guard on. Default **`False`** — the guard stays — because lifting it is `BND-1` and the arithmetic screens then have to replace it by hand |
| `MERGE_INTO_CSV` | **off**, always. §9 does the upsert: one period at a time, oldest first, `force_differs=False` |
| `ONNX_ONLY` | **`True`** for any ticker whose rows were bootstrapped on a T4 — the 53-layer cascade is the one that wrote them, and the full 55 is a different procedure (`TSS-1`) |
| everything else | the generic notebook's value, untouched |

### The header the clone carries

A per-ticker clone exists to hold **what is true of that ticker and of no other**, so cell 2 opens
with a measured note: filed quarters, open cells, which statement they are in, the settled ones, and
any conclusion a past run withdrew. ⚠️ **That last part is the reason a clone is worth having at
all** — a table can carry a count, and only prose carries *"this was measured, and the conclusion
drawn from it was wrong"* (§8a; CLAUDE.md §6-2-septquadragies).

### Then you say run

Open it, run top to bottom, and read §2 and §3 before the OCR cell. The clone is **deleted when the
ticker is finished** — [§8](#8-what-is-still-missing--across-every-ticker-at-once) has the test, and
it is `complete = True` **and** `outstanding = 0`, never `complete` alone.

---

## 2. Choosing the filings

`QUARTERS` and `PERIODS` are both optional and they **INTERSECT**. Each one **raises** when it
matches nothing, because a filter that matches nothing is a run that parses nothing and reports
success.

| you want | write |
|---|---|
| one quarter | `QUARTERS = ["2014-Q3"]` — or `["2014-03"]`, the same quarter |
| a batch, in any order | `QUARTERS = ["2013-Q4", "2014-Q1"]` |
| the repo-native form | `PERIODS = ["Q3-2014"]`, `QUARTERS = []` |
| one quarter, stated twice | `QUARTERS = ["2014-Q3"]` **and** `PERIODS = ["Q3-2014"]` → Q3-2014 |
| **everything** ⚠️ | `QUARTERS = []` — 70+ documents, hours of GPU |
| **exactly the gaps** ⭐ | `QUARTERS = []` **and** `ONLY_MISSING = True` — see below |

⚠️ **AN EMPTY LIST MEANS EVERY QUARTER, NEVER NONE.** `[]` and `None` build the identical job
inside `plan()`. That is its contract, and it is why the default is the *absence* of a filter
rather than a list anything recomputes.

⚠️ **AND `QUARTERS` IS A LIST AND NOTHING ELSE SINCE 2026-09-03.** The control notebooks took
the strings `"ALL"` and `"OUTSTANDING"` beside the list until then; both are REFUSED now, by one
`TypeError` covering a string, a `None` and anything else that is not a list. **`"ALL"` is `[]`,
and `"OUTSTANDING"` is `ONLY_MISSING = True`.** Two types in one parameter had cost three
readings that each reported the wrong mistake: `QUARTERS = ""` is a FALSY string, so it fell
past the sentinel test and opened every quarter SILENTLY; `"  "` fell the same way and then
raised about the quarter FORM; and a bare `"2014-Q4"` with the brackets forgotten raised about
the MODE.

### ⭐ `ONLY_MISSING = True` — the gaps, resolved from disk (2026-09-02)

Read only when `QUARTERS` is empty. §3 reads the three statement CSVs and the PDF index, prints every
`(quarter, statement)` cell this ticker is still missing, and fills `QUARTERS` with the quarters
that have at least one cell a re-run could still WIN — dropping the ones already measured
permanently absent.

⚠️ **THE NOTEBOOK COULD NOT ANSWER THAT QUESTION UNTIL 2026-09-02, AND IT COST A SESSION.** A
request to parse "VCB Q2-2009 and Q3-2009" arrived on a ticker whose Q3-2009 had read `pdf` in
all three statements since it was first parsed, while the two cells actually missing — the
Q1-2009 and Q2-2009 BALANCE SHEETS — went unnamed. Finding that out meant an ad-hoc script over
the CSVs, so a request naming the wrong quarters looked exactly like one naming the right ones
until GPU had been spent on it.

⚠️ **IT IS NOT A SECOND RULE.** The quarters come from `documents()` through `plan()` — the same
call the run makes — and "already done" is `parsed_reports()`, which is `pdf` and nothing else.

⚠️ **AN EMPTY RESULT RAISES, and it has to.** `plan()` reads an empty `quarters` list as EVERY
quarter, so a "nothing left to do" answer that fell through would open 70 filings. Name the
quarters explicitly if you meant to re-parse something anyway. **VCB has read `OUTSTANDING` as
this raise since 2026-09-02**, which is the sentinel working, not a fault.

⚠️ **AND WHAT IS OUTSTANDING IS NOT WHAT IS WRONG.** This resolves cells that are `missing`; a
cell that is `pdf` and WRONG looks finished to it. `EQW-1`, `NST-1` and `PAR-1` are each a set of
`pdf` rows carrying a wrong figure, and the screens that find those live in `P47`/`P48`, not here.

⚠️ **TWO SPELLINGS, AND ONLY TWO.** `2014-Q3` and the zero-padded `2014-03` are folded onto the
first before anything is named — the job name, the payload directory and the Kaggle kernel slug
all come off this list, so two spellings reaching them would be two runs racing for one slug.
Everything else RAISES: `2014-3` (one digit is a keystroke from a MONTH), `2014Q3`, `2014-Q5`,
and the repo-native `Q3-2014`, which names the same quarter and is what `periods` takes.

### Why a QUARTER is the unit

`orchestration` §2a: the statement BUILD skips whole **years**, because `_decumulate` needs
Q1..Q(q-1) of the same year and a partial skip deletes the very quarter a run exists to fix.
⚠️ **That argument never bound this path** — nothing here de-cumulates — so the unit became a
quarter on 2026-08-29, and asking for 17 quarters no longer opens the 27 filings of the seven
years they fall in. The hazard still exists where the WRITE is, and the merge already refuses a
cumulative income statement a one-document run cannot de-cumulate — unless the quarters it would subtract were never filed, in which case it is written with `months` saying how long a span it covers.

### `TEMPLATE`

`None` resolves it — `templates.csv` first, then CafeF's own fingerprint over the network — and
the run folder records **which route answered**, because *"read off templates.csv"* and
*"guessed from a line-item count"* are not the same claim. State it (`bank` / `corp` /
`securities` / `insurance`) only when you are comparing two machines and want both down one
path.

⚠️ It is never defaulted to `bank`. That line existed until 2026-08-28 and was a silent wrong
answer for the **761 of 781** listed names that are not banks.

---

## 3. What travels, and what it costs

The payload is a zip of exactly the files the parse touches — chosen by
`pdf_ocr_job.plan()`, i.e. **the same function the worker calls**, so the payload cannot
diverge from the worker's own choice.

| in the payload | why |
|---|---|
| the ticker's PDF index | `documents()` re-runs on the worker |
| the chosen filings | ~1-14 MB each |
| the twelve charts of accounts | `schema_of` raises without them — **after** the OCR |
| **the statement CSVs already on disk** | twice over: `seed_history` rebuilds the magnitude band `sane` needs, and `compare()` scores the run against them |
| `deepdoc_det.onnx`, `vgg_seq2seq.pth` **and `vietocr_vgg_seq2seq.yml`** | otherwise the engine downloads them from HuggingFace and vocr.vn. ⚠️ **The third one is the recogniser's CONFIG, and it was the one nobody counted**: vietocr re-fetches it on every `Predictor` build and caches nothing, so when vocr.vn's certificate expired every `onnx@*` layer RAISED (`VCR-1`). The names live once, in `pdf_ocr_job.MODEL_FILES`, and the export raises rather than shipping fewer |
| **`vie.traineddata`** (12.4 MB) | ⚠️ **the fourth file, and without it the worker silently runs 53 of the 55 layers.** `tesseract@200` is layer 4 and `tesseract@400+relax` layer 7; a layer whose engine is not ready is dropped with a bare `continue`, and `ocr_ready` looks for this file in `TESSDATA_DIR` — which defaulted to a Windows path and therefore always missed on a worker (`TSS-1`). ⚠️ **The ENGINE was never absent**: `pymupdf` statically embeds Tesseract, measured in the manylinux wheel. ⚠️ **And it is OUR copy, never `apt-get install tesseract-ocr-vie`** — Ubuntu's build reads different characters, so its pin is the md5 in `pdf_ocr_job.TESSDATA_MD5` and every run records which model it read |

**Measured costs** — and read the caveat under the table before budgeting on any of them:

| | |
|---|---|
| a filing accepted at layer 1 | ~1 min (VCB Q1-2026) |
| a filing that defeats the cascade | **26-32 min** (BID Q4-2016, layer 45 of 47; VIC Q1-2026, 47 of 47) |
| the Kaggle **queue**, before anything starts | **~5 min**, every time |
| payload upload | ~1 min per 85 MB |
| Kaggle GPU quota | 30 h/week, free |

⚠️ **THE T4 IS NOT FASTER THAN THIS LAPTOP — measured twice, interleaved.** Per OCR page:
local **0.62-0.68 s** against T4 **0.78-0.95** (median local/T4 = **0.80×**). A T4 is a 2018
Turing part; what it has more of is VRAM, which this workload does not need. **What Kaggle
buys is a SECOND machine running in parallel, free, without occupying this one** — that is
worth having and it is not a multiplier. CLAUDE.md §6-2-duodetricies.

⚠️ And the cost of a document tracks its **size** as much as its difficulty: 9.1 min/MB on one
annual report against 2.5 on a quarterly, both running the full cascade (§6-2-noviesdecies).

---

## 4. Reading the result

`kgpu` merges the run folder into `reports/pdf_ocr/<run_id>__<exchange>_<symbol>__pdf_ocr/`.
The notebook's **7 · The result** and **8 · Refused vs written** read it; `metadata.json`
carries the whole scorecard in `results`, one row per `(period, report)`.

| verdict | what it means |
|---|---|
| `REPRODUCED` | every cell, the winning **layer**, the unit and `publish_date` match disk |
| `DIFFERS` | one of those moved — the run names which, with both figures |
| `absent in this run` | the cascade refused the statement; the log says why |
| `no pdf row on disk to compare against` | ⚠️ **a RECOVERY, not a reproduction** — nothing scored it |
| *(refused)* | a cumulative income statement is not scored against a row covering a different span; that would report every cell as changed. Two rows whose `months` MATCH are compared normally |

⚠️ **`compare()` READS EVERY COLUMN, NOT THE FIGURES.** Two runs have been caught losing a
single `publish_date` and nothing else — a figures-only diff called both of them clean.

### ⚠️ Did it actually WRITE anything? — ask the artefact, then ask the CSV

**"The run finished" and "the CSV changed" are different facts, and only the second was ever
the point.** They came apart twice: HOSE_BSR (14 documents) and HOSE_CTG (**8 h 40 m, 201
accepted cells**) both finished green, wrote complete run folders and created **no statement
CSV at all**, because every statement was refused for an empty `sane` band (`BND-1`).

Since 2026-08-30 there are three places to look, and the notebook's sections **7-9** are them:

| | says |
|---|---|
| `metadata.json` -> **`merge`** (schema **v3**) | one event per upsert plus their union — `statements_written`, `statements_skipped`, `periods_written`, and every decision with its reason |
| `inputs.merged_into_csv` | ⚠️ **what HAPPENED, not what was asked for.** True only if something was actually written. The request is still there, as `inputs.merge_into_csv` |
| the CSVs themselves | the notebook's **10 · Did it land?** reads `bs_/is_/cf_<EXCHANGE>_<SYMBOL>.csv` and prints `pdf`/`missing`/`cafef` per report, and how many rows came from this run |

⚠️ **UNTIL THAT BLOCK EXISTED, `merged_into_csv` READ `false` ON EVERY KAGGLE RUN AND COULD NOT
HAVE READ ANYTHING ELSE** — `metadata.json` is written by the WORKER, which has no path to this
disk, and the pull that does the merge wrote nothing back. On a v2 folder the **absent** block
means *"this run predates the field"*, never *"nothing was written"*. `MRG-1`.

⚠️ **AND A MERGE DECISION IS NOT IN THE WORKER'S `run.log`.** The merge runs HERE, after the
pull, so a filter over that log finds only the PARSE's refusals — the notebook prints the two
under separate headings for that reason. A cell that conflated them once printed *"no refusals
— every statement was accepted"* over the CTG run that wrote nothing.

⚠️ **A FOLDER ALREADY IN `reports/pdf_ocr/` IS NOT RE-MERGED.** `pull` offers the upsert only
what it copied THIS time, so a re-pull or a second push of the same job leaves the CSVs
untouched — deliberately, and it now says so and names the command:

```powershell
python -m kgpu merge <job>            # finishes it
python -m kgpu merge <job> --force-empty-band     # ...for a ticker with no CSV yet
```

### The log's shape — one line, leading with the OVERALL % (2026-08-30)

Both machines print the same shape, from the same formatter (`utils/progress.py`), so a
LOCAL run and a KAGGLE run read alike:

```
 33.7% - doc 2/3 HOSE_TCB Q3-2013 - layer 12/47 onnx@300 - page 40/96  ~76 s left   <- the OCR
 42.5% - step 5/15 HOSE_TCB 3q - wait kernel - [ 1.5 min] RUNNING  25% of last      <- the control
```

`xx.x% - task - sub-task - detail`. ⚠️ **The percentage is a POSITION IN THE PLAN, not a
fraction of the time left.** LOCAL it is `documents finished + this document's place in the
47-layer cascade`, so a filing accepted at layer 1 (~1 min) jumps its whole share at once
while one that defeats the cascade (33 min) crawls through it — the number is a LOWER BOUND,
and low is the honest direction to be wrong in: a run finishes early, it does not stall at
99 %.

⚠️ **ON KAGGLE IT STANDS STILL THROUGH `wait kernel` UNLESS THIS EXACT JOB HAS COMPLETED
ONCE BEFORE.** `kernels_status` reports QUEUED / RUNNING / COMPLETE and no fraction, so the
only honest clock is this job's own last duration — and a job is named after its ticker and
quarters, so its first run has none. The detail keeps printing the elapsed minutes; nothing
here invents a curve to make the bar move.

⚠️ **A READER THAT MATCHED THE START OF A LOG LINE IS NOW BROKEN.** `run.log` lines begin
with the percentage — `progress.detail_of(line)` returns the segment that used to BE the
line, and the control notebook's own "what was refused" cell was the first thing this broke.

#### ⚠️ AND SINCE 2026-09-04 THE WHOLE NOTEBOOK IS ONE PLAN — the % is of the SESSION

`RUN__pdf_ocr_control.ipynb` §2 builds ONE `progress.Stages` and every cell after it reports
through that, so a line from §3 and a line from §9 are positions in the same plan:

```
  0.9% - step 2/10 HOSE_CTG 2014-Q4 - what is left   - 70 quarter(s) filed, 70 complete
 92.2% - step 5/10 HOSE_CTG 2014-Q4 - OCR the filings - 1/1  HOSE_CTG Q4-2014
 98.3% - step 9/10 HOSE_CTG 2014-Q4 - did it land    - balance_sheet  70 quarters  pdf=70
```

**15 steps on KAGGLE, 10 on LOCAL, and the OCR is ~86 % of either** — the difference is that
the six round-trip steps (`kgpu.runner.RUN_STAGES`) are stages of THIS plan on KAGGLE, so
`runner.run` is handed the notebook's own reporter and one number stays on the line. Before
this, §5 and §6 each ran a plan of their own and the other nine cells printed bare prose, so a
reader had three denominators and no way to tell a session 3 % in from one 96 % in.

⚠️ **RUN IT TOP TO BOTTOM, AND RE-RUN §2 AFTER EDITING §1** — the plan's SHAPE depends on
`ENVIRONMENT`. The number is monotone by construction, so a cell re-run out of order re-prints
its step at the percentage already reached rather than winding the bar back.

⚠️ **THE STAGE WEIGHTS ARE NOMINAL AND THE NOTEBOOK SAYS SO.** They put the OCR where it
belongs and they measure no run. ⚠️ **A skipped step CLAIMS its weight** rather than
redistributing it — at `EXECUTE = False` the bar jumps to 92 %, which is honest: the plan is
the plan, and nothing is left to do.

⚠️ **A DOCUMENT'S OWN PER-PAGE LINES ARE NOT IN THE CELL.** `ISOLATE_DOCUMENTS` runs one
subprocess per filing and a subprocess INHERITS stdout, so those lines go to the kernel log.
The cell carries the batch's position, one line per document; the pages are in that document's
`run.log`, in the same shape.

### When a statement is `absent`, read the FIRST refusal

`_parse_cascaded` prints the distinct reasons with the first layer that gave each. ⚠️ **A
cascade's FINAL refusal names the hardest path tried, not the blocking defect.** The label
`fx not mapped` sent this repo down a wrong diagnosis for two days that way, and six of the
seven quarters it was blamed for then parsed at a **strict** layer with no FX change at all
(CLAUDE.md §6-2-duovicies).

⚠️ **AND ONE REFUSAL IS NOT A DEFECT AT ALL: `no such statement on any page of this filing`.**
It means what it says — the filing does not CONTAIN that statement, and no layer can conjure
one. A **`BÁO CÁO TÀI CHÍNH TÓM TẮT` (Mẫu CBTT-03)** is a condensed disclosure form carrying a
balance sheet and a four-line P&L and **no cash flow**; ACB's 2009 quarterlies are three pages
and are exactly this. `missing` is then the correct and permanent answer (§5 rule 24). ⚠️ It
reads identically to a scan the OCR could not handle, which is what makes it worth naming: those
two quarters were re-run twice before anyone opened the PDF (§6-2-sesquadragies).

⚠️ **`only N rows parsed` on such a form is the OTHER half of the same story.** `MIN_ROWS` = 12
keeps a page that is not a statement out, and a condensed P&L has four lines. That is what
`onnx@200+unit+condensed` — last in the cascade — is for, and it fires only when the statement's
own pages carry the P&L's summary wording. It cannot license a cash flow at all.

---

## 5. The things that have actually gone wrong

Each of these is measured, not anticipated.

**⚠️ An empty magnitude band.** `sane` compares a statement's probe against quarters already
accepted; with no band it **fails open**, and that is the documented way a run writes a wrong
figure (§6-2-octodecies). `kgpu rehearse` prints the band and WARNs when it is empty — a ticker
with nothing on disk yet (VIC was one) gets no band at all, so its first run is unguarded by
construction. Run the rehearsal and read that line.

**⚠️ A green GPU run that is half on the CPU (`ORT-1`).** `get_available_providers()` is an
advertisement; `session.get_providers()` is the measurement. A bare `onnxruntime-gpu` resolves
to a wheel needing CUDA 13 against Kaggle's 12.8, and detection silently falls back to the CPU
— 21 % slower, with one warning buried in ANSI noise. The notebook now reports what the
**session** holds, and the run folder records it: check `detection` and `recognition` in the
result cell. **The two OCR halves fail independently** — detection is onnxruntime, recognition
is torch — so *"the GPU was used"* is two questions.

**⚠️ The payload and the parameters are two copies of one filter.** `data.documents` decides
which filings are UPLOADED; `PERIODS`/`QUARTERS` decide which the worker OPENS. A mismatch ships
one set and parses another, and the worker reports the shortfall as `missing` — the same word a
genuinely unreadable filing gets. `pdf_ocr.job()` builds both from your arguments and
`_validate` still checks them against each other.

**⚠️ Two runs with the same name overwrite each other.** `cfg.name` decides the payload
directory, the rehearsal directory *and* the Kaggle kernel slug. The name is derived from
`SYMBOL` + a scope built from your filter (`pdf-ocr-vic-q3-2014`, `pdf-ocr-vic-2014`,
`pdf-ocr-vic-2013-2014`, `pdf-ocr-vic-all`), so changing a parameter changes the job. Two
different filters that produce the same scope — `[2010…2020]` and `[2010, 2020]` both give
`2010-2020` — **do** collide; pass `scope="…"` to separate them.

**⚠️ Version identity is neither necessary nor sufficient for output identity.** Mismatched
onnxruntime (1.20.1 local vs 1.22.0 Kaggle) once REPRODUCED 98 of 98 cells; identical
onnxruntime 1.20.2 on both DIVERGED, and the fallback layer wrote a row-slid income statement
that both gates accepted. The invariant that holds is **verified-equal output**, which is what
`compare()` measures. `ORT-2`.

---

## 6. Merging a recovered quarter into `raw_data/`

It happens on its own: `MERGE_INTO_CSV = True` is the control notebook's default, so
`kgpu run` merges as soon as the folder lands — and **since 2026-08-30 every merge records what
it did into the run folder's own `merge` block**, so *"did it write?"* is answerable from the
artefact rather than from a log on the wrong machine (§4, `MRG-1`). To do it by hand, or to
look first:

```powershell
cd src\kaggle_gpu
python -m kgpu merge pdf-ocr-vic-q3-2014              # writes, after a backup
python -m kgpu merge pdf-ocr-vic-q3-2014 --dry-run    # prints every decision, touches nothing
```

`pdf_ocr_merge` does not write the CSV itself — it calls `FinancialsBuilder._write(merge=True)`,
the same upsert `build()` uses, so **only the quarters this run produced are rewritten** and
every other row keeps what the file already holds. A backup of all three CSVs goes to
`raw_data/_backup/statements/<timestamp>__<EXCHANGE>_<SYMBOL>/` before anything is touched.

✅ **Verified on the first real merge, 2026-08-29** — VIC Q3-2014's income statement, diffed
against the backup **column by column across all three files**: `income_statement` 21 → 22
parsed, exactly one period and 22 columns changed, and **balance sheet and cash flow moved not
one cell** even though `_write` rewrote all three files. ⚠️ That check also found the backup
landing under `src/kaggle_gpu/` — `BACKUP_ROOT` was relative to the CWD, so the one thing that
makes a merge reversible went where nobody looks. It is anchored to the repo now.

### ⚠️ The four refusals, and the measurement behind each

| refused | why | override |
|---|---|---|
| a **cumulative income statement whose priors WERE filed** | an annual or half-year filing prints the year to date; the CSV column holds the standalone quarter, and this job cannot de-cumulate — a one-document run has no Q1..Q(q-1). An authoritative `build()` over the whole ticker CAN, so writing the YTD figure now would pre-empt a better answer with a worse one | `force_cumulative` |
| ⚠️ *(not refused)* **a cumulative income statement whose priors were NEVER filed** | nothing will ever subtract quarters that were not reported, so the choice is *cumulative now or nothing, ever*. It is WRITTEN, with `months = 6` or `12` saying what it covers — BSR Q4-2016, whose only 2016 filing is the FY-2016 annual. ⚠️ **Read `months` before summing or diffing two rows of one column** | — |
| a statement whose **`sane` band was empty** | with no band the magnitude guard fails open, so the figure passed no guard at all. A ticker with nothing on disk yet has no band by construction — ⚠️ **so a green run on a NEW ticker writes NOTHING until this is overridden**, and the band then stays empty for the next run too (`BND-1`). Measured on HOSE_BSR, 2026-08-30: 14 documents, 0 of 42 statements written | `force_empty_band` — `FORCE_EMPTY_BAND` in the notebook, `--force-empty-band` on either CLI. ⚠️ It lifts the guard, so screen the artefact (unit per report, total assets quarter on quarter) before quoting anything |
| a figure that **DIFFERS from a good `pdf` row** | `compare()` already scored it; two runs disagreeing about a number is not resolved by taking the newer one | `force_differs` |
| a document whose parse **RAISED** | a layer that refuses has measured the FILING; one that raises has measured the MACHINE, so whatever won did so by default. Measured 2026-08-29: `vocr.vn`'s certificate expired, every `onnx@*` layer raised, and `tesseract@200` rewrote 13 columns of a filing that had reproduced 98 of 98 cells (`VCR-1`) | `force_engine_errors` |

⚠️ **AND THE FIRST RUN THIS WAS BUILT FOR IS WHY THE SECOND REFUSAL EXISTS.** On 2026-08-29 the
worker ACCEPTED a VIC Q3-2014 income statement that the full local run had REFUSED
(`sane: probe exactly equals an already-accepted quarter`). Nothing about the machine differed
— the cash flow reproduced bit for bit at the same layer, and the balance sheet was refused for
the same reason on both. What differed is the **population the gate compares against**:
`seed_history` reconstructs the band from the `pdf` rows on DISK, while a full run accumulates
it IN THE RUN, over more quarters and over pre-de-cumulation figures. **A statement a worker
accepts is not a statement a full run would accept**, and the merge cannot tell the difference
— only you can, by reading the figures against the filing.

### ⚠️ SCREEN BEFORE YOU MERGE A BOOTSTRAP RUN — `statement_screens.py` (2026-09-04)

`FORCE_EMPTY_BAND` lifts a real guard (`BND-1`), and **the arithmetic screens are what replaces
it**. They are CODE now (`P47`(b)) rather than a script rewritten per ticker — free, no OCR, no
network:

```python
from web_scraper import statement_screens as screens
flagged = screens.screen_run(folders)      # {(period, report): [why]}
screens.report(flagged)
```

`screen_document` checks the identities a filing asserts about ITSELF — `assets == resources`,
`A + B = TỔNG CỘNG TÀI SẢN` and `C + D = TỔNG CỘNG NGUỒN VỐN` on a `corp` chart, and the cash
identity — and `screen_run` adds total-assets continuity, **which needs the whole batch because
a figure wrong by 10^6 reconciles perfectly against itself**. Hold the flagged pairs out of the
merge through `merge_run`'s own `periods`/`reports` filter, never by editing the artefact.

⚠️ **THE `unit` SCREEN IS DELIBERATELY NOT PART OF IT.** Taking the MINORITY `unit` of a
report as the suspect convicted 8 TCB statements correctly and then flagged **32 CTG ones that
were all right**. `accepted.values` are ALREADY scaled, so the declared unit is a fact about the
FILING and never evidence about the figure — what convicts is the MAGNITUDE.

⚠️ **AND CONTINUITY IS A PER-QUARTER RATE, because a batch parses the OUTSTANDING quarters
and "consecutive" is therefore not consecutive on the calendar.** FPT's run held Q2-2009 and
then Q2-2010 and the honest 1.79x between them was flagged, while every neighbour of the pair
confirmed both figures.

⚠️ **They are not `sane` and do not replace it.** `sane` compares against the magnitudes a run
has already ACCEPTED; these are identities. A statement can pass every one and still be the
wrong column of the right page — `PYR-1` is exactly that. Use them to decide what NOT to merge.

### Still the safer route, when a quarter matters

Materialise `raw/cafef_financials` through **Dagster** with the preceding quarters in `periods`
so `sane` has the same history the probe had, then **diff every column** against a backup and
restore any non-target quarter that moved. That is more work and it is what a full run's band
buys you. CLAUDE.md §6-2-quinvicies; RUNBOOK.md.

## 7. The CLI instead of the notebook

**LOCAL** needs no job and no `kgpu` at all — it is one module:

```powershell
cd src
python -m web_scraper.pdf_ocr_job --symbol VIC --quarters 2014-Q3 --merge
python -m web_scraper.pdf_ocr_job --symbol VIC --quarters 2014-Q3 --overwrite --merge
python -m web_scraper.pdf_ocr_job --symbol VIC            # every quarter, write nothing
```

⚠️ **`--merge` IS OPT-IN HERE AND ON IN THE NOTEBOOK, DELIBERATELY.** The module's product is a
run folder, and a run that writes nothing cannot silently downgrade a quarter; the notebook is
where a person has read the plan and asked for the write. `--overwrite` re-parses quarters
already `pdf` in all three statements and lets the merge replace what disk holds.

**KAGGLE** is a wrapper over four verbs. To use them from the CLI a job has to exist in
`kaggle_config.json` — that is the difference, and it is why the notebook exists.

```powershell
cd src\kaggle_gpu
python -m kgpu plan     <job>    # what would run; touches nothing
python -m kgpu data     <job>    # export the filings -> private dataset
python -m kgpu rehearse <job>    # the WORKER side, locally, no quota
python -m kgpu run      <job>    # push, wait, download, merge
```

From Python, with no config file at all:

```python
from kgpu import export, pdf_ocr, runner
cfg = pdf_ocr.job("VIC", quarters=["2014-Q3"], template="corp")
runner.plan(cfg)                       # touches nothing
export.export(cfg)                     # -> .payload/<job>/   ⚠️ REQUIRED before a rehearsal
runner.rehearse(cfg)                   # the worker side, against that payload
runner.run(cfg, refresh_data=True)     # re-exports, uploads, pushes, waits, pulls, merges
```

⚠️ **`export` IS NOT OPTIONAL AND THIS SNIPPET OMITTED IT UNTIL 2026-08-29.** `rehearse` runs
the worker against `.payload/<job>/`, so without it the FIRST run of any job raises
`no staged payload` — and the fix that error names, `python -m kgpu data <job>`, cannot resolve
a job that was COMPUTED here rather than written into `kaggle_config.json`. Exporting is local
and free; only `data`/`run` upload. CLAUDE.md §6-2-sextricies.

---

## 8. What is still MISSING — across every ticker at once

Sections 4 and 6 are about ONE run. The question they cannot answer is the planning one:
**which quarters are left, on every ticker that has ever been parsed?**

⚠️ **FOR ONE TICKER, THE CONTROL NOTEBOOK'S §3 ANSWERS IT AND `ONLY_MISSING = True` ACTS ON
IT** ([§2](#2-choosing-the-filings)). This one is the cross-ticker view: it ranks tickers, it does
not name a cell, and it is where you decide WHICH ticker to open next.

⚠️ **AND A PER-TICKER COPY OF THE CONTROL NOTEBOOK IS DELETED WHEN THIS TABLE SAYS THE TICKER IS
DONE.** `RUN__pdf_ocr_control_<ticker>.ipynb` exists only while a ticker is being finished;
keeping a finished one is how a session spends hours re-parsing a ticker with nothing left to
win. `RUN__pdf_ocr_control_ctg.ipynb` went on 2026-09-03, at `complete = True` with **0
outstanding cells** on all three statements. ⚠️ **`complete` ALONE IS NOT THE TEST** — it measures
continuity from the START of the filing chain, so ACB, BID and BSR read `True` today with 5, 7
and 2 cells still open. **Read `outstanding` beside it**, and delete only on both. ⚠️ `RUN__pdf_ocr_control_tcb.ipynb` is KEPT for now although TCB is 171/171 from Q2-2012 — Q4-2008 is still open and the ticker's own note records two conclusions that were withdrawn (§8a), which is the thing a future session most needs and which no table carries.

```powershell
jupyter lab src\kaggle_gpu\RUN__pdf_ocr_summary.ipynb    # read-only: no OCR, no network, no write
```

It reads every `statements/**/*.csv` (all four templates), joins them to the **PDF index**
through `FinancialsBuilder.documents()` itself, and returns one row per ticker:

| column | |
|---|---|
| `exchange` | the listing board, read from the CSV's own column |
| `complete` | `bool` — **all three marks `<= first_report`**: every statement's read run reaches back at least to where the FILING chain starts. A `—` column is NaN, every comparison with NaN is False, so one unread statement drops the row on its own |
| `first_report` | the quarter that OPENS the contiguous filing chain, read from `raw_data/cafef/pdfs/files/` — a fact about **filings**, not about the parse |
| `balance_sheet` / `cash_flow` / `income_statement` | the FURTHEST-BACK quarter that statement has been read from without a break, anchored at its own newest quarter READ |

Quarters print as `2008-Q4`, which sorts and is the `--quarters` / `QUARTERS` spelling
[§2](#2-choosing-the-filings) takes — so a cell of this table pastes straight into a parse.

⚠️ **THE THREE STATEMENT COLUMNS ARE A START MARK, NOT THE "LATEST OUTSTANDING QUARTER" THEY
WERE UNTIL 2026-08-31.** Each answers *"how far back has this statement been read without a
break"*, anchored at its own newest quarter READ — so a hole at the TOP does not empty the
column. BID is missing only **2026-Q2** and still reads bs **2008-Q4** / is **2011-Q3** / cf
**2011-Q3**; VIC, stopped half way, reads **2011-Q1 / 2014-Q1 / 2014-Q2**. ⚠️ The cost is that the
column alone cannot say whether the run reaches the present — `complete` and `outstanding` answer
that, which is why the three numbers are read together.

⚠️ **`first_report` IS READ OFF THE PDF FILES ON DISK, NOT THE INDEX AND NOT THE CSVs (2026-08-31).**
The index is what CafeF advertises; **the files are what an OCR run can open**. The two are not the
same: measured over the 7 parsed tickers, exactly **1 quarter has a filing in the index and no PDF
on disk** — ACB 2009-Q3, which is still carrying `pdf` rows parsed from the file that has since
gone. The notebook WARNs on it. A filename carries its own period (`Q3-2011_…`, `FY-2016_…`), the
annual folds onto Q4 exactly as `documents()` folds it, and the 3 `NA-<year>` files across all 784
tickers are skipped rather than guessed (§5 rule 2).

⚠️ **`complete` MEASURES THE START OF THE CHAIN, NOT ITS TOP — the one thing it cannot see.** An
outstanding quarter at the TOP does not drop it: **BID reads `True` while 2026-Q2 is unparsed**.
Measured 2026-08-31 over the 7 tickers: ACB and BID `True`, the other five `False`.

⚠️ **So the table names no missing quarter, and since 2026-08-31 nothing else does either** — the
per-statement listing that used to print under it was removed on request. The `outstanding` grid
(every filed quarter × three statements, no `pdf` row) is still computed in the notebook and is
where that list comes from.

⚠️ **THE DENOMINATOR IS THE FILING INDEX, AND IT HAS TO COME FROM OUTSIDE THE CSVs.** A
`missing` row carries no `document` (CLAUDE.md §6-2-terdecies removed provenance from rows
nothing produced), so within the statements CSV *"the company never filed"* and *"a filing
exists and the parse failed"* are **the same word**. Outstanding cells therefore come in two
shapes and both count: `missing` (a row exists, it was tried, a gate refused it) and `absent`
(**no row at all** — the CSV grid never reached that quarter; VIC has 45 of these from the run
that was stopped half way, and BID has 2026-Q2, published after the parse finished).

### ⚠️ The FILING-CHAIN mark — CONTIGUITY is live, the INDEX source is not

⚠️ **Rule 2 (contiguity) is what `first_report` still is, and it is also how the three
statement columns are built. Rules 1 and 3 are HISTORY as of 2026-08-31**: the mark no longer
comes from the index at all — it is read off the PDF FILES — so the `quarterly_filing` column
that rule 3 fixed is still computed and asserted but no longer read by anything. Rule 1's reason
is kept because it is the price the current `complete` knowingly pays: it counts from the START
of the chain and cannot see a hole at the top.

1. **It comes from the INDEX, never from the parse.** A mark taken from the first quarter that
   *parsed* pushes every early failure out of its own denominator — `SAN-1`'s shape, a measure
   learning its baseline from the thing it is measuring. BID read `complete = True` while its
   Q3-2011 income statement was `missing`, on a filing whose other two statements read fine at
   `onnx@200`.
2. **The chain must be CONTIGUOUS, walking back from the ticker's NEWEST filing** (2026-08-30).
   Touching a quarter with no filing ends it; an isolated old filing opens no chain. Anchored at
   the ticker's own newest filing, not the calendar quarter — a delisted name still has a chain,
   and the calendar anchor would break every ticker at step one.
3. **"The ticker filed a QUARTERLY report" is read off the index's own `quarter` column**
   (2026-08-30). `documents()` returns ONE filing per quarter and prefers the audited **annual**
   at Q4, so its `annual` flag answers *"is the CHOSEN document a quarterly one"* — not *"did the
   company file one"*. **ACB 2009-Q4 is the counter-example sitting in this corpus**: the index
   carries both `Báo cáo tài chính quý 4 năm 2009` (`quarter=4`) and the audited annual
   (`quarter=5`), the annual wins, and the old column answered `False` for a quarter ACB **did**
   file quarterly. Measured over the 7 parsed tickers: **98 quarters flip `False` → `True`, none
   the other way, and NOT ONE `first_report` moves** — every chain today starts at a non-Q4
   quarter, so this is a **latent** defect: it bites only when a ticker's chain opens on such a
   Q4, and then it pushes the mark a quarter late and takes three cells out of the denominator
   in silence. The converse (`documents()` says quarterly ⇒ the index holds a `quarter` 1..4 row)
   is **asserted** in the notebook, 0 violations. ⚠️ The test does **not** filter `consolidated`,
   because the denominator is `allow_parent=True` and a narrower test would drop ACB 2009-Q4,
   which is a standalone filing.

| ticker | the PDF index says | before | **after** | cells leaving the denominator |
|---|---|---|---|---|
| **ACB** | 2008-Q1, then **four empty quarters**, then 2009-Q2 → 2026-Q1 unbroken | 2008-Q1 | **2009-Q2** | 3 — all of one lone quarter |
| **BSR** | 2016-Q4, 2017-Q3, 2017-Q4, **no 2018-Q1**, then 2018-Q2 → 2020-Q4 | 2017-Q3 | **2018-Q2** | 3 |
| BID · TCB · VIC · VCB · CTG | no break after the mark | — | **unchanged** | 0 |

⚠️ **CONTIGUITY IS MEASURED OVER EVERY FILING WHILE THE MARK IS STILL A QUARTERLY ONE — three
variants were run before one was chosen.** `documents()` folds the audited annual onto Q4, so
demanding contiguity over *quarterly* filings alone breaks at **every Q4** and the longest chain
is three quarters. Taking the chain's own first quarter as the mark instead pulls VCB and CTG
back to 2008-Q4 — a year that filed nothing but its annual — and hands VCB one more blocking
cell, the cumulative Q4-2008 income statement that can never be split
([§6](#6-merging-a-recovered-quarter-into-raw_data), CLAUDE.md §6-2-unquadragies). **Filter for
contiguity first, then take the earliest QUARTERLY filing still inside the chain.**

⚠️ **THIS TABLE COUNTS QUARTERS `pdf_ocr_job` REFUSES TO OPEN AT ITS DEFAULT — read the
listing under the table before you launch a parse.** The denominator here is `allow_parent=True`
(the widest set on disk); `pdf_ocr_job.plan()` defaults to `allow_parent=False`. A quarter whose
only filing is a **standalone** report is therefore reported missing here *and* answered with
`"HOSE_ACB files no document for quarter(s) ['2009-Q2', '2009-Q3', '2009-Q4']"` there — two
opposite answers about a quarter whose PDF is sitting in `raw_data/cafef/pdfs/files/`. Measured
2026-08-30: **ACB 4 quarters (2008-Q1, 2009-Q2, 2009-Q3, 2009-Q4 — every one of ACB's outstanding
quarters), TCB 2, VIC 1, BID/CTG/VCB/BSR 0.** The `parent_only` column measures it. ⚠️ **The listing that used to name those quarters and the
knob was removed on 2026-08-31**, so nothing prints it now — it had also been **commented out
from the day it was written** until 2026-08-30 while the notebook's own prose promised it, which
is a document promising an output that does not exist. This time the removal is deliberate and
is recorded in all three places that used to promise it.

⚠️ **`complete = False` means *at least one statement is not yet continuous to the newest filing*,
never *the parser is broken*.** A filing may not contain that statement at all, and a cumulative
income statement whose Q1..Q(q-1) it cannot subtract is refused by the merge by design.

⚠️ **THAT REFUSAL IS NARROWER THAN IT WAS, TWICE, AND THE "CTG HAS 32 SUCH QUARTERS" MEASURED
HERE ON 2026-08-30 IS SPENT** — all 35 of CTG's Q2/Q4 income statements read `pdf` today. Two
changes closed them: `SPN-1`'s `SPAN_OPERANDS`, which re-parses a prior whose `months` is blank
so the Q4 becomes splittable (no figure moves — the merge's `fills_span` branch writes the span
and nothing else), and `SGN-1` on 2026-09-03, which made the subtraction itself sound. ⚠️ **A
count of refusals is a fact about the CODE on the day it was taken**, and this one outlived two
fixes; re-measure rather than quoting it.

⚠️ **THE PDF INDEX IS NOT IN GIT** — `raw_data/` is ignored except `financials/` — so a fresh
checkout cannot prove which quarters were filed. Those tickers read `complete = False` with a
warning rather than a guess (CLAUDE.md §5 rule 2).

### ⚠️ 8a. TWO THINGS THIS TABLE CANNOT TELL YOU, AND BOTH COST A WRONG CONCLUSION ON TCB

Measured 2026-09-04, finishing TCB. **The ticker went from 168 to 171 of 171 cells from Q2-2012**,
and three of the five recoveries came only after a conclusion recorded in `CLAUDE.md`,
`ISSUES.md` and a control notebook was withdrawn. Both mistakes have the same shape, and this
table's own columns are where a reader will meet them.

**1 · `open — a re-run could still win it` is not `winnable`, and `SETTLED` is not proof.**
`settled_absences` marks a cell permanent on ONE reason, `no such statement on any page of this
filing`, on the ground that it is a verdict on the DOCUMENT. ⚠️ **It is a verdict on the PAGE
CLASSIFIER** (`SET-2`). TCB's Q1-2017 and Q3-2017 print the notes title **and the notes form
code** (`Mẫu B050/TCTD - HN`) on the cash flow's own FIRST page, so no cash-flow page is found,
all 67 layers report that reason, and both quarters were recorded as filings containing no cash
flow. **Page 8 of Q1-2017 prints "LƯU CHUYỂN TIỀN THUẦN TỪ HOẠT ĐỘNG KINH DOANH" over 67
figures.** Both now read `pdf`, identity residual **0**.

⚠️ **A SETTLED CELL IS DROPPED BY §3 BEFORE ANY OCR**, so re-trying one means naming its quarter
in `QUARTERS` explicitly — and a settled record written before 2026-09-04 still says PERMANENT
about a filing whose statement was merely mis-titled.

**2 · `documents()` returns ONE filing per period, and a quarter can have several.** TCB's
Q2-2019 closing cash balance is printed **under the company's round stamp** in the AUDITED
consolidated filing: 47.141.880 reads as 171414880 / 17141880 / 19111880 / 17141.880 at 200 /
300+500 / 400+pad6 / 600 dpi, never the printed figure, because the ink is over the digits. All
of that is true, and it is a fact about ONE DOCUMENT — CafeF holds **three** consolidated
Q2-2019 filings, and the REVIEWED one reads the whole tail cleanly at layer 1. `build()` had had
an alternate-filing retry since 2026-08-25 and `pdf_ocr_job` was documented as deliberately
lacking it; that reasoning was `_decumulate`'s, not its own (`ALT-1`).

⚠️ **§7 OF THE CONTROL NOTEBOOK NOW NAMES THE FILING EACH STATEMENT CAME FROM**, because the row
on disk names the alternate and nothing else a reader sees would say so. The ENTITY is fixed by
`alternates`, so a fallback can never change which company a row describes; the ASSURANCE may
drop, and that is the trade.

**⚠️ SO THE PRACTICAL RULE IS: BEFORE CALLING A QUARTER UNPARSEABLE, READ ITS PDF INDEX AND LOOK
AT THE PAGE.** Both cost minutes; both were skipped twice; both would have answered it. Rendering
one 612×792 strip is what settled that TCB's mis-titled headers are the DOCUMENT's own and not
OCR damage — which is the difference between a page-classification fix and a week of layer
hunting.

⚠️ **AND ONE SCREEN MUST NOT BE RUN ON THIS TICKER.** `P43`'s cumulative-cash invariant — a
cumulative cash flow prints ONE opening balance per year — does **not** hold for TCB's 2016-2017
filings: Q1-2017 opens at 12,816,151 and Q3-2017 at 14,193,097, and the same disagreement is
already on disk for 2016 in rows written long before. Both 2017 readings were verified against
the rendered page and each one's prior-year column reproduces the corresponding quarter on disk
to the đồng. Which period those openings anchor to is **not established**.

---

## 9. Where the rest is written down

| | |
|---|---|
| the parse itself — the cascade, the gates, the 47 layers | [`src/web_scraper/CONTEXT.md`](../../web_scraper/CONTEXT.md) §3a, §3b |
| `kgpu` in general — payload modes, the measured traps | [`README.md`](../README.md) |
| what the OCR has produced, ticker by ticker | `CLAUDE.md` §6-2-octies … §6-2-unsexagies |
| open defects — read `CRP-1` before quoting any non-bank figure, and `SET-2`/`ALT-1` before calling a quarter unparseable | [`docs/ISSUES.md`](../../../docs/ISSUES.md) |
| what to run next | [`docs/TODO.md`](../../../docs/TODO.md) — `P38`, `P6`, `P5` |
