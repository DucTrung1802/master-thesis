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

```python
ENVIRONMENT = "LOCAL"        # "LOCAL" = parse here | "KAGGLE" = ship it to a T4
EXCHANGE    = "HOSE"         # HOSE | HNX | UPCOM
SYMBOL      = "VIC"
QUARTERS = ["2014-Q3"]       # [] or None = every quarter the ticker files. YYYY-QQ.
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
saying only *"DIFFERS in 2 columns"*. The notebook's **10 · Repair one row** takes explicit
`(quarter, statement)` pairs and scopes the write through `merge_run`'s own `periods`/`reports`
filter, so nothing outside the list can move.

Prefer a terminal? The same thing, four verbs — see [§7](#7-the-cli-instead-of-the-notebook).

---

## 2. Choosing the filings

`QUARTERS` and `PERIODS` are both optional and they **INTERSECT**. Each one **raises** when it
matches nothing, because a filter that matches nothing is a run that parses nothing and reports
success.

| you want | write |
|---|---|
| one quarter | `QUARTERS = ["2014-Q3"]` — or `["2014-03"]`, the same quarter |
| a batch, in any order | `QUARTERS = ["2013-Q4", "2014-Q1"]` |
| the repo-native form | `PERIODS = ["Q3-2014"]`, `QUARTERS = None` |
| one quarter, stated twice | `QUARTERS = ["2014-Q3"]` **and** `PERIODS = ["Q3-2014"]` → Q3-2014 |
| **everything** ⚠️ | `QUARTERS = []` — 70+ documents, hours of GPU |

⚠️ **AN EMPTY LIST MEANS EVERY QUARTER, NEVER NONE.** `[]` and `None` build the identical job.
That is `plan()`'s contract, and it is why the default is the *absence* of a filter rather than
a list anything recomputes.

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
| `deepdoc_det.onnx`, `vgg_seq2seq.pth` **and `vietocr_vgg_seq2seq.yml`** | otherwise the engine downloads them from HuggingFace and vocr.vn. ⚠️ **The third one is the recogniser's CONFIG, and it was the one nobody counted**: vietocr re-fetches it on every `Predictor` build and caches nothing, so when vocr.vn's certificate expired every `onnx@*` layer RAISED (`VCR-1`). The three names live once, in `pdf_ocr_job.MODEL_FILES`, and the export raises rather than shipping fewer |

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
| the CSVs themselves | the notebook's **9 · Did it land?** reads `bs_/is_/cf_<EXCHANGE>_<SYMBOL>.csv` and prints `pdf`/`missing`/`cafef` per report, and how many rows came from this run |

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
 42.5% - step 4/6 HOSE_TCB 2013-Q3 - wait kernel - [ 1.5 min] RUNNING  25% of last  <- the control
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
| `balance_sheet` / `income_statement` / `cash_flow` | the FURTHEST-BACK quarter that statement has been read from without a break, anchored at its own newest quarter READ |

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
income statement with no Q1..Q(q-1) to subtract is refused by the merge by design (CTG has 32
such quarters).

⚠️ **THE PDF INDEX IS NOT IN GIT** — `raw_data/` is ignored except `financials/` — so a fresh
checkout cannot prove which quarters were filed. Those tickers read `complete = False` with a
warning rather than a guess (CLAUDE.md §5 rule 2).

---

## 9. Where the rest is written down

| | |
|---|---|
| the parse itself — the cascade, the gates, the 47 layers | [`src/web_scraper/CONTEXT.md`](../../web_scraper/CONTEXT.md) §3a, §3b |
| `kgpu` in general — payload modes, the measured traps | [`README.md`](../README.md) |
| what the OCR has produced, ticker by ticker | `CLAUDE.md` §6-2-octies … §6-2-untricies |
| open defects — read `CRP-1` before quoting any non-bank figure | [`docs/ISSUES.md`](../../../docs/ISSUES.md) |
| what to run next | [`docs/TODO.md`](../../../docs/TODO.md) — `P38`, `P6`, `P5` |
