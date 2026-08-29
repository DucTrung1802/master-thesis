# Running the filing OCR — the guide

> **Two notebooks, and you only ever open one.**
>
> | | |
> |---|---|
> | **[`src/kaggle_gpu/RUN__pdf_ocr_control.ipynb`](../RUN__pdf_ocr_control.ipynb)** | the one you open. **One parameter cell**, and `ENVIRONMENT` decides where the OCR happens: `"LOCAL"` parses on this machine, `"KAGGLE"` builds the job, ships the filings, starts the kernel, waits and pulls the run folder back |
> | `src/web_scraper/RUN__pdf_ocr.ipynb` | runs **on Kaggle**. `kgpu` patches its parameter cell and uploads it — you do not open or edit it |
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

Edit cell 1, run everything. That is the whole procedure, and `ENVIRONMENT` is the only thing
that decides which machine does the OCR.

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
cumulative income statement a one-document run cannot de-cumulate.

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
The notebook's last two cells read it; `metadata.json` carries the whole scorecard in
`results`, one row per `(period, report)`.

| verdict | what it means |
|---|---|
| `REPRODUCED` | every cell, the winning **layer**, the unit and `publish_date` match disk |
| `DIFFERS` | one of those moved — the run names which, with both figures |
| `absent in this run` | the cascade refused the statement; the log says why |
| `no pdf row on disk to compare against` | ⚠️ **a RECOVERY, not a reproduction** — nothing scored it |
| *(refused)* | a cumulative income statement is not scored against a de-cumulated row; that would report every cell as changed |

⚠️ **`compare()` READS EVERY COLUMN, NOT THE FIGURES.** Two runs have been caught losing a
single `publish_date` and nothing else — a figures-only diff called both of them clean.

### When a statement is `absent`, read the FIRST refusal

`_parse_cascaded` prints the distinct reasons with the first layer that gave each. ⚠️ **A
cascade's FINAL refusal names the hardest path tried, not the blocking defect.** The label
`fx not mapped` sent this repo down a wrong diagnosis for two days that way, and six of the
seven quarters it was blamed for then parsed at a **strict** layer with no FX change at all
(CLAUDE.md §6-2-duovicies).

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
`kgpu run` merges as soon as the folder lands. To do it by hand, or to look first:

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
| a **cumulative income statement** | an annual or half-year filing prints the year to date; the CSV column holds the standalone quarter, and this job cannot de-cumulate — a one-document run has no Q1..Q(q-1). Writing it puts a 9-month total in a 3-month column | `force_cumulative` |
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

## 8. Where the rest is written down

| | |
|---|---|
| the parse itself — the cascade, the gates, the 47 layers | [`src/web_scraper/CONTEXT.md`](../../web_scraper/CONTEXT.md) §3a, §3b |
| `kgpu` in general — payload modes, the measured traps | [`README.md`](../README.md) |
| what the OCR has produced, ticker by ticker | `CLAUDE.md` §6-2-octies … §6-2-untricies |
| open defects — read `CRP-1` before quoting any non-bank figure | [`docs/ISSUES.md`](../../../docs/ISSUES.md) |
| what to run next | [`docs/TODO.md`](../../../docs/TODO.md) — `P38`, `P6`, `P5` |
