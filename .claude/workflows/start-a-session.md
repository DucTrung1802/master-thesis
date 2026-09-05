# Workflow — start a session

> **Goal:** know what state this repo is in, and what you are allowed to believe about it, before
> touching anything. **Cost: about ten minutes, of which ~40 seconds is commands.**
>
> ⚠️ **The expensive mistake this prevents is reading the corpus.** It is **106 `.md` files,
> ~639k tokens, about 3× a context window** — bulk-loading it is not slow, it is impossible.
> `CLAUDE.md` and `docs/INDEX.md` are already in context; **everything else is opened one file at
> a time, when you touch that thing.**

---

## 1. Read what is already loaded — 0 s

`CLAUDE.md` (44.2k), `docs/INDEX.md` and `.claude/rules/common.md` load themselves. Two sections
carry the answer to most questions:

- **§2 — THE VERDICT.** Single-stock short-horizon prediction has failed **five** independent
  times here. If the job in front of you is "try a model on one stock at h=5", §2 has already
  answered it.
- **§6 — State today.** ⚠️ **Its own header says that when it disagrees with the database, the
  database is right and the section is the bug.** It has been seven days stale once.

## 2. Ask the machine, not the file — ~40 s

| step | runbook ID | what you are testing |
|---|---|---|
| 2a | **O1** `python -m pipeline` | is any stage of the chain stale? Read the `why` column, not the colour |
| 2b | **O3** `python -m pipeline.freshness --layer silver` | is the DATA fresh, and for **how many tickers**? |

⚠️ **2b is not optional and `MAX(date)` is not a substitute.** It once read 2026-08-19 from **five
tickers** while 757 of 781 were frozen — a 24-name cross-section looks like a working pipeline to
anything reading one number.

**Read the shape of what O3 returns:**

| shape | means |
|---|---|
| many tickers stopping on ONE date (a **cliff**) | a scrape scope — 599 of 781 on one date was 77 % |
| **scattered** end dates, largest group tiny | delistings and suspensions — 5 of 784 is 0.6 %, and correct |

## 3. Find out what is broken before you trust a number

`docs/ISSUES.md` is **42.2k and you do not open it whole.** Seven codes change how a number may be
*read*, and `CLAUDE.md` §6's closing table lists them: `NUL-1`, `NUL-3`, `RPR-1`, `OUT-1`, `CFB-1`,
`TPL-1`/`CRP-1`, `FLT-1`/`SHP-1`. **Open the file only for the code that touches your job.**

## 4. Route to the ONE file you need

`docs/INDEX.md` is the map and it carries a measured token cost per row. Budget against it.

| your job | open | cost |
|---|---|---|
| a Dagster asset, a table, a scrape, the filter layer | `src/orchestration/CONTEXT.md` | 47.5k |
| a scraper or the PDF/OCR parser | `src/web_scraper/CONTEXT.md` | 61.9k |
| a selection, an IC, a null, a bar | `src/feature_selection/CONTEXT.md` | 45.0k |
| whether a result survives more than one split | `src/walkforward/CONTEXT.md` | 16.0k |
| a specific `§6-2-*` citation about the filing parser | `docs/OCR_PARSER_LOG.md` — ⚠️ **123.6k, open it for ONE section** | 123.6k |
| what a module's code actually contains | `.claude/module_descriptions/<module>.md` | ~3k |

⚠️ **A `CONTEXT.md` answers *"what did we measure and what did it prove"*; a
`module_descriptions/` file answers *"what is in this folder and what will bite me"*.** They are
different questions — pick the one you are asking.

## 5. Check the calendar on what you just read

- **A number without a date cannot be told from a stale one.** Dates are on findings by convention
  here; if one is missing, treat the number as unverified.
- ⚠️ **A `P<n>` in `docs/TODO.md` written before 2026-08-23 means a DIFFERENT item** — three
  renumbers preceded the freeze. Take the date of what you are reading, then TODO's crosswalks.
- ⚠️ **A HYPHENATED code (`P1-9`, `PRF-8`, `M-3`) is RETIRED.** A bare `P<n>` is live.

---

## Done when

- [ ] **O1** shows a stage list you understand, including which rows are `MANUAL` by design
- [ ] **O3** shows a freshness shape you can name — cliff or scatter
- [ ] you know which single `CONTEXT.md` (if any) your job needs, and have opened at most one
- [ ] you know which `ISSUES.md` codes constrain the numbers you are about to touch

## Traps

⚠️ **"I will just skim a few CONTEXT files to get oriented."** That is 150k+ tokens for context
the hub already summarises. The hub exists precisely so this is unnecessary.

⚠️ **Trusting §6 over the database.** The section says so itself. When they disagree, measure and
then fix the section — see [record-a-finding.md](record-a-finding.md).

⚠️ **Treating a memory as current.** Recalled memories reflect what was true when written. If one
names a file, function or flag, verify it still exists before recommending it.
