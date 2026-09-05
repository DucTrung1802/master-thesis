# `.claude/workflows/` — the step-by-step guides

> **One file per JOB.** A job is a thing you set out to do — *run the chain*, *refresh the data*,
> *OCR a ticker*, *quote a number*, *commit* — and each file here is the ordered list of steps that
> finishes it, including the steps that are easy to skip and expensive to have skipped.
>
> ⚠️ **A workflow is the ORDER; the runbook is the COMMANDS.** Every step that runs something cites
> a row ID from [../runbook/RUNBOOK.md](../runbook/RUNBOOK.md) (`C5`, `O3`, `K4`, …) rather than
> repeating the command, so a flag changes in exactly one place. If a step here disagrees with
> that table, the table wins; if the table disagrees with
> [docs/RUNBOOK.md](../../docs/RUNBOOK.md), that file wins.
>
> **These are lazily loaded.** Nothing in this folder is auto-loaded into a session — only
> `CLAUDE.md` and what it imports (`docs/INDEX.md`, `.claude/rules/common.md`) are. **Open the one
> file for the job in front of you.**

---

## The workflows

| open this | when the job is… | first step |
|---|---|---|
| [start-a-session.md](start-a-session.md) | *"I have just opened this repo and I do not know what state it is in"* | `O1` + `O3` — two commands, ~6 s, before reading anything |
| [run-the-chain.md](run-the-chain.md) | *"train a model on a target and score it honestly"* — stages 1→9 | `O1`, and read its `why` column |
| [run-a-selection.md](run-a-selection.md) | *"which channels carry signal?"* — stage 2/4, local or on a T4 | decide the ROOT before the draws |
| [refresh-the-data.md](refresh-the-data.md) | *"the corpus is stale"* — scrape → carry up → verify | `O3`, to see whether it actually is |
| [ocr-a-ticker.md](ocr-a-ticker.md) | *"OCR ticker `<SYM>` LOCAL\|KAGGLE"* — the standing request shape | clone the control notebook, then **wait** |
| [quote-a-number.md](quote-a-number.md) | *"is this number safe to put in a document?"* | `O1` — a green run on a stale table is a number about a table that no longer exists |
| [record-a-finding.md](record-a-finding.md) | *"I measured something / I found a defect / I finished an item"* | decide which of the four registers owns it |
| [finish-and-commit.md](finish-and-commit.md) | *"the work is done"* | `O5` — `python docs/state_check.py` |

---

## How to use one

1. **Read the whole file before step 1.** Several steps exist to be done *before* the expensive
   one, and reading them afterwards is how a six-hour run gets thrown away.
2. **Do not skip a step because it writes nothing.** The read-only steps are the ones that catch
   the failure this repo keeps producing: a command that goes green having done nothing
   (`CLAUDE.md` §5 rules 10, 11, 14).
3. **Stop at the STOP markers.** Two workflows deliberately end in waiting rather than in an
   action — that is the deliverable, not an unfinished step.

## How to add one

1. **Only after you have done the job at least once.** A workflow written from the code rather
   than from a run is a guess, and this folder's value is that its steps were walked.
2. **Cite runbook IDs, never copy commands.** A duplicated flag drifts; a cited one cannot.
3. **Give it a "done when" section.** A job with no completion test is a job that gets
   half-finished twice.
4. **Add its row to the table above, and a row to [docs/INDEX.md](../../docs/INDEX.md)** —
   `python docs/check_index.py` fails on an unrouted `.md`, and an unrouted file is a file no
   session knows exists.
5. **English, per [../rules/common.md](../rules/common.md) R1.** The file is the artefact; the
   conversation about it stays Vietnamese.
