# `.claude/workflows/` — the step-by-step guides

> **One file per JOB.** A job is a thing you set out to do — *run the chain*, *refresh the data*,
> *OCR a ticker*, *quote a number*, *commit* — and each file here is the ordered list of steps that
> finishes it, including the steps that are easy to skip and expensive to have skipped.
>
> ⚠️ **A workflow is the ORDER; the runbook is the COMMANDS.** Every step that runs something cites
> a row ID from [../runbook/RUNBOOK.md](../runbook/RUNBOOK.md) (`C5`, `O3`, `K4`, …) rather than
> repeating the command, so a flag changes in exactly one place. If a step here disagrees with
> that table, the table wins.
>
> **These are lazily loaded.** Nothing in this folder is auto-loaded into a session — only
> `CLAUDE.md` and what it imports (`../current_state/INDEX.md`, `.claude/rules/common.md`) are. **Open the one
> file for the job in front of you.**
>
> ⚠️ **Each job also has a slash command — `/wf-<the file's name>`, plus `/wf-list` for the
> menu** (added 2026-09-06, [`../commands/`](../commands/wf-list.md)). The command READS the
> workflow and executes it; it copies nothing, exactly as a workflow copies no command. **A
> new workflow needs a launcher beside it or it is a job nobody can start by name.**

---

## The workflows

| open this | run it with | when the job is… | first step |
|---|---|---|---|
| [start-a-session.md](start-a-session.md) | `/wf-start-a-session` | *"open a new Claude tab and work in that one"* | `O8` — one command, then ⛔ stop. **No arguments, nothing else** — §1b is the measurement behind that |
| [run-the-chain.md](run-the-chain.md) | `/wf-run-the-chain` | *"train a model on a target and score it honestly"* — stages 1→9 | `O1`, and read its `why` column |
| [run-a-selection.md](run-a-selection.md) | `/wf-run-a-selection` | *"which channels carry signal?"* — stage 2/4, local or on a T4 | decide the ROOT before the draws |
| [refresh-the-data.md](refresh-the-data.md) | `/wf-refresh-the-data` | *"the corpus is stale"* — scrape → carry up → verify | `O3`, to see whether it actually is |
| [ocr-a-ticker.md](ocr-a-ticker.md) | `/wf-ocr-a-ticker` | *"OCR ticker `<SYM>` LOCAL\|KAGGLE"* — the standing request shape | clone the control notebook, then **wait** |
| [summarize-ocr.md](summarize-ocr.md) | `/wf-summarize-ocr` | *"which tickers are still worth OCRing?"* — top X of 784, most liquid first | `F4` — one `Get-Content`, and ⚠️ **open nothing else** |
| [quote-a-number.md](quote-a-number.md) | `/wf-quote-a-number` | *"is this number safe to put in a document?"* | `O1` — a green run on a stale table is a number about a table that no longer exists |
| [record-a-finding.md](record-a-finding.md) | `/wf-record-a-finding` | *"I measured something / I found a defect / I finished an item"* | decide which of the four registers owns it |
| [finish-and-commit.md](finish-and-commit.md) | `/wf-finish-and-commit` | *"the work is done"* | `O5` — `python ../tools/state_check.py` |

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
4. **Give it a launcher.** `../commands/wf-<name>.md`, cloned from any existing one — the body
   names the workflow and nothing else. ⚠️ **A workflow with no launcher is reachable only by
   someone who already knows this folder exists.**
5. **Add its row to the table above, and a row to [../current_state/INDEX.md](../current_state/INDEX.md)** —
   `python ../tools/check_index.py` fails on an unrouted `.md`, and an unrouted file is a file no
   session knows exists.
6. **English, per [../rules/common.md](../rules/common.md) R1.** The file is the artefact; the
   conversation about it stays Vietnamese.
