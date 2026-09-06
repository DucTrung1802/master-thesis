---
description: List the workflow commands — what each job is, and which command runs it. Runs nothing.
---

Read [`../workflows/README.md`](../workflows/README.md) and render its table for me, adding the
slash command that launches each workflow:

| command | the job | first step |
|---|---|---|
| `/wf-start-a-session` | I just opened this repo and do not know what state it is in | `O1` + `O3` |
| `/wf-run-the-chain` | train a model on a target and score it honestly — stages 1→9 | `O1` |
| `/wf-run-a-selection` | which channels carry signal? — stage 2/4 | decide the ROOT |
| `/wf-refresh-the-data` | the corpus is stale — scrape → carry up → verify | `O3` |
| `/wf-ocr-a-ticker` | OCR ticker `<SYM>` LOCAL\|KAGGLE | clone, then **wait** |
| `/wf-quote-a-number` | is this number safe to put in a document? | `O1` |
| `/wf-record-a-finding` | I measured something / found a defect / finished an item | pick the register |
| `/wf-finish-and-commit` | the work is done | `O5` |

⛔ **This command runs nothing.** It is the menu. Reply with the table and stop.
