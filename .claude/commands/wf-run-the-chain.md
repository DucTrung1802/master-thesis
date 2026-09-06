---
description: Run the model chain end to end — stages 1 to 9, from pools to a scored, costed run.
argument-hint: [target] [universe] [horizon]
---

Run the workflow in [`../workflows/run-the-chain.md`](../workflows/run-the-chain.md), start to finish.

1. **Read that file in full before step 1.** Several of its steps exist to be done *before*
   the expensive one, and reading them afterwards is how a long run gets thrown away.
2. **Follow its steps in order.** Every step that runs something cites a row ID from
   [`../runbook/RUNBOOK.md`](../runbook/RUNBOOK.md) — open that row for the command, its
   measured cost and its `before → after` steps. **If the workflow disagrees with the
   table, the table wins.**
3. **Do not skip a step because it writes nothing.** The read-only steps are the ones that
   catch this repo's recurring failure: a command that goes green having done nothing
   (`CLAUDE.md` §5 rules 10, 11, 14).
4. **Stop at every STOP / ⛔ marker.** Where a workflow ends in waiting, the wait is the
   deliverable, not an unfinished step.
5. **Ask me before any step that is expensive or hard to reverse** — a scrape, a training
   run, a `--replace`, a `git commit`, a push. Read-only steps run without asking.
6. Finish on the workflow's **"Done when"** section and report which items are met and
   which are not. Read its **Traps** section before you report.

⚠️ **Step 0 is in writing and is not optional**: decide the experiment, the `--root` and the
`--scope` before stage 1. `final_features` groups on `(schema, target, setup)`, a key with no
term for which pools — `--root` + `--scope` are the only things keeping two experiments off one
table name.

Arguments (may be empty — resolve what is missing from the workflow, or ask me): $ARGUMENTS
