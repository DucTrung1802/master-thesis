---
description: Decide whether a number is safe to put in a document, a thesis chapter or a commit message.
argument-hint: <the number or claim>
---

Run the workflow in [`../workflows/quote-a-number.md`](../workflows/quote-a-number.md), start to finish.

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

⚠️ **Say what the number does NOT establish** — that section is the point of the workflow, not
its epilogue. Open `NUL-1`, `NUL-3`, `RPR-1` and `OUT-1` before quoting anything.

Arguments (may be empty — resolve what is missing from the workflow, or ask me): $ARGUMENTS
