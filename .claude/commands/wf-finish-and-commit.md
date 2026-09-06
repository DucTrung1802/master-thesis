---
description: Leave the repo in a state the next session can trust, then commit.
argument-hint: [commit subject]
---

Run the workflow in [`../workflows/finish-and-commit.md`](../workflows/finish-and-commit.md), start to finish.

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

⚠️ **`O5` first — `python ../tools/state_check.py` — and resolve every row it reports.** It
REPORTS and never rewrites. Ask me before the `git commit`, and never push unless I say so.

Arguments (may be empty — resolve what is missing from the workflow, or ask me): $ARGUMENTS
