---
description: Move fresh prices from CafeF up to something a model reads, and PROVE it arrived.
argument-hint: [layer or ticker scope]
---

Run the workflow in [`../workflows/refresh-the-data.md`](../workflows/refresh-the-data.md), start to finish.

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

⚠️ **This job is hours, not minutes** — D1 alone measured 1h 05m over 780 tickers and D2's
`gold.stocks_ta` 40 min. Confirm with me before D1, and again before D2.

Arguments (may be empty — resolve what is missing from the workflow, or ask me): $ARGUMENTS
