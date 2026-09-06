---
description: Open a new Claude tab with Remote Control on, and stop. Takes no arguments.
---

Run the workflow in [`../workflows/start-a-session.md`](../workflows/start-a-session.md): **one
command, row `O8`, then stop.**

```powershell
python .claude/tools/open_claude_tab.py
```

⛔ **That is the whole job.** No arguments, no orientation, no `O1`, nothing typed into the new
tab. If the command exits non-zero, report what it printed and tell me to press **`Ctrl+Alt+C`**;
either way this session then stops.

⚠️ **AND DO NOT ADD ARGUMENTS.** A session name, a model and an effort level were attempted on
2026-09-06 and **every route was measured dead** — [`../runbook/RUNBOOK.md`](../runbook/RUNBOOK.md)
§2 `O8` has the table, `TAB-1` in [`../current_state/ISSUES.md`](../current_state/ISSUES.md) is the
code. What the new session runs on is decided **in the new session**.
