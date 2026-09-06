# Workflow — start a session

> **Goal:** open a new Claude tab, and stop. **Cost: 0.3 s, one command.**
>
> ⚠️ **THE TAB IS THE WHOLE DELIVERABLE.** No parameters, no orientation, no `O1`, no freshness
> check — what the new session works on is decided *in the new session*. The tab comes up with
> Remote Control on, so the job can be driven from a phone.
>
> ⚠️ **This workflow takes NO arguments** — `/wf-start-a-session` and nothing after it — and §1b is
> why that is a MEASUREMENT and not a preference.

---

## 1. Open the tab — row `O8`

```powershell
python .claude/tools/open_claude_tab.py
```

Run from the **repo root** — it is not a package under `src\` and imports nothing from there.

It checks `remoteControlAtStartup`, the keybinding and the foreground window, then fires the key
bound to `claude-vscode.editor.open`, and **exits 1 at the first thing that is false with the fix
beside it**. Details, and the routes that do NOT work, are [`../runbook/RUNBOOK.md`](../runbook/RUNBOOK.md) §2 `O8`.

**If it exits non-zero, hand the user one line and stop:** press **`Ctrl+Alt+C`**.

### 1b. ⚠️ Why there are no parameters — measured 2026-09-06, do not re-derive

`--session-name`, `--model` and `--effort` were asked for, built, and **withdrawn after every
delivery route was measured dead.** A synthetic keystroke reaches VS Code's **keybinding
dispatcher** and very little else:

| attempted | result |
|---|---|
| `/model sonnet` + Enter into the new tab | model stayed `claude-opus-5` — on a fresh tab AND on one open for minutes, with and without `Escape` to dismiss the slash menu |
| the command palette from a Claude tab | never opened — the rename changed no title, and a control (*Preferences: Open Settings (UI)*) never opened Settings |
| `claude-vscode.renameSessionTab` fired by a bound key | title unchanged, with a transcript on disk and without |
| plain text + Enter, after firing `claude-vscode.focus` | ✅ **the one thing that works** — `ping` became a real message |

⚠️ **AND ONE FALSE POSITIVE ALMOST CLOSED IT AS A SUCCESS.** `effortLevel` read back as `xhigh`
from the new session's transcript, which looked like proof that `/effort xhigh` had landed — until
a session that was **never typed into** read back `xhigh` too. `"ultracode": true` sets it.
That is `CLAUDE.md` §5 rule 21 exactly: **a metric that cannot fail is not a pass.**

`TAB-1` in [`../current_state/ISSUES.md`](../current_state/ISSUES.md) carries this; the runbook has
the routes. ⚠️ **`ctrl+escape` is the Start Menu, not VS Code** — synthesising it opened Windows
Search and launched a browser.

## 2. Stop

⚠️ **This session does nothing else.** Two sessions on one working tree is how a `git status` in
one stops describing what the other is half-way through writing.

---

## Done when

- [ ] a new Claude tab is open — by `O8`, or by the user pressing `Ctrl+Alt+C` after it refused
- [ ] this session ran nothing else and stopped
