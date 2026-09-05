# master-thesis

Predicting Vietnamese stock prices — a data pipeline (scrape → PostgreSQL medallion
layers → feature pools → LSTM → scored run) and the research record of what it has been
able to prove.

**Start here: [CLAUDE.md](CLAUDE.md)** — the whole project in one file: the verdict, the
pipeline end to end, the standing rules, the current state, and a routing table to the
twelve per-package `CONTEXT.md` files that hold the detail.

**All prose documentation lives in [`.claude/`](.claude/)**, mapped by
**[.claude/current_state/INDEX.md](.claude/current_state/INDEX.md)** — 127 `.md` files in four tiers with a measured token
cost each. `CLAUDE.md` imports that index (`@.claude/current_state/INDEX.md`), so every Claude Code session
starts holding the map. ⚠️ The corpus is ~511k tokens, ~2.5× a context window, so the index
routes rather than inlines: **open one file, when you touch that thing.**

## The four registers — one job each, no overlap

| file | answers |
|---|---|
| **[CLAUDE.md](CLAUDE.md)** | *what is this, and what has it PROVED?* |
| **[.claude/runbook/RUNBOOK.md](.claude/runbook/RUNBOOK.md)** | *how do I RUN it?* |
| **[.claude/current_state/ISSUES.md](.claude/current_state/ISSUES.md)** | *what is BROKEN?* |
| **[.claude/current_state/TODO.md](.claude/current_state/TODO.md)** | *what is NEXT?* |

Deliverable write-ups live in [`.claude/docs/`](.claude/docs/) — `THESIS_PROGRESS_2026.md`
(EN), `THESIS_PROGRESS_2026_VI.md` and `THESIS_SUMMARY_2026_VI.md` (VI).

⚠️ `CLAUDE.md` and this file stay at the repo root on purpose: Claude Code auto-loads
`CLAUDE.md` from the root only.

⚠️ **The headline result is negative and deliberately so.** Single-stock short-horizon
prediction does not work on this data, established four independent times; CLAUDE.md §2
is the verdict and §2b the one thing that survived its own null.
