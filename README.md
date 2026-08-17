# master-thesis

Predicting Vietnamese stock prices — a data pipeline (scrape → PostgreSQL medallion
layers → feature pools → LSTM → scored run) and the research record of what it has been
able to prove.

**Start here: [CLAUDE.md](CLAUDE.md)** — the whole project in one file: the verdict, the
pipeline end to end, the standing rules, the current state, and a routing table to the
twelve per-package `CONTEXT.md` files that hold the detail.

## The four root registers — one job each, no overlap

| file | answers |
|---|---|
| **[CLAUDE.md](CLAUDE.md)** | *what is this, and what has it PROVED?* |
| **[RUNBOOK.md](RUNBOOK.md)** | *how do I RUN it?* |
| **[ISSUES.md](ISSUES.md)** | *what is BROKEN?* |
| **[TODO.md](TODO.md)** | *what is NEXT?* |

Deliverable write-ups live in `THESIS_PROGRESS_2026.md` (EN),
`THESIS_PROGRESS_2026_VI.md` and `THESIS_SUMMARY_2026_VI.md` (VI).

⚠️ **The headline result is negative and deliberately so.** Single-stock short-horizon
prediction does not work on this data, established four independent times; CLAUDE.md §2
is the verdict and §2b the one thing that survived its own null.
