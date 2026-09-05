# `.claude/current_state/` — measured snapshots of where the project stands

> **One file per subject, each a SNAPSHOT: what was true, when it was measured, and by which
> command.** *"How many tickers are fresh"*, *"which tables exist"*, *"what the parser has
> parsed"* — facts that a command can re-derive in seconds and that go stale on their own.
>
> ⚠️ **THIS IS NOT A FIFTH REGISTER.** The four are `CLAUDE.md` (*what is PROVED*),
> `docs/RUNBOOK.md` (*how to RUN*), `docs/ISSUES.md` (*what is BROKEN*), `docs/TODO.md` (*what is
> NEXT*), and this folder must not compete with any of them. The distinction that keeps it honest:
>
> | | holds | goes stale by |
> |---|---|---|
> | **`CLAUDE.md` §6** | the **narrative** — what the state MEANS, and what it changes about a verdict | someone learning something |
> | **`.claude/current_state/`** | the **readout** — the numbers a command printed | the world moving on its own |
>
> A finding that changes a verdict belongs in `CLAUDE.md`, always. A number you re-measure every
> few days belongs here.
>
> ⚠️ **AND WHEN A FILE HERE DISAGREES WITH THE DATABASE, THE DATABASE IS RIGHT.** That is the same
> rule `CLAUDE.md` §6 states about itself, and §6 has been seven days stale once. **A snapshot is
> evidence of what a command said on a date — never of what is true now.**
>
> **The folder is empty until someone measures something.** That is the correct state, not a gap
> to fill: writing a number here that nobody measured is the one failure this folder can produce.

---

## The contract for a file here

**Every file opens with a provenance block, before any content:**

```markdown
# <subject> — snapshot

> **Measured 2026-09-06** by `python -m pipeline.freshness --layer silver`.
> Re-derive with that command; it takes ~1 s and writes nothing.
> ⚠️ If this disagrees with the database, the database is right and this file is stale.
```

Then the numbers, as a table.

| rule | why |
|---|---|
| **Name the COMMAND that produced it** | a snapshot nobody can re-derive is a rumour. Cite a [../runbook/RUNBOOK.md](../runbook/RUNBOOK.md) row ID where one exists (`O3`, `D6`, `F3`) |
| **Date it** | ⚠️ a number without a date cannot be told from a stale one |
| **Record what was MEASURED, not what was concluded** | the conclusion belongs in `CLAUDE.md`; keep this reproducible |
| **A DISTRIBUTION, never a scalar, for anything per-ticker** | ⚠️ `MAX(date)` once read fresh from **five** tickers while 757 of 781 were frozen. A scalar cannot see a frozen source |
| **Say what was NOT measured** | ⚠️ §5 rule 2 — an absent measurement is absent, never inferred. A condition everything cleared and a condition nothing was measured for look identical |
| **English** | [../rules/common.md](../rules/common.md) R1. Vietnamese ticker names, exchange labels and account labels are DATA and stay verbatim |
| **One subject per file** | so a stale one can be deleted without taking a fresh one with it |
| **Delete a snapshot you no longer re-measure** | ⚠️ a stale snapshot is worse than none — it is what the next session budgets against |

## Subjects worth a file, once measured

None of these exist yet. Each names the command that would produce it — **do not write the file
until you have run the command.**

| file | subject | produced by |
|---|---|---|
| `data-freshness.md` | how many tickers are current per layer, and the SHAPE of the stragglers (cliff vs scatter) | **O3** / **O4** |
| `chain-artefacts.md` | which `__final__` tables, datasets and run folders exist, per universe | **O1**, plus `information_schema` |
| `parser-coverage.md` | filing cells parsed vs filed per ticker, and how many are `missing` **because the company never filed** | **F3** |
| `fundamental-sources.md` | the `source` breakdown — how many rows are `pdf`, how many are not | **D6** |
| `unified-schemas.md` | which schemas exist, which pools each holds, and which are `pools_behind` | `pipeline.status_data` |

⚠️ **`parser-coverage.md` needs two columns, not one.** `complete` is CONTINUITY from the start of
the filing chain and **coverage is the cell count**; they disagree — one ticker reads ✅ at 210/210
and another ✅ at 40/51, while a third reads ❌ at 210/213. ⚠️ **And a missing count is not work
available**: of one measurement's 130 missing cells, **66 were quarters the company never filed**.

## Writing one

1. **Run the command.** Not a similar one — the one you will cite.
2. **Paste the readout as a table**, with the provenance block above it.
3. **Add the caveat that makes it readable** — the one thing a reader would otherwise
   over-conclude from it.
4. **Add a row to [docs/INDEX.md](../../docs/INDEX.md)**; `python docs/check_index.py` fails
   without one (**O6**).
5. **If it changed a verdict, it also belongs in `CLAUDE.md`** — see
   [../workflows/record-a-finding.md](../workflows/record-a-finding.md).

## Re-measuring one

Overwrite it and move the date. ⚠️ **Do not keep a history here** — that is what `git log` is for,
and a file holding five dated snapshots is a file where a reader picks the wrong one.
