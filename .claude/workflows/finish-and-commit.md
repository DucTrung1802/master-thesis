# Workflow — finish and commit

> **Goal:** leave the repo in a state where the next session can trust it. **Cost: ~2 minutes plus
> whatever the state check reports.**
>
> ⚠️ **NOTHING ENFORCES THIS AT COMMIT TIME, AND THAT IS A CHOICE.** A git hook that blocks an
> unrelated commit costs more than the drift it prevents, and `--no-verify` would train the reflex
> to bypass it. **So running the check is the discipline.**

---

## 1. Record the state — **O5**

```powershell
python docs/state_check.py      # about two seconds, exits 1 on drift
```

⚠️ **It REPORTS and never rewrites.** Every finding is handed back as a decision, because these
numbers cannot be derived mechanically without getting them wrong — `ISSUES.md` keeps FIXED rows
inside its Open table on purpose, so a naive row-counter disagrees with the headings.

| the check | what a failure means |
|---|---|
| **`CLAUDE.md` §6 date** | the heading is older than the `.md` files in this commit. If the commit changes what the project KNOWS, bump the date and write the measurement in; if it is a typo fix, ignore the row |
| **`CONTEXT.md` ↔ `CLAUDE.md`** | a package `CONTEXT.md` changed and the hub did not. ⚠️ **A measurement that never reaches the hub is invisible.** *"The detail stays local"* is valid — but decide it |
| **issue counts** | the hub's *"N open, M resolved"* disagrees with `ISSUES.md`'s headings. **Re-SCAN; do not decrement** |
| **`INDEX.md` completeness** | a `.md` exists that the index does not route. **A file missing from the index is a file no session knows exists** |
| **`INDEX.md` token costs** | a claimed cost drifted >20 % from measured. ⚠️ This check exists because **all 16** of the hub's costs had gone stale at once |
| **relative links** | a markdown link points at nothing. ⚠️ **Read the COUNT, not just the colour** — `docs/RUNBOOK.md` §8c records 11 as known-broken from the docs move; measured 2026-09-06 the check reports **304 checked, all resolve**, so that backlog is gone and a new failure is genuinely new |

## 2. Put each change where it is read

| you changed | it goes in |
|---|---|
| a new measurement, or one that moves a verdict | `CLAUDE.md` §6 (+ bump the date), or the package's `CONTEXT.md` |
| a new defect | `docs/ISSUES.md`, with a **permanent** code |
| a finished backlog item | its number moves to `CLAUDE.md` / `CONTEXT.md`; the item is **deleted from `docs/TODO.md`, not ticked** |
| a new `.md` file | a row in `docs/INDEX.md` |
| a new command, flag or stage | `docs/RUNBOOK.md` **and** [../runbook/RUNBOOK.md](../runbook/RUNBOOK.md) |
| a new rules file under `.claude/rules/` | ⚠️ **its `@` import in `CLAUDE.md`, in the SAME commit** — a rules file is not auto-loaded by itself, and without the import it is a file nobody reads |

[record-a-finding.md](record-a-finding.md) is the detail for each row.

## 3. Sanity-check the code, if you changed any

| you touched | run |
|---|---|
| `filters.py` | `python -m pytest src/orchestration/preprocessor/test_filters.py -q` (30 tests, no database) |
| any Dagster asset | `dagster definitions validate -f src/orchestration/definitions.py` |
| `feature_selection.contract` | `python -m pytest src/feature_selection/tests/test_contract.py -q` (23 tests) |
| a DTO in `src/dtos/` | ⚠️ **there are no tests over that package.** Cheapest check: `python -m pipeline.freshness --layer silver` plus one `filter/universe` materialisation |
| anything a Kaggle worker ships | **K3** — `kgpu rehearse`. It is the only thing that catches an import the worker cannot satisfy |

## 4. Commit

- **Branch first if you are on the default branch.**
- **Commit or push only when asked.**
- End the message with:

```
Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
```

- **English**, per [../rules/common.md](../rules/common.md) R1 — commit messages are files.
- If you converted a file to English while editing it for another reason, **say so in the message**.

## 5. Check what will NOT be committed

⚠️ **`src/model/runs/*/` and `src/train_test_set/` are gitignored** (only `index.csv` is tracked),
so a fresh checkout has run folders stripped to `results/`. `raw_data/` is ignored except
`raw_data/cafef/financials/`. `reports/feature_selection/` **is** tracked.

⚠️ **`RPR-1` is what this costs when it goes wrong**: 29 run folders were deleted and are
unrecoverable — two sections of the hub are now citations without their evidence. **If a run
folder carries a number you intend to quote, copy the number OUT of it before it can vanish.**

⚠️ **A new report root needs its `.gitignore` negation pair in the same commit**, or its CSVs are
silently dropped.

---

## Done when

- [ ] **O5** reports nothing unresolved, or you can say why each remaining row is fine
- [ ] every register that should have changed, did
- [ ] the tests for what you touched pass, and you have said so plainly — including if they do not
- [ ] no number you quoted lives only inside a gitignored folder

## Traps

⚠️ **A commit that changes what the project KNOWS must also change where that knowledge is read.**
Code and register move together or the register is a lie.

⚠️ **Do not strip a `⚠️`.** It marks a claim that cost something to learn. Add one when you
measure a new one.

⚠️ **Encoding, and it has killed edits three times in one session**: start any one-off edit script
with `sys.stdout.reconfigure(encoding="utf-8")`, or print counts rather than content. A script
that `print`s the text it is editing dies **halfway through**, after some replacements and before
the file is written — **so the edit is silently partial.**
