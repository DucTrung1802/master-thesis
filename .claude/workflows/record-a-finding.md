# Workflow — record a finding

> **Goal:** put a new measurement, defect or finished item where the next session will actually
> read it. **Cost: minutes.**
>
> ⚠️ **A measurement that never reaches the hub is invisible.** Every session loads `CLAUDE.md`;
> almost none open a given `CONTEXT.md`. *"The detail stays local"* is a valid answer — but it is
> a **decision**, and this workflow is where it gets made rather than defaulted into.

---

## 1. Decide which register owns it — the four have one job each, no overlap

| register | answers | your finding belongs here if… |
|---|---|---|
| **`CLAUDE.md`** | *what is this, and what has it PROVED?* | it changes what the project KNOWS, or moves a verdict |
| [../runbook/RUNBOOK.md](../runbook/RUNBOOK.md) | *how do I RUN it?* | it is a command, a flag, a stage, or a measured runtime |
| **`../current_state/ISSUES.md`** | *what is BROKEN?* | it is a defect — and it gets a **permanent code** |
| **`../current_state/TODO.md`** | *what is NEXT?* | it is work not yet done |

**Movement between them is one-way and worth knowing:**

```
TODO item that turns out to be a defect  →  graduates to ISSUES.md with a code
ISSUES entry that gets fixed             →  keeps its row, marked ✅ FIXED <date> in WORDS
TODO item that gets done                 →  leaves its measurement in CLAUDE.md or a CONTEXT.md,
                                            and is DELETED, not ticked
```

⚠️ **A `P<n>` and an `ISSUES` code are permanent NAMES.** They are never renumbered or reused, so
`TODO.md` starts at `P2` and need not stay monotonic — **priority is the ROW ORDER**. Read the
order, cite the number.

## 2. If it is a MEASUREMENT

- [ ] write **what was measured**, not what was concluded. The tables in these files are still
      trusted months later because they are reproducible checks.
- [ ] give it a **date**. A number without one cannot be told from a stale one.
- [ ] put it where it was made: `CLAUDE.md` §6 for anything that changes the state or a verdict,
      the package's `CONTEXT.md` for detail that stays local — **and decide which, explicitly**.
- [ ] if it changes §6, **bump §6's date in the heading**.
- [ ] add a `⚠️` if it cost something to learn. **Do not strip existing ones.**
- [ ] run it through [quote-a-number.md](quote-a-number.md) first — a finding recorded without its
      null is a finding that gets over-read later.

⚠️ **Record a WRONG PREDICTION too, and leave it in.** One register carries *"I wrote, before
measuring, that it would not beat the naive on magnitude. That was wrong, and the wrong prediction
is left in."* That is the convention working.

## 3. If it is a DEFECT

- [ ] give it a **permanent code** in `../current_state/ISSUES.md` — never reuse or renumber one.
- [ ] say **how it manifests**, not just what is wrong. The most valuable entries here describe a
      failure that **passes every gate** (`SLD-1`, `CFB-1`, `CFV-1`).
- [ ] say **what it does to a NUMBER**, if anything. Seven codes exist mainly because they change
      how a number may be read, and `CLAUDE.md` §6 tabulates them.
- [ ] **measure the blast radius before you fix it**, and write that measurement down. One rebuild
      estimated at *"~11 GB and hours of compute"* took **40 minutes** — the item had sat open for
      a week on a number that was wrong by an order of magnitude.
- [ ] when it is fixed: **mark it `✅ FIXED <date>` in words, in ordinary type.** ⚠️ **No
      strikethrough, anywhere** — 108 markers were removed on 2026-08-23. Struck-out text renders
      as damaged and reads as *"ignore this"*, which is the opposite of what a closed row is for:
      **the measurement it leaves behind is the point**, and rows are cited BY CODE.

## 4. If it is a FINISHED ITEM

- [ ] move its measurement into `CLAUDE.md` or the package's `CONTEXT.md`
- [ ] **DELETE the item from `../current_state/TODO.md`** — do not tick it. Its number stays a permanent name
      wherever it is cited.

## 5. If it is a NEW `.md` FILE

- [ ] add a row to **`../current_state/INDEX.md`**, with a **measured** token cost (`chars/4000`)
- [ ] **O6** — `python ../tools/check_index.py`. ⚠️ **A file missing from the index is a file no
      session knows exists.**
- [ ] ⚠️ **A stale cost is worse than none** — it is what a session budgets against, and it once
      made a file look 1.8× more expensive than it was, in the direction that makes a session
      refuse to open something it could afford.
- [ ] **English**, per [../rules/common.md](../rules/common.md) R1 — the two named exceptions are
      a `*_VI.md` translation and Vietnamese **data**.

## 6. If it is a SNAPSHOT of the current state

It goes in [../current_state/README.md](../current_state/README.md) — see that folder's README for the
contract. ⚠️ **A snapshot is measured and dated, or it is not written.**

## 7. Then commit — [finish-and-commit.md](finish-and-commit.md)

---

## Done when

- [ ] exactly one register owns the finding, and you can say why that one
- [ ] it carries a date, and a `⚠️` if it was expensive to learn
- [ ] **O5** (`python ../tools/state_check.py`) reports nothing you have not resolved
- [ ] nothing was deleted that carried a measurement

## Traps

⚠️ **Do not decrement a count — re-SCAN.** `CLAUDE.md`'s *"N open issues"* is a scan of the tables,
and it has been wrong repeatedly (96, 70, 22 at various points). **A confidently wrong number is
worse than none**, because it is what the next session budgets against.

⚠️ **Do not delete a citation to make a link work.** Three citations in the hub point at a folder
that was removed, and they were **left on purpose**: each is a measurement, and deleting the
citation would delete the record of where it came from. Read them as *a claim whose evidence is
one `git checkout` away*.

⚠️ **A documented feature is not a shipped one.** One section described code that had been written
and run but **never committed**, and stood for three days. **The check that catches it is `grep`.**
