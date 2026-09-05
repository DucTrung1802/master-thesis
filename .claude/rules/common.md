# `.claude/rules/common.md` — rules that hold in EVERY session

> **These rules are always in force.** They are auto-loaded into every session because
> [CLAUDE.md](../../CLAUDE.md) imports this file with `@.claude/rules/common.md`, next to
> `@../current_state/INDEX.md`. ⚠️ **A file dropped into `.claude/rules/` is NOT loaded by itself** — Claude
> Code auto-loads `CLAUDE.md` and whatever `CLAUDE.md` imports, and nothing else. **If you add
> another rules file here, add its `@` import to `CLAUDE.md` in the same commit, or it is a file
> nobody reads.**
>
> ⚠️ **This file is in the always-loaded budget**, alongside `CLAUDE.md` (44.2k) and
> `../current_state/INDEX.md` (3.7k). Keep it to rules — a rule that needs a page of evidence belongs in
> `CLAUDE.md` or a package `CONTEXT.md`, with a one-line pointer from here.
>
> **Scope:** how to WORK in this repo, in any session, regardless of the task. What the project
> KNOWS lives in `CLAUDE.md`; what is BROKEN in `../current_state/ISSUES.md`; what is NEXT in `../current_state/TODO.md`.
> Do not restate any of those here.

---

## R1 — Everything written into a file is in English

**Added 2026-09-06.**

**Every file this repo carries is written in English** — source code, identifiers, comments,
docstrings, log and progress strings, Markdown documentation, notebook prose, commit messages, PR
descriptions, and the contents of `.claude/`. English is the default for anything new, and for any
rewrite of anything old.

⚠️ **THE CONVERSATION IS NOT A FILE.** Chat replies to the user stay in **Vietnamese** (repo
paths, code identifiers and command lines stay verbatim English inside that Vietnamese prose).
The split is the point: **the artefact is English so it survives its author; the conversation is
Vietnamese so it is fast to read.** A rule that blurred the two would silently change one of them.

**Two deliberate exceptions, and both are named rather than inferred:**

1. **A `*_VI.md` file is a translation and is meant to be Vietnamese** — there are three today:
   `../docs/THESIS_PROGRESS_2026_VI.md`, `../docs/THESIS_SUMMARY_2026_VI.md`,
   `src/feature_selection/docs/NULL_DRAWS_VI.md`. Each is a Vietnamese counterpart of an English
   original that carries the same content; **the `_VI` suffix is the marker, and a Vietnamese file
   without it is a defect, not an exception.**
2. **Vietnamese data is data.** Ticker names, exchange labels, CafeF/Simplize field names, filing
   text, OCR output and the account labels of a VAS chart of accounts (`Tài sản`, `Nợ phải trả`,
   `Vốn chủ sở hữu`, …) are values the pipeline reads and must be preserved **exactly**. Never
   translate them, and never "normalise" the diacritics out of them.

⚠️ **This rule is forward-looking and there is a known bill.** Some existing files mix Vietnamese
prose into English documents — `../current_state/TODO.md` absorbed 28 Vietnamese items from
`src/orchestration/todo.md` on 2026-08-17, and scattered comments elsewhere are Vietnamese.
**Do not open a translation project on the strength of this rule.** Convert a file to English when
you are already editing it for another reason, and say so in the commit message; a mass
translation is a separate decision, made by the user, with the diff reviewed.

⚠️ **Encoding, because English does not make this go away** (`CLAUDE.md` §5 rule 18): the corpus
is UTF-8 and carries `⚠️` throughout. Open files with `encoding="utf-8"`, start any one-off script
with `sys.stdout.reconfigure(encoding="utf-8")`, and never put `⚠️` into matplotlib chart text.
