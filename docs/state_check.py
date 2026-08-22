"""Report documentation drift. **Run it before you commit.**

⚠️ **This script REPORTS; it never rewrites.** That is deliberate and it is this repo's own
rule turned on the docs themselves: `CLAUDE.md` §8 says *record what was measured*, and the
counts here cannot be derived mechanically without getting them wrong. `ISSUES.md` keeps
four FIXED rows struck-through inside its Open table on purpose, so a naive row-counter
reports 17 where the truth is 16 — and a confidently wrong number is worse than none,
because it is what the next session budgets against.

So every check below either passes, or hands you a decision. Nothing is auto-edited.

Six checks, in the order they usually break::

    python docs/state_check.py

1. `CLAUDE.md` §6 "State today" date vs. the newest `.md` change in the tree
2. package `CONTEXT.md` files changed without `CLAUDE.md` being touched alongside
3. issue counts: what `CLAUDE.md` claims vs. what `ISSUES.md`'s own headers say
4. `docs/INDEX.md` completeness (delegates to `check_index.py`)
5. `docs/INDEX.md` token costs vs. measured — the failure that had gone stale by 2.7×
6. relative-link integrity across every `.md`

Exit 0 when clean, 1 when something needs a human. ⚠️ **Nothing calls this automatically.**
It is not wired to a git hook by choice (2026-08-22) — the checks are advisory, and a hook
that blocks an unrelated commit costs more than the drift it prevents. Wire it later if
that trade changes.
"""

from __future__ import annotations

import datetime as dt
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "docs"))

from check_index import index_patterns, repo_markdown  # noqa: E402

# A claimed cost may drift this far before it is worth a human's attention. Below this,
# ordinary editing noise would make the check cry wolf on every commit.
COST_TOLERANCE = 0.20


class Report:
    """Collects findings so every check runs even after the first one fails."""

    def __init__(self) -> None:
        self.rows: list[tuple[str, str, str]] = []
        self.drift = 0

    def ok(self, check: str, detail: str) -> None:
        self.rows.append(("OK", check, detail))

    def warn(self, check: str, detail: str) -> None:
        self.rows.append(("NEEDS A DECISION", check, detail))
        self.drift += 1

    def render(self) -> int:
        width = max(len(c) for _, c, _ in self.rows)
        print(f"\ndocumentation state — {dt.date.today().isoformat()}\n")
        for status, check, detail in self.rows:
            mark = "  " if status == "OK" else "->"
            print(f"{mark} {check.ljust(width)}  {detail}")
        if self.drift:
            print(f"\n{self.drift} item(s) need a human decision before you commit.")
            print("Nothing was rewritten — update the file yourself, then re-run.")
            return 1
        print("\nclean — CLAUDE.md and docs/ agree with the tree.")
        return 0


def _git(*args: str) -> list[str]:
    try:
        out = subprocess.run(
            ["git", *args], cwd=REPO, capture_output=True, text=True, timeout=30
        )
    except (OSError, subprocess.SubprocessError):
        return []
    return [line for line in out.stdout.splitlines() if line.strip()]


def changed_markdown() -> set[str]:
    """`.md` files whose CONTENT changes in this commit, staged or not.

    ⚠️ **A pure rename is not a change, and git reports it as one.** The 2026-08-22 docs
    move staged nine `R100` renames — 100 % similarity, zero content difference — and every
    one of them tripped the date check. `-M` asks git to detect renames and hand back the
    similarity score so the untouched ones can be dropped; without it `--name-only` reports
    a moved file exactly like a rewritten one.
    """
    changed: set[str] = set()
    for scope in (("--cached",), ()):
        for line in _git("diff", "--name-status", "-M", *scope):
            parts = line.split("\t")
            status, paths = parts[0], parts[1:]
            if status == "R100":  # moved, byte-identical
                continue
            changed.update(p for p in paths if p.endswith(".md"))
    return changed


def carries_measurements(path: str) -> bool:
    """Does this file hold RESULTS, as opposed to navigation or prose?

    ⚠️ **The date check fired on every commit before this existed**, because *any* `.md`
    edit tripped it — including edits to the index and this very script's own docs. A check
    that always fires is a check nobody reads, so the trigger is narrowed to the files that
    actually carry numbers. `README.md`, `docs/INDEX.md`, `docs/RUNBOOK.md` and the thesis
    write-ups are deliberately NOT here: changing them cannot make §6 stale.
    """
    return path.endswith("CONTEXT.md") or path in {
        "docs/ISSUES.md",
        "docs/TODO.md",
        "docs/pipeline.md",
        "docs/PIPELINE_h10_CAGR74.md",
    }


def check_state_date(rep: Report) -> None:
    text = (REPO / "CLAUDE.md").read_text(encoding="utf-8")
    m = re.search(r"^## 6\. State today \((\d{4}-\d{2}-\d{2})\)", text, re.M)
    if not m:
        rep.warn("CLAUDE.md §6 date", "no `## 6. State today (YYYY-MM-DD)` heading found")
        return
    stamped = dt.date.fromisoformat(m.group(1))
    age = (dt.date.today() - stamped).days
    bearing = sorted(p for p in changed_markdown() if carries_measurements(p))
    if bearing and age > 0:
        rep.warn(
            "CLAUDE.md §6 date",
            f"stamped {stamped} ({age}d old) while {', '.join(bearing[:2])}"
            f"{'…' if len(bearing) > 2 else ''} change in this commit",
        )
    else:
        rep.ok("CLAUDE.md §6 date", f"stamped {stamped} ({age}d old), no result files in this commit")


def check_context_without_hub(rep: Report) -> None:
    """A measurement written into a CONTEXT.md that never reached the hub is invisible."""
    changed = changed_markdown()
    contexts = sorted(p for p in changed if p.endswith("CONTEXT.md"))
    if not contexts:
        rep.ok("CONTEXT.md ↔ CLAUDE.md", "no package CONTEXT.md in this commit")
        return
    if "CLAUDE.md" in changed:
        rep.ok("CONTEXT.md ↔ CLAUDE.md", f"{len(contexts)} changed, CLAUDE.md updated too")
        return
    rep.warn(
        "CONTEXT.md ↔ CLAUDE.md",
        f"{', '.join(contexts)} changed but CLAUDE.md did not — "
        "does the hub need the finding?",
    )


def check_issue_counts(rep: Report) -> None:
    hub = (REPO / "CLAUDE.md").read_text(encoding="utf-8")
    issues = (REPO / "docs" / "ISSUES.md").read_text(encoding="utf-8")

    heads = dict(re.findall(r"^## (Open|Resolved) \((\d+)\)", issues, re.M))
    if len(heads) != 2:
        rep.warn("issue counts", "ISSUES.md is missing an `## Open (n)` / `## Resolved (n)` heading")
        return
    truth = (int(heads["Open"]), int(heads["Resolved"]))

    m = re.search(r"\*\*(\d+) open\*\*, (\d+) resolved", hub)
    if not m:
        rep.warn("issue counts", f"ISSUES.md says {truth[0]} open / {truth[1]} resolved; CLAUDE.md states neither")
        return
    claimed = (int(m.group(1)), int(m.group(2)))
    if claimed == truth:
        rep.ok("issue counts", f"{truth[0]} open / {truth[1]} resolved — CLAUDE.md agrees")
    else:
        rep.warn(
            "issue counts",
            f"CLAUDE.md says {claimed[0]} open / {claimed[1]} resolved, "
            f"ISSUES.md headings say {truth[0]} / {truth[1]}",
        )


def check_index_complete(rep: Report) -> None:
    index = REPO / "docs" / "INDEX.md"
    literals, globs = index_patterns(index.read_text(encoding="utf-8"))
    unrouted = [
        p.relative_to(REPO).as_posix()
        for p in repo_markdown()
        if p.relative_to(REPO).as_posix() not in literals
        and p.name not in literals
        and not any(g.match(p.relative_to(REPO).as_posix()) for g in globs)
    ]
    if unrouted:
        rep.warn("INDEX.md completeness", f"{len(unrouted)} unrouted: {', '.join(unrouted[:3])}…")
    else:
        rep.ok("INDEX.md completeness", f"{len(repo_markdown())} .md files, all routed")


def check_index_costs(rep: Report) -> None:
    """⚠️ The check that exists because all 16 of CLAUDE.md §7's costs had gone stale."""
    index = REPO / "docs" / "INDEX.md"
    stale: list[str] = []
    for m in re.finditer(r"\[[^\]]+\]\(([^)]+\.md)\)\s*\|\s*\*{0,2}~?([\d.]+)k", index.read_text(encoding="utf-8")):
        target = (index.parent / m.group(1)).resolve()
        if not target.exists():
            continue
        actual = len(target.read_text(encoding="utf-8", errors="replace")) / 4000
        claimed = float(m.group(2))
        if claimed and abs(actual - claimed) / claimed > COST_TOLERANCE:
            stale.append(f"{m.group(1)} claims {claimed}k, measures {actual:.1f}k")
    if stale:
        rep.warn("INDEX.md token costs", f"{len(stale)} drifted >{COST_TOLERANCE:.0%}: {stale[0]}")
    else:
        rep.ok("INDEX.md token costs", f"all within {COST_TOLERANCE:.0%} of measured")


def check_links(rep: Report) -> None:
    pattern = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
    broken, total = [], 0
    for md in repo_markdown():
        for m in pattern.finditer(md.read_text(encoding="utf-8", errors="replace")):
            target = m.group(1).strip()
            if target.startswith(("http://", "https://", "#", "mailto:")):
                continue
            path = target.split("#")[0]
            if not path:
                continue
            total += 1
            if not (md.parent / path).exists():
                broken.append(f"{md.relative_to(REPO).as_posix()} -> {target}")
    if broken:
        rep.warn("relative links", f"{len(broken)} of {total} broken (first: {broken[0]})")
    else:
        rep.ok("relative links", f"{total} checked, all resolve")


def main() -> int:
    # ⚠️ CLAUDE.md §5 rule 18, and this script tripped it on its first run: Windows
    # defaults stdout to cp1252, which cannot encode `↔` (U+2194) and raises
    # UnicodeEncodeError mid-report — after some rows have already printed, so the
    # failure looks like a check crashing rather than a console limitation.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, OSError):
            pass

    rep = Report()
    check_state_date(rep)
    check_context_without_hub(rep)
    check_issue_counts(rep)
    check_index_complete(rep)
    check_index_costs(rep)
    check_links(rep)
    return rep.render()


if __name__ == "__main__":
    raise SystemExit(main())
