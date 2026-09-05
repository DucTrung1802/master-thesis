"""Fail if any `.md` in the repo is not routed by `.claude/current_state/INDEX.md`.

⚠️ **The index is auto-loaded into every session, so a file missing from it is a file no
session knows exists.** That is the same failure shape as CLAUDE.md §5 rule 12 — silence
is never how something gets left out — and the check that catches it is this one.

Routing is matched two ways, because the index deliberately does not name all 126 files:

* an explicit link or backticked path to the file, or
* a brace/glob pattern covering it (`experiment_{8,9}/out*/report.md`), which is how the
  71 generated run READMEs and the vendor tree are routed as GROUPS rather than one by one.

Run it before committing documentation::

    python check_index.py

Exits 1 and lists the unrouted files; prints the token budget either way.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
INDEX = REPO / ".claude" / "current_state" / "INDEX.md"

# Directories that hold no documentation of ours.
SKIP_DIRS = {".git", "mt_env", ".pytest_cache", "node_modules", ".dagster"}


def repo_markdown() -> list[Path]:
    """Every `.md` file in the repo, excluding environments and caches."""
    return sorted(
        p
        for p in REPO.rglob("*.md")
        if not (SKIP_DIRS & set(p.relative_to(REPO).parts))
    )


def _brace_glob_to_regex(pattern: str) -> re.Pattern[str]:
    """`experiment_{8,9}/out*/report.md` -> a regex matching either alternative.

    ⚠️ `fnmatch` cannot do brace expansion, and the index relies on it to route the
    generated folders as one row instead of seventy-one.
    """
    out: list[str] = []
    for part in re.split(r"(\{[^}]*\})", pattern):
        if part.startswith("{") and part.endswith("}"):
            alts = (re.escape(a.strip()) for a in part[1:-1].split(","))
            out.append(f"(?:{'|'.join(alts)})")
        else:
            # ⚠️ `**` must be substituted BEFORE `*`, and it crosses `/` while `*` does
            # not — otherwise `vendor/**` matches only the vendor folder's own files and
            # silently misses everything nested below it.
            esc = re.escape(part)
            esc = esc.replace(r"\*\*", "\0").replace(r"\*", "[^/]*").replace("\0", ".*")
            out.append(esc.replace(r"\?", "."))
    return re.compile("^" + "".join(out) + "$")


def index_patterns(text: str) -> tuple[set[str], list[re.Pattern[str]]]:
    """Literal paths and glob patterns the index mentions, normalised to repo-relative."""
    raw: set[str] = set()
    raw.update(m.group(1) for m in re.finditer(r"\]\(([^)]+\.md)\)", text))
    raw.update(m.group(1) for m in re.finditer(r"`([^`]*?\.md)`", text))
    raw.update(m.group(1) for m in re.finditer(r"`([^`]*?/\*\*)`", text))

    literals: set[str] = set()
    globs: list[re.Pattern[str]] = []
    for r in raw:
        # Index links are written relative to .claude/current_state/; strip the climb back to the root.
        norm = re.sub(r"^(\.\./)+", "", r.split("#")[0].strip())
        if not norm or norm.startswith(("http", "mailto")):
            continue
        norm = norm.removeprefix("./")
        if any(ch in norm for ch in "*{"):
            globs.append(_brace_glob_to_regex(norm))
        else:
            literals.add(norm)
            # A bare filename in the index routes that filename anywhere.
            literals.add(Path(norm).name)
    return literals, globs


def main() -> int:
    if not INDEX.exists():
        print(f"MISSING: {INDEX.relative_to(REPO)}", file=sys.stderr)
        return 1

    text = INDEX.read_text(encoding="utf-8")
    literals, globs = index_patterns(text)

    files = repo_markdown()
    unrouted: list[Path] = []
    total = 0
    for path in files:
        rel = path.relative_to(REPO).as_posix()
        total += len(path.read_text(encoding="utf-8", errors="replace"))
        if rel in literals or path.name in literals:
            continue
        if any(g.match(rel) for g in globs):
            continue
        unrouted.append(path)

    print(f"{len(files)} markdown files, ~{total / 4 / 1000:.0f}k tokens total")
    print(f"index: .claude/current_state/INDEX.md ({len(INDEX.read_text(encoding='utf-8')) / 4 / 1000:.1f}k tokens, auto-loaded)")

    if unrouted:
        print(f"\n{len(unrouted)} UNROUTED — add each to a tier in .claude/current_state/INDEX.md:", file=sys.stderr)
        for path in unrouted:
            print(f"  {path.relative_to(REPO).as_posix()}", file=sys.stderr)
        return 1

    print("\nall files routed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
