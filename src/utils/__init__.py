# src\utils\__init__.py
"""Marks `utils` a REGULAR package, which is what lets it hold its own tests.

Added 2026-08-30 with `test_progress.py`, and the reason is a trap rather than a
preference. `utils/` also holds a MODULE called `utils.py`; pytest's default import mode
inserts a test file's own directory at the front of `sys.path`, so a test living here made
`import utils` resolve to `src/utils/utils.py` — and every `from utils.constants import …`
below it then failed with *"'utils' is not a package"*, which reads as a missing dependency
rather than as a shadowed name. With this file the insertion walks up to `src/` instead,
where `utils` is the package it has always been at run time.

⚠️ It changes nothing about how anything imports `utils` — that was already
`from utils.constants import …` everywhere, and a namespace package and a regular one
answer that identically. 9 of the 12 packages under `src/` already carry one.
"""
