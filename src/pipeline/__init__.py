# src\pipeline\__init__.py
"""The five stages as one chain, and the check that they still agree.

    python -m pipeline                 # what exists, what is stale — writes nothing
    python -m pipeline --apply         # run every stage that is not ready
    python -m pipeline --only model --apply

⚠️ This does not pass data between stages. Each stage already reads its input from
the previous stage's output and verifies it (`d`/`h` parsed from the table name, the
dataset asserted against the config, the metrics recomputed from predictions). What
was missing was one place to ask **which stage is stale** — that is `status()`.
"""

from pipeline.stages import Stage, StageState, run, stages, status

__all__ = ["Stage", "StageState", "run", "stages", "status"]
