# orchestration\__init__.py
"""Dagster orchestration layer — the DAG that `src/main.py` runs as a script today.

Nothing here holds pipeline LOGIC. Every asset is a thin wrapper over a method that
already exists in `src/`, so this package can be deleted without losing anything but
the scheduling/observability, and `main.py` keeps working alongside it.
"""

from orchestration._bootstrap import bootstrap

bootstrap()
