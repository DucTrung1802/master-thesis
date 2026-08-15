"""Repo notebook -> Kaggle GPU (with its data) -> run folder back in the repo.

    config.py    kaggle_config.json -> JobConfig / DataConfig, validated hard
    export.py    PostgreSQL -> parquet + source.zip, staged flat in .payload/
    dataset.py   the payload as a private Kaggle dataset, waited until READY
    notebook.py  your notebook + patched parameters + two injected cells
    runner.py    push / wait / download / merge into reports/
    remote/      the two files that run ON the worker, shipped in the payload
"""

__version__ = "1.0.0"
