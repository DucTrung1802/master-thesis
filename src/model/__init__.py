# src\model\__init__.py
"""The model stage: a referenced dataset in, an immutable scored run folder out.

    common/   the framework every model shares — Dataset, RunDir, Trainer, registry
    lstm/     one model: architecture, configs, `python -m model.lstm`
    runs/     every run from every model, plus the shared `index.csv` leaderboard

⚠️ Nothing here computes a metric. Scoring belongs to `result_evaluator`, which reads
a run's `results/predictions_<split>.csv` — so a metric can be added or corrected
across every past run without retraining one of them.
"""
