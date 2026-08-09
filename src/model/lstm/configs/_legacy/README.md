# Legacy configs — kept for the record, not for running

These 27 configs drove the pre-2026-08-09 lookback sweeps (`return_5day`,
`direction_5day`, `probability_gain_5pct_5day` at `lb1…lb30`). Their datasets are
still on disk under `src/train_test_set/` and every one of them still resolves, so
they **do** run — which is why they were moved rather than deleted: two live naming
conventions in one folder, with nothing saying which is current, is issue OLD-2.

⚠️ **Do not use them for a new result.** They describe a pipeline that no longer
exists:

| | legacy | current |
|---|---|---|
| source | `<target>__lb<L>__final` VIEW (dropped) | `<target>__final__d<d>_h<h>` table |
| split | no purge — train labels reached `h` days into val | purged by `d + h - 1` |
| imputation | `ffill().bfill()` — leading gaps filled from the future | train-slice median |
| scoring | metrics in-notebook, **no null** | `result_evaluator`, block-shuffled bar |
| `dir_accuracy` | sign hit rate at 0 | now called `sign_accuracy`; the core's `hit_rate` is at the score's median |

The current configs are one directory up: `vcb__return_5day__final__d20_h5.yaml` and
`bank__rank_5day__final__d20_h5.yaml`.

⚠️ A run produced from a legacy config lands in `runs/index.csv` with the core
columns **blank**, because the legacy notebooks compute their own metrics. Run
`python -m result_evaluator --rescore` afterwards to fill them — that path reads
`predictions_*.csv` and needs no GPU.
