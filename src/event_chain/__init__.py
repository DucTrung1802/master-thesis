"""The EVENT chain — predict the sessions on which a ticker rises by at least g % within
the next h sessions, end to end, on the repo's own stages.

```
utils/event_target.py          THE PARAMETERS (gain %, horizon, rule) — one place
   ▼ dagster  unified/pool__targets           writes the 0/1 label `up_<g>pct_<h>day`
   ▼ dagster  gold/stocks_event_features → unified/pool__event_features
   ▼ select   feature_selection.run, ONE POOL AT A TIME, holdout = the val start
   ▼ final    final_features → unified_schema_<t>.up_<g>pct_<h>day__final__d<d>_h<h>
   ▼ dataset  train_test_creator (classification: y unscaled, purge d+h-1)
   ▼ train    model.{baseline,gbt,forest,lstm,gru,cnn,mlp,tcn} with task=classification
   ▼ report   event metrics (ROC-AUC vs a block-shuffled null, PR-AUC, precision@k)
```

`python -m event_chain` prints the plan and writes nothing; `--apply` runs it.
"""
