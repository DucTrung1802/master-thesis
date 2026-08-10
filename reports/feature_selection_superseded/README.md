# Superseded runs — kept for the record, read by nothing

`final_features.plan_from_reports` scans ONE root and unions every run under it, so a
run that has been redone must not sit beside the run that replaced it: they share a
`(schema, target, setup)` key and would be counted as two contributions to one table.
Moving it here is the whole mechanism — this directory is not a report root and no
stage points at it.

Nothing here should be quoted as a current result. Each folder's numbers survive in
`feature_selection/CONTEXT.md`, which is where they are interpreted.

## `2026-08-10_204449__vcb__basic+economy_japan__return_5day`

The first `japan` run through `analysis/feature_selection_economy`, superseded 58
minutes later by `2026-08-10_214214__…` in `reports/feature_selection_economy/`.

Kept because it measured two things that the run replacing it cannot:

1. ⚠️ **It is the run that found NUL-4.** It reported `p_value = 0.0476` — the
   `1/(n+1)` floor, which asserts that no shuffled draw beat the real data — while its
   own `null_max` of **+0.0916** sat far above its `observed` of **+0.0509**. That
   contradiction is what exposed `max(k, 1) / (n + 1)` where the add-one estimator
   `(k + 1) / (n + 1)` was meant. ⚠️ **Its `metadata.json` therefore still carries the
   WRONG p** and cannot be corrected in place, because the raw draws are not stored —
   only their summary. The corrected value is **0.0952**.
2. **It is the `device=cuda` half-measure**, and the baseline for
   `CONTEXT.md` §16h: it reported `device=cuda` while spending **67.3 %** of its wall
   clock on the host, because a width gate kept `lasso` on coordinate descent and
   `mutual_info` on sklearn. Removing both gates took the same selection pass from
   294.4 s to 89.7 s and the whole run from 53.0 min to 28.9 min.

It is not reproducible from current code: the width gate it ran under is gone.
