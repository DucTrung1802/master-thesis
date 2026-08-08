# src\final_features\__init__.py
"""Every run's `outstanding.csv` → one `<target>__final__*` table per setup.

`feature_selection` deliberately writes nothing to the database: a selection is a
result object and a set of figures. **This package is the step that does write**, and
it is a separate package for exactly that reason — the boundary is visible in the
import graph rather than living in a comment.

    python -m final_features                 # plan only, touches nothing
    python -m final_features --apply         # create the tables

See `builder.py` for the grouping rule and `CONTEXT.md` for why the table name
carries the setup.
"""

from final_features.builder import (
    FinalTablePlan,
    build_all,
    plan_from_reports,
    table_name,
)

__all__ = ["FinalTablePlan", "build_all", "plan_from_reports", "table_name"]
