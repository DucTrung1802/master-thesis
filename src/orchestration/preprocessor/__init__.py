# src\orchestration\preprocessor\__init__.py
"""The transform library every asset wraps — moved here from `src/data_preprocessor`
on 2026-08-05, which is what finally deleted that package.

Nothing about the code changed in the move; what changed is that it now lives inside
the only thing that calls it. `src/data_preprocessor` had exactly ONE real importer
(`orchestration/resources.py`) plus a notebook, so "remove data_preprocessor" was a
relocation, not the multi-week rewrite it looks like from the line count.

⚠️ **This is still a library and still has no entry point.** The three
`ingest_*_data()` methods and `_run_layer` went in phase 5 (2026-08-05); every
`_ingest_*` method here is called directly by the asset that wraps it, so an exception
propagates and the asset goes red. Read [CONTEXT.md](CONTEXT.md) for how a table is
BUILT; add new pipeline steps as assets in `../assets/`.

`DataPreprocessor` is re-exported so callers write
`from orchestration.preprocessor import DataPreprocessor` rather than repeating the
module name.
"""

from orchestration.preprocessor.preprocessor import DataPreprocessor

__all__ = ["DataPreprocessor"]
