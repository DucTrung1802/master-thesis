# src\kaggle_gpu\kgpu\remote\kgpu_remote_reader.py
"""`UnifiedSchemaReader`, with the SQL swapped for parquet. Runs ON THE WORKER.

⚠️ **THIS SUBCLASSES THE REAL READER; IT DOES NOT REIMPLEMENT IT.** `join()` is
inherited verbatim — the key intersection, the one-to-one validation, the
duplicate-column drop and the `join_log` are the same code that runs locally. A
second copy of that method is a second place for the row-multiplication guard to
drift, and a panel that has silently doubled still looks like a panel.

What IS overridden is exactly the four methods that talk to PostgreSQL:

    tables()        <- the manifest's shipped table list
    column_types()  <- the manifest's information_schema dump
    overview()      <- the manifest's overview, WITH a `shipped` column
    read()          <- the parquet file, already typed at export time

`__init__` deliberately does not call `super().__init__`: the parent builds a
`Logger` and a `PostgreSQLDriver`, and on the worker those are the import stubs
`kgpu_bootstrap` installed. Constructing them would work and mean nothing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd

from feature_selection.unified_reader import (
    UnifiedSchemaReader,
    unified_schema_name,
)

# Set by `kgpu_bootstrap.setup()` so the class stays signature-compatible with
# `UnifiedSchemaReader(TICKER)` — the notebooks construct it with one argument
# and must not have to know they are on a worker.
DEFAULT_DATA_DIR: Optional[Path] = None


class ParquetSchemaReader(UnifiedSchemaReader):
    """Read one exported `unified_schema_<ticker>` payload from disk."""

    def __init__(
        self,
        ticker: str,
        database: Optional[str] = None,
        logger=None,
        log_file: Optional[str] = None,
        data_dir: Optional[str | Path] = None,
    ):
        folder = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
        if folder is None:
            raise RuntimeError(
                "ParquetSchemaReader has no data directory — call "
                "kgpu_bootstrap.setup() before constructing a reader."
            )
        self._dir = Path(folder)
        self._manifest = json.loads(
            (self._dir / "manifest.json").read_text(encoding="utf-8")
        )

        # ⚠️ The payload is for ONE ticker. A notebook left on TICKER="VCB" while
        # the job exported BANK would otherwise read VCB's schema name over BANK's
        # rows and every line of the report would name the wrong universe.
        exported = str(self._manifest["ticker"]).upper()
        if ticker.upper() != exported:
            raise ValueError(
                f"this payload holds {exported}, but the notebook asked for "
                f"{ticker.upper()}. Set the job's data.ticker and the notebook's "
                f"TICKER to the same universe and re-export."
            )

        self.ticker = ticker.upper()
        self.schema = unified_schema_name(ticker)
        self.database = database or self._manifest.get("database", "")
        self._connected = True
        self.join_log: List[Dict] = []
        self._cache: Dict[str, pd.DataFrame] = {}

    # ---------------------------------------------------------------- lifecycle

    def connect(self) -> "ParquetSchemaReader":
        return self

    def close(self) -> None:
        self._cache.clear()

    @property
    def driver(self):
        raise RuntimeError(
            "there is no database on a Kaggle worker — this run reads the parquet "
            "payload. Anything needing raw SQL has to run locally."
        )

    # ------------------------------------------------------------- introspection

    @property
    def payload(self) -> dict:
        """The manifest — export time, git commit, row counts, date spans."""
        return self._manifest

    def tables(self, prefix: str = "") -> List[str]:
        names = sorted(self._manifest["tables"])
        return [n for n in names if n.startswith(prefix)]

    def pools(self) -> List[str]:
        return self.tables(prefix="pool__")

    def column_types(self, table: str) -> Dict[str, str]:
        entry = self._manifest["tables"].get(table)
        if entry is None:
            raise ValueError(
                f"{table!r} was not shipped in this payload. Shipped: "
                f"{sorted(self._manifest['tables'])}.\n"
                f"  Add it to the job's POOLS (or data.tables) in "
                f"kaggle_config.json and re-run: python -m kgpu data <job>"
            )
        return dict(entry["column_types"])

    def overview(self) -> pd.DataFrame:
        """Every pool in the source schema, with `shipped` marking what travelled.

        ⚠️ The un-shipped rows are kept on purpose. This frame is the notebook's
        orientation table, and one showing only the payload would make a schema of
        76 pools look like a schema of two.
        """
        rows = self._manifest.get("overview")
        if not rows:
            return pd.DataFrame(
                [
                    {
                        "table": name,
                        "rows": entry["rows"],
                        "columns": entry["columns"],
                        "key_columns": ", ".join(
                            k
                            for k in ("date", "exchange", "ticker")
                            if k in entry["column_types"]
                        ),
                        "first_date": entry["first_date"],
                        "last_date": entry["last_date"],
                        "shipped": True,
                    }
                    for name, entry in sorted(self._manifest["tables"].items())
                ]
            )
        frame = pd.DataFrame(rows)
        for column in ("first_date", "last_date"):
            if column in frame.columns:
                frame[column] = pd.to_datetime(frame[column]).dt.date
        return frame

    # -------------------------------------------------------------------- read

    def read(
        self,
        table: str,
        columns: Optional[Sequence[str]] = None,
        order_by: Sequence[str] = ("date",),
    ) -> pd.DataFrame:
        """One shipped table. Types came from `information_schema` at export."""
        types = self.column_types(table)
        if columns is not None:
            unknown = [c for c in columns if c not in types]
            if unknown:
                raise ValueError(f"{self.schema}.{table} has no column(s) {unknown}")

        if table not in self._cache:
            path = self._dir / self._manifest["tables"][table]["file"]
            if not path.exists():
                raise FileNotFoundError(
                    f"the manifest lists {table} but {path.name} is not in the "
                    f"mounted dataset — the upload is incomplete. Re-run "
                    f"`python -m kgpu data <job>` locally."
                )
            self._cache[table] = pd.read_parquet(path)

        frame = self._cache[table]
        frame = frame[list(columns)] if columns is not None else frame.copy()

        order = [c for c in order_by if c in frame.columns]
        if order:
            frame = frame.sort_values(list(order)).reset_index(drop=True)
        if frame.empty:
            raise ValueError(f"{self.schema}.{table} is empty.")
        return frame
