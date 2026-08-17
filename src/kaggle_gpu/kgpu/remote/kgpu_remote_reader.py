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

# The single artefact a PANEL payload ships. Kept in step with `kgpu.config.PANEL_TABLE`,
# which is unreachable from here: `kgpu` is not shipped, these files travel flat.
PANEL_TABLE = "panel"


def load_panel(data_dir: Optional[str | Path] = None):
    """The finished cross-sectional panel, as `feature_selection.run.ProvidedPanel`.

    ⚠️ **PANEL MODE EXISTS BECAUSE THE CROSS-SECTIONAL READ CANNOT BE SWAPPED.** Every
    other notebook reaches the database through `UnifiedSchemaReader`, which is why
    `ParquetSchemaReader` above can stand in for it. `read_universe_panel` does not: it
    is one hand-written SQL statement reaching for `reader.driver`, so on a worker it
    hits *"there is no database on a Kaggle worker"* whatever parameters it is given
    (`CSP-1` in its second form). The join therefore runs at EXPORT time and this
    function only hands the result over — with the schema, the channel→pool map and the
    universe the local reader would have supplied.

    ⚠️ **THE SHIPPED FRAME IS CHECKED AGAINST THE MANIFEST, not trusted.** A partial
    upload or a mounted PREVIOUS dataset version both produce a readable parquet with
    the wrong number of rows, and both have precedent here (§7 of the README). A shape
    that disagrees is raised on, not selected over.

    ⚠️ Read with `pd.read_parquet` rather than through `ParquetSchemaReader.read`, which
    caches the frame and then returns a `.copy()` of it — three simultaneous copies of a
    1.6 GB panel, for no gain: the parquet was typed at export.
    """
    folder = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
    if folder is None:
        raise RuntimeError(
            "load_panel has no data directory — call kgpu_bootstrap.setup() first, "
            "or pass the mounted payload directory explicitly."
        )
    folder = Path(folder)
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))

    spec = manifest.get("panel")
    entry = manifest.get("tables", {}).get(PANEL_TABLE)
    if spec is None or entry is None:
        raise ValueError(
            f"this payload ships no panel — it holds {sorted(manifest.get('tables', {}))}.\n"
            f"  A cross-sectional job needs a `data.panel` block in kaggle_config.json; "
            f"re-run `python -m kgpu data <job>` after adding one."
        )

    frame = pd.read_parquet(folder / entry["file"])
    if (len(frame), frame.shape[1]) != (entry["rows"], entry["columns"]):
        raise ValueError(
            f"{entry['file']} is {len(frame):,} x {frame.shape[1]} but the manifest "
            f"exported {entry['rows']:,} x {entry['columns']}. The mounted dataset is "
            f"not the one this manifest describes — a version still processing mounts "
            f"the PREVIOUS one and completes normally (README §7)."
        )

    from feature_selection.run import ProvidedPanel

    return ProvidedPanel(
        frame=frame,
        schema=manifest["schema"],
        database=manifest.get("database", ""),
        columns_by_table=spec["columns_by_table"],
        universe=spec.get("universe"),
        note=(
            f"{spec.get('join', 'joined at export')} "
            f"[exported {manifest.get('exported_at')} @ {manifest.get('git_commit')}]"
        ),
    )


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
