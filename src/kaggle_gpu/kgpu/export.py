# src\kaggle_gpu\kgpu\export.py
"""Stage the payload dataset: PostgreSQL -> parquet, repo source -> zip.

⚠️ **A KAGGLE WORKER CANNOT REACH `database_main_v2`.** That single fact is why
this module exists and why running a notebook on a Kaggle GPU is not "point the
kernel at the repo". The pools the notebook joins are read here, on the machine
that can see the database, typed by `UnifiedSchemaReader` exactly as the notebook
would have typed them, and written to parquet — so the frame that arrives on the
GPU is the frame the local run would have built.

What travels, and why each piece is needed:

| file | why |
|---|---|
| `<pool>.parquet` | the data. Typed at export, so `numeric` never arrives as `object` |
| `manifest.json` | the `information_schema` types, the row counts, the date spans, the full schema overview — everything `UnifiedSchemaReader` gets from SQL |
| `source.zip` | `src/feature_selection/*.py` and friends. The Kaggle image has torch/xgboost/sklearn; it does not have this repo |
| `kgpu_bootstrap.py` | the first cell's entry point, on the remote side |
| `kgpu_remote_reader.py` | `UnifiedSchemaReader` with the SQL swapped for parquet — **subclassed, so `join()` is inherited rather than re-implemented** |

⚠️ **THE PAYLOAD IS FLAT ON PURPOSE.** `dataset_create_version(dir_mode="skip")`
silently ignores subdirectories, so a payload with a `data/` folder uploads as a
dataset holding nothing but the loose files, and the run fails on the GPU with a
missing-file error that says nothing about why. Every file here is at the top
level; the only nesting is inside `source.zip`, which the remote side unpacks.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from .config import (
    DOCUMENTS_ARCHIVE, PANEL_TABLE, REPO_ROOT, JobConfig, repo_src_on_path,
)

# The remote-side files, shipped flat beside the parquet.
REMOTE_DIR = Path(__file__).resolve().parent / "remote"
REMOTE_FILES = ("kgpu_bootstrap.py", "kgpu_remote_reader.py")

# What never travels: bytecode, checkpoints, and the notebooks themselves. The
# notebook being run is uploaded as the KERNEL; the four `study_*.ipynb` files
# beside it are 1.4 MB each of finished write-up and would be dead weight in
# every dataset version.
# ⚠️ **`.txt` JOINED `.py` ON 2026-08-28 FOR ONE FILE, AND IT IS LOAD-BEARING**:
# `src/web_scraper/requirements-ocr.txt` is the single source of truth for the OCR stack, and
# the worker's notebook installs FROM it. Shipping only `.py` would have left the worker
# raising on a missing file — after the queue and the upload. Measured across all three
# `source_dirs`, this adds exactly one 8 KB file, so it costs nothing; if a large `.txt` ever
# lands in a shipped package, narrow this rather than widening it further.
SOURCE_INCLUDE = (".py", ".txt")
SOURCE_EXCLUDE_DIRS = {"__pycache__", ".ipynb_checkpoints", ".git", "runs"}

MANIFEST = "manifest.json"


# Moved to `config.py` (2026-08-16) — `runner` and `__main__` want it too, and three
# copies of one `sys.path.insert` is three places for the repo layout to be wrong.
_repo_src_on_path = repo_src_on_path


def git_commit() -> str | None:
    """The commit the payload was cut at — `report._git_commit`'s answer, locally.

    ⚠️ Recorded and replayed on the worker. `feature_selection.report` shells out
    to git for the provenance line in every `metadata.json`, and a Kaggle worker
    has no repo — so without this the whole point of that field is lost on exactly
    the runs that are hardest to reproduce.
    """
    try:
        head = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=10,
        )
        if head.returncode != 0:
            return None
        dirty = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=10,
        )
        return head.stdout.strip() + ("+dirty" if dirty.stdout.strip() else "")
    except Exception:  # noqa: BLE001 - a missing git is not a failed export
        return None


def _zip_sources(dirs: List[str], out: Path) -> Dict[str, int]:
    """Zip the given repo-relative directories, paths preserved from the repo root."""
    counts: Dict[str, int] = {}
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        for rel in dirs:
            base = REPO_ROOT / rel
            n = 0
            for path in sorted(base.rglob("*")):
                if path.is_dir():
                    continue
                if SOURCE_EXCLUDE_DIRS & set(path.relative_to(base).parts[:-1]):
                    continue
                if path.suffix not in SOURCE_INCLUDE:
                    continue
                archive.write(path, path.relative_to(REPO_ROOT).as_posix())
                n += 1
            if n == 0:
                raise ValueError(
                    f"source_dirs entry {rel!r} contributed 0 files — only "
                    f"{SOURCE_INCLUDE} are shipped, and a package with no .py in it "
                    f"will fail to import on the worker."
                )
            counts[rel] = n
    return counts


def _dataset_metadata(cfg: JobConfig) -> dict:
    data = cfg.data
    assert data is not None
    return {
        "title": data.title or f"{cfg.title} payload",
        "id": data.id,
        "licenses": [{"name": "other"}],
    }


def _payload_hash(folder: Path) -> str:
    """Content hash of the staged payload — what `run` compares against uploads.

    ⚠️ `uploaded.json` is excluded: it is written INTO the payload after the hash
    is taken, so including it would make every payload stale the instant it was
    uploaded — a staleness check that always fires is a staleness check nobody reads.
    """
    digest = hashlib.sha256()
    for path in sorted(folder.rglob("*")):
        if path.is_file() and path.name != "uploaded.json":
            digest.update(path.relative_to(folder).as_posix().encode())
            digest.update(str(path.stat().st_size).encode())
            digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()[:16]


def _export_panel(cfg, folder: Path, manifest: dict, quiet: bool = False) -> None:
    """Join the universe panel HERE and ship it as one parquet.

    ⚠️ **This is the whole reason panel mode exists.** `feature_selection.
    cross_sectional.read_universe_panel` builds `pool__basic ⋈ pool__targets` with one
    hand-written SQL statement and derives `cs_rank_{h}day` from it, so it reaches for
    `reader.driver._cursor_ctx()` — and on a Kaggle worker `ParquetSchemaReader.driver`
    raises *"there is no database on a Kaggle worker"*. No `--pools` value, no notebook
    parameter and no config key routes around that: the cross-sectional read bypasses
    every abstraction the parquet payload replaces. `CSP-1` in its second form.

    ⚠️ **THE UNIVERSE IS RANKED ON DATA BEFORE THE EVALUATION WINDOW.** "Top N by
    turnover" over the whole sample is look-ahead — it selects the names that turned out
    to be liquid, the same defect §2c records for non-point-in-time index membership.
    `liquidity_before` is REQUIRED rather than defaulted: a silent default would be
    invisible in the artefact.

    ⚠️ The derived `cs_rank` is a rank within the SHIPPED universe, not within all 781.
    That is the intended experiment — a tradeable liquid cross-section — and it is
    recorded in the manifest so a reader cannot mistake it for the full-universe rank.
    """
    spec = dict(cfg.data.panel or {})
    top_n = int(spec.get("top_n", 0))
    cutoff = spec.get("liquidity_before")
    horizons = [int(h) for h in spec.get("horizons", [])]
    min_width = int(spec.get("min_width", 5))
    # ⚠️ **THE SELECTION WINDOW, ADDED 2026-08-19 FOR `PRF-7`.** `read_universe_panel`
    # has always taken `start`/`end`; nothing here passed them, so every panel job saw
    # the whole sample. PRF-7 needs a run that saw ONLY data an early walk-forward fold
    # could have seen, to measure how much of the shortlist is selection look-ahead.
    # ⚠️ Deliberately SEPARATE from `liquidity_before`: the universe must stay identical
    # between the two runs or the comparison confounds "which names" with "which dates".
    date_start = spec.get("date_start")
    date_end = spec.get("date_end")
    if not (top_n and cutoff and horizons):
        raise ValueError(
            f"job {cfg.name!r}: data.panel needs top_n, liquidity_before and horizons. "
            f"liquidity_before has no default on purpose — ranking liquidity over the "
            f"whole sample is look-ahead, and a silent default would hide it."
        )

    import pandas as pd

    from feature_selection.cross_sectional import read_universe_panel
    from feature_selection.unified_reader import KEY_COLS, UnifiedSchemaReader

    with UnifiedSchemaReader(cfg.data.ticker) as reader:
        schema = reader.schema
        # ⚠️ **THE CHANNEL→POOL MAP IS CAPTURED HERE OR NOWHERE.** `read_universe_panel`
        # is one hand-written SQL join, so there is no `reader.join_log` and no
        # `reader.columns_by_table` behind the finished frame — and on the worker there
        # is no `information_schema` to ask. Without it `outstanding` cannot fill
        # `source_table`, `contract.validate_shortlist` refuses the shortlist, and the
        # run comes home invisible to `final_features` (the 2026-08-15 defect, one layer
        # down). Read while the cursor is open; filtered against the frame below.
        source_types = {
            table: reader.column_types(table)
            for table in ("pool__basic", "pool__targets")
        }
        with reader.driver._cursor_ctx() as cur:
            cur.execute(
                f"SELECT ticker FROM {schema}.pool__basic "
                f"WHERE date < %s AND value_matched IS NOT NULL GROUP BY ticker "
                f"ORDER BY PERCENTILE_DISC(0.5) WITHIN GROUP "
                f"(ORDER BY value_matched) DESC",
                (cutoff,),
            )
            ranked = [row[0] for row in cur.fetchall()]
    if len(ranked) < top_n:
        raise ValueError(
            f"only {len(ranked)} tickers have turnover before {cutoff}; {top_n} asked."
        )
    universe = ranked[:top_n]

    frame = read_universe_panel(
        tickers=universe, horizons=tuple(horizons), min_width=min_width,
        schema_ticker=cfg.data.ticker, start=date_start, end=date_end,
    )

    # ⚠️ **EXTRA POOLS, ADDED 2026-08-19 FOR `PRF-9`.** `read_universe_panel` reads
    # `pool__basic ⋈ pool__targets` only, so the 13 channels behind every cross-sectional
    # result are the survivors of **90 candidates, not of 800** — and `pool__ta` alone
    # holds 711 more that CAN rank a cross-section (`PRF-9`'s survey: 71 of 76 gold tables
    # are date-only and structurally cannot). They join HERE, for the same reason the base
    # panel does: the worker has no database (`CSP-1`).
    #
    # ⚠️ **`channels` IS A LABEL-FREE ALLOWLIST AND MUST STAY ONE.** It exists because of
    # `MEM-1`, not because of any belief about which channels are good — the design is
    # `rows × channels × 6 stats` and the box is finite. Choosing the list by correlation
    # with the TARGET would build `PRF-7`'s bias into the candidate set before the
    # selection ever ran, and no null downstream could price it. `feature_selection.prune`
    # is the label-free chooser; anything else here is a defect.
    extra = spec.get("pools") or {}
    extra_types: Dict[str, dict] = {}
    if extra:
        with UnifiedSchemaReader(cfg.data.ticker) as reader:
            for table, channels in extra.items():
                available = reader.column_types(table)
                unknown = [c for c in channels if c not in available]
                if unknown:
                    raise ValueError(
                        f"{schema}.{table} has no channel(s) {unknown[:6]}"
                        f"{' …' if len(unknown) > 6 else ''} — an allowlist naming a "
                        f"column the source does not have is a silent no-op, so it raises."
                    )
                wanted = list(KEY_COLS) + [c for c in channels if c not in KEY_COLS]
                # ⚠️ **FILTER IN SQL, NOT IN PANDAS — this line was the wide export's
                # ceiling.** Reading the pool whole and narrowing afterwards materialises
                # every ticker in the schema to keep the panel's: for `pool__ta` on
                # `unified_schema_all` that is **2,381,858 rows of 781 names to keep
                # 150**, and at 143 channels it died with *"Unable to allocate 1.54 GiB
                # for an array with shape (87, 2381858)"* (2026-08-21). `PRF-9` never hit
                # it because 30 channels fit; the fix is the same either way, since the
                # discarded rows never needed to exist.
                piece = reader.read(table, columns=wanted, tickers=universe)
                piece["date"] = pd.to_datetime(piece["date"])
                piece["ticker"] = piece["ticker"].astype(str).str.upper()
                # ⚠️ Kept as a belt-and-braces narrowing: `read(tickers=...)` is a no-op
                # on a table with no `ticker` column, and this is the line that would
                # catch a pool that turns out to be date-only.
                piece = piece[piece["ticker"].isin(set(universe))]
                before = len(frame)
                # ⚠️ INNER, matching `read_universe_panel`'s own join and `join_log`. A
                # LEFT join would invent rows the ranking never saw — and `pool__ta` stops
                # 2026-06-26 (`STA-1`), so this is exactly where the chain loses its last
                # sessions rather than silently carrying NULLs into a window design.
                frame = frame.merge(piece, on=list(KEY_COLS), how="inner")
                extra_types[table] = {c: available[c] for c in channels}
                if not quiet:
                    print(f"  + {table}: {len(channels)} channels, "
                          f"{before:,} -> {len(frame):,} rows")

    path = folder / f"{PANEL_TABLE}.parquet"
    frame.to_parquet(path, index=False)

    manifest["panel"] = {
        "top_n": top_n,
        "liquidity_before": cutoff,
        "liquidity_rule": "median value_matched over dates strictly before the cutoff",
        "horizons": horizons,
        "min_width": min_width,
        # ⚠️ Recorded even when None, so a reader can tell "the whole sample" from
        # "nobody wrote it down". §5 rule 2 at the manifest.
        "date_start": date_start,
        "date_end": date_end,
        "universe": universe,
        "cs_rank_scope": (
            f"within-date rank over the {top_n} SHIPPED names, not over all 781"
        ),
        # The two pools the panel is made of, each restricted to the columns the frame
        # actually carries. ⚠️ `cs_rank_{h}day` belongs to NEITHER: it is derived after
        # the read, which is the same reason `final_features` cannot store it
        # (`final_features/CONTEXT.md` §5). It is the target, never a candidate.
        "columns_by_table": {
            table: [
                c
                for c in types
                if c not in KEY_COLS and c in frame.columns
            ]
            for table, types in {**source_types, **extra_types}.items()
        },
        # ⚠️ Recorded so a later reader can tell a 90-channel run from an 800-channel one
        # without diffing column lists, and so `PRF-9`'s prune is auditable from the
        # artefact rather than from whoever ran it.
        "extra_pools": {t: len(c) for t, c in (extra or {}).items()},
        # ⚠️ **ASCII ONLY, AND THE REASON IS MEASURED.** This string is printed by the
        # worker notebook, so it lands in Kaggle's run log — and `kernels_output` writes
        # that log to disk with the process's default encoding, which on Windows is
        # cp1252. A `⋈` here made `kgpu pull` raise `UnicodeEncodeError: 'charmap' codec
        # can't encode character '⋈'` AFTER a 23-minute run had COMPLETED
        # (2026-08-18). CLAUDE.md §5 rule 18 one step further out: it is not enough for
        # OUR writers to use utf-8, because a third party's writer handles this text too.
        "join": (
            f"read_universe_panel over the top {top_n} names by {cutoff}-cutoff median "
            f"turnover; pool__basic JOIN pool__targets server-side, cs_rank_* derived "
            f"after, joined locally at export because a Kaggle worker has no database"
        ),
    }
    manifest["tables"][PANEL_TABLE] = {
        "file": path.name,
        "rows": int(len(frame)),
        "columns": int(frame.shape[1]),
        "column_types": {c: str(t) for c, t in frame.dtypes.items()},
        "first_date": str(frame["date"].min().date()),
        "last_date": str(frame["date"].max().date()),
        "bytes": path.stat().st_size,
    }
    if not quiet:
        print(
            f"  panel {len(frame):>9,} x {frame.shape[1]:>4}  "
            f"{path.stat().st_size / 1024**2:>7.1f} MB  "
            f"{frame['ticker'].nunique()} tickers, {frame['date'].nunique():,} dates "
            f"-> {frame['date'].max():%Y-%m-%d}",
            flush=True,
        )


def _export_documents(cfg, folder: Path, manifest: dict, quiet: bool = False) -> None:
    """Ship the FILES the PDF parse reads — one zip, no database, no parquet.

    ⚠️ **THE PAYLOAD IS CHOSEN BY THE SAME FUNCTION THAT WILL OPEN IT.** `pdf_ocr_job.plan`
    calls `FinancialsBuilder.documents()` here, so the filings in the zip are exactly the
    filings the worker will parse — consolidated preferred, the audited annual standing in for
    Q4, the `allow_parent` fallback if asked. Picking the files by a glob instead would let the
    payload and the worker's own choice diverge, and a worker that cannot find its document
    reports the quarter as `missing`, which is what a genuinely unreadable filing reports too.

    Four things travel and each one is load-bearing:

      * **the PDF index**, because `documents()` re-runs on the worker and reads it;
      * **the chosen filings**;
      * **the twelve charts of accounts**, because `schema_of` raises without them — after the
        OCR (`utils/inputs.py` is named for the 2.4 h that cost once);
      * **the statement CSVs already on disk**, twice over: `seed_history` reconstructs the
        magnitude band `sane` needs from them, and `compare()` scores the run against them cell
        by cell. ⚠️ Without them `sane` FAILS OPEN, which is the documented way a subset run
        writes a wrong figure (§6-2-octodecies), and the run comes home with nothing to be read
        against.

    ⚠️ **AND THE TWO OCR MODELS**, because otherwise the engine downloads them — `det.onnx`
    from HuggingFace and `vgg_seq2seq.pth` from vocr.vn — which a kernel with
    `enable_internet: false` cannot do at all, and a kernel with internet does on every cold
    start.
    """
    from web_scraper import pdf_ocr_job as job
    from web_scraper.cafef_financials import FinancialsBuilder
    from web_scraper.cafef_financials import REPORT_PREFIX, statement_path

    spec = dict(cfg.data.documents or {})
    exchange = str(spec.get("exchange", "HOSE")).upper()
    symbol = str(spec["symbol"]).upper()
    periods = spec.get("periods") or None
    # ⚠️ EMPTY LIST AND ABSENT MEAN THE SAME THING — every year — and both must reach `plan`
    # as None rather than as `[]`, because `[]` is falsy there too but only by accident.
    years = spec.get("years") or None
    allow_parent = bool(spec.get("allow_parent", False))
    period_min = spec.get("period_min", "Q1-2008")
    with_statements = bool(spec.get("with_statements", True))
    with_models = bool(spec.get("with_models", True))

    job.use_data_root()                       # the repo's own raw_data/cafef
    builder = FinancialsBuilder(logger=None)
    tasks = job.plan(builder, exchange, symbol, periods=periods, years=years,
                     allow_parent=allow_parent, period_min=period_min)
    if not tasks:
        raise ValueError(
            f"{exchange}_{symbol} has no filing to ship for periods={periods!r} "
            f"years={years!r} — an empty payload is a run that parses nothing and "
            f"reports success."
        )
    template = tasks[0].template
    root = job.data_root()

    staged: Dict[str, int] = {}
    archive = folder / DOCUMENTS_ARCHIVE
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:

        def _add(path: Path, arcname: str) -> None:
            zf.write(path, arcname)
            staged[arcname] = path.stat().st_size

        index = root / "pdfs" / "index" / f"{exchange}_{symbol}.csv"
        _add(index, f"data/cafef/pdfs/index/{index.name}")

        for task in tasks:
            src = Path(task.path)
            if not src.is_file():
                raise FileNotFoundError(
                    f"{task.period}: {src} is not on disk — scrape it first "
                    f'(dagster asset materialize --select "raw/cafef_pdfs" '
                    f"--partition {exchange}_{symbol})"
                )
            _add(src, f"data/cafef/pdfs/files/{exchange}_{symbol}/{src.name}")

        for schema in sorted((root / "financials" / "schema").glob("*.csv")):
            _add(schema, f"data/cafef/financials/schema/{schema.name}")
        templates = root / "financials" / "templates.csv"
        if templates.is_file():
            _add(templates, "data/cafef/financials/templates.csv")

        baseline: Dict[str, dict] = {}
        if with_statements:
            for report, prefix in REPORT_PREFIX.items():
                path = Path(statement_path(template, report, exchange, symbol))
                if not path.is_file():
                    continue
                _add(path, (f"data/cafef/financials/statements/{template}/{report}/"
                            f"{path.name}"))
                rows = builder._existing(exchange, symbol, template, report)
                baseline[report] = {
                    "rows": len(rows),
                    "pdf_rows": sum(1 for r in rows.values() if r.get("source") == "pdf"),
                    "shipped_periods": {
                        t.period: {"source": rows.get(t.period, {}).get("source", "absent"),
                                   "layer": rows.get(t.period, {}).get("method", "")}
                        for t in tasks
                    },
                }

        models: Dict[str, Optional[str]] = {"det": None, "vietocr": None}
        if with_models:
            det = Path(os.environ.get("CAFEF_ONNX_DET", "")) if os.environ.get(
                "CAFEF_ONNX_DET") else job.DEFAULT_MODELS_DIR / "deepdoc_det.onnx"
            if not det.is_file():
                raise FileNotFoundError(
                    f"no DeepDoc detector at {det}. It is gitignored, so a fresh checkout has "
                    f"none: run the parse once locally (it fetches the model), or set "
                    f"CAFEF_ONNX_DET."
                )
            _add(det, "models/deepdoc_det.onnx")
            models["det"] = det.name

            weights = job.find_vietocr_weights()
            if weights is None:
                raise FileNotFoundError(
                    "no VietOCR checkpoint found. Looked at:\n  "
                    + "\n  ".join(str(p) for p in job.vietocr_weight_candidates())
                    + "\n  Any local run of the onnx engine leaves one in the temp dir; "
                      "otherwise set CAFEF_ONNX_VIETOCR_WEIGHTS."
                )
            _add(weights, "models/vgg_seq2seq.pth")
            models["vietocr"] = weights.name

    manifest["documents"] = {
        "exchange": exchange,
        "symbol": symbol,
        "template": template,
        "allow_parent": allow_parent,
        "period_min": period_min,
        # ⚠️ Recorded even when the job named none, so a reader can tell "every period this
        # ticker files" from "nobody wrote it down" — §5 rule 2 at the manifest.
        "periods_requested": list(periods) if periods else None,
        "years_requested": [int(y) for y in years] if years else None,
        "years_shipped": sorted({int(t.period.split("-")[1]) for t in tasks}),
        "filings": [
            {"period": t.period, "file": t.file, "consolidated": t.consolidated,
             "assurance": t.assurance, "cumulative": t.cumulative,
             "bytes": Path(t.path).stat().st_size}
            for t in tasks
        ],
        "baseline": baseline,
        "models": models,
        "archive": archive.name,
        "files": len(staged),
    }
    if not quiet:
        total = sum(staged.values())
        print(f"  {len(tasks)} filing(s) of {exchange}_{symbol} "
              f"({', '.join(t.period for t in tasks)})")
        print(f"  {DOCUMENTS_ARCHIVE}: {len(staged)} files, "
              f"{archive.stat().st_size / 1024**2:.1f} MB "
              f"({total / 1024**2:.1f} MB uncompressed)")


def export(cfg: JobConfig, quiet: bool = False) -> Path:
    """Read the job's tables out of PostgreSQL and stage the whole payload.

    Returns the payload folder. Raises if the job declares no `data` block —
    a notebook that needs no database needs no payload and should say so by
    omitting it.
    """
    if cfg.data is None:
        raise ValueError(
            f"job {cfg.name!r} has no 'data' block, so there is nothing to export. "
            f"Add one naming the dataset id and the ticker, or run the job as-is."
        )

    _repo_src_on_path()
    # ⚠️ Explicit path, per the repo's standing rule: this module can be invoked
    # from anywhere, and `find_dotenv()` from the wrong CWD gives an empty
    # password and a connection error that blames PostgreSQL.
    from dotenv import load_dotenv

    load_dotenv(REPO_ROOT / ".env")

    tables = cfg.tables()
    folder = cfg.payload_dir
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True)

    manifest: dict = {
        "kgpu_payload_version": 1,
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git_commit": git_commit(),
        "ticker": cfg.data.ticker,
        "job": cfg.name,
        # ⚠️ **WRITTEN FOR EVERY MODE, INCLUDING THE TWO THAT PREDATE IT.** The worker branches
        # on this; before it existed the branch was `if manifest.get("panel")`, i.e. a mode
        # inferred from the presence of a key. A third mode makes that unreadable, and an
        # inferred mode is exactly the kind of thing that keeps working until it does not.
        "mode": cfg.data.mode,
        "tables": {},
    }

    # ⚠️ **DOCUMENTS MODE OPENS NO DATABASE AT ALL** — the PDF parse reads files. Everything
    # below this branch is `UnifiedSchemaReader`, so it must not be entered: on a machine with
    # no `POSTGRES_*` a documents export would otherwise fail on a connection it never needed.
    if cfg.data.is_documents:
        _export_documents(cfg, folder, manifest, quiet=quiet)
        return _finish_payload(cfg, folder, manifest, quiet=quiet)

    from feature_selection.unified_reader import UnifiedSchemaReader

    with UnifiedSchemaReader(cfg.data.ticker) as reader:
        manifest["schema"] = reader.schema
        manifest["database"] = reader.database

        # ⚠️ **PANEL MODE — the join happens HERE because the worker cannot do it.**
        # `read_universe_panel` is one hand-written SQL statement and reaches for
        # `reader.driver`, which `ParquetSchemaReader` answers with "there is no
        # database on a Kaggle worker". No parameter routes around that, so a
        # cross-sectional job ships the FINISHED panel: `cs_rank_{h}day` is derived
        # on the full universe before anything leaves this machine.
        if cfg.data.is_panel:
            _export_panel(cfg, folder, manifest, quiet=quiet)
        else:
            available = set(reader.pools())
            missing = [t for t in tables if t not in available and not t.startswith("_")]
            if missing:
                raise ValueError(
                    f"{reader.schema} has no table(s) {missing}. Materialise them first:\n"
                    f'  dagster asset materialize -f src/orchestration/definitions.py '
                    f'--select "group:unified" --partition {cfg.data.ticker}'
                )

            # ⚠️ The FULL schema overview travels, not just the shipped tables'. The
            # notebook prints it as its orientation table, and an overview showing
            # only what was shipped would hide that 74 other pools exist — the exact
            # information a reader uses to notice a pool was left behind.
            overview = reader.overview()
            overview["shipped"] = overview["table"].isin(tables)
            manifest["overview"] = json.loads(
                overview.to_json(orient="records", date_format="iso")
            )

            for index, table in enumerate(tables, start=1):
                frame = reader.read(table)
                types = reader.column_types(table)
                path = folder / f"{table}.parquet"
                frame.to_parquet(path, index=False)

                manifest["tables"][table] = {
                    "file": path.name,
                    "rows": int(len(frame)),
                    "columns": int(frame.shape[1]),
                    "column_types": types,
                    "first_date": str(frame["date"].min().date()),
                    "last_date": str(frame["date"].max().date()),
                    "bytes": path.stat().st_size,
                }
                if not quiet:
                    # A count of TABLES, not of bytes — one wide pool can be 100x
                    # another, so this is a position in the list, not a time estimate.
                    print(
                        f"  [{index}/{len(tables)} {index / len(tables):>4.0%}] "
                        f"{table:<40} {len(frame):>7,} x {frame.shape[1]:>5}  "
                        f"{path.stat().st_size / 1024**2:>7.1f} MB  "
                        f"-> {frame['date'].max():%Y-%m-%d}",
                        flush=True,
                    )

    # ⚠️ THE CALENDAR CHECK, HERE AS WELL AS IN THE NOTEBOOK. The notebook's guard
    # cell raises on the worker — after the queue, the upload and the startup. The
    # same assertion costs nothing here and fails before anything is spent.
    ends = {t: m["last_date"] for t, m in manifest["tables"].items()}
    if len(set(ends.values())) != 1:
        raise ValueError(
            f"the pools end on different dates {ends}\n"
            f"  The notebook's INNER join would truncate the panel to the earliest "
            f"and every number in the report would silently describe the shorter "
            f"window. Re-materialise the lagging pool before exporting."
        )

    return _finish_payload(cfg, folder, manifest, quiet=quiet)


def _finish_payload(cfg: JobConfig, folder: Path, manifest: dict,
                    quiet: bool = False) -> Path:
    """The half every mode shares: the source zip, the remote files, the manifest.

    Factored out when documents mode arrived (2026-08-28) rather than copied, because these
    five files are the CONTRACT with `kgpu_bootstrap` — it finds the payload by
    `manifest.json` beside `kgpu_remote_reader.py`, so a mode that staged its own tail and
    forgot one would be a payload the worker cannot recognise at all.
    """
    source_counts = _zip_sources(cfg.data.source_dirs, folder / "source.zip")
    manifest["source_dirs"] = source_counts

    for name in REMOTE_FILES:
        shutil.copy2(REMOTE_DIR / name, folder / name)

    (folder / MANIFEST).write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    (folder / "dataset-metadata.json").write_text(
        json.dumps(_dataset_metadata(cfg), indent=2), encoding="utf-8"
    )

    total = sum(p.stat().st_size for p in folder.iterdir() if p.is_file())
    if not quiet:
        print(
            # ⚠️ "files", not ".py" — `SOURCE_INCLUDE` gained `.txt` on 2026-08-28 and this
            # line kept saying `.py`, which is a count that describes the wrong thing.
            f"  source.zip: {sum(source_counts.values())} files from "
            f"{', '.join(source_counts)}"
        )
        print(f"staged payload -> {folder}  ({total / 1024**2:.1f} MB total)")
    return folder


def load_manifest(cfg: JobConfig) -> dict:
    path = cfg.payload_dir / MANIFEST
    if not path.exists():
        raise FileNotFoundError(
            f"no staged payload for job {cfg.name!r} ({path}).\n"
            f"  Run: python -m kgpu data {cfg.name}"
        )
    return json.loads(path.read_text(encoding="utf-8"))


def payload_hash(cfg: JobConfig) -> str:
    return _payload_hash(cfg.payload_dir)


def upload_record(cfg: JobConfig) -> dict | None:
    """What `kgpu data` recorded about the last upload of THIS job to THIS dataset.

    ⚠️ **THE RECORD IS KEYED BY JOB NAME AND THE DATASET IS NOT** — so a job that keeps its
    name and changes `data.id` would read the previous dataset's record and believe itself
    uploaded. `ensure_uploaded` then passes (the payload hash matches, because the payload
    really is the same files) and the kernel mounts a dataset that does not exist, which
    surfaces on Kaggle rather than here. Found 2026-08-29 when `kgpu.pdf_ocr` began deriving
    both names from the filter: the two moved independently for the first time.
    """
    path = cfg.payload_dir / "uploaded.json"
    if not path.exists():
        return None
    record = json.loads(path.read_text(encoding="utf-8"))
    if cfg.data is not None and record.get("dataset") != cfg.data.id:
        return None                       # a record for a different dataset is not a record
    return record


def write_upload_record(cfg: JobConfig, version: str | int | None) -> dict:
    record = {
        "dataset": cfg.data.id if cfg.data else None,
        "version": version,
        "payload_hash": payload_hash(cfg),
        "tables": cfg.tables(),
        "uploaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    (cfg.payload_dir / "uploaded.json").write_text(
        json.dumps(record, indent=2), encoding="utf-8"
    )
    return record
