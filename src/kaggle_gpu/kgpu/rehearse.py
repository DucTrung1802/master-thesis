# src\kaggle_gpu\kgpu\rehearse.py
"""Run the WORKER side, here, against the staged payload. No network, no quota.

⚠️ **THIS IS THE CHEAPEST TEST IN THE LOOP AND THE ONLY ONE THAT COVERS THE
INTEGRATION.** Everything `kgpu` adds happens in the first cell of a notebook
running on a machine that has no repo and no database: an unpacked source tree,
nine stub modules, a swapped reader class and a report root that must resolve
inside the collected output directory. Every one of those fails at import time,
which on Kaggle means: queue, start, fail, read the log, fix, push again — and
`ModuleNotFoundError` after ten minutes of queue costs the same as a real run.

Run as a SUBPROCESS with the working directory set to the rehearsal folder, so
the repo's own `src/` is not importable. If it were, `import feature_selection`
would resolve to the local package rather than the unpacked one, the report root
would anchor to the real repo, and the rehearsal would pass by testing nothing.

⚠️ **IT EXECUTES THE BUILT NOTEBOOK'S OWN FIRST CELL**, not a re-creation of it,
against a fake `/kaggle/input` in the shape Kaggle actually mounts. Both defects
this integration has had were in payload discovery and unpacking — the mount is
`/kaggle/input/datasets/<owner>/<slug>/` and Kaggle deletes `source.zip` after
extracting it — and a rehearsal that called `setup(payload_dir=…)` directly would
have missed both while reporting a pass.

    python -m kgpu rehearse feature-selection
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def _run_injected_cell(notebook: str) -> None:
    """Execute cell 0 of the built notebook — the real bootstrap, verbatim."""
    nb = json.loads(Path(notebook).read_text(encoding="utf-8"))
    source = "".join(nb["cells"][0]["source"])
    if "kgpu_bootstrap" not in source:
        raise AssertionError(
            f"cell 0 of {notebook} is not the injected bootstrap — rebuild it."
        )
    exec(compile(source, "<kgpu-injected-cell>", "exec"), {"__name__": "__main__"})


def main(payload_dir: str, work_dir: str, notebook: str | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):  # Windows/cp1252 on a redirected pipe
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass

    input_root = Path(payload_dir)  # the fake /kaggle/input
    work = Path(work_dir)
    os.environ["KGPU_INPUT_DIR"] = str(input_root)
    os.environ["KGPU_WORK_DIR"] = str(work)

    # 0. Discovery + the whole bootstrap, through the notebook's own first cell.
    _run_injected_cell(notebook) if notebook else None

    import kgpu_bootstrap

    payload = kgpu_bootstrap.find_payload()
    if payload is None:
        raise AssertionError(f"find_payload() found nothing under {input_root}")
    if notebook is None:  # no built notebook to drive it — bootstrap directly
        kgpu_bootstrap.setup()
    print(f"  payload discovered           : OK ({payload.relative_to(input_root)})")

    report = {"git_commit": None}
    manifest_probe = json.loads((payload / "manifest.json").read_text(encoding="utf-8"))
    report["git_commit"] = manifest_probe.get("git_commit")

    # ⚠️ **A DOCUMENTS PAYLOAD SHIPS NO `feature_selection` AT ALL**, so the three checks below
    # would fail on the import rather than on what they test. Everything above this line —
    # discovery, the source unpack, the stubs — is shared and is where every defect this
    # integration has had actually lived.
    if manifest_probe.get("mode") == "documents":
        return _rehearse_documents(payload, work, manifest_probe)

    # 1. The report root must land inside the directory Kaggle collects. If this
    #    resolves anywhere else, the run completes and downloads nothing.
    from feature_selection import report as fs_report

    root = Path(fs_report.DEFAULT_REPORT_ROOT).resolve()
    if work.resolve() not in root.parents:
        raise AssertionError(
            f"report root {root} is not under the collected output {work} — the "
            f"run would write its folder somewhere Kaggle never downloads."
        )
    print(f"\n  report root under output dir : OK ({root})")

    # 2. The provenance the worker cannot compute for itself.
    # ⚠️ **A MISMATCH HERE IS A STALE PAYLOAD, NOT AN UNPINNED COMMIT — and the old
    # message said the second, which cost a minute of looking in the wrong place
    # (2026-08-18).** The bootstrap pins the commit the NOTEBOOK was built at, while the
    # manifest carries the one the PAYLOAD was exported at. They agree only while
    # `source.zip` is the working tree, so a disagreement means the rehearsal is about to
    # exercise code that is not the code on disk — which is the one thing a rehearsal
    # must never do quietly.
    if fs_report._git_commit() != report.get("git_commit"):
        raise AssertionError(
            f"the build pinned {fs_report._git_commit()!r} but the staged payload was "
            f"exported at {report.get('git_commit')!r} — THE PAYLOAD PREDATES THE "
            f"WORKING TREE, so its source.zip is older code.\n"
            f"  Re-stage it: python -m kgpu export <job>  (or `data`, which uploads too)"
        )
    print(f"  git commit pinned            : OK ({fs_report._git_commit()})")

    # 3. The reader: the notebook's own construction, guard cell and join.
    from feature_selection.unified_reader import UnifiedSchemaReader

    if UnifiedSchemaReader.__name__ != "ParquetSchemaReader":
        raise AssertionError("UnifiedSchemaReader was not swapped for the parquet one")

    manifest = json.loads((payload / "manifest.json").read_text(encoding="utf-8"))

    # ⚠️ **A PANEL PAYLOAD IS A DIFFERENT WORKER SIDE, AND THE MANIFEST SAYS WHICH.**
    # A cross-sectional job ships ONE finished `panel.parquet` because
    # `read_universe_panel` needs a database the worker does not have (`CSP-1` in its
    # second form), so there are no pools to join and `reader.join` is not the code
    # under test. Everything above this line — discovery, unpacking, the stubs, the
    # report root, the pinned commit — is shared, and it is where every defect this
    # integration has had actually lived.
    if manifest.get("panel"):
        return _rehearse_panel(payload, manifest)
    return _rehearse_pools(payload, manifest)


def _rehearse_documents(payload, work, manifest: dict) -> int:
    """The PDF-parse job: the filings, the charts of accounts and the models all arrived.

    ⚠️ **THE POINT IS THE INPUTS, NOT A PARSE.** OCR is the expensive step and the reason the
    job goes to a GPU at all, so this rehearsal deliberately does not run one. What it does
    check is every input whose absence would only show up AFTER the OCR: a missing chart of
    accounts makes `schema_of` raise ~2.4 h in (`utils/inputs.py` is named for it), a missing
    statement CSV makes `sane` fail open, and a missing checkpoint turns into a download on a
    worker that may have no internet.
    """
    import os
    from pathlib import Path

    from web_scraper import pdf_ocr_job as job
    from web_scraper.cafef_financials import FinancialsBuilder

    spec = manifest["documents"]
    root = Path(os.environ["CAFEF_DATA_ROOT"])
    print(f"\n  data root                    : OK ({root})")

    # ⚠️ **NOT `str(work) in str(root)` — the payload is unpacked OUTSIDE the collected output
    # on purpose.** ~100 MB of filings and checkpoints under /kaggle/working would be
    # downloaded home again by `kgpu pull` for nothing. What must land inside `work` is the RUN
    # FOLDER, and that is what is asserted.
    out = Path(job.DEFAULT_OUT_ROOT).resolve()
    if Path(work).resolve() not in out.parents:
        raise AssertionError(
            f"the run folder root {out} is not under the collected output {work} — the run "
            f"would write its results somewhere Kaggle never downloads."
        )
    print(f"  run folder root under output : OK ({out})")

    job.use_data_root(root)
    models = job.use_models()
    for name, path in models.items():
        if path is None:
            # ⚠️ TWO DIFFERENT CONSEQUENCES, ONE ASSERTION — because both are invisible on the
            # worker. An absent onnx file is a DOWNLOAD a kernel with enable_internet: false
            # cannot make; an absent `vie.traineddata` is a SMALLER CASCADE, executed in
            # silence, and that one produced a real disagreement between the two machines.
            raise AssertionError(
                f"the {name} file ({job.MODEL_FILES[name]}) did not travel — the worker would "
                f"either DOWNLOAD it, which a kernel with enable_internet: false cannot do, or "
                f"run a SMALLER cascade: without the language model the two tesseract layers "
                f"are dropped and the worker executes 53 of the 55 layers this machine does "
                f"(CLAUDE.md §6-2-quinquagies)."
            )
    print(f"  models                       : OK ({', '.join(Path(p).name for p in models.values())})")

    # ⚠️ **THE CASCADE'S OWN QUESTION, ASKED HERE WHERE IT IS FREE.** `_parse_cascaded` skips a
    # layer whose engine is not ready, so "the file travelled" and "the layer will run" are two
    # facts and only the second one matters. The md5 is what makes the two machines comparable:
    # the ENGINE is embedded in a pinned pymupdf, so the language model is the only half that
    # can differ, and a version number cannot tell two builds of it apart.
    tess = job.tesseract_report()
    # ⚠️ **THREE STATES, NOT TWO, AND THE THIRD ONE FIRED ON ITS FIRST RUN.** A rehearsal
    # executes the PAYLOAD's copy of the repo, so a payload staged before `TESSDATA_MD5`
    # existed reports no `matches_pin` at all — and reading that absence as False printed
    # "NOT the pinned model" over the pinned model. §5 rule 2 at a progress line: an absent
    # measurement is absent, never a failing one.
    _PIN = {True: "", False: " ⚠️ NOT the pinned model",
            None: " (this payload's code predates the pin)"}
    model = tess.get("language_model") or {}
    print(f"  tesseract layers             : "
          f"{'READY' if tess.get('ready') is True else 'NOT READY — 53 of 55 layers'}"
          + (f" ({job.MODEL_FILES['tessdata']} md5 {model['md5']}{_PIN[model.get('matches_pin')]}"
             f", {model['bytes'] / 1024 ** 2:.1f} MB)" if model else ""))

    builder = FinancialsBuilder(logger=None)
    # ⚠️ **EVERY FILTER THE EXPORT APPLIED, OR THIS CHECK CANNOT PASS.** It read only
    # `periods_requested` until 2026-08-29, so a payload narrowed by the BATCH filter compared
    # a filtered shipment against an unfiltered `documents()` and always tripped the assertion
    # below. That was latent for as long as the batch filter existed — `years` had the same
    # defect — because every job rehearsed until then was narrowed by `periods` alone.
    tasks = job.plan(builder, spec["exchange"], spec["symbol"],
                     periods=spec.get("periods_requested"),
                     quarters=spec.get("quarters_requested"),
                     allow_parent=spec.get("allow_parent", False),
                     period_min=spec.get("period_min"))
    shipped = {f["file"] for f in spec["filings"]}
    chosen = {t.file for t in tasks}
    if chosen != shipped:
        raise AssertionError(
            f"the worker's own `documents()` chose {sorted(chosen)} and the payload ships "
            f"{sorted(shipped)} — a filing that is not on disk reads as `missing`, which is "
            f"exactly what an unreadable one reads as."
        )
    for task in tasks:
        if not os.path.isfile(task.path):
            raise AssertionError(f"{task.period}: {task.path} is not in the payload")
        print(f"  {task.period:<8} {task.file[:52]:<52} "
              f"{os.path.getsize(task.path) / 1024**2:>6.1f} MB")

    schema = builder.schema_of(tasks[0].template, "balance_sheet")
    print(f"  chart of accounts            : OK ({len(schema)} lines, "
          f"{tasks[0].template})")

    for task in tasks:
        history = job.seed_history(builder, task.exchange, task.symbol, task.template,
                                   before=task.period)
        sizes = {r: len(v.get(task.consolidated, [])) for r, v in history.items()}
        if not any(sizes.values()):
            print(f"  WARNING {task.period}: the magnitude band is EMPTY — `sane` will fail "
                  f"open, which is how a subset run writes a wrong figure (SAN-1).")
        else:
            print(f"  {task.period} magnitude band       : "
                  + ", ".join(f"{r.split('_')[0]} {n}" for r, n in sizes.items()))

    print(f"  cascade                      : {len(FinancialsBuilder.LAYERS)} layers")
    print("\nrehearsal passed — the worker side of this payload has every input the parse "
          "reads.\n⚠️ It did NOT run an OCR pass: that is what the GPU is for.")
    return 0


def _rehearse_panel(payload, manifest: dict) -> int:
    """The panel job: the shipped frame loads, agrees with its manifest, and selects.

    ⚠️ **CONSTRUCTING THE SELECTOR IS THE POINT, not a formality.** It is where the
    panel is sorted `(date, ticker)` — a full copy of a ~1.6 GB frame — and where the
    exclude list, the by-date CV and the `cs_` target all have to agree. Failing here
    costs seconds; failing there costs the queue, the upload and the GPU session.
    """
    import kgpu_remote_reader

    from feature_selection import cross_sectional, windows
    from feature_selection.cross_sectional import CrossSectionalSelector
    from feature_selection.run import ProvidedPanel

    spec = manifest["panel"]
    provided = kgpu_remote_reader.load_panel(payload)
    if not isinstance(provided, ProvidedPanel):
        raise AssertionError("load_panel did not return a run.ProvidedPanel")
    panel = provided.frame

    horizon = int(spec["horizons"][0])
    target = f"cs_rank_{horizon}day"
    if target not in panel.columns:
        raise AssertionError(
            f"the shipped panel has no {target} — export derives it from "
            f"return_{horizon}day, so the horizons in data.panel and the notebook's "
            f"TARGET disagree. Columns like it: "
            f"{[c for c in panel.columns if c.startswith('cs_rank')]}"
        )
    print(
        f"  panel loaded                 : {len(panel):,} x {panel.shape[1]} "
        f"({panel['date'].min():%Y-%m-%d} -> {panel['date'].max():%Y-%m-%d})"
    )
    print(f"  shape agrees with manifest   : OK ({spec['top_n']} names asked)")
    print(
        f"  universe                     : {panel['ticker'].nunique()} tickers, "
        f"ranked by {spec['liquidity_rule']} before {spec['liquidity_before']}"
    )
    print(f"  cs_rank scope                : {spec['cs_rank_scope']}")

    # ⚠️ Without this map `outstanding` cannot name a channel's pool,
    # `contract.validate_shortlist` refuses the shortlist, and the run comes home
    # invisible to `final_features` — the 2026-08-15 defect, which passed every check
    # that existed at the time.
    mapped = {c for cols in provided.columns_by_table.values() for c in cols}
    unmapped = [
        c
        for c in panel.columns
        if c not in mapped
        and not c.startswith(("cs_rank_", "return_"))
        and c not in ("date", "exchange", "ticker")
    ]
    if unmapped:
        raise AssertionError(
            f"{len(unmapped)} panel column(s) belong to no pool in "
            f"columns_by_table: {unmapped[:8]} — a shortlist naming one of them "
            f"would be refused by contract.validate_shortlist."
        )
    print(
        "  channel -> pool map          : OK ("
        + ", ".join(f"{t} {len(c)}" for t, c in provided.columns_by_table.items())
        + ")"
    )

    summary = cross_sectional.panel_summary(panel, target)
    print(
        f"  labelled                     : {summary['labelled_rows']:,} rows over "
        f"{summary['dates']:,} dates -> n_eff = {summary['dates'] / horizon:.0f}"
    )

    CrossSectionalSelector(
        panel=panel,
        target=target,
        horizon=horizon,
        lookback=20,
        window_stats=windows.WINDOW_STATS,
        device="cpu",
    )
    print("  CrossSectionalSelector       : OK")
    print("\nrehearsal passed — the worker side of this panel loads and selects.")
    return 0


def _rehearse_pools(payload, manifest: dict) -> int:
    """The pool job: the shipped pools share a calendar, join, and build a selector."""
    from feature_selection.unified_reader import UnifiedSchemaReader

    pools = [t for t in manifest["tables"] if t != "pool__targets"]
    pools.append("pool__targets")

    with UnifiedSchemaReader(manifest["ticker"]) as reader:
        spans = {t: reader.read(t)["date"].agg(["min", "max", "count"]) for t in pools}
        labels = [
            c
            for c in reader.column_types("pool__targets")
            if c not in ("date", "exchange", "ticker")
        ]
        overview = reader.overview()

    for table, span in spans.items():
        print(
            f"  {table:<44} {int(span['count']):>7,} rows  "
            f"{span['min']:%Y-%m-%d} .. {span['max']:%Y-%m-%d}"
        )
    ends = {t: s["max"] for t, s in spans.items()}
    if len(set(ends.values())) != 1:
        raise AssertionError(f"pools end on different dates {ends}")
    print("  one calendar                 : OK")
    print(f"  labels in pool__targets      : {labels}")
    print(
        f"  schema overview              : {len(overview)} pools, "
        f"{int(overview['shipped'].sum())} shipped"
    )

    reader = UnifiedSchemaReader(manifest["ticker"]).connect()
    panel = reader.join(pools)
    join_log = reader.join_log
    reader.close()
    print(
        f"  joined panel                 : {panel.shape[0]:,} x {panel.shape[1]} "
        f"({panel['date'].min():%Y-%m-%d} -> {panel['date'].max():%Y-%m-%d})"
    )
    for entry in join_log:
        print(
            f"    + {entry['table']:<28} on {entry['join_keys']:<24} "
            f"{entry['rows_before']:,} -> {entry['rows_after']:,} rows, "
            f"+{entry['columns_added']} columns"
        )
    if panel.empty:
        raise AssertionError("the join produced 0 rows")

    # 4. The selector imports and constructs — torch, xgboost and sklearn are all
    #    reachable and the constructor's own validation passes. NOT run: the
    #    selection itself is what the GPU is for.
    from feature_selection import windows
    from feature_selection.selector import FeatureSelector

    FeatureSelector(
        panel=panel,
        target=labels[0],
        exclude=[c for c in labels[1:]],
        horizon=5,
        lookback=20,
        window_stats=windows.WINDOW_STATS,
        device="cpu",
    )
    print("  FeatureSelector constructs   : OK")

    print("\nrehearsal passed — the worker side of this payload imports and reads.")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:4]))
