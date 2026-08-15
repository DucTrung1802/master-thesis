# src\kaggle_gpu\kgpu\notebook.py
"""Build the notebook that is uploaded — your notebook, unedited, plus two cells.

⚠️ **THE SOURCE NOTEBOOK IS NEVER MODIFIED.** `RUN__feature_importance_report.ipynb`
stays exactly as it is on disk and keeps working locally; what goes to Kaggle is a
copy in `.build/`. Three transformations, in this order:

1. **Outputs stripped.** The report notebook is 1.2 MB of stored figures and
   frames; the same notebook without outputs is ~60 KB. That is uploaded on every
   push, and a version history of 1.2 MB blobs is a version history nobody reads.

2. **Parameters patched IN PLACE, in the cell that already defines them.**

   ⚠️ **NOT appended as an override cell after it, which is the obvious design and
   is wrong here.** The parameter cell ends with
   `EXCLUDE = IDENTITY + [c for c in ALL_TARGETS if c != TARGET]` — a DERIVED value.
   Override `TARGET` in a later cell and `EXCLUDE` still excludes the *old* target,
   which means the run's own label is handed to `FeatureSelector` as a candidate
   feature. It does not raise. It reports an IC near 1 and looks like a discovery.
   Rewriting the assignment where it stands leaves every derived line correct.

3. **Two cells injected**: `kgpu_bootstrap.setup()` at the top, and a cleanup at
   the bottom that removes the mirrored source so the download holds results only.
   Both are no-ops off Kaggle, so the built notebook still runs locally.

The patcher works on the AST, not on text. A regex for `^TARGET = ` would also
match `TARGET = ...` inside a docstring or a comment block, and this notebook is
two-thirds prose.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import nbformat

from .config import BUILD_DIR, JobConfig

BOOTSTRAP_TEMPLATE = '''\
# ─── injected by kgpu — do not edit here; edit src/kaggle_gpu/kaggle_config.json ───
# Makes a Kaggle worker look like the repo: unpacks the shipped source, stubs the
# database modules and swaps UnifiedSchemaReader for a parquet-backed subclass.
# Off Kaggle this is a no-op, so this notebook still runs locally as it is.
#
# ⚠️ RAISES when the job ships data and none is mounted. Falling through to the
# "plain local notebook" branch on a WORKER is how a missing dataset turned into
# a RuntimeError six cells later, about a repo root, saying nothing about the
# mount — measured 2026-08-15, one wasted run.
import glob
import os
import sys

# ⚠️ `/kaggle/input/datasets/<owner>/<slug>/`, NOT `/kaggle/input/<slug>/` —
# measured 2026-08-15. The depth is not fixed, so match on the file, not the path.
# KGPU_INPUT_DIR is the seam `python -m kgpu rehearse` uses to run THIS cell
# against a staged payload; unset, it is `/kaggle/input` and this is the worker.
_input = os.environ.get("KGPU_INPUT_DIR", "/kaggle/input")
_found = sorted(
    p
    for depth in ("*", "*/*", "*/*/*", "*/*/*/*")
    for p in glob.glob(f"{{_input}}/{{depth}}/kgpu_bootstrap.py")
)
_on_kaggle = os.path.isdir("/kaggle/working") or bool(os.environ.get("KGPU_INPUT_DIR"))
if _found:
    sys.path.insert(0, os.path.dirname(_found[0]))  # not rsplit("/") — see rehearse
    import kgpu_bootstrap

    kgpu_bootstrap.setup(git_commit={git_commit!r})
elif _on_kaggle and {require!r}:
    _tree = []
    for _root, _dirs, _files in os.walk(_input):
        if _root.count("/") > 6:
            _dirs[:] = []
            continue
        _tree.append(f"    {{_root}}: {{sorted(_dirs)[:8]}} {{sorted(_files)[:8]}}")
    raise RuntimeError(
        "kgpu: this job ships a payload dataset and none is mounted.\\n"
        + "\\n".join(_tree[:20])
        + "\\n  Check the kernel's dataset_sources and that the dataset is READY."
    )
elif _on_kaggle:
    print("kgpu: no payload dataset — this job ships none")
else:
    print("kgpu: not on Kaggle — running against the local repo and database")
'''

CLEANUP_TEMPLATE = '''\
# ─── injected by kgpu ───
# The shipped source was mirrored into /kaggle/working so the notebook could find
# the repo. /kaggle/working is also what gets downloaded, so drop it again — the
# only thing worth carrying home is what the run wrote.
import shutil
from pathlib import Path

for _stale in (Path("/kaggle/working/src"),):
    if _stale.is_dir():
        shutil.rmtree(_stale, ignore_errors=True)
        print(f"kgpu: removed {_stale} from the output")
'''


class ParameterPatchError(RuntimeError):
    """A parameter could not be patched — never patched silently, never skipped."""


def _find_assignments(source: str, names: set) -> Dict[str, List[Tuple[int, int, bool]]]:
    """`{name: [(start_line, end_line, is_sole_target), ...]}`, 1-indexed, top level.

    Only module-level statements count. A name bound inside an `if` or a function
    is not the parameter cell's knob and rewriting it would change behaviour
    somewhere the config author never looked.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:  # a cell that is not valid Python holds no parameters
        return {}

    found: Dict[str, List[Tuple[int, int, bool]]] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        sole = len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
        targets: List[ast.Name] = []
        for target in node.targets:
            if isinstance(target, ast.Name):
                targets.append(target)
            elif isinstance(target, (ast.Tuple, ast.List)):
                targets += [e for e in target.elts if isinstance(e, ast.Name)]
        for target in targets:
            if target.id in names:
                found.setdefault(target.id, []).append(
                    (node.lineno, node.end_lineno or node.lineno, sole)
                )
    return found


def _trailing_comment(line: str) -> str:
    """The `# …` at the end of a single-line statement, or ''.

    ⚠️ Tokenised, not split on `#`. `NORMALIZE = "none"  # "none" | "zscore"` and
    `SEP = "#"` are the same character to a string search, and the parameter cell
    of the report notebook carries a `⚠️` comment on half its knobs — dropping
    them on exactly the lines the config changed is how a warning stops travelling
    with the thing it warns about.
    """
    import io
    import tokenize

    try:
        for token in tokenize.generate_tokens(io.StringIO(line).readline):
            if token.type == tokenize.COMMENT:
                return "  " + token.string
    except (tokenize.TokenError, IndentationError, SyntaxError):
        pass
    return ""


def patch_parameters(source: str, overrides: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    """Rewrite the top-level assignments named in `overrides`. Returns (src, applied).

    A name assigned by a tuple unpacking (`N_SPLITS, MIN_TRAIN = 5, 500`) cannot
    have its statement replaced without rewriting the other half, so the override
    is inserted on the line AFTER it — still inside the same cell, still ahead of
    anything derived from it.
    """
    if not overrides:
        return source, {}

    found = _find_assignments(source, set(overrides))
    if not found:
        return source, {}

    lines = source.splitlines()
    applied: Dict[str, str] = {}

    # ⚠️ **GROUPED BY STATEMENT, NOT ONE EDIT PER NAME.** A tuple-target override is
    # an INSERT after its statement, and an insert positioned at the following
    # statement's first line collides with that statement's own replacement —
    # whichever is applied second overwrites the first, silently. Measured on
    # `N_SPLITS, MIN_TRAIN = 5, 500` followed by `DEVICE = …`: the N_SPLITS override
    # was consumed and DEVICE kept its old value while both were REPORTED as
    # patched. One replacement per statement range makes the edits disjoint.
    plan: Dict[Tuple[int, int], Dict[str, object]] = {}
    for name, spans in found.items():
        # The LAST binding wins — it is the one in force when the cell ends.
        start, end, sole = spans[-1]
        comment = _trailing_comment(lines[end - 1]) if start == end else ""
        literal = f"{name} = {overrides[name]!r}{comment}"
        entry = plan.setdefault((start, end), {"replace": None, "appends": []})
        if sole:
            entry["replace"] = literal
        else:
            entry["appends"].append(literal)  # type: ignore[union-attr]
        applied[name] = literal
        if len(spans) > 1:
            applied[name] += f"   (⚠️ {len(spans)} top-level bindings; patched the last)"

    for (start, end), entry in sorted(plan.items(), reverse=True):
        head = [entry["replace"]] if entry["replace"] else lines[start - 1 : end]
        lines[start - 1 : end] = list(head) + list(entry["appends"])  # type: ignore[arg-type]

    patched = "\n".join(lines)
    if source.endswith("\n"):
        patched += "\n"
    ast.parse(patched)  # a patch that does not parse must not reach the GPU
    return patched, applied


def build_notebook(cfg: JobConfig, git_commit: str | None = None) -> Path:
    """Stage `.build/<notebook>.ipynb` — stripped, patched, bootstrapped."""
    nb = nbformat.read(str(cfg.notebook_path), as_version=4)

    for cell in nb.cells:
        if cell.get("cell_type") == "code":
            cell["outputs"] = []
            cell["execution_count"] = None
            cell.get("metadata", {}).pop("execution", None)

    applied: Dict[str, str] = {}
    remaining = dict(cfg.parameters)
    for cell in nb.cells:
        if cell.get("cell_type") != "code" or not remaining:
            continue
        patched, done = patch_parameters(cell["source"], remaining)
        if done:
            cell["source"] = patched
            applied.update(done)
            for name in done:
                remaining.pop(name, None)

    if remaining:
        raise ParameterPatchError(
            f"job {cfg.name!r}: no top-level assignment of "
            f"{sorted(remaining)} in {cfg.notebook_path.name}.\n"
            f"  A parameter that is not found is a parameter that changes nothing, "
            f"so this refuses to push rather than run the notebook's own defaults.\n"
            f"  Check the spelling against the notebook's parameter cell."
        )

    nb.cells.insert(
        0,
        nbformat.v4.new_code_cell(
            BOOTSTRAP_TEMPLATE.format(
                git_commit=git_commit, require=cfg.data is not None
            )
        ),
    )
    nb.cells.append(nbformat.v4.new_code_cell(CLEANUP_TEMPLATE))

    # ⚠️ Kaggle runs the notebook with its own kernel; a kernelspec naming a local
    # venv is at best ignored and at worst rejected.
    nb.metadata["kernelspec"] = {
        "name": "python3",
        "display_name": "Python 3",
        "language": "python",
    }

    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    out = cfg.built_notebook
    nbformat.write(nb, str(out))

    # Fail here rather than after a slow upload.
    json.loads(out.read_text(encoding="utf-8"))
    return out


def describe_patches(cfg: JobConfig) -> Dict[str, str]:
    """The patches `build_notebook` would apply — for the dry-run plan."""
    nb = nbformat.read(str(cfg.notebook_path), as_version=4)
    applied: Dict[str, str] = {}
    remaining = dict(cfg.parameters)
    for cell in nb.cells:
        if cell.get("cell_type") != "code" or not remaining:
            continue
        _, done = patch_parameters(cell["source"], remaining)
        applied.update(done)
        for name in done:
            remaining.pop(name, None)
    for name in remaining:
        applied[name] = "⚠️ NOT FOUND — push will refuse"
    return applied
