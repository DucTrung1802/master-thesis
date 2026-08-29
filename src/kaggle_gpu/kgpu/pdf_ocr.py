"""Build a PDF-OCR `JobConfig` from parameters instead of from `kaggle_config.json`.

⚠️ **WHY THIS EXISTS.** Every other `kgpu` job is a fixed experiment: one universe, one
horizon, one question, written down once and re-run to reproduce a number. The PDF parse is
not that — it is the same procedure applied to **784 tickers × ~70 quarters**, and the only
thing that changes between two runs is *which filings*. Adding a hand-written block to
`kaggle_config.json` per ticker-and-quarter would put thousands of near-identical jobs in a
file whose whole value is that a reader can see the experiments at a glance.

So the config is COMPUTED here and validated by exactly the same `_validate` that a file-borne
job goes through — the guards are not bypassed, only the typing is. `PDF_OCR.md` beside this
file is the guide; `RUN__pdf_ocr_control.ipynb` is the notebook that drives it.

⚠️ **THE NAMES ARE DERIVED FROM THE FILTER, AND THAT IS LOAD-BEARING.** `cfg.name` decides the
payload directory, the rehearsal directory and the Kaggle kernel slug. Two runs sharing a name
share all three: the second overwrites the first's payload, and its kernel REPLACES the first
on Kaggle. Deriving the name from `symbol + scope` means changing a parameter changes the
job, which is what makes "edit the cell and run it again" safe.
"""
from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Sequence

from .config import DataConfig, JobConfig, _validate, load_credentials

# The WORKER notebook — the one that runs on Kaggle. Not the control notebook that calls this.
NOTEBOOK = "src/web_scraper/RUN__pdf_ocr.ipynb"
RESULTS_INTO = "reports/pdf_ocr"
SOURCE_DIRS = ["src/web_scraper", "src/utils"]

# Kaggle rejects a dataset title outside 6-50 characters, and it says so only AFTER the whole
# payload has been uploaded (`config._validate` carries the measurement). Every title this
# module builds is checked against it before anything is spent.
TITLE_MAX = 50


def kaggle_user() -> str:
    """The Kaggle username of the credentials actually in use.

    ⚠️ **RESOLVED, NOT ASKED FOR.** A username typed into a notebook is a username that can
    disagree with the token beside it, and that failure arrives as a 403 from the upload —
    after the payload has been built — rather than as "wrong username".

    ⚠️ **AND IT IS NOT ALWAYS IN THE ENVIRONMENT.** `load_credentials` accepts three shapes:
    `KAGGLE_USERNAME`+`KAGGLE_KEY`, a `KAGGLE_API_TOKEN`, or `~/.kaggle/kaggle.json`. Only the
    first puts a username in `os.environ`; this machine uses the second, where the name lives
    inside the API client's own config. Reading only the env var would have worked here in
    testing and failed on any machine using a token.
    """
    load_credentials()
    user = os.environ.get("KAGGLE_USERNAME")
    if user:
        return user
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi

        api = KaggleApi()
        api.authenticate()
        user = (getattr(api, "config_values", {}) or {}).get("username")
    except Exception:                       # noqa: BLE001 — any auth failure is "no name"
        user = None
    if not user:
        raise RuntimeError(
            "cannot determine the Kaggle username. Set KAGGLE_USERNAME, or put a valid "
            "KAGGLE_API_TOKEN / kaggle.json where `load_credentials` looks, or pass "
            "`user=` explicitly."
        )
    return user


def _slug(text: str) -> str:
    """Kaggle's own rule for a slug: lower case, everything else to `-`, collapsed."""
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", text.lower())).strip("-")


def scope_of(periods: Optional[Sequence[str]], years: Optional[Sequence[int]]) -> str:
    """A short, PREDICTABLE name for the filter — the half of the job name that varies.

    Predictable matters more than short: a reader looking at `reports/pdf_ocr/` or at the
    Kaggle kernel list has to be able to work out which run produced what, and a hash would
    make every one of those lookups a search.

        periods=["Q3-2014"]                  -> "q3-2014"
        years=[2014]                         -> "2014"
        years=[2013, 2014]                   -> "2013-2014"
        years=[2010 … 2020]                  -> "2010-2020"      (a RANGE, not a list)
        periods=["Q1-2014", "Q3-2014"]       -> "2p"
        neither                              -> "all"
    """
    if periods and len(periods) == 1:
        return _slug(periods[0])
    if years:
        ys = sorted({int(y) for y in years})
        if len(ys) == 1:
            return str(ys[0])
        if len(ys) == 2:
            return f"{ys[0]}-{ys[1]}"
        return f"{ys[0]}-{ys[-1]}"          # ⚠️ a RANGE even when the list is sparse
    if periods:
        return f"{len(periods)}p"
    return "all"


def job(
    symbol: str,
    *,
    exchange: str = "HOSE",
    periods: Optional[Sequence[str]] = None,
    years: Optional[Sequence[int]] = None,
    allow_parent: bool = False,
    template: Optional[str] = None,
    layers: Optional[Sequence[str]] = None,
    compare: bool = True,
    align_torch: bool = False,
    notes: str = "",
    merge_statements: bool = True,
    scope: Optional[str] = None,
    user: Optional[str] = None,
    name: Optional[str] = None,
) -> JobConfig:
    """One PDF-OCR job, validated exactly as a `kaggle_config.json` job is.

    ⚠️ **`periods` AND `years` ARE WRITTEN TWICE ON PURPOSE** — into `data.documents`, which
    decides which filings are UPLOADED, and into `parameters`, which decides which the worker
    OPENS. They are built from the same arguments here, and `_validate` still checks the two
    against each other: this function is not the only way a job can be built, and the check
    costs nothing.

    ⚠️ **AN EMPTY LIST MEANS EVERY YEAR, NEVER NONE.** That is `plan()`'s contract and it is
    preserved here: `years=[]` and `years=None` produce the same job.
    """
    symbol = symbol.upper()
    exchange = exchange.upper()
    periods = list(periods) if periods else None
    years = [int(y) for y in years] if years else None
    scope = scope or scope_of(periods, years)
    user = user or kaggle_user()

    name = name or f"pdf-ocr-{_slug(symbol)}-{scope}"
    kernel_title = f"MT pdf ocr {symbol} {scope}"
    dataset_title = f"MT CafeF filings {symbol} {scope}"
    for what, title in (("kernel", kernel_title), ("dataset", dataset_title)):
        if not 6 <= len(title) <= TITLE_MAX:
            raise ValueError(
                f"{what} title is {len(title)} characters ({title!r}); Kaggle requires "
                f"6-{TITLE_MAX}. Pass a shorter `scope=`."
            )

    documents: Dict[str, Any] = {
        "exchange": exchange,
        "symbol": symbol,
        "allow_parent": allow_parent,
    }
    if periods:
        documents["periods"] = periods
    if years:
        documents["years"] = years

    parameters: Dict[str, Any] = {
        "ALIGN_TORCH": align_torch,
        "MODE": "kgpu",
        "TEMPLATE": template,
        "EXCHANGE": exchange,
        "SYMBOL": symbol,
        "PERIODS": periods,
        "YEARS": years,
        "ALLOW_PARENT": allow_parent,
        "LAYERS": list(layers) if layers else None,
        "COMPARE": compare,
        "NOTES": notes or _default_notes(exchange, symbol, periods, years, template),
    }

    return _validate(JobConfig(
        name=name,
        # ⚠️ ON by default since 2026-08-29, by request: `kgpu run`/`pull` upserts the accepted
        # statements into raw_data/ as soon as the folder lands — backup first, every changed
        # cell printed, and the three refusals `pdf_ocr_merge` documents still in force. The
        # merge runs on THIS machine; a Kaggle worker has no path to this disk.
        merge_statements=merge_statements,
        id=f"{user}/{_slug(kernel_title)}",
        title=kernel_title,
        notebook=NOTEBOOK,
        enable_internet=True,        # the worker pip-installs the pinned OCR stack
        results_into=RESULTS_INTO,
        parameters=parameters,
        data=DataConfig(
            id=f"{user}/{_slug(dataset_title)}",
            title=dataset_title,
            ticker=symbol,
            source_dirs=list(SOURCE_DIRS),
            documents=documents,
        ),
    ))


def _default_notes(exchange: str, symbol: str, periods, years, template) -> str:
    """What the run folder should say about itself when the caller wrote nothing.

    ⚠️ A blank `notes` is how a run folder becomes unreadable six months later, and the
    filter is the one thing that is never obvious from the artefact alone once several runs
    of one ticker sit side by side.
    """
    which = []
    if periods:
        which.append(f"periods={periods}")
    if years:
        which.append(f"years={years}")
    filt = ", ".join(which) if which else "every period this ticker files"
    tpl = f"template={template} (stated)" if template else "template resolved on the worker"
    return (
        f"{exchange}_{symbol} PDF parse on a Kaggle T4 — {filt}; {tpl}. "
        f"Writes a run folder and NO statement CSV: compare() scores every parsed cell "
        f"against the CSVs on disk, and merging a recovered quarter stays a deliberate "
        f"Dagster act with a pre-run backup. Nothing from a non-bank template may be "
        f"quoted as a fundamental (CRP-1)."
    )


def describe(cfg: JobConfig) -> List[str]:
    """The lines the control notebook prints — the resolved job, before anything is spent."""
    d = cfg.data.documents or {}
    return [
        f"job          : {cfg.name}",
        f"kernel       : {cfg.id}",
        f"dataset      : {cfg.data.id}",
        f"filings      : {d['exchange']}_{d['symbol']}  "
        f"periods={d.get('periods') or 'all'}  years={d.get('years') or 'all'}  "
        f"allow_parent={d['allow_parent']}",
        f"template     : {cfg.parameters['TEMPLATE'] or 'resolve on the worker'}",
        f"results into : {cfg.results_into}",
    ]
