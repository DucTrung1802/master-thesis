# src\orchestration\enabled.py
"""THE config reader for this package — `src/orchestration/config.json`.

One file, three sections:

```jsonc
{
  "assets":     { "raw/trading_view_data": true },        // which assets load
  "partitions": { "raw/trading_view": { "stocks": false } },  // which SUB-SOURCES exist
  "run":        { "skip_existing": false, "max_browsers": 4 } // what the job launches with
}
```

It replaced `assets_enabled.json` + `tv_full_refresh.yaml` on 2026-08-05. Three files
for one package's configuration meant three places to look and three to forget; the
`run` block in particular lived only in a YAML file that the UI never read, so the UI
and the CLI could launch the same job with different settings.

⚠️ **`partitions` IS THE HALF THAT IS NOT ABOUT LOADING.** Half the sources here are not
assets — TradingView's nine asset classes, CafeF's 100 filing tickers and the three
unified universes are PARTITIONS — so "turn off TradingView stocks" had no expression at
all until this section existed, and the only lever was typing the right `--partition` at
launch. That lever failed on 2026-08-05, when a backfill took all 100 `cafef_pdfs`
tickers at once. A partition set false here is REMOVED from the `PartitionsDefinition`:
unmaterialisable, un-backfillable, invisible, and `--partition <it>` fails before any
work starts.

⚠️ **THE KEY UNDER `partitions` IS THE SET'S OWNER, WHICH IS NOT ALWAYS AN ASSET.**
Assets sharing one `PartitionsDefinition` OBJECT must share its toggles:
`raw/trading_view_data` cannot offer a class `raw/trading_view_links` does not, because
the data step reads the link CSV that same partition wrote. Those live under a group
name; sets owned by one asset keep that asset's key. `register()` records every valid
owner and its full key list, which is what lets a typo raise instead of quietly
disabling nothing.
"""

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set

CONFIG_PATH = Path(__file__).resolve().parent / "config.json"

# The two files config.json replaced. They are checked for and REJECTED rather than
# ignored: a stale config left in place, silently doing nothing, is the failure this
# package keeps having (see switch_config.json's trailing-slash bug).
SUPERSEDED = ("assets_enabled.json", "tv_full_refresh.yaml")

# owner name -> its FULL partition key list, as the asset module declared it (before any
# filtering). Populated at import time by `register()`; read by `validate()`.
KNOWN_PARTITIONS: Dict[str, List[str]] = {}

_CONFIG: Dict[str, Any] | None = None


def _strip_comments(d: Dict[str, Any]) -> Dict[str, Any]:
    """Drop `//` keys — JSON has no comment syntax and this file needs comments."""
    return {k: v for k, v in d.items() if not k.startswith("//")}


def config() -> Dict[str, Any]:
    """The file, comments stripped, read once.

    ⚠️ FAILS LOUDLY ON A BAD FILE, ON PURPOSE. `SwitchHandler._load_config` swallows a
    read error and returns `{}` — every switch false, the pipeline a complete no-op, one
    ERROR line in a log nobody reads. The equivalent slip here would silently disable
    everything, so a malformed file raises. An ABSENT file is different and is fine: it
    means "no opinion", i.e. everything enabled.

    Read as `utf-8-sig`, because this file is hand-edited on Windows and PowerShell 5.1's
    `Out-File -Encoding utf8` writes a BOM — the same trap that cost this repo a silent
    no-op run.
    """
    global _CONFIG
    if _CONFIG is not None:
        return _CONFIG

    for stale in SUPERSEDED:
        path = CONFIG_PATH.parent / stale
        if path.exists():
            raise ValueError(
                f"{path} still exists but is NO LONGER READ — its contents moved into "
                f"{CONFIG_PATH.name} on 2026-08-05 (assets/partitions/run). Delete it "
                f"rather than leaving a config file that looks live and is not."
            )

    if not CONFIG_PATH.exists():
        _CONFIG = {}
        return _CONFIG

    try:
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8-sig"))
    except Exception as e:
        raise ValueError(
            f"{CONFIG_PATH} is not valid JSON: {e}. Refusing to load — an unreadable "
            f"config must never be read as 'disable everything'."
        ) from e

    if not isinstance(raw, dict):
        raise ValueError(
            f"{CONFIG_PATH} must be a JSON object, got {type(raw).__name__}."
        )

    raw = _strip_comments(raw)
    unknown = set(raw) - {"assets", "partitions", "run"}
    if unknown:
        raise ValueError(
            f"{CONFIG_PATH}: unknown top-level section(s) {sorted(unknown)}. "
            f"Valid sections: assets, partitions, run."
        )

    # ⚠️ COMMENTS ARE STRIPPED AT BOTH LEVELS. `partitions` is a dict of dicts, and a
    # `// owner` comment's value is a STRING — descending into it without stripping first
    # is an AttributeError at import time, which in Dagster reads as "code location
    # failed to load" with the real cause four frames down.
    _CONFIG = {
        "assets": _strip_comments(raw.get("assets", {})),
        "partitions": {
            owner: _strip_comments(keys)
            for owner, keys in _strip_comments(raw.get("partitions", {})).items()
        },
        "run": _strip_comments(raw.get("run", {})),
    }

    bad = {o: type(k).__name__ for o, k in _CONFIG["partitions"].items() if not isinstance(k, dict)}
    if bad:
        raise ValueError(
            f"{CONFIG_PATH}: each entry under `partitions` must be an object of "
            f"partition -> bool; got {bad}. (A note about an owner belongs on a "
            f"`// owner` comment key.)"
        )
    return _CONFIG


def disabled_assets() -> Set[str]:
    """Asset keys explicitly set to `false`."""
    return {k for k, v in config().get("assets", {}).items() if v is False}


def run_config() -> Dict[str, Any]:
    """The `run` section — what `trading_view_full_refresh` launches with."""
    return dict(config().get("run", {}))


def register(owner: str, keys: Sequence[str]) -> List[str]:
    """Record `owner`'s full partition list and return only the ENABLED keys.

    Call this instead of passing a literal list to `StaticPartitionsDefinition`.

    ⚠️ Raises if every partition is disabled rather than building an empty
    `PartitionsDefinition`: an asset with no partitions is unmaterialisable in a way that
    reads as a Dagster bug at launch time. "None of them" is what the asset-level `false`
    is for, and the message says so.
    """
    KNOWN_PARTITIONS[owner] = list(keys)
    switches = config().get("partitions", {}).get(owner, {})
    kept = [k for k in keys if switches.get(k) is not False]

    if not kept:
        raise ValueError(
            f"{CONFIG_PATH}: every partition of '{owner}' is disabled "
            f"({', '.join(keys)}). An asset cannot have zero partitions — to switch the "
            f"whole thing off, set its key under `assets` to false instead."
        )
    return kept


def validate(all_asset_keys: Iterable[str]) -> None:
    """Raise on any key that matches no asset and no partition.

    ⚠️ A silently-ignored key is the exact failure `switch_config.json`'s trailing-slash
    bug had: the file said a thing was off, the loader never matched it, and the thing
    ran. Both sections are checked, and for partitions both halves — the owner AND the
    partition key.

    `all_asset_keys` must be the FULL list, before disabled assets are dropped, or every
    disabled key would report itself as unknown.
    """
    known_assets = set(all_asset_keys)
    problems: List[str] = []

    for key in config().get("assets", {}):
        if key not in known_assets:
            problems.append(f"assets: '{key}' matches no asset")

    for owner, switches in config().get("partitions", {}).items():
        if owner not in KNOWN_PARTITIONS:
            hint = (
                " (it is an asset, but its partitions are registered under a group "
                "name — see this module's docstring)"
                if owner in known_assets
                else ""
            )
            problems.append(f"partitions: '{owner}' is not a partition set{hint}")
            continue
        for key in switches:
            if key not in KNOWN_PARTITIONS[owner]:
                problems.append(
                    f"partitions: '{owner}' has no partition '{key}'. Valid: "
                    f"{', '.join(KNOWN_PARTITIONS[owner])}"
                )

    if problems:
        raise ValueError(
            f"{CONFIG_PATH} has {len(problems)} bad key(s):\n  "
            + "\n  ".join(problems)
            + f"\n\nValid asset keys: {sorted(known_assets)}"
            + "\nValid partition sets: "
            + "; ".join(
                f"{o}[{', '.join(p)}]" for o, p in sorted(KNOWN_PARTITIONS.items())
            )
        )
