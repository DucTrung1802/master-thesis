# src\orchestration\enabled.py
"""THE config reader for this package — `src/orchestration/config.json`.

One file, three sections:

```jsonc
{
  "assets":     { "raw/trading_view_data": true },        // which assets load
  "partitions": { "raw/trading_view": { "stocks": false } },  // which SUB-SOURCES exist
  "run":        { "skip_existing": false, "max_browsers": 12 } // what the job launches with
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

# The switch every group carries. `"enabled": false` gates the whole group off however
# its children are set — the same rule `SwitchHandler.is_enabled` applies to
# switch_config.json, where EVERY ancestor must be true.
GROUP_GATE = "enabled"

# The files config.json replaced, as paths RELATIVE TO THIS PACKAGE. They are checked
# for and REJECTED rather than ignored: a stale config left in place, silently doing
# nothing, is the failure this package keeps having (see switch_config.json's
# trailing-slash bug).
#
# ⚠️ `../switch_config.json` sits one level UP, in `src/`, which is why these are
# relative paths rather than bare names. It was folded in on 2026-08-06: its TradingView
# parameter tree became the `parameters` section, and its other 19 keys were run-plan
# switches that Phase 5 had already made dead.
SUPERSEDED = (
    "assets_enabled.json",
    "tv_full_refresh.yaml",
    "../switch_config.json",
)

# The nine TradingView asset classes, which are also `raw/trading_view`'s partition keys.
# Named here so a typo under `parameters` raises instead of silently enumerating nothing.
TV_ASSET_CLASSES = (
    "stocks", "funds", "futures", "forex", "crypto",
    "indices", "bonds", "economy", "options",
)

# The three phase prefixes the old flat paths carried. `collected_links` takes no
# parameters (it walks the links folder) but is listed so `is_enabled` answers True for
# it under the run-plan force.
TV_PHASES = ("links", "data", "collected_links")

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
        path = (CONFIG_PATH.parent / stale).resolve()
        if path.exists():
            raise ValueError(
                f"{path} still exists but is NO LONGER READ — its contents moved into "
                f"{CONFIG_PATH.name} (assets/partitions/run on 2026-08-05, parameters "
                f"on 2026-08-06). Delete it rather than leaving a config file that "
                f"looks live and is not."
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
    unknown = set(raw) - {"assets", "partitions", "parameters", "run"}
    if unknown:
        raise ValueError(
            f"{CONFIG_PATH}: unknown top-level section(s) {sorted(unknown)}. "
            f"Valid sections: assets, partitions, parameters, run."
        )

    # ⚠️ COMMENTS ARE STRIPPED AT BOTH LEVELS — `assets` and `partitions` are dicts OF
    # dicts, and a `// name` comment's value is a STRING.
    #
    # ⚠️ SHAPE IS CHECKED BEFORE DESCENDING. Stripping inside a group before checking it
    # IS a group turns `"cafef": false` — the obvious thing to write — into an
    # AttributeError, which Dagster surfaces only as "code location failed to load".
    sections = {
        name: _strip_comments(raw.get(name, {})) for name in ("assets", "partitions")
    }
    for name, block in sections.items():
        bad = {k: type(v).__name__ for k, v in block.items() if not isinstance(v, dict)}
        if bad:
            raise ValueError(
                f"{CONFIG_PATH}: each entry under `{name}` must be an OBJECT, got "
                f'{bad}. A group is `{{"{GROUP_GATE}": false, ...}}` — a bare `false` '
                f"is not enough, because the group must still list its modules. (A note "
                f"about a group belongs on a `// name` comment key.)"
            )

    _CONFIG = {
        "assets": {g: _strip_comments(b) for g, b in sections["assets"].items()},
        "partitions": {o: _strip_comments(b) for o, b in sections["partitions"].items()},
        # ⚠️ NOT `_strip_comments`d shallowly like the others — `parameters` is a TREE up
        # to four levels deep, so comments are stripped during the recursive walk in
        # `_flatten_parameters` instead.
        "parameters": raw.get("parameters", {}),
        "run": _strip_comments(raw.get("run", {})),
    }

    ungated = [g for g, b in _CONFIG["assets"].items() if GROUP_GATE not in b]
    if ungated:
        raise ValueError(
            f'{CONFIG_PATH}: asset group(s) {ungated} have no `"{GROUP_GATE}"` key. '
            f"Every group carries its own gate, and its absence would silently read as "
            f"OFF for the whole group."
        )
    return _CONFIG


def enabled_assets() -> Set[str]:
    """Every asset key that is switched ON — group gate AND its own value.

    ⚠️ **ABSENT MEANS OFF (2026-08-05, was: absent means on).** The old default was
    "true or absent = loaded", which is the friendly reading — a newly added asset works
    without touching the config. It is also the reading under which a 777-ticker scrape
    or a 2.4-hour OCR parse can join a run because nobody remembered to write a line
    about it. The file now has to SAY yes: what is not listed, or listed under a group
    whose gate is false, does not load.

    ⚠️ **THE GROUP GATE IS THE HIERARCHY.** A group is an object holding `"enabled"` plus
    its assets, and an asset needs BOTH. That keeps every module listed — the menu stays
    the whole menu — while letting one line switch off a whole layer, and it is the rule
    `switch_config.json` already uses: every ancestor must be true.
    """
    out: Set[str] = set()
    for group, block in config().get("assets", {}).items():
        if not block.get(GROUP_GATE, False):
            continue
        out |= {
            key
            for key, value in block.items()
            if key != GROUP_GATE and value is True
        }
    return out


def run_config() -> Dict[str, Any]:
    """The `run` section — what `trading_view_full_refresh` launches with."""
    return dict(config().get("run", {}))


# ══════════════════════════════════════════════════════════════════════════════
# `parameters` — the TradingView scrape dimensions, ex-switch_config.json
# ══════════════════════════════════════════════════════════════════════════════
# ⚠️ THIS SECTION IS NOT A RUN PLAN, AND THAT DISTINCTION IS THE WHOLE REASON
# switch_config.json WAS SO HARD TO KILL. That file mixed two things in one tree: the
# run plan ("scrape TradingView at all") and the parameters ("which countries, which
# sectors"). Dagster took over the first in Phase 5 — selection IS the run plan — but
# the second had nowhere to go, so a dead file stayed alive for one live subtree, and
# `build_unblocked` existed purely to force the dead half true so the live half could be
# read. The parameters live here now and the file is gone.
#
# ⚠️ THE FLAT PATH FORMAT IS PRESERVED EXACTLY, ON PURPOSE. The fifteen
# `get_enabled_paths(...)` call sites in `web_scraper/trading_view_scraper.py` index
# their results positionally — `parts[4]` is the country, `parts[5]` the stock type,
# `parts[6]` the sector — so this module rebuilds `web_scraper/trading_view/<phase>/…`
# from the nested tree and hands it to an ordinary `SwitchHandler`. Re-shaping the
# scraper to read a tree would have touched every adder for no gain; this package's
# history is mostly widened refactors going wrong.


def _flatten_parameters(node: Any, prefix: str, out: Dict[str, bool]) -> None:
    """Walk the nested tree, writing `a/b/c -> bool` for every branch and leaf.

    A value is either a BOOL (a leaf) or a DICT (a branch). Branches are written as
    `True` because `SwitchHandler.is_enabled` requires every ancestor to be true and the
    leaves are what actually gate; a whole subtree is switched off by writing `false`
    where its dict would go, which lands here as an ordinary disabled leaf.
    """
    if isinstance(node, bool):
        out[prefix] = node
        return
    if not isinstance(node, dict):
        # ⚠️ Report the path the USER wrote, not the synthesized one. `prefix` here is
        # already `web_scraper/trading_view/links/bonds`, and quoting that back at
        # someone who wrote `"bonds": "yes"` names a path that appears nowhere in the
        # file they are looking at.
        authored = prefix.split("/", 3)[-1] if prefix.count("/") >= 3 else prefix
        raise ValueError(
            f"{CONFIG_PATH}: parameters/trading_view/{authored} must be a boolean (a "
            f"leaf) or an object (a branch), got {type(node).__name__}."
        )
    out[prefix] = True
    for key, child in node.items():
        if key.startswith("//"):
            continue
        _flatten_parameters(child, f"{prefix}/{key}", out)


def trading_view_switches() -> Dict[str, bool]:
    """`parameters.trading_view` as the flat paths the scraper's adders expect.

    The ONE tree is emitted under BOTH phase prefixes. switch_config.json carried two
    trees — `.../links/…` and `.../data/…` — that were byte-identical at 326 keys each
    and differed on exactly one value (`crypto`), which is how that drift got in. They
    share one tree here for the same reason `raw/trading_view_links` and
    `raw/trading_view_data` share one `PartitionsDefinition`: the data step reads the
    link CSV its own leaf wrote, so they cannot legitimately disagree.

    ⚠️ Run-plan ancestors are NOT forced here — see `trading_view_run_plan_switches`.
    """
    tv = config().get("parameters", {}).get("trading_view", {})
    if not isinstance(tv, dict):
        raise ValueError(
            f"{CONFIG_PATH}: parameters/trading_view must be an object, got "
            f"{type(tv).__name__}."
        )

    unknown = {k for k in tv if not k.startswith("//")} - set(TV_ASSET_CLASSES)
    if unknown:
        raise ValueError(
            f"{CONFIG_PATH}: parameters/trading_view has unknown asset class(es) "
            f"{sorted(unknown)}. Valid: {', '.join(TV_ASSET_CLASSES)}. (A typo here "
            f"would enumerate nothing and the asset would fail on an empty folder — "
            f"which is a slow way to learn about a spelling mistake.)"
        )

    out: Dict[str, bool] = {}
    for phase in ("links", "data"):
        for asset_class, node in tv.items():
            if asset_class.startswith("//"):
                continue
            _flatten_parameters(
                node, f"web_scraper/trading_view/{phase}/{asset_class}", out
            )
    return out


def data_only_sources() -> Dict[str, List[str]]:
    """`parameters.data_only`: per asset class, the leaves the DATA phase may fetch.

    ⚠️ **THE ONE PLACE LINKS AND DATA ARE ALLOWED TO DISAGREE, AND ONLY IN ONE
    DIRECTION** (2026-08-14). `trading_view_switches` emits ONE tree under both phase
    prefixes, deliberately — two trees is how `switch_config.json` drifted to a
    one-value difference nobody noticed. That rule stays for the dangerous direction:
    **data may never enable what links does not**, because the data adder reads the
    links CSV its own leaf wrote, and a leaf with no CSV is a warning and a silent
    no-op.

    The SAFE direction is a real requirement. Enumerating a class is cheap (forex:
    47 brokers, 5.5 MB, minutes); fetching it is not (~50 s per symbol behind a global
    8-second navigation gate). Wanting the whole universe listed and a subset fetched
    is the normal case, and before this it had no expression at all — the only lever
    was switching a broker off in the shared tree, which also stopped its links being
    collected, so the universe silently shrank to whatever was being fetched.

    An ABSENT class means "no restriction" — every leaf its tree enables is fetched.
    An EMPTY list is rejected in `validate()`: "fetch nothing" is what the asset-level
    switch is for, and an empty list here reads as an accident.

    ⚠️ **IT FILTERS THE FIRST LEVEL BELOW THE CLASS ONLY.** `forex/<broker>` is one
    level and that is what this was built for; `stocks/<country>/<type>/<sector>` would
    be filtered on `<country>` alone. Deeper selection would need a path prefix, and
    inventing that before something needs it is how the last config grew to 676 keys.
    """
    raw = config().get("parameters", {}).get("data_only", {})
    if not isinstance(raw, dict):
        raise ValueError(
            f"{CONFIG_PATH}: parameters/data_only must be an object mapping an asset "
            f"class to a list of leaf names, got {type(raw).__name__}."
        )
    out: Dict[str, List[str]] = {}
    for asset_class, leaves in raw.items():
        if asset_class.startswith("//"):
            continue
        if not isinstance(leaves, list) or not all(isinstance(x, str) for x in leaves):
            raise ValueError(
                f"{CONFIG_PATH}: parameters/data_only/{asset_class} must be a LIST of "
                f"leaf names, got {type(leaves).__name__}."
            )
        out[asset_class] = list(leaves)
    return out


def trading_view_run_plan_switches(phase: str) -> Dict[str, bool]:
    """The parameter paths PLUS the run-plan ancestors `phase` needs, forced true.

    This is what `SwitchConfig.build_trading_view` hands to the scraper, and it replaces
    `build_unblocked`'s four forced prefixes.

    ⚠️ ON THE `data` PHASE, `parameters.data_only` NARROWS THE TREE — see
    `data_only_sources`. Links still enumerate everything; only the fetch is restricted.

    ⚠️ THE ASSET-CLASS PREFIX IS DELIBERATELY *NOT* FORCED, WHICH CLOSES A REAL BUG.
    `build_unblocked` forced `web_scraper/trading_view/<phase>/<asset_class>` true, and
    for a class with no children in the tree that made the CLASS ITSELF a leaf — so
    `get_enabled_paths` returned a 4-part path where the adder expected 5+ and
    `_add_options_links_tasks` raised `IndexError` before queueing anything (partition
    `options`, 2026-07-31; reachable only through Dagster, so main.py never hit it). The
    class prefix now comes from the tree or not at all: a class the tree does not list
    enumerates nothing, quietly and correctly. The guards in the crypto and options
    adders stay as defence, but this path can no longer trip them.
    """
    if phase not in TV_PHASES:
        raise ValueError(
            f"Unknown TradingView phase {phase!r}. Valid: {', '.join(TV_PHASES)}."
        )
    switches = trading_view_switches()

    if phase == "data":
        for asset_class, allowed in data_only_sources().items():
            prefix = f"web_scraper/trading_view/data/{asset_class}/"
            keep = {f"{prefix}{leaf}" for leaf in allowed}
            for path in list(switches):
                # ⚠️ `startswith(prefix)` and then the FIRST segment, so a deeper tree
                # keeps every descendant of an allowed leaf rather than only the leaf.
                if not path.startswith(prefix):
                    continue
                first = path[len(prefix):].split("/", 1)[0]
                if f"{prefix}{first}" not in keep:
                    switches[path] = False

    switches["web_scraper"] = True
    switches["web_scraper/trading_view"] = True
    switches[f"web_scraper/trading_view/{phase}"] = True
    return switches


def register(owner: str, keys: Sequence[str]) -> List[str]:
    """Record `owner`'s full partition list and return only the ENABLED keys.

    Call this instead of passing a literal list to `StaticPartitionsDefinition`.

    ⚠️ Raises if every partition is disabled rather than building an empty
    `PartitionsDefinition`: an asset with no partitions is unmaterialisable in a way that
    reads as a Dagster bug at launch time. "None of them" is what the asset-level `false`
    is for, and the message says so.
    """
    KNOWN_PARTITIONS[owner] = list(keys)
    partitions = config().get("partitions", {})

    # ⚠️ ABSENT MEANS OFF **INSIDE** A LISTED BLOCK, AND "NO OPINION" IF THE WHOLE BLOCK
    # IS ABSENT — the one place the two defaults differ, and it is deliberate. A
    # partition set nobody has configured (say `raw/cafef_pdfs`' 100 tickers) would
    # otherwise resolve to ZERO partitions and raise below, at import time, for an asset
    # that is very likely switched off anyway. Mention an owner and you own its list;
    # say nothing and the asset keeps every partition it declared.
    if owner not in partitions:
        return list(keys)

    switches = partitions[owner]
    kept = [k for k in keys if switches.get(k) is True]

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

    # ⚠️ Force the `parameters` tree to be walked HERE, at definition-validation time.
    # Its own checks (unknown asset class, a non-bool non-dict node) otherwise fire
    # only when a TradingView asset actually runs — which for a typo'd country means
    # discovering it after the browsers are up.
    switches = trading_view_switches()

    # ⚠️ `data_only` IS CHECKED AGAINST THE LINKS TREE, NOT JUST FOR SPELLING. A leaf
    # the data phase names and the links phase does not enable would send the adder to
    # a links folder nobody filled — `log_warning` and 0 tasks, which is the silent
    # no-op this whole file exists to prevent. Both failures raise here, before a
    # browser starts.
    for asset_class, leaves in data_only_sources().items():
        if asset_class not in TV_ASSET_CLASSES:
            problems.append(
                f"parameters/data_only: '{asset_class}' is not a TradingView asset "
                f"class. Valid: {', '.join(TV_ASSET_CLASSES)}"
            )
            continue
        if not leaves:
            problems.append(
                f"parameters/data_only/{asset_class} is EMPTY. An empty list fetches "
                f"nothing, which reads as an accident — to switch the fetch off, set "
                f"`raw/trading_view_data` false under `assets`."
            )
            continue
        prefix = f"web_scraper/trading_view/links/{asset_class}/"
        known = {p[len(prefix):].split("/", 1)[0] for p in switches if p.startswith(prefix)}
        for leaf in leaves:
            if leaf not in known:
                problems.append(
                    f"parameters/data_only/{asset_class}: '{leaf}' is not enabled under "
                    f"parameters/trading_view/{asset_class}, so the links phase would "
                    f"never write its CSV and the data phase would fetch nothing. "
                    f"Valid: {', '.join(sorted(known))}"
                )

    listed: Set[str] = set()
    for group, block in config().get("assets", {}).items():
        for key in block:
            if key == GROUP_GATE:
                continue
            listed.add(key)
            if key not in known_assets:
                problems.append(f"assets/{group}: '{key}' matches no asset")

    # ⚠️ With absent = OFF, an asset missing from the file is SILENTLY off — which is the
    # very failure this file raises about everywhere else. So completeness is enforced:
    # add an asset to the graph and you must say yes or no to it here.
    unlisted = known_assets - listed
    if unlisted:
        problems.append(
            f"assets: {sorted(unlisted)} are not listed. Since absent now means OFF, an "
            f"unlisted asset would vanish from the UI with nothing saying so — list it "
            f"under its group with true or false."
        )

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
