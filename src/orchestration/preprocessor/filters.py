# src\orchestration\preprocessor\filters.py
"""THE FILTER LAYER — `silver_schema` / `gold_schema` → `filter_schema.universe__<screen>`.

The fifth layer, and the only one that produces **no time series at all**. Every other
layer answers *"what happened to this ticker on this date"*; this one answers the single
question that sits between gold and unified:

    WHICH TICKERS ARE ALLOWED INTO A UNIFIED SCHEMA?

Before this module that question had exactly three answers, hard-coded in
`DataPreprocessor.UNIFIED_MEMBER_FILTERS`: everything (`ALL`), one GICS industry
(`BANK`), and a frozen index list (`VN30`). Adding a fourth meant writing SQL inside a
class constant. A **screen** is that same seam made declarative: a named list of
CONDITIONS, each of which measures one number per `(exchange, ticker)` and compares it
to a threshold. Materialising a screen writes ONE table holding **every** candidate with
**every** measurement, and the universe is `WHERE passes`.

```
gold_schema / silver_schema
      │
      ▼
filter_schema.universe__<screen>    one row per (exchange, ticker) — ALL of them —
      │                             with val__<cond>, pass__<cond>, passes, first_failed
      ▼
unified_schema_<screen>             pool__basic filtered to WHERE passes; every other
                                    pool inherits it from the spine
```

──────────────────────────────────────────────────────────────────────────────────
ADDING A CONDITION IS ONE ENTRY IN `CONDITIONS`. ADDING A UNIVERSE IS ONE IN `SCREENS`.
──────────────────────────────────────────────────────────────────────────────────

```python
register(Condition(
    name="close_raw_min_10k",
    description="close_raw never below 10,000 VND on any session from 2026-01-01",
    source=f"{SILVER_SCHEMA}.stocks_basic",
    metric="MIN(close_raw)",
    op=">=", threshold=10_000.0, unit="VND",
    where="date >= %(start)s", params={"start": date(2026, 1, 1)},
))

SCREENS["PRICE10K"] = Screen("PRICE10K", "…", ("close_raw_min_10k",))
```

Then add `"PRICE10K"` to `UNIFIED_PARTITIONS` in `assets/unified.py` and to
`config.json`. Nothing else changes — `UNIFIED_MEMBER_FILTERS` is EXTENDED from
`SCREENS` at class-definition time, and `_helper_unified_member_filter` already returns
a bare boolean predicate over `silver.stocks_basic`, which is exactly the shape a
membership sub-select has.

──────────────────────────────────────────────────────────────────────────────────
⚠️ A SCREEN IS **NOT POINT-IN-TIME**, AND THAT IS THE ONE TRAP IN THIS FILE
──────────────────────────────────────────────────────────────────────────────────

Membership is decided ONCE, from a window of data, and then applied to the WHOLE
history of every pool built on it. `PRICE10K` selects the names that traded above
10,000 VND **in 2026** and carries that membership back to 2009 — so a 2012 row exists
in `unified_schema_price10k` because of something that happened fourteen years later.

That is `CLAUDE.md` §2c's defect in its purest form, the same one recorded for
`UNIFIED_VN30` (today's index list with no history) and for `kgpu`'s `liquidity_before`.
It is BENIGN for a within-date shuffle null — every draw sees the same basket, so a `z`
is protected — and it INVALIDATES any CAGR, Sharpe or hit rate read off a screened
universe. There is no defence in this module beyond saying so: every condition's window
is written into the table's `COMMENT` and into the asset metadata, so a later reader can
see exactly which window chose the basket.

⚠️ **The honest version of a screen is a `where` window that ENDS before the first test
fold.** `liquidity_before` in `kgpu.export` is what that looks like, and it has no
default for the same reason: a silent one makes the look-ahead invisible in the artefact.
Every `Condition` here therefore carries its window explicitly in `where`/`params`.

──────────────────────────────────────────────────────────────────────────────────
⚠️ `metric` AND `where` ARE RAW SQL, ON PURPOSE
──────────────────────────────────────────────────────────────────────────────────

They are CODE IN THIS REPOSITORY, reviewed in `git diff`, never user input — the same
trust boundary `UNIFIED_MEMBER_FILTERS`' predicates already sit on. What is NOT trusted
is anything that could arrive from elsewhere: `name`, `source` and `latest_by` are
validated as identifiers, `op` comes from a whitelist, and **every threshold and every
`params` value is a BOUND PARAMETER**, never interpolated. A GICS code has no business
being inlined and neither does a price.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Mapping, Tuple

from utils.constants import FILTER_SCHEMA, GOLD_SCHEMA, SILVER_SCHEMA
from utils.exceptions import PipelineError

# ⚠️ The candidate set — every `(exchange, ticker)` a screen may consider. It is the
# spine `pool__basic` is cut from, so a screen can only ever REMOVE names, never invent
# one that silver does not carry.
CANDIDATE_SOURCE = f"{SILVER_SCHEMA}.stocks_basic"

# Condition names become COLUMN names (`val__x`, `pass__x`) and screen names become part
# of a TABLE name and a SCHEMA name, so both are held to identifier rules. Screen names
# are additionally constrained by `DataPreprocessor.UNIFIED_TICKER_PATTERN`, which they
# must satisfy to name `unified_schema_<screen>`.
_CONDITION_NAME = re.compile(r"^[a-z][a-z0-9_]{0,40}$")
_SCREEN_NAME = re.compile(r"^[A-Z][A-Z0-9_]{2,19}$")
_QUALIFIED_TABLE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
_PARAM_REF = re.compile(r"%\((\w+)\)s")

# ⚠️ A WHITELIST, not a regex over the string. `op` is interpolated into SQL beside a
# bound threshold, and the set of comparisons a screen can express is small and closed.
OPERATORS = (">=", ">", "<=", "<", "=", "<>")

# What a missing measurement means. There is no default that is right for both cases,
# which is why it is an explicit field with the CONSERVATIVE default:
#
#   "reject" — no measurement means the ticker is OUT. Right for anything derived from
#              price/volume, where an absent value means the name did not trade.
#   "keep"   — no measurement means the condition ABSTAINS. Required wherever the
#              source covers a fraction of the universe: `gold.stocks_financials_bank_fa`
#              holds TWO tickers (ACB, VCB, measured 2026-08-22), so a debt/equity
#              condition on "reject" empties any screen it joins.
ON_MISSING = ("reject", "keep")


@dataclass(frozen=True)
class Condition:
    """ONE measurable statement about a ticker: `<metric> <op> <threshold>`.

    Two modes, chosen by `latest_by`:

    * **aggregate** (`latest_by=None`) — `metric` is a SQL AGGREGATE evaluated over
      every row the `where` window keeps, grouped by `(exchange, ticker)`:
      `MIN(close_raw)`, `COUNT(*)`,
      `AVG(CASE WHEN volume_matched > 0 THEN 1.0 ELSE 0.0 END)`,
      `percentile_cont(0.5) WITHIN GROUP (ORDER BY value_matched)`.

    * **latest** (`latest_by="date"`) — `metric` is a ROW expression read off the most
      recent row **on which it is not NULL**. That NULL guard is generated, not written
      by hand: `gold.stocks_financials_bank_fa` carries a balance-sheet column on 3,005
      of ACB's 4,030 rows, so "the latest row" and "the latest reported value" are
      different questions and only the second one is ever meant.

    ⚠️ **THE METRIC MUST BE NUMERIC**, because `value` is one `double precision` column
    for every condition — one column type is what lets the table hold six unrelated
    measurements side by side. A DATE metric is expressed as a numeric DISTANCE:
    `MAX(date) - %(cutoff)s` is a count of days and compares to `>= 0`. See
    `still_trading_2026_06`.

    ⚠️ **`threshold` and every `params` value are BOUND**, never interpolated. `metric`
    and `where` are raw SQL — see the module docstring on why that is safe here and what
    it would take to make it unsafe.
    """

    name: str
    description: str
    source: str
    metric: str
    op: str
    threshold: float
    where: str = ""
    params: Mapping[str, Any] = field(default_factory=dict)
    latest_by: str | None = None
    on_missing: str = "reject"
    unit: str = ""

    def __post_init__(self) -> None:
        if not _CONDITION_NAME.match(self.name or ""):
            raise PipelineError(
                f"Condition name {self.name!r} is not usable as a column name. Expected "
                f"lower snake_case, 1-41 chars, starting with a letter — it becomes "
                f"`val__{self.name}` and `pass__{self.name}`."
            )
        if not _QUALIFIED_TABLE.match(self.source or ""):
            raise PipelineError(
                f"Condition {self.name!r}: source {self.source!r} must be a qualified "
                f"lower-case `schema.table`, e.g. '{SILVER_SCHEMA}.stocks_basic'."
            )
        if self.op not in OPERATORS:
            raise PipelineError(
                f"Condition {self.name!r}: operator {self.op!r} is not one of "
                f"{', '.join(OPERATORS)}."
            )
        if self.on_missing not in ON_MISSING:
            raise PipelineError(
                f"Condition {self.name!r}: on_missing must be one of {ON_MISSING}, got "
                f"{self.on_missing!r}."
            )
        if self.latest_by is not None and not _IDENTIFIER.match(self.latest_by):
            raise PipelineError(
                f"Condition {self.name!r}: latest_by {self.latest_by!r} must be a plain "
                f"column name."
            )
        if not str(self.metric).strip():
            raise PipelineError(f"Condition {self.name!r}: metric is empty.")

        # ⚠️ BOTH DIRECTIONS. An undeclared `%(x)s` fails at execution with a psycopg2
        # message that names neither the condition nor the screen; an unused declared
        # param is a renamed placeholder whose old window is silently still in force.
        referenced = set(_PARAM_REF.findall(f"{self.metric} {self.where}"))
        declared = set(self.params or {})
        if referenced - declared:
            raise PipelineError(
                f"Condition {self.name!r} references undeclared parameter(s) "
                f"{sorted(referenced - declared)}. Declared: "
                f"{sorted(declared) or 'none'}."
            )
        if declared - referenced:
            raise PipelineError(
                f"Condition {self.name!r} declares parameter(s) "
                f"{sorted(declared - referenced)} that no expression uses. A dangling "
                f"parameter is usually a renamed placeholder, i.e. a window that is "
                f"still whatever it used to be."
            )

    # ── rendering ────────────────────────────────────────────────────────────────
    @property
    def value_column(self) -> str:
        return f"val__{self.name}"

    @property
    def pass_column(self) -> str:
        return f"pass__{self.name}"

    def window(self) -> str:
        """The `where` window with its parameters substituted, FOR DISPLAY ONLY.

        Written into the table comment and the asset metadata so the look-ahead the
        module docstring warns about is legible without reading this file. Never used
        to build SQL — the real statement binds every one of these.
        """
        if not self.where:
            return "every row"
        text = self.where
        for key, value in (self.params or {}).items():
            text = text.replace(f"%({key})s", repr(value))
        return text

    def describe(self) -> str:
        unit = f" {self.unit}" if self.unit else ""
        mode = f"latest non-null by {self.latest_by}" if self.latest_by else "aggregate"
        return (
            f"{self.name}: {self.metric} {self.op} {self.threshold:g}{unit} "
            f"[{mode}; {self.source}; {self.window()}; missing → {self.on_missing}]"
        )


@dataclass(frozen=True)
class Screen:
    """A named universe: the conditions a ticker must satisfy, ALL of them (AND).

    ⚠️ **AND, never OR, and that is a deliberate limit.** A screen is a list of reasons
    to be excluded, so `first_failed` can name exactly one of them and the table stays
    readable as an audit. An OR belongs INSIDE one condition's `metric`, where it is
    visible as the single number it really is.
    """

    name: str
    description: str
    conditions: Tuple[str, ...]

    def __post_init__(self) -> None:
        if not _SCREEN_NAME.match(self.name or ""):
            raise PipelineError(
                f"Screen name {self.name!r} must be UPPER_SNAKE, 3-20 chars, starting "
                f"with a letter. It names both `{FILTER_SCHEMA}.universe__<lower>` and "
                f"`unified_schema_<lower>`."
            )
        # ⚠️ EVERY VN TICKER IS EXACTLY 3 CHARACTERS (measured 2026-08-22 over all 781
        # names in `silver.stocks_basic`), so a 4+ character screen name cannot collide
        # with a listing — the same argument that makes `ALL` and `BANK` safe sentinels,
        # made structural instead of checked one name at a time.
        if len(self.name) < 4:
            raise PipelineError(
                f"Screen name {self.name!r} is {len(self.name)} characters. Every VN "
                f"ticker is exactly 3, so a screen must be 4 or more to be sure it "
                f"never collides with a real listing."
            )
        if not self.conditions:
            raise PipelineError(
                f"Screen {self.name!r} has no conditions. A screen that filters nothing "
                f"is `ALL`, which already exists."
            )
        if len(set(self.conditions)) != len(self.conditions):
            raise PipelineError(
                f"Screen {self.name!r} lists a condition twice: {self.conditions}. Each "
                f"becomes a column, so a duplicate is a duplicate column name."
            )

    @property
    def slug(self) -> str:
        return self.name.lower()

    @property
    def table(self) -> str:
        return f"universe__{self.slug}"

    @property
    def qualified_table(self) -> str:
        return f"{FILTER_SCHEMA}.{self.table}"

    def resolve(self) -> List[Condition]:
        missing = [c for c in self.conditions if c not in CONDITIONS]
        if missing:
            raise PipelineError(
                f"Screen {self.name!r} names unknown condition(s) {missing}. Known: "
                f"{', '.join(sorted(CONDITIONS))}."
            )
        return [CONDITIONS[c] for c in self.conditions]


# ══════════════════════════════════════════════════════════════════════════════
# THE CONDITION LIBRARY — add one entry, get a reusable filter
# ══════════════════════════════════════════════════════════════════════════════
# ⚠️ EVERY WINDOW BELOW IS A LITERAL DATE, NOT A RELATIVE ONE. `date >= NOW() - INTERVAL
# '1 year'` would make a screen's membership change under an already-built unified schema
# with nothing in `git diff` saying so — the same defect `UNIFIED_VN30_TICKERS` is frozen
# in Python to avoid. A window moves when somebody edits this file.
#
# ⚠️ `value_matched` IS BILLIONS OF VND (median 0.13, max 4,299 measured 2026-08-22 over
# `date >= 2026-01-01`), while `foreign_*_value` and `prop_*_val` are plain VND. That
# mismatch already manufactured a participation ratio of 215,150,099 once — see
# CLAUDE.md §"pool__basic CARRIES DERIVED FEATURES NOW". A threshold here is in BILLIONS.

#: The screening window for everything liquidity- and continuity-shaped: the trailing
#: year to the last session silver holds (2026-08-19, measured 2026-08-22). One name so
#: the four conditions that use it cannot drift apart.
_RECENT_YEAR = date(2025, 8, 22)

CONDITIONS: Dict[str, Condition] = {}


def register(condition: Condition) -> Condition:
    """Add a condition to the library, refusing a silent redefinition."""
    if condition.name in CONDITIONS:
        raise PipelineError(
            f"Condition {condition.name!r} is already defined. Redefining it would "
            f"change every screen that names it, including ones already materialised."
        )
    CONDITIONS[condition.name] = condition
    return condition


# ── price ────────────────────────────────────────────────────────────────────────
register(
    Condition(
        name="close_raw_min_10k",
        description=(
            "close_raw never dips below 10,000 VND on ANY session from 2026-01-01 "
            "onward. MIN is the literal reading of 'không dưới 10,000' — one session at "
            "9,900 disqualifies the name. Switch `metric` to "
            "`percentile_cont(0.5) WITHIN GROUP (ORDER BY close_raw)` for the softer "
            "'typically above 10,000' reading."
        ),
        source=f"{SILVER_SCHEMA}.stocks_basic",
        metric="MIN(close_raw)",
        op=">=",
        threshold=10_000.0,
        unit="VND",
        where="date >= %(start)s",
        params={"start": date(2026, 1, 1)},
        on_missing="reject",
    )
)

register(
    Condition(
        name="close_raw_median_5k",
        description=(
            "Median close_raw at or above 5,000 VND over the trailing year — the "
            "penny-stock screen. MEDIAN, not MIN, because this one is about where the "
            "name normally trades rather than about its worst session."
        ),
        source=f"{SILVER_SCHEMA}.stocks_basic",
        metric="percentile_cont(0.5) WITHIN GROUP (ORDER BY close_raw)",
        op=">=",
        threshold=5_000.0,
        unit="VND",
        where="date >= %(start)s",
        params={"start": _RECENT_YEAR},
        on_missing="reject",
    )
)

# ── liquidity ────────────────────────────────────────────────────────────────────
register(
    Condition(
        name="turnover_median_1bn",
        description=(
            "Median MATCHED turnover at or above 1 bn VND/session over the trailing "
            "year. ⚠️ MATCHED only — the negotiated channel is block trades and is not "
            "liquidity you can take (ABB 2026-06-26: 19 bn matched against 393 bn "
            "negotiated)."
        ),
        source=f"{SILVER_SCHEMA}.stocks_basic",
        metric="percentile_cont(0.5) WITHIN GROUP (ORDER BY value_matched)",
        op=">=",
        threshold=1.0,
        unit="bn VND",
        where="date >= %(start)s",
        params={"start": _RECENT_YEAR},
        on_missing="reject",
    )
)

register(
    Condition(
        name="traded_days_ratio_80",
        description=(
            "At least 80% of the trailing year's sessions had a non-zero matched "
            "volume — the halted/suspended/no-bid screen. A name can clear a turnover "
            "MEDIAN and still be untradeable if it prints nothing half the time."
        ),
        source=f"{SILVER_SCHEMA}.stocks_basic",
        metric="AVG(CASE WHEN volume_matched > 0 THEN 1.0 ELSE 0.0 END)",
        op=">=",
        threshold=0.80,
        unit="fraction",
        where="date >= %(start)s",
        params={"start": _RECENT_YEAR},
        on_missing="reject",
    )
)

# ── continuity ───────────────────────────────────────────────────────────────────
register(
    Condition(
        name="sessions_min_200",
        description=(
            "At least 200 sessions in the trailing year — roughly a full listed year. "
            "Drops names that listed mid-window, which would otherwise enter a panel "
            "with a lookback window that cannot be filled."
        ),
        source=f"{SILVER_SCHEMA}.stocks_basic",
        metric="COUNT(*)",
        op=">=",
        threshold=200.0,
        unit="sessions",
        where="date >= %(start)s",
        params={"start": _RECENT_YEAR},
        on_missing="reject",
    )
)

register(
    Condition(
        name="still_trading_2026_06",
        description=(
            "Last session on or after 2026-06-01 — the name is still quoted. ⚠️ A DATE "
            "metric is expressed as a numeric DISTANCE in days (`MAX(date) - cutoff`) "
            "because `value` is one double-precision column for every condition; the "
            "test is therefore `>= 0`, and the number the table stores is 'days past "
            "the cutoff'. ⚠️ This is a STALENESS check on the DATA, not a delisting "
            "check on the MARKET: `silver.stocks_basic` holds no delisted name at all "
            "(CLAUDE.md §2c), so what it actually catches is a ticker whose scrape "
            "stopped."
        ),
        source=f"{SILVER_SCHEMA}.stocks_basic",
        metric="MAX(date) - %(cutoff)s",
        op=">=",
        threshold=0.0,
        unit="days past cutoff",
        params={"cutoff": date(2026, 6, 1)},
        on_missing="reject",
    )
)

# ── leverage ─────────────────────────────────────────────────────────────────────
register(
    Condition(
        name="debt_to_equity_max_12",
        description=(
            "Latest reported total liabilities / owners' equity at or below 12. "
            "⚠️ TWELVE, NOT THREE, because the only fundamentals this database holds "
            "are BANK fundamentals and a bank is levered by construction — measured "
            "2026-08-22, ACB is 9.44x and VCB 9.90x. A 2-3 threshold, right for an "
            "industrial, rejects every bank in the country. "
            "⚠️ AND IT ABSTAINS ON A MISSING VALUE (`on_missing='keep'`) because "
            "`gold.stocks_financials_bank_fa` holds TWO tickers of 781 — on 'reject' "
            "this one condition would cut any screen it joins down to ACB and VCB. "
            "Read `val__debt_to_equity_max_12` before trusting a pass: a NULL there "
            "means NOT MEASURED, which CLAUDE.md §5 rule 2 says is an unknown and "
            "never a pass."
        ),
        source=f"{GOLD_SCHEMA}.stocks_financials_bank_fa",
        metric=(
            "balance_sheet_tong_no_phai_tra "
            "/ NULLIF(balance_sheet_viii_von_chu_so_huu, 0)"
        ),
        op="<=",
        threshold=12.0,
        unit="x",
        latest_by="date",
        where="date >= %(start)s",
        params={"start": date(2009, 1, 1)},
        on_missing="keep",
    )
)


# ══════════════════════════════════════════════════════════════════════════════
# THE SCREENS — one entry here is one new `unified_schema_<name>`
# ══════════════════════════════════════════════════════════════════════════════
SCREENS: Dict[str, Screen] = {
    s.name: s
    for s in (
        Screen(
            name="PRICE10K",
            description=(
                "Every name whose close_raw never fell below 10,000 VND on any session "
                "from 2026-01-01. ONE condition, so the screen and the condition mean "
                "the same thing and the table is a clean reference for what that cut "
                "alone costs."
            ),
            conditions=("close_raw_min_10k",),
        ),
        Screen(
            name="LIQUID",
            description=(
                "Tradeable names: 1 bn VND/session median matched turnover, trading on "
                "80%+ of sessions, 200+ sessions of history, still quoted. NO price and "
                "NO leverage condition — this is the liquidity cut by itself, so its "
                "difference against QUALITY prices the other two."
            ),
            conditions=(
                "turnover_median_1bn",
                "traded_days_ratio_80",
                "sessions_min_200",
                "still_trading_2026_06",
            ),
        ),
        Screen(
            name="QUALITY",
            description=(
                "LIQUID plus the two 'bad stock' cuts: a 5,000 VND median price floor "
                "and a debt/equity ceiling. ⚠️ The leverage condition ABSTAINS on 779 "
                "of 781 names for want of fundamentals — see `debt_to_equity_max_12`."
            ),
            conditions=(
                "close_raw_median_5k",
                "turnover_median_1bn",
                "traded_days_ratio_80",
                "sessions_min_200",
                "still_trading_2026_06",
                "debt_to_equity_max_12",
            ),
        ),
    )
}


def screen(name: str) -> Screen:
    key = (name or "").upper()
    if key not in SCREENS:
        raise PipelineError(
            f"Unknown screen {name!r}. Defined: {', '.join(sorted(SCREENS))}. Add one "
            f"to `SCREENS` in {__name__}, then to `UNIFIED_PARTITIONS` and config.json."
        )
    return SCREENS[key]


def is_screen(name: str) -> bool:
    return (name or "").upper() in SCREENS


def member_predicate(name: str) -> Tuple[str, tuple]:
    """The `silver.stocks_basic` predicate selecting this screen's members.

    The shape `DataPreprocessor._helper_unified_member_filter` returns: a bare boolean
    expression with no `WHERE`/`AND`, plus its bound parameters.

    ⚠️ **A SUB-SELECT, NOT AN INLINED TICKER LIST**, and the difference is when
    membership is read. A list would freeze at the moment this function was called;
    the sub-select reads the screen table at query time, so re-materialising the filter
    layer and then rebuilding `pool__basic` tracks it. The cost is that the screen table
    must EXIST — `_helper_unified_member_filter` checks for it and names the command
    rather than letting PostgreSQL raise `relation does not exist` from inside a CTAS.
    """
    s = screen(name)
    return (
        f"(exchange, ticker) IN ("
        f"SELECT exchange, ticker FROM {s.qualified_table} WHERE passes)",
        (),
    )


# ══════════════════════════════════════════════════════════════════════════════
# SQL GENERATION — pure, so it is testable without a database
# ══════════════════════════════════════════════════════════════════════════════
def _namespace(condition: Condition, index: int) -> Tuple[str, str, Dict[str, Any]]:
    """Rewrite one condition's `%(x)s` refs to `%(c<i>__x)s` and return its params.

    Every condition becomes a CTE in ONE statement, so two conditions using `start` for
    different windows would otherwise collide on a single dict key and the SECOND one
    would silently win.
    """
    prefix = f"c{index}__"

    def rename(text: str) -> str:
        return _PARAM_REF.sub(lambda m: f"%({prefix}{m.group(1)})s", text)

    params = {f"{prefix}{k}": v for k, v in (condition.params or {}).items()}
    return rename(condition.metric), rename(condition.where), params


def _cte(condition: Condition, index: int) -> Tuple[str, Dict[str, Any]]:
    """One condition's CTE: `(exchange, ticker, value)`, at most one row per ticker."""
    metric, where, params = _namespace(condition, index)
    alias = f"c{index}"

    if condition.latest_by:
        # ⚠️ `(metric) IS NOT NULL` is GENERATED, not left to the author. "The latest
        # row" and "the latest row that reported this number" differ by 1,025 rows on
        # ACB alone, and only the second is ever what a fundamental screen means.
        clauses = [f"({metric}) IS NOT NULL"]
        if where:
            clauses.insert(0, f"({where})")
        body = (
            f"SELECT DISTINCT ON (exchange, ticker) exchange, ticker, "
            f"({metric})::double precision AS value "
            f"FROM {condition.source} "
            f"WHERE {' AND '.join(clauses)} "
            f"ORDER BY exchange, ticker, {condition.latest_by} DESC"
        )
    else:
        where_sql = f" WHERE {where}" if where else ""
        body = (
            f"SELECT exchange, ticker, ({metric})::double precision AS value "
            f"FROM {condition.source}{where_sql} "
            f"GROUP BY exchange, ticker"
        )
    return f"{alias} AS ({body})", params


def _pass_expression(condition: Condition, index: int) -> str:
    """The total boolean for one condition — NEVER NULL, whatever the data does.

    `passes` is `AND` over these, and a NULL anywhere would make `passes` NULL, which
    reads downstream as "not selected" while meaning "not known". `on_missing` is what
    turns the three-valued comparison back into two.
    """
    value = f"c{index}.value"
    compare = f"{value} {condition.op} (%(c{index}__threshold)s)::double precision"
    if condition.on_missing == "keep":
        return f"({value} IS NULL OR {compare})"
    return f"({value} IS NOT NULL AND {compare})"


def build_universe_sql(scr: Screen) -> Tuple[str, Dict[str, Any]]:
    """`(CREATE TABLE AS …, params)` for one screen. Pure — touches no database.

    ⚠️ **`CREATE TABLE AS`, never a pandas round-trip**, for the reason CLAUDE.md §5
    rule 15 gives: psycopg2 hands `numeric` back as `Decimal`, a DataFrame carries that
    as dtype `object`, and the writer maps `object` to VARCHAR. A screen read out and
    written back would hold every measurement as TEXT while looking fine.

    ⚠️ **EVERY CANDIDATE IS WRITTEN, NOT ONLY THE SURVIVORS.** 781 rows with their
    measurements, `passes` and `first_failed`. A table of survivors alone answers "who
    is in" and nothing else; this one answers "why is HPG out", which is the question
    anyone tuning a threshold actually has.
    """
    conditions = scr.resolve()
    ctes: List[str] = [
        f"candidates AS (SELECT DISTINCT exchange, ticker FROM {CANDIDATE_SOURCE})"
    ]
    params: Dict[str, Any] = {}
    selects: List[str] = ["b.exchange", "b.ticker"]
    joins: List[str] = []
    passes: List[str] = []
    cases: List[str] = []

    for i, condition in enumerate(conditions):
        cte, cte_params = _cte(condition, i)
        ctes.append(cte)
        params.update(cte_params)
        params[f"c{i}__threshold"] = float(condition.threshold)

        expression = _pass_expression(condition, i)
        passes.append(expression)
        selects.append(f'c{i}.value AS "{condition.value_column}"')
        selects.append(f'{expression} AS "{condition.pass_column}"')
        joins.append(
            f"LEFT JOIN c{i} ON c{i}.exchange = b.exchange AND c{i}.ticker = b.ticker"
        )
        # ⚠️ The CASE arms are in SCREEN ORDER, so `first_failed` names the FIRST
        # condition a ticker fails and the column is a stable audit rather than
        # whichever one the planner happened to evaluate.
        cases.append(f"WHEN NOT {expression} THEN '{condition.name}'")

    selects.append(f"({' AND '.join(passes)}) AS passes")
    selects.append(f"CASE {' '.join(cases)} ELSE NULL END AS first_failed")

    sql = (
        f"CREATE TABLE {scr.qualified_table} AS WITH "
        + ", ".join(ctes)
        + " SELECT "
        + ", ".join(selects)
        + " FROM candidates b "
        + " ".join(joins)
    )
    return sql, params


def build_comment(scr: Screen, built_at: datetime | None = None) -> str:
    """The table `COMMENT` — the screen's full definition, as JSON, on the table itself.

    ⚠️ This is the only copy of the definition that travels WITH the data. A screen
    edited in Python and not re-materialised otherwise looks identical from the database
    side; comparing this comment against `build_comment` for the current `SCREENS` is
    how a stale universe table is detected rather than assumed fresh.
    """
    stamp = (built_at or datetime.now(timezone.utc)).isoformat(timespec="seconds")
    payload = {
        "screen": scr.name,
        "description": scr.description,
        "built_at": stamp,
        "not_point_in_time": (
            "Membership is decided from the windows below and applied to the WHOLE "
            "history of every pool built on it. A z against a within-date shuffle is "
            "protected; any CAGR/Sharpe read off this universe is not. CLAUDE.md §2c."
        ),
        "conditions": [
            {
                "name": c.name,
                "source": c.source,
                "metric": c.metric,
                "test": f"{c.op} {c.threshold:g}{(' ' + c.unit) if c.unit else ''}",
                "window": c.window(),
                "mode": (
                    f"latest non-null by {c.latest_by}" if c.latest_by else "aggregate"
                ),
                "on_missing": c.on_missing,
                "description": c.description,
            }
            for c in scr.resolve()
        ],
    }
    return json.dumps(payload, ensure_ascii=False, default=str)


def parse_comment(comment: str | None) -> Dict[str, Any]:
    """`build_comment`'s inverse, tolerant of an absent or hand-edited comment."""
    if not comment:
        return {}
    try:
        parsed = json.loads(comment)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def definition_fingerprint(payload: Mapping[str, Any]) -> str:
    """Everything about a screen EXCEPT when it was built, as a comparable string.

    `built_at` moves on every materialisation and says nothing about whether the
    definition changed, so it is excluded — the same reason a run fingerprint excludes
    its own timestamp.
    """
    body = {k: v for k, v in dict(payload).items() if k != "built_at"}
    return json.dumps(body, ensure_ascii=False, sort_keys=True, default=str)
