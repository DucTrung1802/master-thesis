"""The ONE definition of the binary EVENT label — "does the stock rise by at least
`gain_pct` percent within the next `horizon` sessions?".

⚠️ **THE PARAMETERS ARE THE THREE CONSTANTS BELOW, AND NOTHING ELSE NAMES THEM.** Change
`EVENT_GAIN_PCT` / `EVENT_HORIZON` / `EVENT_RULE`, re-materialise
`unified/pool__targets` for the partition, and every stage of the event chain moves with
them: the label column `pool__targets` writes, the columns `feature_selection.run`
refuses to treat as features, the table name `final_features` builds, the classifier
dataset `train_test_creator` saves and the model configs `event_chain` writes. Each
of those READS this module; none re-spells a column name.

⚠️ **TWO RULES, BECAUSE "tăng 5% trong 5 ngày tới" HAS TWO HONEST READINGS.**

| rule | column | 1 when |
|---|---|---|
| `close` | `up_5pct_5day` | `close[t+h] >= (1 + g) · close[t]` — the price h sessions out is g % higher |
| `any` | `upany_5pct_5day` | `max(close[t+1 .. t+h]) >= (1 + g) · close[t]` — some close inside the window touches +g % |
| `open` | `upopen_5pct_5day` | `close[t+h+1] >= (1 + g) · open_adjust[t+1]` — bought at the OPEN of the next session and sold `h` sessions AFTER THE ENTRY, so it holds `h + 1` sessions (`t+1 … t+h+1`) |
| `hold` | `uphold_5pct_5day` | `close[t+h] >= (1 + g) · open_adjust[t+1]` — ⚠️ **THE ONE THE USER TRADES**: bought at the OPEN of the next session and sold at the close of `t+h`, so it holds exactly `h` sessions (`t+1 … t+h`) |

⚠️ **`open` AND `hold` DIFFER BY ONE SESSION AND THE DIFFERENCE IS A WEEKEND** (2026-09-18).
Signal on a Friday: `hold` buys Monday's open and sells Friday's close — five sessions, one
calendar week. `open` buys the same Monday and sells the FOLLOWING MONDAY's close — six sessions
and a second weekend held. Measured on PNJ 2025-01-10, the extra session was **+0.96 pp of a
+2.55 % move**. ⚠️ **`h` counts differently in the two rules** — sessions after the ENTRY in
`open`, sessions HELD in `hold` — which is why they are two rules and not one parameter: a name
must say which, and `uphold_5pct_5day` reads as "five sessions held" because that is what it is.

⚠️ **THE `open` RULE EXISTS BECAUSE THE CLOSE OF `t` IS NOT A FILL** (2026-09-18): a signal
computed from session `t`'s own close can first be traded the next morning, and the picks of a
close-to-close model gap up +1.7 % overnight (+4.4 % on the names at their ceiling), which is
most of what it was paid for (`event_chain.md` §6-§7, `EXE-1`). Its label is NULL for the last `h + 1`
sessions of a series, one more than the other two rules, because it reads one session further.

Both are carried in `pool__targets` for the configured `(g, h)`; `EVENT_RULE` picks the
one the chain trains on. `any ⊇ close` by construction, so its base rate is higher.

⚠️ **THE LABEL IS NULL EXACTLY WHERE `close[t+h]` DOES NOT EXIST YET** — the last `h`
sessions of every series, the same tail as `return_{h}day`. The `any` rule could be
decided early (a touch on day t+1 already answers it) and is deliberately NOT: a label
that exists for some tail rows and not others would make the unlabelled tail depend on
the future path, which is a look-ahead in the SAMPLE SET rather than in a feature.

⚠️ **ADJUSTED CLOSE, NEVER `high`.** `pool__basic.open/high/low` are RAW prices (VCB
2009-06-30: high 60,000 against `close_adjust` 9,060), so an intraday-touch rule on
`high` would read every split as a crash. The rules above use `close_adjust` only.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple

# ---------------------------------------------------------------- THE PARAMETERS
EVENT_GAIN_PCT: float = 5.0   # percent, > 0
EVENT_HORIZON: int = 5        # trading sessions, >= 1
EVENT_RULE: str = "hold"      # "close" | "any" | "open" | "hold" — see the module docstring
# -------------------------------------------------------------------------------

RULE_PREFIX = {"close": "up", "any": "upany", "open": "upopen", "hold": "uphold"}
# The realised return of the trade each rule describes — the auxiliary target the magnitude
# models fit, and the column `event_chain` reads to price a basket. The two close rules share
# `return_{h}day`; each open-entry rule needs its own, because they exit on different sessions.
RULE_RETURN = {"close": "return_{h}day", "any": "return_{h}day",
               "open": "return_open_{h}day", "hold": "return_hold_{h}day"}
PREFIX_RULE = {v: k for k, v in RULE_PREFIX.items()}

EVENT_COLUMN = re.compile(
    r"^(?P<prefix>upany|upopen|uphold|up)_(?P<pct>\d+(?:p\d+)?)pct_(?P<horizon>\d+)day$"
)


def _pct_token(gain_pct: float) -> str:
    """`5.0 -> "5"`, `2.5 -> "2p5"` — a column name cannot carry a dot."""
    text = f"{float(gain_pct):.4f}".rstrip("0").rstrip(".")
    return text.replace(".", "p")


@dataclass(frozen=True)
class EventTarget:
    gain_pct: float = EVENT_GAIN_PCT
    horizon: int = EVENT_HORIZON
    rule: str = EVENT_RULE

    def __post_init__(self):
        if self.rule not in RULE_PREFIX:
            raise ValueError(f"rule must be one of {sorted(RULE_PREFIX)}, got {self.rule!r}")
        if not float(self.gain_pct) > 0:
            raise ValueError(f"gain_pct must be > 0, got {self.gain_pct!r}")
        if int(self.horizon) < 1:
            raise ValueError(f"horizon must be >= 1 session, got {self.horizon!r}")

    @property
    def column(self) -> str:
        return f"{RULE_PREFIX[self.rule]}_{_pct_token(self.gain_pct)}pct_{int(self.horizon)}day"

    @property
    def multiplier(self) -> str:
        """`1 + g/100` as an exact decimal literal, so `numeric` arithmetic stays exact
        at the tick boundary (20,000 -> 21,000 is exactly +5 % and must label 1)."""
        return f"{1.0 + float(self.gain_pct) / 100.0:.10f}".rstrip("0")

    @property
    def enters_at_open(self) -> bool:
        """True when the trade is bought at the NEXT session's open rather than at `t`'s
        close — the two rules whose tail also depends on a scraped `open`."""
        return self.rule in ("open", "hold")

    @property
    def exit_offset(self) -> int:
        """Sessions after `t` at which the position is SOLD — `h`, and `h + 1` for the
        `open` rule, whose `h` counts from the entry rather than from the signal."""
        return int(self.horizon) + (1 if self.rule == "open" else 0)

    @property
    def held_sessions(self) -> int:
        """How many sessions the position is actually held: `t+1 … t+exit_offset` for an
        open-entry rule, and `h` for a close-to-close one."""
        return self.exit_offset if self.enters_at_open else int(self.horizon)

    @property
    def unlabelled(self) -> int:
        """Rows at the END of a series that cannot carry this label — the last row that
        can be labelled is the one whose exit session exists."""
        return self.exit_offset

    @property
    def aux_return(self) -> str:
        """The `pool__targets` column holding the realised return of THIS rule's trade."""
        return RULE_RETURN[self.rule].format(h=int(self.horizon))

    def sql(self, px: str = "px", window: str = "w", op: str = "op") -> str:
        """The label as one SQL expression over a named `WINDOW` ordered by date.

        `window` must be `PARTITION BY exchange, ticker ORDER BY date` with NO frame
        clause — the `any` rule adds its own frame, which PostgreSQL permits only on a
        window that has none. `op` is the ADJUSTED open, which only the `open` rule reads.
        """
        h = int(self.horizon)
        if self.enters_at_open:
            # ⚠️ Entry is the next session's open. `open` exits h sessions AFTER the entry
            # (`t+h+1`) and `hold` exits at `t+h`, holding exactly h sessions.
            entry = f"LEAD({op}, 1) OVER {window}"
            exit_ = f"LEAD({px}, {self.exit_offset}) OVER {window}"
            return (
                f"(CASE WHEN {exit_} IS NULL OR {entry} IS NULL OR {entry} = 0 THEN NULL "
                f"WHEN {exit_} >= {entry} * {self.multiplier} THEN 1.0 ELSE 0.0 END)"
                f"::double precision"
            )
        future = f"LEAD({px}, {h}) OVER {window}"
        if self.rule == "close":
            hit = f"{future} >= {px} * {self.multiplier}"
        else:
            hit = (
                f"MAX({px}) OVER ({window} ROWS BETWEEN 1 FOLLOWING AND {h} FOLLOWING)"
                f" >= {px} * {self.multiplier}"
            )
        return (
            f"(CASE WHEN {future} IS NULL OR {px} IS NULL OR {px} = 0 THEN NULL "
            f"WHEN {hit} THEN 1.0 ELSE 0.0 END)::double precision"
        )

    def describe(self) -> str:
        if self.enters_at_open:
            return (
                f"1 when close_adjust[t+{self.exit_offset}] >= (1 + {self.gain_pct:g}%) x "
                f"open_adjust[t+1] — bought at the OPEN of t+1 and sold at the close of "
                f"t+{self.exit_offset}, {self.held_sessions} sessions held — else 0; "
                f"NULL for the last {self.unlabelled} sessions"
            )
        if self.rule == "close":
            return (
                f"1 when close_adjust[t+{self.horizon}] >= (1 + {self.gain_pct:g}%) x "
                f"close_adjust[t], else 0; NULL for the last {self.horizon} sessions"
            )
        return (
            f"1 when max(close_adjust[t+1..t+{self.horizon}]) >= (1 + {self.gain_pct:g}%) "
            f"x close_adjust[t], else 0; NULL for the last {self.horizon} sessions"
        )


def parse(column: str) -> Optional[EventTarget]:
    """`"up_2p5pct_10day"` -> `EventTarget(2.5, 10, "close")`, or None."""
    match = EVENT_COLUMN.match(column or "")
    if not match:
        return None
    return EventTarget(
        gain_pct=float(match["pct"].replace("p", ".")),
        horizon=int(match["horizon"]),
        rule=PREFIX_RULE[match["prefix"]],
    )


def is_event_column(column: str) -> bool:
    return parse(column) is not None


# The event the chain TRAINS on.
DEFAULT_EVENT = EventTarget(EVENT_GAIN_PCT, EVENT_HORIZON, EVENT_RULE)

# Every event column `pool__targets` CARRIES — both rules at the configured (g, h), the
# default first. A label nobody trains on still has to be excluded from the features,
# which `feature_selection.run` does by pattern, not by this tuple.
EVENT_TARGETS: Tuple[EventTarget, ...] = (DEFAULT_EVENT,) + tuple(
    EventTarget(EVENT_GAIN_PCT, EVENT_HORIZON, rule)
    for rule in RULE_PREFIX
    if rule != EVENT_RULE
)

__all__ = [
    "DEFAULT_EVENT",
    "EVENT_GAIN_PCT",
    "EVENT_HORIZON",
    "EVENT_RULE",
    "EVENT_TARGETS",
    "EventTarget",
    "is_event_column",
    "parse",
]
