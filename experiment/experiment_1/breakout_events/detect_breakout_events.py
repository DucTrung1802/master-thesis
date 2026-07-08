"""
Detect dramatic upward price run-ups ("breakout events") for VCB.

Reads close prices from unified_schema.unified_vcb (database_main_v2) and writes
one standardized event table per (metric, threshold) filter next to this script.

Detection is a fixed swing-high catalog, independent of any threshold: a day is
an apex if its close is the highest within +/- SWING_WIN trading days. Each output
file then selects the apexes whose gain meets that file's threshold, so lower
thresholds always yield supersets of higher ones (monotonic).

The event WINDOW is tied to the file's gain horizon N (5 or 10 trading days). The
"correct N-day period" is [peak-N, peak]; we pad it by PAD trading days on each
side so the window is tight around the actual move:

    start_date = peak_date - N - PAD       (PAD days before the period start)
    end_date   = peak_date + PAD           (PAD days after the apex)

For each event we report:
    exchange          listing exchange (HOSE)
    ticker            instrument (VCB)
    start_date        peak_date - N - PAD  (left edge of the padded window)
    peak_date         apex: highest close within +/- SWING_WIN days
    end_date          peak_date + PAD      (right edge of the padded window)
    start_close       close on start_date
    peak_close        close on peak_date (apex price)
    end_close         close on end_date
    gain_5d_pct       trailing 5-day  gain of close, measured at peak_date
    gain_10d_pct      trailing 10-day gain of close, measured at peak_date
    max_1d_pct        largest single-day pct gain inside the N-day period
    post_peak_10d_pct forward 10-day return after the apex (continuation vs reversion)
"""

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# ── Parameters ────────────────────────────────────────────────────────────
SWING_WIN = 5          # apex = highest close within +/- this many trading days
PAD       = 2          # trading days padded on each side of the N-day period

# Each (column, threshold, horizon_N) emits one file, filtered on the apex's
# value of that column; the metric and threshold are encoded in the file name.
GAIN_FILTERS = [
    ("gain_10d_pct", 15.0, 10),   # breakout_events_gain10d_gte15pct.csv
    ("gain_10d_pct", 10.0, 10),   # breakout_events_gain10d_gte10pct.csv
    ("gain_10d_pct",  5.0, 10),   # breakout_events_gain10d_gte5pct.csv
    ("gain_5d_pct",   5.0,  5),   # breakout_events_gain5d_gte5pct.csv
]
SHORT = {"gain_10d_pct": "gain10d", "gain_5d_pct": "gain5d"}

HERE = os.path.dirname(os.path.abspath(__file__))


def load_close() -> pd.DataFrame:
    load_dotenv(os.path.join(HERE, "..", "..", "..", ".env"))
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2",
    )
    df = pd.read_sql(
        "SELECT exchange, ticker, date, close "
        "FROM unified_schema.unified_vcb ORDER BY date", conn)
    conn.close()
    df["date"] = pd.to_datetime(df["date"])
    return df


def find_apexes(s: pd.Series) -> list[int]:
    """Positions of swing-high apexes: close is the max over +/- SWING_WIN days."""
    n = len(s)
    roll_max = s.rolling(2 * SWING_WIN + 1, center=True).max()
    cand = [i for i in range(SWING_WIN, n - SWING_WIN)
            if s.iloc[i] == roll_max.iloc[i]]
    # collapse tied/adjacent apexes (flat tops) within SWING_WIN -> keep highest
    apex_pos: list[int] = []
    for i in cand:
        if apex_pos and i - apex_pos[-1] <= SWING_WIN:
            if s.iloc[i] > s.iloc[apex_pos[-1]]:
                apex_pos[-1] = i
        else:
            apex_pos.append(i)
    return apex_pos


def build_events(df: pd.DataFrame, apex_pos: list[int], horizon: int) -> pd.DataFrame:
    """Build the event table for a given gain horizon N (window = N + 2*PAD days)."""
    exchange = df["exchange"].iloc[0]
    ticker = df["ticker"].iloc[0]
    s = df.set_index("date")["close"]
    n = len(s)

    r5 = (s / s.shift(5) - 1) * 100
    r10 = (s / s.shift(10) - 1) * 100
    ret1 = s.pct_change() * 100

    rows = []
    for pk in apex_pos:
        peak_date = s.index[pk]
        peak_close = s.iloc[pk]

        st = max(0, pk - horizon - PAD)       # padded window start
        en = min(n - 1, pk + PAD)             # padded window end
        gp = max(0, pk - horizon)             # start of the N-day gain period

        max_1d = ret1.iloc[gp + 1: pk + 1].max() if pk > gp else float("nan")
        post = (s.iloc[min(pk + 10, n - 1)] / peak_close - 1) * 100

        rows.append({
            "exchange": exchange,
            "ticker": ticker,
            "start_date": s.index[st].date(),
            "peak_date": peak_date.date(),
            "end_date": s.index[en].date(),
            "start_close": round(s.iloc[st], 1),
            "peak_close": round(peak_close, 1),
            "end_close": round(s.iloc[en], 1),
            "gain_5d_pct": round(r5.iloc[pk], 2),
            "gain_10d_pct": round(r10.iloc[pk], 2),
            "max_1d_pct": round(max_1d, 2),
            "post_peak_10d_pct": round(post, 2),
        })

    return pd.DataFrame(rows)


if __name__ == "__main__":
    df = load_close()
    apex_pos = find_apexes(df.set_index("date")["close"])

    for col, thr, horizon in GAIN_FILTERS:
        events = build_events(df, apex_pos, horizon)
        sub = events[events[col] >= thr].sort_values(
            "start_date", ascending=True).reset_index(drop=True)
        out = os.path.join(
            HERE, f"breakout_events_{SHORT[col]}_gte{int(thr)}pct.csv")
        sub.to_csv(out, index=False)
        print(f"{col} >= {thr:g}%  (N={horizon})  ->  {len(sub):3d} events  ->  "
              f"{os.path.basename(out)}")
