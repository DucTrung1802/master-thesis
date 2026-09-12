"""The fleet's decisions — which lane gets which ticker, and what a lane is told.

⚠️ Nothing here starts a process, opens a PDF, touches a card or authenticates with Kaggle.
Every test is about a choice that decides where hours of GPU go, and two of them are about
failures this repo has already paid for once: two lanes sharing a ticker (one job name, one
payload directory, one kernel slug — *"the second overwrites the first's payload, and its
kernel REPLACES the first"*) and a relative data root (`CWD-2`).
"""
import pytest

from kgpu import fleet


class _Plan:
    """The three fields `assign` reads off a `TickerPlan`."""

    def __init__(self, symbol, documents):
        self.symbol = symbol
        self.quarters = [f"2020-Q{i % 4 + 1}" for i in range(documents)]


# ── the partition ─────────────────────────────────────────────────────────────

def test_a_ticker_lands_on_exactly_one_lane():
    """⚠️ THE FAILURE IS A SILENT OVERWRITE, NOT A RACE. A pdf-ocr job's name derives from its
    symbol and scope, and the payload directory, the rehearsal directory and the kernel slug
    all come off that name — so two lanes holding one ticker do not collide loudly, the second
    simply replaces the first's kernel and parses its payload.
    """
    plans = [_Plan(s, 10) for s in ("AAA", "BBB", "CCC", "DDD", "EEE")]
    lanes = fleet.assign(plans, local_lanes=1, kaggle_accounts=["one", "two"])

    placed = [t for lane in lanes for t in lane.tickers]
    assert sorted(placed) == ["AAA", "BBB", "CCC", "DDD", "EEE"]
    assert len(placed) == len(set(placed))


def test_the_longest_ticker_goes_first_so_the_lanes_finish_together():
    """⚠️ LONGEST-PROCESSING-TIME-FIRST, AND ROUND-ROBIN IS THE THING IT REPLACES.

    A 54-document ticker beside a 6-document one is nine hours against one. The universe file
    is ordered by liquidity, so round-robin over it puts the big names on the first lanes and
    leaves the rest idle. LPT is the standard greedy answer: biggest remaining ticker onto the
    emptiest lane.
    """
    plans = [_Plan("BIG", 50), _Plan("MID", 30), _Plan("SMALL", 20)]
    lanes = fleet.assign(plans, local_lanes=2, kaggle_accounts=[])

    loads = sorted(lane.documents for lane in lanes)
    assert loads == [50, 50]        # not [80, 20], which is what order-preserving gives


def test_two_sessions_per_account_are_two_lanes():
    """⚠️ KAGGLE ALLOWS 2 CONCURRENT GPU SESSIONS PER ACCOUNT, SO TWO ACCOUNTS ARE FOUR LANES
    — and this repo drove it as two until 2026-09-12. It buys WALL-CLOCK and not quota: the
    30 GPU-h/week is spent twice as fast.
    """
    plans = [_Plan(f"T{i}", 5) for i in range(8)]
    lanes = fleet.assign(plans, local_lanes=1, kaggle_accounts=["one", "two"])

    assert len(lanes) == 5
    assert sum(1 for l in lanes if l.kind == "kaggle") == 4
    assert {l.name for l in lanes if l.account == "one"} == {"kaggle:one#1", "kaggle:one#2"}


def test_a_lane_with_nothing_on_it_is_not_returned():
    """⚠️ A lane handed an empty plan spends a process launch to be told so — `ONLY_MISSING`'s
    retirement measured 70 of those in one run, reported as `exit 1 and NO run folder`."""
    lanes = fleet.assign([_Plan("ONLY", 3)], local_lanes=1, kaggle_accounts=["one", "two"])

    assert [l.name for l in lanes] == ["local"]


def test_no_lane_at_all_raises_rather_than_running_nothing():
    with pytest.raises(ValueError, match="no lanes"):
        fleet.assign([_Plan("AAA", 1)], local_lanes=0, kaggle_accounts=[])


# ── the universe ──────────────────────────────────────────────────────────────

def test_the_vn30_file_reads_thirty_hose_tickers():
    """⚠️ `utf-8-sig`, because these files carry a BOM: without it the first header reads
    `﻿no` and every lookup of the first column misses (§5 rule 18's other half)."""
    names = fleet.universe("vn30")

    assert len(names) == 30
    assert all(exchange == "HOSE" for _t, exchange in names)
    assert ("VCB", "HOSE") in names


def test_an_unknown_universe_raises_instead_of_parsing_nothing():
    with pytest.raises(ValueError, match="unknown universe"):
        fleet.universe("VN500")


# ── the cascade ───────────────────────────────────────────────────────────────

def test_onnx_only_is_the_default_because_a_fleet_spans_two_machines():
    """⚠️ `TSS-1`: `tesseract@200` is a real layer HERE and does not exist on a Kaggle worker,
    so leaving it in has the local lane and the T4 lanes running DIFFERENT cascades — and then
    `exhausted_quarters`, which reuses a refusal only when a past run brought at least this
    cascade to it, can no longer compare the two lanes' verdicts.
    """
    layers = fleet._cascade(None, None)

    assert layers and all(name.startswith("onnx") for name in layers)


def test_an_explicit_layer_list_wins_over_everything():
    assert fleet._cascade(["onnx@200"], ["easyocr"]) == ["onnx@200"]


def test_an_engine_no_layer_uses_raises_rather_than_widening_the_run():
    """⚠️ The rule is `pdf_ocr_job.layers_for_engines` and is NOT re-implemented here — a
    filter that merely dropped unknown names would run the WHOLE cascade for a typo."""
    with pytest.raises(ValueError, match="no parse layer uses engine"):
        fleet._cascade(None, ["tesseractt"])


# ── what a lane is told ───────────────────────────────────────────────────────

def test_a_kaggle_lane_is_always_told_its_account():
    """⚠️ THE ACCOUNT IS FORCED, NEVER CHOSEN. Letting `accounts.select` re-rank by quota
    inside a lane would let two lanes converge on one account — and the tightest-fit rule
    exists to answer *"which balance covers this run"*, which the fleet answered when it built
    the lanes.
    """
    lane = fleet.Lane(name="kaggle:one#2", kind="kaggle", account="one", tickers=["AAA", "BBB"])
    argv = fleet._lane_command(lane, apply=True, rehearse=False)

    assert "--account" in argv and argv[argv.index("--account") + 1] == "one"
    assert argv[argv.index("--tickers") + 1] == "AAA,BBB"
    assert "--dry-run" not in argv


def test_a_dry_run_lane_is_told_so_and_a_rehearsal_is_opt_in():
    lane = fleet.Lane(name="local", kind="local", tickers=["AAA"])
    argv = fleet._lane_command(lane, apply=False, rehearse=True, engines=["easyocr"])

    assert "--dry-run" in argv and "--rehearse" in argv
    assert argv[argv.index("--engine") + 1] == "easyocr"


def test_the_data_root_is_anchored_to_the_repo_and_not_to_the_cwd(monkeypatch):
    """⚠️ `CWD-2`, AND IT FIRED ON THE FIRST FLEET PLAN EVER RUN. `DEFAULT_DATA_ROOT` is the
    relative `raw_data/cafef` and every `kgpu` command runs from `src/kaggle_gpu/`, so the plan
    asked for `src/kaggle_gpu/raw_data/cafef/pdfs/index/HOSE_ACB.csv` and reported *"run
    CafeFPdfScraper for ACB first"* about a ticker whose index had been on disk for weeks.
    """
    seen = {}
    fleet.repo_src_on_path()
    from web_scraper import pdf_ocr_job as job

    monkeypatch.setattr(job, "use_data_root", lambda root: seen.setdefault("root", root))
    fleet.anchor()

    assert seen["root"] == fleet.REPO_ROOT / "raw_data" / "cafef"
    assert seen["root"].is_absolute()
