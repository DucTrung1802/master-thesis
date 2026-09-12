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


# ── the alternate mode: the one block where GPU still buys cells (`ALT-2`) ────

_CASH_FLOW = "cash_flow"          # `cafef_pdf_parser.CASH_FLOW`, spelled here so this file
                                  # imports no parser module to name one report


class _AltBuilder:
    """A builder that answers the three questions `alternate_quarters` asks of one."""

    def __init__(self, alternates):
        self._alternates = alternates

    def alternates(self, exchange, symbol, chosen):
        return list(self._alternates.get(chosen.get("period"), []))


def _alt_env(monkeypatch, tmp_path, *, open_by_period, alternates, on_disk):
    """Stand the three modules `alternate_quarters` reads behind a canned universe."""
    fleet.anchor()
    from web_scraper import cafef_financials as fin
    from web_scraper import pdf_ocr_job as job

    monkeypatch.setattr(fin, "PDFS_DIR", str(tmp_path))
    for name in on_disk:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"%PDF-1.4")

    tasks = [job.DocumentTask(
        exchange="HOSE", symbol="AAA", period=period, template="corp",
        path="/dev/null", file=f"{period}.pdf", consolidated="True", assurance="audited",
        cumulative=False, index_row={"period": period}) for period in open_by_period]

    monkeypatch.setattr(fleet, "anchor", lambda: None)
    monkeypatch.setattr(job, "resolve_template", lambda b, s: ("corp", "resolved"))
    monkeypatch.setattr(job, "plan", lambda *a, **k: list(tasks))
    monkeypatch.setattr(job, "parsed_reports",
                        lambda b, task: [r for r in job.REPORTS
                                         if r not in open_by_period[task.period]])
    monkeypatch.setattr(fin, "FinancialsBuilder", lambda **k: _AltBuilder(alternates))
    return job


def test_a_quarter_whose_alternate_was_never_asked_IS_the_plan(monkeypatch, tmp_path):
    """⚠️ **`ASK-1` RETIRES THIS WORK CORRECTLY AND THAT IS WHY IT NEEDS ITS OWN CENSUS.** The
    period HAS been opened — on the chosen filing — so `unasked_quarters` counts it asked and
    a `skip_exhausted=True` plan proposes none of it. `documents()` returns one filing per
    period and **asking a DIFFERENT filing is a new question** (`ALT-2`): 101 documents and
    121 cells over VN30 on 2026-09-13, after 414 of 536 open cells had met the full cascade.
    """
    job = _alt_env(
        monkeypatch, tmp_path,
        open_by_period={"Q1-2020": [_CASH_FLOW]},
        alternates={"Q1-2020": [{"path": "files/HOSE_AAA/alt.pdf", "file": "alt.pdf",
                                 "assurance": "reviewed"}]},
        on_disk=["files/HOSE_AAA/alt.pdf"])
    monkeypatch.setattr(fleet, "_retried_periods", lambda *a, **k: set())

    census = fleet.alternate_quarters([("AAA", "HOSE")])

    assert census["AAA"]["quarters"] == ["2020-Q1"]
    assert census["AAA"]["cells"] == 1


def test_a_period_some_run_ALREADY_retried_is_not_asked_twice(monkeypatch, tmp_path):
    """⚠️ **THE TEST IS THE RUN FOLDER'S LOG AND NOT A FIELD, BECAUSE THERE IS NO FIELD.**
    `_alternate_retry` announces itself with `retrying on the …`, and it over-counts a period
    holding two alternates of which one was asked — which can only make the plan SMALLER, and
    that is the conservative direction for a plan that spends GPU.
    """
    job = _alt_env(
        monkeypatch, tmp_path,
        open_by_period={"Q1-2020": [_CASH_FLOW]},
        alternates={"Q1-2020": [{"path": "files/HOSE_AAA/alt.pdf", "file": "alt.pdf",
                                 "assurance": "reviewed"}]},
        on_disk=["files/HOSE_AAA/alt.pdf"])
    monkeypatch.setattr(fleet, "_retried_periods", lambda *a, **k: {"2020-Q1"})

    assert fleet.alternate_quarters([("AAA", "HOSE")]) == {}


def test_an_alternate_only_in_the_INDEX_is_not_a_question_this_machine_can_ask(
        monkeypatch, tmp_path):
    """⚠️ The index is what CafeF ADVERTISES and the files are what was scraped. Putting an
    unscraped filing on a lane buys a document that is skipped with a warning — `ALT-2`'s
    other half, which is the warning existing at all.
    """
    job = _alt_env(
        monkeypatch, tmp_path,
        open_by_period={"Q1-2020": [_CASH_FLOW]},
        alternates={"Q1-2020": [{"path": "files/HOSE_AAA/never_scraped.pdf",
                                 "file": "never_scraped.pdf", "assurance": "reviewed"}]},
        on_disk=[])
    monkeypatch.setattr(fleet, "_retried_periods", lambda *a, **k: set())

    assert fleet.alternate_quarters([("AAA", "HOSE")]) == {}


def test_a_SOLID_quarter_is_never_re_opened_for_its_alternate(monkeypatch, tmp_path):
    """A period with all three statements on disk has nothing to win, whatever it holds."""
    job = _alt_env(
        monkeypatch, tmp_path,
        open_by_period={"Q1-2020": []},
        alternates={"Q1-2020": [{"path": "files/HOSE_AAA/alt.pdf", "file": "alt.pdf",
                                 "assurance": "reviewed"}]},
        on_disk=["files/HOSE_AAA/alt.pdf"])
    monkeypatch.setattr(fleet, "_retried_periods", lambda *a, **k: set())

    assert fleet.alternate_quarters([("AAA", "HOSE")]) == {}


def test_the_mode_reaches_the_lane_subprocess_as_an_ARGUMENT():
    """⚠️ **A LANE THAT DEFAULTED TO `open` WOULD SPEND THE WHOLE FLEET ON THE WRONG WORK, AND
    IT WOULD LOOK LIKE A SUCCESSFUL RUN** — 311 documents of cells the cascade has already
    lost, ~13 h, and a coverage delta of about zero. The parent decides the mode and the child
    is TOLD, the same way the account is (`run_kaggle`'s `force_label`).
    """
    argv = fleet._lane_command(
        fleet.Lane(name="local", kind="local", tickers=["AAA"]),
        apply=True, rehearse=False, mode="alternates")

    assert argv[argv.index("--mode") + 1] == "alternates"
    # and the default is the ordinary gap plan, never the narrower question
    plain = fleet._lane_command(fleet.Lane(name="local", kind="local", tickers=["AAA"]),
                                apply=True, rehearse=False)
    assert plain[plain.index("--mode") + 1] == "open"


def test_a_quarter_BLOCKED_on_a_decumulation_operand_is_not_put_on_a_lane(
        monkeypatch, tmp_path):
    """⚠️ **`OPB-1` ARRIVING BY A SECOND ROUTE, AND THE FIRST ALTERNATES FLEET WALKED INTO
    IT.** A quarter whose ONLY open report is a cumulative income statement cannot be written
    until its Q1..Q(q-1) operands are `pdf` rows, and re-reading a different FILING of it
    changes nothing about that. Measured 2026-09-13: the census proposed **101 documents** and
    **59 of them were in this state** — the local lane planned 21 PLX documents and `run_batch`
    skipped 11 at run time, each printing *"its only open report is a CUMULATIVE income
    statement the merge still cannot write"*. The honest plan is **42 documents**.

    ⚠️ **THE TEST IS THE MERGE'S OWN `_quarter_priors` AND NOT A SECOND COPY**, the same rule
    `plan_batch` applies — asked here with NO `pending`, because a one-document alternate retry
    brings no other quarter with it. That is stricter than `plan_batch`'s question, where a
    batch may win the operand in the same pass, and stricter is the right direction for a plan
    that spends GPU on a cell it cannot bank.
    """
    fleet.anchor()
    from web_scraper import cafef_financials as fin
    from web_scraper import pdf_ocr_merge

    job = _alt_env(
        monkeypatch, tmp_path,
        open_by_period={"Q4-2020": [fin.INCOME_STATEMENT]},
        alternates={"Q4-2020": [{"path": "files/HOSE_AAA/alt.pdf", "file": "alt.pdf",
                                 "assurance": "reviewed"}]},
        on_disk=["files/HOSE_AAA/alt.pdf"])
    monkeypatch.setattr(fleet, "_retried_periods", lambda *a, **k: set())
    # the task has to be CUMULATIVE for the block to apply, and its operand absent
    monkeypatch.setattr(pdf_ocr_merge, "_quarter_priors",
                        lambda *a, **k: (None, "Q1-2020 is `missing` on disk"))
    from web_scraper import pdf_ocr_job as jb
    tasks = [jb.DocumentTask(
        exchange="HOSE", symbol="AAA", period="Q4-2020", template="corp", path="/dev/null",
        file="q4.pdf", consolidated="True", assurance="audited", cumulative=True,
        index_row={"period": "Q4-2020"})]
    monkeypatch.setattr(jb, "plan", lambda *a, **k: list(tasks))

    assert fleet.alternate_quarters([("AAA", "HOSE")]) == {}


def test_a_quarter_missing_MORE_than_the_income_statement_is_still_asked(
        monkeypatch, tmp_path):
    """⚠️ **THE BLOCK IS NARROW ON PURPOSE.** `OPB-1` applies only where the income statement
    is the ONLY thing still open — a quarter also missing its balance sheet has a cell the
    alternate can win outright, and refusing it would be the block over-reaching.
    """
    fleet.anchor()
    from web_scraper import cafef_financials as fin
    from web_scraper import pdf_ocr_job as jb
    from web_scraper import pdf_ocr_merge

    _alt_env(monkeypatch, tmp_path,
             open_by_period={"Q4-2020": [fin.INCOME_STATEMENT, fin.BALANCE_SHEET]},
             alternates={"Q4-2020": [{"path": "files/HOSE_AAA/alt.pdf", "file": "alt.pdf",
                                      "assurance": "reviewed"}]},
             on_disk=["files/HOSE_AAA/alt.pdf"])
    monkeypatch.setattr(fleet, "_retried_periods", lambda *a, **k: set())
    monkeypatch.setattr(pdf_ocr_merge, "_quarter_priors",
                        lambda *a, **k: (None, "Q1-2020 is `missing` on disk"))
    tasks = [jb.DocumentTask(
        exchange="HOSE", symbol="AAA", period="Q4-2020", template="corp", path="/dev/null",
        file="q4.pdf", consolidated="True", assurance="audited", cumulative=True,
        index_row={"period": "Q4-2020"})]
    monkeypatch.setattr(jb, "plan", lambda *a, **k: list(tasks))

    census = fleet.alternate_quarters([("AAA", "HOSE")])

    assert census["AAA"]["quarters"] == ["2020-Q4"]
    assert census["AAA"]["cells"] == 2
