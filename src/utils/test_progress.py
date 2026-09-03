# src\utils\test_progress.py
"""What `utils/progress.py` promises: one line, one shape, a number that only goes up.

⚠️ No network, no GPU, no PDF — these pin decisions, not a run. The two consumers are on
two machines (`web_scraper.pdf_ocr_job.Progress` where the OCR happens,
`kgpu.runner.RUN_STAGES` where the round trip is driven) and the whole reason the formatter
is one module is that a second copy would drift the moment one of them gained a decimal.
"""

import io
import sys

import pytest

from utils import progress


# ──────────────────────────────────────────────────────────────────────────────
# The line
# ──────────────────────────────────────────────────────────────────────────────


def test_the_shape_is_percent_task_sub_detail():
    line = progress.format_line(0.337, "doc 2/3 HOSE_TCB Q3-2013",
                                "layer 12/47 onnx@300", "page 40/96")
    assert line == " 33.7% - doc 2/3 HOSE_TCB Q3-2013 - layer 12/47 onnx@300 - page 40/96"


def test_one_decimal_because_a_whole_document_must_move_the_number():
    """⚠️ `34%` does not move while a 3-document run finishes a document; `33.3%` does.
    That is the request this module was written for, and it is the format, not a preference."""
    assert progress.format_line(1 / 3, "t") == " 33.3% - t"
    assert progress.format_line(0.0, "t").startswith("  0.0%")
    assert progress.format_line(1.0, "t").startswith("100.0%")


def test_empty_segments_are_dropped_not_padded():
    """` 33.7% -  -  - up` reads as three things that failed to be named."""
    assert progress.format_line(0.5, "", "", "up") == " 50.0% - up"
    assert progress.format_line(0.5, "task", "", "") == " 50.0% - task"


def test_a_fraction_outside_zero_to_one_is_clamped_not_printed():
    """A progress line is not the place to raise — but `-12.0%` would advertise the caller's
    arithmetic bug, so the number is pinned to the range it claims to be in."""
    assert progress.format_line(-3.0, "t").startswith("  0.0%")
    assert progress.format_line(9.9, "t").startswith("100.0%")


def test_detail_of_is_what_a_reader_that_used_to_match_the_line_start_needs():
    """⚠️ Every line gained a prefix on 2026-08-30, so `startswith("WRITE ")` matches
    nothing. `detail_of` is the segment that used to BE the line."""
    line = progress.format_line(0.5, "doc 1/1", "merge", "WRITE  Q3-2013   balance_sheet")
    assert progress.detail_of(line) == "WRITE  Q3-2013   balance_sheet"
    assert progress.detail_of("not one of ours") == "not one of ours"


# ──────────────────────────────────────────────────────────────────────────────
# The number
# ──────────────────────────────────────────────────────────────────────────────


def _reporter(**kwargs):
    lines = []
    stages = progress.Stages(
        [("export", "stage payload", 10.0), ("wait", "wait kernel", 80.0),
         ("merge", "merge into repo", 10.0)],
        label="HOSE_TCB 2013-Q3", emit=lines.append, **kwargs)
    return stages, lines


def test_a_stage_floor_is_its_share_of_the_plan():
    stages, lines = _reporter()
    stages.begin("export")
    assert stages.fraction == pytest.approx(0.0)
    stages.begin("wait")
    assert stages.fraction == pytest.approx(0.10)
    stages.inside(0.5)
    assert stages.fraction == pytest.approx(0.50)
    stages.begin("merge")
    assert stages.fraction == pytest.approx(0.90)
    stages.done()
    assert stages.fraction == pytest.approx(1.0)
    assert lines[0].startswith("  0.0% - step 1/3 HOSE_TCB 2013-Q3 - stage payload")


def test_the_number_only_ever_moves_forward():
    """⚠️ A percentage that retreats is read as a bug in the run rather than in the
    reporting. `inside()` with a ratio behind where the caller already is must do nothing."""
    stages, _ = _reporter()
    stages.begin("wait")
    stages.inside(0.9)
    high = stages.fraction
    stages.inside(0.1)
    assert stages.fraction == high


def test_a_skipped_stage_claims_its_weight_rather_than_redistributing_it():
    """The plan is the plan: "we did not have to do that" is progress through it, and a
    silently re-weighted plan would make two runs of one job report different percentages
    for the same work."""
    stages, lines = _reporter()
    stages.skip("export", "refresh_data=False")
    assert stages.fraction == pytest.approx(0.10)
    assert "refresh_data=False" in lines[-1]


def test_an_unknown_stage_raises_rather_than_freezing_the_bar():
    """⚠️ A `begin("dowload")` that silently did nothing would leave the percentage frozen
    while the work ran — which looks exactly like a hung stage, and is the one failure a
    progress readout must not manufacture."""
    stages, _ = _reporter()
    with pytest.raises(KeyError):
        stages.begin("dowload")
    with pytest.raises(RuntimeError):
        stages.inside(0.5)          # before any begin(): there is no current stage


def test_a_plan_needs_stages_and_they_need_distinct_names_and_real_weights():
    with pytest.raises(ValueError):
        progress.Stages([])
    with pytest.raises(ValueError):
        progress.Stages([("a", "A", 1.0), ("a", "A again", 1.0)])
    with pytest.raises(ValueError):
        progress.Stages([("a", "A", 0.0)])


# ──────────────────────────────────────────────────────────────────────────────
# Somebody else's print()
# ──────────────────────────────────────────────────────────────────────────────


def test_capture_turns_another_module_s_output_into_the_detail_segment():
    stages, lines = _reporter()
    stages.begin("wait")
    with stages.capture():
        print("baseline: last COMPLETE run took 5.9 min")
        print("  [  3.7 min] RUNNING   63% of last")
    assert [progress.detail_of(x) for x in lines[-2:]] == [
        "baseline: last COMPLETE run took 5.9 min",
        "[  3.7 min] RUNNING   63% of last",
    ]
    assert all(" - wait kernel - " in x for x in lines[-2:])


def test_capture_emits_on_each_newline_so_a_long_poll_is_not_silent():
    """⚠️ §5 rule 20. Buffering a stage and printing it when it returns is the failure that
    lost a four-hour run — and a 30-minute Kaggle wait would show nothing until it ended."""
    stages, lines = _reporter()
    stages.begin("wait")
    with stages.capture():
        sys.stdout.write("half a ")
        assert len(lines) == 1, "a partial line must not be emitted yet"
        sys.stdout.write("line\n")
        assert len(lines) == 2, "a completed line must be emitted at once"
        sys.stdout.write("no trailing newline")
    assert progress.detail_of(lines[-1]) == "no trailing newline"


def test_a_carriage_return_ends_a_line_because_a_rewrite_is_a_new_state():
    stages, lines = _reporter()
    stages.begin("wait")
    with stages.capture():
        sys.stdout.write("\rQUEUED\rRUNNING\r")
    assert [progress.detail_of(x) for x in lines[-2:]] == ["QUEUED", "RUNNING"]


def test_a_line_emitted_from_inside_a_capture_is_not_captured_again():
    """⚠️ THE RECURSION THIS EXISTS TO PREVENT: `wait`'s poll hook advances the number while
    the capture is open, and if that line went back through the shim it would never return."""
    stages = progress.Stages([("wait", "wait kernel", 1.0)], label="X")
    written = io.StringIO()
    real, sys.stdout = sys.stdout, written
    try:
        stages.begin("wait")
        with stages.capture():
            print("from the client")
            stages.inside(0.5, "from the hook")
    finally:
        sys.stdout = real
    assert [progress.detail_of(x) for x in written.getvalue().splitlines()][-2:] == [
        "from the client", "from the hook"]


def test_the_shim_is_not_a_tty_because_kgpu_wait_rewrites_in_place_on_one():
    """`runner.wait` prints a state change or a 5-point step when stdout is redirected and
    rewrites one line when it is a terminal. Captured, it must take the first branch or a
    12-hour poll becomes 2,880 formatted lines."""
    stages, _ = _reporter()
    stages.begin("wait")
    with stages.capture():
        assert sys.stdout.isatty() is False
        assert sys.stdout.encoding == "utf-8"       # a client that reads it must get one


# ──────────────────────────────────────────────────────────────────────────────
# One line, ONE number — composing a plan out of parts that each already report
# ──────────────────────────────────────────────────────────────────────────────


def test_split_line_recognises_our_own_shape_and_nothing_else():
    ours = progress.format_line(0.337, "doc 2/3 HOSE_TCB Q3-2013",
                                "layer 12/47 onnx@300", "page 40/96")
    assert progress.split_line(ours) == ("layer 12/47 onnx@300", "page 40/96")
    # ⚠️ A caller's own text that merely LOOKS like it must pass through untouched, or a
    # capture would strip a segment the line never had.
    assert progress.split_line("50% - done") is None
    assert progress.split_line("no separator at all") is None
    assert progress.split_line("") is None


def test_a_line_with_only_a_detail_still_splits_and_reports_no_sub():
    assert progress.split_line(progress.format_line(0.5, "", "", "up")) == ("", "up")


def test_nested_capture_keeps_one_number_on_the_line():
    """The point of `nested`: `job.run` and any inner `Stages` lead with a percentage of
    THEIR OWN denominator, and printed verbatim the outer line carries two."""
    stages, lines = _reporter()
    stages.begin("wait")
    with stages.capture(nested=True):
        print(progress.format_line(0.9, "doc 1/1 HOSE_VCB Q1-2026",
                                   "layer 3/55 onnx@300", "page 8/53"))
    assert len(lines[-1].split(progress.SEPARATOR)) == 4      # percent, task, sub, detail
    assert lines[-1].count("%") == 1
    assert "90.0%" not in lines[-1]                           # the inner number is dropped
    assert lines[-1].endswith("layer 3/55 onnx@300 - page 8/53")
    assert "HOSE_TCB 2013-Q3" in lines[-1]                    # the OUTER task, not the inner


def test_nested_capture_leaves_ordinary_text_alone():
    """Which is why the flag is safe on a stage whose output is mixed."""
    stages, lines = _reporter()
    stages.begin("wait")
    with stages.capture(nested=True):
        print("uploading 86 MB")
    assert progress.detail_of(lines[-1]) == "uploading 86 MB"


def test_a_nested_run_finishing_does_not_finish_a_plan_that_outlives_it():
    """`final=False`: `kgpu.runner.run` ends by calling `done()`, and the PDF-OCR control
    notebook embeds its six stages as six of fifteen. Without this the notebook would read
    `100.0%` from the run cell onward while nine sections still had to happen."""
    stages, _ = _reporter(final=False)
    stages.begin("wait")
    stages.done("COMPLETE and pulled")
    assert stages.fraction == pytest.approx(0.9)              # the ceiling of `wait`, not 1.0
    stages.begin("merge")
    stages.end()
    assert stages.fraction == pytest.approx(1.0)


def test_final_is_the_default_so_an_ordinary_plan_is_unchanged():
    stages, _ = _reporter()
    stages.begin("export")
    stages.done()
    assert stages.fraction == pytest.approx(1.0)
