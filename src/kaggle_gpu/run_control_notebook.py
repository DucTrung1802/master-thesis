"""Execute a `RUN__pdf_ocr_control*.ipynb` clone headlessly, streaming its output to a log.

⚠️ **THIS IS THE `F2` RUN, NOT A NEW MECHANISM.** It executes the clone's own cells, in order,
top to bottom — the same thing opening it in Jupyter does. Nothing here decides a parameter,
skips a cell, or writes a statement CSV: the notebook does all of that.

Two reasons it exists rather than `jupyter nbconvert --execute`:

  * ⚠️ **nbconvert shows nothing until the notebook ends**, and a whole-ticker parse is hours.
    A run whose progress cannot be read is a run nobody can tell from a hung one. Every stream
    line is written here as it arrives and flushed (`CLAUDE.md` §5 rule 20).
  * ⚠️ **The executed notebook must survive an interrupt.** The notebook is saved after EVERY
    cell, so stopping the run keeps the outputs of the cells that finished. The parse's own
    durability is separate and stronger — `MERGE_EACH` upserts each finished quarter — this
    only protects the record of it.

⚠️ **`PYTHONUTF8` IS SET BY THE CALLER, NOT HERE.** The parse logs Vietnamese account labels
and this machine is cp1252 (§5 rule 18); `sys.stdout.reconfigure` below covers this process,
and the kernel is a subprocess that needs the environment variable.
"""

import argparse
import sys
import time
from pathlib import Path

import nbformat
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("notebook", type=Path)
    ap.add_argument("--log", type=Path, required=True)
    ap.add_argument("--timeout", type=int, default=-1,
                    help="per-cell timeout in seconds; -1 (the default) is none, which is "
                         "what a 65-filing parse needs")
    args = ap.parse_args()

    nb_path = args.notebook.resolve()
    log = args.log.open("w", encoding="utf-8", buffering=1)

    def say(line: str) -> None:
        print(line, flush=True)
        log.write(line + "\n")

    say(f"=== {nb_path.name} ===")
    say(f"started {time.strftime('%Y-%m-%d %H:%M:%S')}")
    say("")

    nb = nbformat.read(nb_path, as_version=4)
    started = time.time()

    class Streaming(NotebookClient):
        """Print each cell's stream output as it arrives, and save after every cell."""

        def output(self, outs, msg, display_id, cell_index):
            out = super().output(outs, msg, display_id, cell_index)
            content = msg.get("content", {})
            if msg.get("msg_type") == "stream":
                for line in content.get("text", "").splitlines():
                    say(line)
            elif msg.get("msg_type") == "error":
                say("⚠️ " + content.get("ename", "") + ": " + content.get("evalue", ""))
                for line in content.get("traceback", []):
                    say(line)
            return out

    # ⚠️ `on_cell_executed` is a traitlets hook on NotebookClient, NOT a method — overriding it
    # as one is silently never called. It is passed in.
    def after_cell(cell=None, cell_index=None, execute_reply=None):
        # ⚠️ Saved after EVERY cell: an interrupt at hour 6 keeps five hours of record.
        nbformat.write(nb, nb_path)
        say(f"--- cell {cell_index} done, {(time.time() - started) / 60:.1f} min elapsed ---")

    client = Streaming(nb, timeout=args.timeout, kernel_name="python3",
                       resources={"metadata": {"path": str(nb_path.parent)}},
                       allow_errors=False, on_cell_executed=after_cell)

    status = 0
    try:
        client.execute()
        say("")
        say(f"✅ notebook finished, {(time.time() - started) / 60:.1f} min")
    except CellExecutionError as exc:
        say("")
        say(f"⛔ a cell RAISED after {(time.time() - started) / 60:.1f} min — "
            f"the notebook is saved up to that cell")
        say(str(exc)[:4000])
        status = 1
    except KeyboardInterrupt:
        say("")
        say(f"⛔ interrupted after {(time.time() - started) / 60:.1f} min")
        status = 130
    finally:
        nbformat.write(nb, nb_path)
        say(f"finished {time.strftime('%Y-%m-%d %H:%M:%S')}")
        log.close()
    return status


if __name__ == "__main__":
    raise SystemExit(main())
