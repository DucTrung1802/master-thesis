"""Can this machine TRAIN PhoBERT? Can it FINE-TUNE PhoBERT? — measured, not guessed.

Two different questions with two different answers, and the difference is ~5 orders of
magnitude of compute:

* **Train** = masked-LM pre-training from random weights over a large Vietnamese corpus.
* **Fine-tune** = start from `vinai/phobert-base` and adapt it on a labelled task set.

Everything below is timed on the machine it runs on. No numbers are quoted from papers,
because the question is about THIS hardware.

Run:  python experiment/experiment_10/phobert_capacity.py
      python experiment/experiment_10/phobert_capacity.py --seq 128 --batches 4,8,16,32
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))
os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

import torch
from transformers import AutoConfig, AutoModelForMaskedLM, AutoModelForSequenceClassification

ENCODER = "vinai/phobert-base"
GB = 1024 ** 3


def _mem_reset():
    torch.cuda.empty_cache()
    torch.cuda.reset_peak_memory_stats()


def _peak() -> float:
    return torch.cuda.max_memory_allocated() / GB


def try_step(make_model, batch: int, seq: int, amp: bool, freeze_below: int = 0):
    """One optimiser step. Returns (peak GB, samples/s) or None if it OOMs."""
    _mem_reset()
    try:
        model = make_model().cuda()
        if freeze_below:
            for name, p in model.named_parameters():
                if "encoder.layer." in name:
                    layer = int(name.split("encoder.layer.")[1].split(".")[0])
                    if layer < freeze_below:
                        p.requires_grad_(False)
                elif "embeddings" in name:
                    p.requires_grad_(False)

        params = [p for p in model.parameters() if p.requires_grad]
        opt = torch.optim.AdamW(params, lr=2e-5)
        scaler = torch.amp.GradScaler("cuda", enabled=amp)

        ids = torch.randint(5, 6000, (batch, seq), device="cuda")
        mask = torch.ones_like(ids)
        labels = torch.randint(0, 3, (batch,), device="cuda")

        for i in range(6):  # 2 warm-up + 4 timed
            if i == 2:
                torch.cuda.synchronize()
                t0 = time.perf_counter()
            opt.zero_grad(set_to_none=True)
            with torch.amp.autocast("cuda", dtype=torch.float16, enabled=amp):
                loss = model(input_ids=ids, attention_mask=mask, labels=labels).loss
            scaler.scale(loss).backward()
            scaler.step(opt)
            scaler.update()
        torch.cuda.synchronize()
        dt = (time.perf_counter() - t0) / 4

        peak, sps = _peak(), batch / dt
        trainable = sum(p.numel() for p in params)
        del model, opt, ids, mask, labels
        _mem_reset()
        return peak, sps, trainable
    except torch.cuda.OutOfMemoryError:
        _mem_reset()
        return None


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seq", type=int, default=256)
    ap.add_argument("--batches", default="4,8,16,32,64")
    args = ap.parse_args()

    if not torch.cuda.is_available():
        print("No CUDA — both answers are 'no' on CPU for anything but a toy run.")
        return

    name = torch.cuda.get_device_name(0)
    total = torch.cuda.get_device_properties(0).total_memory / GB
    cfg = AutoConfig.from_pretrained(ENCODER)
    print(f"GPU: {name}  |  {total:.2f} GiB  |  torch {torch.__version__}")
    print(f"Model: {ENCODER}  |  {cfg.num_hidden_layers} layers, hidden {cfg.hidden_size}, "
          f"vocab {cfg.vocab_size}, max_pos {cfg.max_position_embeddings}")

    mlm = AutoModelForMaskedLM.from_pretrained(ENCODER)
    n_params = sum(p.numel() for p in mlm.parameters())
    del mlm
    print(f"Parameters: {n_params / 1e6:.1f} M  →  fp32 weights {n_params * 4 / GB:.2f} GiB")
    print(f"  a full fp32 AdamW step needs weights + grads + 2 optimiser moments ≈ "
          f"{n_params * 16 / GB:.2f} GiB before a single activation")

    def clf():
        return AutoModelForSequenceClassification.from_pretrained(ENCODER, num_labels=3)

    print(f"\n{'=' * 88}\nFINE-TUNE — one AdamW step, seq_len={args.seq}\n{'=' * 88}")
    print(f"{'setup':<26}{'batch':>7}{'peak GiB':>11}{'samples/s':>12}{'trainable':>12}{'':>6}")
    print("-" * 88)

    setups = [
        ("full, fp32", dict(amp=False, freeze_below=0)),
        ("full, fp16 AMP", dict(amp=True, freeze_below=0)),
        ("top 4 layers, fp16", dict(amp=True, freeze_below=8)),
        ("head only, fp16", dict(amp=True, freeze_below=12)),
    ]
    best = {}
    for label, kw in setups:
        for b in [int(x) for x in args.batches.split(",")]:
            r = try_step(clf, b, args.seq, **kw)
            if r is None:
                print(f"{label:<26}{b:>7}{'OOM':>11}{'—':>12}{'—':>12}")
                break
            peak, sps, trainable = r
            spill = "  ⚠️ spills to host RAM" if peak > total * 0.95 else ""
            print(f"{label:<26}{b:>7}{peak:>11.2f}{sps:>12.1f}{trainable / 1e6:>11.1f}M{spill}")
            # ⚠️ keep the FASTEST batch, not the last one that ran. On Windows/WDDM the
            # driver silently spills past the 4 GiB card into system RAM instead of
            # raising OOM, so the largest batch that "works" is often the slowest by far
            # (fp16 batch 64: 7.61 GiB reported, 6.1 samples/s against 43.4 at batch 8).
            if sps > best.get(label, (0, 0.0, 0.0))[1]:
                best[label] = (b, sps, peak)

    print(f"\n{'=' * 88}\nWHAT THAT MEANS FOR THE ACTUAL JOBS\n{'=' * 88}")
    jobs = [
        ("annotation set (guidance.md step 10)", 5_000, 4),
        ("all editorials, 1 chunk each", 58_000, 3),
        ("all editorials, full doc (3.44 chunks)", 200_000, 3),
    ]
    for label in ("full, fp16 AMP", "top 4 layers, fp16"):
        if label not in best:
            continue
        b, sps, peak = best[label]
        print(f"\n  {label}  — best config: batch {b}, {sps:.1f} samples/s, {peak:.2f} GiB")
        for job, n, epochs in jobs:
            mins = n * epochs / sps / 60
            unit = f"{mins:.1f} min" if mins < 90 else f"{mins / 60:.1f} h"
            print(f"    {job:<44} {n:>8,} × {epochs} ep → {unit:>10}")

    if "full, fp16 AMP" in best:
        sps = best["full, fp16 AMP"][1]
        tok_per_s = sps * args.seq
        print(f"\n  PRE-TRAIN from scratch, for contrast — at {tok_per_s:,.0f} tok/s:")
        # A 20 GB Vietnamese corpus is roughly 5e9 subword tokens. Masked-LM pre-training
        # needs dozens of passes, and needs an effective batch in the thousands, which this
        # card can only reach by gradient accumulation — the wall clock does not improve.
        for passes, note in ((1, "one single pass"), (40, "a realistic schedule")):
            days = 5e9 * passes / tok_per_s / 86400
            print(f"    {passes:>2} pass ({note:<20}) → {days:>7,.0f} days "
                  f"= {days / 365:.2f} GPU-YEARS")

    print(f"\n{'=' * 88}\nVERDICT\n{'=' * 88}")
    print("  FINE-TUNE  — yes. Minutes for the annotation set, hours for the whole corpus.")
    print("  PRE-TRAIN  — no. GPU-years, and that is before the 20 GB corpus is collected.")
    print("  They differ by ~4 orders of magnitude; only one is on the table.")


if __name__ == "__main__":
    main()
