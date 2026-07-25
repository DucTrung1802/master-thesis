"""
Fetch what experiment_9 runs on: the deepdoc_vietocr checkout and its ONNX models.

    ../../ocr_env9/Scripts/python.exe setup_vendor.py

Two sources, because the repo does not carry its own models:

  * the CODE from https://github.com/hoaivannguyen/deepdoc_vietocr, cloned into `vendor/`.
    It must be cloned with LFS smudging OFF: the repo's `onnx/*.onnx` are Git-LFS pointers and
    the account is over its LFS budget — "This repository exceeded its LFS budget" — so a normal
    clone dies at the first pointer and leaves the working tree EMPTY (checkout aborts, code and
    all). Skipping the smudge filter checks out the code and leaves 133-byte placeholders where
    the models would be.
  * the MODELS from the HuggingFace repo `InfiniFlow/deepdoc`, which is where RAGFlow publishes
    them and where the fork's own fallback path goes looking. They land in `vendor/…/onnx/`,
    overwriting those placeholders.

The VietOCR checkpoints (`vietocr/weight/*.pth`, ~90 MB each) are NOT LFS and come down with the
clone, so nothing extra is needed for the recogniser.
"""

# ===== Standard Library =====
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
VENDOR = os.path.join(HERE, "vendor", "deepdoc_vietocr")
ONNX_DIR = os.path.join(VENDOR, "onnx")
REPO_URL = "https://github.com/hoaivannguyen/deepdoc_vietocr.git"

# What the OCR engine and the table-structure script actually load. The HF repo also carries
# layout.laws/manual/paper variants and a few models for other RAGFlow stages; there is no reason
# to pull ~1 GB to run this experiment.
MODEL_FILES = ["det.onnx", "rec.onnx", "ocr.res", "layout.onnx", "tsr.onnx"]


def clone() -> None:
    if os.path.isdir(os.path.join(VENDOR, "module")):
        print(f"vendor checkout present: {VENDOR}")
        return
    os.makedirs(os.path.dirname(VENDOR), exist_ok=True)
    env = dict(os.environ, GIT_LFS_SKIP_SMUDGE="1")
    print(f"cloning {REPO_URL} …")
    subprocess.run(["git", "clone", "--depth", "1", REPO_URL, VENDOR], check=True, env=env)


def models() -> None:
    from huggingface_hub import snapshot_download

    missing = [f for f in MODEL_FILES
               if not os.path.exists(os.path.join(ONNX_DIR, f))
               or os.path.getsize(os.path.join(ONNX_DIR, f)) < 10_000]   # an LFS pointer
    if not missing:
        print(f"models present: {ONNX_DIR}")
        return
    print(f"downloading {missing} from InfiniFlow/deepdoc …")
    snapshot_download(repo_id="InfiniFlow/deepdoc", local_dir=ONNX_DIR,
                      allow_patterns=MODEL_FILES)


def check() -> int:
    ok = True
    for f in MODEL_FILES + [os.path.join("vietocr", "weight", "vgg_seq2seq.pth")]:
        path = os.path.join(VENDOR, "onnx", f) if f.endswith((".onnx", ".res")) \
            else os.path.join(VENDOR, f)
        size = os.path.getsize(path) if os.path.exists(path) else 0
        print(f"  {'OK ' if size > 10_000 else 'MISSING'}  {size / 1e6:8.1f} MB  {path}")
        ok = ok and size > 10_000
    return 0 if ok else 1


if __name__ == "__main__":
    clone()
    models()
    sys.exit(check())
