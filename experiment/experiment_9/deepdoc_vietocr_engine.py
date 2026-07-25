"""
experiment_9's OCR engine — https://github.com/hoaivannguyen/deepdoc_vietocr.

That repo is RAGFlow's **DeepDoc** document pipeline with its Chinese/English recogniser swapped
for **VietOCR**. What it adds over a bare detector+recogniser pair is the document machinery
around them, all of it ONNX and CPU-first:

    det.onnx        PaddleOCR DB text detection, exported to ONNX
    rec.onnx        the original recogniser — REPLACED here by VietOCR (vgg_seq2seq)
    layout.onnx     YOLOv10, 10 page-element classes (text / title / table / figure / …)
    tsr.onnx        table structure: columns, rows, column headers, row headers, spanning cells

`t_ocr.py` drives detection+recognition, `t_recognizer.py --mode=layout|tsr` drives the other
two. This module is the first of those as an engine for `ocr_pipeline.py`; `table_structure.py`
next to it is the second, kept separate because layout/TSR answer a different question (where is
the table) than the one the statement parser asks (what does each row say).

THREE ADJUSTMENTS, all documented where they are made:

  * `utils.file_utils` / `utils.settings` are shimmed. They are RAGFlow's, not the fork's, and
    the fork imports them for exactly two things — where to keep the ONNX models, and how many
    GPUs there are.
  * recognition is BATCHED and runs on the GPU. Their `TextRecognizer.__call__` loops
    `predict()` over crops one at a time, pinned to the CPU; for a 16-page filing that is ~3,000
    sequential forward passes. Same model, same checkpoint, same preprocessing — only the loop
    changes, and it has to, or the comparison against experiment_8 measures batching rather than
    OCR.
  * the detector's resize limit is adjustable. Theirs is fixed at 960 px on the long side, which
    for an A4 page scan means reading a financial table at ~85 dpi.
"""

# ===== Standard Library =====
import os
import sys
import time
import types
from typing import List, Optional, Tuple

# ===== Third-party =====
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
VENDOR = os.path.join(HERE, "vendor", "deepdoc_vietocr")
# Everything after the OCR — page classification, rows, columns, the chart of accounts, the
# comparison against CafeF — is experiment_8's `ocr_pipeline`, imported rather than reimplemented
# so that the ONLY difference between the two experiments is the engine in this file.
EXPERIMENT_8 = os.path.abspath(os.path.join(HERE, "..", "experiment_8"))
for _p in (HERE, EXPERIMENT_8):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ===== Local / Custom Modules =====
from ocr_pipeline import page_raster, reading_order_text, to_words  # noqa: E402
ONNX_DIR = os.path.join(VENDOR, "onnx")
VIETOCR_WEIGHTS = os.path.join(VENDOR, "vietocr", "weight", "vgg_seq2seq.pth")

ENGINE_NAME = "deepdoc-det-onnx + vietocr-vgg_seq2seq"


def _shim_ragflow_utils() -> None:
    """Satisfy the two RAGFlow imports the fork inherited.

    `module/ocr.py` opens with `from utils.file_utils import get_project_base_directory` and
    `from utils.settings import PARALLEL_DEVICES` — a package that belongs to the full RAGFlow
    server, not to this fork, and one whose name collides with THIS repo's own `src/utils` (both
    would be imported as `utils`, and `web_scraper` needs ours for `utils.constants`). Rather
    than fight the name, both submodules are pre-registered in `sys.modules`: an import finds
    them there and never touches the filesystem, so each `utils` stays intact.

    `get_project_base_directory()` is where the ONNX models live — pointed at the vendor
    checkout, so `snapshot_download` fills `vendor/deepdoc_vietocr/onnx/`, exactly where the
    repo's own (LFS-stripped) placeholders sit.
    """
    if "utils.file_utils" not in sys.modules:
        fu = types.ModuleType("utils.file_utils")
        fu.get_project_base_directory = lambda *a: (os.path.join(VENDOR, *a) if a else VENDOR)
        sys.modules["utils.file_utils"] = fu
    if "utils.settings" not in sys.modules:
        st = types.ModuleType("utils.settings")
        # None = "one device, no round-robin". Their multi-GPU branch builds one detector AND one
        # recogniser per GPU; on a 4 GB card that is how you run out of VRAM.
        st.PARALLEL_DEVICES = None
        sys.modules["utils.settings"] = st


class BatchedVietOcr:
    """deepdoc's `TextRecognizer`, batched and on the GPU.

    Drop-in for theirs: takes the same list of BGR crops, returns the same
    `([(text, score), …], elapse)`. The checkpoint is the fork's own
    `vietocr/weight/vgg_seq2seq.pth`, loaded through vietocr's own `Predictor`, so the model and
    its preprocessing are unchanged — this only stops feeding it one image at a time.

    Crops are sorted by aspect ratio before chunking because vietocr's `predict_batch` buckets by
    the width its resize produces and a batch must share one; sorting keeps a chunk to one or two
    real batches instead of twenty.
    """

    def __init__(self, weights: str = VIETOCR_WEIGHTS, config_name: str = "vgg_seq2seq",
                 device: Optional[str] = None, batch_size: int = 24, verbose: bool = True):
        import torch
        from vietocr.tool.config import Cfg
        from vietocr.tool.predictor import Predictor

        cfg = Cfg.load_config_from_name(config_name)
        cfg["weights"] = weights
        cfg["cnn"]["pretrained"] = False          # the checkpoint overwrites it anyway
        cfg["predictor"]["beamsearch"] = False
        cfg["device"] = device or ("cuda:0" if torch.cuda.is_available() else "cpu")
        if verbose:
            print(f"  building VietOCR ({config_name}) on {cfg['device']}…", flush=True)
        self.predictor = Predictor(cfg)
        self.batch_size = batch_size

    def __call__(self, img_list) -> Tuple[List[Tuple[str, float]], float]:
        import cv2
        from PIL import Image

        t0 = time.time()
        if not img_list:
            return [], 0.0
        crops = [Image.fromarray(cv2.cvtColor(im, cv2.COLOR_BGR2RGB)) for im in img_list]
        order = sorted(range(len(crops)),
                       key=lambda i: crops[i].width / max(1, crops[i].height))

        out: List[Tuple[str, float]] = [("", 0.0)] * len(crops)
        for start in range(0, len(order), self.batch_size):
            idx = order[start:start + self.batch_size]
            texts, probs = self.predictor.predict_batch([crops[i] for i in idx],
                                                        return_prob=True)
            for i, text, prob in zip(idx, texts, probs):
                out[i] = (text, float(prob))
        return out, time.time() - t0


class DeepDocVietOcr:
    """DeepDoc's ONNX detector + VietOCR, page in / positioned words out."""

    def __init__(self, dpi: int = 200, device: Optional[str] = None, batch_size: int = 24,
                 min_score: float = 0.25, det_side_len: Optional[int] = 1600,
                 config_name: str = "vgg_seq2seq", verbose: bool = True):
        self.dpi = dpi
        self.min_score = min_score
        self.det_side_len = det_side_len
        self.verbose = verbose
        self._ocr = None
        self._rec_kwargs = dict(device=device, batch_size=batch_size,
                                config_name=config_name, verbose=verbose)

    @property
    def ocr(self):
        """Their `OCR`, built once, with the recogniser replaced.

        Note `OCR(model_dir=…)` cannot be used: the fork's constructor only populates
        `text_detector` / `text_recognizer` inside the `if not model_dir:` branch, so passing a
        directory leaves the object with no models at all. The shimmed
        `get_project_base_directory()` is what points it at the vendor checkout instead.
        """
        if self._ocr is not None:
            return self._ocr

        _shim_ragflow_utils()
        if VENDOR not in sys.path:
            sys.path.insert(0, VENDOR)
        if self.verbose:
            print("  building DeepDoc ONNX detector…", flush=True)

        import module.ocr as vendor

        # Substituted BEFORE `OCR()` rather than swapped in afterwards, because building theirs
        # is not free and not safe: it sets `cnn.pretrained=True`, which downloads 548 MB of
        # ImageNet VGG19 weights that the checkpoint then overwrites, and it resolves that
        # checkpoint against a RELATIVE path (r"vietocr\weight\vgg_seq2seq.pth"), so it raises
        # anywhere but the vendor root. Both problems disappear if it is never constructed.
        kwargs = self._rec_kwargs
        vendor.TextRecognizer = lambda model_dir=None, device_id=None: BatchedVietOcr(**kwargs)
        ocr = vendor.OCR()

        # Their recogniser reports a flat score of 1.0, so OCR.__call__'s own `drop_score` filter
        # is a no-op for it. Ours reports the real per-crop probability; the filter is moved to
        # `read_page` so a genuine confidence floor applies.
        ocr.drop_score = 0.0
        self._drop_gpu_run_options(ocr)
        if self.det_side_len:
            self._resize_detector(ocr, self.det_side_len)
        self._ocr = ocr
        return ocr

    @staticmethod
    def _drop_gpu_run_options(ocr) -> None:
        """Stop the ONNX session asking to shrink a GPU arena it does not have.

        `load_model` decides whether to attach `memory.enable_memory_arena_shrinkage=gpu:0` by
        asking TORCH whether a GPU exists — not by asking whether the session it just built is
        running on one. On this machine torch has CUDA and onnxruntime is the CPU build, so every
        inference raised `INVALID_ARGUMENT: Did not find an arena based allocator registered for
        device-id combination in the memory arena shrink list: gpu:0`. The option is a memory
        optimisation, so dropping it costs nothing; it is dropped only when the session really
        has no CUDA provider.
        """
        import onnxruntime as ort

        for det in ocr.text_detector:
            if "CUDAExecutionProvider" not in det.predictor.get_providers():
                det.run_options = ort.RunOptions()

    @staticmethod
    def _resize_detector(ocr, side_len: int) -> None:
        """Raise the detector's input resolution.

        DeepDoc fixes `limit_side_len` at 960 px. An A4 page scanned at 200 dpi is 2367 px tall,
        so that reads a dense financial table at roughly 85 dpi — the thousands separators and
        the difference between 3 and 8 are the first casualties. Rebuilding the resize operator
        is the whole change; the model is fully convolutional and does not care.
        """
        from module.ocr import create_operators

        ops = [{"DetResizeForTest": {"limit_side_len": side_len, "limit_type": "max"}},
               {"NormalizeImage": {"std": [0.229, 0.224, 0.225], "mean": [0.485, 0.456, 0.406],
                                   "scale": "1./255.", "order": "hwc"}},
               {"ToCHWImage": None},
               {"KeepKeys": {"keep_keys": ["image", "shape"]}}]
        for det in ocr.text_detector:
            det.preprocess_op = create_operators(ops)

    def read_page(self, page):
        """(text in reading order, word tuples in visual pdf-point space)."""
        img, scale = page_raster(page, self.dpi)
        results = self.ocr(img[:, :, ::-1]) or []      # their pipeline expects BGR

        boxes = []
        for quad, (text, score) in results:
            if not text or not text.strip() or score < self.min_score:
                continue
            pts = np.asarray(quad, dtype=float)
            boxes.append((pts[:, 0].min(), pts[:, 1].min(),
                          pts[:, 0].max(), pts[:, 1].max(), text))
        words = to_words(boxes, scale)
        return reading_order_text(words), words


def build(**kwargs) -> DeepDocVietOcr:
    return DeepDocVietOcr(**kwargs)
