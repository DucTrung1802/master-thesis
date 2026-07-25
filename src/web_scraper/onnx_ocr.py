"""
ONNX OCR engine for the CafeF PDF parser — DeepDoc DB detection + VietOCR recognition.

The third OCR back-end behind `cafef_pdf_parser._ocr_page`, alongside `tesseract` (default) and
`easyocr`. Selected with `CAFEF_OCR_ENGINE=onnx`. Experiments 8-9 measured it against the
PaddleOCR-server + VietOCR stack over ACB Q1-2014 … Q4-2016: the two TIE on accuracy (both
reconcile 34/36 statements at ~83% figure match) and this one is ~10× faster (1.4 vs 13.9 s/page),
because detection runs a 4.7 MB DB model under onnxruntime on the CPU rather than PaddleOCR's
server detector. That speed is what makes re-OCRing the whole archive feasible, so it is the
engine to build a re-parse on.

Two stages, the same split every modern OCR uses:

  * DETECTION — DeepDoc's `det.onnx` (a DB text detector) finds every text region. The
    pre-processing operators and the DB post-processing are vendored verbatim in `_deepdoc/`
    (Apache-2.0, from RAGFlow), so detection is byte-identical to what the experiments validated.
    Runs on the **GPU** when `onnxruntime-gpu` is installed (CUDAExecutionProvider, ~7× faster
    than the CPU wheel: ~0.25 vs ~1.8 s/page) and falls back to CPU otherwise — a dependency
    swap, no code change. onnxruntime-gpu must match the CUDA the machine has (the 1.20.x line for
    CUDA 12.x / cuDNN 9.x here); `_DbTextDetector._enable_cuda_dlls` points it at torch's bundled
    CUDA/cuDNN so no separate system CUDA install is needed.
  * RECOGNITION — VietOCR (`vgg_seq2seq`), a Vietnamese-specific CNN+Transformer, reads each
    detected crop. PaddleOCR's own recogniser is not trained on Vietnamese diacritics, which is
    the whole reason the DeepDoc fork swapped it out; we do the same, and batch the crops so the
    GPU is not idled between them.

The engine exposes ONE method the parser needs — `read_page(page) -> (text, words)` — with
`words` shaped exactly like PyMuPDF's `page.get_text("words")` (`x0,y0,x1,y1,text,block,line,n`)
in VISUAL pdf-point space, so the row builder downstream cannot tell which engine produced them.
"""

# ===== Standard Library =====
import os
from typing import List, Optional, Tuple

# ===== Third-party =====
import numpy as np

# The DB detector's input resolution cap. DeepDoc ships 960 px on the long side, which reads an
# A4 page scanned at 200 dpi (~2367 px) at roughly 85 dpi — thousands separators and the
# difference between 3 and 8 are the first casualties. 1600 is what experiments 8-9 used.
DET_SIDE_LEN = int(os.environ.get("CAFEF_ONNX_DET_SIDE_LEN", "1600"))

# The DB detector model. Bundled at `src/web_scraper/models/deepdoc_det.onnx`; overridable, and
# fetched from HuggingFace (`InfiniFlow/deepdoc`) if absent.
_MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models")
DET_MODEL = os.environ.get("CAFEF_ONNX_DET", os.path.join(_MODELS_DIR, "deepdoc_det.onnx"))

# VietOCR architecture + the render DPI. 200 dpi matches the Tesseract path (`OCR_DPI`) and the
# experiments; the model weights download once from the vietocr project on first use.
VIETOCR_ARCH = os.environ.get("CAFEF_ONNX_VIETOCR", "vgg_seq2seq")
RENDER_DPI = 200
MIN_SCORE = 0.25            # drop recognitions below this confidence (scan speckle detects as text)
REC_BATCH = 24


def _transform(data, ops):
    """Run the pre-processing operator chain (DeepDoc's `transform`)."""
    for op in ops:
        data = op(data)
        if data is None:
            return None
    return data


def _create_operators(op_param_list):
    """Instantiate the pre-processing operators from `_deepdoc/operators.py` (DeepDoc's
    `create_operators`): a list of one-key dicts `{OperatorName: params}`."""
    from ._deepdoc import operators as ops_mod

    ops = []
    for operator in op_param_list:
        name = list(operator)[0]
        param = operator[name] or {}
        ops.append(getattr(ops_mod, name)(**param))
    return ops


def ensure_det_model(path: str = DET_MODEL) -> str:
    """Return a local path to `det.onnx`, downloading it once if the bundled copy is missing."""
    if os.path.exists(path) and os.path.getsize(path) > 10_000:
        return path
    from huggingface_hub import hf_hub_download

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    got = hf_hub_download(repo_id="InfiniFlow/deepdoc", filename="det.onnx")
    import shutil

    shutil.copyfile(got, path)
    return path


class _DbTextDetector:
    """DeepDoc's DB detector, wired to a bare onnxruntime session.

    A trimmed copy of the vendored `TextDetector`: the pre-processing operator list and the DB
    post-processing parameters are IDENTICAL (so boxes match the experiments), only `limit_side_len`
    is raised and the model is loaded through a plain `InferenceSession` rather than RAGFlow's
    cached `load_model`.
    """

    @staticmethod
    def _enable_cuda_dlls() -> None:
        """Make torch's bundled CUDA 12 + cuDNN 9 DLLs discoverable to onnxruntime-gpu on Windows.

        `onnxruntime-gpu` does not ship the CUDA runtime; it looks for cudart/cublas/cudnn on the
        DLL search path. torch (cu121) carries a matching set in `torch/lib` (cudart64_12,
        cublas64_12, cudnn64_9…), so adding that directory lets the CUDAExecutionProvider load
        without a separate system CUDA install. Without this the provider silently falls back to
        CPU. Importing torch first also pre-loads the DLLs into the process.
        """
        try:
            import torch

            tlib = os.path.join(os.path.dirname(torch.__file__), "lib")
            if os.path.isdir(tlib):
                os.add_dll_directory(tlib)
        except Exception:
            pass

    def __init__(self, model_path: str, side_len: int = DET_SIDE_LEN):
        self._enable_cuda_dlls()
        import onnxruntime as ort

        from ._deepdoc.postprocess import build_post_process

        self._transform = _transform
        self.preprocess_op = _create_operators([
            {"DetResizeForTest": {"limit_side_len": side_len, "limit_type": "max"}},
            {"NormalizeImage": {"std": [0.229, 0.224, 0.225], "mean": [0.485, 0.456, 0.406],
                                "scale": "1./255.", "order": "hwc"}},
            {"ToCHWImage": None},
            {"KeepKeys": {"keep_keys": ["image", "shape"]}},
        ])
        self.postprocess_op = build_post_process({
            "name": "DBPostProcess", "thresh": 0.3, "box_thresh": 0.5, "max_candidates": 1000,
            "unclip_ratio": 1.5, "use_dilation": False, "score_mode": "fast", "box_type": "quad"})

        providers = (["CUDAExecutionProvider", "CPUExecutionProvider"]
                     if "CUDAExecutionProvider" in ort.get_available_providers()
                     else ["CPUExecutionProvider"])
        self.session = ort.InferenceSession(model_path, providers=providers)
        self.input_name = self.session.get_inputs()[0].name

    @staticmethod
    def _order_clockwise(pts):
        rect = np.zeros((4, 2), dtype="float32")
        s = pts.sum(axis=1)
        rect[0] = pts[np.argmin(s)]
        rect[2] = pts[np.argmax(s)]
        tmp = np.delete(pts, (np.argmin(s), np.argmax(s)), axis=0)
        diff = np.diff(np.array(tmp), axis=1)
        rect[1] = tmp[np.argmin(diff)]
        rect[3] = tmp[np.argmax(diff)]
        return rect

    def __call__(self, img) -> np.ndarray:
        data = self._transform({"image": img.copy()}, self.preprocess_op)
        norm, shape_list = data
        if norm is None:
            return np.empty((0, 4, 2))
        outputs = self.session.run(
            None, {self.input_name: np.expand_dims(norm, axis=0).copy()})
        post = self.postprocess_op({"maps": outputs[0]}, np.expand_dims(shape_list, axis=0))
        boxes = post[0]["points"]

        h, w = img.shape[:2]
        out = []
        for box in boxes:
            box = self._order_clockwise(np.array(box) if isinstance(box, list) else box)
            box[:, 0] = np.clip(box[:, 0], 0, w - 1)
            box[:, 1] = np.clip(box[:, 1], 0, h - 1)
            if (int(np.linalg.norm(box[0] - box[1])) > 3
                    and int(np.linalg.norm(box[0] - box[3])) > 3):
                out.append(box)
        return np.array(out) if out else np.empty((0, 4, 2))


class _BatchedVietOcr:
    """VietOCR recognition, batched on the GPU when there is one.

    The stock vietocr `Predictor.predict` reads one crop at a time; a filing is thousands of
    crops, so they are bucketed by width (vietocr batches must share a width) and run in chunks.
    """

    def __init__(self, arch: str = VIETOCR_ARCH, device: Optional[str] = None,
                 batch_size: int = REC_BATCH):
        import torch
        from vietocr.tool.config import Cfg
        from vietocr.tool.predictor import Predictor

        cfg = Cfg.load_config_from_name(arch)
        cfg["cnn"]["pretrained"] = False       # the OCR checkpoint overwrites the backbone anyway
        cfg["predictor"]["beamsearch"] = False
        cfg["device"] = device or ("cuda:0" if torch.cuda.is_available() else "cpu")
        self.predictor = Predictor(cfg)
        self.batch_size = batch_size

    def __call__(self, crops) -> List[Tuple[str, float]]:
        if not crops:
            return []
        order = sorted(range(len(crops)),
                       key=lambda i: crops[i].width / max(1, crops[i].height))
        out: List[Tuple[str, float]] = [("", 0.0)] * len(crops)
        for start in range(0, len(order), self.batch_size):
            idx = order[start:start + self.batch_size]
            texts, probs = self.predictor.predict_batch([crops[i] for i in idx],
                                                        return_prob=True)
            for i, text, prob in zip(idx, texts, probs):
                out[i] = (text, float(prob))
        return out


class OnnxOcr:
    """DB detection + VietOCR, page in / positioned words out. Built lazily, once per parser."""

    def __init__(self, logger=None, dpi: int = RENDER_DPI, min_score: float = MIN_SCORE,
                 side_len: int = DET_SIDE_LEN, device: Optional[str] = None):
        self._logger = logger
        self.dpi = dpi
        self.min_score = min_score
        self.side_len = side_len
        self.device = device
        self._det = None
        self._rec = None

    def _log(self, msg: str) -> None:
        if self._logger:
            self._logger.log_info(msg)

    @property
    def detector(self) -> _DbTextDetector:
        if self._det is None:
            self._det = _DbTextDetector(ensure_det_model(), self.side_len)
        return self._det

    @property
    def recognizer(self) -> _BatchedVietOcr:
        if self._rec is None:
            self._rec = _BatchedVietOcr(device=self.device)
        return self._rec

    def read_page(self, page) -> Tuple[str, list]:
        """(page text in reading order, PyMuPDF-shaped word tuples in visual pdf-point space).

        `get_pixmap` ALREADY renders the page as displayed — it applies the page's `/Rotate`, so a
        `/Rotate 180` scan comes out UPRIGHT with a plain scale matrix. Do NOT `prerotate` on top
        of that: it rotates a 180° page a second time, back upside-down, and OCR returns pure
        garbage ("Các thuyết minh…" → "NYHI Y NYHD…"). A rotation-0 page is unaffected either way,
        which is why the bug hid until a reviewed/annual scan (all `/Rotate 180`) was parsed.
        The upright raster's pixel coordinates ARE visual coordinates, so dividing by the render
        scale is the only conversion back to pdf points — the same visual space the Tesseract
        path reaches via `_to_visual`.
        """
        import fitz
        from PIL import Image

        scale = self.dpi / 72.0
        pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale))
        img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(
            pix.height, pix.width, pix.n)[:, :, :3].copy()

        boxes = self.detector(img[:, :, ::-1])            # detector expects BGR
        crops, rects = [], []
        for quad in boxes:
            xs, ys = quad[:, 0], quad[:, 1]
            x0, y0, x1, y1 = int(xs.min()), int(ys.min()), int(xs.max()), int(ys.max())
            if x1 - x0 < 4 or y1 - y0 < 4:
                continue
            crops.append(Image.fromarray(img[y0:y1, x0:x1]))
            rects.append((x0, y0, x1, y1))

        read = self.recognizer(crops)
        words, lines = [], []
        for (x0, y0, x1, y1), (text, prob) in zip(rects, read):
            text = (text or "").strip()
            if not text or prob < self.min_score:
                continue
            words.append((x0 / scale, y0 / scale, x1 / scale, y1 / scale,
                          text, 0, 0, len(words)))
            lines.append((y0, x0, text))

        # Reading order: top-to-bottom, then left-to-right, grouped into lines so the page
        # classifier reads a coherent header.
        text = _reading_order(lines)
        return text, words


def _reading_order(items: List[Tuple[float, float, str]], y_tol: float = 4.0) -> str:
    """items = (y, x, text) -> the page as text, top-to-bottom then left-to-right."""
    rows: dict = {}
    for y, x, t in sorted(items):
        key = next((k for k in rows if abs(k - y) <= y_tol), y)
        rows.setdefault(key, []).append((x, t))
    return "\n".join(" ".join(t for _, t in sorted(rows[k])) for k in sorted(rows))
