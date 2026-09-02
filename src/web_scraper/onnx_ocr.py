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
from typing import Dict, List, Optional, Tuple

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

# ⚠️ **A LOCAL CHECKPOINT, FOR A MACHINE WITH NO INTERNET.** `Cfg.load_config_from_name` sets
# `weights` to `https://vocr.vn/data/vietocr/vgg_seq2seq.pth` and vietocr's `download_weights`
# fetches it on the first `Predictor` build — which a Kaggle kernel with `enable_internet:
# false` cannot do, and which a kernel WITH internet does on every cold start. `download_weights`
# returns any non-`http` value unchanged, so pointing this at a shipped file is the supported
# way in. Empty = the URL, i.e. exactly the behaviour every run had before this existed.
VIETOCR_WEIGHTS = os.environ.get("CAFEF_ONNX_VIETOCR_WEIGHTS", "")

# ⚠️ **AND THE CONFIG IS A SECOND DOWNLOAD NOBODY HAD COUNTED.** `Cfg.load_config_from_name`
# fetches `base.yml` AND `<arch>.yml` from `https://vocr.vn/data/vietocr/config/` on EVERY
# `Predictor` construction — vietocr caches neither — so shipping the weights alone never made
# the recogniser offline, and the "the payload ships both models so nothing is downloaded"
# claim was incomplete for as long as it stood.
#
# ⚠️ **MEASURED 2026-08-29, WHEN THAT HOST'S TLS CERTIFICATE EXPIRED**: every `onnx@*` layer
# raised `SSLError`, the cascade fell through to `tesseract@200`, and a filing that had read
# `onnx@200` with 98 of 98 cells reproducing came back with 13 different columns — both gates
# passing, because a tesseract parse of a real document is a real parse. **A degraded engine
# does not look like a failure; it looks like a different answer.**
#
# Empty = the download, i.e. exactly what every run did before this existed. Point it at a
# MERGED yaml (base + arch): `Cfg.load_config_from_file` starts from `{}` and will not fetch
# `base.yml` for you.
VIETOCR_CONFIG = os.environ.get(
    "CAFEF_ONNX_VIETOCR_CONFIG", os.path.join(_MODELS_DIR, "vietocr_vgg_seq2seq.yml"))
RENDER_DPI = 200
MIN_SCORE = 0.25            # drop recognitions below this confidence (scan speckle detects as text)
# ⚠️ Raised 24 -> 64 on 2026-08-30, WITH the width-bucketing in `_BatchedVietOcr.__call__`
# and not before it. At 24, sorted by aspect ratio, a chunk fragmented into a dozen decode
# loops of one or two images, so the number was not a batch size at all — it was a cap on how
# badly the crops could be regrouped. Bucketed first, a chunk IS one batch. 64 rather than
# larger because a page carries ~68 crops over ~44 widths: past that the chunk stops binding
# and only the VRAM grows.
# ⚠️ **HOW MANY CROPS THE RECOGNISER DECODES AT ONCE — AND IT IS THE ONE VRAM LEVER THAT DOES
# NOT CHANGE THE OUTPUT.** `_BatchedVietOcr` buckets crops by EXACT width before it chunks them
# (§6-2-duoquadragies: bucketing first is 1.11-1.22x faster with 0 of 542 crops changed, while
# PADDING to a common width is 2x faster and changes 70), so every chunk already shares one
# width and a smaller chunk is the same decode on fewer images. Lowering it costs speed and
# nothing else — which is what makes it the right knob when a 4 GiB card is short (`GPU-1`),
# where the alternatives (float16, a smaller `DET_SIDE_LEN`) change what is read.
# ✅ **MEASURED, NOT ARGUED (2026-09-02).** CTG Q3-2019 parsed at 64 and at 12 gives the same
# winning layer and the **IDENTICAL `rows_sha` on all three statements** — b167ec214dfb,
# b0b45f6c0831, d59f7eb9b00b — i.e. every row the OCR read, mapped or not. And on CTG Q1-2009,
# the one filing that OOMed 4 of its 53 layers with 3,303 MiB free, `REC_BATCH=12` returned
# **0 engine errors** with the same accepted statement at the same layer.
REC_BATCH = int(os.environ.get("CAFEF_ONNX_REC_BATCH", "64"))

# Margin added around each detected box BEFORE recognition, in pdf points (scaled to pixels at
# the render DPI). The detector returns a box hugging the glyphs, and VietOCR misreads a crop
# that tight: on ACB's Q1-2023 cash flow it read 96.922.247 as **196**.922.247 and 6.654.779 as
# **16**.654.779, inventing a leading digit at the clipped edge. That is not a Vietnamese-
# language problem and not a resolution one — it survived 200, 300, 400, 500 and 600 dpi, and
# vgg_transformer, EasyOCR and Tesseract all read the SAME cell correctly from a looser crop.
# One point is enough to fix both cells and the reading stays stable out to 6; 2 leaves margin
# without pulling in the neighbouring column (the nearest column edge is ~27pt away).
#
# The padding feeds the RECOGNISER only. The box reported downstream is the detector's own, so
# the right-edge column clustering is unchanged.
#
# ⚠️ THE ERROR RUNS BOTH WAYS, and the opposite one needs MORE than 2. A crop that clips the
# glyphs makes the recogniser INVENT a leading digit (96.922.247 -> 196.922.247); a detector box
# that starts inside the number instead makes it LOSE one, and no amount of resolution helps
# because the missing pixels were never in the crop. ACB's Q3-2023 reads 93.261.018 as 261.018
# at every DPI, and at pad 6 reads it correctly — see the `+pad6` layers, which is where a wider
# crop is applied rather than here, so no quarter that parses today is re-rendered.
CROP_PAD_PT = 2.0


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

        # ⚠️ **onnxruntime 1.21+ NEEDS TO BE TOLD WHERE THE `nvidia-*` PIP WHEELS PUT CUDA.**
        # On Linux the GPU wheel no longer adds them to the loader path itself, so on an image
        # that has CUDA only through pip (a Kaggle worker) `CUDAExecutionProvider` is
        # ADVERTISED and then fails to load. `preload_dlls` is that call; it does not exist
        # before 1.21 and it is a no-op where the libraries are already resolvable, hence the
        # guard rather than a version test.
        if hasattr(ort, "preload_dlls"):
            try:
                ort.preload_dlls()
            except Exception:  # noqa: BLE001 — a preload that fails must not stop a CPU run
                pass
        providers = (["CUDAExecutionProvider", "CPUExecutionProvider"]
                     if "CUDAExecutionProvider" in ort.get_available_providers()
                     else ["CPUExecutionProvider"])
        # ⚠️ **`get_available_providers()` IS AN ADVERTISEMENT, NOT A MEASUREMENT** — measured
        # on Kaggle 2026-08-28, where it listed `CUDAExecutionProvider` and the session then
        # came back CPU-only because the wheel wanted CUDA 13 on a CUDA 12.8 image. The session
        # silently downgrades, so the only honest answer is what the SESSION holds afterwards:
        # `session.get_providers()`, which `pdf_ocr_job.engine_report` reads into the artefact.
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

    ⚠️ **THE BUCKETING HAS TO HAPPEN BEFORE THE CHUNKING, AND IT USED TO HAPPEN AFTER.**
    `predict_batch` re-groups whatever it is handed by the EXACT padded width
    `process_input` produces (height 32, width rounded up to a multiple of 10), and a batch
    is one autoregressive decode that runs until its longest sequence ends. Handing it a
    chunk of `batch_size` crops sorted by ASPECT RATIO therefore split into a dozen buckets
    of one or two images each: measured 2026-08-30 on BID's FY-2016 filing, **542 crops over
    44 distinct widths**, i.e. ~12 crops per bucket spread across 23 separate calls. Grouping
    by width FIRST and chunking each group makes every batch as full as the page allows.

    ⚠️ **NOTHING IS PADDED TO A COMMON WIDTH, AND THAT IS A MEASUREMENT, NOT CAUTION.** Doing
    so would fill the batches completely and is ~2x faster again — and it CHANGES WHAT THE
    RECOGNISER READS: 70 of those 542 crops came back different, `'Deloitte'` as
    `'Deloitte.'`, `'ĐÃ ĐƯỢC KIỂM TOÁN TH'` as `'ĐÃ ĐƯỢC KIỂM TOÁN TRUNG'`. The recogniser is
    width-sensitive, so a faster batch bought a different answer. Re-grouping alone is
    verified **0 of 542 crops changed**, which is what makes it shippable at all.

    ⚠️ **WITHIN A PAGE THIS IS WORTH 1.11-1.22x (four interleaved pairs), NOT MORE.** 68 crops
    over 44 widths is a thin bucket however it is chunked; bucketing across the whole DOCUMENT
    is **2.35x** at the same 0 mismatches. That is deliberately not taken: `PdfParser.scan`
    reads each page's TEXT to decide whether to read the next one, so deferring recognition to
    a block boundary would change WHICH PAGES ARE READ — a change to the parse rather than to
    its cost.
    """

    def __init__(self, arch: str = VIETOCR_ARCH, device: Optional[str] = None,
                 batch_size: int = REC_BATCH, weights: Optional[str] = None):
        import torch
        from vietocr.tool.config import Cfg
        from vietocr.tool.predictor import Predictor

        # Read the module global rather than a default frozen at def-time, for the same
        # reason `VIETOCR_WEIGHTS` is: `pdf_ocr_job.use_models` sets it after import.
        local_cfg = VIETOCR_CONFIG
        if local_cfg and os.path.isfile(local_cfg):
            cfg = Cfg.load_config_from_file(local_cfg)
        else:
            try:
                cfg = Cfg.load_config_from_name(arch)
            except Exception as exc:            # noqa: BLE001 — say WHICH dependency failed
                raise RuntimeError(
                    f"vietocr could not fetch its config for {arch!r} from vocr.vn: "
                    f"{type(exc).__name__}: {exc}\n"
                    f"  This is the RECOGNISER's config, not its weights — vietocr downloads "
                    f"it on every Predictor build and caches nothing, so an unreachable or "
                    f"expired host takes the whole onnx engine down and the cascade falls "
                    f"through to tesseract with DIFFERENT figures.\n"
                    f"  Put a merged base+arch yaml at {local_cfg} (or point "
                    f"CAFEF_ONNX_VIETOCR_CONFIG at one) and this stops being a dependency."
                ) from exc
        cfg["cnn"]["pretrained"] = False       # the OCR checkpoint overwrites the backbone anyway
        cfg["predictor"]["beamsearch"] = False
        # Read the module global rather than binding it as a default: `pdf_ocr_job.use_models`
        # sets it after import when a payload ships the checkpoint, and a default frozen at
        # def-time would silently keep the URL.
        local = weights or VIETOCR_WEIGHTS
        if local:
            if not os.path.isfile(local):
                raise FileNotFoundError(
                    f"CAFEF_ONNX_VIETOCR_WEIGHTS points at {local!r}, which does not exist. "
                    f"An unreadable local checkpoint must not fall back to the download: on a "
                    f"worker with no internet that turns into a connection error minutes into "
                    f"the first page.")
            cfg["weights"] = local
        cfg["device"] = device or ("cuda:0" if torch.cuda.is_available() else "cpu")
        self.predictor = Predictor(cfg)
        self.batch_size = batch_size

    def __call__(self, crops) -> List[Tuple[str, float]]:
        if not crops:
            return []
        # Group by the width `process_input` will pad each crop to — the same key
        # `predict_batch` buckets on internally — so a chunk handed to it is ONE batch and
        # one decode loop, not a dozen. Aspect ratio was a proxy for this and a poor one:
        # it orders the crops correctly and then cuts across the buckets.
        # ⚠️ **`resize` AND NOT `process_input` — the width, not the tensor.** vietocr's own
        # `process_image` calls exactly this to decide the padded width, so importing it is
        # what keeps the two in step; recomputing the rounding here would be a second copy of
        # a rule that lives one function away. `process_input` would give the same answer and
        # would also convert, LANCZOS-resize and normalise every crop a SECOND time —
        # `predict_batch` does all of that itself moments later.
        from vietocr.tool.translate import resize

        cfg = self.predictor.config["dataset"]
        widths: Dict[int, List[int]] = {}
        for i, crop in enumerate(crops):
            w, _ = resize(crop.width, crop.height, cfg["image_height"],
                          cfg["image_min_width"], cfg["image_max_width"])
            widths.setdefault(w, []).append(i)
        out: List[Tuple[str, float]] = [("", 0.0)] * len(crops)
        for group in widths.values():
            for start in range(0, len(group), self.batch_size):
                idx = group[start:start + self.batch_size]
                texts, probs = self.predictor.predict_batch([crops[i] for i in idx],
                                                            return_prob=True)
                for i, text, prob in zip(idx, texts, probs):
                    out[i] = (text, float(prob))
        return out


class OnnxOcr:
    """DB detection + VietOCR, page in / positioned words out. Built lazily, once per parser."""

    def __init__(self, logger=None, dpi: int = RENDER_DPI, min_score: float = MIN_SCORE,
                 side_len: int = DET_SIDE_LEN, device: Optional[str] = None,
                 crop_pad: float = CROP_PAD_PT):
        self._logger = logger
        self.dpi = dpi
        self.min_score = min_score
        self.side_len = side_len
        self.device = device
        # Per-instance so a PARSE LAYER can widen it (see FinancialsBuilder.LAYERS); the module
        # constant remains the default every ordinary parse uses.
        self.crop_pad = crop_pad
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
        h_img, w_img = img.shape[:2]
        pad = max(1, int(round(self.crop_pad * scale)))
        crops, rects = [], []
        for quad in boxes:
            xs, ys = quad[:, 0], quad[:, 1]
            x0, y0, x1, y1 = int(xs.min()), int(ys.min()), int(xs.max()), int(ys.max())
            if x1 - x0 < 4 or y1 - y0 < 4:
                continue
            # Recognise a PADDED crop but report the box the detector actually found: the extra
            # pixels are there for the recogniser's benefit, and widening the reported box would
            # corrupt the right-edge column clustering downstream.
            px0, py0 = max(0, x0 - pad), max(0, y0 - pad)
            px1, py1 = min(w_img, x1 + pad), min(h_img, y1 + pad)
            crops.append(Image.fromarray(img[py0:py1, px0:px1]))
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
