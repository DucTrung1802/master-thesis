"""
experiment_8's OCR engine — the pipeline of https://github.com/bmd1905/vietnamese-ocr.

That repo is two models bolted together, and its `predict.py` is 40 lines long:

    detector  = PaddleOCR(use_angle_cls=False, lang="vi", use_gpu=True)   # DB text detection
    recognitor = Predictor(Cfg.load_config_from_name('vgg_transformer'))  # VietOCR
    boxes = detector.ocr(img, det=True, rec=False)                        # where the text is
    for box in boxes:                                                     # pad, crop, read
        texts.append(recognitor.predict(Image.fromarray(crop)))

The idea is the interesting part: PaddleOCR's DB detector is excellent at FINDING text and its
recogniser is not trained on Vietnamese diacritics, so the recognition step is handed to VietOCR
— a Vietnamese-specific CNN+Transformer — one crop at a time. This module is that pipeline,
against the current PaddleOCR API (3.x moved detection into `paddleocr.TextDetection`; the
`.ocr(det=True, rec=False)` call the repo uses was removed), plus the two things a 40-line demo
does not need:

  * BATCHED recognition. One `predict()` per box is ~3,000 forward passes for this filing and
    leaves the GPU idle between them. Crops are bucketed by width (VietOCR's own requirement:
    a batch must share a width) and run in chunks — same model, same weights, ~10x the speed.
  * a CONFIDENCE floor. The demo keeps whatever comes back; a scanned table has speckle that
    detects as text, and a garbage token landing in the middle of a row can capture a value
    column.

Everything downstream (rows, columns, chart of accounts) is in `ocr_pipeline.py`, shared with
experiment_9 so the two experiments differ only in this file.
"""

# ===== Standard Library =====
from typing import List, Optional, Tuple

# ===== Third-party =====
# IMPORT ORDER IS LOAD-BEARING ON WINDOWS. paddlepaddle brings its own MKL/OpenMP runtime, and
# once it is resident torch cannot load its DLLs at all — "[WinError 127] Error loading
# torch\lib\shm.dll or one of its dependencies", raised from `import torch` several minutes into
# a run. Importing torch first claims the runtime and the two coexist, so this import is here on
# purpose and must stay above anything that reaches paddle.
import torch  # noqa: F401

# ===== Local / Custom Modules =====
from ocr_pipeline import page_raster, reading_order_text, to_words


class VietnameseOcr:
    """PaddleOCR-DB detection + VietOCR recognition, page in / positioned words out."""

    def __init__(self, dpi: int = 200, padding: int = 4, device: Optional[str] = None,
                 batch_size: int = 24, min_prob: float = 0.25,
                 det_side_len: int = 1600, box_thresh: float = 0.5,
                 unclip_ratio: float = 1.8, beamsearch: bool = False,
                 weights: str = "vgg_transformer", verbose: bool = True):
        self.dpi = dpi
        self.padding = padding
        self.batch_size = batch_size
        self.min_prob = min_prob
        self.verbose = verbose
        self._det = None
        self._rec = None
        self._det_kwargs = dict(limit_side_len=det_side_len, limit_type="max",
                                box_thresh=box_thresh, unclip_ratio=unclip_ratio)
        self._rec_kwargs = dict(device=device, beamsearch=beamsearch, weights=weights)

    # ── models ────────────────────────────────────────────────────────────────
    # Both are expensive to build (PaddleOCR fetches and warms an inference model, VietOCR loads
    # ~100 MB of weights onto the GPU) and stateless once built, so they are made once, lazily.

    @property
    def detector(self):
        if self._det is None:
            from paddleocr import TextDetection
            if self.verbose:
                print("  building PaddleOCR DB detector…", flush=True)
            self._det = TextDetection(**self._det_kwargs)
        return self._det

    @property
    def recognizer(self):
        if self._rec is None:
            import torch
            from vietocr.tool.config import Cfg
            from vietocr.tool.predictor import Predictor

            cfg = Cfg.load_config_from_name(self._rec_kwargs["weights"])
            # `pretrained` would pull the ImageNet VGG backbone from the internet only to
            # overwrite it with the OCR checkpoint a line later.
            cfg["cnn"]["pretrained"] = False
            cfg["predictor"]["beamsearch"] = self._rec_kwargs["beamsearch"]
            cfg["device"] = self._rec_kwargs["device"] or (
                "cuda:0" if torch.cuda.is_available() else "cpu")
            if self.verbose:
                print(f"  building VietOCR ({self._rec_kwargs['weights']}) "
                      f"on {cfg['device']}…", flush=True)
            self._rec = Predictor(cfg)
        return self._rec

    # ── the pipeline ──────────────────────────────────────────────────────────

    def detect(self, img) -> List[Tuple[int, int, int, int]]:
        """Axis-aligned boxes around every text region the DB detector finds.

        DB returns quadrilaterals; a financial statement is not skewed enough for that to matter
        and the row builder wants rectangles, so each quad is reduced to its bounding box and
        PADDED. The padding is the repo's, and it earns its place: DB draws its box on the ink,
        which clips Vietnamese diacritics (the tone mark above ế, the tail of ộ) and those are
        exactly the strokes that distinguish one word from another.
        """
        res = self.detector.predict(img[:, :, ::-1])          # detector expects BGR
        polys = res[0]["dt_polys"] if res else []
        h, w = img.shape[:2]
        p = self.padding
        boxes = []
        for quad in polys:
            xs = [int(pt[0]) for pt in quad]
            ys = [int(pt[1]) for pt in quad]
            x0, y0 = max(0, min(xs) - p), max(0, min(ys) - p)
            x1, y1 = min(w, max(xs) + p), min(h, max(ys) + p)
            if x1 - x0 >= 4 and y1 - y0 >= 4:
                boxes.append((x0, y0, x1, y1))
        return boxes

    def recognise(self, img, boxes) -> List[Tuple[str, float]]:
        """Read every crop, in batches.

        VietOCR resizes a crop to a fixed height and a width proportional to its aspect ratio,
        then requires a batch to share that width — so the crops are sorted by aspect ratio
        before chunking, which puts similar widths together and keeps each chunk to one or two
        real batches instead of twenty. The 4 GB card is why chunking exists at all: a bucket of
        several hundred wide crops does not fit.
        """
        from PIL import Image

        if not boxes:
            return []
        crops = [Image.fromarray(img[y0:y1, x0:x1]) for x0, y0, x1, y1 in boxes]
        order = sorted(range(len(crops)),
                       key=lambda i: crops[i].width / max(1, crops[i].height))

        out: List[Tuple[str, float]] = [("", 0.0)] * len(crops)
        for start in range(0, len(order), self.batch_size):
            idx = order[start:start + self.batch_size]
            texts, probs = self.recognizer.predict_batch([crops[i] for i in idx],
                                                         return_prob=True)
            for i, text, prob in zip(idx, texts, probs):
                out[i] = (text, float(prob))
        return out

    def read_page(self, page):
        """(text in reading order, word tuples in visual pdf-point space)."""
        img, scale = page_raster(page, self.dpi)
        boxes = self.detect(img)
        read = self.recognise(img, boxes)
        kept = [(x0, y0, x1, y1, text)
                for (x0, y0, x1, y1), (text, prob) in zip(boxes, read)
                if text.strip() and prob >= self.min_prob]
        words = to_words(kept, scale)
        return reading_order_text(words), words


def build(**kwargs) -> VietnameseOcr:
    """The engine `run_acb_2013.py` and `ocr_pipeline` consume."""
    return VietnameseOcr(**kwargs)


ENGINE_NAME = "paddleocr-db + vietocr-vgg_transformer"
