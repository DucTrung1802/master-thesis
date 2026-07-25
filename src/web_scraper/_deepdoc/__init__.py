"""Vendored text-detection stack from DeepDoc (RAGFlow), Apache-2.0.

`operators.py` and `postprocess.py` are copied VERBATIM from
https://github.com/hoaivannguyen/deepdoc_vietocr (`module/operators.py`,
`module/postprocess.py`, themselves from infiniflow/ragflow) so the DB text detector behaves
byte-for-byte as validated in experiments 8-9. Only the pieces `onnx_ocr.py` needs are used —
the pre-processing operators (`DetResizeForTest`, `NormalizeImage`, `ToCHWImage`, `KeepKeys`) and
`DBPostProcess` / `build_post_process`. They depend only on numpy, cv2, six, shapely and
pyclipper, which the project already has; nothing here reaches RAGFlow's server code.
"""
