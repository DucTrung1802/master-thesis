# experiment_9 — DeepDoc + VietOCR on the same filing

Same document, same downstream, different OCR: **https://github.com/hoaivannguyen/deepdoc_vietocr**
— RAGFlow's **DeepDoc** document pipeline (all ONNX, CPU-first) with its Chinese/English
recogniser replaced by **VietOCR**. Read [experiment_8](../experiment_8/README.md) first: it
explains the document, the shared downstream, and the three parser findings that apply here too.

```
det.onnx     PaddleOCR DB text detection, exported to ONNX
rec.onnx     the original recogniser — REPLACED by VietOCR (vgg_seq2seq)
layout.onnx  YOLOv10 over 10 page-element classes (text / title / table / figure / …)
tsr.onnx     table structure: table column / row / column header / row header / spanning cell
```

The last two are why the repo is interesting. They answer directly — *where are this table's rows
and columns?* — the question `cafef_pdf_parser` answers geometrically, by clustering the right
edges of numbers.

## What was built

```
setup_vendor.py              clone the repo + fetch the ONNX models
deepdoc_vietocr_engine.py    the engine: their ONNX detector + batched VietOCR
run_acb_2013.py              the CLI — identical outputs to experiment_8
table_structure.py           their layout + TSR models on the same pages
vendor/deepdoc_vietocr/      the checkout (gitignored, ~200 MB of models)
out/                         statements, comparisons, the OCR read, report.md
```

Everything after the OCR is `experiment_8/ocr_pipeline.py`, imported, not reimplemented — the
engine is the only difference between the two experiments, which is what makes the numbers below
comparable to experiment_8's.

## Running it

```bash
python -m venv --system-site-packages ocr_env9            # from the repo root
ocr_env9/Scripts/python -m pip install onnxruntime vietocr pdfplumber trio ruamel.yaml cachetools
cd experiment/experiment_9
../../ocr_env9/Scripts/python setup_vendor.py             # once
../../ocr_env9/Scripts/python run_acb_2013.py --max-pages 16
../../ocr_env9/Scripts/python table_structure.py          # layout + TSR on the balance sheet
```

**The repo cannot be cloned normally.** Its `onnx/*.onnx` are Git-LFS pointers and the account is
over budget — `batch response: This repository exceeded its LFS budget` — so a plain `git clone`
aborts the checkout and leaves an EMPTY working tree, code included. `setup_vendor.py` clones
with `GIT_LFS_SKIP_SMUDGE=1` and then pulls the models from `InfiniFlow/deepdoc` on HuggingFace,
which is where RAGFlow publishes them and where the fork's own fallback path looks anyway. The
VietOCR checkpoints (`vietocr/weight/*.pth`) are not LFS and come down with the clone.

## Result

| statement | pages | rows parsed | mapped | reconciles | agree | differ | missed |
|---|---|---|---|---|---|---|---|
| balance_sheet | 8-10 | 62 | 44 | **yes** | 21 | 20 | 30 |
| income_statement | 11-12 | 25 | 19 | **yes** | 7 | 9 | 1 |
| cash_flow | 13-14 | 38 | 16 | **yes** | 11 | 3 | 22 |

**OCR: 15 pages in 27 s — 1.8 s/page.** experiment_8 takes **234 s for the same 15 pages**
(15.6 s/page) at the same DPI on the same machine: **8.6x slower** for statements that come out
within a line or two of each other. Detection is the whole of it — DeepDoc runs a 4.7 MB DB model
under onnxruntime on the CPU, PaddleOCR 3.x builds and runs the much larger PP-OCRv5 *server*
detector. Recognition is the same model class in both (VietOCR, batched, on the GPU) and is not
where the time goes.

Where they differ at all, it is the **cash flow**: 16 columns mapped against experiment_8's 11,
and 11 agreeing against 6, from an identical 38 parsed rows. Both engines read the same figures;
DeepDoc's boxes group into rows that survive the schema walk slightly better.

The trade runs the other way on individual digits, and the margin is thin either way. VietOCR's
`vgg_transformer` (experiment_8) reads retained earnings as **1,351,706**, which the page's own
broken text layer confirms (`t 351.706`); `vgg_seq2seq` here reads 1,331,706. It also takes the
2012 column for *Cam kết trong nghiệp vụ L/C*. Two lines out of ~125 — real, but not the reason
to pick one stack over the other; the 8.6x is.

`hdkd_i_luu_chuyen_tien_thuan_tu_hoat_dong_kinh_doanh` comes out as 2,223,450 from **both**
engines where CafeF says −6,133,590 — two independent reads agreeing against the third source is
a mapping or vintage difference, not a misread.

## Layout and table structure — `table_structure.py`

Not wired into the statements; run separately as evidence for whether it should be.

Over the three balance-sheet pages the models find **68 table rows and 15 table columns** (5 per
page: section numbering, label, *Thuyết minh* note reference, and the two period columns) plus 2
column headers and 3 spanning cells. That is the correct grid, and it is the part the current
parser has to infer — including the note column that had to be dropped by magnitude in
experiment_8 because it clusters exactly like a period column.

DeepDoc's own `construct_table` reconstruction (`out/tsr_page*.md`) is good on plain rows:

```
| I  | Tiền mặt, vàng bạc, đá quý                | 4 | 2.043.490 | 7.096.310 |
| II | Tiền gửi tại Ngân hàng Nhà nước Việt Nam  | 5 | 3.065.322 | 5.554.977 |
```

and poor on two things: the page header is emitted as a table row repeated across every cell, and
a section with sub-items collapses into ONE cell (`III I 2 3 | Tiền gửi và cho vay … Tiền gửi lại
… Cho vay … Dự phòng …`) via the projected-row-header/spanning logic. As it stands the markdown
is not a substitute for the row builder — but the *regions* it detects (`layout_tsr_regions.csv`)
are, and they are what a future column-detection step would use.

## What had to be adjusted, and why it matters if this is adopted

The fork is a working demo, not a library. Four things, all in `deepdoc_vietocr_engine.py`:

1. **`utils.file_utils` / `utils.settings` are shimmed.** They belong to the full RAGFlow server,
   not the fork, and the name collides with this repo's own `src/utils` — both import as `utils`,
   and `web_scraper` needs ours for `utils.constants`. Pre-registering the two submodules in
   `sys.modules` satisfies the fork without either package moving.
2. **`OCR(model_dir=…)` cannot be used.** The constructor only populates `text_detector` /
   `text_recognizer` inside its `if not model_dir:` branch, so passing a directory yields an
   object with no models. The shimmed `get_project_base_directory()` is what points it at the
   vendor checkout instead.
3. **The recogniser is substituted BEFORE construction.** Theirs sets `cnn.pretrained=True`,
   which downloads 548 MB of ImageNet VGG19 weights that the checkpoint immediately overwrites,
   and resolves that checkpoint against a RELATIVE path (`vietocr\weight\vgg_seq2seq.pth`), so it
   raises anywhere but the vendor root. It also loops `predict()` one crop at a time on the CPU —
   ~3,000 sequential forward passes for this filing. The replacement is the same model and the
   same checkpoint, batched, on the GPU.
4. **The GPU memory-arena option is dropped.** `load_model` decides whether to attach
   `memory.enable_memory_arena_shrinkage=gpu:0` by asking **torch** whether a GPU exists, not by
   asking whether the session it just built is on one. With torch+CUDA and a CPU onnxruntime,
   every single inference raises `INVALID_ARGUMENT: Did not find an arena based allocator
   registered for device-id combination in the memory arena shrink list: gpu:0`.

One tuning change, exposed as `--det-side-len` (default 1600): DeepDoc fixes the detector's input
at **960 px** on the long side, which reads an A4 page scanned at 200 dpi (2367 px) at roughly
85 dpi. Thousands separators and the difference between 3 and 8 are the first casualties.

## Batch — a whole range of quarters (`run_batch_acb.py`)

The model called **`onnx`** in the head-to-head. Same range (ACB Q1-2014…Q4-2016), same scorer
(`batch.py`, imported from experiment_8), same reference — the engine is the only difference.

```bash
../../ocr_env9/Scripts/python run_batch_acb.py     # writes out_batch/
python ../compare_models.py                        # after both models have run
```

**Result (`out_batch/report.md`): 34/36 statements reconcile, 721/868 figures match** the
production reference by magnitude — all 12 income statements and all 12 cash flows reconcile; two
balance sheets (Q1-2016, Q3-2016) are rejected on a schema-mapping collision, not an OCR failure.
OCR cost **237 s for 168 pages (1.4 s/page)** across the twelve filings. The residual `differ` is
schema mapping on the bank sub-item lines and CafeF-vintage zeros, the same as the single doc.

## Files

| file | what it is |
|---|---|
| `setup_vendor.py` | clone (LFS-skipped) + `InfiniFlow/deepdoc` model download + a size check |
| `deepdoc_vietocr_engine.py` | the engine, the four adjustments above, `BatchedVietOcr` |
| `run_acb_2013.py` | single-doc CLI (`--det-side-len`, `--dpi`, `--min-score`, `--device`, …) |
| `run_batch_acb.py` | batch CLI over ACB Q1-2014…Q4-2016 (writes `out_batch/`) |
| `table_structure.py` | layout + TSR → `layout_tsr_regions.csv`, `tsr_page*.md` |
| `out/…` | single-doc outputs, same layout as experiment_8's `out/` |
| `out_batch/…` | `cells.csv`, `detail.csv`, `report.md` — directly comparable to experiment_8's |
