# TODO — `src/orchestration`

> Danh sách việc, đánh số liên tục. **Phần A** là việc chính đang làm (luồng news
> sentiment). **Phần B** là backlog có sẵn của pipeline. **Phần C** là lỗi đã biết chưa sửa.
>
> Lý do và thiết kế: [`experiment/experiment_10/guidance.md`](../../experiment/experiment_10/guidance.md).
> Hiện trạng pipeline: [CONTEXT.md](CONTEXT.md). File này chỉ trả lời *"làm gì tiếp"*.
>
> Mọi lệnh chạy từ **repo root**, trong `mt_env`, với `$env:DAGSTER_HOME` là đường dẫn tuyệt đối.
> `Clear-Content logs\app.log` trước mỗi run.

---

## ⚠️ Quy tắc bắt buộc — mọi bảng bronze / silver / gold PHẢI là Dagster asset

**Không script rời, không notebook, không hàm gọi tay.** Một bảng = một asset. Áp dụng cho
mọi mục có chữ `silver.` hoặc `gold.` trong file này: **4, 5, 13, 14** (Phần A) và
**17, 18, 19** (Phần B).

Asset là **wrapper mỏng** — nguyên tắc đã có trong [CONTEXT.md](CONTEXT.md) §3:

> *"No pipeline logic lives here. Every asset is a thin wrapper over a method that already
> exists in `src/`. Delete `src/orchestration/` and nothing is lost but the scheduling."*

**Ba tầng, không được trộn:**

| tầng | ở đâu | chứa gì |
|---|---|---|
| 1. **pure functions** | `src/sentiment/*.py` | biến đổi DataFrame, **không biết gì về DB**, import torch lazily |
| 2. **ingest method** | `src/data_preprocessor/data_preprocessor.py` | `_ingest_{silver,gold}_*` — đọc bảng, gọi tầng 1, ghi bảng |
| 3. **asset** | `src/orchestration/assets/{silver,gold}.py` | `@asset` gọi tầng 2, khai `deps`, trả metadata |

**Checklist cho MỖI asset mới — thiếu một dòng là chưa xong:**

- [ ] `@asset(name=…, key_prefix=["silver"|"gold"], group_name=…, deps=[…])`
- [ ] **`deps` khai theo cái code THỰC SỰ MỞ, không theo văn xuôi.** Bài học §2 của
      CONTEXT.md: 3 edge đã bị khai sai lần trước vì đọc mô tả thay vì đọc code.
- [ ] **row count vào metadata** (`MetadataValue.int`) — đây là thứ để đọc.
      ⚠️ *Green ≠ dữ liệu mới*: `skip_existing=True` khiến asset có thể xanh trong 500 ms mà
      không làm gì.
- [ ] **invariant assert TRONG asset và RAISE nếu sai** — không phải comment, không phải
      `log_error` rồi `return`. Precedent: `silver/stocks_basic_financials_bank` đếm dòng có
      `publish_date > date` và **raise**; `gold/stocks_financials_bank_fa` raise nếu row
      count khác silver.
- [ ] ⚠️ **Asset gold phải tự DROP bảng trước khi ghi.** `_ingest_gold_table` đã sửa; nếu
      asset mới tự viết đường ghi riêng thì lần materialise **thứ hai** chết trên PK
      (`duplicate key value violates unique constraint`). Re-materialise là đời sống bình
      thường của một asset.
- [ ] Có mặt trong [`assets_enabled.json`](assets_enabled.json) — `true` **hoặc vắng mặt**
      = được load. Thêm `//` comment key nếu asset đắt.
- [ ] `dagster definitions validate` pass và **asset count tăng đúng số**.

⚠️ **`bootstrap()` phải được gọi lại trong resource.** `sys.path` không sống sót vào step
subprocess của Dagster; module import lazily (như `ta.ta_functions` đã từng) sẽ chết
`ModuleNotFoundError` trong step trong khi mọi asset trước đó vẫn xanh.

---

## Phần A — Luồng news sentiment

**Trạng thái đầu vào:** `bronze.cafef_news` = 405.320 dòng / 777 mã (đã green 2026-08-01).
Chưa có silver hay gold nào cho news. `silver.cafef_news_sentiment` tồn tại nhưng **stale**
(dựng khi news còn 3 mã).

**⚠️ Nguyên tắc xuyên suốt:** bước 1–5 không cần NLP. Làm xong bước 5 mới biết có đáng đầu
tư vào scorer không.

---

## ✅ KẾT QUẢ MỤC 1–6 (chạy 2026-08-03)

### 🔴 Kết luận: `if_news` KHÔNG có tác dụng. Tiêu chí dừng của mục 6 đã kích hoạt một nửa.

`gold.news_weekly_panel`, VN-top100 theo thanh khoản trailing, walk-forward 6 fold có purge
+ embargo, chi phí khứ hồi 0,5%, nhãn phân vị 25/50/25:

| horizon | features | MCC | macro-F1 | CAGR | bench | Sharpe |
|---|---|---:|---:|---:|---:|---:|
| **4 tuần** | controls | **+0,053** | 0,276 | **14,63%** | 10,57% | **0,64** |
| | **news** | **+0,001** | 0,224 | 3,21% | 10,57% | 0,25 |
| | controls + news | +0,051 | 0,276 | 12,19% | 10,57% | 0,57 |
| **13 tuần** | controls | **+0,053** | 0,274 | **17,62%** | 12,27% | **0,84** |
| | **news** | **−0,000** | 0,224 | 10,15% | 12,27% | 0,50 |
| | controls + news | +0,056 | 0,276 | 17,08% | 12,27% | 0,81 |

**Ba điều đọc được, theo thứ tự quan trọng:**

1. **`news` một mình = MCC 0,000–0,008 ở MỌI chân trời.** Per-fold: `+0.015 −0.007 −0.007
   −0.001 +0.000 +0.004` — dao động quanh 0, vài fold âm. **Publication effect của paper 57
   không đo được trên VN.**
2. **Thêm news vào controls làm KÉM ĐI**, ở h=1, 4, 8: CAGR 14,63% → **12,19%** (h=4),
   16,40% → **14,93%** (h=8). Chênh lệch MCC nằm trọn trong biên độ giữa các fold.
3. **Tín hiệu duy nhất là momentum/thanh khoản** — `controls` cho MCC **+0,052…+0,061**,
   **dương ở cả 6 fold, ở cả 5 chân trời**, và đánh bại benchmark ở h=4/8/13.

⚠️ **Đây là §6a của `src/sentiment/CONTEXT.md` tái lập ở 777 mã thay vì 3.** Kết luận cũ
(*"adding sentiment makes the model WORSE… the only faint signal is price/TA"*) **giữ nguyên
sau khi tăng độ rộng 259 lần.** Đòn bẩy mà chính module đó gọi là *"the biggest lever"* đã
được kéo, và nó không đổi kết quả.

**Nhưng tiêu chí dừng chỉ đúng MỘT nửa** — nó có hai vế:

| vế | thực tế |
|---|---|
| *"`if_news` không làm gì"* | ✅ **đúng** |
| *"panel quá thưa"* | ❌ **SAI** — trên universe top-100 thanh khoản, **64,2% ticker-tuần có tin**, **32,5% có editorial** |

Panel **không** thưa ở nơi giao dịch được. Vậy nên đây **chưa phải** bằng chứng rằng *sắc thái*
vô dụng — mục 6 chỉ đo **số lượng tin**, không đo **nội dung**. Xem "Quyết định cần lấy" dưới.

### Chi tiết từng mục

**✅ 1. Lỗ hổng 2012 — CÓ THẬT, và sắc nét hơn dự đoán.** Không phải "2012 ít tin" mà là
**hố 6 tháng, dropout ~98%**:

| tháng | 2012-05 | **06** | **07** | **08** | **09** | **10** | **11** | 2012-12 | 2013-01 |
|---|---|---|---|---|---|---|---|---|---|
| dòng | 293 | **37** | **35** | **24** | **23** | **24** | **20** | 309 | 1.600 |

Ảnh hưởng 458 mã (2011: 464) → là **hố theo thời gian**, không phải thiếu một nhóm mã.
→ **Sàn dữ liệu đặt ở 2013-01-01** (`--start` mặc định của runner).

**✅ 2. Bronze khớp đĩa CHÍNH XÁC.** `type` 326.722 / 78.017 / 581 ✓ · cả 6 `category` ✓ ·
date-only 89.639 disclosure + 59 editorial = **89.698** ✓ · 405.320 = 405.322 − 2 (null-key).
Chênh duy nhất: `avg(length(content))` editorial 2.695 (DB) vs 2.687 (đĩa) — do `length(NULL)`
bị `avg()` bỏ qua trong khi pandas đếm chuỗi rỗng là 0. Không phải lỗi.

**✅ 3. `src/sentiment/news_clean.py`** — tầng 1, thuần pandas, lịch phiên là tham số.

**✅ 4. `silver.cafef_news` — 395.470 dòng × 18 cột**, 777 mã, 1,7 phút.
Bỏ 9.850: `error` 581 · rỗng 1 · **không có phiên / gap quá xa 7.322** · trùng 1.946.

> ### 🐛 Bug bắt được nhờ đọc metadata thay vì đọc màu xanh
>
> Lần materialise đầu **xanh** với 400.191 dòng — nhưng `trading_date` nhỏ nhất là
> **2009-01-02**, trong khi bronze bắt đầu 2007-02. `silver.stocks_basic` chỉ có lịch từ
> 2009-01-02, nên `searchsorted` **dồn toàn bộ 4.841 bài trước 2009 vào đúng một phiên** —
> một cú nhảy tin giả trên chính cột (`n_docs`) sinh ra để đếm tin.
>
> Sửa: `MAX_SESSION_GAP_DAYS = 15`, chọn theo phân bố thật chứ không theo cảm tính — các
> pile-up hợp lệ đều là **Tết/lễ** (2026-02-02: 761, 2025-02-04: 726, 2020-01-30: 627,
> 2021-05-06: 558), gap dài nhất hợp lệ **11 ngày**; các gap sai nằm ở **155–302 ngày**.
> Sau khi sửa: `max_gap = 15`, mọi pile-up còn lại đều là Tết/lễ.
>
> **Bài học ghi vào quy tắc đầu file:** row count + max/min vào metadata, và *đọc chúng*.

**✅ 5. `gold.news_weekly_panel` — 497.207 ticker-tuần × 27 cột**, 781 mã, 1,1 phút.
Toàn panel: **39,0%** ticker-tuần có tin, **10,5%** có editorial.
**Materialise HAI LẦN liên tiếp đều xanh, số liệu giống hệt** → drop-self đạt.

**✅ 6. `src/sentiment/weekly_xsec.py` + `run_weekly_prototype.py`** — bảng kết quả ở trên.
Thêm hai số phụ đáng ghi:
- **99,0% số tuần có q75 vượt chi phí khứ hồi 0,5%** → kiểm tra ngưỡng của paper 56 **đạt**;
  nhãn không chứa toàn những cú chạy không trả nổi phí.
- Max drawdown **−46%…−83%** trên mọi cấu hình. Đây là long-only top-quartile tái cân bằng
  theo tuần, **không có kiểm soát rủi ro** — nó là phép thử tín hiệu, không phải chiến lược.

### ⛔ Quyết định cần lấy trước khi sang mục 7

Mục 6 bác bỏ **số lượng tin**, không bác bỏ **sắc thái**. Ba lựa chọn:

| | việc | lập luận |
|---|---|---|
| **A** | **Dừng Phần A**, viết negative result | Tiết kiệm mục 7–13. Bằng chứng: MCC ≈ 0 ở 5 chân trời × 6 fold, cộng §6a cũ, cộng paper 51 (MCC 0,069 với 8,5 triệu bài) và paper 63 (50,4% out-of-sample). |
| **B** | **Làm tiếp mục 7–13** | Panel **không** thưa (64,2% coverage). Chưa ai đo sắc thái tiếng Việt hiệu chỉnh tài chính trên VN. Chi phí: ~1–2 ngày gán nhãn + fine-tune. |
| **C** | **Đổi hướng sang controls** | Tín hiệu thật duy nhất tìm được là momentum/thanh khoản: MCC +0,053, dương ở **30/30 fold**, CAGR 17,62% vs 12,27% ở h=13. Đây là kết quả **dương** đầu tiên, và nó không cần NLP. |

**Khuyến nghị: B trên một tập con, rồi quyết.** Chạy mục 7–11 **chỉ với 30 mã đưa tin nhiều
nhất** (VIC/HPG/MWG/HAG/FPT/VNM/NVL/VCB/STB/MSN…), nơi coverage editorial cao nhất. Nếu sắc
thái không thêm gì ở đó thì nó sẽ không thêm gì ở bất kỳ đâu, và câu trả lời tốn ~1/3 công.

---

### A1. Kiểm tra dữ liệu trước khi xây

- [x] **1. Kiểm tra lỗ hổng 2012 trong `bronze.cafef_news`.**
  2012 chỉ có 3.781 dòng, so với 11.318 (2011) và 15.703 (2013). Xác định là lỗi scrape hay
  thật. **Acceptance:** biết fold nào không dùng được; ghi kết luận vào guidance.md §2.4.

- [x] **2. Đếm phân bố `type` / `category` / timestamp date-only trực tiếp trên bảng bronze**,
  đối chiếu với số đo trên đĩa (78.017 editorial · 326.722 disclosure · 581 error ·
  89.698 dòng `00:00:00`). **Acceptance:** khớp, hoặc biết vì sao lệch (bronze drop 2 dòng
  null-key).

### A2. Silver — làm sạch

- [x] **3. `src/sentiment/news_clean.py` — TẦNG 1, pure functions, không đụng DB.**
  Loại `type='error'` + content rỗng · dedup `(ticker, ngày, headline chuẩn hoá)` · bóc
  boilerplate (`Normal 0 false false false EN-US X-NONE …`, `- File đính kèm: *.pdf`,
  `Theo HOSE`) · `ts_resolved` + `ts_is_date_only` · **`trading_date` = phiên đầu tiên mở
  cửa sau `ts_resolved`** (09:00 ICT) · `relevance_score` · `text_for_scoring` (đã tách từ) ·
  `is_editorial`.
  Nhận DataFrame, trả DataFrame. Lịch phiên truyền vào như tham số, **không tự query**.

- [x] **4. Bảng `silver.cafef_news` — ĐỦ BA TẦNG.**

  | tầng | việc |
  |---|---|
  | 2 | `data_preprocessor._ingest_silver_cafef_news` — đọc `bronze.cafef_news` + lịch phiên từ `silver.stocks_basic`, gọi `news_clean`, ghi bảng |
  | 3 | `@asset(name="cafef_news", key_prefix=["silver"], group_name="silver")` trong [assets/silver.py](assets/silver.py), `deps=["bronze/cafef_news", "silver/stocks_basic"]` |

  ```powershell
  dagster asset materialize -f src/orchestration/definitions.py --select "silver/cafef_news"
  ```
  **Acceptance — assert TRONG asset và RAISE, không phải comment:**
  - row count ≤ 405.320, và **vào metadata**
  - mọi `trading_date` là phiên có thật trong `silver.stocks_basic`
  - **0 dòng có `trading_date` = ngày đăng khi bài đăng sau 15:00 ICT** ← chống rò rỉ kiểu paper 46/47/50
  - `dagster definitions validate` pass, asset count **49 → 50**

### A3. Gold — panel tuần, chưa có sentiment

- [x] **5. Bảng `gold.news_weekly_panel`, phiên bản TỐI THIỂU — ĐỦ BA TẦNG.**
  Chỉ `if_news`, `n_docs`, `n_days` + 5 cột event count. **Chưa chấm sentiment.**
  Grain `(exchange, ticker, iso_week)`. Cửa sổ 09:00 ICT phiên đầu tuần `w` → phiên đầu tuần `w+1`.

  | tầng | việc |
  |---|---|
  | 1 | `src/sentiment/news_panel.py` — gộp DataFrame → panel tuần |
  | 2 | `data_preprocessor._ingest_gold_news_weekly_panel` — ⚠️ **phải DROP bảng trước khi ghi** |
  | 3 | `@asset(name="news_weekly_panel", key_prefix=["gold"], group_name="gold")` trong [assets/gold.py](assets/gold.py), `deps=["silver/cafef_news", "silver/stocks_basic"]` |

  ```powershell
  dagster asset materialize -f src/orchestration/definitions.py --select "gold/news_weekly_panel"
  ```
  **Acceptance:** assert grain **1 dòng / (mã, tuần)** và RAISE nếu sai — cùng cách
  `gold/stocks_financials_bank_fa` assert row count của nó · row count vào metadata ·
  **materialise HAI LẦN liên tiếp phải cùng xanh** (đây là bài kiểm tra cho việc drop-self).

- [x] **6. ⭐ Chạy Model 2 + walk-forward có tính phí, CHỈ với `if_news` / `n_docs`.**
  Dùng `purged_walkforward_folds` có sẵn. Universe VN100. Target `rel_h`, h ∈ {1,2,4,8,13} tuần,
  nhãn phân vị 25/50/25.
  **Đây là điểm quyết định của cả Phần A.** Nó kiểm định publication effect của paper 57 và
  thiết lập baseline mà khối sentiment phải vượt.
  **Acceptance:** báo cáo MCC + Brier + max drawdown + baseline, **theo từng fold**.
  ⛔ **Nếu `if_news` không làm gì và panel quá thưa → cân nhắc dừng Phần A tại đây và báo cáo
  negative result.** Tiết kiệm bước 7–13.

### A4. Scorer — chỉ làm nếu bước 6 đáng tiếp

- [ ] **7. Viết `src/sentiment/annotation.py`** — chọn mẫu ~5.000 câu, phân tầng theo năm,
  sàn, `category`, độ dài. Bắt buộc đủ 6 category, nhất là `dividends_and_record_date` và
  `capital_increase_and_treasury_shares` (hai nhóm scorer hiện tại sai nặng nhất).

- [ ] **8. LLM gán nhãn 5.000 câu** (3 lớp, rubric = *tác động lên giá cổ phiếu dưới góc nhìn
  nhà đầu tư*, **không phải sắc thái ngôn ngữ**).

- [ ] **9. ⭐ Tự tay gán 600–800 câu làm tập vàng.** Lưu vào
  `raw_data/annotation/news_sentiment/` và **commit vào git** (là input được curate, không
  phải thứ một lần chạy sinh lại).
  **Acceptance:** đồng thuận LLM ↔ người **≥ 80%**.
  ⛔ **Dưới 80% → sửa prompt, quay lại bước 8. Chưa được fine-tune.**

- [ ] **10. Viết `src/sentiment/scorer_finetune.py`** — fine-tune PhoBERT-base trên 5.000
  nhãn LLM. Checkpoint → `src/model/runs/news_sentiment/<version>/`.
  **Acceptance:** **macro-F1 ≥ 0,75** trên tập vàng người gán.
  ⛔ Dưới ngưỡng → chưa dùng được.

- [ ] **11. ⚠️ Viết `src/sentiment/explain.py` — cổng LIME/SHAP. Chạy TRƯỚC mọi backtest.**
  ⛔ **Trượt nếu top-20 token trọng số cao là tên mã, tên công ty, ngày tháng, boilerplate
  hoặc stopword** (*"Vietcombank"*, *"HOSE"*, *"quý 3"*, *"theo đó"*, *"Ngân hàng TMCP"*).
  Trượt → quay lại bước 8/10, **không đi tiếp**.

- [ ] **12. Sửa `sentiment_functions.py`** — trỏ sang checkpoint mới, **đổi `model_version`**,
  giữ nguyên 3 xác suất (không ghi `sentiment_score = p(pos) − p(neg)` làm cột chính nữa).

- [ ] **13. Bảng `silver.cafef_news_sentiment` — build lại cho 777 mã, ĐỦ BA TẦNG.**
  Bảng đã tồn tại nhưng **stale** (dựng khi news còn 3 mã) và hiện chỉ có ingest method,
  **chưa có asset**.

  | tầng | việc |
  |---|---|
  | 1 | `sentiment_functions.score_news_frame` — đã có, giữ interface |
  | 2 | `_ingest_silver_cafef_news_sentiment` — **sửa nguồn: đọc `silver.cafef_news`, không phải bronze** |
  | 3 | `@asset(name="cafef_news_sentiment", key_prefix=["silver"], group_name="silver")`, `deps=["silver/cafef_news"]` — **asset MỚI** |

  ⚠️ Asset này dùng GPU → cân nhắc `tags={"resource": "gpu"}` như `cafef_pdfs`, vì
  `max_concurrent=4` mà hai step cùng nạp PhoBERT là hết VRAM trên RTX 3050 4 GB.
  Ước tính ~3 phút cho 78k editorial, ~13 phút nếu chấm cả 405k.
  **Acceptance:** `model_version` mới trong metadata · row count = row count của
  `silver.cafef_news` · `HF_HUB_OFFLINE=1` vẫn còn.

### A5. Panel đầy đủ + kết quả

- [ ] **14. Mở rộng `gold.news_weekly_panel` lên đủ ~40 cột** — sửa cả ba tầng của mục 5.
  Thêm scope `s_*` / `k_*` / `m_*` (sector lấy từ cây GICS trong `silver.stocks_basic`,
  **loại chính nó**) + 5 cột cross-sectional. Trung bình không tổng · Z-score theo từng cửa
  sổ · `if_news=0` giữ dòng, cột sentiment NULL.
  ⚠️ **Thêm `deps=["silver/cafef_news_sentiment"]`** vào asset — cạnh mới, phải khai.
  **Acceptance:** grain KHÔNG đổi (vẫn 1 dòng/(mã, tuần)) — feature build chỉ được thêm CỘT,
  không bao giờ thêm DÒNG; assert cũ của mục 5 phải vẫn pass.

- [ ] **15. Chạy Model 2 đầy đủ + 6 ablation** (§7.3 của guidance.md):
  `If_news` → `+Pos/Neg` · scope · chân trời · tuần-vs-ngày trên top-30 · content-vs-headline ·
  áp sentiment nơi mô hình giá yếu (ước lượng độ mạnh **in-sample**).
  **Acceptance:** A/B cùng GBM cùng fold, có vs không khối text. **Đây là kết quả luận văn.**

- [ ] **16. Nhánh riêng theo khuôn paper 63** — nhãn từ phản ứng giá, báo cáo negative
  result trung thực. Tái dùng `price_reaction_labels.py` + `text_reaction_model.py`.
  ⚠️ **KHÔNG nối đầu ra vào Model 2** (đó là paper 46). Nếu vẫn thử: đầu ra tầng 1 **bắt
  buộc sinh out-of-fold**.

---

## Phần B — Backlog pipeline có sẵn

### B1. Silver còn thiếu (7 assets đã có / còn ~8 leaf)

> Mục 17–19 đều là việc asset — **áp dụng nguyên checklist ở đầu file**. Ở đây tầng 1 và 2
> đã tồn tại (leaf chạy được từ `main.py`), nên chỉ còn tầng 3: wrapper + `deps` + metadata
> + invariant.

- [ ] **17. Asset hoá các silver leaf còn lại:** `bonds`, `forex`, `funds`, `indices`,
  `gics`, `cafef_carry_ups`. Edge đã có sẵn trong `data_preprocessor/CONTEXT.md` §4, chép
  thẳng sang — **nhưng verify lại bằng cách đọc code mở file gì**, đừng chép văn xuôi.

### B2. Gold còn thiếu (3/7)

- [ ] **18. Asset hoá `gold/bonds`, `gold/forex`, `gold/funds`** (leaf đã chạy được, chỉ
  thiếu asset).

- [ ] **19. ⚠️ Sửa `_ingest_gold_stocks` — hiện STALE và RAISE.** `gold.stocks` giữ cột của
  thời trước bản rewrite 2026-07-19 (`close`, `volume`, `f_buy_vol`, `own_pct`); nguồn hiện
  tại không có `close` lẫn `volume` nên tầng TA đầu tiên chết.
  Cách sửa đã biết: cặp `prepare_fn` + `volume_col="volume_matched"` mà asset `_fa` đang dùng.
  ⚠️ **Nhưng bật lên sẽ định nghĩa lại `open`/`high`/`low` của `gold.stocks` thành giá đã điều
  chỉnh, và cam kết một lần rebuild ~2,4 triệu dòng × ~900 cột.** Đây là quyết định riêng,
  không phải tác dụng phụ.

### B3. Chạy thật các asset nặng

- [ ] **20. Chạy end-to-end 4 asset chưa từng chạy qua Dagster:** `trading_view_links` /
  `trading_view_data` (Selenium, hàng giờ) · 5 tab CafeF cổ phiếu + `cafef_news` (network
  toàn universe) · `cafef_pdfs` (100 partition, GB mỗi cái) · `cafef_financials` (2 partition,
  ~2,4 h mỗi cái).
  ⚠️ *"Built is not run"* — đã validate và wire, chưa quan sát chạy xong.

- [ ] **21. ⚠️ Nếu backfill TradingView: dùng single-run backfill.**
  `tag_concurrency_limits` là config của **executor**, tức per-run. Backfill 9 partition theo
  cách mặc định = 9 run × 8 browser = **72 Chrome**. `.dagster/dagster.yaml` đang rỗng.

### B4. Phase 5 — dọn switch config

- [ ] **22. Xoá mọi key `data_preprocessor/data_quality_*` trong `switch_config.json`** và
  3 entry point `ingest_{bronze,silver,gold}_data`. Hiện chúng inert (asset gọi `_ingest_*`
  trực tiếp) nhưng vẫn là nguồn sự thật thứ hai.

- [ ] **23. Xoá switch chết `data_quality_unified`** — không code nào đọc.

- [ ] **24. Quyết định số phận `trading_view_collected_links`** — không gì đọc nó; nó là leaf
  chứ không phải hub. Giữ vì `main.py` vẫn ghi file, nhưng nên ghi rõ hoặc bỏ.

---

## Phần C — Lỗi đã biết, chưa sửa

- [ ] **25. `bronze.cafef_price` có 262 dòng `high` < `low`** (vd ACB 2018-07-31: high 35.800,
  low 36.500). Lỗi của CafeF, hiện nổi lên ở gold thành `range_hl` âm.
  **Cần một data-quality screen ở bronze**, không phải vá ở gold.

- [ ] **26. `landed()` không phát hiện được "run NÀY có sinh ra gì không".**
  Nó rglob cả thư mục, nơi file dated của run trước vẫn nằm đó — nên 140 CSV chỉ có header
  vẫn xanh (sự cố 2026-07-31). Nếu muốn check per-run thì phải so với file output của chính
  run đó, không phải với thư mục.

- [ ] **27. Partition `crypto` và `options` của `trading_view_links` đỏ vĩnh viễn** — queue 0
  task (không có con trong `switch_config.json`), thư mục chưa từng tồn tại, `landed(require=True)`
  bắt đỏ dù không có gì sai. Chọn: cho `require=False` như các bước DATA, hoặc chấp nhận 2
  partition đỏ.

- [ ] **28. `logs/app.log` giờ có nhiều writer.** Executor là multiprocess, mọi step process
  cùng append vào một file → bản ghi có thể interleave, không còn là chronology chặt. Nếu
  thành vấn đề, fix là đặt tên file theo process trong `Logger`, **không phải** quay về chạy
  tuần tự.

---

## Ghi chú vận hành

- **Asset mới bật mặc định.** `assets_enabled.json`: `true` **hoặc vắng mặt** = được load.
  Chỉ set `false` cho thứ "không bao giờ được load trong repo này".
- **Tắt một asset KHÔNG tắt downstream.** Đã verify: downstream vẫn resolve và vẫn đọc thư
  mục từ đĩa. Muốn chặn cả chuỗi thì phải tắt cả downstream.
- **Green ≠ dữ liệu mới.** `skip_existing=True` trong scraper nghĩa là một asset có thể xanh
  trong 500 ms mà không fetch gì. **Đọc row-count metadata**, đừng đọc màu.
- **`bootstrap()` phải gọi lại trong resource** — `sys.path` không sống sót vào step subprocess.
- **`definitions.py` lặp lại `sys.path` insert INLINE — không phải thừa.** Đừng "dọn" đi.
