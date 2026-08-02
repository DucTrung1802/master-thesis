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

### A1. Kiểm tra dữ liệu trước khi xây

- [ ] **1. Kiểm tra lỗ hổng 2012 trong `bronze.cafef_news`.**
  2012 chỉ có 3.781 dòng, so với 11.318 (2011) và 15.703 (2013). Xác định là lỗi scrape hay
  thật. **Acceptance:** biết fold nào không dùng được; ghi kết luận vào guidance.md §2.4.

- [ ] **2. Đếm phân bố `type` / `category` / timestamp date-only trực tiếp trên bảng bronze**,
  đối chiếu với số đo trên đĩa (78.017 editorial · 326.722 disclosure · 581 error ·
  89.698 dòng `00:00:00`). **Acceptance:** khớp, hoặc biết vì sao lệch (bronze drop 2 dòng
  null-key).

### A2. Silver — làm sạch

- [ ] **3. `src/sentiment/news_clean.py` — TẦNG 1, pure functions, không đụng DB.**
  Loại `type='error'` + content rỗng · dedup `(ticker, ngày, headline chuẩn hoá)` · bóc
  boilerplate (`Normal 0 false false false EN-US X-NONE …`, `- File đính kèm: *.pdf`,
  `Theo HOSE`) · `ts_resolved` + `ts_is_date_only` · **`trading_date` = phiên đầu tiên mở
  cửa sau `ts_resolved`** (09:00 ICT) · `relevance_score` · `text_for_scoring` (đã tách từ) ·
  `is_editorial`.
  Nhận DataFrame, trả DataFrame. Lịch phiên truyền vào như tham số, **không tự query**.

- [ ] **4. Bảng `silver.cafef_news` — ĐỦ BA TẦNG.**

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

- [ ] **5. Bảng `gold.news_weekly_panel`, phiên bản TỐI THIỂU — ĐỦ BA TẦNG.**
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

- [ ] **6. ⭐ Chạy Model 2 + walk-forward có tính phí, CHỈ với `if_news` / `n_docs`.**
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
