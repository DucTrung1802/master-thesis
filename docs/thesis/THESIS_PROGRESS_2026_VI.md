# Luận văn Thạc sĩ — Báo cáo Tiến độ, Tháng 01 → Tháng 07/2026

> ⚠️ **Deliverable write-up, not an operating document.** For how to run the pipeline see
> [RUNBOOK.md](../RUNBOOK.md); for what it has proved, [CLAUDE.md](../../CLAUDE.md); for what is
> broken, [ISSUES.md](../ISSUES.md); for what is next, [TODO.md](../TODO.md).

**Repository:** `master-thesis` · **Tác giả:** DucTrung1802
**Giai đoạn:** 2026-01-01 → 2026-07-27 (commit cuối cùng tại thời điểm viết báo cáo)
**Nguồn:** tái dựng từ lịch sử git (`git log`), nội dung chi tiết của các commit, và các tài liệu bàn giao `CONTEXT.md`.

| Chỉ số | Giá trị |
| --- | --- |
| Số commit trong giai đoạn | 517 |
| Số pull request đã merge | 54 (`#137` → `#190`) |
| Dòng nhánh tích hợp | `main_v2` → `main_v3` (T3) → `main_v4` (T4–T5) → `main_v5` (T6) → quay lại `main_v2` (từ 30/06, nhánh hiện tại) |
| Các nhánh tính năng đang dùng | `web_scraper(_v2)`, `data_preprocessor(_v2)`, `data_postprocessor`, `database_driver`, `train_test_creator(_v2)`, `model(_v2)`, `evaluator`, `experiment`, `thread_manager` |

### Số commit theo tháng

| Tháng | Commit | Chủ đề chính |
| --- | --- | --- |
| 01/2026 | 3 | Tồn đọng: nguồn dữ liệu tỷ giá |
| 02/2026 | 17 | tsfresh → mô hình CNN đầu tiên trên VN-Index |
| 03/2026 | 78 | Hạ tầng huấn luyện CNN, TensorBoard, quét thử nghiệm hàng loạt |
| 04/2026 | 93 | Thư viện phân tích kỹ thuật đầy đủ + chọn lọc đặc trưng |
| 05/2026 | 125 | Pipeline CSDL medallion, train/test creator, R² dương đầu tiên trên tập test |
| 06/2026 | 98 | Scraper đa nguồn, tái xây dựng bronze/silver/gold, LSTM, chuyển hướng cross-sectional |
| 07/2026 | 103 | Dữ liệu cơ bản từ PDF báo cáo tài chính (OCR), sentiment, bảng FA ở tầng silver |

---

## Tóm tắt điều hành

Năm 2026 chia thành ba giai đoạn rõ rệt.

1. **T1–T4 — xây dựng bộ máy.** Một mô hình CNN được dựng lên trên VN-Index, sau đó nỗ lực chuyển sang phần nền bên dưới nó: một thư viện đặc trưng phân tích kỹ thuật hoàn chỉnh (~60 hàm chỉ báo tự viết), một driver PostgreSQL đa luồng, và một bộ tạo train/test có thể lặp lại được.
2. **T5–T6 — xây dựng nền tảng dữ liệu, rồi phát hiện ra mô hình không có gì để học.** Một kho dữ liệu medallion bronze/silver/gold được xây trên ba nguồn thu thập (TradingView, CafeF, Simplize). Khi các mô hình đã có thể chạy sạch sẽ và lặp lại được, hết lượt quét này đến lượt quét khác đều trả về cùng một kết luận: **lợi suất/hướng giá ngắn hạn của một cổ phiếu đơn lẻ là không thể dự báo được từ giá + chỉ báo kỹ thuật.** R² ngoài mẫu ≈ 0; ROC-AUC hướng giá ≈ 0.52.
3. **T6–T7 — chấp nhận kết luận và thay đổi dữ liệu đầu vào, không phải mô hình.** Kết luận rút ra là ràng buộc quyết định nằm ở *dữ liệu*, không phải kiến trúc mô hình. Phần còn lại của giai đoạn là một chiến dịch thu thập dữ liệu bền bỉ: lịch công bố thông tin doanh nghiệp, số lượng cổ phiếu lưu hành theo thời điểm, một kho tin tức đầy đủ, và — khối lượng công việc lớn nhất — đọc trực tiếp báo cáo tài chính quý từ các file PDF scan của báo cáo nộp bằng OCR, đạt tỷ lệ bóc tách 100 % trên hai mã đã thực hiện.

Kết quả học thuật quan trọng nhất của năm 2026 là một **kết quả phủ định, được thiết lập chặt chẽ và ghi chép đầy đủ**: không tồn tại lợi thế (edge) giao dịch 5 ngày bền vững trong bối cảnh thị trường hiện tại chỉ dựa trên giá + dòng tiền khối ngoại, và sentiment từ văn bản đóng góp giá trị gia tăng *âm*. Toàn bộ công việc sau phát hiện đó là nỗ lực tìm kiếm thông tin thực sự trực giao (orthogonal).

---

## Tháng 01/2026 — tồn đọng, tỷ giá hối đoái

**Thay đổi.** Chỉ 3 commit; phần tiếp nối công việc tháng 12. Chuỗi vĩ mô `EXCHANGE_RATE` được trỏ sang một nguồn khác (`1275cd3`), và nhánh `data_preprocessor` được merge (PR #137).

**Vấn đề.** Nguồn tỷ giá trước đó không đạt yêu cầu để làm đầu vào cho mô hình.

**Giải pháp.** Đổi nguồn và viết lại luồng thu thập (154 dòng thêm / 82 dòng xóa).

---

## Tháng 02/2026 — từ tsfresh đến CNN trên VN-Index

**Thay đổi.**
- Bắt đầu bằng một notebook `tsfresh` để trích xuất đặc trưng chuỗi thời gian tự động (`a1b8881`), xây `X`/`y`, rồi tách train/test và tạo `X_train` đã chọn lọc đặc trưng.
- Chuyển hướng giữa tháng: **"UPDATE: change to CNN, re-evaluate the whole flow"** (`ad5b889`) — xóa 6.491 dòng, thêm 723 dòng. Cách trích xuất đặc trưng bề rộng kiểu tsfresh bị loại bỏ, thay bằng một mô hình tích chập chạy trực tiếp trên chuỗi thô.
- Các notebook `cnn_vn_index_1` và `cnn_vn_index_2`; đã tạo ra một dự báo thực tế (`e52a772`: giá đóng cửa VN-Index ngày 27/02/2026 = 1843.247).
- Cuối tháng: `REBUILD: scrape macroeconomics_gdp` — bắt đầu tái xây dựng tầng scraper bên dưới mô hình.

**Vấn đề.**
- Kích thước tensor của dataloader bị sai — một commit đánh dấu rõ điểm nghẽn: `CHECKPOINT: need to ensure the shape of dataloader` (`5e92781`).
- Pipeline tsfresh tạo ra ma trận đặc trưng quá cồng kềnh (các commit đơn lẻ thêm 133k và 233k dòng chính là output notebook ở quy mô đó).

**Giải pháp.** Xây lại toàn bộ luồng quanh một CNN với kích thước tensor được kiểm chứng tường minh, và thu hẹp lại phạm vi đặc trưng về mức mà mô hình thực sự tiêu thụ được.

---

## Tháng 03/2026 — hạ tầng huấn luyện và quét thử nghiệm có kỷ luật

Tháng mà các thí nghiệm trở nên tái lập được thay vì tùy hứng.

**Thay đổi.**
- **Scraper/ingest cho chỉ số thị trường:** xây lại `_scrape_data_stock_market_vn_hnx_index_price()`, thêm `_hnx_index_order()`, sửa scraper giá/lệnh của VN-Index, rồi hoàn thiện các bước `_ingest_*`, `_clean_*` và `_transform_stock_market_vn_index()` tương ứng.
- **Database driver:** hỗ trợ `join_clause` được đưa vào `PostgreSQLDriver` (PR #138).
- **TensorBoard end-to-end** (`6834477`), sau đó là các đợt quét thử nghiệm có hệ thống: `run_version_0` … `run_version_27`, sau đó khởi động lại thành `version_0` … `version_53`, trên các notebook `cnn_vn_index_1` … `cnn_vn_index_9`.
- Tiện ích: hàm `get_weekends()`, thông báo Windows khi huấn luyện xong (`f63274b`), ghim phiên bản thư viện trong `requirements.txt`.

**Vấn đề và cách xử lý.**

| Vấn đề | Cách giải quyết |
| --- | --- |
| Các lần chạy không so sánh được với nhau — ghi log thiếu metadata | `REWORK: add information for log and train from start` (`f75534e`) — chạm vào 152 file, xóa 872 dòng; **hủy toàn bộ kết quả trước đó và chạy lại** theo cơ chế log mới |
| Việc kiểm chứng scaling được đặt sai vị trí trong luồng | Hai biến thể cố ý: `cnn_vn_index_1 - Validate after scaling` và `cnn_vn_index_2 - Validate before scaling`, chạy song song để so sánh |
| Huấn luyện bị vượt ngưỡng (overshoot) | Notebook riêng `cnn_vn_index_2_overshoot` + `cnn_vn_index_1 - handle overshoot` |
| Môi trường trôi lệch giữa các lần chạy | Ghim mọi phiên bản thư viện; `REWORK: start training from the beginning` (`7a64a00`) xóa 168 file kết quả cũ |
| Chạy huấn luyện dài mà không có phản hồi | Thông báo Windows khi hoàn thành |

**Ghi chú về phương pháp.** Mô thức lặp lại trong tháng này — *hủy toàn bộ kết quả và chạy lại từ đầu mỗi khi cơ chế log thay đổi* — là bản năng đúng, và sẽ còn lặp lại về sau trong năm.

---

## Tháng 04/2026 — thư viện phân tích kỹ thuật, rồi chọn lọc đặc trưng

Tháng năng suất cơ học cao nhất: một thư viện đặc trưng tương đương TA-Lib, tự viết trong dự án.

**Thay đổi.**
- **~60 hàm chỉ báo được cài đặt**, nhóm theo phân loại của TA-Lib và mỗi hàm một commit riêng:
  - *Overlap studies:* SMA, EMA, DEMA, TEMA, TRIMA, WMA, KAMA, T3, BBANDS, MIDPOINT, MIDPRICE, SAR
  - *Momentum:* ADX, AROON, BOP, CCI, CMO, MACD, MFI, MOM, PPO, ROC, RSI, STOCH, STOCHRSI, TRIX, ULTOSC, WILLR
  - *Volume:* AD, ADOSC, OBV
  - *Cycle:* HT_DCPERIOD, HT_DCPHASE, HT_PHASOR, HT_SINE, HT_TRENDMODE
  - *Price transform:* AVGPRICE, MEDPRICE, TYPPRICE, WCLPRICE
  - *Volatility:* ATR, NATR, TRANGE
  - Hai chỉ báo được khảo sát và **loại bỏ một cách tường minh** (`NO ADD: add_hilbert_transform()`, `NO ADD: add_mesa_adaptive_moving_average()`) — một kết quả phủ định được ghi nhận thay vì âm thầm bỏ đi.
- Một đợt `REWORK:` quét lại toàn bộ các hàm đã viết, chuẩn hóa chữ ký hàm (`add_sma`, `add_ema`, `add_bbands`, … tất cả được làm lại trong ngày 21/04), tiếp theo là `UPDATE: clean old functions` xóa 1.514 + 75 dòng mã đã lỗi thời.
- **Notebook `feature_selection_vn_index`**; xếp hạng đặc trưng dựa trên XGBoost regressor; hoàn tất từ 28–30/04 trên 303 file.
- **Scraper:** việc thu thập giá VN-Index chuyển sang quét theo từng trang với hàm hỗ trợ `_wait_until_text_not_equals`; lớp `SwitchHandler` được đưa vào để bật/tắt từng tác vụ scrape riêng lẻ.

**Vấn đề và cách xử lý.**

| Vấn đề | Cách giải quyết |
| --- | --- |
| Chữ ký hàm không nhất quán giữa ~40 chỉ báo viết trong hai tuần | Một đợt `REWORK:` gọn trong một ngày chuẩn hóa toàn bộ, rồi xóa các cài đặt cũ |
| `ta_functions.py` quá chậm khi bật hết chỉ báo | Hai đợt tối ưu hiệu năng liên tiếp (`77eb3f2`, `993e883`) — đợt hai giảm ròng 2.318 dòng |
| Việc scrape bảng giá chỉ số bị thiếu dữ liệu / timeout | Quét phân trang kết hợp chờ theo thay đổi nội dung thay vì `sleep` cố định |
| Các tác vụ scrape chỉ chạy được kiểu tất-cả-hoặc-không-gì | `SwitchHandler` + `switch_config.json` — mỗi tác vụ có thể chạy lại độc lập |

---

## Tháng 05/2026 — kho dữ liệu medallion, và R² dương đầu tiên trên tập test

**Thay đổi.**
- **Web scraper được tổng quát hóa** từ chỗ chỉ chạy trên chỉ số sang toàn thị trường: `add_stock_market_data_scraping_tasks()`, rồi *toàn bộ* giá cổ phiếu HOSE (`863dfc3`, `147f758`), rồi dữ liệu cấp doanh nghiệp.
- **Dựng schema medallion:** `_create_tables()` cho BRONZE, SILVER và GOLD; các phương thức `_ingest_ / _clean_ / _transform_` cho chỉ số và cho giá cổ phiếu theo ngày. Cuối tháng, **toàn bộ 3.427 dòng hàm `create_table` bị xóa** để chuyển sang suy luận schema tự động.
- **Gia cố `PostgreSQLDriver`:** join nhiều cột, `IS / IS NOT NULL`, danh sách `IN / NOT IN`, và hai đợt tối ưu đa luồng (PR #144–#150).
- **`ThreadManager`** được đưa vào và tích hợp vào luồng ingest.
- **`train_test_creator` v1 → v2:** tensor theo cửa sổ trượt, chuẩn hóa `y`, tham số `STRIDE`, sửa lại `train_range`/`val_range`/`test_range`, lưu tensor và scaler cho cả train+val+test.
- **`data_evaluator` v1** và **`result_evaluator`** — phần đánh giá của vòng lặp thí nghiệm.
- **`0081ebf` — "UPDATE: first positive R2 on test set."** Kết quả mô hình đầu tiên thực sự đáng khích lệ trong năm.
- Scraper vĩ mô được làm lại cuối tháng (tỷ giá USD/VND, lãi suất liên ngân hàng Việt Nam), chuẩn hóa theo TradingView.

**Vấn đề và cách xử lý.**

| Vấn đề | Cách giải quyết |
| --- | --- |
| Ingest giá cổ phiếu theo ngày quá chậm để dùng thực tế | `ThreadManager` + `PostgreSQLDriver` đa luồng; viết lại `_ingest_enterprise_daily_price()` để tận dụng |
| Viết tay `CREATE TABLE` cho từng bảng không khả thi ở quy mô toàn thị trường | Xóa toàn bộ (3.427 dòng) và suy luận schema từ dataframe |
| Các cột chỉ có một giá trị duy nhất làm nhiễu ma trận đặc trưng | Thêm bước `Drop columns with only one unique value` vào post-processor |
| Khoảng train/val/test bị sai | Sửa tường minh (`fff1cf2`), xóa 316.437 dòng dữ liệu sinh ra không hợp lệ |
| Không thể review notebook qua diff | Thêm `ipynb_to_txt.py` để kiểm tra nội dung notebook dưới dạng text |

---

## Tháng 06/2026 — ba nguồn dữ liệu, kho dữ liệu tái xây dựng, và bước chuyển hướng

Tháng bước ngoặt. Hai việc diễn ra song song: nền tảng dữ liệu được xây lại bài bản, và nỗ lực mô hình hóa đi vào ngõ cụt.

### 6.1 Tái xây dựng scraper (`web_scraper_v2`)

- **TradingView, toàn diện:** khám phá link trên Stocks, Funds, Futures, Forex, Bonds, Economy (Crypto và Indices được bỏ qua có chủ đích), rồi thu thập dữ liệu cho từng nhóm.
- **Kiến trúc:** `BaseScraper(ABC)` + registry/factory `@register_scraper`. `WebScraper` → `TradingViewScraper`. Thêm một nguồn mới giờ chỉ cần một lớp con. Dữ liệu thô được tổ chức lại theo `raw_data/<source>/`.
- **Điều chỉnh cổ tức** (`87518f7`): nút *"Adjust data for dividends"* của TradingView giờ được bấm trước khi scrape, nên giá đã được điều chỉnh hồi tố.
- **Ba nguồn mới:** `CafeFScraper`, `GicsScraper` (bộ phân ngành GICS 2023 chính thức của MSCI: 11 sector / 25 group / 74 industry / 163 sub-industry), và `SimplizeScraper` (một endpoint JSON, không cần trình duyệt, OHLC đã điều chỉnh đầy đủ + khối lượng thực + dòng tiền khối ngoại từ 2009).

### 6.2 Tái xây dựng kho dữ liệu (`data_preprocessor_v2`)

- Bronze → silver → gold được xây lại với ép kiểu tường minh, cơ chế switch theo từng bảng, và một **tầng `unified_schema`**: mỗi mã một bảng, ghép các dòng gold của cổ phiếu với bối cảnh vĩ mô đã forward-fill, đặc trưng thời gian và biến mục tiêu có giám sát.
- **Quyết định về chất lượng nguồn, đã kiểm chứng trên VN30:** Simplize là nguồn chính (OHLC điều chỉnh đầy đủ, khối lượng thực, dòng tiền ngoại); CafeF đóng góp tách khớp lệnh/thỏa thuận và tỷ lệ sở hữu; TradingView chỉ là nguồn dự phòng cho OHLC.

### 6.3 Mô hình hóa (`model_v2`, `experiment`)

- Framework **LSTM** trên PyTorch Lightning cho VCB; thêm R² vào báo cáo; rồi một **LSTM hai chiều có attention và regularization** với hàm mất mát Huber.
- Đưa vào `experiment_history.csv` và `feature_groups.md` — mọi cấu hình đã thử đều được ghi lại cùng chỉ số của nó.
- **`experiment_1` – `experiment_3`:** nghiên cứu sự kiện breakout, so sánh mô hình trên đầu vào dạng cửa sổ (GBM/MLP/LSTM/GRU/CNN1D/Transformer), và backtest walk-forward.

### 6.4 Vấn đề và cách xử lý

| Vấn đề | Nguyên nhân gốc | Cách giải quyết |
| --- | --- | --- |
| Ingest tầng gold mất ~68 phút cho bảng stocks | Profiling cho thấy **88 %** thời gian nằm ở bước insert, và trong đó là `pandas.to_csv` trên 910 cột — *không phải* ở khâu tính TA (~12 %) | Đường đi nhanh `COPY FROM STDIN` với serialize bằng `pyarrow.write_csv`; **nhanh hơn ~4.5 lần, 68 → 15 phút** |
| Dữ liệu forex ở silver/gold rỗng hoàn toàn | Scraper lưu forex ở cột `value` (giống bonds/economy), nhưng `_ingest_silver_forex` lại đi theo nhánh OHLC và bỏ mất cột đó | Chuyển sang nhánh value kèm ép kiểu số |
| Bảng unified vượt **giới hạn 1600 cột của PostgreSQL** với các mã có lịch sử ngắn | `_helper_macro_wide` ghép ngang toàn bộ **621** mã, sinh ra hơn 600 cột vĩ mô | Giới hạn phép join chỉ với **các mã cùng ngành GICS** (~7–49 mã). Số cột giảm còn ~1.050–1.075 — và bối cảnh trở nên có ý nghĩa về mặt nội dung (các mã cùng ngành, không phải cả thị trường) |
| `COPY` ở tầng gold bị hủy vì vài tỷ số TA phát nổ | Giá trị ±inf và vượt biên so với kiểu `REAL` của PostgreSQL | Làm sạch: `>3.4e38` → NaN, số dưới chuẩn `<1e-37` → 0 |
| Thông tin ngành bị mất khỏi dữ liệu cổ phiếu, làm phẳng cây thư mục output | Scraper chỉ đọc một loại link con | Đọc cả ba loại một cách tổng quát; output phản chiếu đúng cây ngành và có thêm cột `sector` |
| **Mô hình overfitting trên VCB** | Dung lượng mô hình quá lớn so với lượng tín hiệu thực có | Top-40 đặc trưng, hidden 48, dropout 0.4, weight decay 1e-3, attention pooling, hàm mất mát Huber. **Overfitting được xử lý — nhưng R² ngoài mẫu vẫn ≈ 0** |
| "Multi-task direction head" là sai về mặt khái niệm | Chỉ có một biến mục tiêu (lợi suất 5 ngày); hướng giá là đại lượng *dẫn xuất* từ nó | Bỏ head đó; `dir_acc`/AUC nay được tính từ chính dự báo lợi suất. Các dòng lỗi thời được đánh dấu trong `experiment_history.csv` |

### 6.5 Bước chuyển hướng

`afa53fc` định nghĩa lại biến mục tiêu từ lợi suất của một cổ phiếu đơn lẻ sang **lợi suất tương đối cross-sectional** — *mã nào trong VN30 sẽ vượt trội so với toàn rổ trong h ngày tới*. Ridge, walk-forward mở rộng dần, mô hình chi phí dựa trên vòng quay danh mục.

> **Kết quả:** rank IC ≈ +0.03, ổn định qua 10 năm walk-forward; dương ròng sau chi phí 40 bps khi tái cân bằng hàng tháng (long/short ≈ +6.6 %/năm, chỉ long ≈ +3.9 %/năm).

Và kết luận của `experiment_3` về công việc trên cổ phiếu đơn lẻ:

> AUC 0.77 trên tín hiệu breakout là một **bộ dò chế độ biến động, không phải alpha có thể giao dịch**. Sharpe walk-forward của ML là 0.67 ≈ mua-và-nắm-giữ 0.66. Long/short cross-sectional trên VN30: **Sharpe −0.53, drawdown −88 %** (tín hiệu xếp hạng theo độ biến động chứ không theo hướng). Trên sáu biến mục tiêu ngắn hạn, không cái nào vượt được thị trường sau chi phí. **Ràng buộc quyết định là DỮ LIỆU, không phải biến mục tiêu hay mô hình.**

---

## Tháng 07/2026 — thu thập dữ liệu trực giao (báo cáo tài chính, tin tức, OCR)

Sau khi đã xác lập rằng tinh chỉnh mô hình thêm nữa cũng vô ích, cả tháng được dành cho việc thu thập những thông tin mà mô hình trước đó chưa từng có.

### 7.1 Khép lại giai đoạn mô hình hóa

- `src/model/common/`: framework tái sử dụng — tham chiếu dataset bằng content-hash, thư mục run bất biến, vòng lặp huấn luyện GPU, bộ chỉ số (gồm cả baseline-về-0 và IC), và bảng xếp hạng chỉ-ghi-thêm `runs/index.csv`.
- **Ba đợt quét lookback đầy đủ (1 → 30)**, tổng cộng 27 lần chạy:
  - Hồi quy `return_5day` — **không lookback nào vượt được RMSE baseline-về-0 là 0.0357**
  - Phân loại `direction_5day` — `dir_auc` trên test trung bình **0.519** (khoảng 0.47–0.55)
  - `probability_gain_5pct_5day` — ROC-AUC trên test trung bình **0.545**, nhưng AUC của val và test *nghịch tương quan* (lb15: val tệ nhất, test tốt nhất) → là nhiễu, không phải lợi thế
- **`025956a` — trần khả năng giao dịch, được ghi chép lại.** Dòng tiền khối ngoại là tín hiệu duy nhất còn sống sót sau 2021 (các yếu tố giá đã đảo chiều); bộ phân loại tốt nhất ngoài mẫu chững lại ở AUC ≈ 0.52–0.53; Sharpe gộp của long/short đạt 1.3–1.6 nhưng bị triệt tiêu bởi vòng quay 65–78 %/vế mỗi tuần. Kiểm soát vòng quay đưa lợi nhuận ròng@20bps lên +0.46 tổng thể — nhưng đó là +1.46 (2017-20) so với **−0.51 (2022-26)**. Kết luận: không có lợi thế 5 ngày bền vững có thể giao dịch trong bối cảnh hiện tại.

### 7.2 Chiến dịch dữ liệu

| Thí nghiệm | Sản phẩm | Khó khăn đáng chú ý |
| --- | --- | --- |
| **experiment_4** | **Ngày công bố** báo cáo tài chính VCB 2009→nay, theo chuỗi ưu tiên: thủ công > ngày ký trong PDF > tin Vietstock > tên file CafeF | Phải tách Q4 quý và báo cáo năm đã kiểm toán; mức độ đảm bảo xếp hạng Chưa kiểm toán < Soát xét < Kiểm toán |
| **experiment_5** | **Số cổ phiếu lưu hành theo thời điểm** của VCB | **Phương pháp ban đầu sai và phải làm lại.** Xem bên dưới. |
| **experiment_6** | **Kho tin tức VCB** — 1.629 tiêu đề duy nhất 2008→2026 kèm nội dung bài | Tiêu đề phải đọc từ nội dung thẻ anchor (thuộc tính `title=""` bị hỏng với dấu nháy nhúng trong dữ liệu cũ) |
| **experiment_7** | **Báo cáo tài chính quý**, cho mọi mã, từ API JSON BCTC của CafeF + một tầng PDF | Khối lượng công việc chính của tháng — xem §7.3 |
| **experiments 8 & 9** | So kè OCR tiếng Việt: PaddleOCR+VietOCR vs DeepDoc-ONNX+VietOCR | ONNX nhanh hơn **~10 lần** (1.4 vs 13.9 giây/trang) ở cùng độ chính xác → được chọn cho production |

**Việc tự phát hiện sai sót ở experiment_5 — kết quả quan trọng nhất về mặt phương pháp trong tháng.** Phương pháp ban đầu neo vào số cổ phiếu hiện tại rồi đi ngược lịch sử sự kiện doanh nghiệp của CafeF. Khi đối chiếu với chính bảng cân đối kế toán VCB đã nộp, nhật ký sự kiện đó hóa ra **không đầy đủ** — nó bỏ sót ba đợt tăng vốn giai đoạn 2010–2012. Chuỗi số liệu vì thế thổi phồng giữa năm 2011 lên **31.8 %** và mốc nền trước 2010 lên **44 %**, âm thầm làm sai lệch mọi tính toán vốn hóa trước 2013. Nó chỉ đúng từ giữa 2014 trở đi — và đó chính xác là lý do một lần kiểm tra chéo trước đó với các báo cáo sau 2016 đã "qua".
*Cách khắc phục:* nguồn có thẩm quyền nay là vốn điều lệ do chính công ty công bố (mã 411 trên bảng cân đối) ÷ mệnh giá 10.000 VND — đầy đủ và có báo cáo làm chứng trên cả 65 quý đã nộp. Nhật ký sự kiện vẫn được lấy về, nhưng chỉ để *gán ngày và nhãn* cho từng bước thay đổi.

### 7.3 Đọc báo cáo tài chính scan tiếng Việt — pipeline OCR

Nỗ lực kỹ thuật lớn nhất trong năm. API JSON của CafeF có lỗ hổng (VCB thiếu 20 quý báo cáo); các file PDF nộp là nơi duy nhất còn giữ những con số đó — và **~90 % báo cáo của VCB là ảnh scan không có tầng text**, kể cả các báo cáo gần đây (Q1-2026 là 53 trang ảnh).

**Đã xây dựng:** `cafef_pdf_scraper` (tải kho tài liệu), `cafef_schema.py` (hệ thống tài khoản chuẩn), `cafef_pdf_parser.py` (một báo cáo → các bảng), `cafef_financials.py` (kho tài liệu → bảng dữ liệu CSV), cùng `onnx_ocr.py` và bộ dò `_deepdoc` được vendor vào.

**Các quyết định thiết kế then chốt:**
- **Bốn hệ thống tài khoản, không phải hai** — ngân hàng (TCTD), doanh nghiệp (DN), chứng khoán (CTCK), bảo hiểm (DNBH). Chúng *không* chia sẻ chỉ tiêu nào: hệ nào cũng có "mã 1" và mỗi hệ mã đó mang một nghĩa khác nhau. Template được xác định bằng **fingerprint** số lượng chỉ tiêu của mã đó, chứ không phải bằng phân loại ngành nghề — GICS cho biết doanh nghiệp *là gì*, còn hệ thống tài khoản cho biết *báo cáo trông như thế nào*, và hai thứ đó mâu thuẫn nhau (HVA nằm trong nhóm ngành chứng khoán nhưng lập báo cáo theo template doanh nghiệp).
- **Template là một thư mục, không phải một cột**, để mỗi thư mục đồng nhất về schema.
- **`publish_date` đọc từ bên trong chính file báo cáo.** Q4-2025 của VCB kết thúc ngày 31/12/2025 nhưng mãi đến **27/03/2026** mới được công bố — nếu ghép dữ liệu cơ bản theo ngày kết thúc kỳ thì mỗi năm mô hình lại được "nhìn trước" mười hai tuần.

**Các bài toán khó và cách xử lý:**

| Vấn đề | Nguyên nhân | Cách khắc phục |
| --- | --- | --- |
| Các bảng sinh ra 332 cột trong khi hệ thống tài khoản chỉ có 90 — không có gì khớp nhau theo thời gian | Cột được đặt tên theo *những gì OCR đọc được* | Ánh xạ các dòng đã bóc tách về đúng schema chuẩn |
| Không thể đơn giản hạ ngưỡng so khớp mờ | Một tên tài khoản ngắn là chuỗi con của tên dài thường xuyên hơn ta tưởng — "TỔNG VỐN CHỦ SỞ HỮU" đạt 0.75 so với "TỔNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU". Ở mức 0.72, **48 trong 69 bảng cân đối** bị loại | Cố định ngưỡng ở **0.80**, cộng thêm kiểm tra containment ràng buộc trên chuỗi *ngắn hơn* |
| Cả bảng báo cáo biến mất | OCR làm hỏng mã biểu mẫu ("Mẫu B02" → "BU2"/"Bữ2"/"BUT") | **Bốn** tín hiệu độc lập: mã biểu mẫu → tiêu đề trong header (so khớp mờ, giới hạn trong vùng header) → tính liền kề trang → thứ tự các bảng |
| Bản scan lưu ở `/Rotate 180` cho ra toạ độ chữ bị lật gương ("nhãn bên trái, số bên phải" bị đảo ngược) | PyMuPDF rasterize đúng chiều để OCR nhưng trả về toạ độ trong không gian chưa xoay | Xử lý phép lật gương tường minh; **không** pre-rotate khi dùng ONNX (`get_pixmap` đã áp dụng `/Rotate` rồi) |
| Báo cáo lưu chuyển tiền tệ của ACB 2013-15 biến mất | Tầng text dùng font legacy lỗi CMap trả về chuỗi *dài nhưng rác*, nên cổng kiểm tra chỉ dựa trên độ dài đã bỏ qua bước OCR | Hai bộ dò mojibake: tỷ lệ token ≤2 ký tự (tiếng Việt thật ≈ 0.23 vs mojibake ≈ 0.45) và cổng tỷ lệ dấu thanh cho mojibake dạng thay thế ký tự |
| VietOCR bịa thêm chữ số đầu: `96.922.247` → `196.922.247`, lặp lại ổn định ở mọi mức 200–600 dpi | Không phải giới hạn của bộ nhận dạng — mà là **hộp phát hiện bám sát nét chữ** | `CROP_PAD_PT = 2.0`. Một cuộc so kè (vgg_seq2seq, vgg_transformer, EasyOCR, Tesseract) cho thấy cả bốn đều đọc đúng khi crop rộng hơn. *Hãy kiểm tra vùng crop trước khi đi tìm một mô hình OCR khác.* |
| Có những trang không bao giờ được OCR | Một trang mà toàn bộ tầng text chỉ là **con dấu chữ ký** vẫn vượt qua ngưỡng text tối thiểu và không kích hoạt bộ dò mojibake nào | `_page_content_text` — 27 trang thay đổi trên toàn kho |

**Ba lớp lỗi mà việc đối chiếu số liệu *không thể* phát hiện** (cả ba đều tự cân đối nội bộ, và cả ba đều đã xảy ra):
1. **Đơn vị** — phần lớn báo cáo dùng triệu VND, nhưng các báo cáo năm 2009 dùng đồng: sai lệch 10⁶ mà vẫn khớp hoàn hảo.
2. **Lũy kế vs. riêng quý** — báo cáo bán niên chỉ in cột lũy kế tháng 1–6. Lấy nguyên con số đó khiến LNTT Q2-2024 của VCB thành 20.835 tỷ thay vì 10.116 tỷ. Phải suy ra bằng 6 tháng − Q1. Tương tự với báo cáo năm đã kiểm toán: Q4 = cả năm − (Q1+Q2+Q3).
3. **Dấu** — CafeF lưu chi phí dưới dạng số dương, còn báo cáo in trong ngoặc đơn.
Chỉ có **kiểm tra độ lớn so với các quý liền kề** mới bắt được lỗi 1 và 2; chốt kiểm tra đó nay chạy trước mọi thao tác ghi.

**Độ phủ đạt được.** Chấm điểm theo giai đoạn thực sự có file báo cáo (một quý trước lần nộp đầu tiên của mã đó không bao giờ có thể là `pdf` — nó chỉ tồn tại trong tab của CafeF):

| Mã | Khoảng thời gian | Số ô | Từ PDF |
| --- | --- | --- | --- |
| VCB | Q3-2008 → Q1-2026, 71 quý | 213 | **213/213 (100 %)**, 210 có ngày công bố |
| ACB | Q1-2010 → Q1-2026, 65 quý | 195 | **195/195 (100 %)** |

Con số của ACB đi qua các mốc `98 → 161 → 186 → 189 → 193 → 195` sau năm đợt gia cố liên tiếp, mỗi sửa lỗi đều truy được về một nguyên nhân cụ thể và được khoanh vùng để không thể ảnh hưởng tới quý đã bóc tách thành công — được kiểm chứng bằng cách chạy lại chính những báo cáo đã hoạt động tốt.

### 7.4 Kho dữ liệu và các tầng phía sau, tháng 7

- Bronze của CafeF được tách thành mỗi thư mục scraper một bảng trung thực với dữ liệu thô; **mọi khóa được tách từ `EXCHANGE:TICKER` thành hai cột riêng `exchange` + `ticker`**; tên cột được chuẩn hóa xuyên suốt (`close_adj` → `close_adjust`, `f_*_vol` → `foreign_*_volume`, …).
- Silver được xây lại thành phép join bốn chiều dữ liệu ngày của CafeF (2.388.368 dòng, không bị nhân bản) + cây phân ngành GICS đầy đủ (99.7 % số dòng được phân loại); đổi tên `stocks` → `stocks_basic`.
- **`silver.stocks_basic_financials_bank`** — giá theo ngày × báo cáo tài chính quý qua `merge_asof` **trên `publish_date`**, để mỗi ngày giao dịch mang theo quý *đã công bố* gần nhất: không có look-ahead theo thiết kế.
- **`silver.stocks_basic_financials_bank_fa`** — 26 chỉ số cơ bản (P/E, P/B, ROE, ROA, EPS, NIM, CIR, LDR, tăng trưởng…). Đã kiểm chứng với số liệu mới nhất của VCB: P/E 14.13, P/B 2.56, ROE 22.2 %, NIM 2.69 %.
- **`src/sentiment`** — bộ chấm điểm cảm xúc tiếng Việt 3 lớp dựa trên PhoBERT chạy trên kho tin tức, kèm các thí nghiệm dự báo giá với bộ chia walk-forward *có thanh lọc và vùng đệm (purged, embargoed)*.

**Kết quả phủ định nổi bật khác của tháng 7.** Cảm xúc từ văn bản chỉ học được khi nhãn cũng được suy ra từ chính văn bản (QWK 0.61). Mọi biến mục tiêu neo vào giá đều thất bại: `close[N+5]` thắng bước ngẫu nhiên 0/7 lần, dự báo hướng ≈ 0.49, P(tăng ≥5 %) AUC ≈ 0.5. Và phép khảo sát bổ sung còn tệ hơn mức trung tính — **thêm sentiment lên trên giá/TA làm kết quả xấu đi** (AUC hướng giá 0.543 → 0.534; QWK 5 mức 0.175 → 0.045, khi vector embedding 768 chiều nhấn chìm 14 đặc trưng giá).

**Một lỗi hạ tầng đáng ghi nhận.** `ThreadManager` chỉ nhận tham số phần trăm `power`, và công thức `cpu * power/100 * 0.4` cho ra số luồng *thập phân* (2.4 trên máy 20 nhân) — nên pool luôn chạy ~2 luồng bất kể cấu hình máy, khiến đợt scrape toàn thị trường của CafeF chạy rất chậm. Đã sửa bằng tham số `max_workers` tường minh (mặc định 16), có `max(1, int(...))` bảo vệ; đồng thời sửa một lỗi tiềm ẩn `AttributeError` khi `power` không hợp lệ. Một commit tiếp theo phát hiện `CafeFNewsScraper`/`CafeFPdfScraper` override `__init__` nhưng không forward `max_workers`, gây `TypeError` ngay khi truyền tham số này.

---

## Các chủ đề xuyên suốt

**1. Các dạng lỗi lặp lại.** Ba lớp lỗi xuất hiện nhiều lần và đáng được gọi tên:
- *Lỗi đúng-sai âm thầm mà vẫn vượt qua chính các kiểm tra của nó* — việc tái dựng số cổ phiếu, việc đọc nhầm cột lũy kế, lỗi đơn vị, quy ước dấu. Mỗi trường hợp đều khớp hoàn hảo trong khi vẫn sai. Biện pháp đối phó đã được áp dụng — **kiểm tra độ lớn so với các quý liền kề, cộng với xác nhận chéo từ nguồn độc lập** — là bài học phải trả giá mới có.
- *Cấu hình âm thầm xuống cấp* — số luồng thập phân, ký tự `_` trong `LIKE` khớp với mọi ký tự (`lb2__%` khớp cả `lb20`), các cột silver quay về VARCHAR. Tất cả đều tạo ra hệ thống chạy-được-nhưng-sai.
- *Chạm giới hạn quy mô muộn* — trần 1600 cột của PostgreSQL, `to_csv` trên 910 cột, ~97 GB file PDF.

**2. Kỷ luật đã mang lại kết quả.** Xóa toàn bộ kết quả mỗi khi cơ chế log thay đổi. Commit cả những chỉ báo bị loại dưới dạng `NO ADD:` thay vì bỏ đi trong im lặng. Đưa `experiment_history.csv` và `runs/index.csv` vào git. Viết các tài liệu bàn giao `CONTEXT.md` (năm tài liệu, tổng cộng 183 KB) ghi lại *tại sao*, chứ không chỉ *cái gì*. Không con số nào được ghi vào báo cáo tài chính trừ khi nó khớp với chính các chỉ tiêu tổng in trên báo cáo đó.

**3. Mạch phát triển tư duy.** Mô hình → mô hình tốt hơn → đặc trưng tốt hơn → dữ liệu tốt hơn → *dữ liệu mới là ràng buộc* → đi tìm dữ liệu khác. Mỗi bước đều bị bằng chứng ép buộc chứ không phải giả định trước, và mỗi kết quả phủ định đều được ghi chép lại thay vì che giấu. Đó chính là phần lõi có thể bảo vệ được của luận văn.

---

## Các hạng mục còn dở dang tính đến 27/07/2026

1. **Kiểm thử hồi quy chưa hoàn tất trên pipeline OCR (đã được đánh dấu trong repo).** Sửa lỗi số 8 (`CROP_PAD_PT`) và số 9 (`Y_TOL 3.0 → 4.0`) thay đổi vùng crop và cách gom dòng trên **mọi** trang chạy ONNX, cho **mọi** mã, và lần chạy kiểm thử hồi quy của chúng đã bắt đầu ba lần nhưng đều bị mất. Cần chạy lại `regress_cf` (16 báo cáo lưu chuyển tiền tệ đã được chấp nhận, kỳ vọng 16/0) và `verify_cascade` — mất khoảng 50–60 phút — trước khi tin dùng parser ngoài phạm vi ACB. Hướng dẫn nằm trong `src/web_scraper/CONTEXT.md`.
2. **Các cổng kiểm tra chỉ chứng minh các chỉ tiêu tổng, không phải từng dòng.** Một dòng lấy từ PDF có thể mỏng hơn dòng CafeF mà nó thay thế (28 chỉ tiêu so với 47), và một chỉ tiêu chi tiết có thể sai trong khi cả báo cáo vẫn khớp. Bên tiêu thụ nếu cần một chỉ tiêu phụ nên đối chiếu thêm với `from_api`.
3. **Độ phủ mới chỉ là hai mã.** VCB và ACB đã hoàn tất; template ngân hàng là template duy nhất đã được bóc tách. Ba template còn lại (doanh nghiệp, chứng khoán, bảo hiểm) đã có schema nhưng chưa có mã nào được bóc tách. Ở tầng dưới, `silver.*_financials_bank_fa` hiện chỉ chứa VCB.
4. **Kho ngữ liệu sentiment còn mỏng ở cấp độ từng mã** để đưa ra kết luận — tin tức hiện đã có cho toàn bộ 777 mã (~405 nghìn dòng), nhưng các thí nghiệm sentiment mới chỉ chạy trên 3 mã.
5. **Câu hỏi về khả năng giao dịch vẫn chưa có lời giải theo hướng khẳng định.** Chiến lược cross-sectional VN30 là kết quả dương ròng duy nhất, và lợi thế của nó nằm ở cấp danh mục, suy giảm sau 2021, và nhạy cảm với vòng quay.
6. **Thay đổi chưa commit** tại thời điểm viết: `src/web_scraper/cafef_financials.py`, `src/web_scraper/cafef_pdf_parser.py`.

---

*Được tạo ngày 28/07/2026 từ lịch sử git của repository.*
