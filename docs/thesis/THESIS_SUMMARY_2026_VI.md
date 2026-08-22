# Luận văn Thạc sĩ — Tóm tắt tiến độ 2026

> ⚠️ **Deliverable write-up, not an operating document.** For how to run the pipeline see
> [RUNBOOK.md](../RUNBOOK.md); for what it has proved, [CLAUDE.md](../../CLAUDE.md); for what is
> broken, [ISSUES.md](../ISSUES.md); for what is next, [TODO.md](../TODO.md).

**Giai đoạn:** 01/01/2026 → 27/07/2026 · **517 commit** · **54 pull request** (`#137`→`#190`)
*(Bản chi tiết: [THESIS_PROGRESS_2026_VI.md](THESIS_PROGRESS_2026_VI.md))*

Mỗi tháng được trình bày theo 5 mục: **thay đổi kỹ thuật · dữ liệu · mô hình · các lần chạy & tham số · kết quả & metric**.

---

## Toàn cảnh

| Tháng | Commit | Thay đổi kỹ thuật | Dữ liệu | Mô hình | Metric |
| --- | --- | --- | --- | --- | --- |
| 01 | 3 | Đổi nguồn tỷ giá | `EXCHANGE_RATE` | — | — |
| 02 | 17 | Bỏ tsfresh → CNN | VN-Index | CNN | — |
| 03 | 78 | TensorBoard, log run | + HNX index | CNN | chỉ trong TensorBoard |
| 04 | 93 | `ta_functions.py` | + đặc trưng TA | — | xếp hạng XGBoost |
| 05 | 125 | CSDL medallion 3 tầng | Toàn bộ HOSE | CNN | R² dương (không ghi số) |
| 06 | 98 | `BaseScraper` + registry | +CafeF, GICS, Simplize | LSTM | test R² **0.007**, AUC **0.505** |
| 07 | 103 | Framework `model/common` | + BCTC từ PDF, tin tức | LSTM ×27, PhoBERT | AUC **0.505 / 0.519 / 0.545** |

---

## Tháng 01 — Tỷ giá

**Thay đổi kỹ thuật.** Viết lại luồng thu thập tỷ giá (154 dòng thêm / 82 xóa). Merge PR #137.
**Dữ liệu.** Chuyển `EXCHANGE_RATE` sang nguồn khác do nguồn cũ không đạt yêu cầu làm đầu vào mô hình.
**Mô hình · Lần chạy · Metric.** Không có.

---

## Tháng 02 — Từ tsfresh sang CNN

**Thay đổi kỹ thuật.**
- Bỏ hướng trích xuất đặc trưng bề rộng bằng `tsfresh` (xóa 6.491 dòng), xây lại toàn bộ luồng quanh mô hình tích chập.
- Sửa lỗi kích thước tensor của dataloader — điểm nghẽn được đánh dấu bằng một commit `CHECKPOINT` riêng.

**Dữ liệu.** VN-Index (giá + lệnh). Bắt đầu tái xây dựng scraper vĩ mô (`macroeconomics_gdp`).

**Mô hình.** CNN 1 chiều chạy trực tiếp trên chuỗi thô, thay cho ma trận đặc trưng tsfresh.

**Các lần chạy & tham số.** Chưa có cơ chế cấu hình hay ghi tham số ra file; mọi tham số nằm trong notebook.

**Kết quả & metric.**
- Output thực tế đầu tiên: **dự báo VN-Index đóng cửa 27/02/2026 = 1843.247**.
- Chưa có metric out-of-sample được lưu lại.

---

## Tháng 03 — Hạ tầng huấn luyện

**Thay đổi kỹ thuật.**
- Dựng **TensorBoard end-to-end**; mỗi lần chạy sinh một thư mục `version_N` riêng.
- `PostgreSQLDriver` hỗ trợ `join_clause`.
- Ghim phiên bản toàn bộ thư viện trong `requirements.txt` để loại trôi lệch môi trường.
- Thêm thông báo Windows khi huấn luyện xong.
- **Khi phát hiện log thiếu metadata → hủy toàn bộ kết quả cũ và chạy lại từ đầu** (xóa 168 file, rồi 152 file).

**Dữ liệu.** Hoàn thiện scraper + ingest + clean + transform cho VN-Index và HNX-Index (giá và lệnh).

**Mô hình.** Vẫn là CNN; 9 biến thể notebook `cnn_vn_index_1…9`.

**Các lần chạy & tham số.**
- Khoảng **80 lần chạy**: `run_version_0…27`, sau đó khởi động lại thành `version_0…53`.
- Hai biến thể được thiết kế để so sánh trực tiếp: *validate sau khi scaling* (`cnn_vn_index_1`) vs *validate trước khi scaling* (`cnn_vn_index_2`).
- Notebook riêng xử lý hiện tượng overshoot khi huấn luyện.

**Kết quả & metric.**
- Metric chỉ tồn tại rời rạc trong TensorBoard của từng run, **chưa được tổng hợp thành bảng so sánh**.
- Chính hạn chế này dẫn tới việc lập `experiment_history.csv` ở tháng 6.

---

## Tháng 04 — Thư viện đặc trưng

**Thay đổi kỹ thuật.**
- Viết **~60 hàm chỉ báo kỹ thuật** (tương đương TA-Lib), mỗi hàm một commit: overlap, momentum, volume, cycle, price transform, volatility.
- Một đợt chuẩn hóa lại toàn bộ chữ ký hàm, sau đó xóa ~1.600 dòng mã cũ.
- Hai đợt tối ưu hiệu năng cho `ta_functions.py`.
- Thêm `SwitchHandler` + `switch_config.json` để bật/tắt từng tác vụ scrape độc lập.
- Scraper VN-Index chuyển sang quét phân trang với hàm chờ theo thay đổi nội dung (thay cho `sleep` cố định).

**Dữ liệu.** Bộ đặc trưng TA đầy đủ sinh từ OHLCV. Hai chỉ báo được khảo sát rồi **loại bỏ có ghi nhận** (`NO ADD: add_hilbert_transform`, `NO ADD: add_mesa_adaptive_moving_average`).

**Mô hình.** Không huấn luyện mô hình mới; tháng này tập trung vào đầu vào.

**Các lần chạy & tham số.** `feature_selection_vn_index.ipynb` — xếp hạng đặc trưng bằng `xgb.XGBRegressor`.

**Kết quả & metric.** Bảng xếp hạng độ quan trọng đặc trưng. **Chưa có metric dự báo out-of-sample.**

---

## Tháng 05 — Kho dữ liệu medallion

**Thay đổi kỹ thuật.**
- Dựng schema **bronze / silver / gold**, sau đó **xóa 3.427 dòng `CREATE TABLE` viết tay** để chuyển sang suy luận schema tự động từ dataframe.
- Đa luồng hóa `PostgreSQLDriver` + thêm `ThreadManager`.
- Bổ sung cho driver: join nhiều cột, `IS / IS NOT NULL`, `IN / NOT IN`.
- Thêm `ipynb_to_txt.py` để review notebook qua diff.

**Dữ liệu.**
- Mở rộng scraper từ chỉ số sang **toàn bộ cổ phiếu HOSE**, rồi dữ liệu cấp doanh nghiệp.
- Scraper vĩ mô làm lại (USD/VND, lãi suất liên ngân hàng), chuẩn hóa theo TradingView.
- Bỏ các cột chỉ có một giá trị duy nhất.
- **Sửa lỗi khoảng train/val/test bị sai** → xóa hơn 316.000 dòng dữ liệu sinh ra không hợp lệ.

**Mô hình.** CNN (tiếp tục).

**Các lần chạy & tham số.**
- `train_test_creator` v2: cửa sổ trượt, chuẩn hóa `y` riêng, tham số **`STRIDE`**, lưu tensor + scaler cho cả 3 tập.
- `data_evaluator` v1 + `result_evaluator` — hoàn thiện nửa đánh giá của vòng lặp.

**Kết quả & metric.**
- **R² dương đầu tiên trên tập test** (commit `0081ebf`).
- ⚠ **Giá trị cụ thể không được ghi lại** — cơ chế log metric có hệ thống mãi tới 14/06 mới có. Kết quả này không tái lập được và **không nên trích dẫn trong luận văn**.

---

## Tháng 06 — Ba nguồn dữ liệu, LSTM, và bước chuyển hướng

**Thay đổi kỹ thuật.**
- Tái cấu trúc scraper thành **`BaseScraper(ABC)` + registry/factory**; thêm nguồn mới chỉ cần một lớp con.
- Tổ chức lại dữ liệu thô theo `raw_data/<source>/`.
- **Tăng tốc ingest tầng gold ~4.5 lần** (68 → 15 phút): thay `pandas.to_csv` bằng `COPY FROM STDIN` + `pyarrow` — profiling cho thấy 88 % thời gian ở khâu insert, chỉ 12 % ở tính toán TA.
- Xử lý **vượt trần 1600 cột của PostgreSQL**: chỉ ghép các mã cùng ngành GICS (621 → 7–49 mã) thay vì toàn thị trường.
- Làm sạch giá trị cho kiểu `REAL`: `±inf` và `>3.4e38` → NaN, số dưới chuẩn `<1e-37` → 0.

**Dữ liệu.**
- Thêm 3 nguồn: **CafeF**, **GICS (MSCI 2023: 11/25/74/163)**, **Simplize** (OHLC điều chỉnh đầy đủ + khối lượng thực + dòng tiền ngoại từ 2009).
- Bật điều chỉnh cổ tức khi scrape TradingView.
- Chọn **Simplize làm nguồn chính** (đã kiểm chứng trên VN30); CafeF bổ sung khớp lệnh/thỏa thuận và sở hữu; TradingView chỉ dự phòng OHLC.
- Bảng `unified_<ticker>` cho **30 mã VN30**: VCB 4.213 dòng × 1.036 cột → sau làm sạch còn 1.015–1.025 cột.
- Biến mục tiêu: `pct_return` với horizon 5 ngày.

**Mô hình.** Chuyển từ CNN sang **LSTM** (PyTorch Lightning). Ba biến thể kiến trúc: `last_hidden` → `bidir_attn` (hai chiều + attention pooling).

**Các lần chạy & tham số** — 9 cấu hình ghi trong `experiment_history.csv`:

| ID | Kiến trúc | Hidden | Lớp | Dropout | Loss | WD | LR | Batch | Epoch | Đặc trưng | Lookback |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| e01 | last_hidden | 128 | 2 | 0.2 | MSE | 1e-5 | 1e-3 | 64 | 100 | 200 | 20 |
| e02 | last_hidden | 48 | 1 | 0.4 | MSE | 1e-3 | 1e-3 | 64 | 100 | 40 | 20 |
| e03 | bidir_attn | 48 | 1 | 0.4 | Huber | 1e-3 | 1e-3 | 64 | 100 | 40 | 20 |
| **e04** | **bidir_attn** | **48** | **1** | **0.4** | **Huber** | **1e-3** | **1e-3** | **64** | **100** | **80** | **20** |
| e05 | bidir_attn | 48 | 1 | 0.4 | Huber | 1e-3 | 1e-3 | 64 | 100 | 120 | 20 |
| e06 | last_hidden | 48 | 1 | 0.4 | BCE | 1e-3 | 1e-3 | 64 | 100 | 40 | 20 |
| e07/e08 | bidir_attn (multitask) | 48 | 1 | 0.4 | Huber+BCE | 1e-3 | 1e-3 | 64 | 100 | 80 | 40 / 20 |
| e09 | LSTM + ticker_emb | — | — | — | — | — | — | — | — | 60 | 20 |

*Chung cho mọi run:* chia 70/15/15 theo thời gian · scaler `std` fit trên train · clip input ±10 · horizon 5 ngày · TA điều chỉnh chu kỳ động (`dynta=yes`).
*e09:* bộ dữ liệu gộp 30 mã VN30 (52k/19k/19k cửa sổ) — **đã dựng xong nhưng chưa huấn luyện**.

**Kết quả & metric.**

| ID | Cấu hình | Train R² | Val R² | Test R² | Corr | Dir acc | Dir AUC |
| --- | --- | --- | --- | --- | --- | --- | --- |
| e01 | Cơ sở | 0.239 | −0.184 | −0.050 | — | 0.551 | — |
| e02 | + regularization | 0.040 | −0.009 | −0.009 | 0.008 | 0.407 | — |
| e03 | + bidir/attn/Huber | 0.057 | −0.022 | −0.012 | 0.058 | 0.464 | — |
| **e04** | **Chuẩn** | **0.065** | **0.003** | **0.007** | **0.141** | **0.483** | **0.505** |
| e05 | 120 đặc trưng | 0.066 | −0.021 | 0.016 | 0.157 | 0.500 | — |
| e06 | Phân loại BCE | — | — | — | — | 0.460 | 0.511 |

> e01 → e02: overfitting được xử lý (khoảng cách train–val từ **0.42 → 0.05**). Nhưng R² test chỉ đi từ −0.050 lên **0.007** — tức vừa đúng bằng dự đoán giá trị trung bình. AUC 0.505 ≈ 0.5: **không phân biệt được ngày tăng và ngày giảm**. e05 có R² test cao nhất (0.016) nhưng val âm → val và test mâu thuẫn, là nhiễu.

**Metric từ các thí nghiệm khảo sát tín hiệu:**

| Thí nghiệm | Nội dung | Kết quả |
| --- | --- | --- |
| exp_1 | GBM dò breakout +5 %/5 ngày (VCB) | **AUC 0.77** (VN30 gộp ≈ 0.65) |
| exp_3 | Backtest walk-forward VCB long/flat | Sharpe **0.67** vs mua-nắm-giữ **0.66** |
| exp_3 | Long/short cross-sectional VN30 | Sharpe **−0.53**, drawdown **−88 %** |
| exp_3 | So sánh 6 biến mục tiêu | Tốt nhất `rel5`, rank IC **0.052** — không cái nào thắng thị trường sau phí |
| — | Cross-sectional VN30 (Ridge, walk-forward 10 năm) | rank IC **+0.03**; sau phí 40 bps: L/S **+6.6 %/năm**, long-only **+3.9 %/năm** |

> **Bước ngoặt:** AUC 0.77 nghe rất cao nhưng backtest cho thấy nó chỉ là **bộ dò chế độ biến động** — Sharpe 0.67 ngang hệt mua-và-nắm-giữ 0.66, không tạo alpha. Kết luận: ràng buộc là *dữ liệu*, không phải mô hình.

---

## Tháng 07 — Dữ liệu trực giao

**Thay đổi kỹ thuật.**
- **`src/model/common/`** — framework huấn luyện tái sử dụng: tham chiếu dataset bằng content-hash, thư mục run bất biến (ghi kèm config + git SHA + môi trường), vòng lặp GPU, early stopping, TensorBoard, checkpoint best/last, bảng xếp hạng chỉ-ghi-thêm `runs/index.csv`.
- Cấu hình chuyển sang **file YAML** thay vì tham số trong notebook → mỗi run tái lập được.
- Pipeline OCR đọc PDF: `cafef_pdf_scraper` → `cafef_pdf_parser` → `cafef_financials`, với kiến trúc **`ParseLayer`** thử lần lượt `onnx@200 → onnx@300 → onnx@400 → tesseract@200 → +relax`.
- Sửa `ThreadManager`: công thức cũ cho ra số luồng thập phân (2.4 trên máy 20 nhân) nên pool luôn chạy ~2 luồng → thêm `max_workers` tường minh (mặc định 16).
- Sửa lỗi `LIKE` trong SQL: ký tự `_` khớp mọi ký tự nên `lb2__%` khớp cả `lb20`.

**Dữ liệu.**
- **BCTC quý bóc tách từ PDF scan tiếng Việt** — 4 hệ thống tài khoản (ngân hàng / doanh nghiệp / chứng khoán / bảo hiểm); ~90 % báo cáo VCB là ảnh scan không có tầng text.
- Tách khóa `EXCHANGE:TICKER` thành 2 cột riêng trên toàn bộ bronze; chuẩn hóa tên cột xuyên suốt.
- `silver.stocks_basic`: **2.388.368 dòng × 38 cột**, 99.7 % số dòng có phân ngành GICS.
- `silver.stocks_basic_financials_bank`: ghép giá ngày × BCTC quý qua `merge_asof` **theo `publish_date`** → **không look-ahead** (4.235 dòng × 216 cột).
- `silver.stocks_basic_financials_bank_fa`: **+26 chỉ số cơ bản** (242 cột).
- Kho tin tức **777 mã, ~405 nghìn dòng**; kho PDF **~97 GB** (VN100).

**Mô hình.**
- LSTM (giữ nguyên kiến trúc từ tháng 6) nhưng **task-aware**: hồi quy (MSE) và phân loại (BCEWithLogitsLoss) dùng chung `model.py`.
- **PhoBERT** 3 lớp cho sentiment tiếng Việt + bộ phân loại text → nhãn phản ứng giá.

**Các lần chạy & tham số** — **27 run** ghi trong `runs/index.csv`:

| Nhóm | Biến mục tiêu | Task | Loss | Lookback | Số đặc trưng | Số run |
| --- | --- | --- | --- | --- | --- | --- |
| (a) | `return_5day` | Hồi quy | MSE | 1,2,3,5,10,15,20,25,30 | 139–156 | 9 |
| (b) | `direction_5day` | Phân loại | BCE | 1,2,3,5,10,15,20,25,30 | 145–167 | 9 |
| (c) | `probability_gain_5pct_5day` | Phân loại | BCE | 1,2,3,5,10,15,20,25,30 | 130–160 | 9 |

*Chung:* VCB · horizon 5 ngày · chia 70/15/15 · scaler `std` · early stopping trên val.
⚠ **`best_epoch` của phần lớn run là 1–5** (chỉ lb30 đạt 25) — mô hình ngừng cải thiện gần như ngay lập tức, một dấu hiệu trực tiếp cho thấy không có gì để học.

**Kết quả & metric.**

**(a) Hồi quy `return_5day`** — chuẩn so sánh: RMSE của dự báo-bằng-0 = **0.035748**

| Lookback | 1 | 2 | 3 | 5 | 10 | 15 | 20 | 25 | 30 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Test RMSE | **0.0368** | 0.0430 | 0.0398 | 0.0374 | 0.0448 | 0.0402 | 0.0425 | 0.0447 | 0.0378 |
| Test AUC | 0.551 | 0.560 | 0.520 | 0.506 | 0.474 | 0.453 | 0.465 | 0.496 | 0.518 |
| Test IC | 0.114 | 0.120 | −0.032 | 0.022 | −0.063 | −0.166 | −0.097 | −0.043 | 0.027 |

→ **0/9 run vượt chuẩn so sánh.** Tốt nhất (lb1) vẫn tệ hơn **2.9 %** so với dự báo hằng số 0. AUC trung bình **0.505**.

**(b) Phân loại `direction_5day`** — tỷ lệ lớp đa số 0.4355

| Lookback | 1 | 2 | 3 | 5 | 10 | 15 | 20 | 25 | 30 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Test AUC | 0.523 | 0.523 | 0.528 | **0.549** | 0.500 | 0.526 | 0.473 | 0.540 | 0.512 |

→ AUC trung bình **0.519** (0.473–0.549). Chỉ 1/9 run vượt lớp đa số, lookback lân cận không xác nhận → nhiễu.

**(c) Phân loại `probability_gain_5pct_5day`** — tỷ lệ nền 0.0708

| Lookback | 1 | 2 | 3 | 5 | 10 | 15 | 20 | 25 | 30 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Test ROC-AUC | 0.641 | 0.411 | 0.539 | 0.518 | 0.518 | **0.655** | 0.501 | 0.643 | 0.477 |
| Test PR-AUC | 0.129 | 0.089 | 0.103 | 0.118 | 0.097 | 0.120 | 0.079 | 0.110 | 0.108 |

→ ROC-AUC trung bình **0.545**, PR-AUC trung bình **0.106** (nền 0.071). Nhìn qua có vẻ khá hơn (b), **nhưng val và test nghịch nhau**: lb15 có AUC val tệ nhất (0.332) lại cho AUC test tốt nhất (0.655) → không tái lập được.

**Cross-sectional:**

| Nội dung | Kết quả |
| --- | --- |
| Bộ phân loại tốt nhất ngoài mẫu (XGB, mục tiêu trung hòa ngành) | AUC **0.52** toàn rổ / **0.53** tercile |
| Sharpe gộp long/short | **1.3 – 1.6** |
| Vòng quay | **65–78 %/vế/tuần** → triệt tiêu lợi nhuận |
| Ròng @20 bps sau kiểm soát vòng quay | **+0.46** — nhưng **+1.46** (2017-20) vs **−0.51** (2022-26) |
| Riêng VCB | AUC tương đối ngành **0.491**, tuyệt đối **0.518**, Sharpe ròng **−0.12** |

**Sentiment:**

| Bài toán | Kết quả |
| --- | --- |
| Nhãn suy ra từ chính văn bản | QWK **0.61** ✓ |
| `close[N+5]` | **0/7** lần vượt bước ngẫu nhiên |
| Hướng giá | AUC ≈ **0.49** |
| Xác suất tăng ≥5 % (nền 11.3 %) | AUC ≈ **0.50** |
| Nhãn theo phản ứng giá 5 mức | QWK ≈ **0.00** |
| **Thêm sentiment lên trên giá/TA** | AUC **0.543 → 0.534**; QWK **0.175 → 0.045** ✗ **làm xấu đi** |

**Bóc tách BCTC:**

| Mã | Khoảng | Số ô | Đọc được từ PDF |
| --- | --- | --- | --- |
| VCB | Q3-2008 → Q1-2026 (71 quý) | 213 | **213/213 (100 %)** |
| ACB | Q1-2010 → Q1-2026 (65 quý) | 195 | **195/195 (100 %)** |

Kiểm chứng chỉ số cơ bản VCB kỳ mới nhất: **P/E 14.13 · P/B 2.56 · ROE 22.2 % · NIM 2.69 %**.

---

## Tổng hợp: mô hình hiện đang có gì

| Bài toán | Metric tốt nhất | Chuẩn so sánh | Kết luận |
| --- | --- | --- | --- |
| Lợi suất 5 ngày (VCB) | R² **0.007**, RMSE 0.0368 | R² 0, RMSE 0.0357 | ✗ Không vượt |
| Hướng giá 5 ngày (VCB) | AUC **0.549** | 0.50 | ✗ Nhiễu |
| Xác suất tăng ≥5 % (VCB) | AUC **0.655** | 0.50 | ✗ Không tái lập |
| Sentiment → giá | AUC **0.49–0.50** | 0.50 | ✗ Bằng ngẫu nhiên |
| Breakout GBM (VCB) | AUC **0.77** | 0.50 | ⚠ Cao nhưng Sharpe 0.67 ≈ mua-nắm-giữ |
| **Cross-sectional VN30** | **rank IC +0.03, L/S +6.6 %/năm** | thị trường | **✓ Dương ròng duy nhất** |

**Diễn giải.** Với dữ liệu giá + chỉ báo kỹ thuật + sentiment, mọi bài toán ở cấp **một cổ phiếu đơn lẻ** đều cho AUC trong khoảng 0.47–0.55 — gần như không phân biệt được gì so với tung đồng xu. Trường hợp duy nhất có AUC cao (0.77) hóa ra là dò *chế độ biến động* chứ không phải dò *hướng giá*, và backtest chứng minh điều đó. Kết quả dương duy nhất nằm ở **cấp danh mục**, và ngay cả nó cũng suy giảm rõ sau 2021.

Kết luận này được rút ra từ nhiều góc độ độc lập — nhiều kiến trúc (LSTM/GRU/CNN/Transformer/GBM), nhiều biến mục tiêu (lợi suất, hướng, ngưỡng, tương đối), nhiều lookback (1→30), nhiều cách chia dữ liệu, có tính chi phí giao dịch — chứ không phải từ một thí nghiệm đơn lẻ. **Đó là giá trị học thuật của luận văn: một kết quả phủ định được chứng minh chặt chẽ.**

Hạ tầng dữ liệu cơ bản xây trong tháng 7 (BCTC, ngày công bố, số cổ phiếu, tin tức) là để mở đường cho hướng chưa được kiểm chứng: **liệu dữ liệu cơ bản có bổ sung thông tin mà giá không có.**

---

## Việc còn dở dang

1. **Mô hình gộp 30 mã VN30 chưa huấn luyện** — dữ liệu đã dựng xong (52k/19k/19k cửa sổ, dòng `e09`), chỉ còn chạy. Đây là thí nghiệm rẻ nhất còn lại.
2. **Chưa mô hình nào dùng dữ liệu cơ bản** — bảng 26 chỉ số FA vừa xong tháng 7, chưa vào thí nghiệm nào.
3. **Kiểm thử hồi quy 2 sửa lỗi OCR cuối chưa hoàn tất** (~50–60 phút) — cần chạy trước khi dùng parser ngoài phạm vi ACB.
4. Mới bóc tách 2 mã (VCB, ACB) và 1 trong 4 template kế toán; bảng FA hiện chỉ có VCB.
5. Thí nghiệm sentiment mới chạy trên 3 mã.
