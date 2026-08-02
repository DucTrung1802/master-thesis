# Guidance — Xây dựng luồng NLP sentiment cho tin tức CafeF

> Tài liệu kế hoạch triển khai. Ba nguồn: **23 paper** đã phân tích trong
> [CONTEXT.md](CONTEXT.md) · **đo đạc thực tế** trên `raw_data/cafef/news/` (777 file,
> 405.320 dòng, quét ngày 2026-08-03) · **hiện trạng** của
> [`src/sentiment`](../../src/sentiment/CONTEXT.md) và
> [`src/orchestration`](../../src/orchestration/CONTEXT.md).
>
> CONTEXT.md trả lời *"literature nói gì"*. File này trả lời *"vậy phải gõ những gì,
> ở đâu, theo thứ tự nào"*.

---

## 0. ⚠️ ĐỌC TRƯỚC — đây KHÔNG phải làm mới từ đầu

`src/sentiment/` **đã tồn tại và đã chạy xong toàn bộ thí nghiệm này**, và kết quả là một
**negative result có bằng chứng tốt**. Trích `src/sentiment/CONTEXT.md` §6a:

| Target | features | metric | value | kết luận |
|---|---|---|---:|---|
| direction (5d) | sentiment-only | ROC-AUC | 0.482 | dưới 0.5 |
| direction (5d) | **price/TA-only** | ROC-AUC | **0.543** | có tín hiệu yếu |
| direction (5d) | price + sentiment | ROC-AUC | 0.534 | **tệ hơn chỉ dùng price** |
| 5-level reaction | text-emb-only | QWK | −0.012 | ≈ 0 |
| 5-level reaction | **price/TA-only** | QWK | **0.175** | tín hiệu yếu, thật |
| 5-level reaction | text + price | QWK | 0.045 | **sụp so với price alone** |

**Nhưng** chính file đó, §7, liệt kê 5 "next moves" và đặt cái này lên hàng thứ 3:

> *"**More tickers** — the news scraper covers only 3; breadth is the biggest lever."*

Và §0: *"⚠️ **Only 3 tickers have news** (VCB/FPT/PNJ) — too few for a real
market/sentiment relationship; the negative result is about *this* data."*

**Đòn bẩy đó vừa được giao.** Ngày 2026-08-01, `bronze.cafef_news` nhảy từ **5.599 → 405.320
dòng**, từ **3 → 777 mã** (`orchestration/CONTEXT.md` §"Phase 1a"). Kèm theo là 4 next-move
còn lại vẫn còn nguyên giá trị.

> **→ Nhiệm vụ không phải "xây mô hình sentiment". Nhiệm vụ là: chạy lại thí nghiệm đã
> thất bại, ở độ rộng gấp 70 lần, với ba sửa chữa cụ thể mà chính module đó đã chỉ ra.**

Ba sửa chữa, theo thứ tự quan trọng:

| # | Sửa gì | Vì sao | Nguồn |
|---|---|---|---|
| 1 | **777 mã thay vì 3** | đã có sẵn dữ liệu, chỉ cần chạy lại | `sentiment/CONTEXT.md` §7.3 |
| 2 | **Mục tiêu cắt ngang (`rel`), không phải giá tuyệt đối** | experiment_3.3 đã chốt; paper 57 là paper duy nhất làm long–short cắt ngang | §7.2 + paper 57 |
| 3 | **Scorer hiệu chỉnh tài chính, thay `mr4/phobert-base-vi-sentiment-analysis`** | model hiện tại chấm *"VCB: chi trả cổ tức 2025"* = **−0.97** | §7.4 + paper 45, 53 |

Và một sửa chữa thứ tư mà module chưa nêu, đến từ chính dữ liệu:

| 4 | **Tần suất TUẦN, không phải NGÀY** | độ phủ editorial theo ngày chỉ **1,6%** panel; theo tuần **8,7%** (top-30: 12,2% → **51,7%**) | đo đạc §2 + paper 57 |

---

## 1. Hiện trạng — cái gì đã có, cái gì chưa

### 1.1 Bảng dữ liệu sẵn dùng

| bảng | shape | dùng làm gì trong kế hoạch này |
|---|---|---|
| `bronze.cafef_news` | **405.320 × ~11**, 777 mã, 2007-02 → 2026-07 | **nguồn text duy nhất** |
| `silver.stocks_basic` | 2.388.368 stock-days × 38, **781 mã** | `close_adjust`, volume, foreign flow, **cây GICS** (→ scope ngành) |
| `gold.stock_market` | 6.339 phiên × 162, 6 chỉ số | **VNINDEX** → benchmark cho lợi suất vượt trội |
| `gold.economy` | 6.935 ngày × 1.034 series | khối **vĩ mô**, đã as-of theo publication lag (chống look-ahead sẵn) |
| `silver.cafef_news_sentiment` | ⚠️ **stale** — dựng khi news còn 3 mã | phải build lại |

`bronze.cafef_news` schema: `row_id` (md5 của `exchange|ticker|url`) · `exchange` · `ticker` ·
`news_order` · `timestamp` · `type` · `category` · `headline` · `content` · `url` · `pdf_url`.

### 1.2 Module đã có, tái dùng được nguyên vẹn

| file | hàm | trạng thái |
|---|---|---|
| `sentiment_features.py` | **`purged_walkforward_folds`** | ⭐ **giữ nguyên** — purge + embargo H ngày, đã đúng |
| `sentiment_features.py` | `build_event_panel` | mở rộng: ngày → tuần, + scope hierarchy |
| `sentiment_functions.py` | `VietnameseSentimentModel`, `score_news_frame` | giữ interface, **đổi checkpoint** |
| `sentiment_functions.py` | `build_scored_text` | mở rộng: thêm relevance + segmentation |
| `price_reaction_labels.py` | `exchange_limit`, `build_price_reaction_labels` | giữ — dùng cho nhánh thí nghiệm paper-63 |
| `text_reaction_model.py` | `embed_texts` | giữ cho nhánh 63; **không dùng cho Model 2** (xem §6.3) |
| `jump_predictor.py` / `price_predictor.py` | `evaluate`, `format_report` | khuôn mẫu ablation + baseline, sao chép cấu trúc |

### 1.3 ⚠️ Ba thứ CHƯA có và phải biết trước khi lập kế hoạch

1. **Không có bảng silver/gold nào cho news.** `bronze.cafef_news` là *"event-based, do
   **not** 1:1-join onto a daily row — wiring them into a signal is future work"*
   (`data_preprocessor/CONTEXT.md`). Toàn bộ §4 dưới đây là "future work" đó.

2. **⚠️ Khối "phân tích cơ bản" CHỈ CÓ 2 MÃ.** `gold.stocks_financials_bank_fa` = 8.265
   stock-days, **VCB + ACB**, và chỉ cho template ngân hàng. `gold.stocks` thì
   *"stale AND raises"*. **Nên khối FA phải bị loại khỏi v1** — không tồn tại ở độ rộng
   panel. Đưa nó vào chỉ khi chain financials đã chạy cho ≥30 mã.

3. **Scorer hiện tại sai về mặt tài chính.** Không phải lỗi code — là domain mismatch,
   đúng như paper **53** đo (LMFinance thắng SenticNet/SentiWordNet/Vader trên 12 mã × 3
   model) và paper **45** đo (FinBERT 86% vs VADER 54% trên văn bản tài chính).
   **Bằng chứng nội bộ mạnh hơn cả hai paper**: model hiện tại chấm một thông báo chi trả
   cổ tức là −0.97, và chấm 51% editorial là VERY_NEGATIVE khi đọc full content.

---

## 2. Số liệu đo được trên corpus (2026-08-03)

Mọi con số dưới đây quét trực tiếp từ 777 file CSV. **Chúng là ràng buộc thiết kế, không
phải thống kê mô tả.**

### 2.1 Corpus tách làm hai, và chỉ một nửa là văn bản

| type | rows | content TB | thực chất |
|---|---|---|---|
| `disclosure` | **326.722** | 288 ký tự | boilerplate công bố + link PDF — **không phải văn bản** |
| `editorial` | **78.017** | 2.687 ký tự | báo chí thật — **210 triệu ký tự tiếng Việt** |
| `error` | 581 | 101 ký tự | **loại bỏ** |

→ **Corpus để chấm sentiment là 78.017 văn bản.** 326.722 disclosure là một **lịch sự kiện**
(giá trị riêng, xem §4.4) chứ không mang sắc thái.

### 2.2 ⚠️ Độ phủ — con số quyết định toàn bộ thiết kế

Từ 2015 (4.162 phiên, 604 tuần, 777 mã), tỷ lệ ô panel có ≥1 tin:

| | ticker-**ngày** | ticker-**tuần** |
|---|---|---|
| 777 mã, editorial | **1,60%** | **8,67%** |
| 777 mã, mọi type | 7,85% | 36,81% |
| **top-30 mã, editorial** | **12,2%** | **51,7%** |

**Feature sentiment theo NGÀY sẽ NaN 98,4% số dòng.** GBM sẽ bỏ qua nó — không phải vì vô
dụng mà vì gần như không tồn tại. Chỉ ô "tuần × nhóm thanh khoản" là feature sống được.

Paper **57** đến cùng kết luận bằng đường khác: tin theo ngày dự báo được 1–2 ngày
(Ngày 3: t = 1,2, hết), tin theo tuần dự báo **13 tuần** (+2,15%, t = 8,2), vì phân vị
sentiment cắt ngang theo ngày quá bất ổn để xếp hạng.

### 2.3 Phân bố theo mã — đuôi rất dài

| | editorial/mã |
|---|---|
| p25 | 21 |
| **p50** | **45** |
| p75 | 104 |
| p90 | 238 |
| max | 1.389 (`HOSE_VIC`) |

**3 mã có 0 editorial · 72 mã có <10 · 411 mã có <50.** Top-20: VIC 1389, HPG 1277,
MWG 1111, HAG 1098, FPT 1060, VNM 981, NVL 954, VCB 902, STB 894, MSN 747, CTG 743…

→ **Universe cho v1 phải là VN100 hoặc top-100 theo editorial count**, không phải cả 777.

### 2.4 Ba vấn đề chất lượng phải xử ở silver

| vấn đề | số đo | xử ở đâu |
|---|---|---|
| **timestamp chỉ có ngày** (`00:00:00`) | **89.698 dòng = 22,2%** — nhưng **89.639 là disclosure, chỉ 59 là editorial**; tập trung 2023–2026 | §4.2, quy tắc riêng |
| **65,5% bài đăng ngoài 09:00–15:00 ICT**, đỉnh 17h | đo trên VCB; toàn corpus đỉnh 17h (54.655) rồi 16h (43.407) | §4.2, căn 09:00→09:00 |
| **lỗ hổng 2012** | 3.781 dòng, so với 11.318 (2011) và 15.703 (2013) | kiểm tra trước khi chia fold |

Trùng lặp thì **không đáng lo**: 382.829 URL riêng biệt / 405.320 dòng = **1,06×**; chỉ
7.343 URL (1,9%) gắn >1 mã. → Cosine dedup của paper 58 gần như không cần; chỉ cần dedup
theo `(headline, ngày)` (riêng file VCB đã có 28 headline trùng dù 0 URL trùng).

### 2.5 Mật độ toàn thị trường — đủ dày cho scope "market"

| năm | bài riêng biệt | /phiên |
|---|---|---|
| 2015 | 17.727 | 70,9 |
| 2020 | 30.192 | 120,8 |
| 2025 | 34.172 | **136,7** |

→ Scope **market** và **sector** của paper 43 hoàn toàn khả thi. Chỉ scope **stock** là thưa.

---

## 3. Các quyết định đã chốt, và paper nào chống lưng

| # | Quyết định | Paper | Ghi chú |
|---|---|---|---|
| 1 | **Hai tầng, hợp nhất muộn** — text encoder riêng, nối vào GBM | **28**, **51** | không end-to-end |
| 2 | **Nhãn scorer từ annotation, KHÔNG từ giá** | **54** (vòng lặp từ điển), **46** (vòng lặp từ giá), **63** | xem §5.4 — quyết định quan trọng nhất |
| 3 | **Đơn vị = (mã, TUẦN)** | **57** + đo đạc §2.2 | không phải (mã, ngày) |
| 4 | **Mục tiêu = lợi suất tương đối cắt ngang** | **57** + experiment_3.3 | không phải giá tuyệt đối |
| 5 | **Nhãn theo phân vị**, ngưỡng > chi phí khứ hồi | **53**, **56** | tỷ lệ nền biết trước |
| 6 | **Giữ softmax 3 chiều**, không gộp thành 1 số | **48**, **56** | 56: full dict 0,55 vs polarity ratio 0,22–0,28 |
| 7 | **Trung bình, không tổng**; `n_docs` là feature riêng | **9** (lỗi), **45** (sửa) | tổng đo *khối lượng*, không đo sắc thái |
| 8 | **Căn cửa sổ 09:00→09:00 ICT**, lag `d+1` | **45**, **59** | 65,5% bài ngoài giờ giao dịch |
| 9 | **Phân rã theo scope** market/sector/stock | **43** | sector = cây GICS đã có trong `stocks_basic` |
| 10 | **`If_news` / `Positive` / `Negative` tách riêng** | **57** | mã được đưa tin ≠ mã không, bất kể sắc thái |
| 11 | **Kiểm soát momentum** | **57** | chiến lược news của họ tương quan **0,80** với momentum |
| 12 | **Dùng `content`, không phải headline** | **51**, **63** | 51: MCC 0,069 vs −0,023 |
| 13 | **Tách từ (VnCoreNLP/underthesea) trước encoder** | **46** | bắt buộc với PhoBERT |
| 14 | **Vai trò: EXCLUSION, không phải predictor** | **51**, **55**, **57** | HOSE không short được single-stock |
| 15 | **Cổng LIME/SHAP trước mọi backtest** | **61** | bắt được cả lỗi 54 lẫn 52 |
| 16 | **MCC + Brier + baseline, walk-forward có phí** | **51**, **62** | accuracy một mình vô nghĩa |

---

## 4. Luồng dữ liệu: bronze → silver → gold

Theo đúng convention của repo: **pure functions ở `src/sentiment`, DB logic ở
`src/data_preprocessor`, asset ở `src/orchestration/assets`** (precedent: `src/ta`).

```
bronze.cafef_news  (405.320, KHÔNG ĐỤNG VÀO)
      │
      ├─► silver.cafef_news              ← làm sạch + căn phiên + relevance
      │        │
      │        └─► silver.cafef_news_sentiment   ← chấm điểm (1 dòng / row_id)
      │                     │
      │                     ▼
      │        gold.news_weekly_panel     ← (mã, tuần) × ~40 feature
      │                     │
silver.stocks_basic ────────┤   (close_adjust, GICS tree, volume)
gold.stock_market ──────────┤   (VNINDEX → benchmark)
gold.economy ───────────────┘   (vĩ mô, as-of)
                             │
                             ▼
                   Model 2 (GBM cắt ngang)
```

### 4.1 BRONZE — không đụng vào

Bronze trung thành với đĩa. Mọi làm sạch thuộc silver. (Quy tắc này đã có trong repo:
*"Bronze is faithful to disk; don't 'fix' it here."*)

### 4.2 ⭐ SILVER 1 — `silver.cafef_news` (bảng MỚI)

**Grain:** 1 dòng / `row_id`. **Chỉ lọc và thêm cột — không gộp.**

| bước | làm gì | căn cứ |
|---|---|---|
| 1 | **Loại `type = 'error'`** (581 dòng) và `content` rỗng | §2.1 |
| 2 | **Dedup theo `(ticker, ngày, headline chuẩn hoá)`** | §2.4 |
| 3 | **Bóc boilerplate**: `Normal 0 false false false EN-US X-NONE … MicrosoftInternetExplorer4`, `- File đính kèm: *.pdf`, `Theo HOSE`, `Hose` | quan sát trực tiếp trên disclosure |
| 4 | **Sửa timestamp**: cột `ts_resolved` + cờ `ts_is_date_only` | §2.4 — 89.698 dòng |
| 5 | **⭐ Căn phiên**: `trading_date` = phiên đầu tiên **mở cửa sau** `ts_resolved` (09:00 ICT) | paper 45, 59 |
| 6 | **`relevance_score`**: số lần mã/tên công ty xuất hiện trong `headline+content`, chuẩn hoá theo độ dài | paper **49** (cờ `label` giảm MAPE), **57** (lọc relevance <35%) |
| 7 | **`text_for_scoring`**: `headline` + `[SEP]` + lead của `content`, đã **tách từ** | quyết định 12, 13 |
| 8 | **`is_editorial`** boolean | §2.1 |

**⚠️ Quy tắc timestamp chỉ-có-ngày** (89.698 dòng): với `disclosure`, gán `trading_date` =
**phiên kế tiếp** (giả định công bố sau giờ) — thận trọng, không bao giờ nhìn trước. Với
59 editorial thiếu giờ: xử lý y hệt. **Không bao giờ gán vào phiên cùng ngày** — đó chính là
lỗi làm hỏng paper **46**, **47**, **50**.

**Module mới:** `src/sentiment/news_clean.py` — pure functions.
**DB:** `data_preprocessor._ingest_silver_cafef_news` (mới).
**Asset:** `silver/cafef_news` trong `assets/silver.py`, dep `bronze/cafef_news` +
`silver/stocks_basic` (cần lịch phiên).

**Invariant phải assert:** row count ≤ bronze; mọi `trading_date` phải là phiên có thật;
**0 dòng có `trading_date` ≤ ngày của `ts_resolved` khi bài đăng sau 15:00.**

### 4.3 SILVER 2 — `silver.cafef_news_sentiment` (đã có, BUILD LẠI)

**Grain:** 1 dòng / `row_id`. Cột: `prob_negative`, `prob_neutral`, `prob_positive`,
`sentiment_label`, `model_version`.

Thay đổi so với bản hiện tại:
- đọc từ `silver.cafef_news` (không phải bronze),
- checkpoint = model đã fine-tune (§5), **`model_version` phải đổi**,
- **giữ nguyên 3 xác suất** — không ghi `sentiment_score = p(pos) − p(neg)` làm cột chính
  nữa (quyết định 6; paper **56** đo được việc gộp làm sập từ 0,55 xuống 0,22–0,28).

**Chi phí:** model hiện tại chạy ~500 text/s trên RTX 3050 → 78k editorial ≈ **3 phút**.
Kể cả chấm cả 405k dòng cũng chỉ ~13 phút.

### 4.4 ⭐ GOLD — `gold.news_weekly_panel` (bảng MỚI)

**Grain:** 1 dòng / `(exchange, ticker, iso_week)`. **Đây là feature block đưa vào Model 2.**

Cửa sổ hình thành: **09:00 ICT phiên đầu tuần `w` → 09:00 ICT phiên đầu tuần `w+1`**.

| nhóm | cột | nguồn |
|---|---|---|
| **stock** (`s_*`) | `if_news`, `n_docs`, `n_days`, `pos_mean`, `neu_mean`, `neg_mean`, `pos_max`, `neg_max`, `pos_lenw`, `neg_lenw`, `relevance_mean` | editorial gắn mã, đã lọc relevance |
| **sector** (`k_*`) | `if_news`, `n_docs`, `pos_mean`, `neu_mean`, `neg_mean` | editorial của các mã cùng **GICS sub-group**, **loại chính nó** |
| **market** (`m_*`) | `n_docs`, `pos_mean`, `neu_mean`, `neg_mean` | toàn bộ editorial trong tuần |
| **event** | `n_earnings`, `n_insider_txn`, `n_dividend`, `n_personnel`, `n_capital`, `if_earnings_week` | **disclosure** theo `category` |
| **cross-sectional** | `xs_pos_rank`, `xs_neg_rank`, `xs_ndocs_rank`, `resid_pos`, `resid_neg` | rank trong VN100 của tuần đó; resid = `s_ − k_ − m_` |

- **Trung bình, không tổng** — `n_docs` là cột riêng (quyết định 7).
- **Cây GICS lấy từ `silver.stocks_basic`** (đã có sẵn 6 cột GICS trong 38 cột).
- **Chuẩn hoá Z-score theo từng cửa sổ**, không dùng thống kê toàn cục (paper **53** Eq. 7).
- **`s_if_news = 0` giữ nguyên dòng, các cột sentiment = NULL.** Không được drop —
  paper **28** drop ngày không tin và làm đứt chuỗi; và `if_news = 0` **chính là tín hiệu**
  paper **57** đo (mã có tin vượt mã không tin **2,24%/tuần** ở nhóm vốn hoá nhỏ).

**Module mới:** `src/sentiment/news_panel.py`.
**DB:** `data_preprocessor._ingest_gold_news_weekly_panel`.
**Asset:** `gold/news_weekly_panel`, deps: `silver/cafef_news_sentiment`, `silver/stocks_basic`.

**Invariant:** assert grain — 1 dòng/(mã, tuần), như `gold/stocks_financials_bank_fa` đã làm
(*"it fails if its row count differs from silver's"*).

---

## 5. Mô hình NLP

### 5.1 Đặt ở đâu

| thứ | đường dẫn | lý do |
|---|---|---|
| **pure functions** | `src/sentiment/` | precedent đã có; DB-agnostic, import torch lazily |
| — làm sạch | `src/sentiment/news_clean.py` | **mới** |
| — chọn mẫu + gán nhãn LLM | `src/sentiment/annotation.py` | **mới** |
| — fine-tune | `src/sentiment/scorer_finetune.py` | **mới** |
| — chấm điểm | `src/sentiment/sentiment_functions.py` | **sửa** — đổi checkpoint |
| — cổng giải thích | `src/sentiment/explain.py` | **mới** |
| — gộp panel | `src/sentiment/news_panel.py` | **mới** |
| **checkpoint model** | `src/model/runs/news_sentiment/<version>/` | theo precedent `src/model/runs/` |
| **tập nhãn** | `raw_data/annotation/news_sentiment/` | dữ liệu vào, versioned, **phải commit** |
| **DB ingest** | `src/data_preprocessor/data_preprocessor.py` | precedent: *"orchestration/DB in data_preprocessor"* |
| **asset** | `src/orchestration/assets/{silver,gold}.py` | selection = run plan |

⚠️ **Tập nhãn vàng phải nằm trong git.** Nó là input được curate, không phải thứ một lần
chạy sinh lại — cùng lý do 12 file chart-of-accounts được track
(`raw_data/cafef/financials/` là ngoại lệ duy nhất trong `.gitignore`). Mất nó là mất khả
năng tái lập.

### 5.2 Đầu vào

```
text = tách_từ( headline + " [SEP] " + lead_200_token(content) )
       → PhoBERT tokenizer, max_length = 256
```

- **Tách từ bằng VnCoreNLP hoặc underthesea** trước — PhoBERT bắt buộc (paper **46**).
- **`content`, không phải headline** (quyết định 12). PhoBERT chỉ nhận 256 token còn
  editorial trung bình ~700+, nên ba biến thể phải thử như **ablation**:
  1. `headline + lead 200 token` ← baseline
  2. **chấm từng câu → trung bình** ← đúng quy trình paper **51**
  3. `headline-only` ← để **chứng minh nó tệ hơn**

⚠️ Ghi chú từ `sentiment/CONTEXT.md` §1-bis: sweep content-vs-headline đã chạy một lần —
content-only đẩy QWK lên 0,75 **nhưng lệch nhãn về phía tiêu cực**; `headline+content` là
lựa chọn cân bằng. Đó là với model general-domain; **phải chạy lại sau khi fine-tune.**

### 5.3 Đầu ra

```
softmax 3 chiều:  P(negative), P(neutral), P(positive)
```

**Ba số, giữ nguyên, không gộp** (quyết định 6).

**Vì sao 3 chứ không 5:** nhãn đến từ người/LLM, và mức đồng thuận giữa người gán về ranh
giới *"tích cực"* vs *"rất tích cực"* rất thấp. Financial PhraseBank (16 người gán, 4.846
câu) là 3 mức; FinBERT trong paper **45** và **51** đều 3 mức.

> **Quy tắc:** nhãn do **người gán** → **3 mức**. Nhãn từ **phân vị phản ứng giá** → 5 mức
> được, vì ranh giới do *xây dựng* chứ không do *phán đoán* (paper **53**).

5 mức chỉ dùng ở **nhánh thí nghiệm paper-63** (§6.3), nơi `price_reaction_labels.py` đã có
sẵn 5 mức exchange-aware (HOSE ±7%, HNX ±10%, UPCoM ±15%).

### 5.4 ⭐⭐ CÓ CẦN GÁN NHÃN THỦ CÔNG KHÔNG? — **CÓ, nhưng chỉ 600–800 câu**

**Đây là quyết định quan trọng nhất trong toàn bộ tài liệu.** Trả lời rõ:

| việc | ai làm | số lượng |
|---|---|---|
| Gán nhãn tập huấn luyện | **LLM** (Claude, few-shot, prompt tiếng Việt) | **~5.000 câu** |
| **Gán nhãn tập vàng** | **⭐ CHÍNH BẠN, bằng tay** | **600–800 câu** |
| Đo đồng thuận LLM ↔ người | tự động | trên tập vàng |

**Vì sao KHÔNG thể bỏ bước thủ công — ba lý do, lý do thứ ba là mạnh nhất:**

1. **Paper 54 — vòng lặp từ điển.** Gán nhãn tự động bằng từ điển rồi train model trên
   chính văn bản đó chỉ đo lại từ điển. Accuracy 90% của họ đo *"model tái tạo lại quy tắc
   đã sinh ra nhãn của nó tốt đến đâu"*. Không có Financial PhraseBank tiếng Việt, nên đây
   là cái bẫy đang chờ sẵn.

2. **Paper 45 và 51 là hai paper duy nhất trong 23 paper kiểm định scorer trước khi dùng** —
   và cũng là hai paper đáng tin nhất. 45 đo VADER 68% trên tweet / 54% trên tin, FinBERT
   53% / **86%**. 51 báo FinBERT accuracy **0,836** / weighted F1 **0,837** trên PhraseBank
   held-out. Paper **52** cho thấy hậu quả khi bỏ bước này: bốn scorer chưa kiểm định tạo ra
   ma trận tương quan **ngược dấu nhau** trên cùng dữ liệu.

3. **⭐ Bằng chứng nội bộ, mạnh hơn cả hai paper trên.** Scorer hiện tại của repo chưa từng
   được kiểm định, và nó **sai theo cách có hệ thống**: chấm *"VCB: …chi trả cổ tức 2025…"*
   = **−0,97**, chấm hồ sơ *"an toàn tài chính"* là tích cực, và đẩy **51% editorial** về
   VERY_NEGATIVE khi đọc full content. Nếu không có tập vàng, **không có cách nào biết
   điều đó** — và mọi kết quả downstream sẽ mang lỗi này mà không ai phát hiện.

**Quy trình gán nhãn thủ công:**

- **Cấp độ câu**, không phải cấp bài.
- **Rubric:** gán theo *"tác động lên giá cổ phiếu của công ty này, dưới góc nhìn nhà đầu
  tư"* — **không phải sắc thái ngôn ngữ chung**. Đây chính là chỗ model hiện tại sai: "chi
  trả cổ tức" trung tính/tích cực về tài chính nhưng model đọc ra tiêu cực về ngôn ngữ.
- **Phân tầng mẫu:** theo năm, sàn, `category`, độ dài. Bắt buộc có đủ 6 category — nhất là
  `dividends_and_record_date` (19.008 dòng) và `capital_increase_and_treasury_shares`
  (17.044 dòng), hai nhóm mà scorer hiện tại sai nặng nhất.
- **Ngưỡng chấp nhận:** đồng thuận LLM ↔ người **≥ 80%**. Dưới ngưỡng → sửa **prompt**,
  không phải sửa model.
- **Chi phí thực tế:** ~600–800 câu ≈ **1–2 ngày công**. Đây là khoản đầu tư rẻ nhất và có
  lợi suất cao nhất trong cả kế hoạch.

**Fine-tune:** PhoBERT-base trên 5.000 nhãn LLM, đánh giá trên tập vàng người gán, báo cáo
**accuracy + macro-F1** đúng như cách paper 51 báo cáo. Kỳ vọng **0,80–0,86**.

### 5.5 ⚠️ Cổng LIME/SHAP — chạy TRƯỚC mọi backtest

`src/sentiment/explain.py`. Paper **61** là paper duy nhất trong 23 paper hỏi *"token nào
đang lái dự đoán"*, và câu trả lời phơi bày mô hình: trọng số âm lớn nhất là **"SBI"** —
tên một ngân hàng — và **"Surge"** bị gán **sai dấu**. Nhóm tác giả trình bày đó như một
thành công.

**Tiêu chí trượt:** nếu top-20 token trọng số cao là **tên mã, tên công ty, ngày tháng,
câu khuôn mẫu hoặc stopword**, thì feature là **ghi nhớ, không phải tín hiệu**. Với tiếng
Việt, token đáng ngờ: *"Vietcombank"*, *"HOSE"*, *"quý 3"*, *"theo đó"*, *"ngày"*, *"Ngân
hàng TMCP"*.

**Một bước kiểm tra này bắt được cả hai lỗi của paper 54 (vòng lặp nhãn) và 52 (tool bất
đồng).** Tốn một buổi chiều.

---

## 6. Model 2 — tầng dự báo

### 6.1 Một mẫu huấn luyện

**Đơn vị = (mã, tuần hình thành).** Vị thế mở tại phiên đầu tuần `w+1`.

```
X = gold.news_weekly_panel   (~40 cột, §4.4)
  + price/TA từ silver.stocks_basic
  + vĩ mô từ gold.economy    (đã as-of theo publication lag)
  + controls: mom_12w, mom_1w, log_mktcap, turnover      ← paper 57, BẮT BUỘC
  ⚠️ KHÔNG có FA — chỉ có 2 mã (§1.3)

y = rel_h(i,w) = r_i(w+1…w+h) − r_VNINDEX(w+1…w+h),  h ∈ {1,2,4,8,13} tuần

  3 lớp theo phân vị cắt ngang trong tuần đó:
    2 "long"     rel_h > p75
    1 "neutral"  p25 ≤ rel_h ≤ p75     ← tỷ lệ nền 25/50/25, biết trước
    0 "avoid"    rel_h < p25
```

⚠️ **Ranh giới p75 phải > chi phí khứ hồi** (VN ~0,4–0,8%). Tuần nào không đạt → báo cáo là
tuần không giao dịch được, **không gán nhãn bừa** (paper **56**).

⚠️ **`mom_12w` không phải tuỳ chọn.** Chiến lược news của paper 57 tương quan **0,80** với
momentum. Không có nó thì không trả lời được câu hỏi đầu tiên của người phản biện.

### 6.2 Mô hình

**GBM hiện có + khối text.** Experiment 1.6, 1.7, 2.1, 2.3 đã cho thấy sequence model thua
point-in-time GBM trên chính panel này. Dùng `src/model/cross_sectional/`.

### 6.3 ⚠️ TUYỆT ĐỐI KHÔNG làm hai việc này

**(a) Không đưa embedding 768 chiều vào Model 2.** Đây là lỗi đã đo được, không phải giả
định: `sentiment/CONTEXT.md` §6a — *"the 768-dim text embedding swamps the 14 price
features: textbook noise-feature degradation"*, QWK **0,175 → 0,045**. **Chỉ đưa ~40 feature
đã gộp của `gold.news_weekly_panel`.**

**(b) Không nối nhánh "gán nhãn theo phản ứng giá" vào Model 2.** Nhánh đó
(`price_reaction_labels.py` + `text_reaction_model.py`) **giữ lại như một thí nghiệm RIÊNG**,
theo khuôn paper **63**, và báo cáo kết quả trung thực. Nhưng đẩy đầu ra của nó vào làm
feature cho một mô hình dự báo giá = **paper 46 nguyên văn**:

> *nhãn từ giá tương lai → model học dự báo lợi suất từ text → đầu ra thành feature để dự
> báo lợi suất. Vòng lặp, và cách xây nhãn với tay về phía trước.*

**Nếu vẫn muốn thử:** đầu ra tầng 1 **bắt buộc phải sinh out-of-fold** (walk-forward lồng
nhau, train lại tầng 1 chỉ trên dữ liệu trước mỗi fold). Nếu không, tầng 1 in-sample đạt
~97,5% (paper 63) trong khi out-of-sample là ~50,4% — GBM sẽ học phụ thuộc vào một feature
sụp đổ trên test, và **thất bại này hoàn toàn im lặng**.

### 6.4 Vai trò: EXCLUSION, không phải predictor

GBM giá xếp hạng như hiện tại; sentiment chỉ được **loại bỏ** một mã khi văn bản mâu thuẫn,
**không bao giờ lật ngược tín hiệu**.

Ba paper hội tụ: **51** (biến thể divest-only thắng shorting trên mọi metric: return
9,92% vs 7,67%, drawdown 18,67% vs 25,52%) · **55** (bảng chân trị chỉ abstain) · **57**
(cơ chế: tin xấu dự báo cả quý *vì* ràng buộc bán khống ngăn arbitrage; tin tốt hết sau 1
tuần). **HOSE chặt hơn Mỹ → phía tiêu cực là phía dai dẳng, và exclusion là cách duy nhất
danh mục long-only hành động được.**

---

## 7. Giao thức đánh giá

**Tái dùng `purged_walkforward_folds`** — đã đúng (purge + embargo H ngày, không bao giờ
random split).

### 7.1 Chính — walk-forward có tính phí

| báo cáo | vì sao |
|---|---|
| CAGR, Sharpe, **max drawdown**, turnover | 51: model tốt nhất mua return/risk bằng drawdown 28,2% vs 18,6% của index |
| **A/B: cùng GBM, cùng fold, CÓ vs KHÔNG có khối text** | **đây chính là kết quả luận văn** |
| vs VN-Index buy&hold, vs VN100 equal-weight | baseline ngoài |

### 7.2 Phụ — phân loại, luôn kèm baseline

`macro-F1` · **`MCC`** · `Brier` · **tỷ lệ lớp đa số** (25/50/25, miễn phí) ·
**`acc_trivial` tại ngưỡng đã chọn** (`acc = 6P² − 4P + 1`, paper 56) · ma trận nhầm lẫn ·
**theo từng fold, không gộp**.

⚠️ *"Metric choice is load-bearing"* — `sentiment/CONTEXT.md` §5. Với base rate 11% và
majority 48%, accuracy vô nghĩa. Bài học này đã trả giá một lần rồi.

### 7.3 Ablation, theo thứ tự người phản biện sẽ hỏi

1. **`If_news` một mình → `+ Positive/Negative`.** Nếu sắc thái không thêm gì so với biến
   giả, đó là **một phát hiện sạch và công bố được** (paper 57).
2. **Scope:** stock → +sector → +market. Kiểm định hierarchy paper **43** trên mục tiêu cắt
   ngang mà nó chưa từng được thiết kế cho.
3. **Chân trời** 1/2/4/8/13 tuần — kiểm định luận điểm trung tâm paper **57** trên VN.
4. **Tuần vs ngày** trên top-30 (nơi độ phủ ngày đạt 12,2%) — tái lập Fig. 3 của paper 57.
5. **Content vs headline vs title+content** — kiểm định **51** và **63** trên tiếng Việt.
6. **⭐ Áp sentiment nơi mô hình giá YẾU**, thay vì áp đều. Paper **59** đo mức cải thiện
   tăng từ +2,07 pp (toàn bộ) lên **+9,83 pp** khi loại các mã có mô hình giá vốn mạnh; và
   experiment_1.5 đã có sẵn phân tầng (**VCB 0,767 AUC vs VRE 0,408**).
   ⚠️ Ước lượng độ mạnh mô hình giá phải làm **in-sample** — bộ lọc của chính paper 59 có
   vẻ đã chọn trên kết quả test.

### 7.4 Con số nên kỳ vọng

| nguồn | kết quả |
|---|---|
| **63** — Reuters, tick 1 phút, 375k bài, capacity đã chứng minh 97,5% train | **50,4% out-of-sample**, nhị phân cân bằng |
| **51** — giao thức chuẩn nhất trong 23 paper, 8,5 triệu bài | **MCC 0,069** trước phí, truy về **một tháng năm 2011** |
| **50** — Kappa cho sentiment một mình | **0,078** |
| **53** — nhãn phân vị 25/50/25 | best sector-avg **0,496** (dưới base rate 50%) |
| **nội bộ** — 3 mã, đã chạy | sentiment **làm mô hình tệ đi** |

→ Kỳ vọng 3 lớp cân bằng: **0,35–0,40**. **Nếu ra 0,70 thì gần như chắc chắn có rò rỉ** —
kiểm tra ngay: (a) chia theo dòng thay vì theo ngày (paper **61**: 58,6 bài/ngày chia chung
một nhãn), (b) feature giá chạm vào phiên `d` (paper **46/47/50**).

---

## 8. Thứ tự triển khai + tiêu chí dừng

**Nguyên tắc: dựng nhãn và harness TRƯỚC, scorer SAU.** Scorer là công đoạn dài nhất; nếu
harness cho thấy `if_news` một mình đã không có gì thì tiết kiệm được cả tháng.

| # | Việc | Kết quả | ⛔ Tiêu chí dừng |
|---|---|---|---|
| **0** | Kiểm tra **lỗ hổng 2012** + đếm lại theo năm | biết fold nào không tin được | — |
| **1** | `silver.cafef_news` — làm sạch + căn phiên | bảng + assert invariant | — |
| **2** | `gold.news_weekly_panel` **CHỈ với `if_news` + `n_docs`** — chưa có sentiment | panel (mã, tuần) | — |
| **3** | **Model 2 + walk-forward có phí, chỉ với `if_news`/`n_docs`** | ⭐ **baseline + kiểm định publication effect (paper 57)** | nếu `if_news` không làm gì **và** panel quá thưa → cân nhắc dừng, báo cáo negative |
| **4** | Chọn mẫu 5.000 câu, phân tầng | `raw_data/annotation/…` | — |
| **5** | LLM gán nhãn + **tự tay gán 600–800 câu** | tập vàng | **đồng thuận < 80% → sửa prompt, chưa fine-tune** |
| **6** | Fine-tune PhoBERT, đo trên tập vàng | checkpoint + macro-F1 | **macro-F1 < 0,75 → chưa dùng được** |
| **7** | **⚠️ Cổng LIME/SHAP** | báo cáo top token | **top token là tên mã/ngày/boilerplate → DỪNG, sửa scorer** |
| **8** | `silver.cafef_news_sentiment` build lại 777 mã | ~3–13 phút GPU | — |
| **9** | Panel đầy đủ + Model 2 + ablation §7.3 | **kết quả luận văn** | — |
| **10** | Nhánh riêng theo khuôn paper **63** (nhãn từ phản ứng giá) | negative result trung thực | ⚠️ **không nối vào Model 2** |

**Bước 3 là bước rẻ nhất và cho biết nhiều nhất.** Nó chạy được trong vài ngày, không cần
NLP gì cả, và thiết lập cái baseline mà khối sentiment phải vượt qua.

---

## 9. Gotchas kế thừa — đọc trước khi chạy

Từ `sentiment/CONTEXT.md` §5 và `orchestration/CONTEXT.md` §5:

| ⚠️ | chi tiết |
|---|---|
| **`HF_HUB_OFFLINE=1` BẮT BUỘC** | `from_pretrained` gọi HEAD tới huggingface.co mỗi lần load; mạng chặn → **treo nhiều phút** (đã từng làm một run đứng ~15 phút). Đã baked vào `sentiment_functions.py` — **giữ nguyên** |
| **`silver.stocks_basic` = 2,4 triệu dòng** | đẩy filter ticker **xuống server** (`Condition` + `SqlOperator.IN`); fetch cả bảng rồi filter trong pandas đã từng làm timeout |
| **GPU 4 GB RTX 3050** | process python cũ còn sót làm phân mảnh VRAM; check `nvidia-smi` nếu chạy chậm |
| **`GradientBoosting` exact rất chậm** trên embedding 768-d | dùng `HistGradientBoostingClassifier` |
| **`driver.select` trả `numeric` thành `Decimal`→`object`** | `pd.to_numeric` mọi cột mới |
| **`news.timestamp` thường date-only** | sắp xếp theo `(exchange, ticker, timestamp, news_order)` để có thứ tự xác định |
| **`order` là từ khoá SQL** | trong bronze nó là `news_order` |
| **Truncate `logs/app.log` trước mỗi run** | `Clear-Content logs\app.log` |
| **Gold table drop chính nó trước khi ghi** | `_ingest_gold_table` đã sửa; asset mới phải theo cùng pattern nếu không re-materialise sẽ chết trên PK |
| **`bootstrap()` phải gọi lại trong resource** | `sys.path` không sống sót vào step subprocess của Dagster |

---

## 10. Checklist — dán vào PR

- [ ] Bronze không bị sửa
- [ ] `silver.cafef_news` assert: 0 dòng có `trading_date` cùng ngày với bài đăng sau 15:00
- [ ] `gold.news_weekly_panel` assert grain 1 dòng/(mã, tuần)
- [ ] Chia fold **theo ngày**, không theo dòng
- [ ] Feature giá **chỉ dùng phiên đóng cửa trước** cửa sổ hình thành
- [ ] `mom_12w` có trong X
- [ ] Ngưỡng nhãn **> chi phí khứ hồi**, và có in `acc_trivial`
- [ ] Baseline (majority / random-walk / buy&hold) in cạnh mọi metric
- [ ] MCC + Brier + max drawdown, **theo từng fold**
- [ ] A/B có/không khối text, cùng fold
- [ ] Cổng LIME đã chạy và **đã pass**
- [ ] Tập vàng đã commit vào git
- [ ] `model_version` đã đổi trong `silver.cafef_news_sentiment`
- [ ] Không có embedding 768-d nào lọt vào Model 2
- [ ] Nhánh paper-63 **không** nối vào Model 2
