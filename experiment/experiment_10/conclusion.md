# Kết luận — Tin tức tài chính Việt Nam có dự báo được giá cổ phiếu không?

> Tổng kết experiment_10. Nguồn: 23 paper trong [CONTEXT.md](CONTEXT.md) · kế hoạch trong
> [guidance.md](guidance.md) · nhật ký thi công trong
> [TODO.md](../../docs/TODO.md) — ⚠️ **thay cho `src/orchestration/todo.md`, đã gộp và xoá
> 2026-08-17**; nội dung cũ lấy lại bằng `git show <commit>^:src/orchestration/todo.md` ·
> mã và kết quả trong
> [news_result.py](news_result.py).
>
> Ngày: 2026-08-03 · 9 commit · 52 Dagster asset · 4 bảng dữ liệu tin tức.

---

## 0. Kết luận trong ba câu

1. **Tin tức KHÔNG dự báo được chiều biến động giá.** Bốn hướng tấn công độc lập, mọi
   chân trời từ 1 phiên đến 13 tuần, đều cho kết quả không phân biệt được với ngẫu nhiên.
2. **Tin tức CÓ dự báo được độ lớn biến động** — ngày có tin biến động gấp **1,15×** ngày
   không tin, sau khi chuẩn hoá trong từng mã.
3. **Tín hiệu duy nhất tìm thấy trong toàn bộ thí nghiệm là momentum/thanh khoản**, và nó
   không cần một chữ text nào.

Kết quả này **tái lập §6a của [`src/sentiment/CONTEXT.md`](../../src/sentiment/CONTEXT.md)
ở 777 mã thay vì 3.** Đòn bẩy mà chính module đó gọi là *"the biggest lever"* — thêm mã —
đã được kéo hết cỡ và kết luận không đổi.

---

## 1. Dữ liệu tin tức

### 1.1 Nguồn và quy mô

| | |
|---|---|
| Nguồn | **CafeF**, cào bằng `CafeFNewsScraper` |
| Trên đĩa | `raw_data/cafef/news/` — **777 file CSV, 493 MB** |
| Bronze | `bronze.cafef_news` — **405.320 dòng × 11 cột**, 777 mã |
| Khoảng thời gian | **2007-02-23 → 2026-07-24** (~19,4 năm) |
| Khoá chính | `row_id` = md5 của `(exchange, ticker, url)` |

Bronze khớp đĩa chính xác: 405.320 = 405.322 − 2 dòng bị loại vì khoá null.

### 1.2 ⚠️ Corpus tách làm hai, và chỉ một nửa là văn bản

| `type` | số dòng | độ dài content TB | thực chất |
|---|---:|---:|---|
| `disclosure` | **326.722** (80,6%) | 288 ký tự | Công bố thông tin HOSE — boilerplate + link PDF |
| `editorial` | **78.017** (19,3%) | **2.687 ký tự** | Báo chí thật — **210 triệu ký tự tiếng Việt** |
| `error` | 581 (0,1%) | 101 ký tự | Lỗi cào, loại bỏ |

**Corpus dùng cho NLP là 78.017 văn bản, không phải 405.320.** Phần disclosure là một
*lịch sự kiện* (có giá trị riêng — paper 57 phát hiện phản ứng trễ với tin tập trung vào
tuần công bố KQKD) chứ không mang sắc thái.

Phân loại theo `category`: kết quả kinh doanh 223.593 · giao dịch cổ đông lớn/nội bộ 72.092
· chưa phân loại 51.587 · nhân sự 21.996 · cổ tức 19.008 · tăng vốn/cổ phiếu quỹ 17.044.

### 1.3 Phân bố theo mã — đuôi rất dài

| | editorial/mã |
|---|---:|
| p25 | 21 |
| **p50** | **45** |
| p75 | 104 |
| p90 | 238 |
| max | **1.389** (`HOSE_VIC`) |

**3 mã có 0 editorial · 72 mã có <10 · 411 mã (53%) có <50.**
Top-10: VIC 1.389 · HPG 1.277 · MWG 1.111 · HAG 1.098 · FPT 1.060 · VNM 981 · NVL 954 ·
VCB 902 · STB 894 · MSN 747.

### 1.4 ⚠️ Độ phủ — con số quyết định mọi lựa chọn thiết kế

Từ 2015 (4.162 phiên, 604 tuần, 777 mã), tỷ lệ ô panel có ≥1 tin:

| | ticker-**ngày** | ticker-**tuần** |
|---|---:|---:|
| 777 mã, editorial | **1,60%** | **8,67%** |
| 777 mã, mọi loại | 7,85% | 36,81% |
| **top-30 mã, editorial** | **12,2%** | **51,7%** |

**Một feature sentiment theo ngày sẽ thiếu dữ liệu 98,4% trên toàn panel.** Đây là lý do
mọi mô hình dự báo trong thí nghiệm này chạy ở tần suất tuần, và tại sao paper 57 (tin
theo ngày dự báo 1–2 ngày, tin theo tuần dự báo 13 tuần) khớp với dữ liệu VN.

### 1.5 Độ dài văn bản

Đo trên mẫu 4.000 bài, tokenizer `vinai/phobert-base`:

| | token |
|---|---:|
| headline + **toàn bộ** content | mean **749**, median 573, p95 1.913, **max 5.325** |
| headline + 600 ký tự đầu | mean 177 → **chỉ thấy 38,7% bài báo** |

PhoBERT-base có `max_position_embeddings = 258`, nên đọc hết bài **bắt buộc phải chia
khối**: 254 token/cửa sổ, trung bình **3,44 khối/bài**, cap 16 khối phủ 100% văn bản.

### 1.6 ⚠️ Bốn vấn đề chất lượng

| # | vấn đề | quy mô | xử lý |
|---|---|---|---|
| 1 | **Hố dữ liệu 2012-06 → 2012-11**, dropout ~98% | 37/35/24/23/24/20 dòng/tháng so với 600–1.600 hai bên; 458 mã | Sàn dữ liệu đặt ở **2013-01-01** trong `news_panel.PANEL_START` |
| 2 | **Timestamp chỉ có ngày** (`00:00:00`) | 89.698 dòng (22,2%) — nhưng **89.639 là disclosure, chỉ 59 editorial** | Gắn cờ `ts_is_date_only`, coi như cuối ngày → phiên kế tiếp |
| 3 | **65,5% bài đăng ngoài 09:00–15:00 ICT**, mode 17:00 | — | Căn `trading_date` = phiên đầu tiên mở cửa **sau** bài báo |
| 4 | **`close_adjust` sai hệ số ở corporate action** | 1.002 dòng có lợi suất 5 phiên > +61% (vượt trần HNX) | Lọc theo biên độ sàn trong `news_result.py` |

Ví dụ vấn đề 4, tìm ra khi đọc ví dụ đầu tiên trong file kiểm chứng:

```
BNA  2021-10-06   close_raw 67.900   close_adjust 11.320
BNA  2021-10-07   close_raw 38.700   close_adjust 23.950   ← raw −43%, adjusted +112%
```

Chuỗi raw nhận đúng đợt phát hành thêm; chuỗi adjusted nhảy thay vì liên tục. Chỉ 0,04% số
dòng — **nhưng chúng nằm trọn trong đuôi phân phối, đúng chỗ định nghĩa lớp cực trị.**
**Chỗ sửa thật là data-quality screen ở bronze**, cạnh 262 dòng `high < low` đã ghi ở mục
25 của todo.md (nay là **P4-3** trong [TODO.md](../../docs/TODO.md)).

Trùng lặp chéo giữa các mã **không** phải vấn đề: 382.829 URL riêng / 405.320 dòng =
**1,06×**; chỉ 1,9% URL gắn >1 mã.

### 1.7 ⚠️ Vấn đề thứ năm: gắn nhầm mã

Khoảng **52% corpus là tin tổng hợp gắn nhầm cho một mã cụ thể**. Ví dụ thật từ file kiểm
chứng: *"Lịch chốt quyền nhận cổ tức bằng tiền của 8 doanh nghiệp"* — gắn cho **CPH** nhưng
nội dung nói về VGC, NFC và sáu mã khác. Paper 57 loại tin đa-công-ty và lọc relevance dưới
35% đúng vì lý do này; paper 49 đo được cờ relevance có giúp.

Đo được: chỉ **48,2%** số bài nêu tên mã ít nhất 1 lần/1.000 ký tự.

---

## 2. Các phương pháp đã thử

Ba nhóm, thực hiện theo thứ tự rẻ → đắt.

### 2.1 Luồng dữ liệu đã xây (điều kiện cần cho mọi thứ sau)

Theo đúng quy tắc ba tầng của repo — pure function ở `src/sentiment`, ETL ở
`data_preprocessor`, asset ở `orchestration/assets`:

```
bronze.cafef_news (405.320)
   └─► silver.cafef_news (395.470 × 18)      ← làm sạch, khử trùng lặp, CĂN PHIÊN
          ├─► gold.news_weekly_panel (429.052 ticker-tuần × 28)
          └─► gold.news_daily_panel (2.058.604 stock-day × 26)
```

Bỏ 9.850 dòng ở silver: `error` 581 · rỗng 1 · không có phiên hoặc gap quá xa 7.322 ·
trùng lặp 1.946.

**Asset count 49 → 52.** Mọi asset đều assert invariant và **raise** nếu sai, không phải
log rồi return.

⚠️ **`trading_date` là chốt chống nhìn trước** — bài báo được gán vào **phiên đầu tiên mở
cửa sau nó** (mốc 09:00 ICT); timestamp chỉ có ngày được coi như cuối ngày, tức phiên kế
tiếp, hướng thận trọng. Đây chính là lỗi làm hỏng paper 46, 47 và 50.

### 2.2 Nhóm A — Dự báo CHIỀU (tin → lợi suất tương đối tương lai)

| # | thiết kế | universe | chân trời |
|---|---|---|---|
| A1 | Panel **tuần**, chỉ `if_news` + `n_docs` | top-100 thanh khoản | 1, 2, 4, 8, 13 tuần |
| A2 | như trên | **top-30** mã đưa tin nhiều nhất | 1, 4, 13 tuần |
| A3 | Panel **ngày**, cửa sổ tin trượt 5/10 phiên | top-100 | **rel5, rel10** |
| A4 | như trên | **top-30** | rel5, rel10 |

Năm nhánh feature trên cùng fold: `controls` (momentum 1/4/12/26 tuần + thanh khoản +
số phiên) · `news (all)` · `news (editorial)` · `controls + news` · `controls + editorial`.

**Giao thức chung, theo paper 51:**
- Nhãn **phân vị 25/50/25** của lợi suất **tương đối cắt ngang** (paper 53) → tỷ lệ nền
  biết trước
- **Ngưỡng phải vượt chi phí khứ hồi 0,5%** (paper 56) — đạt ở 99,0% số tuần
- Walk-forward **mở rộng, purge + embargo**, chia theo **NGÀY** không theo dòng
- Universe theo **thanh khoản trailing**, point-in-time — không dùng thành phần VN100 hôm
  nay áp ngược (paper 56, "tyranny of the index")
- **Kiểm định ghép cặp theo fold** — cùng fold, cùng dòng, cùng nhãn, chỉ thêm khối news
- Backtest **có tính phí**, long-only (HOSE không short được single-stock)

### 2.3 Nhóm B — Phân loại PHẢN ỨNG (text → phản ứng giá), nhánh paper 63

| # | thiết kế |
|---|---|
| B1 | 5 lớp ngũ phân vị, **lead 256 token** (headline + 600 ký tự) |
| B2 | + **lọc biên độ sàn** (sau khi phát hiện lỗi `close_adjust`) |
| B3 | + **lọc relevance ≥ 1,0** (chỉ bài thực sự nêu tên mã) |
| B4 | **Toàn bộ nội dung, chia khối** — `mean ‖ max ‖ lead`, 2.304 chiều |
| B5 | **3 lớp tam phân vị** so với 5 lớp |

**X:** PhoBERT-base **đóng băng**, mean-pool theo attention mask, fp16.
**y:** lợi suất **vượt trội thị trường** tại h ∈ {1, 5, 10} phiên, chia phân vị **theo từng
năm** (chế độ biến động 2020 khác 2023).
**Đối chứng:** nhãn xáo trộn trong tập train — cách duy nhất thấy pipeline chấm bao nhiêu
trên nhiễu thuần.

⚠️ Nhánh này **cố ý tách rời** khỏi mô hình dự báo. Nhãn đến từ giá, nên đưa đầu ra của nó
vào làm feature dự báo giá chính là vòng lặp của paper 46. Đặt tên đúng là *"dự báo phản
ứng"* — như paper 63 làm — thì bài toán hợp lệ.

### 2.4 Nhóm C — Mô tả: tin có làm giá động không?

So sánh |lợi suất vượt trội 5 phiên| giữa ngày có tin và ngày không tin, **chuẩn hoá trong
từng mã** (chia cho median của chính mã đó).

### 2.5 ❌ Những gì KHÔNG làm, và vì sao

| | lý do |
|---|---|
| **Fine-tune bộ chấm sentiment tiếng Việt** | Mục 7–13 của todo.md bị **dừng có chủ đích** (nay ở mục *Closed* của [TODO.md](../../docs/TODO.md)) sau khi nhóm A cho kết quả null ở nơi độ phủ cao nhất. Bước đầu tiên và rẻ nhất (publication effect) đã trắng. |
| **Gán nhãn thủ công 600–800 câu** | Cùng lý do — nút thắt là dữ liệu, không phải bộ chấm. |
| **Đồ thị quan hệ (paper 44)** | Wikidata không phủ mã VN ở mức dùng được. |
| **Google Trends (paper 58)** | Chưa cào; là hướng còn lại đáng nhất (xem §5). |

Lưu ý: repo **đã có sẵn** một bộ chấm sentiment tổng quát (`mr4/phobert-base-vi-sentiment-analysis`)
từ công việc trước, và nó **sai hệ thống về mặt tài chính** — chấm *"VCB: chi trả cổ tức
2025"* = **−0,97**, đẩy 51% editorial về VERY_NEGATIVE. Đây là bằng chứng nội bộ cho việc
bộ chấm chưa kiểm định thì không dùng được, mạnh hơn cả paper 45 và 52.

---

## 3. Kết quả

### 3.1 Nhóm A — Dự báo chiều: **null ở mọi cấu hình**

Bảy phép kiểm định ghép cặp (`controls + news` trừ `controls`, cùng fold):

| grain | universe | chân trời | ΔMCC | **t** | fold thắng |
|---|---|---|---:|---:|---:|
| tuần | top-100 | 1 tuần (≈5 phiên) | −0,0001 | −0,04 | 2/6 |
| tuần | top-100 | 2 tuần (≈10 phiên) | +0,0022 | +1,20 | 4/6 |
| tuần | top-30 | 1 tuần | −0,0023 | −0,42 | 2/6 |
| tuần | top-30 | 4 tuần | −0,0010 | −0,22 | 4/6 |
| tuần | top-30 | 13 tuần | +0,0047 | +1,52 | 4/6 |
| **ngày** | top-100 | **rel5** | +0,0002 | +0,11 | 3/6 |
| **ngày** | top-100 | **rel10** | +0,0005 | +0,17 | 2/6 |
| **ngày** | top-30 | **rel5** | −0,0029 | −1,26 | 2/6 |
| **ngày** | top-30 | **rel10** | +0,0003 | +0,12 | 3/6 |

**Mọi |t| < 1,6. Fold thắng quanh một nửa. Dấu đổi chiều giữa các chân trời.** Đây là hình
dạng chuẩn của một hiệu ứng bằng 0.

**Khối `news` một mình: MCC 0,000–0,016 ở mọi chân trời**, nhiều fold âm.

**Thêm news vào controls làm danh mục KÉM ĐI:** top-30 h=4 tuần, CAGR **30,39% → 22,31%**
(benchmark 18,07%). Trả 2–8 pp CAGR để đổi lấy ΔMCC ±0,003.

⚠️ **Một t = +2,16 xuất hiện ở lần chạy universe đầy đủ, h=13 tuần — và nó không sống sót.**
Cùng phép so sánh trên top-30 chỉ còn 1,52; h=1 và h=4 thì âm. Với 5 nhánh feature × 5 chân
trời × 2 universe, một t lẻ loi là đúng bẫy multiple-comparison mà paper 62 mắc.

### 3.2 ⭐ Phát hiện phụ đắt giá: 5–10 phiên là chân trời TỆ NHẤT

Không chỉ news chết ở chân trời ngắn — **momentum cũng chết**:

| chân trời | universe | controls CAGR | benchmark |
|---|---|---:|---:|
| **rel5** | top-100 | **−2,78%** | 9,75% |
| **rel5** | top-30 | **2,68%** | 16,48% |
| rel10 | top-100 | 9,86% | 9,98% |
| rel10 | top-30 | 7,69% | 16,74% |
| 4 tuần | top-30 | **30,39%** | 18,07% |
| 13 tuần | top-30 | **28,63%** | 19,34% |

**Khối `controls` chỉ thắng benchmark từ 4 tuần trở lên.** Khớp với memory
`project-vcb-forecasting-conclusion` (lợi suất ngắn hạn một cổ phiếu không dự báo được) và
với paper 57 (tin theo ngày dự báo 1–2 ngày, Ngày 3 t = 1,2).

### 3.3 Nhóm B — Phân loại phản ứng: **null, và bốn cách sửa đều không cứu được**

58.660 bài, walk-forward purge + embargo, chia theo ngày:

| k | biểu diễn | h | train | test | nền | lift | QWK | **MCC** | MCC xáo trộn |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 5 | lead | 5 | 0,465 | 0,214 | 0,207 | 1,038 | +0,004 | +0,018 | −0,003 |
| 5 | **full doc** | 5 | 0,496 | 0,224 | 0,207 | **1,082** | +0,020 | **+0,030** | −0,002 |
| 3 | **full doc** | 5 | 0,581 | 0,351 | 0,341 | 1,029 | +0,018 | +0,026 | 0,000 |
| 3 | full doc | 10 | 0,610 | 0,352 | 0,342 | 1,029 | +0,024 | +0,029 | −0,009 |

**Train 0,50–0,61 → test dính tỷ lệ nền.** Đây là paper 63 tái lập trên VN: họ 97,5% →
50,4% trên nhị phân cân bằng; đây 49,6% → 22,4% trên 5 lớp nền 20%.

**Bốn cách sửa, kết quả:**

| cách sửa | kết quả |
|---|---|
| **B4. Đọc toàn bộ nội dung** (thay vì 38,7%) | ✅ Giúp — **9/9 delta dương** — nhưng tối đa **+0,9 pp accuracy**, ΔMCC +0,0115 tốt nhất |
| **B5. Giảm còn 3 lớp** | ❌ **Không giúp.** Accuracy nhảy +13 pp nhưng **toàn bộ là tỷ lệ nền** (0,205→0,341); ΔMCC **âm ở 2/3 chân trời**, thắng **1/5 fold** |
| **B3. Lọc relevance** | ❌ **Làm tệ hơn** — test 0,220 so với nền 0,222, tức **dưới mức đoán mò** |
| **B2. Lọc lỗi giá** | ➖ Không đổi kết quả (chỉ 0,04% dòng) — nhưng cần thiết để điểm cắt phân vị không dựa trên số hỏng |

⚠️ **Về việc giảm số lớp:** accuracy tăng +12,6 đến +13,8 điểm và **từng điểm đều là tỷ lệ
nền**. Đây là kết quả paper 56 ở dạng nhẹ nhất — *accuracy tăng chỉ bằng cách đổi cách gán
nhãn, mô hình không đổi gì.* **Lift còn tụt từ 1,08–1,09 xuống 1,03.** Chọn tam phân vị
33/33/33 chứ không phải 25/50/25 cũng vì paper 56: `Σ Pₖ²` cực tiểu khi mọi dải bằng nhau
(0,333 so với 0,375), tức sơ đồ bất lợi nhất cho chính mình.

**Một phát hiện từ việc so 3 vs 5 lớp:** lợi ích của việc đọc hết bài **sạch hơn ở 5 lớp**
(3/5, 4/5, 3/5 fold) so với 3 lớp (**0/5**, 3/5, 2/5). → **Thông tin thêm từ thân bài nằm ở
hai đuôi, và 3 dải gộp mất đúng chỗ đó.**

### 3.4 ⭐ Nhóm C — Kết quả DƯƠNG duy nhất

**Chuẩn hoá trong từng mã, ngày có tin biến động gấp 1,15× ngày không tin** (1,146 vs 0,997).

Con số thô là **0,96×** — tức ngày có tin *ít* biến động hơn. Đó là **hiệu ứng thành phần**:
tin tập trung ở mã lớn thanh khoản cao, vốn ít biến động hơn nhóm small-cap chiếm phần lớn
dòng "không tin". Chỉ sau khi chia cho median của chính mã đó mới đọc được.

Theo `category`: kết quả kinh doanh 4,67% (n=17.443) · giao dịch cổ đông lớn 4,55% · tăng
vốn 4,46% · chưa phân loại 4,41% · cổ tức 4,30% · nhân sự 4,11%.

**→ Tin dự báo được ĐỘ LỚN, không dự báo được CHIỀU.**

### 3.5 Khả thi phần cứng (RTX 3050 Laptop 4 GB, đo thật)

| | |
|---|---|
| **Fine-tune** PhoBERT, 5.000 câu × 4 epoch | **6,0 phút** (2,4 phút nếu chỉ mở 4 layer trên) |
| Fine-tune toàn corpus, full doc × 3 epoch | 3,0 giờ (73 phút nếu 4 layer trên) |
| **Pre-train từ đầu** | **164 ngày = 0,45 GPU-năm** cho lịch 40 lượt trên corpus 20 GB |

Cấu hình tốt nhất: `full fp16 AMP, batch 16, seq 256` — 3,22 GiB, 55,1 samples/s.

⚠️ Trên Windows/WDDM driver **âm thầm tràn qua RAM hệ thống** thay vì báo OOM, nên batch
lớn nhất "chạy được" thường chậm nhất (fp16 batch 64: 7,61 GiB, 6,1 samples/s so với 43,4
ở batch 8). **Nút thắt của bước gán nhãn là công sức con người, không phải phần cứng.**

---

## 4. Tổng kết

### 4.1 Bằng chứng hội tụ

Bảy hướng độc lập, cùng một kết luận:

| # | hướng | kết quả |
|---|---|---|
| 1 | Publication effect (`if_news`, paper 57) | MCC 0,000–0,016, không đo được trên VN |
| 2 | Thêm news vào momentum | ΔMCC ≈ 0, CAGR **giảm** 2–8 pp |
| 3 | Text → phản ứng giá (paper 63) | train 0,50 → test = tỷ lệ nền |
| 4 | Đọc toàn bộ nội dung thay vì 38,7% | +0,9 pp, không đổi kết luận |
| 5 | Giảm số lớp 5 → 3 | không đổi sau hiệu chỉnh, còn kém hơn |
| 6 | Lọc bài thực sự nêu tên mã | **tệ hơn** — dưới mức đoán mò |
| 7 | Đối chứng nhãn xáo trộn | ngang ngửa mô hình, có lúc **thắng** |

Và ba mốc đối chiếu từ literature: paper **51** (giao thức chuẩn nhất trong 23 paper) đạt
**MCC 0,069** với 8,5 triệu bài, quy về một tháng năm 2011 · paper **63** đạt **50,4%**
out-of-sample trên Reuters + tick data · paper **50** công bố **Kappa 0,078** cho sentiment
đơn lẻ. **Kết quả ở đây bằng khoảng một nửa trần của cả literature** — và trần đó vốn đã
gần bằng không.

### 4.2 Vì sao đây là kết quả bảo vệ được, không phải thất bại

| yếu tố | trạng thái |
|---|---|
| Naive benchmark ở mọi bảng | ✅ tỷ lệ nền biết trước theo cấu tạo (paper 53) |
| Chỉ số hiệu chỉnh ngẫu nhiên | ✅ MCC + QWK + Brier, không chỉ accuracy (paper 51) |
| Ngưỡng nhãn vượt chi phí giao dịch | ✅ 99,0% số tuần đạt (paper 56) |
| Chống nhìn trước, ghi thành lời | ✅ `trading_date` + assert raise trong asset (paper 51) |
| Walk-forward purge + embargo, chia theo ngày | ✅ (paper 51, 62; tránh lỗi paper 61) |
| Chi phí giao dịch + max drawdown | ✅ (experiment_3; paper 51 thiếu) |
| **Kiểm định ghép cặp theo fold** | ✅ **không paper nào trong 23 paper chạy** |
| **Đối chứng nhãn xáo trộn** | ✅ **không paper nào trong 23 paper chạy** |

Trên **một corpus 78 nghìn bài tiếng Việt chưa ai dùng**, ở **một thị trường cận biên mà
paper 58 lập luận là cần kiểm định lại** thay vì giả định kết quả từ thị trường phát triển
sẽ chuyển sang.

**Đây là chương "Tin tức không dự báo được lợi suất tương đối cắt ngang trên thị trường
Việt Nam" — một kết quả null được đo tử tế, không phải một thí nghiệm hỏng.**

### 4.3 Hai kết quả dương mang về được

1. **Tin dự báo độ lớn, không dự báo chiều** (1,15×). Phát biểu sắc, đo được, và mở ra
   hướng dự báo **biến động** thay vì hướng.
2. **Momentum/thanh khoản là tín hiệu thật** — MCC +0,052…+0,061, **dương ở 30/30 fold**,
   CAGR 30,39% vs benchmark 18,07% trên top-30 h=4 tuần, Sharpe 1,10. Nó nằm đúng khoảng
   paper 51 đạt được sau 8,5 triệu bài — nhưng **miễn phí**, không cần corpus, không cần
   gán nhãn, không cần fine-tune.

### 4.4 Ba hướng còn lại, xếp theo tỷ lệ lợi ích/chi phí

| | hướng | vì sao |
|---|---|---|
| **1** | **Theo đuổi khối `controls`** | Kết quả dương duy nhất, không cần NLP. Nối vào `src/model/cross_sectional/` và experiment_1.8. |
| **2** | **Dự báo BIẾN ĐỘNG thay vì chiều** | §3.4 cho thấy tin có thông tin về độ lớn. `gold.news_daily_panel` đã sẵn sàng. |
| **3** | **Google Trends** (paper 58) | Miễn phí, dày đặc, **độc lập với việc có tin hay không** — đúng chỗ giải quyết vấn đề thiếu 98,4% ở §1.4. Chưa cào. |

### 4.5 Nợ kỹ thuật để lại

| | |
|---|---|
| ⚠️ **`close_adjust` sai ở corporate action** | 1.002 dòng; cần data-quality screen ở **bronze**, cạnh 262 dòng `high < low` ([TODO.md](../../docs/TODO.md) **P4-3**, đo lại 2026-08-17 vẫn đúng 262) |
| **Hố dữ liệu 2012-06→11** | Đã né bằng `PANEL_START`; nếu cào lại được thì mở rộng được 4 năm |
| **~52% corpus gắn nhầm mã** | Cờ `relevance_score` đã có trong silver; cần quyết định ngưỡng nếu dùng lại |
| `gold.news_daily_panel` | 2 triệu dòng, chưa ai đọc ngoài `run_weekly_prototype.py` |

---

## 5. Sản phẩm bàn giao

| loại | vị trí |
|---|---|
| **Bảng dữ liệu** | `silver.cafef_news` · `gold.news_weekly_panel` · `gold.news_daily_panel` |
| **Dagster asset** | `silver/cafef_news` · `gold/news_weekly_panel` · `gold/news_daily_panel` (49 → **52 asset**) |
| **Pure function** | [`src/sentiment/news_clean.py`](../../src/sentiment/news_clean.py) · [`news_panel.py`](../../src/sentiment/news_panel.py) · [`doc_encoder.py`](../../src/sentiment/doc_encoder.py) · [`weekly_xsec.py`](../../src/sentiment/weekly_xsec.py) |
| **Runner** | [`run_weekly_prototype.py`](../../src/sentiment/run_weekly_prototype.py) · [`news_result.py`](news_result.py) · [`phobert_capacity.py`](phobert_capacity.py) |
| **Biểu đồ** | `news_result_1_impact.png` · `news_result_2_model_{3,5}class.png` · `news_result_3_class_schemes.png` |
| **Ví dụ kiểm chứng** | [`news_result_examples.md`](news_result_examples.md) — 6 nhóm, toàn bộ ngoài mẫu, kèm URL gốc |
| **Tài liệu** | [`CONTEXT.md`](CONTEXT.md) (23 paper) · [`guidance.md`](guidance.md) (kế hoạch) · [`TODO.md`](../../docs/TODO.md) (backlog, đã gộp `src/orchestration/todo.md` vào 2026-08-17) · file này |

Mọi kết quả tái lập được: `random_state=0`, cache theo `row_id`, hai lần chạy liên tiếp cho
số liệu giống hệt.
