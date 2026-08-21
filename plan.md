# PLAN — tầng lọc cổ phiếu (`screen_schema`) và lần chạy lại toàn bộ chain

> Viết 2026-08-21. **Kế hoạch lớn**, chưa có dòng code nào được viết.
> Mọi con số trong file này là **ĐO ĐƯỢC** trên `database_main_v2` ngày 2026-08-21,
> không phải ước lượng — theo quy ước §8 của CLAUDE.md.
>
> | file | trả lời | |
> |---|---|---|
> | [CLAUDE.md](CLAUDE.md) | *đây là cái gì, đã CHỨNG MINH được gì?* | bản đồ + phán quyết |
> | [RUNBOOK.md](RUNBOOK.md) | *chạy nó thế nào?* | lệnh, thứ tự stage |
> | [ISSUES.md](ISSUES.md) | *cái gì đang HỎNG?* | mã vĩnh viễn |
> | [TODO.md](TODO.md) | *làm gì TIẾP?* | một danh sách, `P1` trước |
> | **[plan.md](plan.md)** | ***kế hoạch LỚN đang theo đuổi*** | file này |

---

## 1. Mục tiêu

Thêm một **tầng lọc cổ phiếu** trước toàn bộ chain: cổ phiếu không đạt chuẩn bị loại
**hoàn toàn** — không xuất hiện trong input của feature selection, không xuất hiện trong
output của model, biến mất khỏi mọi bảng mà chain đọc.

Động cơ là [RUNBOOK §8b rule 2](RUNBOOK.md): model **over-pick sàn khó giao dịch nhất** —
UPCOM **2.20×** tỷ trọng của nó, HOSE chỉ **0.76×**; ba mã được chọn nhiều nhất là `DCT`
(108/236 book), `DCS` (106), `EFI` (87), trong khi `VCB` chỉ 30. RUNBOOK gọi đây là
*"mối đe doạ lớn nhất còn mở đối với các mức lợi nhuận"*.

---

## 2. ⚠️ BỐN ĐÁNH ĐỔI ĐÃ ĐƯỢC CHẤP NHẬN — ghi lại để phiên sau không phải tranh luận lại

Bốn quyết định dưới đây do người dùng đưa ra ngày 2026-08-21 **sau khi** các hệ quả được
nêu ra. Chúng là **lựa chọn có chủ đích, không phải sơ suất** — nhưng hệ quả thì vẫn còn
nguyên và mọi con số sinh ra từ chain mới phải được đọc kèm chúng.

| # | quyết định | hệ quả phải mang theo |
|---|---|---|
| 1 | **Chạy lại toàn bộ chain** | nhãn `cs_rank` đổi mẫu số → selection / final table / dataset / walk-forward đều phải làm lại. **CAGR +74.0 % và Sharpe@30 +2.531 hiện tại KHÔNG so sánh trực tiếp được** với số mới; phải báo cáo là hai thí nghiệm khác nhau |
| 2 | **Lọc theo MÃ, không theo (ngày, mã)** — vi phạm bất kỳ điều kiện nào vào bất kỳ ngày nào là loại vĩnh viễn | ⚠️ **ĐÂY LÀ LOOK-AHEAD VÀ SURVIVORSHIP CÓ CHỦ ĐÍCH.** Danh sách mã được quyết bằng dữ liệu của **toàn bộ** 2009-2026, kể cả tương lai của mọi fold test. Đúng dạng lỗi mà comment tại [preprocessor.py:6094](src/orchestration/preprocessor/preprocessor.py#L6094) gọi là *"survivorship AND look-ahead ở dạng tệ nhất"* khi nói về `vn30.csv`. Xem §5 để biết cái gì còn đọc được và cái gì không |
| 3 | **Loại ngay từ đầu**, không đưa vào feature selection | tầng lọc phải nằm **trước** `read_universe_panel`, tức là tại chỗ định nghĩa tập ứng viên |
| 4 | **Chạy lại toàn bộ dữ liệu** | gộp luôn với `P1`/`FRZ-1` (re-scrape 143 mã đóng băng), vì lọc chồng lên một panel chỉ còn 7 tên thì vô nghĩa |

---

## 3. ⚠️ ĐO NGÀY 2026-08-21: RULE "MỌI NGÀY" LÀM RỖNG UNIVERSE

Đây là con số quyết định hình dạng của rule, và nó phải được đọc **trước** khi chọn ngưỡng.

**Toàn thị trường — 781 mã, `silver.stocks_basic`, 2.389.137 dòng, 2009-01-02 → 2026-08-19:**

| rule (áp dụng cho MỌI ngày của mã) | số mã sống sót / 781 |
|---|---|
| `close_raw ≥ 10.000` | 220 |
| `value_matched ≥ 1 tỷ` | **18** |
| `value_matched ≥ 100 triệu` | 56 |
| **cả hai, ngưỡng 1 tỷ** | **13** |
| cả hai, ngưỡng 100 triệu | 33 |
| *(so sánh)* dùng **trung vị** thay vì mọi ngày, cả hai @1 tỷ | 194 |

**Trong universe đang dùng — top-150 theo trung vị `value_matched` trước 2014-01-01:**

| lượng từ | giá | thanh khoản | sống sót / 150 |
|---|---|---|---|
| **mọi ngày (min)** | ≥ 10.000 | ≥ 1 tỷ | **0** ← rỗng hoàn toàn |
| mọi ngày (min) | ≥ 10.000 | ≥ 50 triệu | 11 |
| p05 *(bỏ 5 % ngày tệ nhất)* | ≥ 10.000 | ≥ 50 triệu | 39 |
| p05 | ≥ 10.000 | ≥ 500 triệu | 26 |
| p05 | ≥ 10.000 | ≥ 1 tỷ | 21 |
| **trung vị** | ≥ 10.000 | ≥ 1 tỷ | **74** |
| trung vị | ≥ 10.000 | ≥ 5 tỷ | 53 |

⚠️ **RULE NHƯ PHÁT BIỂU BAN ĐẦU CHO RA ĐÚNG 0 MÃ.** Ngay cả 150 cổ phiếu thanh khoản
nhất Việt Nam, mã nào cũng có ít nhất một phiên mỏng — thường là những ngày đầu niêm yết
hoặc phiên sát nghỉ lễ. Lượng từ **"mọi ngày" không sống được với ngưỡng thanh khoản**.

⚠️ **VÀ `min_width = 20` LÀ SÀN CỨNG.** [cross_sectional.py:190](src/feature_selection/cross_sectional.py#L190)
loại mọi ngày có dưới 20 tên; dưới ~20 mã sống sót thì panel **không dựng được**, không
phải "kết quả yếu". Ba dòng đầu bảng trên đều nằm dưới hoặc sát sàn đó.

### Quyết định cần chốt trước khi code

**Giữ lượng từ "mọi ngày" cho GIÁ, đổi sang phân vị cho THANH KHOẢN.** Giá sàn là một
tính chất bền của cổ phiếu (mã 10.000₫ hiếm khi thành 50.000₫); còn một phiên mỏng là
nhiễu, không phải bản chất. Đề xuất mặc định:

```
ok_price     :  min(close_raw)      ≥ 10.000        →  42/150 sống
ok_turnover  :  p05(value_matched)  ≥ 500 triệu     →  kết hợp còn ~26/150
```

⚠️ Con số kết hợp cuối cùng phải **đo lại**, không suy ra từ hai cột riêng lẻ. Và **26 mã
là một cross-section hẹp** — §6-1-quater của CLAUDE.md đã đo VN30 (30 tên) cho `z = +0.10`
so với top-150 `z = +13.78`, nên hẹp đi là một rủi ro có thật, đã được định lượng ở nơi khác.

---

## 4. Kiến trúc

```
silver_schema ──► gold_schema
      │               │
      └───────┬───────┘
              ▼
   screen_schema.stock_day_screen        (cờ theo NGÀY-MÃ, đủ 781 mã, không xoá dòng)
              │
              ▼
   screen_schema.ticker_screen           (cuộn lên MÃ — đây là cái §2.2 quyết định dùng)
              │
              ▼  INNER JOIN / danh sách mã
   read_universe_panel  +  kgpu export   ◄── ĐIỂM XOÁ HẲN
              │
              ▼
   feature selection ──► final_features ──► dataset ──► model ──► backtest
```

**Hai bảng, không phải một, và lý do là §2.2.**

| bảng | grain | vai trò |
|---|---|---|
| `stock_day_screen` | `(exchange, ticker, date)` | **số dòng LUÔN bằng `silver.stocks_basic`.** Đây là bằng chứng: nó nói mã nào vi phạm, ngày nào, thiếu bao nhiêu. Không có nó thì quyết định §2.2 không kiểm chứng được |
| `ticker_screen` | `(ticker)` | cuộn `stock_day_screen` lên mức mã theo lượng từ đã chốt ở §3. **Đây là bảng mà chain đọc** |

⚠️ **Vẫn giữ bảng ngày-mã dù đã chọn lọc theo mã.** Nó là thứ duy nhất cho phép sau này
đo *"nếu lọc theo (ngày, mã) thay vì theo mã thì kết quả khác bao nhiêu"* mà không phải
dựng lại gì — tức là đo đúng cái giá của đánh đổi §2.2.

### Vì sao là schema riêng chứ không phải một `pool__*`

`UnifiedSchemaReader.pools()` trả về **mọi** bảng có tiền tố `pool__`, và đó chính là
danh sách mà `--pools` chọn từ đó. Đặt tên `pool__eligibility` thì `ok_price` trở thành
**feature ứng viên** cho ranker — một biến nhị phân dẫn xuất từ mức giá, trong một chain
mà §3c đã ghi nhận `close_adjust` xếp hạng #1 nhờ "dự đoán" chính nó ở ρ 0.996. Schema
riêng giữ mặt nạ nằm ngoài không gian feature **về mặt cấu trúc**, không phải nhờ kỷ luật.

### Vì sao KHÔNG lọc ở giữa silver → gold

58 kênh `drv_*` của `pool__basic` và ~900 cột TA của `gold.stocks_ta` đều là **cửa sổ
trượt 21/63/252 phiên**. Xoá dòng ở tầng dưới làm thủng cửa sổ mà `rolling` không nhìn
thấy — đúng cái bẫy PARTIAL frame đã biến một kênh 252 ngày thành kênh 10 ngày trên
188.737 dòng. Quy ước sẵn có của repo là `OUT-1`: **NULL một ô, không bao giờ xoá một
dòng** (611 ô được screen, số dòng không đổi).

⚠️ Lọc theo MÃ ở §2.2 khiến bẫy này **không còn áp dụng** cho lần chạy này — một mã bị
loại thì bị loại toàn bộ lịch sử, không tạo lỗ hổng. Ghi lại đây vì nó sẽ áp dụng trở
lại ngay khi ai đó thử lọc theo (ngày, mã).

---

## 5. ⚠️ CÁI GÌ CÒN ĐỌC ĐƯỢC SAU KHI CHỌN §2.2, VÀ CÁI GÌ KHÔNG

Đây là phần quan trọng nhất của file. Lọc theo mã bằng dữ liệu toàn mẫu **không làm hỏng
mọi thứ** — nó làm hỏng một nửa rất cụ thể.

| đại lượng | còn đọc được? | vì sao |
|---|---|---|
| **`z` so với null xáo trộn trong ngày** | ✅ **CÒN** | mọi draw của null đều rút từ **cùng một rổ đã lọc**, nên bias nằm ở cả hai vế và triệt tiêu. Đây đúng là lập luận CLAUDE.md §2c dùng cho survivorship |
| **IC, `ic_t`, tỷ lệ fold dương** | ✅ còn, cùng lý do | so sánh nội bộ trong một rổ cố định |
| **paired test giữa các arm / horizon** | ✅ còn | hai arm thấy cùng một rổ |
| **CAGR, Sharpe, mức lợi nhuận tuyệt đối** | ❌ **KHÔNG** | rổ được chọn bằng cách biết trước mã nào *sống sót và thanh khoản đến 2026*. Đây là §2c một lần nữa, ở dạng mạnh hơn |
| **so sánh với chain hiện tại** | ❌ **KHÔNG** | khác nhãn (mẫu số `cs_rank` đổi), khác universe |

**Câu được phép viết trong luận văn:** *"trên rổ đã lọc, tín hiệu vượt null ở z = …"*.
**Câu KHÔNG được phép:** *"chiến lược này đạt X %/năm"* — trừ khi đi kèm một đoạn nói rõ
rổ được chọn bằng dữ liệu tương lai.

⚠️ **Cách gỡ bias sau này, nếu muốn**: chọn danh sách mã **chỉ bằng dữ liệu trước
`--first-test`** (2017-01-01), y hệt cơ chế `liquidity_before` đang bắt buộc ở
[kgpu/export.py](src/kaggle_gpu/kgpu/export.py#L160). Cấu trúc hai bảng ở §4 hỗ trợ sẵn:
chỉ cần thêm tham số ngày cắt vào bước cuộn `stock_day_screen → ticker_screen`.
**Không làm ở lần chạy này**, ghi lại để không phải phát hiện lại.

---

## 6. Thiết kế để sau này thêm rule dễ

Dùng **bảng spec**, đúng idiom sẵn có của repo (`DATE_SPINE_POOLS` sinh 4 pool từ 1 spec,
`WIDE_PANELS`, `UNIFIED_MEMBER_FILTERS`):

```python
SCREEN_RULES = {
    # tên      → (biểu thức ĐO,     điều kiện ĐẠT,           lượng từ)
    "price":     ("close_raw",      "scr_price >= 10000",    "min"),
    "turnover":  ("value_matched",  "scr_turnover >= 0.5",   "p05"),   # tỷ VND
    "pe":        ("pe_ttm",         "scr_pe < 100",          "p05"),
    # thêm rule mới = thêm MỘT dòng ở đây
}
```

Ba quy tắc giữ cho việc thêm rule không lan ra ngoài:

1. **`ok_all` tính sẵn trong bảng.** Không consumer nào liệt kê tên rule, nên thêm rule
   không phải sửa `read_universe_panel`, `export.py` hay `backtest`.
2. **Lưu cả SỐ ĐO lẫn KẾT LUẬN.** Đổi ngưỡng = `WHERE` trên cột có sẵn, không phải build
   lại 2,4 triệu dòng. Ngưỡng ghi vào `COMMENT` của bảng (như `final_features` ghi
   `Source runs:`) để bảng cũ báo STALE thay vì âm thầm được chấp nhận.
3. ⚠️ **Không đo được thì `NULL`, không phải `FALSE`.** `FALSE` sẽ xoá sạch thị trường,
   `TRUE` khiến rule vô hiệu âm thầm. Cả hai đều vi phạm §5 rule 2 (*một phép đo vắng mặt
   là vắng mặt, không được suy ra*). Kèm cột `n_rules_null` để biết bộ lọc đã "chết" bao
   nhiêu phần.

### ⚠️ P/E hiện chỉ có 2 / 781 mã

`pe_ttm` tồn tại ở đúng ba bảng — `silver_schema.stocks_basic_financials_bank_fa`,
`gold_schema.stocks_financials_bank_fa`, `unified_schema_vcb.pool__fa` — và cả ba đều chỉ
có **VCB và ACB**, vì đó là chart of accounts của **ngân hàng**; `_ingest_unified_pool_fa`
raise với mã phi ngân hàng. Vẫn ship cột `scr_pe` = NULL và `ok_pe` = NULL ngay từ đầu để
hình dạng đã đúng sẵn khi chain financials phi ngân hàng có mặt.

---

## 7. Ba cái bẫy kỹ thuật đã biết trước

1. ⚠️ **Lọc giá phải dùng `close_raw`, KHÔNG dùng `close_adjust`.** `close_adjust` hồi tố
   chia tách, nên một mã giao dịch ở 60.000₫ năm 2015 rồi chia 6:1 sẽ mang giá điều chỉnh
   10.000₫ cho năm 2015 và bị loại oan. CLAUDE.md §3 đã ghi `open/high/low` của silver là
   **RAW** và bám `close_raw` — đây là cùng một đường nối, nhìn từ phía kia.
2. ⚠️ **`value_matched` tính bằng TỶ VND**, còn `foreign_*_value` / `prop_*_val` là VND
   trần. Đo được ngày 2026-08-21: trung vị `value_matched` = **0,12** (tức 120 triệu),
   max 5.722,99. Một bản nháp trước đây đã báo cáo tỷ lệ tham gia 215.150.099 vì lỗi này.
3. ⚠️ **`percentile_cont` là ordered-set aggregate, không nhận `OVER`** — PostgreSQL
   không có trung vị trượt. Nhưng §2.2 đã chọn lọc theo MÃ, nên phân vị được tính **một
   lần trên toàn lịch sử mỗi mã**, là aggregate thường. Bẫy này chỉ quay lại nếu đổi sang
   lọc theo (ngày, mã).

---

## 8. Các bước, theo thứ tự

| # | việc | file | ước lượng |
|---|---|---|---|
| 0 | **`P1` / `FRZ-1` — re-scrape 143 mã đóng băng** | — | ~1 h + scrape. ⚠️ **Phải xong trước**: sau 2026-06-11 chỉ còn **7/150** mã có dữ liệu |
| 1 | chốt ngưỡng + lượng từ theo bảng §3 | — | quyết định, không phải code |
| 2 | `SCREEN_SCHEMA = "screen_schema"` | [utils/constants.py:114](src/utils/constants.py#L114) | phút |
| 3 | `_ingest_screen_stock_day` + `_ingest_screen_ticker` — **CTAS, không round-trip pandas** (rule 15) | [preprocessor.py](src/orchestration/preprocessor/preprocessor.py) | ~½ ngày |
| 4 | asset mới, `key_prefix=["screen"]`, `group_name="screen"` | `src/orchestration/assets/screen.py` *(mới)* | ~1 h |
| 5 | đăng ký asset + cập nhật docstring/số asset | [definitions.py](src/orchestration/definitions.py) | phút |
| 6 | ⚠️ **liệt kê trong `config.json`** — loader **raise** với asset không được liệt kê (rule 12) | `src/orchestration/config.json` | phút |
| 7 | nối vào chỗ định nghĩa universe | [cross_sectional.py:190](src/feature_selection/cross_sectional.py#L190) + [kgpu/export.py:196](src/kaggle_gpu/kgpu/export.py#L196) | ~2 h |
| 8 | **chạy lại chain**: selection → final_features → dataset → model → walk-forward | [RUNBOOK §3a/§3c](RUNBOOK.md) | selection ~44 min (T4) + sweep ~33 min + scoring ~9 min |
| 9 | ghi kết quả + §5 vào CLAUDE.md; mọi mục đã xong **xoá khỏi file này, không tick** | — | — |

⚠️ **Bước 8 cần quota Kaggle.** Selection ở 162 kênh mất **44 m 12 s** trên T4 (đo
2026-08-21); 233 kênh **chết vì OOM bốn lần**. Ngưỡng an toàn hiện biết là 162.

---

## 9. Chuẩn thành công

Chain mới **không cần thắng** chain cũ. Ba kết quả đều là kết quả dùng được:

- **`z` tăng** → tầng lọc bỏ đi nhiễu, và đó là phát hiện.
- **`z` không đổi** → mã rác không phải nguồn của tín hiệu; `PRF-8`/`PRF-9` đã đóng hai
  đòn bẩy khác đúng theo cách này, và việc đóng thêm một đòn bẩy có giá trị riêng.
- **`z` giảm** → tín hiệu **nằm ở** những mã kém thanh khoản, tức là phần lớn +74 %/năm
  không giao dịch được. ⚠️ Đây là khả năng khó chịu nhất và cũng là khả năng mà
  [RUNBOOK §8b rule 2](RUNBOOK.md) đang ngờ tới — nên nó là lý do đáng làm nhất.

⚠️ **Ghi lại dự đoán trước khi chạy**, theo thông lệ TODO.md: *dự đoán là `z` giảm nhưng
vẫn vượt bar, và Sharpe sau chi phí giảm nhiều hơn `z`.* Nếu sai thì dự đoán sai được giữ
nguyên trong file, không sửa lại.
