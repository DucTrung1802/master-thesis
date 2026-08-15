# `--null-draws 20` là gì, và tại sao chạy mà không có nó lại không có giá trị

> Bản dịch tiếng Việt từ tài liệu gốc, giữ nguyên cấu trúc, thuật ngữ kỹ thuật, số liệu và code.

---

## 1. Câu trả lời trong một câu

**Null draw** là một lần chạy lại hoàn chỉnh toàn bộ pipeline lựa chọn feature — gồm cả 6 ranker, ensemble, correlation prune và purged walk-forward — trên một bản sao của panel, trong đó **label/target đã được xáo trộn để chắc chắn không thể dự đoán được**.

`--null-draws 20` thực hiện việc này **20 lần** và lưu lại 20 giá trị IC tương ứng.

```powershell
python -m feature_selection.run --pools pool__basic --null-draws 20
#                                      └── 1 lần chạy thật + 20 lần chạy giả
```

20 giá trị này cho biết **chính pipeline này đạt được mức điểm bao nhiêu khi không có signal thực sự**.

Do đó, IC của run thật chỉ đáng tin khi nó vượt qua kết quả của null.

---

## 2. Tại sao không thể lấy 0 làm ngưỡng?

Trực giác thông thường là:

> IC > 0 nghĩa là feature có signal.

**Điều này sai đối với pipeline này, và điều này đã được đo thực tế.**

`run.py` chọn các channel tốt nhất dựa trên mức độ chúng fit với label. Ngay cả khi label là ngẫu nhiên, vẫn luôn có một số channel tình cờ fit tốt hơn các channel khác, sau đó pipeline chọn chúng và báo cáo mức độ fit.

Vì vậy, bước selection **có thể tạo ra IC dương từ noise**.

Đo trên `pool__basic`, VCB, `d=20, h=5`:

| Chỉ số | Giá trị |
|---|---:|
| Mean IC của 20 lần **shuffle label** | **+0.0167** |
| P95 của null | **+0.0556** |
| IC của lần chạy **thật** | **+0.0559** |

**Null distribution tập trung quanh +0.017, không phải 0.**

Chọn 12 trong 27 channel dựa trên mức độ fit với label có thể mang lại khoảng +0.017 chỉ từ noise. Vì vậy:

- `IC > 0` không nói lên điều gì.
- `IC > ~0.017` mới là mức bắt đầu có ý nghĩa.
- Ngưỡng thực tế cần vượt qua là p95: **+0.0556**.

IC thật **+0.0559** gần như chỉ vừa chạm ngưỡng này.

Nếu không có null, run sẽ báo:

> "+0.056 out-of-sample IC, 12 channels selected"

và trông giống một phát hiện.

Nhưng với null:

> **z = +1.56, p = 0.050 → không đạt.**

Thậm chí có một shuffled draw đạt **+0.0606**, cao hơn dữ liệu thật.

---

## 3. Một null draw thực sự làm gì?

Mỗi draw thực hiện:

1. Copy joined panel.
2. Thay target bằng `block_shuffle(y, block = lookback + horizon)`.
3. Chạy lại **toàn bộ selection** thông qua `factory`.
4. Tạo `FeatureSelector`, ranker, prune và purged CV mới.
5. Ghi lại mean out-of-sample IC của **feature set được chọn**.

### Shuffle phải theo BLOCK, không phải theo ROW

Các giá trị `return_5day` liên tiếp chia sẻ 4 trong 5 ngày.

Nếu shuffle từng row, ta phá autocorrelation của target, khiến null distribution quá hẹp và có thể làm cho kết quả thật vượt qua một ngưỡng giả tạo.

`block_shuffle` vì vậy xáo trộn các block liên tiếp có kích thước `d + h`, giữ nguyên đặc tính thống kê của label nhưng phá mối liên hệ giữa label và feature.

### Phải chạy lại FEATURE SELECTION trong mỗi draw

Nếu giữ nguyên feature set rồi chỉ tính lại score, ta đang đo sai vấn đề.

Bước selection chính là bước có thể làm IC tăng do overfitting vào noise.

Vì vậy mỗi null draw phải chạy lại **toàn bộ selection pipeline**.

### Cross-sectional target phải shuffle theo panel

Đối với cross-sectional target, không được shuffle row một cách đơn giản.

`cross_sectional.cross_sectional_null` sử dụng `date_block`: chuyển label thành dạng `date × ticker` rồi shuffle theo **block của ngày**.

`run.py` tự chọn phương pháp thích hợp dựa trên prefix `cs_` của target.

---

## 4. 20 giá trị null được sử dụng như thế nào?

`NullResult` biến các draw thành 5 đại lượng:

| Đại lượng | Ý nghĩa |
|---|---|
| `null_mean` | IC mà pipeline có thể đạt được chỉ từ noise |
| `null_p95_BAR` | percentile 95% của null — **ngưỡng cần vượt qua thay cho 0** |
| `null_max` | draw tốt nhất trong số các shuffled draw |
| `z_vs_null` | `(observed − null_mean) / null_sd` |
| `p_value` | `(k + 1) / (n + 1)` |

### Cảnh báo về `null_max`

Nếu:

```text
null_max >= observed
```

thì **không nên chỉ nhìn `clears_bar`**.

Phải nhìn cả `null_max`, bởi vì đã có ít nhất một lần dữ liệu ngẫu nhiên đạt bằng hoặc tốt hơn dữ liệu thật.

`pool__ta` từng vượt p95 với `z = +2.52`, nhưng một trong 20 shuffled draw vẫn đạt cao hơn dữ liệu thật. Vì vậy trường hợp đó **không được xem là pass**.

### Công thức p-value

Công thức đúng là:

```text
p = (k + 1) / (n + 1)
```

không phải:

```text
max(k, 1) / (n + 1)
```

Code cũ từng dùng công thức sai cho đến **2026-08-10** (issue **NUL-4**).

---

## 5. Tại sao là 20, không phải 5 hay 200?

**20 draws cho độ phân giải p-value khoảng 0.05 và một giá trị z có thể sử dụng được.**

P-value tối thiểu là:

```text
1 / (n + 1)
```

nên với 20 draws:

```text
1 / 21 = 0.0476
```

Do đó 20 draws **không thể phân biệt p = 0.05 với p = 0.001**. Đây là lý do các kết luận trong project chủ yếu được trình bày bằng **z**, không phải p.

- **< ~10 draws:** SD được ước lượng quá kém để z có nhiều ý nghĩa.
- **> 20 draws:** chỉ hữu ích khi kết quả thực sự borderline.
- Trong `study_3`, chạy lại toàn bộ grid với 10 draws cho verdict giống nhau ở mọi cell.

### Chi phí

Chi phí null gần như đúng bằng:

> **20 × chi phí của một lần chạy selection.**

Đo thực tế:

| Run | Channels | Draws | Chi phí |
|---|---:|---:|---:|
| `pool__basic`, `return_5day`, Kaggle T4 (2026-08-15) | 15 | 20 | **3.7 phút end-to-end** |
| `pool__forex`, `return_5day` | 357 | 20 | **41 phút** |
| `pool__forex`, `close_adjust_5day` (price LEVEL) | 357 | 0 | **2,016 giây** chỉ riêng run |
| `basic+economy_usa` | 1,458 | 20 | **~68 CPU-hours** |

Target choice có thể làm chi phí null thay đổi **13.7×**.

`lasso` chiếm phần lớn chi phí và đối với return target thì mọi coefficient đều về 0, khiến quá trình hội tụ ngay.

Cùng panel 357 channel:

```text
close_adjust_5day → 2,016 s
return_5day       →   146 s
```

Vì vậy với return target, null 20 draw vẫn có thể chạy được ngay cả trên pool rộng.

---

## 6. Nếu bỏ qua null thì downstream sẽ thế nào?

`--null-draws 0` vẫn hợp lệ.

Nó ghi:

```json
"null": null
```

vào `metadata.json`.

Sau đó `outstanding.py` chuyển thành:

```text
evidence=no_null
```

cho **mọi row trong shortlist**.

Chuỗi này tiếp tục đi qua:

```text
outstanding.csv
    ↓
final_features (table COMMENT)
    ↓
train_test_creator
    ↓
(dataset metadata.json)
    ↓
model run lineage
```

Mọi model được train từ shortlist này đều mang thông tin:

> **"chưa tính null bar"**

Đây là chủ ý của thiết kế: null bị thiếu phải được ghi nhận là thiếu, không được bỏ qua và cũng không được ngầm hiểu là pass.

### Ba giá trị của `evidence`

| Giá trị | Ý nghĩa |
|---|---|
| `no_null` | **Unknown** — chưa đo xem có vượt noise hay không |
| `failed_null` | **Đã đo**, nhưng không vượt |
| `cleared_p95_not_a_pass` | Đã vượt p95, nhưng tên gọi cho biết đây **chưa phải pass hoàn toàn** |

Đây là lý do `--null-draws` mặc định là **20** trong `run.py`, trong khi notebook có `RUN_NULL = False`.

---

## 7. `evidence` và `kept_by` là hai thứ khác nhau

Hai trường này trả lời hai câu hỏi khác nhau.

### `evidence`

Đánh giá run so với **label bị shuffle**:

> Pool này có dự đoán được target tốt hơn noise không?

### `kept_by=consensus`

Đánh giá channel so với **các method selection khác**:

> Channel này có nổi bật trong chính run này không?

Vì vậy hoàn toàn có thể có:

```text
kept_by=consensus
evidence=no_null
```

Nghĩa là 6 ranker đồng ý về một channel, nhưng toàn bộ run chưa từng được chứng minh là tốt hơn noise.

---

## 8. Phải chạy lại null bất cứ khi nào run thay đổi

Một null bar được tính cho configuration này **không có ý nghĩa đối với configuration khác**.

Đo thực tế với cùng data và cùng folds:

| Representation | Null p95 bar |
|---|---:|
| `none` | +0.053 |
| `zscore` | **+0.076** |

Chỉ thay representation đã làm bar tăng:

> **+43%**

Pool width cũng làm bar thay đổi:

```text
27 channels  → +0.0556
162 channels → +0.0740
918 channels → +0.0754
```

Vì vậy null **không được transfer** giữa:

- pools
- targets
- representations
- `d/h`
- devices

Ngược lại, cũng có trường hợp null bar gần như không đổi. Ví dụ VN100:

```text
cs-ranked features → +0.0117
RAW features       → +0.0115
```

Điều này xảy ra vì target được tính theo từng ngày và giới hạn mức noise mà pipeline có thể tạo ra.

Điểm quan trọng là:

> **Những kết luận này được đo bằng experiment, không được giả định.**

---

## 9. Null đã giúp project như thế nào?

Đây là lý do section này tồn tại trong `CLAUDE.md`.

Nếu không có null, project có thể báo cáo một single-stock predictor có vẻ hoạt động.

| Run | Báo cáo không có null | Báo cáo có null |
|---|---|---|
| VCB `pool__basic`, `d=20 h=5` | "+0.056 IC, 12 channels" | **z = +1.56 ❌**, một draw vượt |
| VCB `pool__ta`, 918 channels | "+0.112 IC" | z = +2.52, vượt p95, nhưng **null max +0.1189 > observed ⚠️** |
| VCB `pool__fa`, 162 channels | "+0.016 IC" | **z = −0.25** — thấp hơn cả null mean |
| 5-config grid | 5 config có IC dương | **Không config nào vượt null của chính nó** |
| VN100 cross-section | "+0.029 IC" | **z = +6.09 ✅** |

### Điểm quan trọng ở VN100

Ở VN100:

> **Signal không lớn hơn — mà BAR nhỏ hơn.**

IC thực tế giảm:

```text
+0.056 → +0.029
```

nhưng null mean giảm:

```text
+0.017 → +0.004
```

Kết quả vì vậy trở nên đáng tin:

```text
z = +6.09
```

Đây là ví dụ cho thấy null làm cho kết quả đáng tin hơn thay vì chỉ làm kết quả "khó pass" hơn.

---

## 10. Ví dụ Kaggle ngày 2026-08-15

Kết quả:

```text
observed +0.0494
null mean -0.0004
p95 bar  +0.0510
null MAX +0.0714
z        +1.60
p        0.1429
FAILS
```

Có một shuffled draw đạt:

```text
+0.0714
```

cao hơn observed:

```text
+0.0494
```

Vì vậy kết quả **FAIL**.

Điều này rất quan trọng vì nếu không có null, `+0.0494 IC` có thể trông như một kết quả khá tốt.

20 draws chỉ mất 3.7 phút, nhưng nó ngăn project kết luận sai rằng kết quả đó có ý nghĩa.

---

## 11. Cách sử dụng thực tế

### Mặc định — nên dùng

```powershell
python -m feature_selection.run --pools pool__basic --null-draws 20
```

### Cố ý không tính null

```powershell
python -m feature_selection.run --pools pool__ta --null-draws 0
```

Trường hợp này sẽ ghi:

```text
evidence=no_null
```

và **không được hiểu là pass**.

### Chỉ tăng lên 100 khi kết quả borderline

```powershell
python -m feature_selection.run --pools pool__basic --null-draws 100
```

---

## 12. Các quy tắc quan trọng cuối cùng

### `NULL_SEED`

```text
NULL_SEED = 7
```

Seed này được cố định và tách riêng khỏi `--random-state`.

Lý do:

> Null bar không được thay đổi chỉ vì selector seed thay đổi.

Nếu bar và giá trị mà nó đánh giá không còn comparable thì phép kiểm định mất ý nghĩa.

### Null fail không có nghĩa là bỏ observed run

`run.py` bắt lỗi, in warning, ghi `evidence=no_null` và vẫn tạo report.

Điều này đã xảy ra hai lần vào 2026-08-10.

### Draw bị lỗi vẫn được tính

Một draw raise exception **không được bỏ qua**.

Nó phải được ghi vào:

```text
failed_draws
```

Nếu chỉ giữ lại những draw thành công, null distribution sẽ bị bias.

### Đọc `null_draws.csv`

IC của từng draw được ghi vào:

```text
null_draws.csv
```

trong run folder.

Nên đọc trực tiếp 20 giá trị này.

> **20 raw IC numbers thường cho nhiều thông tin hơn chỉ một p-value.**

---

# Tóm tắt cho project

Pipeline có thể hình dung như sau:

```text
                    REAL RUN
                       │
                       ▼
              Feature Selection
                       │
                       ▼
                 Observed IC
                       │
                       │ so sánh với
                       ▼
                NULL RUN × 20
                       │
                       ▼
                 Shuffle target
                       │
                       ▼
              Feature Selection
                   lại từ đầu
                       │
                       ▼
              20 IC từ pure noise
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
      null_mean    null_p95      null_max
          │            │            │
          └────────────┼────────────┘
                       ▼
                  z_vs_null
                       │
                       ▼
          Observed IC có vượt noise?
```

## Kết luận

**Điểm quan trọng nhất:**

> Với pipeline feature selection này, **IC > 0 không đủ để kết luận có signal**.

Câu hỏi đúng phải là:

> **"Pipeline này có thể tạo ra IC bao nhiêu chỉ từ một target không có quan hệ với feature?"**

`--null-draws 20` chính là phép đo câu hỏi đó.

Vì vậy:

```text
IC > 0
    ≠
Có signal
```

Mà cần xem:

```text
Observed IC
    vs
Null distribution
```

và đặc biệt:

```text
Observed IC > null_p95
```

đồng thời phải kiểm tra:

```text
null_max < observed
```

nếu muốn tránh trường hợp noise đã từng đánh bại kết quả thật.
