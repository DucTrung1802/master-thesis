# Ví dụ tiêu biểu — text bài báo → phản ứng giá
Sinh bởi `news_result.py`. Nhãn = **lợi suất vượt trội thị trường** sau ngày bài báo có thể tác động (`trading_date`), chia ngũ phân vị theo từng năm.

⚠️ Mọi dòng dưới đây đều **ngoài mẫu** — mô hình chưa từng thấy khi huấn luyện.

### 1. Phản ứng dương mạnh nhất

_Text có nói gì báo trước không?_

**VNX** · 2019-04-23 · `dividends_and_record_date`  
> Nhà đầu tư chú ý, hàng loạt doanh nghiệp đang chốt quyền nhận cổ tức tỷ lệ "khủng"

Những ngày cuối quý 1 đầu quý 2, lúc các doanh nghiệp đang gấp rút chuẩn bị tổ chức Đại hội cổ đông thường niên để tổng kết một năm đã qua và trình bày kế hoạch hoạt động cho năm tới, thì cũng là lúc các cổ đông háo hức chờ đợi quyết định chia cổ tức sau những…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +14.38% | **+98.86%** | +299.35% | 4 rất tích cực | 2 trung tính | 0.22 |

<https://cafef.vn/nha-dau-tu-chu-y-hang-loat-doanh-nghiep-dang-chot-quyen-nhan-co-tuc-ty-le-khung-2019042209473207.chn>

**VNX** · 2019-04-09 · `dividends_and_record_date`  
> Lịch chốt quyền nhận cổ tức bằng tiền của 10 doanh nghiệp

CTCP Đường Quảng Ngãi (QNS): Ngày 19/4 – ngày ĐKCC nhận cổ tức còn lại năm 2018 bằng tiền tỷ lệ 5% (01 cổ phiếu nhận 500 đồng). Thời gian thanh toán 9/5/2019. CTCP Tập đoàn Thiên Long (TLG): Ngày 25/4 - ngày ĐKCC nhận cổ tức đợt 1/2018 bằng tiền tỷ lệ 10% (01…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +11.27% | **+94.30%** | +233.46% | 4 rất tích cực | 3 tích cực | 0.29 |

<https://cafef.vn/lich-chot-quyen-nhan-co-tuc-bang-tien-cua-10-doanh-nghiep-20190408091101929.chn>

**MVN** · 2021-08-02 · `business_results_and_analysis`  
> Kinh doanh vận tải có hiệu quả, Vinalines (MVN) báo lãi quý 2/2021 đạt 375 tỷ đồng, cao gấp 6 lần cùng kỳ năm trước

Tổng Công ty Hàng hải Việt Nam - CTCP (UpCOM: MVN) đã công bố BCTC quý 2/2021 và lũy kế 6 tháng đầu năm 2021 với doanh thu và lợi nhuận tăng cao so với cùng kỳ. Cụ thể, riêng quý 2 doanh thu thuần đạt 3.410 tỷ đồng, tăng 34% so với cùng kỳ. Giá vốn hàng bán tă…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +14.62% | **+92.94%** | +143.52% | 4 rất tích cực | 2 trung tính | 0.23 |

<https://cafef.vn/kinh-doanh-van-tai-co-hieu-qua-vinalines-mvn-bao-lai-quy-22021-dat-375-ty-dong-cao-gap-6-lan-cung-ky-nam-truoc-20210730102208424.chn>

**HPT** · 2025-07-28 · `general_uncategorized`  
> Cổ phiếu công nghệ tại Tp.HCM tăng 650%, đang có giá 22.500 đồng, Phó Chủ tịch nói: Giá hợp lý lên đến 90.000 đồng/cp

CTCP Dịch vụ Công nghệ Tin học HPT (mã chứng khoán HPT) vừa tổ chức ĐHĐCĐ thường niên 2025, thông qua kế hoạch năm 2025 với doanh thu 1.400 tỷ, lợi nhuận trước thuế 33 tỷ đồng. Với chỉ tiêu trên, Công ty cho biết sẽ tiếp tục duy trì chính sách cổ tức ổn định v…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +11.33% | **+90.35%** | +29.57% | 4 rất tích cực | 4 rất tích cực | 0.22 |

<https://cafef.vn/co-phieu-cong-nghe-tai-tphcm-tang-650-dang-co-gia-22500-dong-pho-chu-tich-noi-gia-hop-ly-len-den-90000-dongcp-188250727225421797.chn>

**SSN** · 2019-07-08 · `major_and_insider_shareholder_transactions`  
> SSN giảm sâu, một cá nhân vừa tranh thủ mua thêm hơn 4 triệu cổ phần Thủy sản Sài Gòn

Ông Huỳnh Cao Tuấn, một cổ đông lớn vừa chi hơn 10 tỷ đồng để mua thêm hơn 4 triệu cổ phiếu SSN của CTCP XNK Thủy sản Sài Gòn (Seaprodex Saigon). Giao dịch thực hiện theo phương thức thỏa thuận ngày 28/6/2019 với giá thỏa thuận bình quân 2.700 đồng/cổ phiếu. Ô…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +11.77% | **+80.70%** | +119.74% | 4 rất tích cực | 0 rất tiêu cực | 0.25 |

<https://cafef.vn/ssn-roi-tu-14000-xuong-2000-dong-mot-ca-nhan-vua-tranh-thu-mua-them-hon-4-trieu-co-phieu-20190705175910406.chn>

**VNX** · 2019-04-11 · `general_uncategorized`  
> Nhiều cổ phiếu bất ngờ sống lại, giúp nhà đầu tư nhân ba, nhân năm tài khoản

Thị trường chứng khoán những ngày qua không có những biến động lớn, các chỉ số cứ loanh quanh tăng giảm trong "khuôn" hẹp khiến nhiều nhà đầu tư thấy nhàm chán. Nhưng, bên cạnh sự nhàm chán đó của thị trường chứng khoán chung, "những cổ phiếu bất ngờ sống lại"…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +15.25% | **+73.23%** | +241.33% | 4 rất tích cực | 0 rất tiêu cực | 0.26 |

<https://cafef.vn/nhieu-co-phieu-bat-ngo-song-lai-giup-nha-dau-tu-nhan-ba-nhan-nam-tai-khoan-20190410153533842.chn>

### 2. Phản ứng âm mạnh nhất

_Tin xấu có đọc ra được là xấu không?_

**CPH** · 2021-05-17 · `general_uncategorized`  
> Điểm danh những doanh nghiệp chốt quyền nhận cổ tức bằng tiền, bằng cổ phiếu và cổ phiếu thưởng tuần từ 17/5-21/5

Tuần mới từ 17/5 đến 21/5/2021 có 46 doanh nghiệp chốt danh sách cổ đông thực hiện chi trả cổ tức bằng tiền, bằng cổ phiếu và chia cổ phiếu thưởng. Trong số đó cỏ Nhiệt điện Phả Lại (PPC) trả cổ tức bằng tiền tỷ lệ xấp xỉ 19%, Nhựa Bình Minh (BMP), Traphaco (T…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +0.01% | **-46.81%** | -47.37% | 0 rất tiêu cực | 2 trung tính | 0.21 |

<https://cafef.vn/diem-danh-nhung-doanh-nghiep-chot-quyen-nhan-co-tuc-bang-tien-bang-co-phieu-va-co-phieu-thuong-tuan-tu-175215-20210516083631748.chn>

**HVA** · 2026-03-23 · `general_uncategorized`  
> Công ty có quan hệ mật thiết với ONUS và HanaGold: Vốn mỏng, lợi nhuận cả năm chỉ vài tỷ nhưng ôm tham vọng làm sàn tài sản số

Từ chiều ngày 20/3, ứng dụng tiền mã hóa ONUS không thể đăng nhập hay thực hiện các thao tác lấy mật khẩu qua máy chủ. Người dùng app này quyền tiếp cận với tài sản số của mình, đang lưu trữ trên hệ thống này. Tương tự, nhiều khách hàng của HanaGold – fintech…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -15.35% | **-46.75%** | -55.06% | 0 rất tiêu cực | 0 rất tiêu cực | 0.27 |

<https://cafef.vn/cong-ty-co-quan-he-mat-thiet-voi-onus-va-hanagold-von-mong-loi-nhuan-ca-nam-chi-vai-ty-nhung-om-tham-vong-lam-san-tai-san-so-188260322000115613.chn>

**CPH** · 2021-05-14 · `dividends_and_record_date`  
> Lịch chốt quyền nhận cổ tức bằng tiền của 8 doanh nghiệp

Tổng công ty Viglacera – CTCP (VGC): Ngày 25/5 – ngày ĐKCC nhận cổ tức năm 2020 bằng tiền tỷ lệ 11% (01 cổ phiếu nhận 1.100 đồng). Thời gian thanh toán 24/6/2021. CTCP Phân lân Ninh Bình (NFC): Ngày 31/5 – ngày ĐKCC nhận cổ tức năm 2020 bằng tiền tỷ lệ 6% (01…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +0.39% | **-45.68%** | -46.87% | 0 rất tiêu cực | 2 trung tính | 0.34 |

<https://cafef.vn/lich-chot-quyen-nhan-co-tuc-bang-tien-cua-8-doanh-nghiep-20210514082156721.chn>

**SIV** · 2020-07-07 · `general_uncategorized`  
> Điểm danh những doanh nghiệp chốt quyền nhận cổ tức bằng tiền, bằng cổ phiếu và cổ phiếu thưởng tuần 6-10/7

Tuần mới từ 6/7 đến 10/7/2020 có 31 doanh nghiệp chốt danh sách cổ đông thực hiện chi trả cổ tức bằng tiền, bằng cổ phiếu và chia cổ phiếu thưởng. Trong số đó nhà đầu tư chú ý có Tổng công ty Viglacera (VGC) trả cổ tức bằng tiền tỷ lệ 11%. Ngoài ra còn có Chứn…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.41% | **-42.20%** | -42.53% | 0 rất tiêu cực | 2 trung tính | 0.23 |

<https://cafef.vn/diem-danh-nhung-doanh-nghiep-chot-quyen-nhan-co-tuc-bang-tien-bang-co-phieu-va-co-phieu-thuong-tuan-6107-20200703161759622.chn>

**DDG** · 2023-04-25 · `general_uncategorized`  
> Kỳ lạ: Cổ phiếu chia 3 thị giá sau 11 phiên sàn liên tiếp, dư bán sàn hơn 1/4 công ty, doanh nghiệp khẳng định kinh doanh bình thường

Trong bối cảnh thị trường xuất hiện nhiều rung lắc, cổ phiếu DDG của CTCP Đầu tư Công nghiệp Xuất nhập khẩu Đông Dương (Indochine Imex) vẫn “nằm im” tại mức giá sàn phiên thứ 11 liên tiếp. Khối lượng khớp lệnh chỉ “heo hút” vài chục nghìn đơn vị trong khi lượn…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -10.27% | **-40.90%** | -38.66% | 0 rất tiêu cực | 0 rất tiêu cực | 0.28 |

<https://cafef.vn/ky-la-co-phieu-chia-3-thi-gia-sau-11-phien-san-lien-tiep-du-ban-san-hon-14-cong-ty-doanh-nghiep-khang-dinh-kinh-doanh-binh-thuong-188230424220254805.chn>

**DDG** · 2023-04-17 · `general_uncategorized`  
> Cổ phiếu doanh nghiệp cung ứng hệ thống cho các dự án của Heineken, Biwase bỗng chốc "bốc hơi" 40% chỉ sau 5 phiên

Tuần vừa qua là khoảng thời gian đáng quên đối với cổ đông của CTCP Đầu tư Công nghiệp Xuất nhập khẩu Đông Dương (Indochine Imex – mã DDG). Cổ phiếu DDG bất ngờ “trắng sàn” cả 5 phiên, thậm chí có phiên gần như “tắt” thanh khoản. Chỉ sau đúng 1 tuần, cổ phiếu…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -10.02% | **-40.30%** | -64.71% | 0 rất tiêu cực | 0 rất tiêu cực | 0.28 |

<https://cafef.vn/co-phieu-doanh-nghiep-cung-ung-he-thong-cho-cac-du-an-cua-heineken-biwase-bong-choc-boc-hoi-40-chi-sau-5-phien-18823041521342674.chn>

### 3. Mô hình TỰ TIN và ĐÚNG

_Nếu có tín hiệu thật thì nó nằm ở đây — kiểm tra xem có phải chỉ là nhận ra tên mã / câu khuôn mẫu không (bẫy của paper 61)._

**CTG** · 2021-09-30 · `general_uncategorized`  
> Khối ngoại bán mạnh cổ phiếu ngân hàng, nhiều mã tìm đáy mới

Phiên giao dịch ngày 29/9 thị trường chứng khoán biến động mạnh. Dòng tiền chảy vào các cổ phiếu lĩnh vực sản xuất, đầu tư công trong khi chạy khỏi dòng ngân hàng. Ghi nhận trên cả 3 sàn chỉ có 3 mã tăng giá, còn lại 24 mã giảm. Đáng chú ý, khối ngoại mạnh tay…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -2.34% | **-6.38%** | -3.88% | 0 rất tiêu cực | 0 rất tiêu cực | 0.43 |

<https://cafef.vn/khoi-ngoai-ban-manh-co-phieu-ngan-hang-nhieu-ma-tim-day-moi-20210929144005452.chn>

**KDM** · 2023-03-23 · `business_results_and_analysis`  
> Doanh thu nhiều quý bằng 0, một cổ phiếu bất động sản vẫn "bốc đầu" tăng kịch trần 5 phiên ngay khi vừa thoát đình chỉ giao dịch

Thị trường chứng khoán đang ghi nhận sự trồi sụt nhất định, chủ yếu do tâm lý nhà đầu tư bị ảnh hưởng bởi hàng loạt thông tin bủa vây. Giữa bối cảnh đó, một cổ phiếu doanh nghiệp vật liệu xây dựng đang âm thầm có những nhịp bứt phá, đặc biệt là diễn biến chỉ d…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -1.48% | **-11.66%** | -9.52% | 0 rất tiêu cực | 0 rất tiêu cực | 0.41 |

<https://cafef.vn/doanh-thu-nhieu-quy-bang-0-mot-co-phieu-bat-dong-san-van-boc-dau-tang-kich-tran-5-phien-ngay-khi-vua-thoat-dinh-chi-giao-dich-20230322105258292.chn>

**LDP** · 2024-01-02 · `general_uncategorized`  
> Ồn ào tại Dược Lâm Đồng (LDP): Biến động thượng tầng liên quan nhóm Louis Holdings, Ban Kiểm Soát lên tiếng

CTCP Dược Lâm Đồng (LDP) vừa công bố Nghị quyết Ban Kiểm soát, thông qua đề nghị HĐQT Công ty về việc xử lý các vấn đề khủng hoảng truyền thông làm ảnh hưởng đến hình ảnh và hoạt động Công ty, ảnh hưởng đến quyền và lợi ích hợp pháp của Công ty và các cổ đông.…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +1.45% | **-4.97%** | -5.20% | 0 rất tiêu cực | 0 rất tiêu cực | 0.41 |

<https://cafef.vn/on-ao-tai-duoc-lam-dong-ldp-bien-dong-thuong-tang-lien-quan-nhom-louis-holdings-ban-kiem-soat-len-tieng-188231231151323528.chn>

**MVB** · 2021-09-06 · `business_results_and_analysis`  
> Lợi nhuận nhiều doanh nghiệp biến động mạnh sau soát xét bán niên 2021

Những doanh nghiệp bị nghi ngờ khả năng hoạt động liên tục Trong báo cáo tài chính soát xét 6 tháng đầu năm 2021 của nhiều doanh nghiệp, kiểm toán đã chỉ ra nhiều vấn đề tồn tại như lỗ lũy kế lớn, các khoản vay nợ quá hạn… có nguy cơ đe dọa tới khả năng hoạt đ…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -2.10% | **-7.26%** | -0.67% | 0 rất tiêu cực | 0 rất tiêu cực | 0.40 |

<https://cafef.vn/loi-nhuan-nhieu-doanh-nghiep-bien-dong-manh-sau-soat-xet-ban-nien-2021-20210903161354.chn>

**GVR** · 2021-09-06 · `business_results_and_analysis`  
> Lợi nhuận nhiều doanh nghiệp biến động mạnh sau soát xét bán niên 2021

Những doanh nghiệp bị nghi ngờ khả năng hoạt động liên tục Trong báo cáo tài chính soát xét 6 tháng đầu năm 2021 của nhiều doanh nghiệp, kiểm toán đã chỉ ra nhiều vấn đề tồn tại như lỗ lũy kế lớn, các khoản vay nợ quá hạn… có nguy cơ đe dọa tới khả năng hoạt đ…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.32% | **-5.88%** | -10.96% | 0 rất tiêu cực | 0 rất tiêu cực | 0.40 |

<https://cafef.vn/loi-nhuan-nhieu-doanh-nghiep-bien-dong-manh-sau-soat-xet-ban-nien-2021-20210903161354.chn>

**HQC** · 2021-09-06 · `business_results_and_analysis`  
> Lợi nhuận nhiều doanh nghiệp biến động mạnh sau soát xét bán niên 2021

Những doanh nghiệp bị nghi ngờ khả năng hoạt động liên tục Trong báo cáo tài chính soát xét 6 tháng đầu năm 2021 của nhiều doanh nghiệp, kiểm toán đã chỉ ra nhiều vấn đề tồn tại như lỗ lũy kế lớn, các khoản vay nợ quá hạn… có nguy cơ đe dọa tới khả năng hoạt đ…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -3.70% | **-7.17%** | +2.05% | 0 rất tiêu cực | 0 rất tiêu cực | 0.40 |

<https://cafef.vn/loi-nhuan-nhieu-doanh-nghiep-bien-dong-manh-sau-soat-xet-ban-nien-2021-20210903161354.chn>

### 4. Mô hình TỰ TIN và SAI

_Chi phí của việc tin vào mô hình._

**SBT** · 2018-08-27 · `major_and_insider_shareholder_transactions`  
> XNK Bến Tre bán ra hơn 20 triệu phiếu SBT khi cổ phiếu này tăng giá mạnh

…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -1.86% | **-4.09%** | +0.30% | 1 tiêu cực | 0 rất tiêu cực | 0.47 |

<https://cafef.vn/xnk-ben-tre-ban-ra-hon-20-trieu-phieu-sbt-khi-co-phieu-nay-tang-gia-manh-20180824224440908.chn>

**EVG** · 2022-01-20 · `major_and_insider_shareholder_transactions`  
> EVG giảm sâu, Chủ tịch Everland đăng ký mua 3 triệu cổ phiếu

Ông Lê Đình Vinh, Chủ tịch HĐQT CTCP Tập đoàn Everland (mã chứng khoán EVG) vừa thông báo đăng ký mua 3 triệu cổ phiếu EVG để gia tăng tỷ lệ sở hữu tại công ty. Giao dịch dự kiến thực hiện theo phương thức khớp lệnh hoặc thỏa thuận từ 25/1 đến 15/2/2022. Hiện…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +0.30% | **-4.98%** | -0.28% | 1 tiêu cực | 3 tích cực | 0.46 |

<https://cafef.vn/evg-giam-sau-chu-tich-everland-dang-ky-mua-3-trieu-co-phieu-20220119222158111.chn>

**VCS** · 2018-07-06 · `general_uncategorized`  
> Chứng khoán nuôi hy vọng xanh trong nghi ngờ

Liên tục giảm điểm thậm chí hơn 41 điểm sau một ngày giao dịch, khối ngoại rút tiền, bất ổn từ tỷ giá… là những gì hiện hữu với thị trường chứng khoán (TTCK) hiện tại. Nhà đầu tư khủng hoảng, trong cơn bĩ cực chỉ số đã xuất hiện màu xanh. Đi ngược thị trường c…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +0.72% | **+13.06%** | +5.39% | 4 rất tích cực | 3 tích cực | 0.44 |

<https://cafef.vn/chung-khoan-nuoi-hy-vong-xanh-trong-nghi-ngo-20180704151600813.chn>

**FMC** · 2018-07-06 · `general_uncategorized`  
> Chứng khoán nuôi hy vọng xanh trong nghi ngờ

Liên tục giảm điểm thậm chí hơn 41 điểm sau một ngày giao dịch, khối ngoại rút tiền, bất ổn từ tỷ giá… là những gì hiện hữu với thị trường chứng khoán (TTCK) hiện tại. Nhà đầu tư khủng hoảng, trong cơn bĩ cực chỉ số đã xuất hiện màu xanh. Đi ngược thị trường c…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +1.30% | **-0.06%** | +7.46% | 2 trung tính | 3 tích cực | 0.44 |

<https://cafef.vn/chung-khoan-nuoi-hy-vong-xanh-trong-nghi-ngo-20180704151600813.chn>

**MWG** · 2018-07-06 · `general_uncategorized`  
> Chứng khoán nuôi hy vọng xanh trong nghi ngờ

Liên tục giảm điểm thậm chí hơn 41 điểm sau một ngày giao dịch, khối ngoại rút tiền, bất ổn từ tỷ giá… là những gì hiện hữu với thị trường chứng khoán (TTCK) hiện tại. Nhà đầu tư khủng hoảng, trong cơn bĩ cực chỉ số đã xuất hiện màu xanh. Đi ngược thị trường c…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -1.41% | **-1.01%** | +4.67% | 2 trung tính | 3 tích cực | 0.44 |

<https://cafef.vn/chung-khoan-nuoi-hy-vong-xanh-trong-nghi-ngo-20180704151600813.chn>

**YEG** · 2018-07-06 · `general_uncategorized`  
> Chứng khoán nuôi hy vọng xanh trong nghi ngờ

Liên tục giảm điểm thậm chí hơn 41 điểm sau một ngày giao dịch, khối ngoại rút tiền, bất ổn từ tỷ giá… là những gì hiện hữu với thị trường chứng khoán (TTCK) hiện tại. Nhà đầu tư khủng hoảng, trong cơn bĩ cực chỉ số đã xuất hiện màu xanh. Đi ngược thị trường c…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +7.00% | **+9.48%** | -0.56% | 4 rất tích cực | 3 tích cực | 0.44 |

<https://cafef.vn/chung-khoan-nuoi-hy-vong-xanh-trong-nghi-ngo-20180704151600813.chn>

### 5. Trực giác ngược — tin nghe TÍCH CỰC nhưng giá GIẢM mạnh

_Chia cổ tức / kết quả kinh doanh mà giá vẫn rơi. Đây là chỗ một scorer sắc thái tổng quát sẽ sai — và cũng là lý do scorer hiện tại chấm 'VCB: chi trả cổ tức 2025' = −0,97._

**CPH** · 2021-05-14 · `dividends_and_record_date`  
> Lịch chốt quyền nhận cổ tức bằng tiền của 8 doanh nghiệp

Tổng công ty Viglacera – CTCP (VGC): Ngày 25/5 – ngày ĐKCC nhận cổ tức năm 2020 bằng tiền tỷ lệ 11% (01 cổ phiếu nhận 1.100 đồng). Thời gian thanh toán 24/6/2021. CTCP Phân lân Ninh Bình (NFC): Ngày 31/5 – ngày ĐKCC nhận cổ tức năm 2020 bằng tiền tỷ lệ 6% (01…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +0.39% | **-45.68%** | -46.87% | 0 rất tiêu cực | 2 trung tính | 0.34 |

<https://cafef.vn/lich-chot-quyen-nhan-co-tuc-bang-tien-cua-8-doanh-nghiep-20210514082156721.chn>

**DFC** · 2019-07-04 · `dividends_and_record_date`  
> Lịch chốt quyền nhận cổ tức bằng tiền của 14 doanh nghiệp

Tổng Công ty Phát triển đô thị Kinh Bắc - CTCP (KBC): Ngày 15/7 – ngày ĐKCC nhận cổ tức đợt 1 từ nguồn lợi nhuận sau thuế chưa phân phối tính đến 31/12/2018 tỷ lệ 5% (01 cổ phiếu nhận 500 đồng). Thời gian thanh toán 15/8/2019. CTCP Thực phẩm đông lạnh Kido (KD…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.45% | **-37.28%** | -12.58% | 0 rất tiêu cực | 4 rất tích cực | 0.23 |

<https://cafef.vn/lich-chot-quyen-nhan-co-tuc-bang-tien-cua-14-doanh-nghiep-20190704084608828.chn>

**XDH** · 2019-03-12 · `dividends_and_record_date`  
> Điểm danh những doanh nghiệp trả cổ tức bằng tiền, bằng cổ phiếu và cổ phiếu thưởng tuần này

Tuần mới từ 11/3 đến 15/3/2019 có 20 doanh nghiệp chốt danh sách cổ đông thực hiện chi trả cổ tức bằng tiền, bằng cổ phiếu, và chia cổ phiếu thưởng. Trong số đó đáng chú ý PV GAS, Dược Hậu Giang, Sowatco... sẽ chốt quyền nhận cổ tức trong tuần này. Ngày 11/3/2…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.17% | **-36.05%** | -35.51% | 0 rất tiêu cực | 4 rất tích cực | 0.24 |

<https://cafef.vn/diem-danh-nhung-doanh-nghiep-tra-co-tuc-bang-tien-bang-co-phieu-va-co-phieu-thuong-tuan-nay-20190311085649164.chn>

**PDR** · 2022-11-14 · `business_results_and_analysis`  
> Phát Đạt (PDR) dùng 126.336,5m2 đất Vũng Tàu bổ sung tài sản đảm bảo khi lãnh đạo liên tục bị "call margin"

CTCP Phát triển Bất động sản Phát Đạt (mã chứng khoán PDR) vừa có văn bản thông báo bổ sung tài sản đảm bảo. Động thái này diễn ra trong bối cảnh thị giá PDR giảm sâu, chung với áp lực bán giải chấp toàn thị trường. Tài sản đảm bảo bổ sung đợt này của PDR gồm…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -3.29% | **-33.22%** | -59.01% | 0 rất tiêu cực | 4 rất tích cực | 0.24 |

<https://cafef.vn/phat-dat-pdr-dung-1263365m2-dat-vung-tau-bo-sung-tai-san-dam-bao-khi-lanh-dao-lien-tuc-bi-call-margin-20221112171442207.chn>

**CPH** · 2020-06-30 · `dividends_and_record_date`  
> Điểm danh những doanh nghiệp chốt quyền nhận cổ tức bằng tiền, bằng cổ phiếu và cổ phiếu thưởng tuần 29/6-3/7

Tuần mới từ 29/6 đến 3/7/2020 có 42 doanh nghiệp chốt danh sách cổ đông thực hiện chi trả cổ tức bằng tiền, bằng cổ phiếu và chia cổ phiếu thưởng. Trong đó nhà đầu tư chú ý có loạt doanh nghiệp được nhiều người quan tâm như Vinamilk (VNM), FPT Telecom (FOX)...…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -1.04% | **-33.12%** | -34.50% | 0 rất tiêu cực | 2 trung tính | 0.23 |

<https://cafef.vn/diem-danh-nhung-doanh-nghiep-chot-quyen-nhan-co-tuc-bang-tien-bang-co-phieu-va-co-phieu-thuong-tuan-29637-20200628182304576.chn>

**NVL** · 2022-11-17 · `business_results_and_analysis`  
> Novaland giảm sàn 10 phiên liên tiếp, NovaGroup chỉ mua vào 1,8 triệu cổ phiếu NVL trên tổng số 8 triệu đã đăng ký

Trong thông báo mới nhất, Công ty cổ phần NovaGroup đã báo cáo giao dịch cổ phiếu NVL của Công ty cổ phần Tập đoàn Đầu tư Địa ốc No Va (Novaland) Cụ thể, NovaGroup đã mua vào thành công hơn 1,8 triệu trên tổng số 8 triệu cổ phiếu NVL đã đăng ký trong khoảng th…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -8.08% | **-31.85%** | -36.55% | 0 rất tiêu cực | 0 rất tiêu cực | 0.25 |

<https://cafef.vn/novaland-giam-san-10-phien-lien-tiep-novagroup-chi-mua-vao-18-trieu-co-phieu-nvl-tren-tong-so-8-trieu-da-dang-ky-20221116165854973.chn>

### 6. Cùng ngày, cùng mã, phản ứng khác nhau

_Nếu nhiều bài cùng một mã-ngày mà nhãn giống hệt nhau thì bài toán là 'đoán xem hôm đó là ngày nào', không phải đọc hiểu — chính là lỗi paper 61 mắc._

**SBT** · 2018-08-27 · `major_and_insider_shareholder_transactions`  
> XNK Bến Tre bán ra hơn 20 triệu phiếu SBT khi cổ phiếu này tăng giá mạnh

…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -1.86% | **-4.09%** | +0.30% | 1 tiêu cực | 0 rất tiêu cực | 0.47 |

<https://cafef.vn/xnk-ben-tre-ban-ra-hon-20-trieu-phieu-sbt-khi-co-phieu-nay-tang-gia-manh-20180824224440908.chn>

**VCS** · 2018-07-06 · `general_uncategorized`  
> Chứng khoán nuôi hy vọng xanh trong nghi ngờ

Liên tục giảm điểm thậm chí hơn 41 điểm sau một ngày giao dịch, khối ngoại rút tiền, bất ổn từ tỷ giá… là những gì hiện hữu với thị trường chứng khoán (TTCK) hiện tại. Nhà đầu tư khủng hoảng, trong cơn bĩ cực chỉ số đã xuất hiện màu xanh. Đi ngược thị trường c…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +0.72% | **+13.06%** | +5.39% | 4 rất tích cực | 3 tích cực | 0.44 |

<https://cafef.vn/chung-khoan-nuoi-hy-vong-xanh-trong-nghi-ngo-20180704151600813.chn>

**MWG** · 2018-07-06 · `general_uncategorized`  
> Chứng khoán nuôi hy vọng xanh trong nghi ngờ

Liên tục giảm điểm thậm chí hơn 41 điểm sau một ngày giao dịch, khối ngoại rút tiền, bất ổn từ tỷ giá… là những gì hiện hữu với thị trường chứng khoán (TTCK) hiện tại. Nhà đầu tư khủng hoảng, trong cơn bĩ cực chỉ số đã xuất hiện màu xanh. Đi ngược thị trường c…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -1.41% | **-1.01%** | +4.67% | 2 trung tính | 3 tích cực | 0.44 |

<https://cafef.vn/chung-khoan-nuoi-hy-vong-xanh-trong-nghi-ngo-20180704151600813.chn>

**LDP** · 2024-01-02 · `general_uncategorized`  
> Ồn ào tại Dược Lâm Đồng (LDP): Biến động thượng tầng liên quan nhóm Louis Holdings, Ban Kiểm Soát lên tiếng

CTCP Dược Lâm Đồng (LDP) vừa công bố Nghị quyết Ban Kiểm soát, thông qua đề nghị HĐQT Công ty về việc xử lý các vấn đề khủng hoảng truyền thông làm ảnh hưởng đến hình ảnh và hoạt động Công ty, ảnh hưởng đến quyền và lợi ích hợp pháp của Công ty và các cổ đông.…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +1.45% | **-4.97%** | -5.20% | 0 rất tiêu cực | 0 rất tiêu cực | 0.41 |

<https://cafef.vn/on-ao-tai-duoc-lam-dong-ldp-bien-dong-thuong-tang-lien-quan-nhom-louis-holdings-ban-kiem-soat-len-tieng-188231231151323528.chn>

**GVR** · 2021-09-06 · `business_results_and_analysis`  
> Lợi nhuận nhiều doanh nghiệp biến động mạnh sau soát xét bán niên 2021

Những doanh nghiệp bị nghi ngờ khả năng hoạt động liên tục Trong báo cáo tài chính soát xét 6 tháng đầu năm 2021 của nhiều doanh nghiệp, kiểm toán đã chỉ ra nhiều vấn đề tồn tại như lỗ lũy kế lớn, các khoản vay nợ quá hạn… có nguy cơ đe dọa tới khả năng hoạt đ…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.32% | **-5.88%** | -10.96% | 0 rất tiêu cực | 0 rất tiêu cực | 0.40 |

<https://cafef.vn/loi-nhuan-nhieu-doanh-nghiep-bien-dong-manh-sau-soat-xet-ban-nien-2021-20210903161354.chn>

**HQC** · 2021-09-06 · `business_results_and_analysis`  
> Lợi nhuận nhiều doanh nghiệp biến động mạnh sau soát xét bán niên 2021

Những doanh nghiệp bị nghi ngờ khả năng hoạt động liên tục Trong báo cáo tài chính soát xét 6 tháng đầu năm 2021 của nhiều doanh nghiệp, kiểm toán đã chỉ ra nhiều vấn đề tồn tại như lỗ lũy kế lớn, các khoản vay nợ quá hạn… có nguy cơ đe dọa tới khả năng hoạt đ…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -3.70% | **-7.17%** | +2.05% | 0 rất tiêu cực | 0 rất tiêu cực | 0.40 |

<https://cafef.vn/loi-nhuan-nhieu-doanh-nghiep-bien-dong-manh-sau-soat-xet-ban-nien-2021-20210903161354.chn>
