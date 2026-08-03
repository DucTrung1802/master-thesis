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
| +14.38% | **+98.86%** | +299.35% | 2 tích cực | 2 tích cực | 0.39 |

<https://cafef.vn/nha-dau-tu-chu-y-hang-loat-doanh-nghiep-dang-chot-quyen-nhan-co-tuc-ty-le-khung-2019042209473207.chn>

**VNX** · 2019-04-09 · `dividends_and_record_date`  
> Lịch chốt quyền nhận cổ tức bằng tiền của 10 doanh nghiệp

CTCP Đường Quảng Ngãi (QNS): Ngày 19/4 – ngày ĐKCC nhận cổ tức còn lại năm 2018 bằng tiền tỷ lệ 5% (01 cổ phiếu nhận 500 đồng). Thời gian thanh toán 9/5/2019. CTCP Tập đoàn Thiên Long (TLG): Ngày 25/4 - ngày ĐKCC nhận cổ tức đợt 1/2018 bằng tiền tỷ lệ 10% (01…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +11.27% | **+94.30%** | +233.46% | 2 tích cực | 1 trung tính | 0.36 |

<https://cafef.vn/lich-chot-quyen-nhan-co-tuc-bang-tien-cua-10-doanh-nghiep-20190408091101929.chn>

**MVN** · 2021-08-02 · `business_results_and_analysis`  
> Kinh doanh vận tải có hiệu quả, Vinalines (MVN) báo lãi quý 2/2021 đạt 375 tỷ đồng, cao gấp 6 lần cùng kỳ năm trước

Tổng Công ty Hàng hải Việt Nam - CTCP (UpCOM: MVN) đã công bố BCTC quý 2/2021 và lũy kế 6 tháng đầu năm 2021 với doanh thu và lợi nhuận tăng cao so với cùng kỳ. Cụ thể, riêng quý 2 doanh thu thuần đạt 3.410 tỷ đồng, tăng 34% so với cùng kỳ. Giá vốn hàng bán tă…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +14.62% | **+92.94%** | +143.52% | 2 tích cực | 0 tiêu cực | 0.35 |

<https://cafef.vn/kinh-doanh-van-tai-co-hieu-qua-vinalines-mvn-bao-lai-quy-22021-dat-375-ty-dong-cao-gap-6-lan-cung-ky-nam-truoc-20210730102208424.chn>

**HPT** · 2025-07-28 · `general_uncategorized`  
> Cổ phiếu công nghệ tại Tp.HCM tăng 650%, đang có giá 22.500 đồng, Phó Chủ tịch nói: Giá hợp lý lên đến 90.000 đồng/cp

CTCP Dịch vụ Công nghệ Tin học HPT (mã chứng khoán HPT) vừa tổ chức ĐHĐCĐ thường niên 2025, thông qua kế hoạch năm 2025 với doanh thu 1.400 tỷ, lợi nhuận trước thuế 33 tỷ đồng. Với chỉ tiêu trên, Công ty cho biết sẽ tiếp tục duy trì chính sách cổ tức ổn định v…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +11.33% | **+90.35%** | +29.57% | 2 tích cực | 0 tiêu cực | 0.35 |

<https://cafef.vn/co-phieu-cong-nghe-tai-tphcm-tang-650-dang-co-gia-22500-dong-pho-chu-tich-noi-gia-hop-ly-len-den-90000-dongcp-188250727225421797.chn>

**SSN** · 2019-07-08 · `major_and_insider_shareholder_transactions`  
> SSN giảm sâu, một cá nhân vừa tranh thủ mua thêm hơn 4 triệu cổ phần Thủy sản Sài Gòn

Ông Huỳnh Cao Tuấn, một cổ đông lớn vừa chi hơn 10 tỷ đồng để mua thêm hơn 4 triệu cổ phiếu SSN của CTCP XNK Thủy sản Sài Gòn (Seaprodex Saigon). Giao dịch thực hiện theo phương thức thỏa thuận ngày 28/6/2019 với giá thỏa thuận bình quân 2.700 đồng/cổ phiếu. Ô…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +11.77% | **+80.70%** | +119.74% | 2 tích cực | 0 tiêu cực | 0.37 |

<https://cafef.vn/ssn-roi-tu-14000-xuong-2000-dong-mot-ca-nhan-vua-tranh-thu-mua-them-hon-4-trieu-co-phieu-20190705175910406.chn>

**VNX** · 2019-04-11 · `general_uncategorized`  
> Nhiều cổ phiếu bất ngờ sống lại, giúp nhà đầu tư nhân ba, nhân năm tài khoản

Thị trường chứng khoán những ngày qua không có những biến động lớn, các chỉ số cứ loanh quanh tăng giảm trong "khuôn" hẹp khiến nhiều nhà đầu tư thấy nhàm chán. Nhưng, bên cạnh sự nhàm chán đó của thị trường chứng khoán chung, "những cổ phiếu bất ngờ sống lại"…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +15.25% | **+73.23%** | +241.33% | 2 tích cực | 2 tích cực | 0.35 |

<https://cafef.vn/nhieu-co-phieu-bat-ngo-song-lai-giup-nha-dau-tu-nhan-ba-nhan-nam-tai-khoan-20190410153533842.chn>

### 2. Phản ứng âm mạnh nhất

_Tin xấu có đọc ra được là xấu không?_

**CPH** · 2021-05-17 · `general_uncategorized`  
> Điểm danh những doanh nghiệp chốt quyền nhận cổ tức bằng tiền, bằng cổ phiếu và cổ phiếu thưởng tuần từ 17/5-21/5

Tuần mới từ 17/5 đến 21/5/2021 có 46 doanh nghiệp chốt danh sách cổ đông thực hiện chi trả cổ tức bằng tiền, bằng cổ phiếu và chia cổ phiếu thưởng. Trong số đó cỏ Nhiệt điện Phả Lại (PPC) trả cổ tức bằng tiền tỷ lệ xấp xỉ 19%, Nhựa Bình Minh (BMP), Traphaco (T…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +0.01% | **-46.81%** | -47.37% | 0 tiêu cực | 2 tích cực | 0.36 |

<https://cafef.vn/diem-danh-nhung-doanh-nghiep-chot-quyen-nhan-co-tuc-bang-tien-bang-co-phieu-va-co-phieu-thuong-tuan-tu-175215-20210516083631748.chn>

**HVA** · 2026-03-23 · `general_uncategorized`  
> Công ty có quan hệ mật thiết với ONUS và HanaGold: Vốn mỏng, lợi nhuận cả năm chỉ vài tỷ nhưng ôm tham vọng làm sàn tài sản số

Từ chiều ngày 20/3, ứng dụng tiền mã hóa ONUS không thể đăng nhập hay thực hiện các thao tác lấy mật khẩu qua máy chủ. Người dùng app này quyền tiếp cận với tài sản số của mình, đang lưu trữ trên hệ thống này. Tương tự, nhiều khách hàng của HanaGold – fintech…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -15.35% | **-46.75%** | -55.06% | 0 tiêu cực | 0 tiêu cực | 0.37 |

<https://cafef.vn/cong-ty-co-quan-he-mat-thiet-voi-onus-va-hanagold-von-mong-loi-nhuan-ca-nam-chi-vai-ty-nhung-om-tham-vong-lam-san-tai-san-so-188260322000115613.chn>

**CPH** · 2021-05-14 · `dividends_and_record_date`  
> Lịch chốt quyền nhận cổ tức bằng tiền của 8 doanh nghiệp

Tổng công ty Viglacera – CTCP (VGC): Ngày 25/5 – ngày ĐKCC nhận cổ tức năm 2020 bằng tiền tỷ lệ 11% (01 cổ phiếu nhận 1.100 đồng). Thời gian thanh toán 24/6/2021. CTCP Phân lân Ninh Bình (NFC): Ngày 31/5 – ngày ĐKCC nhận cổ tức năm 2020 bằng tiền tỷ lệ 6% (01…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +0.39% | **-45.68%** | -46.87% | 0 tiêu cực | 2 tích cực | 0.37 |

<https://cafef.vn/lich-chot-quyen-nhan-co-tuc-bang-tien-cua-8-doanh-nghiep-20210514082156721.chn>

**SIV** · 2020-07-07 · `general_uncategorized`  
> Điểm danh những doanh nghiệp chốt quyền nhận cổ tức bằng tiền, bằng cổ phiếu và cổ phiếu thưởng tuần 6-10/7

Tuần mới từ 6/7 đến 10/7/2020 có 31 doanh nghiệp chốt danh sách cổ đông thực hiện chi trả cổ tức bằng tiền, bằng cổ phiếu và chia cổ phiếu thưởng. Trong số đó nhà đầu tư chú ý có Tổng công ty Viglacera (VGC) trả cổ tức bằng tiền tỷ lệ 11%. Ngoài ra còn có Chứn…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.41% | **-42.20%** | -42.53% | 0 tiêu cực | 2 tích cực | 0.42 |

<https://cafef.vn/diem-danh-nhung-doanh-nghiep-chot-quyen-nhan-co-tuc-bang-tien-bang-co-phieu-va-co-phieu-thuong-tuan-6107-20200703161759622.chn>

**DDG** · 2023-04-25 · `general_uncategorized`  
> Kỳ lạ: Cổ phiếu chia 3 thị giá sau 11 phiên sàn liên tiếp, dư bán sàn hơn 1/4 công ty, doanh nghiệp khẳng định kinh doanh bình thường

Trong bối cảnh thị trường xuất hiện nhiều rung lắc, cổ phiếu DDG của CTCP Đầu tư Công nghiệp Xuất nhập khẩu Đông Dương (Indochine Imex) vẫn “nằm im” tại mức giá sàn phiên thứ 11 liên tiếp. Khối lượng khớp lệnh chỉ “heo hút” vài chục nghìn đơn vị trong khi lượn…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -10.27% | **-40.90%** | -38.66% | 0 tiêu cực | 0 tiêu cực | 0.39 |

<https://cafef.vn/ky-la-co-phieu-chia-3-thi-gia-sau-11-phien-san-lien-tiep-du-ban-san-hon-14-cong-ty-doanh-nghiep-khang-dinh-kinh-doanh-binh-thuong-188230424220254805.chn>

**DDG** · 2023-04-17 · `general_uncategorized`  
> Cổ phiếu doanh nghiệp cung ứng hệ thống cho các dự án của Heineken, Biwase bỗng chốc "bốc hơi" 40% chỉ sau 5 phiên

Tuần vừa qua là khoảng thời gian đáng quên đối với cổ đông của CTCP Đầu tư Công nghiệp Xuất nhập khẩu Đông Dương (Indochine Imex – mã DDG). Cổ phiếu DDG bất ngờ “trắng sàn” cả 5 phiên, thậm chí có phiên gần như “tắt” thanh khoản. Chỉ sau đúng 1 tuần, cổ phiếu…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -10.02% | **-40.30%** | -64.71% | 0 tiêu cực | 2 tích cực | 0.37 |

<https://cafef.vn/co-phieu-doanh-nghiep-cung-ung-he-thong-cho-cac-du-an-cua-heineken-biwase-bong-choc-boc-hoi-40-chi-sau-5-phien-18823041521342674.chn>

### 3. Mô hình TỰ TIN và ĐÚNG

_Nếu có tín hiệu thật thì nó nằm ở đây — kiểm tra xem có phải chỉ là nhận ra tên mã / câu khuôn mẫu không (bẫy của paper 61)._

**DIG** · 2020-03-12 · `business_results_and_analysis`  
> Đại gia địa ốc Vũng Tàu muốn chuyển nhượng 100ha đất trong năm 2020, dự kiến thu về 6.000 tỷ

Theo đó, đối tác là các nhà đầu tư cấp hai có khả năng tài chính mạnh và giàu kinh nghiệm đầu tư. Việc hợp tác này dự kiến mang lại nguồn thu trên 6.000 tỉ đồng. Kế hoạch ông lớn BĐS này đặt ra trong năm 2020 sẽ đạt doanh thu đạt 3.500 tỉ đồng và lợi nhuận 850…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +0.06% | **+0.23%** | -1.84% | 1 trung tính | 1 trung tính | 0.67 |

<https://cafef.vn/dai-gia-dia-oc-vung-tau-muon-chuyen-nhuong-100ha-dat-trong-nam-2020-du-kien-thu-ve-6000-ty-20200311143546421.chn>

**NSC** · 2021-02-01 · `business_results_and_analysis`  
> Giống cây trồng Việt Nam (NSC): Năm 2020 lãi 194 tỷ đồng, EPS đạt 10.817 đồng

…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +0.42% | **-0.16%** | -1.28% | 1 trung tính | 1 trung tính | 0.65 |

<https://cafef.vn/giong-cay-trong-viet-nam-nsc-nam-2020-lai-194-ty-dong-eps-dat-10817-dong-20210130161534759.chn>

**THG** · 2020-10-28 · `general_uncategorized`  
> Đầu tư và Xây dựng Tiền Giang (THG): 9 tháng lãi sau thuế 122 tỷ đồng, tăng 64% so với cùng kỳ

…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -1.06% | **-1.63%** | -0.63% | 1 trung tính | 1 trung tính | 0.60 |

<https://cafef.vn/dau-tu-va-xay-dung-tien-giang-thg-9-thang-lai-sau-thue-122-ty-dong-tang-64-so-voi-cung-ky-20201027164758912.chn>

**VIC** · 2019-12-19 · `general_uncategorized`  
> Vingroup rút hoàn toàn khỏi bán lẻ: Giải thể điện máy VinPro, sáp nhập Adayroi vào VinID

Ngày 18/12/2019, Tập đoàn Vingroup chính thức công bố rút lui khỏi mảng bán lẻ trực tiếp để tập trung nguồn lực cho Công Nghiệp – Công nghệ. Trong đó, trang thương mại điện tử Adayroi sẽ sáp nhập vào VinID; toàn bộ hệ thống siêu thị điện máy VinPro sẽ giải thể…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.28% | **-1.36%** | -1.85% | 1 trung tính | 1 trung tính | 0.58 |

<https://cafef.vn/vingroup-rut-hoan-toan-khoi-ban-le-giai-the-dien-may-vinpro-sap-nhap-adayroi-vao-vinid-20191218115728123.chn>

**ACB** · 2021-03-11 · `general_uncategorized`  
> Từ ACB, nhìn lại các thương vụ thoái vốn đình đám của Dragon Capital

…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.70% | **+0.35%** | -5.62% | 1 trung tính | 1 trung tính | 0.57 |

<https://cafef.vn/tu-acb-nhin-lai-cac-thuong-vu-thoai-von-dinh-dam-cua-dragon-capital-20210310100157013.chn>

**VNM** · 2020-09-21 · `personnel_changes`  
> Ông Nguyễn Bá Dương rời Hội đồng quản trị Vinamilk

Theo thông tin từ Công ty cổ phần Sữa Việt Nam (Vinamilk), ngày 17/9 vừa qua, ông Nguyễn Bá Dương đã có đơn từ nhiệm chức vụ thành viên Hội đồng quản trị Vinamilk. Nguyên nhân ông Nguyễn Bá Dương từ nhiệm là do sức khỏe cá nhân nên không thể sắp xếp đủ thời gi…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.48% | **-1.97%** | -1.09% | 1 trung tính | 1 trung tính | 0.57 |

<https://cafef.vn/ong-nguyen-ba-duong-roi-hoi-dong-quan-tri-vinamilk-20200918104755943.chn>

### 4. Mô hình TỰ TIN và SAI

_Chi phí của việc tin vào mô hình._

**VTR** · 2020-03-04 · `capital_increase_and_treasury_shares`  
> Giữa tâm dịch Covid-19, Vietravel chốt bán cổ phiếu cho đối tác chiến lược bằng 1/4 thị giá

…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +3.19% | **+3.53%** | +3.68% | 2 tích cực | 1 trung tính | 0.62 |

<https://cafef.vn/giua-tam-dich-covid19-vietravel-chot-ban-co-phieu-cho-doi-tac-chien-luoc-bang-14-thi-gia-20200303104249426.chn>

**FCN** · 2026-06-11 · `general_uncategorized`  
> Khi hạ tầng ngầm mở đường cho kỷ nguyên TOD

…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +0.56% | **+1.44%** | +0.67% | 2 tích cực | 1 trung tính | 0.59 |

<https://cafef.vn/khi-ha-tang-ngam-mo-duong-cho-ky-nguyen-tod-188260610094932599.chn>

**DHM** · 2021-01-25 · `business_results_and_analysis`  
> Khoáng sản Dương Hiếu (DHM) báo lỗ tới 55 tỷ đồng trong quý 4

CTCP Thương mại và khai thác khoáng sản Dương Hiếu (mã CK: DHM) đã công bố BCTC quý 4/2020 và lũy kế cả năm 2020 với khoản thua lỗ lớn. Theo đó riêng quý 4 doanh thu thuần đạt 182 tỷ đồng giảm 18% so với cùng kỳ, giá vốn hàng bán ngốn gần hết doanh thu thuần n…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +1.36% | **+9.00%** | +9.04% | 2 tích cực | 0 tiêu cực | 0.55 |

<https://cafef.vn/khoang-san-duong-hieu-dhm-bao-lo-toi-55-ty-dong-trong-quy-4-20210121154031585.chn>

**DHG** · 2020-07-20 · `general_uncategorized`  
> Dược Hậu Giang (DHG) đặt kế hoạch thận trọng sau khi về với người Nhật, sự hỗ trợ từ Taisho sẽ chỉ rõ nét từ năm 2022

Chính thức trở thành công ty con của Taisho (1 công ty dược phẩm Nhật Bản nắm giữ 51% cổ phần), Dược Hậu Giang (DHG) trong lần chia sẻ với nhà đầu tư mới đây cho biết sự hỗ trợ từ Taisho sẽ có tác động rõ nét hơn đến diễn biến kinh d…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +1.07% | **+4.78%** | +3.62% | 2 tích cực | 1 trung tính | 0.55 |

<https://cafef.vn/duoc-hau-giang-dhg-dat-ke-hoach-than-trong-sau-khi-ve-voi-nguoi-nhat-su-ho-tro-tu-taisho-se-chi-ro-net-tu-nam-2022-2020071612122308.chn>

**BID** · 2019-11-14 · `general_uncategorized`  
> Hành trình trở thành ngân hàng có vốn điều lệ lớn nhất Việt Nam của BIDV

Hành trình trở thành ngân hàng có vốn điều lệ lớn nhất Việt Nam của BIDV…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -1.23% | **-2.72%** | -3.78% | 0 tiêu cực | 1 trung tính | 0.53 |

<https://cafef.vn/hanh-trinh-tro-thanh-ngan-hang-co-von-dieu-le-lon-nhat-viet-nam-cua-bidv-20191113163647734.chn>

**PNJ** · 2020-08-19 · `major_and_insider_shareholder_transactions`  
> Nhóm quỹ Dragon Capital vừa mua thêm hơn 2 triệu cổ phiếu PNJ, nâng tỷ lệ sở hữu lên gần 10%

Theo tin từ Sở GDCK TP.HCM (HoSE), nhóm quỹ do Dragon Capital quản lý đã mua vào gần 2,18 triệu cổ phiếu PNJ, qua đó nâng số lượng sở hữu lên gần 20,88 triệu cổ phiếu, tương ứng 9,27% lượng cổ phiếu lưu hành của công ty. Giao dịch được thực hiện vào ngày 13/8/…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +0.11% | **+3.64%** | +6.38% | 2 tích cực | 0 tiêu cực | 0.53 |

<https://cafef.vn/nhom-quy-dragon-capital-vua-mua-them-hon-2-trieu-co-phieu-pnj-nang-ty-le-so-huu-len-gan-10-202008181641092.chn>

### 5. Trực giác ngược — tin nghe TÍCH CỰC nhưng giá GIẢM mạnh

_Chia cổ tức / kết quả kinh doanh mà giá vẫn rơi. Đây là chỗ một scorer sắc thái tổng quát sẽ sai — và cũng là lý do scorer hiện tại chấm 'VCB: chi trả cổ tức 2025' = −0,97._

**CPH** · 2021-05-14 · `dividends_and_record_date`  
> Lịch chốt quyền nhận cổ tức bằng tiền của 8 doanh nghiệp

Tổng công ty Viglacera – CTCP (VGC): Ngày 25/5 – ngày ĐKCC nhận cổ tức năm 2020 bằng tiền tỷ lệ 11% (01 cổ phiếu nhận 1.100 đồng). Thời gian thanh toán 24/6/2021. CTCP Phân lân Ninh Bình (NFC): Ngày 31/5 – ngày ĐKCC nhận cổ tức năm 2020 bằng tiền tỷ lệ 6% (01…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +0.39% | **-45.68%** | -46.87% | 0 tiêu cực | 2 tích cực | 0.37 |

<https://cafef.vn/lich-chot-quyen-nhan-co-tuc-bang-tien-cua-8-doanh-nghiep-20210514082156721.chn>

**DFC** · 2019-07-04 · `dividends_and_record_date`  
> Lịch chốt quyền nhận cổ tức bằng tiền của 14 doanh nghiệp

Tổng Công ty Phát triển đô thị Kinh Bắc - CTCP (KBC): Ngày 15/7 – ngày ĐKCC nhận cổ tức đợt 1 từ nguồn lợi nhuận sau thuế chưa phân phối tính đến 31/12/2018 tỷ lệ 5% (01 cổ phiếu nhận 500 đồng). Thời gian thanh toán 15/8/2019. CTCP Thực phẩm đông lạnh Kido (KD…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.45% | **-37.28%** | -12.58% | 0 tiêu cực | 2 tích cực | 0.38 |

<https://cafef.vn/lich-chot-quyen-nhan-co-tuc-bang-tien-cua-14-doanh-nghiep-20190704084608828.chn>

**XDH** · 2019-03-12 · `dividends_and_record_date`  
> Điểm danh những doanh nghiệp trả cổ tức bằng tiền, bằng cổ phiếu và cổ phiếu thưởng tuần này

Tuần mới từ 11/3 đến 15/3/2019 có 20 doanh nghiệp chốt danh sách cổ đông thực hiện chi trả cổ tức bằng tiền, bằng cổ phiếu, và chia cổ phiếu thưởng. Trong số đó đáng chú ý PV GAS, Dược Hậu Giang, Sowatco... sẽ chốt quyền nhận cổ tức trong tuần này. Ngày 11/3/2…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.17% | **-36.05%** | -35.51% | 0 tiêu cực | 1 trung tính | 0.38 |

<https://cafef.vn/diem-danh-nhung-doanh-nghiep-tra-co-tuc-bang-tien-bang-co-phieu-va-co-phieu-thuong-tuan-nay-20190311085649164.chn>

**PDR** · 2022-11-14 · `business_results_and_analysis`  
> Phát Đạt (PDR) dùng 126.336,5m2 đất Vũng Tàu bổ sung tài sản đảm bảo khi lãnh đạo liên tục bị "call margin"

CTCP Phát triển Bất động sản Phát Đạt (mã chứng khoán PDR) vừa có văn bản thông báo bổ sung tài sản đảm bảo. Động thái này diễn ra trong bối cảnh thị giá PDR giảm sâu, chung với áp lực bán giải chấp toàn thị trường. Tài sản đảm bảo bổ sung đợt này của PDR gồm…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -3.29% | **-33.22%** | -59.01% | 0 tiêu cực | 2 tích cực | 0.36 |

<https://cafef.vn/phat-dat-pdr-dung-1263365m2-dat-vung-tau-bo-sung-tai-san-dam-bao-khi-lanh-dao-lien-tuc-bi-call-margin-20221112171442207.chn>

**CPH** · 2020-06-30 · `dividends_and_record_date`  
> Điểm danh những doanh nghiệp chốt quyền nhận cổ tức bằng tiền, bằng cổ phiếu và cổ phiếu thưởng tuần 29/6-3/7

Tuần mới từ 29/6 đến 3/7/2020 có 42 doanh nghiệp chốt danh sách cổ đông thực hiện chi trả cổ tức bằng tiền, bằng cổ phiếu và chia cổ phiếu thưởng. Trong đó nhà đầu tư chú ý có loạt doanh nghiệp được nhiều người quan tâm như Vinamilk (VNM), FPT Telecom (FOX)...…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -1.04% | **-33.12%** | -34.50% | 0 tiêu cực | 2 tích cực | 0.36 |

<https://cafef.vn/diem-danh-nhung-doanh-nghiep-chot-quyen-nhan-co-tuc-bang-tien-bang-co-phieu-va-co-phieu-thuong-tuan-29637-20200628182304576.chn>

**NVL** · 2022-11-17 · `business_results_and_analysis`  
> Novaland giảm sàn 10 phiên liên tiếp, NovaGroup chỉ mua vào 1,8 triệu cổ phiếu NVL trên tổng số 8 triệu đã đăng ký

Trong thông báo mới nhất, Công ty cổ phần NovaGroup đã báo cáo giao dịch cổ phiếu NVL của Công ty cổ phần Tập đoàn Đầu tư Địa ốc No Va (Novaland) Cụ thể, NovaGroup đã mua vào thành công hơn 1,8 triệu trên tổng số 8 triệu cổ phiếu NVL đã đăng ký trong khoảng th…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -8.08% | **-31.85%** | -36.55% | 0 tiêu cực | 0 tiêu cực | 0.40 |

<https://cafef.vn/novaland-giam-san-10-phien-lien-tiep-novagroup-chi-mua-vao-18-trieu-co-phieu-nvl-tren-tong-so-8-trieu-da-dang-ky-20221116165854973.chn>

### 6. Cùng ngày, cùng mã, phản ứng khác nhau

_Nếu nhiều bài cùng một mã-ngày mà nhãn giống hệt nhau thì bài toán là 'đoán xem hôm đó là ngày nào', không phải đọc hiểu — chính là lỗi paper 61 mắc._

**THG** · 2020-10-28 · `general_uncategorized`  
> Đầu tư và Xây dựng Tiền Giang (THG): 9 tháng lãi sau thuế 122 tỷ đồng, tăng 64% so với cùng kỳ

…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -1.06% | **-1.63%** | -0.63% | 1 trung tính | 1 trung tính | 0.60 |

<https://cafef.vn/dau-tu-va-xay-dung-tien-giang-thg-9-thang-lai-sau-thue-122-ty-dong-tang-64-so-voi-cung-ky-20201027164758912.chn>

**VIC** · 2019-12-19 · `general_uncategorized`  
> Vingroup rút hoàn toàn khỏi bán lẻ: Giải thể điện máy VinPro, sáp nhập Adayroi vào VinID

Ngày 18/12/2019, Tập đoàn Vingroup chính thức công bố rút lui khỏi mảng bán lẻ trực tiếp để tập trung nguồn lực cho Công Nghiệp – Công nghệ. Trong đó, trang thương mại điện tử Adayroi sẽ sáp nhập vào VinID; toàn bộ hệ thống siêu thị điện máy VinPro sẽ giải thể…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.28% | **-1.36%** | -1.85% | 1 trung tính | 1 trung tính | 0.58 |

<https://cafef.vn/vingroup-rut-hoan-toan-khoi-ban-le-giai-the-dien-may-vinpro-sap-nhap-adayroi-vao-vinid-20191218115728123.chn>

**ACB** · 2021-03-11 · `general_uncategorized`  
> Từ ACB, nhìn lại các thương vụ thoái vốn đình đám của Dragon Capital

…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.70% | **+0.35%** | -5.62% | 1 trung tính | 1 trung tính | 0.57 |

<https://cafef.vn/tu-acb-nhin-lai-cac-thuong-vu-thoai-von-dinh-dam-cua-dragon-capital-20210310100157013.chn>

**NT2** · 2020-07-14 · `general_uncategorized`  
> Điện lực dầu khí Nhơn Trạch 2 (NT2) chi hơn 430 tỷ đồng trả cổ tức bằng tiền

Ngày 24/7 tới đây CTCP Điện lực dầu khí Nhơn Trạch 2 (mã chứng khoán NT2) sẽ chốt danh sách cổ đông thực hiện chi trả cổ tức còn lại năm 2019 bằng tiền tỷ lệ 15%, tương ứng cổ đông sở hữu 1 cổ phiếu được nhận về 1.500 đồng. Thời gian thanh toán 14/8/2020. Như…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.92% | **+0.20%** | +0.59% | 1 trung tính | 1 trung tính | 0.56 |

<https://cafef.vn/dien-luc-dau-khi-nhon-trach-2-nt2-chi-hon-430-ty-dong-tra-co-tuc-bang-tien-20200713182951346.chn>

**ACB** · 2018-05-08 · `general_uncategorized`  
> Ngân hàng lại rục rịch rủ nhau giảm lãi suất

Sau khi hàng loạt nhà băng điều chỉnh giảm lãi suất tiết kiệm trong 2 tháng vừa rồi, mới đây lại có thêm một số ngân hàng nữa hạ lãi suất, thậm chí một số ngân hàng đã giảm trong tháng 3 lại tiếp tục giảm nữa. Cụ thể, trong tháng 4, Techcombank đã hai lần giảm…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -2.25% | **-2.43%** | -7.69% | 1 trung tính | 1 trung tính | 0.52 |

<https://cafef.vn/ngan-hang-lai-ru-nhau-giam-lai-suat-huy-dong-20180507171421368.chn>

**FPT** · 2025-04-16 · `business_results_and_analysis`  
> Số lượng cổ đông tham dự Đại hội FPT cao kỷ lục

Ngày 15/4, Tập đoàn FPT tổ chức Đại hội đồng cổ đông thường niên 2025, thảo luận nhiều nội dung quan trọng. Đại hội năm nay ghi nhận số lượng cổ đông tham dự tăng đột biến so với một năm trước với 2.020 cổ đông tham dự (bao gồm 1.551 cổ đông tham dự trực tiếp…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +1.11% | **+1.83%** | -1.30% | 2 tích cực | 1 trung tính | 0.52 |

<https://cafef.vn/so-luong-co-dong-tham-du-dai-hoi-fpt-cao-ky-luc-188250415144600408.chn>
