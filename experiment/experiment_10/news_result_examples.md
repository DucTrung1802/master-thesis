# Ví dụ tiêu biểu — text bài báo → phản ứng giá
Sinh bởi `news_result.py`. Nhãn = **lợi suất vượt trội thị trường** sau ngày bài báo có thể tác động (`trading_date`), chia ngũ phân vị theo từng năm.

⚠️ Mọi dòng dưới đây đều **ngoài mẫu** — mô hình chưa từng thấy khi huấn luyện.

### 1. Phản ứng dương mạnh nhất

_Text có nói gì báo trước không?_

**MVN** · 2021-08-02 · `business_results_and_analysis`  
> Kinh doanh vận tải có hiệu quả, Vinalines (MVN) báo lãi quý 2/2021 đạt 375 tỷ đồng, cao gấp 6 lần cùng kỳ năm trước

Tổng Công ty Hàng hải Việt Nam - CTCP (UpCOM: MVN) đã công bố BCTC quý 2/2021 và lũy kế 6 tháng đầu năm 2021 với doanh thu và lợi nhuận tăng cao so với cùng kỳ. Cụ thể, riêng quý 2 doanh thu thuần đạt 3.410 tỷ đồng, tăng 34% so với cùng kỳ. Giá vốn hàng bán tă…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +14.62% | **+92.94%** | +143.52% | 4 rất tích cực | 2 trung tính | 0.24 |

<https://cafef.vn/kinh-doanh-van-tai-co-hieu-qua-vinalines-mvn-bao-lai-quy-22021-dat-375-ty-dong-cao-gap-6-lan-cung-ky-nam-truoc-20210730102208424.chn>

**HPT** · 2025-07-28 · `general_uncategorized`  
> Cổ phiếu công nghệ tại Tp.HCM tăng 650%, đang có giá 22.500 đồng, Phó Chủ tịch nói: Giá hợp lý lên đến 90.000 đồng/cp

CTCP Dịch vụ Công nghệ Tin học HPT (mã chứng khoán HPT) vừa tổ chức ĐHĐCĐ thường niên 2025, thông qua kế hoạch năm 2025 với doanh thu 1.400 tỷ, lợi nhuận trước thuế 33 tỷ đồng. Với chỉ tiêu trên, Công ty cho biết sẽ tiếp tục duy trì chính sách cổ tức ổn định v…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +11.33% | **+90.35%** | +29.57% | 4 rất tích cực | 3 tích cực | 0.22 |

<https://cafef.vn/co-phieu-cong-nghe-tai-tphcm-tang-650-dang-co-gia-22500-dong-pho-chu-tich-noi-gia-hop-ly-len-den-90000-dongcp-188250727225421797.chn>

**SSN** · 2019-07-08 · `major_and_insider_shareholder_transactions`  
> SSN giảm sâu, một cá nhân vừa tranh thủ mua thêm hơn 4 triệu cổ phần Thủy sản Sài Gòn

Ông Huỳnh Cao Tuấn, một cổ đông lớn vừa chi hơn 10 tỷ đồng để mua thêm hơn 4 triệu cổ phiếu SSN của CTCP XNK Thủy sản Sài Gòn (Seaprodex Saigon). Giao dịch thực hiện theo phương thức thỏa thuận ngày 28/6/2019 với giá thỏa thuận bình quân 2.700 đồng/cổ phiếu. Ô…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +11.77% | **+80.70%** | +119.74% | 4 rất tích cực | 3 tích cực | 0.25 |

<https://cafef.vn/ssn-roi-tu-14000-xuong-2000-dong-mot-ca-nhan-vua-tranh-thu-mua-them-hon-4-trieu-co-phieu-20190705175910406.chn>

**VNX** · 2019-04-16 · `general_uncategorized`  
> 10 cổ phiếu tăng/giảm mạnh nhất tuần: VIM và VNX tăng trên 90%, VHG tiếp tục gây chú ý

Kết thúc tuần giao dịch, VN-Index đứng mức 982,9 điểm, giảm 0,64% so với tuần trước. HNX-Index giảm 0,16% xuống 107,7 điểm. Các cổ phiếu vốn hóa lớn tiếp tục có sự phân hóa và biến động giằng co. Trong khi đó, nhóm cổ phiếu nhỏ lại thu hút được sự quan tâm của…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +13.35% | **+69.69%** | +237.81% | 4 rất tích cực | 0 rất tiêu cực | 0.30 |

<https://cafef.vn/10-co-phieu-tanggiam-manh-nhat-tuan-vim-va-vnx-tang-tren-90-vhg-tiep-tuc-gay-chu-y-20190413093725073.chn>

**SRA** · 2018-10-09 · `major_and_insider_shareholder_transactions`  
> SRA quay đầu tăng mạnh, Chủ tịch Sara Việt Nam bắt đầu đăng ký mua vào

Ông Đặng Quang Nam, Chủ tịch HĐQT CTCP Sara Việt Nam (mã chứng khoán SRA) vừa thông báo đăng ký mua vào 300.000 cổ phiếu SRA vì nhu cầu của cá nhân. Giao dịch dự kiến thực hiện từ 9/10 đến 7/11/2018. Trước giao dịch này ông Đặng Quang Nam không sở hữu cổ phiếu…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +10.39% | **+62.33%** | +82.41% | 4 rất tích cực | 0 rất tiêu cực | 0.28 |

<https://cafef.vn/sra-quay-dau-tang-manh-chu-tich-sara-viet-nam-bat-dau-dang-ky-mua-vao-20181009074428437.chn>

**SRA** · 2018-10-10 · `general_uncategorized`  
> SRA tiếp tục lãi đột biến 31 tỷ đồng trong quý 3, EPS 9 tháng đạt gần 30.000 đồng

Số liệu nổi bật: + Doanh thu quý 3 tăng từ 2,2 tỷ lên 132 tỷ đồng + Lãi ròng quý 3 đạt 30,6 tỷ đồng, gấp 77 lần cùng kỳ + Lãi ròng 9 tháng đạt 59,5 tỷ đồng tương ứng lãi cơ bản trên cổ phiếu đạt 29.760 đồng Quý 3/2018, CTCP Sara Việt Nam (HNX: SRA) tiếp tục gh…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +12.30% | **+61.40%** | +82.15% | 4 rất tích cực | 0 rất tiêu cực | 0.24 |

<https://cafef.vn/sra-tiep-tuc-lai-dot-bien-31-ty-dong-trong-quy-3-eps-9-thang-dat-gan-30000-dong-20181009162050868.chn>

### 2. Phản ứng âm mạnh nhất

_Tin xấu có đọc ra được là xấu không?_

**HVA** · 2026-03-23 · `general_uncategorized`  
> Công ty có quan hệ mật thiết với ONUS và HanaGold: Vốn mỏng, lợi nhuận cả năm chỉ vài tỷ nhưng ôm tham vọng làm sàn tài sản số

Từ chiều ngày 20/3, ứng dụng tiền mã hóa ONUS không thể đăng nhập hay thực hiện các thao tác lấy mật khẩu qua máy chủ. Người dùng app này quyền tiếp cận với tài sản số của mình, đang lưu trữ trên hệ thống này. Tương tự, nhiều khách hàng của HanaGold – fintech…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -15.35% | **-46.75%** | -55.06% | 0 rất tiêu cực | 0 rất tiêu cực | 0.22 |

<https://cafef.vn/cong-ty-co-quan-he-mat-thiet-voi-onus-va-hanagold-von-mong-loi-nhuan-ca-nam-chi-vai-ty-nhung-om-tham-vong-lam-san-tai-san-so-188260322000115613.chn>

**DDG** · 2023-04-25 · `general_uncategorized`  
> Kỳ lạ: Cổ phiếu chia 3 thị giá sau 11 phiên sàn liên tiếp, dư bán sàn hơn 1/4 công ty, doanh nghiệp khẳng định kinh doanh bình thường

Trong bối cảnh thị trường xuất hiện nhiều rung lắc, cổ phiếu DDG của CTCP Đầu tư Công nghiệp Xuất nhập khẩu Đông Dương (Indochine Imex) vẫn “nằm im” tại mức giá sàn phiên thứ 11 liên tiếp. Khối lượng khớp lệnh chỉ “heo hút” vài chục nghìn đơn vị trong khi lượn…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -10.27% | **-40.90%** | -38.66% | 0 rất tiêu cực | 0 rất tiêu cực | 0.32 |

<https://cafef.vn/ky-la-co-phieu-chia-3-thi-gia-sau-11-phien-san-lien-tiep-du-ban-san-hon-14-cong-ty-doanh-nghiep-khang-dinh-kinh-doanh-binh-thuong-188230424220254805.chn>

**DDG** · 2023-04-17 · `general_uncategorized`  
> Cổ phiếu doanh nghiệp cung ứng hệ thống cho các dự án của Heineken, Biwase bỗng chốc "bốc hơi" 40% chỉ sau 5 phiên

Tuần vừa qua là khoảng thời gian đáng quên đối với cổ đông của CTCP Đầu tư Công nghiệp Xuất nhập khẩu Đông Dương (Indochine Imex – mã DDG). Cổ phiếu DDG bất ngờ “trắng sàn” cả 5 phiên, thậm chí có phiên gần như “tắt” thanh khoản. Chỉ sau đúng 1 tuần, cổ phiếu…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -10.02% | **-40.30%** | -64.71% | 0 rất tiêu cực | 0 rất tiêu cực | 0.32 |

<https://cafef.vn/co-phieu-doanh-nghiep-cung-ung-he-thong-cho-cac-du-an-cua-heineken-biwase-bong-choc-boc-hoi-40-chi-sau-5-phien-18823041521342674.chn>

**DDG** · 2023-04-19 · `general_uncategorized`  
> Cổ phiếu DDG chia đôi, vốn hoá “bốc hơi” 1.300 tỷ sau vài phiên, doanh nghiệp nói gì?

CTCP Đầu tư Công nghiệp Xuất nhập khẩu Đông Dương (Indochine Imex – mã DDG) vừa có văn bản giải trình gửi Sở Giao dịch Chứng khoán Hà Nội (HNX) về việc cổ phiếu giảm sàn 5 phiên liên tiếp (10-14/4/2023). DDG cho biết, công ty hiện đang sản xuất kinh doanh bình…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -10.03% | **-40.23%** | -65.25% | 0 rất tiêu cực | 0 rất tiêu cực | 0.28 |

<https://cafef.vn/co-phieu-ddg-chia-doi-von-hoa-boc-hoi-1300-ty-sau-vai-phien-doanh-nghiep-noi-gi-188230418100918732.chn>

**PDR** · 2022-11-15 · `general_uncategorized`  
> Từng giàu thứ 6 sàn chứng khoán, tài sản của Chủ tịch HĐQT Phát Đạt đã bay mất gần tỷ đô

Mới đây, CTCP Phát triển Bất động sản Phát Đạt (mã chứng khoán: PDR) có văn bản thông báo bổ sung tài sản đảm bảo. Động thái này diễn ra trong bối cảnh thị giá PDR giảm sâu, chung với áp lực bán giải chấp toàn thị trường. Tài sản đảm bảo bổ sung đợt này của PD…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -9.58% | **-38.11%** | -58.15% | 0 rất tiêu cực | 0 rất tiêu cực | 0.28 |

<https://cafef.vn/tung-giau-thu-6-san-chung-khoan-tai-san-cua-chu-tich-hdqt-phat-dat-da-bay-mat-gan-ty-do-20221114135432082.chn>

**PDR** · 2022-11-15 · `general_uncategorized`  
> Doanh nghiệp bất động sản giữa áp lực “xoay vốn”: Trong quý 3, Novaland, Nam Long, Phát Đạt… tìm kiếm dòng vốn từ đâu?

Nguồn vốn đang là vấn đề lớn nhất với các doanh nghiệp, đặc biệt là doanh nghiệp bất động sản khi tín dụng ngân hàng bị thu hẹp, kênh trái phiếu doanh nghiệp đứt gãy và thị trường cổ phiếu giảm mạnh. Trong bối cảnh đó, cấu trúc vốn của các doanh nghiệp bất độn…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -9.58% | **-38.11%** | -58.15% | 0 rất tiêu cực | 0 rất tiêu cực | 0.28 |

<https://cafef.vn/doanh-nghiep-bat-dong-san-giua-ap-luc-xoay-von-trong-quy-3-novaland-nam-long-phat-dat-tim-kiem-dong-von-tu-dau-20221114150752581.chn>

### 3. Mô hình TỰ TIN và ĐÚNG

_Nếu có tín hiệu thật thì nó nằm ở đây — kiểm tra xem có phải chỉ là nhận ra tên mã / câu khuôn mẫu không (bẫy của paper 61)._

**VIB** · 2022-11-04 · `general_uncategorized`  
> VIB và Lazada tặng loạt ưu đãi tiền triệu cho người dùng

Giảm liền 2 triệu cho siêu phẩm iPhone14 Chuyện người dùng năm nào cũng xếp hàng lúc 0h để mua iPhone đời mới, cho thấy sức hấp dẫn không thể chối từ của siêu phẩm này. Các nền tảng thương mại điện tử hàng đầu như Lazada không đứng ngoài cuộc. Tín đồ "táo khuy…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -3.82% | **-4.24%** | +0.64% | 1 tiêu cực | 1 tiêu cực | 0.46 |

<https://cafef.vn/vib-va-lazada-tang-loat-uu-dai-tien-trieu-cho-nguoi-dung-20221104081801405.chn>

**CTP** · 2018-06-28 · `general_uncategorized`  
> Cận cảnh chiếc máy đào bitcoin 3.400 USD gây sốt của hãng máy ảnh Kodak

CTCP Cà phê Thương Phú (CTP: HNX) đã công bố tờ trình ĐHĐCĐTN năm 2018 với những con số kém khả quan về triển vọng kinh doanh trong năm nay. Theo đó, doanh thu dự kiến của Thương Phú cho năm 2018 đạt khoảng 180 tỷ đồng, giảm 20% so với năm trước trong khi lợi…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +1.55% | **-5.32%** | -1.33% | 0 rất tiêu cực | 0 rất tiêu cực | 0.42 |

<https://cafef.vn/gia-co-phieu-pha-day-thuong-phu-muon-phat-hanh-tang-von-voi-gia-chao-ban-gap-doi-thi-gia-20180604144008878.chn>

**PVD** · 2021-09-06 · `business_results_and_analysis`  
> Cổ phiếu PV Drilling (PVD) bị cắt margin do lỗ ròng 6 tháng đầu năm gần 98 tỷ đồng

Mới đây, Sở Giao dịch Chứng khoán TP HCM (HoSE) đã thông báo đưa cổ phiếu của Tổng Công ty Cổ phần Khoan và Dịch vụ Khoan Dầu khí (PV Drilling, HoSE: PVD ) vào danh sách chứng khoán không đủ điều kiện giao dịch ký quỹ (margin). Điều này là do lợi nhuận sau thu…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -3.12% | **-6.51%** | -0.15% | 0 rất tiêu cực | 0 rất tiêu cực | 0.42 |

<https://cafef.vn/co-phieu-pv-drilling-pvd-bi-cat-margin-do-lo-rong-6-thang-dau-nam-gan-98-ty-dong-20210902154738265.chn>

**YEG** · 2022-04-15 · `business_results_and_analysis`  
> Yeah1 (YEG) "thoát nạn" cổ phiếu kiểm soát trên HoSE nhờ có lãi trong năm 2021

Theo thông báo mới nhất, Sở Giao dịch Chứng khoán Tp.HCM (HoSE) đã quyết định đưa cổ phiếu YEG của Tập đoàn Yeah1 ra khỏi diện kiểm soát kể từ ngày 15/4/2022. Lý do đưa ra bởi lợi nhuận sau thuế của cổ đông công ty mẹ năm 2021 là 19,79 tỷ đồng và lợi nhuận sau…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -4.89% | **-18.04%** | -12.38% | 0 rất tiêu cực | 0 rất tiêu cực | 0.40 |

<https://cafef.vn/yeah1-yeg-thoat-nan-co-phieu-kiem-soat-tren-hose-nho-co-lai-trong-nam-2021-20220414152345356.chn>

**ACB** · 2018-07-31 · `general_uncategorized`  
> Thấy gì từ top ngân hàng có lợi nhuận 6 tháng tăng vọt trên 100%?

Thống kế số liệu báo cáo tài chính 6 tháng đầu năm 2018 của 9 ngân hàng thương mại tư nhân và 1 ngân hàng thương mại nhà nước bao gồm ACB, HDBank, LienVietPost Bank, MBB, Sacombank, Techcombank, TPBank, Vietcombank, VIB, VPBank cho thấy ngoại trừ LienVietPost…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.82% | **-1.02%** | +3.92% | 2 trung tính | 2 trung tính | 0.39 |

<https://cafef.vn/thay-gi-tu-top-ngan-hang-co-loi-nhuan-6-thang-tang-vot-tren-100-20180730091005975.chn>

**VIB** · 2018-07-31 · `general_uncategorized`  
> Thấy gì từ top ngân hàng có lợi nhuận 6 tháng tăng vọt trên 100%?

Thống kế số liệu báo cáo tài chính 6 tháng đầu năm 2018 của 9 ngân hàng thương mại tư nhân và 1 ngân hàng thương mại nhà nước bao gồm ACB, HDBank, LienVietPost Bank, MBB, Sacombank, Techcombank, TPBank, Vietcombank, VIB, VPBank cho thấy ngoại trừ LienVietPost…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +0.50% | **-0.29%** | +4.95% | 2 trung tính | 2 trung tính | 0.39 |

<https://cafef.vn/thay-gi-tu-top-ngan-hang-co-loi-nhuan-6-thang-tang-vot-tren-100-20180730091005975.chn>

### 4. Mô hình TỰ TIN và SAI

_Chi phí của việc tin vào mô hình._

**DGW** · 2018-10-22 · `business_results_and_analysis`  
> Mảng điện thoại tăng trưởng mạnh, lãi ròng quý 3 của Digiworld tăng 32% lên 36,6 tỷ đồng

CTCP Thế Giới Số (Digiworld, DGW) vừa công bố BCTC quý 3 năm nay với mức lãi khá cao. Chi tiết, Công ty ghi nhận doanh thu thuần 1.742 tỷ đồng, tăng 52%, tương ứng lợi nhuận gộp 103 tỷ đồng, tăng 24% so với quý 3/2017. Theo Digiworld, tổng doanh thu tăng mạnh…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -1.90% | **-5.71%** | -3.85% | 0 rất tiêu cực | 2 trung tính | 0.50 |

<https://cafef.vn/mang-dien-thoai-tang-truong-manh-lai-rong-quy-3-cua-digiworld-tang-32-len-366-ty-dong-2018101917312389.chn>

**CEO** · 2021-11-15 · `general_uncategorized`  
> Liên tiếp thua lỗ và dòng tiền âm hơn trăm tỷ, cổ phiếu CEO vẫn dậy sóng khi tăng đến 50% thị giá sau 5 phiên kịch trần

Phiên 12/11 chứng kiến sự tăng trưởng mạnh mẽ của nhiều cổ phiếu, đặc biệt nhóm bất động sản, khép lại tuần giao dịch nhiều cảm xúc. Trong đó, mã gây nhiều chú ý là CEO của CTCP Tập đoàn C.E.O. Đầy cũng tâm điểm của giới đầu tư những ngày gần đây khi liên tục…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +10.01% | **+37.26%** | +106.34% | 4 rất tích cực | 0 rất tiêu cực | 0.46 |

<https://cafef.vn/lien-tiep-thua-lo-va-dong-tien-am-hon-tram-ty-co-phieu-ceo-van-day-song-khi-tang-den-50-thi-gia-sau-5-phien-kich-tran-2021111309203235.chn>

**NVL** · 2022-11-25 · `general_uncategorized`  
> Hơn 128 triệu cổ phiếu phiên khớp lệnh kỷ lục về tài khoản, “biệt đội giải cứu” Novaland (NVL) tạm lỗ 14%

Thị trường vừa khép lại một phiên đầy biến động nhưng không có bất ngờ nào xảy ra với cổ phiếu NVL của Novaland – cái tên được chú ý nhất trên sàn chứng khoán thời điểm hiện tại. Cổ phiếu này tiếp tục giảm sàn phiên thứ 16 liên tiếp xuống mức 21.950 đồng/cổ ph…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -2.98% | **+7.82%** | -26.29% | 4 rất tích cực | 0 rất tiêu cực | 0.46 |

<https://cafef.vn/hon-128-trieu-co-phieu-phien-khop-lenh-ky-luc-ve-tai-khoan-biet-doi-giai-cuu-novaland-nvl-tam-lo-14-2022112415485437.chn>

**YEG** · 2021-06-25 · `general_uncategorized`  
> Yeah1 (YEG): Tiếp tục bán vốn tại Yeah1 Network, cổ phiếu vẫn dò đáy

Ngày 23/6/2021, CTCP Giải trí Rồng - công ty con của Tập đoàn Yeah1 (YEG) - đã thực hiện ký kết Hợp đồng chuyển nhượng toàn bộ phần vốn góp tại Công ty Yeah1 Network PTE.LTD cho bên mua là một công ty được thành lập tại Singapore. Việc thoái vốn nằm trong kế h…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.97% | **-3.58%** | -11.63% | 1 tiêu cực | 0 rất tiêu cực | 0.45 |

<https://cafef.vn/yeah1-yeg-tiep-tuc-ban-von-tai-yeah1-network-co-phieu-van-do-day-20210624194513221.chn>

**FPT** · 2021-09-14 · `general_uncategorized`  
> Ông Đỗ Cao Bảo kể về đội ngũ đồng hành lập ra FPT: Hầu hết là Tiến sĩ Toán - Lý, bạn học với Chủ tịch Trương Gia Bình, chung giá trị sống và đam mê khoa học

…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -1.12% | **-4.18%** | -3.65% | 1 tiêu cực | 4 rất tích cực | 0.43 |

<https://cafef.vn/ong-do-cao-bao-ke-ve-doi-ngu-dong-hanh-lap-ra-fpt-hau-het-la-tien-si-toan-ly-ban-hoc-voi-chu-tich-truong-gia-binh-chung-gia-tri-song-va-dam-me-khoa-hoc-20210913152941391.chn>

**HCI** · 2020-07-08 · `general_uncategorized`  
> Một cổ phiếu bất ngờ tăng gấp 3 lần chỉ sau 9 phiên giao dịch

Việc những cổ phiếu trên thị trường chứng khoán bỗng nhiên tăng sốc – giảm sâu đã không còn là bất ngờ lớn với các nhà đầu tư. Đối với hầu hết các cổ phiếu tăng sốc – giảm sâu đều có những "câu chuyện" phía sau nó liên quan đến những tin tốt – tin xấu. Tuy nhi…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.84% | **-1.21%** | -0.92% | 2 trung tính | 0 rất tiêu cực | 0.43 |

<https://cafef.vn/mot-co-phieu-bat-ngo-tang-gap-3-lan-chi-sau-9-phien-giao-dich-20200707092128482.chn>

### 5. Trực giác ngược — tin nghe TÍCH CỰC nhưng giá GIẢM mạnh

_Chia cổ tức / kết quả kinh doanh mà giá vẫn rơi. Đây là chỗ một scorer sắc thái tổng quát sẽ sai — và cũng là lý do scorer hiện tại chấm 'VCB: chi trả cổ tức 2025' = −0,97._

**PDR** · 2022-11-14 · `business_results_and_analysis`  
> Phát Đạt (PDR) dùng 126.336,5m2 đất Vũng Tàu bổ sung tài sản đảm bảo khi lãnh đạo liên tục bị "call margin"

CTCP Phát triển Bất động sản Phát Đạt (mã chứng khoán PDR) vừa có văn bản thông báo bổ sung tài sản đảm bảo. Động thái này diễn ra trong bối cảnh thị giá PDR giảm sâu, chung với áp lực bán giải chấp toàn thị trường. Tài sản đảm bảo bổ sung đợt này của PDR gồm…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -3.29% | **-33.22%** | -59.01% | 0 rất tiêu cực | 0 rất tiêu cực | 0.25 |

<https://cafef.vn/phat-dat-pdr-dung-1263365m2-dat-vung-tau-bo-sung-tai-san-dam-bao-khi-lanh-dao-lien-tuc-bi-call-margin-20221112171442207.chn>

**NVL** · 2022-11-17 · `business_results_and_analysis`  
> Novaland giảm sàn 10 phiên liên tiếp, NovaGroup chỉ mua vào 1,8 triệu cổ phiếu NVL trên tổng số 8 triệu đã đăng ký

Trong thông báo mới nhất, Công ty cổ phần NovaGroup đã báo cáo giao dịch cổ phiếu NVL của Công ty cổ phần Tập đoàn Đầu tư Địa ốc No Va (Novaland) Cụ thể, NovaGroup đã mua vào thành công hơn 1,8 triệu trên tổng số 8 triệu cổ phiếu NVL đã đăng ký trong khoảng th…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -8.08% | **-31.85%** | -36.55% | 0 rất tiêu cực | 0 rất tiêu cực | 0.28 |

<https://cafef.vn/novaland-giam-san-10-phien-lien-tiep-novagroup-chi-mua-vao-18-trieu-co-phieu-nvl-tren-tong-so-8-trieu-da-dang-ky-20221116165854973.chn>

**VGS** · 2022-04-14 · `business_results_and_analysis`  
> Việt Đức VGPIPE (VGS) đặt kế hoạch doanh thu hợp nhất tăng nhẹ năm 2022, giảm 02 thành viên HĐQT

Công ty cổ phần ống thép Việt Đức VGPIPE (mã CK VGS ) đã công bố tài liệu họp Đại hội cổ đông thường niên năm 2022 dự kiến diễn ra vào ngày 16/04 tới đây. Theo đó, công ty lên kế hoạch doanh thu hợp nhất đạt 7.000 tỷ đồng, tăng 5% và lợi nhuận trước thuế hợp n…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -5.83% | **-28.07%** | -14.45% | 0 rất tiêu cực | 0 rất tiêu cực | 0.29 |

<https://cafef.vn/ong-thep-viet-duc-vgpipe-vgs-dat-ke-hoach-loi-nhuan-sut-giam-trong-nam-2022-2022041317471534.chn>

**DFC** · 2019-07-03 · `dividends_and_record_date`  
> Xích líp Đông Anh (DFC) chốt danh sách cổ đông trả cổ tức bằng tiền tỷ lệ 33%

Ngày 18/7 tới đây CTCP Xích líp Đông Anh (mã chứng khoán DFC) sẽ chốt danh sách cổ đông thực hiện chi trả cổ tức năm 2018 bằng tiền tỷ lệ 33%, tương ứng cổ đông sở hữu 1 cổ phiếu được nhận về 3.300 đồng. Ngày giao dịch không hưởng quyền là 17/7/2019. Thời gian…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -0.28% | **-26.07%** | -24.43% | 0 rất tiêu cực | 0 rất tiêu cực | 0.22 |

<https://cafef.vn/xich-lip-dong-anh-dfc-chot-danh-sach-co-dong-tra-co-tuc-bang-tien-ty-le-33-20190702144541625.chn>

**GDA** · 2023-09-08 · `business_results_and_analysis`  
> Cổ phiếu Tôn Đông Á (GDA) tăng 15% trong phiên đầu, bộ 3 doanh nghiệp lớn nhất ngành tôn đều đã lên sàn

Sáng nay ngày 7/9/2023, gần 115 triệu cổ phiếu CTCP Tôn Đông Á (GDA) chính thức giao dịch trên sàn UPCoM với giá chào sàn 30.000 đồng/cp. Chốt phiên giao dịch đầu tiên, GDA tăng 15% lên 34.600 đồng, tương ứng vốn hóa đạt xấp xỉ 4.000 tỷ đồng - thuộc Top 5 công…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -12.26% | **-25.59%** | -23.48% | 0 rất tiêu cực | 0 rất tiêu cực | 0.32 |

<https://cafef.vn/co-phieu-ton-dong-a-gda-tang-15-trong-phien-dau-bo-3-doanh-nghiep-lon-nhat-nganh-ton-deu-da-len-san-188230907134259195.chn>

**VHG** · 2021-01-25 · `business_results_and_analysis`  
> Cao su Quảng Nam (VHG) lỗ tiếp 78 tỷ đồng năm 2020, nâng tổng lỗ lũy kế lên 1.344 tỷ đồng

CTCP Đầu tư Cao su Quảng Nam (mã chứng khoán VHG) công bố báo tài chính quý 4/2020 – quý tiếp theo không phát sinh doanh thu. Đây cũng là quý thứ 4 công ty không phát sinh doanh thu hoạt động, tương ứng doanh thu cả năm bằng 0. Dù không phát sinh doanh thu, nh…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -10.91% | **-24.91%** | -27.94% | 0 rất tiêu cực | 0 rất tiêu cực | 0.25 |

<https://cafef.vn/cao-su-quang-nam-vhg-lo-tiep-78-ty-dong-nam-2020-nang-tong-lo-luy-ke-len-1344-ty-dong-20210122180950123.chn>

### 6. Cùng ngày, cùng mã, phản ứng khác nhau

_Nếu nhiều bài cùng một mã-ngày mà nhãn giống hệt nhau thì bài toán là 'đoán xem hôm đó là ngày nào', không phải đọc hiểu — chính là lỗi paper 61 mắc._

**NVL** · 2022-11-25 · `general_uncategorized`  
> Hơn 128 triệu cổ phiếu phiên khớp lệnh kỷ lục về tài khoản, “biệt đội giải cứu” Novaland (NVL) tạm lỗ 14%

Thị trường vừa khép lại một phiên đầy biến động nhưng không có bất ngờ nào xảy ra với cổ phiếu NVL của Novaland – cái tên được chú ý nhất trên sàn chứng khoán thời điểm hiện tại. Cổ phiếu này tiếp tục giảm sàn phiên thứ 16 liên tiếp xuống mức 21.950 đồng/cổ ph…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -2.98% | **+7.82%** | -26.29% | 4 rất tích cực | 0 rất tiêu cực | 0.46 |

<https://cafef.vn/hon-128-trieu-co-phieu-phien-khop-lenh-ky-luc-ve-tai-khoan-biet-doi-giai-cuu-novaland-nvl-tam-lo-14-2022112415485437.chn>

**FRT** · 2018-04-27 · `general_uncategorized`  
> FPT Retail (FRT) báo lãi 64 tỷ đồng trong quý 1, tăng 33% so với cùng kỳ 2017

CTCP Bán lẻ Kỹ thuật số FPT – FPT Retail (FRT) vừa công bố báo cáo tóm tắt KQKD quý 1/2018. Theo đó, doanh thu quý 1 của FPT Retail đạt 3.884 tỷ đồng, tăng 17% so với cùng kỳ năm trước và hoàn thành 24% kế hoạch năm 2018. Trong đó, 97% doanh thu đến từ chuỗi F…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +5.21% | **-6.64%** | -2.69% | 0 rất tiêu cực | 2 trung tính | 0.42 |

<https://cafef.vn/fpt-retail-frt-bao-lai-64-ty-dong-trong-quy-1-tang-33-so-voi-cung-ky-2017-20180426112120324.chn>

**PVD** · 2021-09-06 · `business_results_and_analysis`  
> Cổ phiếu PV Drilling (PVD) bị cắt margin do lỗ ròng 6 tháng đầu năm gần 98 tỷ đồng

Mới đây, Sở Giao dịch Chứng khoán TP HCM (HoSE) đã thông báo đưa cổ phiếu của Tổng Công ty Cổ phần Khoan và Dịch vụ Khoan Dầu khí (PV Drilling, HoSE: PVD ) vào danh sách chứng khoán không đủ điều kiện giao dịch ký quỹ (margin). Điều này là do lợi nhuận sau thu…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -3.12% | **-6.51%** | -0.15% | 0 rất tiêu cực | 0 rất tiêu cực | 0.42 |

<https://cafef.vn/co-phieu-pv-drilling-pvd-bi-cat-margin-do-lo-rong-6-thang-dau-nam-gan-98-ty-dong-20210902154738265.chn>

**IDI** · 2021-12-10 · `general_uncategorized`  
> Hàng loạt nhóm chat với nghìn nhà đầu tư bỗng biến mất khi "những món quà của thượng đế" giảm sàn mất thanh khoản

Cổ phiếu sàn mất thanh khoản, nhóm chat bị xoá chủ động Thời gian qua hàng loạt các nhóm chat đã được lập ra nhằm thu hút các nhà đầu tư hô hào, cung cấp thông tin về những cổ phiếu đang tăng nóng trên thị trường. Theo ghi nhận, đông đảo nhà đầu tư, đặc biệt c…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +5.80% | **-0.62%** | -5.47% | 2 trung tính | 0 rất tiêu cực | 0.40 |

<https://cafef.vn/hang-loat-nhom-chat-voi-nghin-nha-dau-tu-bong-bien-mat-khi-nhung-mon-qua-cua-thuong-de-giam-san-mat-thanh-khoan-20211209124226195.chn>

**NVL** · 2022-11-23 · `general_uncategorized`  
> Cuộc “giải cứu” được mong chờ cho cổ phiếu của Novaland - Phát Đạt và cái kết

Khép lại phiên giao dịch 22/11/2022, thị trường tiếp tục phục hồi trong nghi ngờ, khi hai mã NVL của Novaland và PDR của Bất động sản Phát Đạt vẫn giảm sàn với dư bán hàng triệu cổ phiếu. Cần nhấn mạnh, sau thông tin họp khẩn giữa các bên liên quan nhằm tháo g…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| -7.00% | **-8.88%** | -26.77% | 0 rất tiêu cực | 0 rất tiêu cực | 0.38 |

<https://cafef.vn/cuoc-giai-cuu-duoc-mong-cho-cho-co-phieu-cua-novaland-phat-dat-va-cai-ket-20221122171900902.chn>

**SHB** · 2021-10-05 · `general_uncategorized`  
> Cổ phiếu ngân hàng ngày 4/10: SHB toả sáng tăng 8%, CTG chưa ngừng làm nhà đầu tư thất vọng

Phiên giao dịch ngày 4/10, thị trường chứng khoán tăng điểm nhờ dòng tiền chảy mạnh vào nhóm cổ phiếu dầu mỏ, thép, phân bón, trong khi cổ phiếu ngân hàng tiếp tục lao dốc. Trong 27 mã giao dịch trên 3 sàn chỉ có 2 mã tăng giá, 2 mã giữ được tham chiếu còn lại…

| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |
|---|---|---|---|---|---|
| +3.88% | **-6.94%** | -9.06% | 0 rất tiêu cực | 0 rất tiêu cực | 0.38 |

<https://cafef.vn/co-phieu-ngan-hang-ngay-410-shb-toa-sang-tang-8-ctg-chua-ngung-lam-nha-dau-tu-that-vong-20211004154624742.chn>
