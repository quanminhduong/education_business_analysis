/* ==========================================================
   1) CLEAN BẢNG SALES
   Mục tiêu:
   - Loại các bản ghi rõ ràng không hợp lệ (ví dụ: tổng tiền = 0 và không có giờ)
   - Kiểm tra nhanh xem có dòng trùng hoàn toàn (duplicate all-columns) hay không
   ========================================================== */
DELETE FROM teenup_technical_test.sales
WHERE `Tổng tiền` = 0
  AND `Giờ` IS NULL;

-- Kiểm tra dữ liệu trùng hoàn toàn (so sánh COUNT(*) với COUNT(DISTINCT *))
-- Nếu bằng nhau -> "Data clean"; khác nhau -> có duplicate
SELECT CASE
         WHEN COUNT(*) = (SELECT COUNT(*)
                          FROM (SELECT DISTINCT *
                                FROM teenup_technical_test.sales) t)
         THEN 'Data clean'
         ELSE 'Duplicate rows found'
       END AS result
FROM teenup_technical_test.sales;

/* ==========================================================
   2) FILL STT + "Mã đơn hàng" CHO BẢNG VẬN ĐƠN
   Mục tiêu:
   - File gốc thiếu STT ở các dòng chi tiết; đồng thời có dòng thiếu "Mã đơn hàng"
   - Fill-down cả STT và "Mã đơn hàng" cho toàn bộ block cho đến khi gặp STT mới

   Cách làm (giữ nguyên logic nhóm theo STT như trước):
   - base: tạo số thứ tự kỹ thuật rn (ROW_NUMBER) để có thứ tự tạm
   - grp : COUNT(STT) OVER(ORDER BY rn) -> mỗi khi gặp STT NON-NULL thì nhóm g tăng 1
   - Fill-down: lấy MAX(STT) và MAX("Mã đơn hàng") trong cùng nhóm g

   Lưu ý:
   - Nếu trong cùng nhóm g có nhiều giá trị "Mã đơn hàng" khác nhau, MAX() sẽ chọn 1 giá trị
     theo thứ tự so sánh của MySQL (không thay đổi theo yêu cầu).
   ========================================================== */
CREATE TABLE teenup_technical_test.vandon_full AS
WITH base AS (
    SELECT v.*,
           ROW_NUMBER() OVER () AS rn           -- số thứ tự kỹ thuật (không định nghĩa thứ tự)
    FROM teenup_technical_test.vandon v
),
grp AS (
    SELECT b.*,
           -- Mỗi lần gặp STT NON-NULL thì COUNT(STT) tăng -> hình thành group g
           COUNT(STT) OVER (ORDER BY rn) AS g
    FROM base b
)
SELECT
    -- Fill-down STT & Mã đơn hàng theo nhóm g
    MAX(STT) OVER (PARTITION BY g)              AS STT_filled,
    MAX(`Mã đơn hàng`) OVER (PARTITION BY g)    AS `MaDonHang_filled`,
    g.*                                         -- giữ lại cột gốc + kỹ thuật để lát dọn
FROM grp g
ORDER BY rn;

-- Dọn schema:
-- - Bỏ STT cũ, "Mã đơn hàng" cũ và các cột kỹ thuật
-- - Đổi tên STT_filled -> STT; MaDonHang_filled -> "Mã đơn hàng"
ALTER TABLE teenup_technical_test.vandon_full
  DROP COLUMN STT,
  DROP COLUMN `Mã đơn hàng`,
  DROP COLUMN g,
  DROP COLUMN rn,
  RENAME COLUMN STT_filled TO STT,
  RENAME COLUMN `MaDonHang_filled` TO `Mã đơn hàng`;

/* ==========================================================
   3) BẢNG ĐƠN HÀNG (MASTER)
   Mục tiêu:
   - Tổng hợp về mức “đơn hàng” (header): gom thông tin định tính bằng MAX, số liệu định lượng bằng SUM
   - Khoá nhóm: STT (đã được fill-down ở bước 2)
   Lưu ý:
   - MAX trên trường định tính chọn 1 giá trị không-null trong nhóm (giữ nguyên theo code).
   - Nếu nhiều giá trị khác nhau trong cùng STT -> kết quả phụ thuộc dữ liệu nguồn.
   ========================================================== */
CREATE TABLE teenup_technical_test.m_don_hang AS
SELECT

  STT,
  -- Định tính (chọn 1 giá trị trong nhóm)
  MAX(`Mã đơn hàng`)          AS `Mã đơn hàng`,
  MAX(`Ghi chú đơn hàng`)     AS `Ghi chú đơn hàng`,
  MAX(`Tags đơn hàng`)        AS `Tags đơn hàng`,
  MAX(`Nhân viên tạo đơn`)    AS `Nhân viên tạo đơn`,
  MAX(`Chi nhánh`)            AS `Chi nhánh`,
  MAX(`Nguồn`)                AS `Nguồn`,
  MAX(`Mã vận đơn`)           AS `Mã vận đơn`,
  MAX(`Tình trạng gói hàng`)  AS `Tình trạng gói hàng`,
  MAX(`Trạng thái đối tác`)   AS `Trạng thái đối tác`,
  MAX(`Lý do hủy đơn`)        AS `Lý do hủy đơn`,
  MAX(`Ngày đóng gói`)        AS `Ngày đóng gói`,
  MAX(`Ngày hẹn giao`)        AS `Ngày hẹn giao`,
  MAX(`Ngày xuất kho`)        AS `Ngày xuất kho`,
  MAX(`Ngày giao hàng`)       AS `Ngày giao hàng`,
  MAX(`Đối tác giao hàng`)    AS `Đối tác giao hàng`,
  MAX(`Dịch vụ giao hàng`)    AS `Dịch vụ giao hàng`,
  MAX(`Khối lượng(g)`)        AS `Khối lượng(g)`,
  MAX(`Kích thước(DxRxC)`)    AS `Kích thước(DxRxC)`,
  MAX(`Tên người nhận`)       AS `Tên người nhận`,
  MAX(`SĐT người nhận`)       AS `SĐT người nhận`,
  MAX(`Địa chỉ giao hàng`)    AS `Địa chỉ giao hàng`,
  MAX(`Tỉnh/Thành`)           AS `Tỉnh/Thành`,
  MAX(`Quận/Huyện`)           AS `Quận/Huyện`,
  MAX(`Phường xã`)            AS `Phường xã`,
  MAX(`Trạng thái đối soát`)  AS `Trạng thái đối soát`,
  MAX(`Hình thức thanh toán`) AS `Hình thức thanh toán`,
  MAX(`Người trả phí`)        AS `Người trả phí`,
  MAX(`Ghi chú đơn giao`)     AS `Ghi chú đơn giao`,

  -- Định lượng (cộng dồn)
  SUM(`Tiền khách phải trả cho đơn`) AS `Tiền khách phải trả cho đơn`,
  SUM(`Khách hàng đã trả`)           AS `Khách hàng đã trả`,
  SUM(`Tổng tiền thu hộ`)            AS `Tổng tiền thu hộ`,
  SUM(`Tổng tiền hàng`)              AS `Tổng tiền hàng`,
  SUM(`CK tổng đơn hàng`)            AS `CK tổng đơn hàng`,
  SUM(`Phí vận chuyển`)              AS `Phí vận chuyển`,
  SUM(`Phí trả đối tác`)             AS `Phí trả đối tác`

FROM teenup_technical_test.vandon_full
GROUP BY STT;

/* ==========================================================
   4) BẢNG CHI TIẾT ĐƠN HÀNG (LINE ITEM)
   Mục tiêu:
   - Tách phần chi tiết sản phẩm (item-level) giữ nguyên theo từng dòng
   - Phục vụ drill-down & tính toán theo sản phẩm
   ========================================================== */
CREATE TABLE teenup_technical_test.m_chi_tiet_don AS
SELECT
  STT,
  `Mã đơn hàng`,
  `Tên sản phẩm`,
  `Ghi chú sản phẩm`,
  `Số lượng`,
  `Serial`,
  `Đơn vị tính`,
  `Đơn giá`,
  `CK sản phẩm`,
  `Thuế cho từng sản phẩm`
FROM teenup_technical_test.vandon_full;

/* ==========================================================
   RFM TỪ BẢNG MASTER ĐƠN HÀNG (m_don_hang)
   Mục tiêu:
   - Tính RFM theo SĐT người nhận (mỗi khách = 1 SĐT)
   - M: tổng tiền đã trả (gmv), F: số lần mua, R: ngày kể từ lần giao gần nhất
   - Chia điểm R/F/M 1..4 theo rank-based quartile (dựa trên DENSE_RANK)
   Lưu ý:
   - DENSE_RANK giảm thiểu split tie (cùng giá trị -> cùng rank), nhưng phân mốc theo MAX(rank),
     tức là phân vị tính theo số lượng GIÁ TRỊ khác nhau, không phải số lượng khách hàng.
   - GIỮ NGUYÊN công thức và điều kiện lọc như code gốc.
   ========================================================== */
CREATE TABLE rfm_analysis AS (WITH rfm_base AS (
  SELECT
    `SĐT người nhận`,                                             -- khóa khách hàng
    `Tên người nhận`,                                   -- lấy 1 tên đại diện cho SĐT
    SUM(CAST(REPLACE(`Khách hàng đã trả`, ',', '') AS DECIMAL(18,2))) AS gmv,  -- Monetary tổng (đã bỏ dấu phẩy)
    COUNT(*) AS freq,                                                       -- Frequency: số dòng/đơn trong m_don_hang cho SĐT
    -- Recency (ngày): hôm nay - ngày giao gần nhất; ép kiểu m/d/yyyy hh:mm -> DATETIME
    DATEDIFF(CURDATE(), DATE(MAX(STR_TO_DATE(`Ngày giao hàng`, '%c/%e/%Y %k:%i')))) AS recency_days
  FROM teenup_technical_test.m_don_hang
  WHERE `Trạng thái đối tác` IN ('Giao hàng thành công','Giao thành công') -- chỉ tính đơn giao thành công do không phản ánh giá trị kinh tế
    AND `Khách hàng đã trả` >0                                             -- loại đơn 0 đồng/quà tặng do không phản ánh giá trị kinh tế
  GROUP BY `SĐT người nhận`
),
ranked AS (
  SELECT
    b.*,
    DENSE_RANK() OVER (ORDER BY gmv ASC)          AS m_rk,    -- rank theo Monetary (nhỏ -> lớn)
    DENSE_RANK() OVER (ORDER BY freq ASC)         AS f_rk,    -- rank theo Frequency (nhỏ -> lớn)
    DENSE_RANK() OVER (ORDER BY recency_days ASC) AS r_rk     -- rank theo Recency days (ít ngày -> rank thấp)
  FROM rfm_base b
)
SELECT
  r.*,
  -- Điểm M: chia 4 khoảng theo rank tối đa (quartile by rank)
  CASE
    WHEN m_rk <= CEIL((SELECT MAX(m_rk) FROM ranked)/4) THEN 1
    WHEN m_rk <= CEIL((SELECT MAX(m_rk) FROM ranked)/2) THEN 2
    WHEN m_rk <= CEIL((SELECT MAX(m_rk) FROM ranked)*3/4.0) THEN 3
    ELSE 4
  END AS M_score,
  -- Điểm F: tương tự M
  CASE
    WHEN f_rk <= CEIL((SELECT MAX(f_rk) FROM ranked)/4) THEN 1
    WHEN f_rk <= CEIL((SELECT MAX(f_rk) FROM ranked)/2) THEN 2
    WHEN f_rk <= CEIL((SELECT MAX(f_rk) FROM ranked)*3/4.0) THEN 3
    ELSE 4
  END AS F_score,
  -- Điểm R: ĐẢO CHIỀU (mới mua -> điểm cao)
  CASE
    WHEN r_rk <= CEIL((SELECT MAX(r_rk) FROM ranked)/4) THEN 4
    WHEN r_rk <= CEIL((SELECT MAX(r_rk) FROM ranked)/2) THEN 3
    WHEN r_rk <= CEIL((SELECT MAX(r_rk) FROM ranked)*3/4.0) THEN 2
    ELSE 1
  END AS R_score
FROM ranked r);