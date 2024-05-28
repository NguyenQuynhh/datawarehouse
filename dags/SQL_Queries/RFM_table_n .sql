DECLARE today_date DATE DEFAULT DATE '1998-05-06';

-- Tạo bảng RFM_table
CREATE OR REPLACE TABLE `marine-catalyst-419003.transformed.RFM_table` AS
WITH RFM_table AS (
  SELECT
    orders.customer_id,
    MAX(CAST(orders.order_date AS DATE)) AS LastPurchaseDate,
    COUNT(DISTINCT orders.order_id) AS TotalTransaction,
    SUM(order_details.quantity * order_details.unitPrice * (1 - order_details.discount)) AS Monetary
  FROM
    `marine-catalyst-419003.demo.orders` AS orders
  JOIN
    `marine-catalyst-419003.transformed.Dim_Order_details` AS order_details
  ON
    orders.order_id = order_details.orderID
  GROUP BY
    orders.customer_id
)

-- Thêm các cột điểm vào bảng mới
SELECT
  customer_id,
  DATE_DIFF(today_date, LastPurchaseDate, DAY) AS Recency,
  TotalTransaction AS Frequency,
  Monetary,
  NULL AS recency_score, -- Khởi tạo cột recency_score với giá trị NULL
  NULL AS frequency_score, -- Khởi tạo cột frequency_score với giá trị NULL
  NULL AS monetary_score -- Khởi tạo cột monetary_score với giá trị NULL
FROM
  RFM_table
WHERE
  Monetary > 0;

select * from `marine-catalyst-419003.transformed.RFM_table`
where customer_id = 'ANTON'
