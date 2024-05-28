CREATE OR REPLACE TABLE `datawarehouse-423912.OLAP_dataset.Dim_date` AS
SELECT
  SAFE_CAST(order_date AS DATE) AS order_date,
  EXTRACT(MONTH FROM SAFE_CAST(order_date AS DATE)) AS month,
  EXTRACT(QUARTER FROM SAFE_CAST(order_date AS DATE)) AS quarter,
  EXTRACT(YEAR FROM SAFE_CAST(order_date AS DATE)) AS year,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM SAFE_CAST(order_date AS DATE)) IN (1, 7) THEN TRUE
    ELSE FALSE
  END AS weekend_flag
FROM
  `datawarehouse-423912.warehouse.orders`
WHERE
  SAFE_CAST(order_date AS DATE) IS NOT NULL;
