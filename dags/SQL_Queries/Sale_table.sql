create or replace table `datawarehouse-423912.OLAP_dataset.Sale_table` AS
SELECT 
    orderID,
    SUM(quantity * unitprice * (1 - discount)) AS total_revenue
FROM 
    `datawarehouse-423912.OLAP_dataset.Dim_Order_details`
GROUP BY 
    orderID;