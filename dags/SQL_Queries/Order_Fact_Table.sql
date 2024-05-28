create or replace table `datawarehouse-423912.OLAP_dataset.Order_fact_table` as
with RFM_table as (
  select
  fact.customer_id,
  fact.order_id,
  fact.order_date,
  rfm.Recency,
  rfm.Frequency,
  rfm.Monetary,
  sale.total_revenue
  from
  `datawarehouse-423912.OLAP_dataset.Fact_table` as fact
  join
  `datawarehouse-423912.OLAP_dataset.RFM_table` as rfm
  on 
 fact.customer_id = rfm.customer_id
  JOIN
  `datawarehouse-423912.OLAP_dataset.Sale_table` as sale
  ON
  fact.order_id = sale.orderID
)

select * from RFM_table;