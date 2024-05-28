DECLARE today_date DATE DEFAULT DATE '1998-05-06';

create or replace table `datawarehouse-423912.OLAP_dataset.Fact_table_temp`
as 
select
orders.order_id,
orders.customer_id,
orders.order_date
from
`datawarehouse-423912.warehouse.orders` as orders
join
`datawarehouse-423912.OLAP_dataset.Dim_Order_details` as order_details
on
orders. order_id = order_details.orderID
