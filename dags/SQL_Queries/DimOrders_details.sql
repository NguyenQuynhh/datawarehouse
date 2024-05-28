create or replace table `datawarehouse-423912.OLAP_dataset.Dim_Order_details` as
select 
*
from
`datawarehouse-423912.warehouse.orderdetails`
