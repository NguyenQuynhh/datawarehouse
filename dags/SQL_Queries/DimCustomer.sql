-- dây là tạo từ oltp sang olap
create or replace table `datawarehouse-423912.OLAP_dataset.Dim_customer` as
select 
customerID,
contactName,
address,
city,
phone,
country
from
`datawarehouse-423912.warehouse.newupdated_customers`
