create or replace table `datawarehouse-423912.OLAP_dataset.Dim_Products` as
select 
products.productID,
products.productname,
products.unitPrice,
categories.categoryID,
categories.categoryName,
from
`datawarehouse-423912.warehouse.products` as products
inner join`datawarehouse-423912.warehouse.categories` as categories
on categories.categoryID = products.categoryID