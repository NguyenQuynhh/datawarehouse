-- quá trình upload dataset lên bogquerry, header của mỗi cột bị sai thành field 0_1_2... nên cần query để đổi lại tên cho mỗi header
create or replace table `datawarehouse-423912.warehouse.newupdated_customers`
as 
select string_field_0 as customerID,
string_field_1 as companyName,
string_field_2 as contactName,
string_field_3 as contactTitle,
string_field_4 as address,
string_field_5 as city,
string_field_6 as region,
string_field_7 as postalCode,
string_field_8 as country,
string_field_9 as phone,
string_field_10 as fax
from `datawarehouse-423912.warehouse.customers`

