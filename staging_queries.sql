-- create uuids for each customer_code
create table customer_uuids (
    customer_code int primary key,
    "customerId" UUID default gen_random_uuid(),
    "customerProfileId" UUID default gen_random_uuid()
);

-- Insert customer UUIDs from the staging table
insert into customer_uuids (customer_code)
select customer_code
from stg_customers
where customer_code not in (select customer_code from customer_uuids);