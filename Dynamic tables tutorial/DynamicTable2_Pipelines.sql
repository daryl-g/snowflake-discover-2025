-- Setup context
use database demo;
use warehouse xsmall_wh;
use schema demo.dt_demo;

-- Create tables
CREATE OR REPLACE DYNAMIC TABLE customer_sales_data_history
    LAG='DOWNSTREAM' -- Refresh table when the downstream tables refresh
    WAREHOUSE=XSMALL_WH
AS
select 
    s.custid as customer_id,
    c.cname as customer_name,
    -- Lots of casting here, new way or writing cast(<something> as <different_type>)
    s.purchase:"prodid"::number(5) as product_id,
    s.purchase:"purchase_amount"::number(10) as saleprice,
    s.purchase:"quantity"::number(5) as quantity,
    s.purchase:"purchase_date"::date as salesdate
from
    cust_info c inner join salesdata s on c.custid = s.custid
;

-- quick sanity check
select * from customer_sales_data_history limit 10;
select count(*) from customer_sales_data_history;

CREATE OR REPLACE DYNAMIC TABLE salesreport
    LAG = '1 MINUTE' -- Refresh every minute
    WAREHOUSE=XSMALL_WH
AS
    Select
        t1.customer_id,
        t1.customer_name, 
        t1.product_id,
        p.pname as product_name,
        t1.saleprice,
        t1.quantity,
        (t1.saleprice/t1.quantity) as unitsalesprice,
        t1.salesdate as CreationTime,
        customer_id || '-' || t1.product_id  || '-' || t1.salesdate AS CUSTOMER_SK,
        LEAD(CreationTime) OVER (PARTITION BY t1.customer_id ORDER BY CreationTime ASC) AS END_TIME
    from 
        customer_sales_data_history t1 inner join prod_stock_inv p 
        on t1.product_id = p.pid
       
;

-- Sanity check
select * from salesreport limit 10;
select count(*) from salesreport;

-- Testing
-- Add new records
insert into salesdata select * from table(gen_cust_purchase(10000,2));

-- Check raw base table
select count(*) from salesdata;

-- Check Dynamic Tables after a minute
select count(*) from customer_sales_data_history; -- Updated when the downstream table (Sales) gets updated
select count(*) from salesreport;