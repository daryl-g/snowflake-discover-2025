-- Setup context
use database demo;
use warehouse xsmall_wh;
use schema demo.dt_demo;

-- Create table (look for insufficient inventory)
CREATE OR REPLACE DYNAMIC TABLE PROD_INV_ALERT
    LAG = '1 MINUTE'
    WAREHOUSE=XSMALL_WH
AS
    SELECT 
        S.PRODUCT_ID, 
        S.PRODUCT_NAME,CREATIONTIME AS LATEST_SALES_DATE,
        STOCK AS BEGINING_STOCK,
        SUM(S.QUANTITY) OVER (PARTITION BY S.PRODUCT_ID ORDER BY CREATIONTIME) TOTALUNITSOLD, 
        (STOCK - TOTALUNITSOLD) AS UNITSLEFT,
        ROUND(((STOCK-TOTALUNITSOLD)/STOCK) *100,2) PERCENT_UNITLEFT,
        CURRENT_TIMESTAMP() AS ROWCREATIONTIME
    FROM SALESREPORT S JOIN PROD_STOCK_INV ON PRODUCT_ID = PID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID ORDER BY CREATIONTIME DESC) = 1
;

-- check products with low inventory and alert
select * from prod_inv_alert where percent_unitleft < 10;

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS
    notification_emailer
    TYPE=EMAIL
    ENABLED=TRUE
    ALLOWED_RECIPIENTS=('dgouilard4@gmail.com')
    COMMENT = 'email integration to update on low product inventory levels'
;

CREATE OR REPLACE ALERT alert_low_inv
  WAREHOUSE = XSMALL_WH
  SCHEDULE = '30 MINUTE'
  IF (EXISTS (
      SELECT *
      FROM prod_inv_alert
      WHERE percent_unitleft < 10 and ROWCREATIONTIME > SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
                'notification_emailer', -- notification integration to use
                'dgouilard4@gmail.com', -- Email
                'Email Alert: Low Inventory of products', -- Subject
                'Inventory running low for certain products. Please check the inventory report in Snowflake table prod_inv_alert' -- Body of email
);

-- Alerts are pause by default, so let's resume it first
ALTER ALERT alert_low_inv RESUME;

-- Add new records
insert into salesdata select * from table(gen_cust_purchase(10000,2));