-- Setup context
use database demo;
use warehouse xsmall_wh;
use schema demo.dt_demo;

SELECT * 
FROM 
    TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE 
    NAME IN ('SALESREPORT','CUSTOMER_SALES_DATA_HISTORY','PROD_INV_ALERT','CUMULATIVE_PURCHASE')
    -- AND REFRESH_ACTION != 'NO_DATA'
ORDER BY 
    DATA_TIMESTAMP DESC, REFRESH_END_TIME DESC LIMIT 10;

-- Clean up
-- Resume the data pipeline
alter dynamic table customer_sales_data_history RESUME;
alter dynamic table salesreport RESUME;
alter dynamic table prod_inv_alert RESUME;

-- Suspend the data pipeline
alter dynamic table customer_sales_data_history SUSPEND;
alter dynamic table salesreport SUSPEND;
alter dynamic table prod_inv_alert SUSPEND;

DROP SCHEMA DEMO.DT_DEMO;

-- if you wish to drop the entire database
DROP DATABASE DEMO;

drop warehouse xsmall_wh;