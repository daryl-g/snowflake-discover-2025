-- Setup context
use database demo;
use warehouse xsmall_wh;
use schema demo.dt_demo;

-- Create tables
CREATE OR REPLACE FUNCTION sum_table (INPUT_NUMBER number)
  returns TABLE (running_total number)
  language python
  runtime_version = '3.8'
  handler = 'gen_sum_table'
as
$$

# Define handler class
class gen_sum_table :

  ## Define __init__ method ro initilize the variable
  def __init__(self) :    
    self._running_sum = 0
  
  ## Define process method
  def process(self, input_number: float) :
    # Increment running sum with data from the input row
    new_total = self._running_sum + input_number
    self._running_sum = new_total

    yield(new_total,)
  
$$
;

CREATE OR REPLACE DYNAMIC TABLE cumulative_purchase
    LAG = '1 MINUTE'
    WAREHOUSE=XSMALL_WH
AS
    select 
        month(creationtime) monthNum,
        year(creationtime) yearNum,
        customer_id, 
        saleprice,
        running_total 
    from 
        salesreport,
        table(sum_table(saleprice) over (partition by creationtime,customer_id order by creationtime, customer_id))
       
;

-- Sanity test
select * from  cumulative_purchase limit 10;