use role sysadmin
use database ecommerce_db

use ecommerce_db.ecommerce_dev



use role accountadmin

use role sysadmin

create or replace stage stg_lineitem_csv_dev
storage_integration = aws_sf_data
url = 's3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/lineitem_snowpipe/csv/'
file_format = csv_load_format;



create or replace pipe line_item_pipe auto_ingest=true as 

copy into lineitem from @stg_lineitem_csv_dev ON_ERROR = continue;

show pipes

truncate table lineitem;

select count(*) from lineitem


select * from information_schema.load_history where table_name='LINEITEM' order by last_load_time desc limit 10;

use role accountadmin;

-- Switch role to accountadmin before running this command -- 
select *
  from table(information_schema.pipe_usage_history(
    date_range_start=>dateadd('hour',-3,current_timestamp()),
    pipe_name=>'line_item_pipe'));

    SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

    DESC TABLE lineitem;



--verifying snowpipe flow
SHOW PIPES;

SELECT *
FROM information_schema.load_history
WHERE table_name = 'LINEITEM'
ORDER BY last_load_time DESC
LIMIT 10;


USE ROLE ACCOUNTADMIN;

SELECT *
FROM TABLE(information_schema.pipe_usage_history(
  date_range_start => dateadd('hour', -3, current_timestamp()),
  pipe_name => 'LINE_ITEM_PIPE'
));

SELECT COUNT(*) FROM lineitem;

SELECT * FROM lineitem LIMIT 10;



CREATE OR REPLACE STAGE stg_lineitem_csv_dev
STORAGE_INTEGRATION = aws_sf_data
URL = 's3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/lineitem_snowpipe/csv/'
FILE_FORMAT = csv_load_format;




CREATE OR REPLACE PIPE line_item_pipe AUTO_INGEST = TRUE AS
COPY INTO lineitem FROM @stg_lineitem_csv_dev ON_ERROR = CONTINUE;








SHOW PIPES;

-- Verify last COPY activity
SELECT *
FROM information_schema.load_history
WHERE table_name = 'LINEITEM'
ORDER BY last_load_time DESC
LIMIT 10;

-- Check pipe execution metadata
SELECT *
FROM TABLE(information_schema.pipe_usage_history(
  date_range_start => dateadd('hour', -3, current_timestamp()),
  pipe_name => 'LINE_ITEM_PIPE'
));

-- Confirm row count
SELECT COUNT(*) FROM lineitem;
