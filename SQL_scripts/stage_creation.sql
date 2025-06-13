use role sysadmin;

use database ecommerce_db;

create schema ecommerce_dev;

create or replace table lineitem cluster by (L_SHIPDATE) as select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM" limit 1;
truncate table lineitem; --deletes the data but not the structure


CREATE FILE FORMAT csv_load_format
    TYPE = 'CSV' 
    COMPRESSION = 'AUTO' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER =1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO';



---- PARQUET FILE FORMAT ----
CREATE OR REPLACE FILE FORMAT parquet_load_format 
TYPE = 'parquet' ;


---- JSON FILE FORMAT ----
CREATE OR REPLACE FILE FORMAT json_load_format
TYPE = 'JSON'  ;


use role accountadmin

CREATE OR REPLACE STORAGE INTEGRATION aws_sf_data
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::375785165212:role/snowflake-aws-load-unload'
STORAGE_ALLOWED_LOCATIONS = ('s3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/');


--craete a stage for linitem table and data load from s3 
CREATE OR REPLACE STAGE stg_lineitem_csv_dev
STORAGE_INTEGRATION = aws_sf_data
URL = ('s3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/')
FILE_FORMAT = csv_load_format;


desc integration aws_sf_data

use role sysadmin;

use database ecommerce_db;

create schema ecommerce_dev;

create or replace table lineitem cluster by (L_SHIPDATE) as select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM" limit 1;
truncate table lineitem; --deletes the data but not the structure


CREATE FILE FORMAT csv_load_format
    TYPE = 'CSV' 
    COMPRESSION = 'AUTO' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER =1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO';

    USE ROLE ACCOUNTADMIN;


CREATE OR REPLACE STORAGE INTEGRATION aws_sf_data
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::375785165212:role/snowflake-aws-load-unload'
STORAGE_ALLOWED_LOCATIONS = ('s3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/');

DESC INTEGRATION aws_sf_data;

   

-- Create a stage for lineitem table  ---
create stage stg_lineitem_csv_dev
storage_integration = aws_sf_data
URL = ('s3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/')
FILE_FORMAT = csv_load_format;



list @stg_lineitem_csv_dev;

copy into lineitem 
from @stg_lineitem_csv_dev 
ON_ERROR = ABORT_STATEMENT;


-- Validate the data----
select * from lineitem limit 10;  

use role accountadmin

show integrations

use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";


-- Extract/Unload data  ---


copy into s3://openaq-dataset-input/ecommerce_dev/lineitem/unloaded/
from
(
  select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM" limit 100000
)
storage_integration=aws_sf_data
single=false
file_format = csv_load_format;

DESC INTEGRATION