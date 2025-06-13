
use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";

-- Extract/Unload data  ---

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
CREATE OR REPLACE STORAGE INTEGRATION aws_sf_data
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::375785165212:role/snowflake-aws-load-unload'
STORAGE_ALLOWED_LOCATIONS = ('s3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/');


--s3://openaq-dataset-input/ecommerce_dev/lineitem/unloaded/
copy into s3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/unloaded/
from
(
  select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM" limit 100000
)
storage_integration=aws_sf_data
single=false
file_format = csv_load_format;

desc integration aws_sf_data


create or replace File format parquet_format_load
type='parquet'

create or replace stage stage_lineitem_parquet
storage_integration=aws_sf_data
url=('s3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/lineitem_parquet/')
FILE_FORMAT = parquet_format_load;

-- Extract/Unload data using partition by ---
copy into s3://sid-snowflake-data/unloaded_data/lineitem_partitioned/
from
(
  select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM" limit 100000
)
--partition by L_SHIPDATE
storage_integration=aws_sf_data
single=false
file_format = csv_load_format;



copy into s3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/unloaded/lineitem_parquet/
from
(
  select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM"
  limit 1000000
)
storage_integration=aws_sf_data
single=false
file_format = parquet_format_load;




create or replace File format json_format_load
type='json'

desc integration aws_sf_data

-- Create a stage for lineitem table  ---
create or replace stage stage_lineitem_json_data
STORAGE_INTEGRATION = aws_sf_data
URL = ('s3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/lineitem_json/')
FILE_FORMAT = json_format_load;

copy into 

s3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/lineitem_json/
from
(
  select 
  object_construct(
  'L_ORDERKEY',L_ORDERKEY,
  'L_PARTKEY',L_PARTKEY,
  'L_SUPPKEY',L_SUPPKEY,
  'L_LINENUMBER',L_LINENUMBER,
   'L_QUANTITY',L_QUANTITY,
   'L_EXTENDEDPRICE',L_EXTENDEDPRICE,
  'L_DISCOUNT',L_DISCOUNT,
    'L_TAX',L_TAX,
    'L_RETURNFLAG',L_RETURNFLAG,
    'L_LINESTATUS',L_LINESTATUS,
    'L_SHIPDATE',L_SHIPDATE,
    'L_COMMITDATE',L_COMMITDATE,
    'L_RECEIPTDATE',L_RECEIPTDATE,
    'L_SHIPINSTRUCT',L_SHIPINSTRUCT,
    'L_SHIPMODE',L_SHIPMODE,
    'L_COMMENT',L_COMMENT
  )
  from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM"
  limit 1000000
)
storage_integration=aws_sf_data
single=false
overwrite=True
file_format = json_format_load;