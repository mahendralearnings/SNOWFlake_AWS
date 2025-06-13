use role sysadmin
--create a json format

create or replace File format json_format_load
type='json'
use role accountadmin

desc integration aws_sf_data

-- Create a stage for lineitem table  ---
create or replace stage stage_lineitem_json_data
STORAGE_INTEGRATION = aws_sf_data
URL = ('s3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/lineitem_json/')
FILE_FORMAT = json_format_load;

 list @stage_lineitem_json_data




create or replace stage stage_lineitem_json_dev
STORAGE_INTEGRATION = aws_sf_data
URL = ('s3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/lineitem_json/')
FILE_FORMAT = json_format_load;

select $1 from @stage_lineitem_json_dev limit 10

-- insert into lineitem directly from staged location --- 

select 
    $1:L_ORDERKEY,
    $1:L_PARTKEY,
    $1:L_SUPPKEY,
    $1:L_LINENUMBER,
    $1:L_QUANTITY,
    $1:L_EXTENDEDPRICE,
    $1:L_DISCOUNT,
    $1:L_TAX,
    $1:L_RETURNFLAG,
    $1:L_LINESTATUS,
    $1:L_SHIPDATE,
    $1:L_COMMITDATE,
    $1:L_RECEIPTDATE,
    $1:L_SHIPINSTRUCT,
    $1:L_SHIPMODE,
    $1:L_COMMENT
from 
    @stage_lineitem_json_dev 
 limit 10;


 create table lineitem_raw_json(src variant)

copy into  lineitem_raw_json from @stage_lineitem_json_dev
 ON_ERROR = ABORT_STATEMENT;



select * from lineitem_raw_json 
limit 10 

select 
    SRC:L_ORDERKEY,
    SRC:L_PARTKEY,
    SRC:L_SUPPKEY,
    SRC:L_LINENUMBER,
    SRC:L_QUANTITY,
    SRC:L_EXTENDEDPRICE,
    SRC:L_DISCOUNT,
    SRC:L_TAX,
    SRC:L_RETURNFLAG,
    SRC:L_LINESTATUS,
    SRC:L_SHIPDATE,
    SRC:L_COMMITDATE,
    SRC:L_RECEIPTDATE,
    SRC:L_SHIPINSTRUCT,
    SRC:L_SHIPMODE,
    SRC:L_COMMENT
 
from lineitem_raw_json limit 10
