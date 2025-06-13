use role sysadmin

create or replace File format parquet_format_load
type='parquet'

use role accountadmin

desc integration aws_sf_data

create or replace stage stage_lineitem_parquet
storage_integration=aws_sf_data
url=('s3://openaq-dataset-input/ecommerce_dev/lineitem/lineitem_csv/lineitem_parquet/')
FILE_FORMAT = parquet_format_load;

-- Create target table manually
--
insert into lineitem
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
    @stage_lineitem_parquet


select * from lineitem limit 10
------------------------------------------------

--craete variant data type with 
create or replace table lineitem_raw_parquet(src variant)

copy into  lineitem_raw_parquet from @stage_lineitem_parquet
 ON_ERROR = ABORT_STATEMENT;

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
from 
    lineitem_raw_parquet 