
## 🧰 Technologies Used

- **Snowflake Cloud Data Platform**
- **AWS S3 (Simple Storage Service)**
- **Snowpipe** (Auto-ingestion using SQS notifications)
- **SQL** (COPY INTO, CREATE STAGE, CREATE PIPE, etc.)
- **IAM Role & Trust Policy Configuration**
- **File formats**: JSON, CSV, Parquet

---

## ✅ Features & Use Cases

- ✅ External Stage Setup using `STORAGE_INTEGRATION`
- ✅ File Format definitions for different data types
- ✅ Snowpipe for real-time data ingestion
- ✅ Data loading from S3 into Snowflake tables
- ✅ Data unloading from Snowflake to S3
- ✅ Partitioned data export by `L_SHIPDATE`
- ✅ Trust relationship configuration for secure cross-account access

---

## 🚀 How to Run (Local + Snowflake)

1. Ensure your AWS IAM Role is configured with `sts:AssumeRole` and trust relationship with Snowflake’s External ID.
2. Update your **STORAGE_INTEGRATION** to allow S3 bucket paths.
3. Upload files to your staging S3 folders:
