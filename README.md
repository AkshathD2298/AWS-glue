
# AWS Glue ETL: S3 to Redshift

This project demonstrates how to build an AWS Glue job to extract CSV data from S3, transform it using PySpark, and load it into Amazon Redshift.

## Tools Used
- AWS Glue
- Amazon S3
- Amazon Redshift
- PySpark

## Steps
1. Upload `sample_users.csv` to S3
2. Create Redshift table using `redshift_table_schema.sql`
3. Deploy `glue_job.py` as a Glue job
4. Trigger job manually or via AWS Lambda

## Sample Data Fields
- id (INT)
- name (STRING)
- email (STRING)
- created_at (DATE)
