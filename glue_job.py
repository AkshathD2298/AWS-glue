
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job name
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read CSV from S3 (sample bucket)
datasource = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://customer-bucket/input_data/"]},
    transformation_ctx="datasource"
)

# Apply transformations (e.g., type casting)
applymapping = ApplyMapping.apply(frame=datasource, mappings=[
    ("id", "string", "id", "int"),
    ("name", "string", "name", "string"),
    ("email", "string", "email", "string"),
    ("created_at", "string", "created_at", "date")
])

# Write data to Redshift (sample connection details)
glueContext.write_dynamic_frame.from_options(
    frame=applymapping,
    connection_type="redshift",
    connection_options={
        "dbtable": "public.customers",
        "database": "customerdb",
        "user": "customer_user",
        "password": "customer_password",
        "url": "jdbc:redshift://customer-redshift-cluster.xxxxxxxxxxxx.region.redshift.amazonaws.com:5439/customerdb"
    },
    transformation_ctx="datasink"
)

# Commit job
job.commit()
