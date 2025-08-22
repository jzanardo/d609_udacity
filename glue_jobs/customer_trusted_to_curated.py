import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read trusted customer data
trusted_customers = glueContext.create_dynamic_frame.from_catalog(database="d209_jzanar1", table_name="customer_trusted")

# Read accelerometer data
accelerometer_data = glueContext.create_dynamic_frame.from_catalog(database="d209_jzanar1", table_name="accelerometer_trusted")

# Convert DynamicFrame to Spark DataFrame
spark_df = accelerometer_data.toDF()

# Apply distinct() to remove duplicate rows
deduplicated_spark_df = spark_df.dropDuplicates(subset=["user"])

# Convert back to DynamicFrame
deduplicated_dynamic_frame = DynamicFrame.fromDF(deduplicated_spark_df, glueContext, "deduplicated_frame")

# Inner join the trusted customers with accelerometer data on email
curated_customers = Join.apply(trusted_customers, deduplicated_dynamic_frame, 'email', 'user')

# Write the curated customer data to the curated zone
glueContext.write_dynamic_frame.from_catalog(frame=curated_customers, database="d209_jzanar1", table_name="customer_curated")

job.commit()