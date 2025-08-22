import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from the accelerometer landing zone
accelerometer_data = glueContext.create_dynamic_frame.from_catalog(database = "d209_jzanar1", table_name = "accelerometer_landing")

# Read trusted customer data
trusted_customers = glueContext.create_dynamic_frame.from_catalog(database = "d209_jzanar1", table_name = "customer_trusted")

# Inner join the accelerometer data with trusted customers on email
trusted_accelerometer_data = Join.apply(accelerometer_data, trusted_customers, 'user', 'email')

# Write the trusted accelerometer data to the trusted zone
glueContext.write_dynamic_frame.from_catalog(frame = trusted_accelerometer_data, database = "d209_jzanar1", table_name = "accelerometer_trusted")

job.commit()