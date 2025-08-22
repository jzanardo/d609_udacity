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

# Read data from the customer landing zone
customer_data = glueContext.create_dynamic_frame.from_catalog(database = "d209_jzanar1", table_name = "customer_landing")

# Filter records where shareWithResearchAsOfDate is not null
trusted_customers = Filter.apply(frame = customer_data, f = lambda x: x["shareWithResearchAsOfDate"] is not None)

# Write the trusted customer data to the trusted zone
glueContext.write_dynamic_frame.from_catalog(frame = trusted_customers, database = "d209_jzanar1", table_name = "customer_trusted")

job.commit()
