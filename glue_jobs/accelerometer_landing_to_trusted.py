import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accelerometer_landing
accelerometer_landing_node1755890484380 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://accelerometer-landing-jzanar1"], "recurse": True}, transformation_ctx="accelerometer_landing_node1755890484380")

# Script generated for node customer_trusted
customer_trusted_node1755890618868 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://customer-trusted-jzanar1"], "recurse": True}, transformation_ctx="customer_trusted_node1755890618868")

# Script generated for node join by customer
SqlQuery0 = '''
select a.*
from accelerometer_landing a, customer_trusted b
where a.user = b.email
'''
joinbycustomer_node1755890667083 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":customer_trusted_node1755890618868, "accelerometer_landing":accelerometer_landing_node1755890484380}, transformation_ctx = "joinbycustomer_node1755890667083")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1755890932388 = glueContext.write_dynamic_frame.from_catalog(frame=joinbycustomer_node1755890667083, database="d209_jzanar1", table_name="accelerometer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="accelerometer_trusted_node1755890932388")

job.commit()