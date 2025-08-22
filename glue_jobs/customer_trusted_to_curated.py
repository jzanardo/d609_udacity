import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node customer_trusted
customer_trusted_node1755893492841 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://customer-trusted-jzanar1"], "recurse": True}, transformation_ctx="customer_trusted_node1755893492841")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1755893590424 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://accelerometer-trusted-jzanar1"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1755893590424")

# Script generated for node join by user
SqlQuery0 = '''
select a.* 
from customer_trusted a, accelerometer_trusted b
where a.email = b.user
'''
joinbyuser_node1755893669107 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_trusted":accelerometer_trusted_node1755893590424, "customer_trusted":customer_trusted_node1755893492841}, transformation_ctx = "joinbyuser_node1755893669107")

# Script generated for node Drop Duplicates
DropDuplicates_node1755894383985 =  DynamicFrame.fromDF(joinbyuser_node1755893669107.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1755894383985")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1755893842558 = glueContext.write_dynamic_frame.from_catalog(frame=DropDuplicates_node1755894383985, database="d209_jzanar1", table_name="customer_curated", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1755893842558")

job.commit()