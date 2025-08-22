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

# Script generated for node step_trainer_landing
step_trainer_landing_node1755807184027 = glueContext.create_dynamic_frame.from_catalog(database="d209_jzanar1", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1755807184027")

# Script generated for node customer_curated
customer_curated_node1755807219930 = glueContext.create_dynamic_frame.from_catalog(database="d209_jzanar1", table_name="customer_curated", transformation_ctx="customer_curated_node1755807219930")

# Script generated for node SQL Query
SqlQuery0 = '''
select a.sensorreadingtime, a.serialnumber, a.distancefromobject
from step_trainer_landing a, customer_curated b
where a.serialnumber = b.serialnumber
'''
SQLQuery_node1755818387880 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":step_trainer_landing_node1755807184027, "customer_curated":customer_curated_node1755807219930}, transformation_ctx = "SQLQuery_node1755818387880")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1755807563182 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1755818387880, database="d209_jzanar1", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1755807563182")

job.commit()