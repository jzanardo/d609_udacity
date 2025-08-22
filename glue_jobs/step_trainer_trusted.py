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
step_trainer_landing_node1755896241804 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://step-trainer-landing-jzanar1"], "recurse": True}, transformation_ctx="step_trainer_landing_node1755896241804")

# Script generated for node customer_curated
customer_curated_node1755896320746 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://customer-curated-jzanar1"], "recurse": True}, transformation_ctx="customer_curated_node1755896320746")

# Script generated for node join by serial number
SqlQuery0 = '''
select a.*
from step_trainer_landing a, customer_curated b
where a.serialnumber = b.serialnumber
'''
joinbyserialnumber_node1755896408866 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_curated":customer_curated_node1755896320746, "step_trainer_landing":step_trainer_landing_node1755896241804}, transformation_ctx = "joinbyserialnumber_node1755896408866")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1755896778595 = glueContext.write_dynamic_frame.from_catalog(frame=joinbyserialnumber_node1755896408866, database="d209_jzanar1", table_name="step_trainer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1755896778595")

job.commit()