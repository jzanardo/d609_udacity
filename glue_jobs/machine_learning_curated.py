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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1755897626367 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://step-trainer-trusted-jzanar1"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1755897626367")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1755897682836 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://accelerometer-trusted-jzanar1"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1755897682836")

# Script generated for node customer_curated
customer_curated_node1755897731383 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://customer-curated-jzanar1"], "recurse": True}, transformation_ctx="customer_curated_node1755897731383")

# Script generated for node aggregation
SqlQuery0 = '''
select 
a.sensorReadingTime,
a.serialNumber,
a.distanceFromObject,
b.user,
b.x,
b.y,
b.z
from 
step_trainer_trusted a, 
accelerometer_trusted b, 
customer_curated c
where
a.sensorReadingTime = b.timestamp
and b.user = c.email
'''
aggregation_node1755820437658 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":step_trainer_trusted_node1755897626367, "accelerometer_trusted":accelerometer_trusted_node1755897682836, "customer_curated":customer_curated_node1755897731383}, transformation_ctx = "aggregation_node1755820437658")

# Script generated for node machine_learning_curated
machine_learning_curated_node1755820520567 = glueContext.write_dynamic_frame.from_catalog(frame=aggregation_node1755820437658, database="d209_jzanar1", table_name="machine_learning_curated", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="machine_learning_curated_node1755820520567")

job.commit()