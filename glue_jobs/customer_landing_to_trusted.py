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

# Script generated for node customer_landing
customer_landing_node1755883237404 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://customer-landing-jzanar1"], "recurse": True}, transformation_ctx="customer_landing_node1755883237404")

# Script generated for node research consent
SqlQuery0 = '''
select * from customer_landing
where sharewithresearchasofdate is not null
'''
researchconsent_node1755885590333 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":customer_landing_node1755883237404}, transformation_ctx = "researchconsent_node1755885590333")

# Script generated for node customer_trusted
customer_trusted_node1755887487927 = glueContext.write_dynamic_frame.from_catalog(frame=researchconsent_node1755885590333, database="d209_jzanar1", table_name="customer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="customer_trusted_node1755887487927")

job.commit()
