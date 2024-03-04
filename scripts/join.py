import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

df1 = spark.read.csv("s3://pavan-aws-bw-requirements/shweta/curate/Customer Flight Activity.csv",header=True, inferSchema=True)
df2 = spark.read.csv("s3://pavan-aws-bw-requirements/shweta/curate/Customer Loyalty History.csv",header=True, inferSchema=True)

joined_df = df1.join(df2, "Loyalty Number")

joined_df.write.parquet('s3://pavan-aws-bw-requirements/shweta/output/', mode = 'overwrite')

