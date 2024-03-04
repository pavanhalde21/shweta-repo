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

df = spark.read.parquet("s3://pavan-aws-bw-requirements/shweta/output/")

conversion_rate = 0.016
df1 = df.withColumn("salary_1", col("salary") * conversion_rate)

def func(stre):
    if len(stre) > 3 and len(stre) <= 5:
        return stre[:-3] + "," + stre[-3:]
    elif len(stre) > 5 and len(stre) <= 7:
        return stre[:-5] + "," + stre[-5:-3] + "," + stre[-3:]
    elif len(stre) > 7 and len(stre) <= 9:
        return stre[:-7] + "," + stre[-7:-5] + "," + stre[-5:-3] + "," + stre[-3:]

format_salary_udf = udf(func, StringType())

df2 = df1.withColumn('salary', format_salary_udf(col('salary')))

df2.write.parquet('s3://pavan-aws-bw-requirements/shweta/output2/', mode = 'overwrite')

