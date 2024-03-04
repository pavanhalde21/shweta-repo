
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

df = spark.read.parquet("s3://pavan-aws-bw-requirements/shweta/output/", header = True)

salary_df = df.withColumn("temp_salary", df["Salary"]).withColumn(
    "Salary",
    expr(
        "CASE " +
        "WHEN length(Salary) = 5 THEN concat_ws(',', substring(Salary, 1, 2), substring(Salary, 3)) " +
        "WHEN length(Salary) = 7 THEN concat_ws(',', substring(Salary, 1, 2), substring(Salary, 3, 2), substring(Salary, 5)) " +
        "ELSE Salary " +
        "END"))
# Create dataset1: total number of flights and total distance traveled by each customer
dataset1 = df.groupBy("Loyalty Number").agg(sum("Total Flights").alias("Total Flights"),
                                                    sum("Distance").alias("Total Distance"))

# Create dataset2: total points accumulated and redeemed by each customer
dataset2 = df.groupBy("Loyalty Number").agg(sum("Points Accumulated").alias("Total Points Accumulated"),
                                                    sum("Points Redeemed").alias("Total Points Redeemed"))


dataset1.write.parquet('s3://pavan-aws-bw-requirements/shweta/output3/dataset1/', mode = 'overwrite')
dataset2.write.parquet('s3://pavan-aws-bw-requirements/shweta/output3/dataset2/', mode = 'overwrite')

df.write.parquet('s3://pavan-aws-bw-requirements/shweta/output3/loyalty_card/', mode = 'overwrite', partitionBy = 'Loyalty Card')