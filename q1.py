import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/")

filtered_df = df.filter(
    (col("Reviews").isNotNull()) &
    (col("Rating").cast("float") >= 3.0)
)

filtered_df.write.mode("overwrite").option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/output/question1/")