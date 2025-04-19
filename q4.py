import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
from pyspark.sql.functions import col, explode, split, trim, count, regexp_replace

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/")

# Only select rows with non-null Cuisine Style
df = df.filter(col("Cuisine Style").isNotNull())

# Clean and split the Cuisine Style list (remove brackets and quotes first)
cleaned = df.withColumn("Cuisine Style", 
    regexp_replace(col("Cuisine Style"), r"[\[\]']", "")
)

# Split by comma and trim spaces
cleaned = cleaned.withColumn("Cuisine", explode(split(col("Cuisine Style"), ",")))
cleaned = cleaned.withColumn("Cuisine", trim(col("Cuisine")))

# Group by City and Cuisine and count
result = cleaned.groupBy("City", "Cuisine").agg(count("*").alias("count"))

# Sort by City then by Cuisine
sorted_result = result.orderBy("City", "Cuisine")

# Save to HDFS
sorted_result.write.mode("overwrite").option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/output/question4/")