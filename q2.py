import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, min as spark_min, lit
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/")

# Cast Rating to float
df = df.withColumn("Rating", col("Rating").cast("float"))

# Compute max and min rating per (City, Price Range)
agg_df = df.groupBy("City", "Price Range").agg(
    spark_max("Rating").alias("max_rating"),
    spark_min("Rating").alias("min_rating")
)

# Join to get restaurants with max rating
max_df = df.join(agg_df, (df["City"] == agg_df["City"]) & 
                          (df["Price Range"] == agg_df["Price Range"]) & 
                          (df["Rating"] == agg_df["max_rating"])) \
           .select(df["*"]).withColumn("Extreme", lit("Highest"))

# Join to get restaurants with min rating
min_df = df.join(agg_df, (df["City"] == agg_df["City"]) & 
                          (df["Price Range"] == agg_df["Price Range"]) & 
                          (df["Rating"] == agg_df["min_rating"])) \
           .select(df["*"]).withColumn("Extreme", lit("Lowest"))

# Combine both
result = max_df.union(min_df)

# Write to HDFS
result.write.mode("overwrite").option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/output/question2/")