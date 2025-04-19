import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
from pyspark.sql.functions import col, avg, desc, asc, when

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/")

# Convert Rating to float
df = df.withColumn("Rating", col("Rating").cast("float"))

# Calculate average rating per city
avg_ratings = df.groupBy("City").agg(avg("Rating").alias("AverageRating")).na.drop()

# Get Top 2 and Bottom 2 cities
top_2 = avg_ratings.orderBy(desc("AverageRating")).limit(2).withColumn("RatingGroup", col("AverageRating") * 0 + 1)
bottom_2 = avg_ratings.orderBy(asc("AverageRating")).limit(2).withColumn("RatingGroup", col("AverageRating") * 0 + 2)

# Label groups
top_2 = top_2.withColumn(
    "RatingGroup",
    when(top_2["RatingGroup"] == "1", "Top").otherwise(top_2["RatingGroup"])
)
bottom_2 = bottom_2.withColumn(
    "RatingGroup",
    when(col("RatingGroup") == "2", "Bottom").otherwise(col("RatingGroup"))
)

# Combine and save
result = top_2.union(bottom_2).orderBy(desc("AverageRating"))
result.write.mode("overwrite").option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/output/question3/")