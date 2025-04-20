import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from itertools import combinations

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assignment 2 Question 5").getOrCreate()

# Read the Parquet file (only one read)
df = spark.read.parquet(f"hdfs://{hdfs_nn}:9000/assignment2/part2/input/")

# Select relevant columns and convert to RDD
rdd = df.select("movie_id", "title", "cast").rdd

# Extract (actor1, actor2) unordered pairs from cast list
def extract_actor_pairs(row):
    movie_id, title, cast_json = row
    try:
        cast_list = json.loads(cast_json)
        actor_names = [member['name'] for member in cast_list]
        # Generate all unique unordered pairs
        pairs = combinations(sorted(set(actor_names)), 2)
        return [((a1, a2), [(movie_id, title)]) for a1, a2 in pairs]
    except:
        return []

# Flatten to ((actor1, actor2), [(movie_id, title)])
pair_rdd = rdd.flatMap(extract_actor_pairs)

# Group by actor pairs and count how many distinct movies they co-starred in
pair_grouped = pair_rdd.reduceByKey(lambda a, b: a + b)

# Keep only those who co-starred in at least 2 movies
filtered = pair_grouped.filter(lambda x: len(set(x[1])) >= 2)

# For each such actor pair, emit all their movies with schema: movie_id, title, actor1, actor2
final_rows = filtered.flatMap(lambda x: [(movie_id, title, x[0][0], x[0][1]) for (movie_id, title) in set(x[1])])

# Convert to DataFrame and write (only one write)
result_df = spark.createDataFrame(final_rows, ["movie_id", "title", "actor1", "actor2"])
result_df.write.parquet(f"hdfs://{hdfs_nn}:9000/assignment2/output/question5/")