from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, count

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()
# workspaces/handson-8-spark-sql-and-dataframes-AdamLowder/
# Load posts data
posts_df = spark.read.option("header", True).csv("inputs/posts.csv")

# TODO: Split the Hashtags column into individual hashtags and count the frequency of each hashtag and sort descending
hashtag_counts = (
    posts_df.withColumn("Hashtag", explode(split(col("Hashtags"), ",")))
    .groupBy("Hashtag")
    .agg(count("*").alias("Count"))
    .orderBy(col("Count").desc())
)

# Save result
hashtag_counts.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)