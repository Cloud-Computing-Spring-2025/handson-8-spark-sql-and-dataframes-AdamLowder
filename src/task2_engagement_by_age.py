from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder.appName("EngagementByAgeGroup").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("inputs/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("inputs/users.csv", inferSchema=True)

# TODO: Implement the task here
# Join posts with users on UserID
joined_df = posts_df.join(users_df, "UserID")

# Calculate average likes and retweets by age group
engagement_df = (
    joined_df.groupBy("AgeGroup")
    .agg(
        avg("Likes").alias("Avg_Likes"),
        avg("Retweets").alias("Avg_Retweets")
    )
    .orderBy(col("Avg_Likes").desc())
)

# Save result
engagement_df.coalesce(1).write.mode("overwrite").csv("outputs/engagement_by_age.csv", header=True)
