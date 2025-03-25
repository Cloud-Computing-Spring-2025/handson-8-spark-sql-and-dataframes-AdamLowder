from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("inputs/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("inputs/users.csv", inferSchema=True)

# TODO: Implement the task here
# Filter only verified users
verified_users_df = users_df.filter(col("Verified") == True)

# Join posts with verified users
verified_posts_df = posts_df.join(verified_users_df, "UserID")

# Calculate total reach per verified user
top_verified = (
    verified_posts_df.groupBy("Username")
    .agg(_sum(col("Likes") + col("Retweets")).alias("Total_Reach"))
    .orderBy(col("Total_Reach").desc())
    .limit(5)
)

# Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)
