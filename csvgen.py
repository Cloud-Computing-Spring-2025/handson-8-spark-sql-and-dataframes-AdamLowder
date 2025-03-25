import pandas as pd
import random
from datetime import datetime, timedelta

# Generate Users Data
user_ids = list(range(1, 51))  # 50 users
usernames = [f"@user{i}" for i in user_ids]
age_groups = random.choices(["Teen", "Adult", "Senior"], k=50)
countries = random.choices(["US", "UK", "India", "Canada", "Germany"], k=50)
verified = [random.choice([True, False]) for _ in range(50)]

users_df = pd.DataFrame({
    "UserID": user_ids,
    "Username": usernames,
    "AgeGroup": age_groups,
    "Country": countries,
    "Verified": verified
})

# Generate Posts Data
posts = []
hashtags_list = ["#tech", "#innovation", "#mood", "#fail", "#trending", "#sports", "#food", "#music", "#news"]

for post_id in range(101, 201):
    user_id = random.choice(user_ids)
    timestamp = datetime(2023, 10, 5) + timedelta(minutes=random.randint(0, 1440))
    likes = random.randint(0, 500)
    retweets = random.randint(0, 200)
    sentiment_score = round(random.uniform(-1, 1), 2)
    hashtags = ",".join(random.sample(hashtags_list, random.randint(1, 3)))
    
    posts.append([post_id, user_id, f"Sample post content {post_id}", timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                  likes, retweets, hashtags, sentiment_score])

posts_df = pd.DataFrame(posts, columns=["PostID", "UserID", "Content", "Timestamp", "Likes", "Retweets", "Hashtags", "SentimentScore"])

# Save as CSV
users_df.to_csv("users.csv", index=False)
posts_df.to_csv("posts.csv", index=False)

print("Generated users.csv and posts.csv with 100+ records each.")