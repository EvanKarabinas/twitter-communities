import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import sys
import psycopg2
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # nopep8

graph_name = sys.argv[1]

# Connect to DB
connection = psycopg2.connect(
    database=config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)

cur = connection.cursor()

hashtag_text = "COVID19"
cur.execute(
    "SELECT * FROM hashtag WHERE hashtag_text=%s AND graph_name = %s", (hashtag_text, graph_name,))
hashtags = cur.fetchall()

tweets = []
for hashtag in hashtags:
    twitter_id = hashtag[1]
    cur.execute("SELECT * FROM tweet WHERE id_str=%s AND graph_name = %s",
                (twitter_id, graph_name,))
    tweets.append(cur.fetchone())


print(f"Tweets: {len(tweets)}")

tweets = [tweet for tweet in tweets if tweet is not None]

tweets.sort(key=lambda x: x[3])
print(tweets[0][3])

tweets_hash = dict()

for tweet in tweets:
    tweets_hash.setdefault(tweet[3].date(), []).append(tweet)

print(f"Days: {len(tweets_hash.keys())}")
x = []
y = []
# print(tweets_hash.keys())
for date, tweets in tweets_hash.items():
    print(f"{date} | tweets: {len(tweets)}\n")
    x.append(date)
    y.append(len(tweets))


tweets_users_hash = dict()
for date, tweets in tweets_hash.items():
    for tweet in tweets:
        cur.execute("SELECT * FROM twitter_user WHERE id_str=%s", (tweet[1],))
        user = cur.fetchone()
        tweets_users_hash.setdefault(date, []).append((user[0], user[1]))

#data = pd.Series(tweets_hash, name='DateValue')

ax = sns.lineplot(x=x, y=y, palette="tab10", linewidth=2.5)
# for date, users in tweets_users_hash.items():
#     print(graph_name+" tweeted at:")
#     for user in users:
#         if(user[1] == graph_name):
#             print(date)
ax.set(xlabel='dates', ylabel='number of tweets containing #COVID19')
plt.tight_layout()
plt.show()
