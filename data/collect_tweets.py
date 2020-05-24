import json
import tweepy
import sys
import psycopg2
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # nopep8


class User:
    def __init__(self, uid, name):
        self.id = uid
        self.name = name

    def __str__(self):
        return f"{self.id}|{self.name}"


class Tweet:
    def __init__(self, id_str, user_id_str, full_text, created_at, hashtags, retweet_count, favorite_count, graph_name):
        self.id_str = id_str
        self.user_id_str = user_id_str
        self.full_text = full_text
        self.created_at = created_at
        self.hashtags = []
        for hashtag in hashtags:
            self.hashtags.append(hashtag["text"])
        self.retweet_count = retweet_count
        self.favorite_count = favorite_count
        self.graph_name = graph_name

    def __str__(self):
        return f"{self.id_str}\n\t{self.user_id_str} \n\t{self.full_text} \n\t{self.created_at}\n\t{self.hashtags}\n\t{self.retweet_count}\n\t{self.favorite_count}\n\t{self.graph_name}"


def fetch_users(cur, graph_name, max_friends):
    users = list()

    cur.execute("SELECT * FROM twitter_user WHERE graph_name = %s AND friends_count < %s",
                (graph_name, max_friends,))
    db_users = cur.fetchall()

    for db_user in db_users:
        user = User(db_user[0], db_user[1])
        users.append(user)

    cur.execute("SELECT avg(statuses_count) FROM twitter_user WHERE graph_name = %s AND friends_count < %s",
                (graph_name, max_friends))
    avg_statuses = cur.fetchone()
    return users, int(avg_statuses[0])


def save_hashtags_to_db(db_connection, hashtags, tweet_id_str, graph_name):
    cur = db_connection.cursor()
    for hashtag in hashtags:
        cur.execute("INSERT INTO hashtag (hashtag_text,tweet_id_str,graph_name) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
                    (hashtag, tweet_id_str, graph_name))
        db_connection.commit()


def save_tweet_to_db(db_connection, tweet):
    cur = db_connection.cursor()
    cur.execute(
        "INSERT INTO tweet (id_str,user_id_str,full_text,created_at,retweet_count,favorite_count,graph_name) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (tweet.id_str, tweet.user_id_str, tweet.full_text, tweet.created_at, tweet.retweet_count, tweet.favorite_count, tweet.graph_name))
    db_connection.commit()
    save_hashtags_to_db(db_connection, tweet.hashtags,
                        tweet.id_str, tweet.graph_name)


def fetch_tweets(cur, api, graph_name, max_friends):
    connection = psycopg2.connect(
        database=config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)

    users, avg_statuses = fetch_users(cur, graph_name, max_friends)
    user = users[0]
    print(user.id, user.name)
    tweets_counter = 0
    print(f"Average tweets : {avg_statuses}")

    for fetched_tweet in tweepy.Cursor(api.user_timeline, id=user.id, tweet_mode='extended', count=200).items():
        tweets_counter += 1
        tweet = (Tweet(fetched_tweet.id_str, fetched_tweet.user.id_str,
                       fetched_tweet.full_text, fetched_tweet.created_at, fetched_tweet.entities[
                           "hashtags"],
                       fetched_tweet.retweet_count, fetched_tweet.favorite_count, graph_name))
        save_tweet_to_db(connection, tweet)

        if(tweets_counter == 2000):
            break


threads_num = int(sys.argv[2])

# Connect to APIs
auths = []
apis = []
for i in range(threads_num):
    auths.append(tweepy.OAuthHandler(
        config.api_keys[i], config.api_secret_keys[i]))
    apis.append(tweepy.API(
        auths[i], wait_on_rate_limit=True))
print(f"Apis : {len(apis)}")

graph_name = sys.argv[1]

# Connect to DB
connection = psycopg2.connect(
    database=config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)
cur = connection.cursor()
cur.execute("DELETE FROM tweet WHERE graph_name = %s", (graph_name,))
rows_deleted = cur.rowcount
print(
    f"Deleted ({rows_deleted}) tweets that belong to graph {graph_name}.")
connection.commit()


fetch_tweets(cur, apis[0], graph_name, 2000)
