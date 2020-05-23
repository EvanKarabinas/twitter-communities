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


def fetch_tweets(cur, api, graph_name, max_friends):
    users, avg_statuses = fetch_users(cur, graph_name, max_friends)
    user = users[0]
    print(user.id, user.name)
    counter = 0
    print(f"Average tweets : {avg_statuses}")
    for fetched_tweet in tweepy.Cursor(api.user_timeline, id=user.id, tweet_mode='extended').items():
        print(
            f"{fetched_tweet.id} \n{fetched_tweet.full_text} \n{fetched_tweet.retweeted}\n\n")
        counter += 1
        if(counter == 10):
            sys.exit()


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


# Connect to DB
connection = psycopg2.connect(
    database=config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)
cur = connection.cursor()

graph_name = sys.argv[1]

fetch_tweets(cur, apis[0], graph_name, 2000)
