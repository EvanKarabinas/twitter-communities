import config
import tweepy
import csv
import time
import timeformatter as tf
import sys
import psycopg2
import threading


class User:
    def __init__(self, uid, name):
        self.id = uid
        self.name = name

    def __str__(self):
        return f"{self.id}|{self.name}"


def fetch_followers(thread_id, api, users, sample_size, cur, level, followers):

    # Connect to DB
    connection = psycopg2.connect(
        database=congig.db_name, user=config.db_user, host=config.db_host)
    user = None

    while(users):

        with users_lock:
            user = users.pop(0)
        cur = connection.cursor()

        try:
            user_fetched_followers = api.followers(
                id=user.id, count=sample_size)
            for fetched_follower in user_fetched_followers:
                follower = User(fetched_follower.id_str,
                                fetched_follower.screen_name)
                followers.append(follower)
                row = [str(user.id), str(user.name), str(
                    follower.id), str(follower.name)]
                with users_lock:
                    cur.execute("INSERT INTO twitter_user (id_str,screen_name,followers_count,friends_count,level,statuses_count,graph_name) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                                (fetched_follower.id_str, fetched_follower.screen_name, fetched_follower.followers_count, fetched_follower.friends_count, level, fetched_follower.statuses_count, graph_name))
                    connection.commit()

        except tweepy.TweepError:
            print(
                f"Failed to fetch followers for user ({user.id},{user.name})")
    connection.close()
    #print(f"Followers {len(followers)}")


def fetch_user(api, name):
    fetched_user = api.get_user(screen_name=name)
    return fetched_user


def expand_graph(apis, depth_level, sample_size, starting_user):
    threads = []
    users = [starting_user]
    next_level_users = []
    all_users = []

    for level in range(0, depth_level):
        followers = []

        for index in range(len(apis)):
            t = threading.Thread(target=fetch_followers,
                                 args=(index, apis[index], users, sample_size[level], cur, level, followers))
            threads.append(t)
            t.start()

        # user_followers = fetch_followers(
        #     user, sample_size[level], cur, level, db_connection)

        #next_level_users += user_followers
        for index, thread in enumerate(threads):
            thread.join()

        users = followers
        print(
            f"Depth level {str(level)} expanded successfully. (Found {len(users)} users)")


threads_num = int(sys.argv[2])

auths = []
apis = []
for i in range(threads_num):
    auths.append(tweepy.OAuthHandler(
        config.api_keys[i], config.api_secret_keys[i]))
    apis.append(tweepy.API(
        auths[i], wait_on_rate_limit=True))
print(f"Apis : {len(apis)}")
# # Connect to API
# auth = tweepy.OAuthHandler(config.api_keys[0], config.api_secret_keys[0])
# api = tweepy.API(auth, wait_on_rate_limit=True,
#                  wait_on_rate_limit_notify=True)

# Connect to DB
connection = psycopg2.connect(
    database="polyzer", user="evank", host="localhost")
cur = connection.cursor()

# Fetch input user (root)
print(f"Input name: {sys.argv[1]}")
starting_user = fetch_user(apis[0], sys.argv[1])
graph_name = starting_user.screen_name

# delete existing twitter users that belong to that graph
cur.execute("DELETE FROM twitter_user WHERE graph_name = %s", (graph_name,))
rows_deleted = cur.rowcount
print(
    f"Deleted ({rows_deleted}) twitter users that belong to graph {graph_name}.")
connection.commit()
cur.execute("INSERT INTO twitter_user (id_str,screen_name,followers_count,friends_count,level,statuses_count,graph_name) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
            (starting_user.id_str, starting_user.screen_name, starting_user.followers_count, starting_user.friends_count, -1, starting_user.statuses_count, graph_name))
connection.commit()
cur.close()
connection.close()

start_time = time.time()

users_lock = threading.Lock()
expand_graph(apis, 3, [100, 20, 40], starting_user)

end_time = time.time()
print(tf.format(start_time, end_time))
