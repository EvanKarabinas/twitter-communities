from pretty_print import *
import timeformatter as tf
from blessings import Terminal
import threading
import json
import tweepy
import sys
import psycopg2
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # nopep8
#space = 69*' '


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


# def print_header(thread_id):
#     global term
#     xoffset = 0
#     yoffset = thread_id*10

#     if(thread_id >= 5):
#         xoffset = 70
#         yoffset = (thread_id-5)*10
#     if(thread_id >= 10):
#         xoffset = 140
#         yoffset = (thread_id-10)*10

#     print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset)+term.white + term.on_bright_black +
#           f"  Thread {thread_id}  " + term.normal)


# def print_current_user_check_bar(thread_id, user):
#     global term
#     xoffset = 4
#     yoffset = thread_id*10+2

#     if(thread_id >= 5):
#         xoffset = 74
#         yoffset = (thread_id-5)*10+2
#     if(thread_id >= 10):
#         xoffset = 144
#         yoffset = (thread_id-10)*10+2

#     print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset) +
#           f"{term.bright_black}Currently checking user - {term.white} {term.bold}{user.name}{term.normal}")


# def print_status_bar(thread_id, msg, type):
#     global term
#     xoffset = 4
#     yoffset = thread_id*10+3

#     if(thread_id >= 5):
#         xoffset = 74
#         yoffset = (thread_id-5)*10+3
#     if(thread_id >= 10):
#         xoffset = 144
#         yoffset = (thread_id-10)*10+3

#     if(type == 'success'):
#         print(term.move(yoffset, xoffset)+space + term.move(yoffset, xoffset) + term.bright_black+"Status: "+term.green +
#               msg + term.normal)
#     elif(type == 'error'):
#         print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset) + term.bright_black+"Status: "+term.red +
#               msg + term.normal)


# def print_pogress_bar(thread_id, user_index, batch_size):
#     xoffset = 4
#     yoffset = thread_id*10+4

#     if(thread_id >= 5):
#         xoffset = 74
#         yoffset = (thread_id-5)*10+4
#     if(thread_id >= 10):
#         xoffset = 144
#         yoffset = (thread_id-10)*10+4

#     global term
#     print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset) +
#           f"{term.bright_black}Checked users:\t{term.cyan}{user_index}/{batch_size}{term.bright_black}." + term.normal)


# def print_estimated_time_bar(thread_id, user_index, batch_size, avg_requests):
#     global term
#     xoffset = 4
#     yoffset = thread_id*10+5

#     if(thread_id >= 5):
#         xoffset = 74
#         yoffset = (thread_id-5)*10+5
#     if(thread_id >= 10):
#         xoffset = 144
#         yoffset = (thread_id-10)*10+5

#     remaining_time_minutes = (batch_size - user_index)*(avg_requests/200)/15
#     if(remaining_time_minutes >= 60):
#         time_string = f"{remaining_time_minutes//60} h. {remaining_time_minutes%60} min."
#     else:
#         time_string = f"{remaining_time_minutes} min."

#     print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset) +
#           f"{term.bright_black}Estimated time:\t{term.cyan}{time_string}" + term.normal)


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
    try:
        cur.execute(
            "INSERT INTO tweet (id_str,user_id_str,full_text,created_at,retweet_count,favorite_count,graph_name) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (tweet.id_str, tweet.user_id_str, tweet.full_text, tweet.created_at, tweet.retweet_count, tweet.favorite_count, tweet.graph_name))
        db_connection.commit()
        save_hashtags_to_db(db_connection, tweet.hashtags,
                            tweet.id_str, tweet.graph_name)
    except:
        pass


def fetch_tweets(thread_id, users, batch_size, api, max_tweets, graph_name):
    connection = psycopg2.connect(
        database=config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)

    with term_print_lock:
        print_header(thread_id)

    tweets_counter = 0

    users_batch = users[(thread_id * batch_size)                        :(thread_id * batch_size + batch_size-1)]
    for user_index, user in enumerate(users_batch):
        tweets_counter = 0
        print_current_user_check_bar(thread_id, user)
        try:
            for fetched_tweet in tweepy.Cursor(api.user_timeline, id=user.id, tweet_mode='extended', count=200).items():
                tweets_counter += 1
                tweet = (Tweet(fetched_tweet.id_str, fetched_tweet.user.id_str,
                               fetched_tweet.full_text, fetched_tweet.created_at, fetched_tweet.entities[
                                   "hashtags"],
                               fetched_tweet.retweet_count, fetched_tweet.favorite_count, graph_name))
                save_tweet_to_db(connection, tweet)
                with term_print_lock:
                    print_status_bar(
                        thread_id, f"Fetched {tweets_counter} tweets successfully.", "success")
                    print_pogress_bar(thread_id, user_index, batch_size)
                    print_estimated_time_bar(
                        thread_id, user_index, batch_size, max_tweets, 15)

                if(tweets_counter == 2000):
                    tweets_counter = 0
                    break
        except tweepy.TweepError:
            with term_print_lock:
                print_status_bar(
                    thread_id, f"Failed to fetch tweets for user ({user.name})", "error")


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

users, avg_tweets = fetch_users(cur, graph_name, 2000)
max_tweets = 2000
threads = list()
batch_size = int(len(users)/threads_num)
term_print_lock = threading.Lock()
init_pretty_print()
#term = Terminal()

# print(term.enter_fullscreen)

for index in range(len(apis)):
    t = threading.Thread(target=fetch_tweets,
                         args=(index, users, batch_size, apis[index], max_tweets, graph_name))
    threads.append(t)
    t.start()


for index, thread in enumerate(threads):
    thread.join()
