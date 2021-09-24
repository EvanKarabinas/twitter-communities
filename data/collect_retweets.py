import tweepy
import csv
import time
import timeformatter as tf
import sys
import psycopg2
import threading
import os
import sys
import inspect
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # nopep8


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch_retweets(thread_id, tweets, batch_size, api, graph_name):
    print(f"HI from thread {thread_id}")
    completed_counter = 0

    connection = psycopg2.connect(
        database=config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)
    cur = connection.cursor()

    tweets_batch = tweets[(thread_id * batch_size):(thread_id * batch_size + batch_size-1)]

    for tweets_chunk in chunks(tweets_batch, 100):

        completed_counter += 100
        #print(f"{completed_counter} - THREAD {thread_id}")

        try:
            fetched_tweets = api.statuses_lookup(tweets_chunk)
            #print(f"FETCHED: {len(fetched_tweets)}, THREAD : {thread_id}\n")
            for fetched_tweet in fetched_tweets:
                if(hasattr(fetched_tweet, "retweeted_status")):
                    if(fetched_tweet.retweeted_status):
                        # print(
                        #    f"{fetched_tweet.id_str} is a retweet of {fetched_tweet.retweeted_status.id_str}\n")
                        # print(fetched_tweet.retweeted_status)
                        #print(f"Retweet : {fetched_tweet.text}\n")
                        #print(f"Original: {fetched_tweet.retweeted_status.text}\n")
                        # os._exit(1)
                        with tweets_lock:
                            cur.execute("INSERT INTO retweet (tweet_id,original_tweet_id,graph_name) VALUES (%s,%s,%s);",
                                        (fetched_tweet.id_str, fetched_tweet.retweeted_status.id_str, graph_name))
                            connection.commit()

        except tweepy.TweepError:
            print(
                f"Failed to fetch info for tweet")

        print(
            f"Checked {completed_counter} tweets. Remaining {len(tweets_batch)-completed_counter}. [THREAD ({thread_id})].\n")
    connection.close()
    #print(f"Followers {len(followers)}")


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
    database=config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)
cur = connection.cursor()

# Fetch input user (root)
print(f"Input name: {sys.argv[1]}")

graph_name = sys.argv[1]

# delete existing twitter users that belong to that graph
cur.execute("DELETE FROM retweet WHERE graph_name = %s", (graph_name,))
rows_deleted = cur.rowcount
print(
    f"Deleted ({rows_deleted}) retweets that belong to graph {graph_name}.")
connection.commit()

connection.commit()


start_time = time.time()

tweets_lock = threading.Lock()

cur.execute("SELECT id_str FROM tweet WHERE graph_name = %s", (graph_name,))
tweets = cur.fetchall()
tweets = [tweet[0] for tweet in tweets]
cur.close()
connection.close()
batch_size = int(len(tweets)/threads_num)
threads = []
for index in range(len(apis)):
    t = threading.Thread(target=fetch_retweets,
                         args=(index, tweets, batch_size, apis[index], graph_name))
    threads.append(t)
    t.start()


for index, thread in enumerate(threads):
    thread.join()

end_time = time.time()
print(tf.format(start_time, end_time))
