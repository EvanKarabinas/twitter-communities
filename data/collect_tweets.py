import tweepy
import sys
import psycopg2
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # nopep8

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
