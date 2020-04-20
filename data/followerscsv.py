import config
import tweepy
import csv
import time
import timeformatter as tf
import sys


class User:
    def __init__(self, uid, name):
        self.id = uid
        self.name = name

    def __str__(self):
        return f"{self.id}|{self.name}"


def fetch_followers(user, sample_size, csv_writer):
    followers = []

    try:
        user_fetched_followers = api.followers(id=user.id, count=sample_size)
        for fetched_follower in user_fetched_followers:
            follower = User(fetched_follower.id, fetched_follower.screen_name)
            followers.append(follower)
            row = [str(user.id), str(user.name), str(
                follower.id), str(follower.name)]
            csv_writer.writerow(row)

    except tweepy.TweepError:
        print(f"Failed to fetch followers for user ({user.id},{user.name})")

    return followers


def fetch_user(name):
    fetched_user = api.get_user(screen_name=name)
    return User(fetched_user.id, fetched_user.screen_name)


def expand_graph(depth_level, sample_size, starting_user, csv_writer, out_file):
    users = [starting_user]
    next_level_users = []
    all_users = []

    for level in range(0, depth_level):
        for user in users:

            user_followers = fetch_followers(
                user, sample_size[level], csv_writer)
            out_file.flush()
            next_level_users += user_followers
            out_file.flush()
        users = next_level_users
        print(
            f"Depth level {str(level)} expanded successfully. (Found {len(users)} users)")
        next_level_users = []


auth = tweepy.OAuthHandler(config.api_key, config.api_secret_key)
api = tweepy.API(auth, wait_on_rate_limit=True,
                 wait_on_rate_limit_notify=True)

print(f"Input name: {sys.argv[1]}")
starting_user = fetch_user(sys.argv[1])

start_time = time.time()

field_names = ["followee_id", "followee_name", "follower_id", "follower_name"]

with open(f"{sys.argv[1]}_followers_graph_large_100_20_20_new.csv", 'w') as followers_csv:
    csv_writer = csv.writer(followers_csv)
    csv_writer.writerow(field_names)
    expand_graph(3, [100, 20, 30], starting_user, csv_writer, followers_csv)

end_time = time.time()
print(tf.format(start_time, end_time))
