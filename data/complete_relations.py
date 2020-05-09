
import tweepy
import csv
import time
import timeformatter as tf
import sys
import threading
import os
from blessings import Terminal
import psycopg2
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # nopep8

space = 69*' '


class User:
    def __init__(self, uid, name):
        self.id = uid
        self.name = name

    def __str__(self):
        return f"{self.id}|{self.name}"

    def __eq__(self, other):
        return self.id == other.id


def print_header(thread_id):
    global term
    xoffset = 0
    yoffset = thread_id*10

    if(thread_id >= 5):
        xoffset = 70
        yoffset = (thread_id-5)*10
    if(thread_id >= 10):
        xoffset = 140
        yoffset = (thread_id-10)*10

    print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset)+term.white + term.on_bright_black +
          f"  Thread {thread_id}  " + term.normal)


def print_current_user_check_bar(thread_id, user):
    global term
    xoffset = 4
    yoffset = thread_id*10+2

    if(thread_id >= 5):
        xoffset = 74
        yoffset = (thread_id-5)*10+2
    if(thread_id >= 10):
        xoffset = 144
        yoffset = (thread_id-10)*10+2

    print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset) +
          f"{term.bright_black}Currently checking user - {term.white} {term.bold}{user.name}{term.normal}")


def print_status_bar(thread_id, msg, type):
    global term
    xoffset = 4
    yoffset = thread_id*10+3

    if(thread_id >= 5):
        xoffset = 74
        yoffset = (thread_id-5)*10+3
    if(thread_id >= 10):
        xoffset = 144
        yoffset = (thread_id-10)*10+3

    if(type == 'success'):
        print(term.move(yoffset, xoffset)+space + term.move(yoffset, xoffset) + term.bright_black+"Status: "+term.green +
              msg + term.normal)
    elif(type == 'error'):
        print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset) + term.bright_black+"Status: "+term.red +
              msg + term.normal)


def print_pogress_bar(thread_id, user_index, batch_size):
    xoffset = 4
    yoffset = thread_id*10+4

    if(thread_id >= 5):
        xoffset = 74
        yoffset = (thread_id-5)*10+4
    if(thread_id >= 10):
        xoffset = 144
        yoffset = (thread_id-10)*10+4

    global term
    print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset) +
          f"{term.bright_black}Checked users:\t{term.cyan}{user_index}/{batch_size}{term.bright_black}." + term.normal)


def print_estimated_time_bar(thread_id, user_index, batch_size, avg_friends):
    global term
    xoffset = 4
    yoffset = thread_id*10+5

    if(thread_id >= 5):
        xoffset = 74
        yoffset = (thread_id-5)*10+5
    if(thread_id >= 10):
        xoffset = 144
        yoffset = (thread_id-10)*10+5

    remaining_time_minutes = (batch_size - user_index)*(avg_friends/200)
    if(remaining_time_minutes >= 60):
        time_string = f"{remaining_time_minutes//60} h. {remaining_time_minutes%60} min."
    else:
        time_string = f"{remaining_time_minutes} min."

    print(term.move(yoffset, xoffset) + space + term.move(yoffset, xoffset) +
          f"{term.bright_black}Estimated time:\t{term.cyan}{time_string}" + term.normal)


def extract_users(cur, graph_name, max_friends):
    users = list()

    cur.execute("SELECT * FROM twitter_user WHERE graph_name = %s AND friends_count < %s",
                (graph_name, max_friends,))
    db_users = cur.fetchall()

    for db_user in db_users:
        user = User(db_user[0], db_user[1])
        users.append(user)

    cur.execute("SELECT avg(friends_count) FROM twitter_user WHERE graph_name = %s AND friends_count < %s",
                (graph_name, max_friends))
    avg_friends = cur.fetchone()
    return users, int(avg_friends[0])


def fetch_friends(user, api, thread_id):
    friends = list()
    try:
        for fetched_friend in tweepy.Cursor(api.friends, id=user.id, count=200).items():
            friend = User(fetched_friend.id_str, fetched_friend.screen_name)
            friends.append(friend)
            with term_print_lock:
                print_status_bar(
                    thread_id, f"Fetched {len(friends)} friends successfully.", "success")
    except tweepy.TweepError:
        with term_print_lock:
            print_status_bar(
                thread_id, f"Failed to fetch friends for user ({user.name})", "error")

    return friends


def save_to_db(db_connection, user, friends, users, graph_name):
    cur = db_connection.cursor()
    for friend in friends:
        for other_user in users:
            if(friend.id == other_user.id):
                cur.execute(
                    "INSERT INTO followship (follower_id,followee_id,graph_name) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING", (user.id, friend.id, graph_name))
                db_connection.commit()


def complete_relations(thread_id, users, batch_size, api, avg_friends, graph_name):

    connection = psycopg2.connect(
        database=config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)

    with term_print_lock:
        print_header(thread_id)

    users_batch = users[(thread_id * batch_size):(thread_id * batch_size + batch_size-1)]

    for user_index, user in enumerate(users_batch):
        with term_print_lock:
            print_current_user_check_bar(thread_id, user)
            print_status_bar(
                thread_id, "-", "success")
            print_pogress_bar(thread_id, user_index, batch_size)
            print_estimated_time_bar(
                thread_id, user_index, batch_size, avg_friends)

        friends = fetch_friends(user, api, thread_id)
        save_to_db(connection, user, friends, users, graph_name)


graph_name = sys.argv[1]
threads_num = int(sys.argv[2])

auths = []
apis = []
for i in range(threads_num):
    auths.append(tweepy.OAuthHandler(
        config.api_keys[i], config.api_secret_keys[i]))
    apis.append(tweepy.API(
        auths[i], wait_on_rate_limit=True))

start_time = time.time()

# Connect to DB
connection = psycopg2.connect(
    database=config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)
cur = connection.cursor()

cur.execute("DELETE FROM followship WHERE graph_name = %s", (graph_name,))
rows_deleted = cur.rowcount
print(
    f"Deleted ({rows_deleted}) twitter users that belong to graph {graph_name}.")
connection.commit()

users = list()

users, avg_friends = extract_users(cur, graph_name, 2000)
# for user in users:
#     print(user)
# sys.exit(0)
threads = list()
batch_size = int(len(users)/threads_num)
term_print_lock = threading.Lock()
term = Terminal()

print(term.enter_fullscreen)

for index in range(len(apis)):
    t = threading.Thread(target=complete_relations,
                         args=(index, users, batch_size, apis[index], avg_friends, graph_name))
    threads.append(t)
    t.start()


for index, thread in enumerate(threads):
    thread.join()


end_time = time.time()

# print(tf.format(start_time, end_time))
