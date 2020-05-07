import config
import tweepy
import csv
import time
import timeformatter as tf
import sys
import threading
from blessings import Terminal


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
    print(term.move(thread_id*10, 0))
    print(term.white + term.on_black +
          f"  Thread {thread_id}  " + term.normal)


def print_current_user_check_bar(thread_id, user):
    global term
    print(term.clear_eol+term.move(thread_id*10+2, 4) +
          f"{term.black}Currently checking user - {term.white} {term.bold}{user.name}{term.normal}")


def print_status_bar(thread_id, msg, type):
    global term
    if(type == 'success'):
        print(term.move(thread_id*10+3, 4) + term.black+"Status: "+term.green +
              msg + term.normal)
    elif(type == 'error'):
        print(term.move(thread_id*10+3, 4) + term.black+"Status: "+term.red +
              msg + term.normal)


def print_pogress_bar(thread_id, user_index, batch_size):
    global term
    print(term.move(thread_id*10+4, 4) +
          f"{term.black}Checked users:\t{term.cyan}{user_index}/{batch_size}{term.black}." + term.normal)


def print_estimated_time_bar(thread_id, user_index, batch_size):
    global term
    remaining_time_minutes = (batch_size - user_index)
    if(remaining_time_minutes >= 60):
        time_string = f"{remaining_time_minutes//60} h. {remaining_time_minutes%60} min."
    else:
        time_string = f"{remaining_time_minutes} min."

    print(term.move(thread_id*10+5, 4) +
          f"{term.black}Estimated time:\t{term.cyan}{time_string}" + term.normal)


def extract_users(input_file):
    users = list()
    user_ids = list()

    file_headers = input_file.readline()
    for line in input_file:
        followee_id, followee_name, follower_id, follower_name = [element.strip() for element in line.split(
            ",")]

        if(followee_id not in user_ids):
            followee = User(followee_id, followee_name)
            users.append(followee)
            user_ids.append(followee_id)
        if(follower_id not in user_ids):
            follower = User(follower_id, follower_name)
            users.append(follower)
            user_ids.append(follower_id)
    return users


def fetch_friends(user, api, thread_id):
    friends = list()
    try:
        for fetched_friend in tweepy.Cursor(api.friends, id=user.id, count=200).items():
            friend = User(fetched_friend.id, fetched_friend.screen_name)
            friends.append(friend)
            with term_print_lock:
                print_status_bar(
                    thread_id, f"Fetched {len(friends)} friends successfully.", "success")
    except tweepy.TweepError:
        with term_print_lock:
            print_status_bar(
                thread_id, f"Failed to fetch friends for user ({user.name})", "error")

    return friends


def save_to_csv(csv_writer, user, friends, users):
    for friend in friends:
        for other_user in users:
            if(friend.id == other_user.id):
                row = [str(friend.id), str(friend.name),
                       str(user.id), str(user.name)]
                csv_writer.writerow(row)


def complete_relations(thread_id, users, batch_size, api, file_name):

    with term_print_lock:
        print_header(thread_id)

    with open(f"{file_name}_thread{thread_id}_complete.csv", 'w') as output_csv:
        csv_writer = csv.writer(output_csv)

        users_batch = users[(thread_id * batch_size): (thread_id * batch_size + batch_size-1)]

        for user_index, user in enumerate(users_batch):
            with term_print_lock:
                print_current_user_check_bar(thread_id, user)
                print_status_bar(
                    thread_id, "-", "success")
                print_pogress_bar(thread_id, user_index, batch_size)
                print_estimated_time_bar(thread_id, user_index, batch_size)

            friends = fetch_friends(user, api, thread_id)
            save_to_csv(csv_writer, user, friends, users)
            output_csv.flush()


input_file = sys.argv[1]
output_file_name = input_file.split(".")[0]
threads_num = int(sys.argv[2])

auths = []
apis = []
for i in range(threads_num):
    auths.append(tweepy.OAuthHandler(
        config.api_keys[i], config.api_secret_keys[i]))
    apis.append(tweepy.API(
        auths[i], wait_on_rate_limit=True))

start_time = time.time()

field_names = ["followee_id", "followee_name", "follower_id", "follower_name"]

users = list()
with open(f"output_files/{input_file}", 'r') as followers_csv:
    users = extract_users(followers_csv)
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
                         args=(index, users, batch_size, apis[index], output_file_name))
    threads.append(t)
    t.start()


for index, thread in enumerate(threads):
    thread.join()


end_time = time.time()
# print(tf.format(start_time, end_time))
