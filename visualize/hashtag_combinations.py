import time
import seaborn as sns
import pandas as pd
import datetime
import sys
import psycopg2
import matplotlib.pyplot as plt
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # nopep8

graph_name = sys.argv[1]
inp_hashtag = sys.argv[2]
sns.set(style="white", color_codes=True)
# Set up the matplotlib figure

# Connect to DB
connection = psycopg2.connect(
    database=config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)

cur = connection.cursor()

cur.execute(
    "SELECT * FROM hashtag WHERE graph_name = %s", (graph_name,))
hashtags = cur.fetchall()

all_hashtag_ids = []
for hashtag in hashtags:
    if(hashtag[0] == inp_hashtag):
        all_hashtag_ids.append(hashtag[1])
print(f"Hashtag {inp_hashtag} is in {len(all_hashtag_ids)} tweets.\n")

hashtag_combinations = []
counter = 0

start_time = time.time()

for hashtag in hashtags:
    if(hashtag[1] in all_hashtag_ids and not hashtag[0] == inp_hashtag):
        hashtag_combinations.append(hashtag[0])

print("--- %s seconds ---" % (time.time() - start_time))
print(hashtag_combinations[0])
hashtags_df = pd.DataFrame(hashtag_combinations, columns=['hashtag'])

sns.countplot(y='hashtag', data=hashtags_df, order=pd.value_counts(
    hashtags_df['hashtag']).iloc[:20].index)

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
