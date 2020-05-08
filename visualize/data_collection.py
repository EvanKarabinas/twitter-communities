import seaborn as sns
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # nopep8


sns.set(style="white", color_codes=True)
# Set up the matplotlib figure
f, axes = plt.subplots(1, 2, figsize=(7, 7), sharex=True)


# Connect to DB
connection = psycopg2.connect(
    database=config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)
cur = connection.cursor()
print(connection)

graph_name = "syriza_gr"
cur.execute(
    "SELECT * FROM twitter_user WHERE graph_name = %s", (graph_name,))
syriza_users = cur.fetchall()

graph_name = "neademokratia"
cur.execute(
    "SELECT * FROM twitter_user WHERE graph_name = %s", (graph_name,))
neademokratia_users = cur.fetchall()

# create DataFrame using data
syriza_df = pd.DataFrame(syriza_users, columns=[
                         'id', 'name', 'followers_count', 'friends_count', 'level', 'statuses_count', 'graph_name'])
nd_df = pd.DataFrame(neademokratia_users, columns=[
                     'id', 'name', 'followers_count', 'friends_count', 'level', 'statuses_count', 'graph_name'])


syriza_friends = syriza_df['friends_count']
syriza_friends_limit = syriza_friends.loc[syriza_df['friends_count'] < 300]
sns.distplot(syriza_friends_limit, color="red",
             kde=False, ax=axes[0]).set_title("@syriza_gr")

nd_friends = nd_df['friends_count']
nd_friends_limit = nd_friends.loc[nd_df['friends_count'] < 300]
sns.distplot(nd_friends_limit, color="blue", kde=False,
             ax=axes[1]).set_title("@neademokratia")


plt.tight_layout()
plt.show()
# df.loc[df['friends_count'] <10000]
