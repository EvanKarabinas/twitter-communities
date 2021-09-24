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

sns.set(style="white", color_codes=True)
# Set up the matplotlib figure

# Connect to DB
connection = psycopg2.connect(
    database=config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)

cur = connection.cursor()


cur.execute(
    "SELECT hashtag_text FROM hashtag WHERE graph_name = %s", (graph_name,))
hashtags = cur.fetchall()
# print(hashtags[0:10])

hashtags_df = pd.DataFrame(hashtags, columns=['hashtag'])
#hashtags_df['ocurancies'] = hashtags_df['ocurancies'].astype(int)
print(hashtags_df)
sns.countplot(y='hashtag', data=hashtags_df, order=pd.value_counts(
    hashtags_df['hashtag']).iloc[:20].index)

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
