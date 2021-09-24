from collections import defaultdict
import datetime
import networkx as nx
from networkx.readwrite import json_graph
from networkx.algorithms.community import greedy_modularity_communities
import json
import os
import sys
import psycopg2
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # nopep8


def graph_stats(graph):
    print("\n===========================")
    print(f"Graph created successfully!")
    print(f"Number of Nodes : {graph.number_of_nodes()}")
    print(f"Number of Edges : {graph.number_of_edges()}")
    print("===========================\n")


input_graph_name = sys.argv[1]
output_file = input_graph_name


# Connect to DB
connection = psycopg2.connect(
    database=config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)

cur = connection.cursor()


G = nx.Graph()
if(input_graph_name == 'both'):
    cur.execute("""SELECT tweet_id,tweet2.user_id_str AS tweet_user_id, original_tweet_id,tweet1.user_id_str
            AS original_tweet_user_id,tweet2.created_at,hashtag_text FROM retweet INNER JOIN tweet AS tweet1 ON original_tweet_id=tweet1.id_str
            INNER JOIN tweet AS tweet2 ON tweet_id=tweet2.id_str LEFT JOIN hashtag ON tweet_id_str=tweet_id;""",
                ())
else:
    cur.execute("""SELECT tweet_id,tweet2.user_id_str AS tweet_user_id, original_tweet_id,tweet1.user_id_str
            AS original_tweet_user_id,tweet2.created_at,hashtag_text FROM retweet INNER JOIN tweet AS tweet1 ON original_tweet_id=tweet1.id_str
            INNER JOIN tweet AS tweet2 ON tweet_id=tweet2.id_str LEFT JOIN hashtag ON tweet_id_str=tweet_id WHERE tweet1.graph_name=%s;""",
                (input_graph_name,))
retweets = cur.fetchall()
print(f"Retweets: {len(retweets)}")
for retweet in retweets[0:5]:
    print(retweet)

for retweet in retweets:
    G.add_edge(retweet[1], retweet[3],
               created_at=retweet[4].date().strftime('%d-%m-%Y'))

if(input_graph_name == 'both'):
    cur.execute("SELECT id_str,screen_name FROM twitter_user;", ())
else:
    cur.execute(
        "SELECT id_str,screen_name FROM twitter_user WHERE graph_name=%s;", (input_graph_name,))

users = cur.fetchall()
for node in list(G):
    for user in users:
        if(node == user[0]):
            G.nodes[node]['screen_name'] = user[1]
#x = nx.betweenness_centrality(G)
# x_list = [(k, v) for k, v in x.items()]
# x_sorted = sorted(x_list, key=lambda x: x[1])
#nx.set_node_attributes(G, x, "centrality")
# for node in x_sorted:
#     G[node[0]]['centrality'] = node[1]
# print(x_sorted[-5:])
# for node in x_sorted[-10:]:
#    cur.execute(
#        "SELECT screen_name FROM twitter_user WHERE id_str=%s", (node[0],))
#    print(cur.fetchone())

c = list(greedy_modularity_communities(G))
print(f"Number of communities : {len(c)}")
for community_id, community in enumerate(c[:10]):
    print(f"Community [{community_id}] : {len(community)} nodes.")
for community_id, community in enumerate(c):
    for node in community:
        G.nodes[node]['community'] = community_id

communities_hashtags = dict()
for retweet in retweets:
    communities_hashtags.setdefault(
        G.nodes[retweet[1]]['community'], []).append(retweet[5])
print("\n\n")
for community_id in range(10):
    hashtags = communities_hashtags[community_id]
    d = defaultdict(int)
    for i in hashtags:
        if i is not None:
            d[i] += 1
    result = max(d.items(), key=lambda x: x[1])
    print(f"Community [{community_id}] : {result}")

#print(f"{community_id} : {hashtags[:5]}")
# break
nx.write_gexf(
    G, f"output_graphs/{output_file}_retweet_graph.gexf")
graph_stats(G)
