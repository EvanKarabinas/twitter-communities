import networkx as nx
from networkx.readwrite import json_graph
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


G = nx.DiGraph()

unique_ids = []
input_graph_name = sys.argv[1]
output_file = input_graph_name


# Connect to DB
connection = psycopg2.connect(
    database=config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)

cur = connection.cursor()


cur.execute("SELECT * FROM followship WHERE graph_name = %s",
            (input_graph_name,))
followships = cur.fetchall()

for followship in followships:

    follower_id = followship[0]
    followee_id = followship[1]

    if(follower_id not in unique_ids):
        cur.execute("SELECT * FROM twitter_user WHERE id_str = %s",
                    (follower_id,))
        follower_info = cur.fetchone()
        if(follower_info[1] == input_graph_name):
            print('found')
            G.add_node(follower_info[0], name=follower_info[1],
                       followers=follower_info[2])
            G.nodes[follower_info[0]]["[z]"] = -1
        else:
            G.add_node(follower_info[0], name=follower_info[1],
                       followers=follower_info[2])
            G.nodes[follower_info[0]]["[z]"] = follower_info[4]
        unique_ids.append(follower_id)

    if(followee_id not in unique_ids):
        cur.execute("SELECT * FROM twitter_user WHERE id_str = %s",
                    (followee_id,))
        followee_info = cur.fetchone()
        if(followee_info[1] == input_graph_name):
            print('found')
            G.add_node(followee_info[0], name=followee_info[1],
                       followers=followee_info[2])
            G.nodes[followee_info[0]]["[z]"] = -1
        else:
            G.add_node(followee_info[0], name=followee_info[1],
                       followers=followee_info[2])
            G.nodes[followee_info[0]]["[z]"] = followee_info[4]
        unique_ids.append(followee_id)

    G.add_edge(follower_id, followee_id)
print("Unique ids : "+str(len(unique_ids)))
# with open(f"../data/output_files/{input_file}") as followers_csv:
#     next(followers_csv)  # skip header of file
#     for line in followers_csv:
#         id_followee, name_followee, id_follower, name_follower = line.split(
#             ",")
#         if (id_followee not in unique_ids):
#             unique_ids.append(id_followee)
#             G.add_node(id_followee)
#         if (id_follower not in unique_ids):
#             unique_ids.append(id_follower)
#             G.add_node(id_follower)
#         G.add_edge(id_follower, id_followee)

nx.write_gexf(G, f"output_graphs/{output_file}_graph.gexf")
graph_stats(G)
