import networkx as nx
from networkx.readwrite import json_graph
import json
import sys


def graph_stats(graph):
    print("\n===========================")
    print(f"Graph created successfully!")
    print(f"Number of Nodes : {graph.number_of_nodes()}")
    print(f"Number of Edges : {graph.number_of_edges()}")
    print("===========================\n")


G = nx.DiGraph()

unique_ids = []
input_file = sys.argv[1]
output_file = input_file.split(".")[0]
with open(f"../data/output_files/{input_file}") as followers_csv:
    next(followers_csv)  # skip header of file
    for line in followers_csv:
        id_followee, name_followee, id_follower, name_follower = line.split(
            ",")
        if (id_followee not in unique_ids):
            unique_ids.append(id_followee)
            G.add_node(id_followee)
        if (id_follower not in unique_ids):
            unique_ids.append(id_follower)
            G.add_node(id_follower)
        G.add_edge(id_follower, id_followee)

    nx.write_gexf(G, f"output_graphs/{output_file}_graph.gexf")
graph_stats(G)
