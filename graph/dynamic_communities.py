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


def get_fronts(t):
    #print("-- CALCULATING FRONTS --")

    fronts = defaultdict(list)
    for d_community, community_id in D[t-1].items():
        tcounter = 1
        while(community_id == ''):
            community_id = D[t-tcounter][d_community]
            tcounter += 1
        t = int(community_id[1])
        #print(f"Current t = {t}")
        for node, community in list(graphs[t].nodes(data='community')):
            #print(f'Checking class [{community_id}] with class [{community}]')
            if(community == community_id[0]):
                fronts[d_community].append(node)
    # print(fronts.keys())
    return fronts


def extract_communities(graph):
    communities = defaultdict(list)
    for node, community in list(graph.nodes(data='community')):
        communities[community].append(node)
    #print(f'Found {len(communities)} communities')
    return communities


def jaccard(nodes_a, nodes_b):
    common = 0
    for node_a in nodes_a:
        for node_b in nodes_b:
            if(node_a == node_b):
                common += 1
    return common/(len(node_a)+len(nodes_b))


def find_community_matches(community_id, community_nodes, fronts):
    matches = list()
    for front_id, front_nodes in fronts.items():
        # print(
        #    f"Checking current community[{community_id} with front of community[{front_id}]")
        j = jaccard(community_nodes, front_nodes)
        # print(f"Jaccard : {j}\n")
        if(j >= 0.3):
            matches.append(front_id)
    return matches


def update_D(matches, t):

    D.append(defaultdict())
    for key in D[t-1].keys():
        D[t][key] = ('', t)
    # print('#############')
    matches.sort(key=lambda tup: tup[0])
    for match in matches:
        if(len(match[1]) == 1):
            print(match)
            if(D[t][match[1][0]]) == ('', t):
                D[t][match[1][0]] = (match[0], t)
            else:
                next_d_community = max(D[t].keys()) + 1
                D[t][next_d_community] = (match[0], t)
                print("########### SPLIT ############")
        if(len(match[1]) >= 2):
            print(match)
            for d_community in match[1]:
                D[t][d_community] = (match[0], t)
        if (len(match[1]) == 0 and int(match[0][2:]) <= 9):
            print(match)
            next_d_community = max(D[t].keys()) + 1
            D[t][next_d_community] = (match[0], t)

    print(D[t])


graphs = list()
D = list()

for i in range(0, 5):
    graph = nx.read_gexf(
        f"output_graphs/both_retweet_graph_2020-0{i+1}-01 00:00:00.gexf")
    for node_id in list(graph.nodes()):
        graph.nodes[node_id]['community'] = f"C{i}{graph.nodes[node_id]['community']}"
    graphs.append(graph)


D.append({0: ('C00', 0), 1: ('C01', 0), 2: ('C02', 0), 3: ('C03', 0), 4: ('C04', 0), 5: ('C05', 0),
          6: ('C06', 0), 7: ('C07', 0), 8: ('C08', 0), 9: ('C09', 0)})  # initialize D
#################################

for t in range(1, 5):
    print(f"\n\n----- T = {t} --------")
    fronts = get_fronts(t)
    all_matches = list()

    for community_id, community_nodes in extract_communities(graphs[t]).items():
        community_matches = find_community_matches(
            community_id, community_nodes, fronts)
        all_matches.append((community_id, community_matches))

    #print(f"Found {len(all_matches)} matches")
    # for match in all_matches:
        # for community in match[1]:
        #    if community < 10:
    #    print(match)

    update_D(all_matches, t)
################################
