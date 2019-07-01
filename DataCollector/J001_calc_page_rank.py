import networkx as nx
import json
G = nx.read_edgelist('tmp/to_pagerank.txt', nodetype=str)

# ノード数とエッジ数を出力
print(nx.number_of_nodes(G))
print(nx.number_of_edges(G))

pagerank = nx.pagerank(G)
json.dump(page_rank, fp=open('tmp/pagerank.json', 'w'), indent=2)
