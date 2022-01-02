import pickle

G = pickle.load(open("token_graph_day.pck","rb"))

print('the nodes of G')
print(list(G.nodes))


print(G.number_of_nodes())
print(G.number_of_edges())
