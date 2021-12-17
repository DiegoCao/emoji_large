import pickle

G = pickle.load(open("token_graph.pck","rb"))
print('the information of graph G is: ', )
print(G.number_of_nodes())
print()