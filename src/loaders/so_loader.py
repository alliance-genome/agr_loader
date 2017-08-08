from neo4j.v1 import GraphDatabase
from .transactions.so import SOTransaction
# import pprint

class SOLoader:

    def __init__(self, graph):
        self.graph = graph

    def load_so(self, data):
        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(data)
        # quit()
        tx = SOTransaction(self.graph)
        tx.so_tx(data)