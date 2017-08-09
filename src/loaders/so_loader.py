from neo4j.v1 import GraphDatabase
from .transactions.so import SOTransaction

class SOLoader:

    def __init__(self, graph):
        self.graph = graph

    def load_so(self, data):
        tx = SOTransaction(self.graph)
        tx.so_tx(data)