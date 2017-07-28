from neo4j.v1 import GraphDatabase
from .transactions.go import GOTransaction

class GOLoader:

    def __init__(self, graph):
        self.graph = graph

    def load_go(self, data):
        tx = GOTransaction(self.graph)

        tx.go_tx(data)