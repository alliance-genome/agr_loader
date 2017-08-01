from neo4j.v1 import GraphDatabase
from .transactions.go_annot import GOAnnotTransaction

class GOAnnotLoader:

    def __init__(self, graph):
        self.graph = graph

    def load_go_annot(self, data):
        tx = GOAnnotTransaction(self.graph)
        tx.go_annot_tx(data)