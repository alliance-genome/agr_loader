from neo4j.v1 import GraphDatabase
from .transactions.go_annot import GOAnnotTransaction

class GOAnnotLoader:

    def __init__(self, graph):
        self.graph = graph

    def load_do_annot(self, data):
        tx = DOAnnotTransaction(self.graph)
        tx.do_annot_tx(data)