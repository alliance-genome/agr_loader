from .transaction import Transaction

class GeneIndexer:

    def __init__(self, graph):
        self.graph = graph

    def index_genes(self, data):
        tx = Transaction(self.graph)
        tx.bgi_merge(data)