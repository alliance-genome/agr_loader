from .transaction import Transaction

class BGILoader:

    def __init__(self, graph):
        self.graph = graph

    def index_genes(self, data):
        tx = Transaction(self.graph)
        tx.bgi_index(data)