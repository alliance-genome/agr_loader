from .transactions.so import SOTransaction

class SOLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_so(self, data):
        tx = SOTransaction(self.graph)
        tx.so_tx(data)