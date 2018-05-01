from .transactions.mi import MITransaction

class MILoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_mi(self, data):
        tx = MITransaction(self.graph)
        tx.mi_tx(data)