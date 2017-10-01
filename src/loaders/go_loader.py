from .transactions.go import GOTransaction

class GOLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_go(self, data):
        tx = GOTransaction(self.graph)
        tx.go_tx(data)