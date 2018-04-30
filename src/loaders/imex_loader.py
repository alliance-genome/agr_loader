from .transactions.imex import IMEXTransaction

class IMEXLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_imex(self, data):
        tx = IMEXTransaction(self.graph)
        tx.imex_tx(data)