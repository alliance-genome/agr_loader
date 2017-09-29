from .transactions.do import DOTransaction

class DOLoader(object):

    def __init__(self, graph):
        self.graph = graph


    def load_do(self, data):
        tx = DOTransaction(self.graph)
        tx.do_tx(data)