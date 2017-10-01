from .transactions.go_annot import DOAnnotTransaction

class GOAnnotLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_do_annot(self, data):
        tx = DOAnnotTransaction(self.graph)
        tx.do_annot_tx(data)