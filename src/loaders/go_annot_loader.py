from .transactions.go_annot import GOAnnotTransaction

class GOAnnotLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_go_annot(self, data):
        tx = GOAnnotTransaction(self.graph)
        tx.go_annot_tx(data)