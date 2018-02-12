from .transactions.feature import AlleleTransaction

class AlleleLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_allele_objects(self, data):
        tx = AlleleTransaction(self.graph)
        tx.allele_tx(data)
