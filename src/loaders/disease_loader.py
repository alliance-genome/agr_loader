from .transactions.disease import DiseaseTransaction

class DiseaseLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_disease_objects(self, data):
        tx = DiseaseTransaction(self.graph)
        tx.disease_object_tx(data)