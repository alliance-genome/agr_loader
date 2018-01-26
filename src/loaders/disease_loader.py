from .transactions.disease_feature import DiseaseFeatureTransaction
from .transactions.disease_gene import DiseaseGeneTransaction


class DiseaseLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_disease_feature_objects(self, data):
        tx = DiseaseFeatureTransaction(self.graph)
        tx.disease_feature_object_tx(data)

    def load_disease_gene_objects(self, data):
        tx = DiseaseGeneTransaction(self.graph)
        tx.disease_gene_object_tx(data)
