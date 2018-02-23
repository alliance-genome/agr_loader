from .transactions.disease_allele import DiseaseAlleleTransaction
from .transactions.disease_gene import DiseaseGeneTransaction


class DiseaseLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_disease_allele_objects(self, data):
        tx = DiseaseAlleleTransaction(self.graph)
        tx.disease_allele_object_tx(data)

    def load_disease_gene_objects(self, data):
        tx = DiseaseGeneTransaction(self.graph)
        tx.disease_gene_object_tx(data)
