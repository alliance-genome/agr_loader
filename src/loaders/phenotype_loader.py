from .transactions.phenotype import PhenotypeTransaction


class PhenotypeLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_phenotype_objects(self, data, species):
        tx = PhenotypeTransaction(self.graph)
        tx.phenotype_object_tx(data, species)