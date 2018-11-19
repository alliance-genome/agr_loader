from .transactions.allele import AlleleTransaction

class AlleleLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_allele_objects(self, alleles, allele_secondaryIds, allele_synonyms, crossReferences):
        tx = AlleleTransaction(self.graph)
        tx.allele_tx(alleles, allele_secondaryIds, allele_synonyms, crossReferences)
