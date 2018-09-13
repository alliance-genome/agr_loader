from .transactions.bgi import BGITransaction

class BGILoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_bgi(self, gene_dataset, synonyms, secondaryIds, genomicLocations, crossReferences):
        tx = BGITransaction(self.graph)
        tx.bgi_tx(gene_dataset, synonyms, secondaryIds, genomicLocations, crossReferences)