from .transactions.generic_anatomical_structure_ontology import GenericAnatomicalStructureOntologyTransaction


class GenericAnatomicalStructureOntologyLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_ontology(self, data, nodeLabel):
        tx = GenericAnatomicalStructureOntologyTransaction(self.graph)
        tx.gaso_tx(data, nodeLabel)
