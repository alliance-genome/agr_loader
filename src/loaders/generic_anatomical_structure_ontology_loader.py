from .transactions.generic_anatomical_structure_ontology import GenericAnatomicalStructureOntologyTransaction


class GenericAnatomicalStructureOntologyLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_ontology(self, data):
        tx = GenericAnatomicalStructureOntologyTransaction(self.graph)
        gaso_data = []
        for n in data.nodes():
            node = data.node(n)
            if node.get('type') == "PROPERTY":
                continue
            if 'oid' in node:   # Primarily filters out the empty nodes
                gaso_data.append(node)
        tx.gaso_tx(gaso_data)