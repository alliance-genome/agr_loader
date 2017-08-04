from .transactions.disease import DiseaseTransaction
import pprint

class DiseaseLoader:

    def __init__(self, graph):
        self.graph = graph

    def load_disease_objects(self, data):
        #pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(data)
        # quit()
        tx = DiseaseTransaction(self.graph)
        tx.disease_object_tx(data)
