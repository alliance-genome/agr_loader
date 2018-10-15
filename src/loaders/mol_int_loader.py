from .transactions.mol_int import MolIntTransaction

class MolIntLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_mol_int(self):
        tx = MolIntTransaction(self.graph)
        tx.mol_int_tx()