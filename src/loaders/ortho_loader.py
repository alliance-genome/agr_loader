from .transactions.orthology import OrthoTransaction

class OrthoLoader:

    def __init__(self, graph):
        self.graph = graph

    def load_ortho(self, data):
        tx = OrthoTransaction(self.graph)
        tx.ortho_tx(data)