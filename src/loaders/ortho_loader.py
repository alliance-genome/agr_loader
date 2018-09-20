from .transactions.orthology import OrthoTransaction

class OrthoLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_ortho(self, ortho_data, matched_algorithm_data, unmatched_algorithm_data, not_called):
        tx = OrthoTransaction(self.graph)
        tx.ortho_tx(ortho_data, matched_algorithm_data, unmatched_algorithm_data, not_called)
