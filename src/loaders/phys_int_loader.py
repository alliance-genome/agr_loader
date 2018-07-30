from .transactions.imex import IMEXTransaction

class PhysIntLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_phys_int(self, data):
        tx = PhysIntTransaction(self.graph)
        tx.phys_int_tx(data)