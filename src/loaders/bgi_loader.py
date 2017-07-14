from .transaction import Transaction

class BGILoader:

    def __init__(self, graph):
        self.graph = graph

    def load_bgi(self, data):
        tx = Transaction(self.graph)
        tx.bgi_tx(data)