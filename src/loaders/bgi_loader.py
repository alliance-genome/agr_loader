from .transactions.bgi import BGITransaction

class BGILoader:

    def __init__(self, graph):
        self.graph = graph

    def load_bgi(self, data):
        tx = BGITransaction(self.graph)
        tx.bgi_tx(data)