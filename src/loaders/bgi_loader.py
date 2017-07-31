from .transactions.bgi import BGITransaction
import pprint

class BGILoader:

    def __init__(self, graph):
        self.graph = graph

    def load_bgi(self, data, species):
        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(data)
        # quit()
        tx = BGITransaction(self.graph)
        tx.bgi_tx(data, species)
