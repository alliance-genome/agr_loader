from .transactions.do import DOTransaction

class DOLoader(object):

    def __init__(self, graph):
        self.graph = graph


    def load_do(self, data):
        tx = DOTransaction(self.graph)
        do_data = []
        for n in data.nodes():
            do_data.append(data.node(n))
        tx.do_tx(do_data)