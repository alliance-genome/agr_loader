from .transactions.do import DOTransaction

class DOLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_do(self, data):
        tx = DOTransaction(self.graph)
        do_data = []
        for n in data.nodes():
            node = data.node(n)
            if node.get('type') == "PROPERTY":
                continue
            if 'oid' in node:   # Primarily filters out the empty nodes
                do_data.append(node)
        tx.do_tx(do_data)