from .transactions.go import GOTransaction

class GOLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_go(self, data):
        tx = GOTransaction(self.graph)
        go_data = []
        for n in data.nodes():
            node = data.node(n)
            if node.get('type') == "PROPERTY":
                continue
            go_data.append(node)
        tx.go_tx(go_data)