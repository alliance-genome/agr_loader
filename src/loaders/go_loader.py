from .transactions.go import GOTransaction

class GOLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_go(self, data):
        tx = GOTransaction(self.graph)
        go_data = []
        for n in data.nodes():
            node = data.node(n)
            node["oid"] = n
            if "def" in node:
                node["definition"] = node["def"]
            if "namespace" in node:
                node["o_type"] = node["namespace"]
            node["href"] = ""  # missing from node DS
            if "label" in node:
                node["name"] = node["label"]
                node["name_key"] = node["name"]
            node["is_obsolete"] = 'false'  # I think ontobio currently drops obsoleted terms
            if "is_a" in node:
                node["isas"] = node["is_a"]
            go_data.append(node)
        tx.go_tx(go_data)