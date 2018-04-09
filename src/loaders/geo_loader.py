from .transactions.geo import GeoXrefTransaction

class GeoLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_geo_xrefs(self, data):
        tx = GeoXrefTransaction(self.graph)
        tx.geo_xref_tx(data)
