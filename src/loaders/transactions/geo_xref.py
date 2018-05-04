from services import CreateCrossReference
from .transaction import Transaction


class GeoXrefTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def geo_xref_tx(self, data):

        #TODO: make one query for all xref stanzas instead of duplicating in 4 different files: go.py, do.py, bgi.py, allele.py
        geoXrefQuery = """

                    UNWIND $data AS event
                    MATCH (o:Gene) where o.primaryKey = event.genePrimaryKey

        """ + CreateCrossReference.get_cypher_xref_text("geo_xref")

        Transaction.execute_transaction(self, geoXrefQuery, data)
