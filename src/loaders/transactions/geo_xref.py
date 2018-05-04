import pprint
from .transaction import Transaction


class GeoXrefTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def geo_xref_tx(self, data):

        #TODO: make one query for all xref stanzas instead of duplicating in 4 different files: go.py, do.py, bgi.py, allele.py
        geoXrefQuery = """

                    UNWIND $data AS row
                    MATCH (g:Gene) where g.primaryKey = row.genePrimaryKey

                    MERGE (id:CrossReference {primaryKey:row.primaryKey})

                    SET id.name = row.id
                    SET id.globalCrossRefId = row.globalCrossRefId
                    SET id.localId = row.localId
                    SET id.crossRefCompleteUrl = row.crossRefCompleteUrl
                    SET id.prefix = row.prefix
                    SET id.crossRefType = row.crossRefType
                    SET id.uuid = row.uuid
                    SET id.displayName = row.displayName
                    SET id.page = row.page
                    SET id.primaryKey = row.primaryKey


                    MERGE (g)-[gcr:CROSS_REFERENCE]->(id)

        """

        Transaction.execute_transaction(self, geoXrefQuery, data)
