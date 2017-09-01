from .transaction import Transaction

class OrthoTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def ortho_tx(self, data):
        '''
        Loads the orthology data into Neo4j.

        '''
        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(data)
        # quit()

        query = """
            UNWIND $data as row

            MERGE(g1:Gene {primaryKey:row.gene1AgrPrimaryId})
            MERGE(g2:Gene {primaryKey:row.gene2AgrPrimaryId})
            MERGE(g1)-[orth:ORTHOLOGOUS]-(g2)
                SET orth.uuid = row.uuid
                SET orth.isBestScore = row.isBestScore
                SET orth.isBestRevScore = row.isBestRevScore
                SET orth.confidence = row.confidence

            //Create the Association node to be used for the object/doTerm
            MERGE (oa:Association {primaryKey:row.uuid})
                SET oa :OrthologyGeneJoin
                SET oa.joinType = 'orthologous'
            MERGE (g1)-[a1:ASSOCIATION]->(oa)
            MERGE (oa)-[a2:ASSOCIATION]->(g2)

            FOREACH (algorithm in row.matched|
                MERGE (match:OrthoAlgorithm {name:algorithm})
                MERGE (oa)-[m:MATCHED]->(match)
            )

            FOREACH (algorithm in row.notMatched|
                MERGE (notmatch:OrthoAlgorithm {name:algorithm})
                MERGE (oa)-[m:NOT_MATCHED]->(notmatch)
            )

            FOREACH (algorithm in row.notCalled|
                MERGE (notcalled:OrthoAlgorithm {name:algorithm})
                MERGE (oa)-[m:NOT_CALLED]->(notcalled)
            )
        """
        Transaction.execute_transaction(self, query, data)