from neo4j.v1 import GraphDatabase
from .transaction import Transaction
import pprint

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

            MERGE(g:Gene {primaryKey:row.})
        """
        Transaction.execute_transaction(self, query, data)