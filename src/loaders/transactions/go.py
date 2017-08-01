from neo4j.v1 import GraphDatabase
from .transaction import Transaction

class GOTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)
        self.batch_size = 500 # Increasing the available memory for Neo4j would eliminate the need to batch:
        '''
        neo4j.exceptions.TransientError: There is not enough memory to perform the current task. 
        Please try increasing 'dbms.memory.heap.max_size' in the neo4j configuration (normally in 'conf/neo4j.conf' 
        or, if you you are using Neo4j Desktop, found through the user interface) or if you are running an embedded 
        installation increase the heap by using '-Xmx' command line flag, and then restart the database.
        '''

    def go_tx(self, data):
        '''
        Loads the GO data into Neo4j.
        TODO: Need metadata for GO release version. Entity node?
        TODO: Split out synonyms into nodes?

        '''
        query = """
            UNWIND $data as row 

            //Create the GOTerm node and set properties. primaryKey is required.
            CREATE (g:GOTerm {primaryKey:row.id})
            SET g.description = row.description
            SET g.synonyms = row.go_synonyms
            SET g.type = row.go_type
            SET g.href = row.href
            SET g.name = row.name 
            SET g.nameKey = row.name_key
        """
        Transaction.execute_transaction_batch(self, query, data, self.batch_size)