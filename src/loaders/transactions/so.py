from .transaction import Transaction

class SOTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)
        self.batch_size = 2000

    def so_tx(self, data):
        '''
        Loads the SO data into Neo4j.
        TODO: Need metadata for SO release version. Entity node?
        TODO: Split out synonyms into nodes?

        '''
        query = """
            UNWIND $data as row 

            //Create the GOTerm node and set properties. primaryKey is required.
            MERGE (s:SOTerm:Ontology {primaryKey:row.id})
                SET s.name = row.name
        """
        Transaction.execute_transaction_batch(self, query, data, self.batch_size)