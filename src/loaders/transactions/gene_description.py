from .transaction import Transaction

class GeneDescriptionTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)
        self.batch_size = 3000

    def gd_tx(self, data):
        '''
        Loads gene descriptions data into Neo4j.
        '''
        query = """
            UNWIND $data as row 

            MATCH (g:Gene {primaryKey:row.gene_id})
                WHERE g.automatedGeneSynopsis is NULL
                SET g.automatedGeneSynopsis = row.description
        """
        Transaction.execute_transaction_batch(self, query, data, self.batch_size)
