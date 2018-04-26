from .transaction import Transaction

class MITransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)
        self.batch_size = 3000

    def mi_tx(self, data):
        '''
        Loads the MI data into Neo4j.
        '''
        
        query = """
            UNWIND $data as row 

            
        """

        Transaction.execute_transaction_batch(self, query, data, self.batch_size)