import logging

from .transaction import Transaction

logger = logging.getLogger(__name__)

class GeneDescriptionTransaction(Transaction):

    def __init__(self):
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
        self.execute_transaction_batch(query, data, self.batch_size)
