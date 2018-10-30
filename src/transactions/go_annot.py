from .transaction import Transaction
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class GOAnnotTransaction(Transaction):

    def __init__(self):
        self.batch_size = 2000

    def go_annot_tx(self, data):
        '''
        Loads the GO annotation data into Neo4j.
        '''
        query = """
            UNWIND $data as row
            MATCH (g:Gene {primaryKey:row.gene_id})
            FOREACH (entry in row.annotations |
                MERGE (go:GOTerm:Ontology {primaryKey:entry.go_id})
                MERGE (g)-[ggo:ANNOTATED_TO]->(go))
        """
        self.execute_transaction_batch(query, data, self.batch_size)
