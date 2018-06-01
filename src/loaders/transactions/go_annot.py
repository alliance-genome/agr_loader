from .transaction import Transaction

class GOAnnotTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)
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
        Transaction.execute_transaction_batch(self, query, data, self.batch_size)