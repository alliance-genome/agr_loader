from .transaction import Transaction

class GOAnnotTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)
        self.batch_size = 2000

    def do_annot_tx(self, data):
        '''
        Loads the GO annotation data into Neo4j.

        '''
        query = """
            UNWIND $data as row
            MERGE (g:Gene {primaryKey:row.gene_id})

            FOREACH (entry in row.do_id |
                MERGE (do:DOTerm:Ontology {primaryKey:entry})
                MERGE (g)-[x:ANNOTATED_TO]->(do))
        """
        Transaction.execute_transaction_batch(self, query, data, self.batch_size)