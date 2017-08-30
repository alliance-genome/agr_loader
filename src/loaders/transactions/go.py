from .transaction import Transaction

class GOTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)
        self.batch_size = 2000

    def go_tx(self, data):
        '''
        Loads the GO data into Neo4j.
        TODO: Need metadata for GO release version. Entity node?
        TODO: Split out synonyms into nodes?

        '''
        query = """
            UNWIND $data as row 

            //Create the GOTerm node and set properties. primaryKey is required.
            MERGE (g:GOTerm:Ontology {primaryKey:row.id})
            ON CREATE SET g.definition = row.definition
            ON CREATE SET g.type = row.go_type
            ON CREATE SET g.href = row.href
            ON CREATE SET g.name = row.name 
            ON CREATE SET g.nameKey = row.name_key

            FOREACH (entry in row.go_synonyms |           
                MERGE (syn:Synonym:Identifier {primaryKey:entry})
                MERGE (g)-[aka:ALSO_KNOWN_AS]->(syn))
        """
        Transaction.execute_transaction_batch(self, query, data, self.batch_size)