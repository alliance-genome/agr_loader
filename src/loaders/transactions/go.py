from .transaction import Transaction
from services import CreateCrossReference

class GOTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)
        self.batch_size = 3000

    def go_tx(self, data):
        '''
        Loads the GO data into Neo4j.
        TODO: Need metadata for GO release version. Entity node?

        '''
        query = """
            UNWIND $data as row 

            //Create the GOTerm node and set properties. primaryKey is required.
            MERGE (g:GOTerm:Ontology {primaryKey:row.oid})
                SET g.definition = row.definition
                SET g.type = row.o_type
                SET g.href = row.href
                SET g.name = row.name 
                SET g.nameKey = row.name_key
                SET g.is_obsolete = row.is_obsolete
                SET g.href = row.href

            FOREACH (entry in row.o_synonyms |
                MERGE (syn:Synonym:Identifier {primaryKey:entry})
                MERGE (g)-[aka:ALSO_KNOWN_AS]->(syn))

            FOREACH (isa in row.isas |
                MERGE (g2:GOTerm:Ontology {primaryKey:isa})
                MERGE (g)-[aka:IS_A]->(g2))


        """

        queryXref = """

            UNWIND $data as row
             WITH row.xref_urls AS events
                UNWIND events AS event
                    MATCH (o:GOTerm:Ontology {primaryKey:event.oid})


        """ + CreateCrossReference.get_cypher_xref_text("gene_ontology")

        Transaction.execute_transaction_batch(self, query, data, self.batch_size)
        #Transaction.execute_transaction_batch(self, queryXref, data, self.batch_size)