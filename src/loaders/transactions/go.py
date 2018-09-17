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
                SET g.definition = row.definition,
                g.type = row.o_type
                g.href = row.href
                g.name = row.name 
                g.subset = row.subset
                g.nameKey = row.name_key
                g.is_obsolete = row.is_obsolete
                g.href = row.href

            FOREACH (entry in row.o_synonyms |
                MERGE (syn:Synonym:Identifier {primaryKey:entry})
                MERGE (g:GOTerm:Ontology)-[aka:ALSO_KNOWN_AS]->(syn:Synonym:Identifier))

            FOREACH (isa in row.isas |
                MERGE (g2:GOTerm:Ontology {primaryKey:isa})
                MERGE (g:GOTerm:Ontology)-[aka:IS_A]->(g2:GOTerm:Ontology))
                
            FOREACH (partof in row.partofs |
                MERGE (g2:GOTerm:Ontology {primaryKey:partof})
                MERGE (g:GOTerm:Ontology)-[aka:PART_OF]->(g2:GOTerm:Ontology))
            
            FOREACH (regulates in row.regulates |
                MERGE (g2:GOTerm:Ontology {primaryKey:regulates})
                MERGE (g:GOTerm:Ontology)-[aka:REGULATES]->(g2:GOTerm:Ontology))
                
            FOREACH (negatively_regulates in row.negatively_regulates |
                MERGE (g2:GOTerm:Ontology {primaryKey:negatively_regulates})
                MERGE (g:GOTerm:Ontology)-[aka:NEGATIVELY_REGULATES]->(g2:GOTerm:Ontology))
                
            FOREACH (positively_regulates in row.positively_regulates |
                MERGE (g2:GOTerm:Ontology {primaryKey:positively_regulates})
                MERGE (g:GOTerm:Ontology)-[aka:POSITIVELY_REGULATES]->(g2:GOTerm:Ontology))

        """

        queryXref = """

            UNWIND $data as row
             WITH row.xref_urls AS events
                UNWIND events AS event
                    MATCH (o:GOTerm:Ontology {primaryKey:event.oid})


        """ + CreateCrossReference.get_cypher_xref_text("gene_ontology")

        Transaction.execute_transaction_batch(self, query, data, self.batch_size)
        #Transaction.execute_transaction_batch(self, queryXref, data, self.batch_size)