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
                SET g.definition = row.definition
                SET g.type = row.o_type
                SET g.href = row.href
                SET g.name = row.name 
                SET g.nameKey = row.name_key
                SET g.is_obsolete = row.is_obsolete

            FOREACH (entry in row.o_synonyms |
                MERGE (syn:Synonym:Identifier {primaryKey:entry})
                MERGE (g)-[aka:ALSO_KNOWN_AS]->(syn))

            FOREACH (isa in row.isas |
                MERGE (g2:GOTerm:Ontology {primaryKey:isa})
                MERGE (g)-[aka:IS_A]->(g2))


        """

        queryXref = """

            UNWIND $data as row
             WITH row.xref_urls AS xrurls
                UNWIND xrurls AS xref
                    MATCH (gt:GOTerm:Ontology {primaryKey:xref.goid})

                    MERGE (cr:CrossReference:Identifier {primaryKey:xref.xrefId})
                     SET cr.localId = xref.local_id
                     SET cr.prefix = xref.prefix
                     SET cr.crossRefCompleteUrl = xref.complete_url
                     SET cr.name = xref.xrefId

                    MERGE (gt)-[aka:CROSS_REFERENCE]->(cr)


        """
        Transaction.execute_transaction_batch(self, query, data, self.batch_size)
        Transaction.execute_transaction_batch(self, queryXref, data, self.batch_size)