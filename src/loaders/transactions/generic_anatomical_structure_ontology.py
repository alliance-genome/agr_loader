from .transaction import Transaction
from services import CreateCrossReference


class GenericAnatomicalStructureOntologyTransaction(Transaction):
    def __init__(self, graph):
        Transaction.__init__(self, graph)
        self.batch_size = 3000

    def gaso_tx(self, data, nodeLabel):
        '''
        Loads the various structure ontology data into Neo4j.

        '''
        query = """
            UNWIND $data as row

            //Create the Term node and set properties. primaryKey is required.
            MERGE (g:%s:Ontology {primaryKey:row.oid})
                SET g.definition = row.definition,
                 g.type = row.o_type,
                 g.href = row.href,
                 g.name = row.name,
                 g.nameKey = row.name_key,
                 g.is_obsolete = row.is_obsolete,
                 g.href = row.href

            FOREACH (entry in row.o_synonyms |
                MERGE (syn:Synonym:Identifier {primaryKey:entry})
                MERGE (g)-[aka:ALSO_KNOWN_AS]->(syn))

            FOREACH (isa in row.isas |
                MERGE (g2:%s:Ontology {primaryKey:isa})
                MERGE (g)-[aka:IS_A]->(g2))

            FOREACH (partof in row.partofs |
                MERGE (g2:%s:Ontology {primaryKey:partof})
                MERGE (g)-[aka:PART_OF]->(g2))

        """ % (nodeLabel, nodeLabel, nodeLabel)

        queryXref = """

            UNWIND $data as row
             WITH row.xref_urls AS events
                UNWIND events AS event
                    MATCH (o:%s:Ontology {primaryKey:event.oid})


        """ % nodeLabel
        # TODO: make this generic and add it above
        # + CreateCrossReference.get_cypher_xref_text("cell_ontology")

        Transaction.execute_transaction_batch(self, query, data, self.batch_size)
        # Transaction.execute_transaction_batch(self, queryXref, data, self.batch_size)