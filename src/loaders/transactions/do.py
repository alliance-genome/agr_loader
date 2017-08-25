from neo4j.v1 import GraphDatabase
from .transaction import Transaction
import pprint

class DOTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)
        self.batch_size = 2000

    def do_tx(self, data):
        '''
        Loads the DO data into Neo4j.
        '''

        query = """
            UNWIND $data as row

            //Create the DOTerm node and set properties. primaryKey is required.
            MERGE (doterm:DOTerm:Ontology {primaryKey:row.id})
            ON CREATE SET doterm.name = row.name
            ON CREATE SET doterm.nameKey = row.name_key
            ON CREATE SET doterm.definition = row.definition

            FOREACH (entry in row.do_crossreferences |
                MERGE (cr:ExternalId:Identifier {primaryKey:entry})
                MERGE (doterm)-[aka:ALSO_KNOWN_AS]->(cr))

            FOREACH (entry in row.syns |
                MERGE (syn:Synonym:Identifier {primaryKey:entry})
                MERGE (doterm)-[aka:ALSO_KNOWN_AS]->(syn))

            FOREACH (isa in row.isas |
                MERGE (doterm2:DOTerm:Ontology {primaryKey:isa})
                MERGE (doterm)-[aka:IS_A]->(doterm2))
        """
        Transaction.execute_transaction_batch(self, query, data, self.batch_size)