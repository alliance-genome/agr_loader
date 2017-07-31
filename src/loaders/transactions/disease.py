from neo4j.v1 import GraphDatabase
from .transaction import Transaction

class DiseaseTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def disease_object_tx(self, data):
        '''
        Loads the Disease data into Neo4j.
        Nodes: merge object (gene, genotype, transgene, allele, etc..., merge disease term,
        '''

        query = """
            UNWIND $data as row

            MERGE (f:Feature {primaryKey:row.primaryId, dateProduced:row.dateProduced, dataProvider:row.dataProvider})

        """
