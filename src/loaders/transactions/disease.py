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

            UNWIND $data as row with row

            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'gene' THEN [1] ELSE [] END |
                MERGE (g:Gene:Gene {primaryKey:row.primaryId}))
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'genotype' THEN [1] ELSE [] END |
                MERGE (g:Genotype:Genotype {primaryKey:row.primaryId}))
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'allele' THEN [1] ELSE [] END |
                MERGE (g:Allele:Allele {primaryKey:row.primaryId}))
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'transgene' THEN [1] ELSE [] END |
                MERGE (g:Transgene:Transgene {primaryKey:row.primaryId}))
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'fish' THEN [1] ELSE [] END |
                MERGE (g:Fish:Fish {primaryKey:row.primaryId}))

        """
        Transaction.execute_transaction(self, query, data)