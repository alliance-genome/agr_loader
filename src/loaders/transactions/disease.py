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
                MERGE (f:Gene:Gene {primaryKey:row.primaryId})
                SET f.name = row.diseaseObjectName
                MERGE (spec:Species {primaryId: row.taxonId})
                SET spec.species = row.species
                CREATE (g)-[:FROM_SPECIES]->(spec)
            )
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'genotype' THEN [1] ELSE [] END |
                MERGE (f:Genotype:Genotype {primaryKey:row.primaryId})
                SET f.name = row.diseaseObjectName
                SET spec.species = row.species
                CREATE (g)-[:FROM_SPECIES]->(spec)
            )
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'allele' THEN [1] ELSE [] END |
                MERGE (f:Allele:Allele {primaryKey:row.primaryId})
                SET f.name = row.diseaseObjectName
                SET spec.species = row.species
                CREATE (g)-[:FROM_SPECIES]->(spec)
            )
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'transgene' THEN [1] ELSE [] END |
                MERGE (f:Transgene:Transgene {primaryKey:row.primaryId})
                SET f.name = row.diseaseObjectName
                SET spec.species = row.species
                CREATE (g)-[:FROM_SPECIES]->(spec)
            )
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'fish' THEN [1] ELSE [] END |
                MERGE (f:Fish:Fish {primaryKey:row.primaryId})
                SET f.name = row.diseaseObjectName
                SET spec.species = row.species
                CREATE (g)-[:FROM_SPECIES]->(spec)
            )

        """
        Transaction.execute_transaction(self, query, data)