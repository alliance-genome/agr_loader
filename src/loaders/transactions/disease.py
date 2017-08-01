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
            MERGE (spec:Species {primaryId: row.taxonId})

            CREATE (g:Gene {primaryKey:row.primaryId, dateProduced:row.dateProduced, dataProvider:row.dataProvider})



            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'gene' THEN [1] ELSE [] END |
                MERGE (f:Gene:Gene {primaryKey:row.primaryId})
                SET f.name = row.diseaseObjectName
                MERGE (f)-[:FROM_SPECIES]->(spec)
                SET f.with = row.with
                MERGE (d:DOTerm {primaryKey:row.doId})
                MERGE (f)-[fa:ANNOTATED_TO]->(d)

                //Create the Association node to be used for the object/doTerm
                CREATE (da:Association {link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.
                CREATE (f)-[fda:ASSOCIATION]->(da)
                CREATE (da)-[dad:ASSOCIATION]->(d)

                //Create nodes for other identifiers.  TODO- do this better. evidence code node needs to be linked up with each
                //of these separately.

                MERGE (evcodes:EvidenceCodes {evidenceCodes:row.evidenceCodes})
                
            )
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'genotype' THEN [1] ELSE [] END |
                MERGE (f:Genotype:Genotype {primaryKey:row.primaryId})
                SET f.name = row.diseaseObjectName
                MERGE (f)-[:FROM_SPECIES]->(spec)
                SET f.with = row.with
                MERGE (d:DOTerm {primaryKey:row.doId})
                MERGE (f)-[fa:ANNOTATED_TO]->(d)

                //Create the Association node to be used for the object/doTerm
                CREATE (da:Association {link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.
                CREATE (f)-[r7:ASSOCIATION]->(da)
                CREATE (da)-[r8:ASSOCIATION]->(d)
            )
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'allele' THEN [1] ELSE [] END |
                MERGE (f:Allele:Allele {primaryKey:row.primaryId})
                SET f.name = row.diseaseObjectName
                MERGE (f)-[:FROM_SPECIES]->(spec)
                SET f.with = row.with
                MERGE (d:DOTerm {primaryKey:row.doId})
                MERGE (f)-[fa:ANNOTATED_TO]->(d)

                //Create the Association node to be used for the object/doTerm
                CREATE (da:Association {link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.
                CREATE (f)-[r7:ASSOCIATION]->(da)
                CREATE (da)-[r8:ASSOCIATION]->(d)
            )
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'transgene' THEN [1] ELSE [] END |
                MERGE (f:Transgene:Transgene {primaryKey:row.primaryId})
                SET f.name = row.diseaseObjectName
                MERGE (f)-[:FROM_SPECIES]->(spec)
                SET f.with = row.with
                MERGE (d:DOTerm {primaryKey:row.doId})
                MERGE (f)-[fa:ANNOTATED_TO]->(d)

                //Create the Association node to be used for the object/doTerm
                CREATE (da:Association {link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.
                CREATE (f)-[r7:ASSOCIATION]->(da)
                CREATE (da)-[r8:ASSOCIATION]->(d)
            )
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'fish' THEN [1] ELSE [] END |
                MERGE (f:Fish:Fish {primaryKey:row.primaryId})
                SET f.name = row.diseaseObjectName
                MERGE (f)-[:FROM_SPECIES]->(spec)
                SET f.with = row.with
                MERGE (d:DOTerm {primaryKey:row.doId})
                MERGE (f)-[fa:ANNOTATED_TO]->(d)

                //Create the Association node to be used for the object/doTerm
                CREATE (da:Association {link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.
                CREATE (f)-[r7:ASSOCIATION]->(da)
                CREATE (da)-[r8:ASSOCIATION]->(d)
            )



        """
        Transaction.execute_transaction(self, query, data)