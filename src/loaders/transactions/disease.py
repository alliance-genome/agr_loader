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

            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'gene' THEN [1] ELSE [] END |

                MERGE (f:Gene:Gene {primaryKey:row.primaryId})

                MERGE (f)-[:FROM_SPECIES]->(spec)
                //SET f.with = row.with

                MERGE (d:DOTerm {primaryKey:row.doId})

                FOREACH (rel IN CASE when row.associationType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_MODEL_OF]->(d))

                FOREACH (rel IN CASE when row.associationType = 'is_marker_for' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_MARKER_FOR]->(d))

                FOREACH (rel IN CASE when row.associationType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_IMPLICATED_IN]->(d))

                //Create the Association node to be used for the object/doTerm
                MERGE (da:Association {link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.
                MERGE (f)-[fda:ASSOCIATION]->(da)
                MERGE (da)-[dad:ASSOCIATION]->(d)

                //Create nodes for other identifiers.  TODO- do this better. evidence code node needs to be linked up with each
                //of these separately.

                FOREACH (ec in row.evidenceCodes| MERGE (e:EvidenceCode {primaryKey: ec.code}))

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