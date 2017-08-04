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

            //WITH row.EvidenceCodes as codes
            //UNWIND codes as code
            //    MERGE (ev:EvidenceCode {primaryKey: code.code})


            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'gene' THEN [1] ELSE [] END |

                MERGE (f:Gene:Gene {primaryKey:row.primaryId})

                MERGE (f)-[:FROM_SPECIES]->(spec)
                SET f.with = row.with

                MERGE (d:DOTerm {primaryKey:row.doId})

                FOREACH (rel IN CASE when row.relationshipType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_MODEL_OF]->(d))

                FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_MARKER_FOR]->(d))

                FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_IMPLICATED_IN]->(d))

                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (f)-[fq:IS_NOT_MODEL_OF]->(d))

                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_marker_of' THEN [1] ELSE [] END |
                    MERGE (f)-[fq:IS_NOT_MARKER_OF]->(d))

                 FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)-[fq:IS_NOT_IMPLICATED_IN]->(d))


                //Create the Association node to be used for the object/doTerm
                MERGE (da:Association {link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.
                MERGE (f)-[fda:ASSOCIATION]->(da)
                MERGE (da)-[dad:ASSOCIATION]->(d)

                //Create nodes for other identifiers.  TODO- do this better. evidence code node needs to be linked up with each
                //of these separately.

                CREATE (ecs:EvicenceCodes {evidenceCodes:row.evidenceCodes})
                //Create Association nodes for other identifiers.
                CREATE (eca:Association {link_from:row.primaryId, link_to:row.evidenceCodes})
                CREATE (f)-[feca:ASSOCIATION]->(eca)
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
                CREATE (f)-[fda:ASSOCIATION]->(da)
                CREATE (da)-[dad:ASSOCIATION]->(d)

                //Create nodes for other identifiers.  TODO- do this better. evidence code node needs to be linked up with each
                //of these separately.

                CREATE (ecs:EvicenceCodes {evidenceCodes:row.evidenceCodes})
                //Create Association nodes for other identifiers.
                CREATE (eca:Association {link_from:row.primaryId, link_to:row.evidenceCodes})
                CREATE (f)-[r1:ASSOCIATION]->(eca)
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
                CREATE (f)-[fda:ASSOCIATION]->(da)
                CREATE (da)-[dad:ASSOCIATION]->(d)

                //Create nodes for other identifiers.  TODO- do this better. evidence code node needs to be linked up with each
                //of these separately.

                CREATE (ecs:EvicenceCodes {evidenceCodes:row.evidenceCodes})
                //Create Association nodes for other identifiers.
                CREATE (eca:Association {link_from:row.primaryId, link_to:row.evidenceCodes})
                CREATE (f)-[feca:ASSOCIATION]->(eca)
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
                CREATE (f)-[fda:ASSOCIATION]->(da)
                CREATE (da)-[dad:ASSOCIATION]->(d)

                //Create nodes for other identifiers.  TODO- do this better. evidence code node needs to be linked up with each
                //of these separately.

                CREATE (ecs:EvicenceCodes {evidenceCodes:row.evidenceCodes})
                //Create Association nodes for other identifiers.
                CREATE (eca:Association {link_from:row.primaryId, link_to:row.evidenceCodes})
                CREATE (f)-[feca:ASSOCIATION]->(eca)
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
                CREATE (f)-[fda:ASSOCIATION]->(da)
                CREATE (da)-[dad:ASSOCIATION]->(d)

                //Create nodes for other identifiers.  TODO- do this better. evidence code node needs to be linked up with each
                //of these separately.

                CREATE (ecs:EvicenceCodes {evidenceCodes:row.evidenceCodes})
                //Create Association nodes for other identifiers.
                CREATE (eca:Association {link_from:row.primaryId, link_to:row.evidenceCodes})
                CREATE (f)-[feca:ASSOCIATION]->(eca)
            )



        """
        Transaction.execute_transaction(self, query, data)