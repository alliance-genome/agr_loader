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
           // UNWIND row.ecode as codes


            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'gene' THEN [1] ELSE [] END |

                MERGE (f:Gene {primaryKey:row.primaryId})

                MERGE (f)-[:FROM_SPECIES]->(spec)
                SET f.with = row.with

                MERGE (d:DOTerm {primaryKey:row.doId})
                SET d.doDisplayId = row.doDisplayId
                SET d.doUrl = row.doUrl
                SET d.doPrefix = row.doPrefix

                FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_MARKER_FOR]->(d))

                FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_IMPLICATED_IN]->(d))

                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_marker_of' THEN [1] ELSE [] END |
                    MERGE (f)-[fq:IS_NOT_MARKER_OF]->(d))

                 FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)-[fq:IS_NOT_IMPLICATED_IN]->(d))


                //Create the Association node to be used for the object/doTerm
                MERGE (da:DiseaseAssociation {link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.
                MERGE (f)-[fda:ASSOCIATION]->(da)
                MERGE (da)-[dad:ASSOCIATION]->(d)

                //Create nodes for other identifiers.  TODO- do this better. evidence code node needs to be linked up with each
                //of these separately.

                CREATE (ecs:EvicenceCodes {evidenceCodes:row.evidenceCodes})
                //Create Association nodes for other identifiers.
                CREATE (eca:EvidenceCodeAssociation {link_from:row.primaryId, link_to:row.evidenceCodes})
                CREATE (f)-[feca:ASSOCIATION]->(eca)

                MERGE (pub:Publication {primaryKey:row.pubPrimaryKey})
                SET pub.pubModId = row.pubModId
                SET pub.pubMedId = row.pubMedId
                SET pub.pubModUrl = row.pubModUrl
                SET pub.pubMedUrl = row.pubMedUrl
            )

            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'genotype' THEN [1] ELSE [] END |

                MERGE (f:Genotype {primaryKey:row.primaryId})

                MERGE (f)-[:FROM_SPECIES]->(spec)
                SET f.with = row.with

                MERGE (d:DOTerm {primaryKey:row.doId})
                SET d.doDisplayId = row.doDisplayId
                SET d.doUrl = row.doUrl
                SET d.doPrefix = row.doPrefix

                FOREACH (rel IN CASE when row.relationshipType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_MODEL_OF]->(d))
                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (f)-[fq:IS_NOT_MODEL_OF]->(d))
                //Create the Association node to be used for the object/doTerm
                MERGE (da:DiseaseAssociation {link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.
                MERGE (f)-[fda:ASSOCIATION]->(da)
                MERGE (da)-[dad:ASSOCIATION]->(d)

                //Create nodes for other identifiers.  TODO- do this better. evidence code node needs to be linked up with each
                //of these separately.

                CREATE (ecs:EvicenceCodes {evidenceCodes:row.evidenceCodes})
                //Create Association nodes for other identifiers.
                CREATE (eca:EvidenceCodeAssociation {link_from:row.primaryId, link_to:row.evidenceCodes})
                CREATE (f)-[feca:ASSOCIATION]->(eca)

                MERGE (pub:Publication {primaryKey:row.pubPrimaryKey})
                SET pub.pubModId = row.pubModId
                SET pub.pubMedId = row.pubMedId
                SET pub.pubModUrl = row.pubModUrl
                SET pub.pubMedUrl = row.pubMedUrl
            )
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'allele' THEN [1] ELSE [] END |
                MERGE (f:Allele {primaryKey:row.primaryId})

                MERGE (f)-[:FROM_SPECIES]->(spec)
                SET f.with = row.with

                MERGE (d:DOTerm {primaryKey:row.doId})
                SET d.doDisplayId = row.doDisplayId
                SET d.doUrl = row.doUrl
                SET d.doPrefix = row.doPrefix

                FOREACH (rel IN CASE when row.relationshipType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_MODEL_OF]->(d))
                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (f)-[fq:IS_NOT_MODEL_OF]->(d))

                //Create the Association node to be used for the object/doTerm
                MERGE (da:DiseaseAssociation {link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.
                MERGE (f)-[fda:ASSOCIATION]->(da)
                MERGE (da)-[dad:ASSOCIATION]->(d)

                //Create nodes for other identifiers.  TODO- do this better. evidence code node needs to be linked up with each
                //of these separately.

                CREATE (ecs:EvicenceCodes {evidenceCodes:row.evidenceCodes})
                //Create Association nodes for other identifiers.
                CREATE (eca:EvidenceCodeAssociation {link_from:row.primaryId, link_to:row.evidenceCodes})
                CREATE (f)-[feca:ASSOCIATION]->(eca)

                MERGE (pub:Publication {primaryKey:row.pubPrimaryKey})
                SET pub.pubModId = row.pubModId
                SET pub.pubMedId = row.pubMedId
                SET pub.pubModUrl = row.pubModUrl
                SET pub.pubMedUrl = row.pubMedUrl


                FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_MARKER_FOR]->(d))

                FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_IMPLICATED_IN]->(d))

                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_marker_of' THEN [1] ELSE [] END |
                    MERGE (f)-[fq:IS_NOT_MARKER_OF]->(d))

                 FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)-[fq:IS_NOT_IMPLICATED_IN]->(d))


            )
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'transgene' THEN [1] ELSE [] END |
                MERGE (f:Transgene {primaryKey:row.primaryId})

                MERGE (f)-[:FROM_SPECIES]->(spec)
                SET f.with = row.with

                MERGE (d:DOTerm {primaryKey:row.doId})
                SET d.doDisplayId = row.doDisplayId
                SET d.doUrl = row.doUrl
                SET d.doPrefix = row.doPrefix

                //Create the Association node to be used for the object/doTerm
                MERGE (da:DiseaseAssociation {link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.
                MERGE (f)-[fda:ASSOCIATION]->(da)
                MERGE (da)-[dad:ASSOCIATION]->(d)

                //Create nodes for other identifiers.  TODO- do this better. evidence code node needs to be linked up with each
                //of these separately.

                CREATE (ecs:EvicenceCodes {evidenceCodes:row.evidenceCodes})
                //Create Association nodes for other identifiers.
                CREATE (eca:EvidenceCodeAssociation {link_from:row.primaryId, link_to:row.evidenceCodes})
                CREATE (f)-[feca:ASSOCIATION]->(eca)

                MERGE (pub:Publication {primaryKey:row.pubPrimaryKey})
                SET pub.pubModId = row.pubModId
                SET pub.pubMedId = row.pubMedId
                SET pub.pubModUrl = row.pubModUrl
                SET pub.pubMedUrl = row.pubMedUrl


                FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_MARKER_FOR]->(d))

                FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_IMPLICATED_IN]->(d))

                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_marker_of' THEN [1] ELSE [] END |
                    MERGE (f)-[fq:IS_NOT_MARKER_OF]->(d))

                 FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)-[fq:IS_NOT_IMPLICATED_IN]->(d))


            )
            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'fish' THEN [1] ELSE [] END |
                MERGE (f:Fish {primaryKey:row.primaryId})

                MERGE (f)-[:FROM_SPECIES]->(spec)
                SET f.with = row.with

                MERGE (d:DOTerm {primaryKey:row.doId})
                SET d.doDisplayId = row.doDisplayId
                SET d.doUrl = row.doUrl
                SET d.doPrefix = row.doPrefix

                //Create the Association node to be used for the object/doTerm
                MERGE (da:DiseaseAssociation {link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.
                MERGE (f)-[fda:ASSOCIATION]->(da)
                MERGE (da)-[dad:ASSOCIATION]->(d)

                //Create nodes for other identifiers.  TODO- do this better. evidence code node needs to be linked up with each
                //of these separately.

                CREATE (ecs:EvidenceCodes {evidenceCodes:row.evidenceCodes})
                //Create Association nodes for other identifiers.
                CREATE (eca:EvidenceCodeAssociation {link_from:row.primaryId, link_to:row.evidenceCodes})
                CREATE (f)-[feca:ASSOCIATION]->(eca)

                MERGE (pub:Publication {primaryKey:row.pubPrimaryKey})
                SET pub.pubModId = row.pubModId
                SET pub.pubMedId = row.pubMedId
                SET pub.pubModUrl = row.pubModUrl
                SET pub.pubMedUrl = row.pubMedUrl

                FOREACH (rel IN CASE when row.relationshipType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (f)-[fa:IS_MODEL_OF]->(d))
                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (f)-[fq:IS_NOT_MODEL_OF]->(d))
            )


           // FOREACH (code in codes |
            //        MERGE (ec:EvidenceCode {primaryKey:code.code}))


        """
        Transaction.execute_transaction(self, query, data)