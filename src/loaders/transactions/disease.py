from .transaction import Transaction

class DiseaseTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def disease_object_tx(self, data):
        
        # Loads the Disease data into Neo4j.

        executeGene = """
            UNWIND $data as row

            MERGE (d:DOTerm:Ontology {primaryKey:row.doId})
               SET d.doDisplayId = row.doDisplayId
               SET d.doUrl = row.doUrl
               SET d.doPrefix = row.doPrefix
               SET d.doId = row.doId

            MATCH (f:Gene {primaryKey:row.primaryId})
                SET f :DiseaseObject

            MERGE (spec:Species {primaryKey: row.taxonId})
            MERGE (f)<-[:FROM_SPECIES]->(spec)
                SET f.with = row.with

            //the foreach statments that represent relationships/associationType are not here for capitalization purposes.
            //instead we mimic a conditional statement by creating a collection with 1 value, and an empty collection.
            //Create the Association node to be used for the object/doTerm
            MERGE (da:Association {primaryKey:row.uuid})
                SET da :DiseaseGeneJoin

            FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                MERGE (f)<-[fa:IS_MARKER_FOR {uuid:row.uuid}]->(d))
                SET da.joinType = 'is_marker_of'

            FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                MERGE (f)<-[fa:IS_IMPLICATED_IN {uuid:row.uuid}]->(d))
                SET da.joinType = 'is_implicated_in'

            //Create the relationship from the object node to association node.
            //Create the relationship from the association node to the DoTerm node.

            MERGE (f)-[fda:ASSOCIATION]->(da)
            MERGE (da)-[dad:ASSOCIATION]->(d)

            // Publications

            MERGE (pub:Publication {primaryKey:row.pubPrimaryKey})
                SET pub.pubModId = row.pubModId
                SET pub.pubMedId = row.pubMedId
                SET pub.pubModUrl = row.pubModUrl
                SET pub.pubMedUrl = row.pubMedUrl

            MERGE (da)-[dapu:EVIDENCE]->(pub)

            FOREACH (entity in row.ecodes|
                    MERGE (ecode1:EvidenceCode {primaryKey:entity})
                    MERGE (da)-[daecode1:EVIDENCE]->(ecode1)
            )

            """

        Transaction.execute_transaction(self, executeGene, data)