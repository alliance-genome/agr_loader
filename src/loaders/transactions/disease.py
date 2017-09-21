from .transaction import Transaction

class DiseaseTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def disease_object_tx(self, data):
        
        # Loads the Disease data into Neo4j.

        executeGene = """
            UNWIND $data as row


            MATCH(f:Gene {primaryKey:row.primaryId})
                //SET f :DiseaseObject


            MATCH (d:DOTerm:Ontology {primaryKey:row.doId})

            //Create the load node(s)
            MERGE (l:Load {primaryKey:row.loadKey,dateProduced:row.dateProduced,dataProvider:row.dataProvider,loadName:"Disease"})

                //TODO: the species add should likely be MATCH
                MERGE (spec:Species {primaryKey: row.taxonId})
                MERGE (f)<-[:FROM_SPECIES]->(spec)
                    //SET f.with = row.with

                MERGE (da:Association {primaryKey:row.uuid})
                    SET da :DiseaseGeneJoin

                FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                    MERGE (f)<-[fa:IS_MARKER_FOR {uuid:row.uuid}]->(d)
                    SET fa.dataProvider = row.dataProvider
                    SET fa.dateProduced = row.dateProduced)
                    SET da.joinType = 'is_marker_of'

                FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)<-[fa:IS_IMPLICATED_IN {uuid:row.uuid}]->(d)
                    SET fa.dataProvider = row.dataProvider
                    SET fa.dateProduced = row.dateProduced)
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

                MERGE (l)-[loadAssociation:LOADED_FROM]-(pub)

                MERGE (da)-[dapu:EVIDENCE]->(pub)

                FOREACH (entity in row.ecodes|
                        MERGE (ecode1:EvidenceCode {primaryKey:entity})
                        MERGE (da)-[daecode1:EVIDENCE]->(ecode1)
                )

            """

        deleteEmptyDONodes = """

            MATCH (dd:DOTerm) WHERE keys(dd)[0] = 'primaryKey' and size(keys(dd)) = 1
            DETACH DELETE (dd)

        """

        Transaction.execute_transaction(self, executeGene, data)
        Transaction.execute_transaction(self, deleteEmptyDONodes, data)