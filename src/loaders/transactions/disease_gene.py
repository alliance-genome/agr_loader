# coding=utf-8
from .transaction import Transaction


class DiseaseGeneTransaction(Transaction):
    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def disease_gene_object_tx(self, data):
        # Loads the Disease data into Neo4j.

        executeGene = """

            UNWIND $data as row

            // GET PRIMARY DATA OBJECTS

            MATCH (d:DOTerm:Ontology {primaryKey:row.doId})
            MATCH (gene:Gene {primaryKey:row.primaryId})

            // LOAD NODES

            MERGE (l:Load {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced
                SET l.loadName = "Disease"

            MERGE (spec:Species {primaryKey: row.taxonId})
            MERGE (gene)<-[:FROM_SPECIES]->(spec)

             MERGE (dga:Association {primaryKey:row.uuid})  
                SET dga :DiseaseEntityJoin

            FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END | 
                MERGE (gene)<-[fafg:IS_MARKER_FOR {uuid:row.uuid}]->(d) 
                    SET fafg.dateProduced = row.dateProduced 
                    SET dga.joinType = 'is_marker_of'     )  

            FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END | 
                MERGE (gene)<-[fafg:IS_IMPLICATED_IN {uuid:row.uuid}]->(d) 
                    SET fafg.dateProduced = row.dateProduced 
                    SET dga.joinType = 'is_implicated_in'     )

            MERGE (gene)-[fdag:ASSOCIATION]->(dga) 
            MERGE (dga)-[dadg:ASSOCIATION]->(d)  

            FOREACH (dataProvider in row.dataProviders |
                MERGE (dp:DataProvider {primaryKey:dataProvider})
                MERGE (dga)-[odp:DATA_PROVIDER]-(dp)
                MERGE (l)-[ldp:DATA_PROVIDER]-(dp))

            // PUBLICATIONS FOR GENE  
            MERGE (pubg:Publication {primaryKey:row.pubPrimaryKey}) 
                SET pubg.pubModId = row.pubModId 
                SET pubg.pubMedId = row.pubMedId 
                SET pubg.pubModUrl = row.pubModUrl 
                SET pubg.pubMedUrl = row.pubMedUrl  

            MERGE (l)-[loadAssociation:LOADED_FROM]-(pubg)  

            MERGE (dga)-[dapug:EVIDENCE]->(pubg)  

            // EVIDENCE CODES FOR GENE  

            FOREACH (entity in row.ecodes| 
                MERGE (ecode1g:EvidenceCode {primaryKey:entity}) 
                MERGE (dga)-[daecode1g:EVIDENCE]->(ecode1g) 
            )  

            """

        deleteEmptyDONodes = """

            MATCH (dd:DOTerm) WHERE keys(dd)[0] = 'primaryKey' and size(keys(dd)) = 1
            DETACH DELETE (dd)

        """

        Transaction.execute_transaction(self, executeGene, data)
        Transaction.execute_transaction(self, deleteEmptyDONodes, data)
