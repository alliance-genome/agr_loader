import logging

from .transaction import Transaction


logger = logging.getLogger(__name__)

class DiseaseGeneTransaction(Transaction):

    def disease_gene_object_tx(self, data):
        # Loads the Disease data into Neo4j.

        executeGene = """

            UNWIND $data as row

            MATCH (d:DOTerm:Ontology {primaryKey:row.doId})
            MATCH (gene:Gene {primaryKey:row.primaryId})
            // LOAD NODES

            MERGE (l:Load {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced,
                 l.dataProvider = row.dataProvider,
                 l.loadName = "Disease"

             MERGE (dga:Association:DiseaseEntityJoin {primaryKey:row.uuid})  

            FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END | 
                MERGE (gene)<-[fafg:IS_MARKER_FOR {uuid:row.uuid}]->(d) 
                    SET fafg.dataProvider = row.dataProvider ,
                     fafg.dateProduced = row.dateProduced ,
                     dga.joinType = 'is_marker_of'     )  

            FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END | 
                MERGE (gene)<-[fafg:IS_IMPLICATED_IN {uuid:row.uuid}]->(d) 
                    SET fafg.dataProvider = row.dataProvider ,
                     fafg.dateProduced = row.dateProduced ,
                     dga.joinType = 'is_implicated_in'     )

            MERGE (gene)-[fdag:ASSOCIATION]->(dga) 
            MERGE (dga)-[dadg:ASSOCIATION]->(d)  

            // PUBLICATIONS FOR GENE  
            MERGE (pubg:Publication {primaryKey:row.pubPrimaryKey}) 
                SET pubg.pubModId = row.pubModId ,
                 pubg.pubMedId = row.pubMedId ,
                 pubg.pubModUrl = row.pubModUrl ,
                 pubg.pubMedUrl = row.pubMedUrl  

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

        self.execute_transaction(executeGene, data)
        self.execute_transaction(deleteEmptyDONodes, data)
