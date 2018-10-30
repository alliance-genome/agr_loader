# coding=utf-8

import uuid
from datetime import datetime, timezone
from .transaction import Transaction
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class GeneDiseaseOrthoTransaction(Transaction):
    
    def __init__(self):

        self.neo_import_dir = '/var/lib/neo4j/import/'
        self.filename = 'disease_by_orthology.csv'

        # Add publication and evidence code
        query = """
              MERGE (pubg:Publication {primaryKey:"MGI:6194238"})
                  SET pubg.pubModId = "MGI:6194238"
                  SET pubg.pubModUrl = "http://www.informatics.jax.org/reference/summary?id=mgi:6194238"
              MERGE (:EvidenceCode {primaryKey:"IEA"})"""
        self.run_single_query(query)

    def retreive_diseases_inferred_by_ortholog(self):
        query = """

        MATCH (disease:DOTerm)-[da:IS_IMPLICATED_IN|IS_MARKER_FOR]-(gene1:Gene)-[o:ORTHOLOGOUS]->(gene2:Gene)
        MATCH (ec:EvidenceCode)-[:EVIDENCE]-(dej:DiseaseEntityJoin)-[a:ASSOCIATION]-(gene1:Gene)-[:FROM_SPECIES]->(species:Species)
             WHERE o.strictFilter
                 AND da.uuid = dej.primaryKey
                 AND NOT ec.primaryKey IN ["IEA", "ISS", "ISO"]
        RETURN DISTINCT gene2.primaryKey AS geneID,
               gene1.primaryKey AS fromGeneID,
               type(da) AS relationType,
               disease.primaryKey AS doId"""

        returnSet = self.run_single_query(query)
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        orthologous_disease_data = []
        for record in returnSet:
            row = dict(primaryId=record["geneID"],
                    fromGeneId=record["fromGeneID"],
                    relationshipType=record["relationType"].lower(),
                    doId=record["doId"],
                    dateProduced=now,
                    uuid=str(uuid.uuid4()))
            orthologous_disease_data.append(row)

        return orthologous_disease_data

    def add_disease_inferred_by_ortho_tx(self, data):
        # Loads the gene to disease via ortho data into Neo4j.

        executeG2D = """

            UNWIND $data as row

            MATCH (d:DOTerm:Ontology {primaryKey:row.doId}),
                  (gene:Gene {primaryKey:row.primaryId}),
                  (fromGene:Gene {primaryKey:row.fromGeneId}),
                  (pub:Publication {primaryKey:"MGI:6194238"}),
                  (ecode:EvidenceCode {primaryKey:"IEA"})

            CREATE (dga:Association:DiseaseEntityJoin {primaryKey:row.uuid})
                SET dga.dataProvider = 'Alliance'

            FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                CREATE (gene)-[fafg:BIOMARKER_VIA_ORTHOLOGY {uuid:row.uuid}]->(d)
                    SET fafg.dataProvider = "Alliance",
                        fafg.dateProduced = row.dateProduced,
                        dga.joinType = 'biomarker_via_orthology')

            FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                CREATE (gene)-[fafg:IMPLICATED_VIA_ORTHOLOGY {uuid:row.uuid}]->(d)
                    SET fafg.dataProvider = "Alliance",
                        fafg.dateProduced = row.dateProduced,
                        dga.joinType = 'implicated_via_orthology')

            CREATE (gene)-[fdag:ASSOCIATION]->(dga)
            CREATE (dga)-[dadg:ASSOCIATION]->(d)
            CREATE (dga)-[dapug:EVIDENCE]->(pub)
            CREATE (dga)-[:FROM_ORTHOLOGOUS_GENE]->(fromGene)
            CREATE (dga)-[daecode1g:EVIDENCE]->(ecode)

            """

        self.execute_transaction(executeG2D, data)
