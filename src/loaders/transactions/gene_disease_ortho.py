# coding=utf-8

import uuid
from datetime import datetime, timezone
from .transaction import Transaction


class GeneDiseaseOrthoTransaction(Transaction):
    def __init__(self, graph):
        Transaction.__init__(self, graph)
        self.neo_import_dir = '/var/lib/neo4j/import/'
        self.filename = 'disease_by_orthology.csv'

        # Add publication and evidence code
        query = """
              MERGE (pubg:Publication {primaryKey:"MGI:6194238"})
                  SET pubg.pubModId = "MGI:6194238"
                  SET pubg.pubModUrl = "http://www.informatics.jax.org/reference/summary?id=mgi:6194238"
              MERGE (:EvidenceCode {primaryKey:"IEA"})"""
        tx = Transaction(graph)
        tx.run_single_query(query)

    def retreive_diseases_inferred_by_ortholog(self):
        query = """
        //MATCH (disease:DOTerm)-[da:IS_IMPLICATED_IN|IS_MARKER_FOR]-(allele:Feature)-[ag:IS_ALLELE_OF]-(gene1:Gene)-[o:ORTHOLOGOUS]->(gene2:Gene)
        //MATCH (ec:EvidenceCode)-[:EVIDENCE]-(dej:DiseaseEntityJoin)--(allele:Feature)-[ag:IS_ALLELE_OF]-(gene1:Gene)-[:FROM_SPECIES]->(species:Species)
        //     WHERE o.strictFilter
        //         AND da.uuid = dej.primaryKey
        //         AND NOT ec.primaryKey IN ["IEA", "ISS", "ISO"]
        //OPTIONAL MATCH (disease:DOTerm)-[da2:ASSOCIATION]-(gene2:Gene)-[ag2:IS_ALLELE_OF]->(:Feature)-[da3:ASSOCIATION]-(disease:DOTerm)
        //    WHERE da2 IS null  // filters relations that already exist
        //         AND da3 IS null // filter where allele already has disease association
        //RETURN DISTINCT gene2.primaryKey AS geneID,
        //       species.primaryKey AS speciesID,
        //       type(da) AS relationType,
        //       disease.primaryKey AS doId
        //UNION
        MATCH (disease:DOTerm)-[da:IS_IMPLICATED_IN|IS_MARKER_FOR]-(gene1:Gene)-[o:ORTHOLOGOUS]->(gene2:Gene)
        MATCH (ec:EvidenceCode)-[:EVIDENCE]-(dej:DiseaseEntityJoin)--(gene1:Gene)-[:FROM_SPECIES]->(species:Species)
             WHERE o.strictFilter
                 AND da.uuid = dej.primaryKey
                 AND NOT ec.primaryKey IN ["IEA", "ISS", "ISO"]
        OPTIONAL MATCH (disease:DOTerm)-[da2:ASSOCIATION]-(gene2:Gene)-[ag:IS_ALLELE_OF]->(:Feature)-[da3:IS_IMPLICATED_IN|IS_MARKER_FOR]-(disease:DOTerm)
            WHERE da2 IS null  // filters relations that already exist
                 AND da3 IS null // filter where allele already has disease association
        RETURN DISTINCT gene2.primaryKey AS geneID,
               gene1.primaryKey AS fromGeneID,
               type(da) AS relationType,
               disease.primaryKey AS doId"""

        tx = Transaction(self.graph)
        returnSet = tx.run_single_query(query)
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        orthologous_disease_data = []        
        for record in returnSet:
            row = dict(primaryId = record["geneID"],
                    fromGeneId = record["fromGeneID"],
                    relationshipType = record["relationType"].lower(),
                    doId = record["doId"],
                    dateProduced = now,
                    uuid = str(uuid.uuid4()))
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
                ON CREATE SET dga.source = "Alliance"

            FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                CREATE (gene)-[fafg:IS_MARKER_FOR {uuid:row.uuid}]->(d)
                    SET fafg.dataProvider = "Alliance",
                        fafg.dateProduced = row.dateProduced,
                        dga.joinType = row.relationshipType)

            FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                CREATE (gene)-[fafg:IS_IMPLICATED_IN {uuid:row.uuid}]->(d)
                    SET fafg.dataProvider = "Alliance",
                        fafg.dateProduced = row.dateProduced,
                        dga.joinType = row.relationshipType)

            CREATE (gene)-[fdag:ASSOCIATION]->(dga)
            CREATE (dga)-[dadg:ASSOCIATION]->(d)
            CREATE (dga)-[dapug:EVIDENCE]->(pub)
            CREATE (dga)-[:FROM_ORTHOLOGOUS_GENE]->(fromGene)
            CREATE (dga)-[daecode1g:EVIDENCE]->(ecode)

            """

        Transaction.execute_transaction(self, executeG2D, data)
