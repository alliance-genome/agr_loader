# coding=utf-8

import uuid
from datetime import datetime, timezone
from .transaction import Transaction


class GeneDiseaseOrthoTransaction(Transaction):
    def __init__(self, graph):
        Transaction.__init__(self, graph)

        # Add publication and evidence code
        query = """
              MERGE (pubg:Publication {primaryKey:"MGI:6194238"})
                  SET pubg.pubModId = "MGI:6194238"
                  SET pubg.pubModUrl = "http://www.informatics.jax.org/reference/summary?id=mgi:6194238"
              MERGE (:EvidenceCode {primaryKey:"IEA"})"""
        tx = Transaction(graph)
        tx.run_single_query(query)

    def retreive_diseases_gene_inferred_by_orthology(self):
        query = """
        MATCH (disease:DOTerm)-[da]-(gene1:Gene)-[o:ORTHOLOGOUS]->(gene2:Gene)
        MATCH (ec)-[:EVIDENCE]-(dej:DiseaseEntityJoin)-[e]-(gene1)-[FROM_SPECIES]->(species:Species)
            WHERE da.uuid = dej.primaryKey
              AND o.strictFilter
              AND NOT ec.primaryKey IN ["IEA", "ISS", "ISO"]
        OPTIONAL MATCH (disease:DOTerm)-[da2]-(gene2:Gene)-[ag:IS_ALLELE_OF]->(:Feature)-[da3]-(disease:DOTerm)
            WHERE da2 IS null  // filters relations that already exist
                 AND da3 IS null // filter where allele already has disease association
        RETURN gene2.primaryKey AS geneID,
               species.primaryKey AS speciesID,
               type(da) AS relationType,
               disease.primaryKey AS doId,
               collect(ec) AS ecs"""

        orthologous_disease_data = []
        tx = Transaction(self.graph)
        returnSet = tx.run_single_query(query)
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        for record in returnSet:
            row = dict(primaryId = record["geneID"],
                       speciesId = record["speciesID"],
                       relationshipType = record["relationType"].lower(),
                       loadKey = record["doId"] + "_Disease",
                       doId = record["doId"],
                       dateProduced = now,
                       uuid = str(uuid.uuid4()))
            orthologous_disease_data.append(row)

        return orthologous_disease_data

    def add_gene_disease_inferred_through_ortho_tx(self, data):
        # Loads the gene to disease via ortho data into Neo4j.

        executeG2D = """

            UNWIND $data as row

            MATCH (d:DOTerm:Ontology {primaryKey:row.doId}),
                  (gene:Gene {primaryKey:row.primaryId}),
                  (species:Species {primaryKey:row.speciesId}),
                  (pub:Publication {primaryKey:"MGI:6194238"}),
                  (ecode:EvidenceCode {primaryKey:"IEA"})

            MERGE (dga:Association {primaryKey:row.uuid})
                SET dga :DiseaseEntityJoin

            FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                MERGE (gene)<-[fafg:IS_MARKER_FOR {uuid:row.uuid}]->(d)
                    SET fafg.dataProvider = "Alliance"
                    SET fafg.dateProduced = row.dateProduced
                    SET dga.joinType = row.relationshipType)

            FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                MERGE (gene)<-[fafg:IS_IMPLICATED_IN {uuid:row.uuid}]->(d)
                    SET fafg.dataProvider = "Alliance"
                    SET fafg.dateProduced = row.dateProduced
                    SET dga.joinType = row.relationshipType)

            MERGE (gene)-[fdag:ASSOCIATION]->(dga)
            MERGE (dga)-[dadg:ASSOCIATION]->(d)

            MERGE (dga)-[dapug:EVIDENCE]->(pub)
            MERGE (dga)-[:FROM_SPECIES]-(species)

            MERGE (dga)-[daecode1g:EVIDENCE]->(ecode)

            """

        Transaction.execute_transaction(self, executeG2D, data)
