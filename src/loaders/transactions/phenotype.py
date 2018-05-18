from .transaction import Transaction


class PhenotypeTransaction(Transaction):
    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def phenotype_object_tx(self, data):
        # Loads the Disease data into Neo4j.

        executeFeature = """
            UNWIND $data as row

            // GET PRIMARY DATA OBJECTS

            MATCH (feature:Feature {primaryKey:row.primaryId})
            MATCH (g:Gene {primaryKey:row.allelicGeneId})

            // LOAD NODES

            MERGE (p:Phenotype {primaryKey:row.phenotypeStatement})

            MERGE (l:Load {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced
                SET l.dataProvider = row.dataProvider
                SET l.loadName = "Phenotype"

            MERGE (pa:Association {primaryKey:row.uuid})
                SET pa :PhenotypeFeatureAssociation


            MERGE (feature)-[fpaf:ASSOCIATION]->(pa)
            MERGE (pa)-[pad:ASSOCIATION]->(p)
            MERGE (g)-[gpa:ASSOCIATION]->(pa)

            // PUBLICATIONS FOR FEATURE

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                SET pubf.pubModId = row.pubModId
                SET pubf.pubMedId = row.pubMedId
                SET pubf.pubModUrl = row.pubModUrl
                SET pubf.pubMedUrl = row.pubMedUrl

            MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)

            MERGE (pa)-[papubf:EVIDENCE]->(pubf)

            """

        Transaction.execute_transaction(self, executeFeature, data)