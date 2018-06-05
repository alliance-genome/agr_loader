from .transaction import Transaction


class PhenotypeTransaction(Transaction):
    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def phenotype_object_tx(self, data):
        # Loads the Phenotype data into Neo4j.

        executeFeature = """
            UNWIND $data as row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (feature:Feature {primaryKey:row.primaryId})
            MATCH (ag:Gene)-[a:IS_ALLELE_OF]-(feature)

            MERGE (p:Phenotype {primaryKey:row.phenotypeStatement})
                SET p.phenotypeStatement = row.phenotypeStatement

            MERGE (l:Load:Entity {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced
                SET l.loadName = "Phenotype"
                SET l.dataProviders = row.dataProviders

            MERGE (pa:Association {primaryKey:row.uuid})
                SET pa :PhenotypeEntityJoin
                SET pa.joinType = 'phenotype'
                SET pa.dataProviders = row.dataProviders

            MERGE (feature)-[featurep:HAS_PHENOTYPE {uuid:row.uuid}]->(p)

            MERGE (feature)-[fpaf:ASSOCIATION]->(pa)
            MERGE (pa)-[pad:ASSOCIATION]->(p)
            MERGE (ag)-[agpa:ASSOCIATION]->(pa)

            //FOREACH (dataProvider in row.dataProviders |
                //MERGE (dp:DataProvider {primaryKey:dataProvider})
                //MERGE (pa)-[odp:DATA_PROVIDER]-(dp)
                //MERGE (l)-[ldp:DATA_PROVIDER]-(dp))

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                SET pubf.pubModId = row.pubModId
                SET pubf.pubMedId = row.pubMedId
                SET pubf.pubModUrl = row.pubModUrl
                SET pubf.pubMedUrl = row.pubMedUrl

            MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)
            MERGE (pa)-[dapuf:EVIDENCE]->(pubf)

            """
        executeGene = """

        UNWIND $data as row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey:row.primaryId})

            MERGE (p:Phenotype {primaryKey:row.phenotypeStatement})
                SET p.phenotypeStatement = row.phenotypeStatement

            MERGE (l:Load {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced
                SET l.loadName = "Phenotype"

            MERGE (pa:Association {primaryKey:row.uuid})
                SET pa :PhenotypeEntityJoin
                SET pa.joinType = 'phenotype'
                
                MERGE (pa)-[pad:ASSOCIATION]->(p)
                MERGE (g)-[gpa:ASSOCIATION]->(pa)
                MERGE (g)-[genep:HAS_PHENOTYPE {uuid:row.uuid}]->(p)

            FOREACH (dataProvider in row.dataProviders |
                MERGE (dp:DataProvider {primaryKey:dataProvider})
                MERGE (pa)-[odp:DATA_PROVIDER]-(dp)
                MERGE (l)-[ldp:DATA_PROVIDER]-(dp))

            // PUBLICATIONS FOR FEATURE

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                SET pubf.pubModId = row.pubModId
                SET pubf.pubMedId = row.pubMedId
                SET pubf.pubModUrl = row.pubModUrl
                SET pubf.pubMedUrl = row.pubMedUrl

            MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)
            MERGE (pa)-[dapuf:EVIDENCE]->(pubf)

        """

        Transaction.execute_transaction(self, executeFeature, data)
        Transaction.execute_transaction(self, executeGene, data)
