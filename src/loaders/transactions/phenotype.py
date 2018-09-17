from .transaction import Transaction


class PhenotypeTransaction(Transaction):
    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def phenotype_object_tx(self, data, species):
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
                SET l.dataProvider = row.dataProvider

            MERGE (pa:Association:PhenotypeEntityJoin {primaryKey:row.uuid})
                SET pa.joinType = 'phenotype',
                pa.dataProviders = row.dataProviders

            MERGE (feature)-[featurep:HAS_PHENOTYPE]->(p:Phenotype)
                SET featurep.uuid = row.uuid

            MERGE (feature)-[fpaf:ASSOCIATION]->(pa:Association:PhenotypeEntityJoin)
            MERGE (pa:Association:PhenotypeEntityJoin)-[pad:ASSOCIATION]->(p:Phenotype)
            MERGE (ag:Gene)-[agpa:ASSOCIATION]->(pa:Association:PhenotypeEntityJoin)

            //FOREACH (dataProvider in row.dataProviders |
                //MERGE (dp:DataProvider {primaryKey:dataProvider})
                //MERGE (pa)-[odp:DATA_PROVIDER]-(dp)
                //MERGE (l)-[ldp:DATA_PROVIDER]-(dp))

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl

            MERGE (l:Load)-[loadAssociation:LOADED_FROM]-(pubf:Publication)
            MERGE (pa:Association:PhenotypeEntityJoin)-[dapuf:EVIDENCE]->(pubf:Publication)

            """
        executeGene = """

        UNWIND $data as row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey:row.primaryId})

            MERGE (p:Phenotype {primaryKey:row.phenotypeStatement})
                SET p.phenotypeStatement = row.phenotypeStatement

            MERGE (l:Load {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced,
                 l.loadName = "Phenotype",
                 l.dataProviders = row.dataProviders,
                 l.dataProvider = row.dataProvider

            MERGE (pa:Association:PhenotypeEntityJoin {primaryKey:row.uuid})
                SET pa.joinType = 'phenotype',
                 pa.dataProviders = row.dataProviders,
                 pa.dataProvider = row.dataProvider
            
             MERGE (pa:Association:PhenotypeEntityJoin)-[dfal:LOADED_FROM]-(l)
                
                MERGE (pa:Association:PhenotypeEntityJoin)-[pad:ASSOCIATION]->(p:Phenotype)
                MERGE (g:Gene)-[gpa:ASSOCIATION]->(pa:Association:PhenotypeEntityJoin)
                MERGE (g:Gene)-[genep:HAS_PHENOTYPE]->(p:Phenotype)
                    SET genep.uuid = row.uuid

            //FOREACH (dataProvider in row.dataProviders |
              //  MERGE (dp:DataProvider {primaryKey:dataProvider})
              //  MERGE (pa)-[odp:DATA_PROVIDER]-(dp)
              //  MERGE (l)-[ldp:DATA_PROVIDER]-(dp))

            // PUBLICATIONS FOR FEATURE

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                SET pubf.pubModId = row.pubModId
                SET pubf.pubMedId = row.pubMedId
                SET pubf.pubModUrl = row.pubModUrl
                SET pubf.pubMedUrl = row.pubMedUrl

            MERGE (l:Load)-[loadAssociation:LOADED_FROM]-(pubf:Publication)
            MERGE (pa:Association:PhenotypeEntityJoin)-[dapuf:EVIDENCE]->(pubf:Publication)

        """

        # this is to prevent SGD and Human from double running phenotype in order to get features.
        # a bit of a hack to avoid checking if something is a feature before executing another query to
        # get the gene of interest.
        speciesWithFeatures = ['Mus musculus', 'Danio rerio', 'Caenorhabditis elegans', 'Rattus norvegicus', 'Drosophila melanogaster']

        if species in speciesWithFeatures:
            Transaction.execute_transaction(self, executeGene, data)
            Transaction.execute_transaction(self, executeFeature, data)
        else:
            Transaction.execute_transaction(self, executeGene, data)

