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
                SET l.dateProduced = row.dateProduced,
                 l.loadName = "Phenotype",
                 l.dataProviders = row.dataProviders,
                 l.dataProvider = row.dataProvider

            MERGE (pa:Association {primaryKey:row.uuid})
                SET pa :PhenotypeEntityJoin,
                 pa.joinType = 'phenotype',
                 pa.dataProviders = row.dataProviders

            MERGE (feature)-[featurep:HAS_PHENOTYPE {uuid:row.uuid}]->(p)

            MERGE (feature)-[fpaf:ASSOCIATION]->(pa)
            MERGE (pa)-[pad:ASSOCIATION]->(p)
            MERGE (ag)-[agpa:ASSOCIATION]->(pa)

            //FOREACH (dataProvider in row.dataProviders |
                //MERGE (dp:DataProvider {primaryKey:dataProvider})
                //MERGE (pa)-[odp:DATA_PROVIDER]-(dp)
                //MERGE (l)-[ldp:DATA_PROVIDER]-(dp))

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl

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
                SET l.dataProviders = row.dataProviders
                SET l.dataProvider = row.dataProvider

            MERGE (pa:Association {primaryKey:row.uuid})
                SET pa :PhenotypeEntityJoin
                SET pa.joinType = 'phenotype'
                SET pa.dataProviders = row.dataProviders
                SET pa.dataProvider = row.dataProvider
            
             MERGE (pa)-[dfal:LOADED_FROM]-(l)
                
                MERGE (pa)-[pad:ASSOCIATION]->(p)
                MERGE (g)-[gpa:ASSOCIATION]->(pa)
                MERGE (g)-[genep:HAS_PHENOTYPE {uuid:row.uuid}]->(p)

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

            MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)
            MERGE (pa)-[dapuf:EVIDENCE]->(pubf)

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

