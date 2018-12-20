import logging, uuid
import multiprocessing

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)


class PhenoTypeETL(ETL):

    execute_feature_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (feature:Feature {primaryKey:row.primaryId})
            
            MATCH (ag:Gene)-[a:IS_ALLELE_OF]-(feature)

            MERGE (p:Phenotype {primaryKey:row.phenotypeStatement})
                ON CREATE SET p.phenotypeStatement = row.phenotypeStatement

            MERGE (pa:Association {primaryKey:row.uuid})
                ON CREATE SET pa :PhenotypeEntityJoin,
                    pa.joinType = 'phenotype',
                    pa.dataProviders = row.dataProviders

            MERGE (feature)-[featurep:HAS_PHENOTYPE {uuid:row.uuid}]->(p)

            MERGE (feature)-[fpaf:ASSOCIATION]->(pa)
            MERGE (pa)-[pad:ASSOCIATION]->(p)
            MERGE (ag)-[agpa:ASSOCIATION]->(pa)

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pubf.pubModId = row.pubModId,
                    pubf.pubMedId = row.pubMedId,
                    pubf.pubModUrl = row.pubModUrl,
                    pubf.pubMedUrl = row.pubMedUrl

            MERGE (pa)-[dapuf:EVIDENCE]->(pubf) """
            
    execute_gene_template = """

            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:Gene {primaryKey:row.primaryId})

            MERGE (p:Phenotype {primaryKey:row.phenotypeStatement})
                ON CREATE SET p.phenotypeStatement = row.phenotypeStatement

            MERGE (pa:Association {primaryKey:row.uuid})
                ON CREATE SET pa :PhenotypeEntityJoin,
                    pa.joinType = 'phenotype',
                    pa.dataProviders = row.dataProviders,
                    pa.dataProvider = row.dataProvider
            
                MERGE (pa)-[pad:ASSOCIATION]->(p)
                MERGE (g)-[gpa:ASSOCIATION]->(pa)
                MERGE (g)-[genep:HAS_PHENOTYPE {uuid:row.uuid}]->(p)

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pubf.pubModId = row.pubModId,
                    pubf.pubMedId = row.pubMedId,
                    pubf.pubModUrl = row.pubModUrl,
                    pubf.pubMedUrl = row.pubMedUrl

            MERGE (pa)-[dapuf:EVIDENCE]->(pubf) """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []
        
        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            p.start()
            thread_pool.append(p)

        for thread in thread_pool:
            thread.join()
  
    def _process_sub_type(self, sub_type):
        
        logger.info("Loading Phenotype Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        data = JSONFile().get_data(filepath)
        logger.info("Finished Loading Phenotype Data: %s" % sub_type.get_data_provider())

        if data is None:
            logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
            return

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()
        
        generators = self.get_generators(data, batch_size)

        query_list = [
            [PhenoTypeETL.execute_gene_template, commit_size, "phenotype_gene_data_" + sub_type.get_data_provider() + ".csv"],
            [PhenoTypeETL.execute_feature_template, commit_size, "phenotype_feature_data_" + sub_type.get_data_provider() + ".csv"],
        ]
            
        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, phenotype_data, batch_size):
        list_to_yield = []
        dateProduced = phenotype_data['metaData']['dateProduced']
        dataProviders = []
        dataProviderObject = phenotype_data['metaData']['dataProvider']
        counter = 0
        dataProviderCrossRef = dataProviderObject.get('crossReference')
        dataProvider = dataProviderCrossRef.get('id')
        dataProviderPages = dataProviderCrossRef.get('pages')
        dataProviderCrossRefSet = []

        loadKey = dateProduced + dataProvider + "_phenotype"

        #TODO: get SGD to fix their files.

        if dataProviderPages is not None:
            for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = ETLHelper.get_page_complete_url(dataProvider, ETL.xrefUrlMap, dataProvider, dataProviderPage)

                dataProviderCrossRefSet.append(ETLHelper.get_xref_dict(dataProvider, dataProvider, dataProviderPage, dataProviderPage, dataProvider, crossRefCompleteUrl, dataProvider + dataProviderPage))

                dataProviders.append(dataProvider)
                logger.debug("data provider: " + dataProvider)

        for pheno in phenotype_data['data']:
            counter = counter + 1
            pubMedId = None
            pubModId = None
            pubMedUrl = None
            pubModUrl = None
            primaryId = pheno.get('objectId')
            phenotypeStatement = pheno.get('phenotypeStatement')

            if self.testObject.using_test_data() is True:
                is_it_test_entry = self.testObject.check_for_test_id_entry(primaryId)
                if is_it_test_entry is False:
                    continue

            evidence = pheno.get('evidence')

            if 'modPublicationId' in evidence:
                pubModId = evidence.get('modPublicationId')

            if 'pubMedId' in evidence:
                pubMedId = evidence.get('pubMedId')

            if pubMedId is not None:
                pubMedPrefix = pubMedId.split(":")[0]
                pubMedLocalId = pubMedId.split(":")[1]
                pubMedUrl = ETLHelper.get_no_page_complete_url(pubMedLocalId, ETL.xrefUrlMap, pubMedPrefix, primaryId)

                pubModId = pheno.get('pubModId')

            if pubModId is not None:
                pubModPrefix = pubModId.split(":")[0]
                pubModLocalId = pubModId.split(":")[1]
                pubModUrl = ETLHelper.get_complete_pub_url(pubModLocalId, pubModId)

            if pubMedId is None:
                pubMedId = ""

            if pubModId is None:
                pubModId = ""

            dateAssigned = pheno.get('dateAssigned')

            if pubModId is None and pubMedId is None:
                logger.info (primaryId + "is missing pubMed and pubMod id")

            phenotype_feature = {
                "primaryId": primaryId.strip(),
                "phenotypeStatement": phenotypeStatement.strip(),
                "dateAssigned": dateAssigned,
                "pubMedId": pubMedId,
                "pubMedUrl": pubMedUrl,
                "pubModId": pubModId,
                "pubModUrl": pubModUrl,
                "pubPrimaryKey": pubMedId + pubModId,
                "uuid": str(uuid.uuid4()),
                "loadKey": loadKey,
                "type": "gene",
                "dataProviders": dataProviders,
                "dateProduced": dateProduced
             }

            list_to_yield.append(phenotype_feature)

            if counter == batch_size:
                yield [list_to_yield, list_to_yield]
                list_to_yield = []
                counter = 0

        if counter > 0:
            yield [list_to_yield, list_to_yield]