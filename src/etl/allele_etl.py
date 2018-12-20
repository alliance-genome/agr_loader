import logging
import multiprocessing
import uuid

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)


class AlleleETL(ETL):

    allele_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:Gene {primaryKey: row.geneId})
            MATCH (s:Species {primaryKey: row.taxonId})

            //Create the Allele node and set properties. primaryKey is required.
            MERGE (o:Feature {primaryKey:row.primaryId})
                ON CREATE SET o.symbol = row.symbol,
                 o.taxonId = row.taxonId,
                 o.dateProduced = row.dateProduced,
                 o.release = row.release,
                 o.localId = row.localId,
                 o.globalId = row.globalId,
                 o.uuid = row.uuid,
                 o.symbolText = row.symbolText,
                 o.modCrossRefCompleteUrl = row.modGlobalCrossRefId,
                 o.dataProviders = row.dataProviders,
                 o.dataProvider = row.dataProvider

            MERGE (o)-[:FROM_SPECIES]-(s)

            MERGE (o)<-[:IS_ALLELE_OF]->(g) """

    allele_secondaryids_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (f:Feature {primaryKey:row.data_id})

            MERGE (second:SecondaryId:Identifier {primaryKey:row.secondary_id})
                SET second.name = row.secondary_id
            MERGE (f)-[aka1:ALSO_KNOWN_AS]->(second) """
    
    allele_synonyms_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (f:Feature {primaryKey:row.data_id})

            MERGE(syn:Synonym:Identifier {primaryKey:row.synonym})
                SET syn.name = row.synonym
            MERGE (f)-[aka2:ALSO_KNOWN_AS]->(syn) """

    allele_xrefs_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Feature {primaryKey:row.dataId}) """ + ETLHelper.get_cypher_xref_text()

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
        
        logger.info("Loading Allele Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        data = JSONFile().get_data(filepath)
        logger.info("Finished Loading Allele Data: %s" % sub_type.get_data_provider())

        if data == None:
            logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
            return

        # This order is the same as the lists yielded from the get_generators function.    
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [AlleleETL.allele_query_template, commit_size, "allele_data_" + sub_type.get_data_provider() + ".csv"],
            [AlleleETL.allele_secondaryids_template, commit_size, "allele_secondaryids_" + sub_type.get_data_provider() + ".csv"],
            [AlleleETL.allele_synonyms_template, commit_size, "allele_synonyms_" + sub_type.get_data_provider() + ".csv"],
            [AlleleETL.allele_xrefs_template, commit_size, "allele_xrefs_" + sub_type.get_data_provider() + ".csv"],
        ]

        # Obtain the generator
        generators = self.get_generators(data, sub_type.get_data_provider(), batch_size)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, allele_data, data_provider, batch_size):

        dataProviders = []
        release = ""
        alleles = []
        allele_synonyms = []
        allele_secondaryIds = []
        crossReferenceList = []

        counter = 0
        dateProduced = allele_data['metaData']['dateProduced']

        dataProviderObject = allele_data['metaData']['dataProvider']

        dataProviderCrossRef = dataProviderObject.get('crossReference')
        dataProvider = dataProviderCrossRef.get('id')
        dataProviderPages = dataProviderCrossRef.get('pages')
        dataProviderCrossRefSet = []

        loadKey = dateProduced + dataProvider + "_BGI"

        #TODO: get SGD to fix their files.

        if dataProviderPages is not None:
            for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = ETLHelper.get_page_complete_url(dataProvider, self.xrefUrlMap, dataProvider, dataProviderPage)

                dataProviderCrossRefSet.append(ETLHelper.get_xref_dict(dataProvider, dataProvider, dataProviderPage,
                                                                             dataProviderPage, dataProvider, crossRefCompleteUrl,
                                                                             dataProvider + dataProviderPage))

                dataProviders.append(dataProvider)
                logger.info("data provider: " + dataProvider)

        if 'release' in allele_data['metaData']:
            release = allele_data['metaData']['release']

        for alleleRecord in allele_data['data']:
            counter = counter + 1
            globalId = alleleRecord['primaryId']
            localId = globalId.split(":")[1]
            modGlobalCrossRefId = ""

            if self.testObject.using_test_data() is True:
                is_it_test_entry = self.testObject.check_for_test_id_entry(globalId)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            allele_dataset = {
                "symbol": alleleRecord.get('symbol'),
                "geneId": alleleRecord.get('gene'),
                "primaryId": alleleRecord.get('primaryId'),
                "globalId": globalId,
                "localId": localId,
                "taxonId": alleleRecord.get('taxonId'),
                "dataProviders": dataProviders,
                "dateProduced": dateProduced,
                "loadKey": loadKey,
                "release": release,
                "modGlobalCrossRefId": modGlobalCrossRefId,
                "uuid": str(uuid.uuid4()),
                "dataProvider": data_provider,
                "symbolText": alleleRecord.get('symbolText'),
                "modGlobalCrossRefId": modGlobalCrossRefId

            }

            alleles.append(allele_dataset)

            if 'crossReferences' in alleleRecord:

                for crossRef in alleleRecord['crossReferences']:
                    crossRefId = crossRef.get('id')
                    local_crossref_id = crossRefId.split(":")[1]
                    prefix = crossRef.get('id').split(":")[0]
                    pages = crossRef.get('pages')

                    # some pages collection have 0 elements
                    if pages is not None and len(pages) > 0:
                        for page in pages:
                            if page == 'allele':
                                modGlobalCrossRefId = ETLHelper.get_page_complete_url(local_crossref_id, self.xrefUrlMap, prefix, page)
                                xref = ETLHelper.get_xref_dict(local_crossref_id, prefix, page, page, crossRefId, modGlobalCrossRefId, crossRefId+page)
                                xref['dataId'] = globalId
                                crossReferenceList.append(xref)

            if 'synonyms' in alleleRecord:
                for syn in alleleRecord.get('synonyms'):
                    allele_synonym = {
                        "data_id": alleleRecord.get('primaryId'),
                        "synonym": syn.strip()
                    }
                    allele_synonyms.append(allele_synonym)

            if 'secondaryIds' in alleleRecord:
                for secondaryId in alleleRecord.get('secondaryIds'):
                    allele_secondaryId = {
                        "data_id": alleleRecord.get('primaryId'),
                        "secondary_id": secondaryId
                    }
                    allele_secondaryIds.append(allele_secondaryId)

            if counter == batch_size:
                yield [alleles, allele_secondaryIds, allele_synonyms, crossReferenceList]
                alleles = []
                allele_secondaryIds = []
                allele_synonyms = []
                crossReferenceList = []
                counter = 0

        if counter > 0:
            yield [alleles, allele_secondaryIds, allele_synonyms, crossReferenceList]