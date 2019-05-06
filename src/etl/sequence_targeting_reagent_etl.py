import logging, uuid
import multiprocessing

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)


class SequenceTargetingReagentETL(ETL):

    sequence_targeting_reagent_query_template = """
    
    USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (s:Species {primaryKey: row.taxonId})

            //Create the Allele node and set properties. primaryKey is required.
            MERGE (o:SequenceTargetingReagent {primaryKey:row.primaryId})
                ON CREATE SET o.name = row.name,
                 o.dateProduced = row.dateProduced,
                 o.release = row.release,
                 o.localId = row.localId,
                 o.globalId = row.globalId,
                 o.uuid = row.uuid,
                 o.modCrossRefCompleteUrl = row.modGlobalCrossRefUrl,
                 o.dataProviders = row.dataProviders,
                 o.dataProvider = row.dataProvider

            MERGE (o)-[:FROM_SPECIES]-(s)

    """
    sequence_targeting_reagent_secondaryids_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (f:SequenceTargetingReagent {primaryKey:row.primaryId})

            MERGE (second:SecondaryId:Identifier {primaryKey:row.secondaryId})
                SET second.name = row.secondary_id
            MERGE (f)-[aka1:ALSO_KNOWN_AS]->(second)
    
    """
    sequence_targeting_reagent_synonyms_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (a:SequenceTargetingReagent {primaryKey:row.primaryId})

            MERGE(syn:Synonym:Identifier {primaryKey:row.synonym})
                SET syn.name = row.synonym
            MERGE (a)-[aka2:ALSO_KNOWN_AS]->(syn)
    
    """
    sequence_targeting_reagent_target_genes_template = """
    USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (a:SequenceTargetingReagent {primaryKey:row.primaryId})
            MATCH (g:Gene {primaryKey:row.geneId})
            
            MERGE (a)-[:TARGETS]-(g)
    
    """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            p.start()
            thread_pool.append(p)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, sub_type):

        logger.info("Loading Sequence Targeting Reagent Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        logger.info(filepath)
        data = JSONFile().get_data(filepath)
        logger.info("Finished Loading Sequence Targeting Reagent Data: %s" % sub_type.get_data_provider())

        if data is None:
            logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
            return

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [SequenceTargetingReagentETL.sequence_targeting_reagent_query_template,
             commit_size, "str_data_" + sub_type.get_data_provider() + ".csv"],
            [SequenceTargetingReagentETL.sequence_targeting_reagent_secondaryids_template, commit_size,
             "str_secondaryids_" + sub_type.get_data_provider() + ".csv"],
            [SequenceTargetingReagentETL.sequence_targeting_reagent_synonyms_template, commit_size,
             "str_synonyms_" + sub_type.get_data_provider() + ".csv"]]

        # Obtain the generator
        generators = self.get_generators(data, sub_type.get_data_provider(), batch_size)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, sqtr_data, data_provider, batch_size):
        dataProviders = []
        sqtrs = []
        sqtr_synonyms = []
        sqtr_secondaryIds = []
        modGlobalCrossRefUrl = ""
        tgs = []

        counter = 0
        dateProduced = sqtr_data['metaData']['dateProduced']

        dataProviderObject = sqtr_data['metaData']['dataProvider']

        dataProviderCrossRef = dataProviderObject.get('crossReference')
        dataProvider = dataProviderCrossRef.get('id')
        dataProviderPages = dataProviderCrossRef.get('pages')
        dataProviderCrossRefSet = []

        loadKey = dateProduced + dataProvider + "_SqTR"


        if dataProviderPages is not None:
            for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = ETLHelper.get_page_complete_url(dataProvider, self.xrefUrlMap, dataProvider,
                                                                      dataProviderPage)

                dataProviderCrossRefSet.append(ETLHelper.get_xref_dict(dataProvider, dataProvider, dataProviderPage,
                                                                       dataProviderPage, dataProvider,
                                                                       crossRefCompleteUrl,
                                                                       dataProvider + dataProviderPage))

                dataProviders.append(dataProvider)
                logger.info("data provider: " + dataProvider)

        for sqtrRecord in sqtr_data['data']:
            counter = counter + 1
            globalId = sqtrRecord['primaryId']
            localId = globalId.split(":")[1]

            if self.testObject.using_test_data() is True:
                is_it_test_entry = self.testObject.check_for_test_id_entry(globalId)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            for sid in sqtrRecord.get('secondaryIds'):
                sqtr_secondaryId_dataset = {
                    "primaryId": sqtrRecord.get('primaryId'),
                    "secondaryId": sid
                }
                sqtr_secondaryIds.append(sqtr_secondaryId_dataset)

            for syn in sqtrRecord.get('synonyms'):
                syn_dataset = {
                    "primaryId": sqtrRecord.get('primaryId'),
                    "synonym": syn
                }
                sqtr_synonyms.append(syn_dataset)

            for tg in sqtrRecord.get('targetGeneIds'):
                tg_dataset = {
                    "primaryId": sqtrRecord.get('primaryId'),
                    "geneId": tg
                }
                tgs.append(tg_dataset)

            if 'crossReferences' in sqtrRecord:

                for crossRef in sqtrRecord['modCrossReference']:
                    crossRefId = crossRef.get('id')
                    local_crossref_id = crossRefId.split(":")[1]
                    prefix = crossRef.get('id').split(":")[0]
                    pages = crossRef.get('pages')

                    # some pages collection have 0 elements
                    if pages is not None and len(pages) > 0:
                        for page in pages:
                            if page == 'sequence_targeting_reagent':
                                modGlobalCrossRefUrl = ETLHelper.get_page_complete_url(local_crossref_id,
                                                                                      self.xrefUrlMap, prefix, page)


            sqtr_dataset = {
                "primaryId": sqtrRecord.get('primaryId'),
                "name": sqtrRecord.get('name'),
                "globalId": globalId,
                "localId": localId,
                "soTerm": sqtrRecord.get('soTermId'),
                "taxonId": sqtrRecord.get('taxonId'),
                "dataProviders": dataProviders,
                "dateProduced": dateProduced,
                "loadKey": loadKey,
                "modGlobalCrossRefUrl": modGlobalCrossRefUrl,
                "dataProvider": data_provider
            }
            sqtrs.append(sqtr_dataset)



            if counter == batch_size:
                yield [sqtrs, sqtr_secondaryIds, sqtr_synonyms, tgs]
                sqtrs = []
                sqtr_secondaryIds = []
                sqtr_synonyms = []
                tgs = []
                counter = 0

        if counter > 0:
            yield [sqtrs, sqtr_secondaryIds, sqtr_synonyms, tgs]
